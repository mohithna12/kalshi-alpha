//! Tests for the REST client's pure logic: limiter arithmetic, clock skew,
//! discovery reconciliation, and the price-grid observation schema.
//!
//! Network behaviour is not tested here; it is covered by the Stage 1 demo run.

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use kalshi_common::Ticker;
use kalshi_ingest::rest::{
    observations_from_discovery, ClockSkew, DiscoveryPass, Market, MarketRegistry,
    PriceRangeSource, RateLimiter,
};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Token bucket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bucket_starts_full_and_spends_without_waiting() {
    // An idle client legitimately holds a full bucket, so the first burst
    // should not be throttled.
    let limiter = RateLimiter::new(200, 200);
    let started = Instant::now();
    for _ in 0..20 {
        limiter.acquire(10).await;
    }
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "a full 200-token bucket should absorb 20x10 tokens immediately"
    );
}

#[tokio::test]
async fn bucket_throttles_once_the_budget_is_spent() {
    // Capacity 20, refill 20/s, cost 10: two immediate, then ~500ms per token.
    let limiter = RateLimiter::new(20, 20);
    limiter.acquire(10).await;
    limiter.acquire(10).await;

    let started = Instant::now();
    limiter.acquire(10).await;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(400),
        "third request should have waited for refill, waited {waited:?}"
    );
}

#[tokio::test]
async fn bucket_refills_on_elapsed_time_not_per_request() {
    // Spend the bucket, idle, and confirm the accrued budget is spendable --
    // i.e. refill is a function of wall-clock elapsed time, not of how many
    // calls were made.
    let limiter = RateLimiter::new(100, 100);
    limiter.acquire(100).await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // ~30 tokens should have accrued over 300ms at 100/s.
    let started = Instant::now();
    limiter.acquire(20).await;
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "idle time did not credit the bucket"
    );
}

#[tokio::test]
async fn bucket_can_be_reconfigured_from_the_reported_tier() {
    let limiter = RateLimiter::new(10, 10);
    limiter.acquire(10).await;
    // Reported tier is much larger; the bucket resets full at the new size.
    limiter.reconfigure(1_000, 2_000).await;
    let started = Instant::now();
    for _ in 0..50 {
        limiter.acquire(10).await;
    }
    assert!(started.elapsed() < Duration::from_millis(200));
}

// ---------------------------------------------------------------------------
// Clock skew
// ---------------------------------------------------------------------------

#[test]
fn parses_the_http_date_header_and_computes_skew() {
    let server = "Wed, 21 Oct 2026 07:28:00 GMT";
    let local = Utc.with_ymd_and_hms(2026, 10, 21, 7, 28, 0).unwrap();
    let skew = ClockSkew::from_date_header(server, local).expect("parses");
    assert_eq!(skew.delta_ms, 0);
    assert!(!skew.is_concerning());
}

#[test]
fn skew_sign_says_which_clock_is_ahead() {
    let server = "Wed, 21 Oct 2026 07:28:00 GMT";

    // Local ahead of the exchange => positive.
    let ahead = Utc.with_ymd_and_hms(2026, 10, 21, 7, 28, 10).unwrap();
    let skew = ClockSkew::from_date_header(server, ahead).expect("parses");
    assert_eq!(skew.delta_ms, 10_000);
    assert!(skew.is_concerning());

    // Local behind => negative, and magnitude is what matters.
    let behind = Utc.with_ymd_and_hms(2026, 10, 21, 7, 27, 50).unwrap();
    let skew = ClockSkew::from_date_header(server, behind).expect("parses");
    assert_eq!(skew.delta_ms, -10_000);
    assert!(skew.is_concerning());
}

#[test]
fn skew_thresholds_bracket_the_documented_levels() {
    let server = "Wed, 21 Oct 2026 07:28:00 GMT";
    let at = |secs: i64| {
        let local =
            Utc.with_ymd_and_hms(2026, 10, 21, 7, 28, 0).unwrap() + ChronoDuration::seconds(secs);
        ClockSkew::from_date_header(server, local).expect("parses")
    };
    assert!(
        !at(4).is_concerning(),
        "4s should be below the WARN threshold"
    );
    assert!(at(5).is_concerning(), "5s should reach the WARN threshold");
    assert!(at(-5).is_concerning());
    assert_eq!(ClockSkew::WARN_MS, 5_000);
    assert_eq!(ClockSkew::ERROR_MS, 30_000);
}

#[test]
fn malformed_date_headers_yield_no_skew_rather_than_a_wrong_one() {
    let local = Utc::now();
    for bad in [
        "",
        "not a date",
        "1700000000",
        "Wed, 99 Xxx 2026 07:28:00 GMT",
    ] {
        assert_eq!(
            ClockSkew::from_date_header(bad, local),
            None,
            "{bad:?} should not parse"
        );
    }
}

// ---------------------------------------------------------------------------
// Discovery is a loop: registry + reconciliation
// ---------------------------------------------------------------------------

fn t(s: &str) -> Ticker {
    Ticker::parse(s).expect("valid ticker")
}

#[test]
fn startup_seeds_the_registry() {
    let mut registry = MarketRegistry::new();
    let added = registry.observe_startup(vec![t("KXNFL-A"), t("KXNFL-B")], Utc::now());
    assert_eq!(added.len(), 2);
    assert_eq!(registry.len(), 2);
    assert!(registry.contains(&t("KXNFL-A")));
}

#[test]
fn the_live_lifecycle_path_grows_the_registry() {
    let mut registry = MarketRegistry::new();
    registry.observe_startup(vec![t("KXNFL-A")], Utc::now());
    assert!(registry.observe_lifecycle(t("KXNFL-C"), Utc::now()));
    // Idempotent: a repeat event is not a new market.
    assert!(!registry.observe_lifecycle(t("KXNFL-C"), Utc::now()));
    assert_eq!(registry.len(), 2);
}

#[test]
fn reconciliation_is_clean_when_the_live_path_kept_up() {
    let mut registry = MarketRegistry::new();
    registry.observe_startup(vec![t("KXNFL-A")], Utc::now());
    registry.observe_lifecycle(t("KXNFL-B"), Utc::now());

    // A later crawl sees exactly what we already know.
    let result = registry.reconcile(vec![t("KXNFL-A"), t("KXNFL-B")], Utc::now());
    assert!(result.is_clean());
    assert!(result.missed_by_live_path.is_empty());
}

#[test]
fn reconciliation_flags_markets_the_live_path_missed() {
    // This is the bug signal: a market that neither startup discovery nor the
    // lifecycle feed reported was unsubscribed, and that interval's data is
    // permanently gone.
    let mut registry = MarketRegistry::new();
    registry.observe_startup(vec![t("KXNFL-A")], Utc::now());

    let result = registry.reconcile(vec![t("KXNFL-A"), t("KXNFL-MISSED")], Utc::now());
    assert!(!result.is_clean(), "a missed market must not read as clean");
    assert_eq!(result.missed_by_live_path, vec![t("KXNFL-MISSED")]);

    // And it is now known, so it is not re-reported on the next pass.
    let second = registry.reconcile(vec![t("KXNFL-A"), t("KXNFL-MISSED")], Utc::now());
    assert!(second.is_clean());
}

#[test]
fn registry_records_how_each_market_was_learned() {
    let mut registry = MarketRegistry::new();
    let now = Utc::now();
    registry.observe_startup(vec![t("KXNFL-A")], now);
    registry.observe_lifecycle(t("KXNFL-B"), now);
    registry.reconcile(vec![t("KXNFL-C")], now);

    use kalshi_ingest::rest::LearnedVia;
    assert_eq!(
        registry.get(&t("KXNFL-A")).map(|m| m.learned_via),
        Some(LearnedVia::StartupDiscovery)
    );
    assert_eq!(
        registry.get(&t("KXNFL-B")).map(|m| m.learned_via),
        Some(LearnedVia::Lifecycle)
    );
    assert_eq!(
        registry.get(&t("KXNFL-C")).map(|m| m.learned_via),
        Some(LearnedVia::Reconciliation)
    );
}

// ---------------------------------------------------------------------------
// price_ranges: observation vs event
// ---------------------------------------------------------------------------

fn market_json(ticker: &str) -> serde_json::Value {
    serde_json::json!({
        "ticker": ticker,
        "event_ticker": "KXNFLGAME-25SEP09",
        "status": "active",
        "price_level_structure": "penny_with_fine_tails",
        "price_ranges": [
            {"start": "0.0100", "end": "0.9900", "step": "0.0100"}
        ]
    })
}

#[test]
fn discovery_observations_carry_no_effective_time() {
    // A discovery read tells us the grid as of when we looked. It says nothing
    // about when that grid took effect, so effective_at must be null rather
    // than being backfilled with the observation time -- otherwise "the grid
    // was in force from 15:04" and "the grid was already in force at 15:04"
    // become indistinguishable months later.
    let raw = market_json("KXNFLGAME-25SEP09-KC");
    let market: Market = serde_json::from_value(raw.clone()).expect("parses");
    let now = Utc::now();
    let pass = DiscoveryPass {
        started_at: now,
        finished_at: now,
        cursors: vec![String::new()],
        markets: vec![market],
        raw_markets: vec![raw],
    };

    let observations = observations_from_discovery(&pass);
    assert_eq!(observations.len(), 1);
    let obs = &observations[0];
    assert_eq!(obs.source, PriceRangeSource::Discovery);
    assert_eq!(
        obs.effective_at, None,
        "discovery must not invent an effective time"
    );
    assert_eq!(obs.observed_at, now, "observed_at is our receive clock");
    assert_eq!(obs.market, t("KXNFLGAME-25SEP09-KC"));
}

#[test]
fn observations_retain_the_raw_grid_json() {
    // If this crate's reading of the price_ranges shape is ever wrong, the raw
    // JSON allows re-deriving the grid without re-capturing the season.
    let raw = market_json("KXNFLGAME-25SEP09-KC");
    let market: Market = serde_json::from_value(raw.clone()).expect("parses");
    let pass = DiscoveryPass {
        started_at: Utc::now(),
        finished_at: Utc::now(),
        cursors: vec![String::new()],
        markets: vec![market],
        raw_markets: vec![raw],
    };
    let observations = observations_from_discovery(&pass);
    let obs = &observations[0];
    assert!(obs.ranges_raw.contains("0.0100"));
    assert!(obs.ranges_raw.contains("step"));
    assert_eq!(
        obs.price_level_structure.as_deref(),
        Some("penny_with_fine_tails")
    );
}

#[test]
fn market_parses_the_documented_grid_into_a_usable_price_ranges() {
    use kalshi_common::Px;
    let market: Market =
        serde_json::from_value(market_json("KXNFLGAME-25SEP09-KC")).expect("parses");
    assert_eq!(market.price_ranges.bands().len(), 1);
    assert!(market
        .price_ranges
        .is_valid(Px::parse_dollars("0.4300").expect("valid")));
    assert!(!market
        .price_ranges
        .is_valid(Px::parse_dollars("0.4350").expect("valid")));
}

#[test]
fn a_market_with_no_price_ranges_yields_an_empty_grid_not_a_default_one() {
    // Capture must not invent a tick grid it was not given.
    let market: Market = serde_json::from_value(serde_json::json!({
        "ticker": "KXNFLGAME-25SEP09-KC"
    }))
    .expect("parses");
    assert!(market.price_ranges.is_empty());
}

// ---------------------------------------------------------------------------
// Pagination bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn a_discovery_pass_records_its_window_and_cursor_sequence() {
    // A crawl is not an atomic snapshot: markets can be created mid-walk. We
    // do not try to fix that, we record exactly what window the pass covers so
    // the reconciliation loop can cover the gap and a suspicious pass can be
    // replayed.
    let started = Utc::now();
    let finished = started + ChronoDuration::seconds(4);
    let pass = DiscoveryPass {
        started_at: started,
        finished_at: finished,
        cursors: vec![String::new(), "cur_a".to_owned(), "cur_b".to_owned()],
        markets: vec![],
        raw_markets: vec![],
    };
    assert_eq!(pass.page_count(), 3);
    assert_eq!(pass.cursors[0], "", "first page is always the empty cursor");
    assert!(pass.finished_at > pass.started_at);
}
