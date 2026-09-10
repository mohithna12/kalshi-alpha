//! Kalshi REST client and market discovery.
//!
//! # Read-only by construction
//!
//! **There is exactly one request primitive in this module — [`RestClient::get_json`]
//! — and it takes no request body.** Every Kalshi write endpoint (order creation,
//! amendment, cancellation, batch operations) is a POST/PUT/DELETE carrying a
//! body, so the capability to reach one is absent from this type's surface
//! rather than merely unused. Do not add a body parameter, a `post` helper, or a
//! generic `request` escape hatch. `tests/read_only_guard.rs` is a backstop, not
//! the guarantee.
//!
//! # Rate limiting
//!
//! Kalshi meters by token bucket, not requests per second. Every request in this
//! client — including each page of the startup crawl, which is by far the
//! burstiest thing the daemon does — passes through the same [`RateLimiter`].
//! There is no bypass path.
//!
//! Verified against <https://docs.kalshi.com>, read 2026-08-28.

use crate::auth::{AuthError, Credentials};
use chrono::{DateTime, Utc};
use kalshi_common::{PriceRanges, Ticker};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

// ===========================================================================
// Token bucket
// ===========================================================================

/// Tokens are tracked in thousandths so sub-token refill over short intervals
/// does not round away to nothing.
const MILLI: i64 = 1_000;

/// A wall-clock token bucket.
///
/// # Refill is by elapsed time, not by request
///
/// The bucket refills continuously against a monotonic clock, matching how
/// Kalshi describes its own buckets: "The bucket refills continuously at your
/// per-second budget, up to its capacity... There are no fixed windows and no
/// per-second resets." A per-request or per-window approximation would either
/// under-use the budget or overshoot it in bursts.
///
/// [`Instant`] is monotonic, so an NTP step cannot make the limiter believe
/// hours of budget accrued.
#[derive(Debug)]
struct Bucket {
    /// Tokens currently held, in milli-tokens.
    tokens: i64,
    /// Maximum milli-tokens the bucket can hold.
    capacity: i64,
    /// Milli-tokens added per second.
    refill_rate: i64,
    last_refill: Instant,
}

impl Bucket {
    fn new(refill_rate_tokens: u32, capacity_tokens: u32, now: Instant) -> Bucket {
        let capacity = i64::from(capacity_tokens) * MILLI;
        Bucket {
            // Start full: an idle client legitimately holds a full bucket.
            tokens: capacity,
            capacity,
            refill_rate: i64::from(refill_rate_tokens) * MILLI,
            last_refill: now,
        }
    }

    /// Credit the bucket for time elapsed since the last refill.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        if elapsed.is_zero() {
            return;
        }
        let millis = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
        let credit = self
            .refill_rate
            .saturating_mul(millis)
            .saturating_div(1_000);
        self.tokens = self.tokens.saturating_add(credit).min(self.capacity);
        self.last_refill = now;
    }

    /// How long until the bucket can cover `cost` tokens. Zero if it can now.
    fn wait_for(&self, cost_tokens: u32) -> Duration {
        let cost = i64::from(cost_tokens) * MILLI;
        if self.tokens >= cost {
            return Duration::ZERO;
        }
        if self.refill_rate <= 0 {
            return Duration::MAX;
        }
        let deficit = cost - self.tokens;
        let millis = deficit
            .saturating_mul(1_000)
            .saturating_div(self.refill_rate);
        Duration::from_millis(u64::try_from(millis.max(1)).unwrap_or(u64::MAX))
    }

    fn spend(&mut self, cost_tokens: u32) {
        self.tokens = self.tokens.saturating_sub(i64::from(cost_tokens) * MILLI);
    }
}

/// Shared token-bucket limiter. Every request in this client goes through it.
#[derive(Debug)]
pub struct RateLimiter {
    bucket: tokio::sync::Mutex<Bucket>,
}

impl RateLimiter {
    #[must_use]
    pub fn new(refill_rate_tokens: u32, capacity_tokens: u32) -> RateLimiter {
        RateLimiter {
            bucket: tokio::sync::Mutex::new(Bucket::new(
                refill_rate_tokens,
                capacity_tokens,
                Instant::now(),
            )),
        }
    }

    /// Block until `cost` tokens are available, then spend them.
    pub async fn acquire(&self, cost_tokens: u32) {
        loop {
            let wait = {
                let mut bucket = self.bucket.lock().await;
                bucket.refill(Instant::now());
                let wait = bucket.wait_for(cost_tokens);
                if wait.is_zero() {
                    bucket.spend(cost_tokens);
                    return;
                }
                wait
            };
            debug!(
                wait_ms = wait.as_millis(),
                cost_tokens, "rate limited locally"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// Reconfigure from the tier the exchange actually reports.
    pub async fn reconfigure(&self, refill_rate_tokens: u32, capacity_tokens: u32) {
        let mut bucket = self.bucket.lock().await;
        *bucket = Bucket::new(refill_rate_tokens, capacity_tokens, Instant::now());
    }
}

// ===========================================================================
// Errors
// ===========================================================================

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("signing request to {path}")]
    Auth {
        path: String,
        #[source]
        source: AuthError,
    },
    #[error("HTTP transport error for {path}")]
    Transport {
        path: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("{path} returned {status}: {body}")]
    Status {
        path: String,
        status: u16,
        body: String,
    },
    #[error("decoding response from {path}")]
    Decode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("gave up on {path} after {attempts} attempts")]
    Exhausted { path: String, attempts: u32 },
}

// ===========================================================================
// Clock skew
// ===========================================================================

/// Difference between this machine's clock and the exchange's, from the `Date`
/// response header.
///
/// # Why this is worth a dedicated check
///
/// Every signature embeds a millisecond timestamp. If NTP drifts far enough,
/// the exchange rejects signatures as stale — and the failure surfaces as a
/// blanket 401, which looks exactly like a bad key, a wrong path, or a wrong
/// salt length. Nothing in the error says "your clock is wrong." Measuring the
/// delta explicitly converts a baffling 3am outage into an obvious one.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClockSkew {
    /// Positive means the local clock is ahead of the exchange.
    pub delta_ms: i64,
}

impl ClockSkew {
    /// WARN beyond this: drifting, not yet fatal.
    pub const WARN_MS: i64 = 5_000;
    /// ERROR beyond this: signatures are at real risk of rejection.
    pub const ERROR_MS: i64 = 30_000;

    /// Compute skew from an HTTP `Date` header value and a local timestamp.
    ///
    /// `Date` is RFC 7231 / RFC 2822 formatted and has one-second resolution,
    /// so the measurement is only good to about a second. That is far finer
    /// than the thresholds we care about.
    #[must_use]
    pub fn from_date_header(date_header: &str, local: DateTime<Utc>) -> Option<ClockSkew> {
        let server = DateTime::parse_from_rfc2822(date_header).ok()?;
        Some(ClockSkew {
            delta_ms: local.timestamp_millis() - server.timestamp_millis(),
        })
    }

    #[must_use]
    pub fn is_concerning(self) -> bool {
        self.delta_ms.abs() >= Self::WARN_MS
    }

    /// Emit at the severity the magnitude deserves.
    pub fn log(self) {
        let delta_ms = self.delta_ms;
        if delta_ms.abs() >= Self::ERROR_MS {
            error!(
                delta_ms,
                "local clock differs from the exchange by more than 30s; \
                 signed request timestamps are likely to be rejected as stale. \
                 Check NTP before debugging auth."
            );
        } else if delta_ms.abs() >= Self::WARN_MS {
            warn!(
                delta_ms,
                "local clock differs from the exchange by more than 5s; \
                 check NTP -- clock drift surfaces as unexplained 401s"
            );
        } else {
            info!(delta_ms, "clock skew against exchange is within tolerance");
        }
    }
}

/// What a single response told us, beyond its body.
#[derive(Clone, Debug)]
pub struct ResponseMeta {
    pub status: u16,
    /// `None` when the response carried no parsable `Date` header.
    pub clock_skew: Option<ClockSkew>,
    /// Our receive clock, stamped as early as the HTTP layer allows.
    pub received_at: DateTime<Utc>,
}

// ===========================================================================
// Account limits
// ===========================================================================

#[derive(Clone, Debug, Deserialize)]
pub struct BucketLimit {
    pub refill_rate: u32,
    pub bucket_capacity: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AccountLimits {
    pub usage_tier: String,
    pub read: BucketLimit,
    pub write: BucketLimit,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EndpointCosts {
    pub default_cost: u32,
}

/// What the client believes about its own budget, and how it came to believe it.
///
/// Distinguishing these matters: an assumed tier that is wrong produces either
/// needless throttling or a stream of 429s, and either way we should be able to
/// tell from the log which happened.
#[derive(Clone, Debug)]
pub enum TierKnowledge {
    /// `GET /account/limits` answered; these are the real numbers.
    Reported {
        tier: String,
        read_refill: u32,
        read_capacity: u32,
    },
    /// The endpoint failed. We fall back to conservative defaults and say so.
    /// Never silently assume Basic.
    Unknown { reason: String },
}

// ===========================================================================
// Market metadata
// ===========================================================================

/// Where a `price_ranges` observation came from.
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceRangeSource {
    /// Read from `GET /markets`. Tells us the grid *as of when we looked*, and
    /// nothing about when it took effect.
    Discovery,
    /// A `price_level_structure_updated` lifecycle message. Tells us the grid
    /// changed, and when.
    Lifecycle,
}

/// One observation of a market's tick grid.
///
/// # Observation time and effective time are different columns, deliberately
///
/// A discovery read says "this was the grid when I looked at 15:04". A
/// lifecycle event says "the grid changed at 14:32". Collapsing both into a
/// single `effective_from` makes those indistinguishable after the fact, and
/// the question "what grid was in force at 14:32 on Nov 8" then has no
/// answer — the discovery row would claim the grid began at 15:04 when it may
/// have been in force for hours.
///
/// So: [`observed_at`](Self::observed_at) is always our own receive clock and
/// is never null; [`effective_at`](Self::effective_at) is populated only when
/// the wire actually gave us a change time.
#[derive(Clone, Debug)]
pub struct PriceRangeObservation {
    pub market: Ticker,
    /// Our receive clock. Always present.
    pub observed_at: DateTime<Utc>,
    pub source: PriceRangeSource,
    /// When the grid took effect, when the wire said so. `None` for discovery
    /// reads, which carry no such information.
    pub effective_at: Option<DateTime<Utc>>,
    pub price_level_structure: Option<String>,
    pub ranges: PriceRanges,
    /// The raw JSON as received, so the parse can be redone later if this
    /// crate's understanding of the shape turns out to be wrong.
    pub ranges_raw: String,
}

/// A market as returned by `GET /markets`.
///
/// Only the fields capture needs are typed. The complete response is retained
/// in [`raw`](Self::raw) so nothing is lost to a field we did not anticipate.
#[derive(Clone, Debug, Deserialize)]
pub struct Market {
    pub ticker: String,
    pub event_ticker: Option<String>,
    pub status: Option<String>,
    pub open_time: Option<DateTime<Utc>>,
    pub close_time: Option<DateTime<Utc>>,
    pub price_level_structure: Option<String>,
    #[serde(default)]
    pub price_ranges: PriceRanges,
}

#[derive(Debug, Deserialize)]
struct GetMarketsResponse {
    #[serde(default)]
    markets: Vec<serde_json::Value>,
    #[serde(default)]
    cursor: String,
}

// ===========================================================================
// Discovery
// ===========================================================================

/// The result of one full paginated crawl.
///
/// # This is not a consistent snapshot, and does not pretend to be
///
/// Markets can be created, opened, or closed while we are walking the cursor,
/// so a market created between page 3 and page 7 may appear in neither. Rather
/// than trying to make the crawl atomic — which the API does not support — we
/// record exactly what the crawl saw and when, and rely on the reconciliation
/// loop plus the live `market_lifecycle_v2` feed to close the gap. The cursor
/// sequence is retained so a suspicious pass can be replayed and audited.
#[derive(Clone, Debug)]
pub struct DiscoveryPass {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// Cursors in the order they were used. The first is always empty.
    pub cursors: Vec<String>,
    pub markets: Vec<Market>,
    /// Raw market JSON, index-aligned with `markets`.
    pub raw_markets: Vec<serde_json::Value>,
}

impl DiscoveryPass {
    #[must_use]
    pub fn page_count(&self) -> usize {
        self.cursors.len()
    }
}

/// How a market first became known to the daemon.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LearnedVia {
    /// The startup crawl.
    StartupDiscovery,
    /// A live `market_lifecycle_v2` `created` event.
    Lifecycle,
    /// A periodic reconciliation pass — meaning **both** of the above missed
    /// it, which is a bug signal.
    Reconciliation,
}

#[derive(Clone, Debug)]
pub struct KnownMarket {
    pub ticker: Ticker,
    pub learned_via: LearnedVia,
    pub first_seen_at: DateTime<Utc>,
}

/// The set of markets the daemon is tracking, and how each was learned.
///
/// # Discovery is a loop, not an init step
///
/// NFL markets are created throughout the week and during games. Three paths
/// feed this registry:
///
/// 1. the startup crawl, which seeds it;
/// 2. live `market_lifecycle_v2` `created` events, which grow it in real time;
/// 3. a periodic full re-crawl, which reconciles.
///
/// If (3) ever finds a market that (1) and (2) both missed, that market was
/// unsubscribed for some interval and its data is permanently gone — the same
/// failure class as a dropped record. So reconciliation discoveries are logged
/// at WARN individually and counted in the metrics line, not treated as
/// routine.
#[derive(Debug, Default)]
pub struct MarketRegistry {
    known: HashMap<Ticker, KnownMarket>,
}

/// What a reconciliation pass turned up.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reconciliation {
    /// Markets the crawl found that neither startup nor the live feed knew
    /// about. **Every entry here is a capture gap.**
    pub missed_by_live_path: Vec<Ticker>,
    /// Markets seen for the first time, total (includes the above).
    pub newly_known: Vec<Ticker>,
}

impl Reconciliation {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.missed_by_live_path.is_empty()
    }
}

impl MarketRegistry {
    #[must_use]
    pub fn new() -> MarketRegistry {
        MarketRegistry::default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.known.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.known.is_empty()
    }

    #[must_use]
    pub fn contains(&self, ticker: &Ticker) -> bool {
        self.known.contains_key(ticker)
    }

    #[must_use]
    pub fn tickers(&self) -> Vec<Ticker> {
        self.known.keys().cloned().collect()
    }

    /// Record a market learned from a live lifecycle event.
    ///
    /// Returns `true` if this was new.
    pub fn observe_lifecycle(&mut self, ticker: Ticker, at: DateTime<Utc>) -> bool {
        self.insert(ticker, LearnedVia::Lifecycle, at)
    }

    /// Record markets from the startup crawl. Returns the newly known ones.
    pub fn observe_startup(
        &mut self,
        tickers: impl IntoIterator<Item = Ticker>,
        at: DateTime<Utc>,
    ) -> Vec<Ticker> {
        let mut added = Vec::new();
        for ticker in tickers {
            if self.insert(ticker.clone(), LearnedVia::StartupDiscovery, at) {
                added.push(ticker);
            }
        }
        added
    }

    /// Reconcile a later crawl against what we already know.
    ///
    /// Anything new here escaped both the startup crawl and the live feed, so
    /// it is reported separately and loudly.
    pub fn reconcile(
        &mut self,
        tickers: impl IntoIterator<Item = Ticker>,
        at: DateTime<Utc>,
    ) -> Reconciliation {
        let mut result = Reconciliation::default();
        for ticker in tickers {
            if self.insert(ticker.clone(), LearnedVia::Reconciliation, at) {
                warn!(
                    market = %ticker,
                    "reconciliation found a market that neither startup discovery \
                     nor the live lifecycle feed reported; it was unsubscribed and \
                     its data for that interval is unrecoverable"
                );
                result.missed_by_live_path.push(ticker.clone());
                result.newly_known.push(ticker);
            }
        }
        result
    }

    fn insert(&mut self, ticker: Ticker, via: LearnedVia, at: DateTime<Utc>) -> bool {
        if self.known.contains_key(&ticker) {
            return false;
        }
        self.known.insert(
            ticker.clone(),
            KnownMarket {
                ticker,
                learned_via: via,
                first_seen_at: at,
            },
        );
        true
    }

    #[must_use]
    pub fn get(&self, ticker: &Ticker) -> Option<&KnownMarket> {
        self.known.get(ticker)
    }
}

// ===========================================================================
// Client
// ===========================================================================

#[derive(Clone, Debug)]
pub struct RestConfig {
    pub base_url: String,
    /// Conservative Basic-tier defaults, used only until `GET /account/limits`
    /// reports the real numbers.
    pub assumed_read_refill: u32,
    pub assumed_read_capacity: u32,
    pub assumed_default_cost: u32,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub max_attempts: u32,
    pub request_timeout: Duration,
}

impl Default for RestConfig {
    fn default() -> RestConfig {
        RestConfig {
            base_url: "https://external-api.demo.kalshi.co/trade-api/v2".to_owned(),
            assumed_read_refill: 200,
            assumed_read_capacity: 200,
            assumed_default_cost: 10,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            max_attempts: 6,
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct RestClient {
    http: reqwest::Client,
    config: RestConfig,
    credentials: Credentials,
    limiter: RateLimiter,
    default_cost: std::sync::atomic::AtomicU32,
    /// Path component of `base_url`, e.g. `/trade-api/v2`.
    ///
    /// # Signatures cover the FULL path, prefix included
    ///
    /// Kalshi's documented example signs
    /// `/trade-api/v2/portfolio/balance` and issues the request against
    /// `base_url + path` where `base_url` is only the host. Because this
    /// client keeps the version prefix inside `base_url`, the prefix has to be
    /// re-attached before signing -- otherwise we sign `/account/limits` while
    /// the server verifies `/trade-api/v2/account/limits`, and every
    /// authenticated call fails with a bare 401 that names nothing.
    ///
    /// Found by calling the API: unauthenticated endpoints worked and every
    /// authenticated one returned `token_authentication_failure`.
    signing_prefix: String,
}

impl RestClient {
    pub fn new(config: RestConfig, credentials: Credentials) -> Result<RestClient, RestError> {
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .user_agent(concat!("kalshi-alpha/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|source| RestError::Transport {
                path: "<client builder>".to_owned(),
                source,
            })?;
        let limiter = RateLimiter::new(config.assumed_read_refill, config.assumed_read_capacity);
        let default_cost = std::sync::atomic::AtomicU32::new(config.assumed_default_cost);
        let signing_prefix = signing_prefix_from(&config.base_url);
        Ok(RestClient {
            http,
            config,
            credentials,
            limiter,
            default_cost,
            signing_prefix,
        })
    }

    /// The exact string signed for `path`: the base URL's path component
    /// followed by the endpoint path, with no query string.
    #[must_use]
    pub fn signing_path(&self, path: &str) -> String {
        format!("{}{}", self.signing_prefix, path)
    }

    fn cost(&self) -> u32 {
        self.default_cost.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// **The only request primitive.** Deliberately has no body parameter.
    ///
    /// Query parameters are sent on the wire but stripped before signing, per
    /// Kalshi's documented rule.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<(T, ResponseMeta), RestError> {
        let mut backoff = self.config.backoff_initial;
        for attempt in 1..=self.config.max_attempts {
            // Every request, including each page of the startup crawl, is paced
            // by the same bucket. There is no bypass.
            self.limiter.acquire(self.cost()).await;

            // Sign the FULL path, including the /trade-api/v2 prefix carried in
            // base_url. Signing the bare endpoint path produces a 401 whose
            // body says only "token authentication failure" -- it names neither
            // the key nor the path, so the cause is invisible from the error.
            let signed_path = self.signing_path(path);
            // The 401 body says only "token authentication failure" -- it names
            // neither the key nor the path, so without this the cause of an
            // auth failure is invisible. Logged at DEBUG; contains no secret.
            debug!(
                signed_path,
                key_id = self.credentials.key_id(),
                "signing request"
            );
            let headers = self
                .credentials
                .sign_rest("GET", &signed_path)
                .map_err(|source| RestError::Auth {
                    path: path.to_owned(),
                    source,
                })?;

            let url = format!("{}{}", self.config.base_url, path);
            let mut request = self.http.get(&url);
            for (name, value) in headers.as_pairs() {
                request = request.header(name, value);
            }
            if !query.is_empty() {
                request = request.query(query);
            }

            let response = request
                .send()
                .await
                .map_err(|source| RestError::Transport {
                    path: path.to_owned(),
                    source,
                })?;

            // Stamp the receive clock as early as possible.
            let received_at = Utc::now();
            let status = response.status();
            let clock_skew = response
                .headers()
                .get(reqwest::header::DATE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| ClockSkew::from_date_header(value, received_at));

            if status.as_u16() == 429 {
                // 429 carries no Retry-After and no X-RateLimit-* headers, and
                // there is no cooldown penalty -- the bucket simply refills. So
                // backoff is blind and self-computed.
                warn!(
                    path,
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    "rate limited by exchange (429); backing off"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(self.config.backoff_max);
                continue;
            }

            let body = response
                .text()
                .await
                .map_err(|source| RestError::Transport {
                    path: path.to_owned(),
                    source,
                })?;

            if !status.is_success() {
                if status.is_server_error() && attempt < self.config.max_attempts {
                    warn!(
                        path,
                        attempt,
                        status = status.as_u16(),
                        "server error; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.config.backoff_max);
                    continue;
                }
                return Err(RestError::Status {
                    path: path.to_owned(),
                    status: status.as_u16(),
                    body: truncate(&body, 512),
                });
            }

            let parsed = serde_json::from_str::<T>(&body).map_err(|source| RestError::Decode {
                path: path.to_owned(),
                source,
            })?;
            return Ok((
                parsed,
                ResponseMeta {
                    status: status.as_u16(),
                    clock_skew,
                    received_at,
                },
            ));
        }
        Err(RestError::Exhausted {
            path: path.to_owned(),
            attempts: self.config.max_attempts,
        })
    }

    /// Confirm the real rate-limit tier and reconfigure the limiter from it.
    ///
    /// Logged at INFO so the assumption is visible in the record rather than
    /// buried in config. On failure we keep the conservative defaults and
    /// report [`TierKnowledge::Unknown`] — we never silently claim Basic.
    pub async fn confirm_tier(&self) -> TierKnowledge {
        match self.get_json::<AccountLimits>("/account/limits", &[]).await {
            Ok((limits, _)) => {
                info!(
                    tier = %limits.usage_tier,
                    read_refill = limits.read.refill_rate,
                    read_capacity = limits.read.bucket_capacity,
                    write_refill = limits.write.refill_rate,
                    "confirmed API tier from GET /account/limits"
                );
                self.limiter
                    .reconfigure(limits.read.refill_rate, limits.read.bucket_capacity)
                    .await;
                TierKnowledge::Reported {
                    tier: limits.usage_tier,
                    read_refill: limits.read.refill_rate,
                    read_capacity: limits.read.bucket_capacity,
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    assumed_refill = self.config.assumed_read_refill,
                    "GET /account/limits failed; tier is UNKNOWN. Continuing with \
                     conservative defaults -- this is not a confirmed Basic tier."
                );
                TierKnowledge::Unknown {
                    reason: err.to_string(),
                }
            }
        }
    }

    /// Learn the real default token cost. Unauthenticated endpoint.
    pub async fn confirm_default_cost(&self) -> Option<u32> {
        match self
            .get_json::<EndpointCosts>("/account/endpoint_costs", &[])
            .await
        {
            Ok((costs, _)) => {
                info!(
                    default_cost = costs.default_cost,
                    "confirmed default token cost"
                );
                self.default_cost
                    .store(costs.default_cost, std::sync::atomic::Ordering::Relaxed);
                Some(costs.default_cost)
            }
            Err(err) => {
                warn!(error = %err, "could not read endpoint costs; keeping assumed default");
                None
            }
        }
    }

    /// Measure clock skew against the exchange and log at an appropriate level.
    pub async fn check_clock_skew(&self) -> Option<ClockSkew> {
        let (_, meta) = self
            .get_json::<serde_json::Value>("/account/endpoint_costs", &[])
            .await
            .ok()?;
        if let Some(skew) = meta.clock_skew {
            skew.log();
        } else {
            warn!("response carried no parsable Date header; clock skew unknown");
        }
        meta.clock_skew
    }

    /// Walk `GET /markets` for one series, following the cursor to exhaustion.
    ///
    /// Each page is paced by the shared limiter. The cursor sequence and the
    /// start/end timestamps are recorded so the caller knows exactly what
    /// window this pass covers — it is not an atomic snapshot.
    pub async fn discover_markets(
        &self,
        series_ticker: &str,
        statuses: &[String],
    ) -> Result<DiscoveryPass, RestError> {
        let started_at = Utc::now();
        let mut cursors = Vec::new();
        let mut markets = Vec::new();
        let mut raw_markets = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        // `GET /markets` accepts only ONE status per request -- supplying a
        // comma-joined list is rejected with
        //   400 bad_request "only one status filter may be supplied"
        // The docs list the accepted values but do not say they are mutually
        // exclusive; this was found by calling the endpoint. So each status is
        // crawled separately and the results merged, de-duplicated by ticker.
        //
        // An empty status list means "no filter", which is a single crawl.
        let passes: Vec<Option<&String>> = if statuses.is_empty() {
            vec![None]
        } else {
            statuses.iter().map(Some).collect()
        };

        for status in passes {
            let mut cursor = String::new();
            loop {
                cursors.push(cursor.clone());
                let mut query = vec![
                    ("series_ticker", series_ticker.to_owned()),
                    // series_ticker requires mve_filter=exclude per the docs.
                    ("mve_filter", "exclude".to_owned()),
                    ("limit", "1000".to_owned()),
                ];
                if let Some(status) = status {
                    query.push(("status", (*status).clone()));
                }
                if !cursor.is_empty() {
                    query.push(("cursor", cursor.clone()));
                }

                let (page, _meta) = self
                    .get_json::<GetMarketsResponse>("/markets", &query)
                    .await?;

                for value in page.markets {
                    match serde_json::from_value::<Market>(value.clone()) {
                        Ok(market) => {
                            // A market can appear under more than one status
                            // pass if it transitions mid-crawl.
                            if seen.insert(market.ticker.clone()) {
                                markets.push(market);
                                raw_markets.push(value);
                            }
                        }
                        Err(err) => {
                            // One unparsable market must not abort discovery of
                            // the rest -- and the raw JSON is kept regardless.
                            warn!(error = %err, "skipping unparsable market in discovery page");
                        }
                    }
                }

                if page.cursor.is_empty() {
                    break;
                }
                cursor = page.cursor;
            }
        }

        let finished_at = Utc::now();
        info!(
            series = series_ticker,
            markets = markets.len(),
            pages = cursors.len(),
            elapsed_ms = (finished_at - started_at).num_milliseconds(),
            "discovery pass complete (not an atomic snapshot)"
        );

        Ok(DiscoveryPass {
            started_at,
            finished_at,
            cursors,
            markets,
            raw_markets,
        })
    }
}

/// Build the price-grid observations implied by a discovery pass.
///
/// Source is [`PriceRangeSource::Discovery`] and `effective_at` is `None`,
/// because a read tells us what the grid *is*, never when it became so.
#[must_use]
pub fn observations_from_discovery(pass: &DiscoveryPass) -> Vec<PriceRangeObservation> {
    let mut out = Vec::new();
    for (market, raw) in pass.markets.iter().zip(pass.raw_markets.iter()) {
        let Ok(ticker) = Ticker::parse(&market.ticker) else {
            warn!(
                ticker = market.ticker,
                "skipping market with unusable ticker"
            );
            continue;
        };
        if !Ticker::is_canonical(&market.ticker) {
            warn!(
                raw_ticker = market.ticker,
                normalized = %ticker,
                "ticker was not uppercase on the wire and has been normalized"
            );
        }
        let ranges_raw = raw
            .get("price_ranges")
            .map(std::string::ToString::to_string)
            .unwrap_or_default();
        out.push(PriceRangeObservation {
            market: ticker,
            observed_at: pass.finished_at,
            source: PriceRangeSource::Discovery,
            effective_at: None,
            price_level_structure: market.price_level_structure.clone(),
            ranges: market.price_ranges.clone(),
            ranges_raw,
        });
    }
    out
}

/// Extract the path component of a base URL, e.g.
/// `https://external-api.kalshi.com/trade-api/v2` -> `/trade-api/v2`.
///
/// Returns an empty string when the base URL is bare host-only, so a client
/// configured with the prefix already stripped still signs correctly.
#[must_use]
pub fn signing_prefix_from(base_url: &str) -> String {
    let without_scheme = base_url
        .split_once("://")
        .map_or(base_url, |(_, rest)| rest);
    match without_scheme.find('/') {
        Some(index) => without_scheme[index..].trim_end_matches('/').to_owned(),
        None => String::new(),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}
