//! Tests for WebSocket protocol logic: message deserialization, sequence
//! tracking, sid lifecycle, the recovery ladder, command encoding, and backoff.
//!
//! Live connection behaviour is covered by the Stage 1 demo run and by
//! `just force-gap`.

use chrono::{Duration as ChronoDuration, Utc};
use kalshi_ingest::wire::{ErrorBody, ServerMessage};
use kalshi_ingest::ws::{
    Backoff, Command, PricingConvention, RecoveryMethod, SeqOutcome, SubscriptionRegistry,
};
use std::time::Duration;

// ---------------------------------------------------------------------------
// use_yes_price: explicit, always
// ---------------------------------------------------------------------------

#[test]
fn orderbook_subscribe_always_states_the_pricing_convention() {
    // The exchange's default for use_yes_price is documented as scheduled to
    // flip from false to true. Relying on it would silently invert the meaning
    // of every NO-side price the day that ships -- no error, no gap, nothing
    // to detect. So the flag is always on the wire.
    for convention in [
        PricingConvention::NoLegPricing,
        PricingConvention::YesLegPricing,
    ] {
        let command = Command::Subscribe {
            id: 1,
            channels: vec!["orderbook_delta".to_owned()],
            market_tickers: vec!["KXNFLGAME-25SEP09-KC".to_owned()],
            use_yes_price: Some(convention.use_yes_price()),
        };
        let json: serde_json::Value =
            serde_json::from_str(&command.to_json().expect("encodes")).expect("valid json");
        assert_eq!(
            json["params"]["use_yes_price"],
            serde_json::json!(convention.use_yes_price()),
            "use_yes_price missing or wrong for {convention:?}"
        );
    }
}

#[test]
fn pricing_convention_has_no_default_and_must_be_configured() {
    // There is deliberately no Default impl -- an unparsable or absent value is
    // a startup error, not a silently chosen convention.
    assert_eq!(
        PricingConvention::parse("no_leg").expect("valid"),
        PricingConvention::NoLegPricing
    );
    assert_eq!(
        PricingConvention::parse("yes_leg").expect("valid"),
        PricingConvention::YesLegPricing
    );
    let err = PricingConvention::parse("").expect_err("empty must not resolve");
    let rendered = err.to_string();
    assert!(
        rendered.contains("no default"),
        "unhelpful error: {rendered}"
    );
    assert!(PricingConvention::parse("true").is_err());
    assert!(PricingConvention::parse("yes").is_err());
}

#[test]
fn the_convention_has_a_stable_label_for_the_parquet_schema() {
    // Written per session so January analysis knows which convention the bytes
    // are in without having to guess.
    assert_eq!(PricingConvention::NoLegPricing.as_str(), "no_leg");
    assert_eq!(PricingConvention::YesLegPricing.as_str(), "yes_leg");
    assert!(!PricingConvention::NoLegPricing.use_yes_price());
    assert!(PricingConvention::YesLegPricing.use_yes_price());
}

// ---------------------------------------------------------------------------
// seq exists only on the orderbook channel
// ---------------------------------------------------------------------------

#[test]
fn only_orderbook_messages_carry_a_sequence_number() {
    // Confirmed against asyncapi.yaml: seq is required on orderbook_snapshot
    // and orderbook_delta, and is not a property at all on ticker, trade,
    // market_lifecycle_v2, event_lifecycle or event_fee_update.
    //
    // The consequence is that a dropped lifecycle message leaves no trace --
    // there is no counter to gap -- which is why REST reconciliation is the
    // only way to learn we missed a market creation.
    let delta: ServerMessage = serde_json::from_str(
        r#"{"type":"orderbook_delta","sid":2,"seq":3,
            "msg":{"market_ticker":"FED-23DEC-T3.00",
                   "market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
                   "price_dollars":"0.960","delta_fp":"-54.00","side":"yes"}}"#,
    )
    .expect("delta parses");
    assert_eq!(delta.seq(), Some(3));

    let lifecycle: ServerMessage = serde_json::from_str(
        r#"{"type":"market_lifecycle_v2","sid":7,
            "msg":{"event_type":"created","market_ticker":"KXNFLGAME-25SEP09-KC"}}"#,
    )
    .expect("lifecycle parses");
    assert_eq!(
        lifecycle.seq(),
        None,
        "lifecycle must not appear to carry a sequence -- gaps there are undetectable"
    );
    assert_eq!(lifecycle.sid(), Some(7));

    let ticker: ServerMessage = serde_json::from_str(
        r#"{"type":"ticker","sid":4,"msg":{"market_ticker":"KXNFLGAME-25SEP09-KC"}}"#,
    )
    .expect("ticker parses");
    assert_eq!(ticker.seq(), None);
}

// ---------------------------------------------------------------------------
// Sequence tracking
// ---------------------------------------------------------------------------

fn registry_with_sid(sid: u64) -> SubscriptionRegistry {
    registry_with_sid_at(sid, Utc::now())
}

fn registry_with_sid_at(sid: u64, now: chrono::DateTime<Utc>) -> SubscriptionRegistry {
    let mut registry = SubscriptionRegistry::new();
    registry.register(
        sid,
        "orderbook_delta".to_owned(),
        vec!["KXNFLGAME-25SEP09-KC".to_owned()],
        now,
    );
    registry
}

#[test]
fn a_new_subscription_is_invalid_until_a_snapshot_arrives() {
    let registry = registry_with_sid(1);
    let state = registry.get(1).expect("registered");
    assert!(
        !state.is_valid(),
        "a subscription must not be readable before its snapshot"
    );
}

#[test]
fn contiguous_sequences_keep_the_book_valid() {
    let now = Utc::now();
    let mut registry = registry_with_sid(1);
    let state = registry.get_mut(1).expect("registered");
    state.accept_snapshot(10, now);
    assert!(state.is_valid());
    assert_eq!(state.observe_seq(11, now), SeqOutcome::InOrder);
    assert_eq!(state.observe_seq(12, now), SeqOutcome::InOrder);
    assert!(state.is_valid());
    assert_eq!(state.gap_count(), 0);
}

#[test]
fn a_skipped_sequence_invalidates_the_book_and_is_counted() {
    // Never interpolate. A gap means the local book is wrong, and it must not
    // be readable again until a fresh snapshot re-seeds it.
    let now = Utc::now();
    let mut registry = registry_with_sid(1);
    let state = registry.get_mut(1).expect("registered");
    state.accept_snapshot(10, now);

    assert_eq!(
        state.observe_seq(14, now),
        SeqOutcome::Gap {
            expected: 11,
            got: 14,
            skipped: 3
        }
    );
    assert!(!state.is_valid(), "a gapped book must not read as valid");
    assert_eq!(state.gap_count(), 1);

    // Still invalid while deltas keep arriving -- only a snapshot restores it.
    assert_eq!(state.observe_seq(15, now), SeqOutcome::InOrder);
    assert!(!state.is_valid());

    state.accept_snapshot(20, now);
    assert!(state.is_valid());
}

#[test]
fn a_sequence_regression_is_reported_rather_than_ignored() {
    let now = Utc::now();
    let mut registry = registry_with_sid(1);
    let state = registry.get_mut(1).expect("registered");
    state.accept_snapshot(10, now);
    assert_eq!(
        state.observe_seq(9, now),
        SeqOutcome::Regression { last: 10, got: 9 }
    );
    assert_eq!(
        state.observe_seq(10, now),
        SeqOutcome::Regression { last: 10, got: 10 },
        "a repeated sequence number is a regression, not in-order"
    );
    // The high-water mark was not rewound by the regression: seq 11 is still
    // the next contiguous value. Rewinding would have made the anomaly vanish.
    assert_eq!(state.observe_seq(11, now), SeqOutcome::InOrder);
}

#[test]
fn invalid_duration_accumulates_across_separate_outages() {
    let start = Utc::now();
    let mut registry = registry_with_sid_at(1, start);
    let state = registry.get_mut(1).expect("registered");

    state.accept_snapshot(1, start);
    // First outage: 5s.
    state.observe_seq(5, start);
    state.accept_snapshot(6, start + ChronoDuration::seconds(5));
    // Second outage: 3s.
    let later = start + ChronoDuration::seconds(10);
    state.observe_seq(20, later);
    state.accept_snapshot(21, later + ChronoDuration::seconds(3));

    let total = state.invalid_duration(later + ChronoDuration::seconds(3));
    assert_eq!(total.num_seconds(), 8, "expected 5s + 3s of invalidity");
    assert_eq!(state.gap_count(), 2);
}

// ---------------------------------------------------------------------------
// sid lifecycle: never reused, never reset
// ---------------------------------------------------------------------------

#[test]
fn a_retired_sid_takes_its_sequence_counter_with_it() {
    // `subscribed` returns a fresh server-generated sid every time, so a
    // resubscribe yields a new sid with a new counter. Carrying the old
    // counter forward would fabricate a gap or, worse, hide one.
    let now = Utc::now();
    let mut registry = registry_with_sid(1);
    registry
        .get_mut(1)
        .expect("registered")
        .accept_snapshot(500, now);

    let retired = registry.retire(1).expect("was registered");
    assert_eq!(retired.sid(), 1);
    assert!(
        registry.get(1).is_none(),
        "a retired sid must be gone, not reset"
    );

    // The server hands us a different sid on resubscribe.
    registry.register(
        2,
        "orderbook_delta".to_owned(),
        vec!["KXNFLGAME-25SEP09-KC".to_owned()],
        now,
    );
    let state = registry.get_mut(2).expect("registered");
    assert!(!state.is_valid(), "the new sid starts invalid");

    // Sequence numbers on the new sid start fresh: seq 1 here is First, not a
    // gap relative to the old sid's 500.
    state.accept_snapshot(1, now);
    assert_eq!(state.observe_seq(2, now), SeqOutcome::InOrder);
    assert_eq!(state.gap_count(), 0);
}

#[test]
fn losing_the_connection_retires_every_sid() {
    let mut registry = registry_with_sid(1);
    registry.register(2, "ticker".to_owned(), vec![], Utc::now());
    assert_eq!(registry.len(), 2);
    registry.retire_all();
    assert!(registry.is_empty());
    assert!(registry.get(1).is_none());
}

#[test]
fn a_gap_on_one_sid_does_not_disturb_another() {
    // The reason orderbook subscriptions shard to one market each: seq is
    // per-sid, so a gap's blast radius is exactly one subscription.
    let now = Utc::now();
    let mut registry = registry_with_sid(1);
    registry.register(
        2,
        "orderbook_delta".to_owned(),
        vec!["OTHER-MARKET".to_owned()],
        now,
    );

    registry.get_mut(1).expect("a").accept_snapshot(1, now);
    registry.get_mut(2).expect("b").accept_snapshot(1, now);

    registry.get_mut(1).expect("a").observe_seq(99, now);

    assert!(!registry.get(1).expect("a").is_valid());
    assert!(
        registry.get(2).expect("b").is_valid(),
        "a gap on sid 1 must not invalidate sid 2"
    );
    assert_eq!(registry.invalid_sids(), vec![1]);
    assert_eq!(registry.total_gaps(), 1);
}

// ---------------------------------------------------------------------------
// Recovery ladder
// ---------------------------------------------------------------------------

#[test]
fn the_recovery_ladder_runs_cheapest_first() {
    assert_eq!(
        RecoveryMethod::LADDER,
        [
            RecoveryMethod::GetSnapshot,
            RecoveryMethod::Resubscribe,
            RecoveryMethod::Reconnect
        ]
    );
    assert_eq!(
        RecoveryMethod::GetSnapshot.escalate(),
        Some(RecoveryMethod::Resubscribe)
    );
    assert_eq!(
        RecoveryMethod::Resubscribe.escalate(),
        Some(RecoveryMethod::Reconnect)
    );
    assert_eq!(
        RecoveryMethod::Reconnect.escalate(),
        None,
        "reconnect is the last rung"
    );
}

#[test]
fn get_snapshot_does_not_modify_the_subscription() {
    // The whole point of the first rung: no sid churn, no teardown, and no
    // other market disturbed.
    let command = Command::GetSnapshot {
        id: 127,
        sid: 456,
        market_tickers: vec!["KXNFLGAME-25SEP09-KC".to_owned()],
    };
    let json: serde_json::Value =
        serde_json::from_str(&command.to_json().expect("encodes")).expect("valid json");
    assert_eq!(json["cmd"], "update_subscription");
    assert_eq!(json["params"]["action"], "get_snapshot");
    assert_eq!(json["params"]["sids"], serde_json::json!([456]));
    assert_eq!(
        json["params"]["market_tickers"],
        serde_json::json!(["KXNFLGAME-25SEP09-KC"])
    );
}

#[test]
fn recovery_methods_are_counted_separately() {
    // So the soak can show which rung actually carries the load.
    let mut registry = registry_with_sid(1);
    registry.record_recovery(RecoveryMethod::GetSnapshot);
    registry.record_recovery(RecoveryMethod::GetSnapshot);
    registry.record_recovery(RecoveryMethod::Reconnect);
    let counts = registry.recovery_counts();
    assert!(counts.contains(&(RecoveryMethod::GetSnapshot, 2)));
    assert!(counts.contains(&(RecoveryMethod::Reconnect, 1)));
}

#[test]
fn terminal_errors_trigger_a_resubscribe_not_a_snapshot_request() {
    use kalshi_ingest::ws::recovery_for_error;
    // 10, 17, 25 are terminal per the spec: the subscription is gone, so
    // get_snapshot against it cannot help.
    for code in [10u32, 17, 25] {
        let error = ErrorBody {
            code,
            msg: "terminal".to_owned(),
            market_ticker: None,
        };
        assert!(error.is_terminal(), "code {code} should be terminal");
        assert_eq!(
            recovery_for_error(&error),
            Some(RecoveryMethod::Resubscribe),
            "code {code} should resubscribe"
        );
    }
    // A non-terminal error needs no recovery.
    let benign = ErrorBody {
        code: 6,
        msg: "already subscribed".to_owned(),
        market_ticker: None,
    };
    assert!(!benign.is_terminal());
    assert_eq!(recovery_for_error(&benign), None);
}

#[test]
fn buffer_overflow_is_recognized_as_our_own_slowness() {
    // Error 25 means the consumer fell behind and the server killed the
    // subscription -- the reason storage must sit behind a channel rather than
    // inline in the read loop.
    let error = ErrorBody {
        code: 25,
        msg: "subscription buffer overflow".to_owned(),
        market_ticker: None,
    };
    assert!(error.is_buffer_overflow());
    assert!(error.is_terminal());
}

// ---------------------------------------------------------------------------
// Deprecated fields must be optional
// ---------------------------------------------------------------------------

#[test]
fn trade_parses_without_any_deprecated_fields() {
    // taker_side, ts and time were protected only until 2026-05-14, which has
    // passed. Their absence must not break deserialization.
    let trade: ServerMessage = serde_json::from_str(
        r#"{"type":"trade","sid":3,
            "msg":{"trade_id":"abc","market_ticker":"KXNFLGAME-25SEP09-KC",
                   "yes_price_dollars":"0.4300","no_price_dollars":"0.5700",
                   "count_fp":"10.00","taker_outcome_side":"yes",
                   "taker_book_side":"bid","is_block_trade":false,
                   "ts_ms":1710000000123}}"#,
    )
    .expect("parses without deprecated fields");
    let ServerMessage::Trade(payload) = trade else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.direction(), Some(kalshi_common::Side::Yes));
    assert_eq!(payload.msg.taker_side, None);
    assert_eq!(payload.msg.ts, None);
}

#[test]
fn trade_prefers_canonical_direction_fields_over_the_deprecated_one() {
    // taker_outcome_side is canonical; taker_side is legacy. If both appear,
    // the canonical one wins.
    let trade: ServerMessage = serde_json::from_str(
        r#"{"type":"trade","sid":3,
            "msg":{"market_ticker":"M","taker_outcome_side":"no",
                   "taker_side":"yes","ts_ms":1}}"#,
    )
    .expect("parses");
    let ServerMessage::Trade(payload) = trade else {
        panic!("wrong variant");
    };
    assert_eq!(
        payload.msg.direction(),
        Some(kalshi_common::Side::No),
        "canonical taker_outcome_side must win over deprecated taker_side"
    );

    // Falls back to the deprecated field only when both canonical ones are
    // absent, so capture survives either side of their removal.
    let legacy: ServerMessage = serde_json::from_str(
        r#"{"type":"trade","sid":3,"msg":{"market_ticker":"M","taker_side":"yes"}}"#,
    )
    .expect("parses");
    let ServerMessage::Trade(payload) = legacy else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.direction(), Some(kalshi_common::Side::Yes));
}

#[test]
fn book_side_maps_to_outcome_side_per_the_spec() {
    // "bid is equivalent to taker_outcome_side yes; ask is equivalent to no."
    for (book_side, expected) in [
        ("bid", kalshi_common::Side::Yes),
        ("ask", kalshi_common::Side::No),
    ] {
        let json = format!(
            r#"{{"type":"trade","sid":1,
                 "msg":{{"market_ticker":"M","taker_book_side":"{book_side}"}}}}"#
        );
        let trade: ServerMessage = serde_json::from_str(&json).expect("parses");
        let ServerMessage::Trade(payload) = trade else {
            panic!("wrong variant");
        };
        assert_eq!(payload.msg.direction(), Some(expected), "for {book_side}");
    }
}

#[test]
fn orderbook_delta_ts_is_a_string_on_this_channel() {
    // Deprecated `ts` is an RFC3339 string on orderbook_delta but integer
    // seconds on trade. Typing it wrong would fail deserialization on live
    // data only.
    let delta: ServerMessage = serde_json::from_str(
        r#"{"type":"orderbook_delta","sid":2,"seq":3,
            "msg":{"market_ticker":"M","market_id":"x","price_dollars":"0.96",
                   "delta_fp":"-54.00","side":"no","ts":"2026-09-01T12:00:00Z",
                   "ts_ms":1710000000123}}"#,
    )
    .expect("parses");
    let ServerMessage::OrderbookDelta(payload) = delta else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.ts.as_deref(), Some("2026-09-01T12:00:00Z"));
    assert_eq!(payload.msg.ts_ms, Some(1710000000123));
}

// ---------------------------------------------------------------------------
// Lifecycle: settlement value and fee overrides
// ---------------------------------------------------------------------------

#[test]
fn determined_events_carry_the_settlement_value() {
    // Ground truth for any later model, arriving on the same stream as prices.
    let message: ServerMessage = serde_json::from_str(
        r#"{"type":"market_lifecycle_v2","sid":9,
            "msg":{"event_type":"determined","market_ticker":"KXNFLGAME-25SEP09-KC",
                   "result":"yes","determination_ts":1710000000,
                   "settlement_value":"1.0000"}}"#,
    )
    .expect("parses");
    let ServerMessage::MarketLifecycle(payload) = message else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.settlement_value.as_deref(), Some("1.0000"));
    assert_eq!(payload.msg.result.as_deref(), Some("yes"));

    // And it parses with the same exact parser used for prices.
    let value =
        kalshi_common::Px::parse_dollars(payload.msg.settlement_value.as_deref().expect("present"))
            .expect("valid fixed-point dollars");
    assert_eq!(value, kalshi_common::Px::ONE_DOLLAR);
}

#[test]
fn price_level_structure_updates_are_recognized_as_grid_changes() {
    let message: ServerMessage = serde_json::from_str(
        r#"{"type":"market_lifecycle_v2","sid":9,
            "msg":{"event_type":"price_level_structure_updated",
                   "market_ticker":"KXNFLGAME-25SEP09-KC",
                   "price_level_structure":"penny",
                   "price_ranges":[{"start":"0.0100","end":"0.9900","step":"0.0100"}]}}"#,
    )
    .expect("parses");
    let ServerMessage::MarketLifecycle(payload) = message else {
        panic!("wrong variant");
    };
    assert!(payload.msg.changes_price_grid());
    assert!(!payload.msg.is_created());
}

#[test]
fn event_fee_updates_are_captured_including_cleared_overrides() {
    // Fee overrides are set and cleared over time and no history is published.
    // A cleared override is itself an event worth recording.
    let set: ServerMessage = serde_json::from_str(
        r#"{"type":"event_fee_update","sid":11,
            "msg":{"event_ticker":"KXNFLGAME-25SEP09","fee_type_override":"quadratic",
                   "fee_multiplier_override":1.25}}"#,
    )
    .expect("parses");
    let ServerMessage::EventFeeUpdate(payload) = set else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.fee_type_override.as_deref(), Some("quadratic"));
    // Kept as a raw token rather than an f64, so no precision is lost before
    // the Phase 2 fee model decides what to do with it.
    assert_eq!(payload.msg.multiplier_raw().as_deref(), Some("1.25"));

    let cleared: ServerMessage = serde_json::from_str(
        r#"{"type":"event_fee_update","sid":11,
            "msg":{"event_ticker":"KXNFLGAME-25SEP09","fee_type_override":null,
                   "fee_multiplier_override":null}}"#,
    )
    .expect("parses with nulls");
    let ServerMessage::EventFeeUpdate(payload) = cleared else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.fee_type_override, None);
    assert_eq!(payload.msg.multiplier_raw(), None);
}

// ---------------------------------------------------------------------------
// Snapshot shape
// ---------------------------------------------------------------------------

#[test]
fn snapshot_levels_parse_into_typed_values_and_keep_their_raw_strings() {
    let message: ServerMessage = serde_json::from_str(
        r#"{"type":"orderbook_snapshot","sid":2,"seq":2,
            "msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"x",
                   "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
                   "no_dollars_fp":[["0.5400","20.00"]]}}"#,
    )
    .expect("parses");
    let ServerMessage::OrderbookSnapshot(payload) = message else {
        panic!("wrong variant");
    };
    assert_eq!(payload.seq, 2);
    assert_eq!(payload.msg.yes_dollars_fp.len(), 2);
    assert_eq!(payload.msg.no_dollars_fp.len(), 1);

    let (price, size) = payload.msg.yes_dollars_fp[0].parse().expect("parses");
    assert_eq!(
        price,
        kalshi_common::Px::parse_dollars("0.0800").expect("valid")
    );
    assert_eq!(size, kalshi_common::Qty::parse_fp("300.00").expect("valid"));
    // Raw strings survive for the dual-column Parquet schema.
    assert_eq!(payload.msg.yes_dollars_fp[0].price_raw(), "0.0800");
    assert_eq!(payload.msg.yes_dollars_fp[0].size_raw(), "300.00");
}

#[test]
fn a_snapshot_with_one_empty_side_still_parses() {
    let message: ServerMessage = serde_json::from_str(
        r#"{"type":"orderbook_snapshot","sid":2,"seq":1,
            "msg":{"market_ticker":"M","market_id":"x","yes_dollars_fp":[]}}"#,
    )
    .expect("parses");
    let ServerMessage::OrderbookSnapshot(payload) = message else {
        panic!("wrong variant");
    };
    assert!(payload.msg.yes_dollars_fp.is_empty());
    assert!(payload.msg.no_dollars_fp.is_empty());
}

// ---------------------------------------------------------------------------
// Control messages
// ---------------------------------------------------------------------------

#[test]
fn subscribed_carries_a_fresh_sid_and_unsubscribed_a_final_seq() {
    let subscribed: ServerMessage = serde_json::from_str(
        r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}"#,
    )
    .expect("parses");
    assert_eq!(subscribed.sid(), Some(1));

    let unsubscribed: ServerMessage =
        serde_json::from_str(r#"{"id":102,"sid":2,"seq":7,"type":"unsubscribed"}"#)
            .expect("parses");
    let ServerMessage::Unsubscribed(payload) = unsubscribed else {
        panic!("wrong variant");
    };
    assert_eq!(payload.sid, 2);
    assert_eq!(payload.seq, Some(7), "final seq for the retired sid");
}

#[test]
fn errors_deserialize_with_their_code() {
    let message: ServerMessage = serde_json::from_str(
        r#"{"id":123,"type":"error","msg":{"code":6,"msg":"Already subscribed"}}"#,
    )
    .expect("parses");
    let ServerMessage::Error(payload) = message else {
        panic!("wrong variant");
    };
    assert_eq!(payload.msg.code, 6);
    assert!(!payload.msg.is_terminal());
}

#[test]
fn an_unknown_message_type_is_an_error_not_a_silent_reclassification() {
    // Tagged rather than untagged deserialization: an unexpected shape is
    // reported, never quietly matched to the wrong variant.
    assert!(serde_json::from_str::<ServerMessage>(
        r#"{"type":"some_future_channel","sid":1,"msg":{}}"#
    )
    .is_err());
}

// ---------------------------------------------------------------------------
// Command encoding
// ---------------------------------------------------------------------------

#[test]
fn subscribe_omits_use_yes_price_for_non_orderbook_channels() {
    // The flag is orderbook-only; sending it elsewhere is meaningless.
    let command = Command::Subscribe {
        id: 5,
        channels: vec!["market_lifecycle_v2".to_owned()],
        market_tickers: vec![],
        use_yes_price: None,
    };
    let json: serde_json::Value =
        serde_json::from_str(&command.to_json().expect("encodes")).expect("valid json");
    assert!(json["params"].get("use_yes_price").is_none());
    // An unfiltered lifecycle subscription sends no market list at all.
    assert!(json["params"].get("market_tickers").is_none());
}

#[test]
fn unsubscribe_encodes_the_documented_shape() {
    let command = Command::Unsubscribe {
        id: 124,
        sids: vec![1, 2],
    };
    let json: serde_json::Value =
        serde_json::from_str(&command.to_json().expect("encodes")).expect("valid json");
    assert_eq!(json["cmd"], "unsubscribe");
    assert_eq!(json["params"]["sids"], serde_json::json!([1, 2]));
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

#[test]
fn backoff_grows_and_is_capped() {
    let mut backoff = Backoff::new(
        Duration::from_millis(500),
        Duration::from_secs(60),
        0.0, // no jitter, so growth is checkable exactly
    );
    assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    assert_eq!(backoff.next_delay(), Duration::from_secs(2));
    for _ in 0..20 {
        backoff.next_delay();
    }
    assert_eq!(backoff.peek(), Duration::from_secs(60), "must cap");
}

#[test]
fn backoff_resets_after_a_successful_connection() {
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(60), 0.0);
    backoff.next_delay();
    backoff.next_delay();
    backoff.reset();
    assert_eq!(backoff.next_delay(), Duration::from_millis(500));
}

#[test]
fn backoff_jitter_spreads_delays_without_leaving_the_envelope() {
    let mut backoff = Backoff::new(Duration::from_secs(10), Duration::from_secs(10), 0.3);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..40 {
        let delay = backoff.next_delay();
        // +/-30% of 10s.
        assert!(
            delay >= Duration::from_secs(7) && delay <= Duration::from_secs(13),
            "jittered delay {delay:?} left the +/-30% envelope"
        );
        seen.insert(delay.as_millis());
    }
    assert!(
        seen.len() > 1,
        "jitter produced identical delays every time"
    );
}
