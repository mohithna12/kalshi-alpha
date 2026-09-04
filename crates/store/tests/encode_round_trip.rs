//! End-to-end: a real wire message → encoder → Parquet → read back → assert
//! every field survived.
//!
//! # Why the existing storage tests could not catch this class of bug
//!
//! `round_trip.rs` and `durability.rs` both start from a hand-constructed
//! `RecordBatch`. That is their shared blind spot: they verify what happens
//! *after* a batch exists and never exercise the path from a wire message to
//! that batch. A field the encoder forgets to populate, mis-maps, or drops is
//! invisible to them, because the test author builds the batch the same way the
//! author of the encoder would.
//!
//! It is the same shape as the `<&str>::deserialize` bug: every test parsed
//! from a `&str` slice, so the one input shape that failed was the one no test
//! used.
//!
//! So these tests start from the JSON bytes the exchange actually sends, taken
//! from `asyncapi.yaml`'s own examples, and assert on what comes back off disk.

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use kalshi_store::schema::Channel;
use kalshi_store::session::{Environment, SessionMetadata};
use kalshi_store::sink::StoreRecord;
use kalshi_store::writer::{ParquetStore, WriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Real wire messages, taken from the AsyncAPI specification's own examples
// ---------------------------------------------------------------------------

const SNAPSHOT: &str = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,
  "msg":{"market_ticker":"FED-23DEC-T3.00",
         "market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
         "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
         "no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;

// Note the three-decimal price. The spec mixes widths within one channel.
const DELTA: &str = r#"{"type":"orderbook_delta","sid":2,"seq":3,
  "msg":{"market_ticker":"FED-23DEC-T3.00",
         "market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
         "price_dollars":"0.960","delta_fp":"-54.00","side":"yes",
         "ts_ms":1710000000123}}"#;

const TICKER: &str = r#"{"type":"ticker","sid":4,
  "msg":{"market_ticker":"KXNFLGAME-25SEP09-KC","market_id":"abc",
         "price_dollars":"0.4300","yes_bid_dollars":"0.4200",
         "yes_ask_dollars":"0.4400","yes_bid_size_fp":"100.00",
         "yes_ask_size_fp":"50.00","volume_fp":"12345.00",
         "open_interest_fp":"6789.00","ts_ms":1710000000123}}"#;

const TRADE_CANONICAL: &str = r#"{"type":"trade","sid":3,
  "msg":{"trade_id":"t-1","market_ticker":"KXNFLGAME-25SEP09-KC",
         "yes_price_dollars":"0.4300","no_price_dollars":"0.5700",
         "count_fp":"10.00","taker_outcome_side":"yes","taker_book_side":"bid",
         "is_block_trade":false,"ts_ms":1710000000123}}"#;

/// The deprecated shape: `taker_side` and integer-seconds `ts`, with neither
/// canonical direction field. Protection for these lapsed on 2026-05-14.
const TRADE_DEPRECATED: &str = r#"{"type":"trade","sid":3,
  "msg":{"trade_id":"t-2","market_ticker":"KXNFLGAME-25SEP09-KC",
         "yes_price_dollars":"0.4100","count_fp":"5.00",
         "taker_side":"no","ts":1710000000}}"#;

const LIFECYCLE_DETERMINED: &str = r#"{"type":"market_lifecycle_v2","sid":9,
  "msg":{"event_type":"determined","market_ticker":"KXNFLGAME-25SEP09-KC",
         "result":"yes","determination_ts":1710000000,
         "settlement_value":"1.0000","settled_ts":1710000100}}"#;

const FEE_UPDATE_SET: &str = r#"{"type":"event_fee_update","sid":11,
  "msg":{"event_ticker":"KXNFLGAME-25SEP09","fee_type_override":"quadratic",
         "fee_multiplier_override":1.25}}"#;

const FEE_UPDATE_CLEARED: &str = r#"{"type":"event_fee_update","sid":11,
  "msg":{"event_ticker":"KXNFLGAME-25SEP09","fee_type_override":null,
         "fee_multiplier_override":null}}"#;

const GARBAGE: &str = r#"{"type":"some_future_channel","sid":1,"msg":{"x":1}}"#;

// ---------------------------------------------------------------------------
// Harness: wire text -> record -> store -> disk -> batch
// ---------------------------------------------------------------------------

fn session() -> SessionMetadata {
    SessionMetadata::new(
        "no_leg",
        Environment::Demo,
        1,
        vec!["orderbook_delta".to_owned()],
        "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2".to_owned(),
        chrono::Utc::now(),
    )
    .expect("valid session")
}

/// Build a record the way the daemon does: parse the wire text, keep the raw.
fn record(channel: Channel, raw: &str) -> StoreRecord {
    StoreRecord {
        channel,
        received_at: chrono::Utc::now(),
        raw: raw.to_owned(),
        parsed: serde_json::from_str(raw).ok(),
    }
}

/// Run records through the real store and read back what landed.
fn round_trip(channel: Channel, records: &[StoreRecord]) -> (RecordBatch, SessionMetadata) {
    let dir = tempfile::tempdir().expect("tempdir");
    let session = session();
    let mut store =
        ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), session.clone())
            .expect("store opens");

    let now = chrono::Utc::now();
    let written = store
        .write_records(channel, records, now)
        .expect("records encode and write");
    assert!(
        written > 0,
        "the encoder produced no rows for {}",
        channel.as_str()
    );

    let stats = store.stats();
    assert_eq!(
        stats.rows_lost(),
        0,
        "rows vanished between the queue and disk for {}: expected {}, wrote {}",
        channel.as_str(),
        stats.rows_expected,
        stats.rows_written
    );

    store.close_all(now).expect("close");

    let mut files = Vec::new();
    collect(dir.path(), &mut files);
    assert_eq!(files.len(), 1, "expected exactly one parquet file");

    let file = std::fs::File::open(&files[0]).expect("open");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("footer readable")
        .build()
        .expect("reader");
    let batch = reader
        .next()
        .expect("at least one batch")
        .expect("batch reads");
    (batch, session)
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
}

fn strings<'a>(batch: &'a RecordBatch, column: &str) -> &'a StringArray {
    batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{column} is not Utf8"))
}

fn ints<'a>(batch: &'a RecordBatch, column: &str) -> &'a Int64Array {
    batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{column} is not Int64"))
}

fn opt_str(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
    let array = strings(batch, column);
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row).to_owned())
    }
}

fn opt_int(batch: &RecordBatch, column: &str, row: usize) -> Option<i64> {
    let array = ints(batch, column);
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

// ===========================================================================
// Orderbook snapshot
// ===========================================================================

#[test]
fn snapshot_fans_out_to_one_row_per_price_level_with_both_sides() {
    let (batch, _) = round_trip(
        Channel::OrderbookSnapshot,
        &[record(Channel::OrderbookSnapshot, SNAPSHOT)],
    );
    // Two YES levels plus two NO levels.
    assert_eq!(batch.num_rows(), 4, "every level must produce a row");

    let sides = strings(&batch, "side");
    let prices = strings(&batch, "price_raw");
    let micros = ints(&batch, "price_micros");
    let sizes = strings(&batch, "size_raw");
    let units = ints(&batch, "size_fp_units");
    let index = batch
        .column_by_name("level_index")
        .expect("level_index")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");

    let mut seen = Vec::new();
    for row in 0..batch.num_rows() {
        seen.push((
            sides.value(row).to_owned(),
            prices.value(row).to_owned(),
            micros.value(row),
            sizes.value(row).to_owned(),
            units.value(row),
            index.value(row),
        ));
    }
    seen.sort();

    assert_eq!(
        seen,
        vec![
            (
                "no".to_owned(),
                "0.5400".to_owned(),
                540_000,
                "20.00".to_owned(),
                2_000,
                0
            ),
            (
                "no".to_owned(),
                "0.5600".to_owned(),
                560_000,
                "146.00".to_owned(),
                14_600,
                1
            ),
            (
                "yes".to_owned(),
                "0.0800".to_owned(),
                80_000,
                "300.00".to_owned(),
                30_000,
                0
            ),
            (
                "yes".to_owned(),
                "0.2200".to_owned(),
                220_000,
                "333.00".to_owned(),
                33_300,
                1
            ),
        ],
        "every level's side, raw price, parsed price, raw size, parsed size and \
         wire position must survive to disk"
    );
}

#[test]
fn snapshot_preserves_the_wire_ordering_via_level_index() {
    // The exchange sends levels ascending with the best bid last. Storing the
    // position means the ordering is reconstructible without re-sorting, and a
    // future ordering change is detectable.
    let (batch, _) = round_trip(
        Channel::OrderbookSnapshot,
        &[record(Channel::OrderbookSnapshot, SNAPSHOT)],
    );
    let sides = strings(&batch, "side");
    let prices = strings(&batch, "price_raw");
    let index = batch
        .column_by_name("level_index")
        .expect("level_index")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");
    for row in 0..batch.num_rows() {
        if sides.value(row) == "yes" && prices.value(row) == "0.2200" {
            assert_eq!(
                index.value(row),
                1,
                "best YES bid is last in the wire array"
            );
        }
    }
}

// ===========================================================================
// Orderbook delta
// ===========================================================================

#[test]
fn delta_survives_with_its_signed_quantity_and_three_decimal_price() {
    let (batch, session) = round_trip(
        Channel::OrderbookDelta,
        &[record(Channel::OrderbookDelta, DELTA)],
    );
    assert_eq!(batch.num_rows(), 1);

    assert_eq!(strings(&batch, "market_ticker").value(0), "FED-23DEC-T3.00");
    assert_eq!(
        strings(&batch, "market_id").value(0),
        "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1"
    );
    assert_eq!(strings(&batch, "side").value(0), "yes");

    // A three-decimal price. A parser assuming a fixed 4-decimal width would
    // read this as 0.0960 and be wrong by 10x on every delta.
    assert_eq!(strings(&batch, "price_raw").value(0), "0.960");
    assert_eq!(ints(&batch, "price_micros").value(0), 960_000);

    // Signed quantity.
    assert_eq!(strings(&batch, "delta_raw").value(0), "-54.00");
    assert_eq!(ints(&batch, "delta_fp_units").value(0), -5_400);

    // Envelope fields.
    assert_eq!(opt_int(&batch, "sid", 0), Some(2));
    assert_eq!(opt_int(&batch, "seq", 0), Some(3));
    assert_eq!(
        opt_int(&batch, "exchange_ts_ms", 0),
        Some(1_710_000_000_123)
    );
    assert_eq!(
        strings(&batch, "session_id").value(0),
        session.session_id,
        "every row must name the session that produced it"
    );
}

#[test]
fn the_complete_original_message_is_stored_on_every_row() {
    // The last line of defence: if every typed column is somehow wrong, this
    // is not. No test covered it before the encoder existed.
    let (batch, _) = round_trip(
        Channel::OrderbookDelta,
        &[record(Channel::OrderbookDelta, DELTA)],
    );
    let raw = strings(&batch, "raw_message").value(0);
    assert_eq!(raw, DELTA, "raw_message must be the exact bytes received");

    // And it re-parses to the same content.
    let reparsed: serde_json::Value = serde_json::from_str(raw).expect("valid JSON");
    assert_eq!(reparsed["msg"]["price_dollars"], "0.960");
    assert_eq!(reparsed["msg"]["delta_fp"], "-54.00");
}

#[test]
fn the_receive_timestamp_is_utc_nanoseconds() {
    let before = chrono::Utc::now();
    let (batch, _) = round_trip(
        Channel::OrderbookDelta,
        &[record(Channel::OrderbookDelta, DELTA)],
    );
    let after = chrono::Utc::now();

    let column = batch
        .column_by_name("received_at_ns")
        .expect("received_at_ns")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("timestamp column");
    let value = column.value(0);
    assert!(
        value >= before.timestamp_nanos_opt().unwrap_or(0)
            && value <= after.timestamp_nanos_opt().unwrap_or(i64::MAX),
        "receive timestamp is outside the window in which the row was made"
    );
    // The exchange timestamp is a separate column: the difference is latency,
    // and conflating them would destroy it.
    assert_ne!(
        Some(value / 1_000_000),
        opt_int(&batch, "exchange_ts_ms", 0),
        "our clock and the exchange's must not be the same column"
    );
}

// ===========================================================================
// Ticker
// ===========================================================================

#[test]
fn every_ticker_price_and_size_survives_in_both_forms() {
    let (batch, _) = round_trip(Channel::Ticker, &[record(Channel::Ticker, TICKER)]);
    assert_eq!(batch.num_rows(), 1);

    let expected: &[(&str, &str, &str, i64)] = &[
        ("price_raw", "price_micros", "0.4300", 430_000),
        ("yes_bid_raw", "yes_bid_micros", "0.4200", 420_000),
        ("yes_ask_raw", "yes_ask_micros", "0.4400", 440_000),
        (
            "yes_bid_size_raw",
            "yes_bid_size_fp_units",
            "100.00",
            10_000,
        ),
        ("yes_ask_size_raw", "yes_ask_size_fp_units", "50.00", 5_000),
        ("volume_raw", "volume_fp_units", "12345.00", 1_234_500),
        (
            "open_interest_raw",
            "open_interest_fp_units",
            "6789.00",
            678_900,
        ),
    ];
    for (raw_col, parsed_col, raw_value, parsed_value) in expected {
        assert_eq!(
            strings(&batch, raw_col).value(0),
            *raw_value,
            "{raw_col} did not survive"
        );
        assert_eq!(
            ints(&batch, parsed_col).value(0),
            *parsed_value,
            "{parsed_col} was mis-parsed"
        );
    }
}

// ===========================================================================
// Trade, including the deprecated-field fallback chain
// ===========================================================================

#[test]
fn trade_stores_the_canonical_direction_fields() {
    let (batch, _) = round_trip(Channel::Trade, &[record(Channel::Trade, TRADE_CANONICAL)]);
    assert_eq!(opt_str(&batch, "trade_id", 0).as_deref(), Some("t-1"));
    assert_eq!(
        opt_str(&batch, "taker_outcome_side", 0).as_deref(),
        Some("yes")
    );
    assert_eq!(
        opt_str(&batch, "taker_book_side", 0).as_deref(),
        Some("bid")
    );
    assert_eq!(
        opt_str(&batch, "taker_side_deprecated", 0),
        None,
        "a message without the deprecated field must store null, not an empty string"
    );
    assert_eq!(strings(&batch, "yes_price_raw").value(0), "0.4300");
    assert_eq!(ints(&batch, "no_price_micros").value(0), 570_000);
    assert_eq!(ints(&batch, "count_fp_units").value(0), 1_000);

    let block = batch
        .column_by_name("is_block_trade")
        .expect("is_block_trade")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Boolean");
    assert!(!block.value(0));
}

#[test]
fn trade_with_only_the_deprecated_fields_still_captures_everything() {
    // `taker_side` and integer-seconds `ts` lost their removal protection on
    // 2026-05-14. A message carrying only the old shape must still round-trip
    // completely -- including the direction, which would otherwise be lost.
    let (batch, _) = round_trip(Channel::Trade, &[record(Channel::Trade, TRADE_DEPRECATED)]);

    assert_eq!(
        opt_str(&batch, "taker_side_deprecated", 0).as_deref(),
        Some("no"),
        "the deprecated direction must be captured when it is the only one present"
    );
    assert_eq!(opt_str(&batch, "taker_outcome_side", 0), None);
    assert_eq!(opt_str(&batch, "taker_book_side", 0), None);

    // The deprecated `ts` is integer SECONDS on this channel; it must be
    // normalized to milliseconds, not stored as-is.
    assert_eq!(
        opt_int(&batch, "exchange_ts_ms", 0),
        Some(1_710_000_000_000),
        "deprecated ts (seconds) must be converted to milliseconds"
    );

    // A field absent from the message must be null, not zero or empty.
    assert_eq!(opt_str(&batch, "no_price_raw", 0), None);
    assert_eq!(opt_int(&batch, "no_price_micros", 0), None);
    // And the raw message is still complete.
    assert_eq!(strings(&batch, "raw_message").value(0), TRADE_DEPRECATED);
}

// ===========================================================================
// Lifecycle: settlement value is model ground truth
// ===========================================================================

#[test]
fn determined_lifecycle_events_store_the_settlement_value_in_both_forms() {
    let (batch, _) = round_trip(
        Channel::MarketLifecycle,
        &[record(Channel::MarketLifecycle, LIFECYCLE_DETERMINED)],
    );
    assert_eq!(strings(&batch, "event_type").value(0), "determined");
    assert_eq!(
        strings(&batch, "market_ticker").value(0),
        "KXNFLGAME-25SEP09-KC"
    );
    assert_eq!(opt_str(&batch, "result", 0).as_deref(), Some("yes"));

    // The outcome, parsed with the same exact parser as prices.
    assert_eq!(
        opt_str(&batch, "settlement_value_raw", 0).as_deref(),
        Some("1.0000")
    );
    assert_eq!(
        opt_int(&batch, "settlement_value_micros", 0),
        Some(kalshi_common::PX_SCALE),
        "settlement value must parse to exactly $1.00"
    );
    assert_eq!(opt_int(&batch, "determination_ts", 0), Some(1_710_000_000));
    assert_eq!(opt_int(&batch, "settled_ts", 0), Some(1_710_000_100));
}

// ===========================================================================
// Fee overrides: raw token, and cleared is an event
// ===========================================================================

#[test]
fn fee_multiplier_is_stored_as_a_raw_token_not_a_float() {
    // The spec types this as `number`, but it feeds a fee model. Routing it
    // through f64 on the way in is the same silent-precision risk the price
    // types exist to avoid.
    let (batch, _) = round_trip(
        Channel::EventFeeUpdate,
        &[record(Channel::EventFeeUpdate, FEE_UPDATE_SET)],
    );
    assert_eq!(
        strings(&batch, "event_ticker").value(0),
        "KXNFLGAME-25SEP09"
    );
    assert_eq!(
        opt_str(&batch, "fee_type_override", 0).as_deref(),
        Some("quadratic")
    );
    assert_eq!(
        opt_str(&batch, "fee_multiplier_override_raw", 0).as_deref(),
        Some("1.25"),
        "the multiplier must be preserved as its wire token"
    );
}

#[test]
fn a_cleared_fee_override_is_recorded_as_an_event_not_skipped() {
    // Overrides are set and cleared over time and no history is published, so
    // the clearing is itself information a fee model needs.
    let (batch, _) = round_trip(
        Channel::EventFeeUpdate,
        &[record(Channel::EventFeeUpdate, FEE_UPDATE_CLEARED)],
    );
    assert_eq!(
        batch.num_rows(),
        1,
        "a cleared override must still be a row"
    );
    assert_eq!(opt_str(&batch, "fee_type_override", 0), None);
    assert_eq!(opt_str(&batch, "fee_multiplier_override_raw", 0), None);
    assert_eq!(strings(&batch, "raw_message").value(0), FEE_UPDATE_CLEARED);
}

// ===========================================================================
// Nothing is ever dropped
// ===========================================================================

#[test]
fn an_unrecognized_message_is_stored_rather_than_discarded() {
    let (batch, _) = round_trip(Channel::Unparsed, &[record(Channel::Unparsed, GARBAGE)]);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(strings(&batch, "raw_message").value(0), GARBAGE);
    assert_eq!(
        opt_str(&batch, "message_type", 0).as_deref(),
        Some("some_future_channel"),
        "even an unknown message usually names its own type"
    );
}

#[test]
fn a_field_that_fails_to_parse_keeps_its_raw_string_with_a_null_integer() {
    // The whole point of the dual columns. A price we cannot parse is still a
    // price we must record -- dropping the row would lose it forever, and
    // storing a zero would be a lie.
    let malformed = r#"{"type":"orderbook_delta","sid":2,"seq":4,
      "msg":{"market_ticker":"M","market_id":"x","price_dollars":"not-a-price",
             "delta_fp":"1.005","side":"yes"}}"#;
    let (batch, _) = round_trip(
        Channel::OrderbookDelta,
        &[record(Channel::OrderbookDelta, malformed)],
    );
    assert_eq!(
        batch.num_rows(),
        1,
        "an unparsable field must not drop the row"
    );

    assert_eq!(strings(&batch, "price_raw").value(0), "not-a-price");
    assert_eq!(
        opt_int(&batch, "price_micros", 0),
        None,
        "an unparsable price must be null, never zero"
    );
    // 1.005 exceeds the documented 2-decimal _fp scale: rejected rather than
    // silently rounded, so the integer is null and the raw survives.
    assert_eq!(strings(&batch, "delta_raw").value(0), "1.005");
    assert_eq!(opt_int(&batch, "delta_fp_units", 0), None);
}

#[test]
fn no_rows_are_lost_across_a_mixed_multi_message_batch() {
    // The reconciliation the metrics line reports: records dequeued versus
    // rows written. A silent discard anywhere shows up here.
    let records: Vec<StoreRecord> = vec![
        record(Channel::OrderbookDelta, DELTA),
        record(Channel::OrderbookDelta, DELTA),
        record(Channel::OrderbookDelta, DELTA),
    ];
    let (batch, _) = round_trip(Channel::OrderbookDelta, &records);
    assert_eq!(batch.num_rows(), 3);
}

// ===========================================================================
// Session metadata travels with the encoded rows
// ===========================================================================

#[test]
fn session_metadata_reaches_the_footer_of_an_encoded_file() {
    // Previously only checked for a hand-built batch. This confirms the real
    // encode path carries it too.
    let dir = tempfile::tempdir().expect("tempdir");
    let session = session();
    let mut store =
        ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), session.clone())
            .expect("store");
    let now = chrono::Utc::now();
    store
        .write_records(
            Channel::OrderbookDelta,
            &[record(Channel::OrderbookDelta, DELTA)],
            now,
        )
        .expect("write");
    store.close_all(now).expect("close");

    let mut files = Vec::new();
    collect(dir.path(), &mut files);
    let file = std::fs::File::open(&files[0]).expect("open");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("footer");
    let kv: std::collections::HashMap<String, String> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("key/value metadata present")
        .iter()
        .filter_map(|entry| entry.value.clone().map(|v| (entry.key.clone(), v)))
        .collect();

    // Without the pricing convention a NO-side price is uninterpretable.
    assert_eq!(
        kv.get("kalshi.pricing_convention").map(String::as_str),
        Some("no_leg")
    );
    assert_eq!(
        kv.get("kalshi.px_scale").map(String::as_str),
        Some("1000000")
    );
    assert_eq!(kv.get("kalshi.qty_scale").map(String::as_str), Some("100"));
    assert_eq!(
        kv.get("kalshi.environment").map(String::as_str),
        Some("demo")
    );
    assert!(kv.contains_key("kalshi.git_sha"));

    // And the sidecar sits beside the data.
    let partition = files[0].parent().expect("parent");
    assert!(partition.join(session.sidecar_filename()).exists());
}
