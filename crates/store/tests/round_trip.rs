//! Parquet round-trip: every `*_raw` string must re-parse to its stored
//! `*_micros` / `*_fp_units` value.
//!
//! # The re-parser here is deliberately a separate implementation
//!
//! The dual-column scheme exists so that a bug in the production parser is
//! survivable — the raw column lets every value be re-derived. A test that
//! verified the columns using `Px::parse_dollars` would therefore prove
//! nothing: a parser that is wrong in the same way twice agrees with itself.
//!
//! So [`independent_parse`] below is written from scratch, with a different
//! algorithm: it normalizes the string and hands it to the standard library's
//! integer parser, rather than accumulating digits byte by byte as the
//! production code does. If the production parser mis-handles a width, a sign,
//! or a boundary, these two disagree and the test fails.

use arrow::array::Array;
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use kalshi_store::schema::{Channel, Scale};
use kalshi_store::session::{Environment, SessionMetadata};
use kalshi_store::writer::{ParquetStore, WriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// An independent decimal parser
// ---------------------------------------------------------------------------

/// Parse a decimal string to a scaled integer, **without** using any code from
/// the crate under test.
///
/// Different algorithm on purpose: strip the decimal point, pad or verify the
/// fraction, then delegate to `i128::from_str_radix`. The production parser
/// accumulates digits manually in a loop. Two independent routes to the same
/// number.
fn independent_parse(input: &str, scale: u32) -> Result<i64, String> {
    if input.is_empty() {
        return Err("empty".to_owned());
    }
    let (sign, body) = match input.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, input),
    };

    let (whole, fraction) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("bad integer part in {input:?}"));
    }
    if !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("bad fraction in {input:?}"));
    }

    let width = scale as usize;
    // Reject non-zero precision beyond the scale, matching the documented
    // contract -- but arrived at independently.
    if fraction.len() > width && fraction[width..].bytes().any(|b| b != b'0') {
        return Err(format!("excess precision in {input:?}"));
    }

    let mut padded = fraction.to_owned();
    padded.truncate(width);
    while padded.len() < width {
        padded.push('0');
    }

    let combined = format!("{whole}{padded}");
    let magnitude = combined
        .parse::<i128>()
        .map_err(|err| format!("could not parse {combined:?}: {err}"))?;
    i64::try_from(sign * magnitude).map_err(|_| format!("{input:?} overflows i64"))
}

#[test]
fn the_independent_parser_agrees_with_known_values() {
    // Sanity-check the checker itself before trusting it as an oracle.
    assert_eq!(independent_parse("0.5500", 6), Ok(550_000));
    assert_eq!(independent_parse("0.960", 6), Ok(960_000));
    assert_eq!(independent_parse("0.0001", 6), Ok(100));
    assert_eq!(independent_parse("1.0000", 6), Ok(1_000_000));
    assert_eq!(independent_parse("0", 6), Ok(0));
    assert_eq!(independent_parse("-0.5500", 6), Ok(-550_000));
    assert_eq!(independent_parse("10.00", 2), Ok(1_000));
    assert_eq!(independent_parse("-54.00", 2), Ok(-5_400));
    assert_eq!(independent_parse("0.01", 2), Ok(1));
    assert!(independent_parse("0.1234567", 6).is_err());
    assert!(independent_parse("abc", 6).is_err());
}

#[test]
fn the_two_parsers_agree_across_the_whole_price_range() {
    // If the production parser and this one ever disagree, one of them is
    // wrong and the raw column is the only way to tell which.
    let mut micros = -2_000_000i64;
    while micros <= 2_000_000 {
        let rendered = kalshi_common::format_scaled(micros, 6);
        let production = kalshi_common::Px::parse_dollars(&rendered).map(|p| p.micros());
        let independent = independent_parse(&rendered, 6);
        assert_eq!(
            production.map_err(|e| e.to_string()),
            independent,
            "parsers disagree on {rendered:?}"
        );
        micros += 7_919; // prime stride
    }
}

// ---------------------------------------------------------------------------
// Write, then read back with an independent parse of every raw column
// ---------------------------------------------------------------------------

fn test_session() -> SessionMetadata {
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

/// Prices and sizes covering the awkward cases: sub-penny ticks, mixed decimal
/// widths, negatives, zero, and the extremes of the range.
const PRICE_CASES: &[&str] = &[
    "0", "0.0001", "0.0010", "0.0800", "0.4300", "0.5500", "0.960", "0.9999", "1.0000", "0.123456",
];
const SIZE_CASES: &[&str] = &[
    "0.01",
    "1.00",
    "10.00",
    "300.00",
    "-54.00",
    "0.00",
    "999999.99",
    "13.00",
    "1.50",
    "-0.01",
];

fn build_delta_batch() -> RecordBatch {
    let schema = Channel::OrderbookDelta.schema();
    let count = PRICE_CASES.len();
    let now_ns = chrono::Utc::now()
        .timestamp_nanos_opt()
        .expect("representable");

    let received: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![now_ns; count]).with_timezone("UTC"));
    let exchange_ts: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_710_000_000_123i64); count]));
    let session: ArrayRef = Arc::new(StringArray::from(vec!["test-session"; count]));
    let sid: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64); count]));
    let seq: ArrayRef = Arc::new(Int64Array::from(
        (0..count).map(|i| Some(i as i64)).collect::<Vec<_>>(),
    ));
    let raw_message: ArrayRef = Arc::new(StringArray::from(vec!["{}"; count]));
    let market: ArrayRef = Arc::new(StringArray::from(vec!["KXNFLGAME-25SEP09-KC"; count]));
    let market_id: ArrayRef = Arc::new(StringArray::from(vec![Some("uuid"); count]));
    let side: ArrayRef = Arc::new(StringArray::from(vec!["yes"; count]));

    // The production parser produces the integer columns -- exactly as the
    // daemon would.
    let price_raw: ArrayRef = Arc::new(StringArray::from(PRICE_CASES.to_vec()));
    let price_micros: ArrayRef = Arc::new(Int64Array::from(
        PRICE_CASES
            .iter()
            .map(|s| kalshi_common::Px::parse_dollars(s).ok().map(|p| p.micros()))
            .collect::<Vec<_>>(),
    ));
    let delta_raw: ArrayRef = Arc::new(StringArray::from(SIZE_CASES.to_vec()));
    let delta_units: ArrayRef = Arc::new(Int64Array::from(
        SIZE_CASES
            .iter()
            .map(|s| kalshi_common::Qty::parse_fp(s).ok().map(|q| q.fp_units()))
            .collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(
        schema,
        vec![
            received,
            exchange_ts,
            session,
            sid,
            seq,
            raw_message,
            market,
            market_id,
            side,
            price_raw,
            price_micros,
            delta_raw,
            delta_units,
        ],
    )
    .expect("batch matches schema")
}

/// Read every Parquet file under `root` and verify each dual column.
///
/// The verification uses [`independent_parse`] only.
fn verify_round_trip(root: &Path, channel: Channel) -> usize {
    let mut files = Vec::new();
    collect_parquet(root, &mut files);
    assert!(
        !files.is_empty(),
        "no parquet files were written under {root:?}"
    );

    let duals = channel.dual_columns();
    assert!(
        !duals.is_empty(),
        "channel declares no dual columns to check"
    );

    let mut rows_checked = 0usize;
    for path in files {
        let file = std::fs::File::open(&path).expect("open parquet");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet footer is readable")
            .build()
            .expect("reader builds");

        for batch in reader {
            let batch = batch.expect("batch reads");
            for dual in &duals {
                let raw_column = batch
                    .column_by_name(dual.raw)
                    .unwrap_or_else(|| panic!("missing raw column {}", dual.raw))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("{} is not Utf8", dual.raw));
                let parsed_column = batch
                    .column_by_name(dual.parsed)
                    .unwrap_or_else(|| panic!("missing parsed column {}", dual.parsed))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("{} is not Int64", dual.parsed));

                let scale = match dual.scale {
                    Scale::Price => 6,
                    Scale::Quantity => 2,
                };

                for row in 0..batch.num_rows() {
                    if raw_column.is_null(row) {
                        continue;
                    }
                    let raw = raw_column.value(row);
                    let stored = if parsed_column.is_null(row) {
                        None
                    } else {
                        Some(parsed_column.value(row))
                    };
                    let recomputed = independent_parse(raw, scale).ok();
                    assert_eq!(
                        stored, recomputed,
                        "column {} row {row}: stored {stored:?} but re-parsing \
                         {raw:?} independently yields {recomputed:?}",
                        dual.parsed
                    );
                    rows_checked += 1;
                }
            }
        }
    }
    rows_checked
}

fn collect_parquet(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_parquet(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
}

#[test]
fn every_raw_column_reproduces_its_parsed_value_after_a_write_and_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store opens");

    let now = chrono::Utc::now();
    store
        .write_batch(Channel::OrderbookDelta, &build_delta_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    let checked = verify_round_trip(dir.path(), Channel::OrderbookDelta);
    assert_eq!(
        checked,
        PRICE_CASES.len() + SIZE_CASES.len(),
        "not every value was checked"
    );
}

#[test]
fn the_round_trip_check_actually_catches_a_corrupted_column() {
    // A verification that cannot fail is not a verification.
    //
    // This writes a real Parquet file whose price_micros column is off by one
    // -- simulating exactly the parser bug the raw column exists to survive --
    // and asserts the round-trip check rejects it. Without this negative
    // control, the passing test above could be passing vacuously.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store opens");

    let now = chrono::Utc::now();
    store
        .write_batch(Channel::OrderbookDelta, &build_corrupted_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    let root = dir.path().to_path_buf();
    let outcome =
        std::panic::catch_unwind(move || verify_round_trip(&root, Channel::OrderbookDelta));
    assert!(
        outcome.is_err(),
        "the round-trip check accepted a corrupted price_micros column; it          cannot detect the parser bug it exists to detect"
    );
}

/// Same as [`build_delta_batch`] but with one integer column deliberately
/// wrong, for the negative control above.
fn build_corrupted_batch() -> RecordBatch {
    let batch = build_delta_batch();
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let index = schema
        .index_of("price_micros")
        .expect("schema has price_micros");
    let original = columns[index]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    // Off by one: the shape a subtle parser bug takes.
    let corrupted: Int64Array = original.iter().map(|v| v.map(|n| n + 1)).collect();
    columns[index] = Arc::new(corrupted);
    RecordBatch::try_new(schema, columns).expect("batch rebuilds")
}

// ---------------------------------------------------------------------------
// Session metadata travels with the data
// ---------------------------------------------------------------------------

#[test]
fn every_partition_carries_a_session_sidecar() {
    // A partition copied or partially synced months later must remain
    // self-describing on its own, so the sidecar goes in every partition
    // directory rather than once at the root.
    let dir = tempfile::tempdir().expect("tempdir");
    let session = test_session();
    let mut store =
        ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), session.clone())
            .expect("store opens");

    let now = chrono::Utc::now();
    store
        .write_batch(Channel::OrderbookDelta, &build_delta_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    let mut parquet_files = Vec::new();
    collect_parquet(dir.path(), &mut parquet_files);
    assert!(!parquet_files.is_empty());

    for file in &parquet_files {
        let partition = file.parent().expect("has a parent directory");
        let sidecar = partition.join(session.sidecar_filename());
        assert!(
            sidecar.exists(),
            "partition {} has no session sidecar",
            partition.display()
        );

        let text = std::fs::read_to_string(&sidecar).expect("sidecar readable");
        let recovered: SessionMetadata = serde_json::from_str(&text).expect("sidecar parses");
        // The facts that make the bytes interpretable.
        assert_eq!(recovered.pricing_convention, "no_leg");
        assert_eq!(recovered.px_scale, kalshi_common::PX_SCALE);
        assert_eq!(recovered.qty_scale, kalshi_common::QTY_SCALE);
        assert_eq!(recovered.environment, Environment::Demo);
        assert_eq!(
            recovered.parser_version,
            kalshi_store::session::PARSER_VERSION
        );
        assert_eq!(recovered.docs_spec_date, "2026-08-28");
        assert!(!recovered.git_sha.is_empty());
        assert_eq!(recovered.orderbook_shard_size, 1);
        assert!(
            recovered.ended_at.is_some(),
            "a cleanly closed session must record its end time"
        );
    }
}

#[test]
fn session_facts_are_also_in_the_parquet_footer() {
    // A single file separated from its sidecar is still interpretable.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store opens");
    let now = chrono::Utc::now();
    store
        .write_batch(Channel::OrderbookDelta, &build_delta_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    let mut files = Vec::new();
    collect_parquet(dir.path(), &mut files);
    let file = std::fs::File::open(&files[0]).expect("open");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("footer readable");
    let metadata = builder.metadata().file_metadata();
    let kv: std::collections::HashMap<String, String> = metadata
        .key_value_metadata()
        .expect("footer carries key/value metadata")
        .iter()
        .filter_map(|entry| entry.value.clone().map(|v| (entry.key.clone(), v)))
        .collect();

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
    assert_eq!(
        kv.get("kalshi.docs_spec_date").map(String::as_str),
        Some("2026-08-28")
    );
    assert!(kv.contains_key("kalshi.git_sha"));
}

// ---------------------------------------------------------------------------
// UTC only
// ---------------------------------------------------------------------------

#[test]
fn partitions_use_utc_dates_across_the_dst_transition() {
    use chrono::TimeZone;
    // 1 November 2026, 01:30 US/Eastern occurs twice. Both instants must land
    // in UTC partitions and neither may be ambiguous. Capture stores no local
    // time anywhere.
    let before_fallback = chrono::Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap();
    let after_fallback = chrono::Utc.with_ymd_and_hms(2026, 11, 1, 6, 30, 0).unwrap();
    assert_ne!(before_fallback, after_fallback);

    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store opens");

    for at in [before_fallback, after_fallback] {
        store
            .write_batch(Channel::OrderbookDelta, &build_delta_batch(), at)
            .expect("write");
    }
    store.close_all(after_fallback).expect("close");

    let mut files = Vec::new();
    collect_parquet(dir.path(), &mut files);
    // Both instants are the same UTC date, so both belong to one partition --
    // which is the point: no local-time repeat, no ambiguity.
    for file in &files {
        let partition = file
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .expect("partition name");
        assert_eq!(partition, "date=2026-11-01", "partition must be a UTC date");
    }
}
