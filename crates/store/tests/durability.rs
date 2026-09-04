//! File rolling, crash quarantine, and grid-history deduplication.

use kalshi_store::grid_history::{GridHistory, GridSource, GridVerdict};
use kalshi_store::quarantine::{inspect, scan_and_quarantine, Defect};
use kalshi_store::schema::Channel;
use kalshi_store::session::{Environment, SessionMetadata};
use kalshi_store::writer::{ParquetStore, WriterConfig};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

fn test_session() -> SessionMetadata {
    SessionMetadata::new(
        "no_leg",
        Environment::Demo,
        1,
        vec!["orderbook_delta".to_owned()],
        "wss://example/trade-api/ws/v2".to_owned(),
        chrono::Utc::now(),
    )
    .expect("valid session")
}

fn minimal_batch() -> arrow::array::RecordBatch {
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
    use std::sync::Arc;
    let schema = Channel::Unparsed.schema();
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().expect("ok");
    let cols: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![now_ns]).with_timezone("UTC")),
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Arc::new(StringArray::from(vec!["s"])),
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Arc::new(StringArray::from(vec!["{}"])),
        Arc::new(StringArray::from(vec![Some("err")])),
        Arc::new(StringArray::from(vec![Some("unknown")])),
    ];
    arrow::array::RecordBatch::try_new(schema, cols).expect("batch")
}

fn collect_parquet(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
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

// ---------------------------------------------------------------------------
// File rolling
// ---------------------------------------------------------------------------

#[test]
fn files_roll_on_the_age_bound_not_only_on_size() {
    // A Parquet file whose footer never lands is unreadable, so the roll
    // interval is the ceiling on what a hard kill destroys. Size alone is not
    // enough: a quiet market would hold one file open for hours.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = WriterConfig::new(dir.path().to_path_buf());
    config.max_file_age = chrono::Duration::minutes(5);
    config.max_file_bytes = u64::MAX; // size can never trigger the roll

    let mut store = ParquetStore::open(config, test_session()).expect("store");
    let start = chrono::Utc::now();

    store
        .write_batch(Channel::Unparsed, &minimal_batch(), start)
        .expect("write");
    assert_eq!(store.stats().open_files, 1);

    // Four minutes later: not yet due.
    store
        .write_batch(
            Channel::Unparsed,
            &minimal_batch(),
            start + chrono::Duration::minutes(4),
        )
        .expect("write");
    assert_eq!(store.stats().files_closed, 0, "rolled too early");

    // Six minutes: due.
    store
        .write_batch(
            Channel::Unparsed,
            &minimal_batch(),
            start + chrono::Duration::minutes(6),
        )
        .expect("write");
    assert_eq!(
        store.stats().files_closed,
        1,
        "did not roll on the age bound"
    );

    store
        .close_all(start + chrono::Duration::minutes(6))
        .expect("close");
}

#[test]
fn an_idle_channel_still_rolls_its_open_file() {
    // The dangerous case: a market goes quiet, no further writes arrive, and a
    // footerless file sits open indefinitely. The flush timer must roll it
    // even with no new data.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = WriterConfig::new(dir.path().to_path_buf());
    config.max_file_age = chrono::Duration::minutes(5);

    let mut store = ParquetStore::open(config, test_session()).expect("store");
    let start = chrono::Utc::now();
    store
        .write_batch(Channel::Unparsed, &minimal_batch(), start)
        .expect("write");

    // No further writes -- only the timer fires.
    let rolled = store
        .roll_due_files(start + chrono::Duration::minutes(6))
        .expect("roll");
    assert_eq!(rolled, 1, "an idle file must still be rolled");
    assert_eq!(store.stats().open_files, 0);

    // And what it wrote is readable, because the footer was written.
    let mut files = Vec::new();
    collect_parquet(dir.path(), &mut files);
    assert_eq!(files.len(), 1);
    assert_eq!(
        inspect(&files[0]).expect("inspect"),
        None,
        "file is complete"
    );
}

#[test]
fn every_file_closed_by_shutdown_is_readable() {
    // SIGINT must leave nothing footerless.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store");
    let now = chrono::Utc::now();
    for _ in 0..3 {
        store
            .write_batch(Channel::Unparsed, &minimal_batch(), now)
            .expect("write");
    }
    store.close_all(now).expect("close");

    let mut files = Vec::new();
    collect_parquet(dir.path(), &mut files);
    assert!(!files.is_empty());
    for file in &files {
        assert_eq!(
            inspect(file).expect("inspect"),
            None,
            "{} was left incomplete by shutdown",
            file.display()
        );
    }
}

#[test]
fn a_file_never_spans_two_utc_dates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = WriterConfig::new(dir.path().to_path_buf());
    config.max_file_age = chrono::Duration::hours(24);
    let mut store = ParquetStore::open(config, test_session()).expect("store");

    use chrono::TimeZone;
    let before = chrono::Utc
        .with_ymd_and_hms(2026, 11, 1, 23, 59, 0)
        .unwrap();
    let after = chrono::Utc.with_ymd_and_hms(2026, 11, 2, 0, 1, 0).unwrap();

    store
        .write_batch(Channel::Unparsed, &minimal_batch(), before)
        .expect("write");
    store
        .write_batch(Channel::Unparsed, &minimal_batch(), after)
        .expect("write");
    store.close_all(after).expect("close");

    let mut files = Vec::new();
    collect_parquet(dir.path(), &mut files);
    let partitions: std::collections::HashSet<String> = files
        .iter()
        .filter_map(|f| f.parent()?.file_name()?.to_str().map(str::to_owned))
        .collect();
    assert!(partitions.contains("date=2026-11-01"));
    assert!(partitions.contains("date=2026-11-02"));
}

// ---------------------------------------------------------------------------
// Crash quarantine
// ---------------------------------------------------------------------------

#[test]
fn a_footerless_file_is_recognized_as_incomplete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("truncated.parquet");
    // A file that begins like Parquet but was killed before its footer -- the
    // exact signature of a hard kill mid-write.
    let mut file = fs::File::create(&path).expect("create");
    file.write_all(b"PAR1").expect("write header");
    file.write_all(&[0u8; 4096]).expect("write body");
    drop(file);

    assert_eq!(
        inspect(&path).expect("inspect"),
        Some(Defect::MissingFooterMagic)
    );
}

#[test]
fn startup_quarantines_incomplete_files_without_deleting_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let partition = dir.path().join("orderbook_delta/date=2026-11-01");
    fs::create_dir_all(&partition).expect("mkdir");

    // One healthy file, written properly.
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store");
    let now = chrono::Utc::now();
    store
        .write_batch(Channel::Unparsed, &minimal_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    // One left behind by a crash.
    let broken = partition.join("crashed.parquet");
    let mut file = fs::File::create(&broken).expect("create");
    file.write_all(b"PAR1").expect("write");
    file.write_all(&[7u8; 2048]).expect("write");
    drop(file);

    let quarantined = scan_and_quarantine(dir.path()).expect("scan");
    assert_eq!(quarantined.len(), 1, "expected exactly one bad file");
    assert_eq!(quarantined[0].defect, Defect::MissingFooterMagic);

    // Moved, not deleted -- the bytes may still be salvageable.
    assert!(
        !broken.exists(),
        "the bad file should have been moved aside"
    );
    assert!(
        quarantined[0].moved_to.exists(),
        "quarantined bytes must be preserved, never deleted"
    );
    assert!(quarantined[0]
        .moved_to
        .starts_with(dir.path().join("_quarantine")));
    assert_eq!(quarantined[0].bytes, 2052);

    // The healthy file was left alone.
    let mut remaining = Vec::new();
    collect_parquet(&dir.path().join("unparsed"), &mut remaining);
    assert_eq!(remaining.len(), 1, "a healthy file must not be quarantined");
    assert_eq!(inspect(&remaining[0]).expect("inspect"), None);
}

#[test]
fn quarantine_leaves_a_clean_directory_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = ParquetStore::open(WriterConfig::new(dir.path().to_path_buf()), test_session())
        .expect("store");
    let now = chrono::Utc::now();
    store
        .write_batch(Channel::Unparsed, &minimal_batch(), now)
        .expect("write");
    store.close_all(now).expect("close");

    assert!(scan_and_quarantine(dir.path()).expect("scan").is_empty());
    // And it is idempotent.
    assert!(scan_and_quarantine(dir.path()).expect("scan").is_empty());
}

#[test]
fn quarantine_does_not_rescan_its_own_output() {
    // The quarantine directory holds broken files by definition; scanning it
    // would re-quarantine them forever.
    let dir = tempfile::tempdir().expect("tempdir");
    let quarantine = dir.path().join("_quarantine/orderbook_delta");
    fs::create_dir_all(&quarantine).expect("mkdir");
    let stale = quarantine.join("already-bad.parquet");
    fs::write(&stale, b"PAR1garbage").expect("write");

    assert!(scan_and_quarantine(dir.path()).expect("scan").is_empty());
    assert!(
        stale.exists(),
        "already-quarantined files must be left alone"
    );
}

// ---------------------------------------------------------------------------
// Grid history: change events, not a poll log
// ---------------------------------------------------------------------------

const GRID_A: &str = r#"[{"start":"0.0100","end":"0.9900","step":"0.0100"}]"#;
const GRID_B: &str = r#"[{"start":"0.0001","end":"0.9999","step":"0.0001"}]"#;

#[test]
fn repeated_identical_observations_are_not_written() {
    // Reconciliation re-reads every market every 300s. Writing each read would
    // produce ~288 identical rows per market per day -- a poll log, not a
    // history.
    let mut history = GridHistory::new();
    assert_eq!(
        history.observe("KXNFLGAME-25SEP09-KC", GridSource::Discovery, GRID_A),
        GridVerdict::FirstObservation
    );
    for _ in 0..288 {
        assert_eq!(
            history.observe("KXNFLGAME-25SEP09-KC", GridSource::Discovery, GRID_A),
            GridVerdict::Unchanged
        );
    }
    assert_eq!(history.changes_recorded(), 1);
    assert_eq!(history.unchanged_observations(), 288);
}

#[test]
fn a_real_grid_change_is_recorded() {
    let mut history = GridHistory::new();
    history.observe("M", GridSource::Discovery, GRID_A);
    assert_eq!(
        history.observe("M", GridSource::Discovery, GRID_B),
        GridVerdict::Changed,
        "a different grid must produce a row"
    );
    // And reverting is also a change.
    assert_eq!(
        history.observe("M", GridSource::Discovery, GRID_A),
        GridVerdict::Changed
    );
    assert_eq!(history.changes_recorded(), 3);
}

#[test]
fn discovery_and_lifecycle_are_tracked_separately() {
    // They carry different information: a lifecycle event knows when the grid
    // changed, a discovery read does not. Seeing the same grid via discovery
    // must not suppress the lifecycle row that dates the change.
    let mut history = GridHistory::new();
    assert_eq!(
        history.observe("M", GridSource::Discovery, GRID_A),
        GridVerdict::FirstObservation
    );
    assert_eq!(
        history.observe("M", GridSource::Lifecycle, GRID_A),
        GridVerdict::FirstObservation,
        "a lifecycle report must not be suppressed by a prior discovery read"
    );
}

#[test]
fn markets_do_not_interfere_with_each_other() {
    let mut history = GridHistory::new();
    history.observe("MARKET-A", GridSource::Discovery, GRID_A);
    assert_eq!(
        history.observe("MARKET-B", GridSource::Discovery, GRID_A),
        GridVerdict::FirstObservation,
        "an identical grid on a different market is still that market's first"
    );
    assert_eq!(history.tracked_markets(), 2);
}

#[test]
fn only_writable_verdicts_produce_rows() {
    assert!(GridVerdict::FirstObservation.should_write());
    assert!(GridVerdict::Changed.should_write());
    assert!(!GridVerdict::Unchanged.should_write());
}

// ---------------------------------------------------------------------------
// Session is structurally required
// ---------------------------------------------------------------------------

#[test]
fn a_session_cannot_omit_the_facts_that_make_data_interpretable() {
    // No Default impl, and empty values are refused rather than filled in.
    assert!(SessionMetadata::new(
        "",
        Environment::Demo,
        1,
        vec!["c".to_owned()],
        "wss://x".to_owned(),
        chrono::Utc::now()
    )
    .is_err());
    assert!(SessionMetadata::new(
        "no_leg",
        Environment::Demo,
        1,
        vec![],
        "wss://x".to_owned(),
        chrono::Utc::now()
    )
    .is_err());
    assert!(SessionMetadata::new(
        "no_leg",
        Environment::Demo,
        1,
        vec!["c".to_owned()],
        String::new(),
        chrono::Utc::now()
    )
    .is_err());
}

#[test]
fn an_unclosed_session_is_visible_as_such() {
    // A sidecar with no ended_at is how a crashed run identifies itself.
    let session = test_session();
    assert_eq!(session.ended_at, None);
    let mut closed = session.clone();
    closed.close(chrono::Utc::now());
    assert!(closed.ended_at.is_some());
}
