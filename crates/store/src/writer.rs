//! Append-only Parquet writing with bounded roll intervals.
//!
//! # Files roll on `max(size, interval)`
//!
//! A Parquet file's footer is written at close. Kill the process before that
//! and the whole file is unreadable — not truncated, unreadable. So the roll
//! interval bounds the blast radius of a hard kill: with a five-minute roll, a
//! `SIGKILL` costs at most five minutes of one channel, not an afternoon of a
//! game. More small files is the correct trade here.
//!
//! # Partitioning is by UTC date and channel
//!
//! `data/{channel}/date=YYYY-MM-DD/`, where the date comes from the UTC receive
//! timestamp. No local timezone is used anywhere: capture runs across the
//! 1 November DST transition, where local time repeats an hour and would make
//! file ordering ambiguous.

use crate::schema::Channel;
use crate::session::SessionMetadata;
use arrow::array::RecordBatch;
use chrono::{DateTime, Datelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("creating directory {path}")]
    CreateDir {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("opening {path}")]
    Open {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("writing session sidecar to {path}")]
    Sidecar {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("serializing session metadata")]
    SerializeSession(#[source] serde_json::Error),
    #[error("encoding records to an arrow batch")]
    Encode {
        #[source]
        source: crate::encode::EncodeError,
    },
    #[error("parquet error on {path}")]
    Parquet {
        path: String,
        #[source]
        source: parquet::errors::ParquetError,
    },
}

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub root: PathBuf,
    /// Roll after this many bytes, uncompressed estimate.
    pub max_file_bytes: u64,
    /// Roll after this long regardless of size.
    ///
    /// Defaults low on purpose: this is the ceiling on how much a hard kill
    /// can destroy.
    pub max_file_age: chrono::Duration,
    /// Rows buffered before a batch is written.
    pub max_batch_rows: usize,
    pub compression: Compression,
}

impl WriterConfig {
    #[must_use]
    pub fn new(root: PathBuf) -> WriterConfig {
        WriterConfig {
            root,
            max_file_bytes: 128 * 1024 * 1024,
            max_file_age: chrono::Duration::minutes(5),
            max_batch_rows: 10_000,
            compression: Compression::SNAPPY,
        }
    }
}

/// One open Parquet file.
struct OpenFile {
    path: PathBuf,
    writer: ArrowWriter<fs::File>,
    opened_at: DateTime<Utc>,
    bytes: u64,
    rows: u64,
    partition_date: String,
}

/// Append-only Parquet store, partitioned by channel and UTC date.
///
/// # A store cannot exist without a session
///
/// [`ParquetStore::open`] takes [`SessionMetadata`] and there is no other
/// constructor. Since writing requires a store, it is structurally impossible
/// to write a data row that is not covered by a session record — the metadata
/// cannot be forgotten, deferred, or written "later".
pub struct ParquetStore {
    config: WriterConfig,
    session: SessionMetadata,
    open: HashMap<(Channel, String), OpenFile>,
    /// Partition directories that already have this session's sidecar.
    sidecars_written: std::collections::HashSet<PathBuf>,
    files_closed: u64,
    rows_written: u64,
    bytes_written: u64,
    /// Records taken off the queue. Compared against `rows_written` in the
    /// metrics line: a divergence means rows are being silently discarded
    /// between dequeue and disk, which is exactly how the encoder gap went
    /// unnoticed. They will not be equal (a snapshot fans out to one row per
    /// price level), which is why `rows_expected` exists alongside.
    records_dequeued: u64,
    /// Rows the encoder should have produced from those records.
    rows_expected: u64,
}

impl ParquetStore {
    /// Open a store for one session.
    ///
    /// The session sidecar is written into every partition directory as it is
    /// created, not once at the root, so a directory copied or partially synced
    /// months later is still self-describing on its own.
    pub fn open(
        config: WriterConfig,
        session: SessionMetadata,
    ) -> Result<ParquetStore, WriteError> {
        fs::create_dir_all(&config.root).map_err(|source| WriteError::CreateDir {
            path: config.root.display().to_string(),
            source,
        })?;
        info!(
            session_id = %session.session_id,
            root = %config.root.display(),
            pricing_convention = %session.pricing_convention,
            environment = session.environment.as_str(),
            git_sha = %session.git_sha,
            roll_minutes = config.max_file_age.num_minutes(),
            "opened parquet store"
        );
        Ok(ParquetStore {
            config,
            session,
            open: HashMap::new(),
            sidecars_written: std::collections::HashSet::new(),
            files_closed: 0,
            rows_written: 0,
            bytes_written: 0,
            records_dequeued: 0,
            rows_expected: 0,
        })
    }

    #[must_use]
    pub fn session(&self) -> &SessionMetadata {
        &self.session
    }

    /// Partition directory for a channel and instant. UTC date only.
    fn partition_dir(&self, channel: Channel, at: DateTime<Utc>) -> PathBuf {
        self.config.root.join(channel.as_str()).join(format!(
            "date={:04}-{:02}-{:02}",
            at.year(),
            at.month(),
            at.day()
        ))
    }

    #[must_use]
    fn partition_key(at: DateTime<Utc>) -> String {
        format!("{:04}-{:02}-{:02}", at.year(), at.month(), at.day())
    }

    /// Write the session sidecar into a partition directory, once per session.
    fn ensure_sidecar(&mut self, dir: &Path) -> Result<(), WriteError> {
        if self.sidecars_written.contains(dir) {
            return Ok(());
        }
        let path = dir.join(self.session.sidecar_filename());
        let json =
            serde_json::to_string_pretty(&self.session).map_err(WriteError::SerializeSession)?;
        fs::write(&path, json).map_err(|source| WriteError::Sidecar {
            path: path.display().to_string(),
            source,
        })?;
        self.sidecars_written.insert(dir.to_path_buf());
        debug!(path = %path.display(), "wrote session sidecar");
        Ok(())
    }

    /// Append a batch, rolling the file first if it is due.
    pub fn write_batch(
        &mut self,
        channel: Channel,
        batch: &RecordBatch,
        at: DateTime<Utc>,
    ) -> Result<(), WriteError> {
        let partition = Self::partition_key(at);
        let key = (channel, partition.clone());

        if self.should_roll(&key, at) {
            self.close_file(&key)?;
        }

        if !self.open.contains_key(&key) {
            self.open_file(channel, &partition, at)?;
        }

        let Some(file) = self.open.get_mut(&key) else {
            return Ok(());
        };
        let path = file.path.display().to_string();
        file.writer
            .write(batch)
            .map_err(|source| WriteError::Parquet { path, source })?;

        let rows = u64::try_from(batch.num_rows()).unwrap_or(0);
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(0);
        file.rows += rows;
        file.bytes += bytes;
        self.rows_written += rows;
        self.bytes_written += bytes;
        Ok(())
    }

    fn should_roll(&self, key: &(Channel, String), at: DateTime<Utc>) -> bool {
        let Some(file) = self.open.get(key) else {
            return false;
        };
        // A crossed UTC date always rolls: a file must never span partitions.
        if file.partition_date != key.1 {
            return true;
        }
        if file.bytes >= self.config.max_file_bytes {
            return true;
        }
        at - file.opened_at >= self.config.max_file_age
    }

    fn open_file(
        &mut self,
        channel: Channel,
        partition: &str,
        at: DateTime<Utc>,
    ) -> Result<(), WriteError> {
        let dir = self.partition_dir(channel, at);
        fs::create_dir_all(&dir).map_err(|source| WriteError::CreateDir {
            path: dir.display().to_string(),
            source,
        })?;
        // The sidecar lands before any data row in this partition.
        self.ensure_sidecar(&dir)?;

        let filename = format!(
            "{}-{}-{}.parquet",
            channel.as_str(),
            at.format("%Y%m%dT%H%M%S%.3fZ"),
            self.session.session_id
        );
        let path = dir.join(filename);
        let file = fs::File::create(&path).map_err(|source| WriteError::Open {
            path: path.display().to_string(),
            source,
        })?;

        // Session facts go into the file's own footer too, so a single file
        // separated from its sidecar is still interpretable.
        let key_values: Vec<KeyValue> = self
            .session
            .as_key_value_pairs()
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, value))
            .collect();

        let properties = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(self.config.compression)
            .set_key_value_metadata(Some(key_values))
            .build();

        let writer =
            ArrowWriter::try_new(file, channel.schema(), Some(properties)).map_err(|source| {
                WriteError::Parquet {
                    path: path.display().to_string(),
                    source,
                }
            })?;

        debug!(path = %path.display(), "opened parquet file");
        self.open.insert(
            (channel, partition.to_owned()),
            OpenFile {
                path,
                writer,
                opened_at: at,
                bytes: 0,
                rows: 0,
                partition_date: partition.to_owned(),
            },
        );
        Ok(())
    }

    /// Close one file, writing its footer.
    fn close_file(&mut self, key: &(Channel, String)) -> Result<(), WriteError> {
        let Some(file) = self.open.remove(key) else {
            return Ok(());
        };
        let path = file.path.display().to_string();
        let rows = file.rows;
        file.writer.close().map_err(|source| WriteError::Parquet {
            path: path.clone(),
            source,
        })?;
        self.files_closed += 1;
        debug!(path, rows, "closed parquet file (footer written)");
        Ok(())
    }

    /// Roll any file older than the configured age, whether or not new data
    /// arrived for it.
    ///
    /// An idle channel would otherwise hold a footerless file open
    /// indefinitely, which is exactly the state the roll interval exists to
    /// avoid. Call this on the flush timer, not only on write.
    pub fn roll_due_files(&mut self, now: DateTime<Utc>) -> Result<usize, WriteError> {
        let due: Vec<(Channel, String)> = self
            .open
            .iter()
            .filter(|(_, file)| now - file.opened_at >= self.config.max_file_age)
            .map(|(key, _)| key.clone())
            .collect();
        let count = due.len();
        for key in due {
            self.close_file(&key)?;
        }
        Ok(count)
    }

    /// Close every open file, writing all footers.
    ///
    /// Called on `SIGINT`/`SIGTERM`. Every file left open here would otherwise
    /// be unreadable, so shutdown must reach this even on an error path.
    pub fn close_all(&mut self, ended_at: DateTime<Utc>) -> Result<(), WriteError> {
        self.session.close(ended_at);
        let keys: Vec<(Channel, String)> = self.open.keys().cloned().collect();
        let mut first_error = None;
        for key in keys {
            // Keep going after a failure: one bad file must not strand the
            // others in an unreadable state.
            if let Err(err) = self.close_file(&key) {
                warn!(error = %err, "failed to close a parquet file during shutdown");
                first_error.get_or_insert(err);
            }
        }
        // Rewrite each sidecar so it carries the end timestamp.
        let dirs: Vec<PathBuf> = self.sidecars_written.iter().cloned().collect();
        for dir in dirs {
            let path = dir.join(self.session.sidecar_filename());
            if let Ok(json) = serde_json::to_string_pretty(&self.session) {
                if let Err(err) = fs::write(&path, json) {
                    warn!(error = %err, path = %path.display(), "could not finalize sidecar");
                }
            }
        }
        info!(
            files_closed = self.files_closed,
            rows_written = self.rows_written,
            "closed parquet store"
        );
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Encode a batch of records for one channel and write the result.
    ///
    /// This is the only path from a wire message to disk. It records what it
    /// dequeued and what it expected to produce, so the metrics line can prove
    /// nothing is being dropped in between.
    pub fn write_records(
        &mut self,
        channel: Channel,
        records: &[crate::sink::StoreRecord],
        at: DateTime<Utc>,
    ) -> Result<usize, WriteError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.records_dequeued += u64::try_from(records.len()).unwrap_or(0);
        self.rows_expected +=
            u64::try_from(crate::encode::expected_rows(channel, records)).unwrap_or(0);

        let session_id = self.session.session_id.clone();
        let batch = crate::encode::encode(channel, &session_id, records)
            .map_err(|source| WriteError::Encode { source })?;
        let Some(batch) = batch else {
            return Ok(0);
        };
        let rows = batch.num_rows();
        self.write_batch(channel, &batch, at)?;
        Ok(rows)
    }

    #[must_use]
    pub fn stats(&self) -> StoreStats {
        StoreStats {
            open_files: self.open.len(),
            files_closed: self.files_closed,
            rows_written: self.rows_written,
            bytes_written: self.bytes_written,
            records_dequeued: self.records_dequeued,
            rows_expected: self.rows_expected,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct StoreStats {
    pub open_files: usize,
    pub files_closed: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub records_dequeued: u64,
    pub rows_expected: u64,
}

impl StoreStats {
    /// Rows the encoder should have produced but did not.
    ///
    /// **Must be zero.** A non-zero value means rows are vanishing between the
    /// queue and disk. The encoder gap this counter was added for showed a 100%
    /// loss and nothing in the output would have revealed it.
    #[must_use]
    pub fn rows_lost(&self) -> i64 {
        i64::try_from(self.rows_expected).unwrap_or(i64::MAX)
            - i64::try_from(self.rows_written).unwrap_or(i64::MAX)
    }
}
