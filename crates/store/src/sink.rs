//! The boundary between the socket read loop and Parquet writing.
//!
//! # The read path must never block. This module enforces it structurally.
//!
//! Two independent failure modes make a blocking write fatal rather than slow:
//!
//! 1. **Pongs stop.** tungstenite queues a Pong when it reads a Ping and
//!    flushes it at the top of the *next* read. If the read task is parked
//!    inside a Parquet write, no Pong goes out and the server closes the
//!    connection after ~10s.
//!
//! 2. **The subscription is killed.** Error 25, `subscription buffer
//!    overflow`, is a *terminal* error: a consumer too slow to drain has its
//!    subscription terminated by the exchange outright.
//!
//! So [`StoreHandle::offer`] is a **synchronous, non-`async` function**. It
//! cannot be `.await`ed, and it cannot be rewritten to await without changing
//! its signature and breaking every caller — including the tests below. That is
//! the mechanical enforcement: swapping `try_send` for `send().await` does not
//! compile here.
//!
//! When the channel is full, records are **dropped and counted** rather than
//! applying backpressure to the socket. Dropping a row is bad; losing the
//! connection and every book on it is worse, and the drop counter makes the
//! former visible in the 60s metrics line.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

/// One unit of work handed to the writer task.
#[derive(Clone, Debug)]
pub struct StoreRecord {
    pub channel: crate::schema::Channel,
    pub received_at: chrono::DateTime<chrono::Utc>,
    /// The raw wire text, always preserved.
    pub raw: String,
    /// Pre-extracted typed fields, or `None` if the message did not parse.
    pub parsed: Option<serde_json::Value>,
}

/// What happened to an offered record.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Offered {
    /// Queued for the writer.
    Queued,
    /// The queue was full. The record was dropped and counted; the caller must
    /// carry on reading rather than retrying.
    Dropped,
    /// The writer task is gone.
    WriterGone,
}

/// Counters shared with the metrics line.
#[derive(Debug, Default)]
pub struct SinkMetrics {
    pub offered: AtomicU64,
    pub queued: AtomicU64,
    pub dropped: AtomicU64,
    pub writer_gone: AtomicU64,
}

impl SinkMetrics {
    #[must_use]
    pub fn snapshot(&self) -> SinkCounterSnapshot {
        SinkCounterSnapshot {
            offered: self.offered.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            writer_gone: self.writer_gone.load(Ordering::Relaxed),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SinkCounterSnapshot {
    pub offered: u64,
    pub queued: u64,
    pub dropped: u64,
    pub writer_gone: u64,
}

/// The read loop's view of storage.
///
/// Cloneable and cheap. Deliberately exposes no `async` method.
#[derive(Clone, Debug)]
pub struct StoreHandle {
    tx: mpsc::Sender<StoreRecord>,
    metrics: Arc<SinkMetrics>,
}

impl StoreHandle {
    /// Offer a record to the writer.
    ///
    /// # This function is synchronous on purpose
    ///
    /// It returns immediately in every case, including when the queue is full.
    /// It is **not** `async` and must never become `async`: the read loop calls
    /// it between socket reads, and a suspension point here would stop Pongs
    /// and get the subscription terminated for buffer overflow.
    ///
    /// If you are tempted to make this `await` so that no record is ever lost,
    /// re-read the module docs. Losing one row is recoverable; losing the
    /// connection and every book on it is not.
    #[must_use]
    pub fn offer(&self, record: StoreRecord) -> Offered {
        self.metrics.offered.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(record) {
            Ok(()) => {
                self.metrics.queued.fetch_add(1, Ordering::Relaxed);
                Offered::Queued
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = self.metrics.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                // Log sparsely: under sustained overload this fires constantly,
                // and drowning the log would itself slow the read loop.
                if dropped.is_power_of_two() {
                    warn!(
                        dropped,
                        "storage queue full; dropping records to keep the read \
                         loop turning. Pongs and subscriptions take priority \
                         over any single row."
                    );
                }
                Offered::Dropped
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.writer_gone.fetch_add(1, Ordering::Relaxed);
                Offered::WriterGone
            }
        }
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<SinkMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Current queue depth, for the metrics line.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

/// The writer task's end of the channel.
pub struct StoreReceiver {
    rx: mpsc::Receiver<StoreRecord>,
}

impl StoreReceiver {
    pub async fn recv(&mut self) -> Option<StoreRecord> {
        self.rx.recv().await
    }

    /// Drain up to `max` records without waiting, for batched writes.
    pub fn drain(&mut self, max: usize) -> Vec<StoreRecord> {
        let mut out = Vec::new();
        while out.len() < max {
            match self.rx.try_recv() {
                Ok(record) => out.push(record),
                Err(_) => break,
            }
        }
        out
    }
}

/// Create the handle/receiver pair.
///
/// `capacity` bounds memory under a stalled writer. Large enough to absorb a
/// normal write hiccup, small enough that a genuinely stuck writer is noticed
/// through the drop counter rather than by exhausting memory.
#[must_use]
pub fn channel(capacity: usize) -> (StoreHandle, StoreReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        StoreHandle {
            tx,
            metrics: Arc::new(SinkMetrics::default()),
        },
        StoreReceiver { rx },
    )
}
