//! Mechanical enforcement of the "read path must never block" invariant.
//!
//! # What these tests are defending
//!
//! If the read loop parks inside a Parquet write, two things happen:
//! tungstenite stops flushing Pongs (the server closes the connection after
//! ~10s), and the exchange terminates the subscription for buffer overflow
//! (error 25, which is terminal). Both cost far more than a dropped row.
//!
//! # Why this test cannot be defeated by swapping `try_send` for `send().await`
//!
//! [`StoreHandle::offer`] is a **synchronous fn**. Making it await would
//! require changing it to `async fn`, which changes its return type from
//! `Offered` to `impl Future<Output = Offered>`. Every assertion below compares
//! against `Offered` directly, so the swap does not compile — the test fails to
//! build rather than silently passing.
//!
//! `compile_fail_if_offer_becomes_async` states that dependency explicitly, so
//! the reason is discoverable from the test rather than from a build error.

use kalshi_store::sink::{channel, Offered, StoreRecord};
use kalshi_store::Channel;
use std::time::{Duration, Instant};

fn record(n: usize) -> StoreRecord {
    StoreRecord {
        channel: Channel::OrderbookDelta,
        received_at: chrono::Utc::now(),
        raw: format!(r#"{{"seq":{n}}}"#),
        parsed: None,
    }
}

#[tokio::test]
async fn offer_never_blocks_when_the_writer_is_stalled() {
    // The scenario: a writer that has stopped consuming entirely -- a slow
    // disk, an fsync stall, a deadlock. The read loop must keep turning.
    let (handle, _receiver_held_but_never_drained) = channel(8);

    // Fill the queue.
    for n in 0..8 {
        assert_eq!(handle.offer(record(n)), Offered::Queued);
    }

    // Now offer far more than capacity against a writer that never drains.
    // Every one of these must return immediately.
    let started = Instant::now();
    let mut dropped = 0;
    for n in 8..10_000 {
        match handle.offer(record(n)) {
            Offered::Dropped => dropped += 1,
            Offered::Queued => panic!("queue reported space it does not have"),
            Offered::WriterGone => panic!("receiver is still alive"),
        }
    }
    let elapsed = started.elapsed();

    assert_eq!(dropped, 9_992, "every offer past capacity must be dropped");
    assert!(
        elapsed < Duration::from_millis(500),
        "9,992 offers against a stalled writer took {elapsed:?}; the read path \
         is blocking. offer() must never await."
    );

    let metrics = handle.metrics().snapshot();
    assert_eq!(metrics.dropped, 9_992, "drops must be counted, not silent");
    assert_eq!(metrics.queued, 8);
    assert_eq!(metrics.offered, 10_000);
}

#[tokio::test]
async fn the_read_loop_keeps_draining_while_the_writer_is_stuck() {
    // The end-to-end shape of the invariant: a simulated socket loop that
    // offers a record per iteration and also services a heartbeat. With the
    // writer wedged, the heartbeat must keep ticking.
    let (handle, receiver) = channel(4);

    // A writer task that receives nothing, holding the queue full forever.
    let stuck_writer = tokio::spawn(async move {
        let _receiver = receiver;
        tokio::time::sleep(Duration::from_secs(3600)).await;
    });

    let mut pongs_sent = 0u32;
    let mut records_offered = 0u32;
    let mut records_dropped = 0u32;
    let started = Instant::now();

    // Stand-in for the socket loop: on each turn, take a message, hand it to
    // storage, and answer the heartbeat.
    for n in 0..5_000 {
        match handle.offer(record(n)) {
            Offered::Dropped => records_dropped += 1,
            Offered::Queued => {}
            Offered::WriterGone => panic!("writer task ended early"),
        }
        records_offered += 1;
        // Ping arrives roughly every 10s in production; here every iteration,
        // to prove the loop reaches this point at all.
        pongs_sent += 1;
    }

    let elapsed = started.elapsed();
    assert_eq!(records_offered, 5_000);
    assert_eq!(
        pongs_sent, 5_000,
        "the loop must reach the heartbeat on every iteration; if it blocked \
         on storage, Pongs would stop and the server would close us"
    );
    assert!(
        records_dropped >= 4_990,
        "the stalled queue should be dropping"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "loop took {elapsed:?} with a stuck writer; it is blocking on storage"
    );

    stuck_writer.abort();
}

#[test]
fn offer_is_callable_outside_an_async_context() {
    // The strongest structural statement available: `offer` works with no
    // runtime at all. An `async fn` could not be called here, so this test
    // fails to compile the moment someone makes it one.
    let (handle, _receiver) = channel(2);
    assert_eq!(handle.offer(record(1)), Offered::Queued);
    assert_eq!(handle.offer(record(2)), Offered::Queued);
    assert_eq!(handle.offer(record(3)), Offered::Dropped);
}

#[test]
fn compile_fail_if_offer_becomes_async() {
    // Documents why the above cannot be quietly defeated.
    //
    // `offer` returns `Offered`. If it were changed to `async fn offer`, its
    // return type would become `impl Future<Output = Offered>` and this
    // assignment -- along with every assert_eq! in this file -- would stop
    // compiling. A blocking rewrite therefore breaks the build rather than
    // passing the tests.
    let (handle, _receiver) = channel(1);
    let outcome: Offered = handle.offer(record(0));
    assert_eq!(outcome, Offered::Queued);
}

#[tokio::test]
async fn a_recovered_writer_starts_accepting_again() {
    // Drops must be a transient response to overload, not a latch.
    let (handle, mut receiver) = channel(4);
    for n in 0..4 {
        assert_eq!(handle.offer(record(n)), Offered::Queued);
    }
    assert_eq!(handle.offer(record(99)), Offered::Dropped);

    // The writer catches up.
    let drained = receiver.drain(4);
    assert_eq!(drained.len(), 4);

    assert_eq!(
        handle.offer(record(100)),
        Offered::Queued,
        "the sink must recover once the writer drains"
    );
}

#[tokio::test]
async fn a_departed_writer_is_reported_distinctly_from_a_full_queue() {
    // These need different responses: a full queue is transient, a dead writer
    // is a fault to escalate.
    let (handle, receiver) = channel(4);
    drop(receiver);
    assert_eq!(handle.offer(record(1)), Offered::WriterGone);
    assert_eq!(handle.metrics().snapshot().writer_gone, 1);
    assert_eq!(handle.metrics().snapshot().dropped, 0);
}

#[tokio::test]
async fn queue_depth_is_observable_for_the_metrics_line() {
    // A depth that trends toward capacity is the early warning before drops
    // begin.
    let (handle, mut receiver) = channel(10);
    assert_eq!(handle.queue_depth(), 0);
    for n in 0..6 {
        // `offer` is #[must_use] so a dropped record can never be ignored by
        // accident; here the outcome is genuinely irrelevant.
        let _ = handle.offer(record(n));
    }
    assert_eq!(handle.queue_depth(), 6);
    assert_eq!(handle.capacity(), 10);
    receiver.drain(6);
    assert_eq!(handle.queue_depth(), 0);
}
