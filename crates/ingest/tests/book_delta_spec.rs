//! Executable specification for `OrderBook::apply_delta`.
//!
//! # These tests are expected to FAIL until `apply_delta` is implemented
//!
//! Every test here panics on `todo!()` today. That is the point: a red test is
//! a more precise specification than a comment, and each one names a decision
//! that has to be made deliberately rather than discovered later in the data.
//!
//! Every test name is prefixed `delta_spec_` so the rest of the suite can be
//! run without them:
//!
//! ```text
//! cargo test --workspace -- --skip delta_spec_   # everything else, green
//! just spec                                      # this file, red until done
//! ```
//!
//! After each mutation these tests call `check_invariants()`, so a violation is
//! reported as a named broken property rather than as surprising numbers.

use kalshi_common::{PriceRange, PriceRanges, Px, Qty, Side, Ticker};
use kalshi_ingest::book::{DeltaOutcome, NoLeg, OrderBook, SnapshotContinuity, YesLeg};

fn market() -> Ticker {
    Ticker::parse("KXNFLGAME-25SEP09-KC").expect("valid ticker")
}

fn px(s: &str) -> Px {
    Px::parse_dollars(s).expect("valid price")
}

fn qty(s: &str) -> Qty {
    Qty::parse_fp(s).expect("valid quantity")
}

/// A seeded `NoLeg` book: YES bids at 0.42/0.43, NO bids at 0.55/0.56
/// (i.e. YES asks at 0.45/0.44).
fn seeded_book() -> OrderBook<NoLeg> {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(
        1,
        10,
        // Ascending, best bid last -- the documented wire order.
        &[(px("0.4200"), qty("100.00")), (px("0.4300"), qty("50.00"))],
        &[(px("0.5500"), qty("70.00")), (px("0.5600"), qty("30.00"))],
    )
    .expect("snapshot is well formed");
    book
}

// ===========================================================================
// The five cases named as most likely to be guessed wrong
// ===========================================================================

#[test]
fn delta_spec_a_delta_on_a_price_not_in_the_book_creates_the_level() {
    // A delta is not necessarily an update to an existing level; the exchange
    // sends the first size at a new price the same way. Treating an unknown
    // price as a no-op silently loses liquidity.
    let mut book = seeded_book();
    let outcome = book.receive_delta(11, Side::Yes, px("0.4100"), qty("25.00"));
    assert_eq!(outcome, DeltaOutcome::Applied);
    book.check_invariants().expect("invariants hold");

    let view = book.levels().expect("book is valid");
    let levels = view.bid_levels();
    assert!(
        levels
            .iter()
            .any(|(p, q)| *p == px("0.4100") && *q == qty("25.00")),
        "a delta at a new price must create the level; got {levels:?}"
    );
    assert_eq!(view.bid_depth(), 3);
}

#[test]
fn delta_spec_a_negative_delta_on_an_absent_level_invalidates_the_book() {
    // Removing size from a level we do not hold means our book and the
    // exchange's have diverged. Invalidate; do not clamp.
    let mut book = seeded_book();
    let _ = book.receive_delta(11, Side::Yes, px("0.4000"), qty("-25.00"));

    book.check_invariants()
        .expect("no negative quantity may ever be stored");
    assert!(
        !book.is_valid(),
        "a delta against an absent level is a desync and must invalidate"
    );
    assert_eq!(
        book.desync_invalidations(),
        1,
        "the desync must be counted separately from sequence gaps"
    );
    assert!(book.levels().is_none());
}

#[test]
fn delta_spec_a_delta_driving_a_level_to_exactly_zero_removes_it() {
    // The single most consequential decision in this function. A retained
    // zero-quantity level is not harmless: it becomes the best bid, so spread
    // and mid are computed against a price where no size rests, and every
    // consumer must special-case it forever.
    let mut book = seeded_book();
    // The 0.4300 level holds 50.00; remove exactly that.
    let outcome = book.receive_delta(11, Side::Yes, px("0.4300"), qty("-50.00"));
    assert_eq!(outcome, DeltaOutcome::Applied);
    book.check_invariants()
        .expect("a zero-size level must be removed, not stored as zero");

    let view = book.levels().expect("book is valid");
    assert!(
        !view.bid_levels().iter().any(|(p, _)| *p == px("0.4300")),
        "an exhausted level must be removed from the book entirely"
    );
    assert_eq!(
        view.best_bid().map(|(p, _)| p),
        Some(px("0.4200")),
        "best bid must fall through to the next real level"
    );
}

#[test]
fn delta_spec_a_delta_exceeding_the_resting_size_invalidates_rather_than_clamping() {
    // 50.00 rests at 0.4300 and a -75.00 delta arrives. The local book and the
    // exchange's have diverged.
    //
    // Clamping to zero would leave a book that looks entirely plausible and is
    // silently wrong -- a real desync hidden behind a reasonable-looking
    // number. Invalidating costs one `get_snapshot`, which no longer disturbs
    // the subscription, and turns an invisible corruption into a counted event.
    let mut book = seeded_book();
    let _ = book.receive_delta(11, Side::Yes, px("0.4300"), qty("-75.00"));

    book.check_invariants()
        .expect("a level must never be left holding a negative quantity");
    assert!(
        !book.is_valid(),
        "an over-sized removal must invalidate the book, not clamp to zero"
    );
    assert_eq!(
        book.desync_invalidations(),
        1,
        "desyncs are counted separately from sequence gaps: same remedy, \
         different cause, different diagnosis"
    );
    assert!(
        book.levels().is_none(),
        "an invalidated book must not be readable"
    );
}

#[test]
fn delta_spec_a_desync_is_not_counted_as_a_sequence_gap() {
    // The two have the same remedy but different meanings: a gap is a message
    // never received, a desync is one received and irreconcilable. Conflating
    // them in the metrics would point diagnosis at the transport when the
    // problem is in the delta logic or a convention mismatch.
    let mut book = seeded_book();
    let outcome = book.receive_delta(11, Side::Yes, px("0.4300"), qty("-75.00"));
    assert_eq!(
        outcome,
        DeltaOutcome::Applied,
        "the sequence was contiguous; the desync is not a sequencing fault"
    );
    assert_eq!(book.desync_invalidations(), 1);
}

#[test]
fn delta_spec_an_off_grid_delta_is_recorded_not_rejected() {
    // The exchange's message is the fact. If a price arrives off the grid we
    // hold, our grid is stale or our reading of price_ranges is wrong -- the
    // message is still true and must be applied. It is counted so the
    // discrepancy surfaces, never dropped or snapped.
    let mut book = seeded_book();
    book.set_grid(
        PriceRanges::new(vec![PriceRange {
            start: px("0.0100"),
            end: px("0.9900"),
            step: px("0.0100"),
        }])
        .expect("valid grid"),
    );

    // 0.4250 is off a penny grid.
    let outcome = book.receive_delta(11, Side::Yes, px("0.4250"), qty("10.00"));
    assert_eq!(
        outcome,
        DeltaOutcome::Applied,
        "an off-grid price must still be applied; the exchange is authoritative"
    );
    book.check_invariants().expect("invariants hold");
    // The counter is the whole point. price_ranges is read as end-inclusive
    // on an assumption the docs do not confirm, and this count is the canary
    // for that assumption. A canary that never increments is a dead canary --
    // it would report "no off-grid prices" forever, including when the grid
    // model is wrong.
    assert_eq!(
        book.off_grid_observations(),
        1,
        "the off-grid observation must be COUNTED, not merely applied; this \
         counter is the runtime canary for the end-inclusive reading of \
         price_ranges and is useless if nothing increments it"
    );

    let view = book.levels().expect("book is valid");
    assert!(
        view.bid_levels().iter().any(|(p, _)| *p == px("0.4250")),
        "the off-grid level must be present, not snapped to 0.4200 or 0.4300"
    );
}

#[test]
fn delta_spec_a_snapshot_arriving_mid_stream_replaces_all_state() {
    // Deltas in flight when a snapshot lands must not be merged into it. The
    // snapshot is a complete statement of the book; anything applied before it
    // that the snapshot does not reflect was already incorporated by the
    // exchange, and re-applying it would double-count.
    let mut book = seeded_book();
    let _ = book.receive_delta(11, Side::Yes, px("0.4300"), qty("25.00"));
    book.check_invariants().expect("invariants hold");

    // A fresh snapshot, describing a completely different book.
    let continuity = book
        .apply_snapshot(
            1,
            20,
            &[(px("0.3000"), qty("5.00"))],
            &[(px("0.6000"), qty("7.00"))],
        )
        .expect("snapshot is well formed");
    assert_eq!(
        continuity,
        SnapshotContinuity::ContinuesStream {
            previous: 11,
            snapshot: 20
        }
    );
    book.check_invariants().expect("invariants hold");

    let view = book.levels().expect("book is valid");
    assert_eq!(
        view.bid_depth(),
        1,
        "prior levels must be discarded entirely"
    );
    assert_eq!(view.best_bid(), Some((px("0.3000"), qty("5.00"))));
    assert!(
        !view.bid_levels().iter().any(|(p, _)| *p == px("0.4300")),
        "state from before the snapshot must not survive it"
    );
    assert_eq!(book.last_seq(), Some(20));
}

#[test]
fn delta_spec_off_grid_deltas_accumulate_on_the_counter() {
    // One increment could be a coincidence. Several distinct off-grid prices
    // must each register, and on-grid prices must not.
    let mut book = seeded_book();
    book.set_grid(
        PriceRanges::new(vec![PriceRange {
            start: px("0.0100"),
            end: px("0.9900"),
            step: px("0.0100"),
        }])
        .expect("valid grid"),
    );

    let _ = book.receive_delta(11, Side::Yes, px("0.4250"), qty("10.00"));
    let _ = book.receive_delta(12, Side::Yes, px("0.4150"), qty("10.00"));
    let _ = book.receive_delta(13, Side::Yes, px("0.4100"), qty("10.00")); // on grid
    assert_eq!(
        book.off_grid_observations(),
        2,
        "each off-grid price must increment; on-grid prices must not"
    );
}

// ===========================================================================
// The invalid window: between detecting a gap and receiving the snapshot
// ===========================================================================

#[test]
fn delta_spec_deltas_arriving_while_invalid_are_neither_applied_nor_sequenced() {
    // The dangerous window. After a gap and before the recovery snapshot,
    // deltas keep arriving. If one were applied, stale state would enter a
    // book that later reports valid. If one merely advanced the sequence
    // counter, the recovery snapshot would appear to leave a gap behind it and
    // the book would invalidate again immediately -- a recovery that can never
    // succeed.
    //
    // Neither may happen: the delta is discarded and the high-water mark does
    // not move.
    let mut book = seeded_book();

    // Open the window with a real gap.
    let outcome = book.receive_delta(20, Side::Yes, px("0.4200"), qty("5.00"));
    assert_eq!(
        outcome,
        DeltaOutcome::Gapped {
            expected: 11,
            got: 20
        }
    );
    assert!(!book.is_valid());
    let seq_after_gap = book.last_seq();

    // Deltas continue to arrive while invalid. None may be applied, and none
    // may move the counter.
    for seq in 21..30 {
        assert_eq!(
            book.receive_delta(seq, Side::Yes, px("0.4200"), qty("1.00")),
            DeltaOutcome::DiscardedInvalid,
            "a delta on an invalid book must be discarded, not applied"
        );
        assert_eq!(
            book.last_seq(),
            seq_after_gap,
            "a discarded delta must not advance the sequence counter"
        );
        assert!(book.levels().is_none(), "the book must stay unreadable");
    }

    // The recovery snapshot re-seeds, and nothing from the invalid window
    // survives into the restored book.
    book.apply_snapshot(1, 30, &[(px("0.4200"), qty("100.00"))], &[])
        .expect("recovery snapshot");
    assert!(book.is_valid());
    book.check_invariants().expect("invariants hold");

    let view = book.levels().expect("valid after recovery");
    let (_, size) = view
        .bid_levels()
        .into_iter()
        .find(|(p, _)| *p == px("0.4200"))
        .expect("level present");
    assert_eq!(
        size,
        qty("100.00"),
        "the recovery snapshot is authoritative: none of the nine deltas \
         discarded during the invalid window may have leaked into it"
    );

    // And the stream resumes cleanly from the snapshot's sequence.
    assert_eq!(book.classify_seq(31), DeltaOutcome::Applied);
}

// ===========================================================================
// Reciprocal pricing and the convention
// ===========================================================================

#[test]
fn delta_spec_a_no_side_delta_mutates_the_yes_ask_side() {
    // Canonical storage: a NO bid is a YES ask. Writing a NO price into the bid
    // map, or keeping a separate NO map, breaks the guarantee that the two
    // views cannot drift.
    let mut book = seeded_book();
    // NO bid at 0.5700 => YES ask at 0.4300 under NoLeg.
    let outcome = book.receive_delta(11, Side::No, px("0.5700"), qty("40.00"));
    assert_eq!(outcome, DeltaOutcome::Applied);
    book.check_invariants().expect("invariants hold");

    let view = book.levels().expect("book is valid");
    assert!(
        view.ask_levels()
            .iter()
            .any(|(p, q)| *p == px("0.4300") && *q == qty("40.00")),
        "a NO bid at 0.5700 must appear as a YES ask at 0.4300; got {:?}",
        view.ask_levels()
    );
    assert!(
        !view.bid_levels().iter().any(|(p, _)| *p == px("0.5700")),
        "a NO price must never be written into the YES bid map"
    );
}

#[test]
fn delta_spec_the_derived_no_view_matches_what_was_sent() {
    // Round trip: what goes in as a NO bid comes back out as the same NO bid.
    let mut book = seeded_book();
    let _ = book.receive_delta(11, Side::No, px("0.5700"), qty("40.00"));
    book.check_invariants().expect("invariants hold");

    let no_bids = book.no_bids().expect("book is valid");
    assert!(
        no_bids
            .iter()
            .any(|(p, q)| *p == px("0.5700") && *q == qty("40.00")),
        "the derived NO view must reproduce the NO bid that was sent; got {no_bids:?}"
    );
}

#[test]
fn delta_spec_the_yes_leg_convention_does_not_complement() {
    // Under use_yes_price:true the exchange has already converted, so applying
    // complement() again would invert every NO-side price -- silently, with no
    // error signal. This is the failure this whole type parameter exists to
    // prevent.
    let mut book = OrderBook::<YesLeg>::new(market());
    book.apply_snapshot(1, 10, &[(px("0.4200"), qty("100.00"))], &[])
        .expect("snapshot");

    // Under YesLeg a "no" side price of 0.4300 is already a YES-leg price.
    let outcome = book.receive_delta(11, Side::No, px("0.4300"), qty("40.00"));
    assert_eq!(outcome, DeltaOutcome::Applied);
    book.check_invariants().expect("invariants hold");

    let view = book.levels().expect("book is valid");
    assert!(
        view.ask_levels().iter().any(|(p, _)| *p == px("0.4300")),
        "under yes_leg the price must be used as sent; got {:?}",
        view.ask_levels()
    );
    assert!(
        !view.ask_levels().iter().any(|(p, _)| *p == px("0.5700")),
        "under yes_leg the price must NOT be complemented"
    );
}

// ===========================================================================
// Properties that must hold after any sequence of deltas
// ===========================================================================

#[test]
fn delta_spec_invariants_hold_after_an_arbitrary_delta_sequence() {
    // A small property test. Applies a deterministic pseudo-random stream of
    // deltas across both sides and asserts the invariants after every single
    // one, so a violation is attributed to the delta that caused it.
    let mut book = seeded_book();
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut seq = 11u64;

    for step in 0..500 {
        // xorshift, so the sequence is reproducible from the seed above.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        let side = if state & 1 == 0 { Side::Yes } else { Side::No };
        // Prices in [0.01, 0.99].
        let cents = 1 + (state >> 8) % 99;
        let price = Px::from_micros(i64::try_from(cents).unwrap_or(50) * 10_000);
        // Deltas in [-50.00, +50.00].
        let magnitude = i64::try_from((state >> 20) % 5_000).unwrap_or(0);
        let signed = if state & 2 == 0 {
            magnitude
        } else {
            -magnitude
        };
        let delta = Qty::from_fp_units(signed);

        let outcome = book.receive_delta(seq, side, price, delta);
        seq += 1;

        book.check_invariants().unwrap_or_else(|violation| {
            panic!(
                "invariant broken at step {step} by delta \
                 (side={side:?}, price={price}, delta={delta}) -> {outcome:?}: {violation}"
            )
        });

        // If a chosen policy invalidates the book, re-seed and continue so the
        // rest of the sequence is still exercised.
        if !book.is_valid() {
            book.apply_snapshot(1, seq, &[(px("0.4200"), qty("100.00"))], &[])
                .expect("re-seed");
            seq += 1;
        }
    }
}

#[test]
fn delta_spec_the_book_never_crosses() {
    // In a binary market a YES bid and a NO bid summing to more than $1.00 is
    // an arbitrage the exchange does not permit. A crossed local book therefore
    // means a mis-applied delta, not a real market state.
    let mut book = seeded_book();
    let _ = book.receive_delta(11, Side::Yes, px("0.4400"), qty("10.00"));
    book.check_invariants()
        .expect("adding a bid below the best ask must not cross the book");

    if let Some(view) = book.levels() {
        if let (Some((bid, _)), Some((ask, _))) = (view.best_bid(), view.best_ask()) {
            assert!(bid < ask, "book is crossed: bid {bid} >= ask {ask}");
            assert!(
                view.spread().is_some(),
                "a non-crossed book must have a computable spread"
            );
        }
    }
}

#[test]
fn delta_spec_applying_then_reversing_a_delta_restores_the_book() {
    // Deltas are additive, so +n then -n must be a no-op. If it is not, sizes
    // are being overwritten rather than accumulated.
    let mut book = seeded_book();
    let before = book.levels().expect("valid").bid_levels();

    let _ = book.receive_delta(11, Side::Yes, px("0.4200"), qty("33.00"));
    book.check_invariants().expect("invariants hold");
    let _ = book.receive_delta(12, Side::Yes, px("0.4200"), qty("-33.00"));
    book.check_invariants().expect("invariants hold");

    let after = book.levels().expect("valid").bid_levels();
    assert_eq!(
        before, after,
        "a delta and its exact reverse must leave the book unchanged"
    );
}

#[test]
fn delta_spec_repeated_positive_deltas_accumulate() {
    let mut book = seeded_book();
    for (i, _) in (0..3).enumerate() {
        let _ = book.receive_delta(
            11 + u64::try_from(i).unwrap_or(0),
            Side::Yes,
            px("0.4200"),
            qty("10.00"),
        );
        book.check_invariants().expect("invariants hold");
    }
    let view = book.levels().expect("valid");
    let (_, size) = view
        .bid_levels()
        .into_iter()
        .find(|(p, _)| *p == px("0.4200"))
        .expect("level present");
    assert_eq!(
        size,
        qty("130.00"),
        "three +10.00 deltas on a 100.00 level must accumulate to 130.00"
    );
}
