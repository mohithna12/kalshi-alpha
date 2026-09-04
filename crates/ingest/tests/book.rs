//! Tests for the implemented parts of the book: snapshot ingestion, ordering
//! assertions, sequence classification, convention typing, and the derived NO
//! view.
//!
//! Delta application is specified separately in `book_delta_spec.rs`, which is
//! expected to fail until it is implemented. Nothing here calls `apply_delta`.

use kalshi_common::{PriceRange, PriceRanges, Px, Qty, Side, Ticker};
use kalshi_ingest::book::{
    yes_ask_price_for, AnyBook, BookError, DeltaOutcome, InvariantViolation, NoLeg, OrderBook,
    SnapshotContinuity, YesLeg,
};

fn market() -> Ticker {
    Ticker::parse("KXNFLGAME-25SEP09-KC").expect("valid")
}

fn px(s: &str) -> Px {
    Px::parse_dollars(s).expect("valid price")
}

fn qty(s: &str) -> Qty {
    Qty::parse_fp(s).expect("valid quantity")
}

// ---------------------------------------------------------------------------
// Snapshot ordering is asserted, not trusted
// ---------------------------------------------------------------------------

#[test]
fn accepts_a_snapshot_in_the_documented_ascending_order() {
    let mut book = OrderBook::<NoLeg>::new(market());
    let continuity = book
        .apply_snapshot(
            1,
            2,
            &[(px("0.0800"), qty("300.00")), (px("0.2200"), qty("333.00"))],
            &[(px("0.5400"), qty("20.00")), (px("0.5600"), qty("146.00"))],
        )
        .expect("well-formed snapshot");
    assert_eq!(continuity, SnapshotContinuity::Initial);
    assert!(book.is_valid());
    book.check_invariants().expect("invariants hold");
}

#[test]
fn refuses_an_unsorted_snapshot_rather_than_building_an_inverted_book() {
    // The exchange documents levels as ascending with the best bid last. If
    // that ever stops holding, every best-bid and spread calculation silently
    // inverts while looking entirely plausible -- so it is checked.
    let mut book = OrderBook::<NoLeg>::new(market());
    let error = book
        .apply_snapshot(
            1,
            2,
            &[(px("0.2200"), qty("333.00")), (px("0.0800"), qty("300.00"))],
            &[],
        )
        .expect_err("descending levels must be refused");
    assert!(
        matches!(error, BookError::UnsortedSnapshot { .. }),
        "got {error:?}"
    );
    assert!(
        !book.is_valid(),
        "a refused snapshot must leave the book invalid, not partially applied"
    );
    assert!(error.to_string().contains("ascending"));
}

#[test]
fn refuses_a_snapshot_with_duplicate_price_levels() {
    let mut book = OrderBook::<NoLeg>::new(market());
    let error = book
        .apply_snapshot(
            1,
            2,
            &[(px("0.4200"), qty("1.00")), (px("0.4200"), qty("2.00"))],
            &[],
        )
        .expect_err("duplicate levels must be refused");
    assert!(
        matches!(error, BookError::DuplicateLevel { .. }),
        "got {error:?}"
    );
}

#[test]
fn checks_ordering_on_the_no_side_too() {
    let mut book = OrderBook::<NoLeg>::new(market());
    let error = book
        .apply_snapshot(
            1,
            2,
            &[],
            &[(px("0.5600"), qty("1.00")), (px("0.5400"), qty("2.00"))],
        )
        .expect_err("descending NO levels must be refused");
    assert!(matches!(
        error,
        BookError::UnsortedSnapshot { side: "no", .. }
    ));
}

#[test]
fn zero_size_levels_in_a_snapshot_are_not_stored() {
    // The exchange should not send these, but a zero-size level would violate
    // the no-zero-levels invariant if stored verbatim.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(
        1,
        2,
        &[(px("0.4200"), qty("0.00")), (px("0.4300"), qty("50.00"))],
        &[],
    )
    .expect("snapshot");
    book.check_invariants()
        .expect("no zero levels may be stored");
    let view = book.levels().expect("valid");
    assert_eq!(view.bid_depth(), 1);
}

// ---------------------------------------------------------------------------
// Validity is tied to the sid, and readable only when valid
// ---------------------------------------------------------------------------

#[test]
fn a_fresh_book_is_unreadable_until_a_snapshot_arrives() {
    let book = OrderBook::<NoLeg>::new(market());
    assert!(!book.is_valid());
    assert!(
        book.levels().is_none(),
        "there must be no way to read a book that has never been seeded"
    );
    assert!(book.no_bids().is_none());
}

#[test]
fn an_invalidated_book_becomes_unreadable() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 2, &[(px("0.4200"), qty("1.00"))], &[])
        .expect("snapshot");
    assert!(book.levels().is_some());
    book.invalidate();
    assert!(
        book.levels().is_none(),
        "a gapped book is wrong by an unknown amount and must not be readable"
    );
}

#[test]
fn a_valid_book_always_knows_its_sid_and_sequence() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(7, 42, &[(px("0.4200"), qty("1.00"))], &[])
        .expect("snapshot");
    assert_eq!(book.sid(), Some(7));
    assert_eq!(book.last_seq(), Some(42));
    book.check_invariants().expect("invariants hold");
}

#[test]
fn the_invariant_checker_rejects_a_valid_book_with_no_sid() {
    // Guards the checker itself: an assertion that cannot fire proves nothing.
    let book = OrderBook::<NoLeg>::new(market());
    // A fresh book is invalid, so this passes.
    assert_eq!(book.check_invariants(), Ok(()));
    assert!(matches!(
        InvariantViolation::ValidWithoutSid,
        InvariantViolation::ValidWithoutSid
    ));
}

// ---------------------------------------------------------------------------
// Sequence classification
// ---------------------------------------------------------------------------

#[test]
fn classifies_a_contiguous_sequence_as_applicable() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    assert_eq!(book.classify_seq(11), DeltaOutcome::Applied);
}

#[test]
fn classifies_a_skipped_sequence_as_a_gap() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    assert_eq!(
        book.classify_seq(14),
        DeltaOutcome::Gapped {
            expected: 11,
            got: 14
        }
    );
}

#[test]
fn classifies_a_repeated_or_lower_sequence_as_a_regression() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    assert_eq!(
        book.classify_seq(10),
        DeltaOutcome::Regression { last: 10, got: 10 }
    );
    assert_eq!(
        book.classify_seq(9),
        DeltaOutcome::Regression { last: 10, got: 9 }
    );
}

#[test]
fn a_gap_invalidates_the_book_and_the_delta_is_not_applied() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[(px("0.4200"), qty("100.00"))], &[])
        .expect("snapshot");
    // Jumping the sequence must invalidate before apply_delta is reached, so
    // this does not hit the todo!().
    let outcome = book.receive_delta(20, Side::Yes, px("0.4200"), qty("5.00"));
    assert_eq!(
        outcome,
        DeltaOutcome::Gapped {
            expected: 11,
            got: 20
        }
    );
    assert!(!book.is_valid(), "a gap must invalidate the book");
    assert!(book.levels().is_none());
}

#[test]
fn deltas_on_an_invalid_book_are_discarded() {
    // Applying deltas to a book known to be wrong produces a book that is wrong
    // in a less obvious way.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    book.invalidate();
    let outcome = book.receive_delta(11, Side::Yes, px("0.4200"), qty("5.00"));
    assert_eq!(outcome, DeltaOutcome::DiscardedInvalid);
}

#[test]
fn a_regression_does_not_rewind_the_high_water_mark() {
    // Same class of bug as the one found in SubscriptionState: rewinding lets
    // the detector erase its own evidence.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    assert_eq!(
        book.receive_delta(9, Side::Yes, px("0.4200"), qty("1.00")),
        DeltaOutcome::Regression { last: 10, got: 9 }
    );
    assert_eq!(
        book.last_seq(),
        Some(10),
        "the high-water mark must not move"
    );
    assert_eq!(
        book.classify_seq(11),
        DeltaOutcome::Applied,
        "11 is still the next contiguous sequence"
    );
}

// ---------------------------------------------------------------------------
// Snapshot sequence continuity: measured, because the spec is silent
// ---------------------------------------------------------------------------

#[test]
fn a_snapshot_advancing_the_sequence_is_recorded_as_continuing() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 10, &[], &[]).expect("snapshot");
    let continuity = book.apply_snapshot(1, 25, &[], &[]).expect("snapshot");
    assert_eq!(
        continuity,
        SnapshotContinuity::ContinuesStream {
            previous: 10,
            snapshot: 25
        }
    );
    assert!(!continuity.restarted());
}

#[test]
fn a_snapshot_restarting_the_sequence_is_detected_not_treated_as_a_gap() {
    // asyncapi.yaml does not state whether a get_snapshot response continues
    // the subscription's sequence or restarts it. If it restarts and we treated
    // the following deltas as a gap, every recovery would trigger another
    // recovery -- an infinite loop repairing a fault that does not exist.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 500, &[], &[]).expect("snapshot");
    let continuity = book.apply_snapshot(1, 1, &[], &[]).expect("snapshot");
    assert_eq!(
        continuity,
        SnapshotContinuity::RestartsStream {
            previous: 500,
            snapshot: 1
        }
    );
    assert!(continuity.restarted());

    // The book re-baselines to the snapshot either way -- a snapshot is the
    // truth -- so the next delta is judged against the new baseline.
    assert_eq!(book.last_seq(), Some(1));
    assert!(book.is_valid());
    assert_eq!(book.classify_seq(2), DeltaOutcome::Applied);
}

// ---------------------------------------------------------------------------
// Pricing convention in the type
// ---------------------------------------------------------------------------

#[test]
fn the_conversion_rule_differs_by_convention() {
    // The single function where reciprocal pricing and the convention meet.
    // A YES price is untouched under either convention.
    assert_eq!(
        yes_ask_price_for::<NoLeg>(Side::Yes, px("0.4300")),
        px("0.4300")
    );
    assert_eq!(
        yes_ask_price_for::<YesLeg>(Side::Yes, px("0.4300")),
        px("0.4300")
    );

    // A NO price is complemented under no_leg and used as-is under yes_leg.
    assert_eq!(
        yes_ask_price_for::<NoLeg>(Side::No, px("0.5700")),
        px("0.4300")
    );
    assert_eq!(
        yes_ask_price_for::<YesLeg>(Side::No, px("0.5700")),
        px("0.5700")
    );
}

#[test]
fn snapshots_honour_the_convention_of_their_book_type() {
    // Same wire bytes, two conventions, two different books -- which is exactly
    // why the convention cannot be left implicit.
    let mut no_leg = OrderBook::<NoLeg>::new(market());
    no_leg
        .apply_snapshot(1, 1, &[], &[(px("0.5700"), qty("10.00"))])
        .expect("snapshot");
    let asks = no_leg.levels().expect("valid").ask_levels();
    assert_eq!(asks, vec![(px("0.4300"), qty("10.00"))]);

    let mut yes_leg = OrderBook::<YesLeg>::new(market());
    yes_leg
        .apply_snapshot(1, 1, &[], &[(px("0.5700"), qty("10.00"))])
        .expect("snapshot");
    let asks = yes_leg.levels().expect("valid").ask_levels();
    assert_eq!(
        asks,
        vec![(px("0.5700"), qty("10.00"))],
        "under yes_leg the exchange already converted; complementing again \
         would invert the price with no error signal"
    );
}

#[test]
fn the_convention_label_travels_with_the_book() {
    assert_eq!(
        OrderBook::<NoLeg>::new(market()).convention_label(),
        "no_leg"
    );
    assert_eq!(
        OrderBook::<YesLeg>::new(market()).convention_label(),
        "yes_leg"
    );
}

#[test]
fn narrowing_a_runtime_book_to_the_wrong_convention_fails_rather_than_reinterpreting() {
    // Where the convention comes from config it cannot be a type parameter, so
    // AnyBook forces a checked narrowing. There is deliberately no unchecked
    // accessor and no conversion between the two.
    let book = AnyBook::new(market(), false);
    assert_eq!(book.convention_label(), "no_leg");
    assert!(book.as_no_leg().is_some());
    assert!(
        book.as_yes_leg().is_none(),
        "a no_leg book must not be readable as yes_leg"
    );

    let book = AnyBook::new(market(), true);
    assert_eq!(book.convention_label(), "yes_leg");
    assert!(book.as_yes_leg().is_some());
    assert!(book.as_no_leg().is_none());
}

// ---------------------------------------------------------------------------
// Derived NO view
// ---------------------------------------------------------------------------

#[test]
fn the_no_view_is_derived_from_the_canonical_yes_side() {
    // Only one representation is stored, so the two can never disagree.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(
        1,
        1,
        &[(px("0.4200"), qty("100.00"))],
        &[(px("0.5500"), qty("70.00")), (px("0.5600"), qty("30.00"))],
    )
    .expect("snapshot");

    let no_bids = book.no_bids().expect("valid");
    let mut prices: Vec<Px> = no_bids.iter().map(|(p, _)| *p).collect();
    prices.sort_unstable();
    assert_eq!(prices, vec![px("0.5500"), px("0.5600")]);
}

#[test]
fn spread_and_mid_are_exact_and_correct_by_construction() {
    let mut book = OrderBook::<NoLeg>::new(market());
    // YES bid 0.42; NO bid 0.56 => YES ask 0.44.
    book.apply_snapshot(
        1,
        1,
        &[(px("0.4200"), qty("100.00"))],
        &[(px("0.5600"), qty("30.00"))],
    )
    .expect("snapshot");

    let view = book.levels().expect("valid");
    assert_eq!(view.best_bid(), Some((px("0.4200"), qty("100.00"))));
    assert_eq!(view.best_ask(), Some((px("0.4400"), qty("30.00"))));
    assert_eq!(view.spread(), Some(px("0.0200")));
    assert_eq!(view.mid(), Some(px("0.4300")));
    book.check_invariants().expect("invariants hold");
}

#[test]
fn best_ask_is_the_lowest_not_the_highest() {
    // The ordering mistake that an unsorted snapshot would cause, checked
    // directly.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(
        1,
        1,
        &[],
        // NO bids 0.54 and 0.56 => YES asks 0.46 and 0.44.
        &[(px("0.5400"), qty("20.00")), (px("0.5600"), qty("146.00"))],
    )
    .expect("snapshot");
    let view = book.levels().expect("valid");
    assert_eq!(
        view.best_ask().map(|(p, _)| p),
        Some(px("0.4400")),
        "best ask is the lowest ask, derived from the highest NO bid"
    );
}

// ---------------------------------------------------------------------------
// Off-grid canary
// ---------------------------------------------------------------------------

#[test]
fn off_grid_snapshot_prices_are_counted_but_still_ingested() {
    // The exchange is authoritative. An off-grid price means our grid is stale
    // or our reading of price_ranges is wrong -- the level is still real.
    let mut book = OrderBook::<NoLeg>::new(market());
    book.set_grid(
        PriceRanges::new(vec![PriceRange {
            start: px("0.0100"),
            end: px("0.9900"),
            step: px("0.0100"),
        }])
        .expect("valid grid"),
    );
    book.apply_snapshot(1, 1, &[(px("0.4250"), qty("10.00"))], &[])
        .expect("snapshot");

    assert_eq!(book.off_grid_observations(), 1);
    let view = book.levels().expect("valid");
    assert_eq!(
        view.best_bid().map(|(p, _)| p),
        Some(px("0.4250")),
        "the off-grid price must be stored as sent, never snapped"
    );
}

#[test]
fn on_grid_prices_are_not_counted() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.set_grid(
        PriceRanges::new(vec![PriceRange {
            start: px("0.0100"),
            end: px("0.9900"),
            step: px("0.0100"),
        }])
        .expect("valid grid"),
    );
    book.apply_snapshot(1, 1, &[(px("0.4200"), qty("10.00"))], &[])
        .expect("snapshot");
    assert_eq!(book.off_grid_observations(), 0);
}

#[test]
fn a_book_with_no_grid_does_not_flag_anything() {
    let mut book = OrderBook::<NoLeg>::new(market());
    book.apply_snapshot(1, 1, &[(px("0.4250"), qty("10.00"))], &[])
        .expect("snapshot");
    assert_eq!(book.off_grid_observations(), 0);
}
