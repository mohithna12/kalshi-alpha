//! Local order book: snapshot handling, sequence-gap detection, and the
//! delta-application stub.
//!
//! # What is and is not implemented here
//!
//! Everything except [`OrderBook::apply_delta`], whose body is `todo!()`. The
//! invariants that function must preserve are expressed as executable
//! assertions in [`OrderBook::check_invariants`], not as prose — see the stub.
//!
//! # Reciprocal pricing, stated once
//!
//! The exchange's book contains **bids only, on both sides**. A NO bid is
//! economically a YES ask at the complementary price. Storing both sides as the
//! exchange sends them would mean holding the same liquidity twice in two
//! representations that can drift apart.
//!
//! So this book stores a single canonical view: YES bids and YES asks. Every
//! NO-side message is converted on the way in by [`yes_ask_price_for`], and the
//! NO view is *derived* on the way out. The two cannot disagree because only
//! one of them exists.

use kalshi_common::{GridSnap, PriceRanges, Px, Qty, Side, Ticker};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use tracing::warn;

// ===========================================================================
// Pricing convention, in the type
// ===========================================================================

mod sealed {
    pub trait Sealed {}
}

/// The pricing convention a book was built under, as a type parameter.
///
/// # Why this is a type and not a field
///
/// Misreading a `no_leg` book as `yes_leg` inverts every NO-side price. There
/// is no error, no gap, and no reconnect — the numbers are all valid prices,
/// they simply mean the opposite thing. It is the one failure mode in this
/// system with no runtime signal at all.
///
/// Making the convention a type parameter means the mistake cannot be
/// expressed: an `OrderBook<NoLeg>` will not typecheck where an
/// `OrderBook<YesLeg>` is expected. Where the convention is only known at
/// runtime (it comes from config), [`AnyBook`] forces a *checked* narrowing
/// with an explicit failure rather than a silent reinterpretation.
pub trait Convention: sealed::Sealed + Copy + Clone + std::fmt::Debug + 'static {
    /// Label written into the Parquet session metadata.
    const LABEL: &'static str;
    /// Value of the `use_yes_price` subscribe parameter this corresponds to.
    const USE_YES_PRICE: bool;
}

/// `use_yes_price: false`. NO-side levels arrive in NO-leg pricing: a NO bid at
/// `$0.5700` must be complemented to become a YES ask at `$0.4300`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NoLeg;

/// `use_yes_price: true`. NO-side levels arrive already expressed in YES-leg
/// pricing, so no complement is applied.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct YesLeg;

impl sealed::Sealed for NoLeg {}
impl sealed::Sealed for YesLeg {}

impl Convention for NoLeg {
    const LABEL: &'static str = "no_leg";
    const USE_YES_PRICE: bool = false;
}

impl Convention for YesLeg {
    const LABEL: &'static str = "yes_leg";
    const USE_YES_PRICE: bool = true;
}

/// Convert an incoming `(side, price)` into this book's canonical YES-side
/// price.
///
/// This is the single place the reciprocal-pricing rule is applied, and the
/// single place the convention matters. Both facts are why it is one small
/// named function rather than a branch inlined at each call site.
///
/// - YES side: the price is already a YES bid price, whatever the convention.
/// - NO side under [`NoLeg`]: a NO bid at `p` is a YES ask at `$1.00 - p`.
/// - NO side under [`YesLeg`]: the exchange already converted it; use as-is.
#[must_use]
pub fn yes_ask_price_for<C: Convention>(side: Side, price: Px) -> Px {
    match side {
        Side::Yes => price,
        Side::No => {
            if C::USE_YES_PRICE {
                price
            } else {
                price.complement()
            }
        }
    }
}

// ===========================================================================
// Errors and invariants
// ===========================================================================

/// A property that must hold after every operation on a book.
///
/// These are the assertions [`OrderBook::apply_delta`] must preserve. They are
/// checked in tests after every mutation, so a violation surfaces as a named
/// failure rather than as data that merely looks odd months later.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvariantViolation {
    #[error(
        "level {price} on the {side} side has quantity {qty}, which is negative. \
         A resting level's size is never negative; a delta that would drive it \
         below zero indicates a lost message, not a short position."
    )]
    NegativeQuantity {
        side: &'static str,
        price: String,
        qty: String,
    },

    #[error(
        "level {price} on the {side} side has quantity zero. A level with no \
         size is not a level: it must be removed from the map, not retained \
         with a zero value, or every consumer has to special-case it."
    )]
    ZeroQuantityLevel { side: &'static str, price: String },

    #[error(
        "a readable book must be tied to the sid whose sequence stream \
         validated it; this book is valid but has no sid"
    )]
    ValidWithoutSid,

    #[error("a valid book must have observed a sequence number; this one has none")]
    ValidWithoutSeq,

    #[error(
        "best bid {bid} is at or above best ask {ask}: the book is crossed. In \
         a binary market this means a YES bid and a NO bid sum to more than \
         $1.00, which is an arbitrage the exchange does not permit and \
         therefore indicates a mis-applied delta."
    )]
    Crossed { bid: String, ask: String },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    #[error(
        "snapshot for {market} has {side} levels out of order: {previous} \
         appears before {current}, but the exchange documents levels as \
         ascending with the best bid last. Refusing to ingest rather than \
         silently building an inverted book."
    )]
    UnsortedSnapshot {
        market: String,
        side: &'static str,
        previous: String,
        current: String,
    },

    #[error("snapshot for {market} repeats price level {price} on the {side} side")]
    DuplicateLevel {
        market: String,
        side: &'static str,
        price: String,
    },

    #[error("could not parse {field} {value:?} in a message for {market}")]
    Parse {
        market: String,
        field: &'static str,
        value: String,
    },
}

// ===========================================================================
// Snapshot sequence continuity
// ===========================================================================

/// How a snapshot's sequence number relates to the stream already in progress.
///
/// # This is measured, not assumed
///
/// `asyncapi.yaml` requires `seq` on `orderbook_snapshot`, so a snapshot
/// obtained via `update_subscription` / `action: get_snapshot` does carry one.
/// What the spec **does not state** is whether that sequence continues the
/// subscription's existing stream or restarts it.
///
/// The distinction matters and gets the recovery path wrong in opposite
/// directions. If the counter restarts at 1 while deltas continue from 500,
/// then re-baselining to 1 makes the next delta look like a 499-message gap —
/// which triggers another recovery, which restarts again, and the daemon spins
/// forever recovering from a gap that does not exist.
///
/// Since the spec is silent, this is classified at runtime on the first
/// observation, logged, and counted. The book always accepts the snapshot as
/// authoritative — a snapshot *is* the truth — and re-baselines its counter to
/// the snapshot's seq, which is correct under either answer. The classification
/// exists so that the loop-guard in the daemon can recognize the restart case
/// and so the behaviour is recorded rather than guessed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SnapshotContinuity {
    /// First snapshot on this subscription; nothing to compare against.
    Initial,
    /// The snapshot's seq is greater than the last seen: it continues the
    /// stream, as an inline snapshot would.
    ContinuesStream { previous: u64, snapshot: u64 },
    /// The snapshot's seq is at or below the last seen: the counter restarted,
    /// or the snapshot is stale. Either way the book re-baselines, but the
    /// caller must not treat the following deltas as a gap.
    RestartsStream { previous: u64, snapshot: u64 },
}

impl SnapshotContinuity {
    #[must_use]
    pub const fn restarted(self) -> bool {
        matches!(self, SnapshotContinuity::RestartsStream { .. })
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SnapshotContinuity::Initial => "initial",
            SnapshotContinuity::ContinuesStream { .. } => "continues",
            SnapshotContinuity::RestartsStream { .. } => "restarts",
        }
    }
}

/// What a sequence number implied when applied to a book.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DeltaOutcome {
    /// Contiguous; the delta was applied.
    Applied,
    /// A gap was detected. **The delta was not applied** and the book is now
    /// invalid until a fresh snapshot arrives. Never interpolated.
    Gapped { expected: u64, got: u64 },
    /// The book was already invalid, so the delta was discarded. Applying
    /// deltas to a book known to be wrong produces a book that is wrong in a
    /// less obvious way.
    DiscardedInvalid,
    /// A sequence at or below the high-water mark. Reported, not applied, and
    /// the high-water mark is not rewound.
    Regression { last: u64, got: u64 },
}

// ===========================================================================
// The book
// ===========================================================================

/// A local order book for one market, in one pricing convention.
///
/// # Canonical storage
///
/// Only YES-side prices are stored: `bids` are YES bids, `asks` are YES asks
/// derived from NO bids. The NO view is computed on demand by
/// [`OrderBook::no_bids`]. There is deliberately no second map to fall out of
/// sync.
///
/// # Validity is tied to a sid
///
/// A book is readable only through [`OrderBook::levels`], which returns `None`
/// while the book is invalid. Because `seq` is scoped per subscription, the sid
/// that validated a book is part of its identity: a snapshot arriving on a
/// different sid replaces the book rather than updating it.
#[derive(Debug)]
pub struct OrderBook<C: Convention> {
    market: Ticker,
    /// The subscription whose sequence stream currently validates this book.
    sid: Option<u64>,
    /// YES bids: price -> resting size. Never zero, never negative.
    bids: BTreeMap<Px, Qty>,
    /// YES asks, derived from NO bids. Never zero, never negative.
    asks: BTreeMap<Px, Qty>,
    valid: bool,
    last_seq: Option<u64>,
    /// The market's tick grid, when known. Used only to flag off-grid prices;
    /// an off-grid price is never rejected or snapped, because the exchange's
    /// message is the fact and our grid may be stale.
    grid: Option<PriceRanges>,
    off_grid_observations: u64,
    /// Times the book invalidated because a delta would have driven a level
    /// negative. Counted separately from sequence gaps: same remedy, different
    /// cause and different diagnosis.
    desync_invalidations: u64,
    _convention: PhantomData<C>,
}

impl<C: Convention> OrderBook<C> {
    #[must_use]
    pub fn new(market: Ticker) -> OrderBook<C> {
        OrderBook {
            market,
            sid: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            valid: false,
            last_seq: None,
            grid: None,
            off_grid_observations: 0,
            desync_invalidations: 0,
            _convention: PhantomData,
        }
    }

    #[must_use]
    pub fn market(&self) -> &Ticker {
        &self.market
    }

    #[must_use]
    pub fn convention_label(&self) -> &'static str {
        C::LABEL
    }

    #[must_use]
    pub fn sid(&self) -> Option<u64> {
        self.sid
    }

    #[must_use]
    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn set_grid(&mut self, grid: PriceRanges) {
        self.grid = Some(grid);
    }

    #[must_use]
    pub fn off_grid_observations(&self) -> u64 {
        self.off_grid_observations
    }

    /// Times this book invalidated because a delta exceeded the resting size.
    ///
    /// Reported separately from sequence gaps in the metrics line. Both end in
    /// a re-seed, but they mean different things: a gap is a message we never
    /// received, whereas a desync is a message we received and could not
    /// reconcile — which points at the delta logic or at a convention
    /// mismatch, not at the transport.
    #[must_use]
    pub fn desync_invalidations(&self) -> u64 {
        self.desync_invalidations
    }

    /// Mark the book desynchronized from the exchange.
    ///
    /// Call this from [`apply_delta`](Self::apply_delta) when a delta would
    /// drive a level below zero. It invalidates the book and counts the event,
    /// so the recovery ladder fetches a fresh snapshot.
    pub fn invalidate_on_desync(&mut self, side: Side, price: Px, delta: Qty, resting: Qty) {
        self.desync_invalidations += 1;
        self.valid = false;
        warn!(
            market = %self.market,
            side = %side,
            price = %price,
            delta = %delta,
            resting = %resting,
            "delta exceeds the resting size at this level: the local book has \
             diverged from the exchange. Invalidating rather than clamping -- \
             a clamped level is a real desync hidden behind a plausible number."
        );
    }

    /// The book's contents, or `None` if it is not currently trustworthy.
    ///
    /// # The only way to read the book
    ///
    /// There is no accessor that returns levels unconditionally. A gapped book
    /// is not "slightly stale" — it is wrong by an unknown amount, and a
    /// consumer that reads it anyway produces conclusions that cannot be
    /// distinguished from correct ones. The type makes that impossible.
    #[must_use]
    pub fn levels(&self) -> Option<BookView<'_>> {
        if !self.valid {
            return None;
        }
        Some(BookView {
            bids: &self.bids,
            asks: &self.asks,
        })
    }

    /// Mark the book unusable. Called on a gap, a terminal error, or the loss
    /// of the subscription.
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Ingest a full snapshot, replacing all state.
    ///
    /// `yes_levels` and `no_levels` are `(price, size)` pairs exactly as the
    /// exchange sent them, in wire order.
    ///
    /// # Ordering is asserted, not trusted
    ///
    /// The exchange documents snapshot levels as ascending with the best bid
    /// last. That is checked here rather than assumed: a silently inverted book
    /// would make every best-bid and spread calculation wrong while looking
    /// entirely plausible. An unsorted snapshot is refused, the book stays
    /// invalid, and the caller must recover.
    pub fn apply_snapshot(
        &mut self,
        sid: u64,
        seq: u64,
        yes_levels: &[(Px, Qty)],
        no_levels: &[(Px, Qty)],
    ) -> Result<SnapshotContinuity, BookError> {
        self.assert_ascending(yes_levels, "yes")?;
        self.assert_ascending(no_levels, "no")?;

        let continuity = match self.last_seq {
            None => SnapshotContinuity::Initial,
            Some(previous) if seq > previous => SnapshotContinuity::ContinuesStream {
                previous,
                snapshot: seq,
            },
            Some(previous) => SnapshotContinuity::RestartsStream {
                previous,
                snapshot: seq,
            },
        };

        if continuity.restarted() {
            // Not an error: the spec does not say whether get_snapshot's
            // sequence continues or restarts. Recorded so the answer is
            // learned from observation rather than assumed, and so the daemon's
            // loop-guard can tell this apart from a real gap.
            warn!(
                market = %self.market,
                sid,
                snapshot_seq = seq,
                previous_seq = ?self.last_seq,
                "snapshot sequence did not advance the stream; re-baselining. \
                 The spec does not state whether get_snapshot continues or \
                 restarts the counter -- this observation settles it."
            );
        }

        self.bids.clear();
        self.asks.clear();

        for (price, size) in yes_levels {
            self.note_grid(*price);
            if size.is_zero() {
                continue;
            }
            self.bids.insert(*price, *size);
        }
        for (price, size) in no_levels {
            self.note_grid(*price);
            if size.is_zero() {
                continue;
            }
            // Converted to the canonical YES-ask representation on the way in,
            // so the NO view can never drift from the YES view.
            let ask = yes_ask_price_for::<C>(Side::No, *price);
            self.asks.insert(ask, *size);
        }

        self.sid = Some(sid);
        self.last_seq = Some(seq);
        self.valid = true;
        Ok(continuity)
    }

    fn assert_ascending(&self, levels: &[(Px, Qty)], side: &'static str) -> Result<(), BookError> {
        for window in levels.windows(2) {
            let (previous, _) = window[0];
            let (current, _) = window[1];
            if current == previous {
                return Err(BookError::DuplicateLevel {
                    market: self.market.to_string(),
                    side,
                    price: previous.to_string(),
                });
            }
            if current < previous {
                return Err(BookError::UnsortedSnapshot {
                    market: self.market.to_string(),
                    side,
                    previous: previous.to_string(),
                    current: current.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Count a price that is not on the market's published grid.
    ///
    /// Never rejects or snaps: the exchange's message is the fact, and our grid
    /// may be stale or our reading of `price_ranges` may be wrong. This is a
    /// canary for the `end`-inclusive assumption in `PriceRanges`.
    fn note_grid(&mut self, price: Px) {
        let Some(grid) = &self.grid else {
            return;
        };
        if grid.is_empty() || grid.is_valid(price) {
            return;
        }
        self.off_grid_observations += 1;
        let snapped = grid.snap_to_grid(price);
        warn!(
            market = %self.market,
            price = %price,
            nearest_on_grid = ?snapped.map(GridSnap::price),
            bands = %grid,
            "observed a book price that is not on this market's published tick \
             grid; the grid may be stale or the end-inclusive reading of \
             price_ranges may be wrong"
        );
    }

    /// Classify a delta's sequence number without mutating the book.
    ///
    /// Separated from application so gap detection is testable independently of
    /// the delta logic, and so [`apply_delta`](Self::apply_delta) receives an
    /// already-validated sequence.
    #[must_use]
    pub fn classify_seq(&self, seq: u64) -> DeltaOutcome {
        if !self.valid {
            return DeltaOutcome::DiscardedInvalid;
        }
        match self.last_seq {
            None => DeltaOutcome::Applied,
            Some(last) if seq == last + 1 => DeltaOutcome::Applied,
            Some(last) if seq <= last => DeltaOutcome::Regression { last, got: seq },
            Some(last) => DeltaOutcome::Gapped {
                expected: last + 1,
                got: seq,
            },
        }
    }

    /// Receive a delta: classify its sequence, then apply it if contiguous.
    ///
    /// A gap invalidates the book and the delta is **not** applied. The book
    /// stays unreadable until a fresh snapshot arrives.
    pub fn receive_delta(&mut self, seq: u64, side: Side, price: Px, delta: Qty) -> DeltaOutcome {
        let outcome = self.classify_seq(seq);
        match outcome {
            DeltaOutcome::Applied => {
                self.note_grid(price);
                self.apply_delta(side, price, delta);
                self.last_seq = Some(seq);
            }
            DeltaOutcome::Gapped { .. } => {
                self.valid = false;
                self.last_seq = Some(seq);
            }
            // Neither a regression nor a delta on an invalid book advances the
            // high-water mark. Rewinding it would let the detector erase its
            // own evidence.
            DeltaOutcome::Regression { .. } | DeltaOutcome::DiscardedInvalid => {}
        }
        outcome
    }

    /// Apply one price-level mutation to the canonical book.
    ///
    /// # NOT IMPLEMENTED — this is the function you are writing
    ///
    /// `delta` is signed: positive adds size at `price`, negative removes it.
    /// `side` is the side **as the exchange sent it**; convert it with
    /// [`yes_ask_price_for::<C>`] before touching `self.asks`, or the NO view
    /// will be stored uncomposed and the whole reciprocal-pricing guarantee is
    /// lost.
    ///
    /// The sequence number has already been validated by
    /// [`receive_delta`](Self::receive_delta), so this function does not need
    /// to consider ordering. It must only mutate the maps.
    ///
    /// ## Invariants to preserve
    ///
    /// These are **executable**, not advisory: [`check_invariants`] checks each
    /// one and every test in `tests/book_delta_spec.rs` calls it after each
    /// mutation. Implement until they pass.
    ///
    /// 1. **No zero-quantity levels.** A level driven to exactly zero is
    ///    *removed* from the map, not retained with a zero value.
    ///    ([`InvariantViolation::ZeroQuantityLevel`])
    /// 2. **A delta exceeding the resting size invalidates the book. Never
    ///    clamp.** If 50 contracts rest at a price and a `-75` delta arrives,
    ///    the local book and the exchange's have diverged. Clamping to zero
    ///    produces a book that looks entirely plausible and is silently wrong,
    ///    hiding a real desync behind a reasonable-looking number — the exact
    ///    failure class the rest of this system is built to eliminate.
    ///
    ///    Call [`invalidate_on_desync`](Self::invalidate_on_desync), which
    ///    marks the book unreadable and counts the event separately from
    ///    sequence gaps (same remedy, different cause: a gap is a message never
    ///    received, a desync is one received and irreconcilable). The recovery
    ///    ladder then fetches a fresh snapshot, which is now cheap —
    ///    `get_snapshot` re-seeds without touching the subscription.
    ///    ([`InvariantViolation::NegativeQuantity`])
    /// 3. **Canonical YES storage.** NO-side deltas mutate `self.asks` at the
    ///    converted price. `self.bids` holds YES bids only. Nothing writes a
    ///    NO-side price into either map unconverted.
    /// 4. **Convention respected.** The conversion goes through
    ///    [`yes_ask_price_for::<C>`]; do not hardcode `complement()`, which is
    ///    correct only under [`NoLeg`].
    /// 5. **Sorted levels.** `BTreeMap` gives this for free — do not replace it
    ///    with a `Vec` and a manual sort.
    /// 6. **Validity changes only via `invalidate_on_desync`.** Do not set
    ///    `self.valid` directly, and never set it to `true` — sequence-gap
    ///    handling belongs to [`receive_delta`](Self::receive_delta), and only
    ///    a snapshot may make a book readable again.
    ///
    /// ## Cases the spec tests cover
    ///
    /// - a delta against a price level not currently in the book;
    /// - a delta driving a level to exactly zero;
    /// - a delta that would drive a level negative (must invalidate);
    /// - a delta at a price off the market's current tick grid;
    /// - a snapshot arriving while deltas are in flight.
    #[allow(unused_variables, clippy::needless_pass_by_value)]
    fn apply_delta(&mut self, side: Side, price: Px, delta: Qty) {
        todo!(
            "apply_delta is intentionally unimplemented -- see the invariants \
             above and the failing tests in tests/book_delta_spec.rs, which \
             specify the required behaviour case by case"
        )
    }

    /// Check every invariant the book must satisfy.
    ///
    /// Called after each mutation in the spec tests. Returns the first
    /// violation found so the failure names the broken property rather than
    /// just showing unexpected numbers.
    pub fn check_invariants(&self) -> Result<(), InvariantViolation> {
        for (price, qty) in &self.bids {
            if qty.is_negative() {
                return Err(InvariantViolation::NegativeQuantity {
                    side: "bid",
                    price: price.to_string(),
                    qty: qty.to_string(),
                });
            }
            if qty.is_zero() {
                return Err(InvariantViolation::ZeroQuantityLevel {
                    side: "bid",
                    price: price.to_string(),
                });
            }
        }
        for (price, qty) in &self.asks {
            if qty.is_negative() {
                return Err(InvariantViolation::NegativeQuantity {
                    side: "ask",
                    price: price.to_string(),
                    qty: qty.to_string(),
                });
            }
            if qty.is_zero() {
                return Err(InvariantViolation::ZeroQuantityLevel {
                    side: "ask",
                    price: price.to_string(),
                });
            }
        }
        if self.valid {
            if self.sid.is_none() {
                return Err(InvariantViolation::ValidWithoutSid);
            }
            if self.last_seq.is_none() {
                return Err(InvariantViolation::ValidWithoutSeq);
            }
        }
        if let (Some(bid), Some(ask)) = (self.best_bid_price(), self.best_ask_price()) {
            if bid >= ask {
                return Err(InvariantViolation::Crossed {
                    bid: bid.to_string(),
                    ask: ask.to_string(),
                });
            }
        }
        Ok(())
    }

    fn best_bid_price(&self) -> Option<Px> {
        self.bids.keys().next_back().copied()
    }

    fn best_ask_price(&self) -> Option<Px> {
        self.asks.keys().next().copied()
    }

    /// The NO-side bids, derived from the canonical YES asks.
    ///
    /// Not stored. A NO bid at `p` *is* a YES ask at `$1.00 - p`, so keeping
    /// both would be keeping the same fact twice, and two copies of a fact can
    /// disagree. Returns `None` while the book is invalid.
    #[must_use]
    pub fn no_bids(&self) -> Option<Vec<(Px, Qty)>> {
        if !self.valid {
            return None;
        }
        Some(
            self.asks
                .iter()
                .map(|(ask, qty)| (ask.complement(), *qty))
                .collect(),
        )
    }
}

/// A read-only view of a valid book.
#[derive(Debug)]
pub struct BookView<'a> {
    bids: &'a BTreeMap<Px, Qty>,
    asks: &'a BTreeMap<Px, Qty>,
}

impl BookView<'_> {
    /// Highest YES bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    /// Lowest YES ask, derived from the best NO bid.
    #[must_use]
    pub fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    /// Best ask minus best bid. Exact in fixed point.
    #[must_use]
    pub fn spread(&self) -> Option<Px> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        ask.checked_sub(bid)
    }

    /// Midpoint, rounded toward negative infinity.
    #[must_use]
    pub fn mid(&self) -> Option<Px> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        Some(bid.midpoint(ask))
    }

    #[must_use]
    pub fn bid_levels(&self) -> Vec<(Px, Qty)> {
        self.bids.iter().map(|(p, q)| (*p, *q)).collect()
    }

    #[must_use]
    pub fn ask_levels(&self) -> Vec<(Px, Qty)> {
        self.asks.iter().map(|(p, q)| (*p, *q)).collect()
    }

    #[must_use]
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    #[must_use]
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }
}

// ===========================================================================
// Runtime-known convention
// ===========================================================================

/// A book whose convention was chosen at runtime from configuration.
///
/// Narrowing to a concrete [`OrderBook<C>`] is **checked**: [`AnyBook::as_no_leg`]
/// and [`AnyBook::as_yes_leg`] return `None` for the other variant rather than
/// reinterpreting the contents. There is deliberately no unchecked accessor and
/// no `From` conversion between the two.
#[derive(Debug)]
pub enum AnyBook {
    NoLeg(OrderBook<NoLeg>),
    YesLeg(OrderBook<YesLeg>),
}

impl AnyBook {
    /// Build a book in the convention named by configuration.
    #[must_use]
    pub fn new(market: Ticker, use_yes_price: bool) -> AnyBook {
        if use_yes_price {
            AnyBook::YesLeg(OrderBook::new(market))
        } else {
            AnyBook::NoLeg(OrderBook::new(market))
        }
    }

    #[must_use]
    pub fn convention_label(&self) -> &'static str {
        match self {
            AnyBook::NoLeg(_) => NoLeg::LABEL,
            AnyBook::YesLeg(_) => YesLeg::LABEL,
        }
    }

    /// Narrow to a `NoLeg` book, or `None` if it is not one.
    #[must_use]
    pub fn as_no_leg(&self) -> Option<&OrderBook<NoLeg>> {
        match self {
            AnyBook::NoLeg(book) => Some(book),
            AnyBook::YesLeg(_) => None,
        }
    }

    /// Narrow to a `YesLeg` book, or `None` if it is not one.
    #[must_use]
    pub fn as_yes_leg(&self) -> Option<&OrderBook<YesLeg>> {
        match self {
            AnyBook::YesLeg(book) => Some(book),
            AnyBook::NoLeg(_) => None,
        }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self {
            AnyBook::NoLeg(book) => book.is_valid(),
            AnyBook::YesLeg(book) => book.is_valid(),
        }
    }

    #[must_use]
    pub fn levels(&self) -> Option<BookView<'_>> {
        match self {
            AnyBook::NoLeg(book) => book.levels(),
            AnyBook::YesLeg(book) => book.levels(),
        }
    }
}
