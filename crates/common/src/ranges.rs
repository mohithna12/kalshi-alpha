//! [`PriceRanges`] — the per-market tick grid.

use crate::px::Px;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PriceRangeError {
    #[error("band {index} has step {step}; a step must be strictly positive")]
    NonPositiveStep { index: usize, step: Px },
    #[error("band {index} has start {start} after end {end}")]
    Inverted { index: usize, start: Px, end: Px },
    #[error(
        "band {index} starts at {start}, before the previous band's start {previous}; \
             bands must be sorted ascending"
    )]
    Unsorted {
        index: usize,
        start: Px,
        previous: Px,
    },
}

/// One `{start, end, step}` band of the tick grid. `end` is **inclusive**.
///
/// # An assumption worth re-checking against live data
///
/// The docs give the band shape but do not state whether `end` is inclusive.
/// Inclusive is assumed here. The choice is benign in practice: adjacent bands
/// that share a boundary price make that price valid under either band, and
/// [`PriceRanges::snap_to_grid`] yields the same value from both. If a live
/// market ever rejects a price this type calls valid, this assumption is the
/// first thing to check.
#[derive(Copy, Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct PriceRange {
    pub start: Px,
    pub end: Px,
    pub step: Px,
}

impl PriceRange {
    #[must_use]
    pub fn contains(&self, price: Px) -> bool {
        price >= self.start && price <= self.end
    }

    /// Whether `price` sits exactly on this band's grid.
    #[must_use]
    pub fn is_on_grid(&self, price: Px) -> bool {
        if !self.contains(price) {
            return false;
        }
        let offset = i128::from(price.micros()) - i128::from(self.start.micros());
        offset % i128::from(self.step.micros()) == 0
    }

    /// Nearest on-grid price within this band. Ties resolve upward.
    #[must_use]
    fn snap(&self, price: Px) -> Option<Px> {
        if self.step.micros() <= 0 || self.start > self.end {
            return None;
        }
        let start = i128::from(self.start.micros());
        let end = i128::from(self.end.micros());
        let step = i128::from(self.step.micros());
        let target = i128::from(price.micros()).clamp(start, end);

        let steps = (target - start) / step;
        let lower = start + steps * step;
        let upper = lower + step;

        let mut best = lower;
        if upper <= end {
            let d_lower = (target - lower).abs();
            let d_upper = (upper - target).abs();
            // Ties resolve upward: `>=` rather than `>`.
            if d_upper <= d_lower {
                best = upper;
            }
        }
        i64::try_from(best).ok().map(Px::from_micros)
    }
}

/// The outcome of snapping a price onto a market's tick grid.
///
/// # Why this is not just a `Px`
///
/// Returning a bare price would silently modify data in exactly the way the
/// parser refuses to: the caller could not tell a price that was already valid
/// from one dragged in from outside the published grid entirely. Those mean
/// very different things. A price outside every band means either the grid we
/// hold is stale or our reading of it is wrong, and both are conditions to
/// surface rather than smooth over.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum GridSnap {
    /// The price was already exactly on the grid. Nothing was changed.
    Exact(Px),
    /// The price fell inside a band but between two ticks, and was moved to the
    /// nearer one. Expected only for prices we compute, never for prices the
    /// exchange sends.
    Snapped(Px),
    /// The price fell outside **every** band and was clamped to the nearest
    /// grid boundary.
    ///
    /// Callers must not treat this as a routine correction. In the capture path
    /// it is logged at WARN with the market, the price and the bands, and
    /// counted in the 60s metrics line — see [`PriceRanges`].
    Clamped(Px),
}

impl GridSnap {
    /// The resulting price, whatever the provenance. Prefer matching on the
    /// variant where the distinction matters.
    #[must_use]
    pub const fn price(self) -> Px {
        match self {
            GridSnap::Exact(p) | GridSnap::Snapped(p) | GridSnap::Clamped(p) => p,
        }
    }

    /// True when the price came from outside the published grid.
    #[must_use]
    pub const fn is_clamped(self) -> bool {
        matches!(self, GridSnap::Clamped(_))
    }

    /// True when the returned price differs from the input.
    #[must_use]
    pub const fn was_modified(self) -> bool {
        !matches!(self, GridSnap::Exact(_))
    }
}

/// A market's complete tick grid: the authoritative set of valid prices.
///
/// # This is the source of truth, and it is time-varying
///
/// Valid prices are **not** uniform across the book, and no tick size is ever
/// hardcoded in this repository. The grid also changes: `market_lifecycle_v2`
/// emits `price_level_structure_updated` carrying a fresh `price_ranges`
/// mid-stream. Persist it as an append-only series with effective-from
/// timestamps, never as a mutable field on a market row — otherwise the
/// question "what was the valid grid for this market at 14:32 on Nov 8" becomes
/// unanswerable a month later.
///
/// # The `end`-inclusive reading is checked at runtime, not just documented
///
/// [`PriceRange`] assumes `end` is inclusive because the docs do not say. A doc
/// comment only helps someone who is already debugging, so the assumption is
/// also a live canary: whenever the capture path observes a book price for
/// which [`is_valid`](Self::is_valid) returns `false`, it logs a WARN carrying
/// the market, the price and the bands, and increments an
/// `off_grid_prices` counter in the 60s metrics line. The exchange only quotes
/// on-grid prices, so a non-zero count means our grid model is wrong — and it
/// says so on day one rather than in November.
///
/// An **empty** grid is represented faithfully rather than defaulted: every
/// price is then invalid and [`snap_to_grid`](Self::snap_to_grid) returns
/// `None`. Capture must not invent a grid it was not given.
#[derive(Clone, PartialEq, Eq, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PriceRanges {
    bands: Vec<PriceRange>,
}

impl PriceRanges {
    /// Build a grid, validating that every step is positive, every band is
    /// non-inverted, and the bands are sorted ascending by `start`.
    ///
    /// Overlapping bands are permitted: a price valid under either is valid.
    pub fn new(bands: Vec<PriceRange>) -> Result<PriceRanges, PriceRangeError> {
        let mut previous: Option<Px> = None;
        for (index, band) in bands.iter().enumerate() {
            if band.step.micros() <= 0 {
                return Err(PriceRangeError::NonPositiveStep {
                    index,
                    step: band.step,
                });
            }
            if band.start > band.end {
                return Err(PriceRangeError::Inverted {
                    index,
                    start: band.start,
                    end: band.end,
                });
            }
            if let Some(previous) = previous {
                if band.start < previous {
                    return Err(PriceRangeError::Unsorted {
                        index,
                        start: band.start,
                        previous,
                    });
                }
            }
            previous = Some(band.start);
        }
        Ok(PriceRanges { bands })
    }

    #[must_use]
    pub fn bands(&self) -> &[PriceRange] {
        &self.bands
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bands.is_empty()
    }

    /// Whether `price` is an exactly representable, on-grid, in-range price.
    ///
    /// Off-grid prices are rejected rather than corrected — this answers "is
    /// what the exchange sent consistent with the grid it published", and a
    /// `false` here is a signal worth logging, not a value to fix up.
    #[must_use]
    pub fn is_valid(&self, price: Px) -> bool {
        self.bands.iter().any(|band| band.is_on_grid(price))
    }

    /// The nearest valid price to `price`, or `None` if the grid is empty.
    ///
    /// Returns a [`GridSnap`] rather than a bare [`Px`] so the caller can tell
    /// an untouched price from a snapped one from a price clamped in from
    /// outside the grid entirely. The last of those is a signal, not a fix-up —
    /// see [`GridSnap::Clamped`].
    ///
    /// Ties resolve upward (toward the higher price). Across bands, the
    /// candidate closest to `price` wins; equidistant candidates again resolve
    /// to the higher price.
    #[must_use]
    pub fn snap_to_grid(&self, price: Px) -> Option<GridSnap> {
        let within_a_band = self.bands.iter().any(|band| band.contains(price));
        let mut best: Option<Px> = None;
        for band in &self.bands {
            let Some(candidate) = band.snap(price) else {
                continue;
            };
            best = Some(match best {
                None => candidate,
                Some(current) => {
                    let d_candidate =
                        (i128::from(candidate.micros()) - i128::from(price.micros())).abs();
                    let d_current =
                        (i128::from(current.micros()) - i128::from(price.micros())).abs();
                    if d_candidate < d_current || (d_candidate == d_current && candidate > current)
                    {
                        candidate
                    } else {
                        current
                    }
                }
            });
        }
        best.map(|snapped| {
            if snapped == price {
                GridSnap::Exact(snapped)
            } else if within_a_band {
                GridSnap::Snapped(snapped)
            } else {
                GridSnap::Clamped(snapped)
            }
        })
    }
}

impl fmt::Display for PriceRanges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, band) in self.bands.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}..={} step {}", band.start, band.end, band.step)?;
        }
        write!(f, "]")
    }
}
