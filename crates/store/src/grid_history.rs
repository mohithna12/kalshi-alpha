//! The tick-grid time series, deduplicated to changes.
//!
//! # Change events, not a poll log
//!
//! Reconciliation re-reads every market's `price_ranges` every 300 seconds. The
//! grid almost never changes, so writing every observation would produce
//! roughly 288 identical rows per market per day — a poll log, in which finding
//! the handful of real changes means diffing consecutive rows forever.
//!
//! So a row is written only when the grid's **content** differs from the last
//! one recorded for that market. Unchanged re-observations are counted in the
//! metrics line and otherwise discarded.
//!
//! The dedupe key is the content hash, not `(market, observed_at, source)`:
//! observation timestamps are always distinct, so keying on them would dedupe
//! nothing. Source is kept in the key so a lifecycle-reported change is
//! recorded even if a discovery read happened to see the same grid first —
//! those carry different information (see `effective_at`).

use std::collections::HashMap;

/// Where an observation came from.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GridSource {
    Discovery,
    Lifecycle,
}

impl GridSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            GridSource::Discovery => "discovery",
            GridSource::Lifecycle => "lifecycle",
        }
    }
}

/// Whether an observation should be written.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GridVerdict {
    /// First time this market's grid has been seen.
    FirstObservation,
    /// The grid differs from the last recorded one. A real change event.
    Changed,
    /// Identical to what is already recorded. Counted, not written.
    Unchanged,
}

impl GridVerdict {
    #[must_use]
    pub const fn should_write(self) -> bool {
        matches!(self, GridVerdict::FirstObservation | GridVerdict::Changed)
    }
}

/// Content hash of a grid, used as the dedupe key.
///
/// FNV-1a over the canonical JSON text. Not cryptographic — it only needs to
/// distinguish grids, and a collision would at worst suppress one change row,
/// which the raw column would still reveal.
#[must_use]
pub fn grid_hash(price_ranges_json: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in price_ranges_json.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Tracks the last recorded grid per `(market, source)`.
#[derive(Debug, Default)]
pub struct GridHistory {
    last: HashMap<(String, GridSource), String>,
    unchanged_observations: u64,
    changes_recorded: u64,
}

impl GridHistory {
    #[must_use]
    pub fn new() -> GridHistory {
        GridHistory::default()
    }

    /// Decide whether this observation is worth a row, and record it if so.
    pub fn observe(
        &mut self,
        market: &str,
        source: GridSource,
        price_ranges_json: &str,
    ) -> GridVerdict {
        let hash = grid_hash(price_ranges_json);
        let key = (market.to_owned(), source);
        match self.last.get(&key) {
            Some(previous) if *previous == hash => {
                self.unchanged_observations += 1;
                GridVerdict::Unchanged
            }
            Some(_) => {
                self.last.insert(key, hash);
                self.changes_recorded += 1;
                GridVerdict::Changed
            }
            None => {
                self.last.insert(key, hash);
                self.changes_recorded += 1;
                GridVerdict::FirstObservation
            }
        }
    }

    /// Suppressed re-observations, for the metrics line. A large number here is
    /// healthy: it is the poll log that was not written.
    #[must_use]
    pub fn unchanged_observations(&self) -> u64 {
        self.unchanged_observations
    }

    #[must_use]
    pub fn changes_recorded(&self) -> u64 {
        self.changes_recorded
    }

    #[must_use]
    pub fn tracked_markets(&self) -> usize {
        self.last.len()
    }
}
