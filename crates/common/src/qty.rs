//! [`Qty`] — a contract quantity at Kalshi's documented `_fp` scale.

use crate::parse::{format_scaled, parse_scaled, ParseFixedError};
use std::fmt;

/// Fractional digits held by [`Qty`]. One contract == `100`.
///
/// **Verified against the docs, not assumed** (fixed_point_migration, read
/// 2026-08-28): `_fp` fields are fixed-point strings that accept 0–2 decimal
/// places on input and are emitted with exactly 2 (`"10.00"`). The minimum
/// contract granularity is 0.01. Kalshi's own suggested integer strategy is to
/// multiply the `_fp` value by 100, which is exactly what this scale is.
///
/// Do not "upgrade" this to 6 to match [`Px`]. They are different scales
/// because the exchange defines them differently.
pub const QTY_SCALE_DIGITS: u32 = 2;

/// `10^QTY_SCALE_DIGITS`. One whole contract in `_fp` units.
pub const QTY_SCALE: i64 = 100;

/// Contract quantity in hundredths of a contract. `1.00 contract == 100`.
///
/// Signed, because `orderbook_delta.delta_fp` is a signed mutation (`"-54.00"`
/// removes 54 contracts from a level). A resting level's size is non-negative,
/// but the delta that produces it is not.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Qty(i64);

impl Qty {
    pub const ZERO: Qty = Qty(0);
    pub const ONE_CONTRACT: Qty = Qty(QTY_SCALE);

    #[must_use]
    pub const fn from_fp_units(units: i64) -> Qty {
        Qty(units)
    }

    /// The raw scaled integer: hundredths of a contract.
    #[must_use]
    pub const fn fp_units(self) -> i64 {
        self.0
    }

    /// Parse an `_fp` wire string, e.g. `"10.00"` → `Qty(1_000)`.
    ///
    /// Rejects rather than truncates beyond 2 decimals: `"1.005"` is an error,
    /// because a half-hundredth of a contract cannot be represented and
    /// silently rounding it would corrupt size accounting.
    pub fn parse_fp(s: &str) -> Result<Qty, ParseFixedError> {
        parse_scaled(s, QTY_SCALE_DIGITS).map(Qty)
    }

    #[must_use]
    pub fn checked_add(self, other: Qty) -> Option<Qty> {
        self.0.checked_add(other.0).map(Qty)
    }

    #[must_use]
    pub fn checked_sub(self, other: Qty) -> Option<Qty> {
        self.0.checked_sub(other.0).map(Qty)
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// Exact decimal string with 2 fractional digits. Inverse of
    /// [`Qty::parse_fp`].
    #[must_use]
    pub fn to_fp_string(self) -> String {
        format_scaled(self.0, QTY_SCALE_DIGITS)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_fp_string())
    }
}

impl serde::Serialize for Qty {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_fp_string())
    }
}

/// Deserialize from any string form.
///
/// Using `<&str>::deserialize` here would compile and pass a test that parses
/// from a `&str` JSON slice, then fail at runtime on anything that cannot lend
/// a borrow -- `serde_json::Value`, a streaming reader, an escaped string. A
/// visitor accepts all of them.
struct QtyVisitor;

impl serde::de::Visitor<'_> for QtyVisitor {
    type Value = Qty;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a fixed-point contract-count string such as \"10.00\"")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Qty, E> {
        Qty::parse_fp(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for Qty {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Qty, D::Error> {
        d.deserialize_str(QtyVisitor)
    }
}
