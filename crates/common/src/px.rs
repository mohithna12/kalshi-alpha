//! [`Px`] — a price in micro-dollars.

use crate::parse::{format_scaled, parse_scaled, ParseFixedError};
use std::fmt;

/// Fractional digits held by [`Px`]. `$1.00` == `1_000_000`.
///
/// The wire format (`*_dollars`) carries **at most 4** decimal places, and the
/// smallest documented tick is `$0.0001`. Six digits is deliberately a superset:
/// it stores every wire value exactly while leaving room for the 6-decimal
/// intermediate values Kalshi's fee math can produce.
pub const PX_SCALE_DIGITS: u32 = 6;

/// `10^PX_SCALE_DIGITS`. One dollar in micro-dollars.
pub const PX_SCALE: i64 = 1_000_000;

/// Price in micro-dollars. `$1.00 == 1_000_000`.
///
/// A distinct newtype so the compiler rejects mixing prices with quantities.
/// There is deliberately no arithmetic against raw `i64` and no operator
/// overloading: every scale conversion is a named, greppable method.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Px(i64);

impl Px {
    pub const ZERO: Px = Px(0);
    /// `$1.00`. A binary market's YES and NO legs sum to exactly this.
    pub const ONE_DOLLAR: Px = Px(PX_SCALE);

    #[must_use]
    pub const fn from_micros(micros: i64) -> Px {
        Px(micros)
    }

    #[must_use]
    pub const fn micros(self) -> i64 {
        self.0
    }

    /// Parse a `*_dollars` wire string, e.g. `"0.5500"` → `Px(550_000)`.
    ///
    /// Exact: no `f64` is involved at any point. Rejects rather than truncates
    /// when the input carries more than 6 significant decimal places.
    pub fn parse_dollars(s: &str) -> Result<Px, ParseFixedError> {
        parse_scaled(s, PX_SCALE_DIGITS).map(Px)
    }

    /// The complementary price: `$1.00 - self`.
    ///
    /// # Why this must be fixed-point
    ///
    /// A YES bid at `$0.4300` *is* a NO ask at `$0.5700`. In fixed point the two
    /// sum to exactly `1_000_000`. In `f64`, `0.43 + 0.57 != 1.0`, and the error
    /// compounds every time a book is converted between representations. This
    /// identity is what lets the order book store levels canonically on the YES
    /// side and derive the NO view, with no possibility of the two drifting.
    ///
    /// Saturates rather than panicking for inputs below `i64::MIN + PX_SCALE`,
    /// a value no wire price can reach (real prices lie in `[0, $1]`). The
    /// daemon must never panic on malformed exchange input; use
    /// [`Px::checked_complement`] where the distinction matters.
    #[must_use]
    pub fn complement(self) -> Px {
        debug_assert!(
            self >= Px::ZERO && self <= Px::ONE_DOLLAR,
            "complement() called on {self} which is outside [0, $1.00]"
        );
        Px(PX_SCALE.saturating_sub(self.0))
    }

    /// [`Px::complement`] that reports overflow instead of saturating.
    #[must_use]
    pub fn checked_complement(self) -> Option<Px> {
        PX_SCALE.checked_sub(self.0).map(Px)
    }

    #[must_use]
    pub fn checked_add(self, other: Px) -> Option<Px> {
        self.0.checked_add(other.0).map(Px)
    }

    /// Difference between two prices — a spread, when applied to an ask and a bid.
    #[must_use]
    pub fn checked_sub(self, other: Px) -> Option<Px> {
        self.0.checked_sub(other.0).map(Px)
    }

    /// Midpoint of two prices, rounded toward negative infinity.
    ///
    /// Computed in `i128` so the sum cannot overflow. The rounding direction is
    /// fixed and documented rather than left to the caller because a mid is a
    /// derived display value, never a settlement quantity.
    #[must_use]
    pub fn midpoint(self, other: Px) -> Px {
        let sum = i128::from(self.0) + i128::from(other.0);
        // Floor division: -1 / 2 == 0 in Rust, but we want -1.
        let mid = sum.div_euclid(2);
        Px(i64::try_from(mid).unwrap_or(if mid.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }))
    }

    /// Exact decimal string with 6 fractional digits. Inverse of
    /// [`Px::parse_dollars`].
    #[must_use]
    pub fn to_dollar_string(self) -> String {
        format_scaled(self.0, PX_SCALE_DIGITS)
    }
}

impl fmt::Display for Px {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.to_dollar_string())
    }
}

impl serde::Serialize for Px {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_dollar_string())
    }
}

/// Deserialize from any string form.
///
/// Using `<&str>::deserialize` here would compile and pass a test that parses
/// from a `&str` JSON slice, then fail at runtime on anything that cannot lend
/// a borrow -- `serde_json::Value`, a streaming reader, an escaped string. A
/// visitor accepts all of them.
struct PxVisitor;

impl serde::de::Visitor<'_> for PxVisitor {
    type Value = Px;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a fixed-point dollar string such as \"0.5500\"")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Px, E> {
        Px::parse_dollars(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for Px {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Px, D::Error> {
        d.deserialize_str(PxVisitor)
    }
}
