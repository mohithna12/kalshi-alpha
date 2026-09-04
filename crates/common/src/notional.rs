//! [`Notional`] — the value of a quantity of contracts at a price.

use crate::parse::format_scaled;
use crate::px::{Px, PX_SCALE_DIGITS};
use crate::qty::{Qty, QTY_SCALE_DIGITS};
use std::fmt;

/// Fractional digits held by [`Notional`].
///
/// `Px` is scale 1e-6 and `Qty` is scale 1e-2, so their product lands at
/// 1e-8 **exactly**. This is the whole reason the product needs no rounding:
/// multiplying two scaled integers adds their scales, and 6 + 2 = 8.
pub const NOTIONAL_SCALE_DIGITS: u32 = PX_SCALE_DIGITS + QTY_SCALE_DIGITS;

/// `10^NOTIONAL_SCALE_DIGITS`.
pub const NOTIONAL_SCALE: i64 = 100_000_000;

/// `Px × Qty`, in hundred-millionths of a dollar (scale 1e-8).
///
/// # Overflow
///
/// The product is formed in `i128` and range-checked on the way back to `i64`,
/// even though `i64` would hold every realistic value: `$1.00 × 1M contracts` is
/// only `1e14`, comfortably inside `i64`'s `9.2e18`. The `i128` intermediate is
/// kept anyway because the cost is nil and the failure mode of getting it wrong
/// is silent corruption of records that cannot be re-captured.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Notional(i64);

impl Notional {
    pub const ZERO: Notional = Notional(0);

    /// `price × quantity`, exact.
    ///
    /// Returns `None` only on `i64` overflow, which requires a notional beyond
    /// ±$92 billion.
    #[must_use]
    pub fn from_px_qty(price: Px, quantity: Qty) -> Option<Notional> {
        let product = i128::from(price.micros()).checked_mul(i128::from(quantity.fp_units()))?;
        i64::try_from(product).ok().map(Notional)
    }

    #[must_use]
    pub const fn from_units(units: i64) -> Notional {
        Notional(units)
    }

    /// The raw scaled integer at scale 1e-8.
    #[must_use]
    pub const fn units(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn checked_add(self, other: Notional) -> Option<Notional> {
        self.0.checked_add(other.0).map(Notional)
    }

    #[must_use]
    pub fn checked_sub(self, other: Notional) -> Option<Notional> {
        self.0.checked_sub(other.0).map(Notional)
    }

    /// **The single scale-reducing function in this crate.**
    ///
    /// Converts scale 1e-8 down to [`Px`]'s scale 1e-6 by dividing by 100.
    ///
    /// # Rounding rule: half away from zero
    ///
    /// A remainder of exactly half a unit rounds away from zero, so `+0.005`
    /// rounds up and `-0.005` rounds down. This is symmetric about zero, which
    /// matters because [`Notional`] is signed: a rounding rule biased in one
    /// direction (such as Rust's default truncation toward zero, or floor)
    /// would accumulate a systematic drift when summing a mixed-sign series of
    /// values, and a book's netted flow is exactly such a series.
    ///
    /// This is a *lossy* conversion by construction — every other operation in
    /// this crate is exact. If you find yourself wanting a second rounding rule,
    /// add a second named function rather than a parameter, so that every
    /// rounding site stays greppable.
    #[must_use]
    pub fn to_micro_dollars(self) -> i64 {
        const DIVISOR: i128 = 100;
        let value = i128::from(self.0);
        let quotient = value / DIVISOR;
        let remainder = value % DIVISOR;
        let rounded = if remainder.abs() * 2 >= DIVISOR {
            quotient + if value.is_negative() { -1 } else { 1 }
        } else {
            quotient
        };
        i64::try_from(rounded).unwrap_or(if rounded.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        })
    }

    /// Exact decimal string with 8 fractional digits.
    #[must_use]
    pub fn to_dollar_string(self) -> String {
        format_scaled(self.0, NOTIONAL_SCALE_DIGITS)
    }
}

impl fmt::Display for Notional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.to_dollar_string())
    }
}
