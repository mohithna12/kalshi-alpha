//! Shared value types for Kalshi market data.
//!
//! Everything here is exact integer arithmetic. No `f64` appears in this crate,
//! not even as a parsing intermediate — the crate denies
//! `clippy::float_arithmetic` so the compiler enforces it rather than a review
//! comment.
//!
//! # Scales, and why they differ
//!
//! | Type | Scale | Wire form |
//! |---|---|---|
//! | [`Px`] | 1e-6 (micro-dollars) | `*_dollars`, ≤ 4 decimals, e.g. `"0.5500"` |
//! | [`Qty`] | 1e-2 (hundredths of a contract) | `*_fp`, exactly 2 decimals, e.g. `"10.00"` |
//! | [`Notional`] | 1e-8 | derived: `Px × Qty` |
//!
//! `Px` and `Qty` are *not* the same scale, because Kalshi does not define them
//! that way. `Qty`'s 1e-2 is taken from the documented `_fp` format (minimum
//! granularity 0.01 contracts); `Px`'s 1e-6 is a deliberate superset of the
//! 4-decimal wire format with room for Kalshi's 6-decimal fee intermediates.
//! Their product lands at 1e-8 exactly, which is why [`Notional`] needs no
//! rounding to form — only to reduce.
//!
//! The three are distinct newtypes with no arithmetic against raw `i64` and no
//! operator overloading, so mixing a price with a quantity is a compile error
//! and every scale conversion is a named, greppable method.

// The daemon must never panic on malformed exchange input; it logs and retries.
// Tests may unwrap freely.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![deny(clippy::float_arithmetic, clippy::as_conversions)]

pub mod notional;
pub mod parse;
pub mod px;
pub mod qty;
pub mod ranges;
pub mod ticker;

pub use notional::{Notional, NOTIONAL_SCALE, NOTIONAL_SCALE_DIGITS};
pub use parse::{format_scaled, parse_scaled, ParseFixedError};
pub use px::{Px, PX_SCALE, PX_SCALE_DIGITS};
pub use qty::{Qty, QTY_SCALE, QTY_SCALE_DIGITS};
pub use ranges::{GridSnap, PriceRange, PriceRangeError, PriceRanges};
pub use ticker::{Side, Ticker, TickerError};
