//! Exact decimal-string → scaled-integer parsing.
//!
//! # Why this exists instead of `s.parse::<f64>()`
//!
//! Kalshi ticks go down to $0.0001. Neither `0.0001` nor `0.55` has an exact
//! binary representation, so any `f64` in the path — even one used "just to
//! validate" and then discarded — introduces drift that is invisible at the
//! point it happens and unrecoverable later. This crate denies
//! `clippy::float_arithmetic` at the crate root so the compiler enforces it.
//!
//! The algorithm is entirely integer: split on the decimal point, validate that
//! both halves are ASCII digits, accumulate the integer part in `i128`, scale it
//! up, then add the fractional digits right-padded to the target scale.
//!
//! # Excess precision is an error, not a truncation
//!
//! If the input carries more fractional digits than the target scale can hold,
//! we return [`ParseFixedError::ExcessPrecision`] rather than dropping them.
//! Silent truncation is precisely the failure the dual raw/parsed column scheme
//! in the storage layer exists to detect, and it is better to log loudly and
//! store the raw string than to quietly round.
//!
//! The one exception is trailing zeros: `"1.0000000"` at scale 1e-6 is accepted
//! and yields exactly `1_000_000`, because no information is lost. Only a
//! *non-zero* digit beyond the scale is rejected. This distinction matters —
//! rejecting harmless trailing zeros would drop capturable messages for no gain.

/// The largest scale this parser will accept, chosen so `10i128.pow(scale)`
/// cannot overflow. Real scales in use are 2 (`_fp`) and 6 (`_dollars`).
const MAX_SCALE_DIGITS: u32 = 18;

/// Everything that can go wrong turning a wire string into a scaled integer.
///
/// Each variant carries the offending input so the daemon can log it verbatim
/// alongside the raw column it stored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseFixedError {
    #[error("cannot parse an empty string as a fixed-point value")]
    Empty,

    #[error(
        "invalid character {ch:?} in {input:?}: expected only ASCII digits, \
             an optional leading '-', and at most one '.'"
    )]
    InvalidChar { input: String, ch: char },

    #[error("no digits before the decimal point in {input:?}")]
    MissingIntegerDigits { input: String },

    #[error("no digits after the decimal point in {input:?}")]
    MissingFractionDigits { input: String },

    #[error("more than one decimal point in {input:?}")]
    MultipleDecimalPoints { input: String },

    #[error(
        "{input:?} carries more precision than scale 1e-{scale} can hold: \
             excess digits {excess:?} are non-zero. Refusing to truncate."
    )]
    ExcessPrecision {
        input: String,
        scale: u32,
        excess: String,
    },

    #[error("{input:?} at scale 1e-{scale} does not fit in i64")]
    Overflow { input: String, scale: u32 },

    #[error("internal: scale {scale} exceeds the supported maximum of {MAX_SCALE_DIGITS}")]
    UnsupportedScale { scale: u32 },
}

/// Parse `input` as a decimal string into an integer scaled by `10^scale`.
///
/// `parse_scaled("0.5500", 6) == Ok(550_000)`.
///
/// Accepts: an optional leading `-`, one or more ASCII digits, optionally a `.`
/// followed by one or more ASCII digits. Everything else — whitespace, a leading
/// `+`, `.5`, `5.`, `1e6`, non-ASCII digits — is rejected.
pub fn parse_scaled(input: &str, scale: u32) -> Result<i64, ParseFixedError> {
    if scale > MAX_SCALE_DIGITS {
        return Err(ParseFixedError::UnsupportedScale { scale });
    }
    if input.is_empty() {
        return Err(ParseFixedError::Empty);
    }

    let (negative, body) = match input.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, input),
    };

    let mut halves = body.split('.');
    let int_part = halves.next().unwrap_or("");
    let frac_part = halves.next();
    if halves.next().is_some() {
        return Err(ParseFixedError::MultipleDecimalPoints {
            input: input.to_owned(),
        });
    }

    if int_part.is_empty() {
        return Err(ParseFixedError::MissingIntegerDigits {
            input: input.to_owned(),
        });
    }
    reject_non_digits(int_part, input)?;

    let frac = match frac_part {
        // A '.' was present, so digits must follow it. "5." is malformed.
        Some("") => {
            return Err(ParseFixedError::MissingFractionDigits {
                input: input.to_owned(),
            })
        }
        Some(f) => {
            reject_non_digits(f, input)?;
            f
        }
        None => "",
    };

    let scale_len =
        usize::try_from(scale).map_err(|_| ParseFixedError::UnsupportedScale { scale })?;

    // Integer part, accumulated in i128 with checked arithmetic so an absurdly
    // long input errors rather than panicking under overflow-checks.
    let overflow = || ParseFixedError::Overflow {
        input: input.to_owned(),
        scale,
    };
    let mut value: i128 = 0;
    for byte in int_part.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(byte - b'0')))
            .ok_or_else(overflow)?;
    }
    value = value.checked_mul(10i128.pow(scale)).ok_or_else(overflow)?;

    // Fractional part, right-padded with zeros to exactly `scale` digits.
    let mut frac_value: i128 = 0;
    for i in 0..scale_len {
        let digit = frac
            .as_bytes()
            .get(i)
            .map_or(0i128, |byte| i128::from(byte - b'0'));
        frac_value = frac_value * 10 + digit;
    }
    value = value.checked_add(frac_value).ok_or_else(overflow)?;

    // Anything beyond the scale must be zero, or we would be truncating.
    if frac.len() > scale_len {
        let excess = &frac[scale_len..];
        if excess.bytes().any(|b| b != b'0') {
            return Err(ParseFixedError::ExcessPrecision {
                input: input.to_owned(),
                scale,
                excess: excess.to_owned(),
            });
        }
    }

    if negative {
        value = -value;
    }
    i64::try_from(value).map_err(|_| overflow())
}

fn reject_non_digits(part: &str, input: &str) -> Result<(), ParseFixedError> {
    match part.chars().find(|c| !c.is_ascii_digit()) {
        Some(ch) => Err(ParseFixedError::InvalidChar {
            input: input.to_owned(),
            ch,
        }),
        None => Ok(()),
    }
}

/// Render a scaled integer back to its exact decimal string with `scale`
/// fractional digits. Pure integer formatting; the inverse of [`parse_scaled`].
pub fn format_scaled(value: i64, scale: u32) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = u128::from(value.unsigned_abs());
    let divisor = 10u128.pow(scale);
    let whole = magnitude / divisor;
    let frac = magnitude % divisor;
    let width = usize::try_from(scale).unwrap_or(0);
    format!("{sign}{whole}.{frac:0width$}")
}
