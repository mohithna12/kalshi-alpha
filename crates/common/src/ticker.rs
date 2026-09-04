//! [`Ticker`] and [`Side`].

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TickerError {
    #[error("empty ticker")]
    Empty,
    #[error("ticker {input:?} contains {ch:?}; expected only A-Z, a-z, 0-9, '-', '.', '_'")]
    InvalidChar { input: String, ch: char },
    #[error("ticker {input:?} is unsafe to use as a path component")]
    UnsafeAsPath { input: String },
    #[error("ticker {input:?} exceeds the {MAX_TICKER_LEN}-byte maximum")]
    TooLong { input: String },
    #[error("ticker {input:?} is not uppercase; use Ticker::parse to normalize it")]
    NotUppercase { input: String },
}

/// Generous upper bound. The longest ticker seen in the docs is ~26 bytes;
/// this exists only to stop an absurd value becoming a filename.
const MAX_TICKER_LEN: usize = 128;

/// A Kalshi market, event, or series ticker.
///
/// # The documented pattern is wrong; do not "restore" it
///
/// `asyncapi.yaml` gives `marketTicker` the pattern `^[A-Z0-9-]+$`, but that
/// regex rejects three of the spec's *own* example tickers — `CPI-22DEC-TN0.1`,
/// `FED-23DEC-T3.00`, `HIGHNY-22DEC23-B53.5` — all of which contain `.`. The
/// examples are evidently right and the pattern is stale.
///
/// Enforcing the documented regex would therefore silently refuse real markets,
/// and a market we cannot parse is a market we cannot capture. Since data not
/// recorded is gone permanently, this validation is deliberately permissive
/// about *format* and strict only about what is genuinely dangerous.
///
/// # What is actually enforced, and why
///
/// Tickers become directory names and Parquet partition keys, so the real risk
/// is path traversal and filesystem confusion rather than an unexpected
/// character class. Rejected: empty, over 128 bytes, anything outside
/// `[A-Za-z0-9._-]`, any leading `.`, and the literal `.` / `..` components.
/// That admits every ticker Kalshi has ever documented while making a ticker
/// unable to escape the data directory.
///
/// # Case is normalized to uppercase, deliberately
///
/// A ticker is a directory name and a Parquet partition key. macOS (APFS,
/// case-insensitive by default) would fold `KXNFL-ABC` and `kxnfl-abc` into one
/// directory; Linux would keep them as two. Capture on one and analyse on the
/// other and the discrepancy shows up months later as silently missing data.
///
/// [`Ticker::parse`] therefore uppercases on construction, so the two can never
/// denote different partitions on any filesystem. Kalshi tickers are uppercase
/// in every documented example, so this should be a no-op in practice — and
/// because it should be, the capture path compares the normalized value against
/// the raw wire string and logs a WARN when they differ. Use
/// [`Ticker::parse_strict`] where a mismatch should be an error instead.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Ticker(String);

impl Ticker {
    /// Validate and normalize to uppercase. See the type docs for why case is
    /// folded rather than preserved.
    pub fn parse(s: &str) -> Result<Ticker, TickerError> {
        Ticker::validate(s)?;
        Ok(Ticker(s.to_ascii_uppercase()))
    }

    /// Validate without normalizing: a lowercase ticker is an error.
    ///
    /// Use where an unexpected case difference should stop the caller rather
    /// than be folded away.
    pub fn parse_strict(s: &str) -> Result<Ticker, TickerError> {
        Ticker::validate(s)?;
        if s.bytes().any(|b| b.is_ascii_lowercase()) {
            return Err(TickerError::NotUppercase {
                input: s.to_owned(),
            });
        }
        Ok(Ticker(s.to_owned()))
    }

    /// Whether `raw` was already in canonical form. The capture path uses this
    /// to decide whether normalization silently changed anything.
    #[must_use]
    pub fn is_canonical(raw: &str) -> bool {
        !raw.bytes().any(|b| b.is_ascii_lowercase())
    }

    /// The ticker as a single filesystem path component. Guaranteed by
    /// construction to contain no separator, no leading `.`, and no whitespace,
    /// so it is safe to join directly onto the data root.
    #[must_use]
    pub fn as_partition_component(&self) -> &str {
        &self.0
    }

    fn validate(s: &str) -> Result<(), TickerError> {
        if s.is_empty() {
            return Err(TickerError::Empty);
        }
        if s.len() > MAX_TICKER_LEN {
            return Err(TickerError::TooLong {
                input: s.to_owned(),
            });
        }
        if let Some(ch) = s
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || *c == '-' || *c == '.' || *c == '_'))
        {
            return Err(TickerError::InvalidChar {
                input: s.to_owned(),
                ch,
            });
        }
        // A ticker becomes a path component. A leading '.' hides it, and '.'
        // or '..' would escape or alias the data directory.
        if s.starts_with('.') {
            return Err(TickerError::UnsafeAsPath {
                input: s.to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Ticker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for Ticker {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

/// Deserialize from any string form.
///
/// Using `<&str>::deserialize` here would compile and pass a test that parses
/// from a `&str` JSON slice, then fail at runtime on anything that cannot lend
/// a borrow -- `serde_json::Value`, a streaming reader, an escaped string. A
/// visitor accepts all of them.
struct TickerVisitor;

impl serde::de::Visitor<'_> for TickerVisitor {
    type Value = Ticker;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a Kalshi ticker")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Ticker, E> {
        Ticker::parse(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for Ticker {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Ticker, D::Error> {
        d.deserialize_str(TickerVisitor)
    }
}

/// Which leg of a binary market a price or order refers to.
///
/// The order book carries **bids only, on both sides**. A `Yes` entry is a bid
/// to buy YES; a `No` entry is a bid to buy NO, which is economically an ask on
/// YES at the complementary price. See [`crate::Px::complement`].
#[derive(
    Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Yes,
    No,
}

impl Side {
    #[must_use]
    pub const fn opposite(self) -> Side {
        match self {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Side::Yes => "yes",
            Side::No => "no",
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
