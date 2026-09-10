//! Arrow schemas for each capture channel.
//!
//! # Every price and quantity is stored twice
//!
//! Each such value gets two columns: the raw wire string exactly as received
//! (`*_raw`, Utf8) beside the parsed integer (`*_micros` or `*_fp_units`,
//! Int64). This is not redundancy for its own sake — it is the only thing that
//! makes a parser bug survivable. If a defect is found in November, the raw
//! column allows re-deriving every value; without it, the season is lost. The
//! cost is a few bytes per row.
//!
//! Parsed columns are **nullable**. A value that fails to parse is written with
//! a null integer and a populated raw string rather than being dropped, so a
//! message we could not understand is still a message we recorded.
//!
//! # Timestamps are UTC nanoseconds, everywhere
//!
//! `received_at_ns` is nanoseconds since the Unix epoch, in UTC. No local
//! timezone appears anywhere in this path. Capture runs across the 1 November
//! DST transition, where a local-time column would repeat an hour and make an
//! ordering ambiguous — so local time is never stored, and partitioning uses
//! the UTC date.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// The channels captured, each written to its own partition.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Channel {
    OrderbookSnapshot,
    OrderbookDelta,
    Ticker,
    Trade,
    MarketLifecycle,
    EventFeeUpdate,
    /// Tick-grid observations, from both discovery and lifecycle events.
    PriceRanges,
    /// Anything that failed to deserialize. Never dropped.
    Unparsed,
    /// Subscription control frames: `subscribed`, `unsubscribed`, `ok`,
    /// `error`.
    ///
    /// These carry no market data, but they DO consume sequence numbers on
    /// their sid. Discarding them punches holes in the seq stream that look
    /// exactly like dropped market data, so gap detection cannot tell a real
    /// loss from a routine acknowledgement. Storing them makes the stream
    /// complete and gaps unambiguous.
    Control,
}

impl Channel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Channel::OrderbookSnapshot => "orderbook_snapshot",
            Channel::OrderbookDelta => "orderbook_delta",
            Channel::Ticker => "ticker",
            Channel::Trade => "trade",
            Channel::MarketLifecycle => "market_lifecycle",
            Channel::EventFeeUpdate => "event_fee_update",
            Channel::PriceRanges => "price_ranges",
            Channel::Unparsed => "unparsed",
            Channel::Control => "control",
        }
    }

    #[must_use]
    pub const fn all() -> [Channel; 9] {
        [
            Channel::OrderbookSnapshot,
            Channel::OrderbookDelta,
            Channel::Ticker,
            Channel::Trade,
            Channel::MarketLifecycle,
            Channel::EventFeeUpdate,
            Channel::PriceRanges,
            Channel::Unparsed,
            Channel::Control,
        ]
    }

    #[must_use]
    pub fn schema(self) -> Arc<Schema> {
        match self {
            Channel::OrderbookSnapshot => orderbook_snapshot_schema(),
            Channel::OrderbookDelta => orderbook_delta_schema(),
            Channel::Ticker => ticker_schema(),
            Channel::Trade => trade_schema(),
            Channel::MarketLifecycle => market_lifecycle_schema(),
            Channel::EventFeeUpdate => event_fee_update_schema(),
            Channel::PriceRanges => price_ranges_schema(),
            Channel::Unparsed => unparsed_schema(),
            Channel::Control => control_schema(),
        }
    }

    /// Pairs of `(raw_column, parsed_column)` that a round-trip check must
    /// verify. Every price/quantity column in the schema appears here.
    #[must_use]
    pub fn dual_columns(self) -> Vec<DualColumn> {
        match self {
            Channel::OrderbookSnapshot => vec![
                DualColumn::price("price_raw", "price_micros"),
                DualColumn::quantity("size_raw", "size_fp_units"),
            ],
            Channel::OrderbookDelta => vec![
                DualColumn::price("price_raw", "price_micros"),
                DualColumn::quantity("delta_raw", "delta_fp_units"),
            ],
            Channel::Ticker => vec![
                DualColumn::price("price_raw", "price_micros"),
                DualColumn::price("yes_bid_raw", "yes_bid_micros"),
                DualColumn::price("yes_ask_raw", "yes_ask_micros"),
                DualColumn::quantity("yes_bid_size_raw", "yes_bid_size_fp_units"),
                DualColumn::quantity("yes_ask_size_raw", "yes_ask_size_fp_units"),
                DualColumn::quantity("volume_raw", "volume_fp_units"),
                DualColumn::quantity("open_interest_raw", "open_interest_fp_units"),
            ],
            Channel::Trade => vec![
                DualColumn::price("yes_price_raw", "yes_price_micros"),
                DualColumn::price("no_price_raw", "no_price_micros"),
                DualColumn::quantity("count_raw", "count_fp_units"),
            ],
            Channel::MarketLifecycle => {
                vec![DualColumn::price(
                    "settlement_value_raw",
                    "settlement_value_micros",
                )]
            }
            Channel::EventFeeUpdate
            | Channel::PriceRanges
            | Channel::Unparsed
            | Channel::Control => vec![],
        }
    }
}

/// The scale a parsed column is written at.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Scale {
    /// Micro-dollars: `$1.00 == 1_000_000`.
    Price,
    /// Hundredths of a contract: `1.00 contract == 100`.
    Quantity,
}

/// A raw-string column paired with the integer column derived from it.
#[derive(Copy, Clone, Debug)]
pub struct DualColumn {
    pub raw: &'static str,
    pub parsed: &'static str,
    pub scale: Scale,
}

impl DualColumn {
    #[must_use]
    pub const fn price(raw: &'static str, parsed: &'static str) -> DualColumn {
        DualColumn {
            raw,
            parsed,
            scale: Scale::Price,
        }
    }

    #[must_use]
    pub const fn quantity(raw: &'static str, parsed: &'static str) -> DualColumn {
        DualColumn {
            raw,
            parsed,
            scale: Scale::Quantity,
        }
    }
}

/// Columns present on every row, whatever the channel.
fn common_fields() -> Vec<Field> {
    vec![
        // Local receive clock, stamped as early as the read path allows.
        // Nanoseconds since the Unix epoch, UTC. Never local time.
        Field::new(
            "received_at_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        // The exchange's own timestamp, when it supplied one. Distinct from
        // ours on purpose: the difference is latency, and conflating them
        // would destroy that.
        Field::new("exchange_ts_ms", DataType::Int64, true),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("sid", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
        // The complete original message. The last line of defence: if every
        // typed column is somehow wrong, this is not.
        Field::new("raw_message", DataType::Utf8, false),
    ]
}

fn schema_with(extra: Vec<Field>) -> Arc<Schema> {
    let mut fields = common_fields();
    fields.extend(extra);
    Arc::new(Schema::new(fields))
}

/// One row per price level, flattened out of the snapshot's arrays.
fn orderbook_snapshot_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("market_ticker", DataType::Utf8, false),
        Field::new("market_id", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("price_raw", DataType::Utf8, false),
        Field::new("price_micros", DataType::Int64, true),
        Field::new("size_raw", DataType::Utf8, false),
        Field::new("size_fp_units", DataType::Int64, true),
        // Which level this was within its side, so the snapshot's ordering is
        // reconstructible without re-sorting.
        Field::new("level_index", DataType::Int32, false),
    ])
}

fn orderbook_delta_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("market_ticker", DataType::Utf8, false),
        Field::new("market_id", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("price_raw", DataType::Utf8, false),
        Field::new("price_micros", DataType::Int64, true),
        // Signed: negative removes size from the level.
        Field::new("delta_raw", DataType::Utf8, false),
        Field::new("delta_fp_units", DataType::Int64, true),
    ])
}

fn ticker_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("market_ticker", DataType::Utf8, false),
        Field::new("market_id", DataType::Utf8, true),
        Field::new("price_raw", DataType::Utf8, true),
        Field::new("price_micros", DataType::Int64, true),
        Field::new("yes_bid_raw", DataType::Utf8, true),
        Field::new("yes_bid_micros", DataType::Int64, true),
        Field::new("yes_ask_raw", DataType::Utf8, true),
        Field::new("yes_ask_micros", DataType::Int64, true),
        Field::new("yes_bid_size_raw", DataType::Utf8, true),
        Field::new("yes_bid_size_fp_units", DataType::Int64, true),
        Field::new("yes_ask_size_raw", DataType::Utf8, true),
        Field::new("yes_ask_size_fp_units", DataType::Int64, true),
        Field::new("volume_raw", DataType::Utf8, true),
        Field::new("volume_fp_units", DataType::Int64, true),
        Field::new("open_interest_raw", DataType::Utf8, true),
        Field::new("open_interest_fp_units", DataType::Int64, true),
    ])
}

fn trade_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("trade_id", DataType::Utf8, true),
        Field::new("market_ticker", DataType::Utf8, false),
        Field::new("yes_price_raw", DataType::Utf8, true),
        Field::new("yes_price_micros", DataType::Int64, true),
        Field::new("no_price_raw", DataType::Utf8, true),
        Field::new("no_price_micros", DataType::Int64, true),
        Field::new("count_raw", DataType::Utf8, true),
        Field::new("count_fp_units", DataType::Int64, true),
        // Canonical direction fields.
        Field::new("taker_outcome_side", DataType::Utf8, true),
        Field::new("taker_book_side", DataType::Utf8, true),
        // Deprecated (protection lapsed 2026-05-14). Captured when present,
        // never relied upon.
        Field::new("taker_side_deprecated", DataType::Utf8, true),
        Field::new("is_block_trade", DataType::Boolean, true),
    ])
}

fn market_lifecycle_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("market_ticker", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, true),
        // Ground truth for a later model, arriving on the price stream.
        Field::new("settlement_value_raw", DataType::Utf8, true),
        Field::new("settlement_value_micros", DataType::Int64, true),
        Field::new("open_ts", DataType::Int64, true),
        Field::new("close_ts", DataType::Int64, true),
        Field::new("determination_ts", DataType::Int64, true),
        Field::new("settled_ts", DataType::Int64, true),
        Field::new("is_deactivated", DataType::Boolean, true),
        Field::new("price_level_structure", DataType::Utf8, true),
    ])
}

fn event_fee_update_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("event_ticker", DataType::Utf8, false),
        // Null means the override was cleared, which is itself an event.
        Field::new("fee_type_override", DataType::Utf8, true),
        // Kept as the raw wire token. Never routed through f64 on the way in;
        // the consumer decides how to interpret it.
        Field::new("fee_multiplier_override_raw", DataType::Utf8, true),
    ])
}

/// The tick-grid time series.
///
/// `observed_at` and `effective_at` are deliberately separate. A discovery read
/// says what the grid *was when we looked*; a lifecycle event says when it
/// *changed*. Collapsing them makes "what grid was in force at 14:32 on Nov 8"
/// unanswerable.
fn price_ranges_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("market_ticker", DataType::Utf8, false),
        // `discovery` or `lifecycle`.
        Field::new("source", DataType::Utf8, false),
        // When the wire said the grid took effect. Null for discovery reads,
        // which carry no such information. Never backfilled from observed_at.
        Field::new(
            "effective_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("price_level_structure", DataType::Utf8, true),
        // The grid as JSON, exactly as received.
        Field::new("price_ranges_raw", DataType::Utf8, false),
        // Content hash, so a change is detectable without re-parsing.
        Field::new("price_ranges_hash", DataType::Utf8, false),
        Field::new("band_count", DataType::Int32, true),
    ])
}

/// Subscription control frames. Stored so the per-sid sequence stream is
/// complete and gap detection is unambiguous.
fn control_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("message_type", DataType::Utf8, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("error_code", DataType::Int32, true),
        Field::new("error_message", DataType::Utf8, true),
    ])
}

/// Messages that failed to deserialize.
///
/// A message we could not understand is still a message we must record. The
/// raw text plus the parse error is enough to recover the content later once
/// the shape is understood.
fn unparsed_schema() -> Arc<Schema> {
    schema_with(vec![
        Field::new("parse_error", DataType::Utf8, true),
        Field::new("message_type", DataType::Utf8, true),
    ])
}
