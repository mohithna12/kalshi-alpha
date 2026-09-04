//! Wire message types for the Kalshi market-data WebSocket.
//!
//! Verified against `https://docs.kalshi.com/asyncapi.yaml`, read 2026-08-28.
//!
//! # Deprecated fields are optional, never required
//!
//! Several fields carry a "will not be removed before May 14, 2026" guarantee —
//! a date that has now passed, so they may vanish at any time. Every one of them
//! is `Option` here and none is used as the canonical source:
//!
//! | deprecated | canonical replacement |
//! |---|---|
//! | `taker_side` | `taker_outcome_side` / `taker_book_side` |
//! | `ts`, `time` | `ts_ms` |
//!
//! Deserialization must not fail when a deprecated field is absent. They are
//! still captured when present, because a field we did not record is a field we
//! cannot recover.
//!
//! Note the `ts` types differ per channel: on `trade` it is an integer of
//! seconds, on `orderbook_delta` it is an RFC3339 **string**. Both are
//! deprecated; we read `ts_ms` and keep the legacy value verbatim as text.

use kalshi_common::{Px, Qty, Side};
use serde::Deserialize;

/// Every message the market-data socket can deliver.
///
/// `#[serde(untagged)]` is deliberately avoided: it discards the underlying
/// error and would silently reclassify a message whose shape we got slightly
/// wrong. Tagging on `type` means an unknown or malformed message is reported
/// as such.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "subscribed")]
    Subscribed(SubscribedPayload),
    #[serde(rename = "unsubscribed")]
    Unsubscribed(UnsubscribedPayload),
    #[serde(rename = "ok")]
    Ok(OkPayload),
    #[serde(rename = "error")]
    Error(ErrorPayload),
    #[serde(rename = "orderbook_snapshot")]
    OrderbookSnapshot(OrderbookSnapshotPayload),
    #[serde(rename = "orderbook_delta")]
    OrderbookDelta(OrderbookDeltaPayload),
    #[serde(rename = "ticker")]
    Ticker(TickerPayload),
    #[serde(rename = "trade")]
    Trade(TradePayload),
    #[serde(rename = "market_lifecycle_v2")]
    MarketLifecycle(MarketLifecyclePayload),
    #[serde(rename = "event_lifecycle")]
    EventLifecycle(EventLifecyclePayload),
    #[serde(rename = "event_fee_update")]
    EventFeeUpdate(EventFeeUpdatePayload),
}

impl ServerMessage {
    /// The subscription this message belongs to, when it has one.
    #[must_use]
    pub fn sid(&self) -> Option<u64> {
        match self {
            ServerMessage::Subscribed(m) => Some(m.msg.sid),
            ServerMessage::Unsubscribed(m) => Some(m.sid),
            ServerMessage::Ok(m) => m.sid,
            ServerMessage::Error(_) => None,
            ServerMessage::OrderbookSnapshot(m) => Some(m.sid),
            ServerMessage::OrderbookDelta(m) => Some(m.sid),
            ServerMessage::Ticker(m) => Some(m.sid),
            ServerMessage::Trade(m) => Some(m.sid),
            ServerMessage::MarketLifecycle(m) => Some(m.sid),
            ServerMessage::EventLifecycle(m) => Some(m.sid),
            ServerMessage::EventFeeUpdate(m) => Some(m.sid),
        }
    }

    /// The sequence number, **which only the orderbook channel carries**.
    ///
    /// # Gap detection is impossible on every other channel
    ///
    /// `seq` is required on `orderbook_snapshot` and `orderbook_delta` and is
    /// not present — not even optionally — on `ticker`, `trade`,
    /// `market_lifecycle_v2`, `event_lifecycle`, or `event_fee_update`.
    ///
    /// So a dropped lifecycle message leaves no trace at all: there is no
    /// counter to gap. That is why REST reconciliation is the *only* mechanism
    /// for detecting a missed market creation, rather than a backstop for one.
    #[must_use]
    pub fn seq(&self) -> Option<u64> {
        match self {
            ServerMessage::OrderbookSnapshot(m) => Some(m.seq),
            ServerMessage::OrderbookDelta(m) => Some(m.seq),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Control
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct SubscribedPayload {
    pub id: Option<u64>,
    pub msg: SubscribedBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SubscribedBody {
    pub channel: String,
    /// Server-generated, and **fresh on every subscribe**. A resubscribe to the
    /// same market yields a different sid with a new sequence counter.
    pub sid: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UnsubscribedPayload {
    pub id: Option<u64>,
    pub sid: u64,
    /// The final sequence number on this subscription.
    pub seq: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OkPayload {
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorPayload {
    pub id: Option<u64>,
    pub msg: ErrorBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorBody {
    pub code: u32,
    pub msg: String,
    pub market_ticker: Option<String>,
}

impl ErrorBody {
    /// Errors that kill the subscription outright, requiring a resubscribe.
    ///
    /// From the spec's "Terminal Errors" table: 10 (channel error), 17
    /// (internal error), 25 (subscription buffer overflow). Note that 25 means
    /// **we were too slow to drain** — the server killed us for it. That is why
    /// the read path must never block on storage.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self.code, 10 | 17 | 25)
    }

    #[must_use]
    pub fn is_buffer_overflow(&self) -> bool {
        self.code == 25
    }
}

// ---------------------------------------------------------------------------
// Orderbook
// ---------------------------------------------------------------------------

/// One `[price, size]` level as it appears in a snapshot array.
#[derive(Clone, Debug, Deserialize)]
pub struct RawLevel(pub String, pub String);

impl RawLevel {
    /// Parse into typed values, keeping the raw strings for the dual-column
    /// Parquet schema.
    pub fn parse(&self) -> Result<(Px, Qty), kalshi_common::ParseFixedError> {
        Ok((Px::parse_dollars(&self.0)?, Qty::parse_fp(&self.1)?))
    }

    #[must_use]
    pub fn price_raw(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn size_raw(&self) -> &str {
        &self.1
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderbookSnapshotPayload {
    pub sid: u64,
    pub seq: u64,
    pub msg: OrderbookSnapshotBody,
}

/// A full book state.
///
/// Both arrays hold **bids**, not asks. A `no_dollars_fp` entry is a bid to buy
/// NO, which is economically an ask on YES at the complementary price — unless
/// `use_yes_price` was set on the subscription, in which case the exchange has
/// already converted it. See [`crate::ws::PricingConvention`].
#[derive(Clone, Debug, Deserialize)]
pub struct OrderbookSnapshotBody {
    pub market_ticker: String,
    pub market_id: Option<String>,
    #[serde(default)]
    pub yes_dollars_fp: Vec<RawLevel>,
    #[serde(default)]
    pub no_dollars_fp: Vec<RawLevel>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderbookDeltaPayload {
    pub sid: u64,
    pub seq: u64,
    pub msg: OrderbookDeltaBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderbookDeltaBody {
    pub market_ticker: String,
    pub market_id: Option<String>,
    pub price_dollars: String,
    /// Signed: `"-54.00"` removes 54 contracts from the level.
    pub delta_fp: String,
    pub side: Side,
    /// Deprecated, and an RFC3339 **string** on this channel (unlike `trade`,
    /// where it is integer seconds). Kept verbatim, never relied upon.
    pub ts: Option<String>,
    pub ts_ms: Option<i64>,
}

// ---------------------------------------------------------------------------
// Ticker and trade
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct TickerPayload {
    pub sid: u64,
    pub msg: TickerBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TickerBody {
    pub market_ticker: String,
    pub market_id: Option<String>,
    pub price_dollars: Option<String>,
    pub yes_bid_dollars: Option<String>,
    pub yes_ask_dollars: Option<String>,
    pub yes_bid_size_fp: Option<String>,
    pub yes_ask_size_fp: Option<String>,
    pub last_trade_size_fp: Option<String>,
    pub volume_fp: Option<String>,
    pub open_interest_fp: Option<String>,
    pub dollar_volume: Option<i64>,
    pub dollar_open_interest: Option<i64>,
    pub ts_ms: Option<i64>,
    /// Deprecated (integer seconds).
    pub ts: Option<i64>,
    /// Deprecated.
    pub time: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TradePayload {
    pub sid: u64,
    pub msg: TradeBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TradeBody {
    pub trade_id: Option<String>,
    pub market_ticker: String,
    pub yes_price_dollars: Option<String>,
    pub no_price_dollars: Option<String>,
    pub count_fp: Option<String>,
    /// **Canonical** direction field.
    pub taker_outcome_side: Option<Side>,
    /// **Canonical** direction field, in book vocabulary: `bid` == outcome
    /// `yes`, `ask` == outcome `no`.
    pub taker_book_side: Option<String>,
    /// Deprecated in favour of the two above. Optional so that its removal does
    /// not break deserialization.
    pub taker_side: Option<Side>,
    pub is_block_trade: Option<bool>,
    pub ts_ms: Option<i64>,
    /// Deprecated (integer seconds on this channel).
    pub ts: Option<i64>,
}

impl TradeBody {
    /// The taker's direction, preferring the canonical fields.
    ///
    /// Falls back to the deprecated `taker_side` only if both canonical fields
    /// are absent, so capture keeps working either side of their removal.
    #[must_use]
    pub fn direction(&self) -> Option<Side> {
        if let Some(side) = self.taker_outcome_side {
            return Some(side);
        }
        if let Some(book_side) = self.taker_book_side.as_deref() {
            return match book_side {
                "bid" => Some(Side::Yes),
                "ask" => Some(Side::No),
                _ => None,
            };
        }
        self.taker_side
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct MarketLifecyclePayload {
    pub sid: u64,
    pub msg: MarketLifecycleBody,
}

/// A market lifecycle transition.
///
/// # `determined` carries the settlement value
///
/// When `event_type == "determined"`, `settlement_value` is the market's
/// outcome as a fixed-point dollar string (e.g. `"0.5000"`). That is ground
/// truth for any later model, arriving on the same stream as the prices — so it
/// is captured here rather than reconstructed from a settlement API later.
#[derive(Clone, Debug, Deserialize)]
pub struct MarketLifecycleBody {
    pub event_type: String,
    pub market_ticker: String,
    pub exchange_index: Option<i64>,
    pub open_ts: Option<i64>,
    pub close_ts: Option<i64>,
    pub result: Option<String>,
    pub determination_ts: Option<i64>,
    /// Fixed-point dollars. Present only on `determined`.
    pub settlement_value: Option<String>,
    pub settled_ts: Option<i64>,
    pub is_deactivated: Option<bool>,
    pub price_level_structure: Option<String>,
    /// The tick grid can change mid-stream via
    /// `price_level_structure_updated`, which is why grids are stored as an
    /// append-only time series rather than a field on a market row.
    pub price_ranges: Option<serde_json::Value>,
    pub strike_type: Option<String>,
    pub floor_strike: Option<serde_json::Value>,
    pub cap_strike: Option<serde_json::Value>,
    pub custom_strike: Option<serde_json::Value>,
    pub yes_sub_title: Option<String>,
    pub additional_metadata: Option<serde_json::Value>,
}

impl MarketLifecycleBody {
    #[must_use]
    pub fn is_created(&self) -> bool {
        self.event_type == "created"
    }

    #[must_use]
    pub fn changes_price_grid(&self) -> bool {
        self.price_ranges.is_some()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventLifecyclePayload {
    pub sid: u64,
    pub msg: EventLifecycleBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventLifecycleBody {
    pub event_ticker: String,
    pub series_ticker: Option<String>,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub collateral_return_type: Option<String>,
    pub strike_date: Option<i64>,
    pub strike_period: Option<String>,
    pub exchange_index: Option<i64>,
}

/// Per-event fee overrides.
///
/// # Captured because it cannot be reconstructed
///
/// Fee type and multiplier overrides are set and cleared over time and the
/// exchange publishes no history of them. A Phase 2 fee model needs to know
/// which override was in force when a given trade printed, and that is
/// answerable only if these messages were recorded as they arrived. `null` in
/// either field means the override was cleared, which is itself an event worth
/// keeping.
#[derive(Clone, Debug, Deserialize)]
pub struct EventFeeUpdatePayload {
    pub sid: u64,
    pub msg: EventFeeUpdateBody,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventFeeUpdateBody {
    pub event_ticker: String,
    /// `None` when the override has been cleared.
    pub fee_type_override: Option<String>,
    /// `None` when the override has been cleared.
    ///
    /// Held as a raw JSON number rather than `f64`. The spec types it as
    /// `number`, but this feeds a Phase 2 fee model, and routing a multiplier
    /// through binary floating point is the same silent-corruption risk the
    /// price types exist to avoid. Storage writes the raw token; conversion is
    /// the consumer's decision, made explicitly.
    pub fee_multiplier_override: Option<serde_json::Number>,
}

impl EventFeeUpdateBody {
    /// The multiplier exactly as it appeared on the wire, for the raw column.
    #[must_use]
    pub fn multiplier_raw(&self) -> Option<String> {
        self.fee_multiplier_override
            .as_ref()
            .map(std::string::ToString::to_string)
    }
}
