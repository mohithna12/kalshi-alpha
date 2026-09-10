//! Wire message → Arrow `RecordBatch`.
//!
//! # The dual-column contract lives here
//!
//! Every price and quantity produces two columns: the raw wire string exactly
//! as received, and the integer parsed from it. When parsing fails the integer
//! is **null and the raw string is still written** — a value we could not
//! understand is still a value we must record, and the raw column is what makes
//! a parser bug survivable.
//!
//! # Nothing is ever dropped
//!
//! Every row carries `raw_message`, the complete original text. A message that
//! does not match any known shape goes to [`Channel::Unparsed`] with its parse
//! error. There is no path through this module that discards a message.

use crate::schema::Channel;
use crate::sink::StoreRecord;
use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampNanosecondArray,
};
use chrono::{DateTime, Utc};
use kalshi_common::{Px, Qty};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("building a record batch for {channel}")]
    Batch {
        channel: &'static str,
        #[source]
        source: arrow::error::ArrowError,
    },
}

/// Accumulates the columns common to every channel.
struct CommonColumns {
    received_at: Vec<i64>,
    exchange_ts: Int64Builder,
    session_id: StringBuilder,
    sid: Int64Builder,
    seq: Int64Builder,
    raw_message: StringBuilder,
}

impl CommonColumns {
    fn new() -> CommonColumns {
        CommonColumns {
            received_at: Vec::new(),
            exchange_ts: Int64Builder::new(),
            session_id: StringBuilder::new(),
            sid: Int64Builder::new(),
            seq: Int64Builder::new(),
            raw_message: StringBuilder::new(),
        }
    }

    fn push(
        &mut self,
        received_at: DateTime<Utc>,
        exchange_ts_ms: Option<i64>,
        session_id: &str,
        sid: Option<u64>,
        seq: Option<u64>,
        raw_message: &str,
    ) {
        // Nanoseconds since the Unix epoch, UTC. Never local time: capture runs
        // across the 1 November DST transition, where local time repeats an
        // hour and would make ordering ambiguous.
        self.received_at
            .push(received_at.timestamp_nanos_opt().unwrap_or(0));
        self.exchange_ts.append_option(exchange_ts_ms);
        self.session_id.append_value(session_id);
        self.sid
            .append_option(sid.and_then(|v| i64::try_from(v).ok()));
        self.seq
            .append_option(seq.and_then(|v| i64::try_from(v).ok()));
        self.raw_message.append_value(raw_message);
    }

    fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(TimestampNanosecondArray::from(self.received_at).with_timezone("UTC")),
            Arc::new(self.exchange_ts.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.sid.finish()),
            Arc::new(self.seq.finish()),
            Arc::new(self.raw_message.finish()),
        ]
    }
}

/// A raw string column paired with the integer parsed from it.
///
/// The two are appended together so they cannot get out of step: there is no
/// way to write a parsed value without its raw source.
struct DualBuilder {
    raw: StringBuilder,
    parsed: Int64Builder,
}

impl DualBuilder {
    fn new() -> DualBuilder {
        DualBuilder {
            raw: StringBuilder::new(),
            parsed: Int64Builder::new(),
        }
    }

    /// Append a price. A value that fails to parse still writes its raw form
    /// with a null integer beside it.
    fn push_price(&mut self, value: Option<&str>) {
        match value {
            Some(text) => {
                self.raw.append_value(text);
                self.parsed
                    .append_option(Px::parse_dollars(text).ok().map(Px::micros));
            }
            None => {
                self.raw.append_null();
                self.parsed.append_null();
            }
        }
    }

    /// Append a quantity, at the documented `_fp` scale of 2 decimals.
    fn push_qty(&mut self, value: Option<&str>) {
        match value {
            Some(text) => {
                self.raw.append_value(text);
                self.parsed
                    .append_option(Qty::parse_fp(text).ok().map(Qty::fp_units));
            }
            None => {
                self.raw.append_null();
                self.parsed.append_null();
            }
        }
    }

    fn finish(mut self) -> (ArrayRef, ArrayRef) {
        (Arc::new(self.raw.finish()), Arc::new(self.parsed.finish()))
    }
}

/// Read a string field from a JSON object, treating explicit null as absent.
fn field<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(serde_json::Value::as_str)
}

fn int_field(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(serde_json::Value::as_i64)
}

fn bool_field(value: &serde_json::Value, key: &str) -> Option<bool> {
    value.get(key).and_then(serde_json::Value::as_bool)
}

/// A number field kept as its raw wire token.
///
/// `fee_multiplier_override` is typed `number` by the spec but feeds a fee
/// model. Routing it through `f64` on the way in is the same silent-precision
/// risk the price types exist to avoid, so the token is preserved verbatim.
fn number_token(value: &serde_json::Value, key: &str) -> Option<String> {
    match value.get(key) {
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Encode a batch of records for one channel.
///
/// All records must share `channel`; the caller groups them.
pub fn encode(
    channel: Channel,
    session_id: &str,
    records: &[StoreRecord],
) -> Result<Option<RecordBatch>, EncodeError> {
    if records.is_empty() {
        return Ok(None);
    }
    let batch = match channel {
        Channel::OrderbookSnapshot => encode_orderbook_snapshot(session_id, records),
        Channel::OrderbookDelta => encode_orderbook_delta(session_id, records),
        Channel::Ticker => encode_ticker(session_id, records),
        Channel::Trade => encode_trade(session_id, records),
        Channel::MarketLifecycle => encode_lifecycle(session_id, records),
        Channel::EventFeeUpdate => encode_fee_update(session_id, records),
        Channel::PriceRanges => encode_price_ranges(session_id, records),
        Channel::Unparsed => encode_unparsed(session_id, records),
        Channel::Control => encode_control(session_id, records),
    }?;
    Ok(Some(batch))
}

/// The parsed message body, or an empty object if the record did not parse.
fn body(record: &StoreRecord) -> serde_json::Value {
    record
        .parsed
        .as_ref()
        .and_then(|v| v.get("msg").cloned())
        .unwrap_or(serde_json::Value::Null)
}

fn envelope_sid(record: &StoreRecord) -> Option<u64> {
    record
        .parsed
        .as_ref()
        .and_then(|v| v.get("sid"))
        .and_then(serde_json::Value::as_u64)
}

fn envelope_seq(record: &StoreRecord) -> Option<u64> {
    record
        .parsed
        .as_ref()
        .and_then(|v| v.get("seq"))
        .and_then(serde_json::Value::as_u64)
}

fn finish(
    channel: Channel,
    common: CommonColumns,
    extra: Vec<ArrayRef>,
) -> Result<RecordBatch, EncodeError> {
    let mut columns = common.finish();
    columns.extend(extra);
    RecordBatch::try_new(channel.schema(), columns).map_err(|source| EncodeError::Batch {
        channel: channel.as_str(),
        source,
    })
}

// ---------------------------------------------------------------------------
// Orderbook snapshot: one row per price level
// ---------------------------------------------------------------------------

fn encode_orderbook_snapshot(
    session_id: &str,
    records: &[StoreRecord],
) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut market, mut market_id, mut side) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut price = DualBuilder::new();
    let mut size = DualBuilder::new();
    let mut level_index = Int32Builder::new();

    for record in records {
        let msg = body(record);
        let ticker = field(&msg, "market_ticker").unwrap_or("");
        let id = field(&msg, "market_id");
        let sid = envelope_sid(record);
        let seq = envelope_seq(record);

        // Both arrays hold BIDS. A no_dollars_fp entry is a bid to buy NO,
        // which is a YES ask at the complementary price. The side is recorded
        // as sent; the reciprocal conversion belongs to the book, not to
        // storage, so the file preserves exactly what arrived.
        for (side_name, key) in [("yes", "yes_dollars_fp"), ("no", "no_dollars_fp")] {
            let Some(levels) = msg.get(key).and_then(serde_json::Value::as_array) else {
                continue;
            };
            for (index, level) in levels.iter().enumerate() {
                let Some(pair) = level.as_array() else {
                    continue;
                };
                common.push(
                    record.received_at,
                    int_field(&msg, "ts_ms"),
                    session_id,
                    sid,
                    seq,
                    &record.raw,
                );
                market.append_value(ticker);
                market_id.append_option(id);
                side.append_value(side_name);
                price.push_price(pair.first().and_then(serde_json::Value::as_str));
                size.push_qty(pair.get(1).and_then(serde_json::Value::as_str));
                level_index.append_value(i32::try_from(index).unwrap_or(-1));
            }
        }
    }

    let (price_raw, price_parsed) = price.finish();
    let (size_raw, size_parsed) = size.finish();
    finish(
        Channel::OrderbookSnapshot,
        common,
        vec![
            Arc::new(market.finish()),
            Arc::new(market_id.finish()),
            Arc::new(side.finish()),
            price_raw,
            price_parsed,
            size_raw,
            size_parsed,
            Arc::new(level_index.finish()),
        ],
    )
}

// ---------------------------------------------------------------------------
// Orderbook delta
// ---------------------------------------------------------------------------

fn encode_orderbook_delta(
    session_id: &str,
    records: &[StoreRecord],
) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut market, mut market_id, mut side) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut price = DualBuilder::new();
    let mut delta = DualBuilder::new();

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            int_field(&msg, "ts_ms"),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        market.append_value(field(&msg, "market_ticker").unwrap_or(""));
        market_id.append_option(field(&msg, "market_id"));
        side.append_value(field(&msg, "side").unwrap_or(""));
        price.push_price(field(&msg, "price_dollars"));
        // Signed: negative removes size from the level.
        delta.push_qty(field(&msg, "delta_fp"));
    }

    let (price_raw, price_parsed) = price.finish();
    let (delta_raw, delta_parsed) = delta.finish();
    finish(
        Channel::OrderbookDelta,
        common,
        vec![
            Arc::new(market.finish()),
            Arc::new(market_id.finish()),
            Arc::new(side.finish()),
            price_raw,
            price_parsed,
            delta_raw,
            delta_parsed,
        ],
    )
}

// ---------------------------------------------------------------------------
// Ticker
// ---------------------------------------------------------------------------

fn encode_ticker(session_id: &str, records: &[StoreRecord]) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut market, mut market_id) = (StringBuilder::new(), StringBuilder::new());
    let mut price = DualBuilder::new();
    let mut yes_bid = DualBuilder::new();
    let mut yes_ask = DualBuilder::new();
    let mut yes_bid_size = DualBuilder::new();
    let mut yes_ask_size = DualBuilder::new();
    let mut volume = DualBuilder::new();
    let mut open_interest = DualBuilder::new();

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            // ts_ms is canonical; the deprecated `ts` (integer seconds) is a
            // fallback only, and is converted rather than stored as seconds.
            int_field(&msg, "ts_ms").or_else(|| int_field(&msg, "ts").map(|s| s * 1_000)),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        market.append_value(field(&msg, "market_ticker").unwrap_or(""));
        market_id.append_option(field(&msg, "market_id"));
        price.push_price(field(&msg, "price_dollars"));
        yes_bid.push_price(field(&msg, "yes_bid_dollars"));
        yes_ask.push_price(field(&msg, "yes_ask_dollars"));
        yes_bid_size.push_qty(field(&msg, "yes_bid_size_fp"));
        yes_ask_size.push_qty(field(&msg, "yes_ask_size_fp"));
        volume.push_qty(field(&msg, "volume_fp"));
        open_interest.push_qty(field(&msg, "open_interest_fp"));
    }

    let mut extra: Vec<ArrayRef> = vec![Arc::new(market.finish()), Arc::new(market_id.finish())];
    for dual in [
        price,
        yes_bid,
        yes_ask,
        yes_bid_size,
        yes_ask_size,
        volume,
        open_interest,
    ] {
        let (raw, parsed) = dual.finish();
        extra.push(raw);
        extra.push(parsed);
    }
    finish(Channel::Ticker, common, extra)
}

// ---------------------------------------------------------------------------
// Trade
// ---------------------------------------------------------------------------

fn encode_trade(session_id: &str, records: &[StoreRecord]) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut trade_id, mut market) = (StringBuilder::new(), StringBuilder::new());
    let mut yes_price = DualBuilder::new();
    let mut no_price = DualBuilder::new();
    let mut count = DualBuilder::new();
    let (mut outcome_side, mut book_side, mut taker_side_deprecated) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut is_block = BooleanBuilder::new();

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            int_field(&msg, "ts_ms").or_else(|| int_field(&msg, "ts").map(|s| s * 1_000)),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        trade_id.append_option(field(&msg, "trade_id"));
        market.append_value(field(&msg, "market_ticker").unwrap_or(""));
        yes_price.push_price(field(&msg, "yes_price_dollars"));
        no_price.push_price(field(&msg, "no_price_dollars"));
        count.push_qty(field(&msg, "count_fp"));

        // Canonical direction fields first. `taker_side` is deprecated with its
        // removal protection already lapsed (2026-05-14), so it is stored when
        // present but never relied upon -- and its absence must not lose the
        // direction, which is why all three columns are written.
        outcome_side.append_option(field(&msg, "taker_outcome_side"));
        book_side.append_option(field(&msg, "taker_book_side"));
        taker_side_deprecated.append_option(field(&msg, "taker_side"));
        is_block.append_option(bool_field(&msg, "is_block_trade"));
    }

    let (yes_raw, yes_parsed) = yes_price.finish();
    let (no_raw, no_parsed) = no_price.finish();
    let (count_raw, count_parsed) = count.finish();
    finish(
        Channel::Trade,
        common,
        vec![
            Arc::new(trade_id.finish()),
            Arc::new(market.finish()),
            yes_raw,
            yes_parsed,
            no_raw,
            no_parsed,
            count_raw,
            count_parsed,
            Arc::new(outcome_side.finish()),
            Arc::new(book_side.finish()),
            Arc::new(taker_side_deprecated.finish()),
            Arc::new(is_block.finish()),
        ],
    )
}

// ---------------------------------------------------------------------------
// Market lifecycle
// ---------------------------------------------------------------------------

fn encode_lifecycle(session_id: &str, records: &[StoreRecord]) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut event_type, mut market, mut result) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut settlement = DualBuilder::new();
    let (mut open_ts, mut close_ts, mut determination_ts, mut settled_ts) = (
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
    );
    let mut is_deactivated = BooleanBuilder::new();
    let mut price_level_structure = StringBuilder::new();

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            int_field(&msg, "ts_ms"),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        event_type.append_value(field(&msg, "event_type").unwrap_or(""));
        market.append_value(
            field(&msg, "market_ticker")
                .or_else(|| field(&msg, "event_ticker"))
                .unwrap_or(""),
        );
        result.append_option(field(&msg, "result"));
        // Present only on `determined`. This is the market's outcome -- ground
        // truth for any later model, arriving on the same stream as the prices,
        // and parsed with the same exact parser.
        settlement.push_price(field(&msg, "settlement_value"));
        open_ts.append_option(int_field(&msg, "open_ts"));
        close_ts.append_option(int_field(&msg, "close_ts"));
        determination_ts.append_option(int_field(&msg, "determination_ts"));
        settled_ts.append_option(int_field(&msg, "settled_ts"));
        is_deactivated.append_option(bool_field(&msg, "is_deactivated"));
        price_level_structure.append_option(field(&msg, "price_level_structure"));
    }

    let (settlement_raw, settlement_parsed) = settlement.finish();
    finish(
        Channel::MarketLifecycle,
        common,
        vec![
            Arc::new(event_type.finish()),
            Arc::new(market.finish()),
            Arc::new(result.finish()),
            settlement_raw,
            settlement_parsed,
            Arc::new(open_ts.finish()),
            Arc::new(close_ts.finish()),
            Arc::new(determination_ts.finish()),
            Arc::new(settled_ts.finish()),
            Arc::new(is_deactivated.finish()),
            Arc::new(price_level_structure.finish()),
        ],
    )
}

// ---------------------------------------------------------------------------
// Event fee update
// ---------------------------------------------------------------------------

fn encode_fee_update(
    session_id: &str,
    records: &[StoreRecord],
) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut event_ticker, mut fee_type, mut multiplier) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            int_field(&msg, "ts_ms"),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        event_ticker.append_value(field(&msg, "event_ticker").unwrap_or(""));
        // Null means the override was cleared, which is itself an event worth
        // recording -- these are never republished as history.
        fee_type.append_option(field(&msg, "fee_type_override"));
        multiplier.append_option(number_token(&msg, "fee_multiplier_override").as_deref());
    }

    finish(
        Channel::EventFeeUpdate,
        common,
        vec![
            Arc::new(event_ticker.finish()),
            Arc::new(fee_type.finish()),
            Arc::new(multiplier.finish()),
        ],
    )
}

// ---------------------------------------------------------------------------
// Price ranges
// ---------------------------------------------------------------------------

fn encode_price_ranges(
    session_id: &str,
    records: &[StoreRecord],
) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut market, mut source, mut price_level_structure) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut effective_at: Vec<Option<i64>> = Vec::new();
    let (mut ranges_raw, mut ranges_hash) = (StringBuilder::new(), StringBuilder::new());
    let mut band_count = Int32Builder::new();

    for record in records {
        let msg = body(record);
        common.push(
            record.received_at,
            int_field(&msg, "ts_ms"),
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        market.append_value(field(&msg, "market_ticker").unwrap_or(""));
        source.append_value(field(&msg, "source").unwrap_or("discovery"));
        // Populated only when the wire supplied a change time. A discovery read
        // tells us what the grid IS, never when it became so, so this stays
        // null rather than being backfilled from the observation time.
        effective_at.push(int_field(&msg, "effective_at_ns"));
        price_level_structure.append_option(field(&msg, "price_level_structure"));
        let raw = msg
            .get("price_ranges")
            .map(std::string::ToString::to_string)
            .unwrap_or_default();
        ranges_hash.append_value(crate::grid_history::grid_hash(&raw));
        let bands = msg
            .get("price_ranges")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len);
        ranges_raw.append_value(&raw);
        band_count.append_option(bands.and_then(|n| i32::try_from(n).ok()));
    }

    finish(
        Channel::PriceRanges,
        common,
        vec![
            Arc::new(market.finish()),
            Arc::new(source.finish()),
            Arc::new(TimestampNanosecondArray::from(effective_at).with_timezone("UTC")),
            Arc::new(price_level_structure.finish()),
            Arc::new(ranges_raw.finish()),
            Arc::new(ranges_hash.finish()),
            Arc::new(band_count.finish()),
        ],
    )
}

// ---------------------------------------------------------------------------
// Unparsed
// ---------------------------------------------------------------------------

fn encode_unparsed(session_id: &str, records: &[StoreRecord]) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut parse_error, mut message_type) = (StringBuilder::new(), StringBuilder::new());

    for record in records {
        // sid and seq are carried even here: an unparsable message still
        // occupies a position in its subscription's sequence stream, and
        // omitting it would look like loss.
        common.push(
            record.received_at,
            None,
            session_id,
            envelope_sid(record),
            envelope_seq(record),
            &record.raw,
        );
        parse_error.append_option(record.parsed.as_ref().and_then(|v| field(v, "parse_error")));
        // Best effort: even an unrecognized message usually carries a `type`.
        message_type.append_option(
            serde_json::from_str::<serde_json::Value>(&record.raw)
                .ok()
                .as_ref()
                .and_then(|v| field(v, "type"))
                .map(std::borrow::ToOwned::to_owned)
                .as_deref(),
        );
    }

    finish(
        Channel::Unparsed,
        common,
        vec![
            Arc::new(parse_error.finish()),
            Arc::new(message_type.finish()),
        ],
    )
}

/// Number of rows a set of records will produce.
///
/// Usually one per record, but a snapshot fans out to one row per price level.
/// The writer uses this to reconcile rows dequeued against rows written.
#[must_use]
pub fn expected_rows(channel: Channel, records: &[StoreRecord]) -> usize {
    match channel {
        Channel::OrderbookSnapshot => records
            .iter()
            .map(|record| {
                let msg = body(record);
                ["yes_dollars_fp", "no_dollars_fp"]
                    .iter()
                    .filter_map(|key| msg.get(*key).and_then(serde_json::Value::as_array))
                    .map(Vec::len)
                    .sum::<usize>()
            })
            .sum(),
        _ => records.len(),
    }
}

// ---------------------------------------------------------------------------
// Control frames
// ---------------------------------------------------------------------------

fn encode_control(session_id: &str, records: &[StoreRecord]) -> Result<RecordBatch, EncodeError> {
    let mut common = CommonColumns::new();
    let (mut message_type, mut channel_name) = (StringBuilder::new(), StringBuilder::new());
    let mut error_code = Int32Builder::new();
    let mut error_message = StringBuilder::new();

    for record in records {
        let envelope = record.parsed.clone().unwrap_or(serde_json::Value::Null);
        let msg = body(record);
        // `unsubscribed` carries seq at the envelope level; `subscribed`
        // carries sid inside msg. Take whichever is present.
        let sid =
            envelope_sid(record).or_else(|| msg.get("sid").and_then(serde_json::Value::as_u64));
        common.push(
            record.received_at,
            None,
            session_id,
            sid,
            envelope_seq(record),
            &record.raw,
        );
        message_type.append_option(field(&envelope, "type"));
        channel_name.append_option(field(&msg, "channel"));
        error_code.append_option(
            msg.get("code")
                .and_then(serde_json::Value::as_i64)
                .and_then(|v| i32::try_from(v).ok()),
        );
        error_message.append_option(field(&msg, "msg"));
    }

    finish(
        Channel::Control,
        common,
        vec![
            Arc::new(message_type.finish()),
            Arc::new(channel_name.finish()),
            Arc::new(error_code.finish()),
            Arc::new(error_message.finish()),
        ],
    )
}
