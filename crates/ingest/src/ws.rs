//! WebSocket transport: authenticated connect, subscribe, receive, recover.
//!
//! Verified against `https://docs.kalshi.com/asyncapi.yaml` and
//! `quick_start_websockets`, read 2026-08-28.
//!
//! # Read-only
//!
//! This client subscribes to public market-data channels only. It never
//! subscribes to the authenticated portfolio channels, and it sends no command
//! capable of affecting an order.

use crate::auth::Credentials;
use crate::wire::{ErrorBody, ServerMessage};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ===========================================================================
// Pricing convention
// ===========================================================================

/// Which pricing convention the orderbook channel reports the NO side in.
///
/// # This must be set explicitly, and recorded with the data
///
/// The `use_yes_price` subscribe parameter currently defaults to `false`, under
/// which NO-side snapshot and delta updates arrive in **no-leg** pricing. The
/// spec states plainly that this default "will be flipped to `true` in a future
/// release, and the flag will then be removed entirely".
///
/// If we relied on the default, the meaning of every NO-side price would invert
/// the day that ships. There would be no error, no sequence gap, and no
/// reconnect — the bytes would keep arriving and would simply mean something
/// else. It would be discovered months later as data that looks subtly wrong.
///
/// Three consequences, all enforced here:
///
/// 1. the flag is **always sent explicitly** on every orderbook subscribe;
/// 2. it has **no default** in configuration — the daemon refuses to start
///    without it, rather than picking one;
/// 3. the active value is recorded per session in the Parquet schema, so
///    analysis months later can tell which convention a given file's bytes are
///    in without guessing.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PricingConvention {
    /// `use_yes_price: false`. NO-side levels are quoted in NO-leg pricing: a
    /// NO bid at `$0.5700` is a bid to buy NO at 57 cents, equivalent to a YES
    /// ask at `$0.4300`.
    NoLegPricing,
    /// `use_yes_price: true`. NO-side levels are already converted to YES-leg
    /// pricing, so one `price_dollars` scale applies to both sides.
    YesLegPricing,
}

impl PricingConvention {
    #[must_use]
    pub const fn use_yes_price(self) -> bool {
        matches!(self, PricingConvention::YesLegPricing)
    }

    /// Stable label written into the Parquet session metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            PricingConvention::NoLegPricing => "no_leg",
            PricingConvention::YesLegPricing => "yes_leg",
        }
    }

    /// Parse from config. There is deliberately no `Default` impl.
    pub fn parse(s: &str) -> Result<PricingConvention, WsError> {
        match s {
            "no_leg" => Ok(PricingConvention::NoLegPricing),
            "yes_leg" => Ok(PricingConvention::YesLegPricing),
            other => Err(WsError::UnknownPricingConvention {
                value: other.to_owned(),
            }),
        }
    }
}

// ===========================================================================
// Errors
// ===========================================================================

#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("signing the WebSocket upgrade")]
    Auth(#[source] crate::auth::AuthError),
    #[error("building the upgrade request for {url}")]
    Request {
        url: String,
        #[source]
        source: tokio_tungstenite::tungstenite::Error,
    },
    #[error("connecting to {url}")]
    Connect {
        url: String,
        #[source]
        source: tokio_tungstenite::tungstenite::Error,
    },
    #[error("socket closed: {reason}")]
    Closed { reason: String },
    #[error("transport error")]
    Transport(#[source] tokio_tungstenite::tungstenite::Error),
    #[error("serializing a command")]
    Encode(#[source] serde_json::Error),
    #[error(
        "pricing convention {value:?} is not recognized; set \
         websocket.pricing_convention to \"no_leg\" or \"yes_leg\" explicitly. \
         There is no default: the exchange's own default is scheduled to flip, \
         which would silently invert the meaning of every NO-side price."
    )]
    UnknownPricingConvention { value: String },
}

// ===========================================================================
// Subscription bookkeeping
// ===========================================================================

/// Tracks the sequence stream of one live subscription.
///
/// # A sid is never reused, so its counter is never reset
///
/// `subscribed` returns a **fresh server-generated sid** on every subscribe,
/// and `unsubscribed` carries a final `seq` for the sid being torn down. A
/// resubscribe therefore produces a *new* sid with a *new* counter — the old
/// one is dead, not restartable.
///
/// This type makes that impossible to get wrong: there is no `reset()`, and
/// [`SubscriptionRegistry::retire`] removes the entry outright. A counter keyed
/// by a sid that no longer exists is dropped, never carried forward.
#[derive(Debug)]
pub struct SubscriptionState {
    sid: u64,
    channel: String,
    /// Markets carried by this subscription. With shard size 1 this is one
    /// market, which bounds a gap's blast radius to that market.
    markets: Vec<String>,
    last_seq: Option<u64>,
    /// False until a snapshot seeds the book, and after any detected gap.
    valid: bool,
    gaps: u64,
    invalid_since: Option<DateTime<Utc>>,
    invalid_total: chrono::Duration,
}

/// What happened when a sequenced message was applied.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SeqOutcome {
    /// Contiguous with the previous message.
    InOrder,
    /// First sequenced message on this subscription.
    First,
    /// One or more messages were skipped. The subscription is now invalid and
    /// must be re-seeded from a fresh snapshot. **Never interpolate.**
    Gap {
        expected: u64,
        got: u64,
        skipped: u64,
    },
    /// A sequence number at or below the last one seen. Should not happen;
    /// treated as suspect and reported rather than silently ignored.
    Regression { last: u64, got: u64 },
}

impl SubscriptionState {
    #[must_use]
    pub fn sid(&self) -> u64 {
        self.sid
    }

    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    #[must_use]
    pub fn markets(&self) -> &[String] {
        &self.markets
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    #[must_use]
    pub fn gap_count(&self) -> u64 {
        self.gaps
    }

    /// Total time this subscription has spent invalid, for the metrics line.
    #[must_use]
    pub fn invalid_duration(&self, now: DateTime<Utc>) -> chrono::Duration {
        match self.invalid_since {
            Some(since) => self.invalid_total + (now - since),
            None => self.invalid_total,
        }
    }

    /// Apply a sequence number, returning what it implied.
    pub fn observe_seq(&mut self, seq: u64, now: DateTime<Utc>) -> SeqOutcome {
        let outcome = match self.last_seq {
            None => SeqOutcome::First,
            Some(last) if seq == last + 1 => SeqOutcome::InOrder,
            Some(last) if seq <= last => SeqOutcome::Regression { last, got: seq },
            Some(last) => SeqOutcome::Gap {
                expected: last + 1,
                got: seq,
                skipped: seq - last - 1,
            },
        };
        match outcome {
            SeqOutcome::Gap { .. } => {
                self.gaps += 1;
                self.invalidate(now);
                self.last_seq = Some(seq);
            }
            // A regression must NOT rewind the high-water mark. Accepting a
            // lower sequence as the new baseline would make the *next* message
            // look contiguous and quietly paper over the anomaly -- exactly
            // the interpolation this design refuses to do. Keep the highest
            // sequence seen and let the caller decide.
            SeqOutcome::Regression { .. } => {}
            SeqOutcome::First | SeqOutcome::InOrder => {
                self.last_seq = Some(seq);
            }
        }
        outcome
    }

    /// Seed from a fresh snapshot: the book is authoritative again.
    pub fn accept_snapshot(&mut self, seq: u64, now: DateTime<Utc>) {
        self.last_seq = Some(seq);
        if !self.valid {
            if let Some(since) = self.invalid_since.take() {
                self.invalid_total += now - since;
            }
            self.valid = true;
        }
    }

    fn invalidate(&mut self, now: DateTime<Utc>) {
        if self.valid {
            self.valid = false;
            self.invalid_since = Some(now);
        }
    }
}

/// How a subscription was brought back to a valid state.
///
/// Ordered cheapest to most disruptive. Counted separately in the metrics line
/// so the soak can show which rung actually carries the load.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum RecoveryMethod {
    /// `update_subscription` with `action: get_snapshot`. Returns a fresh
    /// `orderbook_snapshot` **without modifying the subscription** — no sid
    /// churn, no teardown, nothing else disturbed. The default first attempt.
    GetSnapshot,
    /// Unsubscribe and resubscribe. Yields a new sid; the old counter is
    /// discarded. Other subscriptions on the socket are unaffected.
    Resubscribe,
    /// Tear down the whole connection. Every subscription is re-established
    /// with new sids. The last resort.
    Reconnect,
}

impl RecoveryMethod {
    /// The ladder, cheapest first.
    pub const LADDER: [RecoveryMethod; 3] = [
        RecoveryMethod::GetSnapshot,
        RecoveryMethod::Resubscribe,
        RecoveryMethod::Reconnect,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RecoveryMethod::GetSnapshot => "get_snapshot",
            RecoveryMethod::Resubscribe => "resubscribe",
            RecoveryMethod::Reconnect => "reconnect",
        }
    }

    /// The next rung when this one fails to restore validity.
    #[must_use]
    pub const fn escalate(self) -> Option<RecoveryMethod> {
        match self {
            RecoveryMethod::GetSnapshot => Some(RecoveryMethod::Resubscribe),
            RecoveryMethod::Resubscribe => Some(RecoveryMethod::Reconnect),
            RecoveryMethod::Reconnect => None,
        }
    }
}

/// Live subscriptions, keyed by sid.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    by_sid: HashMap<u64, SubscriptionState>,
    recoveries: HashMap<RecoveryMethod, u64>,
}

impl SubscriptionRegistry {
    #[must_use]
    pub fn new() -> SubscriptionRegistry {
        SubscriptionRegistry::default()
    }

    /// Register a sid the server just handed us.
    ///
    /// A sid is always new, so this never merges into existing state.
    ///
    /// `now` is supplied by the caller rather than read from the clock here:
    /// the whole pipeline is driven by the receive timestamp taken at the
    /// socket, and a second, slightly later `Utc::now()` inside this function
    /// would make invalidity windows disagree with the data by a fraction of a
    /// millisecond -- enough to make accumulated durations come out wrong.
    pub fn register(
        &mut self,
        sid: u64,
        channel: String,
        markets: Vec<String>,
        now: DateTime<Utc>,
    ) {
        self.by_sid.insert(
            sid,
            SubscriptionState {
                sid,
                channel,
                markets,
                last_seq: None,
                // Not valid until a snapshot arrives.
                valid: false,
                gaps: 0,
                invalid_since: Some(now),
                invalid_total: chrono::Duration::zero(),
            },
        );
    }

    /// Drop a sid for good.
    ///
    /// Called on `unsubscribed`, on a terminal error, and on reconnect. The
    /// state — including its sequence counter — is discarded rather than
    /// reset, because the server will never issue that sid again.
    pub fn retire(&mut self, sid: u64) -> Option<SubscriptionState> {
        self.by_sid.remove(&sid)
    }

    /// Drop every sid. Used when the connection itself goes away.
    pub fn retire_all(&mut self) {
        self.by_sid.clear();
    }

    #[must_use]
    pub fn get(&self, sid: u64) -> Option<&SubscriptionState> {
        self.by_sid.get(&sid)
    }

    pub fn get_mut(&mut self, sid: u64) -> Option<&mut SubscriptionState> {
        self.by_sid.get_mut(&sid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_sid.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_sid.is_empty()
    }

    #[must_use]
    pub fn sids(&self) -> Vec<u64> {
        self.by_sid.keys().copied().collect()
    }

    /// Subscriptions currently unusable, for the metrics line.
    #[must_use]
    pub fn invalid_sids(&self) -> Vec<u64> {
        self.by_sid
            .iter()
            .filter(|(_, state)| !state.valid)
            .map(|(sid, _)| *sid)
            .collect()
    }

    #[must_use]
    pub fn total_gaps(&self) -> u64 {
        self.by_sid.values().map(SubscriptionState::gap_count).sum()
    }

    pub fn record_recovery(&mut self, method: RecoveryMethod) {
        *self.recoveries.entry(method).or_insert(0) += 1;
    }

    #[must_use]
    pub fn recovery_counts(&self) -> Vec<(RecoveryMethod, u64)> {
        let mut out: Vec<_> = self
            .recoveries
            .iter()
            .map(|(method, count)| (*method, *count))
            .collect();
        out.sort_by_key(|(method, _)| method.as_str());
        out
    }
}

// ===========================================================================
// Commands
// ===========================================================================

/// Client-to-server commands. Market data only — no order surface exists here.
#[derive(Debug)]
pub enum Command {
    Subscribe {
        id: u64,
        channels: Vec<String>,
        market_tickers: Vec<String>,
        /// Sent explicitly on orderbook subscriptions. See
        /// [`PricingConvention`].
        use_yes_price: Option<bool>,
    },
    Unsubscribe {
        id: u64,
        sids: Vec<u64>,
    },
    /// Request a fresh snapshot **without modifying the subscription**.
    ///
    /// The first rung of the recovery ladder: no sid churn, no teardown, and
    /// no other market disturbed.
    GetSnapshot {
        id: u64,
        sid: u64,
        market_tickers: Vec<String>,
    },
}

impl Command {
    pub fn to_json(&self) -> Result<String, WsError> {
        let value = match self {
            Command::Subscribe {
                id,
                channels,
                market_tickers,
                use_yes_price,
            } => {
                let mut params = serde_json::Map::new();
                params.insert("channels".into(), serde_json::json!(channels));
                if !market_tickers.is_empty() {
                    params.insert("market_tickers".into(), serde_json::json!(market_tickers));
                }
                if let Some(flag) = use_yes_price {
                    // Always explicit when the channel supports it. Never
                    // omitted in the hope the default stays put.
                    params.insert("use_yes_price".into(), serde_json::json!(flag));
                }
                serde_json::json!({ "id": id, "cmd": "subscribe", "params": params })
            }
            Command::Unsubscribe { id, sids } => serde_json::json!({
                "id": id, "cmd": "unsubscribe", "params": { "sids": sids }
            }),
            Command::GetSnapshot {
                id,
                sid,
                market_tickers,
            } => serde_json::json!({
                "id": id,
                "cmd": "update_subscription",
                "params": {
                    "sids": [sid],
                    "action": "get_snapshot",
                    "market_tickers": market_tickers
                }
            }),
        };
        serde_json::to_string(&value).map_err(WsError::Encode)
    }
}

// ===========================================================================
// Reconnect backoff
// ===========================================================================

/// Exponential backoff with jitter.
///
/// Jitter matters even for one client: without it, a reconnect storm after an
/// exchange-side blip has every client retrying in lockstep.
#[derive(Clone, Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    jitter: f64,
    current: Duration,
}

impl Backoff {
    #[must_use]
    pub fn new(initial: Duration, max: Duration, jitter: f64) -> Backoff {
        Backoff {
            initial,
            max,
            jitter: jitter.clamp(0.0, 1.0),
            current: initial,
        }
    }

    /// Next delay, with jitter applied. Advances the schedule.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.max);
        if self.jitter <= 0.0 {
            return base;
        }
        let millis = base.as_millis().min(u128::from(u64::MAX));
        let millis = u64::try_from(millis).unwrap_or(u64::MAX);
        let spread = (millis as f64 * self.jitter) as u64;
        if spread == 0 {
            return base;
        }
        let offset = rand::random_range(0..=spread.saturating_mul(2));
        Duration::from_millis(millis.saturating_sub(spread).saturating_add(offset))
    }

    /// Reset after a successful connection.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    #[must_use]
    pub fn peek(&self) -> Duration {
        self.current
    }
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
pub struct WsConfig {
    pub url: String,
    /// No default. See [`PricingConvention`].
    pub pricing_convention: PricingConvention,
    pub channels: Vec<String>,
    /// Markets per orderbook subscription. 1 bounds a gap to a single market.
    pub orderbook_shard_size: usize,
    /// Pace between subscribe commands. A per-subscription command rate limit
    /// exists (error 27) but is not published numerically, so outbound commands
    /// are paced rather than fired as fast as the socket accepts them.
    pub subscribe_pace: Duration,
    /// No server traffic for this long means the connection is dead. The server
    /// pings every ~10s, so this should be comfortably above that.
    pub read_idle_timeout: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub reconnect_jitter: f64,
}

// ===========================================================================
// Connection
// ===========================================================================

/// One message as received, with the receive clock stamped as early as
/// possible and the raw text preserved.
#[derive(Clone, Debug)]
pub struct ReceivedMessage {
    /// Local receive time in nanoseconds, taken immediately on read.
    pub received_at: DateTime<Utc>,
    /// The exact bytes off the wire, before any parsing. Stored alongside the
    /// parsed form so a parser bug found later is recoverable.
    pub raw: String,
    /// `None` when the payload could not be understood; `raw` is still kept.
    pub parsed: Option<ServerMessage>,
}

/// An authenticated market-data connection.
pub struct Connection {
    socket: Socket,
    next_command_id: u64,
}

impl Connection {
    /// Open and authenticate a connection.
    ///
    /// # The upgrade is signed, even for public data
    ///
    /// Signing uses [`Credentials::sign_websocket_upgrade`], which signs `GET`
    /// over exactly `/trade-api/ws/v2` — not the URL, not a REST path, not a
    /// path with a query. Every channel here carries public data and every one
    /// of them still requires this.
    pub async fn connect(url: &str, credentials: &Credentials) -> Result<Connection, WsError> {
        let headers = credentials
            .sign_websocket_upgrade()
            .map_err(WsError::Auth)?;

        let mut request = url
            .into_client_request()
            .map_err(|source| WsError::Request {
                url: url.to_owned(),
                source,
            })?;
        {
            let map = request.headers_mut();
            for (name, value) in headers.as_pairs() {
                if let Ok(header) = value.parse() {
                    map.insert(name, header);
                }
            }
        }

        let (socket, response) =
            tokio_tungstenite::connect_async(request)
                .await
                .map_err(|source| WsError::Connect {
                    url: url.to_owned(),
                    source,
                })?;

        info!(
            url,
            status = response.status().as_u16(),
            "websocket connected and authenticated"
        );
        Ok(Connection {
            socket,
            next_command_id: 1,
        })
    }

    fn take_command_id(&mut self) -> u64 {
        let id = self.next_command_id;
        self.next_command_id += 1;
        id
    }

    pub async fn send(&mut self, command: &Command) -> Result<(), WsError> {
        let text = command.to_json()?;
        debug!(command = %text, "sending websocket command");
        self.socket
            .send(Message::Text(text.into()))
            .await
            .map_err(WsError::Transport)
    }

    /// Subscribe to `channels` for `market_tickers`.
    ///
    /// `use_yes_price` is sent whenever the orderbook channel is included.
    pub async fn subscribe(
        &mut self,
        channels: &[String],
        market_tickers: &[String],
        convention: PricingConvention,
    ) -> Result<u64, WsError> {
        let id = self.take_command_id();
        let touches_orderbook = channels.iter().any(|c| c == "orderbook_delta");
        let use_yes_price = touches_orderbook.then(|| convention.use_yes_price());
        if touches_orderbook {
            info!(
                pricing_convention = convention.as_str(),
                use_yes_price = convention.use_yes_price(),
                markets = market_tickers.len(),
                "subscribing to orderbook with an explicit pricing convention"
            );
        }
        self.send(&Command::Subscribe {
            id,
            channels: channels.to_vec(),
            market_tickers: market_tickers.to_vec(),
            use_yes_price,
        })
        .await?;
        Ok(id)
    }

    /// Request a fresh snapshot without touching the subscription.
    pub async fn request_snapshot(
        &mut self,
        sid: u64,
        market_tickers: &[String],
    ) -> Result<u64, WsError> {
        let id = self.take_command_id();
        info!(
            sid,
            markets = market_tickers.len(),
            "requesting snapshot (get_snapshot)"
        );
        self.send(&Command::GetSnapshot {
            id,
            sid,
            market_tickers: market_tickers.to_vec(),
        })
        .await?;
        Ok(id)
    }

    pub async fn unsubscribe(&mut self, sids: &[u64]) -> Result<u64, WsError> {
        let id = self.take_command_id();
        self.send(&Command::Unsubscribe {
            id,
            sids: sids.to_vec(),
        })
        .await?;
        Ok(id)
    }

    /// Read the next message.
    ///
    /// # This loop must never block
    ///
    /// tungstenite queues a Pong when it reads a Ping and flushes it at the top
    /// of the *next* read, so Pongs only go out while this loop keeps turning.
    /// Worse, error 25 (`subscription buffer overflow`) is terminal: a consumer
    /// too slow to drain has its subscription killed by the server. Storage
    /// therefore sits behind a channel — never inline here.
    ///
    /// Returns `Ok(None)` for control frames that carry no payload.
    pub async fn next_message(&mut self) -> Result<Option<ReceivedMessage>, WsError> {
        let frame = match self.socket.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(source)) => return Err(WsError::Transport(source)),
            None => {
                return Err(WsError::Closed {
                    reason: "stream ended".to_owned(),
                })
            }
        };

        // Stamp the receive clock before any parsing.
        let received_at = Utc::now();

        match frame {
            Message::Text(text) => {
                let raw = text.to_string();
                let parsed = match serde_json::from_str::<ServerMessage>(&raw) {
                    Ok(message) => Some(message),
                    Err(err) => {
                        // Never drop an unrecognized message: the raw text is
                        // still returned and still written to storage.
                        warn!(
                            error = %err,
                            raw = %truncate(&raw, 256),
                            "could not deserialize a websocket message; \
                             storing raw text only"
                        );
                        None
                    }
                };
                Ok(Some(ReceivedMessage {
                    received_at,
                    raw,
                    parsed,
                }))
            }
            Message::Ping(_) | Message::Pong(_) => Ok(None),
            Message::Close(frame) => Err(WsError::Closed {
                reason: frame
                    .map(|f| format!("{} {}", f.code, f.reason))
                    .unwrap_or_else(|| "no close frame".to_owned()),
            }),
            Message::Binary(_) | Message::Frame(_) => Ok(None),
        }
    }

    /// Close the socket politely.
    pub async fn close(&mut self) {
        if let Err(err) = self.socket.close(None).await {
            debug!(error = %err, "error closing websocket (ignored during shutdown)");
        }
    }
}

/// Decide how to react to a server error message.
#[must_use]
pub fn recovery_for_error(error: &ErrorBody) -> Option<RecoveryMethod> {
    if error.is_buffer_overflow() {
        // We were too slow to drain and the server killed the subscription.
        // get_snapshot cannot help: the subscription itself is gone.
        error!(
            code = error.code,
            "subscription buffer overflow -- the consumer fell behind and the \
             server terminated the subscription. Storage must not block the \
             read path."
        );
        return Some(RecoveryMethod::Resubscribe);
    }
    if error.is_terminal() {
        warn!(code = error.code, msg = %error.msg, "terminal channel error; resubscribing");
        return Some(RecoveryMethod::Resubscribe);
    }
    None
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}
