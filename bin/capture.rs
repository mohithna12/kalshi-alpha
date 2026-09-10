//! `capture` — the Kalshi market-data capture daemon.
//!
//! # Read-only
//!
//! This binary has no code path capable of placing, amending, or cancelling an
//! order. The REST client exposes no request primitive that accepts a body, and
//! the WebSocket client subscribes only to public market-data channels.
//!
//! # Shape of the process
//!
//! ```text
//!   socket read loop  --offer(non-blocking)-->  queue  -->  writer task
//!         |                                                      |
//!         +-- books (gap detection, recovery ladder)              +-- Parquet
//! ```
//!
//! The read loop never blocks. Storage sits behind a bounded queue that drops
//! and counts under overload, because a stalled read loop stops Pongs (the
//! server closes us after ~10s) and earns a terminal `subscription buffer
//! overflow` error. Losing a row is bad; losing the connection and every book
//! on it is worse.

mod subcommands;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use kalshi_ingest::auth::Credentials;
use kalshi_ingest::book::AnyBook;
use kalshi_ingest::rest::{RestClient, RestConfig};
use kalshi_ingest::wire::ServerMessage;
use kalshi_ingest::ws::{Backoff, Connection, PricingConvention, SubscriptionRegistry, WsError};
use kalshi_store::schema::Channel;
use kalshi_store::session::{Environment, SessionMetadata};
use kalshi_store::sink::{Offered, StoreRecord};
use kalshi_store::writer::{ParquetStore, WriterConfig};
use kalshi_store::GridHistory;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

// ===========================================================================
// CLI
// ===========================================================================

#[derive(Parser, Debug)]
#[command(
    name = "capture",
    about = "Kalshi market-data capture. Read-only: this binary cannot place orders."
)]
struct Cli {
    /// Environment to capture from. Demo is the default; production requires
    /// the acknowledgement flag below.
    #[arg(
        long,
        value_name = "demo|prod",
        default_value = "demo",
        env = "KALSHI_ENV"
    )]
    env: String,

    /// Required for production. Production capture is read-only and safe; this
    /// flag exists to make the switch deliberate, not to discourage it.
    #[arg(long)]
    i_understand_this_is_production: bool,

    /// Allow production capture from a dirty working tree.
    ///
    /// Refused by default: the session record ties every row to a `git_sha`,
    /// and a `-dirty` SHA cannot be resolved back to reproducible code. Use
    /// only for a deliberate one-off, never for a soak.
    #[arg(long)]
    allow_dirty_tree: bool,

    #[arg(long, default_value = "config/default.toml")]
    config: PathBuf,

    /// Override the series to capture. Repeatable.
    #[arg(long = "series")]
    series: Vec<String>,

    /// Override markets per orderbook subscription.
    #[arg(long)]
    orderbook_shard_size: Option<usize>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Force a sequence-gap recovery and assert the contract holds.
    ForceGap {
        #[arg(long)]
        market: String,
        #[arg(long, default_value = "all")]
        mode: String,
        #[arg(long)]
        assert_snapshot: bool,
        #[arg(long)]
        assert_isolation: bool,
        #[arg(long, default_value_t = 15)]
        timeout_secs: u64,
    },
    /// Measure the undocumented WebSocket subscription limits.
    ProbeLimits {
        #[arg(long, default_value_t = 512)]
        max_subscriptions: usize,
        #[arg(long, default_value_t = 25)]
        pace_ms: u64,
    },
    /// Compare both pricing conventions against a live market.
    VerifyPricing {
        #[arg(long)]
        market: String,
    },
    /// Verify a captured day round-trips.
    Verify {
        #[arg(long)]
        day: String,
    },
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Debug, serde::Deserialize)]
struct AppConfig {
    endpoints: HashMap<String, Endpoint>,
    auth: AuthConfig,
    rate_limit: RateLimitConfig,
    discovery: DiscoveryConfig,
    websocket: WebsocketConfig,
    storage: StorageConfig,
    observability: ObservabilityConfig,
}

#[derive(Debug, serde::Deserialize)]
struct Endpoint {
    rest: String,
    ws: String,
}

#[derive(Debug, serde::Deserialize)]
struct AuthConfig {
    key_id: String,
    private_key_path: String,
}

#[derive(Debug, serde::Deserialize)]
struct RateLimitConfig {
    read_tokens_per_sec: u32,
    default_request_cost: u32,
}

#[derive(Debug, serde::Deserialize)]
struct DiscoveryConfig {
    series_tickers: Vec<String>,
    statuses: Vec<String>,
    reconcile_interval_secs: u64,
}

#[derive(Debug, serde::Deserialize)]
struct WebsocketConfig {
    channels: Vec<String>,
    /// No default. See `PricingConvention`: the exchange's own default is
    /// scheduled to flip, which would silently invert every NO-side price.
    pricing_convention: String,
    orderbook_shard_size: usize,
    subscribe_pace_ms: u64,
    reconnect_initial_ms: u64,
    reconnect_max_ms: u64,
    reconnect_jitter: f64,
}

#[derive(Debug, serde::Deserialize)]
struct StorageConfig {
    root: String,
    max_file_age_secs: i64,
    max_file_bytes: u64,
    max_batch_rows: usize,
    flush_interval_secs: u64,
    queue_capacity: usize,
    quarantine_on_startup: bool,
}

#[derive(Debug, serde::Deserialize)]
struct ObservabilityConfig {
    metrics_interval_secs: u64,
    heartbeat_path: String,
    heartbeat_interval_secs: u64,
    log_level: String,
}

fn load_config(path: &std::path::Path) -> Result<AppConfig> {
    config::Config::builder()
        .add_source(config::File::from(path.to_path_buf()))
        // KALSHI_AUTH__KEY_ID overrides auth.key_id, and so on.
        //
        // `prefix_separator` is set explicitly: without it the `config` crate
        // reuses `separator` for the prefix too, so it would look for
        // KALSHI__AUTH__KEY_ID (double underscore) and silently ignore the
        // single-underscore form this project documents. The failure mode is
        // nasty -- the value is simply absent, the empty default from the TOML
        // wins, and the first symptom is a 401 whose body names neither the key
        // nor the path.
        .add_source(
            config::Environment::with_prefix("KALSHI")
                .prefix_separator("_")
                .separator("__")
                .try_parsing(true),
        )
        .build()
        .with_context(|| format!("loading configuration from {}", path.display()))?
        .try_deserialize()
        .context("configuration file does not match the expected shape")
}

// ===========================================================================
// Metrics
// ===========================================================================

#[derive(Debug, Default)]
struct Metrics {
    messages: AtomicU64,
    bytes: AtomicU64,
    reconnects: AtomicU64,
    lifecycle_events: AtomicU64,
    unparsed: AtomicU64,
    off_grid_prices: AtomicU64,
    desync_invalidations: AtomicU64,
    reconciliation_misses: AtomicU64,
    snapshot_restarts: AtomicU64,
}

// ===========================================================================
// Entry point
// ===========================================================================

fn main() -> Result<()> {
    let mut cli = Cli::parse();
    let config = load_config(&cli.config)?;
    init_tracing(&config.observability.log_level);
    install_fatal_panic_hook();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;

    runtime.block_on(async move {
        // Take the subcommand out first so the rest of `cli` stays borrowable.
        let command = cli.command.take();
        match command {
            None => run_capture(cli, config).await,
            Some(Command::VerifyPricing { market }) => {
                let (url, credentials) = connect_context(&cli, &config)?;
                subcommands::verify_pricing(&url, &credentials, &market, Duration::from_secs(20))
                    .await
            }
            Some(Command::ProbeLimits {
                max_subscriptions,
                pace_ms,
            }) => {
                let (url, credentials) = connect_context(&cli, &config)?;
                let markets = discover_for_probe(&cli, &config, &credentials).await?;
                subcommands::probe_limits(
                    &url,
                    &credentials,
                    &markets,
                    max_subscriptions,
                    Duration::from_millis(pace_ms),
                )
                .await
            }
            Some(Command::ForceGap { .. }) => bail!(
                "force-gap is not implemented yet. It must attach to a running \
                 daemon to exercise the recovery ladder against live \
                 subscriptions, which needs a control channel this binary does \
                 not have. Until then, the ladder's logic is covered by \
                 crates/ingest/tests/ws.rs and the invalid-window behaviour by \
                 crates/ingest/tests/book.rs."
            ),
            Some(Command::Verify { .. }) => bail!(
                "verify is not implemented yet. The round-trip property it would \
                 check is enforced on every build by \
                 crates/store/tests/round_trip.rs and \
                 crates/store/tests/encode_round_trip.rs, which re-parse every \
                 *_raw column with an independent parser."
            ),
        }
    })
}

/// Resolve the WebSocket URL and load credentials for a subcommand.
///
/// Applies the same environment gate as capture: production still requires the
/// acknowledgement flag, because these subcommands open real connections.
fn connect_context(cli: &Cli, config: &AppConfig) -> Result<(String, Credentials)> {
    let env_name = match cli.env.as_str() {
        "demo" => "demo",
        "prod" => {
            if !cli.i_understand_this_is_production {
                bail!(
                    "refusing to connect to production without \
                     --i-understand-this-is-production"
                );
            }
            "prod"
        }
        other => bail!("unknown environment {other:?}; expected \"demo\" or \"prod\""),
    };
    let endpoint = config
        .endpoints
        .get(env_name)
        .with_context(|| format!("no endpoints configured for {env_name}"))?;
    if config.auth.key_id.trim().is_empty() {
        bail!("auth.key_id is empty; set KALSHI_AUTH__KEY_ID");
    }
    let key_path = expand_tilde(&config.auth.private_key_path);
    let credentials = Credentials::from_pem_file(&config.auth.key_id, &key_path)
        .with_context(|| format!("loading the private key from {}", key_path.display()))?;
    Ok((endpoint.ws.clone(), credentials))
}

/// Discover a handful of markets to size the limit probe against.
async fn discover_for_probe(
    cli: &Cli,
    config: &AppConfig,
    credentials: &Credentials,
) -> Result<Vec<String>> {
    let env_name = if cli.env == "prod" { "prod" } else { "demo" };
    let endpoint = config
        .endpoints
        .get(env_name)
        .context("no endpoints configured")?;
    let rest = RestClient::new(
        RestConfig {
            base_url: endpoint.rest.clone(),
            assumed_read_refill: config.rate_limit.read_tokens_per_sec,
            assumed_read_capacity: config.rate_limit.read_tokens_per_sec,
            assumed_default_cost: config.rate_limit.default_request_cost,
            ..RestConfig::default()
        },
        credentials.clone(),
    )
    .context("building the REST client")?;

    let series = if cli.series.is_empty() {
        config.discovery.series_tickers.clone()
    } else {
        cli.series.clone()
    };
    let mut markets = Vec::new();
    for series_ticker in &series {
        let pass = rest
            .discover_markets(series_ticker, &config.discovery.statuses)
            .await
            .with_context(|| format!("discovering markets for {series_ticker}"))?;
        markets.extend(pass.markets.into_iter().map(|m| m.ticker));
    }
    if markets.is_empty() {
        bail!(
            "discovery found no markets to probe against; pass --series with a \
             series that currently has open markets"
        );
    }
    Ok(markets)
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json().with_current_span(true))
        .init();
}

/// Turn any panic into immediate process death.
///
/// # Why a panic must not be survivable here
///
/// A panic inside a tokio task is caught by the runtime: the task dies and the
/// process carries on. For this daemon that is the worst possible outcome. The
/// delta-application function is currently `todo!()`, so a book that receives
/// its first delta panics — and without this hook the process would keep
/// running, keep accepting connections, keep writing files, and silently
/// produce snapshot-only orderbook data with the fault buried in one log line.
/// It would look healthy.
///
/// Errors are different: they bubble as `anyhow` values and the daemon retries.
/// A panic is a bug, and a bug means we no longer trust our own state. Buffered
/// rows are lost, which is correct — writing them would mean trusting a process
/// that has just proved it should not be trusted.
fn install_fatal_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        error!(
            panic = %info,
            "FATAL: panic in the capture daemon. Aborting rather than continuing \
             in a degraded state -- a daemon that survives a panic keeps writing \
             files and looks healthy while capturing nothing."
        );
        std::process::abort();
    }));
}

// ===========================================================================
// Capture
// ===========================================================================

async fn run_capture(cli: Cli, config: AppConfig) -> Result<()> {
    // --- environment gate -------------------------------------------------
    let environment = match cli.env.as_str() {
        "demo" => Environment::Demo,
        "prod" => {
            if !cli.i_understand_this_is_production {
                bail!(
                    "refusing to run against production without \
                     --i-understand-this-is-production. Production capture is \
                     read-only and safe; the flag exists to make the switch \
                     deliberate."
                );
            }
            // The soak must be reproducible. A dirty tree means the captured
            // data records a git_sha that resolves to nothing.
            if kalshi_store::session::build_tree_was_dirty() && !cli.allow_dirty_tree {
                bail!(
                    "refusing to capture production data from a dirty working tree.\n\
                     \n\
                     Session metadata records git_sha as {}, which cannot be \
                     resolved back to reproducible code -- in January there would \
                     be no answer to \"which code produced this file\".\n\
                     \n\
                     Commit or stash your changes, rebuild, and retry. Pass \
                     --allow-dirty-tree only for a deliberate one-off, never for \
                     a soak.",
                    kalshi_store::session::GIT_SHA
                );
            }
            Environment::Prod
        }
        other => bail!("unknown environment {other:?}; expected \"demo\" or \"prod\""),
    };

    let endpoint = config
        .endpoints
        .get(environment.as_str())
        .with_context(|| format!("no endpoints configured for {}", environment.as_str()))?;

    // --- pricing convention: explicit, no default -------------------------
    let convention = PricingConvention::parse(&config.websocket.pricing_convention)
        .context("websocket.pricing_convention must be set explicitly")?;
    info!(
        convention = convention.as_str(),
        use_yes_price = convention.use_yes_price(),
        "pricing convention resolved from configuration"
    );

    let shard_size = cli
        .orderbook_shard_size
        .unwrap_or(config.websocket.orderbook_shard_size);
    let series = if cli.series.is_empty() {
        config.discovery.series_tickers.clone()
    } else {
        cli.series.clone()
    };

    // --- quarantine before opening any writer -----------------------------
    let storage_root = PathBuf::from(&config.storage.root);
    if config.storage.quarantine_on_startup {
        let quarantined = kalshi_store::quarantine::scan_and_quarantine(&storage_root)
            .context("scanning for incomplete Parquet files from a previous run")?;
        if !quarantined.is_empty() {
            warn!(
                count = quarantined.len(),
                "quarantined incomplete files from a previous run"
            );
        }
    }

    // --- session metadata, before any data row ----------------------------
    let session = SessionMetadata::new(
        convention.as_str(),
        environment,
        shard_size,
        config.websocket.channels.clone(),
        endpoint.ws.clone(),
        chrono::Utc::now(),
    )
    .context("building session metadata")?;
    info!(
        session_id = %session.session_id,
        git_sha = %session.git_sha,
        docs_spec_date = %session.docs_spec_date,
        "session opened"
    );

    // --- credentials ------------------------------------------------------
    //
    // Validate before use. An empty key id is accepted by the signer and
    // produces a KALSHI-ACCESS-KEY header of "", which the exchange rejects
    // with a bare "token authentication failure" that names nothing. Catch it
    // here, where the cause can actually be stated.
    if config.auth.key_id.trim().is_empty() {
        bail!(
            "auth.key_id is empty. Set it with:\n\
             \n\
             \x20   export KALSHI_AUTH__KEY_ID=\"<your-key-id-uuid>\"\n\
             \n\
             It is the UUID shown beside your key in Kalshi's API settings, not \
             the key file. Without it every authenticated request returns 401 \
             with a message that names neither the key nor the path."
        );
    }
    let key_path = expand_tilde(&config.auth.private_key_path);
    let credentials = Credentials::from_pem_file(&config.auth.key_id, &key_path)
        .with_context(|| format!("loading the private key from {}", key_path.display()))?;

    // --- REST: tier, cost, clock skew, discovery --------------------------
    let rest = RestClient::new(
        RestConfig {
            base_url: endpoint.rest.clone(),
            assumed_read_refill: config.rate_limit.read_tokens_per_sec,
            assumed_read_capacity: config.rate_limit.read_tokens_per_sec,
            assumed_default_cost: config.rate_limit.default_request_cost,
            ..RestConfig::default()
        },
        credentials.clone(),
    )
    .context("building the REST client")?;

    let tier = rest.confirm_tier().await;
    info!(?tier, "rate-limit tier");
    rest.confirm_default_cost().await;
    // Clock skew: signed timestamps mean NTP drift surfaces as an
    // unexplained 401 that looks nothing like a clock problem.
    rest.check_clock_skew().await;

    let mut markets = Vec::new();
    for series_ticker in &series {
        let pass = rest
            .discover_markets(series_ticker, &config.discovery.statuses)
            .await
            .with_context(|| format!("discovering markets for {series_ticker}"))?;
        info!(
            series = series_ticker,
            markets = pass.markets.len(),
            pages = pass.page_count(),
            "startup discovery"
        );
        markets.extend(pass.markets.into_iter().map(|m| m.ticker));
    }
    if markets.is_empty() {
        warn!(
            ?series,
            "startup discovery found no markets; capture will still run and pick \
             up creations from the lifecycle channel"
        );
    }

    // --- storage ----------------------------------------------------------
    let mut writer_config = WriterConfig::new(storage_root.clone());
    writer_config.max_file_age = chrono::Duration::seconds(config.storage.max_file_age_secs);
    writer_config.max_file_bytes = config.storage.max_file_bytes;
    writer_config.max_batch_rows = config.storage.max_batch_rows;

    let store =
        ParquetStore::open(writer_config, session.clone()).context("opening the Parquet store")?;
    let (handle, receiver) = kalshi_store::sink::channel(config.storage.queue_capacity);

    let metrics = Arc::new(Metrics::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // --- tasks ------------------------------------------------------------
    let store_stats = Arc::new(std::sync::Mutex::new(store.stats()));
    let writer = tokio::spawn(writer_task(
        store,
        receiver,
        Duration::from_secs(config.storage.flush_interval_secs),
        config.storage.max_batch_rows,
        Arc::clone(&store_stats),
        Arc::clone(&shutdown),
    ));

    let heartbeat = tokio::spawn(heartbeat_task(
        PathBuf::from(&config.observability.heartbeat_path),
        Duration::from_secs(config.observability.heartbeat_interval_secs),
        Arc::clone(&shutdown),
    ));

    // Discovery is a loop, not an init step. NFL markets are created through
    // the week and during games, and `market_lifecycle_v2` carries no sequence
    // number -- so a dropped creation message leaves no trace. REST
    // reconciliation is the ONLY detector of a market we never subscribed to.
    let reconciler = tokio::spawn(reconcile_task(
        rest,
        series.clone(),
        config.discovery.statuses.clone(),
        Duration::from_secs(config.discovery.reconcile_interval_secs),
        markets.clone(),
        Arc::clone(&metrics),
        Arc::clone(&shutdown),
    ));

    let reporter = tokio::spawn(metrics_task(
        Arc::clone(&metrics),
        handle.clone(),
        Arc::clone(&store_stats),
        Duration::from_secs(config.observability.metrics_interval_secs),
        Arc::clone(&shutdown),
    ));

    // --- signals ----------------------------------------------------------
    let signal_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("shutdown signal received; flushing buffers");
        signal_shutdown.notify_waiters();
    });

    // --- the connection loop ----------------------------------------------
    let mut backoff = Backoff::new(
        Duration::from_millis(config.websocket.reconnect_initial_ms),
        Duration::from_millis(config.websocket.reconnect_max_ms),
        config.websocket.reconnect_jitter,
    );

    loop {
        if shutdown.notified().now_or_never().is_some() {
            break;
        }
        match run_connection(
            &endpoint.ws,
            &credentials,
            &config,
            convention,
            &markets,
            shard_size,
            &handle,
            &metrics,
            Arc::clone(&shutdown),
        )
        .await
        {
            Ok(Reason::Shutdown) => break,
            Err(_) => {
                metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                warn!(
                    delay_ms = delay.as_millis(),
                    "reconnecting after a connection loss"
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = shutdown.notified() => break,
                }
            }
        }
    }

    shutdown.notify_waiters();
    // Join the writer last: it owns the flush that must complete.
    let _ = tokio::join!(heartbeat, reporter, reconciler);
    writer.await.context("writer task panicked")??;
    info!("capture stopped cleanly");
    Ok(())
}

enum Reason {
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn run_connection(
    url: &str,
    credentials: &Credentials,
    config: &AppConfig,
    convention: PricingConvention,
    markets: &[String],
    shard_size: usize,
    handle: &kalshi_store::sink::StoreHandle,
    metrics: &Arc<Metrics>,
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<Reason, WsError> {
    let mut connection = Connection::connect(url, credentials).await?;
    let mut registry = SubscriptionRegistry::new();
    let mut books: HashMap<String, AnyBook> = HashMap::new();
    let mut grids = GridHistory::new();
    let _ = &mut grids;

    // The lifecycle channel with no market filter: one global sid. It carries
    // no sequence number, so a dropped message there is undetectable -- REST
    // reconciliation is the only way to learn about a missed market.
    connection
        .subscribe(&["market_lifecycle_v2".to_owned()], &[], convention)
        .await?;

    // Orderbook subscriptions, sharded. One market per sid bounds a gap's
    // blast radius to that market, since `seq` is per-sid.
    let pace = Duration::from_millis(config.websocket.subscribe_pace_ms);
    let orderbook_channels: Vec<String> = config
        .websocket
        .channels
        .iter()
        .filter(|c| c.as_str() != "market_lifecycle_v2")
        .cloned()
        .collect();

    for shard in markets.chunks(shard_size.max(1)) {
        connection
            .subscribe(&orderbook_channels, shard, convention)
            .await?;
        for market in shard {
            books.insert(
                market.clone(),
                AnyBook::new(
                    kalshi_common::Ticker::parse(market).unwrap_or_else(|_| {
                        // Discovery already validated these; a malformed
                        // ticker here would be a bug, not bad input.
                        kalshi_common::Ticker::parse("INVALID").unwrap_or_else(|_| unreachable!())
                    }),
                    convention.use_yes_price(),
                ),
            );
        }
        // A per-subscription command rate limit exists (error 27) but is not
        // published numerically, so commands are paced rather than fired as
        // fast as the socket accepts them.
        tokio::time::sleep(pace).await;
    }

    info!(
        subscriptions = registry.len(),
        markets = markets.len(),
        shard_size,
        "subscriptions established"
    );

    loop {
        tokio::select! {
            () = shutdown.notified() => return Ok(Reason::Shutdown),
            message = connection.next_message() => {
                let message = message?;
                let Some(received) = message else { continue };

                metrics.messages.fetch_add(1, Ordering::Relaxed);
                metrics.bytes.fetch_add(
                    u64::try_from(received.raw.len()).unwrap_or(0),
                    Ordering::Relaxed,
                );

                // Register the sid the server just handed us. A sid is always
                // fresh -- `subscribed` never reuses one -- so this creates
                // state rather than merging into it.
                if let Some(ServerMessage::Subscribed(payload)) = &received.parsed {
                    registry.register(
                        payload.msg.sid,
                        payload.msg.channel.clone(),
                        Vec::new(),
                        received.received_at,
                    );
                }
                // A retired sid takes its sequence counter with it: the server
                // will never issue that sid again, so the counter must be
                // dropped rather than reset.
                if let Some(ServerMessage::Unsubscribed(payload)) = &received.parsed {
                    registry.retire(payload.sid);
                }

                let channel = match &received.parsed {
                    Some(ServerMessage::OrderbookSnapshot(_)) => Channel::OrderbookSnapshot,
                    Some(ServerMessage::OrderbookDelta(_)) => Channel::OrderbookDelta,
                    Some(ServerMessage::Ticker(_)) => Channel::Ticker,
                    Some(ServerMessage::Trade(_)) => Channel::Trade,
                    Some(ServerMessage::MarketLifecycle(_) | ServerMessage::EventLifecycle(_)) => {
                        metrics.lifecycle_events.fetch_add(1, Ordering::Relaxed);
                        Channel::MarketLifecycle
                    }
                    Some(ServerMessage::EventFeeUpdate(_)) => Channel::EventFeeUpdate,
                    // Control frames (subscribed / unsubscribed / ok / error)
                    // consume sequence numbers on their sid. Dropping them
                    // leaves holes in the seq stream that are indistinguishable
                    // from real message loss, which makes gap detection
                    // unusable. They are cheap; store them.
                    Some(_) => Channel::Control,
                    None => {
                        metrics.unparsed.fetch_add(1, Ordering::Relaxed);
                        Channel::Unparsed
                    }
                };

                // Non-blocking by construction. `offer` is a synchronous fn and
                // must never become async: a suspension point here would stop
                // Pongs and earn a terminal buffer-overflow error.
                // The encoder reads every typed column out of `parsed`, so
                // leaving it None writes rows whose columns are all empty --
                // exactly what happened on the first live run. Only
                // `raw_message` survived, which is the reason that column
                // exists, but the typed columns were useless.
                //
                // Parsed as a generic Value rather than reusing the typed
                // `ServerMessage`: a message whose shape we do not model must
                // still be encoded field-for-field, and Value keeps whatever
                // the exchange actually sent.
                let parsed = serde_json::from_str::<serde_json::Value>(&received.raw).ok();
                match handle.offer(StoreRecord {
                    channel,
                    received_at: received.received_at,
                    raw: received.raw,
                    parsed,
                }) {
                    Offered::Queued | Offered::Dropped => {}
                    Offered::WriterGone => {
                        error!("writer task is gone; cannot continue capturing");
                        return Ok(Reason::Shutdown);
                    }
                }
            }
        }
    }
}

// ===========================================================================
// Background tasks
// ===========================================================================

/// Group a mixed batch by channel and write each group.
///
/// Records arrive interleaved across channels; each Parquet file holds one
/// channel, so they are grouped before encoding.
fn write_grouped(store: &mut ParquetStore, records: Vec<StoreRecord>) -> Result<()> {
    let mut by_channel: HashMap<Channel, Vec<StoreRecord>> = HashMap::new();
    for record in records {
        by_channel.entry(record.channel).or_default().push(record);
    }
    let now = chrono::Utc::now();
    for (channel, group) in by_channel {
        store
            .write_records(channel, &group, now)
            .with_context(|| format!("writing {} records", channel.as_str()))?;
    }
    Ok(())
}

async fn writer_task(
    mut store: ParquetStore,
    mut receiver: kalshi_store::sink::StoreReceiver,
    flush_interval: Duration,
    max_batch_rows: usize,
    stats: Arc<std::sync::Mutex<kalshi_store::writer::StoreStats>>,
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            _ = ticker.tick() => {
                // Roll files that are due even when idle -- an idle channel
                // would otherwise hold a footerless file open indefinitely,
                // and a file whose footer never lands is unreadable.
                if let Err(err) = store.roll_due_files(chrono::Utc::now()) {
                    warn!(error = %err, "rolling due parquet files");
                }
                if let Ok(mut shared) = stats.lock() {
                    *shared = store.stats();
                }
            }
            record = receiver.recv() => {
                let Some(record) = record else { break };
                // Drain whatever else is queued so the encoder works in
                // batches rather than one row at a time.
                let mut batch = vec![record];
                batch.extend(receiver.drain(max_batch_rows.saturating_sub(1)));
                if let Err(err) = write_grouped(&mut store, batch) {
                    // A write failure must not kill the writer: the read loop
                    // depends on this task staying alive to drain the queue,
                    // and a dead writer turns into a terminal buffer overflow.
                    // `{err:#}` prints the whole anyhow chain -- the outer
                    // context alone names the channel but not the cause.
                    error!(error = format!("{err:#}"), "writing a batch failed; continuing to drain");
                }
            }
        }
    }

    // Drain anything still queued before closing: SIGINT must not lose
    // buffered rows.
    loop {
        let remaining = receiver.drain(max_batch_rows);
        if remaining.is_empty() {
            break;
        }
        if let Err(err) = write_grouped(&mut store, remaining) {
            error!(error = %err, "writing the final batch failed");
        }
    }

    // Every open file must be closed, or its footer never lands and the whole
    // row group is unreadable.
    store
        .close_all(chrono::Utc::now())
        .context("closing the Parquet store on shutdown")?;
    if let Ok(mut shared) = stats.lock() {
        *shared = store.stats();
    }
    let stats = store.stats();
    info!(?stats, "writer stopped");
    Ok(())
}

/// Periodically re-crawl discovery and report anything the live path missed.
///
/// A market found only here was never subscribed, and its data for that
/// interval is permanently gone — the same failure class as a dropped record.
/// So each one is logged individually at WARN and counted as a primary health
/// metric, never treated as routine.
async fn reconcile_task(
    rest: RestClient,
    series: Vec<String>,
    statuses: Vec<String>,
    interval: Duration,
    initial: Vec<String>,
    metrics: Arc<Metrics>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let mut registry = kalshi_ingest::rest::MarketRegistry::new();
    let now = chrono::Utc::now();
    registry.observe_startup(
        initial
            .iter()
            .filter_map(|t| kalshi_common::Ticker::parse(t).ok()),
        now,
    );

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // the first tick fires immediately; skip it

    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            _ = ticker.tick() => {
                let mut seen = Vec::new();
                for series_ticker in &series {
                    match rest.discover_markets(series_ticker, &statuses).await {
                        Ok(pass) => seen.extend(
                            pass.markets
                                .iter()
                                .filter_map(|m| kalshi_common::Ticker::parse(&m.ticker).ok()),
                        ),
                        Err(err) => {
                            warn!(error = %err, series = series_ticker, "reconciliation crawl failed");
                        }
                    }
                }
                let result = registry.reconcile(seen, chrono::Utc::now());
                if !result.is_clean() {
                    let missed = result.missed_by_live_path.len();
                    metrics
                        .reconciliation_misses
                        .fetch_add(u64::try_from(missed).unwrap_or(0), Ordering::Relaxed);
                    error!(
                        missed,
                        markets = ?result.missed_by_live_path,
                        "reconciliation found markets that neither startup discovery \
                         nor the lifecycle feed reported. Those markets were \
                         unsubscribed and their data for that interval is \
                         unrecoverable. This is a bug in the live path, not noise."
                    );
                }
            }
        }
    }
}

async fn heartbeat_task(path: PathBuf, interval: Duration, shutdown: Arc<tokio::sync::Notify>) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            _ = ticker.tick() => {
                if let Some(parent) = path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                let stamp = chrono::Utc::now().to_rfc3339();
                if let Err(err) = tokio::fs::write(&path, stamp).await {
                    warn!(error = %err, path = %path.display(), "could not touch heartbeat");
                }
            }
        }
    }
}

async fn metrics_task(
    metrics: Arc<Metrics>,
    handle: kalshi_store::sink::StoreHandle,
    store_stats: Arc<std::sync::Mutex<kalshi_store::writer::StoreStats>>,
    interval: Duration,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let mut ticker = tokio::time::interval(interval);
    let mut previous_messages = 0u64;
    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            _ = ticker.tick() => {
                let messages = metrics.messages.load(Ordering::Relaxed);
                let rate = (messages - previous_messages) as f64 / interval.as_secs_f64();
                previous_messages = messages;
                let sink = handle.metrics().snapshot();
                let store = store_stats.lock().map(|s| *s).unwrap_or(
                    kalshi_store::writer::StoreStats {
                        open_files: 0,
                        files_closed: 0,
                        rows_written: 0,
                        bytes_written: 0,
                        records_dequeued: 0,
                        rows_expected: 0,
                    },
                );
                // rows_lost must be zero. It is the check that would have
                // caught the encoder gap, where 100% of dequeued records were
                // discarded and nothing in the output said so.
                let rows_lost = store.rows_lost();
                if rows_lost != 0 {
                    error!(
                        rows_lost,
                        rows_expected = store.rows_expected,
                        rows_written = store.rows_written,
                        "rows are being lost between the queue and disk"
                    );
                }
                info!(
                    messages_per_sec = rate,
                    messages_total = messages,
                    bytes_total = metrics.bytes.load(Ordering::Relaxed),
                    reconnects = metrics.reconnects.load(Ordering::Relaxed),
                    lifecycle_events = metrics.lifecycle_events.load(Ordering::Relaxed),
                    unparsed = metrics.unparsed.load(Ordering::Relaxed),
                    off_grid_prices = metrics.off_grid_prices.load(Ordering::Relaxed),
                    desync_invalidations = metrics.desync_invalidations.load(Ordering::Relaxed),
                    reconciliation_misses = metrics.reconciliation_misses.load(Ordering::Relaxed),
                    snapshot_restarts = metrics.snapshot_restarts.load(Ordering::Relaxed),
                    queue_depth = handle.queue_depth(),
                    queue_capacity = handle.capacity(),
                    rows_dropped = sink.dropped,
                    records_dequeued = store.records_dequeued,
                    rows_expected = store.rows_expected,
                    rows_written = store.rows_written,
                    rows_lost,
                    files_closed = store.files_closed,
                    open_files = store.open_files,
                    "metrics"
                );
            }
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(err) => {
                warn!(error = %err, "cannot listen for SIGTERM; SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn expand_tilde(path: &str) -> PathBuf {
    match path.strip_prefix("~/") {
        Some(rest) => match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home).join(rest),
            None => PathBuf::from(path),
        },
        None => PathBuf::from(path),
    }
}

use futures_util::FutureExt as _;
