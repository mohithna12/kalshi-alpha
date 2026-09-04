//! Operational subcommands: empirical checks that settle questions the
//! documentation leaves open.
//!
//! None of these capture data. They exist to answer questions before the season
//! rather than during it.

use anyhow::{bail, Context, Result};
use kalshi_ingest::auth::Credentials;
use kalshi_ingest::wire::ServerMessage;
use kalshi_ingest::ws::{Connection, PricingConvention};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{info, warn};

/// Collect messages from a fresh connection for up to `timeout`, stopping early
/// once `stop` returns true.
async fn collect_for(
    url: &str,
    credentials: &Credentials,
    channels: &[String],
    markets: &[String],
    convention: PricingConvention,
    timeout: Duration,
    mut stop: impl FnMut(&ServerMessage) -> bool,
) -> Result<Vec<ServerMessage>> {
    let mut connection = Connection::connect(url, credentials)
        .await
        .context("connecting")?;
    connection
        .subscribe(channels, markets, convention)
        .await
        .context("subscribing")?;

    let mut collected = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, connection.next_message()).await {
            Err(_) => break,
            Ok(Err(err)) => return Err(err).context("reading from the socket"),
            Ok(Ok(None)) => {}
            Ok(Ok(Some(received))) => {
                if let Some(message) = received.parsed {
                    if stop(&message) {
                        collected.push(message);
                        break;
                    }
                    collected.push(message);
                }
            }
        }
    }
    connection.close().await;
    Ok(collected)
}

// ===========================================================================
// verify-pricing
// ===========================================================================

/// Subscribe one market under both pricing conventions and print the books side
/// by side.
///
/// # Why this must be measured, not reasoned about
///
/// `use_yes_price` currently defaults to false, under which NO-side levels
/// arrive in NO-leg pricing. The spec states the default will flip to true and
/// the flag will then be removed. Getting the convention wrong inverts every
/// NO-side price with no error, no gap, and no reconnect — the one failure in
/// this system with no runtime signal.
///
/// So the convention is not inferred from the documentation. This subscribes
/// the same market twice, once under each setting, and prints what arrives so
/// `(side, price)` can be read off directly.
pub async fn verify_pricing(
    url: &str,
    credentials: &Credentials,
    market: &str,
    timeout: Duration,
) -> Result<()> {
    info!(market, "subscribing under both pricing conventions");

    let mut books: BTreeMap<&'static str, Vec<(String, String, String)>> = BTreeMap::new();

    for convention in [
        PricingConvention::NoLegPricing,
        PricingConvention::YesLegPricing,
    ] {
        let messages = collect_for(
            url,
            credentials,
            &["orderbook_delta".to_owned()],
            &[market.to_owned()],
            convention,
            timeout,
            |message| matches!(message, ServerMessage::OrderbookSnapshot(_)),
        )
        .await
        .with_context(|| format!("collecting under {}", convention.as_str()))?;

        let snapshot = messages.iter().find_map(|m| match m {
            ServerMessage::OrderbookSnapshot(payload) => Some(payload),
            _ => None,
        });
        let Some(snapshot) = snapshot else {
            warn!(
                convention = convention.as_str(),
                "no snapshot arrived within the timeout; the market may be closed \
                 or have no resting liquidity"
            );
            continue;
        };

        let mut rows = Vec::new();
        for (side, levels) in [
            ("yes", &snapshot.msg.yes_dollars_fp),
            ("no", &snapshot.msg.no_dollars_fp),
        ] {
            for level in levels {
                rows.push((
                    side.to_owned(),
                    level.price_raw().to_owned(),
                    level.size_raw().to_owned(),
                ));
            }
        }
        books.insert(convention.as_str(), rows);
    }

    println!("\n=== market: {market} ===\n");
    for (convention, rows) in &books {
        println!(
            "--- use_yes_price = {} ({convention}) ---",
            convention == &"yes_leg"
        );
        if rows.is_empty() {
            println!("  (no levels)");
        }
        for (side, price, size) in rows {
            println!("  side={side:<4} price={price:<10} size={size}");
        }
        println!();
    }

    // The decisive comparison: for the same NO-side liquidity, do the two
    // conventions report complementary prices?
    if let (Some(no_leg), Some(yes_leg)) = (books.get("no_leg"), books.get("yes_leg")) {
        let no_side_prices = |rows: &Vec<(String, String, String)>| -> Vec<String> {
            rows.iter()
                .filter(|(side, _, _)| side == "no")
                .map(|(_, price, _)| price.clone())
                .collect()
        };
        let a = no_side_prices(no_leg);
        let b = no_side_prices(yes_leg);
        println!("NO-side prices under no_leg : {a:?}");
        println!("NO-side prices under yes_leg: {b:?}");
        if a == b {
            println!(
                "\nThe two are IDENTICAL. Either the flag is being ignored, or the \
                 book is empty on the NO side. Do not conclude anything from this \
                 run -- retry against a market with resting NO liquidity."
            );
        } else {
            println!(
                "\nThe two DIFFER, as expected. Under no_leg a NO bid at p is a YES \
                 ask at $1.00 - p; under yes_leg the exchange has already applied \
                 that conversion. Check that each yes_leg price is the complement \
                 of its no_leg counterpart, then set websocket.pricing_convention \
                 accordingly and record the observation in the README."
            );
        }
    } else {
        println!(
            "Could not collect a snapshot under both conventions; nothing is \
             settled by this run."
        );
    }
    Ok(())
}

// ===========================================================================
// probe-limits
// ===========================================================================

/// Measure the undocumented WebSocket limits: subscriptions per connection,
/// markets per subscription, and the subscribe command rate.
///
/// Run against demo **before** the first large NFL subscribe. The AsyncAPI spec
/// defines error 26 (`subscription market limit exceeded`) and error 27 (`too
/// many requests`) but publishes neither number, and says nothing at all about
/// a per-connection subscription cap.
pub async fn probe_limits(
    url: &str,
    credentials: &Credentials,
    markets: &[String],
    max_subscriptions: usize,
    pace: Duration,
) -> Result<()> {
    if markets.is_empty() {
        bail!("probe-limits needs at least one market; run discovery first");
    }
    let mut connection = Connection::connect(url, credentials)
        .await
        .context("connecting")?;

    let mut established = 0usize;
    let mut first_error: Option<(u32, String)> = None;
    let market = markets[0].clone();

    for attempt in 0..max_subscriptions {
        // Subscribing the same market repeatedly would return error 6 (already
        // subscribed), so cycle through what discovery found.
        let target = markets[attempt % markets.len()].clone();
        connection
            .subscribe(
                &["ticker".to_owned()],
                &[target],
                PricingConvention::NoLegPricing,
            )
            .await
            .context("sending a subscribe command")?;
        tokio::time::sleep(pace).await;

        // Drain whatever the server has said so far.
        loop {
            match tokio::time::timeout(Duration::from_millis(50), connection.next_message()).await {
                Err(_) => break,
                Ok(Ok(Some(received))) => match received.parsed {
                    Some(ServerMessage::Subscribed(_)) => established += 1,
                    // Error 6 (already subscribed) is expected once the market
                    // list wraps; anything else is a real limit.
                    Some(ServerMessage::Error(payload))
                        if payload.msg.code != 6 && first_error.is_none() =>
                    {
                        first_error = Some((payload.msg.code, payload.msg.msg.clone()));
                    }
                    _ => {}
                },
                Ok(Ok(None)) => {}
                Ok(Err(err)) => {
                    warn!(error = %err, "socket error during probe");
                    first_error.get_or_insert((0, err.to_string()));
                    break;
                }
            }
            if first_error.is_some() {
                break;
            }
        }
        if first_error.is_some() {
            break;
        }
    }

    connection.close().await;

    println!("\n=== websocket limit probe ===");
    println!("market used for sizing : {market}");
    println!("subscriptions confirmed: {established}");
    match first_error {
        Some((code, message)) => {
            println!("stopped at error       : {code} — {message}");
            println!(
                "\nRecord this in the README. Code 26 is the per-subscription market \
                 limit; 27 is the subscription command rate limit. Neither number is \
                 published, so this observation is the only source."
            );
        }
        None => {
            println!(
                "no limit reached in {max_subscriptions} attempts\n\n\
                 This does not prove there is no cap -- only that it is above \
                 {max_subscriptions}. Raise --max-subscriptions if you need a \
                 tighter bound before committing to shard-size 1."
            );
        }
    }
    Ok(())
}
