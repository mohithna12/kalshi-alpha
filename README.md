# kalshi-alpha

Market-data capture for Kalshi prediction markets, aimed at the NFL season
opening **Wednesday 9 September 2026**.

This repository is **Phase 0: capture infrastructure only**. It records data. It
contains no model, no strategy, no signal, no sizing, and no order placement.

## Read-only, structurally

There is no code path in this repository capable of placing, modifying, or
cancelling an order — not behind a flag, not commented out, not disabled.

This is enforced by construction rather than by policy: the REST client exposes
**no method that accepts a request body**. Every Kalshi write endpoint
(`/portfolio/orders`, cancellations, amendments) is a POST/PUT/DELETE with a
body, so the capability is absent from the type surface, not merely unused. The
WebSocket client subscribes only to public market-data channels and never
subscribes to `fill`, `user_orders`, or `market_positions`.

Treat this as a safety property of the repo. If you later add trading, it
belongs in a different repository with a different API key.

---

## API reference baseline

> **Documentation read: 2026-08-28, from <https://docs.kalshi.com>.**
>
> Record this date. Kalshi's API changes, and in three months you will need to
> know which version of the docs this code was written against. The pages used
> were:
>
> - `https://docs.kalshi.com/getting_started/api_keys`
> - `https://docs.kalshi.com/getting_started/fixed_point_migration`
> - `https://docs.kalshi.com/getting_started/rate_limits`
> - `https://docs.kalshi.com/getting_started/quick_start_websockets`
> - `https://docs.kalshi.com/getting_started/orderbook_responses`
> - `https://docs.kalshi.com/websockets/orderbook-updates`
> - `https://docs.kalshi.com/websockets/connection-keep-alive`
> - `https://docs.kalshi.com/websockets/market-and-event-lifecycle`
> - `https://docs.kalshi.com/api-reference/market/get-markets`
> - `https://docs.kalshi.com/asyncapi.yaml` (authoritative for `seq` / `sid`)

### Verified facts this code depends on

| Fact | Value | Source |
|---|---|---|
| `_dollars` price scale | fixed-point **string**, up to **4** decimal places (`"0.1200"`) | fixed_point_migration |
| Smallest tick | **$0.0001** | fixed_point_migration |
| Intermediate (fee) math | up to **6** decimal places | fixed_point_migration |
| `_fp` quantity scale | fixed-point **string**, exactly **2** decimals (`"10.00"`); min granularity 0.01 contracts | fixed_point_migration |
| Signature | RSA-PSS / SHA-256, **salt length = digest length (32 bytes)** | api_keys |
| Signed message | `{timestamp_ms}{METHOD}{path}`, path **excluding** query string | api_keys |
| WS signed path | `/trade-api/ws/v2` with method `GET` | quick_start_websockets |
| Server heartbeat | Ping frame `0x9`, body `heartbeat`, every ~10s; client must Pong `0xA` | connection-keep-alive |
| Orderbook contents | **bids only**, on both YES and NO sides | orderbook_responses |
| `seq` scope | **per subscription id (`sid`)**, shared across all markets in that subscription | asyncapi.yaml |
| `seq` presence | **orderbook channel only** — absent from ticker/trade/lifecycle, so those gaps are undetectable | asyncapi.yaml |
| `use_yes_price` | default `false`, **scheduled to flip to `true` then be removed** — must be set explicitly | asyncapi.yaml |
| `get_snapshot` | `update_subscription` action returning a snapshot **without modifying the subscription** | asyncapi.yaml |
| sid reuse | `subscribed` returns a **fresh sid every time**; `unsubscribed` carries a final `seq` | asyncapi.yaml |
| Deprecated fields | `taker_side`, `ts`, `time` protected only until **2026-05-14 (passed)** | asyncapi.yaml |
| `determined` events | carry `settlement_value` as fixed-point dollars — model ground truth, free | asyncapi.yaml |
| Rate limits | token bucket; Basic = 200 read tokens/s, most requests cost 10 → ~20 read req/s | rate_limits |
| 429 handling | no `Retry-After`, no `X-RateLimit-*`, no cooldown penalty | rate_limits |
| WS command limit | separate from REST buckets; exists but **numerically undocumented** (error 27) | asyncapi.yaml |
| Subs per connection | **not documented** — must be measured (`just probe-limits`) | — |
| Markets per subscription | capped, value **not documented** (error 26) | asyncapi.yaml |
| Terminal WS errors | **10, 17, 25** require resubscribe; 25 = subscription buffer overflow | asyncapi.yaml |
| Pong handling | automatic in tungstenite 0.30 `read()` — **only while we keep polling** | tungstenite source |
| `GET /account/limits` | **exists**; returns `usage_tier` + `refill_rate`/`bucket_capacity` per bucket | openapi.yaml |
| `GET /account/endpoint_costs` | **exists, unauthenticated**; returns `default_cost` (currently 10) | openapi.yaml |
| Ticker charset | documented `^[A-Z0-9-]+$` is **stale** — it rejects the spec's own examples (`FED-23DEC-T3.00`) | asyncapi.yaml |

Chosen internal scales:

- `Px` — **micro-dollars**, `$1.00 == 1_000_000`. A strict superset of the
  4-decimal wire format, with headroom for the 6-decimal intermediates.
- `Qty` — **centi-contracts**, `1.00 contract == 100`. Matches the documented
  `_fp` scale exactly.
- `Notional` — `Px × Qty`, i.e. scale `1e8`. Computed in `i128`, reduced in
  exactly one function with a documented rounding rule.

---

## Layout

```
kalshi-alpha/
├── Cargo.toml            workspace + the `capture` binary
├── config/default.toml   defaults; env overrides via KALSHI_*
├── crates/
│   ├── common/           Px, Qty, Notional, Ticker, Side, PriceRanges
│   ├── ingest/
│   │   ├── auth.rs       RSA-PSS request signing
│   │   ├── rest.rs       REST client + market discovery (no request bodies)
│   │   ├── ws.rs         WebSocket transport + reconnect
│   │   └── book.rs       local order book (delta application left to implement)
│   └── store/            append-only Parquet writer
└── bin/capture.rs        the daemon
```

## Non-obvious invariants

These are the things that will bite in November. Each is commented at its
definition site as well.

1. **`seq` is per-`sid`, not per-market.** One `subscribe` command covering N
   markets returns one `sid`, and its sequence stream is shared by all N. A gap
   invalidates *every* book on that subscription. Capture therefore shards
   `orderbook_delta` to one market per subscription by default.
2. **A gap is never interpolated.** On a detected gap the affected books are
   marked invalid and re-seeded from a fresh snapshot. The book type exposes its
   contents only through a method returning `Option`, which is `None` while
   invalid — you cannot read a book that might be wrong.
3. **The book holds bids on both sides.** A YES bid at `$0.4300` *is* a NO ask
   at `$0.5700`. Levels are stored canonically on the YES side and the NO view is
   derived, so the two representations cannot drift.
4. **The WebSocket signature signs `/trade-api/ws/v2`,** not a REST path and not
   a URL with a query string. This is the single most common auth failure.
5. **The tick grid is time-varying.** `price_ranges` can change mid-stream via
   `price_level_structure_updated`, so it is stored as an append-only time
   series with effective-from timestamps, never as a mutable field on a market
   row. No tick size is hardcoded anywhere.
6. **Tickers are uppercased on construction.** macOS APFS is case-insensitive
   and Linux is not; capturing on one and analysing on the other would fold
   `KXNFL-ABC`/`kxnfl-abc` into one partition here and split them into two
   there. The capture path compares the normalized value to the raw wire string
   and WARNs on any difference.
7. **`snap_to_grid` returns `GridSnap`, not `Px`.** `Clamped` means the price
   fell outside every published band — the grid is stale or our reading of it
   is wrong. It is logged and counted, never silently accepted.
8. **No `f64`, ever, on price or quantity.** `$0.0001` has no exact binary
   representation. Parsing is exact string → integer.
9. **Every price/quantity column is stored twice** — raw wire string beside the
   parsed integer — so a parser bug found in November is recoverable.
10. **The read path must never block.** Pongs are queued by tungstenite when a
   Ping is read and flushed at the top of the *next* read. If the read task
   blocks on a Parquet write, Pongs stop and the server closes the connection.
   Worse, error 25 (`subscription buffer overflow`) is a *terminal* error: a
   consumer too slow to drain gets its subscription killed outright. Parquet
   writing therefore lives behind a channel, never inline in the socket loop.
11. **`use_yes_price` is set explicitly on every orderbook subscribe.** Its
    default is scheduled to flip and then the flag is to be removed. Relying on
    the default would invert the meaning of every NO-side price with no error,
    no gap, and nothing to detect. The active convention is written into the
    Parquet session metadata so the bytes are self-describing.
12. **Lifecycle delivery is unverifiable by design.** `seq` exists only on the
    orderbook channel; `ticker`, `trade`, and `market_lifecycle_v2` carry no
    sequence at all, so a dropped market-creation message leaves no trace. REST
    reconciliation is therefore the *only* detector of a missed market, not a
    backstop — hence the 300s interval and the alert on
    `reconciliation_misses`.
13. **Recovery escalates: `get_snapshot` → unsub/resub → reconnect.**
    `get_snapshot` re-seeds a book without touching the subscription, so no sid
    churns and no other market is disturbed. Only failure escalates.
14. **A sid is never reused, so its counter is never reset.** `subscribed`
    returns a fresh sid each time. Retiring a sid discards its state entirely;
    there is no `reset()` to call by mistake.
15. **A sequence regression does not rewind the high-water mark.** Accepting a
    lower seq as the new baseline would make the next message look contiguous
    and quietly erase the anomaly.
16. **Discovery is a loop, not an init step.** Startup seeds the registry, live
    `market_lifecycle_v2` grows it, and a 15-minute re-crawl reconciles. A
    market found only by reconciliation was unsubscribed, and that interval is
    unrecoverable — so it WARNs per market and is counted, never treated as
    routine.
17. **`observed_at` and `effective_at` are different columns.** A discovery read
    says what the grid *is*; a lifecycle event says when it *changed*.
    Collapsing them makes "what grid was in force at 14:32 on Nov 8"
    unanswerable. `effective_at` is null unless the wire supplied one.
18. **Never `<&str>::deserialize` on a wire type.** It compiles, passes tests
    that parse from a `&str` slice, and fails at runtime on `serde_json::Value`,
    a reader, or an escaped string. All wire newtypes use `Visitor`s.
19. **Errors 10, 17 and 25 are terminal** and require resubscribe, independent of
   any sequence gap. The resubscribe path is not only for gaps.

## How the sequence-regression bug was caught

Worth recording, because the class of bug matters more than the instance.

`SubscriptionState::observe_seq` originally advanced `last_seq` on *every*
message, including a sequence number lower than the one already seen. So a book
at seq 10 receiving seq 9 would report `Regression` — and then set its
high-water mark to 9, at which point the *next* message, seq 10, looked
perfectly contiguous and reported `InOrder`.

The detector erased its own evidence: one anomaly, then silence. This is exactly
the silent-repair failure the whole architecture is built to avoid — the same
shape as truncating excess precision, or clamping an off-grid price without
saying so.

It was caught by a test that asserted a *repeated* sequence number is a
regression rather than in-order. That assertion looked almost redundant when
written. The lesson: when a component's job is to detect a fault, test that it
still detects the *second* one.

`observe_seq` now advances the high-water mark only on `First` and `InOrder`;
a regression reports and changes nothing.

## A panic takes the process down, deliberately

`OrderBook::apply_delta` is `todo!()`, so the first delta that reaches a book
panics. A panic inside a tokio task is normally caught by the runtime: the task
dies and the process carries on. That is the worst possible outcome here — the
daemon would keep connecting, keep writing files, and silently produce
snapshot-only orderbook data with the fault buried in one log line. It would
look healthy.

`install_fatal_panic_hook` in `bin/capture.rs` logs the panic and calls
`std::process::abort()`. Verified empirically rather than assumed: a minimal
reproduction panicking inside `tokio::spawn` printed `SURVIVED THE PANIC` and
exited 0 without the hook, and exited 134 (SIGABRT) with it.

Buffered rows are lost when this fires. That is correct — a panic means the
process has just proved its own state untrustworthy, and flushing would mean
writing data produced by something that should not be trusted.

Errors are different: they bubble as `anyhow` values with context and the daemon
logs and retries. A panic is a bug.

## The delta specification is red on purpose

`OrderBook::apply_delta` is `todo!()`. The behaviour it must have is written as
13 failing tests in `crates/ingest/tests/book_delta_spec.rs`, each naming a
decision that has to be made deliberately rather than discovered in the data
later.

```sh
just test      # everything else — must be green
just spec      # the delta specification — red until implemented
just test-all  # both
```

CI runs the same split: the main job skips `delta_spec_`, and a separate
non-blocking job reports the specification's status.

## An open question the docs do not answer

`asyncapi.yaml` requires `seq` on `orderbook_snapshot`, so a snapshot fetched
via `update_subscription` / `action: get_snapshot` carries one. It does **not**
say whether that sequence continues the subscription's existing stream or
restarts it.

This matters: if the counter restarts at 1 while deltas continue from 500, then
re-baselining to 1 makes the next delta look like a 499-message gap, which
triggers another recovery, which restarts again — a loop repairing a fault that
does not exist.

So it is classified at runtime rather than assumed. `SnapshotContinuity` reports
`Initial`, `ContinuesStream`, or `RestartsStream` on every snapshot; the book
always re-baselines to the snapshot's sequence (a snapshot *is* the truth, so
this is correct under either answer), and the classification is logged and
counted so the behaviour is learned from observation. **Check this during the
Stage 2 soak and record the answer here.**

## Soak checklist (Stage 2)

Recovery is tested deliberately, not observed:

- [ ] `just verify-pricing <MARKET>` against a **production** market with
      resting NO liquidity — settles `use_yes_price` empirically. Record the
      answer here and set `websocket.pricing_convention` to match.
- [ ] `just probe-limits` on demo **before** the first 200-market subscribe —
      measures subs/connection, markets/subscription, and the command rate limit
- [ ] `just force-gap <MARKET>` — **not yet implemented**: needs a control
      channel into the running daemon. The ladder's logic is covered by
      `crates/ingest/tests/ws.rs` and the invalid-window behaviour by
      `crates/ingest/tests/book.rs`, but the live path is unexercised.
- [ ] 60s network kill, clean recovery, no manual restart
- [ ] ≥12 lifecycle transitions handled
- [ ] `just soak-shard 1` vs `just soak-shard 25` — compare gap rate per sid to
      decide whether shard-size-1 is worth 200 subscriptions
- [ ] Parquet round-trip: every `*_raw` re-parses to its stored integer
- [ ] **Record whether `get_snapshot` continues or restarts the sequence** —
      grep the logs for `snapshot sequence did not advance`; the answer belongs
      in this README

## Running

```sh
just run-demo    # demo environment (default)
just run-prod    # production, read-only; requires an explicit acknowledgement flag
just test        # unit tests
just check       # fmt + clippy + test
```

## Status

| # | Deliverable | State |
|---|---|---|
| 1 | Workspace, `.gitignore`, README | **done** |
| 2 | `common/` — fixed-point types + parser | **done** (34 tests) |
| 3 | `auth.rs` — RSA-PSS signing | **done** (11 tests) |
| 4 | `rest.rs` — discovery + tick metadata | **done** (18 tests) |
| 5 | `ws.rs` — connect, subscribe, reconnect | **done** (34 tests) |
| 6 | `book.rs` — snapshot + gap detection (delta application stubbed) | **done** (27 pass, 13 spec red by design) |
| 7 | `store/` — Parquet writer | pending |
| 8 | `bin/capture.rs` — daemon wiring | **done** |
| 9 | encoder + operational subcommands | **encoder done**; `force-gap`/`verify` pending |
