# kalshi-alpha task runner. `brew install just` if you do not have it;
# a Makefile with the same targets is provided alongside.

default:
    @just --list

# Install the repository's git hooks. REQUIRED after cloning: hooks live in
# .git/hooks, which is not part of the repository, so they do not clone.
# Install git hooks. Run once after cloning.
setup:
    git config core.hooksPath .githooks
    @echo "git hooks installed from .githooks/"
    @just verify-hooks

# Prove the pre-commit hook actually refuses key material, rather than assuming
# it does. A guard that has never been seen to fire is not a guard.
#
# The decoy marker is assembled from fragments so this file does not itself
# trip the hook -- the same discipline the hook's own source follows.
# Prove the pre-commit hook refuses key material and passes a clean tree.
verify-hooks:
    #!/usr/bin/env bash
    set -uo pipefail
    probe=".hook-probe-$$.txt"
    trap 'git reset --quiet -- "$probe" 2>/dev/null; rm -f "$probe"' EXIT
    printf -- '-----%s %s-----\nZmFrZQ==\n' 'BEGIN' 'PRIVATE KEY' > "$probe"
    git add -f -- "$probe"
    if .githooks/pre-commit >/dev/null 2>&1; then
        echo "FAIL: the pre-commit hook did NOT refuse staged key material" >&2
        exit 1
    fi
    git reset --quiet -- "$probe" 2>/dev/null
    rm -f "$probe"
    if ! .githooks/pre-commit >/dev/null 2>&1; then
        echo "FAIL: the pre-commit hook refuses a clean tree" >&2
        exit 1
    fi
    echo "pre-commit hook verified: refuses key material, passes a clean tree"

build:
    cargo build --workspace --all-targets

# Build the capture binary once, up front.
#
# The long-running recipes execute ./target/release/capture directly rather
# than going through `cargo run`. Two reasons: `cargo run` needs cargo on PATH
# at launch time (it is not there by default -- rustup was installed with
# --no-modify-path), and it can decide to rebuild mid-session, which for a
# capture daemon means an unplanned restart during a game.
build-release:
    cargo build --release --bin capture

# Everything except the delta specification, which is red by design until
# `apply_delta` is implemented. This is the target to watch for regressions.
# Run the suite, skipping the delta spec. THIS is the commit gate.
test:
    cargo test --workspace -- --skip delta_spec_

# The executable specification for OrderBook::apply_delta. EXPECTED TO FAIL
# until that function is written -- each failure names a behaviour that has to
# be decided deliberately. Implement until this is green, then `just test`
# stays green too.
# The delta specification. Red until apply_delta is implemented.
spec:
    cargo test --workspace delta_spec_ -- --nocapture

# Absolutely everything, including the red spec.
# Everything, including the red delta spec.
test-all:
    cargo test --workspace

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Everything CI runs.
# Everything CI runs.
check: fmt clippy test
    cargo fmt --all -- --check

# Demo environment. Synthetic prices and near-zero activity: this proves
# protocol correctness, not reliability.
# Capture against demo. Proves protocol correctness, not reliability.
run-demo: build-release
    KALSHI_ENV=demo ./target/release/capture --env demo

# Production, READ-ONLY. The acknowledgement flag exists to make the switch
# deliberate; production read-only capture is expected and safe.
# Capture against production, read-only. Refuses a dirty tree.
run-prod: build-release
    KALSHI_ENV=prod ./target/release/capture \
        --env prod --i-understand-this-is-production

# Stage 2 soak target: short-horizon crypto series spawn a new event every
# 15 minutes, giving both a high delta rate and constant lifecycle churn.
# August NFL has almost no activity and proves nothing.
# Stage 2 soak against high-churn crypto series.
soak:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production \
        --series KXBTCD --series KXETHD

# --- Recovery-path exercises (deliverable 5/8 implements the subcommands) ---

# Force a sequence-gap recovery on ONE live subscription mid-stream and assert
# the whole recovery contract holds. This is the hot path for the most common
# failure, so it is a deliberate test rather than an observation:
#   1. pick one orderbook_delta sid that is currently valid and receiving data
#   2. unsubscribe it, then resubscribe the same market
#   3. assert a fresh orderbook_snapshot arrives within the timeout
#   4. assert that book returns to valid
#   5. assert NO other sid was invalidated or lost a message
# Exits non-zero on any failed assertion, so it can gate the soak.
# NOT IMPLEMENTED: needs a control channel into the running daemon.
force-gap MARKET:
    cargo run --release --bin capture -- force-gap --market {{MARKET}} \
        --mode all --assert-snapshot --assert-isolation --timeout-secs 15

# Exercise ONE rung of the recovery ladder in isolation. All three are run by
# `just force-gap`; these exist for debugging a single rung.
#   get-snapshot -- update_subscription action:get_snapshot. No sid churn.
#   resubscribe  -- unsubscribe + subscribe. New sid, old counter discarded.
#   reconnect    -- full teardown. Every sid replaced.
# NOT IMPLEMENTED: one rung of the recovery ladder in isolation.
force-gap-rung MARKET RUNG:
    cargo run --release --bin capture -- force-gap --market {{MARKET}} \
        --mode {{RUNG}} --assert-snapshot --assert-isolation --timeout-secs 60

# Verify the two pricing conventions empirically against a live market before
# committing to one for the season. Subscribes the same market twice, once with
# use_yes_price:false and once with true, and prints both books side by side so
# (side, price) can be read off rather than inferred.
# Settle use_yes_price empirically against a live market.
verify-pricing MARKET:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production \
        verify-pricing --market {{MARKET}}

# Same, against demo. Demo books are usually empty, so this proves the command
# runs but settles nothing about the convention -- use the prod form.
# Same, against demo. Demo books are usually empty; settles little.
verify-pricing-demo MARKET:
    cargo run --release --bin capture -- --env demo \
        verify-pricing --market {{MARKET}}

# Empirically measure the undocumented WebSocket caps on DEMO before depending
# on either connection architecture: subscriptions per connection, markets per
# subscription (error 26), and the subscribe command rate limit (error 27).
# Run this before the first 200-market NFL subscribe, not during it.
# Measure the undocumented WebSocket subscription limits.
probe-limits:
    KALSHI_ENV=demo cargo run --release --bin capture -- probe-limits \
        --max-subscriptions 512 --pace-ms 25

# A/B the sharding decision during the soak. shard-size 1 bounds a gap to one
# market but costs one subscription each; larger shards are cheaper but widen
# the blast radius. The 60s metrics line reports gap rate per sid so this can be
# decided on data rather than on caution.
# A/B the orderbook shard size during a soak.
soak-shard N:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production \
        --series KXBTCD --series KXETHD --orderbook-shard-size {{N}}

# --- Pre-flight ---------------------------------------------------------

# Everything that must be true before a real capture run. Runs the daemon
# against demo for a fixed window, then verifies what landed on disk.
#
# This is the check that matters most before the season: 169 unit tests cover
# parsing, encoding and storage, and cover the network path not at all.
# PREFLIGHT: run this before any real capture. Proves the full path works.
preflight SECONDS="90":
    #!/usr/bin/env bash
    set -uo pipefail
    echo "=== 1. credentials ==="
    key="${KALSHI_AUTH__PRIVATE_KEY_PATH:-$HOME/.kalshi/kalshi-private-key.pem}"
    if [ ! -f "$key" ]; then
        echo "FAIL  no private key at $key"; exit 1
    fi
    perms=$(stat -f '%Lp' "$key" 2>/dev/null || stat -c '%a' "$key")
    if [ "$perms" != "600" ]; then
        echo "WARN  key is mode $perms; chmod 600 $key"
    fi
    if [ -z "${KALSHI_AUTH__KEY_ID:-}" ]; then
        echo "FAIL  KALSHI_AUTH__KEY_ID is not set"; exit 1
    fi
    echo "  ok  key present, key id set"

    echo "=== 2. build ==="
    cargo build --release --bin capture 2>&1 | tail -1

    echo "=== 3. capture against demo for {{SECONDS}}s ==="
    rm -f data/.preflight.log
    mkdir -p data
    KALSHI_ENV=demo ./target/release/capture --env demo > data/.preflight.log 2>&1 &
    pid=$!
    sleep {{SECONDS}}
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "FAIL  daemon exited early:"; tail -20 data/.preflight.log; exit 1
    fi
    kill -INT "$pid"
    # Shutdown must flush and write every footer.
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    if kill -0 "$pid" 2>/dev/null; then
        echo "FAIL  daemon did not exit within 30s of SIGINT"; kill -9 "$pid"; exit 1
    fi
    echo "  ok  started, ran {{SECONDS}}s, exited on SIGINT"

    echo "=== 4. startup checks from the log ==="
    grep -q '"message":"websocket connected and authenticated"' data/.preflight.log \
        && echo "  ok  authenticated" \
        || { echo "FAIL  never authenticated"; grep -i 'error\|refus' data/.preflight.log | head -5; exit 1; }
    markets=$(grep -o '"markets":[0-9]*' data/.preflight.log | head -1 | cut -d: -f2)
    echo "  ..  discovery found ${markets:-0} markets"
    if [ "${markets:-0}" = "0" ]; then
        echo "WARN  discovery found no markets -- check discovery.series_tickers"
    fi
    grep -q 'clock skew' data/.preflight.log && \
        echo "  ..  $(grep -o '"delta_ms":[-0-9]*' data/.preflight.log | head -1)"

    echo "=== 5. rows reached disk ==="
    lost=$(grep -o '"rows_lost":[-0-9]*' data/.preflight.log | tail -1 | cut -d: -f2)
    echo "  ..  rows_lost=${lost:-n/a}"
    if [ -n "${lost:-}" ] && [ "${lost}" != "0" ]; then
        echo "FAIL  rows are being lost between the queue and disk"; exit 1
    fi

    echo "=== 6. read back what was written ==="
    python3 scripts/readback.py data || exit 1

    echo
    echo "PREFLIGHT PASSED"

# Offline verification of captured data: re-parses every raw column with an
# independent parser and reports per-sid sequence gaps.
# Verify captured data offline: re-parse raw columns, report per-sid gaps.
readback DAY="":
    #!/usr/bin/env bash
    if [ -n "{{DAY}}" ]; then
        python3 scripts/readback.py data --date {{DAY}}
    else
        python3 scripts/readback.py data
    fi

# Verify captured Parquet round-trips: re-parsing every *_raw column must
# reproduce its stored integer column.
# NOT IMPLEMENTED: use `just readback` instead.
verify-parquet DAY:
    cargo run --release --bin capture -- verify --day {{DAY}}
