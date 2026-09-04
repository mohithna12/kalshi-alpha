# kalshi-alpha task runner. `brew install just` if you do not have it;
# a Makefile with the same targets is provided alongside.

default:
    @just --list

# Install the repository's git hooks. REQUIRED after cloning: hooks live in
# .git/hooks, which is not part of the repository, so they do not clone.
setup:
    git config core.hooksPath .githooks
    @echo "git hooks installed from .githooks/"
    @just verify-hooks

# Prove the pre-commit hook actually refuses key material, rather than assuming
# it does. A guard that has never been seen to fire is not a guard.
#
# The decoy marker is assembled from fragments so this file does not itself
# trip the hook -- the same discipline the hook's own source follows.
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

# Everything except the delta specification, which is red by design until
# `apply_delta` is implemented. This is the target to watch for regressions.
test:
    cargo test --workspace -- --skip delta_spec_

# The executable specification for OrderBook::apply_delta. EXPECTED TO FAIL
# until that function is written -- each failure names a behaviour that has to
# be decided deliberately. Implement until this is green, then `just test`
# stays green too.
spec:
    cargo test --workspace delta_spec_ -- --nocapture

# Absolutely everything, including the red spec.
test-all:
    cargo test --workspace

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Everything CI runs.
check: fmt clippy test
    cargo fmt --all -- --check

# Demo environment. Synthetic prices and near-zero activity: this proves
# protocol correctness, not reliability.
run-demo:
    KALSHI_ENV=demo cargo run --release --bin capture -- --env demo

# Production, READ-ONLY. The acknowledgement flag exists to make the switch
# deliberate; production read-only capture is expected and safe.
run-prod:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production

# Stage 2 soak target: short-horizon crypto series spawn a new event every
# 15 minutes, giving both a high delta rate and constant lifecycle churn.
# August NFL has almost no activity and proves nothing.
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
force-gap MARKET:
    cargo run --release --bin capture -- force-gap --market {{MARKET}} \
        --mode all --assert-snapshot --assert-isolation --timeout-secs 15

# Exercise ONE rung of the recovery ladder in isolation. All three are run by
# `just force-gap`; these exist for debugging a single rung.
#   get-snapshot -- update_subscription action:get_snapshot. No sid churn.
#   resubscribe  -- unsubscribe + subscribe. New sid, old counter discarded.
#   reconnect    -- full teardown. Every sid replaced.
force-gap-rung MARKET RUNG:
    cargo run --release --bin capture -- force-gap --market {{MARKET}} \
        --mode {{RUNG}} --assert-snapshot --assert-isolation --timeout-secs 60

# Verify the two pricing conventions empirically against a live market before
# committing to one for the season. Subscribes the same market twice, once with
# use_yes_price:false and once with true, and prints both books side by side so
# (side, price) can be read off rather than inferred.
verify-pricing MARKET:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production \
        verify-pricing --market {{MARKET}}

# Same, against demo. Demo books are usually empty, so this proves the command
# runs but settles nothing about the convention -- use the prod form.
verify-pricing-demo MARKET:
    cargo run --release --bin capture -- --env demo \
        verify-pricing --market {{MARKET}}

# Empirically measure the undocumented WebSocket caps on DEMO before depending
# on either connection architecture: subscriptions per connection, markets per
# subscription (error 26), and the subscribe command rate limit (error 27).
# Run this before the first 200-market NFL subscribe, not during it.
probe-limits:
    KALSHI_ENV=demo cargo run --release --bin capture -- probe-limits \
        --max-subscriptions 512 --pace-ms 25

# A/B the sharding decision during the soak. shard-size 1 bounds a gap to one
# market but costs one subscription each; larger shards are cheaper but widen
# the blast radius. The 60s metrics line reports gap rate per sid so this can be
# decided on data rather than on caution.
soak-shard N:
    KALSHI_ENV=prod cargo run --release --bin capture -- \
        --env prod --i-understand-this-is-production \
        --series KXBTCD --series KXETHD --orderbook-shard-size {{N}}

# Verify captured Parquet round-trips: re-parsing every *_raw column must
# reproduce its stored integer column.
verify-parquet DAY:
    cargo run --release --bin capture -- verify --day {{DAY}}
