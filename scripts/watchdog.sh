#!/usr/bin/env bash
#
# Watch the capture daemon's heartbeat and restart it when it stops.
#
# The daemon writes data/.heartbeat every 30s. A silent death at 3am on a
# Sunday looks exactly like a quiet market, so this turns that into a
# notification -- and, unless told otherwise, into a restart.
#
# Install (checks every 5 minutes):
#   crontab -e
#   */5 * * * * /Users/mohithram/kalshi-alpha/scripts/watchdog.sh
#
# Cron runs with almost no environment. The daemon needs KALSHI_AUTH__KEY_ID
# and a private key path, so this sources an env file before launching; see
# .env.example. Without credentials it alerts and refuses to start a daemon
# that would only 401 in a loop.
#
# Restarting is refused -- deliberately, loudly -- in three cases where it
# would make things worse:
#
#   1. Low free disk. ENOSPC is what ended the 2026-09-10 session, and a
#      daemon relaunched onto a full volume dies the same way every 5 minutes.
#      This also runs BEFORE anything is killed: a wedged daemon holding
#      buffered rows is worth more than a fresh one that cannot write.
#   2. Missing credentials or binary.
#   3. Too many restarts too quickly -- a crash loop needs a human, and cron
#      will happily run one forever.

set -uo pipefail

ROOT="${KALSHI_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ENV_FILE="${KALSHI_ENV_FILE:-$ROOT/.env}"

# Sourced before anything else is derived, because it names the environment and
# the environment names the heartbeat file. Values already in the environment
# win: an explicit KALSHI_* on the command line beats the file, which beats the
# defaults below. (A plain `set -a; . .env` would invert that.)
load_env_file() {
    [ -f "$1" ] || return 0
    local line key
    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in ''|'#'*) continue ;; esac
        key="${line%%=*}"
        case "$key" in *[!A-Za-z0-9_]*|'') continue ;; esac
        [ -n "${!key:-}" ] && continue
        eval "export $line"
    done < "$1"
}
load_env_file "$ENV_FILE"

KALSHI_CAPTURE_ENV="${KALSHI_CAPTURE_ENV:-prod}"

# Per environment. A demo run and a prod run share a data directory, so one
# shared heartbeat file means starting demo silences the prod watchdog while
# the prod outage carries on.
HEARTBEAT="${KALSHI_HEARTBEAT:-$ROOT/data/.heartbeat.$KALSHI_CAPTURE_ENV}"
PIDFILE="${KALSHI_PIDFILE:-$ROOT/capture.$KALSHI_CAPTURE_ENV.pid}"
CAPTURE_LOG="${KALSHI_CAPTURE_LOG:-$ROOT/capture.log}"
LOG="${KALSHI_WATCHDOG_LOG:-$ROOT/data/.watchdog.log}"
STATE="${KALSHI_WATCHDOG_STATE:-$ROOT/data/.watchdog.state.$KALSHI_CAPTURE_ENV}"
LOCK="${KALSHI_WATCHDOG_LOCK:-$ROOT/data/.watchdog.lock.$KALSHI_CAPTURE_ENV}"
BINARY="${KALSHI_BINARY:-$ROOT/target/release/capture}"

# The daemon writes every 30s; 180s means six consecutive misses.
STALE_SECONDS="${KALSHI_STALE_SECONDS:-180}"
# Set to 0 to go back to alert-only.
RESTART="${KALSHI_WATCHDOG_RESTART:-1}"
# Parquet buffers plus headroom. Below this, writes are the next thing to fail.
MIN_FREE_MB="${KALSHI_MIN_FREE_MB:-2048}"
# More than this many restarts inside RESET_WINDOW_SECONDS is a crash loop.
MAX_RESTARTS="${KALSHI_MAX_RESTARTS:-5}"
RESET_WINDOW_SECONDS="${KALSHI_RESET_WINDOW_SECONDS:-3600}"

note() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" >> "$LOG"; }

alert() {
    local message="$1"
    note "ALERT $message"
    if command -v osascript >/dev/null 2>&1; then
        osascript -e "display notification \"${message}\" with title \"kalshi-alpha capture\"" \
            >/dev/null 2>&1 || true
    fi
    echo "ALERT: $message" >&2
}

# Cron fires every 5 minutes regardless of whether the last run finished. A
# restart takes longer than that if discovery is slow, and two daemons writing
# one data directory is worse than none.
if ! mkdir "$LOCK" 2>/dev/null; then
    note "skip: another watchdog run holds the lock"
    exit 0
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT

mtime() {
    stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null
}

free_mb() {
    df -m "$ROOT" | awk 'NR==2 {print $4}'
}

running_pid() {
    [ -f "$PIDFILE" ] || return 1
    local pid
    pid=$(cat "$PIDFILE" 2>/dev/null) || return 1
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

# --- health ---------------------------------------------------------------
now=$(date +%s)

if [ ! -f "$HEARTBEAT" ]; then
    alert "heartbeat file missing at ${HEARTBEAT} — daemon may never have started"
    age=$(( STALE_SECONDS + 1 ))
else
    age=$(( now - $(mtime "$HEARTBEAT") ))
fi

if [ "$age" -le "$STALE_SECONDS" ]; then
    note "ok heartbeat ${age}s old"
    # A clean run ends the crash-loop streak, so an unrelated failure next
    # week starts from zero rather than from a count set months ago.
    rm -f "$STATE"
    exit 0
fi

alert "heartbeat is ${age}s old (threshold ${STALE_SECONDS}s) — capture has stalled or died"

if [ "$RESTART" != "1" ]; then
    note "restart disabled (KALSHI_WATCHDOG_RESTART=$RESTART); alert only"
    exit 1
fi

# --- refuse to make it worse ----------------------------------------------
free=$(free_mb)
if [ -n "$free" ] && [ "$free" -lt "$MIN_FREE_MB" ]; then
    alert "refusing to restart: ${free}MB free, below the ${MIN_FREE_MB}MB floor. \
Free space first — ENOSPC is what ended the 2026-09-10 session, and a daemon \
relaunched onto a full volume dies the same way."
    exit 1
fi

if [ ! -x "$BINARY" ]; then
    alert "refusing to restart: no executable at ${BINARY} — run 'just build-release'"
    exit 1
fi

if [ -z "${KALSHI_AUTH__KEY_ID:-}" ]; then
    alert "refusing to restart: KALSHI_AUTH__KEY_ID is unset and no env file at \
${ENV_FILE}. Cron has almost no environment; see .env.example."
    exit 1
fi

# --- crash-loop brake ------------------------------------------------------
restarts=0
last=0
if [ -f "$STATE" ]; then
    read -r restarts last < "$STATE" 2>/dev/null || { restarts=0; last=0; }
fi
if [ $(( now - last )) -gt "$RESET_WINDOW_SECONDS" ]; then
    restarts=0
fi
if [ "$restarts" -ge "$MAX_RESTARTS" ]; then
    alert "refusing to restart: ${restarts} restarts inside \
${RESET_WINDOW_SECONDS}s. This is a crash loop, not a blip — check \
${CAPTURE_LOG}. Clear ${STATE} to resume."
    exit 1
fi

# --- restart ---------------------------------------------------------------
# Only now, with every refusal cleared, is killing the old process safe.
if pid=$(running_pid); then
    note "daemon ${pid} is alive but its heartbeat is stale; terminating"
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        note "daemon ${pid} ignored SIGTERM; sending SIGKILL (buffered rows are lost)"
        kill -KILL "$pid" 2>/dev/null || true
    fi
fi

cd "$ROOT" || exit 1
args=(--env "$KALSHI_CAPTURE_ENV")
if [ "$KALSHI_CAPTURE_ENV" = "prod" ]; then
    args+=(--i-understand-this-is-production)
fi

KALSHI_ENV="$KALSHI_CAPTURE_ENV" nohup "$BINARY" "${args[@]}" >> "$CAPTURE_LOG" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PIDFILE"

restarts=$(( restarts + 1 ))
echo "$restarts $now" > "$STATE"

# A daemon that exits during discovery would otherwise be reported as started.
sleep 5
if kill -0 "$new_pid" 2>/dev/null; then
    alert "restarted capture as pid ${new_pid} (restart ${restarts}/${MAX_RESTARTS} \
this hour)"
    exit 0
fi

alert "restart failed: pid ${new_pid} exited within 5s — check ${CAPTURE_LOG}"
exit 1
