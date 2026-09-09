#!/usr/bin/env bash
#
# Alert if the capture daemon has stopped touching its heartbeat file.
#
# The daemon writes data/.heartbeat every 30s. Nothing reads it, so a silent
# death at 3am on a Sunday looks exactly like a quiet market. This turns that
# into a notification.
#
# Install (checks every 5 minutes):
#   crontab -e
#   */5 * * * * /Users/mohithram/kalshi-alpha/scripts/watchdog.sh
#
# On macOS, `terminal-notifier` or osascript gives a desktop alert; adjust
# ALERT_CMD for SMS, Pushover, etc.

set -uo pipefail

HEARTBEAT="${KALSHI_HEARTBEAT:-/Users/mohithram/kalshi-alpha/data/.heartbeat}"
# The daemon writes every 30s; 180s means six consecutive misses.
STALE_SECONDS="${KALSHI_STALE_SECONDS:-180}"
LOG="${KALSHI_WATCHDOG_LOG:-/Users/mohithram/kalshi-alpha/data/.watchdog.log}"

alert() {
    local message="$1"
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ALERT $message" >> "$LOG"
    if command -v osascript >/dev/null 2>&1; then
        osascript -e "display notification \"${message}\" with title \"kalshi-alpha capture\"" \
            >/dev/null 2>&1 || true
    fi
    echo "ALERT: $message" >&2
}

if [ ! -f "$HEARTBEAT" ]; then
    alert "heartbeat file missing at ${HEARTBEAT} — daemon may never have started"
    exit 1
fi

now=$(date +%s)
# macOS stat differs from GNU stat.
if stat -f %m "$HEARTBEAT" >/dev/null 2>&1; then
    modified=$(stat -f %m "$HEARTBEAT")
else
    modified=$(stat -c %Y "$HEARTBEAT")
fi
age=$(( now - modified ))

if [ "$age" -gt "$STALE_SECONDS" ]; then
    alert "heartbeat is ${age}s old (threshold ${STALE_SECONDS}s) — capture has stalled or died"
    exit 1
fi

echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ok heartbeat ${age}s old" >> "$LOG"
exit 0
