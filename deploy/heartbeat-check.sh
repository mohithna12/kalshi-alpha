#!/usr/bin/env bash
# Restart capture when it is running but its heartbeat has gone stale.
#
# systemd restarts a daemon that exits. It does not notice one that is alive
# and wedged -- the 2026-09-10 session sat in exactly that state for 5.9 days.
# The heartbeat file is the daemon's own claim that its loop is turning, so a
# stale one with the unit active means the loop has stopped.
#
# Run every 5 minutes by kalshi-heartbeat.timer, as root (it restarts a unit).
set -uo pipefail
ROOT="${KALSHI_ROOT:-/home/kalshi/kalshi-alpha}"
HEARTBEAT="$ROOT/data/.heartbeat.prod"
STALE_SECONDS="${KALSHI_STALE_SECONDS:-180}"
UNIT=kalshi-capture.service

# Not running is systemd's problem, and a failed unit hit its crash-loop brake
# on purpose. Restarting either from here would defeat the brake.
systemctl is-active --quiet "$UNIT" || exit 0

if [ ! -f "$HEARTBEAT" ]; then
    # A daemon still inside startup discovery has not written one yet.
    started=$(systemctl show -p ActiveEnterTimestampMonotonic --value "$UNIT")
    uptime_us=$(awk '{printf "%d", $1 * 1000000}' /proc/uptime)
    if [ $(( (uptime_us - started) / 1000000 )) -le "$STALE_SECONDS" ]; then
        exit 0
    fi
    age=$(( STALE_SECONDS + 1 ))
else
    age=$(( $(date +%s) - $(stat -c %Y "$HEARTBEAT") ))
fi

if [ "$age" -le "$STALE_SECONDS" ]; then
    exit 0
fi

echo "heartbeat is ${age}s old (threshold ${STALE_SECONDS}s) while ${UNIT} is active; restarting"
systemctl restart "$UNIT"
