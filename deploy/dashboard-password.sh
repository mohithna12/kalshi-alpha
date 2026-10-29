#!/usr/bin/env bash
# Copy the dashboard password from a capture host into ~/.kalshi/, mode 600,
# without printing it.
#
#   deploy/dashboard-password.sh HOST
set -euo pipefail
HOST="${1:?usage: deploy/dashboard-password.sh HOST}"
OUT="$HOME/.kalshi/dashboard-password"
mkdir -p "$HOME/.kalshi"
umask 077
ssh "root@$HOST" cat /etc/caddy/dashboard-password > "$OUT"
echo "saved to $OUT (user: kalshi)"
