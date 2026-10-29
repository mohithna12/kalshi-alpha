#!/usr/bin/env bash
# Copy the Kalshi API credentials to a capture host and start capture there.
# Run from the laptop, after deploy/bootstrap.sh has run on the host.
#
#   deploy/push-credentials.sh HOST
#
# Reads KALSHI_AUTH__KEY_ID and KALSHI_AUTH__PRIVATE_KEY_PATH from the local
# .env. The key goes over ssh only, lands mode 600 owned by the kalshi user,
# and is never written anywhere inside the repository tree.
set -euo pipefail
HOST="${1:?usage: deploy/push-credentials.sh HOST}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KALSHI_AUTH__KEY_ID=$(sed -n 's/^KALSHI_AUTH__KEY_ID=//p' "$ROOT/.env")
key_path=$(sed -n 's/^KALSHI_AUTH__PRIVATE_KEY_PATH=//p' "$ROOT/.env")
key_path="${key_path/#\$HOME/$HOME}"
key_path="${key_path/#\~/$HOME}"
[ -n "$KALSHI_AUTH__KEY_ID" ] || { echo "KALSHI_AUTH__KEY_ID missing from $ROOT/.env" >&2; exit 1; }
[ -f "$key_path" ] || { echo "private key not found at $key_path" >&2; exit 1; }

REMOTE_KEY=/home/kalshi/.kalshi/kalshi-private-key.pem
REMOTE_ENV=/home/kalshi/kalshi-alpha/.env

ssh "root@$HOST" "install -m 600 -o kalshi -g kalshi /dev/stdin $REMOTE_KEY" < "$key_path"
printf 'KALSHI_AUTH__KEY_ID=%s\nKALSHI_AUTH__PRIVATE_KEY_PATH=%s\n' \
    "$KALSHI_AUTH__KEY_ID" "$REMOTE_KEY" |
    ssh "root@$HOST" "install -m 600 -o kalshi -g kalshi /dev/stdin $REMOTE_ENV"

ssh "root@$HOST" 'systemctl restart kalshi-capture.service &&
    systemctl start kalshi-heartbeat.timer &&
    sleep 8 &&
    systemctl --no-pager --lines=8 status kalshi-capture.service'
