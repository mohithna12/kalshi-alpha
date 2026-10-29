#!/usr/bin/env bash
# Copy a capture host's data down to the laptop for analysis.
#
#   deploy/pull-data.sh HOST            # into ./data-server/
#   deploy/pull-data.sh HOST some/dir
#
# It goes into its own directory, not data/: while the laptop and the server
# both capture, the same messages exist in both, and one tree would count
# every overlapping hour twice. Files the daemon is still writing have no
# footer yet; readback skips them, and the next pull picks them up whole.
set -euo pipefail
HOST="${1:?usage: deploy/pull-data.sh HOST [DEST]}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${2:-$ROOT/data-server}"
mkdir -p "$DEST"
# No --delete: data is append-only and irreplaceable, and a mistake on the
# server must never be able to erase the laptop's copy.
rsync -az --stats \
    --exclude '.heartbeat*' --exclude '.watchdog*' \
    "root@$HOST:/home/kalshi/kalshi-alpha/data/" "$DEST/"
