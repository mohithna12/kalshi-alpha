#!/usr/bin/env bash
# Refuse to start capture when free disk is below the floor (default 2GB).
# Run as ExecStartPre by kalshi-capture.service.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIN_FREE_MB="${KALSHI_MIN_FREE_MB:-2048}"
free=$(df -m "$ROOT" | awk 'NR==2 {print $4}')
if [ "$free" -lt "$MIN_FREE_MB" ]; then
    echo "refusing to start: ${free}MB free under ${ROOT}, below the ${MIN_FREE_MB}MB floor" >&2
    exit 1
fi
