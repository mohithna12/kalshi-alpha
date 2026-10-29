#!/usr/bin/env bash
# Turn a fresh Ubuntu 24.04 server into a capture host. Run as root on it.
#
# Idempotent: re-running it pulls the latest main, rebuilds, and reinstalls the
# units, which is also how a server is upgraded. It never starts capture without
# credentials, and never touches data/.
#
#   ssh root@HOST 'bash -s' < deploy/bootstrap.sh
set -euo pipefail

REPO_URL="${KALSHI_REPO_URL:-https://github.com/mohithna12/kalshi-alpha.git}"
BRANCH="${KALSHI_BRANCH:-main}"
USER_NAME=kalshi
HOME_DIR="/home/$USER_NAME"
ROOT="$HOME_DIR/kalshi-alpha"

[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential pkg-config git curl ca-certificates rsync chrony >/dev/null

# Timestamps are the data. received_at_ns is only as good as the clock.
systemctl enable --now chrony >/dev/null 2>&1 || true

# Linking arrow and parquet in release mode wants more than 2GB of memory.
# A small VM without swap gets OOM-killed mid-build.
if [ ! -f /swapfile ] && [ "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -lt 4000000 ]; then
    fallocate -l 4G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

id "$USER_NAME" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$USER_NAME"
install -d -m 700 -o "$USER_NAME" -g "$USER_NAME" "$HOME_DIR/.kalshi"

sudo -u "$USER_NAME" -H bash -euo pipefail -s -- "$REPO_URL" "$BRANCH" "$ROOT" <<'AS_USER'
REPO_URL=$1 BRANCH=$2 ROOT=$3
if ! command -v cargo >/dev/null && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --no-modify-path >/dev/null
fi
export PATH="$HOME/.cargo/bin:$PATH"
if [ -d "$ROOT/.git" ]; then
    git -C "$ROOT" fetch --quiet origin "$BRANCH"
    git -C "$ROOT" checkout --quiet "$BRANCH"
    # --ff-only: a server tree that has diverged from main is a mistake to
    # surface, not to merge over.
    git -C "$ROOT" merge --quiet --ff-only "origin/$BRANCH"
else
    git clone --quiet --branch "$BRANCH" "$REPO_URL" "$ROOT"
fi
cd "$ROOT"
# Production capture refuses a dirty tree, so check here rather than at start.
if [ -n "$(git status --porcelain)" ]; then
    echo "working tree at $ROOT is dirty; capture would refuse to start" >&2
    git status --short >&2
    exit 1
fi
cargo build --release --bin capture
AS_USER

install -m 644 "$ROOT/deploy/kalshi-capture.service" /etc/systemd/system/
install -m 644 "$ROOT/deploy/kalshi-heartbeat.service" /etc/systemd/system/
install -m 644 "$ROOT/deploy/kalshi-heartbeat.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable kalshi-capture.service kalshi-heartbeat.timer >/dev/null

if [ ! -f "$ROOT/.env" ]; then
    echo
    echo "Built and installed. Not started: no credentials yet."
    echo "From the laptop: deploy/push-credentials.sh $(hostname -I | awk '{print $1}')"
    exit 0
fi

# Restart rather than start, so an upgrade picks up the new binary.
systemctl restart kalshi-capture.service
systemctl start kalshi-heartbeat.timer
sleep 5
systemctl --no-pager --lines=5 status kalshi-capture.service || true
