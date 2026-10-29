#!/usr/bin/env bash
# Install or upgrade the status dashboard on a capture host. Run as root on it,
# after deploy/bootstrap.sh:
#
#   ssh root@HOST 'bash -s' < deploy/dashboard.sh
#
# Serves https://<ip-with-dashes>.sslip.io behind a password. sslip.io resolves
# that name to the IP itself, which is what lets Caddy get a real certificate
# for a server with no domain. The password is generated once and stored
# root-only at /etc/caddy/dashboard-password; it is never printed.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }

ROOT=/home/kalshi/kalshi-alpha
VENV=/home/kalshi/dashboard-venv
DUCKDB_VERSION=2.0.0
IP=$(curl -fsS4 https://ifconfig.me || hostname -I | awk '{print $1}')
SITE="${KALSHI_DASHBOARD_SITE:-${IP//./-}.sslip.io}"
PASSWORD_FILE=/etc/caddy/dashboard-password

[ -f "$ROOT/dashboard/server.py" ] || {
    echo "no dashboard in $ROOT; run deploy/bootstrap.sh first" >&2
    exit 1
}

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3-venv caddy ufw >/dev/null

# --- the dashboard process -------------------------------------------------
if [ ! -x "$VENV/bin/python" ]; then
    sudo -u kalshi python3 -m venv "$VENV"
fi
sudo -u kalshi "$VENV/bin/pip" install -q --disable-pip-version-check "duckdb==$DUCKDB_VERSION"
install -m 644 "$ROOT/deploy/kalshi-dashboard.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable kalshi-dashboard.service >/dev/null
systemctl restart kalshi-dashboard.service

# --- Caddy: TLS and a password in front of it ------------------------------
created_password=""
if [ ! -f "$PASSWORD_FILE" ]; then
    created_password=$(head -c 18 /dev/urandom | base64 | tr -d '/+=' | head -c 20)
    install -m 600 /dev/null "$PASSWORD_FILE"
    printf '%s\n' "$created_password" > "$PASSWORD_FILE"
fi
hash=$(caddy hash-password --plaintext "$(cat "$PASSWORD_FILE")")
cat > /etc/caddy/Caddyfile <<CADDY
# Managed by kalshi-alpha deploy/dashboard.sh. Re-running it overwrites this.
$SITE {
	basicauth {
		kalshi $hash
	}
	encode gzip
	reverse_proxy 127.0.0.1:8080
}
CADDY
systemctl enable caddy >/dev/null
systemctl reload caddy 2>/dev/null || systemctl restart caddy

# --- firewall: ssh and the dashboard, nothing else --------------------------
# OpenSSH is allowed before enabling, so this cannot lock out the session
# running it.
ufw allow OpenSSH >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
ufw --force enable >/dev/null

echo "dashboard: https://$SITE  (user: kalshi)"
if [ -n "$created_password" ]; then
    echo "a new password was generated and stored at $PASSWORD_FILE"
fi
echo "fetch it with: deploy/dashboard-password.sh $IP"
