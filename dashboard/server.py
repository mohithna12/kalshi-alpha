#!/usr/bin/env python3
"""Read-only status dashboard for the capture daemon.

Serves one page and a few JSON endpoints, built from what capture already
writes: the heartbeat file, the per-minute metrics log line, the session
records, and the Parquet partitions. It never talks to Kalshi and never writes
under data/, so it cannot affect what is captured.

# Prices are up to one file roll old

A Parquet file has no footer until it is closed, and the daemon closes each
file when it rolls (every `roll_minutes`, 5 by default). Until then the file is
unreadable, so market prices here lag the wire by up to that long. Health
numbers come from the heartbeat and the metrics line instead, which are live.
The page says which is which rather than implying everything is real-time.

# It must not starve capture, even in public

It shares a small VM with the daemon and may be served without a password.
So the work any amount of traffic can cause is bounded by construction, not
only by a rate limit:

- every endpoint is cached, and its arguments are restricted to a finite set
  (captured markets, three chart ranges, one coverage window), so the number
  of distinct queries per cache period is fixed whatever the request rate;
- a cache miss computes once while concurrent requests for the same key wait;
- DuckDB runs one query at a time, on one thread, under a memory cap;
- each client IP is limited to RATE_LIMIT requests a minute.

The systemd unit adds a CPU quota and memory ceiling on top.

Usage:
    python3 dashboard/server.py                       # serves 127.0.0.1:8080
    KALSHI_DASHBOARD_PORT=9000 python3 dashboard/server.py
"""
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import duckdb

ROOT = os.environ.get(
    "KALSHI_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)
DATA = os.path.join(ROOT, os.environ.get("KALSHI_DATA_DIR", "data"))
ENV = os.environ.get("KALSHI_CAPTURE_ENV", "prod")
HOST = os.environ.get("KALSHI_DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.environ.get("KALSHI_DASHBOARD_PORT", "8080"))
# "journal" on a systemd host; a file path for the laptop's capture.log.
LOG_SOURCE = os.environ.get("KALSHI_LOG_SOURCE", "journal")
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
STALE_SECONDS = 180
CHART_HOURS = (6, 24, 168)
COVERAGE_DAYS = 7
RATE_LIMIT = int(os.environ.get("KALSHI_DASHBOARD_RATE_LIMIT", "120"))
PX = 1_000_000
QTY = 100

# Reuse the coverage arithmetic rather than re-deriving it: two copies of
# "what counts as covered" would drift, and the CLI is the one already audited.
#
# No bytecode: importing it would otherwise write scripts/__pycache__/, and an
# untracked file in the tree makes production capture refuse to start.
sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location(
    "coverage", os.path.join(ROOT, "scripts", "coverage.py")
)
coverage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(coverage)


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

_footer_ok = {}


def has_footer(path):
    """True once a Parquet file has been closed.

    An open file is empty or ends mid-row-group; a closed one ends in the
    4-byte magic `PAR1`. Cached by (path, size), so each file is read once.
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return False
    if size < 12:
        return False
    key = (path, size)
    if key not in _footer_ok:
        try:
            with open(path, "rb") as handle:
                handle.seek(-4, os.SEEK_END)
                _footer_ok[key] = handle.read(4) == b"PAR1"
        except OSError:
            return False
    return _footer_ok[key]


def files(channel, since, until=None):
    """Closed Parquet files for `channel` in the UTC date partitions [since, until]."""
    until = until or datetime.now(timezone.utc).date()
    out = []
    day = since
    while day <= until:
        folder = os.path.join(DATA, channel, f"date={day:%Y-%m-%d}")
        try:
            names = os.listdir(folder)
        except OSError:
            names = []
        for name in names:
            path = os.path.join(folder, name)
            if name.endswith(".parquet") and has_footer(path):
                out.append(path)
        day += timedelta(days=1)
    return sorted(out)


_query_slot = threading.Lock()


def query(sql, paths, params=()):
    """Run `sql` with `{files}` bound to a read_parquet over `paths`.

    One query at a time: concurrent page loads queue here rather than each
    taking a core from the capture daemon.
    """
    if not paths:
        return []
    with _query_slot:
        return _run(sql, paths, params)


def _run(sql, paths, params):
    con = duckdb.connect()
    try:
        con.execute("SET threads = 1")
        con.execute("SET memory_limit = '384MB'")
        listing = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
        source = f"read_parquet({listing}, union_by_name = true)"
        return con.execute(sql.replace("{files}", source), params).fetchall()
    finally:
        con.close()


def cached(seconds):
    """Memoize a function's result per argument tuple for `seconds`.

    A miss computes once: other requests for the same key wait for it rather
    than all running the same query at the moment the entry expires.
    """

    def wrap(fn):
        store = {}
        key_locks = defaultdict(threading.Lock)
        lock = threading.Lock()

        def inner(*args):
            with lock:
                key_lock = key_locks[args]
            with key_lock:
                hit = store.get(args)
                if hit and time.monotonic() - hit[0] < seconds:
                    return hit[1]
                value = fn(*args)
                store[args] = (time.monotonic(), value)
                return value

        return inner

    return wrap


# ---------------------------------------------------------------------------
# Status: live, from the heartbeat and the metrics line
# ---------------------------------------------------------------------------


def last_metrics():
    """The most recent `metrics` log line, parsed, with its timestamp."""
    lines = []
    try:
        if LOG_SOURCE == "journal":
            out = subprocess.run(
                ["journalctl", "-u", "kalshi-capture", "-o", "cat", "-n", "300", "--no-pager"],
                capture_output=True, text=True, timeout=5,
            ).stdout
            lines = out.splitlines()
        else:
            with open(LOG_SOURCE, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - 200_000))
                lines = handle.read().decode("utf-8", "replace").splitlines()
    except (OSError, subprocess.SubprocessError):
        return None
    for line in reversed(lines):
        if '"metrics"' not in line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = record.get("fields", {})
        if fields.get("message") == "metrics":
            fields = dict(fields)
            fields.pop("message", None)
            return {"at": record.get("timestamp"), "fields": fields}
    return None


def current_session():
    sessions = coverage.load_sessions(DATA)
    live = [m for m in sessions.values() if not m.get("ended_at")]
    pool = live or list(sessions.values())
    if not pool:
        return None
    meta = max(pool, key=lambda m: m.get("started_at") or "")
    return {
        "session_id": meta.get("session_id"),
        "started_at": meta.get("started_at"),
        "git_sha": meta.get("git_sha"),
        "environment": meta.get("environment"),
        "pricing_convention": meta.get("pricing_convention"),
        "running": not meta.get("ended_at"),
    }


@cached(600)
def data_bytes():
    total = 0
    for folder, _, names in os.walk(DATA):
        for name in names:
            try:
                total += os.path.getsize(os.path.join(folder, name))
            except OSError:
                pass
    return total


def status():
    now = datetime.now(timezone.utc)
    heartbeat_age = None
    path = os.path.join(DATA, f".heartbeat.{ENV}")
    try:
        heartbeat_age = time.time() - os.path.getmtime(path)
    except OSError:
        pass
    disk = shutil.disk_usage(DATA)
    return {
        "now": now.isoformat(),
        "environment": ENV,
        "heartbeat_age_s": heartbeat_age,
        "stale_after_s": STALE_SECONDS,
        "metrics": last_metrics(),
        "session": current_session(),
        "disk": {"free": disk.free, "total": disk.total, "data": data_bytes()},
    }


# ---------------------------------------------------------------------------
# Coverage: from session records
# ---------------------------------------------------------------------------


@cached(60)
def coverage_report(days):
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(days=days - 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    intervals = []
    for meta in coverage.load_sessions(DATA).values():
        start = coverage.parse_ts(meta.get("started_at"))
        end = coverage.parse_ts(meta.get("ended_at"))
        if start is None:
            continue
        if end is None:
            # Same rule as the CLI: an open session under a day old is live.
            if (now - start).total_seconds() >= 86400:
                continue
            end = now
        if end > window_start:
            intervals.append([max(start, window_start), min(end, now)])
    covered = coverage.merge(intervals)

    per_day = []
    day = window_start
    while day < now:
        nxt = min(day + timedelta(days=1), now)
        secs = sum(max(0.0, (min(e, nxt) - max(s, day)).total_seconds()) for s, e in covered)
        span = (nxt - day).total_seconds()
        per_day.append({"date": f"{day:%Y-%m-%d}", "covered_s": secs, "span_s": span})
        day += timedelta(days=1)

    gaps = []
    edges = [[window_start, window_start]] + covered + [[now, now]]
    for (_, end), (start, _) in zip(edges, edges[1:]):
        secs = (start - end).total_seconds()
        if secs >= 120:
            gaps.append({"from": end.isoformat(), "to": start.isoformat(), "seconds": secs})

    up = sum((e - s).total_seconds() for s, e in covered)
    span = (now - window_start).total_seconds()
    return {
        "window_start": window_start.isoformat(),
        "now": now.isoformat(),
        "covered": [[s.isoformat(), e.isoformat()] for s, e in covered],
        "per_day": per_day,
        "gaps": sorted(gaps, key=lambda g: g["from"], reverse=True),
        "covered_s": up,
        "span_s": span,
    }


# ---------------------------------------------------------------------------
# Markets: from the ticker and lifecycle partitions (lag: one file roll)
# ---------------------------------------------------------------------------

EVENT_DATE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})")
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


def event_of(ticker):
    """`KXNFLGAME-26OCT08TBDAL-TB` -> (`KXNFLGAME-26OCT08TBDAL`, date, `TB`)."""
    event, _, outcome = ticker.rpartition("-")
    match = EVENT_DATE.search(event)
    when = None
    if match and match.group(2) in MONTHS:
        try:
            when = date(2000 + int(match.group(1)), MONTHS[match.group(2)], int(match.group(3)))
        except ValueError:
            when = None
    return event, when, outcome


def money(micros):
    return None if micros is None else micros / PX


@cached(60)
def markets():
    today = datetime.now(timezone.utc).date()
    ticker_files = files("ticker", today - timedelta(days=2))
    rows = query(
        """
        SELECT market_ticker,
               arg_max(yes_bid_micros, received_at_ns),
               arg_max(yes_ask_micros, received_at_ns),
               arg_max(price_micros, received_at_ns),
               arg_max(volume_fp_units, received_at_ns),
               arg_max(open_interest_fp_units, received_at_ns),
               epoch_ms(max(received_at_ns)::TIMESTAMPTZ)
        FROM {files}
        GROUP BY market_ticker
        """,
        ticker_files,
    )
    life_files = files("market_lifecycle", today - timedelta(days=14))
    names = sorted({r[0] for r in rows})
    results = {}
    if names and life_files:
        for ticker, result, value in query(
            """
            SELECT market_ticker, arg_max(result, received_at_ns),
                   arg_max(settlement_value_micros, received_at_ns)
            FROM {files}
            WHERE event_type = 'determined' AND list_contains(?, market_ticker)
            GROUP BY market_ticker
            """,
            life_files,
            (names,),
        ):
            results[ticker] = {"result": result, "settlement": money(value)}

    out = []
    for ticker, bid, ask, last, volume, oi, seen in rows:
        event, when, outcome = event_of(ticker)
        out.append({
            "ticker": ticker,
            "event": event,
            "event_date": when.isoformat() if when else None,
            "outcome": outcome,
            "yes_bid": money(bid),
            "yes_ask": money(ask),
            "last": money(last),
            "volume": None if volume is None else volume / QTY,
            "open_interest": None if oi is None else oi / QTY,
            "updated_at": (
                datetime.fromtimestamp(seen / 1000, timezone.utc).isoformat() if seen else None
            ),
            **results.get(ticker, {}),
        })
    newest = max((m["updated_at"] for m in out if m["updated_at"]), default=None)
    return {"as_of": newest, "markets": out}


@cached(60)
def history(ticker, hours):
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    # About 300 points whatever the window, so a week costs what an hour does,
    # snapped to a clock-friendly width so buckets read as "5 min", not "4.8".
    target = hours * 3600 / 300
    bucket = next((b for b in (60, 120, 300, 600, 900, 1800, 3600) if b >= target), 3600)
    rows = query(
        f"""
        SELECT (epoch_ms(received_at_ns::TIMESTAMPTZ) // {bucket * 1000}) * {bucket} AS t,
               arg_max(yes_bid_micros, received_at_ns),
               arg_max(yes_ask_micros, received_at_ns),
               arg_max(price_micros, received_at_ns)
        FROM {{files}}
        WHERE market_ticker = ? AND epoch_ms(received_at_ns::TIMESTAMPTZ) >= ?
        GROUP BY t ORDER BY t
        """,
        files("ticker", since.date()),
        (ticker, int(since.timestamp() * 1000)),
    )
    return {
        "ticker": ticker,
        "bucket_s": bucket,
        "t": [int(r[0]) for r in rows],
        "yes_bid": [money(r[1]) for r in rows],
        "yes_ask": [money(r[2]) for r in rows],
        "last": [money(r[3]) for r in rows],
    }


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


_hits = defaultdict(deque)
_hits_lock = threading.Lock()


def allow(ip):
    """Sliding one-minute window of at most RATE_LIMIT requests per IP."""
    now = time.monotonic()
    with _hits_lock:
        window = _hits[ip]
        while window and now - window[0] > 60:
            window.popleft()
        if len(window) >= RATE_LIMIT:
            return False
        window.append(now)
        if len(_hits) > 10_000:  # forget idle visitors
            for key in [k for k, v in _hits.items() if not v or now - v[-1] > 60]:
                del _hits[key]
        return True


class Handler(BaseHTTPRequestHandler):
    server_version = "kalshi-dashboard"

    def do_GET(self):
        url = urlparse(self.path)
        args = parse_qs(url.query)
        if not allow(self.client()):
            return self.fail(429, "too many requests; try again in a minute")
        try:
            if url.path == "/api/status":
                return self.json(status())
            if url.path == "/api/coverage":
                return self.json(coverage_report(COVERAGE_DAYS))
            if url.path == "/api/markets":
                return self.json(markets())
            if url.path == "/api/history":
                ticker = args.get("market", [""])[0].upper()
                # Only markets actually captured: an arbitrary ticker would be
                # an uncached query per distinct string.
                if ticker not in {m["ticker"] for m in markets()["markets"]}:
                    return self.fail(404, "not a captured market")
                hours = int(args.get("hours", ["24"])[0])
                if hours not in CHART_HOURS:
                    return self.fail(400, f"hours must be one of {CHART_HOURS}")
                return self.json(history(ticker, hours))
            if url.path in ("/", "/index.html"):
                return self.static("index.html", "text/html; charset=utf-8")
            return self.fail(404, "not found")
        except ValueError:
            return self.fail(400, "bad parameter")
        except Exception as exc:  # one bad query must not take the page down
            print(f"error serving {url.path}: {exc!r}", file=sys.stderr)
            return self.fail(500, "query failed")

    def client(self):
        """The visitor's IP. Behind Caddy that is X-Forwarded-For, which is
        only trusted from the local proxy."""
        peer = self.client_address[0]
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded and peer in ("127.0.0.1", "::1"):
            return forwarded.split(",")[-1].strip()
        return peer

    def json(self, payload):
        body = json.dumps(payload, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def static(self, name, content_type):
        with open(os.path.join(STATIC, name), "rb") as handle:
            body = handle.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def fail(self, code, message):
        body = json.dumps({"error": message}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002 -- quiet; errors go to stderr
        pass


def main():
    if not os.path.isdir(DATA):
        sys.exit(f"no data directory at {DATA}")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"dashboard on http://{HOST}:{PORT}  (data: {DATA}, logs: {LOG_SOURCE})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
