#!/usr/bin/env python3
"""Report how much wall-clock time the capture actually covered, and where it did not.

The daemon restarts -- on a crash, on a watchdog kill, when the machine sleeps.
Each restart opens a new session, and the wall-clock between one session's
`ended_at` and the next's `started_at` is data that was never captured. Nothing
in the Parquet files says "a gap belongs here": the rows simply stop and then
resume. So coverage has to be reconstructed from the session records, which is
what this does.

A session with a null `ended_at` either is still running or died without
flushing (SIGKILL, power loss, panic). Both are reported distinctly, because
"still running" is fine and "died hard" means the tail of that session may be
missing rows the metrics counted as written.

Usage:
    python3 scripts/coverage.py data
    python3 scripts/coverage.py data --since 2026-10-01
    python3 scripts/coverage.py data --min-gap 300
"""
import argparse
import glob
import json
import os
import sys
from datetime import datetime, timedelta, timezone


def parse_ts(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_sessions(root):
    """One record per session id, deduplicated across channel directories."""
    sessions = {}
    for path in glob.glob(os.path.join(root, "*", "date=*", "_session-*.json")):
        try:
            with open(path) as handle:
                meta = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        sid = meta.get("session_id")
        if not sid:
            continue
        # The same session writes an identical record into every channel dir
        # it touched; keep whichever copy has an ended_at, since a session that
        # shut down cleanly records it only after its last file is closed.
        existing = sessions.get(sid)
        if existing is None or (not existing.get("ended_at") and meta.get("ended_at")):
            sessions[sid] = meta
    return sessions


def merge(intervals):
    """Union of [start, end) intervals, so overlapping sessions count once."""
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="data")
    ap.add_argument("--since", help="only report days on or after this UTC date")
    ap.add_argument(
        "--min-gap",
        type=int,
        default=120,
        help="ignore gaps shorter than this many seconds (default 120)",
    )
    args = ap.parse_args()

    sessions = load_sessions(args.root)
    if not sessions:
        sys.exit(f"no session records found under {args.root}")

    now = datetime.now(timezone.utc)
    intervals, unclean, running = [], [], []
    for sid, meta in sessions.items():
        start = parse_ts(meta.get("started_at"))
        end = parse_ts(meta.get("ended_at"))
        if start is None:
            continue
        if end is None:
            # Within a metrics interval of now it is plausibly the live one.
            if (now - start).total_seconds() < 86400:
                running.append((sid, start))
                end = now
            else:
                unclean.append((sid, start))
                continue
        intervals.append([start, end])

    covered = merge(intervals)
    span_start = min(i[0] for i in covered)
    span_end = max(i[1] for i in covered)

    print(f"sessions: {len(sessions)}  ({len(running)} running, {len(unclean)} ended without flushing)")
    print(f"span:     {span_start:%Y-%m-%d %H:%M}Z -> {span_end:%Y-%m-%d %H:%M}Z")
    total = (span_end - span_start).total_seconds()
    up = sum((e - s).total_seconds() for s, e in covered)
    print(f"coverage: {up / total * 100:.2f}% of the span ({up / 3600:.1f}h of {total / 3600:.1f}h)")

    # Per-day coverage. A session straddling midnight is split across days so
    # each day's percentage is of that day alone.
    print("\nper-day coverage (UTC):")
    day = span_start.replace(hour=0, minute=0, second=0, microsecond=0)
    while day < span_end:
        nxt = day + timedelta(days=1)
        if args.since and day.strftime("%Y-%m-%d") < args.since:
            day = nxt
            continue
        secs = sum(
            max(0.0, (min(e, nxt) - max(s, day)).total_seconds()) for s, e in covered
        )
        window = (min(nxt, span_end) - max(day, span_start)).total_seconds()
        pct = secs / window * 100 if window else 0.0
        bar = "#" * int(pct / 4)
        flag = "" if pct >= 99 else ("  <-- MAJOR GAP" if pct < 90 else "  <-- gap")
        print(f"  {day:%Y-%m-%d}  {pct:6.2f}%  {bar:<25}{flag}")
        day = nxt

    # The gaps themselves, largest first: this is the list of windows where
    # market activity exists and we have nothing.
    gaps = []
    for (_, end), (start, _) in zip(covered, covered[1:]):
        secs = (start - end).total_seconds()
        if secs >= args.min_gap:
            gaps.append((secs, end, start))
    print(f"\ngaps longer than {args.min_gap}s: {len(gaps)}")
    if gaps:
        tot = sum(g[0] for g in gaps)
        print(f"total missing: {tot / 3600:.1f}h")
        print("\nlargest:")
        for secs, end, start in sorted(gaps, reverse=True)[:12]:
            print(f"  {secs / 60:7.1f} min   {end:%Y-%m-%d %H:%M}Z -> {start:%H:%M}Z")

    if unclean:
        print(f"\nsessions that ended without flushing ({len(unclean)}):")
        for sid, start in sorted(unclean, key=lambda x: x[1])[:10]:
            print(f"  {sid}  started {start:%Y-%m-%d %H:%M}Z")
        print("  These were killed before closing their store. Their last open")
        print("  Parquet file may be footerless; `just readback` reports those.")


if __name__ == "__main__":
    main()
