#!/usr/bin/env python3
"""Verify a capture run: re-parse every raw column, and find sequence gaps.

Two jobs, both of which the daemon cannot currently do for itself:

1. Round-trip check. Every `*_raw` string is re-parsed here with a parser
   written independently of the Rust one, and compared to the stored integer.
   If they disagree, the Rust parser has a bug and the raw column is what lets
   you re-derive the truth.

2. Sequence-gap detection. Live gap detection is not wired into the daemon
   (`observe_seq` is never called from bin/capture.rs), so gaps are found here,
   after the fact. `seq` is per-sid, so continuity is checked per sid, never
   globally.

Usage:
    python3 scripts/readback.py data
    python3 scripts/readback.py data --date 2026-09-09
"""
import argparse
import glob
import json
import os
import sys
from collections import defaultdict

try:
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("pyarrow is required: pip3 install pyarrow")

PX_SCALE_DIGITS = 6
QTY_SCALE_DIGITS = 2

# (raw column, parsed column, scale) per channel, mirroring Channel::dual_columns.
DUAL = {
    "orderbook_snapshot": [
        ("price_raw", "price_micros", PX_SCALE_DIGITS),
        ("size_raw", "size_fp_units", QTY_SCALE_DIGITS),
    ],
    "orderbook_delta": [
        ("price_raw", "price_micros", PX_SCALE_DIGITS),
        ("delta_raw", "delta_fp_units", QTY_SCALE_DIGITS),
    ],
    "ticker": [
        ("price_raw", "price_micros", PX_SCALE_DIGITS),
        ("yes_bid_raw", "yes_bid_micros", PX_SCALE_DIGITS),
        ("yes_ask_raw", "yes_ask_micros", PX_SCALE_DIGITS),
        ("yes_bid_size_raw", "yes_bid_size_fp_units", QTY_SCALE_DIGITS),
        ("yes_ask_size_raw", "yes_ask_size_fp_units", QTY_SCALE_DIGITS),
        ("volume_raw", "volume_fp_units", QTY_SCALE_DIGITS),
        ("open_interest_raw", "open_interest_fp_units", QTY_SCALE_DIGITS),
    ],
    "trade": [
        ("yes_price_raw", "yes_price_micros", PX_SCALE_DIGITS),
        ("no_price_raw", "no_price_micros", PX_SCALE_DIGITS),
        ("count_raw", "count_fp_units", QTY_SCALE_DIGITS),
    ],
    "market_lifecycle": [
        ("settlement_value_raw", "settlement_value_micros", PX_SCALE_DIGITS),
    ],
}


def parse_fixed(text, scale):
    """Exact decimal string -> scaled int. No float, anywhere.

    Written independently of the Rust implementation on purpose: a checker that
    shares the parser it is checking agrees with itself and proves nothing.
    """
    if text is None:
        return None
    s = str(text)
    if not s:
        return None
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    if "." in s:
        whole, frac = s.split(".", 1)
    else:
        whole, frac = s, ""
    if not whole.isdigit() or (frac and not frac.isdigit()):
        return None
    # Excess non-zero precision is an error, matching the Rust contract.
    if len(frac) > scale and frac[scale:].strip("0"):
        return None
    frac = (frac + "0" * scale)[:scale]
    value = int(whole + frac) if (whole + frac) else 0
    return -value if neg else value


def check_round_trip(table, channel):
    """Re-parse each raw column and compare to the stored integer."""
    mismatches = []
    checked = 0
    names = set(table.column_names)
    for raw_col, parsed_col, scale in DUAL.get(channel, []):
        if raw_col not in names or parsed_col not in names:
            continue
        raws = table.column(raw_col).to_pylist()
        parsed = table.column(parsed_col).to_pylist()
        for i, (r, p) in enumerate(zip(raws, parsed)):
            if r is None:
                continue
            checked += 1
            expected = parse_fixed(r, scale)
            if expected != p:
                mismatches.append((raw_col, i, r, p, expected))
                if len(mismatches) >= 10:
                    return checked, mismatches
    return checked, mismatches


def check_sequences_global(per_sid):
    """Per-sid continuity, merged across every channel.

    `seq` is scoped to the subscription id, and one subscription covers all the
    channels it was opened with -- so a single sid's sequence numbers are split
    across orderbook_delta, orderbook_snapshot, ticker, trade and control files.
    Checking any one file in isolation reports enormous phantom gaps.

    Control frames (subscribed / ok / unsubscribed) also consume sequence
    numbers. The daemon stores them for exactly this reason; if they were
    dropped, every acknowledgement would look like message loss.
    """
    gaps = []
    summary = {}
    for sid, values in per_sid.items():
        ordered = sorted(values)
        if not ordered:
            continue
        missing = 0
        for a, b in zip(ordered, ordered[1:]):
            if b != a + 1:
                missing += b - a - 1
                gaps.append((sid, a, b, b - a - 1))
        summary[sid] = {
            "messages": len(values),
            "range": (ordered[0], ordered[-1]),
            "missing": missing,
        }
    return summary, gaps


def check_sequences(table):
    """Per-sid sequence continuity.

    seq is scoped to the subscription id, not the market and not the
    connection, so each sid's stream is checked on its own. A restart (seq
    dropping to a low value) is reported separately from a gap: a fresh
    subscribe issues a new sid, and a snapshot may re-baseline the counter.
    """
    names = set(table.column_names)
    if "sid" not in names or "seq" not in names:
        return {}, []
    sids = table.column("sid").to_pylist()
    seqs = table.column("seq").to_pylist()
    per_sid = defaultdict(list)
    for sid, seq in zip(sids, seqs):
        if sid is None or seq is None:
            continue
        per_sid[sid].append(seq)

    gaps = []
    summary = {}
    for sid, values in per_sid.items():
        ordered = sorted(set(values))
        missing = 0
        for a, b in zip(ordered, ordered[1:]):
            if b != a + 1:
                missing += b - a - 1
                gaps.append((sid, a, b, b - a - 1))
        summary[sid] = {
            "messages": len(values),
            "range": (ordered[0], ordered[-1]) if ordered else None,
            "missing": missing,
        }
    return summary, gaps


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="data")
    ap.add_argument("--date", help="only this UTC partition, e.g. 2026-09-09")
    args = ap.parse_args()

    pattern = os.path.join(args.root, "**", "*.parquet")
    files = sorted(f for f in glob.glob(pattern, recursive=True)
                   if "_quarantine" not in f)
    if args.date:
        files = [f for f in files if f"date={args.date}" in f]

    if not files:
        print(f"FAIL  no parquet files under {args.root}"
              + (f" for {args.date}" if args.date else ""))
        print("      The daemon wrote nothing. Check that discovery found markets")
        print("      and that subscriptions were established.")
        return 1

    print(f"scanning {len(files)} file(s) under {args.root}\n")

    totals = defaultdict(int)
    all_mismatches = []
    sessions = set()
    unreadable = []
    # seq is per-sid and spans every channel of that subscription, so
    # continuity is accumulated globally and checked once at the end.
    seq_by_sid = defaultdict(set)

    for path in files:
        channel = None
        for part in path.split(os.sep):
            if part in DUAL or part in ("event_fee_update", "price_ranges", "unparsed"):
                channel = part
        try:
            pf = pq.ParquetFile(path)
            table = pf.read()
        except Exception as exc:
            unreadable.append((path, str(exc)))
            continue

        totals[channel or "unknown"] += table.num_rows
        totals["_rows"] += table.num_rows

        meta = pf.metadata.metadata or {}
        conv = meta.get(b"kalshi.pricing_convention")
        sess = meta.get(b"kalshi.session_id")
        if sess:
            sessions.add((sess.decode(), conv.decode() if conv else "?"))

        checked, mismatches = check_round_trip(table, channel)
        totals["_checked"] += checked
        all_mismatches.extend((path, *m) for m in mismatches)

        names = set(table.column_names)
        if "sid" in names and "seq" in names:
            for sid, seq in zip(table.column("sid").to_pylist(),
                                table.column("seq").to_pylist()):
                if sid is not None and seq is not None:
                    seq_by_sid[sid].add(seq)

    print("rows by channel:")
    for channel, count in sorted(totals.items()):
        if not channel.startswith("_"):
            print(f"  {channel:<22} {count:>10,}")
    print(f"  {'TOTAL':<22} {totals['_rows']:>10,}\n")

    print("sessions present:")
    for sess, conv in sorted(sessions):
        print(f"  {sess}  pricing_convention={conv}")
    if not sessions:
        print("  NONE — files carry no session metadata, which should be impossible")
    print()

    ok = True

    if unreadable:
        ok = False
        print(f"FAIL  {len(unreadable)} file(s) could not be opened:")
        for path, exc in unreadable[:5]:
            print(f"        {path}\n          {exc}")
        print("      A file whose footer never landed is unreadable, not truncated.")
        print("      Run the daemon again; startup quarantines these automatically.\n")

    print(f"round-trip: re-parsed {totals['_checked']:,} raw values independently")
    if all_mismatches:
        ok = False
        print(f"FAIL  {len(all_mismatches)} mismatch(es) — the Rust parser and this")
        print("      one disagree. Trust the raw column, not the integer.")
        for path, col, i, raw, stored, expected in all_mismatches[:5]:
            print(f"        {os.path.basename(path)} {col}[{i}] "
                  f"raw={raw!r} stored={stored} expected={expected}")
    else:
        print("  PASS  every raw value reproduces its stored integer\n")

    summary, all_gaps = check_sequences_global(seq_by_sid)
    total_msgs = sum(s["messages"] for s in summary.values())
    print(f"sequence continuity: {len(summary)} sid(s), {total_msgs:,} sequenced messages")
    print("  (seq is per-sid and spans every channel on that subscription,")
    print("   so continuity is checked across all files together)")
    if all_gaps:
        ok = False
        total_missing = sum(g[3] for g in all_gaps)
        pct = 100.0 * total_missing / max(total_msgs + total_missing, 1)
        print(f"WARN  {len(all_gaps)} gap(s), {total_missing} message(s) missing ({pct:.2f}%)")
        print("      Live gap detection is not wired into the daemon, so these")
        print("      were never repaired at runtime.")
        for sid, a, b, n in sorted(all_gaps, key=lambda g: -g[3])[:8]:
            print(f"        sid={sid} {a} -> {b} ({n} missing)")
    else:
        print("  PASS  no gaps in any sid\n")

    print("=" * 60)
    print("RESULT:", "PASS" if ok else "PROBLEMS FOUND — see above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
