#!/usr/bin/env python3
"""
Median-of-reps aggregator for the multi-machine W-axis rigorous sweep.

Reads:   bench-sweep-results/rigorous-multi/W{N}/<mode>/<bench>-r{R}.txt
         bench-sweep-results/rigorous-multi/Wfixed/<mode>/<bench>-r{R}.txt

Modes:   mainline | sharded_mio

For each (W, bench, case, mode) it takes the median across reps (each
rep is itself Criterion's median over warmup+measurement), then prints:

  1. Rep coverage banner (counts per W, mode, bench)
  2. Per-bench tables, one row per (case, W), columns per mode +
     Δ%-vs-mainline column (shard/main)
  3. Wfixed table (W coupled to the bench by design — W=6 / W=1)
  4. W=6 overlap sanity check (only meaningful when both lounas and
     lourip data sit under the same tree; in single-tree mode this
     section is skipped)
  5. Wins/regressions rollup, aggregated across all (case, W) cells

Comparison reference is mainline-stable. The sweep dispatcher runs
mainline against the same binary under the same taskset and W as
the shard variant, so each (case, W) cell is a controlled compare.

History: previously a 4-way aggregator that split sharded_mio into
shard_epoll / shard_auto / shard_futex via TOKIO_FUTEX_PARK. That env
var was removed at tokio commit ca7537a1 ("rt(sharded-mio): remove
direct-futex park branch") on worktree-nuke-spin, so the three columns
produced byte-identical runs. Collapsed to one.
"""
import os
import re
import sys
from statistics import median

ROOT = os.path.dirname(os.path.abspath(__file__))
RIG  = os.path.join(ROOT, "rigorous-multi")
MODES = ["mainline", "sharded_mio"]

W_AXIS_BENCHES = ["sync_watch", "sync_broadcast", "sync_notify", "sync_mpsc"]
FIXED_BENCHES = [
    "sync_mpsc_oneshot", "sync_rwlock", "sync_semaphore",
    "remote_spawn", "spawn_blocking", "time_timeout",
]

UNIT_TO_NS = {"ps": 1e-3, "ns": 1.0, "us": 1e3, "µs": 1e3, "ms": 1e6, "s": 1e9}
TIME_RE = re.compile(
    r"time:\s+\[\s*([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s*\]"
)
INLINE_RE = re.compile(
    r"^(\S+)\s+time:\s+\[\s*([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s*\]"
)
HEADER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_/\-#]*( #\d+)?$")


def parse_file(path):
    """Return list of (case_id, median_ns) from one Criterion log."""
    cases, last_header, seen = [], None, set()
    if not os.path.exists(path):
        return cases
    with open(path) as f:
        for raw in f:
            line = raw.rstrip()
            if not line:
                continue
            m = INLINE_RE.match(line)
            if m:
                case = m.group(1)
                med_ns = float(m.group(4)) * UNIT_TO_NS[m.group(5)]
                if case not in seen:
                    cases.append((case, med_ns))
                    seen.add(case)
                last_header = None
                continue
            stripped = line.strip()
            if (HEADER_RE.match(stripped)
                    and not stripped.startswith("Benchmarking")
                    and "time:" not in stripped
                    and ":" not in stripped
                    and "(" not in stripped):
                last_header = stripped
                continue
            m = TIME_RE.search(line)
            if m and last_header:
                med_ns = float(m.group(3)) * UNIT_TO_NS[m.group(4)]
                if last_header not in seen:
                    cases.append((last_header, med_ns))
                    seen.add(last_header)
                last_header = None
    return cases


def discover_w_dirs():
    """Return sorted list of W values present under rigorous-multi/."""
    if not os.path.isdir(RIG):
        return []
    ws = []
    for name in os.listdir(RIG):
        m = re.match(r"^W(\d+)$", name)
        if m:
            ws.append(int(m.group(1)))
    return sorted(ws)


def collect_cell(w_label, mode, bench):
    """
    Return ({case_id: median_across_reps_ns}, n_reps) for one
    (W-label, mode, bench).  w_label is "W6", "W16", or "Wfixed".
    """
    pattern = re.compile(rf"^{re.escape(bench)}-r\d+\.txt$")
    cell_dir = os.path.join(RIG, w_label, mode)
    if not os.path.isdir(cell_dir):
        return {}, 0
    rep_files = [os.path.join(cell_dir, name)
                 for name in os.listdir(cell_dir) if pattern.match(name)]
    per_case = {}
    for path in rep_files:
        for case, ns in parse_file(path):
            per_case.setdefault(case, []).append(ns)
    medians = {case: median(samples) for case, samples in per_case.items()}
    return medians, len(rep_files)


def fmt_ns(ns):
    if ns is None: return "       —"
    if ns >= 1e9: return f"{ns/1e9:7.3f}  s"
    if ns >= 1e6: return f"{ns/1e6:7.3f} ms"
    if ns >= 1e3: return f"{ns/1e3:7.3f} µs"
    return f"{ns:7.1f} ns"


def pct(new, old):
    if old is None or new is None or old <= 0: return ""
    return f"{(new-old)/old*100:+6.1f}%"


def print_bench_table(bench, w_labels, data):
    """
    data[(w_label, mode)] = {case: ns}
    Print one combined table per bench: rows = (case, W), cols = modes + Δ%.
    """
    all_cases = set()
    for w in w_labels:
        for mode in MODES:
            all_cases.update(data.get((w, mode), {}).keys())
    if not all_cases:
        return

    print(f"\n=== bench: {bench} ===")
    hdr = (f"{'case':<32} {'W':>6}  "
           f"{'main':>9} {'shard':>9}  "
           f"{'shard/main':>10}")
    print(hdr)
    print("-" * len(hdr))

    for case in sorted(all_cases):
        for w in w_labels:
            mn = data.get((w, "mainline"),    {}).get(case)
            sh = data.get((w, "sharded_mio"), {}).get(case)
            if mn is None and sh is None:
                continue
            print(f"{case:<32} {w:>6}  "
                  f"{fmt_ns(mn):>9} {fmt_ns(sh):>9}  "
                  f"{pct(sh, mn):>10}")


def rollup(label, deltas, thresh=3.0):
    """`deltas` already contains only cells where BOTH mainline and `label`
    have a measurement, so len(deltas) is the correct per-mode denominator."""
    wins = [(b, c, w, d) for b, c, w, d in deltas if d < -thresh]
    regs = [(b, c, w, d) for b, c, w, d in deltas if d >  thresh]
    total = len(deltas)
    flat = total - len(wins) - len(regs)
    print()
    print(f"== {label} vs mainline-stable ==")
    print(f"  {len(wins):3d} wins / {len(regs):3d} regressions / {flat:3d} flat "
          f"(out of {total} (case, W) cells, |Δ|>{thresh:.0f}%)")
    if wins:
        ws = sorted(d for _,_,_,d in wins)
        print(f"  best win {ws[0]:+.1f}%, median win {ws[len(ws)//2]:+.1f}%")
    if regs:
        rs = sorted((d for _,_,_,d in regs), reverse=True)
        print(f"  worst reg {rs[0]:+.1f}%, median reg {rs[len(rs)//2]:+.1f}%")
    print(f"  -- WINS (>{thresh:.0f}% faster than mainline) --")
    if not wins: print("    (none)")
    for b, c, w, d in sorted(wins, key=lambda r: r[3]):
        print(f"    {b:<20} {c:<32} {w:<6} {d:+6.1f}%")
    print(f"  -- REGRESSIONS (>{thresh:.0f}% slower than mainline) --")
    if not regs: print("    (none)")
    for b, c, w, d in sorted(regs, key=lambda r: -r[3]):
        print(f"    {b:<20} {c:<32} {w:<6} {d:+6.1f}%")


def main():
    if not os.path.isdir(RIG):
        print(f"error: {RIG} does not exist; run run-rigorous-multi.sh first",
              file=sys.stderr)
        sys.exit(1)

    w_values = discover_w_dirs()
    w_axis_labels = [f"W{w}" for w in w_values]
    has_fixed = os.path.isdir(os.path.join(RIG, "Wfixed"))

    # ---- Coverage banner ----------------------------------------------------
    print(f"== rep coverage (under {RIG}) ==")
    print(f"  W-axis Ws discovered: {w_values}")
    print(f"  Wfixed present: {has_fixed}")
    for w in w_axis_labels:
        for mode in MODES:
            counts = []
            for b in W_AXIS_BENCHES:
                _, n = collect_cell(w, mode, b)
                counts.append(f"{b}:{n}")
            print(f"  {w:<8} {mode:<14} {', '.join(counts)}")
    if has_fixed:
        for mode in MODES:
            counts = []
            for b in FIXED_BENCHES:
                _, n = collect_cell("Wfixed", mode, b)
                counts.append(f"{b}:{n}")
            print(f"  {'Wfixed':<8} {mode:<14} {', '.join(counts)}")

    # ---- Per-bench tables and delta accumulation ---------------------------
    deltas = []

    def emit(bench, w_labels):
        data = {(w, mode): collect_cell(w, mode, bench)[0]
                for w in w_labels for mode in MODES}
        print_bench_table(bench, w_labels, data)
        for w in w_labels:
            for case, mn in data[(w, "mainline")].items():
                if mn <= 0:
                    continue
                val = data[(w, "sharded_mio")].get(case)
                if val is not None:
                    deltas.append((bench, case, w, (val - mn) / mn * 100))

    for bench in W_AXIS_BENCHES:
        emit(bench, w_axis_labels)
    if has_fixed:
        for bench in FIXED_BENCHES:
            emit(bench, ["Wfixed"])

    # ---- Wins/regressions rollup -------------------------------------------
    rollup("sharded_mio", deltas)

    # ---- W=6 overlap note --------------------------------------------------
    print()
    print("== W=6 overlap note ==")
    print("  W=6 is run on BOTH lounas and lourip as a sanity check.")
    print("  When both machines' results live in the same tree, the")
    print("  W=6 cell's median spans reps from both hosts. To inspect")
    print("  machine-effect explicitly, aggregate the per-machine")
    print("  rigorous-multi/ trees separately and compare W=6 cells.")


if __name__ == "__main__":
    main()
