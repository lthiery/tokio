#!/usr/bin/env python3
"""
Median-of-reps aggregator for the rigorous stable-vs-stable sweep.

Reads:   bench-sweep-results/rigorous/<mode>/<bench>-r<N>.txt
Modes:   mainline | sharded_mio
For each (mode, bench-id) it takes the median across the rep files
(each rep is itself Criterion's median over warmup+measurement) and
then prints a 2-way comparison table plus a wins/regressions rollup.

Comparison reference is mainline-stable (no --cfg tokio_unstable on
either tree, since the sharded backend no longer requires it after
4328bc74).

History: previously a 4-way aggregator that split sharded_mio into
shard_epoll / shard_auto / shard_futex via TOKIO_FUTEX_PARK. That env
var was removed at tokio commit ca7537a1 ("rt(sharded-mio): remove
direct-futex park branch") on worktree-nuke-spin, so the three columns
produced byte-identical runs. Collapsed to one.
"""
import os
import re
from statistics import median

ROOT = os.path.dirname(os.path.abspath(__file__))
RIG  = os.path.join(ROOT, "rigorous")
MODES = ["mainline", "sharded_mio"]
BENCHES = [
    "sync_watch", "sync_broadcast", "sync_notify", "sync_mpsc",
    "sync_mpsc_oneshot", "remote_spawn", "spawn_blocking", "time_timeout",
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


def collect_mode(mode, bench):
    """Return {case_id: median_across_reps_ns} for one (mode, bench)."""
    pattern = re.compile(rf"^{re.escape(bench)}-r(\d+)\.txt$")
    mode_dir = os.path.join(RIG, mode)
    if not os.path.isdir(mode_dir):
        return {}, 0
    rep_files = []
    for name in os.listdir(mode_dir):
        m = pattern.match(name)
        if m:
            rep_files.append((int(m.group(1)), os.path.join(mode_dir, name)))
    rep_files.sort()
    per_case = {}
    for _, path in rep_files:
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


def main():
    rep_counts = {m: {} for m in MODES}
    rows = []
    for bench in BENCHES:
        per_mode = {}
        for mode in MODES:
            meds, nreps = collect_mode(mode, bench)
            per_mode[mode] = meds
            rep_counts[mode][bench] = nreps
        all_cases = set()
        for mode in MODES:
            all_cases.update(per_mode[mode].keys())
        for case in sorted(all_cases):
            rows.append((bench, case,
                         per_mode["mainline"].get(case),
                         per_mode["sharded_mio"].get(case)))

    # Rep coverage banner
    print("== rep coverage (files per (mode, bench)) ==")
    for mode in MODES:
        counts = ", ".join(f"{b}:{rep_counts[mode][b]}" for b in BENCHES)
        print(f"  {mode:<14} {counts}")
    print()

    hdr = ["bench", "case", "main", "shard", "shard/main"]
    print(f"{hdr[0]:<20} {hdr[1]:<36} {hdr[2]:>9} {hdr[3]:>9}  {hdr[4]:>10}")
    print("-" * 95)

    shard_vs_main = {"wins": [], "regs": []}
    THRESH = 3.0  # percent

    for bench, case, mn, sh in rows:
        d = pct(sh, mn)
        print(f"{bench:<20} {case:<36} {fmt_ns(mn):>9} {fmt_ns(sh):>9}  {d:>10}")

        if mn is not None and sh is not None:
            dpct = (sh - mn) / mn * 100
            if   dpct < -THRESH: shard_vs_main["wins"].append((bench, case, dpct))
            elif dpct >  THRESH: shard_vs_main["regs"].append((bench, case, dpct))

    def report(name, bucket, total):
        wins, regs = bucket["wins"], bucket["regs"]
        print()
        print(f"== {name} vs mainline-stable ==")
        print(f"  {len(wins):2d} wins / {len(regs):2d} regressions / "
              f"{total - len(wins) - len(regs):2d} flat (out of {total} cases, |Δ|>{THRESH:.0f}%)")
        if wins:
            print(f"  best win {min(d for _,_,d in wins):+.1f}%, "
                  f"median win {sorted(d for _,_,d in wins)[len(wins)//2]:+.1f}%")
        if regs:
            print(f"  worst reg {max(d for _,_,d in regs):+.1f}%, "
                  f"median reg {sorted(d for _,_,d in regs)[len(regs)//2]:+.1f}%")

        print(f"  -- WINS (>{THRESH:.0f}% faster than mainline) --")
        if not wins: print("    (none)")
        for b, c, d in sorted(wins, key=lambda r: r[2]):
            print(f"    {b:<20} {c:<36} {d:+6.1f}%")
        print(f"  -- REGRESSIONS (>{THRESH:.0f}% slower than mainline) --")
        if not regs: print("    (none)")
        for b, c, d in sorted(regs, key=lambda r: -r[2]):
            print(f"    {b:<20} {c:<36} {d:+6.1f}%")

    n = sum(1 for _,_,mn,_ in rows if mn is not None)
    report("sharded_mio", shard_vs_main, n)

    # Spotlight: previously-flagged small-input notify cases
    print()
    print("== spotlight: previously-flagged small-input notify cases ==")
    spotlight_cases = {
        "sync_notify": [
            "notify_one/10", "notify_one/50", "notify_one/200",
            "notify_waiters/10", "notify_waiters/50", "notify_waiters/100",
        ],
    }
    for bench, targets in spotlight_cases.items():
        for target in targets:
            for b, c, mn, sh in rows:
                if b == bench and c == target:
                    print(f"  {bench:>14} / {c}")
                    print(f"      mainline-stable    {fmt_ns(mn)}")
                    print(f"      sharded_mio        {fmt_ns(sh)}   ({pct(sh, mn)} vs main)")


if __name__ == "__main__":
    main()
