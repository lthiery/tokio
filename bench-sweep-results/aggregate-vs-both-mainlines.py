#!/usr/bin/env python3
"""
3-way comparison:
  mainline-stable    — upstream tokio, NO --cfg tokio_unstable
  mainline-unstable  — upstream tokio, WITH --cfg tokio_unstable (true apples-to-apples)
  sharded_mio        — sharded-mio runtime (necessarily --cfg tokio_unstable)

Key delta: sharded_mio vs mainline-unstable isolates the sharded backend's
contribution from the cost of `--cfg tokio_unstable` itself.

History: previously a 4-way comparison that split sharded_mio into
shard_auto / shard_futex via TOKIO_FUTEX_PARK. That env var was removed
at tokio commit ca7537a1 ("rt(sharded-mio): remove direct-futex park
branch") on worktree-nuke-spin, so the two columns produced byte-identical
runs. Collapsed to one.
"""
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS = ["mainline", "mainline-unstable", "sharded_mio"]
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
    cases, last_header, seen = [], None, set()
    if not os.path.exists(path): return cases
    with open(path) as f:
        for raw in f:
            line = raw.rstrip()
            if not line: continue
            m = INLINE_RE.match(line)
            if m:
                case = m.group(1)
                med_ns = float(m.group(4)) * UNIT_TO_NS[m.group(5)]
                if case not in seen:
                    cases.append((case, med_ns)); seen.add(case)
                last_header = None; continue
            stripped = line.strip()
            if (HEADER_RE.match(stripped)
                    and not stripped.startswith("Benchmarking")
                    and "time:" not in stripped
                    and ":" not in stripped
                    and "(" not in stripped):
                last_header = stripped; continue
            m = TIME_RE.search(line)
            if m and last_header:
                med_ns = float(m.group(3)) * UNIT_TO_NS[m.group(4)]
                if last_header not in seen:
                    cases.append((last_header, med_ns)); seen.add(last_header)
                last_header = None
    return cases


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
    rows = []
    for bench in BENCHES:
        per_cfg = {c: dict(parse_file(os.path.join(ROOT, c, f"{bench}.txt"))) for c in CONFIGS}
        all_cases = set()
        for c in CONFIGS: all_cases.update(per_cfg[c].keys())
        for case in sorted(all_cases):
            rows.append((bench, case,
                         per_cfg["mainline"].get(case),
                         per_cfg["mainline-unstable"].get(case),
                         per_cfg["sharded_mio"].get(case)))

    hdr = ["bench", "case", "main", "main-uns", "shard",
           "uns/main", "shard/uns", "shard/main"]
    print(f"{hdr[0]:<20} {hdr[1]:<32} {hdr[2]:>9} {hdr[3]:>9} {hdr[4]:>9}  "
          f"{hdr[5]:>9} {hdr[6]:>9} {hdr[7]:>9}")
    print("-" * 130)

    unstable_overhead = []  # mainline-unstable vs mainline-stable
    shard_vs_uns = {"wins": [], "regs": []}  # the apples-to-apples shard delta

    for bench, case, mn, mu, sh in rows:
        d_uns_main   = pct(mu, mn)
        d_shard_uns  = pct(sh, mu)
        d_shard_main = pct(sh, mn)

        print(f"{bench:<20} {case:<32} {fmt_ns(mn):>9} {fmt_ns(mu):>9} "
              f"{fmt_ns(sh):>9}  "
              f"{d_uns_main:>9} {d_shard_uns:>9} {d_shard_main:>9}")

        if mn is not None and mu is not None:
            d = (mu - mn) / mn * 100
            if abs(d) > 3:
                unstable_overhead.append((bench, case, d))
        if mu is not None and sh is not None:
            d = (sh - mu) / mu * 100
            if   d < -3: shard_vs_uns["wins"].append((bench, case, d))
            elif d >  3: shard_vs_uns["regs"].append((bench, case, d))

    print()
    print("== mainline-unstable vs mainline-stable: cost of --cfg tokio_unstable alone ==")
    if not unstable_overhead:
        print("  (no cases >3%)")
    for b, c, d in sorted(unstable_overhead, key=lambda r: -r[2]):
        sign = "REG" if d > 0 else "WIN"
        print(f"  [{sign}] {b:<20} {c:<32} {d:+6.1f}%")

    print()
    print("== sharded_mio vs mainline-unstable: apples-to-apples sharded backend delta ==")
    print("  -- WINS (>3% faster than mainline-unstable) --")
    if not shard_vs_uns["wins"]: print("    (none)")
    for b, c, d in sorted(shard_vs_uns["wins"], key=lambda r: r[2]):
        print(f"    {b:<20} {c:<32} {d:+6.1f}%")
    print()
    print("  -- REGRESSIONS (>3% slower than mainline-unstable) --")
    if not shard_vs_uns["regs"]: print("    (none)")
    for b, c, d in sorted(shard_vs_uns["regs"], key=lambda r: -r[2]):
        print(f"    {b:<20} {c:<32} {d:+6.1f}%")

    # Specifically inspect the user's flagged regressions
    print()
    print("== user-flagged regressions: full breakdown ==")
    for target in ["contention/unbounded", "contention/unbounded_recv_many"]:
        for bench, case, mn, mu, sh in rows:
            if case == target:
                print(f"  {bench:>14} / {case}")
                print(f"      mainline-stable     {fmt_ns(mn)}")
                print(f"      mainline-unstable   {fmt_ns(mu)}   ({pct(mu, mn)} vs stable)")
                print(f"      sharded_mio         {fmt_ns(sh)}   ({pct(sh, mu)} vs unstable, {pct(sh, mn)} vs stable)")

    n  = sum(1 for _,_,_,mu,sh in rows if mu is not None and sh is not None)
    sw = [d for _,_,d in shard_vs_uns["wins"]]
    sr = [d for _,_,d in shard_vs_uns["regs"]]
    print()
    print("== rollup: sharded_mio vs mainline-unstable ==")
    print(f"  {len(sw):2d} wins / {len(sr):2d} regressions / {n - len(sw) - len(sr):2d} flat (out of {n} cases)")
    if sw: print(f"  best win {min(sw):+.1f}%, median win {sorted(sw)[len(sw)//2]:+.1f}%")
    if sr: print(f"  worst reg {max(sr):+.1f}%, median reg {sorted(sr)[len(sr)//2]:+.1f}%")


if __name__ == "__main__":
    main()
