#!/usr/bin/env python3
"""
Single-mode aggregator for the sharded-mio sweep.

History: this was previously a 3-way aggregator comparing
  shard_epoll  — TOKIO_FUTEX_PARK=0   (force legacy epoll path)
  shard_auto   — TOKIO_FUTEX_PARK unset (per-worker has_io_registered latch)
  shard_futex  — TOKIO_FUTEX_PARK=1   (force futex on all non-meta workers)

The TOKIO_FUTEX_PARK env var was removed at tokio commit ca7537a1
("rt(sharded-mio): remove direct-futex park branch") on
worktree-nuke-spin — substrate selection now lives entirely inside the
runtime (sharded_mio_park.rs). The three columns produced byte-identical
runs, so this aggregator now just prints the median wall time per
(bench, case) for the single sharded_mio mode.
"""
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG = "sharded_mio"
BENCHES = [
    "sync_watch",
    "sync_broadcast",
    "sync_notify",
    "sync_mpsc",
    "sync_mpsc_oneshot",
    "remote_spawn",
    "spawn_blocking",
    "time_timeout",
]

UNIT_TO_NS = {
    "ps": 1e-3,
    "ns": 1.0,
    "us": 1e3,
    "µs": 1e3,
    "ms": 1e6,
    "s":  1e9,
}

TIME_RE = re.compile(
    r"time:\s+\[\s*([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s*\]"
)
INLINE_RE = re.compile(
    r"^(\S+)\s+time:\s+\[\s*([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s+([\d.]+)\s*([a-zµ]+)\s*\]"
)
HEADER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_/\-#]*( #\d+)?$")


def parse_file(path):
    cases = []
    last_header = None
    seen = set()
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
                med = float(m.group(4))
                unit = m.group(5)
                med_ns = med * UNIT_TO_NS[unit]
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
                med = float(m.group(3))
                unit = m.group(4)
                med_ns = med * UNIT_TO_NS[unit]
                if last_header not in seen:
                    cases.append((last_header, med_ns))
                    seen.add(last_header)
                last_header = None
    return cases


def fmt_ns(ns):
    if ns >= 1e9:
        return f"{ns/1e9:7.3f}  s"
    if ns >= 1e6:
        return f"{ns/1e6:7.3f} ms"
    if ns >= 1e3:
        return f"{ns/1e3:7.3f} µs"
    return f"{ns:7.1f} ns"


def main():
    print(f"{'bench':<20} {'case':<40} {'sharded_mio':>12}")
    print("-" * 80)
    for bench in BENCHES:
        cases = parse_file(os.path.join(ROOT, CONFIG, f"{bench}.txt"))
        for case, ns in cases:
            print(f"{bench:<20} {case:<40} {fmt_ns(ns):>12}")


if __name__ == "__main__":
    main()
