#!/usr/bin/env python3
"""Tabulate a placement sweep directory: per config, median time over rounds,
cross-group poll fraction, and per-shard registration spread."""
import re, sys, glob, os, statistics as st
d = sys.argv[1]
def parse_time(line):
    m = re.search(r'time:\s*\[([\d.]+) (\S+) ([\d.]+) (\S+) ([\d.]+)', line)
    if not m: return None
    v = float(m.group(3)); u = m.group(4)
    return v * {'ns': 1e-3, 'µs': 1, 'ms': 1e3, 's': 1e6}[u]
rows = {}
for f in sorted(glob.glob(os.path.join(d, '*.txt'))):
    base = os.path.basename(f)
    if base == 'meta.txt': continue
    bench, tag = base[:-4].split('-', 1)
    m = re.match(r's(\d+)-e(\d)-l(\d)-p(\d+)-r(\d+)', tag)
    if not m: continue
    shards, eager, local, par, rnd = m.groups()
    case = None
    for line in open(f):
        line = line.rstrip()
        mm = re.match(r'^([a-z_/0-9]+)\s*(time:.*)?$', line.strip())
        if mm and mm.group(1) and not line.startswith('  '):
            case = mm.group(1)
        t = parse_time(line)
        if t is not None and case and 'time:   [+' not in line and 'time:   [-' not in line:
            key = (bench, case, shards, par, eager, local)
            rows.setdefault(key, {'t': [], 'x': [], 'reg': None})['t'].append(t)
        s = re.search(r'^\s+([a-z_/0-9]+): io_shard_poll_stats same=(\d+) other=(\d+) off_worker=(\d+).*?registered_per_shard=\[([^\]]*)\]', line)
        if s:
            c = s.group(1); same, other = int(s.group(2)), int(s.group(3))
            key = (bench, c, shards, par, eager, local)
            r = rows.setdefault(key, {'t': [], 'x': [], 'reg': None})
            if same + other: r['x'].append(100.0 * other / (same + other))
            r['reg'] = s.group(5)
def pol(e, l): return ('eager' if e == '1' else 'lazy') + '+' + ('local' if l == '1' else 'rr')
print(f"{'bench/case':34} {'sh':>2} {'par':>3} {'policy':12} {'n':>2} {'median µs':>10} {'range':>19} {'cross%':>7}  registered_per_shard")
for key in sorted(rows, key=lambda k: (k[0], k[1], int(k[2]), int(k[3]), k[4], k[5])):
    bench, case, shards, par, eager, local = key
    r = rows[key]
    if not r['t']: continue
    med = st.median(r['t']); lo, hi = min(r['t']), max(r['t'])
    x = f"{st.median(r['x']):6.1f}" if r['x'] else '   n/a'
    print(f"{bench+'/'+case:34} {shards:>2} {par:>3} {pol(eager, local):12} {len(r['t']):>2} {med:10.1f} {lo:8.1f}-{hi:8.1f}  {x:>7}  [{r['reg']}]")
