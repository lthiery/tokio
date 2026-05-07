# Multi-machine W-axis rigorous sweep

Comparison of mainline tokio vs the sharded-mio runtime across the
worker-thread axis, split between two hosts:

| host    | CPU                  | role                                  |
|---------|----------------------|---------------------------------------|
| lounas  | EPYC 7302, 16C/32T   | low W: 1, 2, 4, 6, 8                  |
| lourip  | EPYC 7H12, 64C/128T  | high W: 6, 16, 32, 64                 |

W=6 runs on **both** machines as an overlap sanity check. A
materially-different W=6 cell between hosts indicates uncontrolled
machine effect; treat the cross-host comparison as suspect.

## Files

| file                          | role                                                                  |
|-------------------------------|-----------------------------------------------------------------------|
| `run-rigorous-multi.sh`       | sweep dispatcher; pick host via `MACHINE=lounas\|lourip`              |
| `aggregate-rigorous-multi.py` | median-of-reps aggregator; prints per-bench tables + wins/regs rollup |
| `rigorous-multi/`             | output tree (created by the dispatcher)                               |

## How to run

### 1. Preflight: CPU governor

Both hosts MUST run the `performance` governor. The dispatcher exits
with code 3 if any cpu is on a different governor. To set it:

```sh
for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  echo performance | sudo tee "$c" >/dev/null
done
```

This is non-persistent across reboot. Re-apply after every boot.

### 2. Launch each host

On lounas (in this worktree, `/home/louis/tokio/.claude/worktrees/io-driver-vtable`):

```sh
MACHINE=lounas bash bench-sweep-results/run-rigorous-multi.sh \
  > bench-sweep-results/rigorous-multi/sweep-lounas.log 2>&1 &
```

On lourip (in `/home/louis/tokio-bench/io-driver-vtable`):

```sh
MACHINE=lourip nohup bash bench-sweep-results/run-rigorous-multi.sh \
  > bench-sweep-results/rigorous-multi/sweep-lourip.log 2>&1 &
disown
```

Run in parallel — they don't share the host, so they don't contend.
Wall-time estimate at default `REPS=5 MEASURE=8 WARMUP=2`:

- lounas: ~1.1h (5 W × 4 W-axis benches × 2 modes × 5 reps + Wfixed)
- lourip: ~0.9h (4 W × 4 W-axis benches × 2 modes × 5 reps + Wfixed)

### 3. Aggregate results

After both sweeps finish, copy lourip's `rigorous-multi/` tree onto
lounas (or vice-versa) so all `W{N}/` directories live under one root,
then:

```sh
python3 bench-sweep-results/aggregate-rigorous-multi.py
```

The aggregator walks `rigorous-multi/W{N}/<mode>/<bench>-r{R}.txt`
plus `rigorous-multi/Wfixed/...`, takes the median across reps per
(W, mode, bench, case), prints a table per bench with W on the rows,
and emits a wins/regressions rollup per shard mode vs mainline.

## Configuration knobs

All overridable via env vars on the dispatcher:

| env var               | default (per machine)            | meaning                          |
|-----------------------|----------------------------------|----------------------------------|
| `MACHINE`             | required                         | `lounas` or `lourip`             |
| `WORKER_LIST`         | lounas: `1 2 4 6 8`<br>lourip: `6 16 32 64` | space-sep list of W values |
| `REPS`                | `5`                              | reps per (W, mode, bench) cell   |
| `MEASURE`             | `8`                              | criterion `--measurement-time` (s) |
| `WARMUP`              | `2`                              | criterion `--warm-up-time` (s)   |
| `MAINLINE_DIR`        | per-machine path                 | mainline-baseline tree           |

## Modes

| mode          | runtime  | env applied                               |
|---------------|----------|-------------------------------------------|
| `mainline`    | mainline | none (also gets `TOKIO_BENCH_WORKERS=$W`) |
| `sharded_mio` | shard    | none (also gets `TOKIO_BENCH_WORKERS=$W`) |

This used to be a 4-way matrix: `mainline / shard_epoll / shard_auto /
shard_futex`, where the shard variants toggled `TOKIO_FUTEX_PARK` to
force a substrate. That env var was removed at tokio commit ca7537a1
("rt(sharded-mio): remove direct-futex park branch") on
worktree-nuke-spin — substrate selection now lives entirely inside the
runtime (`sharded_mio_park.rs`, picked per-park from
`worker_has_io_registered` plus the meta-watcher CAS). The three shard
variants therefore produced byte-identical runs and have been collapsed
into one. The most recent 4-way sweep (lounas run 5328b9cd, lourip run
b18e4617) confirmed the columns were within rep noise across all 50
(host, W, sub-case) combos.

## Bench classification

W-axis benches honor `TOKIO_BENCH_WORKERS` via the `workers()` helper
(added in this changeset). Run at every W in `WORKER_LIST`:

- `sync_watch`, `sync_broadcast`, `sync_notify`, `sync_mpsc`

Fixed-W benches have hardcoded fanout coupled to a specific W (usually
6 or 1) and ignore `TOKIO_BENCH_WORKERS`. Run once under `Wfixed/`
with `taskset -c 1-7`:

- `sync_mpsc_oneshot`, `sync_rwlock`, `sync_semaphore`,
  `remote_spawn`, `spawn_blocking`, `time_timeout`

## Output layout

```
bench-sweep-results/rigorous-multi/
├── build-logs/
│   ├── host-lounas.txt           # governor/SMT/THP/turbo snapshot per run
│   ├── host-lourip.txt
│   ├── mainline-lounas.log       # cargo build output
│   ├── mainline-lourip.log
│   ├── shard-lounas.log
│   └── shard-lourip.log
├── sweep-lounas.log              # dispatcher stderr (progress + cell-FAIL warnings)
├── sweep-lourip.log
├── W1/, W2/, W4/, W6/, W8/       # lounas W-axis results
│   └── {mainline,sharded_mio}/
│       └── <bench>-r{1..REPS}.txt
├── W16/, W32/, W64/              # lourip W-axis results
│   └── ...
└── Wfixed/                       # fixed-W benches (one Wfixed dir per host run)
    └── ...
```

## Failure handling

- **Build failure**: dispatcher exits 2 immediately; see `build-logs/`.
- **Per-cell `cargo bench` failure**: dispatcher prints
  `! cell FAILED: ...` to stderr but continues so a transient flake
  doesn't waste hours. Aggregator's coverage banner shows missing
  reps, so they're easy to spot.
- **Wrong governor**: preflight exits 3 before any work starts.

## Comparison with single-machine sweep

`run-rigorous.sh` / `aggregate-rigorous.py` (the non-`-multi` siblings)
remain valid for the original W=6-only flat sweep on a single host;
they're independent of this multi-machine workflow.
