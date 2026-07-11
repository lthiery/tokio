# Reference branch: io_uring readiness reactor on the IoDriver vtable seam

**Status: reference implementation / evidence. NOT proposed for merge,
and may never be.**

This branch exists to demonstrate that the backend-agnostic io-driver
seam (`IoDriver` vtable, extracted as a minimal behavior-neutral PR on
the `io-driver-vtable-pr` branch) is *sufficient*: a complete
`io_uring` readiness reactor — plus a per-worker sharded-mio reactor —
plugs into that seam without forking `Registration`, `PollEvented`,
`AsyncFd`, or the net types. It also carries the benchmark evidence
for what such a backend earns.

## Relationship to `io-driver-vtable-pr`

`io-driver-vtable-pr` is a fresh, minimal extraction on current
master (`9c465e2f`): the vtable seam only, legacy shared-mio as the
sole backend, registration still eager, every vtable entry called
in-tree.

This branch is the research lineage that extraction was distilled
from. It is **deliberately not rebased**: every benchmark run cited
below records the exact commit it measured, and rewriting the lineage
would detach the numbers from the code that produced them. Base:
forked from master at `6c03e038` (`test: remove churn() task from
lifo_stealable (#8070)`). The vtable here predates the extraction's
final trim, so its surface is a superset (fd-keyed `register_local`,
`num_workers`/`unpark_worker` slots, `IoFlavor` builder plumbing,
lazy first-poll registration); the extraction's design doc
(`tokio/docs/io-driver-vtable.md` on that branch) records the exact
deltas.

## What is implemented

Three reactor configurations behind one feature
(`io-uring-reactor`, requires `--cfg tokio_unstable`; builder knob
`enable_uring_reactor()`):

1. **Per-worker rings** (multi-thread default): one `io_uring` per
   worker doing `POLL_ADD_MULTI` readiness, `IORING_OP_MSG_RING`
   cross-worker wakes, fd placement round-robin.
2. **Global single ring** (`TOKIO_URING_GLOBAL=1`, multi-thread):
   single ring on the vtable, stock-parker holder rotation — no
   per-worker rings, no wake fan-out.
3. **current_thread / LocalRuntime** (forced global ring, n=1): the
   degenerate case where a single ring and the single core trivially
   coincide; also where completion-based ops naturally live.

A per-worker **sharded-mio** backend (`io-sharded-mio` feature) fills
the same vtable, isolating "shard the driver" from "use io_uring".

## Test suites (start here when reviewing)

- `tokio/tests/rt_uring_current_thread.rs` — **12-test
  current_thread/LocalRuntime suite**, including
  `core_migrates_across_block_on_threads` (the core-migration
  correctness case: the ring must follow the core when `block_on`
  moves threads), remote-spawn-wakes-parked-runtime, cross-thread
  shutdown, `!Send` I/O on `LocalRuntime`, and a poll-path-only
  smoke.
- `tokio/tests/rt_uring_late_timer.rs` — timer registered after park,
  both per-worker and global modes (regression tests from the
  2026-07-03 correctness review; all findings fixed on this branch).
- `tokio/tests/rt_uring_reactor.rs`, `net_uring_reactor_tcp.rs` —
  multi-thread reactor and TCP integration.

Build/test (the 12th test needs `io-sharded-mio`; without it, 11 run):

```
RUSTFLAGS="--cfg tokio_unstable" cargo test -p tokio \
  --features full,io-uring,io-uring-reactor,io-sharded-mio \
  --test rt_uring_current_thread --test rt_uring_late_timer
```

## Headline results (criterion, n≥5, pinned governors, TIME_WAIT hygiene)

- **64-core EPYC 7H12, global single ring vs mainline mio**: echo
  faster at *every* worker count (−24% to −45%), no high-W cliff, no
  tuning knobs; p99 RTT ~flat where mainline's grows.
- **16-core EPYC, global ring**: parity-or-win vs per-worker rings at
  all W; W2 tail p99 45.9→34.2ms (beats mainline).
- **Registration microbench**: single-queue register/deregister −52%
  to −61% vs mainline, flat across W.
- **current_thread/LocalRuntime**: register/dereg −61%, echo within
  5% of mio, p99 RTT parity. The W=1 multi-thread tail penalty does
  *not* reproduce here — it is a wake/task_work topology artifact of
  the multi-thread parker, not intrinsic to uring readiness.

Full tables, run IDs, and methodology live in the tracking issue.

## Known limitations (documented, not hidden)

- `tokio::signal` / `tokio::process` ride the legacy mio stack; on a
  uring-reactor runtime they need routing work (known hang on
  current_thread). A production backend would route them before
  stabilization.
- Multi-thread W=1/W=2 median echo carries a flat ~1µs/msg per-event
  handling cost vs mio (root-caused: multishot-POLL kernel machinery
  + drain staging, not wake latency). Uring wins from W4 up because
  mainline's park churn grows with W while this tax stays flat.
- Global-ring `register_dereg` pays an unconditional eventfd write in
  `push_op` (+8–15%); wake-only-if-parked is the identified fix.
