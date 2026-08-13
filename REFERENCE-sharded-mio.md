# Reference branch: per-worker sharded-mio backend on the IoDriver vtable seam

**Status: reference implementation / evidence. NOT proposed for merge,
and may never be.**

This branch exists to demonstrate that the backend-agnostic io-driver
seam (`IoDriver` vtable, extracted as a minimal behavior-neutral PR on
the `io-driver-vtable-pr` branch) is *sufficient* for a fundamentally
different driver architecture than the one tokio ships: instead of one
shared `mio::Poll` that any worker may drive, every worker owns its own
`mio::Poll` (feature `io-sharded-mio`, requires `--cfg tokio_unstable`).
Registration routes to the registering worker's shard, and the net
types, `Registration`, `PollEvented`, and `AsyncFd` are untouched: the
backend swap happens entirely behind the vtable.

It is one of two backends built on the seam; the other is the
`io_uring` readiness reactor on the `uring-global` branch (this commit
is in that branch's lineage, so the uring code is present here too;
`uring-global`'s tip carries its later correctness work and is the
branch to read for that backend).

## Relationship to `io-driver-vtable-pr`

`io-driver-vtable-pr` is a fresh, minimal extraction on current
master: the vtable seam only, legacy shared-mio as the sole backend,
every vtable entry called in-tree.

This branch is part of the research lineage that extraction was
distilled from. It is **deliberately not rebased**: every benchmark
run cited below records the exact commit it measured, and rewriting
the lineage would detach the numbers from the code that produced
them. Base: forked from master at `6c03e038`. The vtable here
predates the extraction's final trim, so its surface is a superset
(fd-keyed `register_local`, `num_workers`/`unpark_worker` slots,
`IoFlavor` builder plumbing, lazy first-poll registration); the
extraction's design doc (`tokio/docs/io-driver-vtable.md` on that
branch) records the exact deltas. In particular, a sharded backend
wants the driver to participate in worker park/unpark, which the
trimmed seam deliberately does not expose yet: that is exactly the
kind of follow-up surface this branch exists to inform, not to
preempt.

## What is implemented

- One `mio::Poll` per worker; fds register with the registering
  worker's shard (worker index packed into the `mio::Token`),
  deregistration routes by the stored registering worker with a
  generation guard (safe against cross-worker deregistration races).
- A meta-watcher per L3/CCX group ("chiplet") watches its group's
  shards; a parked worker drives its group.
- **Cross-group fallback drain**: a worker that wakes for its own
  group also drains ready events from other groups' shards. This is
  the load-bearing mechanism: it amortizes wakeups across ~64 ready
  children per wake instead of ~4, cutting aggregate wake *count* on
  the stock unpark path with no scheduler-algorithm change. Kill
  switch: `TOKIO_CHIPLET_XGROUP_DRAIN=0`.

The work-stealing scheduler's algorithm (steal/inject/idle) is stock.
The branch does carry reactor↔scheduler *plumbing* in `worker.rs`
(park-loop integration), which every alternative-driver experiment
needs and which is part of the seam discussion, not specific to this
backend.

## Headline results

Measured at this commit on a 64-core EPYC 7H12 (criterion, n≥5,
pinned governor, TIME_WAIT hygiene; loopback `net_tcp_echo` +
`connect_churn`):

- **vs stock shared-mio tokio: echo −20..−25%** at high worker counts.
- Drain on-vs-off (within-binary env toggle, isolating the mechanism):
  echo −14.4% at W32/W64; connect_churn −22/−26%, restoring churn to
  pre-sharding parity.
- Cost when the drain is off: sharding alone regresses churn — the
  drain is what makes the architecture pay; the numbers above are the
  gated (default-on) configuration.

Caveats: loopback microbenchmarks on one host class; real-NIC and
file-IO workloads untested. The result generalizes as "sharding the
driver only wins if wake count is amortized," which is the
transferable finding for any per-worker-driver design (the io_uring
per-worker-ring cliff on the sibling branch has the same root cause
and an analogous fix).

## Why this matters for the seam discussion

Shared-mio (status quo), per-worker sharded-mio (this branch), and a
single-ring io_uring reactor (`uring-global`) are three architectures
with different tradeoffs on different hardware and workloads. All
three fill the same vtable. The seam is what lets them be compared,
maintained against master, and offered behind unstable features
without forking the runtime.
