# Sharded-mio: EPOLLEXCLUSIVE fanout

**Status:** design — not yet implemented.
**Branch:** `worktree-io-driver-vtable`.
**Supersedes:** [`readiness-stealing.md`](./readiness-stealing.md). Per-worker
readiness stealing (P1 from that doc) shipped on this branch but does
not meet its own bench gate (`busy_owner_3burners` still ~44 ms vs
the < 1 ms target). Root cause documented in
[`readiness-stealing-HANDOFF.md`](./readiness-stealing-HANDOFF.md)
sessions 6–7. This doc replaces the steal-pass approach with kernel-
side load balancing via `EPOLLEXCLUSIVE` fanout.

## TL;DR

Sharded-mio's contention motivation stands: per-worker `mio::Poll`
gives N parallel `epoll_wait` callers and N parallel slab dispatches,
removing the single-driver bottleneck of legacy mio. The flaw in P1
was the *event delivery model*: each fd was registered on exactly
one worker's epoll, so a CPU-bound owner could trap kernel events
indefinitely.

EPOLLEXCLUSIVE fanout fixes this at the kernel layer. Every fd is
registered on every worker's epoll fd with `EPOLLEXCLUSIVE`. The
kernel wakes exactly one of the currently-`epoll_wait`-blocked
workers per event — by construction, never a busy worker. Idle
workers receive events directly; no user-space "stealing" is needed.

A standalone spike (`/tmp/epoll_exclusive_spike.rs` on
`worktree-io-driver-vtable`) validates the kernel mechanic on Linux
6.17:

| Test | Result |
|---|---|
| 100 events, 1 fd, 4 epolls — exact-once delivery | 100 wakes, 0 double-fires |
| 1 worker spin-busy, 3 in `epoll_wait` | 100/100 events to idle workers, 0 to busy |
| 3-of-4 workers spin-busy, kicker fires once | **58 µs** kick→wake (vs 47 900 µs today) |

## Why P1 readiness-stealing was incomplete

The P1 design (`readiness-stealing.md`) had each idle worker call
`try_steal_pass` *once*, in its pre-park path, before blocking in
`poll.poll(None)` on its own (potentially empty) epoll fd. This
fails when:

1. All fds register on burner-pinned workers (probable when those
   workers process the spawn requests).
2. The lone idle stealer parks on its empty epoll.
3. The kicker fires events on burner-pinned epoll fds.
4. Burners are spinning, never run `epoll_wait`, never call
   `try_steal_pass`, never run `poll_and_dispatch`.
5. Stealer is parked with no kernel signal — its own fd has nothing,
   peer fds have queued events but no in-process notification can
   reach it.
6. Events sit until burners die ~`BURNER_MS` later.

The Tokio Waker layer can't compensate: Wakers are per-task
callbacks fired *by* `poll_and_dispatch`. If no worker runs
`poll_and_dispatch`, no Waker fires.

The fix isn't another user-space wake mechanism — it's letting the
kernel decide who wakes. Which is exactly what `EPOLLEXCLUSIVE` was
designed for.

## Kernel mechanic

`EPOLLEXCLUSIVE` (Linux 4.5+) is a flag on `epoll_ctl(EPOLL_CTL_ADD)`
meaning: "when this fd becomes ready, wake at most one of the
threads currently blocked in `epoll_wait` on an epoll containing
this fd." It exists specifically to avoid thundering-herd wakes when
the same fd is in multiple epoll instances.

We rely on three kernel guarantees:

1. **Fanout works.** A single fd may be added to multiple epoll
   instances; each `EPOLL_CTL_ADD` call registers an independent
   "interest record."
2. **One waiter per edge.** With `EPOLLEXCLUSIVE | EPOLLET`, exactly
   one of the currently-blocked `epoll_wait` callers across all
   epolls containing the fd is woken per state-change. No double-
   fire, no thundering herd.
3. **Busy threads are bypassed.** The kernel only considers threads
   *currently in `epoll_wait`*. A spin-busy worker holding an epoll
   that contains the fd does not receive the event.

Spike test 2 confirms (3) directly: 100 events, worker 0 in a tight
spin loop, workers 1-3 in `epoll_wait`. All 100 events delivered to
1-3, zero to 0.

### Caveat: kernel wake-selection is LIFO, not load-balancing

In spike test 1 (all 4 workers idle, 100 events fired sequentially)
the kernel sent all 100 wakes to a single worker rather than
round-robining. This is consistent with kernel wait-queue LIFO
semantics: the most-recently-blocked waiter wakes first, runs the
event, returns to `epoll_wait`, and is again the most-recent.

For the busy-owner case this doesn't matter (only one waiter is
eligible). For evenly-loaded workloads it means events tend to clump
on whichever worker last entered `epoll_wait` until it falls behind,
at which point another worker takes over. Cache locality could be
either positive (hot fd's events repeatedly hit the same L1) or
negative (no per-fd affinity). Worth measuring; not a blocker.

## Design

### Per-worker shape, preserved

- Each worker owns one `mio::Poll` (epoll fd) and one slab of
  `ScheduledIo`. No global epoll, no global slab.
- Each worker's reactor still drains its own epoll via `poll.poll()`
  in the park path.
- The cross-thread unpark path (per-worker eventfd via
  `mio::Waker`) is unchanged.

### Fanout registration

When a registration request arrives — whether from `register_local`
(synchronous, on-worker first poll) or `apply_register` (off-runtime
fallback) — the request becomes:

1. Allocate the `Arc<ScheduledIo>` and slab key in the
   *registering* worker's slab. Compute the token (see below).
2. Call raw `libc::epoll_ctl(EPOLL_CTL_ADD, EPOLLEXCLUSIVE | EPOLLET
   | <interest>)` on **every worker's epoll fd**, with the same
   token. The mio `Registry::register` API doesn't expose
   `EPOLLEXCLUSIVE`, so we bypass it the same way `steal_dispatch`
   today bypasses mio for raw `epoll_wait`.
3. The fd's `mio::event::Source` integration is unchanged — only
   the epoll-side `EPOLL_CTL_ADD` path differs.

The registering worker is still the owner of the slab entry. The
"owner" concept survives only on the *write* side (allocation,
deregistration). The *read* side (dispatch) becomes uniform.

### Token routing

Today: `Token = (gen << 32) | key`, packed into `usize`. The
dispatching worker assumes the slab is its own.

Proposed: extend the token to include the owner's worker index.

```text
bit 63              32  31     N    N-1   0
+-------------------+----+--------+-------+
|        gen        | -- |  key   | wrkr  |
+-------------------+----+--------+-------+
       32 bits     pad   24 bits  4 bits
```

- `wrkr`: 4 bits — fits today's `MAX_WORKERS = 16`. Reserve a
  sentinel value (e.g. `0xF`) for `WAKER_TOKEN` if needed.
- `key`: 24 bits — 16M slab entries per worker, far above any
  realistic working set.
- `gen`: 32 bits — unchanged.

`WAKER_TOKEN` (currently `usize::MAX`) stays distinct as long as we
reserve at least one bit pattern that no real `(gen, key, wrkr)`
combination produces. `wrkr = 0xF` plus any `key` plus
`gen = 0xFFFFFFFF` is a safe sentinel space.

### Uniform dispatch

`Reactor::poll_and_dispatch` (today: looks up token in own slab)
becomes:

```rust
for ev in events {
    if token == WAKER_TOKEN { continue; }
    let (worker_idx, key, gen) = unpack_token(token);
    let registry = handle.workers[worker_idx].shared_registry.get();
    registry.dispatch_one(key, gen, ev.events);
}
```

Where `dispatch_one` is the per-event critical section currently
inside `steal_dispatch` (slab get, gen check, set readiness, fire
waker). The "steal" framing dissolves: every dispatch is a peer
dispatch in the new model, since events arrive on whichever worker
the kernel chose, which is rarely the registering worker.

### Deregistration

`Drop::deregister` calls `epoll_ctl(EPOLL_CTL_DEL)` on every
worker's epoll fd. The slab entry is freed on the owning worker as
today.

Race: between fanout-add (multi-step) and fanout-del (multi-step),
events for an in-flight registration may arrive on workers that
haven't been added yet. With `EPOLL_CTL_ADD` performed
synchronously on the registering thread (no cross-thread queue),
this is bounded: by the time `register_*` returns, all workers'
epolls have the fd. Concurrent deregistration is the user's
problem (same as today: dropping a `Registration` while another
thread does I/O on its fd is undefined).

### Why synchronous fanout-register, not queued

Today's design has a per-worker `pending_ops` queue used when
`register_*` runs off-worker (e.g. `from_std` from a non-runtime
thread). The off-worker case wakes the target worker via mio
waker, which drains the queue inside `poll_and_dispatch`.

With fanout, every register call must hit every worker's epoll fd
synchronously, so the queue serves no purpose. Each worker's
`Registry` clone is `Send + Sync`, so the calling thread can
issue `epoll_ctl` against any of them. The pending-ops queue and
its drain machinery are deletable.

The same-worker fast path
([`io-driver-vtable.md` "Same-worker register fast path"](./io-driver-vtable.md))
still applies for slab allocation, but the fanout side is
synchronous regardless of which thread is registering.

### What gets deleted

- `ShardedMioHandle::try_steal_pass` and all callers in
  `sharded_mio_park.rs`.
- `SharedRegistry::steal_dispatch`, `SharedRegistry::epoll_fd`,
  `ready_from_epoll_events` (its only caller is `steal_dispatch`).
- `WorkerState::shared_registry: OnceLock<SharedRegistry>` —
  workers can publish their `Registry` clones on the handle for
  fanout-register to use directly. The "shared registry" framing
  collapses to "every worker's registry is reachable from the
  handle."
- `pending_ops: [Mutex<Vec<DriverOp>>; N]` — see
  "synchronous fanout-register" above.
- All `steal_*` counters in `lazy_debug.rs`.
- The cross-thread waker re-fire dance in the steal CAS protocol
  (the `STEALING` lock state is no longer needed because there's
  no cross-worker `epoll_wait` race to coordinate).

The Session 7 per-worker counter arrays (`dispatch_woken_pw`,
`register_per_worker_pw`, `begin_park_calls_pw`) stay — they'll
still be useful for measuring fanout dispatch distribution.

### What gets added

- A new `IoDriverHandle::all_registries() -> &[mio::Registry]`
  accessor (or equivalent) so the fanout-register helper can
  iterate.
- A raw-`epoll_ctl` helper (`fanout_register`,
  `fanout_deregister`) in `sharded_mio_driver.rs`. Mirrors the
  shape of today's raw-`epoll_wait` in `steal_dispatch`.
- Token re-pack with `worker_idx` field. Three call sites:
  `pack_token`, `unpack_token`, `WAKER_TOKEN`.

## Concurrency argument

- **Exact-once delivery** is the kernel's responsibility (spike
  test 1). Edge-triggered + `EPOLLEXCLUSIVE` gives one wake per
  edge, dispatched to one worker.
- **Slab access from peer workers** is already exercised by the
  existing `steal_dispatch` path. The change is that *every*
  dispatch now uses this path, not just steals. The per-event
  critical section (slab `get`, gen check, two atomic loads, one
  waker call) is unchanged.
- **Slab lock contention** is the main thing to watch. Today, only
  the owner ever locks its own slab during dispatch. Under fanout,
  any worker may dispatch any token, so a hot fd's events could
  contend on the owner's slab from N workers. In practice the
  kernel's LIFO wake-selection means most events for a given fd
  hit the same waker thread sequentially, so cross-worker slab
  contention is bounded by wake-selection turnover.

  If profiling later shows this is real, the slab is straightforward
  to refactor to `parking_lot::RwLock` over a `Vec<Slot>` or to a
  per-fd `Mutex` inside each `Slot`. Out of scope for this design.
- **Lifetime / shutdown:** unchanged. Fanout-deregister against a
  closed peer epoll returns `EBADF`; we ignore and continue. mio's
  `Drop for Poll` already orders fd close after the last
  registration.

## Risks

1. **`EPOLLEXCLUSIVE` + `EPOLLET` quirks.** Kernel 4.5 introduced
   `EPOLLEXCLUSIVE`; full edge-triggered semantics under fanout
   were stabilized over subsequent point releases. We test on 6.17
   (host) and 6.1 LTS (target floor — Tokio's MSRV-equivalent for
   io-uring is 6.0). Doc the kernel floor; add a runtime check that
   surfaces a clear error if `EPOLL_CTL_ADD` returns `EINVAL` on a
   kernel that doesn't support the flag.
2. **Mio API bypass.** We're already bypassing mio for raw
   `epoll_wait` in `steal_dispatch`. Adding raw `epoll_ctl` is the
   same shape — same audit story, same Linux-only gate.
3. **Per-fd `EPOLL_CTL_ADD` cost grows N×.** For a 4-worker
   runtime, registration becomes 4 syscalls instead of 1. Each
   syscall is ~1 µs. Negligible vs the per-fd amortized event
   rate. Workloads with churn-heavy short-lived fds (already noted
   as `tcp_connect_churn` in `io-driver-vtable.md`) get this
   amplified, but those workloads already paid for cross-worker
   queue wakes; the per-syscall arithmetic comes out similar or
   better.
4. **LIFO wake-selection vs cache locality.** See spike caveat
   above. Could become a perf delta in workloads with one hot fd
   and many idle workers (the hot fd's events clump on one
   worker, leaving the other workers underutilized). Mitigation if
   needed: rotate the order of registries we add to per fd, so
   different fds end up with different "first waker" preferences.
   Ship without this and add only if a real workload exposes it.
5. **Token bit pressure.** 4 bits for `worker_idx` caps us at 16
   workers. Today's `MAX_WORKERS = 16` matches this exactly; if we
   ever raise the cap we either widen `worker_idx` (steal from
   `key`) or change the packing. Document the constraint at
   `pack_token`.

## Phased rollout

Single phase. The change is atomic at the architectural level:
either every register hits every epoll, or it doesn't. Splitting
into "add fanout, keep stealing" and "remove stealing" creates an
intermediate state where both code paths exist and we don't get
the simplification benefit.

1. **Token re-pack.** Add `worker_idx` field; update `pack_token`,
   `unpack_token`, `WAKER_TOKEN` reservation. Tests still pass
   (no behavior change yet — `worker_idx` is always the
   registering worker).
2. **Fanout register/deregister.** Add `fanout_register` /
   `fanout_deregister` helpers. Switch `register_on_worker` and
   `apply_register` to call them. Tests still pass (events still
   reach the registering worker; peers also receive them but
   their `poll_and_dispatch` doesn't yet route by `worker_idx`).
3. **Uniform dispatch.** Update `Reactor::poll_and_dispatch` to
   route by `worker_idx` from the unpacked token. *Now* peer-
   delivered events dispatch correctly. Bench: `busy_owner_3burners`
   should drop to < 1 ms.
4. **Delete stealing.** Remove `try_steal_pass`, `steal_dispatch`,
   `epoll_fd`, `pending_ops`, `STEALING` state, all `steal_*`
   counters. Pending-ops drain in `poll_and_dispatch` becomes a
   no-op and is removed.
5. **Test refresh.** `tests/rt_sharded_mio.rs` keeps the existing
   7 tests; replace any that exercised the steal path explicitly
   with fanout-direct equivalents. Add a regression test:
   "one fd, 4 workers, registering thread parks; event delivered
   within 1 ms" (the busy-owner case at unit-test scale).

Each step is one commit. Steps 1-3 are the architectural change;
step 4 is the cleanup; step 5 is the test surface.

## Bench gates

- `io_busy_owner.rs`:
  - `sharded_mio/busy_owner_idle` ≤ 1.5× of today (fanout adds
    one extra `epoll_ctl` per registration; the bench registers
    16 fds per iter, so 48 extra syscalls per iter ~ 50 µs over
    the 281 µs baseline = ~17% — fine).
  - `sharded_mio/busy_owner_3burners` < 1 ms (the gate the
    original P1 plan promised). Spike says 58 µs is achievable.
- `tcp_echo_throughput`: no regression. Fanout shouldn't change
  the dispatch hot path; events still go to one waker.
- `tcp_connect_churn`: watch carefully. Per-fd registration cost
  is 4× higher; whether the wake-distribution wins offset the
  syscall cost is empirically determined.

## Testing

- **Loom**: extend `loom_registration.rs` with a fanout model:
  N=2 workers, fd registers on worker 0 with fanout, event
  arrives on worker 1's epoll, dispatch routes back to worker 0's
  slab via `worker_idx`. Invariant: `wake()` count = 1.
- **Stress**: replace `tests/rt_sharded_mio_steal.rs` (if any) with
  `tests/rt_sharded_mio_fanout.rs`. 4-worker runtime, fd registered
  while one worker spins forever, peer thread does the wake.
  Assert `.readable().await` returns within 100 ms (without fanout
  it would block until the burner died).
- **Kernel-version probe**: a `OnceLock<bool>` checked at runtime
  startup that does an `epoll_ctl_add` with `EPOLLEXCLUSIVE` on a
  scratch fd. If `EINVAL`, return a clear error from `Builder::build()`
  rather than letting registrations fail mysteriously later.

## Decision points

1. **Single-phase architectural change vs. incremental?** The doc
   above commits to single-phase (atomic switch). Rationale: the
   intermediate "fanout + still stealing" state has no use case and
   bloats the review. Open to revising if Louis prefers stepwise.
2. **`worker_idx` width.** 4 bits matches today's `MAX_WORKERS = 16`
   exactly. Picking 8 bits gives runway for 256 workers but eats
   into key space. Defaulting to 4 unless there's a reason otherwise.
3. **Slab contention follow-up.** Doc punts the slab-lock RwLock /
   per-Slot Mutex refactor as out of scope. Worth flagging if
   profiling under fanout shows it's the new bottleneck.
4. **Kernel-floor signaling.** Runtime `EINVAL` probe vs build-time
   feature gate. Probe is cheaper to ship; gate is cleaner. Probe
   recommended.

## Open questions

- **Does fanout play with `EPOLLONESHOT`?** Tokio's mio integration
  doesn't use ONESHOT today, but if it ever did, fanout + ONESHOT
  needs separate analysis.
- **Cross-runtime isolation.** Two Tokio runtimes in the same
  process today get separate `mio::Poll` instances, fine. With
  fanout, an fd registered on runtime A would only fan out across
  A's workers. Document that fds belong to one runtime (already
  effectively true).
- **`process` and `signal` integration.** They register fds on
  Tokio's mio Poll today via the same `Registration` interface;
  fanout treats them identically. Verify with `tests/process_*`.
