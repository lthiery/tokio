# Per-Worker io_uring Reactor: fd Placement Policy

**Target file:** `tokio/src/runtime/io/uring_driver.rs` (primary),
                 `tokio/src/runtime/scheduler/multi_thread/uring_park.rs` (secondary)
**Supersedes:** Round-robin fd → worker assignment in `UringHandle::add_source`.
**Status:** Design spec for implementation.
**Related:** [Slab-indexed `OpState` refactor](./reactor-refactor.md)-adjacent; this is the locality follow-up.

---

## 1. Problem

The current `UringHandle::add_source` chooses a worker by round-robin:

```rust
let worker = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
```

This policy optimizes for **ring load balance** — every worker's ring gets ~equal numbers of registrations. It is the wrong variable to optimize for on a work-stealing runtime.

### The Three Worker Identities

For any in-flight I/O, three workers matter:

- **`W_ring`** — the worker whose ring owns the `POLL_ADD_MULTI`. Decided at registration time. Immutable for the registration's lifetime (we don't migrate registrations today).
- **`W_task`** — the worker whose LIFO slot / local queue the woken task will land in. Decided by the scheduler when the waker executes; may change over the task's lifetime as the scheduler migrates it.
- **`W_drain`** — the worker currently in `drain_completions`. Always equal to `W_ring` by construction.

### Cost Structure

| Condition | Cost per wake |
|---|---|
| `W_drain == W_task` (task wakes on the draining worker) | Push onto local LIFO. Zero syscalls. |
| `W_drain ≠ W_task`, target worker **busy** | Atomic push to remote queue. Zero syscalls (no unpark needed). |
| `W_drain ≠ W_task`, target worker **parked** | Atomic push + `UringUnparker::unpark` → `MSG_RING` SQE + `submit()` → one `io_uring_enter` syscall. |

The third case is the expensive one. At ~500ns per syscall, it costs ~1.5% CPU per 30k wakes/sec — but more importantly, it serializes a park→unpark round-trip that adds latency to the woken task.

### Round-Robin's Failure Mode

With round-robin placement and N workers:

- A newly-registered fd has a `1/N` chance that `W_ring == W_task` where `W_task` is the *current* worker executing `add_source`.
- As workers migrate the task later, that probability changes but there's no correlation — round-robin placement is independent of task locality, so the hit rate stays at `1/N` on average.

For `N=4`, we're cross-worker 75% of the time on the hot path.

### Empirical Evidence

From `tokio/tests/net_uring_bench.rs` (median of 3 runs, round-trips/sec):

| Scenario | mio median | uring median | Δ |
|---|---:|---:|---:|
| `small_msgs_few_clients` (4w × 8c × 10000 × 64B) | 126,844 | 116,198 | **−8.4%** |
| `small_msgs_many_clients` (4w × 64c × 2000 × 64B) | 136,652 | 152,838 | +11.9% |
| `single_worker_saturation` (1w × 16c × 4000 × 256B) | 53,670 | 53,953 | ±0% |

At low fan-out (8 connections across 4 workers), the mismatch dominates. At high fan-out, statistical averaging hides it: there's always some local task to run.

Run-to-run variance was also telling: uring's `small_msgs_few_clients` spread was 116k/137k/114k (±10%), while mio's was 125k/127k/128k (±1%). High variance with static workload ≡ scheduling-dependent behavior ≡ fd-to-worker mapping is the confounder.

## 2. Design Goals

1. **Prefer `W_ring == W_task`** for the common case — the worker polling the fd should own the registration.
2. **Zero per-round-trip cost.** Placement is decided at most once per registration (not per poll).
3. **Correct under task migration.** If a task migrates from `W_a` to `W_b` after registration, we don't re-register by default; the cost model degrades to the *old* cross-worker case, not worse.
4. **Correct when registration happens off-worker** (e.g., from a `spawn_blocking` thread, or from `Runtime::block_on` on the main thread): fall back to a reasonable policy without panicking.
5. **Zero changes to the scheduler.** All logic lives in `UringHandle::add_source` and a small amount of TLS plumbing.

## 3. Design

### 3.1 `CURRENT_WORKER` thread-local

Add to `uring_park.rs`:

```rust
use std::cell::Cell;

thread_local! {
    /// Set by `UringParker::park_internal` (and cleared on exit) to the
    /// current worker's index. Used by `UringHandle::add_source` to pick
    /// the locality-preferred worker for a new fd registration.
    ///
    /// `None` means we are not on a worker thread (external callers,
    /// `spawn_blocking` threads, the main thread during `block_on`
    /// initialization, etc.) — callers must fall back.
    static CURRENT_WORKER: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(crate) fn current_worker_index() -> Option<usize> {
    CURRENT_WORKER.with(|c| c.get())
}
```

Lifecycle: set on worker-thread entry (alongside the existing `LOCAL_REACTOR` TLS install in `ensure_reactor_installed`), cleared on drop (piggybacks on the existing `ClearUringTls` RAII guard in `worker.rs`).

Note: this is **distinct from** the existing `LOCAL_REACTOR` TLS. `LOCAL_REACTOR` points to *this worker's* `RefCell<Reactor>` for MSG_RING sending; `CURRENT_WORKER` is just the integer index, usable for routing decisions that don't involve the reactor itself.

### 3.2 `UringHandle::add_source` placement policy

Replace the current body:

```rust
pub(crate) fn add_source(&self, fd: RawFd, interest: Interest, io: Arc<ScheduledIo>) {
    let worker = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
    self.push_pending_op(worker, PendingOp::Register { fd, interest, io });
    self.maybe_wake_worker(worker);
}
```

with:

```rust
pub(crate) fn add_source(&self, fd: RawFd, interest: Interest, io: Arc<ScheduledIo>) {
    let worker = match current_worker_index() {
        Some(idx) if idx < self.workers.len() => idx,
        _ => self.fallback_worker(),
    };
    self.push_pending_op(worker, PendingOp::Register { fd, interest, io });
    self.maybe_wake_worker(worker);
}

/// Placement fallback when `add_source` is called off-worker.
/// Round-robin over `next_worker` — same behavior as the old policy, but
/// only for the genuinely off-worker minority of calls.
fn fallback_worker(&self) -> usize {
    self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
}
```

**Key observation:** We *keep* `next_worker` around for the fallback path — it's still the right answer when we have no locality hint. We just stop using it as the primary policy.

### 3.3 Fast-path short-circuit: same-ring wakes

Independent of placement, we can short-circuit the common case where the task's wake target is the same worker that's draining CQEs. In `Reactor::drain_completions`, when we call `io.wake(ready)`:

- `wake()` invokes the stored waker, which calls into the scheduler's `schedule()` path.
- If the scheduler's schedule path can detect "we're running on the target worker" (via the existing `CURRENT` worker TLS that tokio already maintains for its own scheduler), it pushes to local LIFO and skips `unpark` entirely.

Tokio's scheduler already does this for its own `Notified::schedule` path. The question to verify: does the `UringUnparker::unpark` path short-circuit when `current_worker_index() == self.idx`? If not, it should:

```rust
impl UringUnparker {
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        if current_worker_index() == Some(self.idx) {
            // We're already running on the target worker. Wake is
            // unnecessary — the current task will yield soon and the
            // worker loop will pick up whatever was just pushed.
            return;
        }
        self.handle.unpark(self.idx);
    }
}
```

This is defense-in-depth: placement should make `W_drain == W_task` the common case, but when the scheduler migrates a task, `wake` may still originate on the target worker, and we shouldn't pay a MSG_RING round-trip to wake ourselves.

### 3.4 Migration handling: do nothing (v1)

If a task registers its fd on `W_1`, then migrates to `W_2`, every wake goes:

- CQE on `W_1`'s ring.
- `W_1` calls `io.wake()`.
- Scheduler pushes to `W_2`'s queue.
- If `W_2` parked → MSG_RING from `W_1` to `W_2`.

This is one cross-worker hop per CQE, **but the task is local to itself** — it's only paying the old-placement cost. It does not regress worse than round-robin.

Tokio's work-stealing scheduler does not migrate tasks aggressively; migration happens during steal operations and on explicit spawn placement. For long-lived TCP connections, tasks typically stay on their home worker. So we expect migrations to be rare and amortizable.

If profiling later reveals frequent migrations are an issue, add **lazy re-registration on migration** as v2:

- In `PollEvented::poll_read` / `poll_write`, if `current_worker_index() != self.registered_on`, queue a `Deregister` + `Register` pair to rebind the fd to the current worker.
- Cost: one pair of ring SQEs per migration. Win: permanent locality after the first poll on the new worker.
- Requires `ScheduledIo` to carry `registered_on: u16` (or similar) alongside the slab key.

Defer to v2. Gate behind profiling evidence.

## 4. Correctness Considerations

### 4.1 Reading TLS before install

`add_source` can be called at runtime startup before any worker has entered its park loop — e.g., if `runtime::Builder::build()` includes a `TcpListener::bind` call internally (it doesn't today, but the invariant matters). `CURRENT_WORKER` will be `None` and we fall back to round-robin. No panic, no pick-wrong-worker.

### 4.2 External callers

Callers from non-worker threads (other runtimes, `std::thread::spawn`ed code that constructs a `TcpStream` then calls `Runtime::block_on`) have `CURRENT_WORKER == None` on that thread. Fallback applies. Same correctness as today.

### 4.3 Worker index shadowing

Two different runtimes each with 4 workers could both set `CURRENT_WORKER` on shared blocking-pool threads. The existing `ClearUringTls` guard (from the prior session's fix for cross-runtime TLS leak on recycled threads) already handles this — but only for the reactor pointer. We need to extend it to clear `CURRENT_WORKER` too:

```rust
// in worker.rs
impl Drop for ClearUringTls {
    fn drop(&mut self) {
        crate::runtime::io::uring_driver::clear_local_reactor();
        crate::runtime::scheduler::multi_thread::uring_park::clear_current_worker();
    }
}
```

### 4.4 `fd: RawFd` from a `Registration::new_with_interest_and_handle` called while already polling

This is the common case. The poll is running on the worker's task loop, `CURRENT_WORKER` is set, we pick that worker. The fd will be dispatched back to the same worker that's about to `.await` on it. Hit.

### 4.5 Listener/accept pattern

```rust
let listener = TcpListener::bind(addr).await?;
loop {
    let (sock, _) = listener.accept().await?;
    tokio::spawn(async move { handle(sock).await });
}
```

The listener fd is registered on the accept loop's worker, say `W_0`. Every accepted socket is registered synchronously inside the accept loop, so also on `W_0`. Then `tokio::spawn` may distribute the task across workers.

Result: all accepted-socket fds land on `W_0`; tasks spread. **This is the worst case for our policy** — we get `W_ring == W_0`, `W_task` uniform across all workers, so cross-worker fraction = `(N-1)/N` (the same as round-robin's average).

Mitigation: this is exactly the shape that `v2 lazy re-registration` solves — once the spawned task runs on `W_k`, its first poll moves the fd registration to `W_k`. After one rebind per connection, steady state is local.

For v1, accept loops in high-throughput servers will still show the regression at low fan-out. But this is not worse than today; at worst we perform as well as round-robin in this specific pattern.

## 5. Migration Plan

Single PR, four small commits:

1. **TLS plumbing.** Add `CURRENT_WORKER` cell, setter in `UringParker::park_internal`, clear-helper, extend `ClearUringTls` drop.
2. **Placement policy switch.** Rewrite `UringHandle::add_source` to consult TLS first, fall back to round-robin.
3. **Unpark short-circuit.** Check `CURRENT_WORKER` in `UringUnparker::unpark`.
4. **Benchmark update.** Re-run `net_uring_bench` under both backends; expected outcomes documented below.

## 6. Expected Outcomes

Predicted deltas after landing (relative to current uring-reactor numbers):

| Scenario | Current vs mio | Predicted vs mio | Rationale |
|---|---:|---:|---|
| `single_worker_saturation` | ±0% | ±0% | No cross-worker ops possible; no change |
| `large_msgs_many_clients` | +4.7% | +4.7% to +8% | Mostly I/O-bound; small locality win |
| `many_workers_low_concurrency` | +10.4% | +12% to +15% | Already winning; locality tightens the loop |
| `small_msgs_many_clients` | +11.9% | +14% to +18% | High fan-out amplifies locality benefit |
| `small_msgs_few_clients` | **−8.4%** | **+0% to +5%** | **The regression case; should flip** |

Signal-to-noise: the test that most needs to move is `small_msgs_few_clients`. If that swings from −8% to around parity or slight positive, the hypothesis is confirmed and we can move on. If it stays negative, the cost is elsewhere (more profiling needed — strace for syscall count, perf for hot functions).

Variance should also tighten: the 20% run-to-run spread at low fan-out should drop to <5%, matching mio's consistency.

## 7. Open Questions

1. **Does tokio's existing scheduler already expose a "current worker" TLS we can reuse?** The `multi_thread::worker::Context` has worker-identity state; check if it's accessible from the I/O driver without a layering violation. If yes, use that and skip our own TLS.

2. **Should the fallback policy be something smarter than round-robin?** E.g., "least-loaded worker" using `handle.pending_op_count(idx)`. Probably not worth it for a minority-path fallback, but worth noting.

3. **Listener special-casing.** Worth adding an explicit `UringHandle::add_source_accepted(fd, parent_fd_worker)` that lets the caller hint "this came from an accept on a listener on worker W, distribute differently"? Probably yes, as part of v2 lazy-re-registration work, not v1.

4. **Is `MSG_RING` actually the dominant cost, or is it the unpark/park transition itself?** `strace -c` on `small_msgs_few_clients` pre- and post-fix will tell us. If post-fix the syscall count drops but the benchmark doesn't move much, the cost is elsewhere (likely scheduler contention on the remote queue's atomics).

## 8. Summary

We built a per-worker reactor — correct — then placed fds with a policy that ignored task locality — wrong. The fix is small, localized, and preserves all existing correctness properties while predictably eliminating the measured regression.

The broader lesson: **on a work-stealing runtime, per-worker resources must follow scheduler locality decisions, not impose their own.** The scheduler owns "where the task runs"; the reactor should ask, not dictate.

— End —
