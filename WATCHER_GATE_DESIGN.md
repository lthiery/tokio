# Sharded-mio meta-watcher gate — design doc

**Branch:** `worktree-io-driver-vtable` at `fede15a6` (after the simplify cleanup).
**Goal:** Close the residual `busy_owner_idle` gap so sharded-mio matches or beats the traditional I/O driver in **all** `io_busy_owner` benches.

---

## 1. Problem statement

Current bench landscape (`io_busy_owner.rs`, 4 workers, 16 awaiters):

| Bench | traditional | sharded_mio | gap |
|---|---|---|---|
| `busy_owner_idle`     | ~289 µs | ~315 µs | **+9% (sharded loses)** |
| `busy_owner_3burners` | ~333 µs | ~271 µs | -19% (sharded wins) |

`perf stat` localized the residual `idle` gap to **kernel-side syscall overhead**:

- sys time **+38%**
- context-switches **+14.8%**
- cycles / instructions roughly flat

**Root cause:** in sharded-mio, all 4 workers issue `epoll_wait` syscalls when parking. In the traditional driver, **1 worker** issues `epoll_wait` and the other 3 thread-park on a futex (much cheaper park + cheaper unpark). With 4 workers parked while one fd produces events, sharded-mio pays `4 × epoll_wait` overhead vs traditional's `1 × epoll_wait + 3 × futex`.

The architectural fix is to add a **meta-watcher gate**: at most one worker at a time blocks in `epoll_wait` on the meta-epoll fd; the rest thread-park on a futex.

---

## 2. Architecture

### 2.1 Park-time decision

When a worker enters `sharded_mio_park::park_timeout`:

1. Try-CAS-acquire the runtime-wide **meta-watcher slot** (`ShardedMioHandle::meta_watcher_busy`, scaffolding already exists as `try_acquire_meta_watcher` returning `MetaWatcherGuard`).
2. **If acquired:** become the meta-watcher. Block in `epoll_wait` on `meta_epfd` with the requested timeout. On wake, dispatch (see §2.2), then release the guard.
3. **If not acquired:** thread-park on `std::thread::park_timeout` with the requested timeout. On wake, return; the caller will re-poll work and may try-acquire on its next park.

### 2.2 Meta-watcher dispatch

`epoll_wait` on `meta_epfd` returns one or more `epoll_event` entries; each carries a `worker_idx` in `data.u64` identifying which child epoll fired. For each event:

- **`worker_idx == self.idx`** (our own child fired): the most common case is our `external_waker` eventfd, or one of our owned fds. Drain via `reactor.park_timeout(Duration::ZERO)` — same path as the existing owner-side dispatch.
- **`worker_idx != self.idx`** (peer's child fired):
  1. Resolve the peer's `SharedRegistry` from `ShardedMioHandle::workers[worker_idx].shared_registry`.
  2. Call `try_steal_drain` (already exists in `sharded_mio_reactor.rs`) — drains events from the peer's child epoll without contending the peer's slab lock for an extended window. The drain pushes ready tasks into the scheduler via `io.wake(ready)`.
  3. **Fan out to stake-holders:** `swap(0)` the peer's `interested_workers: AtomicU64` bitset. For each set bit `i`, unpark `workers[i]`. (`interested_workers` is the substrate from the spike — see §3.)
  4. **Unpark the owner peer** itself if `park_state == PARKED`, so it resumes into user code after its tasks were woken.

After processing all events, drop the `MetaWatcherGuard` (releases the watcher slot); the next worker that parks will try-acquire it.

### 2.3 Unpark dispatch — two mechanisms, unified entry

A worker can be in one of two park modes; unpark must work for either:

| Mode | Wake mechanism |
|---|---|
| Meta-watcher (`epoll_wait` on meta_epfd) | mio `Waker` eventfd write to its **own** child epoll → propagates up to meta-epoll |
| Thread-parker (`thread::park`) | `std::thread::Thread::unpark` |

**Unified contract:** every worker stashes its `std::thread::Thread` handle in a new `WorkerState::park_thread: OnceLock<Thread>` field at startup (set once when the scheduler first parks; idempotent). The unpark path **always** does both:

1. `external_waker.wake()` — wakes the worker if it's the meta-watcher (eventfd → epoll).
2. `Thread::unpark()` on the stored thread handle — wakes the worker if it's thread-parked.

`Thread::unpark` is idempotent and cross-thread-safe; calling it on a non-parked thread is a no-op (one token is buffered). The eventfd write is similarly idempotent (mio `Waker` collapses redundant wakes). So calling both unconditionally is correct and avoids a mode-check race.

The existing `park_state: AtomicUsize` (EMPTY/PARKED/NOTIFIED) state machine continues to gate redundant unparks at the application level; both paths transition `PARKED → NOTIFIED` via CAS before issuing the wake.

---

## 3. Substrate already in place ("spike scaffolding")

These have been authored across `97cea2ac` ("meta-watcher gate spike") and the working-tree extensions; they currently sit in the working tree as uncommitted changes:

- **`ShardedMioHandle`** (`sharded_mio_driver.rs`):
  - `meta_epfd: RawFd` — runtime-wide meta epoll fd (Linux). Each worker's child epoll fd is added with `data.u64 = worker_idx` during `register_worker`.
  - `meta_watcher_busy: AtomicBool` + `try_acquire_meta_watcher` → `Option<MetaWatcherGuard>` (already implemented; currently `#[allow(dead_code)]`).
  - `MetaWatcherGuard` — RAII guard tied to `Arc<ShardedMioHandle>`; `Drop` releases the slot.
- **`WorkerState`** (`sharded_mio_driver.rs`):
  - `interested_workers: AtomicU64` — bitset of peers who registered a `Waker` on a `ScheduledIo` owned by this worker. Bit `i` ⇒ worker `i` has stake.
  - `record_owner_interest(peer_idx: usize)` — sets bit `peer_idx`.
  - `take_interested_workers() -> u64` — atomic `swap(0)`.
- **TLS** (`sharded_mio_driver.rs`):
  - `LOCAL_HANDLE` — TLS slot holding a raw pointer to the worker's `Arc<ShardedMioHandle>`. Set via `install_local_handle_raw` / cleared via `clear_local_handle` in the parker's setup/teardown. Read via `with_local_handle`.
  - `CURRENT_WORKER` — TLS holding the polling worker's index.
- **`scheduled_io.rs`:**
  - `record_sharded_mio_stake(scheduled_io)` is called from 3 sites where a peer registers a `Waker` on a `ScheduledIo` it doesn't own. It uses `LOCAL_HANDLE` + the `ScheduledIo`'s owner-token to call `record_owner_interest` on the owner's `WorkerState`.
- **`sharded_mio_park.rs`:**
  - `LOCAL_HANDLE` install/clear sites in `ensure_reactor_installed` / `Drop` / shutdown.

**The watcher-gate logic is the only missing piece.** The substrate exists; it's currently dead code (`#[allow(dead_code)]` on the guard, and the `interested_workers` bitset is populated but never consumed by the park path).

---

## 4. Files to touch

| File | Change |
|---|---|
| `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs` | **Largest change.** Park entry point: try-acquire watcher → meta-park or thread-park. Add `park_thread: OnceLock<Thread>` initialization at first park. Wire the meta-watcher dispatch loop. |
| `tokio/src/runtime/io/sharded_mio_driver.rs` | Add `park_thread: OnceLock<Thread>` field on `WorkerState`. Possibly add a helper `unpark_worker(idx)` that does both `external_waker.wake()` + `Thread::unpark()`. Remove `#[allow(dead_code)]` from `try_acquire_meta_watcher` and `interested_workers`. |
| `tokio/src/runtime/io/sharded_mio_reactor.rs` | Verify `try_steal_drain` covers the peer-fanout path; expose a helper `dispatch_peer_event(worker_idx)` if needed. |
| `tokio/src/runtime/io/scheduled_io.rs` | No changes — `record_sharded_mio_stake` already populates `interested_workers`. |

Optionally split out a new module `sharded_mio_meta_watcher.rs` if `sharded_mio_park.rs` grows past ~600 lines. Decide based on what's cleaner.

---

## 5. Correctness invariants

1. **No lost wakeups.** Every `unpark(W)` posted while `W` is parked (in either mode) must wake `W` exactly once before its next return from park. Achieved by:
   - `park_state` CAS `EMPTY → PARKED` *before* the kernel park call (publishes-the-handle ordering).
   - Unparker CAS `PARKED → NOTIFIED` *before* issuing the wake; if the unparker observes `EMPTY`, it stores `NOTIFIED` instead so the next park returns immediately.
   - Both wake mechanisms (`external_waker.wake()` + `Thread::unpark()`) are issued on every unpark, so it doesn't matter which mode the parkee was in.

2. **No lost peer-events.** When the meta-watcher consumes an event for `worker_idx == k`, it must drain `k`'s child epoll *before* releasing the watcher slot, otherwise an event is "owned" by the watcher's epoll-wait return but never dispatched. `try_steal_drain` already handles this.

3. **No deadlock on watcher slot.** If all N workers are parked and a wake arrives, at least one of them must end up dispatching it. Holds because: (a) at most one is the meta-watcher, who will wake from epoll_wait directly; (b) thread-parked workers are woken via `Thread::unpark`; (c) the meta-watcher releases the slot before returning to user code, so the next park can re-acquire.

4. **Watcher rotation, not starvation.** Watcher releases the slot on every wake, so any worker that parks afterward can become the next watcher.

5. **Timeout fidelity.** `park_timeout(d)` must wake within `d ± epsilon` regardless of mode. Both `epoll_wait(timeout_ms)` and `Thread::park_timeout(d)` honor this.

6. **Drop safety.** `MetaWatcherGuard` releases via `Drop` even on panic.

7. **Scheduler shutdown.** Watcher must release on runtime shutdown so the next worker's drop path doesn't deadlock waiting on the gate.

---

## 6. Risks & mitigations

- **3burners regression risk.** With burners, every worker has events to dispatch. Funneling to a single meta-watcher introduces a one-to-many wake fanout that's more expensive than per-worker direct dispatch. **Mitigation:** the meta-watcher only fans out wakes — it doesn't run any peer's user code. Stake-holders are unparked and dispatch their own work locally. The existing 3burners win comes from cache locality (events.is_empty + per-worker slabs); that locality is preserved.
- **Wake-storm on first event.** Bench warmup may produce a thundering herd if all workers acquire the watcher slot simultaneously. **Mitigation:** CAS-acquire serializes naturally; losers thread-park.
- **Thread::unpark token saturation.** `unpark` buffers exactly one token. If the unparker fires twice between parks, the second is lost — but `park_state == NOTIFIED` already covers this via the application-level state machine (the CAS path bails out when already NOTIFIED).
- **Meta-epoll fd added but never consumed.** Currently each worker's child epoll is added to `meta_epfd` at `register_worker` time; if the watcher path is broken, `meta_epfd` is just dead state. Confirm registrations are well-formed before touching the watcher.

**Abandon criteria:** if `busy_owner_3burners` regresses by **>10%** after the gate lands, either gate the gate behind a heuristic (e.g. disable when `dispatch_woken_events_total / dispatch_calls > N`) or back out.

---

## 7. Implementation order

1. **Read-pass** the spike scaffolding to understand the substrate (`MetaWatcherGuard`, `interested_workers`, `LOCAL_HANDLE`, `record_sharded_mio_stake`). Don't modify yet.
2. Add `park_thread: OnceLock<Thread>` field on `WorkerState` + initialization in `sharded_mio_park` first-park hook.
3. Add `unpark_worker(idx)` helper on `ShardedMioHandle` that does both `external_waker.wake()` + `Thread::unpark()` (idempotent, both-or-neither paths).
4. Replace **all** existing `external_waker.wake()` call sites in the unpark path with `unpark_worker(idx)`.
5. Modify `sharded_mio_park::park_timeout`:
   - Try-acquire `meta_watcher_busy`.
   - **Yes:** call new `meta_watcher_park(timeout)` method on the parker.
   - **No:** call `Thread::park_timeout(timeout)`.
6. Implement `meta_watcher_park`: epoll_wait on `meta_epfd`, dispatch each event (own-child path = existing reactor drain; peer-child path = `try_steal_drain` + fanout via `interested_workers` + `unpark_worker`).
7. Build, test, bench.
8. If 3burners regresses, add a heuristic gate or back out.

---

## 8. Verification

```bash
# Build (worktree at /home/louis/tokio/.claude/worktrees/io-driver-vtable):
source ~/.cargo/env
RUSTFLAGS='--cfg tokio_unstable' cargo build -p tokio --features "rt,rt-multi-thread,net,io-sharded-mio"

# Tests:
RUSTFLAGS='--cfg tokio_unstable' cargo test -p tokio --features "rt,rt-multi-thread,net,io-sharded-mio" --lib runtime::io

# Bench (quick mode for iteration):
RUSTFLAGS='--cfg tokio_unstable' cargo bench -p benches --bench io_busy_owner --features "bench-sharded-mio" -- --quick 'busy_owner_idle|busy_owner_3burners'

# Bench (full, for final verification):
RUSTFLAGS='--cfg tokio_unstable' cargo bench -p benches --bench io_busy_owner --features "bench-sharded-mio" -- 'busy_owner_idle|busy_owner_3burners' --sample-size 50

# Optional: perf stat A/B (run benches under `perf stat -e task-clock,context-switches,cycles,instructions,cache-misses` and compare sys time / context-switches between traditional and sharded).
```

**Success:**
- `sharded_mio/busy_owner_idle` ≤ `traditional/busy_owner_idle` (target: ≤ 289 µs).
- `sharded_mio/busy_owner_3burners` stays under traditional (≤ 333 µs; ideally still ≤ 280 µs).
- All 5 `runtime::io::*` tests pass.
- No new flakiness in `cargo test --workspace`.

---

## 9. Out-of-scope

- Reducing the number of `epoll_wait` calls below 1 (e.g. busy-spin watcher) — explicitly not desired; we want kernel parking when idle.
- Refactoring `try_steal_drain` — leave alone unless it actively blocks the gate.
- Changing the slab / `OpsState` representation — orthogonal.
- Touching the uring driver — completely separate code path.
- Cleanup of unrelated `#[allow(dead_code)]` markers — only remove ones that the gate now consumes.
