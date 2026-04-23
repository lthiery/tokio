# Per-Worker `io_uring` Reactor — Design Overview

**Feature:** `io-uring-reactor` (experimental, requires `tokio_unstable`, Linux-only)
**Scope:** The entire `io-uring-reactor` branch.
**Companion doc:** [Slab-indexed OpState refactor](./uring-opstate-slab.md) — the reactor's `user_data` / Arc-lifetime scheme.

This document describes the complete architecture of the per-worker `io_uring` reactor: what it is, why each piece exists, how the pieces compose, and what remains out of scope for v1.

---

## 1. Goals & Non-Goals

### Goals

- **Multicore I/O with distributed state.** Each worker thread owns its own `io_uring` instance and its own timer wheel. No shared driver, no global lock contention on the I/O hot path.
- **Readiness-model parity with the existing mio driver.** `TcpStream`, `UdpSocket`, `UnixStream`, `Registration`-backed user types, pipes — all continue to work unchanged. No API changes visible to callers.
- **Experimental, opt-in.** Gated behind `feature = "io-uring-reactor"`, `tokio_unstable`, and `target_os = "linux"`. Zero impact on default builds.
- **Low per-registration overhead.** `POLL_ADD_MULTI` keeps readiness monitoring amortized; no per-event re-arm.
- **Safe kernel handshakes.** No pointer-in-`user_data` exposed to the kernel; no time-based retention guesses. `Arc<ScheduledIo>` lifetimes are bounded by explicit kernel CQE signals.

### Non-Goals (v1)

- **Completion-model ops** — `RECV_MULTI`, `SEND_ZC`, `ACCEPT_MULTI`, fixed-buffer I/O. These layer on top of a completion-aware `Registration` replacement; deferred to v2.
- **CQE stealing across rings.** Each ring drains only its own completions. No work-stealing at the CQE level.
- **Kernel-version flexibility.** Minimum Linux 6.0 (for mature `DEFER_TASKRUN`). Older kernels fall back to mio at build-time feature gate.
- **Current-thread runtime support.** Only `rt-multi-thread`. The per-worker model doesn't meaningfully apply to single-threaded runtimes; those continue using mio.

## 2. Architectural Overview

```
      ┌──────────────────────────────────────────────────────────────────┐
      │                     Runtime::Builder::build()                    │
      │               (with .enable_uring_reactor() set)                 │
      └──────────────────────────────────┬───────────────────────────────┘
                                         │
                                         ▼
      ┌──────────────────────────────────────────────────────────────────┐
      │               Arc<UringHandle>  ·  shared across workers          │
      │                                                                   │
      │   workers: [WorkerSlot { ring_fd, external_waker }; N]            │
      │   pending_ops: [Mutex<Vec<PendingOp>>; N]                         │
      │   next_worker: AtomicUsize    (round-robin assignment)            │
      │   registrations: RegistrationSet  (shared ScheduledIo storage)    │
      └──────────┬───────────────────────────┬───────────────┬───────────┘
                 │                           │               │
        ┌────────▼────────┐       ┌──────────▼──────┐  ┌─────▼────────┐
        │  Worker 0       │       │  Worker 1       │  │   Worker N-1 │
        │                 │       │                 │  │              │
        │  UringParker ───┼───► LOCAL_REACTOR ►─────┼──┼── ...        │
        │   └─ Reactor    │       │   └─ Reactor    │  │   └─ Reactor │
        │       └─ IoUring│       │       └─ IoUring│  │       └─ IoUring
        │       └─ Slab   │       │       └─ Slab   │  │       └─ Slab
        │       └─ eventfd│       │       └─ eventfd│  │       └─ eventfd
        └─────────────────┘       └─────────────────┘  └──────────────┘
                 ▲                           ▲                  ▲
                 └───── MSG_RING cross-wake ─┴──────────────────┘
                     (peer workers send SQEs onto each other's rings)
```

### The three worker-owned singletons

Per worker, exactly once:
1. **`IoUring`** — submission + completion queues. `IORING_SETUP_SINGLE_ISSUER` binds the submitter identity on first `io_uring_enter`, so the ring **must** be constructed on the thread that will drive it. This is why `Reactor::new` is called lazily from `UringParker::park_internal` the first time the worker parks, rather than eagerly during runtime startup.
2. **`Slab<SlotEntry>`** — in-flight op table. `user_data` sent to the kernel is a 64-bit `(variant, gen, key)` triple; `key` is the slab index.
3. **Eventfd** — external-thread wake mechanism. Registered with `POLL_ADD_MULTI` on the worker's own ring. Writes from non-worker threads produce a CQE that unblocks the park.

### The shared singleton

One `Arc<UringHandle>` per runtime, shared across all workers and the I/O driver. Holds:
- Per-worker ring_fd and external_waker publication slots (populated on worker lazy-init).
- Per-worker pending-op queues for cross-worker registration requests.
- Round-robin fallback counter for placement when no locality hint is available.
- The existing `RegistrationSet` (shared `ScheduledIo` storage — unchanged from the mio path).

## 3. Feature Gating

The build surface is strict:

```toml
# tokio/Cargo.toml
io-uring-reactor = [
    "rt",                 # must have an async runtime
    "rt-multi-thread",    # per-worker design requires multiple workers
    "time",               # per-worker timer wheel integration
    "net",                # the only I/O types that benefit today
    "dep:io-uring",
    "libc",
    "dep:slab",
]
```

Additionally, every uring-specific code path is gated by:

```rust
#[cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt-multi-thread",
    target_os = "linux",
))]
```

This four-way gate appears uniformly across:
- `src/runtime/io/uring_reactor.rs` (module itself)
- `src/runtime/io/uring_driver.rs`
- `src/runtime/scheduler/multi_thread/uring_park.rs`
- Feature-gated branches in `worker.rs`, `scheduler/mod.rs`, `multi_thread/handle.rs`
- Integration tests `tests/net_uring_reactor_tcp.rs`, `tests/net_uring_bench.rs`

## 4. `user_data` Encoding: Slab-Indexed

See [uring-opstate-slab.md](./uring-opstate-slab.md) for the full rationale. Summary:

```
bit 63       bit 56        bit 32                    bit 0
 │            │             │                         │
 ├────────────┼─────────────┼─────────────────────────┤
 │  variant   │     gen     │           key           │
 │  (8 bit)   │   (24 bit)  │        (32 bit)         │
 └────────────┴─────────────┴─────────────────────────┘
```

- **`key`** indexes into the per-reactor `Slab<SlotEntry>`.
- **`gen`** is a 24-bit monotonic counter bumped on every insert. Stale CQEs that arrive after a slot has been recycled are detected by generation mismatch and dropped.
- **`variant`** is a fast discriminator: `0x00 = PollMulti`, `0x01 = Control`, `0x02 = Eventfd`, `0x03 = MsgRingIncoming`.

### Why not pointers

Earlier iterations encoded `user_data` as the exposed pointer of the `ScheduledIo`. That required:
- A per-`POLL_REMOVE` Arc-retention pipeline to keep the `Arc<ScheduledIo>` alive until the kernel stopped referencing it.
- A time-based guess at how long the kernel's CQE-arrival window was after cancellation.
- A forbidden sentinel range in the upper `user_data` bits to distinguish pointers from control ops.

The slab scheme:
- The kernel never sees a pointer; pointer-reuse races are structurally impossible.
- The `Arc<ScheduledIo>` is held **inside the slab entry** and dropped exactly when the terminal CQE for that slot arrives (no `IORING_CQE_F_MORE`). This is a kernel-provided handshake, not a time-based guess.
- Memory scales with active registrations, not with park cadence.
- `unsafe` is eliminated from the drain hot path (slab index is a safe operation; pointer expose/unexpose is not).

### Well-known slots

Two slots are pre-allocated at `Reactor::new`:

- **`KEY_EVENTFD = 0`** — the eventfd `POLL_ADD_MULTI` registration. Reactor-lifetime; never removed.
- **`KEY_MSG_RING_INCOMING = 1`** — the incoming-MSG_RING slot. The slot itself has no kernel registration; peers encode its `user_data` into their own `MsgRingData` SQEs. Because generation 0 is constant for this slot, the peer encoding `MSG_RING_INCOMING_UD = encode(0x03, 0, 1)` is a universal constant — **no per-peer advertisement is needed.**

## 5. Registration Lifecycle

End-to-end flow for `TcpStream::connect`:

```
TcpStream::connect(addr).await
  │
  ├─ mio::net::TcpStream::connect              # still uses mio for the fd
  │
  └─ PollEvented::new(mio_sock)
       │
       └─ Registration::new_with_interest_and_handle(...)
            │
            └─ Handle::add_source(fd, interest, scheduled_io)
                 │
                 │  (uring path — gated on runtime flavor)
                 │
                 └─ UringHandle::add_source(fd, interest, io)
                      │
                      ├─ pick target worker  (round-robin today, locality in v2)
                      ├─ push PendingOp::Register onto worker[idx].pending_ops
                      └─ wake worker[idx]  (if parked)
                           │
                           ▼
                      worker thread's UringParker::park_internal
                           │
                           ├─ apply_pending_ops(reactor, pending)
                           │    │
                           │    └─ reactor.register(fd, interest, &io)
                           │         │
                           │         ├─ ops.insert(PollMulti { io: io.clone(), ... })
                           │         ├─ stamp uring_slab_key onto ScheduledIo
                           │         └─ push PollAdd.multi(true).user_data(encoded)
                           │
                           └─ submit_and_wait(1)
```

### `RegistrationSource` trait

`PollEvented<E>`'s bound was widened from `E: mio::event::Source` to `E: RegistrationSource`:

```rust
pub trait RegistrationSource: mio::event::Source {
    #[cfg(all(tokio_unstable, feature = "io-uring-reactor", ...))]
    fn registration_raw_fd(&self) -> RawFd;
}
```

The method returns the underlying fd for uring registration. Under the uring path, this is how the reactor discovers which fd to `POLL_ADD` — we can't rely on `AsRawFd` because `mio::unix::SourceFd<'_>` doesn't implement it, and a blanket `impl<T: Source + AsRawFd>` would conflict with the explicit impl for `SourceFd`.

Solution: enumerate explicit impls for the known mio types via a macro, plus one hand-written impl for `SourceFd`:

```rust
impl_registration_source_via_asrawfd! {
    mio::net::TcpStream, mio::net::TcpListener, mio::net::UdpSocket,
    mio::net::UnixStream, mio::net::UnixListener, mio::net::UnixDatagram,
    mio::unix::pipe::Sender, mio::unix::pipe::Receiver,
}
impl RegistrationSource for mio::unix::SourceFd<'_> {
    fn registration_raw_fd(&self) -> RawFd { *self.0 }
}
```

### `PendingOp` queue

`add_source` can be called from any thread, but `io_uring` SQEs must be submitted by the ring's owning worker. Solution: queue the operation on the target worker and let its park loop drain.

```rust
pub(crate) enum PendingOp {
    Register { fd: RawFd, interest: Interest, io: Arc<ScheduledIo> },
    Deregister { io: Arc<ScheduledIo> },
}
```

On deregister, the reactor identifies the slab slot via a `uring_slab_key` field stamped onto `ScheduledIo` during `register`. No additional map-lookup required.

### Worker assignment: round-robin

```rust
let worker = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
```

This policy optimizes for ring load balance (every worker's ring ends
up with roughly equal numbers of registrations) rather than task
locality (`W_ring == W_task`).

Locality-preferred placement was prototyped — `add_source` consulting a
`CURRENT_WORKER` TLS and falling back to round-robin only for
off-worker callers — and reverted. On the listener + `tokio::spawn`
shape that dominates `tests/net_uring_bench.rs`, forcing accepted fds
onto the accept-loop worker turns that worker into a single-ring
bottleneck and regresses every scenario relative to round-robin. A v2
lazy re-registration path (rebind the fd on first poll from a new
worker, gated by hysteresis) was also prototyped and removed; no
threshold value won any scenario. See commits `e29114e1` and
`eb5cbd57` for the full analysis.

What the locality experiments _did_ land is the `UringUnparker::unpark`
self-wake short-circuit (§6.3) — which turned out to be the actual
cost, independent of placement.

## 6. Cross-Worker Wakes

Three wake pathways, each with a different cost profile:

### 6.1 External-thread wake (eventfd)

External callers (non-worker threads, other runtimes, `spawn_blocking` tasks calling `waker.wake()`) obtain an `ExternalWaker`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ExternalWaker {
    fd: Arc<OwnedFd>,  // clone of Reactor.external_wake_fd
}
impl ExternalWaker {
    pub(crate) fn wake(&self) -> io::Result<()> {
        let buf = 1u64.to_ne_bytes();
        unsafe { libc::write(self.fd.as_raw_fd(), buf.as_ptr().cast(), 8) };
        /* EAGAIN = already-pending, treat as success */
    }
}
```

The eventfd is registered with `POLL_ADD_MULTI`; a write produces a CQE with `variant = VARIANT_EVENTFD`, unblocking the parked worker.

### 6.2 Worker-to-worker wake (MSG_RING)

When worker `W_a` needs to wake parked worker `W_b` (e.g., a task woken during `W_a`'s drain has its home on `W_b`):

```rust
let sqe = opcode::MsgRingData::new(
    types::Fd(target_ring_fd),    // W_b's ring fd, published in UringHandle
    0,                             // result payload (unused)
    MSG_RING_INCOMING_UD,          // user_data posted on W_b's CQ
    None,
)
.build()
.user_data(encode(VARIANT_CONTROL, gen, control_key));  // our own send-ack

unsafe { self.push_sqe(sqe)? };
self.ring.submit()?;   // flush immediately; don't wait for next park
```

One `io_uring_enter` syscall per cross-worker wake. The receiver sees `VARIANT_MSG_RING_INCOMING` in its drain and knows scheduler state has changed; it doesn't need to dispatch to a specific `ScheduledIo`.

### 6.3 Same-worker wake (no-op)

If `W_a` wakes a task whose home is `W_a`, the scheduler pushes onto local LIFO — no syscall, no MSG_RING. The wake is discovered when the worker returns to its task loop.

`UringUnparker::unpark` short-circuits when `CURRENT_WORKER == self.idx` — a task woken on the same worker it's homed to doesn't pay a MSG_RING round-trip to wake itself. Measured as the dominant cost on short-RTT workloads before the short-circuit landed.

## 7. Park / Unpark / Shutdown

### Park

```rust
pub(crate) fn park(&mut self) -> io::Result<()> {
    self.ring.submit_and_wait(1)?;   // flush SQEs + block for ≥1 CQE
    self.drain_completions();
    Ok(())
}
pub(crate) fn park_timeout(&mut self, timeout: Duration) -> io::Result<()> {
    if timeout.is_zero() {
        self.ring.submit()?;          // non-blocking flush
        self.drain_completions();
        return Ok(());
    }
    // push a TIMEOUT SQE as VARIANT_CONTROL; submit_and_wait(1); drain.
}
```

### Drain

```rust
for cqe in self.ring.completion() {
    let (variant, gen, key) = decode(cqe.user_data());
    match variant {
        VARIANT_POLL_MULTI => {
            let entry = match self.ops.get(key as usize) {
                Some(e) if e.gen == gen => e,
                _ => continue,          // stale CQE; ignore.
            };
            let io = match &entry.state {
                OpState::PollMulti { io, .. } => io,
                _ => continue,
            };
            let result = cqe.result();
            let has_more = cqueue::more(cqe.flags());
            if result >= 0 {
                let ready = ready_from_poll_flags(result);
                io.set_readiness(Tick::Set, |curr| curr | ready);
                io.wake(ready);
            }
            if !has_more { self.ops.remove(key as usize); }  // drops the Arc
        }
        VARIANT_CONTROL => {
            if let Some(e) = self.ops.get(key as usize) {
                if e.gen == gen { self.ops.remove(key as usize); }
            }
        }
        VARIANT_EVENTFD => saw_external_wake = true,
        VARIANT_MSG_RING_INCOMING => { /* scheduler handles around park() */ }
        _ => {}
    }
}
if saw_external_wake { drain_eventfd(external_fd); }
```

The key correctness invariant: the `Arc<ScheduledIo>` in a `PollMulti` slot is dropped **iff** we observe a CQE with `F_MORE == 0` for that slot's current generation. The kernel guarantees this is the terminal CQE — no further references to this `user_data` will be posted.

### Shutdown

`UringParker::shutdown` clears the `LOCAL_REACTOR` TLS and drops the `Box<RefCell<Reactor>>`. `Reactor::drop` lets the `IoUring` drop, which kernel-side cancels any outstanding `POLL_ADD_MULTI` entries.

The `Slab::drop` runs during `Reactor::drop`, dropping any still-held `Arc<ScheduledIo>`s. Since the kernel has already released its ring memory, no late CQEs can land referencing those keys.

**Known correctness subtlety:** `shutdown_core` in `worker.rs` drains all cores on the last-exiting worker, so `UringParker::shutdown` can run on the wrong thread. The `ClearUringTls` RAII guard installed at `run(worker)` entry defensively clears the TLS on thread exit even when `shutdown` ran elsewhere.

## 8. TLS Discipline

Two thread-locals, both scoped to a worker thread:

### `LOCAL_REACTOR` (in `uring_driver.rs`)

```rust
thread_local! {
    static LOCAL_REACTOR: Cell<*const RefCell<Reactor>> = const { Cell::new(null()) };
}
```

Installed by `UringParker::ensure_reactor_installed`, cleared on `shutdown` + `Drop`. Needed so that **other** workers can look up the current thread's reactor when routing MSG_RING SQEs — the sender of a cross-worker wake submits the SQE on its **own** ring, so it needs access to the local reactor regardless of whose ring is the target.

**Install discipline:** raw-pointer-based (`install_local_reactor_raw` / `clear_local_reactor`), not RAII. Panics if an install finds a non-null slot — catches the cross-runtime TLS leak that otherwise occurs when workers from different runtimes recycle through the shared blocking-thread pool.

### `ClearUringTls` RAII guard (in `worker.rs`)

```rust
#[cfg(all(tokio_unstable, feature = "io-uring-reactor", ...))]
struct ClearUringTls;
impl Drop for ClearUringTls {
    fn drop(&mut self) {
        clear_local_reactor();
        clear_current_worker();
    }
}
```

Installed at the top of `run(worker)`. Clears both `LOCAL_REACTOR` and
`CURRENT_WORKER` so that when a blocking-pool thread finishes running
one runtime's worker and gets reassigned to another runtime's worker,
the TLS is clean before the second runtime's
`install_local_reactor_raw` / `set_current_worker_early` runs.

Without this guard, the second `install` panics on "another Reactor is already installed on this thread"; we saw this in practice under parallel test execution with multiple runtimes sharing the global blocking pool.

## 9. Timer Integration

Per-worker timer wheels are required for the same reason as per-worker reactors: a single shared timer wheel would serialize all sleep/wake operations across cores.

Tokio's time crate exposes `TimerFlavor::Alternative` (internal `time_alt` path) which provides per-worker timer wheels. The uring reactor builder configures this flavor automatically when `.enable_uring_reactor()` is set:

```rust
runtime::Builder::new_multi_thread()
    .worker_threads(N)
    .enable_uring_reactor()   // implies TimerFlavor::Alternative + I/O backend swap
    .build()?
```

No API surface beyond the one builder method.

## 10. Scope Annotations

**Visibility policy:** all internal uring-only types are `pub(crate)` or `pub(super)`. `pub(super)` is preferred for fields only used within `runtime::io` to avoid `private_interfaces` warnings. Examples: `UringHandle.registrations`, `UringHandle.synced`.

**`#[allow(dead_code)]`** appears in a few places where code is reachable only under non-default feature combinations (e.g., `Reactor::external_waker` is only called when `ExternalWaker`s are handed out, which v1 wires up partially). These allowances are documented inline.

## 11. Integration Points

### `ScheduledIo`

Gained three fields, behind the uring feature gate:

```rust
#[cfg(all(tokio_unstable, feature = "io-uring-reactor", ...))]
uring_slab_key: AtomicU32,   // slab index; u32::MAX = not registered
uring_gen:      AtomicU32,   // generation of the current slot
uring_worker:   AtomicU32,   // worker idx whose ring owns the POLL_ADD
```

Stamped by `Reactor::register`; read by `Reactor::deregister` to
locate the slab slot and by the cross-worker `POLL_REMOVE` path to
route the remove SQE to the ring that owns the registration
(`io_uring` scopes removes to the originating ring).

### `runtime::Builder`

Added `.enable_uring_reactor()` setter. Feature-gated; on non-matching targets the method is absent and callers get a compile error. No runtime branch in default builds.

### Scheduler

`scheduler::multi_thread::handle::Handle` gained a uring-specific variant path that constructs a `UringParker` per worker instead of the traditional `Parker<IoStack>`. The scheduler loop is unchanged beyond the parker type; park/unpark contracts are preserved.

## 12. Correctness Invariants

1. **One ring per worker thread, submitter-bound.** `Reactor::new` is called on the worker thread during first park; `IORING_SETUP_SINGLE_ISSUER` binds submitter identity at first `io_uring_enter`; subsequent park calls on the same thread are the only legal submitters.

2. **`user_data` contains no pointers.** Slab keys + generation + variant only. Stale CQEs caused by slot recycling are detected by gen-mismatch.

3. **`Arc<ScheduledIo>` lifetime ≥ kernel's last CQE reference.** An Arc held inside an `OpState::PollMulti` slot is dropped only when a CQE with `F_MORE == 0` arrives for that slot. Kernel-provided handshake, not a time-based guess.

4. **No submission from non-owner threads.** `MSG_RING` from `W_a` to `W_b` is submitted on `W_a`'s own ring (respecting `SINGLE_ISSUER`), carrying a target fd that refers to `W_b`'s ring.

5. **TLS clean on thread exit.** `ClearUringTls` guards against cross-runtime leaks via the shared blocking pool.

6. **Worker registrations don't outlive the reactor.** `Reactor::drop` drops the `IoUring` and the `Slab`; kernel cancels outstanding polls during ring close; remaining slab entries drop their Arcs with no possibility of a post-close CQE surfacing to userspace.

## 13. Known Gaps & Limitations

### Placement is round-robin (by design for this workload shape)

See §5. Locality-preferred placement and v2 lazy re-registration were
both prototyped and rejected on the bench matrix. The door is open to
revisit once a workload shows a durable task-to-worker affinity that
round-robin actively hurts — but the current bench matrix doesn't.

### No CQE stealing

Each worker drains only its own CQ. A worker with no local tasks and no incoming wakes will park even if a peer's CQ is full. Mio-driven runtimes don't have this issue because the driver is shared. Mitigation: the MSG_RING wake path keeps wakes visible; stealing tasks (scheduler-level, not CQE-level) distributes work once wakes land.

### Listener/accept fd locality

`TcpListener` is registered on whatever worker called `bind`, and every accepted socket is also registered on that worker (because `accept` runs inside the listener's task on the listener's worker). The accept loop then `tokio::spawn`s handler tasks, which distribute across workers. Result: all connection fds land on the listener's worker, handler tasks spread — every CQE dispatch is cross-worker.

v1 accepts this cost. A lazy re-registration path (rebind on first poll from a new worker) was prototyped in commit `a4a494c4` and ripped out in `eb5cbd57` — no hysteresis threshold won any bench scenario. Revisit if a real workload demonstrates durable task migration that this pattern doesn't already amortize via round-robin placement.

### No completion-model ops

`RECV_MULTI`, `SEND_ZC`, fixed-buffer I/O, `ACCEPT_MULTI` — all deferred. The readiness model used today mirrors mio's behavior exactly; completion-model ops require a re-shaped `Registration` that can track in-flight ops separately from readiness state. Out of scope for v1.

### No io-uring for filesystem ops

`tokio::fs` continues to use the blocking-pool + stdlib strategy. The existing `io-uring` feature (separate from `io-uring-reactor`) experiments with completion-model file I/O; the two features are orthogonal.

### Kernel version floor

Linux 6.0+ required for mature `DEFER_TASKRUN`. Older kernels need to fall back to mio, which today means build-time feature exclusion rather than runtime detection. A runtime fallback (try `Reactor::new`, fall back to mio driver on `EINVAL`/`ENOSYS`) is possible but not yet implemented.

## 14. Testing

### Unit tests

- `uring_reactor::tests::reactor_new_succeeds` — ring construction with our setup flags on a supported kernel.
- `reactor_new_succeeds` — smoke test; skips on `ENOSYS`/`EINVAL` kernels.
- `park_timeout_zero_is_noop` — zero-timeout park returns promptly without error.
- `external_waker_unblocks_park` — eventfd → CQE → drain end-to-end.
- `msg_ring_wakes_peer` — one reactor wakes another via `MSG_RING`.

### Integration tests

- `tests/net_uring_reactor_tcp.rs` — TCP round-trip tests:
  - `tcp_single_worker_round_trip`
  - `tcp_multi_worker_round_trip`
  - `tcp_many_concurrent_connections` (N=32)
  - `tcp_read_blocks_then_wakes` (timer-wheel interaction — currently flaky, unrelated to reactor correctness)
- `tests/rt_uring_reactor.rs` — runtime-level spin-up/shutdown with the uring flavor.

### Benchmarks

- `tests/net_uring_bench.rs` — five TCP echo scenarios exercising the matrix of worker count × connection count × message size. Same binary compiled under mio and uring for direct comparison.

## 15. Performance (Current State)

Median of 3 runs; round-trips per second; higher is better. Numbers
from commit `e29114e1` (round-robin placement + unpark self-wake
short-circuit); `eb5cbd57` (post-v2-rip-out) confirmed the bench
matrix returns to this baseline.

| Scenario | mio | uring | Δ |
|---|---:|---:|---:|
| `single_worker_saturation` (1w × 16c × 4000 × 256B) | 53,551 | 53,537 | ±0% |
| `many_workers_low_concurrency` (8w × 16c × 4000 × 256B) | 183,118 | 203,675 | **+11.2%** |
| `small_msgs_many_clients` (4w × 64c × 2000 × 64B) | 135,066 | 148,330 | **+9.8%** |
| `large_msgs_many_clients` (4w × 32c × 500 × 16KB) | 84,883 | 91,783 | **+8.1%** |
| `small_msgs_few_clients` (4w × 8c × 10000 × 64B) | 124,523 | 129,766 | **+4.2%** |

**Interpretation:**
- Wins across every multi-worker scenario.
- Correct tie at single-worker saturation (no cross-worker cost to avoid).
- `small_msgs_few_clients` — the scenario that regressed by `−8.4%` under locality-preferred placement — is now a modest win under round-robin + self-wake short-circuit. The measurable cost on short-RTT workloads was never the cross-ring `MSG_RING` syscall; it was the redundant park-state CAS (and occasional wasted `MSG_RING`) triggered by scheduler-side notify-current-worker calls while still executing on that same worker. Short-circuiting those eliminates the overhead without perturbing where fds live.

## 16. Forward Roadmap

**Immediate (before landing):**
1. Fix the `tcp_read_blocks_then_wakes` test's timer-wheel flakiness. Likely a `TimerFlavor::Alternative` resolution issue on short sleeps; not a reactor-correctness bug. Needs a direct timer test (no TCP) to isolate before deciding whether to adjust the timer wheel or loosen the test.

**Medium-term:**
2. Completion-model ops: start with `RECV_MULTI` for `TcpStream::read`. Requires a `Registration` variant that owns buffer lifetime.
3. Fixed-file-descriptor registration (`IORING_REGISTER_FILES`) for hot sockets; allows `types::Fixed(...)` instead of `types::Fd(...)` and skips fd refcount work per SQE.
4. Runtime kernel-feature detection and automatic mio fallback.

**Long-term:**
5. `ACCEPT_MULTI` + multishot accept with per-worker acceptors for servers that want to distribute at accept time.
6. Zero-copy send (`SEND_ZC`) for large payloads.
7. Buffered I/O via `IORING_REGISTER_BUFFERS`.

## 17. File Map

| File | Role |
|---|---|
| `tokio/Cargo.toml` | Feature declaration, `io-uring` + `slab` deps |
| `tokio/src/io/poll_evented.rs` | Widen `PollEvented<E>` bound from `Source` to `RegistrationSource` |
| `tokio/src/runtime/io/mod.rs` | Module wiring; gate uring submodules |
| `tokio/src/runtime/io/registration.rs` | `RegistrationSource` trait + explicit impls |
| `tokio/src/runtime/io/scheduled_io.rs` | `uring_slab_key` field on `ScheduledIo` |
| `tokio/src/runtime/io/uring_driver.rs` | `UringHandle`, `PendingOp`, `LOCAL_REACTOR` TLS, `install/clear_local_reactor_raw` |
| `tokio/src/runtime/io/uring_reactor.rs` | Core `Reactor` + slab-indexed `user_data` encoding |
| `tokio/src/runtime/scheduler/mod.rs` | Scheduler flavor branch for uring |
| `tokio/src/runtime/scheduler/multi_thread/handle.rs` | Multi-thread handle with uring parker |
| `tokio/src/runtime/scheduler/multi_thread/uring_park.rs` | `UringParker` / `UringUnparker`, pending-op apply |
| `tokio/src/runtime/scheduler/multi_thread/worker.rs` | `ClearUringTls` RAII guard at `run(worker)` scope |
| `tokio/tests/net_uring_reactor_tcp.rs` | TCP round-trip integration tests |
| `tokio/tests/net_uring_bench.rs` | TCP echo throughput benchmark |
| `tokio/docs/uring-reactor-design.md` | **this document** |
| `tokio/docs/uring-opstate-slab.md` | Slab / `user_data` design |

— End —
