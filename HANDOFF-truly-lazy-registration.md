# HANDOFF: Truly lazy fd registration (no runtime needed at construction)

## Where we are

After the gen-marker fix landed (commit `1d17ed2b`,
`rt(sharded-mio): lazy on-first-poll fd registration with gen-marker
clobber protection`), the sharded-mio backend is correct: no hangs
on `tcp_connect_churn`, no clobbered registrations after fd
recycling, all backends complete cleanly.

The shape of `Registration::new_with_interest_and_handle` today is
**halfway lazy**:

- **No** `epoll_ctl_add` at construction time. ✅
- **No** slab insert at construction time. ✅
- **Yes**, calls `Handle::current()` at construction time to grab
  the IoDriver vtable handle. ❌
- **Yes**, calls `handle.allocate_scheduled_io()` at construction
  time to pre-allocate `Arc::new(ScheduledIo::default())`. ❌

The two `❌`s are why `TcpStream::from_std`, `UnixStream::from_std`,
`tokio::process::Child` etc. all still panic when constructed
outside a runtime context — same as mainline. We didn't *gain*
construction-from-anywhere even though the lazy reshape would
naturally support it.

`register_local` itself unconditionally routes through
`queue_register` (cross-thread mutex push + `mio::Waker::wake`),
even when called from inside a sharded-mio worker. The synchronous
fast path was disabled to cure the wedge, but with the gen marker
in place the wedge is fixed for both paths — sync was kept off
purely for one bench's throughput artifact (see "Sync-on-calling-
worker" below).

## What "truly lazy" means

Construction-time work goes to **zero**. The future stores only
`(fd, interest)` and a `OnceCell` slot for the eventual
`Arc<ScheduledIo>`. At first poll — and only then — it:

1. Calls `Handle::current()` to find the I/O driver. By definition
   the future is being polled by *something*, and on multi-thread
   Tokio that something has a runtime in TLS.
2. Allocates the `Arc<ScheduledIo>` via `handle.allocate_scheduled_io()`.
3. Calls `handle.register_local(&shared, fd, interest)`.
4. Stuffs the Arc into the `OnceCell` so subsequent polls skip
   straight to the readiness check.

```rust
// new shape in tokio/src/runtime/io/registration.rs
pub(crate) struct Registration {
    fd: RawFd,
    interest: Interest,
    shared: OnceCell<Arc<ScheduledIo>>, // populated at first poll
    // NB: no Handle field, no eager Arc<ScheduledIo>
}

impl Registration {
    /// Construct without touching any runtime. Safe to call from
    /// any thread, runtime or not.
    pub(crate) fn new_with_interest(
        source: &mut impl Source,
        interest: Interest,
    ) -> io::Result<Self> {
        Ok(Self {
            fd: source.raw_fd(),
            interest,
            shared: OnceCell::new(),
        })
    }

    /// Must be called from inside a poll context (i.e. from a
    /// future). Panics if no Tokio runtime is in TLS.
    fn ensure_registered(&self) -> io::Result<&Arc<ScheduledIo>> {
        if let Some(shared) = self.shared.get() {
            return Ok(shared);
        }
        let handle = Handle::current().io_driver()?;
        let shared = handle.allocate_scheduled_io();
        let _worker_idx = handle.register_local(
            &shared, self.fd, self.interest,
        )?;
        Ok(self.shared.get_or_init(|| shared))
    }
}
```

User-visible win:

```rust
// works on any std::thread, no runtime in scope
let stream = TcpStream::from_std(some_std_stream).unwrap();

// ship it into Tokio
tokio::spawn(async move {
    // first poll happens here, on a worker thread, in a runtime
    stream.readable().await.unwrap();
});
```

That's the natural shape that "lazy" implies, and it's what we
*should* have built in the first place. The current half-lazy
shape is a layering accident, not a design choice.

## Why we didn't go all the way the first time

`Registration::new_with_interest_and_handle` was kept signature-
compatible with mainline so the `Registration::new(...)` callers
in `tokio/src/io/poll_evented.rs`, `tokio/src/net/tcp/*`,
`tokio/src/net/unix/*`, `tokio/src/process/unix/*`, and
`tokio/src/runtime/io/registration.rs` itself didn't all need
touching at the same time as the lazy work. That kept the diff
contained but also kept the eager `Handle::current()` and eager
`Arc<ScheduledIo>` allocation around as scar tissue.

Going truly lazy means changing that signature, which ripples
into every type that wraps `Registration`. Not deeply complex,
just wide.

## Implementation plan

### Phase 1 — `Registration` shape change

1. Drop the `Handle` field and the eager `Arc<ScheduledIo>` from
   `Registration`. Replace with `shared: OnceCell<Arc<ScheduledIo>>`.
2. Rename `new_with_interest_and_handle` → `new_with_interest`,
   no longer taking `&Handle`.
3. Move `Handle::current()` lookup into `ensure_registered` (or
   inline into every `poll_*` / `try_io` site that touches
   `self.shared`).
4. Update every caller. There are ~6 callsites; mostly mechanical.
5. Mainline-parity docstring update on `from_std`: change "panics
   if not in a runtime" to "panics on first I/O if not in a
   runtime."

### Phase 2 — Restore sync-on-calling-worker as the default

This is independent but reads naturally as a follow-up since
`register_local` is the entry point for both.

```rust
pub(crate) fn register_local(
    &self,
    shared: &Arc<ScheduledIo>,
    fd: RawFd,
    interest: Interest,
) -> io::Result<usize> {
    if let Some(idx) = current_worker_index() {
        // Default: future polled by a sharded-mio worker.
        // Register synchronously on that worker's own reactor —
        // no cross-thread coordination, no Waker::wake().
        return self.register_on_worker(idx, shared, fd, interest);
    }
    // Off-worker fallback: future polled by `Runtime::block_on`,
    // `spawn_blocking`, `LocalSet` on a non-worker thread, etc.
    // Round-robin pick a shard, queue, wake.
    self.queue_register(shared, fd, interest)
}
```

`register_on_worker` is already implemented — it's the function
that was deleted as `register_worker_local` in the cleanup
commit. Reintroduce it without the `clobber_track` diagnostic
calls and without the env-var dispatch wrapper.

The wedge is gone (gen marker), so this is now a pure
correctness-safe perf decision. Verify with the `tcp_connect_churn`
bench plus a more realistic workload (TODO: what bench? Maybe
`net_tcp_echo`'s `tcp_echo_throughput`, or write a new one with
distributed connection origins).

### Phase 3 — Decide what to do with `tcp_connect_churn`

The connect-churn bench shows ~14–25ms median on sharded-mio vs
~700µs on traditional. This is genuinely a bad number, but I
think it's an artifact of the bench's task-injection pattern, not
a property of sharded-mio:

- 30 client tasks per iter, all spawned remotely from the
  block_on root.
- Work-stealing tends to land the whole batch on one idle worker.
- That worker registers all 30 fds (sync path) → all 30 readiness
  events flow through one epoll set → one worker is the
  bottleneck for both readiness dispatch and Drop deregister.

Realistic workloads have connections distributed across worker
threads (incoming connections accepted on different workers,
spawned tasks not arriving as bursts). Need a bench that captures
that. Suggestions:

- A "many-server-many-client" bench: N tcp listeners across N
  workers, M concurrent connect-then-drop loops driving them.
- Real proxy workload? `hyper`'s benchmarks?
- Or: accept the artifact, document it, prioritize phase 1+2 on
  realistic workload data.

## Open design questions

### (a) `ScheduledIo` allocation site

`allocate_scheduled_io` today is a vtable call on `IoDriverHandle`
because the uring backend allocates differently
(`Arc::new_cyclic` for the slot key wiring) than sharded-mio
(`Arc::new(Default::default())`). If we defer allocation to
first-poll, `Handle::current()` has to give us the IoDriverHandle
at first-poll time. That works (it's already a TLS lookup).

Alternative: make `ScheduledIo::default()` work for both backends
(it almost does; uring's slot-key wiring could move to an
`init_with_slot_key` method called by `register_local`). That
would let `Registration::new_with_interest` allocate the Arc
locally without touching any handle, and `register_local` would
just attach it to a slab slot. Cleaner separation; needs a small
uring-side refactor.

### (b) Where does `Handle::current()` lookup happen?

Two options:

1. Inside `ensure_registered`. Pro: one lookup point. Con: every
   `poll_ready` / `try_io` post-first-poll would still go through
   `if self.shared.get().is_some() { return … }` first, but the
   common case is just an Arc deref. Acceptable.
2. Stash the handle on first poll into a `OnceCell<Handle>` next
   to `shared`. Pro: subsequent polls don't re-lookup. Con:
   double-OnceCell awkwardness; you can derive the handle from
   the slot anyway since `ScheduledIo` could carry a back-pointer.

Probably (1) is fine — the lookup is cheap (TLS), and once
`shared` is populated we don't need the handle for normal
readiness checks anyway. The handle is only needed again at
deregister time, which today already loads it from
`Registration::driver: Handle`.

### (c) Behavior change vs mainline

Mainline: `TcpStream::from_std` panics at construction if no
runtime. We'd defer that panic to first I/O.

Pro: enables a real use-case (construct outside runtime, send
into runtime).

Con: the panic moves later — users who currently get a clear
construction-time panic now get a confusing first-poll panic
inside an async context.

Mitigation: detect "no runtime" at construction *if cheap to do
so without grabbing a handle*, and panic eagerly with a
docstring-pointed message. Or: provide a `try_register_now()`
method users can call if they want eager behavior. Or: just
document the change and let users opt-in via the new shape.

This needs a tokio-team conversation; not for this branch alone.

### (d) Does this change `tokio::process::Child`?

`Child` uses `pidfd_reaper` which holds a `Registration`. The
lazy reshape in commit `1d17ed2b` already had to update
`pidfd_reaper.rs` and `process/unix/mod.rs` to use the new shape;
they'd update again here for the simpler signature. Mechanical.

## Test plan

- All existing I/O tests should still pass; behavior change is
  only the construction-time vs first-poll-time panic boundary.
- New test: construct a `TcpStream::from_std` from `std::thread::spawn`
  with no runtime, send via `mpsc` into a `tokio::spawn`, verify
  `.readable().await` works.
- `tcp_connect_churn` and the as-yet-unwritten distributed-load
  bench, before/after sync-register restoration in phase 2.
- `loom` model under `--cfg loom` for the OnceCell-backed first-
  poll registration race (two clones of the same `TcpStream`
  polled concurrently from two workers — should both see the
  same `Arc<ScheduledIo>` after `OnceCell` resolution).

## Files that will change

Phase 1 (lazy shape):
- `tokio/src/runtime/io/registration.rs` — main surgery
- `tokio/src/runtime/io/io_driver.rs` — vtable simplification
  (drop the `Handle` arg from `allocate_scheduled_io` if (a)
  resolves toward `Default`)
- `tokio/src/runtime/io/sharded_mio_driver.rs` — adjust
  `allocate_scheduled_io` if needed
- `tokio/src/runtime/io/uring_driver.rs` — same
- `tokio/src/io/poll_evented.rs` — caller signature update
- `tokio/src/net/tcp/{listener,stream,socket}.rs` — caller updates
- `tokio/src/net/unix/{listener,stream,datagram,socket}.rs` — same
- `tokio/src/process/unix/mod.rs` — same
- `tokio/src/process/unix/pidfd_reaper.rs` — same
- `tokio/src/signal/unix.rs` — if it uses Registration
- `tokio/docs/io-driver-vtable.md` — design note update

Phase 2 (sync-on-calling-worker):
- `tokio/src/runtime/io/sharded_mio_driver.rs` — restore
  `register_on_worker` (the deleted `register_worker_local`,
  shorn of diagnostics) and branch in `register_local`

Phase 3 (bench):
- `benches/net_tcp_echo.rs` or new bench file

## Out of scope for this handoff

- MPSC `pending_ops` rewrite (still believed to be the right
  long-term move for the off-worker fallback path, but not
  required for the lazy reshape).
- The third-party `IoDriver` injection (step 4 in
  `io-driver-vtable.md`) — orthogonal.
- Touching the traditional/legacy mio backend at all. It stays
  eager.

## Suggested commit shape when this lands

1. `rt(io-driver): truly lazy Registration — defer Handle lookup and Arc<ScheduledIo> allocation to first poll`
2. `rt(io-driver): drop Handle arg from allocate_scheduled_io vtable` (if (a) resolves that way)
3. `rt(net): adapt {Tcp,Unix}{Listener,Stream,...}::from_std for runtime-free construction`
4. `rt(process): adapt pidfd_reaper for runtime-free Registration construction`
5. `rt(sharded-mio): re-enable synchronous register on calling worker, queue path becomes off-worker fallback`
6. `bench(net): add distributed-load connect bench` (if we write one)
7. `docs(io-driver): document construction-time-vs-first-poll panic boundary change`

That's the plan. The phase-1 surgery is the meaty bit; phases
2–3 are small layered follow-ups.
