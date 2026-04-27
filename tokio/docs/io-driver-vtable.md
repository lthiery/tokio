# IoDriver vtable: unified io-driver abstraction

**Status:** design, not yet implemented
**Branch target:** new worktree off `uring-reactor`
**Predecessor docs:** see `plan-history/uring-reactor-design.md` for the existing
per-worker uring backend, and the sharded-mio worktree (`tokio-sharded-mio`,
branch `sharded-mio`) for the analog mio backend that motivated this refactor.

## Motivation

The repo currently has two functioning sharded io-driver backends —
`UringHandle` (per-worker `io_uring`) and `ShardedMioHandle` (per-worker
`mio::Poll`) — plus the upstream legacy single-shared-mio driver. Each one
threads through the runtime via its own cfg-gated branch in
`registration.rs`, `worker.rs`, and `park.rs`. The two sharded handles
already expose **the same four-method surface**, and the cfg duplication
hides that.

Goal: extract one `IoDriver` value type so the rest of tokio is
backend-agnostic, and so a third backend can be dropped in without touching
the runtime core.

Non-goals (explicitly out of scope for this work):
- `uring_send` / `uring_recv` / `uring_recv_multi` SQE-submission APIs.
  Those are uring-specific extensions and remain inherent on
  `UringHandle`. The vtable does not see them.
- Performance work on the sharded-mio wake/drain path. Tracked separately;
  this refactor must be performance-neutral on the uring backend.

## Framing: imagine uring replaced upstream's io driver

If upstream tokio scrapped its mio-based driver and adopted uring directly,
the `runtime::Handle` io surface would shrink to exactly:

```text
add_source(source, interest)   -> (Arc<ScheduledIo>, worker_idx)
deregister(source, slab_key)
unpark_worker(worker_idx)
num_workers()
```

CQE drain, slab encoding, pending-op queues, wake fds — all internal.
That four-method surface is the canonical `IoDriver` trait. It is also
exactly what `ShardedMioHandle` already exposes, and what the legacy
shared-mio driver could be wrapped to expose.

## Representation: manual vtable, not `Arc<dyn>`

The driver object lives inside `runtime::Inner` and is dropped with the
runtime. Handles into it are borrow-erased thin pointers + a `&'static`
vtable. No `Arc<dyn>`, no per-handle allocation, no atomic refcount
traffic on `Handle::clone()`.

```rust
pub(crate) struct IoDriver {
    vtable: &'static IoDriverVTable,
    data:   NonNull<()>, // points into runtime::Inner; lives as long as it
}

unsafe impl Send for IoDriver {}
unsafe impl Sync for IoDriver {}

pub(crate) struct IoDriverVTable {
    pub add_source: unsafe fn(
        NonNull<()>, &mut dyn Source, Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)>,

    pub deregister: unsafe fn(
        NonNull<()>, &mut dyn Source, usize,
    ),

    pub unpark_worker: unsafe fn(NonNull<()>, usize),

    pub num_workers:   unsafe fn(NonNull<()>) -> usize,
}
```

Each backend declares one `static` vtable that thin-wraps its inherent
methods:

```rust
static URING_VTABLE: IoDriverVTable = IoDriverVTable {
    add_source:    |p, src, intr| unsafe {
        (&*p.cast::<UringHandle>().as_ptr()).add_source(src, intr)
    },
    deregister:    |p, src, key|  unsafe {
        (&*p.cast::<UringHandle>().as_ptr()).deregister(src, key)
    },
    unpark_worker: |p, idx|       unsafe {
        (&*p.cast::<UringHandle>().as_ptr()).unpark(idx)
    },
    num_workers:   |p|            unsafe {
        (&*p.cast::<UringHandle>().as_ptr()).num_workers()
    },
};
```

Properties this gets vs. `Arc<dyn IoDriver>`:

- **Zero refcount traffic.** Single owner inside `Inner`; `IoDriver`
  values are `Copy`-ish bitmoves (we'll make them `Clone` only, not `Copy`,
  to keep aliasing intentional).
- **No allocation per handle.** Today `Arc<UringHandle>` allocates; this
  becomes a thin embed.
- **Devirtualization headroom.** A `&'static` const vtable lets LLVM see
  the call targets when sites are inlined; fat-pointer `dyn` rarely
  devirtualizes.
- **One unsafe block per shim**, all trivial cast-and-dispatch.

## Why parking is *not* on the vtable

The vtable covers the **shared, cross-thread** side only. Parking is
per-worker, owned, never crosses threads, and one impl (`UringParker`)
holds a `!Send` reactor.

`WorkerParker` stays a static-dispatch enum. The vtable selects which
parker constructor runs at worker startup; from there each worker holds
its concrete parker by value.

## Migration order

Each step is a coherent, reviewable change. Steps 1–3 are pure
refactors; step 4 is forward-looking only.

### Step 1 — scaffold + uring port (no behavior change)

- Add `tokio/src/runtime/io/driver_vtable.rs` defining `IoDriver` and
  `IoDriverVTable`.
- Implement `URING_VTABLE` and `UringHandle::into_io_driver(self: Arc<Self>)`.
- Replace the `uring_handle: Option<Arc<UringHandle>>` field on
  `runtime::Handle` with `io_driver: Option<IoDriver>`. Keep the
  inherent-method shortcuts (`handle.uring_handle()` etc.) as wrappers
  that downcast via vtable identity check, *only where currently used*.
- Other backends (`Traditional`, `ShardedMio`) untouched at this step;
  their existing cfg branches in `registration.rs` remain.

**Acceptance:**
- `cargo test` parity with `uring-reactor` HEAD on the uring feature.
- `tests/net_uring_reactor_*.rs` all pass unchanged.
- `tests/net_uring_bench.rs` median throughput within ±2% of HEAD on
  the same hardware.

### Step 2 — port `ShardedMioHandle` to fill the same vtable

- Add `SHARDED_MIO_VTABLE` and `ShardedMioHandle::into_io_driver`.
- Collapse the cfg branches in `registration.rs::new_with_interest_and_handle`
  and `deregister` to a single
  `if let Some(driver) = handle.io_driver() { driver.add_source(...) }`.
- Vtable identity check (`std::ptr::eq(self.vtable, &URING_VTABLE)`)
  used wherever uring-only behavior is still gated.

**Acceptance:**
- `cargo test --features full,io-sharded-mio` from the sharded-mio
  worktree, ported in.
- `tests/net_sharded_mio_*.rs` all pass.
- Sharded-mio bench numbers match the sharded-mio worktree HEAD within ±2%.

### Step 3 — port the legacy shared-mio driver to fill the vtable

- Wrap the existing `tokio::runtime::io::Handle` (shared `mio::Poll`,
  one driver, one Registry) as a `LegacyMioHandle` with the same
  four-method surface.
- Round-robin `worker_idx` becomes always-`0` (single shard); `unpark_worker`
  pokes the single mio waker.
- All three backends now go through `IoDriver`. The `IoFlavor` enum
  becomes a *runtime-only* selector in `Builder`; the rest of the runtime
  is flavor-agnostic.

**Acceptance:**
- Stock tokio test suite (`cargo test`) passes on default features.
- `IoFlavor::Traditional` uses the new vtable path; the old direct path
  is deleted.
- No cfg gates remain in `registration.rs`, `worker.rs`, or `park.rs`
  for io-driver dispatch.

### Step 4 — (forward-looking only) upstream-shaped diff

Once steps 1–3 land, the diff to present upstream is:

> "The io driver is now an `IoDriver` value with a four-method vtable.
> Here is the legacy-mio impl preserving today's behavior. Here is the
> uring impl behind a feature flag."

This is a presentation decision, not implementation work. Captured here
so future-us remembers the framing.

## Risks / open questions

1. **Vtable-identity downcast for backend-specific call sites.** Step 1
   keeps a few uring-only call sites (e.g. the `ClearUringTls`
   blocking-pool guard). Comparing `&'static IoDriverVTable` pointers is
   reliable if each backend declares exactly one vtable instance; that
   contract should be asserted by a test.

2. **`&mut dyn Source` in the vtable signature.** Currently the
   `RegistrationSource` trait is generic; demoting to `dyn` is fine for
   register/deregister (cold paths) but worth verifying it doesn't
   regress fd-registration latency. Benchmark step 1 with both shapes
   if any uring numbers slip.

3. **`Handle` field rename ripples.** `uring_handle()` accessors are
   referenced in non-runtime code (e.g. registration). Wrapper methods
   on `Handle` keep call sites stable through step 1; they get inlined
   to vtable identity checks at step 2.

4. **Performance regression on uring.** This refactor must be
   performance-neutral. If step 1 regresses uring benchmarks beyond ±2%,
   stop and investigate before touching step 2.

## Out of scope (filed for later)

- Sharded-mio wake/drain optimizations (per-worker single-issuer slab,
  edge-triggered registration, wake coalescing, CPU pinning). Tracked
  separately; the vtable refactor must not depend on or block these.
- `uring_send` / `uring_recv` / `uring_recv_multi` exposure. These stay
  inherent on `UringHandle`, reachable only when the caller already has
  a uring-specific handle in hand; the vtable does not see them.
