# IoDriver vtable: unified io-driver abstraction

**Status:** steps 1 and 2 implemented on `worktree-io-driver-vtable`.
Step 3 (legacy shared-mio backend behind the same vtable) is the next
piece of work and remains as designed below.
**Branch:** `worktree-io-driver-vtable` (off `uring-reactor`).
**Predecessor docs:** see `plan-history/uring-reactor-design.md` for the existing
per-worker uring backend, and the sharded-mio worktree (`tokio-sharded-mio`,
branch `sharded-mio`) for the analog mio backend that motivated this refactor.

## Implementation status

| Step | Status | Landing commits |
|------|--------|-----------------|
| 1 — scaffold + uring port | ✅ done | `d152586e` (scaffold), `dd38bdf2` (Handle field swap), `f32f13fa` (rebind-path rip) |
| 2a — sharded-mio backend imported | ✅ done | `480b83da` |
| 2b — `SHARDED_MIO_VTABLE` + `from_sharded_mio` | ✅ done | `a87b69ef` |
| 2c — collapse `registration.rs` cfg cascade | ✅ done | `fa27c1a1` |
| 3 — legacy shared-mio behind the vtable | not yet started | — |
| 4 — upstream-shaped diff | presentation only | — |

Bonus fix that fell out of step 2: `acd14d4a` removes an unsound
self-wake short-circuit in `ShardedMioUnparker::unpark` and
`UringUnparker::unpark` that deadlocked cap=1 mpsc round-trips by
leaving `num_searching` stuck at 1 after `transition_to_parked` popped
the calling worker off the sleepers list. Pinned by two new stress
regressions (`cross_worker_channel_round_trip_cap1_stress_{2w,4w}`)
in `tests/rt_sharded_mio.rs`.

### Divergences from the original step-1/2 design

- **`deregister` vtable signature.** The doc below shows
  `(NonNull<()>, &Arc<ScheduledIo>, usize)` — `worker_idx: usize` was
  meant to be a caller-tracked index returned by `add_source`. As
  shipped, the signature is
  `(NonNull<()>, &Arc<ScheduledIo>, &mut dyn RegistrationSource)`:
  `worker_idx` came off the vtable because both backends already
  stash it on `ScheduledIo` (`uring_worker` / `sharded_mio_worker`)
  and `Registration::deregister` was already reading it from there.
  `&mut dyn RegistrationSource` came on because sharded-mio needs to
  call `mio::Registry::deregister(source)`; the uring shim ignores it.
- **Accessor surface on `scheduler::Handle`.** The plan kept
  `handle.uring_handle()` and `handle.sharded_mio_handle()` as
  wrappers in step 1, with the intent of inlining them at step 2.
  Step 2c instead removed both accessors entirely and replaced them
  with a single `handle.io_driver() -> Option<&IoDriver>`. The
  removed accessors had no readers outside the registration path that
  collapsed in 2c, so keeping wrappers would have been pure surface
  area without callers.
- **Single `io_driver` field, no parallel `sharded_mio_handle` field.**
  The `multi_thread::Handle` originally grew a separate
  `Option<Arc<ShardedMioHandle>>` field at step 2a so the sharded-mio
  parker construction in `worker::create()` had something to clone
  from. Step 2c folded that field away; the `io_driver: Option<IoDriver>`
  field is now built via `match io_flavor { … from_uring(...) … |
  from_sharded_mio(...) … }`, and the local `Arc<ShardedMioHandle>`
  inside `worker::create()` (still used to construct each
  `ShardedMioParker`) lives only as a stack variable.

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

Handles into the driver are a thin pointer + a `&'static` vtable. The
representation is structurally a do-it-ourselves trait object, but with
our own layout (one shared `&'static` vtable per backend instead of the
fat-pointer encoding `dyn` uses), inlining attributes on shims, and no
`Send + Sync + 'static` plumbing on a trait declaration.

```rust
pub(crate) struct IoDriver {
    vtable: &'static IoDriverVTable,
    data:   NonNull<()>,
}

unsafe impl Send for IoDriver {}
unsafe impl Sync for IoDriver {}

pub(crate) struct IoDriverVTable {
    pub add_source: unsafe fn(
        NonNull<()>, &mut dyn RegistrationSource, Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)>,

    pub deregister: unsafe fn(
        NonNull<()>, &Arc<ScheduledIo>, usize,
    ) -> io::Result<()>,

    pub unpark_worker: unsafe fn(NonNull<()>, usize) -> bool,

    pub num_workers:   unsafe fn(NonNull<()>) -> usize,

    // Refcount lifecycle: each backend's `data` pointer is produced by
    // `Arc::into_raw` on its concrete handle type, and `clone`/`drop`
    // here do `Arc::increment_strong_count` / `Arc::decrement_strong_count`.
    pub clone: unsafe fn(NonNull<()>) -> NonNull<()>,
    pub drop:  unsafe fn(NonNull<()>),
}

impl Clone for IoDriver { /* via vtable.clone */ }
impl Drop  for IoDriver { /* via vtable.drop  */ }
```

### Why the vtable shape uses `&mut dyn RegistrationSource`

The two backends today take different argument types in their inherent
`add_source` impls — `UringHandle` takes `RawFd`, `ShardedMioHandle`
takes `&mut dyn Source`. The existing `RegistrationSource` trait
already bridges this: it extends `mio::event::Source` with
`registration_raw_fd() -> RawFd`. The vtable signature uses
`&mut dyn RegistrationSource` so each backend's shim extracts what it
needs.

### Step-1 ownership: still `Arc` internally

The aspirational form of this design is: driver lives in
`runtime::Inner`, no refcount, `IoDriver` values are pure borrow-erased
references into it. That requires lifetime plumbing through
`UringParker` (which today owns an `Arc<UringHandle>` field). Out of
scope for step 1 because it isn't a no-behavior-change refactor.

For step 1 the `data` pointer is produced by `Arc::into_raw` on the
backend's concrete handle, and `IoDriver: Clone` bumps that Arc via the
vtable. Dispatch and refcount cost match today's `Arc<UringHandle>`
exactly; we gain layout control, inlining headroom, and the cleaner
flavor-agnostic call sites in `registration.rs`. Eliminating the Arc
entirely is filed as future work in the open-questions section.

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

### Step 1 — scaffold + uring port (no behavior change) ✅ shipped

- Add `tokio/src/runtime/io/io_driver.rs` defining `IoDriver` and
  `IoDriverVTable`. (As shipped: `io_driver.rs`, not `driver_vtable.rs`.)
- Implement `URING_VTABLE` and `IoDriver::from_uring(handle: Arc<UringHandle>)`.
- Replace the `uring_handle: Option<Arc<UringHandle>>` field on
  `multi_thread::Handle` with `io_driver: Option<IoDriver>`.
- Other backends (`Traditional`, `ShardedMio`) untouched at this step;
  their existing cfg branches in `registration.rs` remain.

**Acceptance (met):**
- `rt_uring_reactor` integration tests pass under
  `--features rt-multi-thread,io-uring-reactor` (where the unrelated
  process/net feature collision in dev-deps allows them to compile).
- Refcount lifecycle covered by `uring_vtable_dispatch_and_refcount`
  and `as_uring_arc_bumps_count` lib unit tests.

### Step 2 — port `ShardedMioHandle` to fill the same vtable ✅ shipped

Done in three sub-commits:

- **2a — import sharded-mio backend** (`480b83da`).
  `ShardedMioHandle`, `ShardedMioParker`, sharded reactor module
  brought over from the sibling `sharded-mio` worktree, plus the
  five-test `tests/rt_sharded_mio.rs` smoke set.
- **2b — vtable shim** (`a87b69ef`).
  `SHARDED_MIO_VTABLE` static, `IoDriver::from_sharded_mio`,
  `IoDriver::as_sharded_mio`, `IoDriver::as_sharded_mio_arc`
  mirroring the uring shim. Vtable's `deregister` signature
  changed to take `&mut dyn RegistrationSource` (sharded-mio needs
  it for `mio::Registry::deregister`); the uring shim ignores
  source and reads `worker_idx` off the `ScheduledIo`.
- **2c — collapse the cfg cascade** (`fa27c1a1`).
  Both branches in `Registration::new_with_interest_and_handle` and
  `Registration::deregister` reduced to a single
  `if let Some(driver) = handle.io_driver() { driver.add_source/deregister(...) }`.
  Standalone `sharded_mio_handle` field on `multi_thread::Handle`
  removed; `io_driver` field now feeds both backends. Per-backend
  accessors (`uring_handle()`, `sharded_mio_handle()`) on
  `scheduler::Handle` deleted; replaced by `io_driver()`.

**Acceptance (met):**
- `rt_sharded_mio` integration suite: 7/7 in 0.22s
  (5 originals + 2 new cap=1 stress regressions added with the
  parker fix `acd14d4a`).
- `rt_threaded` (Traditional path): 29/29 in 3.16s — Traditional path
  unaffected by the field collapse.
- `tcp_into_split`: 3/3 in 0.01s — default mio registration path
  unaffected.
- Lib unit tests `runtime::io::io_driver::tests`: 5/5 across all four
  feature combos (sharded-only / uring-only / both / neither).

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

1. **Vtable-identity downcast for backend-specific call sites.**
   _Resolved by step 2b._ The `vtable_identity_does_not_alias` lib
   unit test asserts that `IoDriver::from_uring` and
   `IoDriver::from_sharded_mio` produce distinguishable instances —
   `as_uring()` returns `None` on a sharded driver and vice versa.
   Each backend declares exactly one `&'static IoDriverVTable`.

2. **`&mut dyn Source` in the vtable signature.** _Open in principle,
   not blocking._ As shipped, `add_source` and `deregister` both take
   `&mut dyn RegistrationSource`. No fd-registration latency check
   has been run; uring's hot path goes through the same shim it had
   before (`source.registration_raw_fd()` extraction is identical),
   and sharded-mio's hot path is `mio::Registry::register`/`deregister`
   which is already a virtual call. Worth a benchmark only if a
   workload-level regression shows up.

3. **`Handle` field rename ripples.** _Resolved by step 2c._ The
   `uring_handle()` and `sharded_mio_handle()` accessors are gone;
   their callers all moved to `handle.io_driver()`. No transitional
   wrappers shipped because no readers existed outside the
   registration path that collapsed in 2c.

4. **Performance regression on uring.** _Open._ No bench numbers
   collected yet for the vtable-routed path vs. the pre-step-1
   inherent-method path. The pre-existing
   `process` × `io-uring-reactor` test-build incompat (dev-deps drag
   `tokio` in with `full`, which forces `RegistrationSource` to be
   implemented for `process::imp::Pipe`) blocks running
   `tests/net_uring_*.rs` from this worktree. Filing as a follow-up.

## Out of scope (filed for later)

- **Eliminating the internal `Arc`.** The aspirational design has the
  driver owned by `runtime::Inner` with `IoDriver` as a borrow-erased
  reference (zero refcount traffic). Step 1 keeps the existing
  `Arc<UringHandle>` ownership to stay no-behavior-change. The promotion
  to borrow-only is a follow-up that requires reshaping `UringParker`'s
  `handle: Arc<UringHandle>` field into a borrow.
- Sharded-mio wake/drain optimizations (per-worker single-issuer slab,
  edge-triggered registration, wake coalescing, CPU pinning). Tracked
  separately; the vtable refactor must not depend on or block these.
- `uring_send` / `uring_recv` / `uring_recv_multi` exposure. These stay
  inherent on `UringHandle`, reachable only when the caller already has
  a uring-specific handle in hand; the vtable does not see them.
