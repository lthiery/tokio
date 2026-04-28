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
| 4 — third-party `IoDriver` injection (`from_impl<T>`) | designed, not implemented | — |
| 5 — upstream-shaped diff | presentation only | — |

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

### Step 4 — third-party `IoDriver` injection (the real motivation for the vtable)

The vtable's value isn't that it lets us swap uring for sharded-mio
internally — feature-gated module selection would have done that. The
value is that it lets a *user* plug in an io-driver tokio has never
heard of (an io_uring variant with extension opcodes, a SPDK-backed
driver, a hypervisor-virtio driver, a test-only mock for fault
injection) without a fork.

We follow the `bytes::Bytes` precedent here, with one important twist
documented below.

#### What `bytes` does, exactly

`bytes::Bytes` is the canonical "fat pointer + manual vtable" Rust
type:

```rust
// bytes-1.11.1/src/bytes.rs
pub struct Bytes {
    ptr:    *const u8,
    len:    usize,
    data:   AtomicPtr<()>,        // opaque backend state
    vtable: &'static Vtable,      // backend dispatch
}

pub(crate) struct Vtable {        // ← pub(crate), NOT pub
    pub clone:     unsafe fn(&AtomicPtr<()>, *const u8, usize) -> Bytes,
    pub into_vec:  unsafe fn(&AtomicPtr<()>, *const u8, usize) -> Vec<u8>,
    pub into_mut:  unsafe fn(&AtomicPtr<()>, *const u8, usize) -> BytesMut,
    pub is_unique: unsafe fn(&AtomicPtr<()>) -> bool,
    pub drop:      unsafe fn(&mut AtomicPtr<()>, *const u8, usize),
}

// pub(crate) — external crates cannot call this
pub(crate) unsafe fn with_vtable(...) -> Bytes { ... }
```

Both `Vtable` and `with_vtable` are crate-private. Bytes ships four
hand-rolled static vtables (`STATIC_VTABLE`, `OwnedVtable::VTABLE`,
`PROMOTABLE_{EVEN,ODD}_VTABLE`, `SHARED_VTABLE`); none are reachable
to a third party. The single public extension point is:

```rust
pub fn from_owner<T>(owner: T) -> Bytes
where T: AsRef<[u8]> + Send + Sync + 'static
```

`from_owner` boxes `T`, then pairs it with a *crate-owned* generic
vtable (`OwnedVtable::<T>::VTABLE`) whose `drop` shim runs `T`'s
destructor. The user supplies *storage and a `Drop` impl*; bytes
supplies *the vtable shape*.

So bytes's pattern is **closed vtable, open owner**: the vtable layout
is an internal implementation detail; the user's hook is a generic
constructor that monomorphizes a per-`T` shim.

#### The `IoDriver::from_impl<T>` design

We adopt the same pattern. `IoDriverVTable` stays `pub(crate)`. We add
a public `IoDriverImpl` trait whose four methods mirror the vtable
exactly, and a single public generic constructor:

```rust
// New public surface in tokio::runtime::io.
pub trait IoDriverImpl: Send + Sync + 'static {
    fn add_source(
        &self,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)>;

    fn deregister(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()>;

    fn unpark_worker(&self, worker_idx: usize) -> bool;

    fn num_workers(&self) -> usize;
}

impl IoDriver {
    /// `bytes::from_owner` analog: user supplies the impl, tokio
    /// synthesizes the vtable.
    pub fn from_impl<T: IoDriverImpl>(driver: T) -> Self {
        let raw = Arc::into_raw(Arc::new(driver));
        unsafe {
            Self::from_raw_parts(
                NonNull::new_unchecked(raw as *mut ()),
                &Shim::<T>::VTABLE,
            )
        }
    }

    // Crate-private; mirrors bytes::with_vtable.
    pub(crate) unsafe fn from_raw_parts(
        data: NonNull<()>,
        vtable: &'static IoDriverVTable,
    ) -> Self { ... }
}

// Per-T monomorphized shim, mirroring bytes's OwnedVtable<T>::VTABLE.
struct Shim<T>(PhantomData<T>);

impl<T: IoDriverImpl> Shim<T> {
    const VTABLE: IoDriverVTable = IoDriverVTable {
        add_source:    Self::add_source_shim,
        deregister:    Self::deregister_shim,
        unpark_worker: Self::unpark_worker_shim,
        num_workers:   Self::num_workers_shim,
        clone:         Self::clone_shim,
        drop:          Self::drop_shim,
    };

    unsafe fn add_source_shim(
        data: NonNull<()>,
        src: &mut dyn RegistrationSource,
        intr: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        // SAFETY: `data` was produced by Arc::into_raw on Arc<T>.
        let driver = &*data.cast::<T>().as_ptr();
        driver.add_source(src, intr)
    }

    unsafe fn clone_shim(data: NonNull<()>) -> NonNull<()> {
        Arc::increment_strong_count(data.cast::<T>().as_ptr());
        data
    }

    unsafe fn drop_shim(data: NonNull<()>) {
        Arc::decrement_strong_count(data.cast::<T>().as_ptr());
    }

    // ... the rest analogous.
}
```

The vtable per `T` is one `static` per monomorphization — same
codegen shape as `OwnedVtable<T>::VTABLE` in bytes. The shims are
trivial cast-and-dispatch; LLVM can see through them when call sites
are inlined, identical to the hand-rolled `URING_VTABLE` /
`SHARDED_MIO_VTABLE`.

#### Why this is strictly better than exposing the raw vtable

| | Public raw vtable (`pub fn from_raw_parts`) | `from_impl<T>` (chosen) |
|---|---|---|
| Public ABI surface | Every field of `IoDriverVTable` | Just the `IoDriverImpl` trait |
| Vtable evolution | Breaking: new field changes layout | Non-breaking: new defaulted method |
| Safety surface | Every user writes `unsafe` shims | Only crate writes `unsafe`; user writes safe trait |
| `Send`/`Sync` audit | User responsibility | Trait bound enforces it |
| Refcount semantics | User responsibility | `Arc<T>` machinery hidden |
| Loss of generality | None | None — any state expressible as `T: IoDriverImpl + Send + Sync + 'static` |

The hand-rolled in-tree vtables (`URING_VTABLE`, `SHARDED_MIO_VTABLE`,
future `LEGACY_MIO_VTABLE`) are unaffected. They keep their direct
shims for the perf-sensitive in-tree backends — same role as bytes's
hand-rolled `STATIC_VTABLE` / `SHARED_VTABLE` coexisting alongside
`from_owner`'s generic vtable.

#### Builder integration

`Builder` needs a way to *accept* a user-supplied `IoDriver`. Two
shapes are reasonable; both are unstable-gated.

**Option A — single-driver value:**

```rust
impl Builder {
    pub fn io_driver(&mut self, driver: IoDriver) -> &mut Self;
}
```

Simple but does not compose with per-worker drivers. uring and
sharded-mio both want one logical driver per worker, constructed
*after* the worker thread starts (each worker registers its eventfd
into its own `Poll`). A single-value API forces the user into the
shared-mio shape only.

**Option B — per-worker factory (recommended):**

```rust
impl Builder {
    pub fn io_driver_factory<F>(&mut self, factory: F) -> &mut Self
    where
        F: Fn(&IoDriverWorkerCtx) -> IoDriver + Send + Sync + 'static;
}

pub struct IoDriverWorkerCtx {
    pub worker_idx: usize,
    pub num_workers: usize,
    // future: pinning hints, NUMA node, etc.
}
```

The runtime calls `factory` once per worker during startup. uring and
sharded-mio map onto this directly: the factory builds the per-worker
`Poll`/`io_uring` instance and returns
`IoDriver::from_impl(MyPerWorkerDriver { ... })`. Shared-mio backends
have the factory return clones of one shared `IoDriver` (cheap —
`IoDriver: Clone` bumps the inner `Arc`).

Recommendation: **ship only Option B publicly.** It's the superset; A
is trivially expressible inside it (`factory = move |_| io_driver.clone()`).
Internal in-tree construction (`IoFlavor::Traditional` / `UringPerWorker`
/ `ShardedMio`) keeps using direct constructors and never goes through
the factory.

#### What the `IoFlavor` enum becomes

After step 3, `IoFlavor` is a closed enum identifying the three
in-tree backends. After step 4 it gets a fourth variant or is replaced
outright:

```rust
enum IoConfig {
    Traditional,                  // legacy mio
    UringPerWorker,               // unstable
    ShardedMio,                   // unstable
    Custom(Box<dyn Fn(&IoDriverWorkerCtx) -> IoDriver + Send + Sync + 'static>),
}
```

`Builder::io_driver_factory` sets `Custom(...)`. The default stays
`Traditional`. The runtime startup path is one match arm per variant,
all of them ending in "construct an `IoDriver` for each worker."

#### Acceptance for step 4

- `IoDriverImpl` trait and `IoDriver::from_impl<T>` constructor
  added behind `tokio_unstable`.
- `Builder::io_driver_factory` added behind `tokio_unstable`.
- Integration test: a third-party-style `MockDriver` in
  `tests/io_driver_custom.rs` that implements `IoDriverImpl` over
  a single shared `mio::Poll` (i.e. user-space-reimplements
  legacy mio via the public hook), driven through a real
  `TcpStream` round-trip on a multi-thread runtime built with
  `Builder::io_driver_factory`.
- Refcount lifecycle test: `from_impl_vtable_refcount` asserts
  `Clone` / `Drop` on the user-facing handle calls
  `T`'s `Arc` increment/decrement exactly once each, with `T`'s
  `Drop` running exactly once on final release.
- Vtable identity: `IoDriver::as_uring()` / `as_sharded_mio()`
  return `None` on a `from_impl<T>`-constructed driver
  (extends the existing `vtable_identity_does_not_alias` test).
- No regression on in-tree backends: all of step 1/2/3's
  acceptance tests still pass.

### Step 5 — (forward-looking only) upstream-shaped diff

Once steps 1–4 land, the diff to present upstream is:

> "The io driver is now an `IoDriver` value with a four-method vtable.
> Here is the legacy-mio impl preserving today's behavior. Here is the
> uring impl behind a feature flag. Here is the user-injection hook
> (`Builder::io_driver_factory` + `IoDriverImpl` trait) behind
> `tokio_unstable`."

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

5. **`IoDriverImpl` trait stability.** _Open, blocks step 4._ Once
   published (even under `tokio_unstable`), adding a method to
   `IoDriverImpl` is a breaking change for downstream impls.
   Mitigations to decide before shipping step 4:
   (a) every method gets a default impl where semantically possible
   (e.g. `unpark_worker` could default to no-op for single-shard
   backends, `num_workers` could default to `1`);
   (b) a sealed marker / version method so we can detect old impls
   at runtime if we ever need to extend with non-defaultable methods;
   (c) document that `tokio_unstable` traits may break across minor
   releases (the standing `tokio_unstable` policy already covers this,
   but the trait extension pattern deserves an explicit callout).

6. **Per-worker construction ordering.** _Open, blocks step 4._ The
   factory shape (`Fn(&IoDriverWorkerCtx) -> IoDriver`) implies the
   factory runs *on each worker thread* after that thread has started
   (so the user can register thread-local state, eventfd, etc.).
   That ordering needs to slot cleanly into the existing worker
   startup sequence in `multi_thread::worker::create()`, which today
   constructs the per-worker `IoDriver` *before* spawning the worker
   thread and hands it across via the launch tuple. Either:
   (a) we move construction to first-poll on the worker thread
   (latency hit on first task; needs a `OnceLock` per worker), or
   (b) we run the factory on the spawning thread and document that
   `IoDriverImpl` instances must be constructible off-worker
   (matches what uring/sharded-mio already do internally —
   the per-worker `Poll`/`io_uring` is created by the spawning
   thread and moved across, only the parker eventfd registration
   happens on-worker). Option (b) preserves today's semantics; lock
   that in unless a real third-party use case forces (a).

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
