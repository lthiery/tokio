//! Backend-agnostic io-driver value, dispatched via a manual vtable.
//!
//! See `tokio/docs/io-driver-vtable.md` for the design doc this implements.
//!
//! ## Step-1 scope
//!
//! This module currently exposes only the uring vtable (`URING_VTABLE`) plus
//! the [`IoDriver`] container that wraps a `*const`-erased backend handle.
//! No production call site goes through it yet — that is gated behind
//! follow-up work that swaps `HandleInner::uring_handle` to
//! `HandleInner::io_driver`. The vtable shape is exercised by unit tests in
//! this module to prove the boundary compiles and dispatches.
//!
//! ## Ownership
//!
//! Step 1 keeps `Arc` ownership internally: the `data` pointer is produced
//! by [`Arc::into_raw`] on the backend's concrete handle, and the vtable's
//! `clone` / `drop` shims do `Arc::increment_strong_count` /
//! `Arc::decrement_strong_count`. Dispatch and refcount cost match today's
//! `Arc<UringHandle>` exactly. Eliminating the Arc entirely is filed as
//! future work in the design doc.

// Some helpers (e.g. `add_source`/`deregister`/`unpark_worker` invoked
// through the vtable) are not yet wired into the rest of tokio in step 1
// — only `from_uring`, `as_uring`, and the `Clone`/`Drop` lifecycle are
// reachable from production code. They get used in steps 2/3 when the
// flavor-agnostic call sites land.
#![allow(dead_code)]

use crate::io::interest::Interest;
use crate::loom::sync::Arc;
use crate::runtime::io::ScheduledIo;
use crate::runtime::io::registration::RegistrationSource;

use std::io;
use std::os::fd::RawFd;
use std::ptr::NonNull;

/// Backend-agnostic io-driver value.
///
/// Logically equivalent to a `dyn IoDriver` reference, but encoded as a
/// `(NonNull<()>, &'static IoDriverVTable)` pair we control directly.
/// See the module docs for why we hand-roll the vtable instead of using
/// `Arc<dyn IoDriver>`.
pub(crate) struct IoDriver {
    vtable: &'static IoDriverVTable,
    /// Type-erased pointer at the backend's concrete handle. The pointer
    /// is produced by `Arc::into_raw` on construction; the vtable's
    /// `clone_data` and `drop_data` shims keep the inner refcount honest.
    data: NonNull<()>,
}

// SAFETY: every backend that fills the vtable must have its concrete
// handle type be `Send + Sync` (the existing `UringHandle` is, since it
// was previously stored as `Arc<UringHandle>` in cross-thread contexts).
// The vtable itself is `&'static` and thread-safe; `data` is just a
// pointer at a `Send + Sync` value.
unsafe impl Send for IoDriver {}
unsafe impl Sync for IoDriver {}

/// Function-pointer table specifying how each operation dispatches to a
/// concrete backend. One `&'static` instance per backend.
pub(crate) struct IoDriverVTable {
    /// Allocate a fresh `Arc<ScheduledIo>` without binding it to any
    /// worker yet. Called from
    /// [`Registration::ensure_registered`][reg] on the first poll of a
    /// lazy-shape registration, just before [`register_local`][rl].
    ///
    /// Both backends implement this by `Arc::new(ScheduledIo::default())`.
    /// Kept in the vtable so future backends with non-trivial Arc
    /// initialisation (e.g. interior init that depends on driver
    /// state) have a hook.
    ///
    /// [reg]: super::registration::Registration
    /// [rl]: Self::register_local
    pub allocate_scheduled_io: unsafe fn(NonNull<()>) -> Arc<ScheduledIo>,

    /// Register a previously-allocated `Arc<ScheduledIo>` with `fd` /
    /// `interest`. Returns the worker index the registration was
    /// bound to.
    ///
    /// The sharded-mio backend tries the worker-local fast path
    /// (caller-thread is a sharded-mio worker → register directly on
    /// that worker's registry); on miss, queues a Register op onto a
    /// round-robin-picked worker. The uring backend always queues a
    /// `POLL_ADD_MULTI` SQE onto a round-robin-picked ring (uring
    /// SQEs must be submitted by the ring's owning worker, so there
    /// is no in-place fast path).
    pub register_local: unsafe fn(
        NonNull<()>,
        &Arc<ScheduledIo>,
        RawFd,
        Interest,
    ) -> io::Result<usize>,

    /// Deregister a previously-registered `ScheduledIo`. The owning
    /// worker index lives on the `ScheduledIo` itself (`uring_worker` /
    /// `sharded_mio_worker`), so each backend reads it directly. The
    /// `source` is required by the mio-based backends to call
    /// `mio::Registry::deregister`; the uring shim ignores it because
    /// `POLL_REMOVE` is keyed off `ScheduledIo` alone.
    pub deregister: unsafe fn(
        NonNull<()>,
        &Arc<ScheduledIo>,
        &mut dyn RegistrationSource,
    ) -> io::Result<()>,

    /// Wake `worker_idx`'s parker. Returns `true` if a real wake was
    /// delivered to the kernel (for metrics).
    pub unpark_worker: unsafe fn(NonNull<()>, usize) -> bool,

    /// Number of workers this driver fans out across.
    pub num_workers: unsafe fn(NonNull<()>) -> usize,

    /// `Arc::increment_strong_count` on the backing handle, returning a
    /// fresh data pointer (same address; new strong reference).
    pub clone_data: unsafe fn(NonNull<()>) -> NonNull<()>,

    /// `Arc::decrement_strong_count` on the backing handle.
    pub drop_data: unsafe fn(NonNull<()>),
}

impl IoDriver {
    /// Construct an `IoDriver` from an owned `Arc<H>` where `H` is the
    /// concrete backend handle type matched by `vtable`.
    ///
    /// # Safety
    ///
    /// `vtable`'s shims must be the ones for `H`. Mixing a vtable with a
    /// handle type it wasn't written for is undefined behavior.
    pub(crate) unsafe fn from_arc<H>(
        handle: Arc<H>,
        vtable: &'static IoDriverVTable,
    ) -> Self {
        let raw = Arc::into_raw(handle) as *mut ();
        // `Arc::into_raw` never returns null for a live Arc.
        let data = NonNull::new(raw).expect("Arc::into_raw returned null");
        Self { vtable, data }
    }

    /// Vtable identity check, used by call sites that still need to recover
    /// a backend-specific reference (e.g. for inherent uring-only methods).
    pub(crate) fn vtable_is(&self, expected: &'static IoDriverVTable) -> bool {
        std::ptr::eq(self.vtable, expected)
    }

    /// Number of workers (cheap; just an atomic load inside the backend).
    pub(crate) fn num_workers(&self) -> usize {
        // SAFETY: `self.data` was produced by an `into_arc` constructor
        // matching `self.vtable`; the shim casts it to the right type.
        unsafe { (self.vtable.num_workers)(self.data) }
    }

    /// Allocate a fresh `Arc<ScheduledIo>` for a brand-new
    /// registration. Pairs with [`Self::register_local`] — the caller
    /// keeps ownership of the Arc between the two calls so they can
    /// stash it in the registration even if `register_local` is
    /// deferred to a later poll.
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        // SAFETY: see `num_workers`.
        unsafe { (self.vtable.allocate_scheduled_io)(self.data) }
    }

    /// Register `shared`/`fd`/`interest`. Returns the worker the
    /// registration was bound to.
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        // SAFETY: see `num_workers`.
        unsafe { (self.vtable.register_local)(self.data, shared, fd, interest) }
    }

    pub(crate) fn deregister(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        // SAFETY: see `num_workers`.
        unsafe { (self.vtable.deregister)(self.data, io, source) }
    }

    pub(crate) fn unpark_worker(&self, worker_idx: usize) -> bool {
        // SAFETY: see `num_workers`.
        unsafe { (self.vtable.unpark_worker)(self.data, worker_idx) }
    }
}

impl Clone for IoDriver {
    fn clone(&self) -> Self {
        // SAFETY: the vtable's `clone_data` is required to do an
        // appropriate `Arc::increment_strong_count` on the backing type.
        let data = unsafe { (self.vtable.clone_data)(self.data) };
        Self { vtable: self.vtable, data }
    }
}

impl Drop for IoDriver {
    fn drop(&mut self) {
        // SAFETY: the vtable's `drop_data` is required to do an
        // appropriate `Arc::decrement_strong_count` on the backing type.
        unsafe { (self.vtable.drop_data)(self.data) }
    }
}

impl std::fmt::Debug for IoDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoDriver")
            .field("vtable", &(self.vtable as *const IoDriverVTable))
            .field("data", &self.data.as_ptr())
            .finish()
    }
}

// =====================================================================
// Uring backend vtable
// =====================================================================

cfg_io_uring_reactor! {
    use crate::runtime::io::uring_driver::UringHandle;

    /// VTable for `UringHandle`. Each shim casts the type-erased data
    /// pointer back to `*const UringHandle` and forwards to the inherent
    /// method.
    pub(crate) static URING_VTABLE: IoDriverVTable = IoDriverVTable {
        allocate_scheduled_io: uring_allocate_scheduled_io,
        register_local:        uring_register_local,
        deregister:            uring_deregister,
        unpark_worker:         uring_unpark_worker,
        num_workers:           uring_num_workers,
        clone_data:            uring_clone_data,
        drop_data:             uring_drop_data,
    };

    #[inline]
    unsafe fn as_uring(data: NonNull<()>) -> &'static UringHandle {
        // SAFETY: caller guarantees `data` came from
        // `Arc::into_raw(arc: Arc<UringHandle>)` and the strong count is
        // still positive (held by this `IoDriver` value). The reference
        // lifetime is bounded by the surrounding shim's call frame; we
        // erase to `'static` here to keep the shim signature simple, and
        // never leak the reference past the shim's return.
        unsafe { &*(data.as_ptr() as *const UringHandle) }
    }

    unsafe fn uring_allocate_scheduled_io(data: NonNull<()>) -> Arc<ScheduledIo> {
        let handle = unsafe { as_uring(data) };
        handle.allocate_scheduled_io()
    }

    unsafe fn uring_register_local(
        data: NonNull<()>,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        let handle = unsafe { as_uring(data) };
        handle.register_local(shared, fd, interest)
    }

    unsafe fn uring_deregister(
        data: NonNull<()>,
        io: &Arc<ScheduledIo>,
        _source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        let handle = unsafe { as_uring(data) };
        // Uring keys `POLL_REMOVE` off `ScheduledIo` alone — no
        // mio-source needed. `worker_idx` is recovered from the io
        // object itself, where `add_source` stashed it.
        let worker_idx = io
            .uring_worker
            .load(std::sync::atomic::Ordering::Relaxed) as usize;
        handle.deregister_source(io, worker_idx)
    }

    unsafe fn uring_unpark_worker(data: NonNull<()>, worker_idx: usize) -> bool {
        let handle = unsafe { as_uring(data) };
        handle.unpark(worker_idx)
    }

    unsafe fn uring_num_workers(data: NonNull<()>) -> usize {
        let handle = unsafe { as_uring(data) };
        handle.num_workers()
    }

    unsafe fn uring_clone_data(data: NonNull<()>) -> NonNull<()> {
        // SAFETY: `data` was produced by `Arc::into_raw` for an
        // `Arc<UringHandle>` whose strong count is held alive by the
        // `IoDriver` invoking the clone. Reconstitute the borrow with
        // `from_raw`, clone it (bumping the count), then `forget` the
        // borrow to leave the original `IoDriver`'s strong reference
        // intact. The new strong reference is handed back as raw bytes.
        //
        // Idiom (rather than `Arc::increment_strong_count`) keeps this
        // module compilable under `cfg(loom)`, where loom's `Arc` shim
        // does not expose the static increment/decrement helpers.
        let arc = unsafe { Arc::<UringHandle>::from_raw(data.as_ptr() as *const UringHandle) };
        let bumped = Arc::clone(&arc);
        std::mem::forget(arc);
        let raw = Arc::into_raw(bumped) as *mut ();
        // SAFETY: `Arc::into_raw` is documented to return a non-null
        // pointer for a live Arc.
        unsafe { NonNull::new_unchecked(raw) }
    }

    unsafe fn uring_drop_data(data: NonNull<()>) {
        // SAFETY: see `uring_clone_data`. Reconstituting and dropping
        // releases exactly one strong reference, freeing the inner
        // allocation if this was the last one. Loom-safe.
        let arc = unsafe { Arc::<UringHandle>::from_raw(data.as_ptr() as *const UringHandle) };
        drop(arc);
    }

    impl IoDriver {
        /// If this driver is the uring backend, return a borrow of the
        /// underlying handle. Used at the few call sites that still need
        /// uring-specific inherent methods (e.g. SQE submission helpers
        /// that are out of scope for the vtable).
        pub(crate) fn as_uring(&self) -> Option<&UringHandle> {
            if self.vtable_is(&URING_VTABLE) {
                // SAFETY: vtable identity proves `data` came from
                // `Arc::into_raw(_: Arc<UringHandle>)`; the Arc is alive
                // for as long as `self` is.
                Some(unsafe { &*(self.data.as_ptr() as *const UringHandle) })
            } else {
                None
            }
        }

        /// If this driver is the uring backend, return a fresh owning
        /// `Arc<UringHandle>` (bumps the strong count). Used by the
        /// scheduler builder when constructing per-worker parkers.
        pub(crate) fn as_uring_arc(&self) -> Option<Arc<UringHandle>> {
            if self.vtable_is(&URING_VTABLE) {
                // SAFETY: vtable identity proves `data` came from
                // `Arc::into_raw(_: Arc<UringHandle>)`. Reconstitute the
                // borrow, clone it to produce a fresh strong reference
                // for the caller, and `forget` the reconstituted borrow
                // so the original `IoDriver`'s reference stays valid.
                // (Loom-safe idiom; see `uring_clone_data`.)
                unsafe {
                    let arc = Arc::<UringHandle>::from_raw(
                        self.data.as_ptr() as *const UringHandle,
                    );
                    let bumped = Arc::clone(&arc);
                    std::mem::forget(arc);
                    Some(bumped)
                }
            } else {
                None
            }
        }

        /// Construct an `IoDriver` from an owned `Arc<UringHandle>`.
        pub(crate) fn from_uring(handle: Arc<UringHandle>) -> Self {
            // SAFETY: `URING_VTABLE`'s shims expect `data` to be the
            // `Arc::into_raw` of an `Arc<UringHandle>`; that's what
            // `from_arc::<UringHandle>` produces.
            unsafe { Self::from_arc(handle, &URING_VTABLE) }
        }
    }
}

// =====================================================================
// Sharded-mio backend vtable
// =====================================================================

cfg_io_sharded_mio! {
    use crate::runtime::io::sharded_mio_driver::ShardedMioHandle;

    /// VTable for `ShardedMioHandle`. Each shim casts the type-erased
    /// data pointer back to `*const ShardedMioHandle` and forwards to
    /// the inherent method, mirroring [`URING_VTABLE`].
    pub(crate) static SHARDED_MIO_VTABLE: IoDriverVTable = IoDriverVTable {
        allocate_scheduled_io: sharded_mio_allocate_scheduled_io,
        register_local:        sharded_mio_register_local,
        deregister:            sharded_mio_deregister,
        unpark_worker:         sharded_mio_unpark_worker,
        num_workers:           sharded_mio_num_workers,
        clone_data:            sharded_mio_clone_data,
        drop_data:             sharded_mio_drop_data,
    };

    #[inline]
    unsafe fn as_sharded_mio_handle(data: NonNull<()>) -> &'static ShardedMioHandle {
        // SAFETY: caller guarantees `data` came from
        // `Arc::into_raw(arc: Arc<ShardedMioHandle>)` and the strong
        // count is still positive (held by this `IoDriver` value). The
        // reference lifetime is bounded by the surrounding shim's call
        // frame; we erase to `'static` here to keep the shim signature
        // simple and never leak the reference past the shim's return.
        unsafe { &*(data.as_ptr() as *const ShardedMioHandle) }
    }

    unsafe fn sharded_mio_allocate_scheduled_io(data: NonNull<()>) -> Arc<ScheduledIo> {
        let handle = unsafe { as_sharded_mio_handle(data) };
        handle.allocate_scheduled_io()
    }

    unsafe fn sharded_mio_register_local(
        data: NonNull<()>,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        let handle = unsafe { as_sharded_mio_handle(data) };
        handle.register_local(shared, fd, interest)
    }

    unsafe fn sharded_mio_deregister(
        data: NonNull<()>,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        let handle = unsafe { as_sharded_mio_handle(data) };
        // Sharded-mio's deregister is queued onto the owning worker
        // (registry mutation stays on a single thread per shard).
        // Read the fd from the source so the worker can call
        // `Registry::deregister(SourceFd(&fd), ...)` later — the
        // original source value may be dropped by then.
        let fd = source.registration_raw_fd();
        handle.queue_deregister(io, fd);
        Ok(())
    }

    unsafe fn sharded_mio_unpark_worker(data: NonNull<()>, worker_idx: usize) -> bool {
        let handle = unsafe { as_sharded_mio_handle(data) };
        handle.unpark(worker_idx)
    }

    unsafe fn sharded_mio_num_workers(data: NonNull<()>) -> usize {
        let handle = unsafe { as_sharded_mio_handle(data) };
        handle.num_workers()
    }

    unsafe fn sharded_mio_clone_data(data: NonNull<()>) -> NonNull<()> {
        // SAFETY: see `uring_clone_data` — same reconstitute/clone/forget
        // idiom, just monomorphized on `ShardedMioHandle`.
        let arc = unsafe {
            Arc::<ShardedMioHandle>::from_raw(data.as_ptr() as *const ShardedMioHandle)
        };
        let bumped = Arc::clone(&arc);
        std::mem::forget(arc);
        let raw = Arc::into_raw(bumped) as *mut ();
        // SAFETY: `Arc::into_raw` is documented to return a non-null
        // pointer for a live Arc.
        unsafe { NonNull::new_unchecked(raw) }
    }

    unsafe fn sharded_mio_drop_data(data: NonNull<()>) {
        // SAFETY: see `uring_drop_data`.
        let arc = unsafe {
            Arc::<ShardedMioHandle>::from_raw(data.as_ptr() as *const ShardedMioHandle)
        };
        drop(arc);
    }

    impl IoDriver {
        /// If this driver is the sharded-mio backend, return a borrow
        /// of the underlying handle. Used at the few call sites that
        /// still need backend-specific inherent methods.
        pub(crate) fn as_sharded_mio(&self) -> Option<&ShardedMioHandle> {
            if self.vtable_is(&SHARDED_MIO_VTABLE) {
                // SAFETY: vtable identity proves `data` came from
                // `Arc::into_raw(_: Arc<ShardedMioHandle>)`; the Arc
                // is alive for as long as `self` is.
                Some(unsafe { &*(self.data.as_ptr() as *const ShardedMioHandle) })
            } else {
                None
            }
        }

        /// If this driver is the sharded-mio backend, return a fresh
        /// owning `Arc<ShardedMioHandle>` (bumps the strong count).
        /// Used by the scheduler builder when constructing per-worker
        /// parkers.
        pub(crate) fn as_sharded_mio_arc(&self) -> Option<Arc<ShardedMioHandle>> {
            if self.vtable_is(&SHARDED_MIO_VTABLE) {
                // SAFETY: vtable identity proves `data` came from
                // `Arc::into_raw(_: Arc<ShardedMioHandle>)`. Reconstitute
                // / clone / forget idiom; see `as_uring_arc`.
                unsafe {
                    let arc = Arc::<ShardedMioHandle>::from_raw(
                        self.data.as_ptr() as *const ShardedMioHandle,
                    );
                    let bumped = Arc::clone(&arc);
                    std::mem::forget(arc);
                    Some(bumped)
                }
            } else {
                None
            }
        }

        /// Construct an `IoDriver` from an owned `Arc<ShardedMioHandle>`.
        pub(crate) fn from_sharded_mio(handle: Arc<ShardedMioHandle>) -> Self {
            // SAFETY: `SHARDED_MIO_VTABLE`'s shims expect `data` to be
            // the `Arc::into_raw` of an `Arc<ShardedMioHandle>`;
            // that's what `from_arc::<ShardedMioHandle>` produces.
            unsafe { Self::from_arc(handle, &SHARDED_MIO_VTABLE) }
        }
    }
}

// =====================================================================
// Legacy mio (single-shard shared `mio::Poll`) backend vtable
// =====================================================================
//
// The legacy backend wraps `runtime::io::Handle` (the upstream single
// shared `mio::Poll` driver). It has no per-worker sharding: there is
// one registry, one waker, one reactor running inline on whichever
// worker holds the `Driver`. The vtable shims report `num_workers = 1`
// and route `unpark_worker` to the single global `mio::Waker`.
//
// The `register_local` shim returns `0` always — there is no
// per-worker stamp on legacy `ScheduledIo`, so the index is consumed
// only by the `Registration` bookkeeping and never read back.

use crate::runtime::io::Handle as LegacyMioHandle;

/// VTable for the legacy single-shared-mio `runtime::io::Handle`.
/// Mirrors `URING_VTABLE` / `SHARDED_MIO_VTABLE` shape; the legacy
/// backend's special-cases (single shard, sync registry-mutation
/// path, panicking `unpark`) are absorbed into the shims.
pub(crate) static LEGACY_MIO_VTABLE: IoDriverVTable = IoDriverVTable {
    allocate_scheduled_io: legacy_mio_allocate_scheduled_io,
    register_local:        legacy_mio_register_local,
    deregister:            legacy_mio_deregister,
    unpark_worker:         legacy_mio_unpark_worker,
    num_workers:           legacy_mio_num_workers,
    clone_data:            legacy_mio_clone_data,
    drop_data:             legacy_mio_drop_data,
};

#[inline]
unsafe fn as_legacy_mio_handle(data: NonNull<()>) -> &'static LegacyMioHandle {
    // SAFETY: caller guarantees `data` came from
    // `Arc::into_raw(arc: Arc<LegacyMioHandle>)` and the strong count
    // is still positive (held by this `IoDriver` value). The reference
    // lifetime is bounded by the surrounding shim's call frame; we
    // erase to `'static` here to keep the shim signature simple, and
    // never leak the reference past the shim's return.
    unsafe { &*(data.as_ptr() as *const LegacyMioHandle) }
}

unsafe fn legacy_mio_allocate_scheduled_io(_data: NonNull<()>) -> Arc<ScheduledIo> {
    // Matches uring / sharded-mio shape: infallible `Arc::new`. The
    // legacy `RegistrationSet` linkage happens lazily in
    // `legacy_mio_register_local` via `Handle::register_existing`,
    // mirroring the existing two-step `allocate_existing` + register
    // contract the sharded backends use.
    Arc::new(ScheduledIo::default())
}

unsafe fn legacy_mio_register_local(
    data: NonNull<()>,
    shared: &Arc<ScheduledIo>,
    fd: RawFd,
    interest: Interest,
) -> io::Result<usize> {
    let handle = unsafe { as_legacy_mio_handle(data) };
    let mut source = mio::unix::SourceFd(&fd);
    handle.register_existing(shared, &mut source, interest)?;
    // Legacy mio is single-shard: the returned worker idx is
    // synthetic. Callers stash it on `Registration` but the legacy
    // backend never reads it back (no `legacy_mio_worker` field on
    // `ScheduledIo`).
    Ok(0)
}

unsafe fn legacy_mio_deregister(
    data: NonNull<()>,
    io: &Arc<ScheduledIo>,
    source: &mut dyn RegistrationSource,
) -> io::Result<()> {
    let handle = unsafe { as_legacy_mio_handle(data) };
    // `RegistrationSource: mio::event::Source`, and
    // `Registry::deregister<S: Source + ?Sized>` accepts the unsized
    // trait object directly. `Handle::deregister_source` is likewise
    // generic over `S: Source + ?Sized`, so the dyn-call routes
    // through without an extra coercion.
    handle.deregister_source(io, source)
}

unsafe fn legacy_mio_unpark_worker(data: NonNull<()>, _worker_idx: usize) -> bool {
    let handle = unsafe { as_legacy_mio_handle(data) };
    // Legacy backend has a single global `mio::Waker`; `worker_idx`
    // is ignored. `Handle::unpark` panics on internal waker error
    // (matches the pre-vtable behaviour); the vtable contract only
    // says return `true` if a wake was delivered, so a successful
    // return is `true`.
    handle.unpark();
    true
}

unsafe fn legacy_mio_num_workers(_data: NonNull<()>) -> usize {
    // Legacy mio fans out to a single shared reactor; report `1`.
    1
}

unsafe fn legacy_mio_clone_data(data: NonNull<()>) -> NonNull<()> {
    // SAFETY: see `uring_clone_data` — same reconstitute/clone/forget
    // idiom, just monomorphized on `LegacyMioHandle`.
    let arc = unsafe { Arc::<LegacyMioHandle>::from_raw(data.as_ptr() as *const LegacyMioHandle) };
    let bumped = Arc::clone(&arc);
    std::mem::forget(arc);
    let raw = Arc::into_raw(bumped) as *mut ();
    // SAFETY: `Arc::into_raw` is documented to return a non-null
    // pointer for a live Arc.
    unsafe { NonNull::new_unchecked(raw) }
}

unsafe fn legacy_mio_drop_data(data: NonNull<()>) {
    // SAFETY: see `uring_drop_data`.
    let arc = unsafe { Arc::<LegacyMioHandle>::from_raw(data.as_ptr() as *const LegacyMioHandle) };
    drop(arc);
}

impl IoDriver {
    /// Construct an `IoDriver` from an owned `Arc<runtime::io::Handle>`
    /// (the legacy single-shared-mio backend).
    ///
    /// No matching `as_legacy_mio*` accessors are provided: the
    /// legacy `Handle` is reachable via `handle.driver().io()` for
    /// the few call sites that still need it (signal driver
    /// construction, file-backend `uring_context`, `cancel_op`), and
    /// no vtable-routed call site needs to recover the concrete
    /// type.
    pub(crate) fn from_legacy_mio(handle: Arc<LegacyMioHandle>) -> Self {
        // SAFETY: `LEGACY_MIO_VTABLE`'s shims expect `data` to be the
        // `Arc::into_raw` of an `Arc<LegacyMioHandle>`; that's what
        // `from_arc::<LegacyMioHandle>` produces.
        unsafe { Self::from_arc(handle, &LEGACY_MIO_VTABLE) }
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[cfg(feature = "io-uring-reactor")]
    use crate::runtime::io::uring_driver::UringHandle;
    #[cfg(feature = "io-sharded-mio")]
    use crate::runtime::io::sharded_mio_driver::ShardedMioHandle;

    /// Constructing an `IoDriver` from an `Arc<UringHandle>`, cloning it,
    /// and dropping both clones must not leak the inner allocation, and
    /// must keep `num_workers` queryable through the vtable.
    #[cfg(feature = "io-uring-reactor")]
    #[test]
    fn uring_vtable_dispatch_and_refcount() {
        let inner = Arc::new(UringHandle::new(4));
        let weak = Arc::downgrade(&inner);
        assert_eq!(Arc::strong_count(&inner), 1);

        let driver = IoDriver::from_uring(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);
        assert_eq!(driver.num_workers(), 4);
        assert!(driver.vtable_is(&URING_VTABLE));

        // Vtable-driven clone bumps strong count.
        let driver2 = driver.clone();
        assert_eq!(Arc::strong_count(&inner), 3);
        assert_eq!(driver2.num_workers(), 4);

        // Dropping clones decrements; original Arc remains.
        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver2);
        assert_eq!(Arc::strong_count(&inner), 1);

        // Once we drop the last external Arc the allocation should be
        // gone (weak upgrades fail).
        drop(inner);
        assert!(weak.upgrade().is_none());
    }

    #[cfg(feature = "io-uring-reactor")]
    #[test]
    fn as_uring_arc_bumps_count() {
        let inner = Arc::new(UringHandle::new(2));
        let driver = IoDriver::from_uring(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);

        let recovered = driver.as_uring_arc().expect("uring vtable matches");
        assert_eq!(Arc::strong_count(&inner), 3);
        assert!(Arc::ptr_eq(&recovered, &inner));

        drop(recovered);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 1);
    }

    /// Mirror of [`uring_vtable_dispatch_and_refcount`] for the
    /// sharded-mio backend: refcount accounting through the vtable's
    /// `clone_data` / `drop_data` shims must match `Arc<ShardedMioHandle>`
    /// directly, and `num_workers` must read back via the vtable.
    #[cfg(feature = "io-sharded-mio")]
    #[test]
    fn sharded_mio_vtable_dispatch_and_refcount() {
        let inner = Arc::new(ShardedMioHandle::new(4));
        let weak = Arc::downgrade(&inner);
        assert_eq!(Arc::strong_count(&inner), 1);

        let driver = IoDriver::from_sharded_mio(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);
        assert_eq!(driver.num_workers(), 4);
        assert!(driver.vtable_is(&SHARDED_MIO_VTABLE));

        let driver2 = driver.clone();
        assert_eq!(Arc::strong_count(&inner), 3);
        assert_eq!(driver2.num_workers(), 4);

        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver2);
        assert_eq!(Arc::strong_count(&inner), 1);

        drop(inner);
        assert!(weak.upgrade().is_none());
    }

    #[cfg(feature = "io-sharded-mio")]
    #[test]
    fn as_sharded_mio_arc_bumps_count() {
        let inner = Arc::new(ShardedMioHandle::new(2));
        let driver = IoDriver::from_sharded_mio(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);

        let recovered = driver
            .as_sharded_mio_arc()
            .expect("sharded-mio vtable matches");
        assert_eq!(Arc::strong_count(&inner), 3);
        assert!(Arc::ptr_eq(&recovered, &inner));

        drop(recovered);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 1);
    }

    /// Cross-vtable identity: `as_uring*` returns `None` for a
    /// sharded-mio driver, and vice versa. Guards against accidental
    /// `vtable_is` mismatches if one of the statics gets shuffled
    /// across compilation units.
    #[cfg(all(feature = "io-uring-reactor", feature = "io-sharded-mio"))]
    #[test]
    fn vtable_identity_does_not_alias() {
        let uring_driver = IoDriver::from_uring(Arc::new(UringHandle::new(1)));
        let sharded_driver = IoDriver::from_sharded_mio(Arc::new(ShardedMioHandle::new(1)));

        assert!(uring_driver.as_uring().is_some());
        assert!(uring_driver.as_sharded_mio().is_none());
        assert!(sharded_driver.as_sharded_mio().is_some());
        assert!(sharded_driver.as_uring().is_none());
    }

    /// Mirror of [`uring_vtable_dispatch_and_refcount`] /
    /// [`sharded_mio_vtable_dispatch_and_refcount`] for the legacy
    /// single-shared-mio backend. Constructing an `IoDriver` from an
    /// `Arc<runtime::io::Handle>`, cloning, and dropping must keep
    /// the refcount honest. `num_workers` reads back as `1`
    /// (legacy mio is single-shard).
    #[test]
    fn legacy_mio_vtable_dispatch_and_refcount() {
        let (_drv, handle) = crate::runtime::io::Driver::new(1024)
            .expect("io::Driver::new");
        let inner = Arc::new(handle);
        let weak = Arc::downgrade(&inner);
        assert_eq!(Arc::strong_count(&inner), 1);

        let driver = IoDriver::from_legacy_mio(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);
        assert_eq!(driver.num_workers(), 1);
        assert!(driver.vtable_is(&LEGACY_MIO_VTABLE));

        let driver2 = driver.clone();
        assert_eq!(Arc::strong_count(&inner), 3);
        assert_eq!(driver2.num_workers(), 1);

        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver2);
        assert_eq!(Arc::strong_count(&inner), 1);

        drop(inner);
        assert!(weak.upgrade().is_none());
    }

    /// Identity-aliasing check for the legacy backend: a
    /// `LEGACY_MIO_VTABLE`-backed `IoDriver` reports `None` from
    /// `as_uring()` / `as_sharded_mio()`. The reverse direction is
    /// already covered by `vtable_identity_does_not_alias` (when
    /// both sharded features are on).
    #[cfg(any(feature = "io-uring-reactor", feature = "io-sharded-mio"))]
    #[test]
    fn legacy_mio_vtable_identity_does_not_alias() {
        let (_drv, handle) = crate::runtime::io::Driver::new(1024)
            .expect("io::Driver::new");
        let legacy_driver = IoDriver::from_legacy_mio(Arc::new(handle));

        assert!(legacy_driver.vtable_is(&LEGACY_MIO_VTABLE));
        #[cfg(feature = "io-uring-reactor")]
        assert!(legacy_driver.as_uring().is_none());
        #[cfg(feature = "io-sharded-mio")]
        assert!(legacy_driver.as_sharded_mio().is_none());
    }
}
