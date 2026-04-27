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
use crate::runtime::io::ScheduledIo;
use crate::runtime::io::registration::RegistrationSource;

use std::io;
use std::ptr::NonNull;
use std::sync::Arc;

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
    /// Register `source` with `interest`. Returns the `ScheduledIo` arc
    /// the caller will park readiness against, plus the worker index the
    /// fd was placed on (for `deregister` / `unpark` later).
    pub add_source: unsafe fn(
        NonNull<()>,
        &mut dyn RegistrationSource,
        Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)>,

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

    pub(crate) fn add_source(
        &self,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        // SAFETY: see `num_workers`.
        unsafe { (self.vtable.add_source)(self.data, source, interest) }
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
        add_source:    uring_add_source,
        deregister:    uring_deregister,
        unpark_worker: uring_unpark_worker,
        num_workers:   uring_num_workers,
        clone_data:    uring_clone_data,
        drop_data:     uring_drop_data,
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

    unsafe fn uring_add_source(
        data: NonNull<()>,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        let handle = unsafe { as_uring(data) };
        let fd = source.registration_raw_fd();
        handle.add_source(fd, interest)
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
        // `Arc<UringHandle>`. Bumping the strong count keeps that Arc
        // alive; the returned pointer aliases the same allocation.
        unsafe { Arc::<UringHandle>::increment_strong_count(data.as_ptr() as *const UringHandle); }
        data
    }

    unsafe fn uring_drop_data(data: NonNull<()>) {
        // SAFETY: `data` was produced by `Arc::into_raw` for an
        // `Arc<UringHandle>`. `decrement_strong_count` reconstructs the
        // owning Arc and drops it, freeing the allocation if this was
        // the last reference.
        unsafe { Arc::<UringHandle>::decrement_strong_count(data.as_ptr() as *const UringHandle); }
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
                // `Arc::into_raw(_: Arc<UringHandle>)`. We bump the count
                // and reconstruct an Arc that we hand out; the original
                // Arc inside `self` remains valid.
                unsafe {
                    Arc::<UringHandle>::increment_strong_count(
                        self.data.as_ptr() as *const UringHandle,
                    );
                    Some(Arc::from_raw(self.data.as_ptr() as *const UringHandle))
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
        add_source:    sharded_mio_add_source,
        deregister:    sharded_mio_deregister,
        unpark_worker: sharded_mio_unpark_worker,
        num_workers:   sharded_mio_num_workers,
        clone_data:    sharded_mio_clone_data,
        drop_data:     sharded_mio_drop_data,
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

    unsafe fn sharded_mio_add_source(
        data: NonNull<()>,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        let handle = unsafe { as_sharded_mio_handle(data) };
        // Forwarding `&mut dyn RegistrationSource` into the generic
        // `S: RegistrationSource + ?Sized` parameter resolves
        // `S = dyn RegistrationSource`; `mio::Registry::register`
        // accepts `?Sized` sources, so the inner mio call works
        // through the trait object's vtable.
        handle.add_source(source, interest)
    }

    unsafe fn sharded_mio_deregister(
        data: NonNull<()>,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        let handle = unsafe { as_sharded_mio_handle(data) };
        // Worker index is read off the `ScheduledIo` inside
        // `deregister_source`; no caller-tracked index needed.
        handle.deregister_source(io, source)
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
        // SAFETY: `data` was produced by `Arc::into_raw` for an
        // `Arc<ShardedMioHandle>`. Bumping the strong count keeps that
        // Arc alive; the returned pointer aliases the same allocation.
        unsafe {
            Arc::<ShardedMioHandle>::increment_strong_count(
                data.as_ptr() as *const ShardedMioHandle,
            );
        }
        data
    }

    unsafe fn sharded_mio_drop_data(data: NonNull<()>) {
        // SAFETY: `data` was produced by `Arc::into_raw` for an
        // `Arc<ShardedMioHandle>`. `decrement_strong_count`
        // reconstructs the owning Arc and drops it, freeing the
        // allocation if this was the last reference.
        unsafe {
            Arc::<ShardedMioHandle>::decrement_strong_count(
                data.as_ptr() as *const ShardedMioHandle,
            );
        }
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
                // `Arc::into_raw(_: Arc<ShardedMioHandle>)`. We bump
                // the count and reconstruct an Arc that we hand out;
                // the original Arc inside `self` remains valid.
                unsafe {
                    Arc::<ShardedMioHandle>::increment_strong_count(
                        self.data.as_ptr() as *const ShardedMioHandle,
                    );
                    Some(Arc::from_raw(
                        self.data.as_ptr() as *const ShardedMioHandle,
                    ))
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
// Tests
// =====================================================================

#[cfg(all(
    test,
    tokio_unstable,
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
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
}
