//! Provided-buffer ring for multi-shot recv (POC).
//!
//! A buffer ring lets us hand a pool of receive buffers to the kernel
//! up front; subsequent `RecvMulti` SQEs then draw from the pool
//! automatically via `IOSQE_BUFFER_SELECT`, returning the selected
//! buffer id in the CQE `flags` (see [`io_uring::cqueue::buffer_select`]).
//! This removes the per-recv SQE allocation, the per-recv slab entry,
//! and the per-recv `recv(2)`-equivalent syscall that an owned-`Bytes`
//! [`super::uring_reactor::Reactor::submit_recv_bytes`] currently pays.
//!
//! # Layout
//!
//! Two page-aligned allocations (kernel requirement for the ring; we
//! keep the data region page-aligned for consistency):
//!
//! * `ring`: `RING_ENTRIES × sizeof(io_uring_buf)` — the descriptor
//!   ring the kernel reads. Entry 0's `resv` field doubles as the
//!   shared tail cursor; we publish refills by release-storing into
//!   it (see [`BufRingEntry::tail`]).
//! * `data`: `RING_ENTRIES × BUF_LEN` bytes — the actual receive
//!   scratch, split evenly across the slots. Buffer id `i` occupies
//!   bytes `[i*BUF_LEN, (i+1)*BUF_LEN)`.
//!
//! # Lifetime / unregister
//!
//! The ring is registered with `register_buf_ring_with_flags` at
//! [`BufferRing::new_registered`] time. Unregister is the reactor's
//! responsibility and happens via [`BufferRing::unregister`] which
//! must be called on the owning worker's [`IoUring`] before the
//! reactor shuts down. The `BufferRing` itself is held behind an
//! `Arc`, so any outstanding [`BufferLease`]s (buffers that have been
//! handed to user code and not yet dropped) keep the allocation alive
//! past the reactor's lifetime; their drops still recycle the bid
//! into the ring memory, but the kernel will have long since stopped
//! consulting that ring.
//!
//! # Multishot & buffer selection flow
//!
//! ```text
//!   submit RecvMulti{fd, bgid}
//!          │
//!          ▼
//!   kernel: pops bid off this ring's head, recv()s into that slot,
//!           posts CQE(res=n, flags=F_BUFFER|F_MORE|(bid<<SHIFT))
//!          │
//!          ▼
//!   drain:  res, bid ← cqueue::buffer_select(flags)
//!           deliver (res, BufferLease{bgid, bid, len=res})
//!          │
//!          ▼ (user drops lease when done)
//!   release(bid):  rewrite slot `bid` at tail, RELEASE-store tail+1
//! ```
//!
//! # POC status
//!
//! Capacity is fixed: one group per reactor, group id 0, 256 × 4 KiB.
//! Resizing and multiple groups per reactor are deliberately deferred
//! until the proof-of-concept benchmark shows the approach pays off.

use io_uring::types::BufRingEntry;
use io_uring::IoUring;

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::io;
use std::os::fd::RawFd;
use std::ptr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Mutex;

/// Number of receive slots per ring. Must be a power of two so the
/// kernel's head/tail wrap via `& (entries - 1)` works.
pub(crate) const RING_ENTRIES: u16 = 256;

/// Mask for `tail & MASK` addressing of the `RING_ENTRIES` slots.
const RING_MASK: u16 = RING_ENTRIES - 1;

/// Per-slot buffer length. Sized to cover the small- and mid-payload
/// benchmark variants (≤4 KiB) in a single recv; larger messages will
/// simply deliver multiple CQEs.
pub(crate) const BUF_LEN: usize = 4096;

/// Buffer group id used by the reactor's single ring. Picked as 0 for
/// the POC; a real implementation would allocate per-ring.
pub(crate) const DEFAULT_BGID: u16 = 0;

/// Per-reactor provided-buffer ring.
///
/// Held behind an `Arc`: the reactor holds one reference, each
/// outstanding [`BufferLease`] holds another. Memory survives as long
/// as any reference exists; the kernel registration is torn down
/// explicitly by the reactor via [`BufferRing::unregister`].
pub(crate) struct BufferRing {
    /// Base of the `RING_ENTRIES`-long `BufRingEntry` array. Page
    /// aligned per the io_uring pbuf ring requirement.
    ring_ptr: *mut BufRingEntry,
    ring_layout: Layout,

    /// Base of the backing data region — `RING_ENTRIES * BUF_LEN`
    /// bytes, also page-aligned (not strictly required, but keeps
    /// slot starts on their own cacheline-class boundary).
    data_ptr: *mut u8,
    data_layout: Layout,

    /// Buffer group id. A future multi-group design would partition
    /// by bgid; for now every reactor uses [`DEFAULT_BGID`].
    bgid: u16,

    /// Shadow of the kernel's tail cursor. We own writes (via
    /// [`Self::release`]); the kernel only reads it. The actual
    /// memory location the kernel consults is entry 0's `resv`
    /// field, addressed through [`BufRingEntry::tail`]; this field
    /// is a same-value cache we use to compute the next slot index
    /// without reloading from the shared cursor.
    tail: AtomicU16,

    /// Serializes concurrent [`Self::release`] callers. Lease drops
    /// are expected to happen on worker threads but may occur from
    /// anywhere (a user could `tokio::spawn` a task that holds a
    /// lease and gets scheduled off the worker), so we lock.
    refill: Mutex<()>,
}

// SAFETY: `ring_ptr` and `data_ptr` are stable heap allocations owned
// by `self`. All concurrent access to the ring memory is either
// (a) the kernel reading entry 0's tail via release/acquire pairing
// with our `tail` store, or (b) `release` callers mutating the slot
// they hold exclusive ownership of (the `bid` in the `BufferLease`
// they are dropping) under `refill`.
unsafe impl Send for BufferRing {}
unsafe impl Sync for BufferRing {}

impl BufferRing {
    /// Allocate, initialize, and register a new buffer ring with the
    /// given `IoUring`.
    ///
    /// All `RING_ENTRIES` slots are published to the kernel at
    /// construction time: the ring is fully usable as soon as this
    /// returns.
    ///
    /// # Safety
    ///
    /// Callers must ensure `ring` is the submitter's ring (i.e. the
    /// reactor calls this on its own worker thread). The registration
    /// is a non-submission operation so it does not conflict with
    /// `SINGLE_ISSUER`.
    pub(crate) fn new_registered(ring: &IoUring) -> io::Result<Self> {
        let page = page_size();
        // Round each allocation's alignment up to the page size — the
        // pbuf ring requires page alignment, and aligning the data
        // region too keeps slot starts well-behaved.
        let ring_bytes = RING_ENTRIES as usize * std::mem::size_of::<BufRingEntry>();
        let data_bytes = RING_ENTRIES as usize * BUF_LEN;

        let ring_layout = Layout::from_size_align(ring_bytes, page)
            .expect("ring layout valid");
        let data_layout = Layout::from_size_align(data_bytes, page)
            .expect("data layout valid");

        // SAFETY: both layouts are non-zero-sized; `alloc_zeroed`
        // returns null on OOM which we convert to an io::Error.
        let ring_ptr = unsafe { alloc_zeroed(ring_layout) } as *mut BufRingEntry;
        if ring_ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "buf_ring alloc"));
        }
        // SAFETY: as above.
        let data_ptr = unsafe { alloc_zeroed(data_layout) };
        if data_ptr.is_null() {
            // SAFETY: `ring_ptr` came from the matching layout.
            unsafe { dealloc(ring_ptr as *mut u8, ring_layout) };
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "buf_data alloc"));
        }

        // Publish every slot. Entry i gets buffer id i, addr pointing
        // at data_ptr + i*BUF_LEN, len BUF_LEN. No tail update yet —
        // we batch the release-store below.
        for i in 0..RING_ENTRIES {
            // SAFETY: `ring_ptr[i]` is within the allocation we just
            // zeroed; `BufRingEntry` is `repr(transparent)` over a
            // POD struct.
            unsafe {
                let entry = &mut *ring_ptr.add(i as usize);
                entry.set_addr(data_ptr.add(i as usize * BUF_LEN) as u64);
                entry.set_len(BUF_LEN as u32);
                entry.set_bid(i);
            }
        }

        // Register the ring with the kernel *before* publishing the
        // tail: until registration lands, the kernel has no ring for
        // bgid, and any recv that raced would fail with -ENOBUFS. In
        // practice the reactor calls `new_registered` before
        // submitting any `RecvMulti`, so ordering is enforced by the
        // call site; we keep the order here for defense-in-depth.
        //
        // SAFETY: `ring_ptr` and length are valid for the full
        // lifetime of `self`; the `Drop` impl frees them only after
        // `unregister_buf_ring` has been called (via
        // [`Self::unregister`]).
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                ring_ptr as u64,
                RING_ENTRIES,
                DEFAULT_BGID,
                0,
            )
        }
        .map_err(|e| {
            // Roll back the allocations on registration failure —
            // older kernels or running without `CAP_SYS_ADMIN` / the
            // right mount options can refuse `IORING_REGISTER_PBUF_RING`.
            // SAFETY: both layouts match their allocations.
            unsafe {
                dealloc(data_ptr, data_layout);
                dealloc(ring_ptr as *mut u8, ring_layout);
            }
            e
        })?;

        // Publish all 256 entries atomically. The kernel's side
        // pairs this Release store with its Acquire load on entry
        // 0's `resv` when it next tries to select a buffer.
        //
        // SAFETY: `ring_ptr` is a valid, properly-aligned, page-
        // aligned `BufRingEntry` array base with length
        // `RING_ENTRIES`; `BufRingEntry::tail` returns a pointer
        // into the first entry's `resv` which we own.
        let tail_raw = unsafe { BufRingEntry::tail(ring_ptr as *const BufRingEntry) } as *mut u16;
        // SAFETY: `tail_raw` points to a properly-aligned `u16` in
        // our owned allocation. Casting to `&AtomicU16` is sound
        // because `AtomicU16` has the same in-memory layout as
        // `u16` and we will only ever access this location through
        // atomic operations from now on. (On MSRV 1.75+ we would
        // use `AtomicU16::from_ptr`; tokio's MSRV is 1.71.)
        let atomic_tail: &AtomicU16 = unsafe { &*(tail_raw as *const AtomicU16) };
        atomic_tail.store(RING_ENTRIES, Ordering::Release);

        Ok(Self {
            ring_ptr,
            ring_layout,
            data_ptr,
            data_layout,
            bgid: DEFAULT_BGID,
            tail: AtomicU16::new(RING_ENTRIES),
            refill: Mutex::new(()),
        })
    }

    /// Buffer group id for SQE construction
    /// ([`io_uring::opcode::RecvMulti::new`] takes `buf_group`).
    pub(crate) fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Materialize a read-only view of the data delivered into buffer
    /// `bid`. `len` is the kernel's `res` byte count from the CQE and
    /// must be `<= BUF_LEN`.
    ///
    /// # Safety
    ///
    /// Caller must hold a [`BufferLease`] that owns `bid` (i.e. it was
    /// handed to them by a terminal CQE drain and not yet dropped),
    /// and must not construct overlapping views of the same `bid`.
    pub(crate) unsafe fn slice(&self, bid: u16, len: u32) -> &[u8] {
        debug_assert!(bid < RING_ENTRIES);
        debug_assert!(len as usize <= BUF_LEN);
        // SAFETY: contract delegated to caller. The underlying
        // allocation lives as long as `self`.
        unsafe {
            std::slice::from_raw_parts(
                self.data_ptr.add(bid as usize * BUF_LEN),
                len as usize,
            )
        }
    }

    /// Re-publish buffer `bid` to the kernel. Called from the
    /// [`BufferLease`] drop path after the user is done reading.
    ///
    /// Serialized by `refill`: a lease can be dropped from any
    /// thread.
    pub(crate) fn release(&self, bid: u16) {
        debug_assert!(bid < RING_ENTRIES);

        let _guard = self.refill.lock().unwrap_or_else(|p| p.into_inner());

        // Current tail — only we write it, so Relaxed under the lock
        // is fine.
        let tail = self.tail.load(Ordering::Relaxed);
        let slot = tail & RING_MASK;

        // Rewrite the slot. len stays BUF_LEN (kernel may have
        // shrunk `len` on a short recv; we restore capacity), bid
        // stays the same id we got back — addr already points into
        // the correct data-region window and never changes.
        //
        // SAFETY: `ring_ptr[slot]` is within the owned allocation.
        unsafe {
            let entry = &mut *self.ring_ptr.add(slot as usize);
            entry.set_addr(self.data_ptr.add(bid as usize * BUF_LEN) as u64);
            entry.set_len(BUF_LEN as u32);
            entry.set_bid(bid);
        }

        // Release-store the advanced tail so the kernel observes a
        // fully-initialized slot.
        //
        // SAFETY: same justification as in `new_registered` — the
        // resv field of entry 0 is the shared tail cursor.
        let tail_raw = unsafe {
            BufRingEntry::tail(self.ring_ptr as *const BufRingEntry)
        } as *mut u16;
        // SAFETY: aligned, owned, only accessed atomically.
        let atomic_tail: &AtomicU16 = unsafe { &*(tail_raw as *const AtomicU16) };
        let new_tail = tail.wrapping_add(1);
        atomic_tail.store(new_tail, Ordering::Release);
        self.tail.store(new_tail, Ordering::Relaxed);
    }

    /// Unregister this ring from the kernel. Must be called on the
    /// owning reactor's thread, on the same `IoUring` passed to
    /// [`Self::new_registered`], before the `BufferRing` is dropped.
    ///
    /// After unregister, in-flight `RecvMulti` ops for this bgid
    /// will complete with `-ENOBUFS` (or, more commonly, have
    /// already completed because the reactor cancels them during
    /// shutdown). Lease drops that race with unregister only touch
    /// our owned ring memory — safe — the kernel simply stops
    /// reading it.
    #[allow(dead_code)]
    pub(crate) fn unregister(&self, ring: &IoUring, _ring_fd: RawFd) -> io::Result<()> {
        ring.submitter().unregister_buf_ring(self.bgid)
    }
}

impl Drop for BufferRing {
    fn drop(&mut self) {
        // SAFETY: both pointers came from `alloc_zeroed` with the
        // stored layouts; no other references can exist because the
        // drop runs only when the last `Arc<BufferRing>` goes away.
        unsafe {
            if !self.data_ptr.is_null() {
                dealloc(self.data_ptr, self.data_layout);
                self.data_ptr = ptr::null_mut();
            }
            if !self.ring_ptr.is_null() {
                dealloc(self.ring_ptr as *mut u8, self.ring_layout);
                self.ring_ptr = ptr::null_mut();
            }
        }
    }
}

/// An owned view of one received buffer. Derefs to `&[u8]`. Drops
/// recycle the buffer id back into the kernel ring.
pub struct BufferLease {
    ring: crate::loom::sync::Arc<BufferRing>,
    bid: u16,
    len: u32,
}

impl BufferLease {
    /// Construct a lease. Called by the reactor drain loop after
    /// decoding a multishot CQE.
    pub(crate) fn new(
        ring: crate::loom::sync::Arc<BufferRing>,
        bid: u16,
        len: u32,
    ) -> Self {
        Self { ring, bid, len }
    }

    /// The kernel's delivered byte count.
    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    /// Buffer id — useful for diagnostics.
    #[allow(dead_code)]
    pub(crate) fn bid(&self) -> u16 {
        self.bid
    }
}

impl std::ops::Deref for BufferLease {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: we uniquely own `bid` (no two leases for the same
        // bid exist because the kernel hands each bid out at most
        // once between consecutive recycles, and we recycle only at
        // drop) and `len <= BUF_LEN` was checked by the drain loop.
        unsafe { self.ring.slice(self.bid, self.len) }
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        self.ring.release(self.bid);
    }
}

impl std::fmt::Debug for BufferLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferLease")
            .field("bid", &self.bid)
            .field("len", &self.len)
            .finish()
    }
}

/// System page size via `sysconf`. Cached on first call.
fn page_size() -> usize {
    use std::sync::atomic::AtomicUsize;
    static CACHE: AtomicUsize = AtomicUsize::new(0);
    let cached = CACHE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    // SAFETY: `sysconf` with `_SC_PAGESIZE` is a pure query.
    let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if raw > 0 { raw as usize } else { 4096 };
    CACHE.store(page, Ordering::Relaxed);
    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::IoUring;

    fn build_ring() -> Option<IoUring> {
        IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(32)
            .ok()
    }

    #[test]
    fn register_and_unregister_roundtrip() {
        let Some(ring) = build_ring() else {
            eprintln!("skipping: io_uring unavailable");
            return;
        };
        let br = match BufferRing::new_registered(&ring) {
            Ok(b) => b,
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                eprintln!("skipping: PBUF_RING unsupported: {e}");
                return;
            }
            Err(e) => panic!("unexpected: {e}"),
        };
        br.unregister(&ring, 0).expect("unregister");
    }

    #[test]
    fn release_advances_tail() {
        let Some(ring) = build_ring() else {
            return;
        };
        let Ok(br) = BufferRing::new_registered(&ring) else {
            eprintln!("skipping");
            return;
        };
        let baseline = br.tail.load(Ordering::Relaxed);
        br.release(7);
        assert_eq!(br.tail.load(Ordering::Relaxed), baseline.wrapping_add(1));
        br.unregister(&ring, 0).expect("unregister");
    }
}
