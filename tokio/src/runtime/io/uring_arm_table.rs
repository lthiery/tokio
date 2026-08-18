//! "Arm" table for the single shared uring reactor.
//!
//! Each [`POLL_ADD_MULTI`] registration lives in a slab slot on the one
//! ring, mutated only by whichever worker currently holds (drives) it. That
//! slab is `!Sync`: `slab::Slab`'s backing `Vec` may reallocate on insert,
//! invalidating any reference another thread tried to hold onto. So to let
//! cross-thread paths affect a live registration (specifically, to set a
//! "disarm this slot, cancel is inbound" flag) we publish a parallel,
//! stably-addressed table keyed on the same slab index.
//!
//! # Why not sharded-slab (the crate)?
//!
//! `sharded-slab::Slab::get(key)` returns a `Ref<'_>` guard that increments
//! and decrements a page refcount on every access. That refcount exists
//! to protect cross-shard readers from concurrent `remove` on another
//! shard's entry. In our model the ring's current holder is the **sole
//! remover** and it is also the **sole drain reader** (both serialized by
//! the reactor's `TryLock`), so the race the crate guards against cannot
//! happen, and the two atomics per drain CQE are pure overhead on a path
//! that runs once per readiness event.
//!
//! This table gives us the Sync structure we actually need: one
//! `AtomicU64` per slot, no per-read refcount, and stable addressing
//! reachable from any thread holding an `Arc<ArmTable>`.
//!
//! # Layout
//!
//! ```text
//! AtomicU64 state:
//!   bit  0      DISARMED    (1 = drain suppresses wake; cancel is the
//!                            owner's responsibility)
//!   bits 1..8   reserved
//!   bits 8..32  gen         (24-bit; matches `uring_reactor::encode`)
//!   bits 32..   reserved
//! ```
//!
//! Slot allocation is chunked: [`ARM_CHUNK`] slots per chunk, up to
//! [`ARM_MAX_CHUNKS`] chunks. Each chunk is cache-line-aligned and each
//! slot is itself cache-line-padded to prevent cross-thread false
//! sharing on the disarm write (other threads flip the disarm bit
//! concurrently). Chunks are allocated lazily by the ring's current holder
//! on first `publish` into their index range; pointer
//! stability is preserved for the lifetime of the table.
//!
//! [`POLL_ADD_MULTI`]: io_uring::opcode::PollAdd::multi

use std::alloc::{alloc_zeroed, Layout};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// Slots per allocation chunk. 64 slots × 64 bytes = one 4 KiB page. Keeps
/// cold-start allocation granular while matching OS page size for locality.
pub(crate) const ARM_CHUNK: usize = 64;

/// Maximum chunks. 4096 × 64 = 262 144 in-flight PollMulti
/// slots on the ring, several orders of magnitude above realistic peak fd
/// counts. The per-`ArmTable` pointer array costs 32 KiB up front;
/// individual chunks are allocated on demand.
pub(crate) const ARM_MAX_CHUNKS: usize = 4096;

/// Bit flag: the slot has been marked for teardown and no further wakes
/// should be delivered on its CQEs. Set by local deregister or by a
/// migrating peer; cleared only by re-publication of the slot for a
/// fresh registration.
const DISARMED: u64 = 1 << 0;

const GEN_SHIFT: u32 = 8;
const GEN_MASK_RAW: u64 = 0x00FF_FFFF; // 24 bits, matches user_data encoding.

#[inline]
fn pack(gen: u32) -> u64 {
    ((gen as u64) & GEN_MASK_RAW) << GEN_SHIFT
}

#[inline]
fn state_gen(state: u64) -> u32 {
    ((state >> GEN_SHIFT) & GEN_MASK_RAW) as u32
}

#[inline]
fn state_disarmed(state: u64) -> bool {
    (state & DISARMED) != 0
}

/// Single slot. Cache-line aligned to prevent false sharing on disarm
/// writes from other threads.
#[repr(C, align(64))]
struct ArmSlot {
    state: AtomicU64,
}

#[repr(C, align(64))]
struct ArmChunk {
    slots: [ArmSlot; ARM_CHUNK],
}

impl ArmChunk {
    /// Allocate a zero-initialized chunk. The all-zero bit pattern
    /// represents `state = 0` for every slot, i.e. gen = 0 and
    /// `DISARMED = 0`: a "no live registration" marker.
    fn new_zeroed() -> *mut Self {
        let layout = Layout::new::<Self>();
        // SAFETY: `ArmChunk` is `#[repr(C)]` and contains only `AtomicU64`
        // fields; the all-zero bit pattern is a valid representation for
        // each. `alloc_zeroed` guarantees zero-filled memory of the right
        // size and alignment.
        unsafe {
            let raw = alloc_zeroed(layout) as *mut ArmChunk;
            if raw.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            raw
        }
    }
}

/// Chunked, cross-thread-accessible table of per-slot arm state.
///
/// Indexed by slab key. Grown by the ring's current holder as new slab
/// slots are allocated; other threads read and RMW individual slot atoms
/// without ever mutating the `chunks` array itself.
pub(crate) struct ArmTable {
    /// Fixed-size pointer array. Each entry is lazily populated with a
    /// boxed `ArmChunk` on first `publish` into its index range.
    /// Published with `Release`, read with `Acquire`, so chunk contents
    /// (themselves atomics) are visible to any later reader.
    chunks: Box<[AtomicPtr<ArmChunk>]>,
}

impl std::fmt::Debug for ArmTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't walk the chunks; they contain live atomics touched by
        // other threads. Summarize capacity only.
        f.debug_struct("ArmTable")
            .field("max_chunks", &self.chunks.len())
            .field("chunk_size", &ARM_CHUNK)
            .finish_non_exhaustive()
    }
}

impl ArmTable {
    pub(crate) fn new() -> Self {
        let mut v = Vec::with_capacity(ARM_MAX_CHUNKS);
        for _ in 0..ARM_MAX_CHUNKS {
            v.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        Self {
            chunks: v.into_boxed_slice(),
        }
    }

    /// Locate (and install, if absent) the slot at `key`. Returns `None`
    /// if `key` is beyond the table's addressable capacity; callers must
    /// treat that as a failed publication, not a fatal error (an earlier
    /// version asserted here; the panic killed the worker thread and the
    /// runtime hung on its deaf ring).
    ///
    /// Called only from the ring's current holder on the `publish` path. The CAS
    /// handles the should-be-impossible race of two concurrent installs
    /// on the same chunk (single writer by contract, but the CAS keeps
    /// us safe if the contract is ever violated, e.g. by a test).
    fn slot_or_install(&self, key: u32) -> Option<&ArmSlot> {
        let chunk_idx = (key as usize) / ARM_CHUNK;
        let slot_idx = (key as usize) % ARM_CHUNK;
        if chunk_idx >= ARM_MAX_CHUNKS {
            return None;
        }
        let mut ptr = self.chunks[chunk_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            let new_chunk = ArmChunk::new_zeroed();
            match self.chunks[chunk_idx].compare_exchange(
                std::ptr::null_mut(),
                new_chunk,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => ptr = new_chunk,
                Err(existing) => {
                    // Lost the CAS: drop our allocation and use the
                    // winner's. SAFETY: `new_chunk` was just allocated
                    // by `alloc_zeroed` with `Layout::new::<ArmChunk>()`
                    // and never published anywhere.
                    unsafe {
                        std::alloc::dealloc(
                            new_chunk.cast::<u8>(),
                            Layout::new::<ArmChunk>(),
                        );
                    }
                    ptr = existing;
                }
            }
        }
        // SAFETY: once installed, the chunk pointer is stable for the
        // lifetime of the `ArmTable`. It points at a valid `ArmChunk`
        // that we (or a prior caller) allocated.
        let chunk: &ArmChunk = unsafe { &*ptr };
        Some(&chunk.slots[slot_idx])
    }

    /// Read-only slot access. Returns `None` if the chunk has never been
    /// installed, which means no registration has ever lived at this
    /// key, so there is no arm state to observe.
    fn slot(&self, key: u32) -> Option<&ArmSlot> {
        let chunk_idx = (key as usize) / ARM_CHUNK;
        let slot_idx = (key as usize) % ARM_CHUNK;
        if chunk_idx >= ARM_MAX_CHUNKS {
            return None;
        }
        let ptr = self.chunks[chunk_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }
        // SAFETY: as in `slot_or_install`; the chunk is stable once
        // installed.
        let chunk: &ArmChunk = unsafe { &*ptr };
        Some(&chunk.slots[slot_idx])
    }

    /// Publish a fresh registration at `key`. Called by the owning
    /// worker on `Reactor::register`, atomically setting gen to the new
    /// value and clearing the `DISARMED` bit (in case the slot was
    /// previously occupied by a torn-down registration).
    ///
    /// Returns `false` if `key` exceeds the table's capacity, in which
    /// case nothing was published and the caller must fail the
    /// registration (a slot the table can't address could never be
    /// disarmed, so arming a poll against it would leak).
    ///
    /// `Release` so any peer `Acquire` load in `try_disarm` sees the
    /// new gen immediately.
    pub(crate) fn try_publish(&self, key: u32, gen: u32) -> bool {
        match self.slot_or_install(key) {
            Some(slot) => {
                slot.state.store(pack(gen), Ordering::Release);
                true
            }
            None => false,
        }
    }

    /// Zero the slot after the kernel's terminal CQE for the slab entry
    /// at `key`. Called by the ring's current holder on slab removal.
    ///
    /// After `clear`, a peer's `try_disarm` call with the old gen will
    /// observe gen mismatch and correctly no-op. Not strictly required
    /// for correctness (`publish` also resets) but keeps arm state
    /// consistent with slab state, which makes debugging saner.
    pub(crate) fn clear(&self, key: u32) {
        if let Some(slot) = self.slot(key) {
            slot.state.store(0, Ordering::Release);
        }
    }

    /// Owning-worker drain-path read. Returns `true` if `DISARMED` is
    /// set for the slot.
    ///
    /// `Acquire` pairs with the `AcqRel` CAS in `try_disarm` so that
    /// any state written by the disarming worker before the CAS is
    /// visible here. On x86-64 this compiles to a plain `mov`, no
    /// cost vs. `Relaxed`.
    pub(crate) fn is_disarmed(&self, key: u32) -> bool {
        match self.slot(key) {
            Some(slot) => state_disarmed(slot.state.load(Ordering::Acquire)),
            None => false,
        }
    }

    /// Cross-thread: try to flip `DISARMED` from 0 to 1 for the slot
    /// at `key`, conditional on the stored gen matching `expected_gen`.
    ///
    /// Returns `true` if the caller flipped the bit; the caller then
    /// owns the responsibility of kicking off the actual cancellation
    /// (a POLL_REMOVE queued for the ring's current holder to submit).
    ///
    /// Returns `false` if:
    /// - the chunk containing `key` is uninstalled (no such slot);
    /// - gen has advanced past `expected_gen` (the slot was already
    ///   recycled by a new registration; the old one we wanted to
    ///   disarm is already gone);
    /// - `DISARMED` was already set (someone else beat us to it).
    ///
    /// In every "false" case the caller must not assume responsibility
    /// for cancelling the op; it will either be handled by another
    /// path or has already completed.
    pub(crate) fn try_disarm(&self, key: u32, expected_gen: u32) -> bool {
        let slot = match self.slot(key) {
            Some(s) => s,
            None => return false,
        };
        let mut cur = slot.state.load(Ordering::Acquire);
        loop {
            if state_gen(cur) != expected_gen {
                return false;
            }
            if state_disarmed(cur) {
                return false;
            }
            match slot.state.compare_exchange_weak(
                cur,
                cur | DISARMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

impl Drop for ArmTable {
    fn drop(&mut self) {
        for slot in self.chunks.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                // SAFETY: `ptr` was installed by `slot_or_install` via
                // `ArmChunk::new_zeroed` + `alloc_zeroed`; deallocate
                // with the matching layout.
                unsafe {
                    std::alloc::dealloc(ptr.cast::<u8>(), Layout::new::<ArmChunk>());
                }
            }
        }
    }
}

// `ArmTable` holds only raw pointers into a private allocation plus
// `AtomicPtr` / `AtomicU64` which are themselves `Send + Sync`. We never
// alias the `Box<ArmChunk>`: ownership passes once into `AtomicPtr` and
// stays there until `Drop`. Access through the pointer is exclusively
// via `&ArmSlot`, which is `Sync` thanks to its `AtomicU64`.
unsafe impl Send for ArmTable {}
unsafe impl Sync for ArmTable {}

// ===== tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_and_disarm_roundtrip() {
        let t = ArmTable::new();
        assert!(t.try_publish(7, 42));
        assert!(!t.is_disarmed(7));
        assert!(t.try_disarm(7, 42));
        assert!(t.is_disarmed(7));
        // Idempotent on second try: already disarmed, caller does not
        // own responsibility again.
        assert!(!t.try_disarm(7, 42));
    }

    #[test]
    fn gen_mismatch_rejects_disarm() {
        let t = ArmTable::new();
        assert!(t.try_publish(3, 100));
        assert!(!t.try_disarm(3, 99), "older gen should not disarm");
        assert!(!t.is_disarmed(3));
        assert!(!t.try_disarm(3, 101), "newer gen should not disarm");
        assert!(!t.is_disarmed(3));
        assert!(t.try_disarm(3, 100), "matching gen disarms");
    }

    #[test]
    fn republish_clears_disarm() {
        let t = ArmTable::new();
        assert!(t.try_publish(1, 10));
        assert!(t.try_disarm(1, 10));
        // Slab slot recycled with a new registration.
        assert!(t.try_publish(1, 11));
        assert!(!t.is_disarmed(1), "publish should reset DISARMED");
        assert!(!t.try_disarm(1, 10), "old gen disarm after republish must no-op");
        assert!(t.try_disarm(1, 11));
    }

    #[test]
    fn uninstalled_chunk_is_disarmable_noop() {
        let t = ArmTable::new();
        // Key in a chunk that was never installed.
        let far_key = (ARM_CHUNK * 50) as u32 + 3;
        assert!(!t.is_disarmed(far_key));
        assert!(!t.try_disarm(far_key, 0));
    }

    #[test]
    fn clear_after_publish_makes_gen_check_fail() {
        let t = ArmTable::new();
        assert!(t.try_publish(5, 200));
        t.clear(5);
        assert!(!t.try_disarm(5, 200));
    }

    #[test]
    fn multi_chunk_spans() {
        let t = ArmTable::new();
        for i in 0..ARM_CHUNK * 3 {
            assert!(t.try_publish(i as u32, (i as u32) + 1));
        }
        for i in 0..ARM_CHUNK * 3 {
            assert!(t.try_disarm(i as u32, (i as u32) + 1));
        }
    }

    #[test]
    fn cross_thread_disarm() {
        use std::sync::Arc;
        use std::thread;

        let t = Arc::new(ArmTable::new());
        assert!(t.try_publish(42, 7));

        let t2 = Arc::clone(&t);
        let h = thread::spawn(move || t2.try_disarm(42, 7));
        assert!(h.join().unwrap(), "peer thread should have flipped DISARMED");
        assert!(t.is_disarmed(42));
    }
}
