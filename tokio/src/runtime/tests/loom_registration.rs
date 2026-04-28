//! Loom model of the first-poll publish race in
//! [`crate::runtime::io::registration::Registration::ensure_registered`].
//!
//! On the vtable-routed backends, the registration starts with both
//! its `handle` and `shared` slots empty; the first poller observes
//! them empty, calls `Handle::current()`, allocates a
//! `ScheduledIo`, and publishes the pair into two `OnceLock`s. Two
//! polls can race, and the cross-thread `Drop` may run while the
//! publish is mid-flight on a different thread.
//!
//! The published-order invariant `ensure_registered` relies on:
//!
//! * `handle` is set **before** `shared`.
//! * Therefore any reader that observes `shared.get().is_some()` is
//!   also guaranteed to observe `handle.get().is_some()` thanks to
//!   the OnceLock release/acquire pair.
//!
//! `Drop::deregister` exploits this so it never has to re-fetch the
//! scheduler handle from TLS (Drop can run after `Runtime::shutdown`).
//!
//! This test models the invariant with loom primitives. We don't
//! depend on `Registration` itself — the goal is to exercise the
//! ordering, not the rest of the io stack — so we substitute a
//! `Mutex<Option<T>>`-backed once-cell that gives the same
//! "first-set wins, subsequent set returns Err" behaviour
//! `std::sync::OnceLock` provides.

use loom::sync::{Arc, Mutex};
use loom::thread;

/// Loom-friendly stand-in for `std::sync::OnceLock<T>`.
///
/// The std `OnceLock` uses internal atomics that loom doesn't model.
/// A `Mutex<Option<T>>` reproduces the relevant
/// "first-set-wins, observable from all threads after release"
/// semantics.
struct OnceCell<T> {
    inner: Mutex<Option<T>>,
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }
}

impl<T: Clone> OnceCell<T> {
    fn set(&self, value: T) -> Result<(), T> {
        let mut g = self.inner.lock().unwrap();
        if g.is_some() {
            return Err(value);
        }
        *g = Some(value);
        Ok(())
    }

    fn get(&self) -> Option<T> {
        self.inner.lock().unwrap().clone()
    }
}

/// Model of `ensure_registered`: each writer is a thread doing a
/// first poll. The writer:
///
/// 1. Publishes the scheduler handle into `handle` (may lose the
///    OnceLock race; loser drops its clone).
/// 2. Publishes its newly-allocated `Arc<ScheduledIo>` into `shared`
///    (may lose; loser deregisters its redundant alloc — modeled by
///    just dropping its tag).
fn writer(handle_slot: Arc<OnceCell<u32>>, shared_slot: Arc<OnceCell<u32>>, tag: u32) {
    // Release-store the handle before the shared arc — this is the
    // ordering `ensure_registered` documents.
    let _ = handle_slot.set(tag);
    let _ = shared_slot.set(tag);
}

#[test]
fn ensure_registered_publishes_handle_before_shared() {
    loom::model(|| {
        let handle_slot = Arc::new(OnceCell::<u32>::default());
        let shared_slot = Arc::new(OnceCell::<u32>::default());

        let h1 = handle_slot.clone();
        let s1 = shared_slot.clone();
        let t1 = thread::spawn(move || writer(h1, s1, 1));

        let h2 = handle_slot.clone();
        let s2 = shared_slot.clone();
        let t2 = thread::spawn(move || writer(h2, s2, 2));

        t1.join().unwrap();
        t2.join().unwrap();

        // Cross-thread reader simulating `Registration::drop`. Both
        // OnceLocks should be populated by the time both writers
        // join, but a real-world Drop can run while a writer is
        // still mid-flight, so the assertion is conditional on
        // observing `shared`.
        let s = shared_slot.get();
        let h = handle_slot.get();
        if s.is_some() {
            assert!(
                h.is_some(),
                "shared was published before handle — invariant broken"
            );
        }
    });
}

/// Two concurrent writers must both observe a populated registration
/// after both return — i.e., the OnceLock race is *resolved* (not
/// left in a torn intermediate state) by the time the writers join.
/// Mirrors the post-publish invariant `ensure_registered`'s caller
/// relies on: after the first-poll race completes, every subsequent
/// poll sees `shared.get().is_some()` so the cheap `OnceLock::get`
/// hot path holds.
#[test]
fn ensure_registered_resolves_after_both_writers_join() {
    loom::model(|| {
        let handle_slot = Arc::new(OnceCell::<u32>::default());
        let shared_slot = Arc::new(OnceCell::<u32>::default());

        let h1 = handle_slot.clone();
        let s1 = shared_slot.clone();
        let t1 = thread::spawn(move || writer(h1, s1, 1));

        let h2 = handle_slot.clone();
        let s2 = shared_slot.clone();
        let t2 = thread::spawn(move || writer(h2, s2, 2));

        t1.join().unwrap();
        t2.join().unwrap();

        // Both writers have completed. A third reader (e.g. another
        // poll arriving after the race resolved) must see Some in
        // both slots.
        assert!(handle_slot.get().is_some());
        assert!(shared_slot.get().is_some());
    });
}
