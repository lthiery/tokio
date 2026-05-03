// Currently, rust warns when an unsafe fn contains an unsafe {} block. However,
// in the future, this will change to the reverse. For now, suppress this
// warning and generally stick with being explicit about unsafety.
#![allow(unused_unsafe)]
#![cfg_attr(not(feature = "rt"), allow(dead_code))]

//! Time driver.

mod entry;
pub(crate) use entry::TimerEntry;
use entry::{EntryList, TimerHandle, TimerShared, MAX_SAFE_MILLIS_DURATION};

mod handle;
pub(crate) use self::handle::Handle;

mod source;
pub(crate) use source::TimeSource;

mod wheel;

#[cfg(feature = "rt-alt-timer")]
use super::time_alt;

use crate::loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::loom::sync::Mutex;
use crate::runtime::driver::{self, IoHandle, IoStack};
use crate::time::error::Error;
use crate::time::{Clock, Duration};
use crate::util::WakeList;

use std::fmt;
use std::{num::NonZeroU64, ptr::NonNull};

/// Time implementation that drives [`Sleep`][sleep], [`Interval`][interval], and [`Timeout`][timeout].
///
/// A `Driver` instance tracks the state necessary for managing time and
/// notifying the [`Sleep`][sleep] instances once their deadlines are reached.
///
/// It is expected that a single instance manages many individual [`Sleep`][sleep]
/// instances. The `Driver` implementation is thread-safe and, as such, is able
/// to handle callers from across threads.
///
/// After creating the `Driver` instance, the caller must repeatedly call `park`
/// or `park_timeout`. The time driver will perform no work unless `park` or
/// `park_timeout` is called repeatedly.
///
/// The driver has a resolution of one millisecond. Any unit of time that falls
/// between milliseconds are rounded up to the next millisecond.
///
/// When an instance is dropped, any outstanding [`Sleep`][sleep] instance that has not
/// elapsed will be notified with an error. At this point, calling `poll` on the
/// [`Sleep`][sleep] instance will result in panic.
///
/// # Implementation
///
/// The time driver is based on the [paper by Varghese and Lauck][paper].
///
/// A hashed timing wheel is a vector of slots, where each slot handles a time
/// slice. As time progresses, the timer walks over the slot for the current
/// instant, and processes each entry for that slot. When the timer reaches the
/// end of the wheel, it starts again at the beginning.
///
/// The implementation maintains six wheels arranged in a set of levels. As the
/// levels go up, the slots of the associated wheel represent larger intervals
/// of time. At each level, the wheel has 64 slots. Each slot covers a range of
/// time equal to the wheel at the lower level. At level zero, each slot
/// represents one millisecond of time.
///
/// The wheels are:
///
/// * Level 0: 64 x 1 millisecond slots.
/// * Level 1: 64 x 64 millisecond slots.
/// * Level 2: 64 x ~4 second slots.
/// * Level 3: 64 x ~4 minute slots.
/// * Level 4: 64 x ~4 hour slots.
/// * Level 5: 64 x ~12 day slots.
///
/// When the timer processes entries at level zero, it will notify all the
/// `Sleep` instances as their deadlines have been reached. For all higher
/// levels, all entries will be redistributed across the wheel at the next level
/// down. Eventually, as time progresses, entries with [`Sleep`][sleep] instances will
/// either be canceled (dropped) or their associated entries will reach level
/// zero and be notified.
///
/// [paper]: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
/// [sleep]: crate::time::Sleep
/// [timeout]: crate::time::Timeout
/// [interval]: crate::time::Interval
#[derive(Debug)]
pub(crate) struct Driver {
    /// Parker to delegate to.
    park: IoStack,
}

enum Inner {
    Traditional {
        // The state is split like this so `Handle` can access `is_shutdown` without locking the mutex
        state: Mutex<InnerState>,

        /// True if the driver is being shutdown.
        is_shutdown: AtomicBool,

        /// Lock-free mirror of `state.next_wake`.
        ///
        /// Holds the earliest pending tick known to the wheel, or
        /// [`NO_TIMER`] (= `u64::MAX`) when no timer is registered.
        ///
        /// This is updated under the `state` mutex by every code path that
        /// also writes `lock.next_wake` (`Driver::park_internal`,
        /// `Handle::next_wake_tick`, `Handle::process_at_time`), and by
        /// `Handle::reregister` when an insert lowers the minimum.
        ///
        /// Sharded-mio and io-uring parkers read it without taking the
        /// mutex on the pre-park (`next_wake_tick`) and post-park
        /// (`parker_process`) hot paths, allowing them to skip the wheel
        /// walk entirely when no timer is registered. The cached value is
        /// conservative: it is never larger than any actually-pending
        /// deadline, so a reader that observes [`Self::NO_TIMER`] can
        /// safely conclude the wheel is empty (any concurrent registrar
        /// will have called `unpark` after lowering it).
        ///
        /// The wheel cannot represent ticks beyond `1 << 36` (≈ 2 years
        /// from boot at millisecond resolution), so `u64::MAX` is safe to
        /// use as an "empty" sentinel without aliasing any real deadline.
        next_wake_atomic: AtomicU64,

        // When `true`, a call to `park_timeout` should immediately return and time
        // should not advance. One reason for this to be `true` is if the task
        // passed to `Runtime::block_on` called `task::yield_now()`.
        //
        // While it may look racy, it only has any effect when the clock is paused
        // and pausing the clock is restricted to a single-threaded runtime.
        #[cfg(feature = "test-util")]
        did_wake: AtomicBool,
    },

    #[cfg(feature = "rt-alt-timer")]
    Alternative {
        /// True if the driver is being shutdown.
        is_shutdown: AtomicBool,

        // When `true`, a call to `park_timeout` should immediately return and time
        // should not advance. One reason for this to be `true` is if the task
        // passed to `Runtime::block_on` called `task::yield_now()`.
        //
        // While it may look racy, it only has any effect when the clock is paused
        // and pausing the clock is restricted to a single-threaded runtime.
        #[cfg(feature = "test-util")]
        did_wake: AtomicBool,
    },
}

/// Time state shared which must be protected by a `Mutex`
struct InnerState {
    /// The earliest time at which we promise to wake up without unparking.
    next_wake: Option<NonZeroU64>,

    /// Timer wheel.
    wheel: wheel::Wheel,
}

/// Sentinel for `Inner::next_wake_atomic` indicating no timer is registered.
const NO_TIMER: u64 = u64::MAX;

/// Converts an `Option<u64>` next-wake tick into the atomic encoding.
#[inline]
fn next_wake_to_atomic(next_wake: Option<u64>) -> u64 {
    next_wake.unwrap_or(NO_TIMER)
}

// ===== impl Driver =====

impl Driver {
    /// Creates a new `Driver` instance that uses `park` to block the current
    /// thread and `time_source` to get the current time and convert to ticks.
    ///
    /// Specifying the source of time is useful when testing.
    pub(crate) fn new(park: IoStack, clock: &Clock) -> (Driver, Handle) {
        let time_source = TimeSource::new(clock);

        let handle = Handle {
            time_source,
            inner: Inner::Traditional {
                state: Mutex::new(InnerState {
                    next_wake: None,
                    wheel: wheel::Wheel::new(),
                }),
                is_shutdown: AtomicBool::new(false),
                next_wake_atomic: AtomicU64::new(NO_TIMER),

                #[cfg(feature = "test-util")]
                did_wake: AtomicBool::new(false),
            },
        };

        let driver = Driver { park };

        (driver, handle)
    }

    #[cfg(feature = "rt-alt-timer")]
    pub(crate) fn new_alt(clock: &Clock) -> Handle {
        let time_source = TimeSource::new(clock);

        Handle {
            time_source,
            inner: Inner::Alternative {
                is_shutdown: AtomicBool::new(false),
                #[cfg(feature = "test-util")]
                did_wake: AtomicBool::new(false),
            },
        }
    }

    pub(crate) fn park(&mut self, handle: &driver::Handle) {
        self.park_internal(handle, None);
    }

    pub(crate) fn park_timeout(&mut self, handle: &driver::Handle, duration: Duration) {
        self.park_internal(handle, Some(duration));
    }

    pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.time();

        if handle.is_shutdown() {
            return;
        }

        match &handle.inner {
            Inner::Traditional { is_shutdown, .. } => {
                is_shutdown.store(true, Ordering::SeqCst);
            }
            #[cfg(feature = "rt-alt-timer")]
            Inner::Alternative { is_shutdown, .. } => {
                is_shutdown.store(true, Ordering::SeqCst);
            }
        }

        // Advance time forward to the end of time.

        handle.process_at_time(u64::MAX);

        self.park.shutdown(rt_handle);
    }

    fn park_internal(&mut self, rt_handle: &driver::Handle, limit: Option<Duration>) {
        let handle = rt_handle.time();
        let mut lock = handle.inner.lock();

        assert!(!handle.is_shutdown());

        let next_wake = lock.wheel.next_expiration_time();
        lock.next_wake =
            next_wake.map(|t| NonZeroU64::new(t).unwrap_or_else(|| NonZeroU64::new(1).unwrap()));
        // Mirror to lock-free cache so sharded-mio/uring fast-path readers
        // observe the same value without taking the mutex.
        handle
            .inner
            .next_wake_atomic()
            .store(next_wake_to_atomic(next_wake), Ordering::Release);

        drop(lock);

        match next_wake {
            Some(when) => {
                let now = handle.time_source.now(rt_handle.clock());
                // Note that we effectively round up to 1ms here - this avoids
                // very short-duration microsecond-resolution sleeps that the OS
                // might treat as zero-length.
                let mut duration = handle
                    .time_source
                    .tick_to_duration(when.saturating_sub(now));

                if duration > Duration::from_millis(0) {
                    if let Some(limit) = limit {
                        duration = std::cmp::min(limit, duration);
                    }

                    self.park_thread_timeout(rt_handle, duration);
                } else {
                    self.park.park_timeout(rt_handle, Duration::from_secs(0));
                }
            }
            None => {
                if let Some(duration) = limit {
                    self.park_thread_timeout(rt_handle, duration);
                } else {
                    self.park.park(rt_handle);
                }
            }
        }

        // Process pending timers after waking up
        handle.process(rt_handle.clock());
    }

    cfg_test_util! {
        fn park_thread_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
            let handle = rt_handle.time();
            let clock = rt_handle.clock();

            if clock.can_auto_advance() {
                self.park.park_timeout(rt_handle, Duration::from_secs(0));

                // If the time driver was woken, then the park completed
                // before the "duration" elapsed (usually caused by a
                // yield in `Runtime::block_on`). In this case, we don't
                // advance the clock.
                if !handle.did_wake() {
                    // Simulate advancing time
                    if let Err(msg) = clock.advance(duration) {
                        panic!("{}", msg);
                    }
                }
            } else {
                self.park.park_timeout(rt_handle, duration);
            }
        }
    }

    cfg_not_test_util! {
        fn park_thread_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
            self.park.park_timeout(rt_handle, duration);
        }
    }
}

impl Handle {
    pub(self) fn process(&self, clock: &Clock) {
        let now = self.time_source().now(clock);

        self.process_at_time(now);
    }

    /// Wrapper around [`Handle::process`] used by the sharded-mio parker.
    ///
    /// In the legacy + traditional-park path, the time driver wraps the
    /// IoStack, so `Driver::park_internal` reads `next_wake` and processes
    /// the wheel after wake. Sharded-mio bypasses that wrapper (each worker
    /// owns its own `mio::Poll`), so the parker has to advance the wheel
    /// itself. This is the entry point for that.
    ///
    /// Includes a lock-free empty-wheel fast path: if no timer is
    /// registered, this returns without acquiring the inner mutex or
    /// walking the wheel. This makes timer-free workloads (e.g. pure
    /// `Notify` / `watch` notification storms) pay zero cost per park
    /// for the timer subsystem under sharded-mio / io-uring.
    #[cfg(all(
        any(feature = "io-sharded-mio", feature = "io-uring-reactor"),
        target_os = "linux",
    ))]
    pub(crate) fn parker_process(&self, clock: &Clock) {
        // Fast path: nothing to fire if the wheel is known-empty.
        if self.inner.next_wake_atomic().load(Ordering::Acquire) == NO_TIMER {
            return;
        }
        self.process(clock);
    }

    /// Returns the absolute tick of the next pending timer, or `None` if
    /// no timers are registered. Sharded-mio's parker uses this to compute
    /// its `mio::Poll::poll` timeout (`min(io_timeout, time_timeout)`).
    ///
    /// Mirrors the pre-park logic in [`Driver::park_internal`]: queries the
    /// wheel directly (so newly-registered timers that haven't been through
    /// `process_at_time` yet are still seen) and republishes the result
    /// into `lock.next_wake` so that subsequent timer registrations can use
    /// the standard `add_entry` short-circuit (`when < next_wake → unpark`).
    ///
    /// On the empty-wheel fast path, this is wait-free: it reads
    /// `next_wake_atomic` (an `Acquire` load) and returns `None` if no
    /// timer is registered, skipping the wheel walk and mutex acquisition
    /// entirely. The non-empty path still locks and re-derives the value
    /// from the wheel so newly-registered timers are picked up.
    #[cfg(all(
        any(feature = "io-sharded-mio", feature = "io-uring-reactor"),
        target_os = "linux",
    ))]
    pub(crate) fn next_wake_tick(&self) -> Option<u64> {
        // Fast path: lock-free empty check. If the cache says NO_TIMER,
        // the wheel has been observed empty and any subsequent registrar
        // will (a) lower the cached value and (b) call `unpark` before
        // we could miss the wake. Safe to short-circuit.
        if self.inner.next_wake_atomic().load(Ordering::Acquire) == NO_TIMER {
            return None;
        }
        let mut lock = self.inner.lock();
        let next_wake = lock.wheel.next_expiration_time();
        lock.next_wake = next_wake
            .map(|t| NonZeroU64::new(t).unwrap_or_else(|| NonZeroU64::new(1).unwrap()));
        // Refresh the lock-free mirror from the authoritative wheel state.
        self.inner
            .next_wake_atomic()
            .store(next_wake_to_atomic(next_wake), Ordering::Release);
        next_wake
    }

    pub(self) fn process_at_time(&self, mut now: u64) {
        let mut waker_list = WakeList::new();

        let mut lock = self.inner.lock();

        if now < lock.wheel.elapsed() {
            // Time went backwards! This normally shouldn't happen as the Rust language
            // guarantees that an Instant is monotonic, but can happen when running
            // Linux in a VM on a Windows host due to std incorrectly trusting the
            // hardware clock to be monotonic.
            //
            // See <https://github.com/tokio-rs/tokio/issues/3619> for more information.
            now = lock.wheel.elapsed();
        }

        while let Some(entry) = lock.wheel.poll(now) {
            debug_assert!(unsafe { entry.is_pending() });

            // SAFETY: We hold the driver lock, and just removed the entry from any linked lists.
            if let Some(waker) = unsafe { entry.fire(Ok(())) } {
                waker_list.push(waker);

                if !waker_list.can_push() {
                    // Wake a batch of wakers. To avoid deadlock, we must do this with the lock temporarily dropped.
                    drop(lock);

                    waker_list.wake_all();

                    lock = self.inner.lock();
                }
            }
        }

        let next_wake_tick = lock.wheel.poll_at();
        lock.next_wake = next_wake_tick
            .map(|t| NonZeroU64::new(t).unwrap_or_else(|| NonZeroU64::new(1).unwrap()));
        // Mirror to lock-free cache for sharded-mio / io-uring readers.
        self.inner
            .next_wake_atomic()
            .store(next_wake_to_atomic(next_wake_tick), Ordering::Release);

        drop(lock);

        waker_list.wake_all();
    }

    #[cfg(feature = "rt-alt-timer")]
    pub(crate) fn process_at_time_alt(
        &self,
        wheel: &mut time_alt::Wheel,
        mut now: u64,
        wake_queue: &mut time_alt::WakeQueue,
    ) {
        if now < wheel.elapsed() {
            // Time went backwards! This normally shouldn't happen as the Rust language
            // guarantees that an Instant is monotonic, but can happen when running
            // Linux in a VM on a Windows host due to std incorrectly trusting the
            // hardware clock to be monotonic.
            //
            // See <https://github.com/tokio-rs/tokio/issues/3619> for more information.
            now = wheel.elapsed();
        }

        wheel.take_expired(now, wake_queue);
    }

    #[cfg(feature = "rt-alt-timer")]
    pub(crate) fn shutdown_alt(&self, wheel: &mut time_alt::Wheel) {
        // self.is_shutdown.store(true, Ordering::SeqCst);
        // Advance time forward to the end of time.
        // This will ensure that all timers are fired.
        let max_tick = u64::MAX;
        let mut wake_queue = time_alt::WakeQueue::new();
        self.process_at_time_alt(wheel, max_tick, &mut wake_queue);
        wake_queue.wake_all();
    }

    /// Removes a registered timer from the driver.
    ///
    /// The timer will be moved to the cancelled state. Wakers will _not_ be
    /// invoked. If the timer is already completed, this function is a no-op.
    ///
    /// This function always acquires the driver lock, even if the entry does
    /// not appear to be registered.
    ///
    /// SAFETY: The timer must not be registered with some other driver, and
    /// `add_entry` must not be called concurrently.
    pub(self) unsafe fn clear_entry(&self, entry: NonNull<TimerShared>) {
        unsafe {
            let mut lock = self.inner.lock();

            if entry.as_ref().might_be_registered() {
                lock.wheel.remove(entry);
            }

            entry.as_ref().handle().fire(Ok(()));
        }
    }

    /// Removes and re-adds an entry to the driver.
    ///
    /// SAFETY: The timer must be either unregistered, or registered with this
    /// driver. No other threads are allowed to concurrently manipulate the
    /// timer at all (the current thread should hold an exclusive reference to
    /// the `TimerEntry`)
    pub(self) unsafe fn reregister(
        &self,
        unpark: &IoHandle,
        new_tick: u64,
        entry: NonNull<TimerShared>,
    ) {
        let waker = unsafe {
            let mut lock = self.inner.lock();

            // We may have raced with a firing/deregistration, so check before
            // deregistering.
            if unsafe { entry.as_ref().might_be_registered() } {
                lock.wheel.remove(entry);
            }

            // Now that we have exclusive control of this entry, mint a handle to reinsert it.
            let entry = entry.as_ref().handle();

            if self.is_shutdown() {
                unsafe { entry.fire(Err(crate::time::error::Error::shutdown())) }
            } else {
                entry.set_expiration(new_tick);

                // Note: We don't have to worry about racing with some other resetting
                // thread, because add_entry and reregister require exclusive control of
                // the timer entry.
                match unsafe { lock.wheel.insert(entry) } {
                    Ok(when) => {
                        let need_unpark = lock
                            .next_wake
                            .map(|next_wake| when < next_wake.get())
                            .unwrap_or(true);
                        if need_unpark {
                            // The new entry is earlier than any cached
                            // wake; refresh both the mutex-protected and
                            // lock-free copies so the parker (and any
                            // subsequent `add_entry` short-circuit) sees
                            // the new minimum.
                            lock.next_wake = Some(
                                NonZeroU64::new(when)
                                    .unwrap_or_else(|| NonZeroU64::new(1).unwrap()),
                            );
                            self.inner
                                .next_wake_atomic()
                                .store(when, Ordering::Release);
                            unpark.unpark();
                        }

                        None
                    }
                    Err((entry, crate::time::error::InsertError::Elapsed)) => unsafe {
                        entry.fire(Ok(()))
                    },
                }
            }

            // Must release lock before invoking waker to avoid the risk of deadlock.
        };

        // The timer was fired synchronously as a result of the reregistration.
        // Wake the waker; this is needed because we might reset _after_ a poll,
        // and otherwise the task won't be awoken to poll again.
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    cfg_test_util! {
        pub(super) fn did_wake(&self) -> bool {
            match &self.inner {
                Inner::Traditional { did_wake, .. } => did_wake.swap(false, Ordering::SeqCst),
                #[cfg(feature = "rt-alt-timer")]
                Inner::Alternative { did_wake, .. } => did_wake.swap(false, Ordering::SeqCst),
            }
        }
    }
}

// ===== impl Inner =====

impl Inner {
    /// Locks the driver's inner structure
    pub(super) fn lock(&self) -> crate::loom::sync::MutexGuard<'_, InnerState> {
        match self {
            Inner::Traditional { state, .. } => state.lock(),
            #[cfg(feature = "rt-alt-timer")]
            Inner::Alternative { .. } => unreachable!("unreachable in alternative timer"),
        }
    }

    // Check whether the driver has been shutdown
    pub(super) fn is_shutdown(&self) -> bool {
        match self {
            Inner::Traditional { is_shutdown, .. } => is_shutdown.load(Ordering::SeqCst),
            #[cfg(feature = "rt-alt-timer")]
            Inner::Alternative { is_shutdown, .. } => is_shutdown.load(Ordering::SeqCst),
        }
    }

    /// Returns a reference to the lock-free next-wake mirror.
    ///
    /// Mirrors `state.next_wake`; see the field doc on
    /// [`Inner::Traditional::next_wake_atomic`] for semantics.
    fn next_wake_atomic(&self) -> &AtomicU64 {
        match self {
            Inner::Traditional { next_wake_atomic, .. } => next_wake_atomic,
            #[cfg(feature = "rt-alt-timer")]
            Inner::Alternative { .. } => {
                unreachable!("alternative timer does not use next_wake_atomic")
            }
        }
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Inner").finish()
    }
}

#[cfg(test)]
mod tests;
