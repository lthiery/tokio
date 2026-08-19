//! Regression tests for timers inserted after workers have parked, under
//! the experimental io_uring reactor.
//!
//! Two historical failure shapes from the research branch:
//!
//! * F1: the time driver's insert-unpark used to target the traditional
//!   `IoHandle`, whose mio driver nobody polls under the uring backend.
//!   A timer inserted while every worker was parked never fired
//!   (`late_external_timer_fires` hung).
//! * F2: a worker that inserted a timer and then lost the ring race
//!   condvar-parked with the raw scheduler timeout, with no mechanism to
//!   update the ring holder's stale deadline
//!   (`late_worker_timer_fires_forced` hung).
//!
//! Both are fixed by routing the insert-side wake to the uring handle
//! (`UringHandle::unpark_for_timer`), which kicks the shared ring's
//! eventfd unconditionally so the (current or next) holder re-parks with
//! a fresh `next_wake_tick()`.
//!
//! The failure mode is a HANG, not an assert, so each test arms a
//! watchdog that aborts the process loudly if the runtime wedges.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt-multi-thread",
    target_os = "linux",
))]
#![warn(rust_2018_idioms)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn build_rt(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_uring_reactor()
        .enable_time()
        .build()
        .unwrap()
}

/// Aborts the process if the guard is still armed after `timeout`. The
/// regression under test is a lost timer wake (a hang), so a plain
/// assert can never fire; this converts the hang into a loud failure.
struct HangWatchdog {
    done: Arc<AtomicBool>,
}

impl HangWatchdog {
    fn arm(name: &'static str, timeout: Duration) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        std::thread::spawn(move || {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                if flag.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if !flag.load(Ordering::Acquire) {
                eprintln!("{name}: timer wake lost, runtime wedged, aborting");
                std::process::abort();
            }
        });
        Self { done }
    }
}

impl Drop for HangWatchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);
    }
}

/// F1: external-thread (`block_on`) timer insert while every worker is
/// already parked. Before the fix the insert's unpark went to the
/// never-polled mio driver and the parked workers slept with their
/// stale (timer-free) deadlines forever.
#[test]
fn late_external_timer_fires() {
    let _watchdog = HangWatchdog::arm("late_external_timer_fires", Duration::from_secs(30));
    let rt = build_rt(2);
    rt.block_on(async {
        // Give both workers time to go idle and park (one becomes ring
        // holder with no timer deadline, the other condvar-parks).
        std::thread::sleep(Duration::from_millis(200));
        let start = Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "sleep took {elapsed:?}: timer wake was lost"
        );
    });
}

/// F2: forced interleaving of the holder stranding. Worker A parks as
/// ring holder with no deadline while worker B is still busy; B then
/// inserts a timer and (pre-fix) condvar-parked, dropping its computed
/// deadline on the floor while A slept forever.
#[test]
fn late_worker_timer_fires_forced() {
    let _watchdog = HangWatchdog::arm("late_worker_timer_fires_forced", Duration::from_secs(30));
    let rt = build_rt(2);
    rt.block_on(async {
        std::thread::sleep(Duration::from_millis(200)); // both workers park
        let start = Instant::now();
        // Wake both workers: a no-op task (its worker parks again quickly,
        // becoming the ring holder with no timer deadline) and a task that
        // busy-waits past that re-park before inserting its timer.
        let noop = tokio::spawn(async {});
        let sleeper = tokio::spawn(async {
            std::thread::sleep(Duration::from_millis(100));
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        noop.await.unwrap();
        sleeper.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "forced sleep took {elapsed:?}: timer wake was lost"
        );
    });
}

/// Control: worker-created timer without the forced interleaving. The
/// spawned task's worker inserts the timer and re-parks; it may or may
/// not lose the ring race. Passed before the fix too.
#[test]
fn late_worker_timer_fires() {
    let _watchdog = HangWatchdog::arm("late_worker_timer_fires", Duration::from_secs(30));
    let rt = build_rt(2);
    rt.block_on(async {
        // Let workers park first.
        std::thread::sleep(Duration::from_millis(200));
        let start = Instant::now();
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        })
        .await
        .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "spawned sleep took {elapsed:?}: timer wake was lost"
        );
    });
}
