//! Phase 0 microbench for the readiness-stealing design.
//!
//! Reproduces the scenario described in `tokio/docs/io-driver-vtable.md`'s
//! follow-up design discussion: a worker is CPU-saturated handling tasks
//! and therefore not calling `epoll_wait`, while a different (idle)
//! worker has a future awaiting an fd registered on the busy worker.
//! The future cannot make progress until the busy worker yields, even
//! though the kernel has the readiness queued.
//!
//! ## What the bench does
//!
//! Each iteration:
//!
//! 1. Creates `K` Unix socketpairs.
//! 2. Spawns one awaiter task per pair. The awaiter calls
//!    [`tokio::io::unix::AsyncFd::readable`], which triggers first-poll
//!    registration of the `a` end on whichever worker happens to run the
//!    awaiter task. With 4 workers and 16 fds the registration is
//!    distributed roughly uniformly.
//! 3. Spawns `num_burners` CPU-burner tasks. Each burner spins on
//!    `std::hint::spin_loop` for `BURNER_MS` milliseconds **without
//!    yielding to the scheduler**, monopolizing whichever worker picks
//!    it up. With 4 workers and 3 burners, exactly one worker is free
//!    to drive readiness for the duration of the iter.
//! 4. From a regular OS thread (not a Tokio task — avoids scheduler
//!    interference), writes one byte to each `b` end. The kernel marks
//!    the corresponding `a` end readable.
//! 5. `await`s all probe handles. Records elapsed time from kick to
//!    "all probes woke."
//!
//! ## Expected signal
//!
//! With `num_burners = 0`: every worker is free to harvest. All probes
//! wake within sub-millisecond. Iter time ≈ syscall + dispatch overhead.
//!
//! With `num_burners = NUM_WORKERS - 1`: roughly `(N-1)/N` of the fds
//! are registered on busy workers. Their awaiters cannot wake until
//! the burner monopolizing that worker finishes (after `BURNER_MS`).
//! Iter time ≈ `BURNER_MS` for the slow tail.
//!
//! The ratio `busy / idle` is the upper bound on what readiness
//! stealing could save. If the ratio is small (say <2×), the cost
//! probably isn't worth chasing. If it's large (10×+), the design has
//! a real target.
//!
//! ## Backends
//!
//! - `traditional`: shared global mio reactor. Any parking worker can
//!   call `epoll_wait`, so a busy worker does not stall readiness.
//!   Expected: idle ≈ busy.
//! - `sharded_mio`: per-worker epoll fds. A busy worker stalls all
//!   fds registered on it. Expected: busy ≫ idle.
//!
//! The gap between traditional and sharded_mio in the busy case is the
//! cost the readiness-stealing design is meant to recover.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::runtime::{Builder, Runtime};

const NUM_WORKERS: usize = 4;
const NUM_PROBES: usize = 16;
const BURNER_MS: u64 = 50;

fn workers() -> usize {
    std::env::var("TOKIO_BENCH_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(NUM_WORKERS)
}

fn rt_traditional() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers())
        .enable_all()
        .build()
        .unwrap()
}

#[cfg(all(tokio_unstable, feature = "bench-sharded-mio", target_os = "linux"))]
fn rt_sharded_mio() -> Runtime {
    let mut b = Builder::new_multi_thread();
    b.worker_threads(workers()).enable_all();
    b.enable_sharded_mio();
    b.build().unwrap()
}

/// Run one iteration: kick K probes, wait for all to wake, return
/// elapsed time. Burners (if any) run for `BURNER_MS` and end on
/// their own deadline; they do not gate the bench measurement
/// directly, but they gate readiness harvest on the workers they pin.
async fn one_iter(num_burners: usize) -> Duration {
    // Hold the `b` ends in a Vec so they outlive the kicker thread.
    // The `a` ends move into per-probe spawned tasks.
    let mut writers: Vec<UnixStream> = Vec::with_capacity(NUM_PROBES);
    let mut probe_handles = Vec::with_capacity(NUM_PROBES);

    for _ in 0..NUM_PROBES {
        let (a, b) = UnixStream::pair().expect("socketpair");
        a.set_nonblocking(true).expect("set_nonblocking on a");
        b.set_nonblocking(true).expect("set_nonblocking on b");
        writers.push(b);

        let probe = tokio::spawn(async move {
            // First call to .readable() triggers lazy first-poll
            // registration of `a` on the running worker (sharded-mio).
            let async_a = AsyncFd::with_interest(a, Interest::READABLE)
                .expect("AsyncFd::with_interest");
            let mut guard = async_a.readable().await.expect("readable");

            // Drain the byte so the AsyncFd wouldn't re-fire if
            // anyone polled it again. We don't actually re-poll it;
            // `clear_ready` is for hygiene.
            let mut buf = [0u8; 1];
            let _ = unsafe {
                libc::read(
                    async_a.get_ref().as_raw_fd(),
                    buf.as_mut_ptr() as *mut _,
                    1,
                )
            };
            guard.clear_ready();
        });
        probe_handles.push(probe);
    }

    // Yield long enough for every awaiter to reach its `.await` and
    // register its fd. Without this pause, registration races with
    // burner spawn and which worker owns each fd becomes harder to
    // reason about.
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Spawn burners AFTER fds have registered. Each burner spins
    // without yielding for `BURNER_MS`. The Tokio scheduler will pin
    // each to one worker; with `num_burners = NUM_WORKERS - 1`, that
    // monopolizes all but one worker. Crucially, no `.await` inside
    // the burner means no chance for the worker's park loop to run
    // — i.e., no `epoll_wait` until the burner exits.
    let stop_signal = Arc::new(AtomicBool::new(false));
    let burner_deadline = Instant::now() + Duration::from_millis(BURNER_MS);
    let burners: Vec<_> = (0..num_burners)
        .map(|_| {
            let stop_signal = stop_signal.clone();
            tokio::spawn(async move {
                while !stop_signal.load(Ordering::Relaxed)
                    && Instant::now() < burner_deadline
                {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();

    // Brief pause so the runtime has a chance to start every burner
    // on a distinct worker. Work-stealing should distribute them.
    tokio::time::sleep(Duration::from_millis(1)).await;

    // Kick from a regular OS thread. Writing to the `b` end makes
    // the corresponding `a` end kernel-readable. The kicker doesn't
    // touch the Tokio scheduler.
    let kick_fds: Vec<i32> = writers.iter().map(|w| w.as_raw_fd()).collect();
    let start = Instant::now();
    let kicker = std::thread::spawn(move || {
        let buf = [0u8; 1];
        for fd in kick_fds {
            unsafe {
                libc::write(fd, buf.as_ptr() as *const _, 1);
            }
        }
    });
    kicker.join().expect("kicker thread");

    // Wait for every probe to wake.
    for h in probe_handles {
        h.await.expect("probe");
    }
    let elapsed = start.elapsed();

    // Tear down burners (most should already be at deadline).
    stop_signal.store(true, Ordering::Relaxed);
    for h in burners {
        h.await.expect("burner");
    }

    // Hold writers alive past the kicker.
    drop(writers);

    elapsed
}

fn run_busy_owner(rt: &Runtime, num_burners: usize, b: &mut Bencher) {
    b.iter_custom(|iters| {
        rt.block_on(async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += one_iter(num_burners).await;
            }
            total
        })
    });
}

fn bench_traditional(c: &mut Criterion) {
    let rt = rt_traditional();
    c.bench_function("traditional/busy_owner_idle", |b| {
        run_busy_owner(&rt, 0, b)
    });
    c.bench_function("traditional/busy_owner_3burners", |b| {
        run_busy_owner(&rt, NUM_WORKERS - 1, b)
    });
}

#[cfg(all(tokio_unstable, feature = "bench-sharded-mio", target_os = "linux"))]
fn bench_sharded_mio(c: &mut Criterion) {
    let rt = rt_sharded_mio();
    c.bench_function("sharded_mio/busy_owner_idle", |b| {
        run_busy_owner(&rt, 0, b)
    });
    c.bench_function("sharded_mio/busy_owner_3burners", |b| {
        run_busy_owner(&rt, NUM_WORKERS - 1, b)
    });
}

#[cfg(not(all(tokio_unstable, feature = "bench-sharded-mio", target_os = "linux")))]
fn bench_sharded_mio(_c: &mut Criterion) {}

criterion_group!(io_busy_owner, bench_traditional, bench_sharded_mio);
criterion_main!(io_busy_owner);
