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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::runtime::{Builder, Runtime};

// ---- Per-phase wall-clock instrumentation (diagnostic) ----
//
// Each phase keeps two counters: a sum of microseconds across all
// trials, and a sample count. Activated by setting
// `BENCH_PHASE_DEBUG=1` in the env. When enabled, each call to
// `one_iter` also emits one `eprintln!` line with the phase
// breakdown if the trial number matches the print policy
// (first 3 + every 25th + last 3 of the run).
//
// The phases bracket the four legs of `one_iter`:
//   * pre_burner_sleep_us   — wall time between probe spawn and the
//                              end of the 2ms registration sleep
//   * post_burner_sleep_us  — wall time between burner spawn and
//                              the end of the 1ms pre-kick sleep
//   * kick_to_first_wake_us — wall time from the start of the
//                              kicker thread to the moment the first
//                              probe's `.readable().await` returns
//   * first_to_last_wake_us — wall time from first probe wake to
//                              last probe wake (probe_handles fully
//                              awaited)
//
// Sums + counts are dumped at process exit by the existing criterion
// teardown machinery; each criterion bench function also dumps on
// drop via `PhaseDumpOnDrop`.
struct PhaseStats {
    spawn_probes_us_sum:   AtomicU64,
    spawn_probes_us_count: AtomicU64,
    pre_burner_sleep_us_sum:   AtomicU64,
    pre_burner_sleep_us_count: AtomicU64,
    spawn_burners_us_sum:   AtomicU64,
    spawn_burners_us_count: AtomicU64,
    post_burner_sleep_us_sum:   AtomicU64,
    post_burner_sleep_us_count: AtomicU64,
    kicker_thread_us_sum:   AtomicU64,
    kicker_thread_us_count: AtomicU64,
    kick_to_first_wake_us_sum:   AtomicU64,
    kick_to_first_wake_us_count: AtomicU64,
    first_to_last_wake_us_sum:   AtomicU64,
    first_to_last_wake_us_count: AtomicU64,
    iter_total_us_sum:   AtomicU64,
    iter_total_us_count: AtomicU64,
}

impl PhaseStats {
    const fn new() -> Self {
        Self {
            spawn_probes_us_sum: AtomicU64::new(0),
            spawn_probes_us_count: AtomicU64::new(0),
            pre_burner_sleep_us_sum: AtomicU64::new(0),
            pre_burner_sleep_us_count: AtomicU64::new(0),
            spawn_burners_us_sum: AtomicU64::new(0),
            spawn_burners_us_count: AtomicU64::new(0),
            post_burner_sleep_us_sum: AtomicU64::new(0),
            post_burner_sleep_us_count: AtomicU64::new(0),
            kicker_thread_us_sum: AtomicU64::new(0),
            kicker_thread_us_count: AtomicU64::new(0),
            kick_to_first_wake_us_sum: AtomicU64::new(0),
            kick_to_first_wake_us_count: AtomicU64::new(0),
            first_to_last_wake_us_sum: AtomicU64::new(0),
            first_to_last_wake_us_count: AtomicU64::new(0),
            iter_total_us_sum: AtomicU64::new(0),
            iter_total_us_count: AtomicU64::new(0),
        }
    }

    fn record(&self, sum: &AtomicU64, count: &AtomicU64, us: u64) {
        sum.fetch_add(us, Ordering::Relaxed);
        count.fetch_add(1, Ordering::Relaxed);
    }

    fn mean_us(&self, sum: &AtomicU64, count: &AtomicU64) -> u64 {
        let n = count.load(Ordering::Relaxed);
        if n == 0 {
            0
        } else {
            sum.load(Ordering::Relaxed) / n
        }
    }

    fn reset(&self) {
        for a in [
            &self.spawn_probes_us_sum,
            &self.spawn_probes_us_count,
            &self.pre_burner_sleep_us_sum,
            &self.pre_burner_sleep_us_count,
            &self.spawn_burners_us_sum,
            &self.spawn_burners_us_count,
            &self.post_burner_sleep_us_sum,
            &self.post_burner_sleep_us_count,
            &self.kicker_thread_us_sum,
            &self.kicker_thread_us_count,
            &self.kick_to_first_wake_us_sum,
            &self.kick_to_first_wake_us_count,
            &self.first_to_last_wake_us_sum,
            &self.first_to_last_wake_us_count,
            &self.iter_total_us_sum,
            &self.iter_total_us_count,
        ] {
            a.store(0, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> Vec<(&'static str, u64, u64)> {
        vec![
            (
                "spawn_probes_us",
                self.spawn_probes_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.spawn_probes_us_sum, &self.spawn_probes_us_count),
            ),
            (
                "pre_burner_sleep_us",
                self.pre_burner_sleep_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.pre_burner_sleep_us_sum, &self.pre_burner_sleep_us_count),
            ),
            (
                "spawn_burners_us",
                self.spawn_burners_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.spawn_burners_us_sum, &self.spawn_burners_us_count),
            ),
            (
                "post_burner_sleep_us",
                self.post_burner_sleep_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.post_burner_sleep_us_sum, &self.post_burner_sleep_us_count),
            ),
            (
                "kicker_thread_us",
                self.kicker_thread_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.kicker_thread_us_sum, &self.kicker_thread_us_count),
            ),
            (
                "kick_to_first_wake_us",
                self.kick_to_first_wake_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.kick_to_first_wake_us_sum, &self.kick_to_first_wake_us_count),
            ),
            (
                "first_to_last_wake_us",
                self.first_to_last_wake_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.first_to_last_wake_us_sum, &self.first_to_last_wake_us_count),
            ),
            (
                "iter_total_us",
                self.iter_total_us_count.load(Ordering::Relaxed),
                self.mean_us(&self.iter_total_us_sum, &self.iter_total_us_count),
            ),
        ]
    }
}

static PHASE: PhaseStats = PhaseStats::new();
static TRIAL_NUM: AtomicU64 = AtomicU64::new(0);

fn phase_debug_enabled() -> bool {
    std::env::var_os("BENCH_PHASE_DEBUG")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

fn should_print_trial(n: u64) -> bool {
    n <= 3 || n % 25 == 0
}

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

#[cfg(all(feature = "bench-sharded-mio", target_os = "linux"))]
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
    let debug = phase_debug_enabled();
    let trial_num = TRIAL_NUM.fetch_add(1, Ordering::Relaxed) + 1;

    // Shared "first-wake" timestamp. Each probe attempts to record
    // `Instant::now()` when its `.readable().await` returns; only the
    // first writer wins (subsequent compare-and-swap-style stores are
    // skipped via the `Option::is_none()` check under the mutex).
    let first_wake: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    // Hold the `b` ends in a Vec so they outlive the kicker thread.
    // The `a` ends move into per-probe spawned tasks.
    let mut writers: Vec<UnixStream> = Vec::with_capacity(NUM_PROBES);
    let mut probe_handles = Vec::with_capacity(NUM_PROBES);

    let spawn_probes_start = Instant::now();

    for _ in 0..NUM_PROBES {
        let (a, b) = UnixStream::pair().expect("socketpair");
        a.set_nonblocking(true).expect("set_nonblocking on a");
        b.set_nonblocking(true).expect("set_nonblocking on b");
        writers.push(b);

        let first_wake_clone = Arc::clone(&first_wake);
        let probe = tokio::spawn(async move {
            // First call to .readable() triggers lazy first-poll
            // registration of `a` on the running worker (sharded-mio).
            let async_a = AsyncFd::with_interest(a, Interest::READABLE)
                .expect("AsyncFd::with_interest");
            let mut guard = async_a.readable().await.expect("readable");

            // Stash the first-wake timestamp.
            let now = Instant::now();
            let mut slot = first_wake_clone.lock().expect("first_wake mutex");
            if slot.is_none() {
                *slot = Some(now);
            }
            drop(slot);

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

    let spawn_probes_us = spawn_probes_start.elapsed().as_micros() as u64;

    // Yield long enough for every awaiter to reach its `.await` and
    // register its fd. Without this pause, registration races with
    // burner spawn and which worker owns each fd becomes harder to
    // reason about.
    let pre_burner_sleep_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let pre_burner_sleep_us = pre_burner_sleep_start.elapsed().as_micros() as u64;

    // Spawn burners AFTER fds have registered. Each burner spins
    // without yielding for `BURNER_MS`. The Tokio scheduler will pin
    // each to one worker; with `num_burners = NUM_WORKERS - 1`, that
    // monopolizes all but one worker. Crucially, no `.await` inside
    // the burner means no chance for the worker's park loop to run
    // — i.e., no `epoll_wait` until the burner exits.
    let stop_signal = Arc::new(AtomicBool::new(false));
    let burner_deadline = Instant::now() + Duration::from_millis(BURNER_MS);
    let spawn_burners_start = Instant::now();
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
    let spawn_burners_us = spawn_burners_start.elapsed().as_micros() as u64;

    // Brief pause so the runtime has a chance to start every burner
    // on a distinct worker. Work-stealing should distribute them.
    let post_burner_sleep_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let post_burner_sleep_us = post_burner_sleep_start.elapsed().as_micros() as u64;

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
    let kicker_thread_us = start.elapsed().as_micros() as u64;

    // Wait for every probe to wake.
    for h in probe_handles {
        h.await.expect("probe");
    }
    let elapsed = start.elapsed();
    let last_wake = Instant::now();

    // Compute the kick-to-first-wake / first-to-last spans.
    let first_wake_t = first_wake.lock().expect("first_wake mutex").take();
    let (kick_to_first_us, first_to_last_us) = match first_wake_t {
        Some(first_t) => {
            let k_to_f = first_t.saturating_duration_since(start).as_micros() as u64;
            let f_to_l = last_wake.saturating_duration_since(first_t).as_micros() as u64;
            (k_to_f, f_to_l)
        }
        None => (0, 0),
    };

    // Tear down burners (most should already be at deadline).
    stop_signal.store(true, Ordering::Relaxed);
    for h in burners {
        h.await.expect("burner");
    }

    // Hold writers alive past the kicker.
    drop(writers);

    let iter_total_us = elapsed.as_micros() as u64;
    PHASE.record(
        &PHASE.spawn_probes_us_sum,
        &PHASE.spawn_probes_us_count,
        spawn_probes_us,
    );
    PHASE.record(
        &PHASE.pre_burner_sleep_us_sum,
        &PHASE.pre_burner_sleep_us_count,
        pre_burner_sleep_us,
    );
    PHASE.record(
        &PHASE.spawn_burners_us_sum,
        &PHASE.spawn_burners_us_count,
        spawn_burners_us,
    );
    PHASE.record(
        &PHASE.post_burner_sleep_us_sum,
        &PHASE.post_burner_sleep_us_count,
        post_burner_sleep_us,
    );
    PHASE.record(
        &PHASE.kicker_thread_us_sum,
        &PHASE.kicker_thread_us_count,
        kicker_thread_us,
    );
    PHASE.record(
        &PHASE.kick_to_first_wake_us_sum,
        &PHASE.kick_to_first_wake_us_count,
        kick_to_first_us,
    );
    PHASE.record(
        &PHASE.first_to_last_wake_us_sum,
        &PHASE.first_to_last_wake_us_count,
        first_to_last_us,
    );
    PHASE.record(
        &PHASE.iter_total_us_sum,
        &PHASE.iter_total_us_count,
        iter_total_us,
    );

    if debug && should_print_trial(trial_num) {
        eprintln!(
            "[phase] trial={:<4} probe_spawn={:>5}us  pre_sleep={:>5}us  \
             burner_spawn={:>5}us  post_sleep={:>5}us  kicker={:>4}us  \
             k_to_1st={:>6}us  1st_to_last={:>5}us  total={:>6}us",
            trial_num,
            spawn_probes_us,
            pre_burner_sleep_us,
            spawn_burners_us,
            post_burner_sleep_us,
            kicker_thread_us,
            kick_to_first_us,
            first_to_last_us,
            iter_total_us,
        );
    }

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

fn dump_phase_summary(label: &str) {
    if !phase_debug_enabled() {
        return;
    }
    eprintln!("---- phase summary: {} ----", label);
    for (name, count, mean_us) in PHASE.snapshot() {
        eprintln!("  {:<24} count={:<6} mean={:>6}us", name, count, mean_us);
    }
    PHASE.reset();
    TRIAL_NUM.store(0, Ordering::Relaxed);
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

#[cfg(all(feature = "bench-sharded-mio", target_os = "linux"))]
fn bench_sharded_mio(c: &mut Criterion) {
    let rt = rt_sharded_mio();
    c.bench_function("sharded_mio/busy_owner_idle", |b| {
        run_busy_owner(&rt, 0, b)
    });
    dump_phase_summary("sharded_mio/busy_owner_idle");
    c.bench_function("sharded_mio/busy_owner_3burners", |b| {
        run_busy_owner(&rt, NUM_WORKERS - 1, b)
    });
    dump_phase_summary("sharded_mio/busy_owner_3burners");
}

#[cfg(not(all(feature = "bench-sharded-mio", target_os = "linux")))]
fn bench_sharded_mio(_c: &mut Criterion) {}

criterion_group!(io_busy_owner, bench_traditional, bench_sharded_mio);
criterion_main!(io_busy_owner);
