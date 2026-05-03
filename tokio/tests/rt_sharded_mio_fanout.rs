//! Regression tests for the EPOLLEXCLUSIVE fanout dispatch path.
//!
//! Each fd is `EPOLL_CTL_ADD`-ed onto every worker's epoll fd with
//! `EPOLLEXCLUSIVE | EPOLLET`. The kernel wakes exactly one of the
//! currently-`epoll_wait`-blocked workers per state-change, never a
//! spin-busy worker. These tests exercise that property at unit-test
//! scale.
//!
//! Pre-fanout (P1 readiness-stealing) baseline: when the registering
//! worker is pinned to a CPU-bound burner, the awaiting task could not
//! make progress until the burner exited (~`BURNER_MS`). With fanout,
//! peer workers in `epoll_wait` receive the kernel notification
//! directly and dispatch via `worker_idx` routing back to the owner's
//! slab. See `tokio/docs/readiness-stealing-fanout.md`.
//!
//! Linux + `tokio_unstable` + `io-sharded-mio` only.

#![cfg(all(
        feature = "io-sharded-mio",
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
#![warn(rust_2018_idioms)]

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::runtime;

fn build_rt(workers: usize) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_sharded_mio()
        .build()
        .expect("sharded-mio runtime builds")
}

/// 4-worker runtime, three workers spin-bound for 500 ms, fourth
/// worker is free. A single fd is registered while a burner happens
/// to be running on its registering worker, then a peer thread
/// writes to make the fd readable. The `.readable().await` must
/// return within 100 ms — well under the 500 ms burner deadline,
/// proving the wake reached an idle worker via the fanout epoll.
///
/// Without the meta-epoll readiness-stealing path, all fds registered
/// on burner-pinned workers would stall until the burner exited. With
/// it, the idle worker(s) parked on the runtime-wide meta epoll observe
/// the owner's child as fireable (level-triggered) and `try_steal_drain`
/// dispatches the queued event on the owner's behalf — gated on the
/// owner being in `EMPTY` park state (i.e. running user code, not
/// parked) so the peer can never swallow the owner's WAKER eventfd.
#[test]
fn readable_wakes_while_owner_burns() {
    const BURNER_MS: u64 = 500;
    const WAKE_BUDGET_MS: u64 = 100;

    let rt = build_rt(4);
    rt.block_on(async {
        let (a, b) = UnixStream::pair().expect("socketpair");
        a.set_nonblocking(true).expect("set_nonblocking a");
        b.set_nonblocking(true).expect("set_nonblocking b");

        // Light up burners on N-1 workers. The Tokio scheduler will
        // distribute spawned spin-tasks across workers; with 3
        // burners and 4 workers, statistically at least one worker
        // is left free to serve `epoll_wait` and at least one
        // burner ends up sharing a worker with the probe.
        let stop = Arc::new(AtomicBool::new(false));
        let burner_deadline = Instant::now() + Duration::from_millis(BURNER_MS);
        let mut burners = Vec::new();
        for _ in 0..3 {
            let stop = Arc::clone(&stop);
            burners.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed)
                    && Instant::now() < burner_deadline
                {
                    std::hint::spin_loop();
                }
            }));
        }
        // Brief settle so the scheduler places burners on distinct
        // workers before the probe registers.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Probe: register the read end on whichever worker the
        // probe task happens to land on. Under the pre-fanout model
        // and the burner pattern above, this would frequently land
        // on a busy worker.
        let probe = tokio::spawn(async move {
            let async_a =
                AsyncFd::with_interest(a, Interest::READABLE).expect("AsyncFd");
            let mut guard = async_a.readable().await.expect("readable");
            let mut buf = [0u8; 1];
            // Drain so the AsyncFd doesn't immediately re-fire.
            let _ = unsafe {
                libc::read(
                    async_a.get_ref().as_raw_fd(),
                    buf.as_mut_ptr() as *mut _,
                    1,
                )
            };
            guard.clear_ready();
        });

        // Yield long enough for the probe to reach `.readable()` and
        // for first-poll registration to complete (which fanout-adds
        // the fd to all 4 worker epolls).
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Kick from a regular OS thread — bypassing the scheduler so
        // the wake must come strictly through the kernel epoll fanout.
        let kick_fd = b.as_raw_fd();
        let start = Instant::now();
        let kicker = std::thread::spawn(move || {
            let buf = [0u8; 1];
            unsafe {
                libc::write(kick_fd, buf.as_ptr() as *const _, 1);
            }
        });
        kicker.join().expect("kicker");

        // Bound the wait independently of the probe future itself
        // so a regression manifests as a test timeout rather than a
        // hang.
        let woke = tokio::time::timeout(
            Duration::from_millis(WAKE_BUDGET_MS),
            probe,
        )
        .await;

        let elapsed = start.elapsed();
        // Tear burners down on the way out so the test exits quickly
        // even on success.
        stop.store(true, Ordering::Relaxed);
        for h in burners {
            let _ = h.await;
        }
        // Hold `b` past the kicker.
        drop(b);

        let res = woke.unwrap_or_else(|_| {
            panic!(
                "fanout regression: probe did not wake within {WAKE_BUDGET_MS} ms \
                 (burner deadline {BURNER_MS} ms; elapsed {elapsed:?})"
            )
        });
        res.expect("probe task panicked");
    });
}

/// Single fd, 4 workers, no burners. Sanity-checks the fanout path
/// for the "everyone idle" case: the wake must arrive at one of the
/// workers and dispatch correctly back to the slab via `worker_idx`
/// routing. Loops 50 times to shake out any per-iter races.
#[test]
fn readable_wakes_round_trip_idle() {
    let rt = build_rt(4);
    rt.block_on(async {
        for _ in 0..50 {
            let (a, b) = UnixStream::pair().expect("socketpair");
            a.set_nonblocking(true).expect("set_nonblocking a");
            b.set_nonblocking(true).expect("set_nonblocking b");

            let probe = tokio::spawn(async move {
                let async_a = AsyncFd::with_interest(a, Interest::READABLE)
                    .expect("AsyncFd");
                let mut guard = async_a.readable().await.expect("readable");
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

            // Let the probe reach `.readable()` and register.
            tokio::time::sleep(Duration::from_millis(2)).await;

            let kick_fd = b.as_raw_fd();
            let kicker = std::thread::spawn(move || {
                let buf = [0u8; 1];
                unsafe {
                    libc::write(kick_fd, buf.as_ptr() as *const _, 1);
                }
            });
            kicker.join().expect("kicker");

            tokio::time::timeout(Duration::from_millis(200), probe)
                .await
                .expect("idle fanout wake within 200 ms")
                .expect("probe task panicked");

            drop(b);
        }
    });
}
