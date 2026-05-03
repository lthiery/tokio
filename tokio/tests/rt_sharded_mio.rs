//! Smoke tests for the experimental per-worker io_uring reactor.
//!
//! These only run under the `io-sharded-mio` feature, `tokio_unstable`,
//! and target_os = "linux". Everywhere else the file is empty.
//!
//! Scope: exercise the scheduler/parker path without touching I/O
//! registration (which is wired up in a follow-up commit). So the tests
//! here cover: spawning, joining, timers, `yield_now`, and cross-worker
//! wakes. Actual fd-backed I/O (`TcpStream`, etc.) is a separate test
//! file added once `Handle::add_source` is wired into the uring backend.

#![cfg(all(
        feature = "io-sharded-mio",
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
#![warn(rust_2018_idioms)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime;

fn build_rt(workers: usize) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_sharded_mio()
        .build()
        .expect("uring reactor runtime builds")
}

#[test]
fn spawn_and_join_single_worker() {
    let rt = build_rt(1);
    let n = rt.block_on(async { tokio::spawn(async { 42 }).await.unwrap() });
    assert_eq!(n, 42);
}

#[test]
fn spawn_and_join_multi_worker() {
    let rt = build_rt(4);
    let n = rt.block_on(async {
        let mut handles = Vec::new();
        for i in 0..64 {
            handles.push(tokio::spawn(async move { i * 2 }));
        }
        let mut sum = 0u64;
        for h in handles {
            sum += h.await.unwrap() as u64;
        }
        sum
    });
    assert_eq!(n, (0..64u64).map(|i| i * 2).sum::<u64>());
}

#[test]
fn cross_worker_channel_round_trip() {
    // Bounce work across workers through an mpsc channel. Exercises the
    // cross-worker wake path (MSG_RING fast path on the sender side,
    // eventfd fallback from outside the worker pool).
    let rt = build_rt(4);
    let count = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&count);
    rt.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(16);
        let consumer = tokio::spawn(async move {
            let mut total = 0u32;
            while let Some(v) = rx.recv().await {
                total += v;
                c.fetch_add(1, Ordering::Relaxed);
            }
            total
        });
        for i in 1..=100u32 {
            tx.send(i).await.unwrap();
        }
        drop(tx);
        let total = consumer.await.unwrap();
        assert_eq!(total, (1..=100u32).sum());
    });
    assert_eq!(count.load(Ordering::Relaxed), 100);
}

#[test]
fn cross_worker_channel_round_trip_cap1_stress_2w() {
    // Regression for a self-wake bug in `ShardedMioUnparker::unpark` /
    // `UringUnparker::unpark` where the unparker would short-circuit on
    // `current_worker_index() == Some(self.idx)`. That assumption was
    // invalidated by `transition_to_parked → notify_if_work_pending →
    // notify_parked_local`, which can pop the *calling* worker off the
    // sleepers list and route a wake back to itself; short-circuiting
    // there left `park_state == EMPTY`, the worker blocked in
    // `poll.poll`, and `num_searching` was stuck at 1 so subsequent
    // remote unparks were filtered out by `notify_should_wakeup`.
    //
    // Using `cap=1` forces strict ping-pong between sender and
    // receiver, maximising the rate at which both workers traverse
    // `transition_to_parked`, which is what makes this race trip
    // reliably. Two workers + 200 sends × 50 trials hangs within
    // seconds against the buggy unparker; with the fix it completes in
    // < 0.2s.
    for trial in 0..50 {
        let rt = build_rt(2);
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        rt.block_on(async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            let consumer = tokio::spawn(async move {
                let mut total = 0u32;
                while let Some(v) = rx.recv().await {
                    total += v;
                    c.fetch_add(1, Ordering::Relaxed);
                }
                total
            });
            for i in 1..=200u32 {
                tx.send(i).await.unwrap();
            }
            drop(tx);
            let total = consumer.await.unwrap();
            assert_eq!(total, (1..=200u32).sum());
        });
        assert_eq!(count.load(Ordering::Relaxed), 200, "trial {trial}");
    }
}

#[test]
fn cross_worker_channel_round_trip_cap1_stress_4w() {
    // Same regression as the 2-worker case but with a four-worker
    // pool. Wider parallelism increases the chance that
    // `notify_parked_local` pops a different sleeping worker rather
    // than self, while still hitting the self-pop case often enough
    // to deadlock against the buggy unparker.
    for trial in 0..50 {
        let rt = build_rt(4);
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        rt.block_on(async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            let consumer = tokio::spawn(async move {
                let mut total = 0u32;
                while let Some(v) = rx.recv().await {
                    total += v;
                    c.fetch_add(1, Ordering::Relaxed);
                }
                total
            });
            for i in 1..=200u32 {
                tx.send(i).await.unwrap();
            }
            drop(tx);
            let total = consumer.await.unwrap();
            assert_eq!(total, (1..=200u32).sum());
        });
        assert_eq!(count.load(Ordering::Relaxed), 200, "trial {trial}");
    }
}

#[test]
fn sleep_fires_via_alt_timer() {
    // With sharded-mio (legacy timer flavor), the parker drives the legacy
    // timer wheel via the hybrid park flow. Confirm a sleep fires.
    let rt = build_rt(2);
    let started = std::time::Instant::now();
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "sleep returned too early: {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "sleep took too long: {elapsed:?}",
    );
}

#[test]
fn cross_worker_notify_storm() {
    // Stress test for the 5-state `park_state` encoding introduced
    // when `unpark` was specialised to single-route. Hammers
    // `Notify::notify_one` from many threads while the workers split
    // between meta-watcher, own-child, and thread-park branches —
    // exercising every prev-state arm of `unpark`'s match without any
    // I/O registrations, so the wakes are pure parker traffic.
    //
    // The original bug this guards against is the inverse of the
    // double-wake fix: if `begin_park` published the wrong
    // `PARKED_<mode>` (e.g. published the slab-empty `Thread`
    // selection but then took the meta-watcher branch instead),
    // unpark would write to the futex while the parker is in
    // `epoll_wait`, the wake would be silently dropped, and this
    // test would deadlock.
    use tokio::sync::Notify;

    const TASKS: usize = 32;
    const ITERS: usize = 1_000;

    for trial in 0..10 {
        let rt = build_rt(4);
        rt.block_on(async {
            let notifies: Vec<Arc<Notify>> = (0..TASKS)
                .map(|_| Arc::new(Notify::new()))
                .collect();
            let mut handles = Vec::with_capacity(TASKS);
            for i in 0..TASKS {
                let n = notifies[i].clone();
                let next = notifies[(i + 1) % TASKS].clone();
                handles.push(tokio::spawn(async move {
                    for _ in 0..ITERS {
                        n.notified().await;
                        next.notify_one();
                    }
                }));
            }
            // Kick the chain.
            notifies[0].notify_one();
            for h in handles {
                tokio::time::timeout(Duration::from_secs(30), h)
                    .await
                    .unwrap_or_else(|_| panic!("trial {trial}: notify chain deadlocked"))
                    .unwrap();
            }
        });
    }
}

#[test]
fn many_yield_now_across_workers() {
    // Pathological task mix that constantly re-queues itself; exercises
    // the park/unpark atomic fast path repeatedly without actually going
    // to the kernel.
    let rt = build_rt(4);
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..16 {
            handles.push(tokio::spawn(async {
                for _ in 0..500 {
                    tokio::task::yield_now().await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
}
