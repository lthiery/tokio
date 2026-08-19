//! Smoke tests for the experimental io_uring reactor on the multi-thread
//! scheduler.
//!
//! These only run under the `io-uring-reactor` feature, `tokio_unstable`,
//! and target_os = "linux". Everywhere else the file is empty.
//!
//! Scope: exercise the scheduler/parker path without touching I/O
//! registration. The tests here cover: spawning, joining, timers,
//! `yield_now`, and cross-worker wakes. Actual fd-backed I/O
//! (`TcpStream`, etc.) lives in `net_uring_reactor_tcp.rs`.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
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
        .enable_uring_reactor()
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
    // cross-worker wake path (ring eventfd for the holder, condvar for
    // everyone else).
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
fn sleep_fires_via_hybrid_timer_flow() {
    // Under the uring reactor the ring holder drives the traditional
    // timer wheel via the hybrid park flow (deadline folded into the
    // ring timeout, wheel advanced after wake). Confirm a sleep fires.
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

/// Signal (and with it process) listening is rejected deterministically:
/// the signal driver machinery feeds a mio driver nobody polls under the
/// uring backend, so the builder withholds the handle and use panics at
/// the resource constructor instead of silently never firing.
#[test]
#[cfg(feature = "signal")]
fn signal_use_panics_deterministically() {
    let rt = build_rt(1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup());
        })
    }))
    .expect_err("signal listener construction must panic under the uring reactor");
    let msg = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("io_uring"),
        "panic should name the io_uring reactor: {msg:?}"
    );
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
