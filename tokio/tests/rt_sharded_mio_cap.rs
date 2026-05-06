//! Standalone smoke test for the 128-worker cap on sharded-mio.
#![cfg(all(feature = "io-sharded-mio", feature = "rt-multi-thread", target_os = "linux"))]

use tokio::runtime;

fn build_rt(workers: usize) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_sharded_mio()
        .build()
        .expect("rt builds")
}

#[test]
fn workers_64_builds() {
    let rt = build_rt(64);
    rt.block_on(async { tokio::task::yield_now().await });
}

#[test]
fn workers_128_builds() {
    let rt = build_rt(128);
    rt.block_on(async { tokio::task::yield_now().await });
}

#[test]
#[should_panic(expected = "exceeds TOKEN_WORKER_MASK")]
fn workers_129_panics() {
    let _ = build_rt(129);
}
