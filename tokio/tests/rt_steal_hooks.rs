#![warn(rust_2018_idioms)]
#![cfg(all(tokio_unstable, feature = "full", not(target_os = "wasi")))]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};

use tokio::runtime::Builder;

/// A worker with no local work attempts to steal before parking, so the
/// before-steal callback fires promptly on any active multi-worker runtime.
#[test]
fn before_steal_fires() {
    let (tx, rx) = mpsc::channel();
    let fired = Arc::new(AtomicBool::new(false));

    let rt = {
        let fired = fired.clone();
        Builder::new_multi_thread()
            .worker_threads(2)
            .on_before_steal(move || {
                if !fired.swap(true, Ordering::SeqCst) {
                    tx.send(()).unwrap();
                }
            })
            .build()
            .unwrap()
    };

    rt.block_on(async {
        // Wake the workers so at least one runs out of work and searches.
        tokio::spawn(async {}).await.unwrap();
    });

    rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
}

#[test]
fn on_steal_fires() {
    let (tx, rx) = mpsc::channel();
    let fired = Arc::new(AtomicBool::new(false));

    let rt = {
        let fired = fired.clone();
        Builder::new_multi_thread()
            .worker_threads(2)
            .on_steal(move || {
                if !fired.swap(true, Ordering::SeqCst) {
                    tx.send(()).unwrap();
                }
            })
            .build()
            .unwrap()
    };

    rt.block_on(async {
        tokio::spawn(async {}).await.unwrap();
    });

    rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
}

/// The before-steal callback runs in the runtime context and can spawn; work
/// it produces runs on the same worker without requiring a steal.
#[test]
fn before_steal_can_spawn() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let armed = Arc::new(AtomicBool::new(false));

    let rt = {
        let armed = armed.clone();
        Builder::new_multi_thread()
            .worker_threads(2)
            .on_before_steal(move || {
                if armed.swap(false, Ordering::SeqCst) {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        tx.send(()).unwrap();
                    });
                }
            })
            .build()
            .unwrap()
    };

    rt.block_on(async {
        // Arm while the workers are active: as soon as the spawned task
        // completes, its worker runs out of work and fires the callback.
        armed.store(true, Ordering::SeqCst);
        tokio::spawn(async {}).await.unwrap();

        rx.recv().await.unwrap();
    });
}

/// A single-worker runtime never has a steal victim, but the worker still
/// enters the searching phase; both hooks stay callable without deadlock.
#[test]
fn hooks_on_single_worker() {
    let before = Arc::new(AtomicUsize::new(0));
    let steals = Arc::new(AtomicUsize::new(0));

    let rt = {
        let before = before.clone();
        let steals = steals.clone();
        Builder::new_multi_thread()
            .worker_threads(1)
            .on_before_steal(move || {
                before.fetch_add(1, Ordering::SeqCst);
            })
            .on_steal(move || {
                steals.fetch_add(1, Ordering::SeqCst);
            })
            .build()
            .unwrap()
    };

    rt.block_on(async {
        for _ in 0..8 {
            tokio::spawn(async {}).await.unwrap();
        }
    });
    drop(rt);

    // No assertion on counts: this test verifies the hooks do not wedge a
    // single-worker runtime (shutdown completes with the hooks installed).
}
