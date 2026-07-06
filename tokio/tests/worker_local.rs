#![warn(rust_2018_idioms)]
#![cfg(all(tokio_unstable, feature = "worker-local", not(target_os = "wasi")))]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task::spawn_worker_local;

fn rt(workers: usize) -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap()
}

#[test]
fn spawn_and_join() {
    let rt = rt(2);
    rt.block_on(async {
        let handle = tokio::spawn(async {
            let join = spawn_worker_local(async { 42 });
            join.await.unwrap()
        });
        assert_eq!(handle.await.unwrap(), 42);
    });
}

#[test]
fn not_send_future() {
    let rt = rt(2);
    rt.block_on(async {
        let result = tokio::spawn(async {
            let join = spawn_worker_local(async {
                let rc = Rc::new(Cell::new(1));
                let rc2 = rc.clone();
                tokio::task::yield_now().await;
                rc2.set(rc2.get() + 1);
                rc.get()
            });
            join.await.unwrap()
        })
        .await
        .unwrap();
        assert_eq!(result, 2);
    });
}

#[test]
fn stays_on_spawning_thread() {
    let rt = rt(4);
    rt.block_on(async {
        let mut joins = Vec::new();
        for _ in 0..4 {
            joins.push(tokio::spawn(async {
                let join = spawn_worker_local(async {
                    let spawned_on = std::thread::current().id();
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                        assert_eq!(spawned_on, std::thread::current().id());
                    }
                });
                join.await.unwrap();
            }));
        }
        for join in joins {
            join.await.unwrap();
        }
    });
}

#[test]
fn cross_thread_wake() {
    let rt = rt(2);
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<u32>();

        let worker_task = tokio::spawn(async move {
            let join = spawn_worker_local(async move { rx.await.unwrap() });
            join.await.unwrap()
        });

        // Sent from the `block_on` thread, which is not a worker: the wake
        // must travel through the worker-local inject queue and unpark.
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            tx.send(7).unwrap();
        });

        assert_eq!(worker_task.await.unwrap(), 7);
    });
}

#[test]
fn worker_local_task_panic_is_join_error() {
    let rt = rt(2);
    rt.block_on(async {
        let result = tokio::spawn(async {
            let join = spawn_worker_local(async { panic!("boom") });
            join.await
        })
        .await
        .unwrap();
        assert!(result.unwrap_err().is_panic());
    });
}

#[test]
fn drops_pending_tasks_on_shutdown() {
    struct SetOnDrop(Arc<AtomicUsize>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let rt = rt(2);

    rt.block_on(async {
        let drops = drops.clone();
        tokio::spawn(async move {
            let guard = SetOnDrop(drops);
            spawn_worker_local(async move {
                let _guard = guard;
                // Never completes.
                std::future::pending::<()>().await;
            });
        })
        .await
        .unwrap();
    });

    drop(rt);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
#[should_panic(
    expected = "`spawn_worker_local` called from outside of a multi-threaded runtime worker thread"
)]
fn panics_outside_runtime() {
    let _join = spawn_worker_local(async {});
}

#[test]
fn panics_on_current_thread_runtime() {
    let rt = Builder::new_current_thread().build().unwrap();
    let result = rt.block_on(async {
        tokio::spawn(async {
            let result = std::panic::catch_unwind(|| {
                let _join = spawn_worker_local(async {});
            });
            result.is_err()
        })
        .await
        .unwrap()
    });
    assert!(result);
}

#[test]
fn block_in_place_panics_with_live_worker_local_tasks() {
    let rt = rt(2);
    rt.block_on(async {
        let panicked = tokio::spawn(async {
            // The worker-local task itself is alive on this worker while it
            // runs, so `block_in_place` must refuse.
            let join = spawn_worker_local(async {
                std::panic::catch_unwind(|| {
                    tokio::task::block_in_place(|| {});
                })
                .is_err()
            });
            join.await.unwrap()
        })
        .await
        .unwrap();
        assert!(panicked);
    });
}

#[test]
fn block_in_place_allowed_without_worker_local_tasks() {
    let rt = rt(2);
    rt.block_on(async {
        tokio::spawn(async {
            tokio::task::block_in_place(|| {});
        })
        .await
        .unwrap();
    });
}

#[test]
fn many_tasks_many_workers() {
    let rt = rt(4);
    rt.block_on(async {
        let mut joins = Vec::new();
        for i in 0..64u32 {
            joins.push(tokio::spawn(async move {
                let join = spawn_worker_local(async move {
                    let mut acc = 0;
                    for j in 0..i {
                        tokio::task::yield_now().await;
                        acc += j;
                    }
                    acc
                });
                join.await.unwrap()
            }));
        }
        for (i, join) in joins.into_iter().enumerate() {
            let i = i as u32;
            assert_eq!(join.await.unwrap(), (0..i).sum::<u32>());
        }
    });
}
