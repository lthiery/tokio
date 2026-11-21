#![warn(rust_2018_idioms)]
#![cfg(all(
    tokio_unstable,
    feature = "full",
    feature = "worker-local",
    not(target_os = "wasi")
))]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task::{run_on_each_worker, run_on_worker, spawn_worker_local};

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

        // Sent from a non-runtime thread: the wake must travel through the
        // worker-local inject queue and unpark.
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

#[cfg(panic = "unwind")]
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

#[cfg(panic = "unwind")]
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
fn run_on_worker_targets_correct_worker() {
    let rt = rt(4);
    rt.block_on(async {
        for target in 0..4 {
            let (tx, rx) = oneshot::channel();
            run_on_worker(target, move || {
                spawn_worker_local(async move {
                    tx.send(tokio::runtime::worker_index()).unwrap();
                });
            });
            assert_eq!(rx.await.unwrap(), Some(target));
        }
    });
}

#[test]
fn run_on_worker_from_non_runtime_thread() {
    let rt = rt(2);
    let handle = rt.handle().clone();
    let (tx, rx) = oneshot::channel();

    std::thread::spawn(move || {
        let _guard = handle.enter();
        run_on_worker(0, move || {
            tx.send(tokio::runtime::worker_index()).unwrap();
        });
    })
    .join()
    .unwrap();

    assert_eq!(rt.block_on(rx).unwrap(), Some(0));
}

#[test]
fn run_on_each_worker_visits_all() {
    let rt = rt(4);
    rt.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        run_on_each_worker(|index| {
            let tx = tx.clone();
            move || {
                spawn_worker_local(async move {
                    tx.send((index, tokio::runtime::worker_index())).unwrap();
                });
            }
        });

        let mut seen = Vec::new();
        for _ in 0..4 {
            let (index, actual) = rx.recv().await.unwrap();
            assert_eq!(Some(index), actual);
            seen.push(index);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
    });
}

#[test]
fn handle_and_runtime_run_on_worker_without_enter() {
    let rt = rt(4);
    let handle = rt.handle().clone();

    let (tx, rx) = oneshot::channel();
    handle.run_on_worker(2, move || {
        tx.send(tokio::runtime::worker_index()).unwrap();
    });
    assert_eq!(rt.block_on(rx).unwrap(), Some(2));

    // `run_on_each_worker` lets the builder move non-`Clone` per-worker
    // resources (here, individual `oneshot::Sender`s) onto each worker.
    let mut rxs = Vec::new();
    rt.run_on_each_worker(|index| {
        let (tx, rx) = oneshot::channel();
        rxs.push((index, rx));
        move || {
            spawn_worker_local(async move {
                tx.send(tokio::runtime::worker_index()).unwrap();
            });
        }
    });

    rt.block_on(async {
        for (index, rx) in rxs {
            assert_eq!(rx.await.unwrap(), Some(index));
        }
    });
}

#[test]
fn run_on_worker_same_worker_fast_path() {
    let rt = rt(2);
    rt.block_on(async {
        let result = tokio::spawn(async {
            let me = tokio::runtime::worker_index().unwrap();
            let (tx, rx) = oneshot::channel();
            run_on_worker(me, move || {
                tx.send(tokio::runtime::worker_index()).unwrap();
            });
            (me, rx.await.unwrap())
        })
        .await
        .unwrap();
        assert_eq!(Some(result.0), result.1);
    });
}

#[test]
fn run_on_worker_panic_is_contained() {
    let rt = rt(2);
    rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        run_on_worker(0, || panic!("boom"));
        // A panicking closure must not take down the worker: queue another
        // closure on the same worker and observe it running.
        run_on_worker(0, move || {
            tx.send(()).unwrap();
        });
        rx.await.unwrap();
    });
}

#[test]
fn run_on_worker_after_shutdown_drops_closure() {
    let rt = rt(2);
    let handle = rt.handle().clone();

    let sentinel = Arc::new(());
    let weak = Arc::downgrade(&sentinel);

    drop(rt);

    // The closure captures a runtime handle: if the request queue kept it
    // after shutdown, the closure would keep the scheduler alive and the
    // scheduler the closure, leaking both. The closed queue must drop the
    // request immediately instead.
    {
        let _enter = handle.enter();
        let handle = handle.clone();
        run_on_worker(0, move || {
            let _handle = handle;
            let _sentinel = sentinel;
        });
    }

    drop(handle);
    assert!(weak.upgrade().is_none());
}

#[test]
#[should_panic(expected = "out of range")]
fn run_on_worker_index_out_of_range() {
    let rt = rt(2);
    rt.block_on(async {
        run_on_worker(2, || {});
    });
}

#[test]
#[should_panic(expected = "requires the multi-threaded runtime")]
fn run_on_worker_current_thread_panics() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        run_on_worker(0, || {});
    });
}

#[test]
fn abort_from_other_thread() {
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
        let (tx, rx) = oneshot::channel();

        let outer = tokio::spawn(async move {
            let guard = SetOnDrop(drops);
            let join = spawn_worker_local(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            tx.send(join.abort_handle()).unwrap();
            assert!(join.await.unwrap_err().is_cancelled());
        });

        // Abort from the (non-worker) block_on thread: cancellation must
        // route back to the owning worker, which drops the future there.
        let abort = rx.await.unwrap();
        abort.abort();

        // The cancellation completes asynchronously on the owning worker;
        // the outer task observes it via the JoinHandle.
        outer.await.unwrap();
    });

    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn nested_spawn_worker_local() {
    let rt = rt(2);
    rt.block_on(async {
        let value = tokio::spawn(async {
            let outer = spawn_worker_local(async {
                // Spawning from inside a worker-local task lands on the
                // same worker.
                let before = std::thread::current().id();
                let inner = spawn_worker_local(async move {
                    assert_eq!(before, std::thread::current().id());
                    5
                });
                inner.await.unwrap() + 1
            });
            outer.await.unwrap()
        })
        .await
        .unwrap();
        assert_eq!(value, 6);
    });
}

#[test]
fn worker_local_and_regular_tasks_share_worker() {
    // A yield-looping worker-local task must not starve regular tasks on
    // the same worker, and vice versa.
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            let local = spawn_worker_local(async {
                for _ in 0..1_000 {
                    tokio::task::yield_now().await;
                }
            });
            let regular = tokio::spawn(async {
                for _ in 0..1_000 {
                    tokio::task::yield_now().await;
                }
            });

            local.await.unwrap();
            regular.await.unwrap();
        })
        .await
        .unwrap();
    });
}

#[cfg(panic = "unwind")]
#[test]
fn spawn_local_still_panics_on_worker() {
    // The worker-local feature must not change `spawn_local` semantics:
    // outside a LocalSet it panics, it does not fall back to worker-local.
    let rt = rt(2);
    rt.block_on(async {
        let panicked = tokio::spawn(async {
            std::panic::catch_unwind(|| {
                drop(tokio::task::spawn_local(async {}));
            })
            .is_err()
        })
        .await
        .unwrap();
        assert!(panicked);
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

#[test]
fn regular_tasks_progress_with_global_queue_interval_one() {
    // With `global_queue_interval(1)` the worker-local priority slot fires
    // on every tick. It must still fall through to the regular queues, so a
    // worker-local task that always reschedules itself cannot starve
    // regular tasks on a single-worker runtime, where no other worker could
    // steal them. The task self-wakes from `poll` (unlike `yield_now`,
    // whose wakes are deferred until the worker is out of work), so it is
    // runnable again on every tick.
    struct SelfWake {
        stop: Arc<AtomicBool>,
        started: Option<std::sync::mpsc::Sender<()>>,
    }

    impl std::future::Future for SelfWake {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
            }
            if self.stop.load(Ordering::SeqCst) {
                std::task::Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .global_queue_interval(1)
        .build()
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    {
        let _guard = rt.handle().enter();
        let stop = stop.clone();
        run_on_worker(0, move || {
            spawn_worker_local(SelfWake {
                stop,
                started: Some(started_tx),
            });
        });
    }

    // Only spawn the regular task once the worker-local task is running.
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    rt.spawn(async move {
        done_tx.send(()).unwrap();
    });

    let done = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    stop.store(true, Ordering::SeqCst);
    done.expect("regular task starved by worker-local task");
}

#[test]
fn worker_local_tasks_visible_in_metrics() {
    let rt = rt(1);
    let metrics = rt.handle().metrics();
    assert_eq!(metrics.num_alive_tasks(), 0);

    let (observed_tx, observed_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();

    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            // This closure runs inside a worker-local wrapper task (one
            // alive task being polled). The task spawned here is bound and
            // queued but not yet polled: one more alive task, one queued.
            spawn_worker_local(async move {
                release_rx.await.unwrap();
            });

            let metrics = tokio::runtime::Handle::current().metrics();
            observed_tx
                .send((
                    metrics.num_alive_tasks(),
                    metrics.worker_local_queue_depth(0),
                ))
                .unwrap();
        });
    }

    let (alive, depth) = rt.block_on(observed_rx).unwrap();
    assert_eq!(alive, 2);
    assert_eq!(depth, 1);

    release_tx.send(()).unwrap();

    // The task completes and is released on the worker thread; wait for the
    // relaxed metric counters to observe both updates.
    for _ in 0..500 {
        if metrics.num_alive_tasks() == 0 && metrics.worker_local_queue_depth(0) == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(metrics.num_alive_tasks(), 0);
    assert_eq!(metrics.worker_local_queue_depth(0), 0);
}

#[cfg(target_has_atomic = "64")]
#[test]
fn worker_local_polls_hit_poll_time_histogram() {
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(1);
    builder.enable_metrics_poll_time_histogram();
    let rt = builder.build().unwrap();

    let (tx, rx) = oneshot::channel();
    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            spawn_worker_local(async move {
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                tx.send(()).unwrap();
            });
        });
    }
    rt.block_on(rx).unwrap();

    // Poll-time histogram counts are flushed to `WorkerMetrics` on worker
    // maintenance/park, which may happen slightly after `tx.send(())` wakes
    // `block_on`.
    let metrics = rt.handle().metrics();
    let mut total = 0;
    for _ in 0..500 {
        total = (0..metrics.num_workers())
            .map(|w| {
                (0..metrics.poll_time_histogram_num_buckets())
                    .map(|b| metrics.poll_time_histogram_bucket_count(w, b))
                    .sum::<u64>()
            })
            .sum();
        if total >= 12 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    // The wrapper task (>=1 poll) plus the inner task (11 polls) are the
    // only tasks on the worker, so every count is a worker-local poll.
    assert!(total >= 12, "expected >=12 worker-local polls, got {total}");
}

#[cfg(target_has_atomic = "64")]
#[test]
fn worker_local_tasks_counted_in_spawned_tasks_count() {
    let rt = rt(1);
    let metrics = rt.handle().metrics();
    assert_eq!(metrics.spawned_tasks_count(), 0);

    let (tx, rx) = oneshot::channel();
    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            let j1 = spawn_worker_local(async { 1 });
            let j2 = spawn_worker_local(async { 2 });
            spawn_worker_local(async move {
                let sum = j1.await.unwrap() + j2.await.unwrap();
                tx.send(sum).unwrap();
            });
        });
    }
    assert_eq!(rt.block_on(rx).unwrap(), 3);

    // 1 wrapper task from `run_on_worker` + 3 explicitly spawned worker-local tasks.
    assert_eq!(metrics.spawned_tasks_count(), 4);
}

#[cfg(panic = "unwind")]
#[test]
fn spawn_worker_local_respects_entered_foreign_runtime() {
    let rt_a = rt(1);
    let rt_b = rt(1);
    let handle_b = rt_b.handle().clone();

    let panicked = rt_a.block_on(async move {
        tokio::spawn(async move {
            let _enter_b = handle_b.enter();
            std::panic::catch_unwind(|| {
                drop(spawn_worker_local(async {}));
            })
            .is_err()
        })
        .await
        .unwrap()
    });
    assert!(panicked);
}

#[test]
fn run_on_worker_captures_caller_spawn_location() {
    let seen_file = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_file_hook = seen_file.clone();

    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(1);
    builder.on_task_spawn(move |meta| {
        seen_file_hook
            .lock()
            .unwrap()
            .push(meta.spawned_at().file().to_owned());
    });
    let rt = builder.build().unwrap();

    let (tx, rx) = oneshot::channel();
    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            tx.send(()).unwrap();
        });
    }
    rt.block_on(rx).unwrap();

    let files = seen_file.lock().unwrap();
    assert_eq!(files.len(), 1);
    assert!(
        files[0].ends_with("rt_worker_local.rs"),
        "expected caller location in rt_worker_local.rs, got {}",
        files[0]
    );
}

#[cfg(panic = "unwind")]
#[test]
fn shutdown_contains_panic_in_pending_spawn_request_drop() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("boom on drop");
        }
    }

    struct SetOnDrop(Arc<AtomicUsize>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let rt = rt(1);
    let drops = Arc::new(AtomicUsize::new(0));
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    // Pin a live worker-local task and occupy the single worker thread so
    // the spawn request below stays queued until `pre_shutdown` drains it.
    {
        let _guard = rt.handle().enter();
        let drops = drops.clone();
        run_on_worker(0, move || {
            let guard = SetOnDrop(drops);
            spawn_worker_local(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            busy_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
    }
    busy_rx.recv().unwrap();

    // Enqueue a spawn request whose captured value panics on drop, then
    // trigger shutdown while the worker is still blocked in the first task.
    {
        let _guard = rt.handle().enter();
        let panic_guard = PanicOnDrop;
        run_on_worker(0, move || {
            let _ = &panic_guard;
        });
    }

    let release_thread = std::thread::spawn(move || {
        // Give `drop(rt)` a moment to close `inject` before unblocking the
        // worker so `pre_shutdown` runs before `drain_spawn_requests`.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = release_tx.send(());
    });

    drop(rt);
    release_thread.join().unwrap();

    // Shutdown must not have unwound out of `pre_shutdown`: the live
    // worker-local task was still shut down and dropped cleanly.
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[cfg(target_has_atomic = "64")]
#[test]
fn busy_worker_cross_thread_wake_does_not_inflate_noop_count() {
    let rt = rt(1);
    let metrics = rt.handle().metrics();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (tx, rx) = oneshot::channel::<u32>();
    let (done_tx, done_rx) = oneshot::channel::<u32>();

    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            spawn_worker_local(async move {
                done_tx.send(rx.await.unwrap()).unwrap();
            });
            // Keep the worker busy while the foreign thread wakes the
            // worker-local task spawned above.
            ready_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        });
    }

    ready_rx.recv().unwrap();
    let noop_before = metrics.worker_noop_count(0);

    // Wake the worker-local task from a non-worker thread while worker 0 is
    // actively executing: `is_parked` is false so no spurious unpark token is
    // stored.
    tx.send(99).unwrap();
    resume_tx.send(()).unwrap();

    assert_eq!(rt.block_on(done_rx).unwrap(), 99);

    // Wait briefly for worker 0 to go idle and submit its park metrics.
    for _ in 0..50 {
        if metrics.worker_park_unpark_count(0) % 2 == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert_eq!(
        metrics.worker_noop_count(0),
        noop_before,
        "busy-worker cross-thread wake must not cause a spurious noop park"
    );
}

#[test]
fn batch_cross_thread_wake_preserves_fifo_and_drains_all() {
    let rt = rt(1);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..8 {
        let (tx, rx) = oneshot::channel::<usize>();
        txs.push(tx);
        rxs.push(rx);
    }

    let (parked_tx, parked_rx) = std::sync::mpsc::channel();

    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            for (i, rx) in rxs.into_iter().enumerate() {
                let done_tx = done_tx.clone();
                let parked_tx = parked_tx.clone();
                spawn_worker_local(async move {
                    // Register on the oneshot before signaling ready so all 8
                    // wakes travel through the worker-local inject queue.
                    let mut rx = std::pin::pin!(rx);
                    let mut signaled = false;
                    let val = std::future::poll_fn(|cx| {
                        let poll = std::future::Future::poll(rx.as_mut(), cx);
                        if !signaled {
                            signaled = true;
                            parked_tx.send(()).unwrap();
                        }
                        poll
                    })
                    .await
                    .unwrap();
                    assert_eq!(val, i);
                    done_tx.send(i).unwrap();
                });
            }
        });
    }

    // Wait for all 8 worker-local tasks to register their wakers.
    for _ in 0..8 {
        parked_rx.recv().unwrap();
    }

    // Hold worker 0 busy while waking all 8 tasks from this thread so they
    // accumulate in `inject_shared` and are spliced in one O(1) `drain_all`.
    {
        let _guard = rt.handle().enter();
        run_on_worker(0, move || {
            ready_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        });
    }
    ready_rx.recv().unwrap();

    for (i, tx) in txs.into_iter().enumerate() {
        tx.send(i).unwrap();
    }
    resume_tx.send(()).unwrap();

    let order: Vec<usize> = rt.block_on(async {
        let mut out = Vec::with_capacity(8);
        for _ in 0..8 {
            out.push(done_rx.recv().await.unwrap());
        }
        out
    });
    assert_eq!(order, (0..8).collect::<Vec<_>>());
}
