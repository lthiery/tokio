//! Loom models for worker-local task machinery: the per-worker inject queue,
//! the spawn-request flag protocol, and their park/unpark interplay.
//!
//! Use `LOOM_MAX_PREEMPTIONS=1` to do a "quick" run as a smoke test.

use crate::runtime::tests::loom_oneshot as oneshot;
use crate::runtime::{self, Runtime};
use crate::task::spawn_worker_local;

use loom::sync::atomic::AtomicBool;
use loom::sync::Arc;

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::Ordering::SeqCst;
use std::task::{Context, Poll};

fn mk_pool(num_threads: usize) -> Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        // Set the intervals to avoid tuning logic
        .event_interval(2)
        .build()
        .unwrap()
}

/// A future that becomes ready when a foreign loom thread flips a gate and
/// wakes it. Polled inside a worker-local task, the wake exercises the
/// cross-thread path: worker-local inject queue push + direct unpark.
fn gated_by_thread() -> impl Future<Output = ()> {
    let gate = Arc::new(AtomicBool::new(false));
    let mut fired = false;

    poll_fn(move |cx| {
        if !fired {
            let gate = gate.clone();
            let waker = cx.waker().clone();

            loom::thread::spawn(move || {
                gate.store(true, SeqCst);
                waker.wake_by_ref();
            });

            fired = true;
            return Poll::Pending;
        }

        if gate.load(SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
}

/// A worker-local task woken from a foreign thread must run: the wake goes
/// through the worker-local inject queue and the unpark, and the worker's
/// park transitions must observe it (no lost wakeup).
#[test]
fn cross_thread_wake() {
    loom::model(|| {
        let pool = mk_pool(1);
        let (tx, rx) = oneshot::channel();

        pool.spawn(async move {
            let join = spawn_worker_local(async {
                gated_by_thread().await;
            });
            join.await.unwrap();
            tx.send(());
        });

        rx.recv();
        drop(pool);
    });
}

/// A spawn request pushed from a non-worker thread must be observed by the
/// target worker whether it is busy, searching, or parked: this models the
/// `has_spawn_requests` flag protocol against the park/unpark transitions.
#[test]
fn spawn_request_delivery() {
    loom::model(|| {
        let pool = mk_pool(1);
        let (tx, rx) = oneshot::channel();

        pool.block_on(async move {
            // The `block_on` thread is not a worker: this goes through the
            // spawn-request channel and the direct unpark.
            crate::task::run_on_worker(0, move || {
                tx.send(());
            });
        });

        rx.recv();
        drop(pool);
    });
}

/// A spawn request racing runtime shutdown must either run or be dropped;
/// it can never be left in the queue. A leaked request holding a runtime
/// handle would keep the scheduler alive forever (`Shared` owns the
/// closure, the closure owns `Shared`), so the closure signals on drop and
/// the model requires the signal in every interleaving.
#[test]
fn spawn_request_racing_shutdown() {
    struct SendOnDrop(Option<oneshot::Sender<()>>);
    impl Drop for SendOnDrop {
        fn drop(&mut self) {
            self.0.take().unwrap().send(());
        }
    }

    loom::model(|| {
        let pool = mk_pool(1);
        let handle = pool.handle().clone();
        let (tx, rx) = oneshot::channel();

        let pusher = loom::thread::spawn(move || {
            let _enter = handle.enter();
            let guard = SendOnDrop(Some(tx));
            crate::task::run_on_worker(0, move || {
                let _guard = guard;
            });
        });

        drop(pool);
        pusher.join().unwrap();
        rx.recv();
    });
}

/// A worker-local task that never completes must still be dropped exactly
/// once at shutdown, on the worker that owns it.
#[test]
fn shutdown_drops_pending_task() {
    struct SendOnDrop(Option<oneshot::Sender<()>>);
    impl Drop for SendOnDrop {
        fn drop(&mut self) {
            self.0.take().unwrap().send(());
        }
    }

    loom::model(|| {
        let pool = mk_pool(1);
        let (spawned_tx, spawned_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();

        pool.spawn(async move {
            let guard = SendOnDrop(Some(dropped_tx));
            spawn_worker_local(async move {
                let _guard = guard;
                Pending.await;
            });
            spawned_tx.send(());
        });

        // Ensure the task is spawned before shutting down, so the model
        // always exercises pre_shutdown's worker-local cleanup.
        spawned_rx.recv();
        drop(pool);
        dropped_rx.recv();
    });
}

/// Worker-local and regular tasks sharing one worker must both complete:
/// models the run loop's interleaving of the two queues.
#[test]
fn interleaves_with_regular_tasks() {
    loom::model(|| {
        let pool = mk_pool(1);
        let (tx, rx) = oneshot::channel();

        pool.spawn(async move {
            let local = spawn_worker_local(async {
                crate::task::yield_now().await;
                1
            });
            let regular = crate::spawn(async {
                crate::task::yield_now().await;
                2
            });

            let total = local.await.unwrap() + regular.await.unwrap();
            tx.send(total);
        });

        assert_eq!(rx.recv(), 3);
        drop(pool);
    });
}

/// A `Pending` future without waker registration: used to model tasks that
/// never complete.
struct Pending;

impl Future for Pending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}
