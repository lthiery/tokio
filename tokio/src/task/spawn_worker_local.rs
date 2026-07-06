use crate::runtime::{context, BOX_FUTURE_THRESHOLD};
use crate::task::JoinHandle;
use crate::util::trace::SpawnMeta;

use std::future::Future;

/// Spawns a `!Send` task on the current worker thread of a multi-threaded
/// runtime.
///
/// The task is bound to the worker it is spawned from: it is always polled
/// (and eventually dropped) on this worker's thread, and it is never stolen
/// by other workers. This makes it possible for the task to hold `!Send`
/// data, such as an `Rc` or a thread-affine resource, on the multi-threaded
/// runtime without a [`LocalSet`].
///
/// The spawned task may still be woken from any thread; only polling and
/// dropping are pinned.
///
/// To create worker-local tasks on a *different* worker, spawn from a
/// closure running on that worker. This must be done at a point where the
/// target worker's thread is executing your code (for example from a task
/// already running there).
///
/// # Interaction with `block_in_place`
///
/// While any worker-local tasks are alive on a worker,
/// [`task::block_in_place`] panics on that worker instead of handing its
/// work off to another thread: the handoff would move the `!Send` tasks to
/// a different thread. Spawning worker-local tasks on a worker therefore
/// disables `block_in_place` for all tasks running on that worker until the
/// worker-local tasks complete.
///
/// # Panics
///
/// Panics if called from outside a multi-threaded runtime worker thread:
/// for example from a `current_thread` runtime, from a
/// [`spawn_blocking`](crate::task::spawn_blocking) closure, or from inside
/// [`task::block_in_place`].
///
/// # Examples
///
/// ```
/// use std::rc::Rc;
///
/// #[tokio::main(flavor = "multi_thread")]
/// async fn main() {
///     let handle = tokio::task::spawn_worker_local(async {
///         // `Rc` is `!Send`, but this task never changes threads.
///         let value = Rc::new(42);
///         *value
///     });
///
///     assert_eq!(handle.await.unwrap(), 42);
/// }
/// ```
///
/// **Note**: This is an [unstable API][unstable]. The public API of this may
/// break in 1.x releases. See [the documentation on unstable
/// features][unstable] for details.
///
/// [`LocalSet`]: crate::task::LocalSet
/// [`task::block_in_place`]: crate::task::block_in_place
/// [unstable]: crate#unstable-features
#[track_caller]
pub fn spawn_worker_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let fut_size = std::mem::size_of::<F>();
    if fut_size > BOX_FUTURE_THRESHOLD {
        spawn_worker_local_inner(Box::pin(future), SpawnMeta::new_unnamed(fut_size))
    } else {
        spawn_worker_local_inner(future, SpawnMeta::new_unnamed(fut_size))
    }
}

#[track_caller]
pub(crate) fn spawn_worker_local_inner<F>(future: F, meta: SpawnMeta<'_>) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    use crate::runtime::scheduler::multi_thread::worker_local::SpawnWorkerLocalError;
    use crate::runtime::task;

    // Unlike `spawn`, the future is not rooted with `task::trace::Trace`:
    // taskdump only walks the scheduler-wide `OwnedTasks`, so worker-local
    // tasks do not appear in dumps.
    let id = task::Id::next();
    let task = crate::util::trace::task(future, "task", meta, id.as_u64());

    match context::spawn_worker_local(task, id, meta.spawned_at) {
        Ok(join_handle) => join_handle,
        Err(SpawnWorkerLocalError::NotOnWorker) => panic!(
            "`spawn_worker_local` called from outside of a multi-threaded runtime worker thread"
        ),
        Err(SpawnWorkerLocalError::NoCore) => {
            panic!("`spawn_worker_local` called from within `block_in_place`")
        }
    }
}
