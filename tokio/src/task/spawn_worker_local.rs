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
/// Panics if called from outside a multi-threaded runtime worker thread —
/// for example from a `current_thread` runtime, from a
/// [`spawn_blocking`](crate::task::spawn_blocking) closure, or from inside
/// [`task::block_in_place`]. Note that the thread calling
/// [`Runtime::block_on`](crate::runtime::Runtime::block_on) is *not* a
/// worker thread: to use this function from the future passed to
/// `block_on` (including an `async fn main`), first move onto a worker
/// with [`tokio::spawn`](crate::spawn) or [`run_on_worker`].
///
/// # Examples
///
/// ```
/// use std::rc::Rc;
///
/// #[tokio::main(flavor = "multi_thread")]
/// async fn main() {
///     // `tokio::spawn` first: the `block_on` thread is not a worker.
///     let handle = tokio::spawn(async {
///         let local = tokio::task::spawn_worker_local(async {
///             // `Rc` is `!Send`, but this task never changes threads.
///             let value = Rc::new(42);
///             *value
///         });
///
///         local.await.unwrap()
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

/// Runs a closure on a specific worker thread of the current multi-threaded
/// runtime.
///
/// The closure runs on the target worker's thread, from where it can call
/// [`spawn_worker_local`] to create tasks bound to that worker. This is the
/// building block for setting up per-worker state (for example, one reactor
/// task per worker).
///
/// The closure body runs inside a worker-local task, so a panic in it is
/// contained like any other task panic.
///
/// This is fire-and-forget: there is no handle to await completion or
/// observe panics. To get results out, have the closure spawn a task and
/// send over a channel. If the runtime is shutting down, the closure may be
/// dropped without running.
///
/// # Panics
///
/// Panics if `worker_index` is greater than or equal to the number of worker
/// threads, or if called from outside a multi-threaded runtime.
///
/// # Examples
///
/// ```
/// #[tokio::main(flavor = "multi_thread", worker_threads = 2)]
/// async fn main() {
///     let (tx, rx) = tokio::sync::oneshot::channel();
///
///     tokio::task::run_on_worker(1, move || {
///         tokio::task::spawn_worker_local(async move {
///             let index = tokio::runtime::worker_index();
///             tx.send(index).unwrap();
///         });
///     });
///
///     assert_eq!(rx.await.unwrap(), Some(1));
/// }
/// ```
///
/// **Note**: This is an [unstable API][unstable]. The public API of this may
/// break in 1.x releases. See [the documentation on unstable
/// features][unstable] for details.
///
/// [unstable]: crate#unstable-features
#[track_caller]
pub fn run_on_worker<F>(worker_index: usize, f: F)
where
    F: FnOnce() + Send + 'static,
{
    with_multi_thread_handle("run_on_worker", |handle| {
        handle.push_worker_local_spawn_request(worker_index, wrap_spawn_request(f));
    });
}

/// Runs a closure on every worker thread of the current multi-threaded
/// runtime.
///
/// The closure is called once per worker, on that worker's thread, with the
/// worker's index as its argument. See [`run_on_worker`] for the execution
/// and panic semantics of each invocation.
///
/// # Panics
///
/// Panics if called from outside a multi-threaded runtime.
///
/// **Note**: This is an [unstable API][unstable]. The public API of this may
/// break in 1.x releases. See [the documentation on unstable
/// features][unstable] for details.
///
/// [unstable]: crate#unstable-features
#[track_caller]
pub fn run_on_each_worker<F>(f: F)
where
    F: FnOnce(usize) + Clone + Send + 'static,
{
    with_multi_thread_handle("run_on_each_worker", |handle| {
        for index in 0..handle.num_workers() {
            let f = f.clone();
            handle.push_worker_local_spawn_request(index, wrap_spawn_request(move || f(index)));
        }
    });
}

/// Wraps a user closure so it executes inside a worker-local task on the
/// target worker: the task harness contains panics and fires the usual task
/// hooks. The outer closure runs in the worker's spawn-request drain, where
/// the core is available, so the inner spawn cannot fail.
fn wrap_spawn_request<F>(f: F) -> Box<dyn FnOnce() + Send>
where
    F: FnOnce() + Send + 'static,
{
    Box::new(move || {
        let fut_size = std::mem::size_of::<F>();
        let _ = spawn_worker_local_inner(async move { f() }, SpawnMeta::new_unnamed(fut_size));
    })
}

#[track_caller]
fn with_multi_thread_handle<R>(
    api_name: &str,
    f: impl FnOnce(&crate::runtime::scheduler::multi_thread::Handle) -> R,
) -> R {
    use crate::runtime::scheduler;

    match context::with_current(|handle| match handle {
        scheduler::Handle::MultiThread(handle) => Some(f(handle)),
        #[allow(unreachable_patterns)]
        _ => None,
    }) {
        Ok(Some(ret)) => ret,
        Ok(None) => panic!("`{api_name}` requires the multi-threaded runtime"),
        Err(e) => panic!("`{api_name}` failed: {e}"),
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

    #[cfg(all(
        tokio_unstable,
        feature = "taskdump",
        feature = "rt",
        target_os = "linux",
        any(
            target_arch = "aarch64",
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "s390x"
        )
    ))]
    let future = task::trace::Trace::root(future);
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
