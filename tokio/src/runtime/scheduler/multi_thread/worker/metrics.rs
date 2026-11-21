use super::Shared;

impl Shared {
    pub(crate) fn injection_queue_depth(&self) -> usize {
        self.inject.len()
    }
}

cfg_unstable_metrics! {
    impl Shared {
        pub(crate) fn worker_local_queue_depth(&self, worker: usize) -> usize {
            let depth = self.remotes[worker].steal.len();

            #[cfg(all(tokio_unstable, feature = "worker-local"))]
            let depth = depth + self.worker_locals[worker].queued_tasks();

            depth
        }
    }
}
