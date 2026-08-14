#![cfg_attr(
    not(all(feature = "rt", feature = "net", feature = "io-uring", tokio_unstable)),
    allow(dead_code)
)]
mod driver;
use driver::{Direction, Tick};
pub(crate) use driver::{Driver, Handle, ReadyEvent};

pub(crate) mod registration;
pub(crate) use registration::Registration;

mod registration_set;
use registration_set::RegistrationSet;

mod scheduled_io;
use scheduled_io::ScheduledIo;

mod metrics;
use metrics::IoDriverMetrics;

cfg_io_uring_reactor! {
    // Experimental single shared io_uring reactor. Consumed by the
    // multi_thread scheduler's `UringParker` when `enable_uring_reactor()`
    // is selected on the runtime builder.
    pub(crate) mod uring_reactor;
    pub(crate) mod uring_driver;
    pub(crate) mod uring_arm_table;

    // The reactor-side slab for the in-tree `tokio::fs` io_uring ops, so a
    // uring-reactor runtime routes fs completions onto the one shared ring
    // instead of the legacy fs side-driver. `io-uring-reactor` implies the
    // `io-uring` feature (the ops themselves), so this is unconditional.
    pub(crate) mod uring_fs_ops;
}

// Backend-agnostic IoDriver (manual vtable). Populated by every io
// backend after step 3: legacy mio (`LEGACY_MIO_VTABLE`), per-worker
// uring (`URING_VTABLE`).
//
// Gated to `unix` because the legacy-mio vtable's `register_local`
// shim constructs a `mio::unix::SourceFd` from the captured `RawFd`,
// which is unix-only. Non-unix targets keep the eager `add_source`
// path in `Registration::new_with_interest`.
cfg_io_driver! {
    #[cfg(target_family = "unix")]
    pub(crate) mod io_driver;
}

// Process-wide counters for the lazy-on-first-poll registration path.
// Available under the same cfg as `io_driver`. Always incremented;
// stderr dump activates only when `TOKIO_LAZY_DEBUG=1` is set in the
// environment.
cfg_io_driver! {
    #[cfg(target_family = "unix")]
    pub(crate) mod lazy_debug;
}

use crate::util::ptr_expose::PtrExposeDomain;
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
