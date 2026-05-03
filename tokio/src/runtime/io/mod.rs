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
    // Experimental per-worker io_uring reactor. Consumed by the
    // multi_thread scheduler's `UringParker` when `enable_uring_reactor()`
    // is selected on the runtime builder.
    pub(crate) mod uring_reactor;
    pub(crate) mod uring_driver;
    pub(crate) mod uring_arm_table;
    pub(crate) mod uring_bytes_ops;
    pub(crate) mod uring_buf_ring;
    pub(crate) mod uring_recv_multi;
}

cfg_io_sharded_mio! {
    // Experimental per-worker mio::Poll reactor. Same per-worker sharding
    // shape as the uring reactor, but the IO backend is mio rather than
    // io_uring. Companion to uring-reactor for A/B-measuring driver
    // sharding independently from io_uring. Consumed by the multi_thread
    // scheduler's `ShardedMioParker` when `enable_sharded_mio()` is set
    // on the runtime builder.
    pub(crate) mod sharded_mio_reactor;
    pub(crate) mod sharded_mio_driver;
}

// Backend-agnostic IoDriver (manual vtable). Available whenever any
// per-worker (sharded) backend is in play. Currently only the uring
// vtable is populated; sharded-mio vtable is added in step 2.
#[cfg(any(
    all(tokio_unstable, feature = "io-uring-reactor", feature = "rt", target_os = "linux"),
    all(feature = "io-sharded-mio", feature = "rt-multi-thread", target_os = "linux"),
))]
pub(crate) mod io_driver;

// Process-wide counters for the lazy-on-first-poll registration path.
// Available under the same cfg as `io_driver`. Always incremented;
// stderr dump activates only when `TOKIO_LAZY_DEBUG=1` is set in the
// environment.
#[cfg(any(
    all(tokio_unstable, feature = "io-uring-reactor", feature = "rt", target_os = "linux"),
    all(feature = "io-sharded-mio", feature = "rt-multi-thread", target_os = "linux"),
))]
pub(crate) mod lazy_debug;

use crate::util::ptr_expose::PtrExposeDomain;
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
