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

// Backend-agnostic IoDriver (manual vtable). Step-1 scope: only the uring
// vtable is populated; module gate matches `cfg_io_uring_reactor` for now.
// As more backends are ported the gate widens.
#[cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt",
    target_os = "linux",
))]
pub(crate) mod io_driver;

use crate::util::ptr_expose::PtrExposeDomain;
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
