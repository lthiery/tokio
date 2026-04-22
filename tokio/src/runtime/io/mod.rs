#![cfg_attr(
    not(all(feature = "rt", feature = "net", feature = "io-uring", tokio_unstable)),
    allow(dead_code)
)]
mod driver;
use driver::{Direction, Tick};
pub(crate) use driver::{Driver, Handle, ReadyEvent};

mod registration;
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
}

use crate::util::ptr_expose::PtrExposeDomain;
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
