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

// Backend-agnostic `IoDriver` (`Arc<dyn IoDriverBackend>`). Today the
// only implementor is the mio `Handle`. See
// `tokio/docs/io-driver-vtable.md`.
pub(crate) mod io_driver;

cfg_io_uring_reactor! {
    // Experimental single shared io_uring readiness reactor, an
    // alternative backend behind the `IoDriverBackend` seam. Selected by
    // `Builder::enable_uring_reactor()`.
    pub(crate) mod uring_arm_table;
    pub(crate) mod uring_driver;
    pub(crate) mod uring_reactor;
}

use crate::util::ptr_expose::PtrExposeDomain;
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
