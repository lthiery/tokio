# `IoDriver`: a backend-agnostic io-driver seam

**Status:** implemented (mio backend only).
**Module:** `tokio/src/runtime/io/io_driver.rs`.

## Problem

`runtime::io::Registration`, the type every Tokio I/O resource sits on,
reaches directly into the concrete mio driver
(`handle.driver().io().add_source(...)`). That hard edge makes the
reactor implementation un-swappable: any experiment with a different
readiness driver (e.g. an `io_uring` `POLL_ADD_MULTI` reactor) has to
fork `Registration`, `PollEvented`, `AsyncFd`, and every cfg cascade
above them.

This document describes the seam that removes the hard edge: a
`pub(crate)` trait (`IoDriverBackend`) between `Registration` and the
reactor, held as `Arc<dyn IoDriverBackend>`, with the existing
shared-mio driver as its first (and currently only) backend.

What this change is: ownership and dispatch plumbing, plus the
backend-neutral registration boundary. What it is not: a complete
io-uring-ready interface. A working io-uring readiness backend exists
out of tree and needs machinery this PR deliberately does not carry
(lazy first-poll registration state, completion lifetime management,
runtime parking integration); see "Follow-up work" below for the honest
split.

## Shape

```text
Registration::new_with_interest_and_handle
        │
        ▼
driver::Handle::io_driver() ──► Option<&IoDriver>         (None ⇔ io disabled)
        │                              │
        ▼                              ▼
IoDriver::allocate_scheduled_io   Arc<dyn IoDriverBackend>
IoDriver::register_local               │
IoDriver::deregister                   ▼
                              impl IoDriverBackend for runtime::io::Handle
```

- `IoDriver` is a newtype over `Arc<dyn IoDriverBackend>`; the trait is
  `Send + Sync` and implemented directly by the backend's concrete
  handle type. No unsafe code in the module. The `Arc` is
  `std::sync::Arc` (not `loom::sync::Arc`) for the same reason
  `ScheduledIo` uses it: the io driver is never enabled under loom, so
  the type only needs to compile there, and std unsized coercion keeps
  construction cfg-free.
- The `IoDriver` lives where backend selection already lives: inside
  `runtime::driver::IoHandle::Enabled`, next to the concrete handle the
  driver chain parks on. `Registration` reaches it through the existing
  `scheduler::Handle::driver()` path (the same route `add_source` used),
  so scheduler-flavor handles carry no backend state.
- Registration is a two-step contract: `allocate_scheduled_io()`
  (infallible) then `register_local(&arc, source, interest)` (fallible;
  links into the `RegistrationSet` and registers with the kernel
  poller). The mio backend performs both steps inline, the exact
  work `add_source` did.
- Sources cross the boundary as `&mut dyn RegistrationSource`.
  `RegistrationSource: mio::event::Source` adds one method on unix,
  `registration_raw_fd(&self) -> Option<RawFd>`. Mio-based backends
  use only the `Source` supertrait; fd-keyed backends (io_uring
  submits `POLL_ADD_MULTI` keyed on the fd, without retaining the
  source) read the fd accessor and must fail registration when it
  returns `None`. `None` exists because not every source is fd-backed:
  FreeBSD's `poll_aio` wraps kernel AIO completion. On non-unix
  targets the trait is a blanket alias of `Source`.
- `register_local` returning `Ok` means kernel-side registration
  completed and the backend will deliver readiness to the
  `ScheduledIo`; registration failures reach the resource constructor
  synchronously, exactly as today. This contract deliberately does not
  authorize asynchronous (queued) registration: where a deferred
  kernel failure would surface, and with what error, is a real design
  question that belongs to the backend proposal that needs it, not to
  a behavior-neutral seam.
- `deregister` is synchronous at the kernel-poller boundary: after
  `Ok`, no new readiness is delivered to the `ScheduledIo` (readiness
  already dispatched may still be observed). The mio backend calls
  `Registry::deregister` before returning, then queues the
  registration-set release, unchanged.
- The trait carries only methods this change calls. Backend-specific
  needs arrive with the backends that use them; adding a method to a
  `pub(crate)` trait is trivially additive.

## Why `Arc<dyn Trait>` and not a manual vtable

Both representations were built and A/B benchmarked against each other
(same host, same overlay commit, n=5, W=1..8; run `dc72f48f`, arms
`9049c42c` manual vs `87f664c2` trait object). Echo throughput, p99
RTT, and accept rate: parity within noise at every W. The
register/deregister microloop trended slightly in the trait object's
favor at low concurrency (about -3% W1 / -7% W2), consistent with its
cheaper clone path: `Arc<dyn Trait>` clone is a direct refcount bump,
where the manual table dispatched through a `clone_data` shim.

With no measured cost anywhere, the safe representation wins: the
manual `(NonNull<()>, &'static vtable)` pair required unconditional
`unsafe impl Send/Sync` and an unchecked contract tying the vtable to
the erased handle type; the trait object needs no unsafe code at all.

## Measured cost (vs mainline)

Neutrality A/B on lounas (EPYC), tokio-benchd run `cc5c21df`:
this change (compiled code identical to the benched revision; the only
later delta is this document) vs its exact master base, both arms + an
identical bench-overlay commit, n=5, W=1..8:

- Echo throughput: -0.0% / +0.9% / -2.7% / +0.8% at W=1/2/4/8.
  p99 RTT and accept rate: within noise at every W (spans overlap,
  deltas within +-7% swinging both directions).
- Pure register/deregister microloop: +2.0% W1 / +0.2% W2 / +1.3% W4 /
  +2.4% W8, with overlapping rep ranges at every W (W1: 143-148us
  anchor vs 146-151us seam). The seam type-erases the source, so
  `Registry::{register,deregister}` dispatch dynamically; a manual
  vtable variant of this seam measured that cost at ~2-8% W1
  historically (runs `d95b7a73`, `d455931a`), and the trait object's
  cheaper clone/drop path pulls it down to the ~2% bound above.
- One representation change vs mainline: mainline stores the io
  `Handle` by value inside `driver::IoHandle::Enabled`; this branch
  adds an outer `Arc` around the handle (and `IoDriver` holds a strong
  reference). That is one extra allocation per runtime, not on the
  per-registration or per-event paths. It did not surface in any of
  the workloads above.

## Behavior notes (what this refactor does NOT change)

- Registration remains **eager**: the kernel-side registration still
  happens inside `new_with_interest_and_handle`, with the same panic
  (message and `#[track_caller]` location) when io is disabled, and the
  same error propagation. Syscall order is unchanged.
- Windows / wasi route through the same seam (`RegistrationSource` is
  a blanket alias of `Source` off-unix). FreeBSD `poll_aio` routes
  through it too, reporting `registration_raw_fd() = None`.
- `signal` / `process` drivers are untouched; they ride the same
  `runtime::io::Handle` they always did.

## Follow-up work this seam enables (not in this change)

1. **Lazy first-poll registration.** The two-step
   allocate/register split is compatible with deferring registration
   to first poll, but does not by itself provide it: an actual lazy
   backend needs additional state (deferred fd/interest storage,
   once-published `ScheduledIo`, failure caching, racing-first-poll
   resolution), and moving registration also moves the documented
   construction-time panics. That is a user-visible behavior change
   and is deliberately split into its own future proposal.
2. **An `io_uring` readiness-reactor backend.** A working
   implementation exists against this registration boundary and is the
   planned follow-up PR; benchmark results and the research lineage
   live in the tracking issue. That backend needs machinery this seam
   deliberately does not carry (CQE lifetime, parking, backend
   selection), and it also needs two contract extensions that are open
   design questions in the tracking issue: an explicitly widened
   registration-completion contract (its kernel-side registration is
   queued, not eager), and a place for backend-private per-registration
   identity (an opaque backend-owned token vs fields on the shared
   `ScheduledIo`).
