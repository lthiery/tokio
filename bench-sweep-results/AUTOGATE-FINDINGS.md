# Auto-gate sweep findings (2026-05-03)

> **Status (2026-05-07):** the three-way `shard_epoll / shard_auto /
> shard_futex` split that this document analyses no longer exists in the
> bench harness. The `TOKIO_FUTEX_PARK` env var was removed at tokio
> commit ca7537a1 ("rt(sharded-mio): remove direct-futex park branch")
> on worktree-nuke-spin — substrate selection now lives entirely inside
> the runtime (`sharded_mio_park.rs`, picked per-park from
> `worker_has_io_registered` plus the meta-watcher CAS). The harness
> collapses to a single `sharded_mio` mode; the findings below are kept
> as a historical record of *why* the auto-gate became the default.

Three-way comparison of the sharded-mio park modes, all from the **same
release binary** (built with `--cfg tokio_unstable --features
bench-sharded-mio`):

| key            | `TOKIO_FUTEX_PARK` | gate behaviour |
|----------------|--------------------|----------------|
| `shard_epoll`  | `0`                | force legacy epoll on every non-meta worker |
| `shard_auto`   | unset              | per-worker `has_io_registered` latch (new default) |
| `shard_futex`  | `1`                | force futex on every non-meta worker (upper bound) |

Criterion: `--warm-up-time 2 --measurement-time 4`. Numbers are noisy on
small inputs — the *pattern* is the signal, not individual values.

## Headline pattern

The auto-gate captures most of the futex path's win on the bench
workloads (none of which register `ScheduledIo`):

* **Big wins held by both auto and futex** — gate is taking the futex
  path successfully:
    * `sync_notify/notify_waiters/500`  auto **−28.0%**, futex −18.9%
    * `sync_notify/notify_waiters/200`  auto **−10.2%**, futex  −9.8%
    * `sync_notify/notify_one/100`      auto **−14.2%**, futex −11.3%
    * `remote_spawn/threads/{1,4,8}`    auto −6 / −17 / −13%, futex −4 / −5 / −27%
    * `sync_mpsc/contention/bounded_full_recv_many`  auto **−5.4%**, futex −5.7%

* **Where auto ≈ epoll and futex ≈ epoll** — neither path helps; gate
  decision is irrelevant. (Most `sync_mpsc` cases.)

## Cases worth a second look

These are present in *both* forced-futex and auto, so they're futex-path
issues — not auto-gate bugs:

| bench / case                          | epoll Δ (auto) | epoll Δ (futex) | comment |
|---------------------------------------|---------------:|----------------:|---------|
| `sync_notify/notify_one/10`           |  +15.0%        |  +17.3%         | small-input regression under futex |
| `sync_notify/notify_waiters/10`       |   +9.6%        |   +4.8%         | same |
| `sync_notify/notify_waiters/100`      |  +16.8%        |   +4.8%         | suspicious — auto > futex, possibly noise |
| `remote_spawn/threads/2`              |  +17.8%        |  +15.7%         | regression in both — futex path issue |
| `sync_broadcast/contention/1000`      |   +5.6%        |   +5.2%         | both regress |
| `spawn_blocking/concurrency/1`        |  +30.5%        |  +12.0%         | high variance at conc=1 |

Plausible explanation for the small-input notify regressions: at very
low contention the `futex_wait` syscall's per-call overhead exceeds the
saving from skipping the eventfd write. The previous perf attribution
(`perf-investigation/`) measured the eventfd-on-epoll cost on a sustained
high-rate wake stream where many parks amortise the kernel cost. At
N=10 the cross-worker wake rate is too low to recoup the difference.

## Validation

* Integration tests: 14/14 pass under all three modes
  (`rt_sharded_mio` + `net_sharded_mio_tcp` + `rt_sharded_mio_fanout`).
* The auto-gate is conservative: a worker that ever registers I/O stays
  on the epoll path forever. This is verified to be safe by the TCP
  tests passing with the gate on default.

## Suggested follow-ups

1. **Investigate the small-input notify regressions** under forced
   futex. Likely candidates:
    - per-park `futex_wait` syscall overhead vs the spin-window's reach
    - missing FUTEX_WAKE before `unpark` on the corner case where the
      parker raced past `PARKED_OWN_FUTEX` into `EMPTY` and back
    - measurement-time too short — re-run the small-input cases at
      `--measurement-time 20` to separate noise from signal

2. **Bump `TOKIO_PARK_SPIN_BUDGET` for the futex path only**: a longer
   spin window eats more cross-worker unparks before paying the
   `futex_wait` syscall, which is the dominant cost on small inputs.

3. **Eventually**: replace the permanent latch with a refcount + epoch
   counter so a worker that drops all its registrations can move back
   to the futex path. Not worth doing until a real workload demonstrates
   the asymmetry matters.
