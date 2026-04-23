# Slab-Indexed `OpState` for the Per-Worker `io_uring` Reactor

**Target file:** `tokio/src/runtime/io/uring_reactor.rs`
**Supersedes:** The time-based `deregister_retention: [Vec<Arc<ScheduledIo>>; 3]` pipeline and the `EXPOSE_IO` pointer-in-`user_data` scheme.
**Status:** Implemented.
**Related:** [Design overview](./uring-reactor-design.md).

---

## 1. Motivation

The first iteration of the per-worker reactor encoded CQE `user_data` as an exposed `*const ScheduledIo` pointer, with four reserved sentinel values (`USER_DATA_EVENTFD`, `USER_DATA_MSG_RING`, `USER_DATA_IGNORE`, and the `RESERVED_SENTINEL_FLOOR` guard). `POLL_REMOVE` races with in-flight `POLL_ADD_MULTI` CQEs: the kernel may still post CQEs carrying a `user_data` pointer after we've submitted cancellation. If the `Arc<ScheduledIo>` is freed before the last such CQE is drained, `drain_completions` dereferences a dangling pointer.

The first fix was a retention pipeline — keep the Arc alive for N park cycles after `POLL_REMOVE`, then drop. Depth was tuned empirically: 1 cycle → flaky, 2 → still flaky, 3 → stable-enough. This is **probabilistic, time-based correctness**. It is wrong on first principles:

1. The bound on N is not documented by the kernel — we're reverse-engineering it from flakiness rates.
2. Even if N is finite, the freed slot's pointer value can be reused by the allocator **before** a stale CQE lands. Any stale CQE whose `user_data` now aliases a live `ScheduledIo` silently corrupts state (observed: `SHUTDOWN.unpack(curr) == 0` assertion in `ScheduledIo::set_readiness`).
3. The 3-slot pipeline's worst-case memory retention is unbounded-in-throughput — a burst of 100k deregisters in one park cycle pins 100k `Arc<ScheduledIo>` for 3 parks regardless of whether the kernel is actually still referencing them individually.

The slab-indexed design replaces this with a **deterministic, completion-driven** handshake:

- `user_data` is a `(variant, gen, key)` triple encoded in a `u64`, **not a pointer**.
- Pointer-reuse races become structurally impossible because the kernel never sees a pointer.
- The `Arc<ScheduledIo>` lives inside `slab[key]` and is removed exactly when the CQE stream signals the op is terminal (no `IORING_CQE_F_MORE`).
- Stale CQEs arriving after a slot is recycled are detected via a generation mismatch and silently dropped.

## 2. `user_data` Encoding (64 bits)

```
bit 63      bit 56        bit 32                    bit 0
 │           │             │                         │
 ├───────────┼─────────────┼─────────────────────────┤
 │  variant  │    gen      │          key            │
 │  (8 bit)  │   (24 bit)  │       (32 bit)          │
 └───────────┴─────────────┴─────────────────────────┘
```

- **key (low 32 bits):** Slab index. Up to 2³² concurrent in-flight ops per worker — far beyond any realistic saturation point.
- **gen (middle 24 bits):** Monotonic generation counter, bumped on every slab insert. 24 bits = 16M distinct generations. Wraparound requires an in-flight CQE to survive 16M subsequent inserts on the same reactor — not physically achievable.
- **variant (top 8 bits):** Fast-path discriminator, avoids an enum match per CQE:
  - `0x00` — `VARIANT_POLL_MULTI` (the hot path; every fd readiness event)
  - `0x01` — `VARIANT_CONTROL` (one-shot ack: POLL_REMOVE, TIMEOUT, MSG_RING send)
  - `0x02` — `VARIANT_EVENTFD` (reactor-lifetime slot for the external waker)
  - `0x03` — `VARIANT_MSG_RING_INCOMING` (reactor-lifetime slot for cross-worker wakes)

### Helpers

```rust
const fn encode(variant: u8, gen: u32, key: u32) -> u64 {
    ((variant as u64) << 56) | (((gen as u64) & 0x00FF_FFFF) << 32) | (key as u64)
}
const fn decode(ud: u64) -> (u8, u32, u32) {
    ((ud >> 56) as u8, ((ud >> 32) & 0x00FF_FFFF) as u32, ud as u32)
}
```

Both `const fn` so the well-known `user_data` values for reactor-lifetime slots (`EVENTFD_UD`, `MSG_RING_INCOMING_UD`) can be compile-time constants.

### Deleted sentinels

The old `USER_DATA_EVENTFD`, `USER_DATA_MSG_RING`, `USER_DATA_IGNORE`, and `RESERVED_SENTINEL_FLOOR` are gone. `EXPOSE_IO::expose_provenance` is no longer called from this module (still used by the legacy mio driver — left untouched).

## 3. Slab Layout

```rust
struct SlotEntry {
    gen: u32,
    state: OpState,
}

enum OpState {
    /// Multi-shot POLL_ADD registration.
    /// Lifetime: from `register` until the kernel posts a CQE without
    /// `IORING_CQE_F_MORE` (either `-ECANCELED` after POLL_REMOVE, or
    /// autonomous kernel cleanup, e.g. on POLLHUP).
    PollMulti { io: Arc<ScheduledIo>, removing: bool },

    /// One-shot control op. Removed on first CQE.
    Control,

    /// The eventfd POLL_ADD_MULTI registration. Reactor-lifetime.
    Eventfd,

    /// Receive slot for incoming MSG_RING wakes. Reactor-lifetime.
    /// The slot itself has no kernel registration; peers encode this
    /// slot's user_data into their MsgRingData SQEs.
    MsgRingIncoming,
}
```

### Reactor struct

```rust
pub(crate) struct Reactor {
    ring: IoUring,
    external_wake_fd: Arc<OwnedFd>,
    ops: Slab<SlotEntry>,
    next_gen: u32,  // bumped modulo 2^24 on every insert
}
```

### Well-known slots

Inserted as the first two slab entries during `Reactor::new` so their keys are always `0` and `1`:

```rust
const KEY_EVENTFD: u32 = 0;
const KEY_MSG_RING_INCOMING: u32 = 1;

const EVENTFD_UD: u64 = encode(VARIANT_EVENTFD, 0, KEY_EVENTFD);
const MSG_RING_INCOMING_UD: u64 = encode(VARIANT_MSG_RING_INCOMING, 0, KEY_MSG_RING_INCOMING);
```

Because these slots never recycle, their generation is permanently `0` and their encoded `user_data` is a universal compile-time constant. Peers can `MSG_RING` us without needing per-peer user_data advertisement — they all use `MSG_RING_INCOMING_UD`.

## 4. State Transitions

### `Reactor::new`

1. Build ring with `SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN`.
2. Allocate eventfd.
3. `ops.insert(SlotEntry { gen: 0, state: Eventfd })` — assigns `KEY_EVENTFD = 0`.
4. `ops.insert(SlotEntry { gen: 0, state: MsgRingIncoming })` — assigns `KEY_MSG_RING_INCOMING = 1`.
5. Submit `PollAdd.multi(true)` on the eventfd with `user_data = EVENTFD_UD`.
6. Done.

### `register(fd, interest, io)`

```rust
let gen = self.bump_gen();
let key = self.ops.insert(SlotEntry {
    gen,
    state: OpState::PollMulti { io: io.clone(), removing: false },
}) as u32;
// Stamp the slab coordinates on the ScheduledIo so deregister
// (local or cross-ring) can find the slot and gen-check it.
io.uring_slab_key.store(key, Ordering::Relaxed);
io.uring_gen.store(gen, Ordering::Relaxed);
io.uring_worker.store(self.worker_idx, Ordering::Relaxed);
let ud = encode(VARIANT_POLL_MULTI, gen, key);
unsafe { self.push_sqe(PollAdd::new(Fd(fd), mask).multi(true).build().user_data(ud))? };
Ok(())
```

### `deregister(io)`

1. Load `(gen, key)` from `io.uring_slab_key`.
2. Look up `ops[key]`; verify `gen` matches and state is `PollMulti { removing: false }`.
3. Set `removing = true`. **Do not remove from slab.** The Arc stays alive.
4. Allocate a new `Control` slot for the REMOVE ack.
5. Submit `PollRemove.new(original_ud).user_data(encode(VARIANT_CONTROL, control_gen, control_key))`.
6. Return.

The slot's Arc is dropped only when the original PollMulti slot's terminal CQE arrives (next section).

### `drain_completions`

```rust
for cqe in self.ring.completion() {
    let (variant, gen, key) = decode(cqe.user_data());
    match variant {
        VARIANT_POLL_MULTI => {
            let Some(entry) = self.ops.get(key as usize) else { continue };
            if entry.gen != gen { continue; } // stale CQE from recycled slot.
            let OpState::PollMulti { io, .. } = &entry.state else { continue };
            let result = cqe.result();
            let has_more = cqueue::more(cqe.flags());
            if result >= 0 {
                let ready = ready_from_poll_flags(result);
                io.set_readiness(Tick::Set, |curr| curr | ready);
                io.wake(ready);
            }
            if !has_more {
                self.ops.remove(key as usize);  // drops the Arc.
            }
        }
        VARIANT_CONTROL => {
            if let Some(e) = self.ops.get(key as usize) {
                if e.gen == gen { self.ops.remove(key as usize); }
            }
        }
        VARIANT_EVENTFD => saw_external_wake = true,
        VARIANT_MSG_RING_INCOMING => { /* scheduler handles around park() */ }
        _ => {}  // unknown variant — ignore.
    }
}
if saw_external_wake { drain_eventfd(external_fd); }
```

**Key invariant:** An `Arc<ScheduledIo>` held in `OpState::PollMulti` is dropped if and only if we observed a CQE with `F_MORE == 0` for that slot's current generation. No time-based retention, no guessing.

### `park` / `park_timeout`

The old `rotate_deregistered()` call at the end of each park method is deleted. Shortened:

```rust
pub(crate) fn park(&mut self) -> io::Result<()> {
    self.ring.submit_and_wait(1)?;
    self.drain_completions();
    Ok(())
}
```

### `send_msg_ring(target_ring_fd)`

Unchanged except the send-ack's own user_data becomes a `Control` slot:

```rust
let (control_gen, control_key) = self.alloc_control_slot();
let sqe = MsgRingData::new(Fd(target_ring_fd), 0, MSG_RING_INCOMING_UD, None)
    .build()
    .user_data(encode(VARIANT_CONTROL, control_gen, control_key));
unsafe { self.push_sqe(sqe)? };
self.ring.submit()?;
```

The peer receives a CQE with `user_data == MSG_RING_INCOMING_UD`; our own send-ack CQE comes back as a Control.

## 5. Caller-Side Changes

### `ScheduledIo`

Gained three fields under the uring feature gate:

```rust
#[cfg(all(tokio_unstable, feature = "io-uring-reactor", ...))]
uring_slab_key: AtomicU32,  // slab index; u32::MAX = not registered.
uring_gen:      AtomicU32,  // generation of the current slot.
uring_worker:   AtomicU32,  // worker idx whose ring owns the POLL_ADD.
```

Stamped by `Reactor::register`, read by `Reactor::deregister` (to locate
the slot and gen-check against a possible slab recycle) and by the
cross-worker `POLL_REMOVE` path (to route the remove SQE to the ring
that actually owns the registration — `io_uring` scopes removes to the
originating ring). Three separate `AtomicU32`s rather than one packed
`AtomicU64` because the fields are written together on the owning
worker but read from multiple paths, and the extra atomic is cheaper
than a pack/unpack on every lookup.

### `PendingOp`

```rust
pub(crate) enum PendingOp {
    Register { fd: RawFd, interest: Interest, io: Arc<ScheduledIo> },
    Deregister { io: Arc<ScheduledIo> },
}
```

`Deregister` carries the full `Arc<ScheduledIo>`; the slab key is discovered from the stamped field. No plumbing of keys through the cross-worker queue.

## 6. Correctness Invariants

1. **Kernel never dereferences our memory.** `user_data` is a `u64` integer; not a pointer. No pointer-reuse races possible.

2. **Arc outlives kernel's last CQE reference.** Arc is dropped on terminal CQE (`F_MORE == 0`), which the kernel posts only after all pending readiness events for the poll entry have been flushed and the entry has been detached.

3. **Stale CQEs are detected, not catastrophic.** If a slot is removed and recycled before a late CQE arrives, the new slot will have a different generation. The CQE's `gen` field won't match; the CQE is silently dropped. Result: a spurious wake (or not even that, if the slot was never re-registered) — never a corrupt dispatch.

4. **`SINGLE_ISSUER` respected.** All SQE submission happens from `drain_completions`'s caller thread (the worker). Callers to `register`/`deregister` may be on other threads; their requests route through `PendingOp` and land in the worker's park loop. No cross-thread submission to a given ring.

## 7. Cost Analysis

| Dimension | Retention-pipeline (old) | Slab-handshake (current) |
|---|---|---|
| Correctness | Probabilistic; depends on unverified kernel bound | Deterministic; kernel handshake |
| Memory steady-state | `3 × dereg_burst × sizeof(Arc)` | `in_flight_ops × sizeof(SlotEntry)` |
| Memory worst-case | Unbounded in burst rate per park | Bounded by active registrations |
| Per-CQE cost | Expose-pointer + sentinel compare + enum match | `u64` decode + array index + gen compare |
| Per-register cost | 1 atomic (`expose_provenance`) | 1 slab insert (amortized O(1), no atomic) |
| `unsafe` on drain path | Yes (`from_exposed_addr`) | No |
| Debuggability | Pointer values in traces; stale CQEs silently corrupt | Slot/gen in traces; stale CQEs explicitly detected |

## 8. Open Questions

1. **Should `Control` slots track what they're acking?** Currently `Control` is opaque. If useful for diagnostics, extend to `Control { kind: ControlKind }` with variants for POLL_REMOVE, TIMEOUT, MSG_RING send-ack. Low priority.

2. **Per-reactor op-count cap?** Slab grows unboundedly in principle. In practice bounded by fd count, but misuse (deregister never called) leaks slots. Consider a debug-assertion or metric for `ops.len() > threshold`.

3. **Generation width.** 24 bits chosen for headroom + variant tag fit in 8 bits. Could tighten to 16 to widen key to 40, but 2^32 keys already exceeds any plausible in-flight count.

4. **Kernel version floor.** `IORING_CQE_F_MORE` has been stable since 5.13 (multi-shot poll). Our existing floor is 6.0 (for `DEFER_TASKRUN`). No new kernel requirement from this refactor.

## 9. Summary

The slab replaces a time-bounded probabilistic safety net with a kernel-handshake-bounded deterministic one. `user_data` becomes an integer index into a local table that holds the Arc until the kernel explicitly signals it's done. Correctness is a CQE-stream property; no magic constants.

— End —
