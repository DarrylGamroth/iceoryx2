# MPMC Blackboard (Idiomatic iceoryx2)

## Summary
Enable multiple concurrent writers per key for the blackboard pattern while keeping iceoryx2's lock-free, preallocated, port-centric design. The solution uses per-writer slots and a lock-free "latest" pointer per entry. `max_writers` defaults to `1` to preserve current behavior.

## Goals
- Multiple concurrent writers to the same key.
- Lock-free reads and writes.
- Fixed-capacity, preallocated shared memory.
- Preserve existing reader APIs and zero-copy write flow.

## Non-Goals
- Unbounded or dynamically growing writer capacity.
- Background threads for arbitration or coordination.
- Changing semantics of existing messaging patterns.

## Design Overview
Each entry stores an `EntryMgmt` header plus a fixed array of writer slots. Each writer port owns a slot (by writer index) for the lifetime of the port. Readers use a per-entry atomic "latest" pointer to pick the current slot.

### Entry Layout
- `EntryMgmt`
- `slots[max_writers]`

### EntryMgmt
- `latest: AtomicU64` packs `(seq, slot_index)`
- `seq: AtomicU64` monotonically increases per update

### Slot Content
- `UnrestrictedAtomic<Stamped<T>>`
- `Stamped<T> = { seq: u64, value: T }`

### Write Algorithm
1. `seq = entry.seq.fetch_add(1) + 1`
2. Store `Stamped { seq, value }` in the writer's slot.
3. CAS `latest` to `(seq, slot_index)` if newer.

### Read Algorithm
1. Load `latest` (Acquire), unpack `(seq, slot_index)`.
2. Load `Stamped` from that slot.
3. If `stamped.seq != seq`, retry.
4. Return `stamped.value` and generation counter `seq`.

### Limits
- `max_writers` is clamped to `MAX_WRITER_SLOTS` (2^16 = 65536) since `latest` packs the slot index into 16 bits.
- `max_writers == 0` is adjusted to `1`.

## Configuration Changes
- Add `max_writers` to `config::Blackboard`.
- Add `max_writers` to blackboard static config and builder.
- Default `max_writers = 1`.

## Implementation Phases
- [x] Phase 1: Config, static config, builder, and docs updates.
- [x] Phase 2: Shared entry layout helpers and MPMC entry mgmt.
- [x] Phase 3: Writer path updates for per-writer slots.
- [x] Phase 4: Reader path updates for latest-slot reads.
- [x] Phase 5: FFI updates and conformance tests.
- [x] Phase 6: Documentation updates and validation.

## Progress Log
- 2026-02-04: Plan created.
- 2026-02-05: Implementation completed, tests and FFI updated, phases closed.
- 2026-02-05: `cargo test -p iceoryx2-conformance-tests service_blackboard` passed.
- 2026-02-05: C++ FFI tests failed in IPC list tests due to an unexpected existing service ("Demo/Service"). Blackboard-only filter passed. Python tests not run (missing `poetry`).
- 2026-02-05: C++ FFI tests passed after isolating config in list-related tests.
- 2026-02-05: Python tests passed via Poetry (`maturin develop`, then `pytest tests/*`).
- 2026-02-05: Python tests re-run after installing `patchelf`; 423 passed.
