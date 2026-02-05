# Contiguous Chunk Loaning for Pub/Sub (Design Sketch)

## Summary
Extend the pub/sub data segment allocator and publisher API to support loaning **contiguous groups of fixed-size chunks** so a producer can hand a single contiguous frame to GenTL while still **sending sub-chunks independently** as they become ready.

This adds:
- A **group allocator** capability in the shared-memory pool allocator.
- A new publisher API to loan **N contiguous chunks** in one call.
- A **GroupHandle** that yields per-chunk `SampleMutUninit` handles with a shared lifetime.

## Problem Statement
GenTL expects **contiguous frame buffers** (no scatter-gather). A producer also wants to **send partial data** (tiles/stripes) as they are filled.

Current pub/sub API returns **single buckets** from a lock-free pool allocator with **no adjacency guarantees**, so a “frame divided into K contiguous chunks” cannot be loaned today.

## Goals
- Loan **K contiguous chunks** from the static pool in a single call.
- Preserve existing **lock-free** allocation and return behavior.
- Allow **independent `send()` per chunk**.
- Ensure **exactly-once return** of the entire group (no leaks/double-free).
- Remain compatible with **static allocation** (no realloc).

## Non-Goals
- Guarantee contiguous chunks across **different publishers**.
- Support **dynamic resizing** while preserving contiguity.
- Provide **streaming read-while-write** semantics (requires custom protocol).

## High-Level API

### New Publisher API
```rust
impl Publisher<..., [Payload], ...> {
    pub fn loan_contiguous_chunks_uninit(
        &self,
        chunks: usize,
        elements_per_chunk: usize,
    ) -> Result<SampleChunkGroupUninit<...>, LoanError>;
}
```

### Group Handle
```rust
pub struct SampleChunkGroupUninit<...> {
    // underlying contiguous allocation
}

impl SampleChunkGroupUninit<...> {
    pub fn len(&self) -> usize;
    pub fn chunk(&mut self, index: usize)
        -> SampleMutUninit<..., [MaybeUninit<Payload>], ...>;
}
```

Each `chunk(i)` returns a per-chunk `SampleMutUninit` that can be filled and `send()` independently.

## Allocator Changes

### Unique Index Set
Update file: `iceoryx2-bb/lock-free/src/mpmc/unique_index_set.rs`

Add a lock-free **range acquisition** method:
```
acquire_raw_index_range(k) -> Result<Range(start, len), Failure>
```

Options:
- **LIFO scan for k contiguous indices** in the free list.
- Maintain a side bitmap or fixed-size interval tree (more complex).

We can keep the same allocator memory layout, but add:
- A **small side array** tracking contiguous availability (unsafe but relocatable).
- Or a **fallback slow-path**: repeatedly acquire single indices and check for adjacency, rolling back if not contiguous.

### PoolAllocator / ShmAllocator
Update file: `iceoryx2-cal/src/shm_allocator/pool_allocator.rs`

Add:
```
allocate_contiguous(layout, count) -> Result<PointerOffset>
deallocate_contiguous(offset, count)
```

This maps `count` contiguous buckets starting at a bucket boundary.

## Data Segment Changes
Update file: `iceoryx2/src/port/details/data_segment.rs`

Add:
```
allocate_contiguous(layout, count) -> Result<ShmPointer>
```
Returns a pointer to the first bucket, with the guarantee that `count` buckets are contiguous.

## Publisher Changes
Update file: `iceoryx2/src/port/publisher.rs`

Add a new method:
```
loan_contiguous_chunks_uninit(chunks, elements_per_chunk)
```

Steps:
1. Compute `sample_layout(elements_per_chunk)`.
2. Call `data_segment.allocate_contiguous(sample_layout, chunks)`.
3. Create a `GroupHandle` tracking:
   - base pointer
   - `chunks`
   - `sample_layout.size()`
   - shared state

Each chunk maps to:
```
offset_i = base_offset + i * sample_layout.size()
```

## Sending Semantics
- Each chunk is **independent**:
  - It has its own `Header`, `UserHeader`, `payload`.
  - `send()` publishes that one chunk.
- The group **releases all buckets** only when all chunk handles are dropped or sent.

### Return Strategy
- `SampleChunkGroupUninit` owns the group.
- Each `chunk(i)` yields a sub-handle that **borrows** the group.
- Group keeps a counter: `remaining`.
- When `remaining == 0`, group releases all buckets at once.

## Safety/Correctness
- All `chunk(i)` offsets must be within bounds.
- Each chunk’s `Header` and `UserHeader` are initialized separately.
- No double release: only the group releases.
- Individual chunks cannot outlive the group (lifetime ties).

## Config/Static Constraints
- Requires `AllocationStrategy::Static` for contiguity guarantees.
- If dynamic allocation is used, contiguous allocation **may fail** frequently due to fragmentation.

## Compatibility
- Existing APIs unchanged.
- New API is additive.
- No changes required for subscribers.

## Testing Strategy
- Unit tests for allocator:
  - contiguous acquisition success/failure
  - concurrent allocate/release
- Publisher tests:
  - correct per-chunk send
  - correct release of group
  - failure when pool cannot satisfy contiguous group
- Conformance stress test:
  - multiple publishers loaning groups
  - verify no overlap and no leaks

## Open Questions
- Should contiguous groups be **exclusive to one publisher** (simpler invariants)?
- Do we need a **timeout/blocking** version of `loan_contiguous_chunks_uninit()`?
- Should `elements_per_chunk` be fixed at service creation time?

## Recommended Next Step
Prototype the allocator changes + a minimal Rust API, then evaluate:
- overhead of contiguous acquisition
- success rate under load
- impact on lock-free guarantees
