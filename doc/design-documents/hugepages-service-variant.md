# Hugepages Service Variant

## Status
- Implemented in branch `iox2-0-hugepages-design-v1-scope`
- Target branch: `main`
- Last updated: 2026-02-09

## Terminology
- **Huge page**: A memory page larger than the default page size (e.g. 2 MiB, 1 GiB on Linux).
- **hugetlbfs**: Linux filesystem interface for explicit HugeTLB page-backed files.
- **Service variant**: The backend type selected at `NodeBuilder::create::<S>()` that defines all transport/storage mechanisms.
- **Data segment**: Shared memory backing payload chunks for pub/sub and pipeline edges.

## Overview
Add a first-class service variant for Linux hugepages so payload/data segments are backed by HugeTLB pages while preserving existing iceoryx2 messaging APIs.

This design targets:
- lower TLB pressure for large/high-throughput payload streams,
- more deterministic mapping behavior for large static buffers,
- no API changes for application-level publishers/subscribers/workers.

## Requirements
- **R1: API Compatibility**: Existing messaging APIs (`publish_subscribe`, `pipeline`, `request_response`, `event`, `blackboard`) remain unchanged.
- **R2: Variant Selection**: Hugepage behavior is selected only through service variant type at node creation.
- **R3: Zero-Copy Contract**: Existing zero-copy pointer-offset handoff semantics remain unchanged.
- **R4: Boundedness**: Existing bounded-memory guarantees are preserved.
- **R5: Explicit Failure**: If hugepages are unavailable/misconfigured, creation/open fails with clear diagnostics.
- **R6: No Silent Fallback**: The hugepages variant shall not silently downgrade to regular pages.
- **R7: Dynamic Payload Support**: Dynamic payload services continue to work with resizable segment logic.
- **R8: Cross-Pattern Consistency**: Pipeline and pub/sub share the same hugepage-backed segment behavior.
- **R9: Test Coverage**: Add conformance + targeted stress tests for hugepage segment creation/open/resize.
- **R10: Variant Compatibility**: All participants of a given service instance must use backend-compatible service variants.

## Non-Goals
- Cross-platform hugepages support beyond Linux in v1.
- GPU device memory support in this effort.
- C/C++/Python custom variant selection in v1.
- Runtime switching between regular and hugepage segments.
- memfd-backed hugepage storage in v1 (requires FD control-plane over UDS for named open/list semantics).

## Use Cases
### Use-Case 1: High-Rate Vision Stream
- **As a** camera pipeline developer
- **I want** data segments backed by hugepages
- **So that** TLB misses are reduced for large frame payloads.

### Use-Case 2: Deterministic Static Buffering
- **As a** low-latency system integrator
- **I want** static segments allocated from reserved hugepages
- **So that** paging-related variance is minimized.

## Usage
Application-level usage remains identical except service type:

```rust
use iceoryx2::prelude::*;

let node = NodeBuilder::new().create::<ipc_hugepages::Service>()?;

let service = node
    .service_builder(&"Image/Stream".try_into()?)
    .publish_subscribe::<[u8]>()
    .open_or_create()?;
```

Hugepages-specific config keys:

```toml
[global.service.hugepages]
mount-path = "/dev/hugepages"
# optional; if unset, auto-detected from mount options or /proc/meminfo
hugepage-size-bytes = 2097152
```

## Design
### 1) New Service Variant
Add a new module:
- `iceoryx2/src/service/ipc_hugepages.rs`

Define `ipc_hugepages::Service` by reusing most IPC mechanisms and swapping memory backends:
- `StaticStorage`: recommended IPC
- `DynamicStorage`: recommended IPC
- `Connection`: recommended IPC
- `Event`: recommended IPC
- `Monitoring`: recommended IPC
- `Reactor`: recommended IPC
- `ArcThreadSafetyPolicy`: match `ipc::Service` and ship `_threadsafe` companion in v1
- `SharedMemory`: **hugetlbfs-backed pool allocator memory**
- `ResizableSharedMemory`: dynamic over hugepage-backed shared memory
- `BlackboardPayload`: hugetlbfs-backed bump allocator memory
- Blackboard management/metadata storage remains regular-page-backed in v1.

### 2) New CAL Backends
Add explicit hugepage backends in `iceoryx2-cal`.

#### 2.1 `dynamic_storage::hugetlbfs`
- New module: `iceoryx2-cal/src/dynamic_storage/hugetlbfs.rs`
- Implementation strategy:
- Start from `dynamic_storage::file` behavior (create file, `truncate`, mmap shared).
- Enforce mapping path to hugetlbfs mount.
- Enforce size rounding/alignment to detected hugepage size (with optional explicit override).
- Provide deterministic errors when mount/options are invalid.
- Do not use memfd in v1. memfd is deferred until FD control-plane over UDS is available.

#### 2.2 `shared_memory::hugetlbfs`
- New module: `iceoryx2-cal/src/shared_memory/hugetlbfs.rs`
- Type alias pattern mirrors existing `shared_memory::posix`/`file` wrappers:
- `common::details::Memory<Allocator, dynamic_storage::hugetlbfs::Storage<AllocatorDetails<Allocator>>>`

#### 2.3 `resizable_shared_memory::recommended_hugepages` (or `hugetlbfs`)
- New module wrapping existing `dynamic::DynamicMemory` with hugepage-backed shared memory.
- Keep current segment-id and offset model unchanged.

### 3) Configuration
Add variant-specific config section in `Config` (or module-local environment override) for:
- `hugetlbfs_mount_path`
- `hugepage_size_bytes` (optional override; default: auto-detect from hugetlbfs mount/system)

Rules:
- Values are validated at node/service creation.
- No fallback to non-hugepage mapping.
- Errors include actionable hints (mount path, required privileges, reserved pages).
- Hugepage size detection order in v1:
- explicit `hugepage_size_bytes` override (if set),
- hugetlbfs mount option (`pagesize=`),
- system hugepage inventory fallback.
- Pre-fault/pre-touch is required and always enabled for hugepage mappings in v1.
- `mlock` is not required or enforced by this variant.

### 4) Allocation and Alignment Rules
- All created hugepage-backed files are rounded up to hugepage size multiples.
- Pool allocator bucket layout remains payload-driven, but segment backing size obeys hugepage granularity.
- For dynamic payload resizing, newly created segments also obey hugepage-size multiples.

### 5) Failure Model
Surface specific failures:
- hugepage mount missing/inaccessible,
- insufficient hugepage reservation,
- invalid size alignment/config,
- permission failures on hugetlbfs path.

No silent downgrade is allowed.

### 6) Compatibility and Semantics
- `Sample`/`SampleMut` deref and pointer-offset translation remain unchanged.
- Connection semantics stay unchanged (`ZeroCopyConnection` backend remains recommended IPC).
- Pipeline inherits behavior automatically because stage edges are publish/subscribe-backed.

### 7) Service Variant Interoperability
- Interoperability is backend compatibility based, not Rust type-name equality.
- All participants opening the same service instance must use service variants that resolve the same underlying transport/storage concepts.
- Reason: runtime resource lookup/open uses the selected service variant's backend configuration (named concept configuration, path hint, suffix, and backend implementation).
- `ipc::Service` and `ipc_threadsafe::Service` are compatible because they use the same IPC backends and differ only in local thread-safety policy.
- `ipc_hugepages::Service` and `ipc::Service` are not interoperable in v1 and must not be mixed for the same service instance.
- `local::*` and `ipc::*` variants are not interoperable.
- Requirement for v1: all communicating participants for a hugepages-backed service use `ipc_hugepages::*` variants.

## Implementation Plan
### Milestone 1: Design Freeze
- Finalize module names and config schema.
- Include threadsafe companion (`ipc_hugepages_threadsafe::Service`) in v1.
- Freeze v1 scope to file-backed hugetlbfs implementation (no memfd backend).

**Results:**
- Approved design and error taxonomy.

### Milestone 2: CAL HugeTLB Dynamic Storage
- Implement `dynamic_storage::hugetlbfs` based on file-backed dynamic storage.
- Add path/mount/size validation.
- Implement hugepage size auto-detection and optional explicit override handling.

**Results:**
- Typed storage abstraction capable of hugepage-backed mmap segments.

### Milestone 3: Shared/Resizable Memory Backends
- Implement `shared_memory::hugetlbfs`.
- Implement `resizable_shared_memory` wrapper for hugepage shared memory.

**Results:**
- Drop-in allocator-compatible memory backends.

### Milestone 4: Service Variant Wiring
- Add `ipc_hugepages::Service` in `iceoryx2`.
- Add `ipc_hugepages_threadsafe::Service` in `iceoryx2`.
- Wire blackboard payload backend and data-segment config paths.
- Keep blackboard management/metadata storage on regular pages in v1.

**Results:**
- Applications can opt-in via `NodeBuilder::create::<ipc_hugepages::Service>()`.

### Milestone 5: Tests and Conformance
- Add unit tests for size rounding and config validation.
- Add integration tests for pub/sub, dynamic payload, and pipeline.
- Add conformance instantiation for hugepages variant where environment supports it.

**Results:**
- Behavioral compatibility + failure-mode coverage.

### Milestone 6: Documentation and Examples
- Add example under `examples/rust/service_types`.
- Document Linux prerequisites and troubleshooting.

**Results:**
- Usable developer path with clear operational guidance.

## Validation Plan
- Unit:
- size rounding/alignment invariants,
- invalid mount path/config rejection,
- deterministic error mapping.

- Integration:
- fixed payload pub/sub create/open/send/receive,
- dynamic payload pub/sub with resize,
- pipeline ingress/worker/egress flow on hugepages variant,
- blackboard payload basic read/write.

- Performance sanity:
- benchmark comparison (`ipc::Service` vs `ipc_hugepages::Service`) for large payload throughput and tail latency.

## Operational Prerequisites (Linux)
- HugeTLB pages reserved (`vm.nr_hugepages` or boot-time reservation).
- hugetlbfs mount configured and accessible by process user.
- Sufficient permissions/capabilities for hugetlbfs create/open/mmap.

## Safety and Determinism Notes
- Determinism improves only if hugepages are pre-reserved and allocation strategy avoids late growth.
- Dynamic resizing remains bounded by configured max reallocations and available hugepages.
- Explicit failures are safer than fallback in zero-trust or certified deployments.

## Decisions (2026-02-09)
- v1 uses file-backed hugetlbfs (`dynamic_storage::file`-style) only.
- memfd is explicitly deferred; it requires FD distribution/control-plane via UDS to support named concept semantics.
- Hugepage size is auto-detected by default, with optional explicit override.
- Thread-safe service companion is included in v1.
- Blackboard management/metadata remains regular-page-backed in v1; payload remains hugepage-backed.
- Pre-fault/pre-touch is required for hugepage mappings in v1.
- `mlock` is not enforced by this design.
- `ipc_hugepages::*` and `ipc::*` are intentionally non-mixable for the same service instance in v1.
