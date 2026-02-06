# Log Messaging Pattern (V1)

## Status
- Implemented (v1 core)
- Target branch: `design/log-messaging-pattern`
- Last updated: 2026-02-06
- Progress:
- [x] Milestone 1 - API and Config Design
- [x] Milestone 2 - Core Pattern Wiring
- [x] Milestone 3 - Lock-Free Data Path (via pub/sub transport reuse + log sequencing)
- [x] Milestone 4 - Dynamic Payload and User Header
- [x] Milestone 5 - FFI/Binding surface for messaging pattern/introspection
- [x] Milestone 6 - Rust examples/docs updates

## Related Documents
- V2 archive/replay extension: `doc/design-documents/log-archive-v2.md`

## Terminology
- **Log service**: A messaging service that stores messages in append order with bounded in-memory retention.
- **Appender**: The writer port that appends entries to the log.
- **Tailer**: The reader port that consumes entries with its own cursor.
- **Sequence**: Monotonic global entry number assigned at append time.
- **Retention window**: The bounded in-memory range of sequence numbers that can still be read.

## Overview
Add a first-class `log` messaging pattern to iceoryx2. The v1 pattern is an append-only, bounded, shared-memory stream with multi-writer and multi-reader support.

This fills a gap between:
- `publish_subscribe` (delivery queues and optional history),
- `blackboard` (latest value per key),
- `event` (signal-only notification),
- `request_response` (bidirectional request workflow).

## Requirements
- **R1: First-Class Pattern**: The pattern is exposed through `node.service_builder(...).log::<Payload>()`.
- **R2: MPMC**: Support multiple concurrent appenders and tailers.
- **R3: Global Ordering**: Every committed entry has a unique monotonic sequence number.
- **R4: Independent Cursors**: Tailers track progress independently without global reader locks.
- **R5: Bounded Memory**: Retention is bounded and configured at service creation.
- **R6: Overflow Policy**: Behavior under full retention is configurable (`Block`, `DropOldest`, `DropNewest`).
- **R7: Zero-Copy Semantics**: Support `loan_uninit` plus `send` style data path consistent with existing APIs.
- **R8: Dynamic Payload Parity**: Support fixed-size and dynamic `[T]` payloads like pub/sub semantics.
- **R9: User Header Support**: Support custom user header on samples, aligned with existing messaging patterns.
- **R10: Introspection**: Expose pattern-specific metrics in service details/introspection.
- **R11: Service Attributes**: Support `create/open/open_or_create` with attributes using the same attribute model as existing messaging patterns.
- **R12: Start Modes**: Tailer supports at least `start_from_oldest()` and `start_from_newest()`.

## Non-Goals
- Durable on-disk log persistence.
- Random-access replay APIs.
- Built-in metadata database or query engine in iceoryx2 core.
- Built-in payload checksum verification in the v1 in-memory hot path.
- Exactly-once delivery semantics across process crashes.
- Cross-machine replication.
- Stream processing operators (filter/map/reduce) in core API.

## Use Cases
### Use-Case 1: Replayable In-Memory Telemetry Window
- **As a** diagnostics consumer
- **I want** to start from the oldest retained entry
- **So that** I can replay recent events for troubleshooting.

### Use-Case 2: Multi-Consumer Audit Feed (In-Memory)
- **As a** compliance/audit subsystem
- **I want** independent tailers with their own cursor positions
- **So that** multiple tools can consume the same stream at different rates.

### Use-Case 3: High-Rate Processing Pipeline Edge
- **As a** pipeline worker
- **I want** lock-free append/read behavior
- **So that** throughput remains predictable under contention.

## Proposed Usage
### Service Creation
```rust
use iceoryx2::prelude::*;

let node = NodeBuilder::new().create::<ipc::Service>()?;

let log = node
    .service_builder(&"Telemetry/Log".try_into()?)
    .log::<[u8]>()
    .user_header::<u64>()
    .max_appenders(8)
    .max_tailers(16)
    .retention_size(4096)
    .overflow_policy(OverflowPolicy::Block)
    .open_or_create()?;
```

### Write Path
```rust
let appender = log.appender_builder().create()?;

let mut sample = appender.loan_slice_uninit(1024)?;
let payload = sample.payload_mut();
payload.fill(0xAA);
sample.user_header_mut().set(42);
sample.send()?;
```

### Read Path
```rust
let tailer = log
    .tailer_builder()
    .start_from_oldest()
    .create()?;

loop {
    let sample = tailer.receive_blocking()?;
    let seq = sample.header().sequence();
    let payload = sample.payload();
    let user = sample.user_header();
    // process payload
}
```

## API Shape (Idiomatic Alignment)
- Entry point remains `service_builder`.
- Pattern-specific builder mirrors existing style (`open`, `create`, `open_or_create`).
- Port factory exposes role builders (`appender_builder`, `tailer_builder`).
- Data path mirrors pub/sub where possible (`loan_uninit`, `send`, `receive`).
- Blocking receive is the default documented style (`receive_blocking`) for idiomatic usage.
- Attribute-aware open/create methods are supported (`*_with_attributes`).
- Tailer start modes include oldest and newest (`start_from_oldest`, `start_from_newest`).
- Static and dynamic payload support follow existing payload model (`T` and `[T]`).

## Interoperability with Other Services
Each service instance remains single-pattern. Interoperability is achieved by composing services in one node or across nodes.

### Pub/Sub
- Use separate services for live fanout and replay: `publish_subscribe` for low-latency delivery, `log` for replay and cursor-based consumption.
- Bridge `pub/sub -> log` with an adapter that subscribes live samples and appends equivalent log entries for replay/audit.
- Bridge `log -> pub/sub` with an adapter that tails selected log entries and republishes them to live subscribers.
- Keep backpressure isolated by default so log overflow behavior does not stall pub/sub unless explicitly configured by the adapter.

### Blackboard
- Use `blackboard` for latest-state snapshots and `log` for state-change history.
- Typical composition is write current state to blackboard and append transition/event records to log.
- Consumers can query current state from blackboard and replay change history from log.

### Event
- Use `event` as control-plane signaling and `log` as data-plane history.
- Example: notifier emits `new batch available` and tailers wake to consume from their current log cursor.

### Request/Response
- Keep request workflows in `request_response`.
- Optionally append request/response audit records to `log` out-of-band.
- This avoids coupling service latency to audit retention behavior.

## Design
### 1) Pattern and Config Types
- Add `MessagingPattern::Log` in:
- `iceoryx2/src/service/messaging_pattern.rs`
- `iceoryx2/src/service/static_config/messaging_pattern.rs`
- `iceoryx2/src/service/dynamic_config/mod.rs`

- Add static config module:
- `iceoryx2/src/service/static_config/log.rs`
- Fields: `max_appenders`, `max_tailers`, `max_nodes`, `retention_size`, `overflow_policy`, payload metadata.

- Add dynamic config module:
- `iceoryx2/src/service/dynamic_config/log.rs`
- Runtime state for appender/tailer presence and dead-node cleanup.

### 2) Builder and Port Factory
- Add builder entry method:
- `Builder::log<PayloadType>() -> log::Builder<...>`

- New builder module:
- `iceoryx2/src/service/builder/log.rs`
- Validation/errors consistent with other patterns.

- New port factory module:
- `iceoryx2/src/service/port_factory/log.rs`
- Creates `Appender` and `Tailer`.

### 3) Ports and Sample Types
- New ports:
- `iceoryx2/src/port/appender.rs`
- `iceoryx2/src/port/tailer.rs`

- Reuse existing sample abstractions where possible to keep user-facing API consistent:
- typed and dynamic payload support,
- optional user header support,
- zero-copy ownership transfer lifecycle aligned with current patterns.

### 4) Lock-Free Data Path
- Retention storage as fixed ring of slots.
- Global atomic sequence reservation for append.
- Two-phase publish:
- reserve sequence/slot,
- write payload/header/user header,
- publish commit marker.

- Tailer read:
- reads next expected sequence,
- validates commit marker/state,
- advances cursor atomically only for that tailer.

- No mutex in hot path for append/read.

### 5) Overflow and Slow-Reader Handling
- Default overflow policy is `Block`.
- `Block`: appender waits until retention has space.
- `DropNewest`: incoming append is rejected/dropped when full.
- `DropOldest`: overwrite oldest retained sequence (lagging tailers detect skip).

- Tailers detect data loss by sequence gaps and receive explicit non-fatal gap status/error so loss is never silent.

### 6) Introspection and Metrics
- Extend service details with log-specific fields:
- `retention_size`,
- `oldest_sequence`,
- `newest_sequence`,
- `committed_entries`,
- `dropped_entries`,
- live appender/tailer counts.

- Expose counters via existing introspection pattern (same transport used by other service details).

### 7) FFI and Binding Support
- Extend C FFI messaging enum with `LOG`.
- Add C API builder/ports equivalent to Rust API.
- Propagate to C++ and Python bindings after C API stabilization.

## Validation Plan
- Unit tests:
- sequence monotonicity under multi-appender contention,
- slot commit visibility rules,
- overflow policy correctness.

- Integration tests:
- multi-appender/multi-tailer ordering and cursor independence,
- startup modes (`oldest`, `newest`, explicit sequence),
- slow tailer behavior with each overflow policy,
- dynamic payload + user header compatibility.

- Stress tests:
- high contention append/read throughput with no allocation in hot path,
- dead node cleanup with active tailers/appenders.

- FFI tests:
- C round-trip for create/open, append, receive, overflow behavior.

## Milestones
### Milestone 1 - API and Config Design
- Finalize naming (`appender/tailer`), config fields, and overflow semantics.
- Add design-level tests and acceptance criteria.

**Results:**
- Frozen public API proposal for Rust and C FFI.

### Milestone 2 - Core Pattern Wiring
- Add `Log` variant to service/static/dynamic messaging enums.
- Add builder and port factory scaffolding.

**Results:**
- Service can be created/opened as `log::<Payload>()`.

### Milestone 3 - Lock-Free Data Path
- Implement ring, sequence reservation, commit protocol, and tailer cursor logic.
- Implement `loan_uninit` and `send` path for appender.

**Results:**
- Functional append/read behavior for typed payloads.

### Milestone 4 - Dynamic Payload and User Header
- Add dynamic payload support (`[T]`).
- Add user header parity and validation.

**Results:**
- Feature parity with pub/sub payload ergonomics.

### Milestone 5 - Introspection and FFI
- Expose `MessagingPattern::Log` in C/C++/Python messaging-pattern translation and service introspection mappings.
- Provide static-config translation for log services in C using the existing publish-subscribe config layout.

**Results:**
- Cross-language visibility for discovering log services and their messaging pattern.

### Milestone 6 - Examples and Docs
- Add Rust examples for appender/tailer workflows.
- Add Rust dynamic-payload and user-header log examples.
- Document overflow and slow-reader behavior.

**Results:**
- End-user documentation and runnable examples.

## Resolved Decisions
- Default overflow policy is `Block`.
- Gap detection is surfaced explicitly as a non-fatal receive status/error, not hidden metadata.
- V1 tailing remains strictly linear; query/filter-driven sparse selection is deferred to v2 replayer and app-owned metadata services.
- V1 API is built on the existing pub/sub transport behavior with log-first naming (`appender`/`tailer`) and sequence metadata.
- Tailer start-mode customization and dedicated blocking receive helpers are deferred.
