# Pipeline Messaging Pattern (Plan)

## Status
- Completed (Rust + first-class service-level FFI)
- Branch: `feature/pipeline-pattern-design`
- Last updated: 2026-02-06

## Summary
Add an idiomatic Rust `Pipeline` API for staged, ordered, lock-free handoff through a fixed sequence of processing stages.

The first version composes a pipeline from internal publish-subscribe edge services and keeps capacities bounded per edge.

## Goals
- Add an idiomatic Rust `pipeline::<Payload>()` API integrated into existing builder/port-factory patterns.
- Keep runtime lock-free on the hot path.
- Keep all memory preallocated and bounded.
- Preserve zero-copy semantics at ingress/egress; stage forwarding in this MVP uses explicit receive-loan-send composition.
- Provide explicit backpressure behavior when downstream stages are saturated.

## Non-Goals
- Dynamic stage creation/removal while running.
- Heterogeneous payload types per stage in v1.
- Cross-service transaction semantics.
- Best-effort reordering across stages.
- Blocking/waiting stage-role APIs in C/C++/Python in this branch.

## Terminology
- `Stage`: A numbered processing step in the pipeline (`0..N-1`).
- `Stage Worker`: Port that receives samples for one stage and forwards them.
- `Ingress`: Port that injects new samples into stage 0.
- `Egress`: Port that receives samples that finished stage `N-1`.
- `In-flight sample`: A sample currently owned by ingress, a stage worker, or egress.

## Requirements
- `R1`: The pipeline shall have a fixed number of stages configured at service creation.
- `R2`: The pipeline shall provide bounded in-flight capacity configured at service creation.
- `R3`: Stage handoff shall be lock-free and bounded.
- `R4`: A sample shall be owned by exactly one port at any instant.
- `R5`: Stage order shall be preserved per sample (no skipping stages).
- `R6`: Failure to forward due to full downstream capacity shall be observable and recoverable.
- `R7`: Service open shall validate stage/capacity compatibility like existing patterns.
- `R8`: FFI bindings shall expose equivalent configuration and error semantics.
- `R9`: Conformance tests shall cover all supported service flavors (`ipc`, `ipc_threadsafe`, `local`, `local_threadsafe`).
- `R10`: Examples shall demonstrate minimal ingress-worker-egress operation.
- `R11`: v1 shall allow exactly one active worker per stage.
- `R12`: Stage processing in v1 shall use receive-loan-send forwarding with in-place mutation on the forwarded sample.
- `R13`: The design shall be fanout-ready, but v1 shall default to exactly one egress consumer.
- `R14`: API shape shall follow existing publish-subscribe idioms (`builder` flow, `loan/send/receive` semantics, static/dynamic config access patterns).
- `R15`: Service builder shall expose the standard lifecycle methods (`open_or_create`, `open`, `create`, and `*_with_attributes`).
- `R16`: Receive semantics shall follow existing non-blocking shape (`receive() -> Result<Option<_>, ReceiveError>`).
- `R17`: Pipeline shall support pub/sub-style dynamic payload semantics at ingress (`[T]` payload + `loan_slice_uninit(len)`), with bounded maximum configured at creation time.
- `R18`: Pipeline shall support pub/sub-style user headers (`user_header::<H>()`) with stage-to-stage propagation and in-place mutation.

## Proposed API Shape (Draft)
Rust naming follows existing builder conventions and may be adjusted during implementation review.

### Service Builder
```rust
let pipeline = node
    .service_builder(&"ImagePipeline".try_into()?)
    .pipeline::<FrameChunk>()
    .number_of_stages(4)
    .max_in_flight_samples(64)
    .max_nodes(20)
    .create()?;
```

### Dynamic Payload (Ingress)
```rust
let pipeline = node
    .service_builder(&"ImagePipeline".try_into()?)
    .pipeline::<[u8]>()
    .max_in_flight_samples(64)
    .initial_max_slice_len(4096)
    .create()?;

let ingress = pipeline.ingress_builder()
    .initial_max_slice_len(4096)
    .create()?;

let mut sample = ingress.loan_slice_uninit(frame_len)?;
// write payload bytes
sample.send()?;
```

### Service Builder Lifecycle
```rust
let pipeline = node
    .service_builder(&"ImagePipeline".try_into()?)
    .pipeline::<FrameChunk>()
    .number_of_stages(4)
    .max_in_flight_samples(64)
    .open_or_create()?;

let _opened = node
    .service_builder(&"ImagePipeline".try_into()?)
    .pipeline::<FrameChunk>()
    .number_of_stages(4)
    .max_in_flight_samples(64)
    .open()?;

let _created = node
    .service_builder(&"ImagePipeline2".try_into()?)
    .pipeline::<FrameChunk>()
    .number_of_stages(4)
    .max_in_flight_samples(64)
    .create()?;
```

### Ports
```rust
let ingress = pipeline.ingress_builder().create()?;
let stage1 = pipeline.worker_builder(0).create()?;
let stage2 = pipeline.worker_builder(1).create()?;
let stage3 = pipeline.worker_builder(2).create()?;
let egress = pipeline.egress_builder().create()?;
```

### Data Flow
```rust
let mut sample = ingress.loan()?;
// initialize payload for stage 0
*sample.payload_mut() = FrameChunk::default();
sample.send()?;

if let Some(mut work) = stage1.receive()? {
    // in-place mutation
    *work.payload_mut() = FrameChunk::default();
    work.send()?;
}
```

## Idiomatic API Conventions
- Service entrypoint follows existing pattern builder style:
- `node.service_builder(...).pipeline::<Payload>()`
- Optional user header follows pub/sub conventions:
- `.user_header::<UserHeader>()`
- Lifecycle methods mirror existing patterns:
- `open_or_create`, `open`, `create`, `open_or_create_with_attributes`, `open_with_attributes`, `create_with_attributes`
- Port factory role builders follow `<role>_builder()` naming:
- `ingress_builder()`, `worker_builder(stage_id)`, `egress_builder()`
- Sample flow mirrors pub/sub/request-response semantics:
- `loan()`, `loan_uninit()`, `receive()`, `send()`
- `receive()` follows the existing non-blocking pattern and returns `Result<Option<_>, ReceiveError>`
- Dynamic payload support mirrors pub/sub semantics:
- payload type may be `[T]`
- ingress supports `loan_slice_uninit(len)` with configured `initial_max_slice_len`
- no unbounded runtime growth in v1
- Runtime state introspection follows `dynamic_config()` count/list style:
- `number_of_ingress_ports()`, `number_of_workers(stage_id)`, `number_of_egress_ports()`
- `list_ingresses(...)`, `list_workers(stage_id, ...)`, `list_egresses(...)`

## Decisions (2026-02-05)
- `D1`: One worker per stage in v1 (safest deterministic scheduling baseline).
- `D2`: In-place mutation is the primary processing model for stage workers.
- `D3`: Fanout is a forward-compatibility target, but v1 behavior is single egress consumer by default.
- `D4`: Public API follows publish-subscribe conventions as closely as possible.
- `D5`: Forwarding between stages uses `send()` (no pipeline-specific `send_to_next_stage()` in public API).
- `D6`: Stage role builder name is `worker_builder(stage_id)` to stay concise and consistent with existing role-builder naming.

## Design Approach
Use a per-stage lock-free queue topology by composing existing publish-subscribe services.

- For `number_of_stages = N`, create `N + 1` internal edge services:
- ingress -> stage 0, stage 0 -> stage 1, ..., stage N-1 -> egress
- Each edge is configured as:
- `max_publishers = 1`, `max_subscribers = 1`
- `subscriber_max_buffer_size = max_in_flight_samples`
- `subscriber_max_borrowed_samples = max_in_flight_samples`
- `history_size = 0`
- Worker behavior:
- receive from input edge
- loan uninitialized sample from output edge
- copy payload into forwarded sample
- mutate forwarded sample in place
- send to next edge
- Ordering model:
- FIFO per edge
- no global total-order guarantee across different edges

## Metrics & Observability Model
Follow existing iceoryx2 practice: expose control-plane runtime state via `dynamic_config` and operation outcomes via return values.

- `dynamic_config` style metrics:
- current participant counts (per role)
- list APIs with per-port details (ids, node ids, capacity-relevant fields)
- operation-result metrics:
- send/notify operations returning delivered/connected counts where applicable

Pipeline v1 metrics should therefore include:
- `number_of_ingress_ports`
- `number_of_stage_workers(stage_id)`
- `number_of_egress_ports`
- list APIs for each role with ids/node ids
- optional per-stage queue depth snapshots if lock-free read is cheap and deterministic

## Decision Checklist
- [x] `DCL1` Backpressure policy at stage boundaries
- Selection: return error on full downstream queue (`QueueFull`-style), no implicit drop.
- [x] `DCL2` Blocking behavior defaults
- Selection: non-blocking `receive()` as public default; no blocking receive/forward API in v1.
- Note: this strictly follows existing pub/sub/request-response receive semantics (`Result<Option<_>, _>`).
- [x] `DCL3` Sample recycle trigger
- Selection: recycle occurs when terminal egress-owned sample is dropped/released.
- [x] `DCL4` Worker failure behavior
- Selection: node/port cleanup path reclaims worker-owned samples and returns them to pool.
- [x] `DCL5` Ordering contract
- Selection: per-boundary FIFO only; no stronger global ordering.
- [x] `DCL6` Queue sizing model
- Selection: single `max_in_flight_samples` controls all stage-boundary queues in v1.
- [x] `DCL7` Open-compatibility rules
- Selection: `number_of_stages` must match exactly; capacity-related fields use minimum-supported semantics like existing builders.
- [x] `DCL8` Dynamic introspection scope
- Selection: counts + list-details in v1; queue-depth snapshots deferred unless proven cheap and stable.
- [x] `DCL9` Fanout rollout
- Selection: API remains fanout-ready but v1 enforces single egress consumer with default/limit `1`.
- [x] `DCL10` Error taxonomy
- Selection: add dedicated create/open/runtime pipeline errors mapped across Rust/C/C++/Python like existing patterns.
- [x] `DCL11` Ownership/security guardrails
- Selection: stage progression is tokenized; forwarding without ownership is impossible via safe API.
- [x] `DCL12` v1 acceptance criteria
- Selection: conformance matrix green for all service flavors + no dynamic allocations in steady-state hot path.
- [x] `DCL13` Dynamic payload support scope
- Selection: support pub/sub-style dynamic payloads (`[T]`, `loan_slice_uninit`) at ingress with bounded configured max slice length; no unbounded growth.

## Long-Term Integration Surface
The following areas must receive `Pipeline` wiring equivalent to existing patterns:

- `iceoryx2/src/service/messaging_pattern.rs`
- `iceoryx2/src/service/static_config/messaging_pattern.rs`
- `iceoryx2/src/service/dynamic_config/mod.rs`
- new modules under:
- `iceoryx2/src/service/static_config/pipeline.rs`
- `iceoryx2/src/service/dynamic_config/pipeline.rs`
- `iceoryx2/src/service/builder/pipeline.rs`
- `iceoryx2/src/service/port_factory/pipeline.rs`
- port APIs:
- `iceoryx2/src/port/*` for ingress, stage worker, egress
- FFI/C++/Python enum translation, builders, static config accessors

## Error Model (Draft)
- Create/Open errors mirror existing patterns:
- `IncompatibleMessagingPattern`
- `DoesNotSupportRequestedAmountOfNodes`
- `DoesNotSupportRequestedPipelineStages`
- `DoesNotSupportRequestedInFlightSamples`
- Runtime send/forward errors:
- `OutOfMemory` / `QueueFull` equivalent
- `ConnectionBroken` equivalent
- `SampleAlreadyReleased` guard errors

## Implementation Phases
- [x] Phase 1: Finalize semantics and freeze API names/types.
- [x] Phase 2: Implement Rust service builder wiring (`service_builder(...).pipeline::<Payload>()`).
- [x] Phase 3: Implement pipeline port factory roles (`ingress_builder`, `worker_builder`, `egress_builder`).
- [x] Phase 4: Implement runtime behavior for fixed and dynamic payloads (`[T]`) with bounded capacities.
- [x] Phase 5: Add Rust integration tests for lifecycle, flow, and stage-bound checks.
- [x] Phase 6: Add/update Rust examples and top-level docs to expose pipeline usage.
- [x] Phase 7: Add dynamic-payload + user-header pipeline example set and integration coverage.

## Follow-Up Phases
- [ ] Add first-class static/dynamic config messaging-pattern integration if pipeline becomes a standalone service kind.
- [x] Extend C/C++/Python bindings with equivalent pipeline service-level APIs (builder lifecycle, config, metrics, errors).
- [x] Extend C/C++/Python bindings with runtime stage-role endpoint APIs (`ingress`/`worker`/`egress`) when required.
- [x] Add cross-language and matrix conformance suites for pipeline.

## Validation Plan
- Rust:
- `cargo test -p iceoryx2 --test service_pipeline_tests`
- targeted unit tests for queue/state invariants and ownership transitions
- Examples:
- `cargo check -p example --example pipeline_ingress --example pipeline_worker --example pipeline_egress`

Follow-up validation once bindings are added:
- C++:
- extend service + builder tests for pipeline creation/open/config mismatch
- Python:
- add builder/static-config/runtime tests matching existing harness style
- Cross-language:
- rust/c/c++/python pipeline smoke flow

## Open Questions
- Should per-stage queue depth be included in `dynamic_config` v1 or deferred?

## Progress Log
- 2026-02-05: Initial planning document created.
- 2026-02-05: Decisions recorded (single worker per stage, in-place mutation, fanout-ready with single-consumer default, pub/sub-style API).
- 2026-02-05: API draft aligned to idiomatic lifecycle/port-flow conventions (`open/create` methods, `send()`, role-builder naming).
- 2026-02-05: Decision checklist updated for strict API alignment; `receive()` semantics now match existing non-blocking `Option` pattern.
- 2026-02-05: Dynamic payload decision recorded: pub/sub-style bounded dynamic slice payloads are supported at ingress.
- 2026-02-05: Implemented Rust pipeline builder and port factory modules and wired them into `service::builder` and `service::port_factory`.
- 2026-02-05: Added integration tests in `iceoryx2/tests/service_pipeline_tests.rs` (fixed payload flow, dynamic payload flow, worker stage bounds, open/create lifecycle).
- 2026-02-05: Added Rust pipeline examples at `examples/rust/pipeline/ingress.rs`, `examples/rust/pipeline/worker.rs`, and `examples/rust/pipeline/egress.rs` and updated docs where pipeline was previously marked as planned.
- 2026-02-06: Added C FFI pipeline service-level APIs and tests (`service_builder_pipeline` + `port_factory_pipeline` + static config wiring).
- 2026-02-06: Added Python pipeline service-level bindings and tests (`ServiceBuilder.pipeline(...)`, `PortFactoryPipeline`, static config + messaging pattern/error wiring).
- 2026-02-06: Added C++ first-class pipeline API surface (`ServiceBuilder::pipeline`, `PortFactoryPipeline`, `StaticConfigPipeline`, `Pipeline*Error` enums), enum translation wiring, and dedicated pipeline tests.
- 2026-02-06: Added dedicated user-header mismatch diagnostics (`IncompatibleUserHeaderType`) across Rust, C FFI, and C++ API translations with integration test coverage.
- 2026-02-06: Added runtime stage-role endpoint parity for C/C++/Python (`ingress_builder`, `worker_*_builder`, `egress_builder`) and dynamic role-list APIs for C/C++/Python pipeline port factories.
- 2026-02-06: Added pipeline conformance matrix coverage across Rust service flavors (`ipc`, `local`, `ipc_threadsafe`, `local_threadsafe`) in `iceoryx2/conformance-tests/tests/service_pipeline_tests.rs`.
- 2026-02-06: Added first-class pipeline examples for C/C++/Python as three separate entities (`ingress`, `worker`, `egress`), including dynamic payload + user-header examples for C++/Python and user-header stage forwarding for C.
