# Progressive publish/subscribe upstream reconciliation

The implementation branch is based on upstream commit
`04941b22e36fad80d1a45abcba5b80be1358550b` (workspace version `0.9.999`).

The current implementation still matches the assumptions in the progressive
publish/subscribe implementation plan:

- publish/subscribe delivers one shared-memory offset per sample and receiver;
- `SegmentState` reference-counts complete allocations;
- publisher loans are returned through `Sender::return_loaned_sample`;
- subscriber leases are returned through the existing completion queue;
- `MessageTypeDetails` determines internal-header, user-header, and payload
  placement as well as the pool bucket alignment and size.

The implementation therefore keeps publish/subscribe as the transport and adds
a statically compatible delivery mode, a distinct internal header, and distinct
publisher/subscriber/sample types. Ordinary `Sample` and `SampleMut` remain
unchanged. The progressive header alignment makes both the allocation alignment
and the rounded allocation size multiples of 128 bytes.

No upstream pipeline, DMA-BUF, WaitSet, or alternate sample-tracking work
invalidates the plan. WaitSet notification and external-device coherency remain
explicitly outside this implementation.

## Resolved design questions

- A progressive builder wraps the existing publish/subscribe builder and
  returns distinct progressive port and sample types. Ordinary `Sample` and
  `SampleMut` invariants are unchanged.
- The progressive header owns its origin metadata rather than embedding the
  ordinary header. This keeps the ABI and two-line layout explicit.
- `send()` starts with a zero-length prefix. Safe `write_from_slice` or unsafe
  `set_published_len` advances it afterward.
- Sending with no connected receivers succeeds and returns the active writer,
  matching ordinary delivery's successful zero-recipient result.
- Existing offsets and publisher identity provide the version-1 generation
  protection; no additional shared generation field is added.
- Version 1 includes `write_from_slice`; external acquisition uses the unsafe
  pointer and watermark APIs. WaitSet notification remains deferred.

## Crash behavior

A graceful active-writer drop release-stores `Aborted`. An abrupt publisher
process death cannot run that drop and cleanup cannot safely enumerate private
publisher allocations to mutate their headers. Existing receiver mappings and
whole-sample cleanup still keep a borrowed allocation from being unsafely
reused.

`ProgressiveSample::state_with_publisher_liveness()` resolves the terminal
liveness question explicitly. It first performs the ordinary acquire state
load and, only while the sample is still `Filling`, queries node monitoring. A
dead or already-cleaned origin is reported as a derived `Aborted` state without
modifying the shared header. The regular `state()` call remains an allocation-
and syscall-free atomic hot path. A multi-process test terminates the publisher
without running destructors and verifies that the subscriber retains its
published prefix and observes this derived abort.

## Verification boundaries

- Nightly Miri validates an in-process concurrent model that constructs only
  raw-pointer-derived unpublished write regions and acquire-bounded immutable
  prefixes.
- Compile-fail documentation tests cover the absence of whole-payload mutable
  access, mutable user-header access, `DerefMut`, and payload borrows that
  outlive the subscriber lease.
- A counting global allocator test verifies zero allocations across repeated
  `write_from_slice`, `published_len`, `payload`, and `state` calls.
- Miri does not validate external DMA behavior. Raw pointers copied by an
  external writer cannot be revoked by Rust; using them after a terminal
  operation remains a caller contract violation.

## Configuration matrix

| Area | Axis | Permutation | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Allocation layout | Allocation strategy | Static | `static_allocations_preserve_control_payload_and_stride_alignment` | Covered | Checks three simultaneous loans. |
| Allocation layout | Allocation strategy | BestFit | `best_fit_allocations_preserve_control_payload_and_stride_alignment` | Covered | Checks the resizable best-fit segment. |
| Allocation layout | Allocation strategy | PowerOfTwo | `power_of_two_allocations_preserve_control_payload_and_stride_alignment` | Covered | Checks the resizable power-of-two segment. |
| Payload layout | Requested alignment | 128-byte minimum | All three allocation-strategy tests | Covered | Header, payload, and stride remain 128-byte isolated. |
| Payload layout | Requested alignment | 4096 bytes | `progressive_mode_preserves_larger_requested_payload_alignment` | Covered | Progressive mode preserves alignments larger than its minimum. |
| Delivery failure | Receiver outcome | Earlier receiver succeeds, later receiver fails | `partial_delivery_failure_aborts_and_preserves_reference_accounting` | Covered | Uses deterministic backpressure failure injection. |
| Process failure | Dead participant | Publisher | `abrupt_publisher_process_death_is_reported_as_abort` | Covered | Uses liveness-derived abort. |
| Process failure | Dead participant | Subscriber | `abrupt_subscriber_process_death_reclaims_held_progressive_sample` | Covered | Uses a single preallocated chunk to prove reclamation. |
