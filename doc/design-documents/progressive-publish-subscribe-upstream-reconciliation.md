# Progressive publish/subscribe design and verification

The implementation is maintained against workspace version `0.9.999`.

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

## Terminology

- **announce** makes one loaned allocation discoverable to the subscribers
  connected at that moment while retaining an active writer;
- **commit** advances the contiguous initialized, CPU-visible, immutable prefix;
- **active** means the writer may still commit more bytes;
- **complete** ends a sample successfully without changing its committed length;
- **abort** ends a sample unsuccessfully while preserving its committed prefix;
  and
- **payload** is the acquire-bounded committed prefix exposed by a progressive
  subscriber sample. Capacity remains a separate property of its allocation.

## Authority and lifetime model

Progressive delivery changes access authority without transferring the
allocation to any one subscriber:

- the active writer has exclusive write authority over
  `[committed_len, capacity)`;
- every subscriber lease has shared immutable read authority over
  `[0, committed_len)`; and
- the allocation cannot be reused until the writer reference and every
  subscriber reference have been released.

The committed boundary moves monotonically. Safe Rust exposes only the
uncommitted suffix to an active writer and only the committed prefix to a
subscriber. Raw external writers carry the equivalent unsafe contract.

## Resolved design questions

- A progressive builder wraps the existing publish/subscribe builder and
  returns distinct progressive port and sample types. Ordinary `Sample` and
  `SampleMut` invariants are unchanged.
- The progressive header owns its origin metadata rather than embedding the
  ordinary header. This keeps the ABI and two-line layout explicit.
- `announce()` starts with a zero-length committed prefix. Safe
  `write_from_slice` or unsafe
  `commit_until` advances it afterward.
- Announcement with no connected receivers succeeds and returns the active writer,
  matching ordinary delivery's successful zero-recipient result.
- A subscriber lease prevents reuse while sample access is legal. No storage
  generation is required unless a future API allows buffer identity to outlive
  that lease. Application frame sequences belong in the immutable user header.
- Version 1 includes `write_from_slice`; external acquisition uses the unsafe
  pointer and commit APIs. WaitSet notification remains deferred.

## Atomic progress snapshot

The control cache line contains one `AtomicU64`. Its high 62 bits encode the
committed length and its low two bits encode `Active`, `Complete`, or `Aborted`.
A release store publishes a new committed boundary or terminal state, and one
acquire load returns both fields to a subscriber. A terminal observation can
therefore never be paired with a stale final committed length.

The encoded word is monotonic for one sample: committed length never regresses
and lifecycle state changes only from `Active` to a terminal state. This also
provides an ABA-free value for a future wait-if-unchanged notification protocol
without adding a generation counter to the version-1 sample.

## Admission, overload, and late join

`announce()` delivers the allocation offset once to subscribers connected at
that moment. A subscriber connecting afterward does not receive the active
sample because progressive services have no history. Missing a progress poll
after receiving a lease is harmless because the atomic snapshot is
authoritative.

Queue admission and the configured backpressure strategy apply only during
announcement. Commits neither enqueue offsets nor consume subscriber queue
capacity. A subscriber that retains a lease can still delay allocation reuse;
the configured queue, borrowed-sample, publisher-loan, and pool bounds therefore
remain the overload boundary for later frames. A partial announcement failure
marks the sample aborted for subscribers that already accepted the offset before
returning the publisher reference.

## Crash behavior

A graceful active-writer drop release-stores `Aborted`. An abrupt publisher
process death cannot run that drop and cleanup cannot safely enumerate private
publisher allocations to mutate their headers. Existing receiver mappings and
whole-sample cleanup still keep a borrowed allocation from being unsafely
reused.

`ProgressiveSample::state_with_publisher_liveness()` resolves the terminal
liveness question explicitly. It first performs the ordinary acquire state
load and, only while the sample is still `Active`, queries node monitoring. A
dead or already-cleaned origin is reported as a derived `Aborted` state without
modifying the shared header. The regular `state()` call remains an allocation-
and syscall-free atomic hot path. A multi-process test terminates the publisher
without running destructors and verifies that the subscriber retains its
committed prefix and observes this derived abort.

## Verification boundaries

- Nightly Miri validates an in-process concurrent model that constructs only
  raw-pointer-derived uncommitted write regions and acquire-bounded immutable
  prefixes.
- Compile-fail documentation tests cover the absence of whole-payload mutable
  access, mutable user-header access, `DerefMut`, and payload borrows that
  outlive the subscriber lease.
- A counting global allocator test verifies zero allocations across repeated
  `write_from_slice`, `committed_len`, `payload`, `snapshot`, and
  `state` calls.
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
| Service backend | Memory domain | IPC and local | `progressive_c_ffi_preserves_prefix_and_access_authority` | Covered | One generic C lifecycle test runs against both backends. |
| Commit path | Writer API | Safe copy and unsafe external commit | `committed_length_is_monotonic_bounded_and_prefix_only` and C lifecycle test | Covered | Both paths advance the same packed atomic snapshot. |
| Lifecycle | Terminal transition | Complete, explicit abort, writer-drop abort | `unannounced_drop_and_terminal_paths_return_publisher_loans`, `active_writer_drop_aborts_and_offset_is_delivered_once`, and C lifecycle test | Covered | Terminal state retains the final committed length. |
| Subscriber timing | Join relative to announcement | Connected before and after | `subscriber_connecting_after_announcement_does_not_receive_active_sample` | Covered | Confirms the zero-history late-join contract. |
| Subscriber cardinality | Readers | One and multiple | `multiple_subscribers_observe_identical_content_at_independent_speeds` | Covered | Subscriber cursors remain local. |
| Queue admission | Pressure point | Commit and new announcement | `queue_backpressure_is_evaluated_only_when_announcing_a_new_frame` | Covered | Commits do not consume queue capacity. |
| Delivery failure | Receiver outcome | Earlier receiver succeeds, later receiver fails | `partial_delivery_failure_aborts_and_preserves_reference_accounting` | Covered | Uses deterministic backpressure failure injection. |
| Concurrency | Address space | Threads and processes | `concurrent_prefix_stress_has_no_torn_blocks` and `multi_process_progressive_stress_has_no_torn_blocks` | Covered | Both validate immutable-prefix contents and terminal snapshots. |
| Process failure | Dead participant | Publisher | `abrupt_publisher_process_death_is_reported_as_abort` | Covered | Uses liveness-derived abort. |
| Process failure | Dead participant | Subscriber | `abrupt_subscriber_process_death_reclaims_held_progressive_sample` | Covered | Uses a single preallocated chunk to prove reclamation. |

## C FFI traceability

The experimental C binding uses separate progressive handle families rather
than adding mode checks to ordinary publish/subscribe handles. Unless a
function explicitly consumes an owning handle, all pointers returned from it
remain valid only while that handle remains alive. The raw external-writer
payload pointer is the documented exception: it may be retained across `announce`
and used until the resulting active writer is completed, aborted, or dropped.

| ID | Requirement | Implementation | Verification | Status |
| --- | --- | --- | --- | --- |
| CFFI-01 | The C service transition creates only progressive `[u8]` services and retains the one-publisher, zero-history, non-overflowing restrictions. | `iox2_service_builder_progressive_pub_sub` and `service_builder_progressive_pub_sub` | `progressive_c_ffi_preserves_prefix_and_access_authority` for IPC and local | Covered |
| CFFI-02 | `announce` transfers one private loan into one active-writer handle; complete, abort, and writer drop consume or release it exactly once. | `progressive_publisher` handle implementations | The FFI lifecycle test covers complete, explicit abort, and drop-induced abort. Rust core accounting tests remain authoritative for partial delivery. | Covered |
| CFFI-03 | The application user header is mutable only before `announce` and immutable afterward. | `iox2_progressive_sample_mut_uninit_user_header_mut`, writer/sample const accessors | The FFI lifecycle test initializes and reads a custom `FrameInfo` layout; no announced-writer mutable accessor exists. | Covered |
| CFFI-04 | A subscriber obtains one atomic committed-length/state snapshot and only acquire-bounded immutable payload prefixes, never full capacity. | `iox2_progressive_sample_snapshot`, `iox2_progressive_sample_payload` | The FFI lifecycle test observes active lengths 4, 6, and 8 plus the terminal length while capacity remains 32. | Covered |
| CFFI-05 | External commit publication is monotonic and bounded and documents initialization, immutability, and visibility obligations. | `iox2_progressive_sample_mut_commit_until`, `iox2_progressive_write_error_e` | The FFI lifecycle test commits externally written bytes and rejects a regressive boundary. | Covered |
| CFFI-06 | The ABI supports IPC and local services with the same error/result conventions as ordinary C bindings. | All progressive C unions and existing `IntoCInt` mappings | One generic lifecycle test is instantiated for each backend; the generated header is compiled as C11. | Covered |
| CFFI-07 | Requested payload alignment is preserved above the 128-byte progressive minimum. | Progressive builder type-detail preparation and C alignment setter | The FFI lifecycle test requests and checks 4096-byte payload alignment. | Covered |
| CFFI-08 | C subscribers can distinguish active, complete, abort, and liveness-derived publisher death without modifying shared state. | `iox2_progressive_sample_snapshot`, `iox2_progressive_sample_snapshot_with_publisher_liveness`, and state-only convenience functions | State transitions are covered in the FFI lifecycle test; abrupt publisher death remains covered by the Rust multi-process test. | Covered |

WaitSet notification, DMA cache management, C++, Python, Julia, and
camera-specific adapters remain outside this C ABI increment. The raw-pointer
commit function exposes the existing unsafe CPU/device visibility contract;
it does not make external DMA coherent.
