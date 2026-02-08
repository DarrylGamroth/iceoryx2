# Log Archive V2 Traceability Matrix

## Status
- Draft
- Last updated: 2026-02-08
- Source specification: `doc/design-documents/log-archive-v2.md`
- Scope of this matrix: Phase 0, Phase 1, and Phase 2 requirements and evidence.

## Legend
- `Covered`: requirement is implemented and has automated verification evidence.
- `Partial`: requirement is implemented in part or verified incompletely.
- `Gap`: requirement has no implementation or no acceptable verification evidence yet.

## Requirements Matrix
| ID | Requirement | Source | Implementation | Verification Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `LA2-FF-001` | Archive file header decode `MUST` validate magic, kind, version, flags, CRC, and segment identity. | `log-archive-v2.md` Binary Header + Conformance checks | `iceoryx2/src/service/log_archive/mod.rs` | `log_archive_file_header_tests` (`log_archive_file_header_v1_*`) | Covered | Includes golden fixture test. |
| `LA2-P1-001` | Segment data writer `MUST` append aligned frames. | Phase 1: aligned append | `iceoryx2/src/service/log_archive/runtime.rs` (`align_up(..., 8)`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Alignment is enforced in frame encoder. |
| `LA2-P1-002` | Recorder `MUST` roll segment and seal metadata on rollover/finalize. | Phase 1: roll + `.meta` seal flow | `iceoryx2/src/service/log_archive/runtime.rs` (`seal_active_segment_internal`) | `log_archive_recorder_rolls_segments_and_persists_segment_meta` | Covered | Writes `.meta` summary and catalog entries. |
| `LA2-P1-003` | Recorder `MUST` support segment preallocation and spare-segment handoff. | Phase 1 preallocation requirement | `iceoryx2/src/service/log_archive/runtime.rs` (`open_new_active_segment`, `create_spare_preallocated_segments`) | `log_archive_recorder_rolls_segments_and_persists_segment_meta` | Partial | Segment preallocation covered; metadata-log preallocation not implemented yet. |
| `LA2-P1-004` | Recorder `MUST` support `Volatile`, `Async`, and `Sync` persistence modes. | Durability Modes + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`PersistenceMode`, mode branches) | `log_archive_volatile_mode_avoids_disk_artifacts`, checksum/replay tests with `Async`/`Sync` | Covered | `Sync` issues `sync_data` on append path. |
| `LA2-P1-005` | Recorder `MUST` support checksum write path. | Data integrity + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`ChecksumMode`, `EncodedFrame`) | `log_archive_replayer_detects_corrupted_payload_with_checksum` | Covered | CRC32C frame checksum persisted in header and commit log. |
| `LA2-P1-006` | Out-of-space policy `MUST` be explicit, with default `FailWriter`. | Foundational contracts + resolved decisions | `iceoryx2/src/service/log_archive/runtime.rs` (`OutOfSpacePolicy::FailWriter`) | Unit/integration behavior path present; no deterministic ENOSPC test yet | Partial | Policy is explicit and defaulted; ENOSPC injection test pending. |
| `LA2-P1-007` | Recorder `MUST` expose write accounting and amplification ratio. | Foundational contracts + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`ArchiveRecorderStats`, `amplification_ratio`) | `cargo test -p iceoryx2 --tests` build/runtime coverage | Covered | API present and updated on append/metadata writes. |
| `LA2-P1-008` | Recorder core `MUST` expose log adapter ingest path. | Phase 1 log adapter requirement | `iceoryx2/src/service/log_archive/runtime.rs` (`append_log_record`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Input model is `LogRecordInput`. |
| `LA2-P2-001` | Replayer `MUST` support `read_at_sequence(sequence)`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_at_sequence`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Returns `Ok(None)` for unavailable sequence. |
| `LA2-P2-002` | Replayer `MUST` support `read_range(sequence_start, max_records)`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_range`) | `log_archive_recorder_rolls_segments_and_persists_segment_meta`, budget test | Covered | Spec signature updated from end-sequence to max-records. |
| `LA2-P2-003` | Replayer `MUST` support `seek(sequence)` + `next()`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`seek`, `next`) | `log_archive_replayer_honors_replay_budget_limits` | Covered | Cursor-based sequential replay implemented. |
| `LA2-P2-004` | Replayer `MUST` support locator replay (`read_at_locator`). | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_at_locator`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Uses physical segment id/generation/offset/len. |
| `LA2-P2-005` | Replayer `MUST` support `read_many_locators` preserving input order. | Random Access Contract + resolved decisions | `iceoryx2/src/service/log_archive/runtime.rs` (`read_many_locators`) | API implemented; direct order assertion test pending | Partial | Add explicit order-preservation test. |
| `LA2-P2-006` | Replayer `MUST` verify checksum and report corruption errors. | Data integrity + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`ChecksumMismatch`) | `log_archive_replayer_detects_corrupted_payload_with_checksum` | Covered | Corruption test mutates persisted segment bytes. |
| `LA2-P2-007` | Replay path `MUST` support bounded budget controls. | Phase 2 replay I/O budget controls | `iceoryx2/src/service/log_archive/runtime.rs` (`ReplayBudget`, `next_batch`, `read_range`) | `log_archive_replayer_honors_replay_budget_limits` | Covered | Limits apply to record count and total bytes. |
| `LA2-P2-008` | Replayer `MUST` validate locator target availability and frame bounds. | Metadata contract validation requirements | `iceoryx2/src/service/log_archive/runtime.rs` (`MissingSegment`, frame length/header checks) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` plus checksum/corruption tests | Partial | More negative locator tests should be added. |
| `LA2-P2-009` | Sequence replay `MUST` work without `segment.idx`. | Random Access Contract | `iceoryx2/src/service/log_archive/runtime.rs` (uses `commit.idxlog` + segment reads only) | Integration tests pass without index files | Covered | `segment.idx` not required in implemented path. |
| `LA2-ARC-001` | Ack-level APIs (`Accepted`, `DurableData`, `DurableDataAndCommitLog`) `MUST` exist with timeout semantics. | Foundational throughput contracts | Not implemented in current API | No tests | Gap | Planned for later phase/API integration. |
| `LA2-ARC-002` | Replay/ingest isolation envelope `MUST` be verified under concurrent load. | Foundational contracts + Phase 2 exit criteria | Partial budget control primitives only | No concurrent envelope benchmark/test in-tree | Gap | Requires load harness and acceptance thresholds. |

## Gap List
| Gap ID | Requirement IDs | Description | Planned Action |
| --- | --- | --- | --- |
| `LA2-GAP-001` | `LA2-P1-003` | Metadata-log preallocation/roll preallocation is not yet implemented. | Add metadata-log preallocation strategy and tests in next recorder iteration. |
| `LA2-GAP-002` | `LA2-P1-006` | No deterministic ENOSPC injection test proving `FailWriter` behavior path. | Add fault-injection test harness for write/preallocate failures. |
| `LA2-GAP-003` | `LA2-P2-005`, `LA2-P2-008` | Missing explicit negative locator tests and explicit ordering assertion for `read_many_locators`. | Add dedicated replayer conformance tests for bad locator/bounds/order. |
| `LA2-GAP-004` | `LA2-ARC-001`, `LA2-ARC-002` | Foundational ack contract and replay/ingest envelope verification not implemented. | Track as post-Phase-2 core architecture work item. |

## Verification Evidence
- Command: `cargo test -p iceoryx2 --tests`
- Last successful run: 2026-02-08
- Relevant test files:
- `iceoryx2/tests/log_archive_file_header_tests.rs`
- `iceoryx2/tests/log_archive_recorder_replayer_tests.rs`
