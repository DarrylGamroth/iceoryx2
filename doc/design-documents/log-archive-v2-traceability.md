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
| `LA2-P1-003` | Recorder `MUST` support segment preallocation and spare-segment handoff. | Phase 1 preallocation requirement | `iceoryx2/src/service/log_archive/runtime.rs` (`open_new_active_segment`, `create_spare_preallocated_segments`, `preallocate_metadata_log`) | `log_archive_recorder_rolls_segments_and_persists_segment_meta`, `log_archive_replayer_reads_entries_while_metadata_log_has_preallocated_zero_tail` | Covered | Covers segment preallocation plus metadata-log preallocation and read-path compatibility with zero-tail preallocation bytes. |
| `LA2-P1-004` | Recorder `MUST` support `Volatile`, `Async`, and `Sync` persistence modes. | Durability Modes + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`PersistenceMode`, mode branches) | `log_archive_volatile_mode_avoids_disk_artifacts`, checksum/replay tests with `Async`/`Sync` | Covered | `Sync` issues `sync_data` on append path. |
| `LA2-P1-005` | Recorder `MUST` support checksum write path. | Data integrity + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`ChecksumMode`, `EncodedFrame`) | `log_archive_replayer_detects_corrupted_payload_with_checksum` | Covered | CRC32C frame checksum persisted in header and commit log. |
| `LA2-P1-006` | Out-of-space policy `MUST` be explicit, with default `FailWriter`. | Foundational contracts + resolved decisions | `iceoryx2/src/service/log_archive/runtime.rs` (`OutOfSpacePolicy::FailWriter`, `handle_write_failure`, `handle_commit_write_failure`) | `service::log_archive::runtime::tests::fail_writer_policy_marks_recorder_degraded_on_enospc` | Covered | Deterministic ENOSPC injection path covered via unit test. |
| `LA2-P1-007` | Recorder `MUST` expose write accounting and amplification ratio. | Foundational contracts + Phase 1 | `iceoryx2/src/service/log_archive/runtime.rs` (`ArchiveRecorderStats`, `amplification_ratio`) | `cargo test -p iceoryx2 --tests` build/runtime coverage | Covered | API present and updated on append/metadata writes. |
| `LA2-P1-008` | Recorder core `MUST` expose log adapter ingest path. | Phase 1 log adapter requirement | `iceoryx2/src/service/log_archive/runtime.rs` (`append_log_record`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Input model is `LogRecordInput`. |
| `LA2-P2-001` | Replayer `MUST` support `read_at_sequence(sequence)`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_at_sequence`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Returns `Ok(None)` for unavailable sequence. |
| `LA2-P2-002` | Replayer `MUST` support `read_range(sequence_start, max_records)`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_range`) | `log_archive_recorder_rolls_segments_and_persists_segment_meta`, budget test | Covered | Spec signature updated from end-sequence to max-records. |
| `LA2-P2-003` | Replayer `MUST` support `seek(sequence)` + `next()`. | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`seek`, `next`) | `log_archive_replayer_honors_replay_budget_limits` | Covered | Cursor-based sequential replay implemented. |
| `LA2-P2-004` | Replayer `MUST` support locator replay (`read_at_locator`). | Random Access Contract + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`read_at_locator`) | `log_archive_recorder_and_replayer_support_sequence_and_locator_reads` | Covered | Uses physical segment id/generation/offset/len. |
| `LA2-P2-005` | Replayer `MUST` support `read_many_locators` preserving input order. | Random Access Contract + resolved decisions | `iceoryx2/src/service/log_archive/runtime.rs` (`read_many_locators`) | `log_archive_replayer_read_many_locators_preserves_input_order` | Covered | Order is asserted with shuffled locator input. |
| `LA2-P2-006` | Replayer `MUST` verify checksum and report corruption errors. | Data integrity + Phase 2 | `iceoryx2/src/service/log_archive/runtime.rs` (`ChecksumMismatch`) | `log_archive_replayer_detects_corrupted_payload_with_checksum` | Covered | Corruption test mutates persisted segment bytes. |
| `LA2-P2-007` | Replay path `MUST` support bounded budget controls. | Phase 2 replay I/O budget controls | `iceoryx2/src/service/log_archive/runtime.rs` (`ReplayBudget`, `next_batch`, `read_range`) | `log_archive_replayer_honors_replay_budget_limits` | Covered | Limits apply to record count and total bytes. |
| `LA2-P2-008` | Replayer `MUST` validate locator target availability and frame bounds. | Metadata contract validation requirements | `iceoryx2/src/service/log_archive/runtime.rs` (`MissingSegment`, frame length/header checks) | `log_archive_replayer_reports_missing_segment_for_invalid_locator`, `log_archive_replayer_reports_invalid_frame_length_for_locator_bounds_mismatch` | Covered | Explicit negative locator tests cover availability and bounds mismatches. |
| `LA2-P2-009` | Sequence replay `MUST` work without `segment.idx`. | Random Access Contract | `iceoryx2/src/service/log_archive/runtime.rs` (uses `commit.idxlog` + segment reads only) | Integration tests pass without index files | Covered | `segment.idx` not required in implemented path. |

## Gap List
- No Phase 0-2 gaps remain.

## Deferred Beyond Phase 2
- `LA2-ARC-001`: Ack-level APIs (`Accepted`, `DurableData`, `DurableDataAndCommitLog`) with timeout semantics.
- `LA2-ARC-002`: Replay/ingest isolation envelope validation under concurrent load.

## Verification Evidence
- Command: `cargo test -p iceoryx2 --tests`
- Command: `cargo test -p iceoryx2 log_archive -- --nocapture`
- Last successful run: 2026-02-08
- Relevant test files:
- `iceoryx2/tests/log_archive_file_header_tests.rs`
- `iceoryx2/tests/log_archive_recorder_replayer_tests.rs`
- `iceoryx2/src/service/log_archive/runtime.rs` (unit tests)
