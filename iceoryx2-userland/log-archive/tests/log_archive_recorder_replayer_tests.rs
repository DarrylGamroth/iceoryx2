// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::num::NonZeroUsize;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use iceoryx2_bb_testing::assert_that;
use iceoryx2_userland_log_archive::log_archive::{
    ArchiveRecorderBuilder, ArchiveRecorderError, ArchiveReplayError, ArchiveReplayerBuilder,
    ArchiveSegmentTier, AsyncIoBackend, ChecksumMode, EffectiveAsyncIoBackend, LogRecordInput,
    PersistenceMode, ReplayBudget, ARCHIVE_FILE_HEADER_V1_LEN,
};

#[test]
fn log_archive_recorder_and_replayer_support_sequence_and_locator_reads() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    let mut expected_payloads = Vec::new();
    let mut expected_headers = Vec::new();
    let mut locators = Vec::new();

    for sequence in 1..=5u64 {
        let user_header = vec![sequence as u8, (sequence + 1) as u8];
        let payload = vec![sequence as u8; (sequence as usize) + 3];
        expected_headers.push(user_header.clone());
        expected_payloads.push(payload.clone());

        let commit = recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 100,
                user_header: &user_header,
                payload: &payload,
            })
            .unwrap();
        locators.push(commit.locator);
    }

    recorder.finalize().unwrap();
    assert_that!(storage_path.join("catalog.bin").exists(), eq true);
    assert_that!(metadata_path.join("commit.idxlog").exists(), eq true);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    for sequence in 1..=5u64 {
        let frame = replayer.read_at_sequence(sequence).unwrap().unwrap();
        assert_that!(frame.sequence, eq sequence);
        assert_that!(
            frame.user_header,
            eq expected_headers[(sequence - 1) as usize].clone()
        );
        assert_that!(
            frame.payload,
            eq expected_payloads[(sequence - 1) as usize].clone()
        );

        let by_locator = replayer
            .read_at_locator(locators[(sequence - 1) as usize])
            .unwrap();
        assert_that!(by_locator.sequence, eq sequence);
        assert_that!(
            by_locator.payload,
            eq expected_payloads[(sequence - 1) as usize].clone()
        );
    }
}

#[test]
fn log_archive_recorder_rolls_segments_and_persists_segment_meta() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(256)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    for sequence in 1..=12u64 {
        let user_header = vec![0xAB, sequence as u8];
        let payload = vec![sequence as u8; 16];
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 10,
                user_header: &user_header,
                payload: &payload,
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let stats = recorder.stats();
    assert_that!(stats.rolled_segments > 0, eq true);
    assert_that!(
        storage_path.join("segments/segment-1-g0.meta").exists(),
        eq true
    );

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let records = replayer
        .read_range(1, NonZeroUsize::new(12).unwrap())
        .unwrap();
    assert_that!(records.len(), eq 12);
    for (index, record) in records.iter().enumerate() {
        assert_that!(record.sequence, eq(index + 1) as u64);
    }
}

#[test]
fn log_archive_replayer_detects_corrupted_payload_with_checksum() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Sync)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    let commit = recorder
        .append_log_record(LogRecordInput {
            sequence: 1,
            event_time_ns: 42,
            user_header: &[1, 2, 3, 4],
            payload: &[9, 8, 7, 6, 5, 4, 3, 2],
        })
        .unwrap();
    recorder.finalize().unwrap();

    let segment_path = storage_path.join(format!(
        "segments/segment-{}-g{}.data",
        commit.locator.segment_id, commit.locator.segment_generation
    ));
    let mut segment_file = OpenOptions::new().write(true).open(&segment_path).unwrap();
    segment_file
        .seek(SeekFrom::Start(commit.locator.file_offset + 65))
        .unwrap();
    segment_file.write_all(&[0xEE]).unwrap();
    segment_file.flush().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let result = replayer.read_at_sequence(1);
    assert_that!(
        matches!(
            result,
            Err(ArchiveReplayError::ChecksumMismatch {
                expected: _,
                actual: _,
                locator: _
            })
        ),
        eq true
    );
}

#[test]
fn log_archive_replayer_honors_replay_budget_limits() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    let mut one_frame_len = 0usize;
    for sequence in 1..=8u64 {
        let commit = recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 100,
                user_header: &[0xAA, 0xBB],
                payload: &[0x11; 24],
            })
            .unwrap();
        if sequence == 1 {
            one_frame_len = commit.locator.frame_len as usize;
        }
    }
    recorder.finalize().unwrap();

    let mut replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .replay_budget(ReplayBudget {
            max_records_per_call: 3,
            max_bytes_per_call: one_frame_len * 2 + 8,
        })
        .open()
        .unwrap();

    let range = replayer
        .read_range(1, NonZeroUsize::new(8).unwrap())
        .unwrap();
    assert_that!(range.len(), eq 2);

    replayer.seek(1);
    let batch = replayer.next_batch(NonZeroUsize::new(8).unwrap()).unwrap();
    assert_that!(batch.len(), eq 2);
}

#[test]
fn log_archive_replayer_read_many_locators_preserves_input_order() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    let mut commits = Vec::new();
    for sequence in 1..=5u64 {
        commits.push(
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence * 1000,
                    user_header: &[0x11, 0x22],
                    payload: &[sequence as u8; 12],
                })
                .unwrap(),
        );
    }
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let requested = vec![
        commits[3].locator,
        commits[0].locator,
        commits[4].locator,
        commits[1].locator,
    ];
    let replayed = replayer.read_many_locators(&requested).unwrap();

    assert_that!(replayed.len(), eq requested.len());
    assert_that!(replayed[0].sequence, eq 4);
    assert_that!(replayed[1].sequence, eq 1);
    assert_that!(replayed[2].sequence, eq 5);
    assert_that!(replayed[3].sequence, eq 2);
}

#[test]
fn log_archive_recorder_rejects_zero_io_uring_queue_depth() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");

    let result = ArchiveRecorderBuilder::new(&storage_path)
        .io_uring_queue_depth(0)
        .create();

    assert_that!(
        matches!(
            result,
            Err(ArchiveRecorderError::InvalidConfiguration(
                "io_uring_queue_depth must be > 0"
            ))
        ),
        eq true
    );
}

#[test]
fn log_archive_recorder_supports_explicit_blocking_backend_selection() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .async_io_backend(AsyncIoBackend::Blocking)
        .create()
        .unwrap();

    assert_that!(
        recorder.configured_async_io_backend(),
        eq AsyncIoBackend::Blocking
    );
    assert_that!(
        recorder.effective_async_io_backend(),
        eq EffectiveAsyncIoBackend::Blocking
    );

    recorder
        .append_log_record(LogRecordInput {
            sequence: 1,
            event_time_ns: 1,
            user_header: &[0xAA],
            payload: &[0x01, 0x02, 0x03, 0x04],
        })
        .unwrap();
    recorder.finalize().unwrap();
}

#[test]
fn log_archive_recorder_reports_effective_backend_for_preferred_selection() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");

    let recorder = ArchiveRecorderBuilder::new(&storage_path)
        .io_uring_queue_depth(1)
        .create()
        .unwrap();
    assert_that!(
        recorder.configured_async_io_backend(),
        eq AsyncIoBackend::IoUringPreferred
    );
    #[cfg(target_os = "linux")]
    {
        let expected = if io_uring::IoUring::new(1).is_ok() {
            EffectiveAsyncIoBackend::IoUring
        } else {
            EffectiveAsyncIoBackend::Blocking
        };
        assert_that!(recorder.effective_async_io_backend(), eq expected);
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_that!(
            recorder.effective_async_io_backend(),
            eq EffectiveAsyncIoBackend::Blocking
        );
    }
}

#[test]
fn log_archive_replayer_reports_missing_segment_for_invalid_locator() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    let commit = recorder
        .append_log_record(LogRecordInput {
            sequence: 1,
            event_time_ns: 1,
            user_header: &[1],
            payload: &[2, 3, 4, 5],
        })
        .unwrap();
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let mut invalid = commit.locator;
    invalid.segment_id = commit.locator.segment_id + 999;

    let result = replayer.read_at_locator(invalid);
    assert_that!(matches!(result, Err(ArchiveReplayError::MissingSegment(_))), eq true);
}

#[test]
fn log_archive_replayer_reports_invalid_frame_length_for_locator_bounds_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    let commit = recorder
        .append_log_record(LogRecordInput {
            sequence: 1,
            event_time_ns: 99,
            user_header: &[0xAA],
            payload: &[0xBB; 9],
        })
        .unwrap();
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let mut invalid = commit.locator;
    invalid.frame_len += 8;

    let result = replayer.read_at_locator(invalid);
    assert_that!(
        matches!(
            result,
            Err(ArchiveReplayError::InvalidFrameLength {
                expected: _,
                decoded: _
            })
        ),
        eq true
    );
}

#[test]
fn log_archive_replayer_reads_entries_while_metadata_log_has_preallocated_zero_tail() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .metadata_log_preallocate_entries(8)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();

    for sequence in 1..=2u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence,
                user_header: &[0x10],
                payload: &[0x22; 8],
            })
            .unwrap();
    }
    recorder.flush().unwrap();

    let commit_log_len = std::fs::metadata(metadata_path.join("commit.idxlog"))
        .unwrap()
        .len() as usize;
    let logical_bytes = ARCHIVE_FILE_HEADER_V1_LEN + (2 * 56);
    assert_that!(commit_log_len > logical_bytes, eq true);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let frames = replayer
        .read_range(1, NonZeroUsize::new(8).unwrap())
        .unwrap();
    assert_that!(frames.len(), eq 2);
    assert_that!(frames[0].sequence, eq 1);
    assert_that!(frames[1].sequence, eq 2);
}

#[test]
fn log_archive_open_or_recover_recovers_unsealed_archive_and_continues() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    {
        let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
            .metadata_log_path(&metadata_path)
            .segment_bytes(2048)
            .segment_preallocate(false)
            .spare_preallocated_segments(0)
            .persistence_mode(PersistenceMode::Async)
            .checksum_mode(ChecksumMode::Crc32c)
            .create()
            .unwrap();
        for sequence in 1..=3u64 {
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence * 10,
                    user_header: &[0x10, 0x20],
                    payload: &[sequence as u8; 12],
                })
                .unwrap();
        }
        recorder.flush().unwrap();
    }

    let mut recovered = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();
    let recovery_status = recovered.recovery_status();
    assert_that!(recovery_status.recovered_existing_archive, eq true);
    assert_that!(recovery_status.commit_entries_loaded, eq 3);

    recovered
        .append_log_record(LogRecordInput {
            sequence: 4,
            event_time_ns: 40,
            user_header: &[0x33, 0x44],
            payload: &[0x55; 12],
        })
        .unwrap();
    recovered.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(8).unwrap())
        .unwrap();
    assert_that!(replayed.len(), eq 4);
    assert_that!(replayed[0].sequence, eq 1);
    assert_that!(replayed[1].sequence, eq 2);
    assert_that!(replayed[2].sequence, eq 3);
    assert_that!(replayed[3].sequence, eq 4);
}

#[test]
fn log_archive_open_or_recover_supports_sync_mode_restart() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    {
        let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
            .metadata_log_path(&metadata_path)
            .segment_bytes(2048)
            .segment_preallocate(false)
            .spare_preallocated_segments(0)
            .persistence_mode(PersistenceMode::Sync)
            .checksum_mode(ChecksumMode::Crc32c)
            .create()
            .unwrap();
        recorder
            .append_log_record(LogRecordInput {
                sequence: 1,
                event_time_ns: 11,
                user_header: &[0x44, 0x55],
                payload: &[0x66; 12],
            })
            .unwrap();
    }

    let mut recovered = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Sync)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();
    let recovery_status = recovered.recovery_status();
    assert_that!(recovery_status.recovered_existing_archive, eq true);
    assert_that!(recovery_status.commit_entries_loaded, eq 1);

    recovered
        .append_log_record(LogRecordInput {
            sequence: 2,
            event_time_ns: 22,
            user_header: &[0x77, 0x88],
            payload: &[0x99; 12],
        })
        .unwrap();
    recovered.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(4).unwrap())
        .unwrap();
    assert_that!(replayed.len(), eq 2);
    assert_that!(replayed[0].sequence, eq 1);
    assert_that!(replayed[1].sequence, eq 2);
}

#[test]
fn log_archive_open_or_recover_truncates_active_segment_corrupted_tail() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let segment_path = {
        let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
            .metadata_log_path(&metadata_path)
            .segment_bytes(2048)
            .segment_preallocate(false)
            .spare_preallocated_segments(0)
            .persistence_mode(PersistenceMode::Async)
            .checksum_mode(ChecksumMode::Crc32c)
            .create()
            .unwrap();
        let commit = recorder
            .append_log_record(LogRecordInput {
                sequence: 1,
                event_time_ns: 10,
                user_header: &[0x10],
                payload: &[0x22; 8],
            })
            .unwrap();
        recorder.flush().unwrap();
        storage_path.join(format!(
            "segments/segment-{}-g{}.data",
            commit.locator.segment_id, commit.locator.segment_generation
        ))
    };

    let mut segment_file = OpenOptions::new().append(true).open(&segment_path).unwrap();
    segment_file
        .write_all(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x01, 0x02])
        .unwrap();
    segment_file.flush().unwrap();

    let mut recovered = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();
    let recovery_status = recovered.recovery_status();
    assert_that!(recovery_status.segment_truncated_bytes > 0, eq true);

    recovered
        .append_log_record(LogRecordInput {
            sequence: 2,
            event_time_ns: 20,
            user_header: &[0x11],
            payload: &[0x33; 8],
        })
        .unwrap();
    recovered.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(4).unwrap())
        .unwrap();
    assert_that!(replayed.len(), eq 2);
    assert_that!(replayed[0].sequence, eq 1);
    assert_that!(replayed[1].sequence, eq 2);
}

#[test]
fn log_archive_open_or_recover_truncates_partial_commit_log_tail() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    {
        let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
            .metadata_log_path(&metadata_path)
            .segment_bytes(2048)
            .segment_preallocate(false)
            .spare_preallocated_segments(0)
            .persistence_mode(PersistenceMode::Async)
            .checksum_mode(ChecksumMode::Crc32c)
            .create()
            .unwrap();
        recorder
            .append_log_record(LogRecordInput {
                sequence: 1,
                event_time_ns: 5,
                user_header: &[0x01],
                payload: &[0x02; 8],
            })
            .unwrap();
        recorder.flush().unwrap();
    }

    let commit_log_path = metadata_path.join("commit.idxlog");
    let mut commit_log_file = OpenOptions::new()
        .append(true)
        .open(&commit_log_path)
        .unwrap();
    commit_log_file
        .write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0xFA])
        .unwrap();
    commit_log_file.flush().unwrap();

    let mut recovered = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(2048)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();
    let recovery_status = recovered.recovery_status();
    assert_that!(recovery_status.commit_log_truncated_bytes > 0, eq true);
    assert_that!(recovery_status.commit_entries_loaded, eq 1);

    recovered
        .append_log_record(LogRecordInput {
            sequence: 2,
            event_time_ns: 6,
            user_header: &[0x03],
            payload: &[0x04; 8],
        })
        .unwrap();
    recovered.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(4).unwrap())
        .unwrap();
    assert_that!(replayed.len(), eq 2);
    assert_that!(replayed[0].sequence, eq 1);
    assert_that!(replayed[1].sequence, eq 2);
}

#[test]
fn log_archive_open_or_recover_loads_catalog_from_rolled_segments() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    {
        let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
            .metadata_log_path(&metadata_path)
            .segment_bytes(256)
            .segment_preallocate(false)
            .spare_preallocated_segments(0)
            .persistence_mode(PersistenceMode::Async)
            .checksum_mode(ChecksumMode::Crc32c)
            .create()
            .unwrap();
        for sequence in 1..=12u64 {
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence * 2,
                    user_header: &[0x21, 0x22],
                    payload: &[sequence as u8; 16],
                })
                .unwrap();
        }
        recorder.flush().unwrap();
    }

    let recovered = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(256)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();
    let recovery_status = recovered.recovery_status();
    assert_that!(recovery_status.catalog_segments_loaded > 0, eq true);
    assert_that!(recovery_status.recovered_existing_archive, eq true);
}

#[test]
fn log_archive_volatile_mode_avoids_disk_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("volatile_archive");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .persistence_mode(PersistenceMode::Volatile)
        .create()
        .unwrap();

    recorder
        .append_log_record(LogRecordInput {
            sequence: 1,
            event_time_ns: 0,
            user_header: &[1],
            payload: &[2, 3, 4],
        })
        .unwrap();
    recorder.finalize().unwrap();

    assert_that!(storage_path.exists(), eq false);

    let replay_open = ArchiveReplayerBuilder::new(&storage_path).open();
    assert_that!(
        matches!(replay_open, Err(ArchiveReplayError::MissingCommitLog(_))),
        eq true
    );
}

fn write_rolled_archive(
    storage_path: &std::path::Path,
    metadata_path: &std::path::Path,
    max_disk_bytes: Option<u64>,
    records: u64,
) -> iceoryx2_userland_log_archive::log_archive::ArchiveRecorder {
    let mut builder = ArchiveRecorderBuilder::new(storage_path)
        .metadata_log_path(metadata_path)
        .segment_bytes(256)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c);
    if let Some(value) = max_disk_bytes {
        builder = builder.max_disk_bytes(value);
    }

    let mut recorder = builder.create().unwrap();
    for sequence in 1..=records {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 100,
                user_header: &[0xA1, 0xB2],
                payload: &[sequence as u8; 32],
            })
            .unwrap();
    }
    recorder.finalize().unwrap();
    recorder
}

#[test]
fn log_archive_retention_arbiter_is_deterministic_for_equivalent_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let cap = 560u64;

    let recorder_a = write_rolled_archive(
        &temp.path().join("archive_a"),
        &temp.path().join("metadata_a"),
        Some(cap),
        12,
    );
    let recorder_b = write_rolled_archive(
        &temp.path().join("archive_b"),
        &temp.path().join("metadata_b"),
        Some(cap),
        12,
    );

    let status_a = recorder_a.retention_status().unwrap();
    let status_b = recorder_b.retention_status().unwrap();
    assert_that!(status_a.retained_bytes_total <= cap, eq true);
    assert_that!(status_b.retained_bytes_total <= cap, eq true);

    let remaining_a: Vec<(u64, u32)> = recorder_a
        .list_segments()
        .unwrap()
        .into_iter()
        .map(|segment| (segment.segment_id, segment.segment_generation))
        .collect();
    let remaining_b: Vec<(u64, u32)> = recorder_b
        .list_segments()
        .unwrap()
        .into_iter()
        .map(|segment| (segment.segment_id, segment.segment_generation))
        .collect();

    assert_that!(remaining_a.is_empty(), eq false);
    assert_that!(remaining_a, eq remaining_b);
}

#[test]
fn log_archive_detach_attach_delete_lifecycle_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = write_rolled_archive(&storage_path, &metadata_path, None, 6);
    let initial_segments = recorder.list_segments().unwrap();
    assert_that!(initial_segments.is_empty(), eq false);

    let detached_once = recorder.detach_before_sequence(u64::MAX).unwrap();
    let detached_twice = recorder.detach_before_sequence(u64::MAX).unwrap();
    assert_that!(detached_once, eq initial_segments.len() as u64);
    assert_that!(detached_twice, eq 0);

    let status_detached = recorder.retention_status().unwrap();
    assert_that!(status_detached.segments_hot_attached, eq 0);
    assert_that!(
        status_detached.segments_cold_detached,
        eq initial_segments.len()
    );

    let attached_once = recorder.attach_all_detached().unwrap();
    let attached_twice = recorder.attach_all_detached().unwrap();
    assert_that!(attached_once, eq initial_segments.len() as u64);
    assert_that!(attached_twice, eq 0);

    let _ = recorder.detach_before_sequence(u64::MAX).unwrap();
    let deleted_once = recorder.delete_detached_before_sequence(u64::MAX).unwrap();
    let deleted_twice = recorder.delete_detached_before_sequence(u64::MAX).unwrap();
    assert_that!(deleted_once, eq initial_segments.len() as u64);
    assert_that!(deleted_twice, eq 0);

    let status_deleted = recorder.retention_status().unwrap();
    assert_that!(status_deleted.segments_hot_attached, eq 0);
    assert_that!(status_deleted.segments_cold_detached, eq 0);
    assert_that!(status_deleted.retained_bytes_total, eq 0);
}

#[test]
fn log_archive_trim_respects_replay_snapshot_pins() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = write_rolled_archive(&storage_path, &metadata_path, None, 5);
    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let snapshot = replayer.begin_snapshot().unwrap();

    let pinned_before_trim = recorder
        .list_segments()
        .unwrap()
        .iter()
        .all(|segment| segment.pinned);
    assert_that!(pinned_before_trim, eq true);

    let trimmed_with_pin = recorder.trim_before_sequence(u64::MAX).unwrap();
    assert_that!(trimmed_with_pin, eq 0);
    assert_that!(recorder.list_segments().unwrap().is_empty(), eq false);

    replayer.release_snapshot(snapshot).unwrap();
    let trimmed_after_release = recorder.trim_before_sequence(u64::MAX).unwrap();
    assert_that!(trimmed_after_release > 0, eq true);
    assert_that!(recorder.list_segments().unwrap().is_empty(), eq true);
}

#[test]
fn log_archive_replayer_returns_none_for_trimmed_sequences() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = write_rolled_archive(&storage_path, &metadata_path, None, 4);
    let trimmed = recorder.trim_before_sequence(u64::MAX).unwrap();
    assert_that!(trimmed > 0, eq true);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();
    let replayed = replayer.read_at_sequence(1).unwrap();
    assert_that!(replayed.is_none(), eq true);
}

#[test]
fn log_archive_capacity_enforcement_holds_under_sustained_ingest() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let cap = 700u64;

    let recorder = write_rolled_archive(&storage_path, &metadata_path, Some(cap), 100);
    let status = recorder.retention_status().unwrap();
    assert_that!(status.retained_bytes_total <= cap, eq true);
    assert_that!(status.retained_bytes_hot_attached + status.retained_bytes_cold_detached, eq status.retained_bytes_total);

    let remaining = recorder.list_segments().unwrap();
    assert_that!(remaining.is_empty(), eq false);
    let oldest_retained = remaining
        .iter()
        .map(|segment| segment.sequence_start)
        .min()
        .unwrap();
    assert_that!(oldest_retained > 1, eq true);
    let all_hot = remaining
        .iter()
        .all(|segment| segment.tier == ArchiveSegmentTier::HotAttached);
    assert_that!(all_hot, eq true);
}
