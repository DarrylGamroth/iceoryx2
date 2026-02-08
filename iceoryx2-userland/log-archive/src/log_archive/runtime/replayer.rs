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

use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::min;
use core::num::NonZeroUsize;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::common::*;
use super::storage::read_commit_entries;

/// Builder for [`ArchiveReplayer`].
pub struct ArchiveReplayerBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    replay_budget: ReplayBudget,
    verify_checksums: bool,
}

impl ArchiveReplayerBuilder {
    /// Creates a replayer builder.
    pub fn new(storage_path: &Path) -> Self {
        Self {
            storage_path: storage_path.to_path_buf(),
            metadata_log_path: None,
            replay_budget: ReplayBudget::default(),
            verify_checksums: true,
        }
    }

    /// Overrides metadata-log root path.
    pub fn metadata_log_path(mut self, value: &Path) -> Self {
        self.metadata_log_path = Some(value.to_path_buf());
        self
    }

    /// Sets replay budget limits.
    pub fn replay_budget(mut self, value: ReplayBudget) -> Self {
        self.replay_budget = value;
        self
    }

    /// Enables/disables checksum verification.
    pub fn verify_checksums(mut self, value: bool) -> Self {
        self.verify_checksums = value;
        self
    }

    /// Opens archive replayer.
    pub fn open(self) -> Result<ArchiveReplayer, ArchiveReplayError> {
        let metadata_root = self
            .metadata_log_path
            .clone()
            .unwrap_or_else(|| self.storage_path.clone());
        let commit_log_path = metadata_root.join("commit.idxlog");
        if !commit_log_path.exists() {
            return Err(ArchiveReplayError::MissingCommitLog(commit_log_path));
        }

        let entries = read_commit_entries(&commit_log_path)?;
        let mut index_by_sequence = BTreeMap::new();
        for entry in entries {
            if index_by_sequence.insert(entry.sequence, entry).is_some() {
                return Err(ArchiveReplayError::DuplicateSequence(entry.sequence));
            }
        }
        let ordered_sequences: Vec<u64> = index_by_sequence.keys().copied().collect();

        Ok(ArchiveReplayer {
            segments_path: self.storage_path.join("segments"),
            index_by_sequence,
            ordered_sequences,
            cursor: 0,
            replay_budget: self.replay_budget,
            verify_checksums: self.verify_checksums,
        })
    }
}

/// Replayer core for archived segment data.
#[derive(Debug)]
pub struct ArchiveReplayer {
    segments_path: PathBuf,
    index_by_sequence: BTreeMap<u64, CommitEntry>,
    ordered_sequences: Vec<u64>,
    cursor: usize,
    replay_budget: ReplayBudget,
    verify_checksums: bool,
}

impl ArchiveReplayer {
    /// Returns current replay budget.
    pub fn replay_budget(&self) -> ReplayBudget {
        self.replay_budget
    }

    /// Sets replay budget.
    pub fn set_replay_budget(&mut self, value: ReplayBudget) {
        self.replay_budget = value;
    }

    /// Reads a record by source sequence.
    pub fn read_at_sequence(
        &self,
        sequence: u64,
    ) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        let Some(entry) = self.index_by_sequence.get(&sequence) else {
            return Ok(None);
        };
        self.read_frame_from_entry(entry)
    }

    /// Reads multiple records starting from `start_sequence`.
    pub fn read_range(
        &self,
        start_sequence: u64,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let max_records = min(max_records.get(), self.replay_budget.max_records_per_call);
        let mut records = Vec::with_capacity(max_records);
        let mut accumulated_bytes = 0usize;
        for (_sequence, entry) in self.index_by_sequence.range(start_sequence..) {
            if records.len() >= max_records {
                break;
            }
            if accumulated_bytes + entry.locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !records.is_empty()
            {
                break;
            }
            let frame = self.read_frame_from_entry(entry)?.ok_or(
                ArchiveReplayError::InvalidCommitEntry("commit entry sequence missing in segment"),
            )?;
            accumulated_bytes += frame.locator.frame_len as usize;
            records.push(frame);
        }

        Ok(records)
    }

    /// Positions cursor to the first sequence `>= sequence`.
    pub fn seek(&mut self, sequence: u64) {
        self.cursor = lower_bound(&self.ordered_sequences, sequence);
    }

    /// Reads next record from cursor and advances it.
    pub fn next(&mut self) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        if self.cursor >= self.ordered_sequences.len() {
            return Ok(None);
        }

        let sequence = self.ordered_sequences[self.cursor];
        self.cursor += 1;
        self.read_at_sequence(sequence)
    }

    /// Reads next batch with replay budget limits.
    pub fn next_batch(
        &mut self,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let max_records = min(max_records.get(), self.replay_budget.max_records_per_call);
        let mut records = Vec::with_capacity(max_records);
        let mut accumulated_bytes = 0usize;

        while self.cursor < self.ordered_sequences.len() && records.len() < max_records {
            let sequence = self.ordered_sequences[self.cursor];
            let entry = self.index_by_sequence.get(&sequence).ok_or(
                ArchiveReplayError::InvalidCommitEntry("cursor points to missing sequence"),
            )?;
            if accumulated_bytes + entry.locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !records.is_empty()
            {
                break;
            }
            let frame = self.read_frame_from_entry(entry)?.ok_or(
                ArchiveReplayError::InvalidCommitEntry("commit entry sequence missing in segment"),
            )?;
            accumulated_bytes += frame.locator.frame_len as usize;
            records.push(frame);
            self.cursor += 1;
        }

        Ok(records)
    }

    /// Reads one frame via physical locator.
    pub fn read_at_locator(
        &self,
        locator: ArchiveLocator,
    ) -> Result<ReplayedFrame, ArchiveReplayError> {
        self.read_frame(locator)
    }

    /// Reads multiple frames via locators preserving caller-provided order.
    pub fn read_many_locators(
        &self,
        locators: &[ArchiveLocator],
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let limit = min(locators.len(), self.replay_budget.max_records_per_call);
        let mut result = Vec::with_capacity(limit);
        let mut accumulated_bytes = 0usize;

        for locator in locators.iter().take(limit) {
            if accumulated_bytes + locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !result.is_empty()
            {
                break;
            }
            let frame = self.read_frame(*locator)?;
            accumulated_bytes += frame.locator.frame_len as usize;
            result.push(frame);
        }

        Ok(result)
    }

    fn read_frame_from_entry(
        &self,
        entry: &CommitEntry,
    ) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        let frame = self.read_frame(entry.locator)?;
        if self.verify_checksums
            && entry.frame_checksum != 0
            && frame_crc_from_payload(&frame, self.verify_checksums)? != entry.frame_checksum
        {
            return Err(ArchiveReplayError::ChecksumMismatch {
                expected: entry.frame_checksum,
                actual: frame_crc_from_payload(&frame, self.verify_checksums)?,
                locator: frame.locator,
            });
        }
        Ok(Some(frame))
    }

    fn read_frame(&self, locator: ArchiveLocator) -> Result<ReplayedFrame, ArchiveReplayError> {
        let segment_path = segment_data_path(
            &self.segments_path,
            locator.segment_id,
            locator.segment_generation,
        );
        if !segment_path.exists() {
            return Err(ArchiveReplayError::MissingSegment(segment_path));
        }

        let mut file = File::open(&segment_path).map_err(|source| ArchiveReplayError::Io {
            operation: "open segment data",
            path: segment_path.clone(),
            source,
        })?;
        file.seek(SeekFrom::Start(locator.file_offset))
            .map_err(|source| ArchiveReplayError::Io {
                operation: "seek segment frame",
                path: segment_path.clone(),
                source,
            })?;

        let mut frame_header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut frame_header)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read frame header",
                path: segment_path.clone(),
                source,
            })?;

        let decoded_magic = [
            frame_header[FRAME_OFFSET_MAGIC],
            frame_header[FRAME_OFFSET_MAGIC + 1],
            frame_header[FRAME_OFFSET_MAGIC + 2],
            frame_header[FRAME_OFFSET_MAGIC + 3],
        ];
        if decoded_magic != FRAME_MAGIC {
            return Err(ArchiveReplayError::InvalidFrameMagic(decoded_magic));
        }

        let header_len = read_u16(&frame_header, FRAME_OFFSET_HEADER_LEN);
        if header_len as usize != FRAME_HEADER_LEN {
            return Err(ArchiveReplayError::InvalidFrameHeaderLength(header_len));
        }
        let flags = read_u16(&frame_header, FRAME_OFFSET_FLAGS);
        let frame_len = read_u32(&frame_header, FRAME_OFFSET_FRAME_LEN);
        if frame_len != locator.frame_len {
            return Err(ArchiveReplayError::InvalidFrameLength {
                expected: locator.frame_len,
                decoded: frame_len,
            });
        }

        let variable_len = frame_len as usize - FRAME_HEADER_LEN;
        let mut variable = vec![0u8; variable_len];
        file.read_exact(&mut variable)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read frame payload",
                path: segment_path.clone(),
                source,
            })?;

        let user_header_len = read_u32(&frame_header, FRAME_OFFSET_USER_HEADER_LEN) as usize;
        let payload_len = read_u32(&frame_header, FRAME_OFFSET_PAYLOAD_LEN) as usize;
        if user_header_len + payload_len > variable_len {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "frame user/payload lengths exceed frame bounds",
            ));
        }

        if self.verify_checksums && (flags & FRAME_FLAG_CHECKSUM_CRC32C) != 0 {
            let expected = read_u32(&frame_header, FRAME_OFFSET_CHECKSUM);
            let mut checksum_frame = vec![0u8; frame_len as usize];
            checksum_frame[..FRAME_HEADER_LEN].copy_from_slice(&frame_header);
            checksum_frame[FRAME_OFFSET_CHECKSUM..FRAME_OFFSET_CHECKSUM + 4].fill(0);
            checksum_frame[FRAME_HEADER_LEN..].copy_from_slice(&variable);
            let actual = crc32c::crc32c(&checksum_frame);
            if expected != actual {
                return Err(ArchiveReplayError::ChecksumMismatch {
                    expected,
                    actual,
                    locator,
                });
            }
        }

        let user_header = variable[..user_header_len].to_vec();
        let payload = variable[user_header_len..user_header_len + payload_len].to_vec();

        Ok(ReplayedFrame {
            commit_ordinal: read_u64(&frame_header, FRAME_OFFSET_COMMIT_ORDINAL),
            sequence: read_u64(&frame_header, FRAME_OFFSET_SEQUENCE),
            event_time_ns: read_u64(&frame_header, FRAME_OFFSET_EVENT_TIME_NS),
            commit_time_ns: read_u64(&frame_header, FRAME_OFFSET_COMMIT_TIME_NS),
            user_header,
            payload,
            locator,
        })
    }
}
