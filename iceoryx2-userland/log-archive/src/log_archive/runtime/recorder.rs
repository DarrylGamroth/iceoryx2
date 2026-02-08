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
use alloc::vec::Vec;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::log_archive::{ArchiveFileKind, ARCHIVE_FILE_HEADER_V1_LEN};

use super::common::*;
use super::storage::*;

/// Builder for [`ArchiveRecorder`].
pub struct ArchiveRecorderBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    segment_bytes: usize,
    segment_preallocate: bool,
    spare_preallocated_segments: usize,
    metadata_log_preallocate_entries: usize,
    persistence_mode: PersistenceMode,
    checksum_mode: ChecksumMode,
    out_of_space_policy: OutOfSpacePolicy,
    log_id: [u8; 16],
    segment_generation: u32,
}

impl ArchiveRecorderBuilder {
    /// Creates a builder with throughput-oriented defaults.
    pub fn new(storage_path: &Path) -> Self {
        Self {
            storage_path: storage_path.to_path_buf(),
            metadata_log_path: None,
            segment_bytes: 256 * 1024 * 1024,
            segment_preallocate: true,
            spare_preallocated_segments: 1,
            metadata_log_preallocate_entries: DEFAULT_METADATA_LOG_PREALLOCATE_ENTRIES,
            persistence_mode: PersistenceMode::Async,
            checksum_mode: ChecksumMode::Crc32c,
            out_of_space_policy: OutOfSpacePolicy::FailWriter,
            log_id: [0u8; 16],
            segment_generation: 0,
        }
    }

    /// Overrides metadata-log root path.
    pub fn metadata_log_path(mut self, value: &Path) -> Self {
        self.metadata_log_path = Some(value.to_path_buf());
        self
    }

    /// Configures segment byte size.
    pub fn segment_bytes(mut self, value: usize) -> Self {
        self.segment_bytes = value;
        self
    }

    /// Enables/disables segment preallocation.
    pub fn segment_preallocate(mut self, value: bool) -> Self {
        self.segment_preallocate = value;
        self
    }

    /// Configures number of spare preallocated segments.
    pub fn spare_preallocated_segments(mut self, value: usize) -> Self {
        self.spare_preallocated_segments = value;
        self
    }

    /// Configures number of commit-log entries reserved in each metadata-log preallocation chunk.
    pub fn metadata_log_preallocate_entries(mut self, value: usize) -> Self {
        self.metadata_log_preallocate_entries = value;
        self
    }

    /// Configures durability mode.
    pub fn persistence_mode(mut self, value: PersistenceMode) -> Self {
        self.persistence_mode = value;
        self
    }

    /// Configures frame checksum mode.
    pub fn checksum_mode(mut self, value: ChecksumMode) -> Self {
        self.checksum_mode = value;
        self
    }

    /// Configures out-of-space policy.
    pub fn out_of_space_policy(mut self, value: OutOfSpacePolicy) -> Self {
        self.out_of_space_policy = value;
        self
    }

    /// Configures archive log id embedded into file headers.
    pub fn log_id(mut self, value: [u8; 16]) -> Self {
        self.log_id = value;
        self
    }

    /// Configures segment generation value.
    pub fn segment_generation(mut self, value: u32) -> Self {
        self.segment_generation = value;
        self
    }

    /// Creates a new recorder and fails when archive paths already exist.
    pub fn create(self) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        self.create_internal(false)
    }

    /// Opens an existing recorder archive and runs startup recovery, or creates a new archive.
    pub fn open_or_recover(self) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        self.create_internal(true)
    }

    fn create_internal(
        self,
        recover_existing: bool,
    ) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        let mut config = self.build_config()?;
        if config.persistence_mode == PersistenceMode::Volatile {
            return Ok(new_volatile_recorder(config));
        }

        let archive_exists = config.storage_path.join("catalog.bin").exists()
            || config.storage_path.join("segments").exists();
        if archive_exists {
            if !recover_existing {
                return Err(ArchiveRecorderError::ArchiveAlreadyExists(
                    config.storage_path.clone(),
                ));
            }
            return recover_existing_archive(&mut config);
        }

        create_new_archive(config)
    }

    fn build_config(&self) -> Result<RecorderConfig, ArchiveRecorderError> {
        let minimal_frame_bytes = ARCHIVE_FILE_HEADER_V1_LEN + FRAME_HEADER_LEN + 8;
        if self.segment_bytes <= minimal_frame_bytes {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "segment_bytes is too small to store any frame",
            ));
        }
        if self.metadata_log_preallocate_entries == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "metadata_log_preallocate_entries must be > 0",
            ));
        }

        Ok(RecorderConfig {
            storage_path: self.storage_path.clone(),
            metadata_log_path: self
                .metadata_log_path
                .clone()
                .unwrap_or_else(|| self.storage_path.clone()),
            segment_bytes: self.segment_bytes,
            segment_preallocate: self.segment_preallocate,
            spare_preallocated_segments: self.spare_preallocated_segments,
            metadata_log_preallocate_entries: self.metadata_log_preallocate_entries,
            persistence_mode: self.persistence_mode,
            checksum_mode: self.checksum_mode,
            out_of_space_policy: self.out_of_space_policy,
            log_id: self.log_id,
            segment_generation: self.segment_generation,
        })
    }
}

fn new_volatile_recorder(config: RecorderConfig) -> ArchiveRecorder {
    ArchiveRecorder {
        config,
        disk: None,
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal: 1,
        last_sequence: None,
        index_by_sequence: BTreeMap::new(),
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    }
}

fn create_new_archive(config: RecorderConfig) -> Result<ArchiveRecorder, ArchiveRecorderError> {
    fs::create_dir_all(&config.storage_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create storage directory",
        path: config.storage_path.clone(),
        source,
    })?;
    let segments_path = config.storage_path.join("segments");
    fs::create_dir_all(&segments_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create segments directory",
        path: segments_path.clone(),
        source,
    })?;
    fs::create_dir_all(&config.metadata_log_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create metadata directory",
        path: config.metadata_log_path.clone(),
        source,
    })?;

    let (mut catalog_file, catalog_path) =
        create_new_file(&config.storage_path.join("catalog.bin"))?;
    write_archive_header(
        &mut catalog_file,
        &catalog_path,
        ArchiveFileKind::Catalog,
        config.log_id,
        0,
        0,
    )?;

    let commit_log_path = config.metadata_log_path.join("commit.idxlog");
    let (mut commit_log_file, commit_log_path) = create_new_file(&commit_log_path)?;
    write_archive_header(
        &mut commit_log_file,
        &commit_log_path,
        ArchiveFileKind::CommitIdxLog,
        config.log_id,
        0,
        0,
    )?;
    let commit_log_write_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    let commit_log_preallocated_len = preallocate_metadata_log(
        &mut commit_log_file,
        &commit_log_path,
        commit_log_write_offset,
        config.metadata_log_preallocate_entries,
    )?;

    let mut recorder = ArchiveRecorder {
        config,
        disk: Some(DiskRecorderState {
            segments_path,
            catalog_path,
            commit_log_path,
            catalog_file,
            commit_log_file,
            commit_log_write_offset,
            commit_log_preallocated_len,
            active_segment: None,
        }),
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal: 1,
        last_sequence: None,
        index_by_sequence: BTreeMap::new(),
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    };

    recorder.open_new_active_segment(1)?;
    Ok(recorder)
}

fn recover_existing_archive(
    config: &mut RecorderConfig,
) -> Result<ArchiveRecorder, ArchiveRecorderError> {
    let recovery_start = Instant::now();
    let segments_path = config.storage_path.join("segments");
    let catalog_path = config.storage_path.join("catalog.bin");
    let commit_log_path = config.metadata_log_path.join("commit.idxlog");

    if !catalog_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(catalog_path));
    }
    if !segments_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(segments_path));
    }
    if !commit_log_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(
            commit_log_path,
        ));
    }

    let catalog_summaries = read_catalog_entries(&catalog_path)?;
    let data_segments = list_data_segments(&segments_path)?;

    let mut commit_log_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&commit_log_path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "open commit idxlog for recovery",
            path: commit_log_path.clone(),
            source,
        })?;
    let commit_recovery =
        recover_commit_log_entries(&mut commit_log_file, &commit_log_path, &segments_path)?;
    let commit_log_write_offset = commit_recovery.logical_end_offset;
    let commit_log_preallocated_len = preallocate_metadata_log(
        &mut commit_log_file,
        &commit_log_path,
        commit_log_write_offset,
        config.metadata_log_preallocate_entries,
    )?;

    let mut index_by_sequence = BTreeMap::new();
    let mut next_commit_ordinal = 1u64;
    let mut last_sequence = None;
    for entry in &commit_recovery.entries {
        if index_by_sequence
            .insert(entry.sequence, entry.locator)
            .is_some()
        {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "commit.idxlog contains duplicate sequence",
            ));
        }
        if let Some(previous) = last_sequence {
            if entry.sequence <= previous {
                return Err(ArchiveRecorderError::RecoveryInconsistent(
                    "commit.idxlog sequence is not strictly monotonic",
                ));
            }
        }
        last_sequence = Some(entry.sequence);
        next_commit_ordinal = entry.commit_ordinal.saturating_add(1);
    }

    let (active_segment_id, active_segment_generation) = determine_active_segment_for_recovery(
        &data_segments,
        &catalog_summaries,
        &commit_recovery.entries,
        config.segment_generation,
        &segments_path,
    );
    config.segment_generation = active_segment_generation;

    let mut active_committed_records = 0u64;
    let mut active_sequence_start = None;
    let mut active_sequence_end = None;
    let mut committed_active_write_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    for entry in &commit_recovery.entries {
        if entry.locator.segment_id == active_segment_id
            && entry.locator.segment_generation == active_segment_generation
        {
            active_committed_records += 1;
            active_sequence_start.get_or_insert(entry.sequence);
            active_sequence_end = Some(entry.sequence);
            let end_offset = entry.locator.file_offset + entry.locator.frame_len as u64;
            if end_offset > committed_active_write_offset {
                committed_active_write_offset = end_offset;
            }
        }
    }

    let catalog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&catalog_path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "open catalog for recovery",
            path: catalog_path.clone(),
            source,
        })?;

    let mut recorder = ArchiveRecorder {
        config: config.clone(),
        disk: Some(DiskRecorderState {
            segments_path,
            catalog_path,
            commit_log_path,
            catalog_file,
            commit_log_file,
            commit_log_write_offset,
            commit_log_preallocated_len,
            active_segment: None,
        }),
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal,
        last_sequence,
        index_by_sequence,
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    };

    recorder.open_new_active_segment(active_segment_id)?;
    let segment_recovery = recorder.recover_active_segment_tail(
        committed_active_write_offset,
        active_committed_records,
        active_sequence_start,
        active_sequence_end,
    )?;

    recorder.recovery_status = ArchiveRecoveryStatus {
        recovered_existing_archive: true,
        catalog_segments_loaded: catalog_summaries.len() as u64,
        commit_entries_loaded: commit_recovery.entries.len() as u64,
        active_segment_id,
        active_segment_generation,
        active_segment_records: active_committed_records,
        segment_truncation_events: if segment_recovery.truncated_bytes > 0 {
            1
        } else {
            0
        },
        segment_truncated_bytes: segment_recovery.truncated_bytes,
        commit_log_truncated_bytes: commit_recovery.truncated_bytes,
        recovery_duration_ns: recovery_start.elapsed().as_nanos() as u64,
    };

    Ok(recorder)
}
impl ArchiveRecorder {
    /// Returns current recorder stats.
    pub fn stats(&self) -> ArchiveRecorderStats {
        self.stats
    }

    /// Returns startup recovery status.
    pub fn recovery_status(&self) -> ArchiveRecoveryStatus {
        self.recovery_status
    }

    /// Returns true when recorder entered degraded state.
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Returns effective persistence mode.
    pub fn persistence_mode(&self) -> PersistenceMode {
        self.config.persistence_mode
    }

    /// Returns effective segment size in bytes.
    pub fn segment_bytes(&self) -> usize {
        self.config.segment_bytes
    }

    /// Appends one log record.
    pub fn append_log_record(
        &mut self,
        input: LogRecordInput<'_>,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        if self.finalized {
            return Err(ArchiveRecorderError::Finalized);
        }
        if self.degraded {
            return Err(ArchiveRecorderError::Degraded);
        }
        if let Some(previous) = self.last_sequence {
            if input.sequence <= previous {
                return Err(ArchiveRecorderError::SequenceNotMonotonic {
                    previous,
                    next: input.sequence,
                });
            }
        }

        let commit_ordinal = self.next_commit_ordinal;
        self.next_commit_ordinal += 1;
        self.last_sequence = Some(input.sequence);

        match self.config.persistence_mode {
            PersistenceMode::Volatile => self.append_volatile(input, commit_ordinal),
            PersistenceMode::Async | PersistenceMode::Sync => {
                self.append_disk(input, commit_ordinal)
            }
        }
    }

    /// Flushes recorder output streams.
    pub fn flush(&mut self) -> Result<(), ArchiveRecorderError> {
        if let Some(disk) = self.disk.as_mut() {
            if let Some(active) = disk.active_segment.as_mut() {
                active
                    .file
                    .flush()
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation: "flush active segment",
                        path: segment_data_path(
                            &disk.segments_path,
                            active.segment_id,
                            active.segment_generation,
                        ),
                        source,
                    })?;
            }

            disk.commit_log_file
                .flush()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "flush commit idxlog",
                    path: disk.commit_log_path.clone(),
                    source,
                })?;
            disk.catalog_file
                .flush()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "flush catalog",
                    path: disk.catalog_path.clone(),
                    source,
                })?;
        }

        Ok(())
    }

    /// Finalizes recorder output by sealing the active segment.
    pub fn finalize(&mut self) -> Result<(), ArchiveRecorderError> {
        if self.finalized {
            return Ok(());
        }

        if self.disk.is_some() {
            self.seal_active_segment_internal(false)?;
            self.truncate_commit_log_to_logical_size()?;
            self.flush()?;
        }

        self.finalized = true;
        Ok(())
    }

    fn append_volatile(
        &mut self,
        input: LogRecordInput<'_>,
        commit_ordinal: u64,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let commit_time_ns = now_ns();
        let frame_len = align_up(
            FRAME_HEADER_LEN + input.user_header.len() + input.payload.len(),
            8,
        );
        let locator = ArchiveLocator {
            segment_id: 0,
            segment_generation: 0,
            file_offset: self.volatile_records.len() as u64,
            frame_len: frame_len as u32,
        };
        self.index_by_sequence.insert(input.sequence, locator);
        self.volatile_records.push(VolatileFrame {
            commit_ordinal,
            sequence: input.sequence,
            event_time_ns: input.event_time_ns,
            commit_time_ns,
            user_header: input.user_header.to_vec(),
            payload: input.payload.to_vec(),
            locator,
        });
        self.stats.committed_records += 1;
        self.stats.payload_bytes_committed += input.payload.len() as u64;

        Ok(RecordedCommit {
            commit_ordinal,
            sequence: input.sequence,
            locator,
        })
    }

    fn append_disk(
        &mut self,
        input: LogRecordInput<'_>,
        commit_ordinal: u64,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let frame = EncodedFrame::new(
            commit_ordinal,
            input.sequence,
            input.event_time_ns,
            now_ns(),
            input.user_header,
            input.payload,
            self.config.checksum_mode,
        );

        let max_frame_bytes = self.config.segment_bytes - ARCHIVE_FILE_HEADER_V1_LEN;
        if frame.bytes.len() > max_frame_bytes {
            return Err(ArchiveRecorderError::FrameTooLarge {
                required: frame.bytes.len(),
                segment_bytes: self.config.segment_bytes,
            });
        }

        let disk = self.disk.as_mut().expect("disk recorder state must exist");
        let active = disk
            .active_segment
            .as_mut()
            .expect("active segment must exist");

        if (active.write_offset as usize + frame.bytes.len()) > self.config.segment_bytes {
            self.seal_active_segment_internal(true)?;
        }

        let (locator, segment_path, write_failure) = {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            let active = disk
                .active_segment
                .as_mut()
                .expect("active segment must exist");
            let locator = ArchiveLocator {
                segment_id: active.segment_id,
                segment_generation: active.segment_generation,
                file_offset: active.write_offset,
                frame_len: frame.bytes.len() as u32,
            };
            let segment_path = segment_data_path(
                &disk.segments_path,
                active.segment_id,
                active.segment_generation,
            );

            active
                .file
                .seek(SeekFrom::Start(locator.file_offset))
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "seek active segment",
                    path: segment_path.clone(),
                    source,
                })?;
            let write_failure = active.file.write_all(&frame.bytes).err();
            if write_failure.is_none() {
                active.write_offset += frame.bytes.len() as u64;
                active.sequence_start.get_or_insert(input.sequence);
                active.sequence_end = Some(input.sequence);
                active.records += 1;
            }

            (locator, segment_path, write_failure)
        };

        if let Some(source) = write_failure {
            return self.handle_write_failure(&segment_path, source);
        }

        self.ensure_commit_log_capacity()?;
        let commit_log_path = self
            .disk
            .as_ref()
            .expect("disk recorder state must exist")
            .commit_log_path
            .clone();
        let write_offset = self
            .disk
            .as_ref()
            .expect("disk recorder state must exist")
            .commit_log_write_offset;
        let write_result = {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            write_commit_entry(
                &mut disk.commit_log_file,
                &commit_log_path,
                write_offset,
                CommitEntry {
                    commit_ordinal,
                    sequence: input.sequence,
                    locator,
                    frame_checksum: frame.checksum,
                },
            )
        };
        if let Err(source) = write_result {
            return Err(self.handle_commit_write_failure(source));
        }
        self.disk
            .as_mut()
            .expect("disk recorder state must exist")
            .commit_log_write_offset += COMMIT_ENTRY_LEN as u64;

        self.stats.committed_records += 1;
        self.stats.payload_bytes_committed += input.payload.len() as u64;
        self.stats.data_bytes_written += frame.bytes.len() as u64;
        self.stats.metadata_bytes_written += COMMIT_ENTRY_LEN as u64;
        self.index_by_sequence.insert(input.sequence, locator);

        if self.config.persistence_mode == PersistenceMode::Sync {
            self.sync_data_files()?;
        }

        Ok(RecordedCommit {
            commit_ordinal,
            sequence: input.sequence,
            locator,
        })
    }

    fn handle_commit_write_failure(
        &mut self,
        source: ArchiveRecorderError,
    ) -> ArchiveRecorderError {
        if let ArchiveRecorderError::Io {
            operation,
            path,
            source,
        } = source
        {
            if is_out_of_space(&source) {
                self.stats.out_of_space_events += 1;
                self.degraded = true;
                return match self.config.out_of_space_policy {
                    OutOfSpacePolicy::FailWriter => ArchiveRecorderError::OutOfSpace(path),
                };
            }

            self.degraded = true;
            return ArchiveRecorderError::Io {
                operation,
                path,
                source,
            };
        }

        self.degraded = true;
        source
    }

    fn handle_write_failure(
        &mut self,
        path: &Path,
        source: std::io::Error,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        if is_out_of_space(&source) {
            self.stats.out_of_space_events += 1;
            self.degraded = true;
            return match self.config.out_of_space_policy {
                OutOfSpacePolicy::FailWriter => {
                    Err(ArchiveRecorderError::OutOfSpace(path.to_path_buf()))
                }
            };
        }

        self.degraded = true;
        Err(ArchiveRecorderError::Io {
            operation: "write active segment",
            path: path.to_path_buf(),
            source,
        })
    }

    fn ensure_commit_log_capacity(&mut self) -> Result<(), ArchiveRecorderError> {
        let required = {
            let disk = self.disk.as_ref().expect("disk recorder state must exist");
            let required = disk.commit_log_write_offset + COMMIT_ENTRY_LEN as u64;
            if required <= disk.commit_log_preallocated_len {
                return Ok(());
            }
            required
        };

        let result = {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            preallocate_metadata_log(
                &mut disk.commit_log_file,
                &disk.commit_log_path,
                required,
                self.config.metadata_log_preallocate_entries,
            )
        };
        let preallocated_len = match result {
            Ok(value) => value,
            Err(source) => return Err(self.handle_commit_write_failure(source)),
        };

        let disk = self.disk.as_mut().expect("disk recorder state must exist");
        if required <= disk.commit_log_preallocated_len {
            return Ok(());
        }
        disk.commit_log_preallocated_len = preallocated_len;
        Ok(())
    }

    fn truncate_commit_log_to_logical_size(&mut self) -> Result<(), ArchiveRecorderError> {
        let Some(disk) = self.disk.as_mut() else {
            return Ok(());
        };

        disk.commit_log_file
            .set_len(disk.commit_log_write_offset)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "truncate commit idxlog to logical size",
                path: disk.commit_log_path.clone(),
                source,
            })?;
        disk.commit_log_preallocated_len = disk.commit_log_write_offset;
        Ok(())
    }

    fn recover_active_segment_tail(
        &mut self,
        committed_write_offset: u64,
        committed_records: u64,
        committed_sequence_start: Option<u64>,
        committed_sequence_end: Option<u64>,
    ) -> Result<SegmentRecoveryResult, ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let active = disk
            .active_segment
            .as_mut()
            .expect("active segment must exist");
        let segment_path = segment_data_path(
            &disk.segments_path,
            active.segment_id,
            active.segment_generation,
        );

        let scan_result = scan_active_segment_tail(
            &mut active.file,
            &segment_path,
            self.config.segment_bytes as u64,
        )?;

        if committed_write_offset < ARCHIVE_FILE_HEADER_V1_LEN as u64 {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "committed write offset is below frame area",
            ));
        }
        if committed_write_offset > scan_result.valid_end {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "commit.idxlog points beyond active segment valid boundary",
            ));
        }

        let target_write_offset = committed_write_offset.min(scan_result.valid_end);
        let mut truncated_bytes = 0u64;
        if scan_result.original_len > target_write_offset {
            active.file.set_len(target_write_offset).map_err(|source| {
                ArchiveRecorderError::Io {
                    operation: "truncate active segment recovery tail",
                    path: segment_path.clone(),
                    source,
                }
            })?;
            truncated_bytes = scan_result.original_len - target_write_offset;
        }

        if self.config.segment_preallocate {
            active
                .file
                .set_len(self.config.segment_bytes as u64)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "re-preallocate active segment after recovery",
                    path: segment_path.clone(),
                    source,
                })?;
        }

        active.write_offset = target_write_offset;
        active.records = committed_records;
        active.sequence_start = committed_sequence_start;
        active.sequence_end = committed_sequence_end;

        Ok(SegmentRecoveryResult { truncated_bytes })
    }

    fn sync_data_files(&mut self) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        if let Some(active) = disk.active_segment.as_mut() {
            active
                .file
                .sync_data()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "sync active segment",
                    path: segment_data_path(
                        &disk.segments_path,
                        active.segment_id,
                        active.segment_generation,
                    ),
                    source,
                })?;
        }
        disk.commit_log_file
            .sync_data()
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "sync commit idxlog",
                path: disk.commit_log_path.clone(),
                source,
            })?;
        disk.catalog_file
            .sync_data()
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "sync catalog",
                path: disk.catalog_path.clone(),
                source,
            })?;

        Ok(())
    }

    fn open_new_active_segment(&mut self, segment_id: u64) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let segment_path = segment_data_path(
            &disk.segments_path,
            segment_id,
            self.config.segment_generation,
        );

        let (mut file, created_new) = if segment_path.exists() {
            (
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&segment_path)
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation: "open preallocated segment",
                        path: segment_path.clone(),
                        source,
                    })?,
                false,
            )
        } else {
            let (file, _) = create_new_file(&segment_path)?;
            (file, true)
        };

        write_archive_header(
            &mut file,
            &segment_path,
            ArchiveFileKind::SegmentData,
            self.config.log_id,
            segment_id,
            self.config.segment_generation,
        )?;

        if self.config.segment_preallocate {
            file.set_len(self.config.segment_bytes as u64)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "preallocate active segment",
                    path: segment_path.clone(),
                    source,
                })?;

            if created_new {
                self.stats.preallocated_segments += 1;
            }
        }

        disk.active_segment = Some(ActiveSegment {
            segment_id,
            segment_generation: self.config.segment_generation,
            created_at_ns: now_ns(),
            write_offset: ARCHIVE_FILE_HEADER_V1_LEN as u64,
            sequence_start: None,
            sequence_end: None,
            records: 0,
            file,
        });

        self.create_spare_preallocated_segments(segment_id + 1)?;
        Ok(())
    }

    fn create_spare_preallocated_segments(
        &mut self,
        start_segment_id: u64,
    ) -> Result<(), ArchiveRecorderError> {
        if !self.config.segment_preallocate || self.config.spare_preallocated_segments == 0 {
            return Ok(());
        }

        let disk = self.disk.as_mut().expect("disk state must exist");
        let spare_count = self.config.spare_preallocated_segments as u64;
        for segment_id in start_segment_id..start_segment_id + spare_count {
            let path = segment_data_path(
                &disk.segments_path,
                segment_id,
                self.config.segment_generation,
            );
            if path.exists() {
                continue;
            }

            let (mut file, _) = create_new_file(&path)?;
            write_archive_header(
                &mut file,
                &path,
                ArchiveFileKind::SegmentData,
                self.config.log_id,
                segment_id,
                self.config.segment_generation,
            )?;
            file.set_len(self.config.segment_bytes as u64)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "preallocate spare segment",
                    path: path.clone(),
                    source,
                })?;
            self.stats.preallocated_segments += 1;
        }

        Ok(())
    }

    fn seal_active_segment_internal(
        &mut self,
        open_next: bool,
    ) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let mut active = match disk.active_segment.take() {
            Some(value) => value,
            None => return Ok(()),
        };

        if self.config.persistence_mode != PersistenceMode::Volatile {
            active
                .file
                .flush()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "flush segment before seal",
                    path: segment_data_path(
                        &disk.segments_path,
                        active.segment_id,
                        active.segment_generation,
                    ),
                    source,
                })?;
            active
                .file
                .sync_data()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "sync segment before seal",
                    path: segment_data_path(
                        &disk.segments_path,
                        active.segment_id,
                        active.segment_generation,
                    ),
                    source,
                })?;
        }

        if active.records > 0 {
            let summary = SegmentSummary {
                segment_id: active.segment_id,
                segment_generation: active.segment_generation,
                sequence_start: active.sequence_start.unwrap_or(0),
                sequence_end: active.sequence_end.unwrap_or(0),
                records: active.records,
                created_at_ns: active.created_at_ns,
                sealed_at_ns: now_ns(),
                data_bytes_used: active.write_offset,
                segment_checksum: 0,
            };

            let segment_meta_path = segment_meta_path(
                &disk.segments_path,
                active.segment_id,
                active.segment_generation,
            );
            let (mut meta_file, _) = create_new_file(&segment_meta_path)?;
            write_archive_header(
                &mut meta_file,
                &segment_meta_path,
                ArchiveFileKind::SegmentMeta,
                self.config.log_id,
                active.segment_id,
                active.segment_generation,
            )?;

            let summary_bytes = summary.to_bytes();
            meta_file
                .write_all(&summary_bytes)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "write segment summary",
                    path: segment_meta_path.clone(),
                    source,
                })?;
            self.stats.metadata_bytes_written +=
                ARCHIVE_FILE_HEADER_V1_LEN as u64 + summary_bytes.len() as u64;

            disk.catalog_file.seek(SeekFrom::End(0)).map_err(|source| {
                ArchiveRecorderError::Io {
                    operation: "seek catalog",
                    path: disk.catalog_path.clone(),
                    source,
                }
            })?;
            disk.catalog_file
                .write_all(&summary_bytes)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "append catalog segment summary",
                    path: disk.catalog_path.clone(),
                    source,
                })?;
            self.stats.metadata_bytes_written += summary_bytes.len() as u64;
            self.stats.rolled_segments += 1;
        }

        if open_next {
            self.open_new_active_segment(active.segment_id + 1)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline_recorder_config() -> RecorderConfig {
        RecorderConfig {
            storage_path: PathBuf::from("/tmp/unused"),
            metadata_log_path: PathBuf::from("/tmp/unused"),
            segment_bytes: 1024,
            segment_preallocate: true,
            spare_preallocated_segments: 1,
            metadata_log_preallocate_entries: DEFAULT_METADATA_LOG_PREALLOCATE_ENTRIES,
            persistence_mode: PersistenceMode::Async,
            checksum_mode: ChecksumMode::Crc32c,
            out_of_space_policy: OutOfSpacePolicy::FailWriter,
            log_id: [0u8; 16],
            segment_generation: 0,
        }
    }

    fn baseline_recorder() -> ArchiveRecorder {
        ArchiveRecorder {
            config: baseline_recorder_config(),
            disk: None,
            stats: ArchiveRecorderStats::default(),
            recovery_status: ArchiveRecoveryStatus::default(),
            next_commit_ordinal: 1,
            last_sequence: None,
            index_by_sequence: BTreeMap::new(),
            volatile_records: Vec::new(),
            finalized: false,
            degraded: false,
        }
    }

    #[test]
    fn fail_writer_policy_marks_recorder_degraded_on_enospc() {
        let mut recorder = baseline_recorder();
        let path = Path::new("/tmp/segment-1-g0.data");

        let result = recorder.handle_write_failure(path, std::io::Error::from_raw_os_error(28));
        assert!(matches!(result, Err(ArchiveRecorderError::OutOfSpace(_))));
        assert!(recorder.degraded);
        assert_eq!(recorder.stats.out_of_space_events, 1);
    }

    #[test]
    fn non_enospc_write_failures_return_io_error_and_mark_degraded() {
        let mut recorder = baseline_recorder();
        let path = Path::new("/tmp/segment-1-g0.data");

        let result = recorder.handle_write_failure(path, std::io::Error::from_raw_os_error(5));
        assert!(matches!(result, Err(ArchiveRecorderError::Io { .. })));
        assert!(recorder.degraded);
        assert_eq!(recorder.stats.out_of_space_events, 0);
    }
}
