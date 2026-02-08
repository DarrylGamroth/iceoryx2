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
use core::cmp::min;
use core::num::NonZeroUsize;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    ArchiveFileHeaderError, ArchiveFileHeaderV1, ArchiveFileKind, ARCHIVE_FILE_HEADER_V1_LEN,
};

const FRAME_MAGIC: [u8; 4] = *b"LAR1";
const FRAME_HEADER_LEN: usize = 64;
const FRAME_FLAG_CHECKSUM_CRC32C: u16 = 0x0001;
const FRAME_OFFSET_MAGIC: usize = 0;
const FRAME_OFFSET_HEADER_LEN: usize = 4;
const FRAME_OFFSET_FLAGS: usize = 6;
const FRAME_OFFSET_FRAME_LEN: usize = 8;
const FRAME_OFFSET_COMMIT_ORDINAL: usize = 16;
const FRAME_OFFSET_SEQUENCE: usize = 24;
const FRAME_OFFSET_EVENT_TIME_NS: usize = 32;
const FRAME_OFFSET_COMMIT_TIME_NS: usize = 40;
const FRAME_OFFSET_USER_HEADER_LEN: usize = 48;
const FRAME_OFFSET_PAYLOAD_LEN: usize = 52;
const FRAME_OFFSET_CHECKSUM: usize = 56;

const COMMIT_ENTRY_MAGIC: [u8; 4] = *b"CID1";
const COMMIT_ENTRY_LEN: usize = 56;
const COMMIT_OFFSET_MAGIC: usize = 0;
const COMMIT_OFFSET_ENTRY_LEN: usize = 4;
const COMMIT_OFFSET_FLAGS: usize = 6;
const COMMIT_OFFSET_COMMIT_ORDINAL: usize = 8;
const COMMIT_OFFSET_SEQUENCE: usize = 16;
const COMMIT_OFFSET_SEGMENT_ID: usize = 24;
const COMMIT_OFFSET_SEGMENT_GENERATION: usize = 32;
const COMMIT_OFFSET_FILE_OFFSET: usize = 40;
const COMMIT_OFFSET_FRAME_LEN: usize = 48;
const COMMIT_OFFSET_FRAME_CHECKSUM: usize = 52;

const SEGMENT_SUMMARY_LEN: usize = 88;

/// Durability mode of the archive recorder.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PersistenceMode {
    /// Keeps records only in-memory and does not persist segment files.
    Volatile,
    /// Persists data without forcing a per-append fsync barrier.
    Async,
    /// Persists data and enforces a per-append fsync barrier.
    Sync,
}

/// Checksum strategy for persisted frames.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ChecksumMode {
    /// Do not store per-frame checksum values.
    None,
    /// Store CRC32C checksums per frame.
    Crc32c,
}

/// Out-of-space handling policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OutOfSpacePolicy {
    /// Fail the append operation and mark recorder as degraded.
    FailWriter,
}

/// Physical frame locator in archive storage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ArchiveLocator {
    /// Segment id of the record.
    pub segment_id: u64,
    /// Segment generation of the record.
    pub segment_generation: u32,
    /// File offset of the frame start in the segment file.
    pub file_offset: u64,
    /// Encoded frame length in bytes.
    pub frame_len: u32,
}

/// Returned when a frame was appended successfully.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RecordedCommit {
    /// Monotonic commit ordinal assigned by recorder.
    pub commit_ordinal: u64,
    /// Logical source sequence from log adapter.
    pub sequence: u64,
    /// Physical locator of the persisted frame.
    pub locator: ArchiveLocator,
}

/// Input frame for log pattern adapter ingestion.
#[derive(Debug, Clone, Copy)]
pub struct LogRecordInput<'a> {
    /// Monotonic source sequence.
    pub sequence: u64,
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// User header bytes.
    pub user_header: &'a [u8],
    /// Payload bytes.
    pub payload: &'a [u8],
}

/// Recorder statistics and accounting values.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ArchiveRecorderStats {
    /// Number of committed records.
    pub committed_records: u64,
    /// Number of committed payload bytes.
    pub payload_bytes_committed: u64,
    /// Number of data bytes written (`segment-*.data`).
    pub data_bytes_written: u64,
    /// Number of metadata bytes written (`catalog.bin`, `commit.idxlog`, `segment-*.meta`).
    pub metadata_bytes_written: u64,
    /// Number of segment roll operations.
    pub rolled_segments: u64,
    /// Number of preallocated segment files created.
    pub preallocated_segments: u64,
    /// Number of out-of-space events observed.
    pub out_of_space_events: u64,
}

impl ArchiveRecorderStats {
    /// Returns write amplification ratio (`written / payload`).
    pub fn amplification_ratio(&self) -> f64 {
        if self.payload_bytes_committed == 0 {
            return 0.0;
        }

        (self.data_bytes_written + self.metadata_bytes_written) as f64
            / self.payload_bytes_committed as f64
    }
}

/// Replay budget limits.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ReplayBudget {
    /// Maximum records returned per range/batch call.
    pub max_records_per_call: usize,
    /// Maximum aggregate bytes returned per range/batch call.
    pub max_bytes_per_call: usize,
}

impl Default for ReplayBudget {
    fn default() -> Self {
        Self {
            max_records_per_call: 1024,
            max_bytes_per_call: 64 * 1024 * 1024,
        }
    }
}

/// Decoded frame returned by replayer APIs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplayedFrame {
    /// Commit ordinal from archive.
    pub commit_ordinal: u64,
    /// Source sequence.
    pub sequence: u64,
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// Commit timestamp in nanoseconds.
    pub commit_time_ns: u64,
    /// User header bytes.
    pub user_header: Vec<u8>,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// Physical locator.
    pub locator: ArchiveLocator,
}

/// Errors returned by archive recorder operations.
#[derive(Debug)]
pub enum ArchiveRecorderError {
    /// Invalid configuration value.
    InvalidConfiguration(&'static str),
    /// Recorder storage root already contains archive files.
    ArchiveAlreadyExists(PathBuf),
    /// Recorder has been finalized and no more appends are accepted.
    Finalized,
    /// Recorder entered degraded state after a fatal I/O failure.
    Degraded,
    /// Sequence values must be strictly monotonic.
    SequenceNotMonotonic {
        /// Previous sequence value.
        previous: u64,
        /// Incoming sequence value.
        next: u64,
    },
    /// A single frame cannot fit into the configured segment size.
    FrameTooLarge {
        /// Encoded bytes required for one frame.
        required: usize,
        /// Configured segment byte size.
        segment_bytes: usize,
    },
    /// Out-of-space failure.
    OutOfSpace(PathBuf),
    /// File header encoding/validation failure.
    FileHeader(ArchiveFileHeaderError),
    /// Underlying I/O failure.
    Io {
        /// Operation name.
        operation: &'static str,
        /// Path involved in operation.
        path: PathBuf,
        /// Source error.
        source: std::io::Error,
    },
}

impl Display for ArchiveRecorderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveRecorderError::{self:?}")
    }
}

impl std::error::Error for ArchiveRecorderError {}

impl From<ArchiveFileHeaderError> for ArchiveRecorderError {
    fn from(value: ArchiveFileHeaderError) -> Self {
        Self::FileHeader(value)
    }
}

/// Errors returned by archive replayer operations.
#[derive(Debug)]
pub enum ArchiveReplayError {
    /// `commit.idxlog` file is missing.
    MissingCommitLog(PathBuf),
    /// Required segment file is missing.
    MissingSegment(PathBuf),
    /// Unsupported or malformed header.
    FileHeader(ArchiveFileHeaderError),
    /// Corrupted commit-log entry.
    InvalidCommitEntry(&'static str),
    /// Duplicate sequence in commit-log.
    DuplicateSequence(u64),
    /// Corrupted frame header magic.
    InvalidFrameMagic([u8; 4]),
    /// Corrupted frame header length.
    InvalidFrameHeaderLength(u16),
    /// Corrupted frame length.
    InvalidFrameLength {
        /// Frame length from locator/commit metadata.
        expected: u32,
        /// Frame length decoded from frame header.
        decoded: u32,
    },
    /// Frame checksum mismatch.
    ChecksumMismatch {
        /// Expected checksum.
        expected: u32,
        /// Actual checksum.
        actual: u32,
        /// Locator of the corrupted frame.
        locator: ArchiveLocator,
    },
    /// Underlying I/O failure.
    Io {
        /// Operation name.
        operation: &'static str,
        /// Path involved in operation.
        path: PathBuf,
        /// Source error.
        source: std::io::Error,
    },
}

impl Display for ArchiveReplayError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveReplayError::{self:?}")
    }
}

impl std::error::Error for ArchiveReplayError {}

impl From<ArchiveFileHeaderError> for ArchiveReplayError {
    fn from(value: ArchiveFileHeaderError) -> Self {
        Self::FileHeader(value)
    }
}

/// Builder for [`ArchiveRecorder`].
#[derive(Debug, Clone)]
pub struct ArchiveRecorderBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    segment_bytes: usize,
    segment_preallocate: bool,
    spare_preallocated_segments: usize,
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

    /// Creates a new recorder.
    pub fn create(self) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        let minimal_frame_bytes = ARCHIVE_FILE_HEADER_V1_LEN + FRAME_HEADER_LEN + 8;
        if self.segment_bytes <= minimal_frame_bytes {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "segment_bytes is too small to store any frame",
            ));
        }

        let config = RecorderConfig {
            storage_path: self.storage_path.clone(),
            metadata_log_path: self
                .metadata_log_path
                .clone()
                .unwrap_or_else(|| self.storage_path.clone()),
            segment_bytes: self.segment_bytes,
            segment_preallocate: self.segment_preallocate,
            spare_preallocated_segments: self.spare_preallocated_segments,
            persistence_mode: self.persistence_mode,
            checksum_mode: self.checksum_mode,
            out_of_space_policy: self.out_of_space_policy,
            log_id: self.log_id,
            segment_generation: self.segment_generation,
        };

        if config.persistence_mode == PersistenceMode::Volatile {
            return Ok(ArchiveRecorder {
                config,
                disk: None,
                stats: ArchiveRecorderStats::default(),
                next_commit_ordinal: 1,
                last_sequence: None,
                index_by_sequence: BTreeMap::new(),
                volatile_records: Vec::new(),
                finalized: false,
                degraded: false,
            });
        }

        if config.storage_path.exists() {
            if config.storage_path.join("catalog.bin").exists()
                || config.storage_path.join("segments").exists()
            {
                return Err(ArchiveRecorderError::ArchiveAlreadyExists(
                    config.storage_path.clone(),
                ));
            }
        }

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
        fs::create_dir_all(&config.metadata_log_path).map_err(|source| {
            ArchiveRecorderError::Io {
                operation: "create metadata directory",
                path: config.metadata_log_path.clone(),
                source,
            }
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

        let mut recorder = ArchiveRecorder {
            config,
            disk: Some(DiskRecorderState {
                segments_path,
                catalog_path,
                commit_log_path,
                catalog_file,
                commit_log_file,
                active_segment: None,
            }),
            stats: ArchiveRecorderStats::default(),
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
}

#[derive(Debug, Clone)]
struct RecorderConfig {
    storage_path: PathBuf,
    metadata_log_path: PathBuf,
    segment_bytes: usize,
    segment_preallocate: bool,
    spare_preallocated_segments: usize,
    persistence_mode: PersistenceMode,
    checksum_mode: ChecksumMode,
    out_of_space_policy: OutOfSpacePolicy,
    log_id: [u8; 16],
    segment_generation: u32,
}

#[derive(Debug)]
struct DiskRecorderState {
    segments_path: PathBuf,
    catalog_path: PathBuf,
    commit_log_path: PathBuf,
    catalog_file: File,
    commit_log_file: File,
    active_segment: Option<ActiveSegment>,
}

#[derive(Debug)]
struct ActiveSegment {
    segment_id: u64,
    segment_generation: u32,
    created_at_ns: u64,
    write_offset: u64,
    sequence_start: Option<u64>,
    sequence_end: Option<u64>,
    records: u64,
    file: File,
}

#[derive(Debug)]
#[allow(dead_code)]
struct VolatileFrame {
    commit_ordinal: u64,
    sequence: u64,
    event_time_ns: u64,
    commit_time_ns: u64,
    user_header: Vec<u8>,
    payload: Vec<u8>,
    locator: ArchiveLocator,
}

/// Recorder core for log archive segment files.
#[derive(Debug)]
pub struct ArchiveRecorder {
    config: RecorderConfig,
    disk: Option<DiskRecorderState>,
    stats: ArchiveRecorderStats,
    next_commit_ordinal: u64,
    last_sequence: Option<u64>,
    index_by_sequence: BTreeMap<u64, ArchiveLocator>,
    volatile_records: Vec<VolatileFrame>,
    finalized: bool,
    degraded: bool,
}

impl ArchiveRecorder {
    /// Returns current recorder stats.
    pub fn stats(&self) -> ArchiveRecorderStats {
        self.stats
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

        let commit_log_path = self
            .disk
            .as_ref()
            .expect("disk recorder state must exist")
            .commit_log_path
            .clone();
        let disk = self.disk.as_mut().expect("disk recorder state must exist");
        write_commit_entry(
            &mut disk.commit_log_file,
            &commit_log_path,
            CommitEntry {
                commit_ordinal,
                sequence: input.sequence,
                locator,
                frame_checksum: frame.checksum,
            },
        )
        .map_err(|source| self.handle_commit_write_failure(source))?;

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

/// Builder for [`ArchiveReplayer`].
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy)]
struct CommitEntry {
    commit_ordinal: u64,
    sequence: u64,
    locator: ArchiveLocator,
    frame_checksum: u32,
}

#[derive(Debug, Clone, Copy)]
struct SegmentSummary {
    segment_id: u64,
    segment_generation: u32,
    sequence_start: u64,
    sequence_end: u64,
    records: u64,
    created_at_ns: u64,
    sealed_at_ns: u64,
    data_bytes_used: u64,
    segment_checksum: u32,
}

impl SegmentSummary {
    fn to_bytes(self) -> [u8; SEGMENT_SUMMARY_LEN] {
        let mut bytes = [0u8; SEGMENT_SUMMARY_LEN];
        bytes[0..8].copy_from_slice(&self.segment_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.segment_generation.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.sequence_start.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.sequence_end.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.records.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.created_at_ns.to_le_bytes());
        bytes[44..52].copy_from_slice(&self.sealed_at_ns.to_le_bytes());
        bytes[52..60].copy_from_slice(&self.data_bytes_used.to_le_bytes());
        bytes[60..64].copy_from_slice(&self.segment_checksum.to_le_bytes());
        bytes
    }
}

#[derive(Debug)]
struct EncodedFrame {
    bytes: Vec<u8>,
    checksum: u32,
}

impl EncodedFrame {
    fn new(
        commit_ordinal: u64,
        sequence: u64,
        event_time_ns: u64,
        commit_time_ns: u64,
        user_header: &[u8],
        payload: &[u8],
        checksum_mode: ChecksumMode,
    ) -> Self {
        let frame_len = align_up(FRAME_HEADER_LEN + user_header.len() + payload.len(), 8);
        let mut bytes = vec![0u8; frame_len];
        bytes[FRAME_OFFSET_MAGIC..FRAME_OFFSET_MAGIC + 4].copy_from_slice(&FRAME_MAGIC);
        bytes[FRAME_OFFSET_HEADER_LEN..FRAME_OFFSET_HEADER_LEN + 2]
            .copy_from_slice(&(FRAME_HEADER_LEN as u16).to_le_bytes());
        let mut flags = 0u16;
        if checksum_mode == ChecksumMode::Crc32c {
            flags |= FRAME_FLAG_CHECKSUM_CRC32C;
        }
        bytes[FRAME_OFFSET_FLAGS..FRAME_OFFSET_FLAGS + 2].copy_from_slice(&flags.to_le_bytes());
        bytes[FRAME_OFFSET_FRAME_LEN..FRAME_OFFSET_FRAME_LEN + 4]
            .copy_from_slice(&(frame_len as u32).to_le_bytes());
        bytes[FRAME_OFFSET_COMMIT_ORDINAL..FRAME_OFFSET_COMMIT_ORDINAL + 8]
            .copy_from_slice(&commit_ordinal.to_le_bytes());
        bytes[FRAME_OFFSET_SEQUENCE..FRAME_OFFSET_SEQUENCE + 8]
            .copy_from_slice(&sequence.to_le_bytes());
        bytes[FRAME_OFFSET_EVENT_TIME_NS..FRAME_OFFSET_EVENT_TIME_NS + 8]
            .copy_from_slice(&event_time_ns.to_le_bytes());
        bytes[FRAME_OFFSET_COMMIT_TIME_NS..FRAME_OFFSET_COMMIT_TIME_NS + 8]
            .copy_from_slice(&commit_time_ns.to_le_bytes());
        bytes[FRAME_OFFSET_USER_HEADER_LEN..FRAME_OFFSET_USER_HEADER_LEN + 4]
            .copy_from_slice(&(user_header.len() as u32).to_le_bytes());
        bytes[FRAME_OFFSET_PAYLOAD_LEN..FRAME_OFFSET_PAYLOAD_LEN + 4]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[FRAME_HEADER_LEN..FRAME_HEADER_LEN + user_header.len()].copy_from_slice(user_header);
        bytes[FRAME_HEADER_LEN + user_header.len()
            ..FRAME_HEADER_LEN + user_header.len() + payload.len()]
            .copy_from_slice(payload);

        let checksum = if checksum_mode == ChecksumMode::Crc32c {
            let crc = crc32c::crc32c(&bytes);
            bytes[FRAME_OFFSET_CHECKSUM..FRAME_OFFSET_CHECKSUM + 4]
                .copy_from_slice(&crc.to_le_bytes());
            crc
        } else {
            0
        };

        Self { bytes, checksum }
    }
}

fn write_commit_entry(
    file: &mut File,
    commit_log_path: &Path,
    entry: CommitEntry,
) -> Result<(), ArchiveRecorderError> {
    let mut bytes = [0u8; COMMIT_ENTRY_LEN];
    bytes[COMMIT_OFFSET_MAGIC..COMMIT_OFFSET_MAGIC + 4].copy_from_slice(&COMMIT_ENTRY_MAGIC);
    bytes[COMMIT_OFFSET_ENTRY_LEN..COMMIT_OFFSET_ENTRY_LEN + 2]
        .copy_from_slice(&(COMMIT_ENTRY_LEN as u16).to_le_bytes());
    bytes[COMMIT_OFFSET_FLAGS..COMMIT_OFFSET_FLAGS + 2].copy_from_slice(&0u16.to_le_bytes());
    bytes[COMMIT_OFFSET_COMMIT_ORDINAL..COMMIT_OFFSET_COMMIT_ORDINAL + 8]
        .copy_from_slice(&entry.commit_ordinal.to_le_bytes());
    bytes[COMMIT_OFFSET_SEQUENCE..COMMIT_OFFSET_SEQUENCE + 8]
        .copy_from_slice(&entry.sequence.to_le_bytes());
    bytes[COMMIT_OFFSET_SEGMENT_ID..COMMIT_OFFSET_SEGMENT_ID + 8]
        .copy_from_slice(&entry.locator.segment_id.to_le_bytes());
    bytes[COMMIT_OFFSET_SEGMENT_GENERATION..COMMIT_OFFSET_SEGMENT_GENERATION + 4]
        .copy_from_slice(&entry.locator.segment_generation.to_le_bytes());
    bytes[COMMIT_OFFSET_FILE_OFFSET..COMMIT_OFFSET_FILE_OFFSET + 8]
        .copy_from_slice(&entry.locator.file_offset.to_le_bytes());
    bytes[COMMIT_OFFSET_FRAME_LEN..COMMIT_OFFSET_FRAME_LEN + 4]
        .copy_from_slice(&entry.locator.frame_len.to_le_bytes());
    bytes[COMMIT_OFFSET_FRAME_CHECKSUM..COMMIT_OFFSET_FRAME_CHECKSUM + 4]
        .copy_from_slice(&entry.frame_checksum.to_le_bytes());

    file.seek(SeekFrom::End(0))
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "seek commit idxlog",
            path: commit_log_path.to_path_buf(),
            source,
        })?;
    file.write_all(&bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "append commit idxlog entry",
            path: commit_log_path.to_path_buf(),
            source,
        })?;

    Ok(())
}

fn read_commit_entries(path: &Path) -> Result<Vec<CommitEntry>, ArchiveReplayError> {
    let mut file = File::open(path).map_err(|source| ArchiveReplayError::Io {
        operation: "open commit idxlog",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveReplayError::Io {
            operation: "read commit idxlog header",
            path: path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != ArchiveFileKind::CommitIdxLog {
        return Err(ArchiveReplayError::InvalidCommitEntry(
            "commit.idxlog has invalid file kind",
        ));
    }

    let file_len = file
        .metadata()
        .map_err(|source| ArchiveReplayError::Io {
            operation: "read commit idxlog metadata",
            path: path.to_path_buf(),
            source,
        })?
        .len() as usize;
    let mut remaining = file_len.saturating_sub(ARCHIVE_FILE_HEADER_V1_LEN);
    if remaining % COMMIT_ENTRY_LEN != 0 {
        return Err(ArchiveReplayError::InvalidCommitEntry(
            "commit.idxlog entry area is not aligned to commit entry length",
        ));
    }

    let mut entries = Vec::new();
    while remaining > 0 {
        let mut bytes = [0u8; COMMIT_ENTRY_LEN];
        file.read_exact(&mut bytes)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read commit idxlog entry",
                path: path.to_path_buf(),
                source,
            })?;
        remaining -= COMMIT_ENTRY_LEN;

        let magic = [
            bytes[COMMIT_OFFSET_MAGIC],
            bytes[COMMIT_OFFSET_MAGIC + 1],
            bytes[COMMIT_OFFSET_MAGIC + 2],
            bytes[COMMIT_OFFSET_MAGIC + 3],
        ];
        if magic != COMMIT_ENTRY_MAGIC {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "invalid commit entry magic",
            ));
        }

        let entry_len = read_u16(&bytes, COMMIT_OFFSET_ENTRY_LEN);
        if entry_len as usize != COMMIT_ENTRY_LEN {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "invalid commit entry length",
            ));
        }

        let locator = ArchiveLocator {
            segment_id: read_u64(&bytes, COMMIT_OFFSET_SEGMENT_ID),
            segment_generation: read_u32(&bytes, COMMIT_OFFSET_SEGMENT_GENERATION),
            file_offset: read_u64(&bytes, COMMIT_OFFSET_FILE_OFFSET),
            frame_len: read_u32(&bytes, COMMIT_OFFSET_FRAME_LEN),
        };
        entries.push(CommitEntry {
            commit_ordinal: read_u64(&bytes, COMMIT_OFFSET_COMMIT_ORDINAL),
            sequence: read_u64(&bytes, COMMIT_OFFSET_SEQUENCE),
            locator,
            frame_checksum: read_u32(&bytes, COMMIT_OFFSET_FRAME_CHECKSUM),
        });
    }

    Ok(entries)
}

fn write_archive_header(
    file: &mut File,
    path: &Path,
    file_kind: ArchiveFileKind,
    log_id: [u8; 16],
    segment_id: u64,
    segment_generation: u32,
) -> Result<(), ArchiveRecorderError> {
    let mut header = ArchiveFileHeaderV1::new(file_kind);
    header.log_id = log_id;
    header.created_at_ns = now_ns();
    header.segment_id = segment_id;
    header.segment_generation = segment_generation;
    let bytes = header.to_bytes()?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "seek archive header",
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(&bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "write archive header",
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

fn create_new_file(path: &Path) -> Result<(File, PathBuf), ArchiveRecorderError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "create file",
            path: path.to_path_buf(),
            source,
        })?;
    Ok((file, path.to_path_buf()))
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_nanos() as u64)
        .unwrap_or(0)
}

fn segment_data_path(base: &Path, segment_id: u64, generation: u32) -> PathBuf {
    base.join(format!("segment-{segment_id}-g{generation}.data"))
}

fn segment_meta_path(base: &Path, segment_id: u64, generation: u32) -> PathBuf {
    base.join(format!("segment-{segment_id}-g{generation}.meta"))
}

fn align_up(value: usize, alignment: usize) -> usize {
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + (alignment - remainder)
    }
}

fn is_out_of_space(source: &std::io::Error) -> bool {
    source.raw_os_error() == Some(28)
}

fn lower_bound(values: &[u64], needle: u64) -> usize {
    values.partition_point(|value| *value < needle)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

fn frame_crc_from_payload(frame: &ReplayedFrame, verify: bool) -> Result<u32, ArchiveReplayError> {
    if !verify {
        return Ok(0);
    }

    let encoded = EncodedFrame::new(
        frame.commit_ordinal,
        frame.sequence,
        frame.event_time_ns,
        frame.commit_time_ns,
        &frame.user_header,
        &frame.payload,
        ChecksumMode::Crc32c,
    );
    Ok(encoded.checksum)
}
