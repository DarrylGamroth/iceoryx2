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

#![warn(missing_docs)]

//! SQLite reference sink for `iceoryx2-userland-log-archive` metadata indexing.
//!
//! This crate is intentionally separate from the core log-archive crate so
//! database dependencies remain external tooling concerns.

use std::path::{Path, PathBuf};

use iceoryx2_userland_log_archive::log_archive::{
    ArchiveLocator, ArchiveMetadataSink, ArchiveMetadataSinkError, MetadataCommitRecord,
};

/// SQLite-backed implementation of [`ArchiveMetadataSink`].
///
/// The sink stores locator-first rows that can be queried by sequence or
/// locator in external tooling.
#[derive(Debug, Clone)]
pub struct SqliteMetadataSink {
    db_path: PathBuf,
}

impl SqliteMetadataSink {
    /// Opens or creates a SQLite sink database and ensures schema exists.
    pub fn open(path: &Path) -> Result<Self, ArchiveMetadataSinkError> {
        let sink = Self {
            db_path: path.to_path_buf(),
        };
        sink.initialize_schema()?;
        Ok(sink)
    }

    /// Returns number of indexed rows.
    pub fn record_count(&self) -> Result<u64, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        let mut stmt = connection
            .prepare("SELECT COUNT(*) FROM locator_index")
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("prepare count query failed: {err}"))
            })?;
        let count: i64 = stmt.query_row([], |row| row.get(0)).map_err(|err| {
            ArchiveMetadataSinkError::new(format!("run count query failed: {err}"))
        })?;
        if count < 0 {
            return Err(ArchiveMetadataSinkError::new(
                "sqlite count query returned negative value",
            ));
        }
        Ok(count as u64)
    }

    /// Returns one metadata row by sequence when available.
    pub fn query_by_sequence(
        &self,
        sequence: u64,
    ) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        let sequence_i64 = u64_to_i64(sequence, "sequence")?;
        let mut stmt = connection
            .prepare(
                "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum
                 FROM locator_index
                 WHERE sequence = ?1
                 LIMIT 1",
            )
            .map_err(|err| ArchiveMetadataSinkError::new(format!("prepare sequence query failed: {err}")))?;

        let mut rows = stmt.query([sequence_i64]).map_err(|err| {
            ArchiveMetadataSinkError::new(format!("run sequence query failed: {err}"))
        })?;
        let Some(row) = rows.next().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("read sequence query row failed: {err}"))
        })?
        else {
            return Ok(None);
        };

        let log_id_blob: Vec<u8> = row
            .get(0)
            .map_err(|err| ArchiveMetadataSinkError::new(format!("decode log_id failed: {err}")))?;
        if log_id_blob.len() != 16 {
            return Err(ArchiveMetadataSinkError::new(
                "invalid log_id length in sqlite metadata table",
            ));
        }
        let mut log_id = [0u8; 16];
        log_id.copy_from_slice(&log_id_blob);

        let commit_ordinal = i64_to_u64(
            row.get::<_, i64>(1).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode commit_ordinal failed: {err}"))
            })?,
            "commit_ordinal",
        )?;
        let sequence = i64_to_u64(
            row.get::<_, i64>(2).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode sequence failed: {err}"))
            })?,
            "sequence",
        )?;
        let segment_id = i64_to_u64(
            row.get::<_, i64>(3).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode segment_id failed: {err}"))
            })?,
            "segment_id",
        )?;
        let segment_generation = i64_to_u32(
            row.get::<_, i64>(4).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode segment_generation failed: {err}"))
            })?,
            "segment_generation",
        )?;
        let file_offset = i64_to_u64(
            row.get::<_, i64>(5).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode file_offset failed: {err}"))
            })?,
            "file_offset",
        )?;
        let frame_len = i64_to_u32(
            row.get::<_, i64>(6).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode frame_len failed: {err}"))
            })?,
            "frame_len",
        )?;
        let frame_checksum = i64_to_u32(
            row.get::<_, i64>(7).map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode frame_checksum failed: {err}"))
            })?,
            "frame_checksum",
        )?;

        Ok(Some(MetadataCommitRecord {
            log_id,
            commit_ordinal,
            sequence,
            locator: ArchiveLocator {
                segment_id,
                segment_generation,
                file_offset,
                frame_len,
            },
            frame_checksum,
        }))
    }

    fn open_connection(&self) -> Result<rusqlite::Connection, ArchiveMetadataSinkError> {
        rusqlite::Connection::open(&self.db_path).map_err(|err| {
            ArchiveMetadataSinkError::new(format!("open sqlite connection failed: {err}"))
        })
    }

    fn initialize_schema(&self) -> Result<(), ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS locator_index (
                    commit_ordinal INTEGER PRIMARY KEY NOT NULL,
                    log_id BLOB NOT NULL,
                    sequence INTEGER NOT NULL,
                    segment_id INTEGER NOT NULL,
                    segment_generation INTEGER NOT NULL,
                    file_offset INTEGER NOT NULL,
                    frame_len INTEGER NOT NULL,
                    frame_checksum INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_locator_index_sequence
                    ON locator_index(sequence);
                CREATE INDEX IF NOT EXISTS idx_locator_index_locator
                    ON locator_index(segment_id, segment_generation, file_offset, frame_len);",
            )
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("initialize sqlite schema failed: {err}"))
            })?;
        Ok(())
    }
}

impl ArchiveMetadataSink for SqliteMetadataSink {
    fn on_records(
        &mut self,
        records: &[MetadataCommitRecord],
    ) -> Result<(), ArchiveMetadataSinkError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut connection = self.open_connection()?;
        let tx = connection.transaction().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("begin sqlite transaction failed: {err}"))
        })?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT OR REPLACE INTO locator_index
                    (commit_ordinal, log_id, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                )
                .map_err(|err| ArchiveMetadataSinkError::new(format!("prepare sqlite insert statement failed: {err}")))?;

            for record in records {
                stmt.execute(rusqlite::params![
                    u64_to_i64(record.commit_ordinal, "commit_ordinal")?,
                    record.log_id.as_slice(),
                    u64_to_i64(record.sequence, "sequence")?,
                    u64_to_i64(record.locator.segment_id, "segment_id")?,
                    u32_to_i64(record.locator.segment_generation),
                    u64_to_i64(record.locator.file_offset, "file_offset")?,
                    u32_to_i64(record.locator.frame_len),
                    u32_to_i64(record.frame_checksum),
                ])
                .map_err(|err| {
                    ArchiveMetadataSinkError::new(format!("insert sqlite record failed: {err}"))
                })?;
            }
        }
        tx.commit().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("commit sqlite transaction failed: {err}"))
        })?;
        Ok(())
    }
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, ArchiveMetadataSinkError> {
    if value > i64::MAX as u64 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from u64 to i64"
        )));
    }
    Ok(value as i64)
}

fn u32_to_i64(value: u32) -> i64 {
    value as i64
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64, ArchiveMetadataSinkError> {
    if value < 0 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "negative sqlite value for {field}"
        )));
    }
    Ok(value as u64)
}

fn i64_to_u32(value: i64, field: &str) -> Result<u32, ArchiveMetadataSinkError> {
    let unsigned = i64_to_u64(value, field)?;
    if unsigned > u32::MAX as u64 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from i64 to u32"
        )));
    }
    Ok(unsigned as u32)
}
