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

use iceoryx2_userland_log_archive::log_archive::{
    ArchiveMetadataIndexerBuilder, ArchiveRecorderBuilder, ChecksumMode, LogRecordInput,
    PersistenceMode,
};
use iceoryx2_userland_log_archive_sqlite::SqliteMetadataSink;

#[test]
fn sqlite_metadata_sink_materializes_commitlog_records() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("metadata.sqlite");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    for sequence in 1..=4u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 100,
                user_header: &[0x1, 0x2],
                payload: &[sequence as u8; 8],
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let sink = SqliteMetadataSink::open(&db_path).unwrap();
    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .sink(Box::new(sink))
        .open()
        .unwrap();
    let processed = indexer.catch_up_once().unwrap();
    assert_eq!(processed, 4);

    let verifier = SqliteMetadataSink::open(&db_path).unwrap();
    assert_eq!(verifier.record_count().unwrap(), 4);
    let record = verifier.query_by_sequence(3).unwrap().unwrap();
    assert_eq!(record.sequence, 3);
    assert_eq!(record.commit_ordinal, 3);
}
