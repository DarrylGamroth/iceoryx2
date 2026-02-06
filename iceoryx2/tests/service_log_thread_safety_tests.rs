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

use std::sync::{Arc, Barrier};

use iceoryx2::prelude::*;
use iceoryx2::testing::*;
use iceoryx2_bb_testing::assert_that;
use iceoryx2_bb_testing::watchdog::Watchdog;

#[test]
fn log_builder_aliases_are_applied() {
    let _watchdog = Watchdog::new();

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();

    let log = node
        .service_builder(&service_name)
        .log::<u64>()
        .max_appenders(3)
        .max_tailers(4)
        .retention_size(5)
        .tailer_max_buffer_size(6)
        .tailer_max_borrowed_samples(7)
        .create()
        .unwrap();

    assert_that!(log.static_config().max_appenders(), eq 3);
    assert_that!(log.static_config().max_tailers(), eq 4);
    assert_that!(log.static_config().retention_size(), eq 5);
    assert_that!(log.static_config().tailer_max_buffer_size(), eq 6);
    assert_that!(log.static_config().tailer_max_borrowed_samples(), eq 7);
}

#[test]
fn log_sequence_is_monotonic_for_single_appender() {
    let _watchdog = Watchdog::new();

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();

    const NUMBER_OF_SAMPLES: usize = 64;

    let log = node
        .service_builder(&service_name)
        .log::<u64>()
        .user_header::<u64>()
        .max_appenders(1)
        .max_tailers(1)
        .tailer_max_buffer_size(NUMBER_OF_SAMPLES)
        .create()
        .unwrap();

    let appender = log
        .appender_builder()
        .max_loaned_samples(8)
        .create()
        .unwrap();
    let tailer = log
        .tailer_builder()
        .buffer_size(NUMBER_OF_SAMPLES)
        .create()
        .unwrap();

    for n in 0..NUMBER_OF_SAMPLES {
        let mut sample = appender.loan().unwrap();
        *sample.payload_mut() = n as u64;
        *sample.user_header_mut() = (n * 10) as u64;
        sample.send().unwrap();
    }

    let mut received = 0;
    while received < NUMBER_OF_SAMPLES {
        if let Some(sample) = tailer.receive().unwrap() {
            assert_that!(sample.header().sequence(), eq(received + 1) as u64);
            assert_that!(*sample.payload(), eq received as u64);
            assert_that!(*sample.user_header(), eq(received * 10) as u64);
            received += 1;
        }
    }
}

#[test]
fn log_sequence_is_unique_for_multiple_appenders() {
    let _watchdog = Watchdog::new();
    type ServiceType = ipc_threadsafe::Service;

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ServiceType>()
        .unwrap();

    const APPENDER_THREADS: usize = 2;
    const SAMPLES_PER_APPENDER: usize = 128;
    const TOTAL_SAMPLES: usize = APPENDER_THREADS * SAMPLES_PER_APPENDER;

    let log = node
        .service_builder(&service_name)
        .log::<u64>()
        .max_appenders(APPENDER_THREADS)
        .max_tailers(1)
        .tailer_max_buffer_size(TOTAL_SAMPLES)
        .create()
        .unwrap();

    let tailer = log
        .tailer_builder()
        .buffer_size(TOTAL_SAMPLES)
        .create()
        .unwrap();
    let barrier = Arc::new(Barrier::new(APPENDER_THREADS));

    std::thread::scope(|s| {
        for thread_id in 0..APPENDER_THREADS {
            let appender = log
                .appender_builder()
                .max_loaned_samples(8)
                .create()
                .unwrap();
            let barrier = barrier.clone();

            s.spawn(move || {
                barrier.wait();

                for n in 0..SAMPLES_PER_APPENDER {
                    appender
                        .send_copy((thread_id * SAMPLES_PER_APPENDER + n) as u64)
                        .unwrap();
                }
            });
        }

        let mut seen = vec![false; TOTAL_SAMPLES + 1];
        let mut received = 0;

        while received < TOTAL_SAMPLES {
            if let Some(sample) = tailer.receive().unwrap() {
                let sequence = sample.header().sequence() as usize;
                assert_that!(sequence > 0 && sequence <= TOTAL_SAMPLES, eq true);
                assert_that!(seen[sequence], eq false);
                seen[sequence] = true;
                received += 1;
            }
        }

        assert_that!(seen.iter().skip(1).all(|v| *v), eq true);
    });
}
