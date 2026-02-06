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

use core::time::Duration;

extern crate alloc;
use alloc::boxed::Box;

use examples_common::TransmissionData;
use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_secs(1);

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let log = node
        .service_builder(&"My/Funk/LogService".try_into()?)
        .log::<TransmissionData>()
        .retention_size(4)
        .tailer_max_buffer_size(4)
        .enable_safe_overflow(true)
        .open_or_create()?;

    let tailer = log.tailer_builder().create()?;
    let mut expected_sequence = 1_u64;

    coutln!("Tailer ready to receive data!");

    while node.wait(CYCLE_TIME).is_ok() {
        while let Some(sample) = tailer.receive()? {
            let sequence = sample.header().sequence();
            if sequence != expected_sequence {
                coutln!("gap detected: expected {expected_sequence}, got {sequence}");
            }
            coutln!("received seq={} payload={:?}", sequence, sample.payload());
            expected_sequence = sequence + 1;
        }
    }

    coutln!("exit");

    Ok(())
}
