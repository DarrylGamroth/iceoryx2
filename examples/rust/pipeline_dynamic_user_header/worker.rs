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

use examples_common::CustomHeader;
use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_millis(100);
const SERVICE_NAME: &str = "Example Pipeline Dynamic User Header";

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let pipeline = node
        .service_builder(&SERVICE_NAME.try_into()?)
        .pipeline::<[u8]>()
        .user_header::<CustomHeader>()
        .number_of_stages(1)
        .max_in_flight_samples(16)
        .initial_max_slice_len(64)
        .open_or_create()?;

    let worker = pipeline
        .worker_builder(0)
        .initial_max_slice_len(128)
        .allocation_strategy(AllocationStrategy::PowerOfTwo)
        .create()?;

    while node.wait(CYCLE_TIME).is_ok() {
        while let Some(mut work) = worker.receive()? {
            let payload_len = work.payload_mut().len();
            let first_byte_before = work.payload_mut().first().copied();

            if let Some(value) = first_byte_before {
                work.payload_mut()[0] = value.wrapping_add(1);
            }

            work.user_header_mut().version += 1;
            work.user_header_mut().timestamp += 1_000_000;

            let first_byte_after = work.payload_mut().first().copied();
            coutln!(
                "worker forwarded {payload_len} bytes, first_byte {:?} -> {:?}, user_header: {:?}",
                first_byte_before,
                first_byte_after,
                work.user_header()
            );

            work.send()?;
        }
    }

    coutln!("exit");
    Ok(())
}
