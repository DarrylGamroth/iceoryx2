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

const CYCLE_TIME: Duration = Duration::from_secs(1);

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let log = node
        .service_builder(&"UserHeader/Log".try_into()?)
        .log::<u64>()
        .user_header::<CustomHeader>()
        .retention_size(8)
        .tailer_max_buffer_size(8)
        // Disable safe overflow and drop when a tailer cannot accept more samples.
        .enable_safe_overflow(false)
        .open_or_create()?;

    let appender = log
        .appender_builder()
        .unable_to_deliver_strategy(UnableToDeliverStrategy::DiscardSample)
        .create()?;

    let mut counter: u64 = 0;

    while node.wait(CYCLE_TIME).is_ok() {
        counter += 1;
        let mut sample = appender.loan_uninit()?;

        sample.user_header_mut().version = 123;
        sample.user_header_mut().timestamp = 80_337 + counter;

        let sample = sample.write_payload(counter);

        sample.send()?;

        coutln!("Appended sample {counter} ...");
    }

    coutln!("exit");

    Ok(())
}
