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

use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_secs(1);

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let log = node
        .service_builder(&"Dynamic/Log".try_into()?)
        .log::<[u8]>()
        .retention_size(8)
        .tailer_max_buffer_size(8)
        .enable_safe_overflow(false)
        .open_or_create()?;

    let tailer = log.tailer_builder().create()?;

    coutln!("Tailer ready to receive dynamic data!");

    while node.wait(CYCLE_TIME).is_ok() {
        while let Some(sample) = tailer.receive()? {
            coutln!(
                "received seq={} bytes={}",
                sample.header().sequence(),
                sample.payload().len()
            );
        }
    }

    coutln!("exit");

    Ok(())
}
