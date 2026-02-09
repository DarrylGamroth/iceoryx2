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

use iceoryx2::config::Config;
use iceoryx2::prelude::*;
use iceoryx2_bb_container::semantic_string::SemanticString;

const CYCLE_TIME: Duration = Duration::from_millis(25);

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let mut config = Config::default();
    config.global.service.hugepages.mount_path =
        iceoryx2_bb_system_types::path::Path::new(b"/dev/hugepages")?;

    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc_hugepages::Service>()?;
    let service = node
        .service_builder(&"Service-Variants-Hugepages-Example".try_into()?)
        .publish_subscribe::<u64>()
        .open_or_create()?;

    let publisher = service.publisher_builder().create()?;
    let subscriber = service.subscriber_builder().create()?;
    publisher.send_copy(42)?;

    for _ in 0..20 {
        if let Some(sample) = subscriber.receive()? {
            coutln!("received: {}", sample.payload());
            coutln!("hugepages roundtrip ok");
            return Ok(());
        }

        let _ = node.wait(CYCLE_TIME);
    }

    Err("did not receive hugepages sample in time".into())
}
