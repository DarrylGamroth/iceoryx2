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

extern crate iceoryx2_bb_loggers;

mod service_hugepages_runtime_tests {
    use core::time::Duration;
    use iceoryx2::config::Config;
    use iceoryx2::prelude::*;
    use iceoryx2::testing::{generate_isolated_config, generate_service_name};
    use iceoryx2_bb_container::semantic_string::SemanticString;
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_bb_testing::watchdog::Watchdog;
    use std::fs::OpenOptions;
    use std::path::Path as StdPath;

    const HUGEPAGE_MOUNT: &str = "/dev/hugepages";

    fn has_hugetlbfs_mount(path: &str) -> bool {
        let mounts = match std::fs::read_to_string("/proc/mounts") {
            Ok(content) => content,
            Err(_) => return false,
        };

        mounts.lines().any(|line| {
            let mut fields = line.split_whitespace();
            let _device = fields.next();
            let mount = fields.next();
            let fs_type = fields.next();
            mount == Some(path) && fs_type == Some("hugetlbfs")
        })
    }

    fn can_create_file_in_hugepage_mount(path: &str) -> bool {
        let probe = format!(
            "{path}/iox2-hugepages-runtime-test-probe-{}",
            std::process::id()
        );
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(probe.as_str())
        {
            Ok(_) => {
                let _ = std::fs::remove_file(probe);
                true
            }
            Err(_) => false,
        }
    }

    fn hugepages_environment_is_available() -> bool {
        StdPath::new(HUGEPAGE_MOUNT).exists()
            && has_hugetlbfs_mount(HUGEPAGE_MOUNT)
            && can_create_file_in_hugepage_mount(HUGEPAGE_MOUNT)
    }

    fn hugepages_test_config() -> Config {
        let mut config = generate_isolated_config();
        config.global.service.hugepages.mount_path =
            iceoryx2_bb_system_types::path::Path::new(HUGEPAGE_MOUNT.as_bytes()).unwrap();
        config
    }

    #[test]
    fn publish_subscribe_fixed_payload_roundtrip_works() {
        if !hugepages_environment_is_available() {
            return;
        }

        let _watchdog = Watchdog::new();
        let config = hugepages_test_config();
        let service_name = generate_service_name();
        let node = NodeBuilder::new()
            .config(&config)
            .create::<ipc_hugepages::Service>()
            .unwrap();
        let service = node
            .service_builder(&service_name)
            .publish_subscribe::<u64>()
            .create()
            .unwrap();
        let publisher = service.publisher_builder().create().unwrap();
        let subscriber = service.subscriber_builder().create().unwrap();

        publisher.send_copy(0xfeed_beef).unwrap();

        let mut received = None;
        for _ in 0..20 {
            if let Some(sample) = subscriber.receive().unwrap() {
                received = Some(*sample.payload());
                break;
            }
            let _ = node.wait(Duration::from_millis(5));
        }

        assert_that!(received, eq Some(0xfeed_beef));
    }

    #[test]
    fn publish_subscribe_dynamic_payload_reallocation_roundtrip_works() {
        if !hugepages_environment_is_available() {
            return;
        }

        let _watchdog = Watchdog::new();
        let config = hugepages_test_config();
        let service_name = generate_service_name();
        let node = NodeBuilder::new()
            .config(&config)
            .create::<ipc_hugepages::Service>()
            .unwrap();
        let service = node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .create()
            .unwrap();
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(8)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .unwrap();
        let subscriber = service.subscriber_builder().create().unwrap();

        let mut sample = publisher.loan_slice(4096).unwrap();
        for (idx, byte) in sample.payload_mut().iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }
        sample.send().unwrap();

        let mut received_prefix = None;
        for _ in 0..20 {
            if let Some(sample) = subscriber.receive().unwrap() {
                received_prefix = Some((sample.payload().len(), sample.payload()[123]));
                break;
            }
            let _ = node.wait(Duration::from_millis(5));
        }

        assert_that!(received_prefix, eq Some((4096, (123 % 251) as u8)));
    }

    #[test]
    fn request_response_dynamic_payload_roundtrip_works() {
        if !hugepages_environment_is_available() {
            return;
        }

        let _watchdog = Watchdog::new();
        let config = hugepages_test_config();
        let service_name = generate_service_name();
        let node = NodeBuilder::new()
            .config(&config)
            .create::<ipc_hugepages::Service>()
            .unwrap();
        let service = node
            .service_builder(&service_name)
            .request_response::<[u8], [u8]>()
            .create()
            .unwrap();
        let client = service
            .client_builder()
            .initial_max_slice_len(8)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .unwrap();
        let server = service
            .server_builder()
            .initial_max_slice_len(8)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .unwrap();

        let request = client
            .loan_slice_uninit(1024)
            .unwrap()
            .write_from_fn(|idx| (idx % 127) as u8);
        let pending_response = request.send().unwrap();

        let mut response_sent = false;
        for _ in 0..20 {
            if let Some(active_request) = server.receive().unwrap() {
                let response = active_request
                    .loan_slice_uninit(active_request.payload().len() + 1)
                    .unwrap()
                    .write_from_fn(|idx| (idx % 97) as u8);
                response.send().unwrap();
                response_sent = true;
                break;
            }
            let _ = node.wait(Duration::from_millis(5));
        }
        assert_that!(response_sent, eq true);

        let mut response_len = None;
        for _ in 0..20 {
            if let Some(response) = pending_response.receive().unwrap() {
                response_len = Some(response.payload().len());
                break;
            }
            let _ = node.wait(Duration::from_millis(5));
        }
        assert_that!(response_len, eq Some(1025));
    }

    #[test]
    fn blackboard_payload_read_write_works() {
        if !hugepages_environment_is_available() {
            return;
        }

        let _watchdog = Watchdog::new();
        let config = hugepages_test_config();
        let service_name = generate_service_name();
        let node = NodeBuilder::new()
            .config(&config)
            .create::<ipc_hugepages::Service>()
            .unwrap();
        let service = node
            .service_builder(&service_name)
            .blackboard_creator::<u64>()
            .add::<i32>(1, 0)
            .create()
            .unwrap();

        let writer = service.writer_builder().create().unwrap();
        let writer_handle = writer.entry::<i32>(&1).unwrap();
        writer_handle.update_with_copy(4242);

        let reader = service.reader_builder().create().unwrap();
        let reader_handle = reader.entry::<i32>(&1).unwrap();
        let current_value = reader_handle.get();

        assert_that!(*current_value, eq 4242);
    }
}
