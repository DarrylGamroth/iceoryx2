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

mod service_hugepages_config_tests {
    use iceoryx2::config::Config;
    use iceoryx2::node::{NodeBuilder, NodeCreationFailure};
    use iceoryx2::service::Service;
    use iceoryx2::service::{ipc, ipc_hugepages, ipc_hugepages_threadsafe};
    use iceoryx2::testing::generate_isolated_config;
    use iceoryx2_bb_container::semantic_string::SemanticString;
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_cal::named_concept::NamedConceptConfiguration;

    #[test]
    fn ipc_hugepages_uses_hugetlbfs_mount_path_and_size_override_for_data_segment() {
        let mut config = Config::default();
        config.global.service.hugepages.mount_path =
            iceoryx2_bb_system_types::path::Path::new(b"/dev/hugepages/iox2").unwrap();
        config.global.service.hugepages.hugepage_size_bytes = Some(2 * 1024 * 1024);

        let data_segment_config = <ipc_hugepages::Service as Service>::data_segment_config(&config);

        assert_that!(
            *data_segment_config.get_path_hint(),
            eq config.global.service.hugepages.mount_path
        );
        assert_that!(
            data_segment_config
                .dynamic_storage_config()
                .get_hugepage_size_bytes(),
            eq Some(2 * 1024 * 1024)
        );
    }

    #[test]
    fn ipc_hugepages_threadsafe_uses_same_hugepage_payload_config() {
        let mut config = Config::default();
        config.global.service.hugepages.mount_path =
            iceoryx2_bb_system_types::path::Path::new(b"/dev/hugepages/threadsafe").unwrap();
        config.global.service.hugepages.hugepage_size_bytes = Some(1024 * 1024 * 1024);

        let blackboard_payload_config =
            <ipc_hugepages_threadsafe::Service as Service>::blackboard_payload_config(&config);

        assert_that!(
            *blackboard_payload_config.get_path_hint(),
            eq config.global.service.hugepages.mount_path
        );
        assert_that!(
            blackboard_payload_config
                .dynamic_storage_config()
                .get_hugepage_size_bytes(),
            eq Some(1024 * 1024 * 1024)
        );
    }

    #[test]
    fn regular_ipc_keeps_root_path_for_data_segment() {
        let mut config = Config::default();
        config
            .global
            .set_root_path(&iceoryx2_bb_system_types::path::Path::new(b"/tmp/iox2-root").unwrap());
        config.global.service.hugepages.mount_path =
            iceoryx2_bb_system_types::path::Path::new(b"/dev/hugepages/ignored").unwrap();

        let data_segment_config = <ipc::Service as Service>::data_segment_config(&config);

        assert_that!(
            *data_segment_config.get_path_hint(),
            eq * config.global.root_path()
        );
    }

    #[test]
    fn node_creation_with_hugepages_service_fails_with_non_hugetlbfs_mount() {
        let mut config = generate_isolated_config();
        config.global.service.hugepages.mount_path =
            iceoryx2_bb_system_types::path::Path::new(b"/tmp").unwrap();

        let result = NodeBuilder::new()
            .config(&config)
            .create::<ipc_hugepages::Service>();
        assert_that!(result, is_err);
        assert_that!(result.err().unwrap(), eq NodeCreationFailure::InternalError);
    }
}
