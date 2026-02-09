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

mod dynamic_storage_hugetlbfs_tests {
    use iceoryx2_bb_container::semantic_string::SemanticString;
    use iceoryx2_bb_system_types::path::Path;
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_cal::dynamic_storage::*;
    use iceoryx2_cal::named_concept::*;
    use iceoryx2_cal::testing::*;
    use std::fs::OpenOptions;
    use std::path::Path as StdPath;

    #[derive(Debug)]
    struct TestData {
        value: u64,
    }

    unsafe impl Send for TestData {}
    unsafe impl Sync for TestData {}

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
        let probe = format!("{path}/iox2-hugetlbfs-test-probe-{}", std::process::id());
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

    #[test]
    fn create_fails_when_path_is_not_hugetlbfs() {
        type Sut = iceoryx2_cal::dynamic_storage::hugetlbfs::Storage<TestData>;
        let storage_name = generate_name();
        let config = generate_isolated_config::<Sut>();

        let result = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .create(TestData { value: 0 });

        assert_that!(result, is_err);
        assert_that!(result.err().unwrap(), eq DynamicStorageCreateError::InternalError);
    }

    #[test]
    fn open_reports_does_not_exist_before_hugetlb_validation_failure() {
        type Sut = iceoryx2_cal::dynamic_storage::hugetlbfs::Storage<TestData>;
        let storage_name = generate_name();
        let config = generate_isolated_config::<Sut>();

        let result = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .open();

        assert_that!(result, is_err);
        assert_that!(result.err().unwrap(), eq DynamicStorageOpenError::InternalError);
    }

    #[test]
    fn create_open_remove_succeeds_on_dev_hugepages_when_available() {
        type Sut = iceoryx2_cal::dynamic_storage::hugetlbfs::Storage<TestData>;
        const MOUNT: &str = "/dev/hugepages";
        if !StdPath::new(MOUNT).exists()
            || !has_hugetlbfs_mount(MOUNT)
            || !can_create_file_in_hugepage_mount(MOUNT)
        {
            return;
        }

        let storage_name = generate_name();
        let config =
            generate_isolated_config::<Sut>().path_hint(&Path::new(MOUNT.as_bytes()).unwrap());

        let created = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .create(TestData { value: 0xfeed_beef });
        assert_that!(created, is_ok);

        let created = created.unwrap();
        let opened = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .open();
        assert_that!(opened, is_ok);

        let opened = opened.unwrap();
        assert_that!(opened.get().value, eq 0xfeed_beef);
        drop(opened);
        drop(created);

        let exists = <Sut as NamedConceptMgmt>::does_exist_cfg(&storage_name, &config);
        assert_that!(exists, is_ok);
        assert_that!(exists.unwrap(), eq false);
    }
}
