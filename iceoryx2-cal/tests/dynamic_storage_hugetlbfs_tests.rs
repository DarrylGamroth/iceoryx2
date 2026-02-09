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
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_cal::dynamic_storage::*;
    use iceoryx2_cal::named_concept::*;
    use iceoryx2_cal::testing::*;

    #[derive(Debug)]
    struct TestData {}

    unsafe impl Send for TestData {}
    unsafe impl Sync for TestData {}

    #[test]
    fn create_fails_when_path_is_not_hugetlbfs() {
        type Sut = iceoryx2_cal::dynamic_storage::hugetlbfs::Storage<TestData>;
        let storage_name = generate_name();
        let config = generate_isolated_config::<Sut>();

        let result = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .create(TestData {});

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
}
