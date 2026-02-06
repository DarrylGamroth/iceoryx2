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

use iceoryx2_bb_conformance_test_macros::conformance_test_module;

#[allow(clippy::module_inception)]
#[conformance_test_module]
pub mod service_pipeline {
    use iceoryx2::prelude::*;
    use iceoryx2::service::builder::pipeline::PipelineOpenError;
    use iceoryx2::service::port_factory::pipeline::WorkerCreateError;
    use iceoryx2::service::Service;
    use iceoryx2::testing;
    use iceoryx2_bb_conformance_test_macros::conformance_test;
    use iceoryx2_bb_derive_macros::ZeroCopySend;
    use iceoryx2_bb_elementary::CallbackProgression;
    use iceoryx2_bb_posix::unique_system_id::UniqueSystemId;
    use iceoryx2_bb_testing::assert_that;

    #[derive(Debug, Default, ZeroCopySend)]
    #[repr(C)]
    struct HeaderA {
        stage: u32,
    }

    #[derive(Debug, Default, ZeroCopySend)]
    #[repr(C)]
    struct HeaderB {
        stage: u64,
    }

    fn generate_name() -> ServiceName {
        ServiceName::new(&format!(
            "pipeline_service_tests_{}",
            UniqueSystemId::new().unwrap().value()
        ))
        .unwrap()
    }

    #[conformance_test]
    pub fn creating_non_existing_pipeline_works<Sut: Service>() {
        let service_name = generate_name();
        let config = testing::generate_isolated_config();
        let node = NodeBuilder::new().config(&config).create::<Sut>().unwrap();

        let pipeline = node
            .service_builder(&service_name)
            .pipeline::<u64>()
            .number_of_stages(1)
            .max_in_flight_samples(8)
            .create();

        assert_that!(pipeline, is_ok);
    }

    #[conformance_test]
    pub fn opening_existing_pipeline_with_wrong_user_header_fails<Sut: Service>() {
        let service_name = generate_name();
        let config = testing::generate_isolated_config();
        let node = NodeBuilder::new().config(&config).create::<Sut>().unwrap();

        let created = node
            .service_builder(&service_name)
            .pipeline::<u64>()
            .user_header::<HeaderA>()
            .create();
        assert_that!(created, is_ok);

        let opened = node
            .service_builder(&service_name)
            .pipeline::<u64>()
            .user_header::<HeaderB>()
            .open();

        assert_that!(opened, is_err);
        assert_that!(opened.err().unwrap(), eq PipelineOpenError::IncompatibleUserHeaderType);
    }

    #[conformance_test]
    pub fn runtime_role_builders_and_dynamic_lists_work<Sut: Service>() {
        let service_name = generate_name();
        let config = testing::generate_isolated_config();
        let node = NodeBuilder::new().config(&config).create::<Sut>().unwrap();

        let pipeline = node
            .service_builder(&service_name)
            .pipeline::<u64>()
            .number_of_stages(1)
            .max_in_flight_samples(8)
            .create()
            .unwrap();

        let ingress = pipeline.ingress_builder().create().unwrap();
        let worker = pipeline.worker_builder(0).create().unwrap();
        let egress = pipeline.egress_builder().create().unwrap();

        assert_that!(pipeline.number_of_ingress_ports(), eq 1);
        assert_that!(pipeline.number_of_workers(0), eq Some(1));
        assert_that!(pipeline.number_of_egress_ports(), eq 1);
        assert_that!(pipeline.number_of_workers(1), eq None);

        let mut ingress_count = 0;
        pipeline.list_ingresses(|_| {
            ingress_count += 1;
            CallbackProgression::Continue
        });
        assert_that!(ingress_count, eq 1);

        let mut worker_count = 0;
        pipeline.list_workers(0, |_| {
            worker_count += 1;
            CallbackProgression::Continue
        });
        assert_that!(worker_count, eq 1);

        let mut egress_count = 0;
        pipeline.list_egresses(|_| {
            egress_count += 1;
            CallbackProgression::Continue
        });
        assert_that!(egress_count, eq 1);

        ingress.send_copy(10).unwrap();

        let mut work = None;
        for _ in 0..10_000 {
            work = worker.receive().unwrap();
            if work.is_some() {
                break;
            }
        }
        let mut work = work.expect("worker must receive a sample");
        *work.payload_mut() += 1;
        work.send().unwrap();

        let mut final_sample = None;
        for _ in 0..10_000 {
            final_sample = egress.receive().unwrap();
            if final_sample.is_some() {
                break;
            }
        }

        let final_sample = final_sample.expect("egress must receive a sample");
        assert_that!(*final_sample, eq 11);
    }

    #[conformance_test]
    pub fn worker_stage_bounds_are_enforced<Sut: Service>() {
        let service_name = generate_name();
        let config = testing::generate_isolated_config();
        let node = NodeBuilder::new().config(&config).create::<Sut>().unwrap();

        let pipeline = node
            .service_builder(&service_name)
            .pipeline::<u64>()
            .number_of_stages(1)
            .create()
            .unwrap();

        let create_result = pipeline.worker_builder(1).create();
        assert_that!(create_result, is_err);
        assert_that!(
            create_result.err().unwrap(),
            eq WorkerCreateError::StageOutOfBounds
        );
        assert_that!(pipeline.number_of_workers(1), eq None);
    }
}
