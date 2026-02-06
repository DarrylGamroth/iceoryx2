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

use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::pipeline::WorkerCreateError;
use iceoryx2::testing::*;
use iceoryx2_bb_testing::assert_that;
use iceoryx2_bb_testing::watchdog::Watchdog;

#[test]
fn fixed_payload_pipeline_flow_works() {
    let _watchdog = Watchdog::new();
    type ServiceType = ipc::Service;

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ServiceType>()
        .unwrap();

    let pipeline = node
        .service_builder(&service_name)
        .pipeline::<u64>()
        .number_of_stages(2)
        .max_in_flight_samples(8)
        .create()
        .unwrap();

    let opened_pipeline = node
        .service_builder(&service_name)
        .pipeline::<u64>()
        .number_of_stages(2)
        .max_in_flight_samples(8)
        .open()
        .unwrap();

    assert_that!(pipeline.number_of_stages(), eq 2);
    assert_that!(opened_pipeline.number_of_stages(), eq 2);

    let ingress = pipeline.ingress_builder().create().unwrap();
    let worker_0 = pipeline.worker_builder(0).create().unwrap();
    let worker_1 = pipeline.worker_builder(1).create().unwrap();
    let egress = pipeline.egress_builder().create().unwrap();

    assert_that!(pipeline.number_of_ingress_ports(), eq 1);
    assert_that!(pipeline.number_of_workers(0), eq Some(1));
    assert_that!(pipeline.number_of_workers(1), eq Some(1));
    assert_that!(pipeline.number_of_workers(2), eq None);
    assert_that!(pipeline.number_of_egress_ports(), eq 1);

    ingress.send_copy(11).unwrap();

    let mut stage_0_work = None;
    for _ in 0..10_000 {
        stage_0_work = worker_0.receive().unwrap();
        if stage_0_work.is_some() {
            break;
        }
    }

    let mut stage_0_work = stage_0_work.expect("stage 0 must receive sample");
    *stage_0_work.payload_mut() += 1;
    stage_0_work.send().unwrap();

    let mut stage_1_work = None;
    for _ in 0..10_000 {
        stage_1_work = worker_1.receive().unwrap();
        if stage_1_work.is_some() {
            break;
        }
    }

    let mut stage_1_work = stage_1_work.expect("stage 1 must receive sample");
    *stage_1_work.payload_mut() *= 2;
    stage_1_work.send().unwrap();

    let mut final_sample = None;
    for _ in 0..10_000 {
        final_sample = egress.receive().unwrap();
        if final_sample.is_some() {
            break;
        }
    }

    let final_sample = final_sample.expect("egress must receive sample");
    assert_that!(*final_sample, eq 24);
}

#[test]
fn dynamic_payload_pipeline_flow_works() {
    let _watchdog = Watchdog::new();
    type ServiceType = ipc::Service;

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ServiceType>()
        .unwrap();

    let pipeline = node
        .service_builder(&service_name)
        .pipeline::<[u8]>()
        .number_of_stages(1)
        .max_in_flight_samples(8)
        .initial_max_slice_len(16)
        .create()
        .unwrap();

    let ingress = pipeline
        .ingress_builder()
        .initial_max_slice_len(16)
        .create()
        .unwrap();
    let worker = pipeline
        .worker_builder(0)
        .initial_max_slice_len(16)
        .create()
        .unwrap();
    let egress = pipeline.egress_builder().create().unwrap();

    let sample = ingress.loan_slice_uninit(4).unwrap();
    let sample = sample.write_from_fn(|n| (n + 1) as u8);
    sample.send().unwrap();

    let mut work = None;
    for _ in 0..10_000 {
        work = worker.receive().unwrap();
        if work.is_some() {
            break;
        }
    }

    let mut work = work.expect("worker must receive sample");
    assert_that!(work.payload_mut(), eq [1, 2, 3, 4].as_slice());
    work.payload_mut()[0] = 99;
    work.send().unwrap();

    let mut final_sample = None;
    for _ in 0..10_000 {
        final_sample = egress.receive().unwrap();
        if final_sample.is_some() {
            break;
        }
    }

    let final_sample = final_sample.expect("egress must receive sample");
    assert_that!(final_sample.payload(), eq [99, 2, 3, 4].as_slice());
}

#[test]
fn creating_worker_with_invalid_stage_fails() {
    let _watchdog = Watchdog::new();
    type ServiceType = ipc::Service;

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ServiceType>()
        .unwrap();

    let pipeline = node
        .service_builder(&service_name)
        .pipeline::<u64>()
        .number_of_stages(1)
        .create()
        .unwrap();

    let result = pipeline.worker_builder(1).create();
    assert_that!(result, is_err);
    assert_that!(result.err().unwrap(), eq WorkerCreateError::StageOutOfBounds);
}

#[test]
fn worker_can_discard_sample() {
    let _watchdog = Watchdog::new();
    type ServiceType = ipc::Service;

    let service_name = generate_service_name();
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ServiceType>()
        .unwrap();

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

    ingress.send_copy(7).unwrap();

    let mut work = None;
    for _ in 0..10_000 {
        work = worker.receive().unwrap();
        if work.is_some() {
            break;
        }
    }

    let work = work.expect("worker must receive sample");
    work.discard();

    for _ in 0..100 {
        let sample = egress.receive().unwrap();
        assert_that!(sample, is_none);
    }

    ingress.send_copy(21).unwrap();
    let mut work = None;
    for _ in 0..10_000 {
        work = worker.receive().unwrap();
        if work.is_some() {
            break;
        }
    }

    let mut work = work.expect("worker must receive second sample");
    *work.payload_mut() += 1;
    work.send().unwrap();

    let mut final_sample = None;
    for _ in 0..10_000 {
        final_sample = egress.receive().unwrap();
        if final_sample.is_some() {
            break;
        }
    }

    let final_sample = final_sample.expect("egress must receive second sample");
    assert_that!(*final_sample, eq 22);
}
