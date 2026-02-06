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

#[generic_tests::define]
mod service_builder {
    use crate::tests::*;

    #[test]
    fn basic_service_builder_pipeline_test<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node_handle = create_node::<S>("pipeline-node");

            let service_name = "all/glory/to/pipeline";

            let mut service_name_handle: iox2_service_name_h = core::ptr::null_mut();
            let ret_val = iox2_service_name_new(
                core::ptr::null_mut(),
                service_name.as_ptr() as *const _,
                service_name.len(),
                &mut service_name_handle,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let service_builder_handle = iox2_node_service_builder(
                &node_handle,
                core::ptr::null_mut(),
                iox2_cast_service_name_ptr(service_name_handle),
            );
            iox2_service_name_drop(service_name_handle);

            let service_builder_handle = iox2_service_builder_pipeline(service_builder_handle);
            iox2_service_builder_pipeline_set_number_of_stages(&service_builder_handle, 2);
            iox2_service_builder_pipeline_set_max_in_flight_samples(&service_builder_handle, 16);
            iox2_service_builder_pipeline_set_max_nodes(&service_builder_handle, 10);
            iox2_service_builder_pipeline_set_initial_max_slice_len(&service_builder_handle, 32);

            let mut pipeline_factory: iox2_port_factory_pipeline_h = core::ptr::null_mut();
            let ret_val = iox2_service_builder_pipeline_open_or_create(
                service_builder_handle,
                core::ptr::null_mut(),
                &mut pipeline_factory as *mut _,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut static_config = core::mem::MaybeUninit::<iox2_static_config_pipeline_t>::uninit();
            iox2_port_factory_pipeline_static_config(
                &pipeline_factory,
                static_config.as_mut_ptr(),
            );
            let static_config = static_config.assume_init();

            assert_that!(static_config.number_of_stages, eq 2);
            assert_that!(static_config.max_in_flight_samples, eq 16);
            assert_that!(iox2_port_factory_pipeline_number_of_stages(&pipeline_factory), eq 2);

            let mut has_value = false;
            let workers = iox2_port_factory_pipeline_dynamic_config_number_of_workers(
                &pipeline_factory,
                0,
                &mut has_value,
            );
            assert_that!(has_value, eq true);
            assert_that!(workers, eq 0);

            let mut has_value = false;
            let workers = iox2_port_factory_pipeline_dynamic_config_number_of_workers(
                &pipeline_factory,
                3,
                &mut has_value,
            );
            assert_that!(has_value, eq false);
            assert_that!(workers, eq 0);

            iox2_port_factory_pipeline_drop(pipeline_factory);
            iox2_node_drop(node_handle);
        }
    }

    #[instantiate_tests(<iceoryx2::service::ipc::Service>)]
    mod ipc {}

    #[instantiate_tests(<iceoryx2::service::local::Service>)]
    mod local {}
}
