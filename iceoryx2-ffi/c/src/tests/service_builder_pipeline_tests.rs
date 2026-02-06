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
    use core::ffi::c_void;

    #[derive(Default)]
    struct DynamicListCtx {
        ingresses: usize,
        workers: usize,
        egresses: usize,
    }

    extern "C" fn list_ingresses_callback(
        ctx: iox2_callback_context,
        _details: iox2_publisher_details_ptr,
    ) -> iox2_callback_progression_e {
        let ctx = unsafe { &mut *(ctx as *mut DynamicListCtx) };
        ctx.ingresses += 1;
        iox2_callback_progression_e::CONTINUE
    }

    extern "C" fn list_workers_callback(
        ctx: iox2_callback_context,
        _details: iox2_subscriber_details_ptr,
    ) -> iox2_callback_progression_e {
        let ctx = unsafe { &mut *(ctx as *mut DynamicListCtx) };
        ctx.workers += 1;
        iox2_callback_progression_e::CONTINUE
    }

    extern "C" fn list_egresses_callback(
        ctx: iox2_callback_context,
        _details: iox2_subscriber_details_ptr,
    ) -> iox2_callback_progression_e {
        let ctx = unsafe { &mut *(ctx as *mut DynamicListCtx) };
        ctx.egresses += 1;
        iox2_callback_progression_e::CONTINUE
    }

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

            let mut static_config =
                core::mem::MaybeUninit::<iox2_static_config_pipeline_t>::uninit();
            iox2_port_factory_pipeline_static_config(&pipeline_factory, static_config.as_mut_ptr());
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

    #[test]
    fn pipeline_open_with_incompatible_user_header_fails<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node_handle = create_node::<S>("pipeline-user-header-mismatch-node");
            let service_name = "all/glory/to/pipeline-user-header-mismatch";

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

            let payload_name = "u64";
            let ret_val = iox2_service_builder_pipeline_set_payload_type_details(
                &service_builder_handle,
                iox2_type_variant_e::FIXED_SIZE,
                payload_name.as_ptr().cast(),
                payload_name.len(),
                core::mem::size_of::<u64>(),
                core::mem::align_of::<u64>(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let header_a_name = "header_a";
            let ret_val = iox2_service_builder_pipeline_set_user_header_type_details(
                &service_builder_handle,
                iox2_type_variant_e::FIXED_SIZE,
                header_a_name.as_ptr().cast(),
                header_a_name.len(),
                core::mem::size_of::<u64>(),
                core::mem::align_of::<u64>(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut pipeline_factory: iox2_port_factory_pipeline_h = core::ptr::null_mut();
            let ret_val = iox2_service_builder_pipeline_create(
                service_builder_handle,
                core::ptr::null_mut(),
                &mut pipeline_factory as *mut _,
            );
            assert_that!(ret_val, eq(IOX2_OK));

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
            let payload_name = "u64";
            let ret_val = iox2_service_builder_pipeline_set_payload_type_details(
                &service_builder_handle,
                iox2_type_variant_e::FIXED_SIZE,
                payload_name.as_ptr().cast(),
                payload_name.len(),
                core::mem::size_of::<u64>(),
                core::mem::align_of::<u64>(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let header_b_name = "header_b";
            let ret_val = iox2_service_builder_pipeline_set_user_header_type_details(
                &service_builder_handle,
                iox2_type_variant_e::FIXED_SIZE,
                header_b_name.as_ptr().cast(),
                header_b_name.len(),
                core::mem::size_of::<u32>(),
                core::mem::align_of::<u32>(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut opened_factory: iox2_port_factory_pipeline_h = core::ptr::null_mut();
            let ret_val = iox2_service_builder_pipeline_open(
                service_builder_handle,
                core::ptr::null_mut(),
                &mut opened_factory as *mut _,
            );
            assert_that!(
                ret_val,
                eq iox2_pipeline_open_or_create_error_e::O_INCOMPATIBLE_USER_HEADER_TYPE as i32
            );

            iox2_port_factory_pipeline_drop(pipeline_factory);
            iox2_node_drop(node_handle);
        }
    }

    #[test]
    fn pipeline_runtime_role_builders_and_dynamic_lists_work<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node_handle = create_node::<S>("pipeline-runtime-builders-node");
            let service_name = "all/glory/to/pipeline-runtime-builders";

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
            iox2_service_builder_pipeline_set_number_of_stages(&service_builder_handle, 1);
            iox2_service_builder_pipeline_set_max_in_flight_samples(&service_builder_handle, 8);
            let payload_name = "u64";
            let ret_val = iox2_service_builder_pipeline_set_payload_type_details(
                &service_builder_handle,
                iox2_type_variant_e::FIXED_SIZE,
                payload_name.as_ptr().cast(),
                payload_name.len(),
                core::mem::size_of::<u64>(),
                core::mem::align_of::<u64>(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut pipeline_factory: iox2_port_factory_pipeline_h = core::ptr::null_mut();
            let ret_val = iox2_service_builder_pipeline_open_or_create(
                service_builder_handle,
                core::ptr::null_mut(),
                &mut pipeline_factory as *mut _,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let ingress_builder = iox2_port_factory_pipeline_ingress_builder(
                &pipeline_factory,
                core::ptr::null_mut(),
            );
            assert_that!(ingress_builder.is_null(), eq false);
            let mut ingress: iox2_publisher_h = core::ptr::null_mut();
            let ret_val = iox2_port_factory_publisher_builder_create(
                ingress_builder,
                core::ptr::null_mut(),
                &mut ingress,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut has_value = false;
            let worker_subscriber_builder = iox2_port_factory_pipeline_worker_subscriber_builder(
                &pipeline_factory,
                0,
                core::ptr::null_mut(),
                &mut has_value,
            );
            assert_that!(has_value, eq true);
            assert_that!(worker_subscriber_builder.is_null(), eq false);
            let mut worker_subscriber: iox2_subscriber_h = core::ptr::null_mut();
            let ret_val = iox2_port_factory_subscriber_builder_create(
                worker_subscriber_builder,
                core::ptr::null_mut(),
                &mut worker_subscriber,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut has_value = false;
            let worker_publisher_builder = iox2_port_factory_pipeline_worker_publisher_builder(
                &pipeline_factory,
                0,
                core::ptr::null_mut(),
                &mut has_value,
            );
            assert_that!(has_value, eq true);
            assert_that!(worker_publisher_builder.is_null(), eq false);
            let mut worker_publisher: iox2_publisher_h = core::ptr::null_mut();
            let ret_val = iox2_port_factory_publisher_builder_create(
                worker_publisher_builder,
                core::ptr::null_mut(),
                &mut worker_publisher,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let egress_builder =
                iox2_port_factory_pipeline_egress_builder(&pipeline_factory, core::ptr::null_mut());
            assert_that!(egress_builder.is_null(), eq false);
            let mut egress: iox2_subscriber_h = core::ptr::null_mut();
            let ret_val = iox2_port_factory_subscriber_builder_create(
                egress_builder,
                core::ptr::null_mut(),
                &mut egress,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let ret_val = iox2_publisher_update_connections(&ingress);
            assert_that!(ret_val, eq(IOX2_OK));
            let ret_val = iox2_publisher_update_connections(&worker_publisher);
            assert_that!(ret_val, eq(IOX2_OK));

            assert_that!(
                iox2_port_factory_pipeline_dynamic_config_number_of_ingress_ports(&pipeline_factory),
                eq 1
            );
            let mut has_value = false;
            assert_that!(
                iox2_port_factory_pipeline_dynamic_config_number_of_workers(
                    &pipeline_factory,
                    0,
                    &mut has_value
                ),
                eq 1
            );
            assert_that!(has_value, eq true);
            assert_that!(
                iox2_port_factory_pipeline_dynamic_config_number_of_egress_ports(&pipeline_factory),
                eq 1
            );

            let mut has_value = false;
            let invalid_worker_subscriber_builder =
                iox2_port_factory_pipeline_worker_subscriber_builder(
                    &pipeline_factory,
                    2,
                    core::ptr::null_mut(),
                    &mut has_value,
                );
            assert_that!(has_value, eq false);
            assert_that!(invalid_worker_subscriber_builder.is_null(), eq true);

            let mut has_value = false;
            let invalid_worker_publisher_builder =
                iox2_port_factory_pipeline_worker_publisher_builder(
                    &pipeline_factory,
                    2,
                    core::ptr::null_mut(),
                    &mut has_value,
                );
            assert_that!(has_value, eq false);
            assert_that!(invalid_worker_publisher_builder.is_null(), eq true);

            let mut list_ctx = DynamicListCtx::default();
            iox2_port_factory_pipeline_dynamic_config_list_ingresses(
                &pipeline_factory,
                list_ingresses_callback,
                &mut list_ctx as *mut _ as *mut c_void,
            );
            iox2_port_factory_pipeline_dynamic_config_list_workers(
                &pipeline_factory,
                0,
                list_workers_callback,
                &mut list_ctx as *mut _ as *mut c_void,
            );
            iox2_port_factory_pipeline_dynamic_config_list_egresses(
                &pipeline_factory,
                list_egresses_callback,
                &mut list_ctx as *mut _ as *mut c_void,
            );
            assert_that!(list_ctx.ingresses, eq 1);
            assert_that!(list_ctx.workers, eq 1);
            assert_that!(list_ctx.egresses, eq 1);

            let ingress_payload = 11_u64;
            let ret_val = iox2_publisher_send_copy(
                &ingress,
                (&ingress_payload as *const u64).cast(),
                core::mem::size_of::<u64>(),
                core::ptr::null_mut(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut worker_input_sample: iox2_sample_h = core::ptr::null_mut();
            let ret_val = iox2_subscriber_receive(
                &worker_subscriber,
                core::ptr::null_mut(),
                &mut worker_input_sample,
            );
            assert_that!(ret_val, eq(IOX2_OK));
            assert_that!(worker_input_sample.is_null(), eq false);
            iox2_sample_drop(worker_input_sample);

            let worker_output_payload = 22_u64;
            let ret_val = iox2_publisher_send_copy(
                &worker_publisher,
                (&worker_output_payload as *const u64).cast(),
                core::mem::size_of::<u64>(),
                core::ptr::null_mut(),
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let mut egress_sample: iox2_sample_h = core::ptr::null_mut();
            let ret_val =
                iox2_subscriber_receive(&egress, core::ptr::null_mut(), &mut egress_sample);
            assert_that!(ret_val, eq(IOX2_OK));
            assert_that!(egress_sample.is_null(), eq false);

            let mut payload_ptr: *const c_void = core::ptr::null();
            iox2_sample_payload(&egress_sample, &mut payload_ptr, core::ptr::null_mut());
            let payload = *(payload_ptr as *const u64);
            assert_that!(payload, eq worker_output_payload);
            iox2_sample_drop(egress_sample);

            iox2_subscriber_drop(egress);
            iox2_publisher_drop(worker_publisher);
            iox2_subscriber_drop(worker_subscriber);
            iox2_publisher_drop(ingress);
            iox2_port_factory_pipeline_drop(pipeline_factory);
            iox2_node_drop(node_handle);
        }
    }

    #[instantiate_tests(<iceoryx2::service::ipc::Service>)]
    mod ipc {}

    #[instantiate_tests(<iceoryx2::service::local::Service>)]
    mod local {}
}
