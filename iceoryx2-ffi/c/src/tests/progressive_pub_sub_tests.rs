// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

#[generic_tests::define]
mod progressive_pub_sub {
    use crate::api::*;
    use crate::tests::ServiceTypeMapping;
    use iceoryx2::prelude::*;
    use iceoryx2::testing::generate_service_name;
    use iceoryx2_bb_testing::assert_that;

    extern "C" fn follow_backpressure_strategy(
        _: iox2_backpressure_info_h_ref,
        _: iox2_callback_context,
    ) -> iox2_backpressure_action_e {
        iox2_backpressure_action_e::FOLLOW_BACKPRESSUREY_STRATEGY
    }

    unsafe fn create_node<S: Service + ServiceTypeMapping>(node_name: &str) -> iox2_node_h {
        unsafe {
            let builder = iox2_node_builder_new(core::ptr::null_mut());
            let mut config = core::ptr::null_mut();
            assert_that!(
                iox2_config_default(core::ptr::null_mut(), &mut config),
                eq(IOX2_OK)
            );
            iox2_node_builder_set_config(&builder, &config);
            iox2_config_drop(config);

            let mut name = core::ptr::null_mut();
            assert_that!(
                iox2_node_name_new(
                    core::ptr::null_mut(),
                    node_name.as_ptr().cast(),
                    node_name.len(),
                    &mut name,
                ),
                eq(IOX2_OK)
            );
            iox2_node_builder_set_name(&builder, iox2_cast_node_name_ptr(name));
            iox2_node_name_drop(name);

            let mut node = core::ptr::null_mut();
            assert_that!(
                iox2_node_builder_create(
                    builder,
                    core::ptr::null_mut(),
                    S::service_type(),
                    &mut node,
                ),
                eq(IOX2_OK)
            );
            node
        }
    }

    unsafe fn create_progressive_service<S: Service + ServiceTypeMapping>(
        node: iox2_node_h_ref,
    ) -> iox2_port_factory_progressive_pub_sub_h {
        let service_name = generate_service_name().to_string();
        let mut service_name_handle = core::ptr::null_mut();
        assert_that!(
            unsafe {
                iox2_service_name_new(
                    core::ptr::null_mut(),
                    service_name.as_ptr().cast(),
                    service_name.len(),
                    &mut service_name_handle,
                )
            },
            eq(IOX2_OK)
        );
        let base = unsafe {
            iox2_node_service_builder(
                node,
                core::ptr::null_mut(),
                iox2_cast_service_name_ptr(service_name_handle),
            )
        };
        unsafe { iox2_service_name_drop(service_name_handle) };
        let builder = unsafe { iox2_service_builder_progressive_pub_sub(base) };

        const HEADER_NAME: &str = "FrameInfo";
        assert_that!(
            unsafe {
                iox2_service_builder_progressive_pub_sub_set_user_header_type_details(
                    &builder,
                    iox2_type_variant_e::FIXED_SIZE,
                    HEADER_NAME.as_ptr().cast(),
                    HEADER_NAME.len(),
                    size_of::<u64>(),
                    align_of::<u64>(),
                )
            },
            eq(IOX2_OK)
        );
        assert_that!(
            unsafe {
                iox2_service_builder_progressive_pub_sub_set_payload_alignment(&builder, 4096)
            },
            eq(IOX2_OK)
        );
        unsafe {
            iox2_service_builder_progressive_pub_sub_set_max_subscribers(&builder, 2);
            iox2_service_builder_progressive_pub_sub_set_subscriber_max_buffer_size(&builder, 2);
            iox2_service_builder_progressive_pub_sub_set_subscriber_max_borrowed_samples(
                &builder, 2,
            );
        }

        let mut factory = core::ptr::null_mut();
        assert_that!(
            unsafe {
                iox2_service_builder_progressive_pub_sub_open_or_create(
                    builder,
                    core::ptr::null_mut(),
                    &mut factory,
                )
            },
            eq(IOX2_OK)
        );
        factory
    }

    unsafe fn create_ports(
        factory: iox2_port_factory_progressive_pub_sub_h_ref,
    ) -> (iox2_progressive_publisher_h, iox2_progressive_subscriber_h) {
        let subscriber_builder = unsafe {
            iox2_port_factory_progressive_pub_sub_subscriber_builder(factory, core::ptr::null_mut())
        };
        unsafe {
            iox2_port_factory_progressive_subscriber_builder_set_buffer_size(&subscriber_builder, 2)
        };
        let mut subscriber = core::ptr::null_mut();
        assert_that!(
            unsafe {
                iox2_port_factory_progressive_subscriber_builder_create(
                    subscriber_builder,
                    core::ptr::null_mut(),
                    &mut subscriber,
                )
            },
            eq(IOX2_OK)
        );

        let publisher_builder = unsafe {
            iox2_port_factory_progressive_pub_sub_publisher_builder(factory, core::ptr::null_mut())
        };
        unsafe {
            iox2_port_factory_progressive_publisher_builder_set_initial_max_slice_len(
                &publisher_builder,
                32,
            );
            iox2_port_factory_progressive_publisher_builder_set_max_loaned_samples(
                &publisher_builder,
                2,
            );
            iox2_port_factory_progressive_publisher_builder_set_allocation_strategy(
                &publisher_builder,
                iox2_allocation_strategy_e::STATIC,
            );
            iox2_port_factory_progressive_publisher_builder_set_backpressure_handler(
                &publisher_builder,
                follow_backpressure_strategy,
                core::ptr::null_mut(),
            );
        }
        let mut publisher = core::ptr::null_mut();
        assert_that!(
            unsafe {
                iox2_port_factory_progressive_publisher_builder_create(
                    publisher_builder,
                    core::ptr::null_mut(),
                    &mut publisher,
                )
            },
            eq(IOX2_OK)
        );

        (publisher, subscriber)
    }

    unsafe fn receive(subscriber: iox2_progressive_subscriber_h_ref) -> iox2_progressive_sample_h {
        let mut sample = core::ptr::null_mut();
        assert_that!(
            unsafe {
                iox2_progressive_subscriber_receive(subscriber, core::ptr::null_mut(), &mut sample)
            },
            eq(IOX2_OK)
        );
        assert_that!(sample, ne(core::ptr::null_mut()));
        sample
    }

    #[test]
    fn progressive_c_ffi_preserves_prefix_and_access_authority<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node = create_node::<S>("progressive-c-ffi");
            let factory = create_progressive_service::<S>(&node);
            let (publisher, subscriber) = create_ports(&factory);

            let mut empty = core::ptr::null_mut();
            let mut has_samples = true;
            assert_that!(
                iox2_progressive_subscriber_has_samples(&subscriber, &mut has_samples),
                eq(IOX2_OK)
            );
            assert_that!(has_samples, eq(false));
            assert_that!(
                iox2_progressive_subscriber_receive(&subscriber, core::ptr::null_mut(), &mut empty,),
                eq(IOX2_OK)
            );
            assert_that!(empty, eq(core::ptr::null_mut()));

            let mut private_loan = core::ptr::null_mut();
            assert_that!(
                iox2_progressive_publisher_loan_slice_uninit(
                    &publisher,
                    core::ptr::null_mut(),
                    &mut private_loan,
                    32,
                ),
                eq(IOX2_OK)
            );
            let mut payload = core::ptr::null_mut();
            let mut capacity = 0;
            iox2_progressive_sample_mut_uninit_payload_mut(
                &private_loan,
                &mut payload,
                &mut capacity,
            );
            assert_that!(capacity, eq(32));
            assert_that!(payload as usize % 4096, eq(0));
            core::ptr::copy_nonoverlapping([1_u8, 2, 3, 4].as_ptr(), payload.cast(), 4);
            (iox2_progressive_sample_mut_uninit_user_header_mut(&private_loan) as *mut u64)
                .write(0xfeed_beef);

            let mut writer = core::ptr::null_mut();
            assert_that!(
                iox2_progressive_sample_mut_uninit_announce(
                    private_loan,
                    core::ptr::null_mut(),
                    &mut writer,
                ),
                eq(IOX2_OK)
            );
            assert_that!(
                iox2_progressive_sample_mut_commit_until(&writer, 4),
                eq(IOX2_OK)
            );
            assert_that!(
                iox2_progressive_sample_mut_payload_capacity(&writer),
                eq(32)
            );
            assert_that!(iox2_progressive_sample_mut_committed_len(&writer), eq(4));
            assert_that!(
                *(iox2_progressive_sample_mut_user_header(&writer) as *const u64),
                eq(0xfeed_beef)
            );
            assert_that!(
                iox2_progressive_subscriber_has_samples(&subscriber, &mut has_samples),
                eq(IOX2_OK)
            );
            assert_that!(has_samples, eq(true));

            let sample = receive(&subscriber);
            let mut prefix = core::ptr::null();
            let mut committed_len = 0;
            iox2_progressive_sample_payload(&sample, &mut prefix, &mut committed_len);
            assert_that!(committed_len, eq(4));
            assert_that!(
                core::slice::from_raw_parts(prefix, committed_len),
                eq(&[1, 2, 3, 4])
            );
            assert_that!(iox2_progressive_sample_payload_capacity(&sample), eq(32));
            assert_that!(
                *(iox2_progressive_sample_user_header(&sample) as *const u64),
                eq(0xfeed_beef)
            );
            assert_that!(
                iox2_progressive_sample_state(&sample),
                eq(iox2_progressive_sample_state_e::ACTIVE)
            );
            let mut snapshot = iox2_progressive_sample_snapshot_t {
                committed_len: 0,
                state: iox2_progressive_sample_state_e::ABORTED,
            };
            iox2_progressive_sample_snapshot(&sample, &mut snapshot);
            assert_that!(snapshot.committed_len, eq(4));
            assert_that!(snapshot.state, eq(iox2_progressive_sample_state_e::ACTIVE));
            let mut liveness_state = iox2_progressive_sample_state_e::ABORTED;
            assert_that!(
                iox2_progressive_sample_state_with_publisher_liveness(&sample, &mut liveness_state,),
                eq(IOX2_OK)
            );
            assert_that!(liveness_state, eq(iox2_progressive_sample_state_e::ACTIVE));
            assert_that!(
                iox2_progressive_sample_snapshot_with_publisher_liveness(&sample, &mut snapshot),
                eq(IOX2_OK)
            );
            assert_that!(snapshot.committed_len, eq(4));
            assert_that!(snapshot.state, eq(iox2_progressive_sample_state_e::ACTIVE));

            assert_that!(
                iox2_progressive_sample_mut_commit_until(&writer, 3),
                eq(iox2_progressive_write_error_e::COMMITTED_LENGTH_REGRESSED as i32)
            );
            core::ptr::copy_nonoverlapping([5_u8, 6].as_ptr(), payload.cast::<u8>().add(4), 2);
            assert_that!(
                iox2_progressive_sample_mut_commit_until(&writer, 6),
                eq(IOX2_OK)
            );
            iox2_progressive_sample_payload(&sample, &mut prefix, &mut committed_len);
            assert_that!(committed_len, eq(6));
            assert_that!(
                core::slice::from_raw_parts(prefix, committed_len),
                eq(&[1, 2, 3, 4, 5, 6])
            );
            assert_that!(
                iox2_progressive_sample_mut_write_from_slice(&writer, [7, 8].as_ptr(), 2),
                eq(IOX2_OK)
            );
            iox2_progressive_sample_payload(&sample, &mut prefix, &mut committed_len);
            assert_that!(committed_len, eq(8));
            assert_that!(
                core::slice::from_raw_parts(prefix, committed_len),
                eq(&[1, 2, 3, 4, 5, 6, 7, 8])
            );
            assert_that!(iox2_progressive_sample_mut_complete(writer), eq(IOX2_OK));
            iox2_progressive_sample_snapshot(&sample, &mut snapshot);
            assert_that!(snapshot.committed_len, eq(8));
            assert_that!(
                snapshot.state,
                eq(iox2_progressive_sample_state_e::COMPLETE)
            );
            iox2_progressive_sample_drop(sample);

            for explicit_abort in [true, false] {
                let mut loan = core::ptr::null_mut();
                assert_that!(
                    iox2_progressive_publisher_loan_slice_uninit(
                        &publisher,
                        core::ptr::null_mut(),
                        &mut loan,
                        8,
                    ),
                    eq(IOX2_OK)
                );
                let mut active = core::ptr::null_mut();
                assert_that!(
                    iox2_progressive_sample_mut_uninit_announce(
                        loan,
                        core::ptr::null_mut(),
                        &mut active,
                    ),
                    eq(IOX2_OK)
                );
                let received = receive(&subscriber);
                if explicit_abort {
                    assert_that!(iox2_progressive_sample_mut_abort(active), eq(IOX2_OK));
                } else {
                    iox2_progressive_sample_mut_drop(active);
                }
                assert_that!(
                    iox2_progressive_sample_state(&received),
                    eq(iox2_progressive_sample_state_e::ABORTED)
                );
                iox2_progressive_sample_drop(received);
            }

            iox2_progressive_publisher_drop(publisher);
            iox2_progressive_subscriber_drop(subscriber);
            iox2_port_factory_progressive_pub_sub_drop(factory);
            iox2_node_drop(node);
        }
    }

    #[instantiate_tests(<iceoryx2::service::ipc::Service>)]
    mod ipc {}

    #[instantiate_tests(<iceoryx2::service::local::Service>)]
    mod local {}
}
