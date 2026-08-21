// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(non_camel_case_types)]

use crate::api::{
    AssertNonNullHandle, HandleToType, IOX2_OK, IntoCInt, ProgressivePortFactoryUnion,
    ServiceBuilderUnion, UserHeaderFfi, c_size_t, iox2_port_factory_progressive_pub_sub_h,
    iox2_port_factory_progressive_pub_sub_t, iox2_service_builder_progressive_pub_sub_h,
    iox2_service_builder_progressive_pub_sub_h_ref, iox2_service_type_e, iox2_type_detail_error_e,
    iox2_type_variant_e,
};
use crate::create_type_details;
use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::publish_subscribe::{
    Builder, ProgressiveBuilder, PublishSubscribeCreateError, PublishSubscribeOpenError,
    PublishSubscribeOpenOrCreateError,
};
use iceoryx2::service::port_factory::publish_subscribe::ProgressivePortFactory;

macro_rules! mutate_builder {
    ($handle:expr, $method:ident($value:expr)) => {{
        let builder_struct = &mut *$handle.as_type();
        match builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let nested = ManuallyDrop::take(&mut builder_struct.value.as_mut().ipc);
                let builder = ManuallyDrop::into_inner(nested.progressive_pub_sub);
                builder_struct.set(ServiceBuilderUnion::new_ipc_progressive_pub_sub(
                    builder.$method($value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let nested = ManuallyDrop::take(&mut builder_struct.value.as_mut().local);
                let builder = ManuallyDrop::into_inner(nested.progressive_pub_sub);
                builder_struct.set(ServiceBuilderUnion::new_local_progressive_pub_sub(
                    builder.$method($value),
                ));
            }
        }
    }};
}

/// Sets the application user-header ABI for a progressive service.
///
/// # Safety
///
/// `builder_handle` and `type_name_str` must be valid. `type_name_str` must point to
/// `type_name_len` UTF-8 bytes, and `size`/`alignment` must form a valid layout.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_user_header_type_details(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    type_variant: iox2_type_variant_e,
    type_name_str: *const c_char,
    type_name_len: c_size_t,
    size: c_size_t,
    alignment: c_size_t,
) -> c_int {
    builder_handle.assert_non_null();
    let details = match unsafe {
        create_type_details(type_variant, type_name_str, type_name_len, size, alignment)
    } {
        Ok(value) => value,
        Err(error) => return error,
    };
    unsafe {
        let builder_struct = &mut *builder_handle.as_type();
        match builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let nested = ManuallyDrop::take(&mut builder_struct.value.as_mut().ipc);
                let builder = ManuallyDrop::into_inner(nested.progressive_pub_sub);
                builder_struct.set(ServiceBuilderUnion::new_ipc_progressive_pub_sub(
                    builder.__internal_set_user_header_type_details(&details),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let nested = ManuallyDrop::take(&mut builder_struct.value.as_mut().local);
                let builder = ManuallyDrop::into_inner(nested.progressive_pub_sub);
                builder_struct.set(ServiceBuilderUnion::new_local_progressive_pub_sub(
                    builder.__internal_set_user_header_type_details(&details),
                ));
            }
        }
    }
    IOX2_OK
}

/// Sets the maximum number of nodes that may use the progressive service.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive service-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_max_nodes(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_builder!(builder_handle, max_nodes(value)) };
}

/// Sets the maximum number of progressive subscribers.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive service-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_max_subscribers(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_builder!(builder_handle, max_subscribers(value)) };
}

/// Sets the maximum queue capacity supported by each progressive subscriber.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive service-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_subscriber_max_buffer_size(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_builder!(builder_handle, subscriber_max_buffer_size(value)) };
}

/// Sets the maximum number of samples a progressive subscriber may borrow concurrently.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive service-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_subscriber_max_borrowed_samples(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_builder!(builder_handle, subscriber_max_borrowed_samples(value)) };
}

/// Requests a payload alignment. Progressive mode always enforces at least 128 bytes.
///
/// # Safety
///
/// `builder_handle` must be valid. `value` is validated before it is used as an alignment.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_set_payload_alignment(
    builder_handle: iox2_service_builder_progressive_pub_sub_h_ref,
    value: c_size_t,
) -> c_int {
    builder_handle.assert_non_null();
    let Some(alignment) = Alignment::new(value) else {
        return iox2_type_detail_error_e::INVALID_SIZE_OR_ALIGNMENT_VALUE as c_int;
    };
    unsafe { mutate_builder!(builder_handle, payload_alignment(alignment)) };
    IOX2_OK
}

unsafe fn open_create_impl<E: IntoCInt>(
    builder_handle: iox2_service_builder_progressive_pub_sub_h,
    factory_struct_ptr: *mut iox2_port_factory_progressive_pub_sub_t,
    factory_handle_ptr: *mut iox2_port_factory_progressive_pub_sub_h,
    ipc: impl FnOnce(
        ProgressiveBuilder<UserHeaderFfi, crate::IpcService>,
    ) -> Result<ProgressivePortFactory<crate::IpcService, UserHeaderFfi>, E>,
    local: impl FnOnce(
        ProgressiveBuilder<UserHeaderFfi, crate::LocalService>,
    ) -> Result<ProgressivePortFactory<crate::LocalService, UserHeaderFfi>, E>,
) -> c_int {
    builder_handle.assert_non_null();
    debug_assert!(!factory_handle_ptr.is_null());
    unsafe {
        *factory_handle_ptr = core::ptr::null_mut();
        let builder_struct = &mut *builder_handle.as_type();
        let service_type = builder_struct.service_type;
        let builder = builder_struct
            .take()
            .expect("valid progressive service builder");
        (builder_struct.deleter)(builder_struct);

        let result = match service_type {
            iox2_service_type_e::IPC => {
                let nested = ManuallyDrop::into_inner(builder.ipc);
                let builder: Builder<[u8], UserHeaderFfi, crate::IpcService> =
                    ManuallyDrop::into_inner(nested.progressive_pub_sub);
                ipc(builder.progressive()).map(ProgressivePortFactoryUnion::new_ipc)
            }
            iox2_service_type_e::LOCAL => {
                let nested = ManuallyDrop::into_inner(builder.local);
                let builder: Builder<[u8], UserHeaderFfi, crate::LocalService> =
                    ManuallyDrop::into_inner(nested.progressive_pub_sub);
                local(builder.progressive()).map(ProgressivePortFactoryUnion::new_local)
            }
        };

        match result {
            Ok(factory) => {
                let mut storage = factory_struct_ptr;
                fn no_op(_: *mut iox2_port_factory_progressive_pub_sub_t) {}
                let mut deleter: fn(*mut iox2_port_factory_progressive_pub_sub_t) = no_op;
                if storage.is_null() {
                    storage = iox2_port_factory_progressive_pub_sub_t::alloc();
                    deleter = iox2_port_factory_progressive_pub_sub_t::dealloc;
                }
                (*storage).init(service_type, factory, deleter);
                *factory_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Opens or creates an experimental progressive publish-subscribe service.
///
/// # Safety
///
/// `builder_handle` must be a valid owning handle and is consumed. `factory_handle_ptr`
/// must be writable. `factory_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_open_or_create(
    builder_handle: iox2_service_builder_progressive_pub_sub_h,
    factory_struct_ptr: *mut iox2_port_factory_progressive_pub_sub_t,
    factory_handle_ptr: *mut iox2_port_factory_progressive_pub_sub_h,
) -> c_int {
    unsafe {
        open_create_impl::<PublishSubscribeOpenOrCreateError>(
            builder_handle,
            factory_struct_ptr,
            factory_handle_ptr,
            ProgressiveBuilder::open_or_create,
            ProgressiveBuilder::open_or_create,
        )
    }
}

/// Opens an existing experimental progressive publish-subscribe service.
///
/// # Safety
///
/// `builder_handle` must be a valid owning handle and is consumed. `factory_handle_ptr`
/// must be writable. `factory_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_open(
    builder_handle: iox2_service_builder_progressive_pub_sub_h,
    factory_struct_ptr: *mut iox2_port_factory_progressive_pub_sub_t,
    factory_handle_ptr: *mut iox2_port_factory_progressive_pub_sub_h,
) -> c_int {
    unsafe {
        open_create_impl::<PublishSubscribeOpenError>(
            builder_handle,
            factory_struct_ptr,
            factory_handle_ptr,
            ProgressiveBuilder::open,
            ProgressiveBuilder::open,
        )
    }
}

/// Creates an experimental progressive publish-subscribe service.
///
/// # Safety
///
/// `builder_handle` must be a valid owning handle and is consumed. `factory_handle_ptr`
/// must be writable. `factory_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_progressive_pub_sub_create(
    builder_handle: iox2_service_builder_progressive_pub_sub_h,
    factory_struct_ptr: *mut iox2_port_factory_progressive_pub_sub_t,
    factory_handle_ptr: *mut iox2_port_factory_progressive_pub_sub_h,
) -> c_int {
    unsafe {
        open_create_impl::<PublishSubscribeCreateError>(
            builder_handle,
            factory_struct_ptr,
            factory_handle_ptr,
            ProgressiveBuilder::create,
            ProgressiveBuilder::create,
        )
    }
}
