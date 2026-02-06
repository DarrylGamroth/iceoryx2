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

#![allow(non_camel_case_types)]

use crate::{
    api::{
        iox2_callback_progression_e, iox2_port_factory_publisher_builder_h,
        iox2_port_factory_publisher_builder_t, iox2_port_factory_subscriber_builder_h,
        iox2_port_factory_subscriber_builder_t, iox2_service_type_e, iox2_static_config_pipeline_t,
        iox2_subscriber_details_ptr, AssertNonNullHandle, HandleToType, IntoCInt, PayloadFfi,
        PortFactoryPublisherBuilderUnion, PortFactorySubscriberBuilderUnion, UserHeaderFfi,
    },
    iox2_node_list_impl, IOX2_OK,
};

use iceoryx2::service::dynamic_config::publish_subscribe::{PublisherDetails, SubscriberDetails};
use iceoryx2::service::port_factory::pipeline::PortFactory;
use iceoryx2_bb_elementary::static_assert::*;
use iceoryx2_ffi_macros::iceoryx2_ffi;

use core::{
    ffi::{c_char, c_int},
    mem::ManuallyDrop,
};

use super::{
    iox2_attribute_set_ptr, iox2_callback_context, iox2_node_list_callback,
    iox2_publisher_details_ptr, iox2_service_name_ptr,
};

pub(super) union PortFactoryPipelineUnion {
    ipc: ManuallyDrop<PortFactory<crate::IpcService, PayloadFfi, UserHeaderFfi>>,
    local: ManuallyDrop<PortFactory<crate::LocalService, PayloadFfi, UserHeaderFfi>>,
}

impl PortFactoryPipelineUnion {
    pub(super) fn new_ipc(
        port_factory: PortFactory<crate::IpcService, PayloadFfi, UserHeaderFfi>,
    ) -> Self {
        Self {
            ipc: ManuallyDrop::new(port_factory),
        }
    }

    pub(super) fn new_local(
        port_factory: PortFactory<crate::LocalService, PayloadFfi, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(port_factory),
        }
    }
}

#[repr(C)]
#[repr(align(8))] // alignment of Option<PortFactoryPipelineUnion>
pub struct iox2_port_factory_pipeline_storage_t {
    internal: [u8; 4096], // large enough to store Option<PortFactoryPipelineUnion>
}

#[repr(C)]
#[iceoryx2_ffi(PortFactoryPipelineUnion)]
pub struct iox2_port_factory_pipeline_t {
    service_type: iox2_service_type_e,
    value: iox2_port_factory_pipeline_storage_t,
    deleter: fn(*mut iox2_port_factory_pipeline_t),
}

impl iox2_port_factory_pipeline_t {
    pub(super) fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: PortFactoryPipelineUnion,
        deleter: fn(*mut iox2_port_factory_pipeline_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_port_factory_pipeline_h_t;
/// The owning handle for `iox2_port_factory_pipeline_t`. Passing the handle to a function transfers the ownership.
pub type iox2_port_factory_pipeline_h = *mut iox2_port_factory_pipeline_h_t;
/// The non-owning handle for `iox2_port_factory_pipeline_t`. Passing the handle to a function does not transfer the ownership.
pub type iox2_port_factory_pipeline_h_ref = *const iox2_port_factory_pipeline_h;

impl AssertNonNullHandle for iox2_port_factory_pipeline_h {
    fn assert_non_null(self) {
        debug_assert!(!self.is_null());
    }
}

impl AssertNonNullHandle for iox2_port_factory_pipeline_h_ref {
    fn assert_non_null(self) {
        debug_assert!(!self.is_null());
        unsafe {
            debug_assert!(!(*self).is_null());
        }
    }
}

impl HandleToType for iox2_port_factory_pipeline_h {
    type Target = *mut iox2_port_factory_pipeline_t;

    fn as_type(self) -> Self::Target {
        self as *mut _ as _
    }
}

impl HandleToType for iox2_port_factory_pipeline_h_ref {
    type Target = *mut iox2_port_factory_pipeline_t;

    fn as_type(self) -> Self::Target {
        unsafe { *self as *mut _ as _ }
    }
}

/// Callback used by [`iox2_port_factory_pipeline_dynamic_config_list_ingresses`].
pub type iox2_list_pipeline_ingresses_callback =
    extern "C" fn(iox2_callback_context, iox2_publisher_details_ptr) -> iox2_callback_progression_e;

/// Callback used by [`iox2_port_factory_pipeline_dynamic_config_list_workers`].
pub type iox2_list_pipeline_workers_callback = extern "C" fn(
    iox2_callback_context,
    iox2_subscriber_details_ptr,
) -> iox2_callback_progression_e;

/// Callback used by [`iox2_port_factory_pipeline_dynamic_config_list_egresses`].
pub type iox2_list_pipeline_egresses_callback = extern "C" fn(
    iox2_callback_context,
    iox2_subscriber_details_ptr,
) -> iox2_callback_progression_e;

/// Returnes the service attributes.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_attributes(
    port_factory_handle: iox2_port_factory_pipeline_h_ref,
) -> iox2_attribute_set_ptr {
    use iceoryx2::prelude::PortFactory;

    port_factory_handle.assert_non_null();

    let port_factory = &mut *port_factory_handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.attributes(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.attributes(),
    }
}

/// Set the values in the provided [`iox2_static_config_pipeline_t`] pointer.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_static_config(
    port_factory_handle: iox2_port_factory_pipeline_h_ref,
    static_config: *mut iox2_static_config_pipeline_t,
) {
    port_factory_handle.assert_non_null();
    debug_assert!(!static_config.is_null());

    let port_factory = &mut *port_factory_handle.as_type();

    use iceoryx2::prelude::PortFactory;
    let config = match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.static_config(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.static_config(),
    };

    *static_config = config.into();
}

/// Returns the amount of worker stages.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_number_of_stages(
    handle: iox2_port_factory_pipeline_h_ref,
) -> usize {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.number_of_stages(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.number_of_stages(),
    }
}

/// Returns the current amount of ingress ports.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_number_of_ingress_ports(
    handle: iox2_port_factory_pipeline_h_ref,
) -> usize {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.number_of_ingress_ports(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.number_of_ingress_ports(),
    }
}

/// Returns the current amount of worker ports at `stage_id`.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_number_of_workers(
    handle: iox2_port_factory_pipeline_h_ref,
    stage_id: usize,
    has_value: *mut bool,
) -> usize {
    handle.assert_non_null();
    debug_assert!(!has_value.is_null());

    *has_value = false;

    let port_factory = &mut *handle.as_type();
    let value = match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.number_of_workers(stage_id),
        iox2_service_type_e::LOCAL => port_factory
            .value
            .as_ref()
            .local
            .number_of_workers(stage_id),
    };

    if let Some(v) = value {
        *has_value = true;
        v
    } else {
        0
    }
}

/// Returns the current amount of egress ports.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_number_of_egress_ports(
    handle: iox2_port_factory_pipeline_h_ref,
) -> usize {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.number_of_egress_ports(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.number_of_egress_ports(),
    }
}

/// Calls the callback repeatedly for every connected ingress endpoint and provides all details
/// with [`iox2_publisher_details_ptr`].
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_list_ingresses(
    handle: iox2_port_factory_pipeline_h_ref,
    callback: iox2_list_pipeline_ingresses_callback,
    callback_ctx: iox2_callback_context,
) {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    let callback_tr = |details: &PublisherDetails| callback(callback_ctx, details).into();

    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.list_ingresses(callback_tr),
        iox2_service_type_e::LOCAL => port_factory
            .value
            .as_ref()
            .local
            .list_ingresses(callback_tr),
    };
}

/// Calls the callback repeatedly for every connected worker endpoint at `stage_id` and provides
/// all details with [`iox2_subscriber_details_ptr`].
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_list_workers(
    handle: iox2_port_factory_pipeline_h_ref,
    stage_id: usize,
    callback: iox2_list_pipeline_workers_callback,
    callback_ctx: iox2_callback_context,
) {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    let mut callback_tr = |details: &SubscriberDetails| callback(callback_ctx, details).into();

    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory
            .value
            .as_ref()
            .ipc
            .list_workers(stage_id, &mut callback_tr),
        iox2_service_type_e::LOCAL => port_factory
            .value
            .as_ref()
            .local
            .list_workers(stage_id, &mut callback_tr),
    };
}

/// Calls the callback repeatedly for every connected egress endpoint and provides all details
/// with [`iox2_subscriber_details_ptr`].
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_dynamic_config_list_egresses(
    handle: iox2_port_factory_pipeline_h_ref,
    callback: iox2_list_pipeline_egresses_callback,
    callback_ctx: iox2_callback_context,
) {
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    let callback_tr = |details: &SubscriberDetails| callback(callback_ctx, details).into();

    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.list_egresses(callback_tr),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.list_egresses(callback_tr),
    };
}

/// Instantiates a [`iox2_port_factory_publisher_builder_h`] for ingress endpoints.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_ingress_builder(
    handle: iox2_port_factory_pipeline_h_ref,
    publisher_builder_struct_ptr: *mut iox2_port_factory_publisher_builder_t,
) -> iox2_port_factory_publisher_builder_h {
    handle.assert_non_null();

    let mut publisher_builder_struct_ptr = publisher_builder_struct_ptr;
    fn no_op(_: *mut iox2_port_factory_publisher_builder_t) {}
    let mut deleter: fn(*mut iox2_port_factory_publisher_builder_t) = no_op;
    if publisher_builder_struct_ptr.is_null() {
        publisher_builder_struct_ptr = iox2_port_factory_publisher_builder_t::alloc();
        deleter = iox2_port_factory_publisher_builder_t::dealloc;
    }
    debug_assert!(!publisher_builder_struct_ptr.is_null());

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => {
            let publisher_builder = port_factory
                .value
                .as_ref()
                .ipc
                .__internal_ingress_publisher_builder();
            (*publisher_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactoryPublisherBuilderUnion::new_ipc(publisher_builder),
                deleter,
            );
        }
        iox2_service_type_e::LOCAL => {
            let publisher_builder = port_factory
                .value
                .as_ref()
                .local
                .__internal_ingress_publisher_builder();
            (*publisher_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactoryPublisherBuilderUnion::new_local(publisher_builder),
                deleter,
            );
        }
    };

    (*publisher_builder_struct_ptr).as_handle()
}

/// Instantiates a [`iox2_port_factory_subscriber_builder_h`] for worker input at `stage_id`.
/// If `stage_id` is out of bounds it returns `NULL` and sets `*has_value` to `false`.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_worker_subscriber_builder(
    handle: iox2_port_factory_pipeline_h_ref,
    stage_id: usize,
    subscriber_builder_struct_ptr: *mut iox2_port_factory_subscriber_builder_t,
    has_value: *mut bool,
) -> iox2_port_factory_subscriber_builder_h {
    handle.assert_non_null();
    debug_assert!(!has_value.is_null());
    *has_value = false;

    let mut subscriber_builder_struct_ptr = subscriber_builder_struct_ptr;
    fn no_op(_: *mut iox2_port_factory_subscriber_builder_t) {}
    let mut deleter: fn(*mut iox2_port_factory_subscriber_builder_t) = no_op;
    if subscriber_builder_struct_ptr.is_null() {
        subscriber_builder_struct_ptr = iox2_port_factory_subscriber_builder_t::alloc();
        deleter = iox2_port_factory_subscriber_builder_t::dealloc;
    }
    debug_assert!(!subscriber_builder_struct_ptr.is_null());

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => {
            let subscriber_builder = port_factory
                .value
                .as_ref()
                .ipc
                .__internal_worker_subscriber_builder(stage_id);
            let Some(subscriber_builder) = subscriber_builder else {
                return core::ptr::null_mut();
            };

            *has_value = true;
            (*subscriber_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactorySubscriberBuilderUnion::new_ipc(subscriber_builder),
                deleter,
            );
        }
        iox2_service_type_e::LOCAL => {
            let subscriber_builder = port_factory
                .value
                .as_ref()
                .local
                .__internal_worker_subscriber_builder(stage_id);
            let Some(subscriber_builder) = subscriber_builder else {
                return core::ptr::null_mut();
            };

            *has_value = true;
            (*subscriber_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactorySubscriberBuilderUnion::new_local(subscriber_builder),
                deleter,
            );
        }
    };

    (*subscriber_builder_struct_ptr).as_handle()
}

/// Instantiates a [`iox2_port_factory_publisher_builder_h`] for worker output at `stage_id`.
/// If `stage_id` is out of bounds it returns `NULL` and sets `*has_value` to `false`.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_worker_publisher_builder(
    handle: iox2_port_factory_pipeline_h_ref,
    stage_id: usize,
    publisher_builder_struct_ptr: *mut iox2_port_factory_publisher_builder_t,
    has_value: *mut bool,
) -> iox2_port_factory_publisher_builder_h {
    handle.assert_non_null();
    debug_assert!(!has_value.is_null());
    *has_value = false;

    let mut publisher_builder_struct_ptr = publisher_builder_struct_ptr;
    fn no_op(_: *mut iox2_port_factory_publisher_builder_t) {}
    let mut deleter: fn(*mut iox2_port_factory_publisher_builder_t) = no_op;
    if publisher_builder_struct_ptr.is_null() {
        publisher_builder_struct_ptr = iox2_port_factory_publisher_builder_t::alloc();
        deleter = iox2_port_factory_publisher_builder_t::dealloc;
    }
    debug_assert!(!publisher_builder_struct_ptr.is_null());

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => {
            let publisher_builder = port_factory
                .value
                .as_ref()
                .ipc
                .__internal_worker_publisher_builder(stage_id);
            let Some(publisher_builder) = publisher_builder else {
                return core::ptr::null_mut();
            };

            *has_value = true;
            (*publisher_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactoryPublisherBuilderUnion::new_ipc(publisher_builder),
                deleter,
            );
        }
        iox2_service_type_e::LOCAL => {
            let publisher_builder = port_factory
                .value
                .as_ref()
                .local
                .__internal_worker_publisher_builder(stage_id);
            let Some(publisher_builder) = publisher_builder else {
                return core::ptr::null_mut();
            };

            *has_value = true;
            (*publisher_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactoryPublisherBuilderUnion::new_local(publisher_builder),
                deleter,
            );
        }
    };

    (*publisher_builder_struct_ptr).as_handle()
}

/// Instantiates a [`iox2_port_factory_subscriber_builder_h`] for egress endpoints.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_egress_builder(
    handle: iox2_port_factory_pipeline_h_ref,
    subscriber_builder_struct_ptr: *mut iox2_port_factory_subscriber_builder_t,
) -> iox2_port_factory_subscriber_builder_h {
    handle.assert_non_null();

    let mut subscriber_builder_struct_ptr = subscriber_builder_struct_ptr;
    fn no_op(_: *mut iox2_port_factory_subscriber_builder_t) {}
    let mut deleter: fn(*mut iox2_port_factory_subscriber_builder_t) = no_op;
    if subscriber_builder_struct_ptr.is_null() {
        subscriber_builder_struct_ptr = iox2_port_factory_subscriber_builder_t::alloc();
        deleter = iox2_port_factory_subscriber_builder_t::dealloc;
    }
    debug_assert!(!subscriber_builder_struct_ptr.is_null());

    let port_factory = &mut *handle.as_type();
    match port_factory.service_type {
        iox2_service_type_e::IPC => {
            let subscriber_builder = port_factory
                .value
                .as_ref()
                .ipc
                .__internal_egress_subscriber_builder();
            (*subscriber_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactorySubscriberBuilderUnion::new_ipc(subscriber_builder),
                deleter,
            );
        }
        iox2_service_type_e::LOCAL => {
            let subscriber_builder = port_factory
                .value
                .as_ref()
                .local
                .__internal_egress_subscriber_builder();
            (*subscriber_builder_struct_ptr).init(
                port_factory.service_type,
                PortFactorySubscriberBuilderUnion::new_local(subscriber_builder),
                deleter,
            );
        }
    };

    (*subscriber_builder_struct_ptr).as_handle()
}

/// Calls the callback repeatedly for all [`Node`](iceoryx2::node::Node)s that have opened the service.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_nodes(
    handle: iox2_port_factory_pipeline_h_ref,
    callback: iox2_node_list_callback,
    callback_ctx: iox2_callback_context,
) -> c_int {
    use iceoryx2::prelude::PortFactory;

    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();

    let list_result = match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory
            .value
            .as_ref()
            .ipc
            .nodes(|node_state| iox2_node_list_impl(&node_state, callback, callback_ctx)),
        iox2_service_type_e::LOCAL => port_factory
            .value
            .as_ref()
            .local
            .nodes(|node_state| iox2_node_list_impl(&node_state, callback, callback_ctx)),
    };

    match list_result {
        Ok(_) => IOX2_OK,
        Err(e) => e.into_c_int(),
    }
}

/// Returns the immutable pointer to the service name.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_service_name(
    handle: iox2_port_factory_pipeline_h_ref,
) -> iox2_service_name_ptr {
    use iceoryx2::prelude::PortFactory;

    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();

    match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.name(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.name(),
    }
}

/// Stores the service id in the provided buffer.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_service_id(
    handle: iox2_port_factory_pipeline_h_ref,
    buffer: *mut c_char,
    buffer_len: usize,
) {
    use iceoryx2::prelude::PortFactory;

    debug_assert!(!buffer.is_null());
    handle.assert_non_null();

    let port_factory = &mut *handle.as_type();
    let service_id = match port_factory.service_type {
        iox2_service_type_e::IPC => port_factory.value.as_ref().ipc.service_id(),
        iox2_service_type_e::LOCAL => port_factory.value.as_ref().local.service_id(),
    };

    let len = buffer_len.min(service_id.as_str().len());
    core::ptr::copy_nonoverlapping(service_id.as_str().as_ptr(), buffer.cast(), len);
    buffer.add(len).write(0);
}

/// Destroys the pipeline port factory.
#[no_mangle]
pub unsafe extern "C" fn iox2_port_factory_pipeline_drop(
    port_factory_handle: iox2_port_factory_pipeline_h,
) {
    debug_assert!(!port_factory_handle.is_null());

    let port_factory = &mut *port_factory_handle.as_type();

    match port_factory.service_type {
        iox2_service_type_e::IPC => {
            ManuallyDrop::drop(&mut port_factory.value.as_mut().ipc);
        }
        iox2_service_type_e::LOCAL => {
            ManuallyDrop::drop(&mut port_factory.value.as_mut().local);
        }
    }
    (port_factory.deleter)(port_factory);
}
