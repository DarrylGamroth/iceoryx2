// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(non_camel_case_types)]

use crate::api::{
    AssertNonNullHandle, HandleToType, IOX2_OK, IntoCInt, ProgressivePublisherUnion,
    ProgressiveSubscriberUnion, UnsafeCallbackContextSendWorkaround, UserHeaderFfi,
    backpressure_info_cast, c_size_t, iox2_allocation_strategy_e, iox2_backpressure_handler,
    iox2_backpressure_strategy_e, iox2_callback_context, iox2_port_name_ptr,
    iox2_preallocated_samples_override, iox2_progressive_publisher_h, iox2_progressive_publisher_t,
    iox2_progressive_subscriber_h, iox2_progressive_subscriber_t, iox2_service_type_e,
};
use core::ffi::c_int;
use core::mem::ManuallyDrop;
use iceoryx2::service::port_factory::publish_subscribe::{
    ProgressivePortFactory, ProgressivePortFactoryPublisher, ProgressivePortFactorySubscriber,
};
use iceoryx2_ffi_macros::iceoryx2_ffi;

pub(super) union ProgressivePortFactoryUnion {
    pub(super) ipc: ManuallyDrop<ProgressivePortFactory<crate::IpcService, UserHeaderFfi>>,
    pub(super) local: ManuallyDrop<ProgressivePortFactory<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressivePortFactoryUnion {
    pub(super) fn new_ipc(value: ProgressivePortFactory<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    pub(super) fn new_local(
        value: ProgressivePortFactory<crate::LocalService, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(8))]
pub struct iox2_port_factory_progressive_pub_sub_storage_t {
    internal: [u8; 16],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressivePortFactoryUnion)]
pub struct iox2_port_factory_progressive_pub_sub_t {
    pub(super) service_type: iox2_service_type_e,
    value: iox2_port_factory_progressive_pub_sub_storage_t,
    deleter: fn(*mut iox2_port_factory_progressive_pub_sub_t),
}

impl iox2_port_factory_progressive_pub_sub_t {
    pub(super) fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressivePortFactoryUnion,
        deleter: fn(*mut iox2_port_factory_progressive_pub_sub_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_port_factory_progressive_pub_sub_h_t;
pub type iox2_port_factory_progressive_pub_sub_h = *mut iox2_port_factory_progressive_pub_sub_h_t;
pub type iox2_port_factory_progressive_pub_sub_h_ref =
    *const iox2_port_factory_progressive_pub_sub_h;

macro_rules! impl_handle {
    ($owning:ty, $borrowed:ty, $target:ty) => {
        impl AssertNonNullHandle for $owning {
            fn assert_non_null(self) {
                debug_assert!(!self.is_null());
            }
        }
        impl AssertNonNullHandle for $borrowed {
            fn assert_non_null(self) {
                debug_assert!(!self.is_null());
                unsafe { debug_assert!(!(*self).is_null()) };
            }
        }
        impl HandleToType for $owning {
            type Target = *mut $target;
            fn as_type(self) -> Self::Target {
                self as *mut _ as _
            }
        }
        impl HandleToType for $borrowed {
            type Target = *mut $target;
            fn as_type(self) -> Self::Target {
                unsafe { *self as *mut _ as _ }
            }
        }
    };
}

impl_handle!(
    iox2_port_factory_progressive_pub_sub_h,
    iox2_port_factory_progressive_pub_sub_h_ref,
    iox2_port_factory_progressive_pub_sub_t
);

union ProgressivePublisherBuilderUnion {
    ipc: ManuallyDrop<ProgressivePortFactoryPublisher<'static, crate::IpcService, UserHeaderFfi>>,
    local:
        ManuallyDrop<ProgressivePortFactoryPublisher<'static, crate::LocalService, UserHeaderFfi>>,
}

impl ProgressivePublisherBuilderUnion {
    fn new_ipc(
        value: ProgressivePortFactoryPublisher<'static, crate::IpcService, UserHeaderFfi>,
    ) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    fn new_local(
        value: ProgressivePortFactoryPublisher<'static, crate::LocalService, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(16))]
pub struct iox2_port_factory_progressive_publisher_builder_storage_t {
    internal: [u8; 288],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressivePublisherBuilderUnion)]
pub struct iox2_port_factory_progressive_publisher_builder_t {
    service_type: iox2_service_type_e,
    value: iox2_port_factory_progressive_publisher_builder_storage_t,
    deleter: fn(*mut iox2_port_factory_progressive_publisher_builder_t),
}

impl iox2_port_factory_progressive_publisher_builder_t {
    fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressivePublisherBuilderUnion,
        deleter: fn(*mut iox2_port_factory_progressive_publisher_builder_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_port_factory_progressive_publisher_builder_h_t;
pub type iox2_port_factory_progressive_publisher_builder_h =
    *mut iox2_port_factory_progressive_publisher_builder_h_t;
pub type iox2_port_factory_progressive_publisher_builder_h_ref =
    *const iox2_port_factory_progressive_publisher_builder_h;

impl_handle!(
    iox2_port_factory_progressive_publisher_builder_h,
    iox2_port_factory_progressive_publisher_builder_h_ref,
    iox2_port_factory_progressive_publisher_builder_t
);

union ProgressiveSubscriberBuilderUnion {
    ipc: ManuallyDrop<ProgressivePortFactorySubscriber<'static, crate::IpcService, UserHeaderFfi>>,
    local:
        ManuallyDrop<ProgressivePortFactorySubscriber<'static, crate::LocalService, UserHeaderFfi>>,
}

impl ProgressiveSubscriberBuilderUnion {
    fn new_ipc(
        value: ProgressivePortFactorySubscriber<'static, crate::IpcService, UserHeaderFfi>,
    ) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    fn new_local(
        value: ProgressivePortFactorySubscriber<'static, crate::LocalService, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(16))]
pub struct iox2_port_factory_progressive_subscriber_builder_storage_t {
    internal: [u8; 192],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressiveSubscriberBuilderUnion)]
pub struct iox2_port_factory_progressive_subscriber_builder_t {
    service_type: iox2_service_type_e,
    value: iox2_port_factory_progressive_subscriber_builder_storage_t,
    deleter: fn(*mut iox2_port_factory_progressive_subscriber_builder_t),
}

impl iox2_port_factory_progressive_subscriber_builder_t {
    fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressiveSubscriberBuilderUnion,
        deleter: fn(*mut iox2_port_factory_progressive_subscriber_builder_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_port_factory_progressive_subscriber_builder_h_t;
pub type iox2_port_factory_progressive_subscriber_builder_h =
    *mut iox2_port_factory_progressive_subscriber_builder_h_t;
pub type iox2_port_factory_progressive_subscriber_builder_h_ref =
    *const iox2_port_factory_progressive_subscriber_builder_h;

impl_handle!(
    iox2_port_factory_progressive_subscriber_builder_h,
    iox2_port_factory_progressive_subscriber_builder_h_ref,
    iox2_port_factory_progressive_subscriber_builder_t
);

/// Creates a builder for the single progressive publisher.
///
/// # Safety
///
/// `factory_handle` must be valid. `builder_struct_ptr` must be null or point to
/// uninitialized storage that is large and aligned enough for the declared C type.
/// The factory must outlive the returned builder and the publisher created from it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_pub_sub_publisher_builder(
    factory_handle: iox2_port_factory_progressive_pub_sub_h_ref,
    builder_struct_ptr: *mut iox2_port_factory_progressive_publisher_builder_t,
) -> iox2_port_factory_progressive_publisher_builder_h {
    factory_handle.assert_non_null();
    unsafe {
        let factory = &*factory_handle.as_type();
        let mut storage = builder_struct_ptr;
        fn no_op(_: *mut iox2_port_factory_progressive_publisher_builder_t) {}
        let mut deleter: fn(*mut iox2_port_factory_progressive_publisher_builder_t) = no_op;
        if storage.is_null() {
            storage = iox2_port_factory_progressive_publisher_builder_t::alloc();
            deleter = iox2_port_factory_progressive_publisher_builder_t::dealloc;
        }
        let builder = match factory.service_type {
            iox2_service_type_e::IPC => ProgressivePublisherBuilderUnion::new_ipc(
                factory.value.as_ref().ipc.publisher_builder(),
            ),
            iox2_service_type_e::LOCAL => ProgressivePublisherBuilderUnion::new_local(
                factory.value.as_ref().local.publisher_builder(),
            ),
        };
        (*storage).init(factory.service_type, builder, deleter);
        (*storage).as_handle()
    }
}

/// Creates a builder for a progressive subscriber.
///
/// # Safety
///
/// `factory_handle` must be valid. `builder_struct_ptr` must be null or point to
/// uninitialized storage that is large and aligned enough for the declared C type.
/// The factory must outlive the returned builder and the subscriber created from it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_pub_sub_subscriber_builder(
    factory_handle: iox2_port_factory_progressive_pub_sub_h_ref,
    builder_struct_ptr: *mut iox2_port_factory_progressive_subscriber_builder_t,
) -> iox2_port_factory_progressive_subscriber_builder_h {
    factory_handle.assert_non_null();
    unsafe {
        let factory = &*factory_handle.as_type();
        let mut storage = builder_struct_ptr;
        fn no_op(_: *mut iox2_port_factory_progressive_subscriber_builder_t) {}
        let mut deleter: fn(*mut iox2_port_factory_progressive_subscriber_builder_t) = no_op;
        if storage.is_null() {
            storage = iox2_port_factory_progressive_subscriber_builder_t::alloc();
            deleter = iox2_port_factory_progressive_subscriber_builder_t::dealloc;
        }
        let builder = match factory.service_type {
            iox2_service_type_e::IPC => ProgressiveSubscriberBuilderUnion::new_ipc(
                factory.value.as_ref().ipc.subscriber_builder(),
            ),
            iox2_service_type_e::LOCAL => ProgressiveSubscriberBuilderUnion::new_local(
                factory.value.as_ref().local.subscriber_builder(),
            ),
        };
        (*storage).init(factory.service_type, builder, deleter);
        (*storage).as_handle()
    }
}

macro_rules! mutate_publisher_builder {
    ($handle:expr, $method:ident($value:expr)) => {{
        let builder = &mut *$handle.as_type();
        match builder.service_type {
            iox2_service_type_e::IPC => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().ipc);
                builder.set(ProgressivePublisherBuilderUnion::new_ipc(
                    value.$method($value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().local);
                builder.set(ProgressivePublisherBuilderUnion::new_local(
                    value.$method($value),
                ));
            }
        }
    }};
}

/// Sets the initial maximum progressive payload capacity in bytes.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive publisher-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_initial_max_slice_len(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_publisher_builder!(builder_handle, initial_max_slice_len(value)) };
}

/// Sets the maximum number of simultaneously loaned progressive samples.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive publisher-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_max_loaned_samples(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_publisher_builder!(builder_handle, max_loaned_samples(value)) };
}

/// Selects the progressive publisher's allocation strategy.
///
/// # Safety
///
/// `builder_handle` must be valid and `value` must be a valid C enum value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_allocation_strategy(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    value: iox2_allocation_strategy_e,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_publisher_builder!(builder_handle, allocation_strategy(value.into())) };
}

/// Selects the backpressure strategy used only while sending a new frame.
///
/// # Safety
///
/// `builder_handle` must be valid and `value` must be a valid C enum value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_backpressure_strategy(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    value: iox2_backpressure_strategy_e,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_publisher_builder!(builder_handle, backpressure_strategy(value.into())) };
}

/// Installs the callback invoked when a new progressive sample cannot be delivered.
///
/// # Safety
///
/// `builder_handle` and `handler` must be valid. `ctx` is retained by the publisher;
/// it must remain valid for every callback invocation and be safe to access under the
/// publisher's threading policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_backpressure_handler(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    handler: iox2_backpressure_handler,
    ctx: iox2_callback_context,
) {
    builder_handle.assert_non_null();
    let ctx = UnsafeCallbackContextSendWorkaround { ctx };
    unsafe {
        let builder = &mut *builder_handle.as_type();
        match builder.service_type {
            iox2_service_type_e::IPC => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().ipc);
                builder.set(ProgressivePublisherBuilderUnion::new_ipc(
                    value.set_backpressure_handler(move |info| {
                        let ctx = ctx;
                        handler(backpressure_info_cast(info), ctx.ctx).into()
                    }),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().local);
                builder.set(ProgressivePublisherBuilderUnion::new_local(
                    value.set_backpressure_handler(move |info| {
                        let ctx = ctx;
                        handler(backpressure_info_cast(info), ctx.ctx).into()
                    }),
                ));
            }
        }
    }
}

/// Overrides the number of samples preallocated by the progressive publisher.
///
/// # Safety
///
/// `builder_handle` and `callback` must be valid. `callback_ctx` must remain valid for
/// every callback invocation and be safe to access under the publisher's threading policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_override_samples_preallocation(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    callback: iox2_preallocated_samples_override,
    callback_ctx: iox2_callback_context,
) {
    builder_handle.assert_non_null();
    unsafe {
        let builder = &mut *builder_handle.as_type();
        match builder.service_type {
            iox2_service_type_e::IPC => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().ipc);
                builder.set(ProgressivePublisherBuilderUnion::new_ipc(
                    value.override_sample_preallocation(move |count| callback(count, callback_ctx)),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let value = ManuallyDrop::take(&mut builder.value.as_mut().local);
                builder.set(ProgressivePublisherBuilderUnion::new_local(
                    value.override_sample_preallocation(move |count| callback(count, callback_ctx)),
                ));
            }
        }
    }
}

/// Sets the progressive publisher port name.
///
/// # Safety
///
/// `builder_handle` and `port_name_ptr` must be valid for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_set_name(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h_ref,
    port_name_ptr: iox2_port_name_ptr,
) {
    builder_handle.assert_non_null();
    debug_assert!(!port_name_ptr.is_null());
    unsafe { mutate_publisher_builder!(builder_handle, name(&*port_name_ptr)) };
}

/// Creates the progressive publisher and consumes its builder.
///
/// # Safety
///
/// `builder_handle` must be a valid owning handle and is consumed. `publisher_handle_ptr`
/// must be writable. `publisher_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_publisher_builder_create(
    builder_handle: iox2_port_factory_progressive_publisher_builder_h,
    publisher_struct_ptr: *mut iox2_progressive_publisher_t,
    publisher_handle_ptr: *mut iox2_progressive_publisher_h,
) -> c_int {
    builder_handle.assert_non_null();
    debug_assert!(!publisher_handle_ptr.is_null());
    unsafe {
        *publisher_handle_ptr = core::ptr::null_mut();
        let builder_struct = &mut *builder_handle.as_type();
        let service_type = builder_struct.service_type;
        let builder = builder_struct
            .take()
            .expect("valid progressive publisher builder");
        (builder_struct.deleter)(builder_struct);
        let result = match service_type {
            iox2_service_type_e::IPC => ManuallyDrop::into_inner(builder.ipc)
                .create()
                .map(ProgressivePublisherUnion::new_ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::into_inner(builder.local)
                .create()
                .map(ProgressivePublisherUnion::new_local),
        };
        match result {
            Ok(publisher) => {
                let mut storage = publisher_struct_ptr;
                fn no_op(_: *mut iox2_progressive_publisher_t) {}
                let mut deleter: fn(*mut iox2_progressive_publisher_t) = no_op;
                if storage.is_null() {
                    storage = iox2_progressive_publisher_t::alloc();
                    deleter = iox2_progressive_publisher_t::dealloc;
                }
                (*storage).init(service_type, publisher, deleter);
                *publisher_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

macro_rules! mutate_subscriber_builder {
    ($handle:expr, $method:ident($value:expr)) => {{
        let builder = &mut *$handle.as_type();
        match builder.service_type {
            iox2_service_type_e::IPC => {
                let current = ManuallyDrop::take(&mut builder.value.as_mut().ipc);
                builder.set(ProgressiveSubscriberBuilderUnion::new_ipc(
                    current.$method($value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let current = ManuallyDrop::take(&mut builder.value.as_mut().local);
                builder.set(ProgressiveSubscriberBuilderUnion::new_local(
                    current.$method($value),
                ));
            }
        }
    }};
}

/// Sets the progressive subscriber queue capacity.
///
/// # Safety
///
/// `builder_handle` must be a valid non-owning progressive subscriber-builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_subscriber_builder_set_buffer_size(
    builder_handle: iox2_port_factory_progressive_subscriber_builder_h_ref,
    value: c_size_t,
) {
    builder_handle.assert_non_null();
    unsafe { mutate_subscriber_builder!(builder_handle, buffer_size(value)) };
}

/// Sets the progressive subscriber port name.
///
/// # Safety
///
/// `builder_handle` and `port_name_ptr` must be valid for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_subscriber_builder_set_name(
    builder_handle: iox2_port_factory_progressive_subscriber_builder_h_ref,
    port_name_ptr: iox2_port_name_ptr,
) {
    builder_handle.assert_non_null();
    debug_assert!(!port_name_ptr.is_null());
    unsafe { mutate_subscriber_builder!(builder_handle, name(&*port_name_ptr)) };
}

/// Creates a progressive subscriber and consumes its builder.
///
/// # Safety
///
/// `builder_handle` must be a valid owning handle and is consumed. `subscriber_handle_ptr`
/// must be writable. `subscriber_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_subscriber_builder_create(
    builder_handle: iox2_port_factory_progressive_subscriber_builder_h,
    subscriber_struct_ptr: *mut iox2_progressive_subscriber_t,
    subscriber_handle_ptr: *mut iox2_progressive_subscriber_h,
) -> c_int {
    builder_handle.assert_non_null();
    debug_assert!(!subscriber_handle_ptr.is_null());
    unsafe {
        *subscriber_handle_ptr = core::ptr::null_mut();
        let builder_struct = &mut *builder_handle.as_type();
        let service_type = builder_struct.service_type;
        let builder = builder_struct
            .take()
            .expect("valid progressive subscriber builder");
        (builder_struct.deleter)(builder_struct);
        let result = match service_type {
            iox2_service_type_e::IPC => ManuallyDrop::into_inner(builder.ipc)
                .create()
                .map(ProgressiveSubscriberUnion::new_ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::into_inner(builder.local)
                .create()
                .map(ProgressiveSubscriberUnion::new_local),
        };
        match result {
            Ok(subscriber) => {
                let mut storage = subscriber_struct_ptr;
                fn no_op(_: *mut iox2_progressive_subscriber_t) {}
                let mut deleter: fn(*mut iox2_progressive_subscriber_t) = no_op;
                if storage.is_null() {
                    storage = iox2_progressive_subscriber_t::alloc();
                    deleter = iox2_progressive_subscriber_t::dealloc;
                }
                (*storage).init(service_type, subscriber, deleter);
                *subscriber_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Drops a progressive port factory.
///
/// # Safety
///
/// `factory_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_port_factory_progressive_pub_sub_drop(
    factory_handle: iox2_port_factory_progressive_pub_sub_h,
) {
    factory_handle.assert_non_null();
    unsafe {
        let factory = &mut *factory_handle.as_type();
        match factory.service_type {
            iox2_service_type_e::IPC => ManuallyDrop::drop(&mut factory.value.as_mut().ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::drop(&mut factory.value.as_mut().local),
        }
        (factory.deleter)(factory);
    }
}
