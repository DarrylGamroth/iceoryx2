// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(non_camel_case_types)]

use crate::api::{
    AssertNonNullHandle, HandleToType, IOX2_OK, IntoCInt, UserHeaderFfi, c_size_t,
    iox2_service_type_e,
};
use core::ffi::{c_int, c_void};
use core::mem::ManuallyDrop;
use iceoryx2::port::progressive_subscriber::ProgressiveSubscriber;
use iceoryx2::progressive_sample::ProgressiveSample;
use iceoryx2::service::header::progressive_publish_subscribe::ProgressiveSampleState;
use iceoryx2_bb_elementary::static_assert::*;
use iceoryx2_ffi_macros::iceoryx2_ffi;

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum iox2_progressive_sample_state_e {
    FILLING = 1,
    COMPLETE = 2,
    ABORTED = 3,
}

impl From<ProgressiveSampleState> for iox2_progressive_sample_state_e {
    fn from(value: ProgressiveSampleState) -> Self {
        match value {
            ProgressiveSampleState::Filling => Self::FILLING,
            ProgressiveSampleState::Complete => Self::COMPLETE,
            ProgressiveSampleState::Aborted => Self::ABORTED,
        }
    }
}

pub(super) union ProgressiveSubscriberUnion {
    pub(super) ipc: ManuallyDrop<ProgressiveSubscriber<crate::IpcService, UserHeaderFfi>>,
    pub(super) local: ManuallyDrop<ProgressiveSubscriber<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressiveSubscriberUnion {
    pub(super) fn new_ipc(value: ProgressiveSubscriber<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    pub(super) fn new_local(
        value: ProgressiveSubscriber<crate::LocalService, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(8))]
pub struct iox2_progressive_subscriber_storage_t {
    internal: [u8; 48],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressiveSubscriberUnion)]
pub struct iox2_progressive_subscriber_t {
    pub(super) service_type: iox2_service_type_e,
    value: iox2_progressive_subscriber_storage_t,
    deleter: fn(*mut iox2_progressive_subscriber_t),
}

impl iox2_progressive_subscriber_t {
    pub(super) fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressiveSubscriberUnion,
        deleter: fn(*mut iox2_progressive_subscriber_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_progressive_subscriber_h_t;
pub type iox2_progressive_subscriber_h = *mut iox2_progressive_subscriber_h_t;
pub type iox2_progressive_subscriber_h_ref = *const iox2_progressive_subscriber_h;

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
    iox2_progressive_subscriber_h,
    iox2_progressive_subscriber_h_ref,
    iox2_progressive_subscriber_t
);

union ProgressiveSampleUnion {
    ipc: ManuallyDrop<ProgressiveSample<crate::IpcService, UserHeaderFfi>>,
    local: ManuallyDrop<ProgressiveSample<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressiveSampleUnion {
    fn new_ipc(value: ProgressiveSample<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    fn new_local(value: ProgressiveSample<crate::LocalService, UserHeaderFfi>) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(16))]
pub struct iox2_progressive_sample_storage_t {
    internal: [u8; 96],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressiveSampleUnion)]
pub struct iox2_progressive_sample_t {
    service_type: iox2_service_type_e,
    value: iox2_progressive_sample_storage_t,
    deleter: fn(*mut iox2_progressive_sample_t),
}

impl iox2_progressive_sample_t {
    fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressiveSampleUnion,
        deleter: fn(*mut iox2_progressive_sample_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_progressive_sample_h_t;
pub type iox2_progressive_sample_h = *mut iox2_progressive_sample_h_t;
pub type iox2_progressive_sample_h_ref = *const iox2_progressive_sample_h;

impl_handle!(
    iox2_progressive_sample_h,
    iox2_progressive_sample_h_ref,
    iox2_progressive_sample_t
);

/// Receives a progressive sample offset once. An empty queue returns `IOX2_OK` and a null handle.
///
/// # Safety
///
/// `subscriber_handle` must be valid and `sample_handle_ptr` must be writable.
/// `sample_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_subscriber_receive(
    subscriber_handle: iox2_progressive_subscriber_h_ref,
    sample_struct_ptr: *mut iox2_progressive_sample_t,
    sample_handle_ptr: *mut iox2_progressive_sample_h,
) -> c_int {
    subscriber_handle.assert_non_null();
    debug_assert!(!sample_handle_ptr.is_null());
    unsafe {
        *sample_handle_ptr = core::ptr::null_mut();
        let subscriber = &*subscriber_handle.as_type();
        let result = match subscriber.service_type {
            iox2_service_type_e::IPC => subscriber
                .value
                .as_ref()
                .ipc
                .receive()
                .map(|sample| sample.map(ProgressiveSampleUnion::new_ipc)),
            iox2_service_type_e::LOCAL => subscriber
                .value
                .as_ref()
                .local
                .receive()
                .map(|sample| sample.map(ProgressiveSampleUnion::new_local)),
        };

        match result {
            Ok(Some(sample)) => {
                let mut storage = sample_struct_ptr;
                fn no_op(_: *mut iox2_progressive_sample_t) {}
                let mut deleter: fn(*mut iox2_progressive_sample_t) = no_op;
                if storage.is_null() {
                    storage = iox2_progressive_sample_t::alloc();
                    deleter = iox2_progressive_sample_t::dealloc;
                }
                (*storage).init(subscriber.service_type, sample, deleter);
                *sample_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Ok(None) => IOX2_OK,
            Err(error) => error.into_c_int(),
        }
    }
}

/// Reports whether at least one progressive sample is queued.
///
/// # Safety
///
/// `subscriber_handle` must be valid and `result_ptr` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_subscriber_has_samples(
    subscriber_handle: iox2_progressive_subscriber_h_ref,
    result_ptr: *mut bool,
) -> c_int {
    subscriber_handle.assert_non_null();
    debug_assert!(!result_ptr.is_null());
    unsafe {
        let subscriber = &*subscriber_handle.as_type();
        let result = match subscriber.service_type {
            iox2_service_type_e::IPC => subscriber.value.as_ref().ipc.has_samples(),
            iox2_service_type_e::LOCAL => subscriber.value.as_ref().local.has_samples(),
        };
        match result {
            Ok(value) => {
                *result_ptr = value;
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Returns one acquire-bounded immutable payload prefix snapshot.
///
/// The returned pointer is valid only while the sample handle remains alive. The publisher may
/// extend the prefix, but never mutates bytes already included in this snapshot.
///
/// # Safety
///
/// `sample_handle` must be valid. `payload_ptr` and `published_len` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_payload(
    sample_handle: iox2_progressive_sample_h_ref,
    payload_ptr: *mut *const u8,
    published_len: *mut c_size_t,
) {
    sample_handle.assert_non_null();
    debug_assert!(!payload_ptr.is_null());
    debug_assert!(!published_len.is_null());
    unsafe {
        let sample = &*sample_handle.as_type();
        let payload = match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_ref().ipc.payload(),
            iox2_service_type_e::LOCAL => sample.value.as_ref().local.payload(),
        };
        *payload_ptr = payload.as_ptr();
        *published_len = payload.len();
    }
}

/// Returns the total allocation capacity in bytes.
///
/// # Safety
///
/// `sample_handle` must be a valid non-owning received-sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_payload_capacity(
    sample_handle: iox2_progressive_sample_h_ref,
) -> c_size_t {
    sample_handle.assert_non_null();
    unsafe {
        let sample = &*sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_ref().ipc.payload_capacity(),
            iox2_service_type_e::LOCAL => sample.value.as_ref().local.payload_capacity(),
        }
    }
}

/// Returns the immutable application user header.
///
/// # Safety
///
/// `sample_handle` must be valid. The returned pointer remains valid only while the sample
/// handle owns its lease and has the user-header layout configured on the service.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_user_header(
    sample_handle: iox2_progressive_sample_h_ref,
) -> *const c_void {
    sample_handle.assert_non_null();
    unsafe {
        let sample = &*sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => {
                (sample.value.as_ref().ipc.user_header() as *const UserHeaderFfi).cast()
            }
            iox2_service_type_e::LOCAL => {
                (sample.value.as_ref().local.user_header() as *const UserHeaderFfi).cast()
            }
        }
    }
}

/// Acquire-loads the progressive sample state without performing liveness checks.
///
/// # Safety
///
/// `sample_handle` must be a valid non-owning received-sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_state(
    sample_handle: iox2_progressive_sample_h_ref,
) -> iox2_progressive_sample_state_e {
    sample_handle.assert_non_null();
    unsafe {
        let sample = &*sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_ref().ipc.state().into(),
            iox2_service_type_e::LOCAL => sample.value.as_ref().local.state().into(),
        }
    }
}

/// Returns the sample state while accounting for abrupt publisher death.
/// This may perform operating-system calls when the shared state is still filling.
///
/// # Safety
///
/// `sample_handle` must be valid and `state_ptr` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_state_with_publisher_liveness(
    sample_handle: iox2_progressive_sample_h_ref,
    state_ptr: *mut iox2_progressive_sample_state_e,
) -> c_int {
    sample_handle.assert_non_null();
    debug_assert!(!state_ptr.is_null());
    unsafe {
        let sample = &*sample_handle.as_type();
        let result = match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_ref().ipc.state_with_publisher_liveness(),
            iox2_service_type_e::LOCAL => {
                sample.value.as_ref().local.state_with_publisher_liveness()
            }
        };
        match result {
            Ok(state) => {
                *state_ptr = state.into();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Releases the subscriber's whole-sample reference and consumes the sample handle.
///
/// # Safety
///
/// `sample_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_drop(sample_handle: iox2_progressive_sample_h) {
    sample_handle.assert_non_null();
    unsafe {
        let sample = &mut *sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => ManuallyDrop::drop(&mut sample.value.as_mut().ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::drop(&mut sample.value.as_mut().local),
        }
        (sample.deleter)(sample);
    }
}

/// Drops a progressive subscriber.
///
/// # Safety
///
/// `subscriber_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_subscriber_drop(
    subscriber_handle: iox2_progressive_subscriber_h,
) {
    subscriber_handle.assert_non_null();
    unsafe {
        let subscriber = &mut *subscriber_handle.as_type();
        match subscriber.service_type {
            iox2_service_type_e::IPC => ManuallyDrop::drop(&mut subscriber.value.as_mut().ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::drop(&mut subscriber.value.as_mut().local),
        }
        (subscriber.deleter)(subscriber);
    }
}
