// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(non_camel_case_types)]

use crate::api::{
    AssertNonNullHandle, HandleToType, IOX2_OK, IntoCInt, UserHeaderFfi, c_size_t,
    iox2_service_type_e,
};
use core::ffi::{c_char, c_int, c_void};
use core::mem::ManuallyDrop;
use iceoryx2::port::progressive_publisher::ProgressivePublisher;
use iceoryx2::progressive_sample_mut::{
    ProgressiveSampleMut, ProgressiveSampleMutUninit, ProgressiveWriteError,
};
use iceoryx2_bb_elementary::static_assert::*;
use iceoryx2_bb_elementary_traits::AsCStr;
use iceoryx2_ffi_macros::{CStrRepr, iceoryx2_ffi};

#[repr(C)]
#[derive(Copy, Clone, CStrRepr)]
pub enum iox2_progressive_write_error_e {
    PUBLISHED_LENGTH_REGRESSED = IOX2_OK as isize + 1,
    PUBLISHED_LENGTH_EXCEEDS_CAPACITY,
    SAMPLE_IS_TERMINAL,
    INSUFFICIENT_CAPACITY,
}

impl IntoCInt for ProgressiveWriteError {
    fn into_c_int(self) -> c_int {
        (match self {
            ProgressiveWriteError::PublishedLengthRegressed => {
                iox2_progressive_write_error_e::PUBLISHED_LENGTH_REGRESSED
            }
            ProgressiveWriteError::PublishedLengthExceedsCapacity => {
                iox2_progressive_write_error_e::PUBLISHED_LENGTH_EXCEEDS_CAPACITY
            }
            ProgressiveWriteError::SampleIsTerminal => {
                iox2_progressive_write_error_e::SAMPLE_IS_TERMINAL
            }
            ProgressiveWriteError::InsufficientCapacity => {
                iox2_progressive_write_error_e::INSUFFICIENT_CAPACITY
            }
        }) as c_int
    }
}

pub(super) union ProgressivePublisherUnion {
    pub(super) ipc: ManuallyDrop<ProgressivePublisher<crate::IpcService, UserHeaderFfi>>,
    pub(super) local: ManuallyDrop<ProgressivePublisher<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressivePublisherUnion {
    pub(super) fn new_ipc(value: ProgressivePublisher<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    pub(super) fn new_local(
        value: ProgressivePublisher<crate::LocalService, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(8))]
pub struct iox2_progressive_publisher_storage_t {
    internal: [u8; 48],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressivePublisherUnion)]
pub struct iox2_progressive_publisher_t {
    pub(super) service_type: iox2_service_type_e,
    value: iox2_progressive_publisher_storage_t,
    deleter: fn(*mut iox2_progressive_publisher_t),
}

impl iox2_progressive_publisher_t {
    pub(super) fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressivePublisherUnion,
        deleter: fn(*mut iox2_progressive_publisher_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_progressive_publisher_h_t;
pub type iox2_progressive_publisher_h = *mut iox2_progressive_publisher_h_t;
pub type iox2_progressive_publisher_h_ref = *const iox2_progressive_publisher_h;

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
    iox2_progressive_publisher_h,
    iox2_progressive_publisher_h_ref,
    iox2_progressive_publisher_t
);

pub(super) union ProgressiveSampleMutUninitUnion {
    ipc: ManuallyDrop<ProgressiveSampleMutUninit<crate::IpcService, UserHeaderFfi>>,
    local: ManuallyDrop<ProgressiveSampleMutUninit<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressiveSampleMutUninitUnion {
    fn new_ipc(value: ProgressiveSampleMutUninit<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    fn new_local(value: ProgressiveSampleMutUninit<crate::LocalService, UserHeaderFfi>) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(8))]
pub struct iox2_progressive_sample_mut_uninit_storage_t {
    internal: [u8; 72],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressiveSampleMutUninitUnion)]
pub struct iox2_progressive_sample_mut_uninit_t {
    service_type: iox2_service_type_e,
    value: iox2_progressive_sample_mut_uninit_storage_t,
    deleter: fn(*mut iox2_progressive_sample_mut_uninit_t),
}

impl iox2_progressive_sample_mut_uninit_t {
    fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressiveSampleMutUninitUnion,
        deleter: fn(*mut iox2_progressive_sample_mut_uninit_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_progressive_sample_mut_uninit_h_t;
pub type iox2_progressive_sample_mut_uninit_h = *mut iox2_progressive_sample_mut_uninit_h_t;
pub type iox2_progressive_sample_mut_uninit_h_ref = *const iox2_progressive_sample_mut_uninit_h;

impl_handle!(
    iox2_progressive_sample_mut_uninit_h,
    iox2_progressive_sample_mut_uninit_h_ref,
    iox2_progressive_sample_mut_uninit_t
);

union ProgressiveSampleMutUnion {
    ipc: ManuallyDrop<ProgressiveSampleMut<crate::IpcService, UserHeaderFfi>>,
    local: ManuallyDrop<ProgressiveSampleMut<crate::LocalService, UserHeaderFfi>>,
}

impl ProgressiveSampleMutUnion {
    fn new_ipc(value: ProgressiveSampleMut<crate::IpcService, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(value),
        }
    }

    fn new_local(value: ProgressiveSampleMut<crate::LocalService, UserHeaderFfi>) -> Self {
        Self {
            local: ManuallyDrop::new(value),
        }
    }
}

#[repr(C)]
#[repr(align(8))]
pub struct iox2_progressive_sample_mut_storage_t {
    internal: [u8; 64],
}

#[repr(C)]
#[iceoryx2_ffi(ProgressiveSampleMutUnion)]
pub struct iox2_progressive_sample_mut_t {
    service_type: iox2_service_type_e,
    value: iox2_progressive_sample_mut_storage_t,
    deleter: fn(*mut iox2_progressive_sample_mut_t),
}

impl iox2_progressive_sample_mut_t {
    fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: ProgressiveSampleMutUnion,
        deleter: fn(*mut iox2_progressive_sample_mut_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_progressive_sample_mut_h_t;
pub type iox2_progressive_sample_mut_h = *mut iox2_progressive_sample_mut_h_t;
pub type iox2_progressive_sample_mut_h_ref = *const iox2_progressive_sample_mut_h;

impl_handle!(
    iox2_progressive_sample_mut_h,
    iox2_progressive_sample_mut_h_ref,
    iox2_progressive_sample_mut_t
);

/// Returns a string literal describing a progressive writer error.
///
/// # Safety
///
/// `error` must contain a valid [`iox2_progressive_write_error_e`] value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_write_error_string(
    error: iox2_progressive_write_error_e,
) -> *const c_char {
    error.as_const_cstr().as_ptr() as *const c_char
}

/// Loans a private uninitialized byte slice with `capacity` bytes.
///
/// # Safety
///
/// `publisher_handle` must be valid and `sample_handle_ptr` must be writable.
/// `sample_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_publisher_loan_slice_uninit(
    publisher_handle: iox2_progressive_publisher_h_ref,
    sample_struct_ptr: *mut iox2_progressive_sample_mut_uninit_t,
    sample_handle_ptr: *mut iox2_progressive_sample_mut_uninit_h,
    capacity: c_size_t,
) -> c_int {
    publisher_handle.assert_non_null();
    debug_assert!(!sample_handle_ptr.is_null());
    unsafe {
        *sample_handle_ptr = core::ptr::null_mut();
        let publisher = &*publisher_handle.as_type();
        let result = match publisher.service_type {
            iox2_service_type_e::IPC => publisher
                .value
                .as_ref()
                .ipc
                .loan_slice_uninit(capacity)
                .map(ProgressiveSampleMutUninitUnion::new_ipc),
            iox2_service_type_e::LOCAL => publisher
                .value
                .as_ref()
                .local
                .loan_slice_uninit(capacity)
                .map(ProgressiveSampleMutUninitUnion::new_local),
        };

        match result {
            Ok(sample) => {
                let mut storage = sample_struct_ptr;
                fn no_op(_: *mut iox2_progressive_sample_mut_uninit_t) {}
                let mut deleter: fn(*mut iox2_progressive_sample_mut_uninit_t) = no_op;
                if storage.is_null() {
                    storage = iox2_progressive_sample_mut_uninit_t::alloc();
                    deleter = iox2_progressive_sample_mut_uninit_t::dealloc;
                }
                (*storage).init(publisher.service_type, sample, deleter);
                *sample_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Returns the mutable payload pointer and byte capacity while the loan is private.
///
/// The caller may retain the pointer after `send` for use by the single external writer while
/// the returned active-writer handle remains alive. The external writer must stop before
/// `finish`, `abort`, active-writer drop, private-loan drop, or deallocation.
///
/// # Safety
///
/// `sample_handle` and `payload_ptr` must be valid. If non-null, `capacity` must be writable.
/// All writes must remain in bounds, and bytes below a successfully published watermark must
/// never be modified again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_uninit_payload_mut(
    sample_handle: iox2_progressive_sample_mut_uninit_h_ref,
    payload_ptr: *mut *mut c_void,
    capacity: *mut c_size_t,
) {
    sample_handle.assert_non_null();
    debug_assert!(!payload_ptr.is_null());
    unsafe {
        let sample = &mut *sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => {
                *payload_ptr = sample.value.as_ref().ipc.payload_mut_ptr().cast();
                if !capacity.is_null() {
                    *capacity = sample.value.as_ref().ipc.payload_capacity();
                }
            }
            iox2_service_type_e::LOCAL => {
                *payload_ptr = sample.value.as_ref().local.payload_mut_ptr().cast();
                if !capacity.is_null() {
                    *capacity = sample.value.as_ref().local.payload_capacity();
                }
            }
        }
    }
}

/// Returns the mutable application user header while the sample is private.
///
/// # Safety
///
/// `sample_handle` must be valid. The returned pointer has the layout configured on the
/// service builder and remains valid only until the private loan is sent or dropped.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_uninit_user_header_mut(
    sample_handle: iox2_progressive_sample_mut_uninit_h_ref,
) -> *mut c_void {
    sample_handle.assert_non_null();
    unsafe {
        let sample = &mut *sample_handle.as_type();
        match sample.service_type {
            iox2_service_type_e::IPC => {
                (sample.value.as_mut().ipc.user_header_mut() as *mut UserHeaderFfi).cast()
            }
            iox2_service_type_e::LOCAL => {
                (sample.value.as_mut().local.user_header_mut() as *mut UserHeaderFfi).cast()
            }
        }
    }
}

/// Sends the offset once and transfers the exact publisher loan into an active writer handle.
/// The private-loan handle is consumed even when delivery fails.
///
/// # Safety
///
/// `sample_handle` must be a valid owning handle and is consumed. `writer_handle_ptr` must
/// be writable. `writer_struct_ptr` must be null or point to uninitialized storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_uninit_send(
    sample_handle: iox2_progressive_sample_mut_uninit_h,
    writer_struct_ptr: *mut iox2_progressive_sample_mut_t,
    writer_handle_ptr: *mut iox2_progressive_sample_mut_h,
) -> c_int {
    sample_handle.assert_non_null();
    debug_assert!(!writer_handle_ptr.is_null());
    unsafe {
        *writer_handle_ptr = core::ptr::null_mut();
        let sample_struct = &mut *sample_handle.as_type();
        let service_type = sample_struct.service_type;
        let sample = sample_struct
            .take()
            .expect("valid progressive private loan");
        (sample_struct.deleter)(sample_struct);

        let result = match service_type {
            iox2_service_type_e::IPC => ManuallyDrop::into_inner(sample.ipc)
                .send()
                .map(ProgressiveSampleMutUnion::new_ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::into_inner(sample.local)
                .send()
                .map(ProgressiveSampleMutUnion::new_local),
        };
        match result {
            Ok(writer) => {
                let mut storage = writer_struct_ptr;
                fn no_op(_: *mut iox2_progressive_sample_mut_t) {}
                let mut deleter: fn(*mut iox2_progressive_sample_mut_t) = no_op;
                if storage.is_null() {
                    storage = iox2_progressive_sample_mut_t::alloc();
                    deleter = iox2_progressive_sample_mut_t::dealloc;
                }
                (*storage).init(service_type, writer, deleter);
                *writer_handle_ptr = (*storage).as_handle();
                IOX2_OK
            }
            Err(error) => error.into_c_int(),
        }
    }
}

/// Drops an unsent private loan and returns its publisher reference.
///
/// # Safety
///
/// `sample_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_uninit_drop(
    sample_handle: iox2_progressive_sample_mut_uninit_h,
) {
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

/// Returns the active writer's total capacity.
///
/// # Safety
///
/// `writer_handle` must be a valid non-owning active-writer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_payload_capacity(
    writer_handle: iox2_progressive_sample_mut_h_ref,
) -> c_size_t {
    writer_handle.assert_non_null();
    unsafe {
        let writer = &*writer_handle.as_type();
        match writer.service_type {
            iox2_service_type_e::IPC => writer.value.as_ref().ipc.payload_capacity(),
            iox2_service_type_e::LOCAL => writer.value.as_ref().local.payload_capacity(),
        }
    }
}

/// Acquire-loads the active writer's current published length.
///
/// # Safety
///
/// `writer_handle` must be a valid non-owning active-writer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_published_len(
    writer_handle: iox2_progressive_sample_mut_h_ref,
) -> c_size_t {
    writer_handle.assert_non_null();
    unsafe {
        let writer = &*writer_handle.as_type();
        match writer.service_type {
            iox2_service_type_e::IPC => writer.value.as_ref().ipc.published_len(),
            iox2_service_type_e::LOCAL => writer.value.as_ref().local.published_len(),
        }
    }
}

/// Returns the immutable user header after send.
///
/// # Safety
///
/// `writer_handle` must be valid. The returned pointer remains valid only while the writer
/// handle owns the sample and has the user-header layout configured on the service.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_user_header(
    writer_handle: iox2_progressive_sample_mut_h_ref,
) -> *const c_void {
    writer_handle.assert_non_null();
    unsafe {
        let writer = &*writer_handle.as_type();
        match writer.service_type {
            iox2_service_type_e::IPC => {
                (writer.value.as_ref().ipc.user_header() as *const UserHeaderFfi).cast()
            }
            iox2_service_type_e::LOCAL => {
                (writer.value.as_ref().local.user_header() as *const UserHeaderFfi).cast()
            }
        }
    }
}

/// Copies bytes into the unpublished suffix and release-publishes the enlarged prefix.
///
/// # Safety
///
/// `writer_handle` must be valid. When `len` is nonzero, `bytes` must point to at least
/// `len` readable bytes and must not overlap the destination sample allocation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_write_from_slice(
    writer_handle: iox2_progressive_sample_mut_h_ref,
    bytes: *const u8,
    len: c_size_t,
) -> c_int {
    writer_handle.assert_non_null();
    debug_assert!(!bytes.is_null() || len == 0);
    let bytes = if len == 0 {
        &[]
    } else {
        unsafe { core::slice::from_raw_parts(bytes, len) }
    };
    unsafe {
        let writer = &mut *writer_handle.as_type();
        let result = match writer.service_type {
            iox2_service_type_e::IPC => writer.value.as_mut().ipc.write_from_slice(bytes),
            iox2_service_type_e::LOCAL => writer.value.as_mut().local.write_from_slice(bytes),
        };
        result.map_or_else(IntoCInt::into_c_int, |_| IOX2_OK)
    }
}

/// Release-publishes a new contiguous byte watermark for externally initialized data.
///
/// # Safety
///
/// Before calling, the C caller must ensure every byte below `new_len` is initialized and
/// visible to CPU readers and will never be modified again. This function does not establish
/// DMA cache coherency.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_set_published_len(
    writer_handle: iox2_progressive_sample_mut_h_ref,
    new_len: c_size_t,
) -> c_int {
    writer_handle.assert_non_null();
    unsafe {
        let writer = &mut *writer_handle.as_type();
        let result = match writer.service_type {
            iox2_service_type_e::IPC => writer.value.as_mut().ipc.set_published_len(new_len),
            iox2_service_type_e::LOCAL => writer.value.as_mut().local.set_published_len(new_len),
        };
        result.map_or_else(IntoCInt::into_c_int, |_| IOX2_OK)
    }
}

unsafe fn terminal(writer_handle: iox2_progressive_sample_mut_h, finish: bool) -> c_int {
    writer_handle.assert_non_null();
    unsafe {
        let writer_struct = &mut *writer_handle.as_type();
        let service_type = writer_struct.service_type;
        let writer = writer_struct.take().expect("valid progressive writer");
        (writer_struct.deleter)(writer_struct);
        let result = match (service_type, finish) {
            (iox2_service_type_e::IPC, true) => ManuallyDrop::into_inner(writer.ipc).finish(),
            (iox2_service_type_e::IPC, false) => ManuallyDrop::into_inner(writer.ipc).abort(),
            (iox2_service_type_e::LOCAL, true) => ManuallyDrop::into_inner(writer.local).finish(),
            (iox2_service_type_e::LOCAL, false) => ManuallyDrop::into_inner(writer.local).abort(),
        };
        result.map_or_else(IntoCInt::into_c_int, |_| IOX2_OK)
    }
}

/// Marks the sample complete, releases the publisher loan, and consumes the writer handle.
///
/// # Safety
///
/// `writer_handle` must be a valid owning handle and is consumed even when an error is returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_finish(
    writer_handle: iox2_progressive_sample_mut_h,
) -> c_int {
    unsafe { terminal(writer_handle, true) }
}

/// Marks the sample aborted, releases the publisher loan, and consumes the writer handle.
///
/// # Safety
///
/// `writer_handle` must be a valid owning handle and is consumed even when an error is returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_abort(
    writer_handle: iox2_progressive_sample_mut_h,
) -> c_int {
    unsafe { terminal(writer_handle, false) }
}

/// Drops an active writer. A filling sample becomes aborted and its publisher loan is released.
///
/// # Safety
///
/// `writer_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_sample_mut_drop(
    writer_handle: iox2_progressive_sample_mut_h,
) {
    writer_handle.assert_non_null();
    unsafe {
        let writer = &mut *writer_handle.as_type();
        match writer.service_type {
            iox2_service_type_e::IPC => ManuallyDrop::drop(&mut writer.value.as_mut().ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::drop(&mut writer.value.as_mut().local),
        }
        (writer.deleter)(writer);
    }
}

/// Drops a progressive publisher.
///
/// # Safety
///
/// `publisher_handle` must be a valid owning handle and becomes invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_progressive_publisher_drop(
    publisher_handle: iox2_progressive_publisher_h,
) {
    publisher_handle.assert_non_null();
    unsafe {
        let publisher = &mut *publisher_handle.as_type();
        match publisher.service_type {
            iox2_service_type_e::IPC => ManuallyDrop::drop(&mut publisher.value.as_mut().ipc),
            iox2_service_type_e::LOCAL => ManuallyDrop::drop(&mut publisher.value.as_mut().local),
        }
        (publisher.deleter)(publisher);
    }
}
