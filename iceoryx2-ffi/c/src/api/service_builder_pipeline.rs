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

use crate::api::{
    c_size_t, iox2_port_factory_pipeline_h, iox2_port_factory_pipeline_t,
    iox2_service_builder_pipeline_h, iox2_service_builder_pipeline_h_ref, iox2_service_type_e,
    iox2_type_variant_e, AssertNonNullHandle, HandleToType, IntoCInt, PayloadFfi,
    PortFactoryPipelineUnion, ServiceBuilderUnion, UserHeaderFfi, IOX2_OK,
};
use crate::create_type_details;

use iceoryx2::service::builder::pipeline::{
    Builder, PipelineCreateError, PipelineOpenError, PipelineOpenOrCreateError,
};
use iceoryx2::service::port_factory::pipeline::PortFactory;
use iceoryx2_bb_elementary_traits::AsCStr;
use iceoryx2_ffi_macros::CStrRepr;

use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;

use super::{iox2_attribute_specifier_h_ref, iox2_attribute_verifier_h_ref};

#[repr(C)]
#[derive(Copy, Clone, CStrRepr)]
pub enum iox2_pipeline_open_or_create_error_e {
    #[CStr = "does not exist"]
    O_DOES_NOT_EXIST = IOX2_OK as isize + 1,
    #[CStr = "insufficient permissions"]
    O_INSUFFICIENT_PERMISSIONS,
    #[CStr = "service in corrupted state"]
    O_SERVICE_IN_CORRUPTED_STATE,
    #[CStr = "incompatible messaging pattern"]
    O_INCOMPATIBLE_MESSAGING_PATTERN,
    #[CStr = "incompatible attributes"]
    O_INCOMPATIBLE_ATTRIBUTES,
    #[CStr = "incompatible payload type"]
    O_INCOMPATIBLE_PAYLOAD_TYPE,
    #[CStr = "incompatible user header type"]
    O_INCOMPATIBLE_USER_HEADER_TYPE,
    #[CStr = "hangs in creation"]
    O_HANGS_IN_CREATION,
    #[CStr = "does not support requested amount of nodes"]
    O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_NODES,
    #[CStr = "does not support requested amount of stages"]
    O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_STAGES,
    #[CStr = "does not support requested in-flight samples"]
    O_DOES_NOT_SUPPORT_REQUESTED_IN_FLIGHT_SAMPLES,
    #[CStr = "does not support requested initial max slice length"]
    O_DOES_NOT_SUPPORT_REQUESTED_INITIAL_MAX_SLICE_LEN,
    #[CStr = "exceeds max number of nodes"]
    O_EXCEEDS_MAX_NUMBER_OF_NODES,
    #[CStr = "is marked for destruction"]
    O_IS_MARKED_FOR_DESTRUCTION,
    #[CStr = "invalid configuration"]
    O_INVALID_CONFIGURATION,
    #[CStr = "edge failure"]
    O_EDGE_FAILURE,
    #[CStr = "internal failure"]
    O_INTERNAL_FAILURE,
    #[CStr = "service in corrupted state"]
    C_SERVICE_IN_CORRUPTED_STATE,
    #[CStr = "internal failure"]
    C_INTERNAL_FAILURE,
    #[CStr = "is being created by another instance"]
    C_IS_BEING_CREATED_BY_ANOTHER_INSTANCE,
    #[CStr = "already exists"]
    C_ALREADY_EXISTS,
    #[CStr = "hangs in creation"]
    C_HANGS_IN_CREATION,
    #[CStr = "insufficient permissions"]
    C_INSUFFICIENT_PERMISSIONS,
    #[CStr = "invalid configuration"]
    C_INVALID_CONFIGURATION,
    #[CStr = "edge failure"]
    C_EDGE_FAILURE,
    #[CStr = "same service is created and removed repeatedly"]
    SYSTEM_IN_FLUX,
}

impl IntoCInt for PipelineOpenError {
    fn into_c_int(self) -> c_int {
        (match self {
            PipelineOpenError::DoesNotExist => iox2_pipeline_open_or_create_error_e::O_DOES_NOT_EXIST,
            PipelineOpenError::InsufficientPermissions => {
                iox2_pipeline_open_or_create_error_e::O_INSUFFICIENT_PERMISSIONS
            }
            PipelineOpenError::ServiceInCorruptedState => {
                iox2_pipeline_open_or_create_error_e::O_SERVICE_IN_CORRUPTED_STATE
            }
            PipelineOpenError::IncompatibleMessagingPattern => {
                iox2_pipeline_open_or_create_error_e::O_INCOMPATIBLE_MESSAGING_PATTERN
            }
            PipelineOpenError::IncompatibleAttributes => {
                iox2_pipeline_open_or_create_error_e::O_INCOMPATIBLE_ATTRIBUTES
            }
            PipelineOpenError::IncompatiblePayloadType => {
                iox2_pipeline_open_or_create_error_e::O_INCOMPATIBLE_PAYLOAD_TYPE
            }
            PipelineOpenError::IncompatibleUserHeaderType => {
                iox2_pipeline_open_or_create_error_e::O_INCOMPATIBLE_USER_HEADER_TYPE
            }
            PipelineOpenError::HangsInCreation => {
                iox2_pipeline_open_or_create_error_e::O_HANGS_IN_CREATION
            }
            PipelineOpenError::DoesNotSupportRequestedAmountOfNodes => {
                iox2_pipeline_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_NODES
            }
            PipelineOpenError::DoesNotSupportRequestedAmountOfStages => {
                iox2_pipeline_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_STAGES
            }
            PipelineOpenError::DoesNotSupportRequestedInFlightSamples => {
                iox2_pipeline_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_IN_FLIGHT_SAMPLES
            }
            PipelineOpenError::DoesNotSupportRequestedInitialMaxSliceLen => {
                iox2_pipeline_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_INITIAL_MAX_SLICE_LEN
            }
            PipelineOpenError::ExceedsMaxNumberOfNodes => {
                iox2_pipeline_open_or_create_error_e::O_EXCEEDS_MAX_NUMBER_OF_NODES
            }
            PipelineOpenError::IsMarkedForDestruction => {
                iox2_pipeline_open_or_create_error_e::O_IS_MARKED_FOR_DESTRUCTION
            }
            PipelineOpenError::InvalidConfiguration(_) => {
                iox2_pipeline_open_or_create_error_e::O_INVALID_CONFIGURATION
            }
            PipelineOpenError::EdgeFailure(_) => iox2_pipeline_open_or_create_error_e::O_EDGE_FAILURE,
            PipelineOpenError::InternalFailure => {
                iox2_pipeline_open_or_create_error_e::O_INTERNAL_FAILURE
            }
        }) as c_int
    }
}

impl IntoCInt for PipelineCreateError {
    fn into_c_int(self) -> c_int {
        (match self {
            PipelineCreateError::ServiceInCorruptedState => {
                iox2_pipeline_open_or_create_error_e::C_SERVICE_IN_CORRUPTED_STATE
            }
            PipelineCreateError::InternalFailure => {
                iox2_pipeline_open_or_create_error_e::C_INTERNAL_FAILURE
            }
            PipelineCreateError::IsBeingCreatedByAnotherInstance => {
                iox2_pipeline_open_or_create_error_e::C_IS_BEING_CREATED_BY_ANOTHER_INSTANCE
            }
            PipelineCreateError::AlreadyExists => {
                iox2_pipeline_open_or_create_error_e::C_ALREADY_EXISTS
            }
            PipelineCreateError::HangsInCreation => {
                iox2_pipeline_open_or_create_error_e::C_HANGS_IN_CREATION
            }
            PipelineCreateError::InsufficientPermissions => {
                iox2_pipeline_open_or_create_error_e::C_INSUFFICIENT_PERMISSIONS
            }
            PipelineCreateError::InvalidConfiguration(_) => {
                iox2_pipeline_open_or_create_error_e::C_INVALID_CONFIGURATION
            }
            PipelineCreateError::EdgeFailure(_) => {
                iox2_pipeline_open_or_create_error_e::C_EDGE_FAILURE
            }
        }) as c_int
    }
}

impl IntoCInt for PipelineOpenOrCreateError {
    fn into_c_int(self) -> c_int {
        match self {
            PipelineOpenOrCreateError::PipelineOpenError(error) => error.into_c_int(),
            PipelineOpenOrCreateError::PipelineCreateError(error) => error.into_c_int(),
            PipelineOpenOrCreateError::SystemInFlux => {
                iox2_pipeline_open_or_create_error_e::SYSTEM_IN_FLUX as _
            }
        }
    }
}

/// Returns a string literal describing the provided [`iox2_pipeline_open_or_create_error_e`].
#[no_mangle]
pub unsafe extern "C" fn iox2_pipeline_open_or_create_error_string(
    error: iox2_pipeline_open_or_create_error_e,
) -> *const c_char {
    error.as_const_cstr().as_ptr() as *const c_char
}

/// Sets the payload type details for the pipeline builder.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_payload_type_details(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    type_variant: iox2_type_variant_e,
    type_name_str: *const c_char,
    type_name_len: c_size_t,
    size: c_size_t,
    alignment: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();

    let value =
        match create_type_details(type_variant, type_name_str, type_name_len, size, alignment) {
            Ok(v) => v,
            Err(e) => return e,
        };

    let service_builder_struct = unsafe { &mut *service_builder_handle.as_type() };

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.__internal_set_payload_type_details(&value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.__internal_set_payload_type_details(&value),
            ));
        }
    }

    IOX2_OK
}

/// Sets the user header type details for the pipeline builder.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_user_header_type_details(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    type_variant: iox2_type_variant_e,
    type_name_str: *const c_char,
    type_name_len: c_size_t,
    size: c_size_t,
    alignment: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();

    let value =
        match create_type_details(type_variant, type_name_str, type_name_len, size, alignment) {
            Ok(v) => v,
            Err(e) => return e,
        };

    let service_builder_struct = unsafe { &mut *service_builder_handle.as_type() };

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.__internal_set_user_header_type_details(&value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.__internal_set_user_header_type_details(&value),
            ));
        }
    }

    IOX2_OK
}

/// Defines the amount of worker stages.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_number_of_stages(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    value: usize,
) {
    service_builder_handle.assert_non_null();
    let service_builder_struct = &mut *service_builder_handle.as_type();

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.number_of_stages(value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.number_of_stages(value),
            ));
        }
    }
}

/// Defines the bounded amount of in-flight samples per pipeline edge.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_max_in_flight_samples(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    value: usize,
) {
    service_builder_handle.assert_non_null();
    let service_builder_struct = &mut *service_builder_handle.as_type();

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.max_in_flight_samples(value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.max_in_flight_samples(value),
            ));
        }
    }
}

/// Defines the maximum amount of nodes that can open each internal edge service.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_max_nodes(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    value: usize,
) {
    service_builder_handle.assert_non_null();
    let service_builder_struct = &mut *service_builder_handle.as_type();

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.max_nodes(value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.max_nodes(value),
            ));
        }
    }
}

/// Defines the default maximum dynamic slice length used by ingress/worker publishers.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_set_initial_max_slice_len(
    service_builder_handle: iox2_service_builder_pipeline_h_ref,
    value: usize,
) {
    service_builder_handle.assert_non_null();
    let service_builder_struct = &mut *service_builder_handle.as_type();

    match service_builder_struct.service_type {
        iox2_service_type_e::IPC => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_ipc_pipeline(
                service_builder.initial_max_slice_len(value),
            ));
        }
        iox2_service_type_e::LOCAL => {
            let service_builder =
                ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);
            service_builder_struct.set(ServiceBuilderUnion::new_local_pipeline(
                service_builder.initial_max_slice_len(value),
            ));
        }
    }
}

/// Opens an existing pipeline service chain or creates it when missing.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_open_or_create(
    service_builder_handle: iox2_service_builder_pipeline_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.open_or_create(),
        |service_builder| service_builder.open_or_create(),
    )
}

/// Opens an existing pipeline service chain or creates it when missing with attributes.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_open_or_create_with_attributes(
    service_builder_handle: iox2_service_builder_pipeline_h,
    attribute_verifier_handle: iox2_attribute_verifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    let attribute_verifier_struct = &mut *attribute_verifier_handle.as_type();
    let attribute_verifier = &attribute_verifier_struct.value.as_ref().0;

    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.open_or_create_with_attributes(attribute_verifier),
        |service_builder| service_builder.open_or_create_with_attributes(attribute_verifier),
    )
}

/// Opens an existing pipeline service chain.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_open(
    service_builder_handle: iox2_service_builder_pipeline_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.open(),
        |service_builder| service_builder.open(),
    )
}

/// Opens an existing pipeline service chain with attribute requirements.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_open_with_attributes(
    service_builder_handle: iox2_service_builder_pipeline_h,
    attribute_verifier_handle: iox2_attribute_verifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    let attribute_verifier_struct = &mut *attribute_verifier_handle.as_type();
    let attribute_verifier = &attribute_verifier_struct.value.as_ref().0;

    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.open_with_attributes(attribute_verifier),
        |service_builder| service_builder.open_with_attributes(attribute_verifier),
    )
}

/// Creates a new pipeline service chain.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_create(
    service_builder_handle: iox2_service_builder_pipeline_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.create(),
        |service_builder| service_builder.create(),
    )
}

/// Creates a new pipeline service chain with attributes.
#[no_mangle]
pub unsafe extern "C" fn iox2_service_builder_pipeline_create_with_attributes(
    service_builder_handle: iox2_service_builder_pipeline_h,
    attribute_specifier_handle: iox2_attribute_specifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
) -> c_int {
    let attribute_specifier_struct = &mut *attribute_specifier_handle.as_type();
    let attribute_specifier = &attribute_specifier_struct.value.as_ref().0;

    iox2_service_builder_pipeline_open_create_impl(
        service_builder_handle,
        port_factory_struct_ptr,
        port_factory_handle_ptr,
        |service_builder| service_builder.create_with_attributes(attribute_specifier),
        |service_builder| service_builder.create_with_attributes(attribute_specifier),
    )
}

unsafe fn iox2_service_builder_pipeline_open_create_impl<E: IntoCInt>(
    service_builder_handle: iox2_service_builder_pipeline_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pipeline_h,
    func_ipc: impl FnOnce(
        Builder<PayloadFfi, crate::IpcService, UserHeaderFfi>,
    ) -> Result<PortFactory<crate::IpcService, PayloadFfi, UserHeaderFfi>, E>,
    func_local: impl FnOnce(
        Builder<PayloadFfi, crate::LocalService, UserHeaderFfi>,
    )
        -> Result<PortFactory<crate::LocalService, PayloadFfi, UserHeaderFfi>, E>,
) -> c_int {
    service_builder_handle.assert_non_null();
    debug_assert!(!port_factory_handle_ptr.is_null());

    let init_port_factory_struct_ptr =
        |port_factory_struct_ptr: *mut iox2_port_factory_pipeline_t| {
            let mut port_factory_struct_ptr = port_factory_struct_ptr;
            fn no_op(_: *mut iox2_port_factory_pipeline_t) {}
            let mut deleter: fn(*mut iox2_port_factory_pipeline_t) = no_op;
            if port_factory_struct_ptr.is_null() {
                port_factory_struct_ptr = iox2_port_factory_pipeline_t::alloc();
                deleter = iox2_port_factory_pipeline_t::dealloc;
            }
            debug_assert!(!port_factory_struct_ptr.is_null());

            (port_factory_struct_ptr, deleter)
        };

    let service_builder_struct = unsafe { &mut *service_builder_handle.as_type() };
    let service_type = service_builder_struct.service_type;
    let service_builder = service_builder_struct
        .value
        .as_option_mut()
        .take()
        .unwrap_or_else(|| {
            panic!("Trying to use an invalid 'iox2_service_builder_pipeline_h'!");
        });
    (service_builder_struct.deleter)(service_builder_struct);

    match service_type {
        iox2_service_type_e::IPC => {
            let service_builder = ManuallyDrop::into_inner(service_builder.ipc);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);

            match func_ipc(service_builder) {
                Ok(port_factory) => {
                    let (port_factory_struct_ptr, deleter) =
                        init_port_factory_struct_ptr(port_factory_struct_ptr);
                    (*port_factory_struct_ptr).init(
                        service_type,
                        PortFactoryPipelineUnion::new_ipc(port_factory),
                        deleter,
                    );
                    *port_factory_handle_ptr = (*port_factory_struct_ptr).as_handle();
                }
                Err(error) => {
                    return error.into_c_int();
                }
            }
        }
        iox2_service_type_e::LOCAL => {
            let service_builder = ManuallyDrop::into_inner(service_builder.local);
            let service_builder = ManuallyDrop::into_inner(service_builder.pipeline);

            match func_local(service_builder) {
                Ok(port_factory) => {
                    let (port_factory_struct_ptr, deleter) =
                        init_port_factory_struct_ptr(port_factory_struct_ptr);
                    (*port_factory_struct_ptr).init(
                        service_type,
                        PortFactoryPipelineUnion::new_local(port_factory),
                        deleter,
                    );
                    *port_factory_handle_ptr = (*port_factory_struct_ptr).as_handle();
                }
                Err(error) => {
                    return error.into_c_int();
                }
            }
        }
    }

    IOX2_OK
}
