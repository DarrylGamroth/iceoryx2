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

use iceoryx2::service::builder::CustomPayloadMarker;
use pyo3::prelude::*;

use crate::attribute_specifier::AttributeSpecifier;
use crate::attribute_verifier::AttributeVerifier;
use crate::error::{
    PipelineCreateError, PipelineOpenError, PipelineOpenOrCreateError,
};
use crate::port_factory_pipeline::{PortFactoryPipeline, PortFactoryPipelineType};
use crate::type_detail::TypeDetail;
use crate::type_storage::TypeStorage;

type IpcBuilder =
    iceoryx2::service::builder::pipeline::Builder<[CustomPayloadMarker], crate::IpcService>;
type LocalBuilder =
    iceoryx2::service::builder::pipeline::Builder<[CustomPayloadMarker], crate::LocalService>;

#[derive(Clone)]
pub(crate) enum ServiceBuilderPipelineType {
    Ipc(IpcBuilder),
    Local(LocalBuilder),
}

#[pyclass]
/// Builder to create new `MessagingPattern::Pipeline` based `Service`s.
pub struct ServiceBuilderPipeline {
    pub(crate) value: ServiceBuilderPipelineType,
    pub payload_type_details: TypeStorage,
}

impl ServiceBuilderPipeline {
    pub(crate) fn new(value: ServiceBuilderPipelineType) -> Self {
        Self {
            value,
            payload_type_details: TypeStorage::new(),
        }
    }

    fn clone_ipc(&self, builder: IpcBuilder) -> Self {
        Self {
            value: ServiceBuilderPipelineType::Ipc(builder),
            payload_type_details: self.payload_type_details.clone(),
        }
    }

    fn clone_local(&self, builder: LocalBuilder) -> Self {
        Self {
            value: ServiceBuilderPipelineType::Local(builder),
            payload_type_details: self.payload_type_details.clone(),
        }
    }
}

#[pymethods]
impl ServiceBuilderPipeline {
    pub fn __set_payload_type(&mut self, value: PyObject) {
        self.payload_type_details.value = Some(value)
    }

    /// Defines the payload type details.
    pub fn __payload_type_details(&self, value: &TypeDetail) -> Self {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => {
                let this = v.clone();
                let this = unsafe { this.__internal_set_payload_type_details(&value.0) };
                self.clone_ipc(this)
            }
            ServiceBuilderPipelineType::Local(v) => {
                let this = v.clone();
                let this = unsafe { this.__internal_set_payload_type_details(&value.0) };
                self.clone_local(this)
            }
        }
    }

    /// Defines the amount of worker stages.
    pub fn number_of_stages(&self, value: usize) -> Self {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => self.clone_ipc(v.clone().number_of_stages(value)),
            ServiceBuilderPipelineType::Local(v) => {
                self.clone_local(v.clone().number_of_stages(value))
            }
        }
    }

    /// Defines the bounded amount of in-flight samples per pipeline edge.
    pub fn max_in_flight_samples(&self, value: usize) -> Self {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => {
                self.clone_ipc(v.clone().max_in_flight_samples(value))
            }
            ServiceBuilderPipelineType::Local(v) => {
                self.clone_local(v.clone().max_in_flight_samples(value))
            }
        }
    }

    /// Defines the maximum amount of nodes that can open each internal edge service.
    pub fn max_nodes(&self, value: usize) -> Self {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => self.clone_ipc(v.clone().max_nodes(value)),
            ServiceBuilderPipelineType::Local(v) => self.clone_local(v.clone().max_nodes(value)),
        }
    }

    /// Defines the default maximum dynamic slice length used by ingress/worker publishers.
    pub fn initial_max_slice_len(&self, value: usize) -> Self {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => {
                self.clone_ipc(v.clone().initial_max_slice_len(value))
            }
            ServiceBuilderPipelineType::Local(v) => {
                self.clone_local(v.clone().initial_max_slice_len(value))
            }
        }
    }

    /// If the `Service` exists, it will be opened otherwise a new `Service` will be created.
    pub fn open_or_create(&self) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .open_or_create()
                        .map_err(|e| PipelineOpenOrCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .open_or_create()
                        .map_err(|e| PipelineOpenOrCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }

    /// If the `Service` exists, it will be opened otherwise a new `Service` will be created with
    /// the required attributes.
    pub fn open_or_create_with_attributes(
        &self,
        verifier: &AttributeVerifier,
    ) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .open_or_create_with_attributes(&verifier.0)
                        .map_err(|e| PipelineOpenOrCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .open_or_create_with_attributes(&verifier.0)
                        .map_err(|e| PipelineOpenOrCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }

    /// Opens an existing `Service`.
    pub fn open(&self) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .open()
                        .map_err(|e| PipelineOpenError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .open()
                        .map_err(|e| PipelineOpenError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }

    /// Opens an existing `Service` with attribute requirements.
    pub fn open_with_attributes(&self, verifier: &AttributeVerifier) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .open_with_attributes(&verifier.0)
                        .map_err(|e| PipelineOpenError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .open_with_attributes(&verifier.0)
                        .map_err(|e| PipelineOpenError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }

    /// Creates a new `Service`.
    pub fn create(&self) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .create()
                        .map_err(|e| PipelineCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .create()
                        .map_err(|e| PipelineCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }

    /// Creates a new `Service` with attributes.
    pub fn create_with_attributes(
        &self,
        attributes: &AttributeSpecifier,
    ) -> PyResult<PortFactoryPipeline> {
        match &self.value {
            ServiceBuilderPipelineType::Ipc(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Ipc(
                    v.clone()
                        .create_with_attributes(&attributes.0)
                        .map_err(|e| PipelineCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
            ServiceBuilderPipelineType::Local(v) => Ok(PortFactoryPipeline::new(
                PortFactoryPipelineType::Local(
                    v.clone()
                        .create_with_attributes(&attributes.0)
                        .map_err(|e| PipelineCreateError::new_err(format!("{e:?}")))?,
                ),
                self.payload_type_details.clone(),
            )),
        }
    }
}
