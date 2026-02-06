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

use iceoryx2::prelude::{CallbackProgression, PortFactory};
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use pyo3::prelude::*;

use crate::attribute_set::AttributeSet;
use crate::error::NodeListFailure;
use crate::node_id::NodeId;
use crate::node_state::{
    AliveNodeView, AliveNodeViewType, DeadNodeView, DeadNodeViewType, NodeState,
};
use crate::parc::Parc;
use crate::port_factory_publisher::PortFactoryPublisher;
use crate::port_factory_subscriber::PortFactorySubscriber;
use crate::service_id::ServiceId;
use crate::service_name::ServiceName;
use crate::static_config_pipeline::StaticConfigPipeline;
use crate::type_storage::TypeStorage;

pub(crate) enum PortFactoryPipelineType {
    Ipc(
        iceoryx2::service::port_factory::pipeline::PortFactory<
            crate::IpcService,
            [CustomPayloadMarker],
            CustomHeaderMarker,
        >,
    ),
    Local(
        iceoryx2::service::port_factory::pipeline::PortFactory<
            crate::LocalService,
            [CustomPayloadMarker],
            CustomHeaderMarker,
        >,
    ),
}

#[pyclass]
/// The factory for `MessagingPattern::Pipeline`.
pub struct PortFactoryPipeline {
    pub(crate) value: Parc<PortFactoryPipelineType>,
    payload_type_details: TypeStorage,
    user_header_type_details: TypeStorage,
}

impl PortFactoryPipeline {
    pub(crate) fn new(
        value: PortFactoryPipelineType,
        payload_type_details: TypeStorage,
        user_header_type_details: TypeStorage,
    ) -> Self {
        Self {
            value: Parc::new(value),
            payload_type_details,
            user_header_type_details,
        }
    }
}

#[pymethods]
impl PortFactoryPipeline {
    #[getter]
    /// Returns the `ServiceName` of the service.
    pub fn name(&self) -> ServiceName {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => ServiceName(*v.name()),
            PortFactoryPipelineType::Local(v) => ServiceName(*v.name()),
        }
    }

    #[getter]
    /// Returns the `ServiceId` of the service.
    pub fn service_id(&self) -> ServiceId {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => ServiceId(*v.service_id()),
            PortFactoryPipelineType::Local(v) => ServiceId(*v.service_id()),
        }
    }

    #[getter]
    /// Returns the `AttributeSet` defined on the service.
    pub fn attributes(&self) -> AttributeSet {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => AttributeSet(v.attributes().clone()),
            PortFactoryPipelineType::Local(v) => AttributeSet(v.attributes().clone()),
        }
    }

    #[getter]
    /// Returns the static configuration of the service.
    pub fn static_config(&self) -> StaticConfigPipeline {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => StaticConfigPipeline(*v.static_config()),
            PortFactoryPipelineType::Local(v) => StaticConfigPipeline(*v.static_config()),
        }
    }

    #[getter]
    /// Returns a list of all nodes which have opened the service.
    pub fn nodes(&self) -> PyResult<Vec<NodeState>> {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => {
                let mut ret_val = vec![];
                v.nodes(|state| {
                    match state {
                        iceoryx2::prelude::NodeState::Alive(n) => {
                            ret_val.push(NodeState::Alive(AliveNodeView(AliveNodeViewType::Ipc(n))))
                        }
                        iceoryx2::prelude::NodeState::Dead(n) => {
                            ret_val.push(NodeState::Dead(DeadNodeView(DeadNodeViewType::Ipc(n))))
                        }
                        iceoryx2::prelude::NodeState::Inaccessible(n) => {
                            ret_val.push(NodeState::Inaccessible(NodeId(n)))
                        }
                        iceoryx2::prelude::NodeState::Undefined(n) => {
                            ret_val.push(NodeState::Undefined(NodeId(n)))
                        }
                    }
                    CallbackProgression::Continue
                })
                .map_err(|e| NodeListFailure::new_err(format!("{e:?}")))?;
                Ok(ret_val)
            }
            PortFactoryPipelineType::Local(v) => {
                let mut ret_val = vec![];
                v.nodes(|state| {
                    match state {
                        iceoryx2::prelude::NodeState::Alive(n) => ret_val
                            .push(NodeState::Alive(AliveNodeView(AliveNodeViewType::Local(n)))),
                        iceoryx2::prelude::NodeState::Dead(n) => {
                            ret_val.push(NodeState::Dead(DeadNodeView(DeadNodeViewType::Local(n))))
                        }
                        iceoryx2::prelude::NodeState::Inaccessible(n) => {
                            ret_val.push(NodeState::Inaccessible(NodeId(n)))
                        }
                        iceoryx2::prelude::NodeState::Undefined(n) => {
                            ret_val.push(NodeState::Undefined(NodeId(n)))
                        }
                    }
                    CallbackProgression::Continue
                })
                .map_err(|e| NodeListFailure::new_err(format!("{e:?}")))?;
                Ok(ret_val)
            }
        }
    }

    /// Returns the amount of worker stages.
    pub fn number_of_stages(&self) -> usize {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => v.number_of_stages(),
            PortFactoryPipelineType::Local(v) => v.number_of_stages(),
        }
    }

    /// Returns the current amount of ingress ports.
    pub fn number_of_ingress_ports(&self) -> usize {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => v.number_of_ingress_ports(),
            PortFactoryPipelineType::Local(v) => v.number_of_ingress_ports(),
        }
    }

    /// Returns the current amount of worker ports at the provided stage.
    pub fn number_of_workers(&self, stage_id: usize) -> Option<usize> {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => v.number_of_workers(stage_id),
            PortFactoryPipelineType::Local(v) => v.number_of_workers(stage_id),
        }
    }

    /// Returns the current amount of egress ports.
    pub fn number_of_egress_ports(&self) -> usize {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => v.number_of_egress_ports(),
            PortFactoryPipelineType::Local(v) => v.number_of_egress_ports(),
        }
    }

    /// Returns a list of ingress node ids.
    pub fn list_ingresses(&self) -> Vec<NodeId> {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => {
                let mut ret_val = vec![];
                v.list_ingresses(|details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                ret_val
            }
            PortFactoryPipelineType::Local(v) => {
                let mut ret_val = vec![];
                v.list_ingresses(|details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                ret_val
            }
        }
    }

    /// Returns a list of worker node ids for the provided stage.
    pub fn list_workers(&self, stage_id: usize) -> Option<Vec<NodeId>> {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => {
                if stage_id >= v.number_of_stages() {
                    return None;
                }

                let mut ret_val = vec![];
                v.list_workers(stage_id, |details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                Some(ret_val)
            }
            PortFactoryPipelineType::Local(v) => {
                if stage_id >= v.number_of_stages() {
                    return None;
                }

                let mut ret_val = vec![];
                v.list_workers(stage_id, |details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                Some(ret_val)
            }
        }
    }

    /// Returns a list of egress node ids.
    pub fn list_egresses(&self) -> Vec<NodeId> {
        match &*self.value.lock() {
            PortFactoryPipelineType::Ipc(v) => {
                let mut ret_val = vec![];
                v.list_egresses(|details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                ret_val
            }
            PortFactoryPipelineType::Local(v) => {
                let mut ret_val = vec![];
                v.list_egresses(|details| {
                    ret_val.push(NodeId(details.node_id));
                    CallbackProgression::Continue
                });
                ret_val
            }
        }
    }

    /// Returns a builder for ingress endpoints.
    pub fn ingress_builder(&self) -> PortFactoryPublisher {
        PortFactoryPublisher::from_pipeline_ingress(
            self.value.clone(),
            self.payload_type_details.clone(),
            self.user_header_type_details.clone(),
        )
    }

    /// Returns a builder for worker input endpoints.
    pub fn worker_subscriber_builder(&self, stage_id: usize) -> Option<PortFactorySubscriber> {
        PortFactorySubscriber::from_pipeline_worker(
            self.value.clone(),
            stage_id,
            self.payload_type_details.clone(),
            self.user_header_type_details.clone(),
        )
    }

    /// Returns a builder for worker output endpoints.
    pub fn worker_publisher_builder(&self, stage_id: usize) -> Option<PortFactoryPublisher> {
        PortFactoryPublisher::from_pipeline_worker(
            self.value.clone(),
            stage_id,
            self.payload_type_details.clone(),
            self.user_header_type_details.clone(),
        )
    }

    /// Returns a builder for egress endpoints.
    pub fn egress_builder(&self) -> PortFactorySubscriber {
        PortFactorySubscriber::from_pipeline_egress(
            self.value.clone(),
            self.payload_type_details.clone(),
            self.user_header_type_details.clone(),
        )
    }

    pub fn __payload_type_details(&self) -> Option<PyObject> {
        self.payload_type_details.clone().value
    }

    pub fn __user_header_type_details(&self) -> Option<PyObject> {
        self.user_header_type_details.clone().value
    }
}
