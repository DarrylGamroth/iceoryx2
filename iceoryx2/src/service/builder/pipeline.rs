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

//! Builder for a staged pipeline communication pattern.
//!
//! The current implementation composes a pipeline from a fixed chain of
//! publish-subscribe services and offers dedicated ingress/worker/egress roles.

extern crate alloc;

use alloc::format;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::marker::PhantomData;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_log::warn;

use crate::node::SharedNode;
use crate::service;
use crate::service::attribute::{AttributeSpecifier, AttributeVerifier};
use crate::service::builder::publish_subscribe::{
    PublishSubscribeCreateError, PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};
use crate::service::port_factory::pipeline;
use crate::service::service_name::{ServiceName, ServiceNameError};

use super::Builder as ServiceBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors when validating pipeline configuration.
pub enum PipelineConfigError {
    /// The service name for one internal pipeline edge is invalid.
    InvalidEdgeServiceName(ServiceNameError),
}

impl core::fmt::Display for PipelineConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PipelineConfigError::{self:?}")
    }
}

impl core::error::Error for PipelineConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failures when opening/creating a pipeline.
pub enum PipelineOpenOrCreateError {
    /// Creating the internal edge service name failed.
    InvalidConfiguration(PipelineConfigError),
    /// One edge publish-subscribe service failed.
    EdgeFailure(PublishSubscribeOpenOrCreateError),
}

impl core::fmt::Display for PipelineOpenOrCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PipelineOpenOrCreateError::{self:?}")
    }
}

impl core::error::Error for PipelineOpenOrCreateError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failures when opening an existing pipeline.
pub enum PipelineOpenError {
    /// Creating the internal edge service name failed.
    InvalidConfiguration(PipelineConfigError),
    /// One edge publish-subscribe service failed.
    EdgeFailure(PublishSubscribeOpenError),
}

impl core::fmt::Display for PipelineOpenError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PipelineOpenError::{self:?}")
    }
}

impl core::error::Error for PipelineOpenError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failures when creating a new pipeline.
pub enum PipelineCreateError {
    /// Creating the internal edge service name failed.
    InvalidConfiguration(PipelineConfigError),
    /// One edge publish-subscribe service failed.
    EdgeFailure(PublishSubscribeCreateError),
}

impl core::fmt::Display for PipelineCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PipelineCreateError::{self:?}")
    }
}

impl core::error::Error for PipelineCreateError {}

#[derive(Debug, Clone)]
/// Builder for a staged pipeline.
pub struct Builder<Payload: Debug + ZeroCopySend + ?Sized, ServiceType: service::Service> {
    name: ServiceName,
    shared_node: Arc<SharedNode<ServiceType>>,
    number_of_stages: usize,
    max_in_flight_samples: usize,
    max_nodes: usize,
    initial_max_slice_len: usize,
    _payload: PhantomData<Payload>,
}

impl<Payload: Debug + ZeroCopySend + ?Sized, ServiceType: service::Service>
    Builder<Payload, ServiceType>
{
    pub(crate) fn new(name: ServiceName, shared_node: Arc<SharedNode<ServiceType>>) -> Self {
        Self {
            name,
            max_in_flight_samples: shared_node
                .config()
                .defaults
                .publish_subscribe
                .subscriber_max_buffer_size,
            max_nodes: shared_node.config().defaults.publish_subscribe.max_nodes,
            initial_max_slice_len: 1,
            number_of_stages: 1,
            shared_node,
            _payload: PhantomData,
        }
    }

    /// Defines the amount of worker stages. Stage ids range from `0..number_of_stages`.
    pub fn number_of_stages(mut self, value: usize) -> Self {
        self.number_of_stages = value;
        self
    }

    /// Defines the bounded amount of in-flight samples per pipeline edge.
    pub fn max_in_flight_samples(mut self, value: usize) -> Self {
        self.max_in_flight_samples = value;
        self
    }

    /// Defines the maximum amount of nodes that can open each internal edge service.
    pub fn max_nodes(mut self, value: usize) -> Self {
        self.max_nodes = value;
        self
    }

    /// Defines the default maximum dynamic slice length used by ingress/worker publishers.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.initial_max_slice_len = value;
        self
    }

    fn prepare(&mut self) {
        if self.number_of_stages == 0 {
            warn!(
                from self,
                "Setting the amount of stages to 0 is not supported. Adjust it to 1."
            );
            self.number_of_stages = 1;
        }

        if self.max_in_flight_samples == 0 {
            warn!(
                from self,
                "Setting max in-flight samples to 0 is not supported. Adjust it to 1."
            );
            self.max_in_flight_samples = 1;
        }

        if self.max_nodes == 0 {
            warn!(from self, "Setting max nodes to 0 is not supported. Adjust it to 1.");
            self.max_nodes = 1;
        }

        if self.initial_max_slice_len == 0 {
            warn!(
                from self,
                "Setting initial max slice length to 0 is not supported. Adjust it to 1."
            );
            self.initial_max_slice_len = 1;
        }
    }

    fn edge_service_name(&self, edge_index: usize) -> Result<ServiceName, PipelineConfigError> {
        let name = format!("{}/__iox2_pipeline_edge_{edge_index}", self.name.as_str());
        ServiceName::new(&name).map_err(PipelineConfigError::InvalidEdgeServiceName)
    }

    fn configure_edge_builder<EdgePayload: Debug + ZeroCopySend + ?Sized>(
        &self,
        edge_service_name: &ServiceName,
    ) -> super::publish_subscribe::Builder<EdgePayload, (), ServiceType> {
        ServiceBuilder::new(edge_service_name, self.shared_node.clone())
            .publish_subscribe::<EdgePayload>()
            .max_publishers(1)
            .max_subscribers(1)
            .history_size(0)
            .subscriber_max_buffer_size(self.max_in_flight_samples)
            .subscriber_max_borrowed_samples(self.max_in_flight_samples)
            .max_nodes(self.max_nodes)
    }
}

impl<Payload: Debug + ZeroCopySend + 'static, ServiceType: service::Service>
    Builder<Payload, ServiceType>
{
    /// Opens an existing pipeline service chain or creates it when missing.
    pub fn open_or_create(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenOrCreateError> {
        self.open_or_create_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain or creates it when missing with attributes.
    pub fn open_or_create_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenOrCreateError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineOpenOrCreateError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name);
            let edge_factory = stage
                .open_or_create_with_attributes(verifier)
                .map_err(PipelineOpenOrCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }

    /// Opens an existing pipeline service chain.
    pub fn open(self) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenError> {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain with attribute requirements.
    pub fn open_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineOpenError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name);
            let edge_factory = stage
                .open_with_attributes(verifier)
                .map_err(PipelineOpenError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }

    /// Creates a new pipeline service chain.
    pub fn create(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineCreateError> {
        self.create_with_attributes(&AttributeSpecifier::new())
    }

    /// Creates a new pipeline service chain with attributes.
    pub fn create_with_attributes(
        mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineCreateError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineCreateError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name);
            let edge_factory = stage
                .create_with_attributes(attributes)
                .map_err(PipelineCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }
}

impl<Payload: Debug + ZeroCopySend + 'static, ServiceType: service::Service>
    Builder<[Payload], ServiceType>
{
    /// Opens an existing pipeline service chain or creates it when missing.
    pub fn open_or_create(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenOrCreateError> {
        self.open_or_create_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain or creates it when missing with attributes.
    pub fn open_or_create_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenOrCreateError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineOpenOrCreateError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<[Payload]>(&edge_service_name);
            let edge_factory = stage
                .open_or_create_with_attributes(verifier)
                .map_err(PipelineOpenOrCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }

    /// Opens an existing pipeline service chain.
    pub fn open(self) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenError> {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain with attribute requirements.
    pub fn open_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineOpenError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<[Payload]>(&edge_service_name);
            let edge_factory = stage
                .open_with_attributes(verifier)
                .map_err(PipelineOpenError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }

    /// Creates a new pipeline service chain.
    pub fn create(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineCreateError> {
        self.create_with_attributes(&AttributeSpecifier::new())
    }

    /// Creates a new pipeline service chain with attributes.
    pub fn create_with_attributes(
        mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineCreateError> {
        self.prepare();
        let mut edges = Vec::with_capacity(self.number_of_stages + 1);

        for edge_index in 0..=self.number_of_stages {
            let edge_service_name = self
                .edge_service_name(edge_index)
                .map_err(PipelineCreateError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<[Payload]>(&edge_service_name);
            let edge_factory = stage
                .create_with_attributes(attributes)
                .map_err(PipelineCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(pipeline::PortFactory::new(
            self.name,
            self.number_of_stages,
            self.initial_max_slice_len,
            edges,
        ))
    }
}
