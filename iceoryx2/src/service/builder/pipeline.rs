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

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;
use core::any::TypeId;
use core::fmt::Debug;
use core::marker::PhantomData;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::dynamic_storage::{DynamicStorageCreateError, DynamicStorageOpenError};
use iceoryx2_cal::serialize::Serialize;
use iceoryx2_cal::static_storage::{StaticStorage, StaticStorageLocked};
use iceoryx2_log::{fail, warn};

use crate::service;
use crate::service::attribute::{AttributeSpecifier, AttributeVerifier};
use crate::service::builder::publish_subscribe::{
    PublishSubscribeCreateError, PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};
use crate::service::dynamic_config::pipeline::DynamicConfigSettings;
use crate::service::dynamic_config::MessagingPatternSettings;
use crate::service::port_factory::pipeline;
use crate::service::service_name::{ServiceName, ServiceNameError};
use crate::service::static_config;
use crate::service::static_config::message_type_details::{TypeDetail, TypeVariant};
use crate::service::static_config::messaging_pattern::MessagingPattern;
use crate::service::{dynamic_config, NoResource};

use super::{
    Builder as ServiceBuilder, BuilderWithServiceType, CustomPayloadMarker, OpenDynamicStorageFailure,
    ServiceState, RETRY_LIMIT,
};

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
/// Failures when opening an existing pipeline.
pub enum PipelineOpenError {
    /// The [`Service`] does not exist.
    DoesNotExist,
    /// The process has insufficient permissions to open the [`Service`].
    InsufficientPermissions,
    /// Some underlying resources of the [`Service`] do not exist which indicates a corrupted
    /// [`Service`] state.
    ServiceInCorruptedState,
    /// The [`Service`] has the wrong messaging pattern.
    IncompatibleMessagingPattern,
    /// The [`AttributeVerifier`] required attributes that the [`Service`] does not satisfy.
    IncompatibleAttributes,
    /// The payload type of the existing [`Service`] is not compatible.
    IncompatiblePayloadType,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    HangsInCreation,
    /// The [`Service`] supports fewer [`Node`](crate::node::Node)s than requested.
    DoesNotSupportRequestedAmountOfNodes,
    /// The [`Service`] was created with a different amount of stages.
    DoesNotSupportRequestedAmountOfStages,
    /// The [`Service`] supports fewer in-flight samples than requested.
    DoesNotSupportRequestedInFlightSamples,
    /// The [`Service`] supports a smaller initial max slice length than requested.
    DoesNotSupportRequestedInitialMaxSliceLen,
    /// The maximum number of [`Node`](crate::node::Node)s already opened the [`Service`].
    ExceedsMaxNumberOfNodes,
    /// The [`Service`] is marked for destruction.
    IsMarkedForDestruction,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
    /// Creating one internal edge service name failed.
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

impl From<ServiceState> for PipelineOpenError {
    fn from(value: ServiceState) -> Self {
        match value {
            ServiceState::IncompatibleMessagingPattern => PipelineOpenError::IncompatibleMessagingPattern,
            ServiceState::InsufficientPermissions => PipelineOpenError::InsufficientPermissions,
            ServiceState::HangsInCreation => PipelineOpenError::HangsInCreation,
            ServiceState::Corrupted => PipelineOpenError::ServiceInCorruptedState,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failures when creating a new pipeline.
pub enum PipelineCreateError {
    /// Some underlying resources of the [`Service`] are either missing, corrupted or inaccessible.
    ServiceInCorruptedState,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
    /// Multiple processes are trying to create the same [`Service`].
    IsBeingCreatedByAnotherInstance,
    /// The [`Service`] already exists.
    AlreadyExists,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    HangsInCreation,
    /// The process has insufficient permissions to create the [`Service`].
    InsufficientPermissions,
    /// Creating one internal edge service name failed.
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

impl From<ServiceState> for PipelineCreateError {
    fn from(value: ServiceState) -> Self {
        match value {
            ServiceState::IncompatibleMessagingPattern => PipelineCreateError::AlreadyExists,
            ServiceState::InsufficientPermissions => PipelineCreateError::InsufficientPermissions,
            ServiceState::HangsInCreation => PipelineCreateError::HangsInCreation,
            ServiceState::Corrupted => PipelineCreateError::ServiceInCorruptedState,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failures when opening/creating a pipeline.
pub enum PipelineOpenOrCreateError {
    /// Failures that can occur when a pipeline [`Service`] is opened.
    PipelineOpenError(PipelineOpenError),
    /// Failures that can occur when a pipeline [`Service`] is created.
    PipelineCreateError(PipelineCreateError),
    /// Can occur when another process creates and removes the same [`Service`] repeatedly with a
    /// high frequency.
    SystemInFlux,
}

impl From<PipelineOpenError> for PipelineOpenOrCreateError {
    fn from(value: PipelineOpenError) -> Self {
        PipelineOpenOrCreateError::PipelineOpenError(value)
    }
}

impl From<PipelineCreateError> for PipelineOpenOrCreateError {
    fn from(value: PipelineCreateError) -> Self {
        PipelineOpenOrCreateError::PipelineCreateError(value)
    }
}

impl From<ServiceState> for PipelineOpenOrCreateError {
    fn from(value: ServiceState) -> Self {
        PipelineOpenOrCreateError::PipelineOpenError(value.into())
    }
}

impl core::fmt::Display for PipelineOpenOrCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PipelineOpenOrCreateError::{self:?}")
    }
}

impl core::error::Error for PipelineOpenOrCreateError {}

#[derive(Debug)]
/// Builder for a staged pipeline.
pub struct Builder<Payload: Debug + ZeroCopySend + ?Sized, ServiceType: service::Service> {
    base: BuilderWithServiceType<ServiceType>,
    verify_number_of_stages: bool,
    verify_max_in_flight_samples: bool,
    verify_max_nodes: bool,
    verify_initial_max_slice_len: bool,
    override_payload_type: Option<TypeDetail>,
    _payload: PhantomData<Payload>,
}

impl<Payload: Debug + ZeroCopySend + ?Sized, ServiceType: service::Service> Clone
    for Builder<Payload, ServiceType>
{
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            verify_number_of_stages: self.verify_number_of_stages,
            verify_max_in_flight_samples: self.verify_max_in_flight_samples,
            verify_max_nodes: self.verify_max_nodes,
            verify_initial_max_slice_len: self.verify_initial_max_slice_len,
            override_payload_type: self.override_payload_type,
            _payload: PhantomData,
        }
    }
}

impl<Payload: Debug + ZeroCopySend + ?Sized, ServiceType: service::Service>
    Builder<Payload, ServiceType>
{
    pub(crate) fn new(base: BuilderWithServiceType<ServiceType>) -> Self {
        let mut new_self = Self {
            base,
            verify_number_of_stages: false,
            verify_max_in_flight_samples: false,
            verify_max_nodes: false,
            verify_initial_max_slice_len: false,
            override_payload_type: None,
            _payload: PhantomData,
        };

        new_self.base.service_config.messaging_pattern = MessagingPattern::Pipeline(
            static_config::pipeline::StaticConfig::new(new_self.base.shared_node.config()),
        );

        new_self
    }

    fn config_details(&self) -> &static_config::pipeline::StaticConfig {
        self.base.service_config.pipeline()
    }

    fn config_details_mut(&mut self) -> &mut static_config::pipeline::StaticConfig {
        self.base.service_config.pipeline_mut()
    }

    /// Defines the amount of worker stages. Stage ids range from `0..number_of_stages`.
    pub fn number_of_stages(mut self, value: usize) -> Self {
        self.verify_number_of_stages = true;
        self.config_details_mut().number_of_stages = value;
        self
    }

    /// Defines the bounded amount of in-flight samples per pipeline edge.
    pub fn max_in_flight_samples(mut self, value: usize) -> Self {
        self.verify_max_in_flight_samples = true;
        self.config_details_mut().max_in_flight_samples = value;
        self
    }

    /// Defines the maximum amount of nodes that can open the pipeline service.
    pub fn max_nodes(mut self, value: usize) -> Self {
        self.verify_max_nodes = true;
        self.config_details_mut().max_nodes = value;
        self
    }

    /// Defines the default maximum dynamic slice length used by ingress/worker publishers.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.verify_initial_max_slice_len = true;
        self.config_details_mut().initial_max_slice_len = value;
        self
    }

    fn prepare(&mut self) {
        if self.config_details().number_of_stages == 0 {
            warn!(
                from self,
                "Setting the amount of stages to 0 is not supported. Adjust it to 1."
            );
            self.config_details_mut().number_of_stages = 1;
        }

        if self.config_details().max_in_flight_samples == 0 {
            warn!(
                from self,
                "Setting max in-flight samples to 0 is not supported. Adjust it to 1."
            );
            self.config_details_mut().max_in_flight_samples = 1;
        }

        if self.config_details().max_nodes == 0 {
            warn!(from self, "Setting max nodes to 0 is not supported. Adjust it to 1.");
            self.config_details_mut().max_nodes = 1;
        }

        if self.config_details().initial_max_slice_len == 0 {
            warn!(
                from self,
                "Setting initial max slice length to 0 is not supported. Adjust it to 1."
            );
            self.config_details_mut().initial_max_slice_len = 1;
        }
    }

    fn edge_service_name(
        service_name: &ServiceName,
        edge_index: usize,
    ) -> Result<ServiceName, PipelineConfigError> {
        let name = format!(
            "{}/__iox2_pipeline_edge_{edge_index}",
            service_name.as_str()
        );
        ServiceName::new(&name).map_err(PipelineConfigError::InvalidEdgeServiceName)
    }

    fn configure_edge_builder<EdgePayload: Debug + ZeroCopySend + ?Sized>(
        &self,
        edge_service_name: &ServiceName,
        settings: &static_config::pipeline::StaticConfig,
    ) -> super::publish_subscribe::Builder<EdgePayload, (), ServiceType> {
        ServiceBuilder::new(edge_service_name, self.base.shared_node.clone())
            .publish_subscribe::<EdgePayload>()
            .max_publishers(1)
            .max_subscribers(1)
            .history_size(0)
            .subscriber_max_buffer_size(settings.max_in_flight_samples())
            .subscriber_max_borrowed_samples(settings.max_in_flight_samples())
            .max_nodes(settings.max_nodes())
    }

    fn verify_type_compatibility(
        &self,
        existing: &TypeDetail,
    ) -> Result<(), PipelineOpenError> {
        let requested = self.config_details().payload_type_details;
        if requested.type_name() != existing.type_name()
            || requested.variant() != existing.variant()
            || requested.size() != existing.size()
            || requested.alignment() > existing.alignment()
        {
            fail!(from self, with PipelineOpenError::IncompatiblePayloadType,
                "Unable to open pipeline service since the service offers the payload type \"{:?}\" which is not compatible to the requested payload type \"{:?}\".",
                existing, requested);
        }

        Ok(())
    }

    fn verify_service_configuration(
        &self,
        static_config: &static_config::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<static_config::pipeline::StaticConfig, PipelineOpenError> {
        let msg = "Unable to open pipeline service";

        if let Err(incompatible_key) = verifier.verify_requirements(static_config.attributes()) {
            fail!(from self, with PipelineOpenError::IncompatibleAttributes,
                "{msg} since the service does not satisfy the required attribute key {:?}.",
                incompatible_key);
        }

        let existing_settings = match static_config.messaging_pattern() {
            MessagingPattern::Pipeline(ref v) => v,
            pattern => {
                fail!(from self, with PipelineOpenError::IncompatibleMessagingPattern,
                    "{msg} since a service with the messaging pattern {:?} exists but MessagingPattern::Pipeline is required.",
                    pattern);
            }
        };

        self.verify_type_compatibility(existing_settings.payload_type_details())?;

        let required_settings = self.config_details();

        if self.verify_number_of_stages
            && existing_settings.number_of_stages() != required_settings.number_of_stages()
        {
            fail!(from self, with PipelineOpenError::DoesNotSupportRequestedAmountOfStages,
                "{msg} since the service was created with {} stages but {} are required.",
                existing_settings.number_of_stages(), required_settings.number_of_stages());
        }

        if self.verify_max_in_flight_samples
            && existing_settings.max_in_flight_samples() < required_settings.max_in_flight_samples()
        {
            fail!(from self, with PipelineOpenError::DoesNotSupportRequestedInFlightSamples,
                "{msg} since the service supports {} in-flight samples but {} are required.",
                existing_settings.max_in_flight_samples(), required_settings.max_in_flight_samples());
        }

        if self.verify_max_nodes && existing_settings.max_nodes() < required_settings.max_nodes() {
            fail!(from self, with PipelineOpenError::DoesNotSupportRequestedAmountOfNodes,
                "{msg} since the service supports {} nodes but {} are required.",
                existing_settings.max_nodes(), required_settings.max_nodes());
        }

        if self.verify_initial_max_slice_len
            && existing_settings.initial_max_slice_len() < required_settings.initial_max_slice_len()
        {
            fail!(from self, with PipelineOpenError::DoesNotSupportRequestedInitialMaxSliceLen,
                "{msg} since the service supports an initial max slice length of {} but {} is required.",
                existing_settings.initial_max_slice_len(), required_settings.initial_max_slice_len());
        }

        Ok(*existing_settings)
    }

    fn create_pipeline_service(
        &mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<service::ServiceState<ServiceType, NoResource>, PipelineCreateError> {
        let msg = "Unable to create pipeline service";

        match self.base.is_service_available(msg)? {
            Some(_) => {
                fail!(from self, with PipelineCreateError::AlreadyExists,
                    "{msg} since the service already exists.");
            }
            None => {
                let service_tag = self
                    .base
                    .create_node_service_tag(msg, PipelineCreateError::InternalFailure)?;

                let static_config = match self.base.create_static_config_storage() {
                    Ok(c) => c,
                    Err(service::StaticStorageCreateError::AlreadyExists) => {
                        fail!(from self, with PipelineCreateError::AlreadyExists,
                           "{msg} since the service already exists.");
                    }
                    Err(service::StaticStorageCreateError::Creation) => {
                        fail!(from self, with PipelineCreateError::IsBeingCreatedByAnotherInstance,
                            "{msg} since the service is being created by another instance.");
                    }
                    Err(service::StaticStorageCreateError::InsufficientPermissions) => {
                        fail!(from self, with PipelineCreateError::InsufficientPermissions,
                            "{msg} since the static service information could not be created due to insufficient permissions.");
                    }
                    Err(e) => {
                        fail!(from self, with PipelineCreateError::InternalFailure,
                            "{msg} since the static service information could not be created due to an internal failure ({:?}).", e);
                    }
                };

                let dynamic_config_setting = DynamicConfigSettings;
                let dynamic_config = match self.base.create_dynamic_config_storage(
                    &MessagingPatternSettings::Pipeline(dynamic_config_setting),
                    dynamic_config::pipeline::DynamicConfig::memory_size(&dynamic_config_setting),
                    self.config_details().max_nodes(),
                ) {
                    Ok(dynamic_config) => dynamic_config,
                    Err(DynamicStorageCreateError::AlreadyExists) => {
                        fail!(from self, with PipelineCreateError::ServiceInCorruptedState,
                            "{msg} since the dynamic config of a previous instance of the service still exists.");
                    }
                    Err(e) => {
                        fail!(from self, with PipelineCreateError::InternalFailure,
                            "{msg} since the dynamic service segment could not be created ({:?}).", e);
                    }
                };

                self.base.service_config.attributes = attributes.0.clone();
                let service_config = fail!(from self,
                    when ServiceType::ConfigSerializer::serialize(&self.base.service_config),
                    with PipelineCreateError::ServiceInCorruptedState,
                    "{msg} since the configuration could not be serialized.");

                let unlocked_static_details = fail!(from self,
                    when static_config.unlock(service_config.as_slice()),
                    with PipelineCreateError::ServiceInCorruptedState,
                    "{msg} since the configuration could not be written to the static storage.");

                unlocked_static_details.release_ownership();
                if let Some(service_tag) = service_tag {
                    service_tag.release_ownership();
                }

                Ok(service::ServiceState::new(
                    self.base.service_config.clone(),
                    self.base.shared_node.clone(),
                    dynamic_config,
                    unlocked_static_details,
                    NoResource,
                ))
            }
        }
    }

    fn open_pipeline_service(
        &mut self,
        verifier: &AttributeVerifier,
    ) -> Result<service::ServiceState<ServiceType, NoResource>, PipelineOpenError> {
        let msg = "Unable to open pipeline service";

        let mut service_open_retry_count = 0;
        loop {
            match self.base.is_service_available(msg)? {
                None => {
                    fail!(from self, with PipelineOpenError::DoesNotExist,
                        "{msg} since the service does not exist.");
                }
                Some((static_config, static_storage)) => {
                    let pipeline_static_config =
                        self.verify_service_configuration(&static_config, verifier)?;

                    let service_tag = self
                        .base
                        .create_node_service_tag(msg, PipelineOpenError::InternalFailure)?;

                    let dynamic_config = match self.base.open_dynamic_config_storage() {
                        Ok(v) => v,
                        Err(OpenDynamicStorageFailure::IsMarkedForDestruction) => {
                            fail!(from self, with PipelineOpenError::IsMarkedForDestruction,
                                "{msg} since the service is marked for destruction.");
                        }
                        Err(OpenDynamicStorageFailure::ExceedsMaxNumberOfNodes) => {
                            fail!(from self, with PipelineOpenError::ExceedsMaxNumberOfNodes,
                                "{msg} since it would exceed the maximum number of supported nodes.");
                        }
                        Err(OpenDynamicStorageFailure::DynamicStorageOpenError(
                            DynamicStorageOpenError::DoesNotExist,
                        )) => {
                            fail!(from self, with PipelineOpenError::ServiceInCorruptedState,
                                "{msg} since the dynamic segment of the service is missing.");
                        }
                        Err(e) => {
                            if self.base.is_service_available(msg)?.is_none() {
                                fail!(from self, with PipelineOpenError::DoesNotExist,
                                    "{msg} since the service does not exist.");
                            }

                            service_open_retry_count += 1;
                            if RETRY_LIMIT < service_open_retry_count {
                                fail!(from self, with PipelineOpenError::ServiceInCorruptedState,
                                    "{msg} since the dynamic service information could not be opened ({:?}). This could indicate a corrupted system or a misconfigured system where services are created/removed with a high frequency.",
                                    e);
                            }

                            continue;
                        }
                    };

                    self.base.service_config.messaging_pattern =
                        MessagingPattern::Pipeline(pipeline_static_config);

                    if let Some(service_tag) = service_tag {
                        service_tag.release_ownership();
                    }

                    return Ok(service::ServiceState::new(
                        static_config,
                        self.base.shared_node.clone(),
                        dynamic_config,
                        static_storage,
                        NoResource,
                    ));
                }
            }
        }
    }

    fn open_or_create_pipeline_service(
        &mut self,
        verifier: &AttributeVerifier,
    ) -> Result<service::ServiceState<ServiceType, NoResource>, PipelineOpenOrCreateError> {
        let msg = "Unable to open or create pipeline service";

        let mut retry_count = 0;
        loop {
            if RETRY_LIMIT < retry_count {
                fail!(from self, with PipelineOpenOrCreateError::SystemInFlux,
                      "{msg} since an instance is creating and removing the same service repeatedly.");
            }
            retry_count += 1;

            match self.base.is_service_available(msg)? {
                Some(_) => match self.open_pipeline_service(verifier) {
                    Ok(factory) => return Ok(factory),
                    Err(PipelineOpenError::DoesNotExist) => continue,
                    Err(e) => return Err(e.into()),
                },
                None => {
                    match self
                        .create_pipeline_service(&AttributeSpecifier(verifier.required_attributes().clone()))
                    {
                        Ok(factory) => return Ok(factory),
                        Err(PipelineCreateError::AlreadyExists)
                        | Err(PipelineCreateError::IsBeingCreatedByAnotherInstance) => {
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }
}

impl<Payload: Debug + ZeroCopySend + 'static, ServiceType: service::Service>
    Builder<Payload, ServiceType>
{
    fn prepare_config_details(&mut self) {
        if let Some(details) = &self.override_payload_type {
            self.config_details_mut().payload_type_details = *details;
        } else {
            self.config_details_mut().payload_type_details = TypeDetail::new::<Payload>(TypeVariant::FixedSize);
        }
    }

    fn create_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        attributes: &AttributeSpecifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, Payload, ()>>,
        PipelineCreateError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(PipelineCreateError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name, settings);
            let edge_factory = stage
                .create_with_attributes(attributes)
                .map_err(PipelineCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

    fn open_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, Payload, ()>>,
        PipelineOpenError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(PipelineOpenError::InvalidConfiguration)?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name, settings);
            let edge_factory = stage
                .open_with_attributes(verifier)
                .map_err(PipelineOpenError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

    fn open_or_create_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, Payload, ()>>,
        PipelineOpenOrCreateError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(|e| PipelineOpenOrCreateError::PipelineCreateError(PipelineCreateError::InvalidConfiguration(e)))?;
            let stage = self.configure_edge_builder::<Payload>(&edge_service_name, settings);
            let edge_factory = stage
                .open_or_create_with_attributes(verifier)
                .map_err(|e| match e {
                    PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(v) => {
                        PipelineOpenOrCreateError::PipelineOpenError(PipelineOpenError::EdgeFailure(v))
                    }
                    PublishSubscribeOpenOrCreateError::PublishSubscribeCreateError(v) => {
                        PipelineOpenOrCreateError::PipelineCreateError(PipelineCreateError::EdgeFailure(v))
                    }
                    PublishSubscribeOpenOrCreateError::SystemInFlux => {
                        PipelineOpenOrCreateError::SystemInFlux
                    }
                })?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

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
        self.prepare_config_details();

        let service_state = self.open_or_create_pipeline_service(verifier)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.open_or_create_edges(&settings, verifier)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
    }

    /// Opens an existing pipeline service chain.
    pub fn open(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenError> {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain with attribute requirements.
    pub fn open_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, Payload>, PipelineOpenError> {
        self.prepare();
        self.prepare_config_details();

        let service_state = self.open_pipeline_service(verifier)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.open_edges(&settings, verifier)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
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
        self.prepare_config_details();

        let service_state = self.create_pipeline_service(attributes)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.create_edges(&settings, attributes)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
    }
}

impl<Payload: Debug + ZeroCopySend + 'static, ServiceType: service::Service>
    Builder<[Payload], ServiceType>
{
    fn maybe_apply_payload_type_override(
        &self,
        stage: super::publish_subscribe::Builder<[Payload], (), ServiceType>,
    ) -> super::publish_subscribe::Builder<[Payload], (), ServiceType> {
        if TypeId::of::<Payload>() != TypeId::of::<CustomPayloadMarker>() {
            return stage;
        }

        let stage = unsafe {
            core::mem::transmute::<
                super::publish_subscribe::Builder<[Payload], (), ServiceType>,
                super::publish_subscribe::Builder<[CustomPayloadMarker], (), ServiceType>,
            >(stage)
        };

        let stage = if let Some(details) = self.override_payload_type.as_ref() {
            unsafe { stage.__internal_set_payload_type_details(details) }
        } else {
            stage
        };

        unsafe {
            core::mem::transmute::<
                super::publish_subscribe::Builder<[CustomPayloadMarker], (), ServiceType>,
                super::publish_subscribe::Builder<[Payload], (), ServiceType>,
            >(stage)
        }
    }

    fn prepare_config_details(&mut self) {
        if let Some(details) = &self.override_payload_type {
            self.config_details_mut().payload_type_details = *details;
        } else {
            self.config_details_mut().payload_type_details = TypeDetail::new::<Payload>(TypeVariant::Dynamic);
        }
    }

    fn create_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        attributes: &AttributeSpecifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, [Payload], ()>>,
        PipelineCreateError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(PipelineCreateError::InvalidConfiguration)?;
            let stage = self.maybe_apply_payload_type_override(
                self.configure_edge_builder::<[Payload]>(&edge_service_name, settings),
            );
            let edge_factory = stage
                .create_with_attributes(attributes)
                .map_err(PipelineCreateError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

    fn open_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, [Payload], ()>>,
        PipelineOpenError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(PipelineOpenError::InvalidConfiguration)?;
            let stage = self.maybe_apply_payload_type_override(
                self.configure_edge_builder::<[Payload]>(&edge_service_name, settings),
            );
            let edge_factory = stage
                .open_with_attributes(verifier)
                .map_err(PipelineOpenError::EdgeFailure)?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

    fn open_or_create_edges(
        &self,
        settings: &static_config::pipeline::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<
        Vec<crate::service::port_factory::publish_subscribe::PortFactory<ServiceType, [Payload], ()>>,
        PipelineOpenOrCreateError,
    > {
        let mut edges = Vec::with_capacity(settings.number_of_stages() + 1);

        for edge_index in 0..=settings.number_of_stages() {
            let edge_service_name = Self::edge_service_name(self.base.service_config.name(), edge_index)
                .map_err(|e| PipelineOpenOrCreateError::PipelineCreateError(PipelineCreateError::InvalidConfiguration(e)))?;
            let stage = self.maybe_apply_payload_type_override(
                self.configure_edge_builder::<[Payload]>(&edge_service_name, settings),
            );
            let edge_factory = stage
                .open_or_create_with_attributes(verifier)
                .map_err(|e| match e {
                    PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(v) => {
                        PipelineOpenOrCreateError::PipelineOpenError(PipelineOpenError::EdgeFailure(v))
                    }
                    PublishSubscribeOpenOrCreateError::PublishSubscribeCreateError(v) => {
                        PipelineOpenOrCreateError::PipelineCreateError(PipelineCreateError::EdgeFailure(v))
                    }
                    PublishSubscribeOpenOrCreateError::SystemInFlux => {
                        PipelineOpenOrCreateError::SystemInFlux
                    }
                })?;
            edges.push(edge_factory);
        }

        Ok(edges)
    }

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
        self.prepare_config_details();

        let service_state = self.open_or_create_pipeline_service(verifier)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.open_or_create_edges(&settings, verifier)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
    }

    /// Opens an existing pipeline service chain.
    pub fn open(
        self,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenError> {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing pipeline service chain with attribute requirements.
    pub fn open_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<pipeline::PortFactory<ServiceType, [Payload]>, PipelineOpenError> {
        self.prepare();
        self.prepare_config_details();

        let service_state = self.open_pipeline_service(verifier)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.open_edges(&settings, verifier)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
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
        self.prepare_config_details();

        let service_state = self.create_pipeline_service(attributes)?;
        let settings = *service_state.static_config.pipeline();
        let edges = self.create_edges(&settings, attributes)?;

        Ok(pipeline::PortFactory::new(service_state, edges))
    }
}

impl<ServiceType: service::Service> Builder<[CustomPayloadMarker], ServiceType> {
    #[doc(hidden)]
    pub unsafe fn __internal_set_payload_type_details(mut self, value: &TypeDetail) -> Self {
        self.override_payload_type = Some(*value);
        self
    }
}
