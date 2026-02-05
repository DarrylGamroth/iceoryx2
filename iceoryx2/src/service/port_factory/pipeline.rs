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

//! Pipeline port factory and role specific endpoint builders.

extern crate alloc;

use alloc::vec::Vec;
use core::fmt::Debug;

use iceoryx2_bb_elementary::CallbackProgression;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::port::publisher::{Publisher, PublisherCreateError};
use crate::port::subscriber::{Subscriber, SubscriberCreateError};
use crate::port::unable_to_deliver_strategy::UnableToDeliverStrategy;
use crate::port::{LoanError, ReceiveError, SendError};
use crate::prelude::AllocationStrategy;
use crate::sample_mut::SampleMut;
use crate::service::dynamic_config::publish_subscribe::{PublisherDetails, SubscriberDetails};
use crate::service::port_factory::publish_subscribe;
use crate::service::port_factory::PortFactory as _;
use crate::service::service_name::ServiceName;
use crate::service::Service;

#[derive(Debug)]
/// Pipeline factory built from a fixed chain of internal publish-subscribe edges.
pub struct PortFactory<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> {
    name: ServiceName,
    number_of_stages: usize,
    initial_max_slice_len: usize,
    edges: Vec<publish_subscribe::PortFactory<ServiceType, Payload, ()>>,
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static>
    PortFactory<ServiceType, Payload>
{
    pub(crate) fn new(
        name: ServiceName,
        number_of_stages: usize,
        initial_max_slice_len: usize,
        edges: Vec<publish_subscribe::PortFactory<ServiceType, Payload, ()>>,
    ) -> Self {
        Self {
            name,
            number_of_stages,
            initial_max_slice_len,
            edges,
        }
    }

    /// Returns the base pipeline service name.
    pub fn name(&self) -> &ServiceName {
        &self.name
    }

    /// Returns the number of worker stages.
    pub fn number_of_stages(&self) -> usize {
        self.number_of_stages
    }

    /// Returns a builder for ingress endpoints.
    pub fn ingress_builder(&self) -> IngressBuilder<'_, ServiceType, Payload> {
        IngressBuilder::new(self)
    }

    /// Returns a builder for workers assigned to a stage.
    pub fn worker_builder(&self, stage_id: usize) -> WorkerBuilder<'_, ServiceType, Payload> {
        WorkerBuilder::new(self, stage_id)
    }

    /// Returns a builder for egress endpoints.
    pub fn egress_builder(&self) -> EgressBuilder<'_, ServiceType, Payload> {
        EgressBuilder::new(self)
    }

    /// Returns the current amount of ingress ports.
    pub fn number_of_ingress_ports(&self) -> usize {
        self.edges[0].dynamic_config().number_of_publishers()
    }

    /// Returns the current amount of worker ports at `stage_id`.
    pub fn number_of_workers(&self, stage_id: usize) -> Option<usize> {
        if stage_id >= self.number_of_stages {
            return None;
        }
        Some(
            self.edges[stage_id]
                .dynamic_config()
                .number_of_subscribers(),
        )
    }

    /// Returns the current amount of egress ports.
    pub fn number_of_egress_ports(&self) -> usize {
        self.edges[self.number_of_stages]
            .dynamic_config()
            .number_of_subscribers()
    }

    /// Iterates over ingress publisher details.
    pub fn list_ingresses<F: FnMut(&PublisherDetails) -> CallbackProgression>(&self, callback: F) {
        self.edges[0].dynamic_config().list_publishers(callback);
    }

    /// Iterates over worker details for `stage_id`.
    pub fn list_workers<F: FnMut(&SubscriberDetails) -> CallbackProgression>(
        &self,
        stage_id: usize,
        mut callback: F,
    ) {
        if stage_id < self.number_of_stages {
            self.edges[stage_id]
                .dynamic_config()
                .list_subscribers(|details| callback(details));
        }
    }

    /// Iterates over egress subscriber details.
    pub fn list_egresses<F: FnMut(&SubscriberDetails) -> CallbackProgression>(&self, callback: F) {
        self.edges[self.number_of_stages]
            .dynamic_config()
            .list_subscribers(callback);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors when creating an ingress port.
pub enum IngressCreateError {
    /// Forwarded publisher creation failure.
    PublisherCreateFailure(PublisherCreateError),
}

impl core::fmt::Display for IngressCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "IngressCreateError::{self:?}")
    }
}

impl core::error::Error for IngressCreateError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors when creating a worker port.
pub enum WorkerCreateError {
    /// The provided stage id is out of bounds.
    StageOutOfBounds,
    /// Forwarded subscriber creation failure.
    SubscriberCreateFailure(SubscriberCreateError),
    /// Forwarded publisher creation failure.
    PublisherCreateFailure(PublisherCreateError),
}

impl core::fmt::Display for WorkerCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "WorkerCreateError::{self:?}")
    }
}

impl core::error::Error for WorkerCreateError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors when creating an egress port.
pub enum EgressCreateError {
    /// Forwarded subscriber creation failure.
    SubscriberCreateFailure(SubscriberCreateError),
}

impl core::fmt::Display for EgressCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "EgressCreateError::{self:?}")
    }
}

impl core::error::Error for EgressCreateError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors during worker receive-to-forward preparation.
pub enum WorkerReceiveError {
    /// The incoming sample could not be received.
    ReceiveFailure(ReceiveError),
    /// Outgoing sample loan failed.
    LoanFailure(LoanError),
}

impl core::fmt::Display for WorkerReceiveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "WorkerReceiveError::{self:?}")
    }
}

impl core::error::Error for WorkerReceiveError {}

#[derive(Debug)]
/// Ingress endpoint.
pub struct Ingress<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> {
    publisher: Publisher<ServiceType, Payload, ()>,
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> core::ops::Deref
    for Ingress<ServiceType, Payload>
{
    type Target = Publisher<ServiceType, Payload, ()>;

    fn deref(&self) -> &Self::Target {
        &self.publisher
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> core::ops::DerefMut
    for Ingress<ServiceType, Payload>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.publisher
    }
}

#[derive(Debug)]
/// Egress endpoint.
pub struct Egress<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> {
    subscriber: Subscriber<ServiceType, Payload, ()>,
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> core::ops::Deref
    for Egress<ServiceType, Payload>
{
    type Target = Subscriber<ServiceType, Payload, ()>;

    fn deref(&self) -> &Self::Target {
        &self.subscriber
    }
}

#[derive(Debug)]
/// Worker endpoint.
pub struct Worker<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> {
    subscriber: Subscriber<ServiceType, Payload, ()>,
    publisher: Publisher<ServiceType, Payload, ()>,
}

#[derive(Debug)]
/// Mutable work item created from a worker receive operation.
pub struct WorkMut<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static> {
    sample: SampleMut<ServiceType, Payload, ()>,
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static>
    WorkMut<ServiceType, Payload>
{
    /// Returns a mutable payload reference.
    pub fn payload_mut(&mut self) -> &mut Payload {
        self.sample.payload_mut()
    }

    /// Sends the updated payload to the next stage.
    pub fn send(self) -> Result<usize, SendError> {
        self.sample.send()
    }
}

#[derive(Debug)]
/// Builder for ingress ports.
pub struct IngressBuilder<
    'factory,
    ServiceType: Service,
    Payload: Debug + ZeroCopySend + ?Sized + 'static,
> {
    factory: &'factory PortFactory<ServiceType, Payload>,
    max_loaned_samples: Option<usize>,
    unable_to_deliver_strategy: Option<UnableToDeliverStrategy>,
    initial_max_slice_len: usize,
    allocation_strategy: AllocationStrategy,
}

impl<'factory, ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static>
    IngressBuilder<'factory, ServiceType, Payload>
{
    fn new(factory: &'factory PortFactory<ServiceType, Payload>) -> Self {
        Self {
            factory,
            max_loaned_samples: None,
            unable_to_deliver_strategy: None,
            initial_max_slice_len: factory.initial_max_slice_len,
            allocation_strategy: AllocationStrategy::Static,
        }
    }

    /// Defines the maximum parallel loan count.
    pub fn max_loaned_samples(mut self, value: usize) -> Self {
        self.max_loaned_samples = Some(value);
        self
    }

    /// Sets the inability to deliver strategy.
    pub fn unable_to_deliver_strategy(mut self, value: UnableToDeliverStrategy) -> Self {
        self.unable_to_deliver_strategy = Some(value);
        self
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    IngressBuilder<'_, ServiceType, Payload>
{
    /// Creates the ingress endpoint.
    pub fn create(self) -> Result<Ingress<ServiceType, Payload>, IngressCreateError> {
        let mut builder = self.factory.edges[0].publisher_builder();
        if let Some(value) = self.max_loaned_samples {
            builder = builder.max_loaned_samples(value);
        }
        if let Some(value) = self.unable_to_deliver_strategy {
            builder = builder.unable_to_deliver_strategy(value);
        }

        let publisher = builder
            .create()
            .map_err(IngressCreateError::PublisherCreateFailure)?;
        Ok(Ingress { publisher })
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    IngressBuilder<'_, ServiceType, [Payload]>
{
    /// Sets the maximum dynamic slice length.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.initial_max_slice_len = value;
        self
    }

    /// Sets the allocation strategy for dynamic payload.
    pub fn allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.allocation_strategy = value;
        self
    }

    /// Creates the ingress endpoint.
    pub fn create(self) -> Result<Ingress<ServiceType, [Payload]>, IngressCreateError> {
        let mut builder = self.factory.edges[0].publisher_builder();
        if let Some(value) = self.max_loaned_samples {
            builder = builder.max_loaned_samples(value);
        }
        if let Some(value) = self.unable_to_deliver_strategy {
            builder = builder.unable_to_deliver_strategy(value);
        }

        let builder = builder
            .initial_max_slice_len(self.initial_max_slice_len)
            .allocation_strategy(self.allocation_strategy);

        let publisher = builder
            .create()
            .map_err(IngressCreateError::PublisherCreateFailure)?;
        Ok(Ingress { publisher })
    }
}

#[derive(Debug)]
/// Builder for worker ports.
pub struct WorkerBuilder<
    'factory,
    ServiceType: Service,
    Payload: Debug + ZeroCopySend + ?Sized + 'static,
> {
    factory: &'factory PortFactory<ServiceType, Payload>,
    stage_id: usize,
    max_loaned_samples: Option<usize>,
    unable_to_deliver_strategy: Option<UnableToDeliverStrategy>,
    initial_max_slice_len: usize,
    allocation_strategy: AllocationStrategy,
}

impl<'factory, ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static>
    WorkerBuilder<'factory, ServiceType, Payload>
{
    fn new(factory: &'factory PortFactory<ServiceType, Payload>, stage_id: usize) -> Self {
        Self {
            factory,
            stage_id,
            max_loaned_samples: None,
            unable_to_deliver_strategy: None,
            initial_max_slice_len: factory.initial_max_slice_len,
            allocation_strategy: AllocationStrategy::Static,
        }
    }

    /// Defines the maximum parallel loan count on forward path.
    pub fn max_loaned_samples(mut self, value: usize) -> Self {
        self.max_loaned_samples = Some(value);
        self
    }

    /// Sets the inability to deliver strategy for forward path.
    pub fn unable_to_deliver_strategy(mut self, value: UnableToDeliverStrategy) -> Self {
        self.unable_to_deliver_strategy = Some(value);
        self
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    WorkerBuilder<'_, ServiceType, Payload>
{
    /// Creates the worker endpoint.
    pub fn create(self) -> Result<Worker<ServiceType, Payload>, WorkerCreateError> {
        if self.stage_id >= self.factory.number_of_stages {
            return Err(WorkerCreateError::StageOutOfBounds);
        }

        let subscriber = self.factory.edges[self.stage_id]
            .subscriber_builder()
            .create()
            .map_err(WorkerCreateError::SubscriberCreateFailure)?;

        let mut publisher_builder = self.factory.edges[self.stage_id + 1].publisher_builder();
        if let Some(value) = self.max_loaned_samples {
            publisher_builder = publisher_builder.max_loaned_samples(value);
        }
        if let Some(value) = self.unable_to_deliver_strategy {
            publisher_builder = publisher_builder.unable_to_deliver_strategy(value);
        }

        let publisher = publisher_builder
            .create()
            .map_err(WorkerCreateError::PublisherCreateFailure)?;

        Ok(Worker {
            subscriber,
            publisher,
        })
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    WorkerBuilder<'_, ServiceType, [Payload]>
{
    /// Sets the maximum dynamic slice length.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.initial_max_slice_len = value;
        self
    }

    /// Sets the allocation strategy for dynamic payload.
    pub fn allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.allocation_strategy = value;
        self
    }

    /// Creates the worker endpoint.
    pub fn create(self) -> Result<Worker<ServiceType, [Payload]>, WorkerCreateError> {
        if self.stage_id >= self.factory.number_of_stages {
            return Err(WorkerCreateError::StageOutOfBounds);
        }

        let subscriber = self.factory.edges[self.stage_id]
            .subscriber_builder()
            .create()
            .map_err(WorkerCreateError::SubscriberCreateFailure)?;

        let mut publisher_builder = self.factory.edges[self.stage_id + 1].publisher_builder();
        if let Some(value) = self.max_loaned_samples {
            publisher_builder = publisher_builder.max_loaned_samples(value);
        }
        if let Some(value) = self.unable_to_deliver_strategy {
            publisher_builder = publisher_builder.unable_to_deliver_strategy(value);
        }

        let publisher = publisher_builder
            .initial_max_slice_len(self.initial_max_slice_len)
            .allocation_strategy(self.allocation_strategy)
            .create()
            .map_err(WorkerCreateError::PublisherCreateFailure)?;

        Ok(Worker {
            subscriber,
            publisher,
        })
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static> Worker<ServiceType, Payload> {
    /// Receives the next sample for this stage, prepares mutable output and returns it.
    pub fn receive(&self) -> Result<Option<WorkMut<ServiceType, Payload>>, WorkerReceiveError> {
        let incoming = self
            .subscriber
            .receive()
            .map_err(WorkerReceiveError::ReceiveFailure)?;
        let Some(incoming) = incoming else {
            return Ok(None);
        };

        let mut outgoing = self
            .publisher
            .loan_uninit()
            .map_err(WorkerReceiveError::LoanFailure)?;

        unsafe {
            core::ptr::copy_nonoverlapping(
                incoming.payload() as *const Payload,
                outgoing.payload_mut().as_mut_ptr(),
                1,
            );
        }

        let outgoing = unsafe { outgoing.assume_init() };
        Ok(Some(WorkMut { sample: outgoing }))
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static> Worker<ServiceType, [Payload]> {
    /// Receives the next sample for this stage, prepares mutable output and returns it.
    pub fn receive(&self) -> Result<Option<WorkMut<ServiceType, [Payload]>>, WorkerReceiveError> {
        let incoming = self
            .subscriber
            .receive()
            .map_err(WorkerReceiveError::ReceiveFailure)?;
        let Some(incoming) = incoming else {
            return Ok(None);
        };

        let incoming_payload = incoming.payload();
        let mut outgoing = self
            .publisher
            .loan_slice_uninit(incoming_payload.len())
            .map_err(WorkerReceiveError::LoanFailure)?;

        unsafe {
            core::ptr::copy_nonoverlapping(
                incoming_payload.as_ptr(),
                outgoing.payload_mut().as_mut_ptr() as *mut Payload,
                incoming_payload.len(),
            );
        }

        let outgoing = unsafe { outgoing.assume_init() };
        Ok(Some(WorkMut { sample: outgoing }))
    }
}

#[derive(Debug)]
/// Builder for egress ports.
pub struct EgressBuilder<
    'factory,
    ServiceType: Service,
    Payload: Debug + ZeroCopySend + ?Sized + 'static,
> {
    factory: &'factory PortFactory<ServiceType, Payload>,
}

impl<'factory, ServiceType: Service, Payload: Debug + ZeroCopySend + ?Sized + 'static>
    EgressBuilder<'factory, ServiceType, Payload>
{
    fn new(factory: &'factory PortFactory<ServiceType, Payload>) -> Self {
        Self { factory }
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    EgressBuilder<'_, ServiceType, Payload>
{
    /// Creates the egress endpoint.
    pub fn create(self) -> Result<Egress<ServiceType, Payload>, EgressCreateError> {
        let subscriber = self.factory.edges[self.factory.number_of_stages]
            .subscriber_builder()
            .create()
            .map_err(EgressCreateError::SubscriberCreateFailure)?;
        Ok(Egress { subscriber })
    }
}

impl<ServiceType: Service, Payload: Debug + ZeroCopySend + 'static>
    EgressBuilder<'_, ServiceType, [Payload]>
{
    /// Creates the egress endpoint.
    pub fn create(self) -> Result<Egress<ServiceType, [Payload]>, EgressCreateError> {
        let subscriber = self.factory.edges[self.factory.number_of_stages]
            .subscriber_builder()
            .create()
            .map_err(EgressCreateError::SubscriberCreateFailure)?;
        Ok(Egress { subscriber })
    }
}
