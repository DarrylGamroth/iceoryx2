// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

//! # Example
//!
//! ```
//! use iceoryx2::prelude::*;
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let node = NodeBuilder::new().create::<ipc::Service>()?;
//! let pubsub = node.service_builder(&"My/Funk/ServiceName".try_into()?)
//!     .publish_subscribe::<u64>()
//!     .open_or_create()?;
//!
//! println!("name:                             {:?}", pubsub.name());
//! println!("service id:                       {:?}", pubsub.service_hash());
//! println!("type details:                     {:?}", pubsub.static_config().message_type_details());
//! println!("max publishers:                   {:?}", pubsub.static_config().max_publishers());
//! println!("max subscribers:                  {:?}", pubsub.static_config().max_subscribers());
//! println!("subscriber buffer size:           {:?}", pubsub.static_config().subscriber_max_buffer_size());
//! println!("history size:                     {:?}", pubsub.static_config().history_size());
//! println!("subscriber max borrowed samples:  {:?}", pubsub.static_config().subscriber_max_borrowed_samples());
//! println!("safe overflow:                    {:?}", pubsub.static_config().has_safe_overflow());
//! println!("number of active publishers:      {:?}", pubsub.dynamic_config().number_of_publishers());
//! println!("number of active subscribers:     {:?}", pubsub.dynamic_config().number_of_subscribers());
//!
//! let publisher = pubsub.publisher_builder().create()?;
//! let subscriber = pubsub.subscriber_builder().create()?;
//!
//! # Ok(())
//! # }
//! ```
extern crate alloc;
use alloc::sync::Arc;
use iceoryx2_bb_elementary_traits::iceoryx_send::IceoryxSend;
use iceoryx2_cal::static_storage::StaticStorage;

use super::nodes;
use super::{publisher::PortFactoryPublisher, subscriber::PortFactorySubscriber};
use crate::identifiers::UniqueServiceId;
use crate::node::NodeListFailure;
use crate::port::BackpressureFn;
use crate::port::backpressure_strategy::BackpressureStrategy;
use crate::port::port_name::PortName;
use crate::port::progressive_publisher::ProgressivePublisher;
use crate::port::progressive_subscriber::ProgressiveSubscriber;
use crate::port::publisher::PublisherCreateError;
use crate::port::subscriber::SubscriberCreateError;
use crate::service::attribute::AttributeSet;
use crate::service::marker::Flatbuffer;
use crate::service::resource::publish_subscribe::PublishSubscribeResources;
use crate::service::service_hash::ServiceHash;
use crate::service::service_name::ServiceName;
use crate::service::{self, ServiceState, SharedServiceState, dynamic_config, static_config};
use core::ptr::NonNull;
use core::{fmt::Debug, marker::PhantomData};
use iceoryx2_bb_elementary::CallbackProgression;
use iceoryx2_bb_elementary_traits::testing::abandonable::Abandonable;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::dynamic_storage::DynamicStorage;
use iceoryx2_cal::shm_allocator::AllocationStrategy;

/// The factory for
/// [`MessagingPattern::PublishSubscribe`](crate::service::messaging_pattern::MessagingPattern::PublishSubscribe).
/// It can acquire dynamic and static service information and create
/// [`crate::port::publisher::Publisher`]
/// or [`crate::port::subscriber::Subscriber`] ports.
#[derive(Debug)]
pub struct PortFactory<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> {
    pub(crate) service: SharedServiceState<Service, PublishSubscribeResources<Service>>,
    _payload: PhantomData<Payload>,
    _user_header: PhantomData<UserHeader>,
}

/// Port factory for experimental single-publisher progressive byte slices.
#[derive(Debug)]
pub struct ProgressivePortFactory<Service: service::Service, UserHeader: Debug + ZeroCopySend> {
    inner: PortFactory<Service, [u8], UserHeader>,
}

unsafe impl<Service: service::Service, UserHeader: Debug + ZeroCopySend> Send
    for ProgressivePortFactory<Service, UserHeader>
{
}
unsafe impl<Service: service::Service, UserHeader: Debug + ZeroCopySend> Sync
    for ProgressivePortFactory<Service, UserHeader>
{
}

impl<Service: service::Service, UserHeader: Debug + ZeroCopySend> Abandonable
    for ProgressivePortFactory<Service, UserHeader>
{
    unsafe fn abandon_in_place(mut this: NonNull<Self>) {
        let this = unsafe { this.as_mut() };
        unsafe { PortFactory::abandon_in_place(NonNull::iox2_from_mut(&mut this.inner)) };
    }
}

impl<Service: service::Service, UserHeader: Debug + ZeroCopySend>
    crate::service::port_factory::PortFactory for ProgressivePortFactory<Service, UserHeader>
{
    type Service = Service;
    type StaticConfig = static_config::publish_subscribe::StaticConfig;
    type DynamicConfig = dynamic_config::publish_subscribe::DynamicConfig;

    fn name(&self) -> &ServiceName {
        self.inner.service.static_config().name()
    }

    fn unique_service_id(&self) -> UniqueServiceId {
        self.inner.service.static_config().unique_service_id()
    }

    fn service_hash(&self) -> &ServiceHash {
        self.inner.service.static_config().service_hash()
    }

    fn attributes(&self) -> &AttributeSet {
        self.inner.service.static_config().attributes()
    }

    fn static_config(&self) -> &Self::StaticConfig {
        self.inner.service.static_config().publish_subscribe()
    }

    fn dynamic_config(&self) -> &Self::DynamicConfig {
        self.inner
            .service
            .dynamic_storage()
            .get()
            .publish_subscribe()
    }

    fn nodes<F: FnMut(crate::node::NodeState<Service>) -> CallbackProgression>(
        &self,
        callback: F,
    ) -> Result<(), NodeListFailure> {
        nodes(
            self.inner.service.dynamic_storage().get(),
            self.inner.service.shared_node().config(),
            callback,
        )
    }
}

impl<Service: service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressivePortFactory<Service, UserHeader>
{
    pub(crate) fn new(inner: PortFactory<Service, [u8], UserHeader>) -> Self {
        Self { inner }
    }

    /// Returns a builder for the single progressive publisher.
    pub fn publisher_builder(&self) -> ProgressivePortFactoryPublisher<'_, Service, UserHeader> {
        ProgressivePortFactoryPublisher {
            inner: self.inner.publisher_builder(),
        }
    }

    /// Returns a builder for a progressive subscriber.
    pub fn subscriber_builder(&self) -> ProgressivePortFactorySubscriber<'_, Service, UserHeader> {
        ProgressivePortFactorySubscriber {
            inner: self.inner.subscriber_builder(),
        }
    }
}

/// Builder for a progressive publisher endpoint.
#[derive(Debug)]
pub struct ProgressivePortFactoryPublisher<
    'factory,
    Service: service::Service,
    UserHeader: Debug + ZeroCopySend,
> {
    inner: PortFactoryPublisher<'factory, Service, [u8], UserHeader>,
}

impl<'factory, Service: service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressivePortFactoryPublisher<'factory, Service, UserHeader>
{
    /// Reduces the number of samples preallocated by the publisher.
    pub fn override_sample_preallocation<F: Fn(usize) -> usize + 'static>(
        mut self,
        callback: F,
    ) -> Self {
        self.inner = self.inner.override_sample_preallocation(callback);
        self
    }

    /// Sets the initially reserved maximum byte capacity.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.inner = self.inner.initial_max_slice_len(value);
        self
    }

    /// Sets the maximum number of simultaneous publisher loans.
    pub fn max_loaned_samples(mut self, value: usize) -> Self {
        self.inner = self.inner.max_loaned_samples(value);
        self
    }

    /// Sets the allocation strategy.
    pub fn allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.inner = self.inner.allocation_strategy(value);
        self
    }

    /// Sets queue-full backpressure behavior for sending a new frame.
    pub fn backpressure_strategy(mut self, value: BackpressureStrategy) -> Self {
        self.inner = self.inner.backpressure_strategy(value);
        self
    }

    /// Installs a handler for queue-full backpressure while sending a frame.
    pub fn set_backpressure_handler<F: BackpressureFn + 'static>(mut self, handler: F) -> Self {
        self.inner = self.inner.set_backpressure_handler(handler);
        self
    }

    /// Sets the publisher name.
    pub fn name(mut self, value: &PortName) -> Self {
        self.inner = self.inner.name(value);
        self
    }

    /// Creates the progressive publisher.
    pub fn create(self) -> Result<ProgressivePublisher<Service, UserHeader>, PublisherCreateError> {
        Ok(ProgressivePublisher {
            inner: self.inner.create()?,
        })
    }
}

/// Builder for a progressive subscriber endpoint.
#[derive(Debug)]
pub struct ProgressivePortFactorySubscriber<
    'factory,
    Service: service::Service,
    UserHeader: Debug + ZeroCopySend,
> {
    inner: PortFactorySubscriber<'factory, Service, [u8], UserHeader>,
}

impl<'factory, Service: service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressivePortFactorySubscriber<'factory, Service, UserHeader>
{
    /// Sets the subscriber queue capacity in samples.
    pub fn buffer_size(mut self, value: usize) -> Self {
        self.inner = self.inner.buffer_size(value);
        self
    }

    /// Sets the subscriber name.
    pub fn name(mut self, value: &PortName) -> Self {
        self.inner = self.inner.name(value);
        self
    }

    /// Creates a progressive subscriber.
    pub fn create(
        self,
    ) -> Result<ProgressiveSubscriber<Service, UserHeader>, SubscriberCreateError> {
        Ok(ProgressiveSubscriber {
            inner: self.inner.create()?,
        })
    }
}

unsafe impl<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> Send for PortFactory<Service, Payload, UserHeader>
{
}
unsafe impl<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> Sync for PortFactory<Service, Payload, UserHeader>
{
}

impl<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> Abandonable for PortFactory<Service, Payload, UserHeader>
{
    unsafe fn abandon_in_place(mut this: NonNull<Self>) {
        let this = unsafe { this.as_mut() };
        unsafe { SharedServiceState::abandon_in_place(NonNull::from_mut(&mut this.service)) };
    }
}

impl<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> crate::service::port_factory::PortFactory for PortFactory<Service, Payload, UserHeader>
{
    type Service = Service;
    type StaticConfig = static_config::publish_subscribe::StaticConfig;
    type DynamicConfig = dynamic_config::publish_subscribe::DynamicConfig;

    fn name(&self) -> &ServiceName {
        self.service.static_config().name()
    }

    fn unique_service_id(&self) -> UniqueServiceId {
        self.service.static_config().unique_service_id()
    }

    fn service_hash(&self) -> &ServiceHash {
        self.service.static_config().service_hash()
    }

    fn attributes(&self) -> &AttributeSet {
        self.service.static_config().attributes()
    }

    fn static_config(&self) -> &static_config::publish_subscribe::StaticConfig {
        self.service.static_config().publish_subscribe()
    }

    fn dynamic_config(&self) -> &dynamic_config::publish_subscribe::DynamicConfig {
        self.service.dynamic_storage().get().publish_subscribe()
    }

    fn nodes<F: FnMut(crate::node::NodeState<Service>) -> CallbackProgression>(
        &self,
        callback: F,
    ) -> Result<(), NodeListFailure> {
        nodes(
            self.service.dynamic_storage().get(),
            self.service.shared_node().config(),
            callback,
        )
    }
}

impl<
    Service: service::Service,
    Payload: IceoryxSend + Debug + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> PortFactory<Service, Payload, UserHeader>
{
    pub(crate) fn new(service: ServiceState<Service, PublishSubscribeResources<Service>>) -> Self {
        Self {
            service: SharedServiceState {
                state: Arc::new(service),
            },
            _payload: PhantomData,
            _user_header: PhantomData,
        }
    }

    /// Returns a [`PortFactorySubscriber`] to create a new
    /// [`crate::port::subscriber::Subscriber`] port.
    ///
    /// # Example
    ///
    /// ```
    /// use iceoryx2::prelude::*;
    ///
    /// # fn main() -> Result<(), Box<dyn core::error::Error>> {
    /// let node = NodeBuilder::new().create::<ipc::Service>()?;
    /// let pubsub = node.service_builder(&"My/Funk/ServiceName".try_into()?)
    ///     .publish_subscribe::<u64>()
    ///     .open_or_create()?;
    ///
    /// let subscriber = pubsub.subscriber_builder().create()?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscriber_builder(&self) -> PortFactorySubscriber<'_, Service, Payload, UserHeader> {
        PortFactorySubscriber::new(self)
    }

    /// Returns a [`PortFactoryPublisher`] to create a new
    /// [`crate::port::publisher::Publisher`] port.
    ///
    /// # Example
    ///
    /// ```
    /// use iceoryx2::prelude::*;
    ///
    /// # fn main() -> Result<(), Box<dyn core::error::Error>> {
    /// let node = NodeBuilder::new().create::<ipc::Service>()?;
    /// let pubsub = node.service_builder(&"My/Funk/ServiceName".try_into()?)
    ///     .publish_subscribe::<u64>()
    ///     .open_or_create()?;
    ///
    /// let publisher = pubsub.publisher_builder()
    ///                     .max_loaned_samples(6)
    ///                     .backpressure_strategy(BackpressureStrategy::DiscardData)
    ///                     .create()?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn publisher_builder(&self) -> PortFactoryPublisher<'_, Service, Payload, UserHeader> {
        PortFactoryPublisher::new(self)
    }
}

impl<Service: service::Service, Payload, UserHeader: Debug + ZeroCopySend>
    PortFactory<Service, Flatbuffer<Payload>, UserHeader>
{
    /// Returns the [`StaticStorageView`](iceoryx2_cal::static_storage::StaticStorageView) that contains the type definition.
    pub fn type_definition(&self) -> Option<&<Service::StaticStorage as StaticStorage>::View> {
        self.service
            .additional_resource()
            .type_definition()
            .map(|v| v.view())
    }
}
