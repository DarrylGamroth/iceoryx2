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

use alloc::format;
use core::fmt::Debug;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::shm_allocator::AllocationStrategy;
use iceoryx2_log::fail;

use crate::port::{
    appender::Appender,
    publisher::{Publisher, PublisherCreateError},
    unable_to_deliver_strategy::UnableToDeliverStrategy,
    DegradationAction, DegradationCallback,
};
use crate::service;
use crate::service::header::log::UserHeaderStorage;

use super::log::PortFactory;
use super::publisher::LocalPublisherConfig;

/// Factory to create a new [`Appender`] port/endpoint for
/// [`MessagingPattern::Log`](crate::service::messaging_pattern::MessagingPattern::Log) based
/// communication.
#[derive(Debug)]
pub struct PortFactoryAppender<
    'factory,
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> {
    config: LocalPublisherConfig,
    pub(crate) factory: &'factory PortFactory<Service, Payload, UserHeader>,
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Send for PortFactoryAppender<'_, Service, Payload, UserHeader>
{
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > PortFactoryAppender<'_, Service, Payload, UserHeader>
{
    #[doc(hidden)]
    /// # Safety
    ///
    ///   * does not clone the degradation callback
    pub unsafe fn __internal_partial_clone(&self) -> Self {
        Self {
            config: LocalPublisherConfig {
                max_loaned_samples: self.config.max_loaned_samples,
                unable_to_deliver_strategy: self.config.unable_to_deliver_strategy,
                degradation_callback: None,
                initial_max_slice_len: self.config.initial_max_slice_len,
                allocation_strategy: self.config.allocation_strategy,
            },
            factory: self.factory,
        }
    }
}

impl<
        'factory,
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > PortFactoryAppender<'factory, Service, Payload, UserHeader>
{
    pub(crate) fn new(factory: &'factory PortFactory<Service, Payload, UserHeader>) -> Self {
        Self {
            config: LocalPublisherConfig {
                allocation_strategy: AllocationStrategy::Static,
                degradation_callback: None,
                initial_max_slice_len: 1,
                max_loaned_samples: factory
                    .service
                    .shared_node
                    .config()
                    .defaults
                    .publish_subscribe
                    .publisher_max_loaned_samples,
                unable_to_deliver_strategy: factory
                    .service
                    .shared_node
                    .config()
                    .defaults
                    .publish_subscribe
                    .unable_to_deliver_strategy,
            },
            factory,
        }
    }

    /// Defines how many samples the [`Appender`] can loan in parallel.
    pub fn max_loaned_samples(mut self, value: usize) -> Self {
        self.config.max_loaned_samples = value;
        self
    }

    /// Sets the [`UnableToDeliverStrategy`].
    pub fn unable_to_deliver_strategy(mut self, value: UnableToDeliverStrategy) -> Self {
        self.config.unable_to_deliver_strategy = value;
        self
    }

    /// Sets the [`DegradationCallback`] of the [`Appender`].
    pub fn set_degradation_callback<
        F: Fn(&service::static_config::StaticConfig, u128, u128) -> DegradationAction + 'static,
    >(
        mut self,
        callback: Option<F>,
    ) -> Self {
        match callback {
            Some(c) => self.config.degradation_callback = Some(DegradationCallback::new(c)),
            None => self.config.degradation_callback = None,
        }

        self
    }

    /// Creates a new [`Appender`] or returns an error on failure.
    pub fn create(self) -> Result<Appender<Service, Payload, UserHeader>, PublisherCreateError> {
        let origin = format!("{self:?}");
        let appender = fail!(from origin,
            when Publisher::<Service, Payload, UserHeaderStorage<UserHeader>>::new(
                self.factory.service.clone(),
                self.factory.service.static_config.publish_subscribe(),
                self.config,
            ),
            "Failed to create new Appender port."
        );

        Ok(Appender::new(appender))
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend,
        UserHeader: Debug + ZeroCopySend,
    > PortFactoryAppender<'_, Service, [Payload], UserHeader>
{
    /// Sets the maximum slice length that a user can allocate with
    /// [`Appender::loan_slice()`](crate::port::appender::Appender::loan_slice) or
    /// [`Appender::loan_slice_uninit()`](crate::port::appender::Appender::loan_slice_uninit).
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.config.initial_max_slice_len = value;
        self
    }

    /// Defines the allocation strategy that is used when
    /// [`PortFactoryAppender::initial_max_slice_len()`] is exhausted.
    pub fn allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.config.allocation_strategy = value;
        self
    }
}
