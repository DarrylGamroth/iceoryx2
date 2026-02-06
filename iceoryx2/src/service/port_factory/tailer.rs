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
use iceoryx2_log::fail;

use crate::port::{
    subscriber::{Subscriber, SubscriberCreateError},
    tailer::Tailer,
    DegradationAction, DegradationCallback,
};
use crate::service;
use crate::service::header::log::UserHeaderStorage;

use super::log::PortFactory;
use super::subscriber::SubscriberConfig;

/// Factory to create a new [`Tailer`] port/endpoint for
/// [`MessagingPattern::Log`](crate::service::messaging_pattern::MessagingPattern::Log) based
/// communication.
#[derive(Debug)]
pub struct PortFactoryTailer<
    'factory,
    Service: service::Service,
    PayloadType: Debug + ZeroCopySend + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> {
    config: SubscriberConfig,
    pub(crate) factory: &'factory PortFactory<Service, PayloadType, UserHeader>,
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Send for PortFactoryTailer<'_, Service, Payload, UserHeader>
{
}

impl<
        'factory,
        Service: service::Service,
        PayloadType: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > PortFactoryTailer<'factory, Service, PayloadType, UserHeader>
{
    #[doc(hidden)]
    /// # Safety
    ///
    ///   * does not clone the degradation callback
    pub unsafe fn __internal_partial_clone(&self) -> Self {
        Self {
            config: SubscriberConfig {
                buffer_size: self.config.buffer_size,
                degradation_callback: None,
            },
            factory: self.factory,
        }
    }

    pub(crate) fn new(factory: &'factory PortFactory<Service, PayloadType, UserHeader>) -> Self {
        Self {
            config: SubscriberConfig {
                buffer_size: None,
                degradation_callback: None,
            },
            factory,
        }
    }

    /// Defines the buffer size of the [`Tailer`]. Smallest possible value is `1`.
    pub fn buffer_size(mut self, value: usize) -> Self {
        self.config.buffer_size = Some(value.max(1));
        self
    }

    /// Sets the [`DegradationCallback`] of the [`Tailer`].
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

    /// Creates a new [`Tailer`] or returns an error on failure.
    pub fn create(self) -> Result<Tailer<Service, PayloadType, UserHeader>, SubscriberCreateError> {
        let origin = format!("{self:?}");
        let tailer = fail!(from origin,
            when Subscriber::<Service, PayloadType, UserHeaderStorage<UserHeader>>::new(
                self.factory.service.clone(),
                self.factory.service.static_config.publish_subscribe(),
                self.config,
            ),
            "Failed to create new Tailer port."
        );

        Ok(Tailer::new(tailer))
    }
}
