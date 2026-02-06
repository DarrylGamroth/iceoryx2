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

//! # Example
//!
//! ```
//! use iceoryx2::prelude::*;
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let node = NodeBuilder::new().create::<ipc::Service>()?;
//! let log = node.service_builder(&"My/Funk/ServiceName".try_into()?)
//!     .log::<u64>()
//!     .open_or_create()?;
//!
//! println!("name:                             {:?}", log.name());
//! println!("service id:                       {:?}", log.service_id());
//! println!("type details:                     {:?}", log.static_config().message_type_details());
//! println!("max appenders:                    {:?}", log.static_config().max_appenders());
//! println!("max tailers:                      {:?}", log.static_config().max_tailers());
//! println!("tailer buffer size:               {:?}", log.static_config().tailer_max_buffer_size());
//! println!("retention size:                   {:?}", log.static_config().retention_size());
//! println!("tailer max borrowed samples:      {:?}", log.static_config().tailer_max_borrowed_samples());
//! println!("safe overflow:                    {:?}", log.static_config().has_safe_overflow());
//! println!("number of active appenders:       {:?}", log.dynamic_config().number_of_appenders());
//! println!("number of active tailers:         {:?}", log.dynamic_config().number_of_tailers());
//!
//! let appender = log.appender_builder().create()?;
//! let tailer = log.tailer_builder().create()?;
//!
//! # Ok(())
//! # }
//! ```
extern crate alloc;

use alloc::sync::Arc;
use core::{fmt::Debug, marker::PhantomData};

use iceoryx2_bb_elementary::CallbackProgression;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::dynamic_storage::DynamicStorage;

use crate::node::NodeListFailure;
use crate::service::attribute::AttributeSet;
use crate::service::service_id::ServiceId;
use crate::service::service_name::ServiceName;
use crate::service::{self, dynamic_config, static_config, NoResource, ServiceState};

use super::nodes;
use super::{appender::PortFactoryAppender, tailer::PortFactoryTailer};

/// The factory for
/// [`MessagingPattern::Log`](crate::service::messaging_pattern::MessagingPattern::Log).
/// It can acquire dynamic and static service information and create
/// [`crate::port::appender::Appender`]
/// or [`crate::port::tailer::Tailer`] ports.
#[derive(Debug)]
pub struct PortFactory<
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: Debug + ZeroCopySend,
> {
    pub(crate) service: Arc<ServiceState<Service, NoResource>>,
    _payload: PhantomData<Payload>,
    _user_header: PhantomData<UserHeader>,
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Send for PortFactory<Service, Payload, UserHeader>
{
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Sync for PortFactory<Service, Payload, UserHeader>
{
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > crate::service::port_factory::PortFactory for PortFactory<Service, Payload, UserHeader>
{
    type Service = Service;
    type StaticConfig = static_config::log::StaticConfig;
    type DynamicConfig = dynamic_config::log::DynamicConfig;

    fn name(&self) -> &ServiceName {
        self.service.static_config.name()
    }

    fn service_id(&self) -> &ServiceId {
        self.service.static_config.service_id()
    }

    fn attributes(&self) -> &AttributeSet {
        self.service.static_config.attributes()
    }

    fn static_config(&self) -> &static_config::log::StaticConfig {
        self.service.static_config.log()
    }

    fn dynamic_config(&self) -> &dynamic_config::log::DynamicConfig {
        self.service.dynamic_storage.get().log()
    }

    fn nodes<F: FnMut(crate::node::NodeState<Service>) -> CallbackProgression>(
        &self,
        callback: F,
    ) -> Result<(), NodeListFailure> {
        nodes(
            self.service.dynamic_storage.get(),
            self.service.shared_node.config(),
            callback,
        )
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > PortFactory<Service, Payload, UserHeader>
{
    pub(crate) fn new(service: ServiceState<Service, NoResource>) -> Self {
        Self {
            service: Arc::new(service),
            _payload: PhantomData,
            _user_header: PhantomData,
        }
    }

    /// Returns a [`PortFactoryAppender`] to create a new
    /// [`crate::port::appender::Appender`] port.
    pub fn appender_builder(&self) -> PortFactoryAppender<'_, Service, Payload, UserHeader> {
        PortFactoryAppender::new(self)
    }

    /// Returns a [`PortFactoryTailer`] to create a new
    /// [`crate::port::tailer::Tailer`] port.
    pub fn tailer_builder(&self) -> PortFactoryTailer<'_, Service, Payload, UserHeader> {
        PortFactoryTailer::new(self)
    }
}
