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

use core::fmt::Debug;
use core::ops::Deref;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::port::port_identifiers::{UniquePublisherId, UniqueSubscriberId};
use crate::port::subscriber::Subscriber;
use crate::port::update_connections::{ConnectionFailure, UpdateConnections};
use crate::port::ReceiveError;
use crate::sample;
use crate::service;
use crate::service::header::log::{Header, UserHeaderStorage};
use crate::service::header::publish_subscribe;

fn create_header<UserHeader: ZeroCopySend>(
    header: &publish_subscribe::Header,
    user_header: &UserHeaderStorage<UserHeader>,
) -> Header {
    Header::new(
        header.node_id(),
        header.publisher_id(),
        user_header.sequence,
        header.number_of_elements(),
    )
}

/// Stores the payload acquired by a [`Tailer`] with [`Tailer::receive`].
pub struct Sample<
    Service: crate::service::Service,
    Payload: Debug + ?Sized + ZeroCopySend,
    UserHeader: ZeroCopySend,
> {
    sample: sample::Sample<Service, Payload, UserHeaderStorage<UserHeader>>,
}

impl<
        Service: crate::service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: ZeroCopySend,
    > Deref for Sample<Service, Payload, UserHeader>
{
    type Target = Payload;

    fn deref(&self) -> &Self::Target {
        self.sample.payload()
    }
}

impl<
        Service: crate::service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: ZeroCopySend,
    > Sample<Service, Payload, UserHeader>
{
    pub(crate) fn new(
        sample: sample::Sample<Service, Payload, UserHeaderStorage<UserHeader>>,
    ) -> Self {
        Self { sample }
    }

    /// Returns the payload.
    pub fn payload(&self) -> &Payload {
        self.sample.payload()
    }

    /// Returns the user header.
    pub fn user_header(&self) -> &UserHeader {
        &self.sample.user_header().user_header
    }

    /// Returns the sample header.
    pub fn header(&self) -> Header {
        create_header(self.sample.header(), self.sample.user_header())
    }

    /// Returns the [`UniquePublisherId`] of the source appender.
    pub fn origin(&self) -> UniquePublisherId {
        self.sample.origin()
    }
}

/// Receiving endpoint of a log-based communication.
#[derive(Debug)]
pub struct Tailer<
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized + 'static,
    UserHeader: Debug + ZeroCopySend,
> {
    tailer: Subscriber<Service, Payload, UserHeaderStorage<UserHeader>>,
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Send for Tailer<Service, Payload, UserHeader>
where
    Service::ArcThreadSafetyPolicy<crate::port::subscriber::SubscriberSharedState<Service>>:
        Send + Sync,
{
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Sync for Tailer<Service, Payload, UserHeader>
where
    Service::ArcThreadSafetyPolicy<crate::port::subscriber::SubscriberSharedState<Service>>:
        Send + Sync,
{
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Tailer<Service, Payload, UserHeader>
{
    pub(crate) fn new(tailer: Subscriber<Service, Payload, UserHeaderStorage<UserHeader>>) -> Self {
        Self { tailer }
    }

    /// Returns the unique tailer id.
    pub fn id(&self) -> UniqueSubscriberId {
        self.tailer.id()
    }

    /// Returns the internal tailer buffer size.
    pub fn buffer_size(&self) -> usize {
        self.tailer.buffer_size()
    }

    /// Returns true if the tailer has samples in the internal buffer.
    pub fn has_samples(&self) -> Result<bool, ConnectionFailure> {
        self.tailer.has_samples()
    }

    /// Updates connections to appenders.
    pub fn update_connections(&self) -> Result<(), ConnectionFailure> {
        self.tailer.update_connections()
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend,
        UserHeader: Debug + ZeroCopySend,
    > Tailer<Service, Payload, UserHeader>
{
    /// Receives the next sample if available.
    pub fn receive(&self) -> Result<Option<Sample<Service, Payload, UserHeader>>, ReceiveError> {
        self.tailer.receive().map(|sample| sample.map(Sample::new))
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend,
        UserHeader: Debug + ZeroCopySend,
    > Tailer<Service, [Payload], UserHeader>
{
    /// Receives the next sample if available.
    pub fn receive(&self) -> Result<Option<Sample<Service, [Payload], UserHeader>>, ReceiveError> {
        self.tailer.receive().map(|sample| sample.map(Sample::new))
    }
}
