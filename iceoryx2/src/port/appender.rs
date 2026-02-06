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
use core::mem::MaybeUninit;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::arc_sync_policy::ArcSyncPolicy;
use iceoryx2_cal::dynamic_storage::DynamicStorage;

use crate::port::port_identifiers::UniquePublisherId;
use crate::port::publisher::Publisher;
use crate::port::update_connections::{ConnectionFailure, UpdateConnections};
use crate::port::{LoanError, SendError};
use crate::sample_mut;
use crate::sample_mut_uninit;
use crate::service::header::log::{Header, UserHeaderStorage};
use crate::service::header::publish_subscribe;
use crate::service::{self, NoResource, ServiceState};

use alloc::sync::Arc;

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

fn claim_sequence<Service, Payload, UserHeader>(
    sample: &mut sample_mut::SampleMut<Service, Payload, UserHeaderStorage<UserHeader>>,
) where
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
{
    let sequence = sample
        .publisher_shared_state
        .lock()
        .sender
        .service_state
        .dynamic_storage
        .get()
        .log()
        .claim_sequence();

    sample.user_header_mut().sequence = sequence;
}

/// Acquired by an [`Appender`] via
/// [`Appender::loan()`] or [`Appender::loan_slice()`].
pub struct SampleMut<
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> {
    sample: sample_mut::SampleMut<Service, Payload, UserHeaderStorage<UserHeader>>,
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: ZeroCopySend,
    > SampleMut<Service, Payload, UserHeader>
{
    pub(crate) fn new(
        sample: sample_mut::SampleMut<Service, Payload, UserHeaderStorage<UserHeader>>,
    ) -> Self {
        Self { sample }
    }

    /// Returns the sample header.
    pub fn header(&self) -> Header {
        create_header(self.sample.header(), self.sample.user_header())
    }

    /// Returns the user header.
    pub fn user_header(&self) -> &UserHeader {
        &self.sample.user_header().user_header
    }

    /// Returns the mutable user header.
    pub fn user_header_mut(&mut self) -> &mut UserHeader {
        &mut self.sample.user_header_mut().user_header
    }

    /// Returns the payload.
    pub fn payload(&self) -> &Payload {
        self.sample.payload()
    }

    /// Returns the mutable payload.
    pub fn payload_mut(&mut self) -> &mut Payload {
        self.sample.payload_mut()
    }

    /// Sends the sample to all connected tailers.
    pub fn send(self) -> Result<usize, SendError> {
        let mut sample = self.sample;
        claim_sequence(&mut sample);
        sample.send()
    }
}

/// Acquired by an [`Appender`] via
/// [`Appender::loan_uninit()`] or [`Appender::loan_slice_uninit()`].
#[repr(transparent)]
pub struct SampleMutUninit<
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> {
    sample: sample_mut_uninit::SampleMutUninit<Service, Payload, UserHeaderStorage<UserHeader>>,
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: ZeroCopySend,
    > SampleMutUninit<Service, Payload, UserHeader>
{
    pub(crate) fn new(
        sample: sample_mut_uninit::SampleMutUninit<Service, Payload, UserHeaderStorage<UserHeader>>,
    ) -> Self {
        Self { sample }
    }

    /// Returns the sample header.
    pub fn header(&self) -> Header {
        create_header(self.sample.header(), self.sample.user_header())
    }

    /// Returns the user header.
    pub fn user_header(&self) -> &UserHeader {
        &self.sample.user_header().user_header
    }

    /// Returns the mutable user header.
    pub fn user_header_mut(&mut self) -> &mut UserHeader {
        &mut self.sample.user_header_mut().user_header
    }

    /// Returns the payload.
    pub fn payload(&self) -> &Payload {
        self.sample.payload()
    }

    /// Returns the mutable payload.
    pub fn payload_mut(&mut self) -> &mut Payload {
        self.sample.payload_mut()
    }
}

impl<Service: service::Service, Payload: Debug + ZeroCopySend, UserHeader: ZeroCopySend>
    SampleMutUninit<Service, MaybeUninit<Payload>, UserHeader>
{
    /// Writes the payload to the sample and labels the sample as initialized.
    pub fn write_payload(self, value: Payload) -> SampleMut<Service, Payload, UserHeader> {
        SampleMut::new(self.sample.write_payload(value))
    }

    /// Labels the sample as initialized.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the payload is fully initialized.
    pub unsafe fn assume_init(self) -> SampleMut<Service, Payload, UserHeader> {
        SampleMut::new(self.sample.assume_init())
    }
}

impl<Service: service::Service, Payload: Debug + ZeroCopySend, UserHeader: ZeroCopySend>
    SampleMutUninit<Service, [MaybeUninit<Payload>], UserHeader>
{
    /// Labels the sample as initialized.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all slice elements are fully initialized.
    pub unsafe fn assume_init(self) -> SampleMut<Service, [Payload], UserHeader> {
        SampleMut::new(self.sample.assume_init())
    }

    /// Writes the payload by calling `initializer(index)` per element.
    pub fn write_from_fn<F: FnMut(usize) -> Payload>(
        self,
        initializer: F,
    ) -> SampleMut<Service, [Payload], UserHeader> {
        SampleMut::new(self.sample.write_from_fn(initializer))
    }
}

impl<Service: service::Service, Payload: Debug + Copy + ZeroCopySend, UserHeader: ZeroCopySend>
    SampleMutUninit<Service, [MaybeUninit<Payload>], UserHeader>
{
    /// Writes the payload by copying from `value` and labels the sample as initialized.
    pub fn write_from_slice(self, value: &[Payload]) -> SampleMut<Service, [Payload], UserHeader> {
        SampleMut::new(self.sample.write_from_slice(value))
    }
}

/// Sending endpoint of a log-based communication.
#[derive(Debug)]
pub struct Appender<
    Service: service::Service,
    Payload: Debug + ZeroCopySend + ?Sized + 'static,
    UserHeader: Debug + ZeroCopySend,
> {
    appender: Publisher<Service, Payload, UserHeaderStorage<UserHeader>>,
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Send for Appender<Service, Payload, UserHeader>
where
    Service::ArcThreadSafetyPolicy<crate::port::publisher::PublisherSharedState<Service>>:
        Send + Sync,
{
}

unsafe impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Sync for Appender<Service, Payload, UserHeader>
where
    Service::ArcThreadSafetyPolicy<crate::port::publisher::PublisherSharedState<Service>>:
        Send + Sync,
{
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend + ?Sized,
        UserHeader: Debug + ZeroCopySend,
    > Appender<Service, Payload, UserHeader>
{
    pub(crate) fn new(
        appender: Publisher<Service, Payload, UserHeaderStorage<UserHeader>>,
    ) -> Self {
        Self { appender }
    }

    pub(crate) fn __internal_service_state(&self) -> Arc<ServiceState<Service, NoResource>> {
        self.appender
            .publisher_shared_state
            .lock()
            .sender
            .service_state
            .clone()
    }

    /// Returns the unique appender id.
    pub fn id(&self) -> UniquePublisherId {
        self.appender.id()
    }

    /// Updates connections to tailers.
    pub fn update_connections(&self) -> Result<(), ConnectionFailure> {
        self.appender.update_connections()
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend,
        UserHeader: Default + Debug + ZeroCopySend,
    > Appender<Service, Payload, UserHeader>
{
    /// Loans uninitialized payload memory.
    pub fn loan_uninit(
        &self,
    ) -> Result<SampleMutUninit<Service, MaybeUninit<Payload>, UserHeader>, LoanError> {
        self.appender.loan_uninit().map(SampleMutUninit::new)
    }
}

impl<
        Service: service::Service,
        Payload: Default + Debug + ZeroCopySend,
        UserHeader: Default + Debug + ZeroCopySend,
    > Appender<Service, Payload, UserHeader>
{
    /// Loans default-initialized payload memory.
    pub fn loan(&self) -> Result<SampleMut<Service, Payload, UserHeader>, LoanError> {
        self.appender.loan().map(SampleMut::new)
    }

    /// Sends a copied payload value.
    pub fn send_copy(&self, value: Payload) -> Result<usize, SendError>
    where
        Payload: Copy,
    {
        let sample = self.loan_uninit()?;
        let sample = sample.write_payload(value);
        sample.send()
    }
}

impl<
        Service: service::Service,
        Payload: Debug + ZeroCopySend,
        UserHeader: Default + Debug + ZeroCopySend,
    > Appender<Service, [Payload], UserHeader>
{
    /// Loans uninitialized slice payload memory.
    pub fn loan_slice_uninit(
        &self,
        slice_len: usize,
    ) -> Result<SampleMutUninit<Service, [MaybeUninit<Payload>], UserHeader>, LoanError> {
        self.appender
            .loan_slice_uninit(slice_len)
            .map(SampleMutUninit::new)
    }
}

impl<
        Service: service::Service,
        Payload: Default + Debug + ZeroCopySend,
        UserHeader: Default + Debug + ZeroCopySend,
    > Appender<Service, [Payload], UserHeader>
{
    /// Loans default-initialized slice payload memory.
    pub fn loan_slice(
        &self,
        slice_len: usize,
    ) -> Result<SampleMut<Service, [Payload], UserHeader>, LoanError> {
        self.appender.loan_slice(slice_len).map(SampleMut::new)
    }
}
