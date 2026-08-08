// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Subscriber endpoint for progressive publish/subscribe.

use core::fmt::Debug;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::identifiers::UniqueSubscriberId;
use crate::port::ReceiveError;
use crate::port::port_name::PortName;
use crate::port::subscriber::Subscriber;
use crate::progressive_sample::ProgressiveSample;

/// Experimental progressive subscriber endpoint for byte slices.
#[derive(Debug)]
pub struct ProgressiveSubscriber<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
{
    pub(crate) inner: Subscriber<Service, [u8], UserHeader>,
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressiveSubscriber<Service, UserHeader>
{
    /// Receives an allocation announced while this subscriber was connected.
    ///
    /// Progressive services have no history, so samples announced before this
    /// subscriber connected are not returned.
    pub fn receive(&self) -> Result<Option<ProgressiveSample<Service, UserHeader>>, ReceiveError> {
        self.inner.receive_progressive()
    }

    /// Returns whether at least one sample is queued.
    pub fn has_samples(&self) -> Result<bool, crate::port::update_connections::ConnectionFailure> {
        self.inner.has_samples()
    }

    /// Returns the subscriber identifier.
    pub fn id(&self) -> UniqueSubscriberId {
        self.inner.id()
    }

    /// Returns the subscriber name.
    pub fn name(&self) -> &PortName {
        self.inner.name()
    }
}
