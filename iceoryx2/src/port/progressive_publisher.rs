// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Publisher endpoint for progressive publish/subscribe.

use core::fmt::Debug;

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::identifiers::UniquePublisherId;
use crate::port::LoanError;
use crate::port::port_name::PortName;
use crate::port::publisher::Publisher;
use crate::progressive_sample_mut::ProgressiveSampleMutUninit;

/// Experimental single-publisher progressive endpoint for byte slices.
#[derive(Debug)]
pub struct ProgressivePublisher<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
{
    pub(crate) inner: Publisher<Service, [u8], UserHeader>,
}

impl<Service: crate::service::Service, UserHeader: Default + Debug + ZeroCopySend>
    ProgressivePublisher<Service, UserHeader>
{
    /// Loans a private byte-slice allocation with the requested capacity.
    pub fn loan_slice_uninit(
        &self,
        capacity: usize,
    ) -> Result<ProgressiveSampleMutUninit<Service, UserHeader>, LoanError> {
        self.inner.loan_progressive_slice_uninit(capacity)
    }

    /// Returns the publisher identifier.
    pub fn id(&self) -> UniquePublisherId {
        self.inner.id()
    }

    /// Returns the publisher name.
    pub fn name(&self) -> &PortName {
        self.inner.name()
    }
}
