// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Subscriber lease for a progressive byte-slice sample.

use core::fmt::Debug;

use iceoryx2_bb_concurrency::atomic::Ordering;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::arc_sync_policy::ArcSyncPolicy;
use iceoryx2_cal::zero_copy_connection::ChannelId;

use crate::port::details::chunk_details::ChunkDetails;
use crate::port::subscriber::SubscriberSharedState;
use crate::service::header::progressive_publish_subscribe::{Header, ProgressiveSampleState};

/// A subscriber's whole-allocation lease for one progressive sample.
#[derive(Debug)]
pub struct ProgressiveSample<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> {
    pub(crate) subscriber_shared_state:
        Service::ArcThreadSafetyPolicy<SubscriberSharedState<Service>>,
    pub(crate) details: ChunkDetails,
    pub(crate) header: *const Header,
    pub(crate) user_header: *const UserHeader,
    pub(crate) payload: *const u8,
    pub(crate) capacity: usize,
}

unsafe impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Send
    for ProgressiveSample<Service, UserHeader>
where
    Service::ArcThreadSafetyPolicy<SubscriberSharedState<Service>>: Send + Sync,
{
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Drop
    for ProgressiveSample<Service, UserHeader>
{
    fn drop(&mut self) {
        self.subscriber_shared_state
            .lock()
            .receiver
            .release_offset(&self.details, ChannelId::new(0));
    }
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressiveSample<Service, UserHeader>
{
    fn control(
        &self,
    ) -> &crate::service::header::progressive_publish_subscribe::ProgressiveControl {
        unsafe { &*self.header }.control()
    }

    /// Returns the currently published immutable prefix.
    ///
    /// This constructs a slice of exactly the acquire-loaded length; it never
    /// materializes a full-capacity slice.
    pub fn payload(&self) -> &[u8] {
        let published_len = self.published_len();
        assert!(
            published_len <= self.capacity,
            "corrupt progressive watermark"
        );
        unsafe { core::slice::from_raw_parts(self.payload, published_len) }
    }

    /// Acquire-loads the current published byte length.
    pub fn published_len(&self) -> usize {
        usize::try_from(self.control().published_len(Ordering::Acquire))
            .expect("published length is unsupported by this target")
    }

    /// Acquire-loads the current terminal state.
    pub fn state(&self) -> ProgressiveSampleState {
        self.control().state(Ordering::Acquire)
    }

    /// Returns the immutable application user header.
    pub fn user_header(&self) -> &UserHeader {
        unsafe { &*self.user_header }
    }

    /// Returns the total allocation capacity in bytes.
    pub fn payload_capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the internal progressive header.
    pub fn header(&self) -> &Header {
        unsafe { &*self.header }
    }
}
