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

/// An atomically observed committed length and lifecycle state.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ProgressiveSampleSnapshot {
    committed_len: usize,
    state: ProgressiveSampleState,
}

impl ProgressiveSampleSnapshot {
    /// Returns the immutable committed prefix length in bytes.
    pub fn committed_len(&self) -> usize {
        self.committed_len
    }

    /// Returns the lifecycle state observed with the committed length.
    pub fn state(&self) -> ProgressiveSampleState {
        self.state
    }
}

/// A subscriber's whole-allocation lease for one progressive sample.
///
/// The lease grants shared immutable read access only to the prefix reported by
/// [`ProgressiveSample::snapshot`]. Retaining the lease prevents allocation
/// reuse even after the writer completes or aborts.
///
/// A payload borrow cannot outlive its sample lease:
///
/// ```compile_fail
/// use iceoryx2::prelude::*;
/// use iceoryx2::progressive_sample::ProgressiveSample;
///
/// fn payload_cannot_outlive_sample<'a>(
///     sample: ProgressiveSample<ipc::Service, ()>,
/// ) -> &'a [u8] {
///     sample.payload()
/// }
/// ```
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

    /// Atomically acquire-loads the committed length and lifecycle state.
    pub fn snapshot(&self) -> ProgressiveSampleSnapshot {
        let snapshot = self.control().snapshot(Ordering::Acquire);
        let committed_len = usize::try_from(snapshot.committed_len)
            .expect("committed length is unsupported by this target");
        assert!(
            committed_len <= self.capacity,
            "corrupt progressive committed length"
        );
        ProgressiveSampleSnapshot {
            committed_len,
            state: snapshot.state,
        }
    }

    /// Returns the currently committed immutable prefix.
    ///
    /// This constructs a slice of exactly the atomically observed length; it never
    /// materializes a full-capacity slice.
    pub fn payload(&self) -> &[u8] {
        let snapshot = self.snapshot();
        unsafe { core::slice::from_raw_parts(self.payload, snapshot.committed_len) }
    }

    /// Acquire-loads the current committed byte length.
    pub fn committed_len(&self) -> usize {
        self.snapshot().committed_len
    }

    /// Acquire-loads the current lifecycle state.
    pub fn state(&self) -> ProgressiveSampleState {
        self.snapshot().state
    }

    /// Returns a snapshot while also accounting for abrupt publisher death.
    ///
    /// Unlike [`Self::snapshot`], this is not a hot-path operation: when the
    /// shared state is still [`ProgressiveSampleState::Active`], it queries
    /// the node-monitoring backend and may perform operating-system calls. A
    /// dead or already-cleaned origin node is reported as `Aborted` without
    /// mutating the shared header. Inaccessible or undefined node state is
    /// handled conservatively as `Active`.
    pub fn snapshot_with_publisher_liveness(
        &self,
    ) -> Result<ProgressiveSampleSnapshot, crate::node::NodeListFailure> {
        use crate::node::NodeState;

        let mut snapshot = self.snapshot();
        if snapshot.state != ProgressiveSampleState::Active {
            return Ok(snapshot);
        }

        let node_id = unsafe { &*self.header }.node_id();
        let shared_state = self.subscriber_shared_state.lock();
        let config = shared_state.receiver.service_state.shared_node().config();
        snapshot.state = match NodeState::<Service>::new(&node_id, config)? {
            None | Some(NodeState::Dead(_)) => ProgressiveSampleState::Aborted,
            Some(NodeState::Alive(_))
            | Some(NodeState::Inaccessible(_))
            | Some(NodeState::Undefined(_)) => ProgressiveSampleState::Active,
        };
        Ok(snapshot)
    }

    /// Returns the lifecycle state while accounting for abrupt publisher death.
    ///
    /// This is the state-only convenience form of
    /// [`Self::snapshot_with_publisher_liveness`].
    pub fn state_with_publisher_liveness(
        &self,
    ) -> Result<ProgressiveSampleState, crate::node::NodeListFailure> {
        Ok(self.snapshot_with_publisher_liveness()?.state)
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
