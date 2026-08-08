// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Cache-line-isolated control and origin metadata for progressive samples.

use core::mem::{align_of, offset_of, size_of};

use iceoryx2_bb_concurrency::atomic::{AtomicU64, Ordering};
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::identifiers::{UniqueNodeId, UniquePublisherId};

/// Alignment used to isolate independently accessed progressive sample data.
pub const PROGRESSIVE_CONTROL_ALIGNMENT: usize = 128;

const STATE_ACTIVE: u64 = 0;
const STATE_COMPLETE: u64 = 1;
const STATE_ABORTED: u64 = 2;
const STATE_MASK: u64 = 0b11;
const COMMITTED_LEN_SHIFT: u32 = 2;

pub(crate) const MAX_COMMITTED_LEN: u64 = u64::MAX >> COMMITTED_LEN_SHIFT;

/// Subscriber-visible terminal state of a progressive sample.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressiveSampleState {
    /// The publisher may still extend the immutable prefix.
    Active,
    /// The publisher finished the sample successfully.
    Complete,
    /// The publisher aborted or dropped the active writer.
    Aborted,
}

/// One atomically observed progressive control snapshot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ProgressiveControlSnapshot {
    pub(crate) committed_len: u64,
    pub(crate) state: ProgressiveSampleState,
}

/// Publisher-written control data occupying exactly one 128-byte region.
#[derive(Debug)]
#[repr(C, align(128))]
pub struct ProgressiveControl {
    /// The committed length occupies the high 62 bits and the state the low 2.
    /// Every subscriber therefore observes both values from one atomic load.
    snapshot: AtomicU64,
    reserved: [u8; 120],
}

unsafe impl ZeroCopySend for ProgressiveControl {}

impl ProgressiveControl {
    pub(crate) fn new() -> Self {
        Self {
            snapshot: AtomicU64::new(Self::encode(0, ProgressiveSampleState::Active)),
            reserved: [0; 120],
        }
    }

    #[inline]
    const fn encode(committed_len: u64, state: ProgressiveSampleState) -> u64 {
        let state = match state {
            ProgressiveSampleState::Active => STATE_ACTIVE,
            ProgressiveSampleState::Complete => STATE_COMPLETE,
            ProgressiveSampleState::Aborted => STATE_ABORTED,
        };
        (committed_len << COMMITTED_LEN_SHIFT) | state
    }

    #[inline]
    fn decode(value: u64) -> ProgressiveControlSnapshot {
        let state = match value & STATE_MASK {
            STATE_ACTIVE => ProgressiveSampleState::Active,
            STATE_COMPLETE => ProgressiveSampleState::Complete,
            STATE_ABORTED => ProgressiveSampleState::Aborted,
            value => panic!("invalid progressive sample state {value}"),
        };
        ProgressiveControlSnapshot {
            committed_len: value >> COMMITTED_LEN_SHIFT,
            state,
        }
    }

    #[inline]
    pub(crate) fn snapshot(&self, ordering: Ordering) -> ProgressiveControlSnapshot {
        Self::decode(self.snapshot.load(ordering))
    }

    #[inline]
    pub(crate) fn commit(&self, value: u64) {
        debug_assert!(value <= MAX_COMMITTED_LEN);
        self.snapshot.store(
            Self::encode(value, ProgressiveSampleState::Active),
            Ordering::Release,
        );
    }

    #[inline]
    pub(crate) fn complete(&self) {
        let current = self.snapshot(Ordering::Acquire);
        debug_assert_eq!(current.state, ProgressiveSampleState::Active);
        self.snapshot.store(
            Self::encode(current.committed_len, ProgressiveSampleState::Complete),
            Ordering::Release,
        );
    }

    #[inline]
    pub(crate) fn abort(&self) {
        let current = self.snapshot(Ordering::Acquire);
        debug_assert_eq!(current.state, ProgressiveSampleState::Active);
        self.snapshot.store(
            Self::encode(current.committed_len, ProgressiveSampleState::Aborted),
            Ordering::Release,
        );
    }
}

/// Internal header for an experimental progressive publish/subscribe sample.
///
/// The hot control line and immutable origin metadata occupy separate 128-byte
/// regions. Since this type is also the allocation's alignment, sample layouts
/// and pool bucket strides are multiples of 128 bytes.
#[derive(Debug)]
#[repr(C, align(128))]
pub struct Header {
    control: ProgressiveControl,
    node_id: UniqueNodeId,
    publisher_port_id: UniquePublisherId,
    number_of_elements: u64,
    reserved: [u8; 88],
}

unsafe impl ZeroCopySend for Header {}

impl Header {
    pub(crate) fn new(
        node_id: UniqueNodeId,
        publisher_port_id: UniquePublisherId,
        number_of_elements: u64,
    ) -> Self {
        Self {
            control: ProgressiveControl::new(),
            node_id,
            publisher_port_id,
            number_of_elements,
            reserved: [0; 88],
        }
    }

    pub(crate) fn control(&self) -> &ProgressiveControl {
        &self.control
    }

    /// Returns the source node identifier.
    pub fn node_id(&self) -> UniqueNodeId {
        self.node_id
    }

    /// Returns the source publisher identifier.
    pub fn publisher_id(&self) -> UniquePublisherId {
        self.publisher_port_id
    }

    /// Returns the byte capacity of the progressive sample.
    pub fn number_of_elements(&self) -> u64 {
        self.number_of_elements
    }
}

const _: () = assert!(align_of::<ProgressiveControl>() == PROGRESSIVE_CONTROL_ALIGNMENT);
const _: () = assert!(size_of::<ProgressiveControl>() == PROGRESSIVE_CONTROL_ALIGNMENT);
const _: () = assert!(align_of::<Header>() == PROGRESSIVE_CONTROL_ALIGNMENT);
const _: () = assert!(size_of::<Header>() == 2 * PROGRESSIVE_CONTROL_ALIGNMENT);
const _: () = assert!(offset_of!(Header, control) == 0);
const _: () = assert!(offset_of!(Header, node_id) >= PROGRESSIVE_CONTROL_ALIGNMENT);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_and_header_layout_are_cache_line_isolated() {
        assert_eq!(align_of::<ProgressiveControl>(), 128);
        assert_eq!(size_of::<ProgressiveControl>(), 128);
        assert_eq!(align_of::<Header>(), 128);
        assert_eq!(size_of::<Header>(), 256);
        assert_eq!(offset_of!(Header, control), 0);
        assert!(offset_of!(Header, node_id) >= 128);
    }

    #[test]
    fn committed_length_and_state_share_one_atomic_snapshot() {
        let control = ProgressiveControl::new();
        assert_eq!(
            control.snapshot(Ordering::Acquire),
            ProgressiveControlSnapshot {
                committed_len: 0,
                state: ProgressiveSampleState::Active,
            }
        );

        control.commit(73);
        assert_eq!(
            control.snapshot(Ordering::Acquire),
            ProgressiveControlSnapshot {
                committed_len: 73,
                state: ProgressiveSampleState::Active,
            }
        );

        control.complete();
        assert_eq!(
            control.snapshot(Ordering::Acquire),
            ProgressiveControlSnapshot {
                committed_len: 73,
                state: ProgressiveSampleState::Complete,
            }
        );
    }
}
