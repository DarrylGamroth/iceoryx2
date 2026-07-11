// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Cache-line-isolated control and origin metadata for progressive samples.

use core::mem::{align_of, offset_of, size_of};

use iceoryx2_bb_concurrency::atomic::{AtomicU32, AtomicU64, Ordering};
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::identifiers::{UniqueNodeId, UniquePublisherId};

/// Alignment used to isolate independently accessed progressive sample data.
pub const PROGRESSIVE_CONTROL_ALIGNMENT: usize = 128;

const STATE_FILLING: u32 = 1;
const STATE_COMPLETE: u32 = 2;
const STATE_ABORTED: u32 = 3;

/// Subscriber-visible terminal state of a progressive sample.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressiveSampleState {
    /// The publisher may still extend the immutable prefix.
    Filling,
    /// The publisher finished the sample successfully.
    Complete,
    /// The publisher aborted or dropped the active writer.
    Aborted,
}

/// Publisher-written control data occupying exactly one 128-byte region.
#[derive(Debug)]
#[repr(C, align(128))]
pub struct ProgressiveControl {
    published_len: AtomicU64,
    state: AtomicU32,
    change_counter: AtomicU32,
    reserved: [u8; 112],
}

unsafe impl ZeroCopySend for ProgressiveControl {}

impl ProgressiveControl {
    pub(crate) fn new() -> Self {
        Self {
            published_len: AtomicU64::new(0),
            state: AtomicU32::new(STATE_FILLING),
            change_counter: AtomicU32::new(0),
            reserved: [0; 112],
        }
    }

    #[inline]
    pub(crate) fn published_len(&self, ordering: Ordering) -> u64 {
        self.published_len.load(ordering)
    }

    #[inline]
    pub(crate) fn publish_len(&self, value: u64) {
        self.published_len.store(value, Ordering::Release);
    }

    #[inline]
    pub(crate) fn state(&self, ordering: Ordering) -> ProgressiveSampleState {
        match self.state.load(ordering) {
            STATE_FILLING => ProgressiveSampleState::Filling,
            STATE_COMPLETE => ProgressiveSampleState::Complete,
            STATE_ABORTED => ProgressiveSampleState::Aborted,
            value => panic!("invalid progressive sample state {value}"),
        }
    }

    #[inline]
    pub(crate) fn complete(&self) {
        self.state.store(STATE_COMPLETE, Ordering::Release);
    }

    #[inline]
    pub(crate) fn abort(&self) {
        self.state.store(STATE_ABORTED, Ordering::Release);
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
}
