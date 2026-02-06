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

use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;

use crate::{node::NodeId, port::port_identifiers::UniquePublisherId};

/// Sample header used by
/// [`MessagingPattern::Log`](crate::service::messaging_pattern::MessagingPattern::Log).
#[derive(Debug, Copy, Clone, ZeroCopySend, PartialEq, Eq)]
#[repr(C)]
pub struct Header {
    node_id: NodeId,
    appender_port_id: UniquePublisherId,
    sequence: u64,
    number_of_elements: u64,
}

impl Header {
    pub(crate) fn new(
        node_id: NodeId,
        appender_port_id: UniquePublisherId,
        sequence: u64,
        number_of_elements: u64,
    ) -> Self {
        Self {
            node_id,
            appender_port_id,
            sequence,
            number_of_elements,
        }
    }

    /// Returns the source [`NodeId`].
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the source appender id.
    pub fn appender_id(&self) -> UniquePublisherId {
        self.appender_port_id
    }

    /// Returns the globally monotonic sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the amount of payload elements in the sample.
    pub fn number_of_elements(&self) -> u64 {
        self.number_of_elements
    }
}

/// Internal user-header representation for the log pattern.
///
/// It stores the user supplied header together with the log sequence number.
#[derive(Debug, Copy, Clone, ZeroCopySend, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct UserHeaderStorage<UserHeader: ZeroCopySend> {
    pub(crate) sequence: u64,
    pub(crate) user_header: UserHeader,
}

impl<UserHeader: ZeroCopySend + Default> Default for UserHeaderStorage<UserHeader> {
    fn default() -> Self {
        Self {
            sequence: 0,
            user_header: UserHeader::default(),
        }
    }
}
