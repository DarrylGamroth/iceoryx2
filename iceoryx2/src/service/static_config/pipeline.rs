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

//! Static configuration of a staged pipeline service.

use crate::config;
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use serde::{Deserialize, Serialize};

use super::message_type_details::TypeDetail;

/// The static configuration of an
/// [`MessagingPattern::Pipeline`](crate::service::messaging_pattern::MessagingPattern::Pipeline)
/// based service.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, ZeroCopySend, Serialize, Deserialize)]
#[repr(C)]
pub struct StaticConfig {
    pub(crate) number_of_stages: usize,
    pub(crate) max_in_flight_samples: usize,
    pub(crate) max_nodes: usize,
    pub(crate) initial_max_slice_len: usize,
    pub(crate) payload_type_details: TypeDetail,
    pub(crate) user_header_type_details: TypeDetail,
}

impl StaticConfig {
    pub(crate) fn new(config: &config::Config) -> Self {
        Self {
            number_of_stages: 1,
            max_in_flight_samples: config.defaults.publish_subscribe.subscriber_max_buffer_size,
            max_nodes: config.defaults.publish_subscribe.max_nodes,
            initial_max_slice_len: 1,
            payload_type_details: TypeDetail::default(),
            user_header_type_details: TypeDetail::default(),
        }
    }

    /// Returns the amount of worker stages.
    pub fn number_of_stages(&self) -> usize {
        self.number_of_stages
    }

    /// Returns the bounded amount of in-flight samples on every stage boundary.
    pub fn max_in_flight_samples(&self) -> usize {
        self.max_in_flight_samples
    }

    /// Returns the maximum supported amount of [`Node`](crate::node::Node)s.
    pub fn max_nodes(&self) -> usize {
        self.max_nodes
    }

    /// Returns the default initial max slice length used by dynamic payload publishers.
    pub fn initial_max_slice_len(&self) -> usize {
        self.initial_max_slice_len
    }

    /// Returns payload type details of this pipeline service.
    pub fn payload_type_details(&self) -> &TypeDetail {
        &self.payload_type_details
    }

    /// Returns user header type details of this pipeline service.
    pub fn user_header_type_details(&self) -> &TypeDetail {
        &self.user_header_type_details
    }
}
