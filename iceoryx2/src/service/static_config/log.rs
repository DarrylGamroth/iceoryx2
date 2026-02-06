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

use super::message_type_details::MessageTypeDetails;
use crate::config;
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use serde::{Deserialize, Serialize};

/// The static configuration of a
/// [`MessagingPattern::Log`](crate::service::messaging_pattern::MessagingPattern::Log)
/// based service.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, ZeroCopySend, Serialize, Deserialize)]
#[repr(C)]
pub struct StaticConfig {
    pub(crate) max_subscribers: usize,
    pub(crate) max_publishers: usize,
    pub(crate) max_nodes: usize,
    pub(crate) history_size: usize,
    pub(crate) subscriber_max_buffer_size: usize,
    pub(crate) subscriber_max_borrowed_samples: usize,
    pub(crate) enable_safe_overflow: bool,
    pub(crate) message_type_details: MessageTypeDetails,
}

impl StaticConfig {
    pub(crate) fn new(config: &config::Config) -> Self {
        Self {
            max_subscribers: config.defaults.publish_subscribe.max_subscribers,
            max_publishers: config.defaults.publish_subscribe.max_publishers,
            max_nodes: config.defaults.publish_subscribe.max_nodes,
            history_size: config.defaults.publish_subscribe.publisher_history_size,
            subscriber_max_buffer_size: config
                .defaults
                .publish_subscribe
                .subscriber_max_buffer_size,
            subscriber_max_borrowed_samples: config
                .defaults
                .publish_subscribe
                .subscriber_max_borrowed_samples,
            enable_safe_overflow: config.defaults.publish_subscribe.enable_safe_overflow,
            message_type_details: MessageTypeDetails::default(),
        }
    }

    /// Returns the maximum supported amount of [`Node`](crate::node::Node)s.
    pub fn max_nodes(&self) -> usize {
        self.max_nodes
    }

    /// Returns the maximum supported amount of appender ports.
    pub fn max_appenders(&self) -> usize {
        self.max_publishers
    }

    /// Returns the maximum supported amount of tailer ports.
    pub fn max_tailers(&self) -> usize {
        self.max_subscribers
    }

    /// Returns the maximum retained sample count.
    pub fn retention_size(&self) -> usize {
        self.history_size
    }

    /// Returns the maximum supported tailer buffer size.
    pub fn tailer_max_buffer_size(&self) -> usize {
        self.subscriber_max_buffer_size
    }

    /// Returns how many samples a tailer can borrow in parallel at most.
    pub fn tailer_max_borrowed_samples(&self) -> usize {
        self.subscriber_max_borrowed_samples
    }

    /// Returns true if safe overflow is enabled.
    pub fn has_safe_overflow(&self) -> bool {
        self.enable_safe_overflow
    }

    /// Returns the type details of the service.
    pub fn message_type_details(&self) -> &MessageTypeDetails {
        &self.message_type_details
    }

    /// Returns the maximum supported amount of publisher ports.
    pub fn max_publishers(&self) -> usize {
        self.max_publishers
    }

    /// Returns the maximum supported amount of subscriber ports.
    pub fn max_subscribers(&self) -> usize {
        self.max_subscribers
    }

    /// Returns the maximum history size.
    pub fn history_size(&self) -> usize {
        self.history_size
    }

    /// Returns the maximum supported subscriber buffer size.
    pub fn subscriber_max_buffer_size(&self) -> usize {
        self.subscriber_max_buffer_size
    }

    /// Returns how many samples a subscriber can borrow in parallel at most.
    pub fn subscriber_max_borrowed_samples(&self) -> usize {
        self.subscriber_max_borrowed_samples
    }
}
