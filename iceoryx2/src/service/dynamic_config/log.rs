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

use iceoryx2_bb_concurrency::atomic::{AtomicU64, Ordering};
use iceoryx2_bb_elementary::CallbackProgression;
use iceoryx2_bb_memory::bump_allocator::BumpAllocator;

use crate::node::NodeId;
use crate::port::port_identifiers::UniquePortId;

use super::{publish_subscribe, PortCleanupAction};

/// Contains the communication settings of connected appenders.
pub type AppenderDetails = publish_subscribe::PublisherDetails;

/// Contains the communication settings of connected tailers.
pub type TailerDetails = publish_subscribe::SubscriberDetails;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DynamicConfigSettings {
    pub number_of_tailers: usize,
    pub number_of_appenders: usize,
}

/// The dynamic configuration of a
/// [`crate::service::messaging_pattern::MessagingPattern::Log`]
/// based service.
#[repr(C)]
#[derive(Debug)]
pub struct DynamicConfig {
    pub(crate) inner: publish_subscribe::DynamicConfig,
    pub(crate) next_sequence: AtomicU64,
}

impl DynamicConfig {
    pub(crate) fn new(config: &DynamicConfigSettings) -> Self {
        Self {
            inner: publish_subscribe::DynamicConfig::new(
                &publish_subscribe::DynamicConfigSettings {
                    number_of_subscribers: config.number_of_tailers,
                    number_of_publishers: config.number_of_appenders,
                },
            ),
            // Sequence 0 is reserved as "not initialized".
            next_sequence: AtomicU64::new(1),
        }
    }

    pub(crate) unsafe fn init(&mut self, allocator: &BumpAllocator) {
        self.inner.init(allocator);
    }

    pub(crate) fn memory_size(config: &DynamicConfigSettings) -> usize {
        publish_subscribe::DynamicConfig::memory_size(&publish_subscribe::DynamicConfigSettings {
            number_of_subscribers: config.number_of_tailers,
            number_of_publishers: config.number_of_appenders,
        })
    }

    pub(crate) unsafe fn remove_dead_node_id<
        PortCleanup: FnMut(UniquePortId) -> PortCleanupAction,
    >(
        &self,
        node_id: &NodeId,
        port_cleanup_callback: PortCleanup,
    ) {
        self.inner
            .remove_dead_node_id(node_id, port_cleanup_callback);
    }

    /// Returns how many appender ports are currently connected.
    pub fn number_of_appenders(&self) -> usize {
        self.inner.number_of_publishers()
    }

    /// Returns how many tailer ports are currently connected.
    pub fn number_of_tailers(&self) -> usize {
        self.inner.number_of_subscribers()
    }

    /// Iterates over all tailers and calls the callback with [`TailerDetails`].
    pub fn list_tailers<F: FnMut(&TailerDetails) -> CallbackProgression>(&self, callback: F) {
        self.inner.list_subscribers(callback);
    }

    /// Iterates over all appenders and calls the callback with [`AppenderDetails`].
    pub fn list_appenders<F: FnMut(&AppenderDetails) -> CallbackProgression>(&self, callback: F) {
        self.inner.list_publishers(callback);
    }

    pub(crate) fn claim_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }
}
