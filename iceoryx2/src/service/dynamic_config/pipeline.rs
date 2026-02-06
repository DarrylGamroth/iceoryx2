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

//! Dynamic configuration of a staged pipeline service.

use iceoryx2_bb_memory::bump_allocator::BumpAllocator;

use crate::{
    node::NodeId,
    port::port_identifiers::UniquePortId,
    service::dynamic_config::PortCleanupAction,
};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DynamicConfigSettings;

/// Dynamic runtime state of a pipeline service.
#[repr(C)]
#[derive(Debug)]
pub struct DynamicConfig;

impl DynamicConfig {
    pub(crate) fn new(_: &DynamicConfigSettings) -> Self {
        Self
    }

    pub(crate) unsafe fn init(&mut self, _: &BumpAllocator) {}

    pub(crate) fn memory_size(_: &DynamicConfigSettings) -> usize {
        0
    }

    pub(crate) unsafe fn remove_dead_node_id<
        PortCleanup: FnMut(UniquePortId) -> PortCleanupAction,
    >(
        &self,
        _node_id: &NodeId,
        _port_cleanup_callback: PortCleanup,
    ) {
    }
}
