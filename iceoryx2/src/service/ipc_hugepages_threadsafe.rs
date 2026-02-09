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

//! # Example
//!
//! ```
//! use iceoryx2::prelude::*;
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let node = NodeBuilder::new().create::<ipc_hugepages_threadsafe::Service>()?;
//!
//! let service = node
//!     .service_builder(&"My/Funk/ServiceName".try_into()?)
//!     .publish_subscribe::<u64>()
//!     .open_or_create()?;
//!
//! let publisher = service.publisher_builder().create()?;
//! let subscriber = service.subscriber_builder().create()?;
//!
//! # Ok(())
//! # }
//! ```
//!
//! See [`Service`](crate::service) for more detailed examples.

use alloc::vec::Vec;
use core::fmt::Debug;

use crate::service::dynamic_config::DynamicConfig;
use iceoryx2_cal::named_concept::NamedConceptConfiguration;
use iceoryx2_cal::shm_allocator::bump_allocator::BumpAllocator;
use iceoryx2_cal::shm_allocator::pool_allocator::PoolAllocator;
use iceoryx2_cal::shm_allocator::ShmAllocator;
use iceoryx2_cal::*;

/// Defines a threadsafe zero copy inter-process communication setup with hugepage-backed payload segments.
#[derive(Debug, Clone)]
pub struct Service {}

#[doc(hidden)]
pub struct ServiceNameHasher {
    value: hash::HashValue,
}

impl hash::Hash for ServiceNameHasher {
    fn new(bytes: &[u8]) -> Self {
        let mut prefixed_input = Vec::with_capacity(bytes.len() + 15);
        prefixed_input.extend_from_slice(b"iox2_hugepages:");
        prefixed_input.extend_from_slice(bytes);

        let hash = hash::recommended::Recommended::new(&prefixed_input);
        Self {
            value: hash.value(),
        }
    }

    fn value(&self) -> hash::HashValue {
        self.value.clone()
    }
}

impl Service {
    fn hugepage_config<Allocator: ShmAllocator + Debug>(
        global_config: &crate::config::Config,
        suffix: &iceoryx2_bb_system_types::file_name::FileName,
    ) -> <shared_memory::recommended::IpcHugepages<Allocator> as named_concept::NamedConceptMgmt>::Configuration{
        let mut config = <<shared_memory::recommended::IpcHugepages<Allocator> as named_concept::NamedConceptMgmt>::Configuration>::default()
            .prefix(&global_config.global.prefix)
            .suffix(suffix)
            .path_hint(&global_config.global.service.hugepages.mount_path);

        let mut dynamic_storage_config = config.dynamic_storage_config().clone();
        dynamic_storage_config
            .set_hugepage_size_bytes(global_config.global.service.hugepages.hugepage_size_bytes);
        config = config.with_dynamic_storage_config(dynamic_storage_config);

        config
    }
}

impl crate::service::Service for Service {
    type StaticStorage = static_storage::recommended::Ipc;
    type ConfigSerializer = serialize::recommended::Recommended;
    type DynamicStorage = dynamic_storage::recommended::Ipc<DynamicConfig>;
    type ServiceNameHasher = ServiceNameHasher;
    type SharedMemory = shared_memory::recommended::IpcHugepages<PoolAllocator>;
    type ResizableSharedMemory = resizable_shared_memory::recommended::IpcHugepages<PoolAllocator>;
    type Connection = zero_copy_connection::recommended::Ipc;
    type Event = event::recommended::Ipc;
    type Monitoring = monitoring::recommended::Ipc;
    type Reactor = reactor::recommended::Ipc;
    type ArcThreadSafetyPolicy<T: Send + Debug> =
        arc_sync_policy::mutex_protected::MutexProtected<T>;
    type BlackboardMgmt<KeyType: Send + Sync + Debug + 'static> =
        dynamic_storage::recommended::Ipc<KeyType>;
    type BlackboardPayload = shared_memory::recommended::IpcHugepages<BumpAllocator>;

    fn data_segment_config(
        global_config: &crate::config::Config,
    ) -> <Self::SharedMemory as named_concept::NamedConceptMgmt>::Configuration {
        Self::hugepage_config::<PoolAllocator>(
            global_config,
            &global_config.global.service.data_segment_suffix,
        )
    }

    fn resizable_data_segment_config(
        global_config: &crate::config::Config,
    ) -> <Self::ResizableSharedMemory as named_concept::NamedConceptMgmt>::Configuration {
        Self::data_segment_config(global_config)
    }

    fn blackboard_payload_config(
        global_config: &crate::config::Config,
    ) -> <Self::BlackboardPayload as named_concept::NamedConceptMgmt>::Configuration {
        Self::hugepage_config::<BumpAllocator>(
            global_config,
            &global_config.global.service.blackboard_data_suffix,
        )
    }
}

impl crate::service::internal::ServiceInternal<Service> for Service {}
