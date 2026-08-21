// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

use iceoryx2::prelude::*;
use iceoryx2::testing::generate_service_name;
use iceoryx2_bb_concurrency::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn commit_and_polling_hot_paths_do_not_allocate() {
    const CAPACITY: usize = 128;
    let node = NodeBuilder::new()
        .create::<local_threadsafe::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(CAPACITY)
        .create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();
    let mut writer = publisher
        .loan_slice_uninit(CAPACITY)
        .unwrap()
        .announce()
        .unwrap();
    let sample = subscriber.receive().unwrap().unwrap();

    eprintln!("IOX2_HOT_PATH_BEGIN");
    ALLOCATIONS.store(0, Ordering::SeqCst);
    for index in 0..CAPACITY {
        writer.write_from_slice(&[index as u8]).unwrap();
        let snapshot = sample.snapshot();
        assert_eq!(snapshot.committed_len(), index + 1);
        assert_eq!(sample.payload()[index], index as u8);
        assert_eq!(&sample.payload()[index..], &[index as u8]);
        let _ = sample.state();
    }
    let hot_path_allocations = ALLOCATIONS.load(Ordering::SeqCst);
    eprintln!("IOX2_HOT_PATH_END");

    assert_eq!(hot_path_allocations, 0);
    writer.complete().unwrap();
}
