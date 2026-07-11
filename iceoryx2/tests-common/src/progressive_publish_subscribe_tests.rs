// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

use iceoryx2::port::publisher::PublisherCreateError;
use iceoryx2::port::{BackpressureAction, LoanError};
use iceoryx2::prelude::*;
use iceoryx2::progressive_sample_mut::ProgressiveWriteError;
use iceoryx2::service::header::progressive_publish_subscribe::{
    PROGRESSIVE_CONTROL_ALIGNMENT, ProgressiveSampleState,
};
use iceoryx2::service::static_config::publish_subscribe::SampleDeliveryMode;
use iceoryx2::testing::{generate_isolated_config, generate_service_name};
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_testing_macros::test;

#[derive(Debug, Default, ZeroCopySend)]
#[repr(C)]
struct FrameInfo {
    sequence: u64,
}

#[test]
fn progressive_service_configuration_is_restricted_and_incompatible() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let name = generate_service_name();
    let service = node
        .service_builder(&name)
        .publish_subscribe::<[u8]>()
        .user_header::<FrameInfo>()
        .progressive()
        .create()
        .unwrap();

    assert_eq!(service.static_config().max_publishers(), 1);
    assert_eq!(service.static_config().history_size(), 0);
    assert!(!service.static_config().has_safe_overflow());
    assert_eq!(
        service.static_config().sample_delivery_mode(),
        SampleDeliveryMode::Progressive
    );

    assert!(
        node.service_builder(&name)
            .publish_subscribe::<[u8]>()
            .user_header::<FrameInfo>()
            .open()
            .is_err()
    );

    let _publisher = service.publisher_builder().create().unwrap();
    assert_eq!(
        service.publisher_builder().create().unwrap_err(),
        PublisherCreateError::ExceedsMaxSupportedPublishers
    );
}

#[test]
fn complete_service_cannot_be_opened_as_progressive() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let name = generate_service_name();
    let _service = node
        .service_builder(&name)
        .publish_subscribe::<[u8]>()
        .user_header::<FrameInfo>()
        .create()
        .unwrap();

    assert!(
        node.service_builder(&name)
            .publish_subscribe::<[u8]>()
            .user_header::<FrameInfo>()
            .progressive()
            .open()
            .is_err()
    );
}

#[test]
fn watermark_is_monotonic_bounded_and_prefix_only() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc_threadsafe::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .user_header::<FrameInfo>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(64)
        .create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();

    let mut loan = publisher.loan_slice_uninit(8).unwrap();
    loan.user_header_mut().sequence = 73;
    let mut writer = loan.send().unwrap();
    let sample = subscriber.receive().unwrap().unwrap();

    assert_eq!(sample.payload(), &[]);
    assert_eq!(sample.state(), ProgressiveSampleState::Filling);
    assert_eq!(sample.user_header().sequence, 73);

    writer.write_from_slice(&[1, 2, 3]).unwrap();
    assert_eq!(sample.payload(), &[1, 2, 3]);
    let early_prefix = sample.payload();

    unsafe { writer.set_published_len(3) }.unwrap();
    assert_eq!(
        unsafe { writer.set_published_len(2) }.unwrap_err(),
        ProgressiveWriteError::PublishedLengthRegressed
    );
    assert_eq!(
        unsafe { writer.set_published_len(9) }.unwrap_err(),
        ProgressiveWriteError::PublishedLengthExceedsCapacity
    );
    assert_eq!(early_prefix, &[1, 2, 3]);

    writer.write_from_slice(&[4, 5]).unwrap();
    assert_eq!(sample.payload(), &[1, 2, 3, 4, 5]);
    writer.finish().unwrap();
    assert_eq!(sample.state(), ProgressiveSampleState::Complete);
    assert_eq!(sample.published_len(), 5);
    assert_eq!(sample.payload(), &[1, 2, 3, 4, 5]);
}

#[test]
fn active_writer_drop_aborts_and_offset_is_delivered_once() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();

    let mut writer = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    let sample = subscriber.receive().unwrap().unwrap();
    assert!(subscriber.receive().unwrap().is_none());
    writer.write_from_slice(&[9, 8]).unwrap();
    assert!(subscriber.receive().unwrap().is_none());
    drop(writer);

    assert_eq!(sample.state(), ProgressiveSampleState::Aborted);
    assert_eq!(sample.payload(), &[9, 8]);
}

#[test]
fn multiple_subscribers_observe_identical_content_at_independent_speeds() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(32)
        .create()
        .unwrap();
    let first = service.subscriber_builder().create().unwrap();
    let second = service.subscriber_builder().create().unwrap();

    let mut writer = publisher.loan_slice_uninit(32).unwrap().send().unwrap();
    let first_sample = first.receive().unwrap().unwrap();
    let second_sample = second.receive().unwrap().unwrap();

    for block in 0..4u8 {
        let bytes = [block, block ^ 0x5a, block.wrapping_mul(17), 0xa5];
        writer.write_from_slice(&bytes).unwrap();
        assert_eq!(first_sample.payload(), second_sample.payload());
        assert_eq!(first_sample.published_len(), (block as usize + 1) * 4);
    }
    writer.finish().unwrap();
    assert_eq!(first_sample.state(), ProgressiveSampleState::Complete);
    assert_eq!(second_sample.state(), ProgressiveSampleState::Complete);
}

fn assert_consecutive_allocation_alignment(allocation_strategy: AllocationStrategy) {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .user_header::<FrameInfo>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(64)
        .max_loaned_samples(4)
        .allocation_strategy(allocation_strategy)
        .create()
        .unwrap();

    let loans = [
        publisher.loan_slice_uninit(64).unwrap(),
        publisher.loan_slice_uninit(64).unwrap(),
        publisher.loan_slice_uninit(64).unwrap(),
    ];
    let mut headers = [0usize; 3];
    let mut payloads = [0usize; 3];
    for (index, loan) in loans.iter().enumerate() {
        headers[index] = loan.header() as *const _ as usize;
        payloads[index] = unsafe { loan.payload_mut_ptr() } as usize;
        assert_eq!(headers[index] % PROGRESSIVE_CONTROL_ALIGNMENT, 0);
        assert_eq!(payloads[index] % PROGRESSIVE_CONTROL_ALIGNMENT, 0);
        assert!(payloads[index] >= headers[index] + 2 * PROGRESSIVE_CONTROL_ALIGNMENT);
    }
    headers.sort_unstable();
    payloads.sort_unstable();
    for pair in headers.windows(2) {
        assert_eq!((pair[1] - pair[0]) % PROGRESSIVE_CONTROL_ALIGNMENT, 0);
    }
    for pair in payloads.windows(2) {
        assert_eq!((pair[1] - pair[0]) % PROGRESSIVE_CONTROL_ALIGNMENT, 0);
    }
}

#[test]
fn static_allocations_preserve_control_payload_and_stride_alignment() {
    assert_consecutive_allocation_alignment(AllocationStrategy::Static);
}

#[test]
fn best_fit_allocations_preserve_control_payload_and_stride_alignment() {
    assert_consecutive_allocation_alignment(AllocationStrategy::BestFit);
}

#[test]
fn power_of_two_allocations_preserve_control_payload_and_stride_alignment() {
    assert_consecutive_allocation_alignment(AllocationStrategy::PowerOfTwo);
}

#[test]
fn progressive_mode_preserves_larger_requested_payload_alignment() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .payload_alignment(Alignment::ALIGN_4096)
        .progressive()
        .create()
        .unwrap();
    assert_eq!(
        service
            .static_config()
            .message_type_details()
            .payload
            .alignment(),
        Alignment::ALIGN_4096.value()
    );
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(64)
        .max_loaned_samples(3)
        .create()
        .unwrap();
    let loans = [
        publisher.loan_slice_uninit(64).unwrap(),
        publisher.loan_slice_uninit(64).unwrap(),
        publisher.loan_slice_uninit(64).unwrap(),
    ];
    for loan in &loans {
        assert_eq!(
            unsafe { loan.payload_mut_ptr() } as usize % Alignment::ALIGN_4096.value(),
            0
        );
    }
}

#[test]
fn unsent_drop_and_terminal_paths_return_publisher_loans() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .max_loaned_samples(1)
        .create()
        .unwrap();

    drop(publisher.loan_slice_uninit(8).unwrap());
    publisher
        .loan_slice_uninit(8)
        .unwrap()
        .send()
        .unwrap()
        .finish()
        .unwrap();
    publisher
        .loan_slice_uninit(8)
        .unwrap()
        .send()
        .unwrap()
        .abort()
        .unwrap();
    drop(publisher.loan_slice_uninit(8).unwrap().send().unwrap());
    assert!(publisher.loan_slice_uninit(8).is_ok());
}

#[test]
fn subscriber_lease_prevents_allocation_reuse_after_publisher_finish() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .allocation_strategy(AllocationStrategy::Static)
        .create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();

    let mut writer = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    let sample = subscriber.receive().unwrap().unwrap();
    writer.write_from_slice(&[42]).unwrap();
    writer.finish().unwrap();
    let held_payload = sample.payload().as_ptr();

    for _ in 0..8 {
        let loan = publisher.loan_slice_uninit(8).unwrap();
        assert_ne!(unsafe { loan.payload_mut_ptr() }.cast_const(), held_payload);
        drop(loan);
    }

    drop(sample);
    // The next allocation also drives completion-queue reclamation. Its exact
    // free-list position is allocator-specific; safety requires only that the
    // held address was not reused before this drop.
    assert!(publisher.loan_slice_uninit(8).is_ok());
}

#[test]
fn one_subscriber_may_drop_while_another_continues() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .create()
        .unwrap();
    let first = service.subscriber_builder().create().unwrap();
    let second = service.subscriber_builder().create().unwrap();

    let mut writer = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    let first_sample = first.receive().unwrap().unwrap();
    let second_sample = second.receive().unwrap().unwrap();
    writer.write_from_slice(&[1, 2, 3]).unwrap();
    drop(first_sample);
    writer.write_from_slice(&[4, 5]).unwrap();
    assert_eq!(second_sample.payload(), &[1, 2, 3, 4, 5]);
    writer.finish().unwrap();
    assert_eq!(second_sample.state(), ProgressiveSampleState::Complete);
}

#[test]
fn queue_backpressure_is_evaluated_only_when_sending_a_new_frame() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .max_loaned_samples(2)
        .backpressure_strategy(BackpressureStrategy::DiscardData)
        .create()
        .unwrap();
    let subscriber = service
        .subscriber_builder()
        .buffer_size(1)
        .create()
        .unwrap();

    let mut first = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    first.write_from_slice(&[1, 2, 3, 4]).unwrap();
    // Watermark updates did not enqueue another offset, so the queue still
    // contains exactly the first frame. Sending a second frame exercises the
    // configured queue-full policy.
    let second = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    let sample = subscriber.receive().unwrap().unwrap();
    assert_eq!(sample.payload(), &[1, 2, 3, 4]);
    assert!(subscriber.receive().unwrap().is_none());
    second.abort().unwrap();
    first.finish().unwrap();
}

#[test]
fn partial_delivery_failure_aborts_and_preserves_reference_accounting() {
    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
        .create::<ipc::Service>()
        .unwrap();
    let service = node
        .service_builder(&generate_service_name())
        .publish_subscribe::<[u8]>()
        .max_subscribers(2)
        .subscriber_max_buffer_size(1)
        .subscriber_max_borrowed_samples(1)
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .max_loaned_samples(2)
        .allocation_strategy(AllocationStrategy::Static)
        .override_sample_preallocation(|_| 2)
        .backpressure_strategy(BackpressureStrategy::RetryUntilDelivered)
        .set_backpressure_handler(|_| BackpressureAction::DiscardDataAndFail)
        .create()
        .unwrap();
    let first = service
        .subscriber_builder()
        .buffer_size(1)
        .create()
        .unwrap();
    let second = service
        .subscriber_builder()
        .buffer_size(1)
        .create()
        .unwrap();

    // Fill only the later subscriber's queue: both receive this offset, then
    // the first subscriber returns its reference while the second retains it.
    let prefill = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    drop(first.receive().unwrap().unwrap());
    prefill.finish().unwrap();

    // Delivery reaches the first subscriber before the full second queue
    // causes the injected error. The failed send publishes Aborted and returns
    // its publisher loan exactly once.
    let failed_loan = publisher.loan_slice_uninit(8).unwrap();
    let failed_payload = unsafe { failed_loan.payload_mut_ptr() };
    assert!(failed_loan.send().is_err());
    let aborted = first.receive().unwrap().unwrap();
    assert_eq!(aborted.state(), ProgressiveSampleState::Aborted);

    // The two preallocated chunks are both still held by subscriber
    // references, proving the failed send did not release the delivered
    // subscriber reference or double-release the publisher reference.
    assert_eq!(
        publisher.loan_slice_uninit(8).unwrap_err(),
        LoanError::OutOfMemory
    );

    drop(aborted);
    let reclaimed = publisher.loan_slice_uninit(8).unwrap();
    assert_eq!(unsafe { reclaimed.payload_mut_ptr() }, failed_payload);
    assert_eq!(
        publisher.loan_slice_uninit(8).unwrap_err(),
        LoanError::OutOfMemory
    );
    drop(reclaimed);
    drop(second.receive().unwrap().unwrap());
}

#[cfg(feature = "std")]
#[test]
fn concurrent_prefix_stress_has_no_torn_blocks() {
    const BLOCKS: usize = 512;
    const BLOCK_SIZE: usize = 16;
    const CAPACITY: usize = BLOCKS * BLOCK_SIZE;

    let config = generate_isolated_config();
    let node = NodeBuilder::new()
        .config(&config)
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
    let first = service.subscriber_builder().create().unwrap();
    let second = service.subscriber_builder().create().unwrap();
    let writer = publisher
        .loan_slice_uninit(CAPACITY)
        .unwrap()
        .send()
        .unwrap();
    let first_sample = first.receive().unwrap().unwrap();
    let second_sample = second.receive().unwrap().unwrap();

    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut writer = writer;
            for sequence in 0..BLOCKS as u64 {
                let mut block = [0u8; BLOCK_SIZE];
                block[..8].copy_from_slice(&sequence.to_le_bytes());
                block[8..].copy_from_slice(&(!sequence).to_le_bytes());
                writer.write_from_slice(&block).unwrap();
                if sequence % 7 == 0 {
                    std::thread::yield_now();
                }
            }
            writer.finish().unwrap();
        });

        for (reader_index, sample) in [first_sample, second_sample].into_iter().enumerate() {
            scope.spawn(move || {
                let mut consumed = 0;
                while consumed < BLOCKS {
                    let payload = sample.payload();
                    while consumed < payload.len() / BLOCK_SIZE {
                        let start = consumed * BLOCK_SIZE;
                        let sequence =
                            u64::from_le_bytes(payload[start..start + 8].try_into().unwrap());
                        let inverse =
                            u64::from_le_bytes(payload[start + 8..start + 16].try_into().unwrap());
                        assert_eq!(sequence, consumed as u64);
                        assert_eq!(inverse, !sequence);
                        consumed += 1;
                    }
                    if (consumed + reader_index) % 11 == 0 {
                        std::thread::yield_now();
                    }
                }
                while sample.state() == ProgressiveSampleState::Filling {
                    std::thread::yield_now();
                }
                assert_eq!(sample.state(), ProgressiveSampleState::Complete);
                assert_eq!(sample.published_len(), CAPACITY);
            });
        }
    });
}

#[cfg(feature = "std")]
#[test]
fn multi_process_progressive_stress_has_no_torn_blocks() {
    const ROLE: &str = "IOX2_PROGRESSIVE_TEST_ROLE";
    const NAME: &str = "IOX2_PROGRESSIVE_TEST_SERVICE";
    const BLOCKS: usize = 128;
    const BLOCK_SIZE: usize = 16;
    const CAPACITY: usize = BLOCKS * BLOCK_SIZE;

    let run_role = |role: &str, service_name: &str| {
        let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
        let name: ServiceName = service_name.try_into().unwrap();
        let service = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .progressive()
            .open_or_create()
            .unwrap();

        if role == "publisher" {
            let publisher = service
                .publisher_builder()
                .initial_max_slice_len(CAPACITY)
                .create()
                .unwrap();
            std::thread::sleep(core::time::Duration::from_millis(250));
            let mut writer = publisher
                .loan_slice_uninit(CAPACITY)
                .unwrap()
                .send()
                .unwrap();
            for sequence in 0..BLOCKS as u64 {
                let mut block = [0u8; BLOCK_SIZE];
                block[..8].copy_from_slice(&sequence.to_le_bytes());
                block[8..].copy_from_slice(&(!sequence).to_le_bytes());
                writer.write_from_slice(&block).unwrap();
                if sequence % 5 == 0 {
                    std::thread::yield_now();
                }
            }
            writer.finish().unwrap();
            std::thread::sleep(core::time::Duration::from_millis(100));
        } else {
            let subscriber = service.subscriber_builder().create().unwrap();
            let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
            let sample = loop {
                if let Some(sample) = subscriber.receive().unwrap() {
                    break sample;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for frame"
                );
                std::thread::yield_now();
            };
            let mut consumed = 0;
            while consumed < BLOCKS {
                let payload = sample.payload();
                while consumed < payload.len() / BLOCK_SIZE {
                    let start = consumed * BLOCK_SIZE;
                    let sequence =
                        u64::from_le_bytes(payload[start..start + 8].try_into().unwrap());
                    let inverse =
                        u64::from_le_bytes(payload[start + 8..start + 16].try_into().unwrap());
                    assert_eq!(sequence, consumed as u64);
                    assert_eq!(inverse, !sequence);
                    consumed += 1;
                }
                assert_ne!(sample.state(), ProgressiveSampleState::Aborted);
                std::thread::yield_now();
            }
            while sample.state() == ProgressiveSampleState::Filling {
                std::thread::yield_now();
            }
            assert_eq!(sample.state(), ProgressiveSampleState::Complete);
            assert_eq!(sample.published_len(), CAPACITY);
        }
    };

    if let (Ok(role), Ok(service_name)) = (std::env::var(ROLE), std::env::var(NAME)) {
        run_role(&role, &service_name);
        return;
    }

    let service_name = generate_service_name().to_string();
    let executable = std::env::current_exe().unwrap();
    let filter = "multi_process_progressive_stress_has_no_torn_blocks";
    let spawn = |role: &str| {
        std::process::Command::new(&executable)
            .arg(filter)
            .env(ROLE, role)
            .env(NAME, &service_name)
            .spawn()
            .unwrap()
    };

    let mut first = spawn("subscriber-fast");
    let mut second = spawn("subscriber-slow");
    std::thread::sleep(core::time::Duration::from_millis(100));
    let mut publisher = spawn("publisher");

    assert!(publisher.wait().unwrap().success());
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
}

#[cfg(feature = "std")]
#[test]
fn abrupt_publisher_process_death_is_reported_as_abort() {
    const ROLE: &str = "IOX2_PROGRESSIVE_CRASH_TEST_ROLE";
    const NAME: &str = "IOX2_PROGRESSIVE_CRASH_TEST_SERVICE";

    let run_role = |role: &str, service_name: &str| {
        let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
        let name: ServiceName = service_name.try_into().unwrap();
        let service = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .progressive()
            .open_or_create()
            .unwrap();

        if role == "publisher" {
            let publisher = service
                .publisher_builder()
                .initial_max_slice_len(16)
                .create()
                .unwrap();
            std::thread::sleep(core::time::Duration::from_millis(250));
            let mut writer = publisher.loan_slice_uninit(16).unwrap().send().unwrap();
            writer.write_from_slice(&[1, 2, 3, 4]).unwrap();
            // Model an abrupt process failure: neither writer nor publisher
            // destructors run, so no shared Aborted store is possible.
            std::process::abort();
        }

        let subscriber = service.subscriber_builder().create().unwrap();
        let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
        let sample = loop {
            if let Some(sample) = subscriber.receive().unwrap() {
                break sample;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for frame"
            );
            std::thread::yield_now();
        };
        while sample.published_len() < 4 {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for published prefix"
            );
            std::thread::yield_now();
        }
        assert_eq!(sample.payload(), &[1, 2, 3, 4]);

        loop {
            if sample.state_with_publisher_liveness().unwrap() == ProgressiveSampleState::Aborted {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out detecting dead publisher"
            );
            std::thread::sleep(core::time::Duration::from_millis(10));
        }

        // Already published bytes remain readable after the derived abort.
        assert_eq!(sample.payload(), &[1, 2, 3, 4]);
    };

    if let (Ok(role), Ok(service_name)) = (std::env::var(ROLE), std::env::var(NAME)) {
        run_role(&role, &service_name);
        return;
    }

    let service_name = generate_service_name().to_string();
    let executable = std::env::current_exe().unwrap();
    let filter = "abrupt_publisher_process_death_is_reported_as_abort";
    let spawn = |role: &str| {
        std::process::Command::new(&executable)
            .arg(filter)
            .env(ROLE, role)
            .env(NAME, &service_name)
            .spawn()
            .unwrap()
    };

    let mut subscriber = spawn("subscriber");
    std::thread::sleep(core::time::Duration::from_millis(100));
    let mut publisher = spawn("publisher");
    assert!(!publisher.wait().unwrap().success());
    assert!(subscriber.wait().unwrap().success());
}

#[cfg(feature = "std")]
#[test]
fn abrupt_subscriber_process_death_reclaims_held_progressive_sample() {
    const ROLE: &str = "IOX2_PROGRESSIVE_SUBSCRIBER_DEATH_ROLE";
    const NAME: &str = "IOX2_PROGRESSIVE_SUBSCRIBER_DEATH_SERVICE";
    const READY: &str = "IOX2_PROGRESSIVE_SUBSCRIBER_DEATH_READY";

    if let (Ok(role), Ok(service_name), Ok(ready_path)) = (
        std::env::var(ROLE),
        std::env::var(NAME),
        std::env::var(READY),
    ) {
        assert_eq!(role, "subscriber");
        let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
        let name: ServiceName = service_name.as_str().try_into().unwrap();
        let service = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .progressive()
            .open()
            .unwrap();
        let subscriber = service
            .subscriber_builder()
            .buffer_size(1)
            .create()
            .unwrap();
        std::fs::write(&ready_path, b"ready").unwrap();
        let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
        loop {
            if subscriber.receive().unwrap().is_some() {
                // Exit without returning the held sample through its completion
                // queue or running any subscriber/node destructors.
                std::process::abort();
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
    }

    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service_name = generate_service_name();
    let service = node
        .service_builder(&service_name)
        .publish_subscribe::<[u8]>()
        .max_subscribers(1)
        .subscriber_max_buffer_size(1)
        .subscriber_max_borrowed_samples(1)
        .progressive()
        .create()
        .unwrap();
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(8)
        .max_loaned_samples(1)
        .allocation_strategy(AllocationStrategy::Static)
        .override_sample_preallocation(|_| 1)
        .create()
        .unwrap();

    let ready_path = std::env::temp_dir().join(format!(
        "iox2-progressive-subscriber-death-{}",
        service_name
    ));
    let _ = std::fs::remove_file(&ready_path);
    let executable = std::env::current_exe().unwrap();
    let mut subscriber = std::process::Command::new(executable)
        .arg("abrupt_subscriber_process_death_reclaims_held_progressive_sample")
        .env(ROLE, "subscriber")
        .env(NAME, service_name.to_string())
        .env(READY, &ready_path)
        .spawn()
        .unwrap();

    let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
    while !ready_path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for subscriber readiness"
        );
        std::thread::sleep(core::time::Duration::from_millis(10));
    }

    let mut writer = publisher.loan_slice_uninit(8).unwrap().send().unwrap();
    writer.write_from_slice(&[1, 2, 3, 4]).unwrap();
    assert!(!subscriber.wait().unwrap().success());
    writer.finish().unwrap();

    let cleanup_deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
    let reclaimed = loop {
        let _ = service.try_cleanup_dead_nodes();
        match publisher.loan_slice_uninit(8) {
            Ok(sample) => break sample,
            Err(LoanError::OutOfMemory) => {
                assert!(
                    std::time::Instant::now() < cleanup_deadline,
                    "dead subscriber permanently pinned the allocation"
                );
                std::thread::sleep(core::time::Duration::from_millis(10));
            }
            Err(error) => panic!("unexpected loan error after subscriber death: {error:?}"),
        }
    };
    drop(reclaimed);
    let _ = std::fs::remove_file(ready_path);
}
