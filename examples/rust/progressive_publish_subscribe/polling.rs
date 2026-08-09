// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iceoryx2::service::header::progressive_publish_subscribe::ProgressiveSampleState;

const ROWS: usize = 32;
const ROW_BYTES: usize = 256;
const FRAME_BYTES: usize = ROWS * ROW_BYTES;

#[derive(Debug, Default, ZeroCopySend)]
#[repr(C)]
struct FrameInfo {
    rows: u32,
    row_bytes: u32,
}

fn validate_row(row_index: usize, row: &[u8]) {
    for (column, value) in row.iter().enumerate() {
        assert_eq!(*value, (row_index as u8).wrapping_add(column as u8));
    }
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let node = NodeBuilder::new().create::<ipc_threadsafe::Service>()?;
    let service = node
        .service_builder(&"Progressive/Image/Polling".try_into()?)
        .publish_subscribe::<[u8]>()
        .user_header::<FrameInfo>()
        .progressive()
        .open_or_create()?;

    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(FRAME_BYTES)
        .create()?;
    let fast_subscriber = service.subscriber_builder().create()?;
    let slow_subscriber = service.subscriber_builder().create()?;

    let mut loan = publisher.loan_slice_uninit(FRAME_BYTES)?;
    *loan.user_header_mut() = FrameInfo {
        rows: ROWS as u32,
        row_bytes: ROW_BYTES as u32,
    };
    let writer = loan.announce()?;
    println!(
        "announced frame to {} subscribers",
        writer.number_of_recipients()
    );
    let fast_sample = fast_subscriber
        .receive()?
        .expect("fast subscriber missed frame");
    let slow_sample = slow_subscriber
        .receive()?
        .expect("slow subscriber missed frame");

    let epoch = Instant::now();
    let publication_times: Arc<[AtomicU64; ROWS]> =
        Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

    let (fast_latency, slow_latency) = std::thread::scope(|scope| {
        let writer_times = publication_times.clone();
        let writer_thread = scope.spawn(move || {
            let mut writer = writer;
            for row_index in 0..ROWS {
                let mut row = [0u8; ROW_BYTES];
                for (column, value) in row.iter_mut().enumerate() {
                    *value = (row_index as u8).wrapping_add(column as u8);
                }
                writer_times[row_index].store(epoch.elapsed().as_nanos() as u64, Ordering::Relaxed);
                writer.write_from_slice(&row).unwrap();
                std::thread::sleep(Duration::from_millis(2));
            }
            writer.complete().unwrap();
        });

        let fast_times = publication_times.clone();
        let fast_thread = scope.spawn(move || {
            let mut consumed_rows = 0;
            let mut latency_ns = 0u128;
            while consumed_rows < ROWS {
                let payload = fast_sample.payload();
                let available = &payload[consumed_rows * ROW_BYTES..];
                for row in available.chunks_exact(ROW_BYTES) {
                    validate_row(consumed_rows, row);
                    let published = fast_times[consumed_rows].load(Ordering::Relaxed);
                    latency_ns += epoch.elapsed().as_nanos().saturating_sub(published as u128);
                    consumed_rows += 1;
                }
                assert_ne!(fast_sample.state(), ProgressiveSampleState::Aborted);
                core::hint::spin_loop();
            }
            latency_ns / ROWS as u128
        });

        let slow_thread = scope.spawn(move || {
            let mut consumed_rows = 0;
            let mut latency_ns = 0u128;
            while consumed_rows < ROWS {
                let payload = slow_sample.payload();
                let available = &payload[consumed_rows * ROW_BYTES..];
                for row in available.chunks_exact(ROW_BYTES) {
                    validate_row(consumed_rows, row);
                    let published = publication_times[consumed_rows].load(Ordering::Relaxed);
                    latency_ns += epoch.elapsed().as_nanos().saturating_sub(published as u128);
                    consumed_rows += 1;
                }
                assert_ne!(slow_sample.state(), ProgressiveSampleState::Aborted);
                std::thread::sleep(Duration::from_millis(1));
            }
            latency_ns / ROWS as u128
        });

        writer_thread.join().unwrap();
        (fast_thread.join().unwrap(), slow_thread.join().unwrap())
    });

    println!("validated {ROWS} rows in two readers");
    println!("fast reader mean observation latency: {fast_latency} ns");
    println!("slow reader mean observation latency: {slow_latency} ns");
    Ok(())
}
