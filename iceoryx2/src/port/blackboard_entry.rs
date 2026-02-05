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

use core::mem::{align_of, size_of};

use iceoryx2_bb_concurrency::atomic::{AtomicU64, Ordering};
use iceoryx2_bb_elementary::math::align;
use iceoryx2_bb_lock_free::spmc::unrestricted_atomic::UnrestrictedAtomicMgmt;

pub(crate) const LATEST_SLOT_BITS: u32 = 16;
const LATEST_SLOT_MASK: u64 = (1u64 << LATEST_SLOT_BITS) - 1;

pub(crate) const MAX_WRITER_SLOTS: usize = 1usize << LATEST_SLOT_BITS;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct Stamped<T: Copy> {
    pub(crate) seq: u64,
    pub(crate) value: T,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct EntryMgmt {
    latest: AtomicU64,
    seq: AtomicU64,
}

impl EntryMgmt {
    pub(crate) fn new() -> Self {
        Self {
            latest: AtomicU64::new(pack_latest(0, 0)),
            seq: AtomicU64::new(0),
        }
    }

    pub(crate) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn load_latest(&self) -> u64 {
        self.latest.load(Ordering::Acquire)
    }

    pub(crate) fn publish_latest(&self, seq: u64, slot_index: u32) {
        let desired = pack_latest(seq, slot_index);
        let mut current = self.latest.load(Ordering::Relaxed);
        loop {
            let (current_seq, _) = unpack_latest(current);
            if current_seq >= seq {
                return;
            }
            match self
                .latest
                .compare_exchange(current, desired, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(updated) => current = updated,
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct EntryLayout {
    pub(crate) max_writers: usize,
    pub(crate) value_size: usize,
    pub(crate) value_alignment: usize,
    pub(crate) stamped_size: usize,
    pub(crate) stamped_alignment: usize,
    pub(crate) stamped_value_offset: usize,
    pub(crate) slot_stride: usize,
    pub(crate) slots_offset: usize,
    pub(crate) entry_size: usize,
    pub(crate) entry_alignment: usize,
}

pub(crate) fn pack_latest(seq: u64, slot_index: u32) -> u64 {
    (seq << LATEST_SLOT_BITS) | ((slot_index as u64) & LATEST_SLOT_MASK)
}

pub(crate) fn unpack_latest(value: u64) -> (u64, u32) {
    let seq = value >> LATEST_SLOT_BITS;
    let slot = (value & LATEST_SLOT_MASK) as u32;
    (seq, slot)
}

pub(crate) fn stamped_layout(value_size: usize, value_alignment: usize) -> (usize, usize, usize) {
    let stamped_alignment = align_of::<u64>().max(value_alignment);
    let value_offset = align(size_of::<u64>(), value_alignment);
    let stamped_size = align(value_offset + value_size, stamped_alignment);
    (stamped_size, stamped_alignment, value_offset)
}

pub(crate) fn entry_layout(
    value_size: usize,
    value_alignment: usize,
    max_writers: usize,
) -> EntryLayout {
    let (stamped_size, stamped_alignment, stamped_value_offset) =
        stamped_layout(value_size, value_alignment);

    let slot_alignment =
        UnrestrictedAtomicMgmt::__internal_get_unrestricted_atomic_alignment(stamped_alignment);
    let slot_size =
        UnrestrictedAtomicMgmt::__internal_get_unrestricted_atomic_size(stamped_size, stamped_alignment);
    let slot_stride = align(slot_size, slot_alignment);

    let mgmt_size = size_of::<EntryMgmt>();
    let mgmt_alignment = align_of::<EntryMgmt>();

    let entry_alignment = mgmt_alignment.max(slot_alignment);
    let slots_offset = align(mgmt_size, slot_alignment);
    let entry_size = align(slots_offset + max_writers * slot_stride, entry_alignment);

    EntryLayout {
        max_writers,
        value_size,
        value_alignment,
        stamped_size,
        stamped_alignment,
        stamped_value_offset,
        slot_stride,
        slots_offset,
        entry_size,
        entry_alignment,
    }
}

pub(crate) unsafe fn entry_mgmt_ptr(entry_base: *mut u8) -> *mut EntryMgmt {
    entry_base as *mut EntryMgmt
}

pub(crate) unsafe fn slot_ptr(entry_base: *mut u8, layout: &EntryLayout, slot_index: usize) -> *mut u8 {
    entry_base.add(layout.slots_offset + slot_index * layout.slot_stride)
}

pub(crate) unsafe fn atomic_mgmt_and_data_ptr(
    slot_ptr: *mut u8,
    layout: &EntryLayout,
) -> (*mut UnrestrictedAtomicMgmt, *mut u8) {
    let mgmt_ptr = slot_ptr as *mut UnrestrictedAtomicMgmt;
    let data_ptr = align(
        mgmt_ptr as usize + size_of::<UnrestrictedAtomicMgmt>(),
        layout.stamped_alignment,
    ) as *mut u8;
    (mgmt_ptr, data_ptr)
}

pub(crate) unsafe fn load_stamped_bytes(
    atomic_mgmt_ptr: *const UnrestrictedAtomicMgmt,
    data_ptr: *const u8,
    layout: &EntryLayout,
    stamped_out_ptr: *mut u8,
) {
    (*atomic_mgmt_ptr).load(
        stamped_out_ptr,
        layout.stamped_size,
        layout.stamped_alignment,
        data_ptr,
    );
}
