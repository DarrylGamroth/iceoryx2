// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Publisher-side typestates for progressive byte-slice samples.

use core::fmt::Debug;
use core::mem::MaybeUninit;

use iceoryx2_bb_concurrency::atomic::Ordering;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::arc_sync_policy::ArcSyncPolicy;
use iceoryx2_cal::shm_allocator::PointerOffset;

use crate::port::SendError;
use crate::port::publisher::PublisherSharedState;
use crate::service::header::progressive_publish_subscribe::{Header, ProgressiveSampleState};

/// A progressive commit or writer operation failed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressiveWriteError {
    /// The requested committed length is smaller than the current committed length.
    CommittedLengthRegressed,
    /// The requested committed length exceeds the allocation capacity.
    CommittedLengthExceedsCapacity,
    /// The sample is already complete or aborted.
    SampleIsTerminal,
    /// The input does not fit in the uncommitted suffix.
    InsufficientCapacity,
}

impl core::fmt::Display for ProgressiveWriteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ProgressiveWriteError::{self:?}")
    }
}

impl core::error::Error for ProgressiveWriteError {}

/// A private, unannounced progressive publisher loan.
///
/// The complete allocation is private in this state. `announce()` transfers the
/// existing publisher loan into [`ProgressiveSampleMut`] without returning or
/// reacquiring it.
#[derive(Debug)]
pub struct ProgressiveSampleMutUninit<
    Service: crate::service::Service,
    UserHeader: Debug + ZeroCopySend,
> {
    pub(crate) publisher_shared_state:
        Service::ArcThreadSafetyPolicy<PublisherSharedState<Service>>,
    pub(crate) header: *mut Header,
    pub(crate) user_header: *mut UserHeader,
    pub(crate) payload: *mut u8,
    pub(crate) capacity: usize,
    pub(crate) offset_to_chunk: PointerOffset,
    pub(crate) sample_size: usize,
    pub(crate) owns_loan: bool,
}

unsafe impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Send
    for ProgressiveSampleMutUninit<Service, UserHeader>
where
    Service::ArcThreadSafetyPolicy<PublisherSharedState<Service>>: Send + Sync,
{
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Drop
    for ProgressiveSampleMutUninit<Service, UserHeader>
{
    fn drop(&mut self) {
        if self.owns_loan {
            self.publisher_shared_state
                .lock()
                .sender
                .return_loaned_sample(self.offset_to_chunk);
        }
    }
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressiveSampleMutUninit<Service, UserHeader>
{
    /// Returns the byte capacity of the allocation.
    pub fn payload_capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the internal header while the sample is private.
    pub fn header(&self) -> &Header {
        unsafe { &*self.header }
    }

    /// Returns a mutable view of the private, uninitialized payload.
    pub fn payload_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { core::slice::from_raw_parts_mut(self.payload.cast(), self.capacity) }
    }

    /// Returns an immutable reference to the application user header.
    pub fn user_header(&self) -> &UserHeader {
        unsafe { &*self.user_header }
    }

    /// Returns a mutable reference to the application user header before announcement.
    pub fn user_header_mut(&mut self) -> &mut UserHeader {
        unsafe { &mut *self.user_header }
    }

    /// Returns the allocation's raw payload pointer for an external writer.
    ///
    /// # Safety
    ///
    /// The caller must ensure there is only one writer, all writes remain in
    /// bounds, committed bytes are never modified again, and the external
    /// writer stops before complete, abort, drop, or deallocation.
    pub unsafe fn payload_mut_ptr(&self) -> *mut u8 {
        self.payload
    }

    /// Announces the allocation once to every currently connected subscriber
    /// and transfers this exact publisher loan to an active writer.
    ///
    /// Subscribers connecting after this call do not receive the active sample.
    /// Queue admission and configured backpressure are evaluated by this call;
    /// later commits update shared state without enqueuing another offset. The
    /// returned writer records the number of subscribers that received the
    /// announcement; see [`ProgressiveSampleMut::number_of_recipients`].
    pub fn announce(mut self) -> Result<ProgressiveSampleMut<Service, UserHeader>, SendError> {
        let result = self
            .publisher_shared_state
            .lock()
            .announce_progressive_sample(self.offset_to_chunk, self.sample_size);

        match result {
            Ok(number_of_recipients) => {
                self.owns_loan = false;
                Ok(ProgressiveSampleMut {
                    publisher_shared_state: self.publisher_shared_state.clone(),
                    header: self.header,
                    user_header: self.user_header.cast_const(),
                    payload: self.payload,
                    capacity: self.capacity,
                    offset_to_chunk: self.offset_to_chunk,
                    number_of_recipients,
                    owns_loan: true,
                })
            }
            Err(error) => {
                // Delivery can fail after a subset of receivers accepted the
                // offset. Make those leases terminal before the publisher loan
                // is returned by this value's drop path.
                unsafe { &*self.header }.control().abort();
                Err(error)
            }
        }
    }
}

/// Active progressive writer. It can mutate only the uncommitted suffix.
///
/// The announced typestate intentionally has no whole-payload mutable API, mutable
/// user-header API, or `DerefMut` implementation:
///
/// ```compile_fail
/// use iceoryx2::prelude::*;
/// use iceoryx2::progressive_sample_mut::ProgressiveSampleMut;
///
/// fn whole_payload_is_unavailable(
///     writer: &mut ProgressiveSampleMut<ipc::Service, ()>,
/// ) {
///     let _ = writer.payload_mut();
/// }
/// ```
///
/// ```compile_fail
/// use iceoryx2::prelude::*;
/// use iceoryx2::progressive_sample_mut::ProgressiveSampleMut;
///
/// fn mutable_user_header_is_unavailable(
///     writer: &mut ProgressiveSampleMut<ipc::Service, ()>,
/// ) {
///     let _ = writer.user_header_mut();
/// }
/// ```
///
/// ```compile_fail
/// use iceoryx2::prelude::*;
/// use iceoryx2::progressive_sample_mut::ProgressiveSampleMut;
///
/// fn deref_mut_is_unavailable(
///     writer: &mut ProgressiveSampleMut<ipc::Service, ()>,
/// ) {
///     let _: &mut [u8] = &mut *writer;
/// }
/// ```
#[derive(Debug)]
pub struct ProgressiveSampleMut<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
{
    publisher_shared_state: Service::ArcThreadSafetyPolicy<PublisherSharedState<Service>>,
    header: *const Header,
    user_header: *const UserHeader,
    payload: *mut u8,
    capacity: usize,
    offset_to_chunk: PointerOffset,
    number_of_recipients: usize,
    owns_loan: bool,
}

unsafe impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Send
    for ProgressiveSampleMut<Service, UserHeader>
where
    Service::ArcThreadSafetyPolicy<PublisherSharedState<Service>>: Send + Sync,
{
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend> Drop
    for ProgressiveSampleMut<Service, UserHeader>
{
    fn drop(&mut self) {
        if self.owns_loan {
            let control = unsafe { &*self.header }.control();
            if control.snapshot(Ordering::Acquire).state == ProgressiveSampleState::Active {
                control.abort();
            }
            self.publisher_shared_state
                .lock()
                .sender
                .return_loaned_sample(self.offset_to_chunk);
            self.owns_loan = false;
        }
    }
}

impl<Service: crate::service::Service, UserHeader: Debug + ZeroCopySend>
    ProgressiveSampleMut<Service, UserHeader>
{
    fn control(
        &self,
    ) -> &crate::service::header::progressive_publish_subscribe::ProgressiveControl {
        unsafe { &*self.header }.control()
    }

    fn validate_active(
        &self,
    ) -> Result<
        crate::service::header::progressive_publish_subscribe::ProgressiveControlSnapshot,
        ProgressiveWriteError,
    > {
        let snapshot = self.control().snapshot(Ordering::Acquire);
        if snapshot.state != ProgressiveSampleState::Active {
            return Err(ProgressiveWriteError::SampleIsTerminal);
        }
        Ok(snapshot)
    }

    /// Returns the immutable application user header.
    pub fn user_header(&self) -> &UserHeader {
        unsafe { &*self.user_header }
    }

    /// Returns the total byte capacity.
    pub fn payload_capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of subscribers that received the sample when it was announced.
    ///
    /// This is a fixed fact about the announcement. It does not report whether those
    /// subscribers are still connected or have processed the sample.
    pub fn number_of_recipients(&self) -> usize {
        self.number_of_recipients
    }

    /// Returns the current committed prefix length.
    pub fn committed_len(&self) -> usize {
        usize::try_from(self.control().snapshot(Ordering::Acquire).committed_len)
            .expect("committed length is unsupported by this target")
    }

    /// Returns a mutable view of only the uncommitted suffix.
    pub fn uncommitted_mut(&mut self) -> Result<&mut [MaybeUninit<u8>], ProgressiveWriteError> {
        let committed_len = usize::try_from(self.validate_active()?.committed_len)
            .expect("committed length is unsupported by this target");
        let suffix_len = self
            .capacity
            .checked_sub(committed_len)
            .ok_or(ProgressiveWriteError::CommittedLengthExceedsCapacity)?;
        let suffix_ptr = unsafe { self.payload.add(committed_len) };
        Ok(unsafe { core::slice::from_raw_parts_mut(suffix_ptr.cast(), suffix_len) })
    }

    /// Copies bytes into the current uncommitted suffix and release-commits
    /// the enlarged immutable prefix.
    pub fn write_from_slice(&mut self, bytes: &[u8]) -> Result<(), ProgressiveWriteError> {
        let old_len = self.committed_len();
        let new_len = old_len
            .checked_add(bytes.len())
            .ok_or(ProgressiveWriteError::CommittedLengthExceedsCapacity)?;
        if new_len > self.capacity {
            return Err(ProgressiveWriteError::InsufficientCapacity);
        }
        let suffix = self.uncommitted_mut()?;
        for (dst, src) in suffix[..bytes.len()].iter_mut().zip(bytes) {
            dst.write(*src);
        }
        self.control().commit(new_len as u64);
        Ok(())
    }

    /// Commits the contiguous initialized byte prefix through `new_len`.
    ///
    /// # Safety
    ///
    /// The caller guarantees that every byte below `new_len` is initialized
    /// and visible to CPU readers and that no byte below it will be modified
    /// again. This does not establish external DMA cache coherency.
    pub unsafe fn commit_until(&mut self, new_len: usize) -> Result<(), ProgressiveWriteError> {
        let old_len = usize::try_from(self.validate_active()?.committed_len)
            .expect("committed length is unsupported by this target");
        if new_len < old_len {
            return Err(ProgressiveWriteError::CommittedLengthRegressed);
        }
        if new_len > self.capacity {
            return Err(ProgressiveWriteError::CommittedLengthExceedsCapacity);
        }
        self.control().commit(new_len as u64);
        Ok(())
    }

    /// Atomically marks the current committed length complete and releases the
    /// publisher loan exactly once.
    pub fn complete(mut self) -> Result<(), ProgressiveWriteError> {
        let snapshot = self.validate_active()?;
        if snapshot.committed_len > self.capacity as u64 {
            return Err(ProgressiveWriteError::CommittedLengthExceedsCapacity);
        }
        self.control().complete();
        self.release_loan();
        Ok(())
    }

    /// Marks the sample aborted and releases the publisher loan exactly once.
    pub fn abort(mut self) -> Result<(), ProgressiveWriteError> {
        self.validate_active()?;
        self.control().abort();
        self.release_loan();
        Ok(())
    }

    fn release_loan(&mut self) {
        if self.owns_loan {
            self.publisher_shared_state
                .lock()
                .sender
                .return_loaned_sample(self.offset_to_chunk);
            self.owns_loan = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::sync::Arc;
    use core::mem::MaybeUninit;

    use iceoryx2_bb_concurrency::cell::UnsafeCell;

    use super::*;
    use crate::service::header::progressive_publish_subscribe::ProgressiveControl;

    const CAPACITY: usize = 64;

    #[repr(C, align(128))]
    struct AliasingModel {
        control: ProgressiveControl,
        payload: UnsafeCell<[MaybeUninit<u8>; CAPACITY]>,
    }

    // The test models the protocol's monotonic capability split: the sole
    // writer touches only bytes above the release-committed boundary and the
    // reader constructs only acquire-bounded immutable prefixes.
    unsafe impl Sync for AliasingModel {}

    #[test]
    fn progressive_aliasing_model_preserves_prefix_suffix_split() {
        let storage = Arc::new(AliasingModel {
            control: ProgressiveControl::new(),
            payload: UnsafeCell::new([MaybeUninit::uninit(); CAPACITY]),
        });

        std::thread::scope(|scope| {
            let writer_storage = storage.clone();
            scope.spawn(move || {
                let payload = writer_storage.payload.get().cast::<u8>();
                for index in 0..CAPACITY {
                    // Construct only the one-byte uncommitted region directly
                    // from the stored raw pointer, never a complete slice.
                    unsafe { payload.add(index).write(index as u8) };
                    writer_storage.control.commit((index + 1) as u64);
                }
                writer_storage.control.complete();
            });

            let reader_storage = storage.clone();
            scope.spawn(move || {
                let payload = reader_storage.payload.get().cast::<u8>();
                let mut checked = 0;
                while checked < CAPACITY {
                    let snapshot = reader_storage.control.snapshot(Ordering::Acquire);
                    let committed = snapshot.committed_len as usize;
                    let prefix = unsafe { core::slice::from_raw_parts(payload, committed) };
                    for (index, value) in prefix[checked..].iter().enumerate() {
                        assert_eq!(*value, (checked + index) as u8);
                    }
                    checked = committed;
                    core::hint::spin_loop();
                }
                while reader_storage.control.snapshot(Ordering::Acquire).state
                    == ProgressiveSampleState::Active
                {
                    core::hint::spin_loop();
                }
                assert_eq!(
                    reader_storage.control.snapshot(Ordering::Acquire).state,
                    ProgressiveSampleState::Complete
                );
            });
        });
    }
}
