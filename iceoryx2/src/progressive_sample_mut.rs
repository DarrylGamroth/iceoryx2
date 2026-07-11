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

/// A progressive watermark or writer operation failed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressiveWriteError {
    /// The requested watermark is smaller than the current watermark.
    PublishedLengthRegressed,
    /// The requested watermark exceeds the allocation capacity.
    PublishedLengthExceedsCapacity,
    /// The sample is already complete or aborted.
    SampleIsTerminal,
    /// The input does not fit in the unpublished suffix.
    InsufficientCapacity,
}

impl core::fmt::Display for ProgressiveWriteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ProgressiveWriteError::{self:?}")
    }
}

impl core::error::Error for ProgressiveWriteError {}

/// A private, unsent progressive publisher loan.
///
/// The complete allocation is private in this state. `send()` transfers the
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

    /// Returns a mutable reference to the application user header before send.
    pub fn user_header_mut(&mut self) -> &mut UserHeader {
        unsafe { &mut *self.user_header }
    }

    /// Returns the allocation's raw payload pointer for an external writer.
    ///
    /// # Safety
    ///
    /// The caller must ensure there is only one writer, all writes remain in
    /// bounds, published bytes are never modified again, and the external
    /// writer stops before finish, abort, drop, or deallocation.
    pub unsafe fn payload_mut_ptr(&self) -> *mut u8 {
        self.payload
    }

    /// Delivers the allocation offset once to every currently connected
    /// subscriber and transfers this exact publisher loan to an active writer.
    pub fn send(mut self) -> Result<ProgressiveSampleMut<Service, UserHeader>, SendError> {
        let result = self
            .publisher_shared_state
            .lock()
            .send_progressive_sample(self.offset_to_chunk, self.sample_size);

        match result {
            Ok(_) => {
                self.owns_loan = false;
                Ok(ProgressiveSampleMut {
                    publisher_shared_state: self.publisher_shared_state.clone(),
                    header: self.header,
                    user_header: self.user_header.cast_const(),
                    payload: self.payload,
                    capacity: self.capacity,
                    offset_to_chunk: self.offset_to_chunk,
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

/// Active progressive writer. It can mutate only the unpublished suffix.
///
/// The sent typestate intentionally has no whole-payload mutable API, mutable
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
            if control.state(Ordering::Acquire) == ProgressiveSampleState::Filling {
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

    fn validate_active(&self) -> Result<(), ProgressiveWriteError> {
        if self.control().state(Ordering::Acquire) != ProgressiveSampleState::Filling {
            return Err(ProgressiveWriteError::SampleIsTerminal);
        }
        Ok(())
    }

    /// Returns the immutable application user header.
    pub fn user_header(&self) -> &UserHeader {
        unsafe { &*self.user_header }
    }

    /// Returns the total byte capacity.
    pub fn payload_capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current published prefix length.
    pub fn published_len(&self) -> usize {
        usize::try_from(self.control().published_len(Ordering::Acquire))
            .expect("published length is unsupported by this target")
    }

    /// Returns a mutable view of only the unpublished suffix.
    pub fn unpublished_mut(&mut self) -> Result<&mut [MaybeUninit<u8>], ProgressiveWriteError> {
        self.validate_active()?;
        let published_len = self.published_len();
        let suffix_len = self
            .capacity
            .checked_sub(published_len)
            .ok_or(ProgressiveWriteError::PublishedLengthExceedsCapacity)?;
        let suffix_ptr = unsafe { self.payload.add(published_len) };
        Ok(unsafe { core::slice::from_raw_parts_mut(suffix_ptr.cast(), suffix_len) })
    }

    /// Copies bytes into the current unpublished suffix and release-publishes
    /// the enlarged immutable prefix.
    pub fn write_from_slice(&mut self, bytes: &[u8]) -> Result<(), ProgressiveWriteError> {
        let old_len = self.published_len();
        let new_len = old_len
            .checked_add(bytes.len())
            .ok_or(ProgressiveWriteError::PublishedLengthExceedsCapacity)?;
        if new_len > self.capacity {
            return Err(ProgressiveWriteError::InsufficientCapacity);
        }
        let suffix = self.unpublished_mut()?;
        for (dst, src) in suffix[..bytes.len()].iter_mut().zip(bytes) {
            dst.write(*src);
        }
        self.control().publish_len(new_len as u64);
        Ok(())
    }

    /// Advances the contiguous initialized byte watermark.
    ///
    /// # Safety
    ///
    /// The caller guarantees that every byte below `new_len` is initialized
    /// and visible to CPU readers and that no byte below it will be modified
    /// again. This does not establish external DMA cache coherency.
    pub unsafe fn set_published_len(
        &mut self,
        new_len: usize,
    ) -> Result<(), ProgressiveWriteError> {
        self.validate_active()?;
        let old_len = self.published_len();
        if new_len < old_len {
            return Err(ProgressiveWriteError::PublishedLengthRegressed);
        }
        if new_len > self.capacity {
            return Err(ProgressiveWriteError::PublishedLengthExceedsCapacity);
        }
        self.control().publish_len(new_len as u64);
        Ok(())
    }

    /// Marks the sample complete and releases the publisher loan exactly once.
    pub fn finish(mut self) -> Result<(), ProgressiveWriteError> {
        self.validate_active()?;
        if self.published_len() > self.capacity {
            return Err(ProgressiveWriteError::PublishedLengthExceedsCapacity);
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
    // writer touches only bytes above the release-published watermark and the
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
                    // Construct only the one-byte unpublished region directly
                    // from the stored raw pointer, never a complete slice.
                    unsafe { payload.add(index).write(index as u8) };
                    writer_storage.control.publish_len((index + 1) as u64);
                }
                writer_storage.control.complete();
            });

            let reader_storage = storage.clone();
            scope.spawn(move || {
                let payload = reader_storage.payload.get().cast::<u8>();
                let mut checked = 0;
                while checked < CAPACITY {
                    let published =
                        reader_storage.control.published_len(Ordering::Acquire) as usize;
                    let prefix = unsafe { core::slice::from_raw_parts(payload, published) };
                    for (index, value) in prefix[checked..].iter().enumerate() {
                        assert_eq!(*value, (checked + index) as u8);
                    }
                    checked = published;
                    core::hint::spin_loop();
                }
                while reader_storage.control.state(Ordering::Acquire)
                    == ProgressiveSampleState::Filling
                {
                    core::hint::spin_loop();
                }
                assert_eq!(
                    reader_storage.control.state(Ordering::Acquire),
                    ProgressiveSampleState::Complete
                );
            });
        });
    }
}
