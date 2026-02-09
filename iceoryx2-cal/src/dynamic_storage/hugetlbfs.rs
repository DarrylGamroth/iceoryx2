// Copyright (c) 2025 Contributors to the Eclipse Foundation
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

pub use crate::dynamic_storage::*;
use crate::named_concept::NamedConceptDoesExistError;
use crate::named_concept::NamedConceptListError;
pub use core::ops::Deref;

use core::fmt::Debug;
use core::marker::PhantomData;
use core::ptr::NonNull;
use iceoryx2_bb_concurrency::atomic::Ordering;

use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;

use iceoryx2_bb_concurrency::atomic::AtomicU64;
use iceoryx2_bb_elementary::package_version::PackageVersion;
use iceoryx2_bb_posix::adaptive_wait::AdaptiveWaitBuilder;
use iceoryx2_bb_posix::directory::*;
use iceoryx2_bb_posix::file::File;
use iceoryx2_bb_posix::file::FileAccessError;
use iceoryx2_bb_posix::file::FileBuilder;
use iceoryx2_bb_posix::file::FileCreationError;
use iceoryx2_bb_posix::file::FileOpenError;
use iceoryx2_bb_posix::file::FileRemoveError;
use iceoryx2_bb_posix::file_descriptor::FileDescriptor;
use iceoryx2_bb_posix::file_descriptor::FileDescriptorBased;
use iceoryx2_bb_posix::file_descriptor::FileDescriptorManagement;
use iceoryx2_bb_posix::memory_mapping::MappingBehavior;
use iceoryx2_bb_posix::memory_mapping::MappingPermission;
use iceoryx2_bb_posix::memory_mapping::MemoryMapping;
use iceoryx2_bb_posix::memory_mapping::MemoryMappingBuilder;
use iceoryx2_bb_posix::shared_memory::*;
use iceoryx2_bb_system_types::file_path::FilePath;
use iceoryx2_bb_system_types::path::Path;
use iceoryx2_log::fail;
use iceoryx2_log::warn;

use crate::static_storage::file::NamedConceptConfiguration;
use crate::static_storage::file::NamedConceptRemoveError;

use self::dynamic_storage_configuration::DynamicStorageConfiguration;

const INIT_PERMISSIONS: Permission = Permission::OWNER_WRITE;

const PROC_MOUNTS: FilePath = unsafe { FilePath::new_unchecked_const(b"/proc/mounts") };
const PROC_MEMINFO: FilePath = unsafe { FilePath::new_unchecked_const(b"/proc/meminfo") };

#[cfg(not(feature = "dev_permissions"))]
const FINAL_PERMISSIONS: Permission = Permission::OWNER_ALL;

#[cfg(feature = "dev_permissions")]
const FINAL_PERMISSIONS: Permission = Permission::ALL;

/// The builder of [`Storage`].
#[derive(Debug)]
pub struct Builder<'builder, T: Send + Sync + Debug> {
    storage_name: FileName,
    call_drop_on_destruction: bool,
    supplementary_size: usize,
    has_ownership: bool,
    config: Configuration<T>,
    timeout: Duration,
    initializer: Initializer<'builder, T>,
    _phantom_data: PhantomData<T>,
}

#[derive(Debug)]
pub struct Configuration<T: Send + Sync + Debug> {
    suffix: FileName,
    prefix: FileName,
    path: Path,
    hugepage_size_bytes: Option<usize>,
    _data: PhantomData<T>,
    type_name: String,
}

impl<T: Send + Sync + Debug> Clone for Configuration<T> {
    fn clone(&self) -> Self {
        Self {
            suffix: self.suffix,
            prefix: self.prefix,
            path: self.path,
            hugepage_size_bytes: self.hugepage_size_bytes,
            _data: PhantomData,
            type_name: self.type_name.clone(),
        }
    }
}

#[repr(C)]
struct Data<T: Send + Sync + Debug> {
    version: AtomicU64,
    call_drop_on_destruction: bool,
    data: T,
}

impl<T: Send + Sync + Debug> Default for Configuration<T> {
    fn default() -> Self {
        Self {
            path: Storage::<()>::default_path_hint(),
            suffix: Storage::<()>::default_suffix(),
            prefix: Storage::<()>::default_prefix(),
            hugepage_size_bytes: None,
            _data: PhantomData,
            type_name: core::any::type_name::<T>().to_string(),
        }
    }
}

impl<T: Send + Sync + Debug> Configuration<T> {
    /// Optional override for the hugepage size in bytes.
    pub fn hugepage_size_bytes(mut self, value: Option<usize>) -> Self {
        self.hugepage_size_bytes = value;
        self
    }

    pub fn set_hugepage_size_bytes(&mut self, value: Option<usize>) {
        self.hugepage_size_bytes = value;
    }

    pub fn get_hugepage_size_bytes(&self) -> Option<usize> {
        self.hugepage_size_bytes
    }
}

impl<T: Send + Sync + Debug> DynamicStorageConfiguration for Configuration<T> {
    fn type_name(&self) -> &str {
        &self.type_name
    }
}

impl<T: Send + Sync + Debug> NamedConceptConfiguration for Configuration<T> {
    fn prefix(mut self, value: &FileName) -> Self {
        self.prefix = *value;
        self
    }

    fn get_prefix(&self) -> &FileName {
        &self.prefix
    }

    fn suffix(mut self, value: &FileName) -> Self {
        self.suffix = *value;
        self
    }

    fn path_hint(mut self, value: &Path) -> Self {
        self.path = *value;
        self
    }

    fn get_suffix(&self) -> &FileName {
        &self.suffix
    }

    fn get_path_hint(&self) -> &Path {
        &self.path
    }

    fn path_for(&self, value: &FileName) -> iceoryx2_bb_system_types::file_path::FilePath {
        self.path_for_with_type(value)
    }

    fn extract_name_from_file(&self, value: &FileName) -> Option<FileName> {
        self.extract_name_from_file_with_type(value)
    }
}

fn parse_hugepage_size_to_bytes(raw_value: &str) -> Option<usize> {
    let value = raw_value.trim();
    if value.is_empty() {
        return None;
    }

    let digit_end = value.bytes().take_while(|v| v.is_ascii_digit()).count();
    if digit_end == 0 {
        return None;
    }

    let base_value = value[..digit_end].parse::<usize>().ok()?;
    let unit = value[digit_end..].trim();
    let multiplier = match unit {
        "" | "b" | "B" => 1usize,
        "k" | "K" | "kB" | "KB" => 1024usize,
        "m" | "M" | "mB" | "MB" => 1024usize * 1024usize,
        "g" | "G" | "gB" | "GB" => 1024usize * 1024usize * 1024usize,
        _ => return None,
    };

    base_value.checked_mul(multiplier)
}

fn decode_proc_mount_field(value: &str) -> String {
    let mut result = Vec::<u8>::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            let o1 = bytes[index + 1];
            let o2 = bytes[index + 2];
            let o3 = bytes[index + 3];
            if o1.is_ascii_digit()
                && o2.is_ascii_digit()
                && o3.is_ascii_digit()
                && o1 < b'8'
                && o2 < b'8'
                && o3 < b'8'
            {
                let decoded = (o1 - b'0') * 64 + (o2 - b'0') * 8 + (o3 - b'0');
                result.push(decoded);
                index += 4;
                continue;
            }
        }

        result.push(bytes[index]);
        index += 1;
    }

    match String::from_utf8(result) {
        Ok(v) => v,
        Err(_) => value.to_string(),
    }
}

fn normalize_mount_path(path: &str) -> &str {
    if path.len() > 1 {
        path.trim_end_matches('/')
    } else {
        path
    }
}

fn is_path_on_mount(path: &str, mount_path: &str) -> bool {
    if path == mount_path {
        return true;
    }

    if mount_path == "/" {
        return path.starts_with('/');
    }

    path.starts_with(mount_path)
        && matches!(
            path.as_bytes().get(mount_path.len()),
            Some(value) if *value == b'/'
        )
}

fn hugepage_size_from_mount_options(options: &str) -> Option<usize> {
    options
        .split(',')
        .find_map(|option| option.strip_prefix("pagesize="))
        .and_then(parse_hugepage_size_to_bytes)
}

fn read_small_file(path: &FilePath) -> Result<String, FileOpenError> {
    let file = FileBuilder::new(path).open_existing(AccessMode::Read)?;
    let mut content = String::new();
    match file.read_to_string(&mut content) {
        Ok(_) => Ok(content),
        Err(_) => Err(FileOpenError::UnknownError(0)),
    }
}

fn find_hugetlbfs_mount_in_mounts(
    path_as_str: &str,
    mounts_content: &str,
) -> Result<(String, Option<usize>), ()> {
    let normalized_path = normalize_mount_path(path_as_str);
    let mut best_mount: Option<(usize, String, Option<usize>)> = None;
    for line in mounts_content.lines() {
        let mut fields = line.split_whitespace();
        let _device = match fields.next() {
            Some(v) => v,
            None => continue,
        };
        let mount_path_raw = match fields.next() {
            Some(v) => v,
            None => continue,
        };
        let fs_type = match fields.next() {
            Some(v) => v,
            None => continue,
        };
        let options = match fields.next() {
            Some(v) => v,
            None => continue,
        };

        if fs_type != "hugetlbfs" {
            continue;
        }

        let decoded_mount_path = decode_proc_mount_field(mount_path_raw);
        let normalized_mount_path = normalize_mount_path(&decoded_mount_path);
        if !is_path_on_mount(normalized_path, normalized_mount_path) {
            continue;
        }

        let mount_path_len = normalized_mount_path.len();
        let is_better_match = match best_mount.as_ref() {
            Some((best_len, _, _)) => mount_path_len > *best_len,
            None => true,
        };
        if is_better_match {
            best_mount = Some((
                mount_path_len,
                normalized_mount_path.to_string(),
                hugepage_size_from_mount_options(options),
            ));
        }
    }

    best_mount
        .map(|(_, mount_path, hugepage_size)| (mount_path, hugepage_size))
        .ok_or(())
}

fn find_hugetlbfs_mount(path: &Path) -> Result<(String, Option<usize>), ()> {
    let path_as_str = match core::str::from_utf8(path.as_bytes()) {
        Ok(v) => v,
        Err(_) => return Err(()),
    };
    let mounts_content = match read_small_file(&PROC_MOUNTS) {
        Ok(v) => v,
        Err(_) => return Err(()),
    };

    find_hugetlbfs_mount_in_mounts(path_as_str, &mounts_content)
}

fn hugepage_size_from_meminfo() -> Option<usize> {
    let meminfo = read_small_file(&PROC_MEMINFO).ok()?;
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("Hugepagesize:"))
        .and_then(parse_hugepage_size_to_bytes)
}

fn validate_hugepage_size(hugepage_size: usize) -> Option<usize> {
    let base_page_size = iceoryx2_bb_posix::system_configuration::SystemInfo::PageSize.value();
    if hugepage_size == 0 || base_page_size == 0 || hugepage_size % base_page_size != 0 {
        return None;
    }

    Some(hugepage_size)
}

fn resolve_hugepage_size(config: &Path, override_hugepage_size: Option<usize>) -> Result<usize, ()> {
    let (_mount_path, mount_pagesize) = find_hugetlbfs_mount(config)?;
    if let Some(explicit_hugepage_size) = override_hugepage_size {
        return validate_hugepage_size(explicit_hugepage_size).ok_or(());
    }

    if let Some(mount_hugepage_size) = mount_pagesize {
        return validate_hugepage_size(mount_hugepage_size).ok_or(());
    }

    validate_hugepage_size(hugepage_size_from_meminfo().unwrap_or(0)).ok_or(())
}

fn round_up_to_multiple(value: usize, alignment: usize) -> Option<usize> {
    if alignment == 0 {
        return None;
    }

    let remainder = value % alignment;
    if remainder == 0 {
        return Some(value);
    }

    value.checked_add(alignment - remainder)
}

fn pre_touch_mapping(memory_mapping: &MemoryMapping, hugepage_size: usize) {
    if memory_mapping.size() == 0 || hugepage_size == 0 {
        return;
    }

    let start = memory_mapping.base_address() as *const u8;
    let size = memory_mapping.size();
    let mut offset = 0usize;
    while offset < size {
        unsafe { core::ptr::read_volatile(start.add(offset)) };
        offset = offset.saturating_add(hugepage_size);
    }

    let last_page_start = (size - 1) / hugepage_size * hugepage_size;
    if last_page_start + 1 < size {
        unsafe { core::ptr::read_volatile(start.add(size - 1)) };
    }
}

impl<T: Send + Sync + Debug> NamedConceptBuilder<Storage<T>> for Builder<'_, T> {
    fn new(storage_name: &FileName) -> Self {
        Self {
            call_drop_on_destruction: true,
            has_ownership: true,
            storage_name: *storage_name,
            supplementary_size: 0,
            config: Configuration::default(),
            timeout: Duration::ZERO,
            initializer: Initializer::new(|_, _| true),
            _phantom_data: PhantomData,
        }
    }

    fn config(mut self, config: &Configuration<T>) -> Self {
        self.config = config.clone();
        self
    }
}

impl<T: Send + Sync + Debug> Builder<'_, T> {
    fn open_impl(&self) -> Result<Storage<T>, DynamicStorageOpenError> {
        let msg = "Failed to open dynamic_storage::hugetlbfs::DynamicStorage";
        let hugepage_size = fail!(from self,
            when resolve_hugepage_size(&self.config.path, self.config.hugepage_size_bytes),
            with DynamicStorageOpenError::InternalError,
            "{msg} since path \"{}\" is not on a hugetlbfs mount or the hugepage size could not be resolved.",
            self.config.path);

        let full_path = self.config.path_for(&self.storage_name);
        let mut wait_for_read_write_access = fail!(from self, when AdaptiveWaitBuilder::new().create(),
                                    with DynamicStorageOpenError::InternalError,
                                    "{} since the AdaptiveWait could not be initialized.", msg);

        let mut elapsed_time = Duration::ZERO;
        let file = loop {
            match FileBuilder::new(&full_path).open_existing(AccessMode::ReadWrite) {
                Ok(v) => break v,
                Err(FileOpenError::FileDoesNotExist) => {
                    fail!(from self, with DynamicStorageOpenError::DoesNotExist,
                    "{} since a file with that name does not exists.", msg);
                }
                Err(FileOpenError::InsufficientPermissions) => {
                    if elapsed_time >= self.timeout {
                        fail!(from self, with DynamicStorageOpenError::InitializationNotYetFinalized,
                        "{} since it is not readable - (it is not initialized after {:?}).",
                        msg, self.timeout);
                    }
                }
                Err(_) => {
                    fail!(from self, with DynamicStorageOpenError::InternalError, "{} since the underlying file could not be opened.", msg);
                }
            };

            elapsed_time = fail!(from self, when wait_for_read_write_access.wait(),
                                    with DynamicStorageOpenError::InternalError,
                                    "{} since the adaptive wait call failed.", msg);
        };

        let raw_fd = unsafe { file.file_descriptor().native_handle() };
        let fd = unsafe { FileDescriptor::non_owning_new_unchecked(raw_fd) };

        let file_size = match file.metadata() {
            Ok(m) => m.size(),
            Err(e) => {
                fail!(from self, with DynamicStorageOpenError::InternalError,
                    "{msg} since the file size could not be acquired ({e:?}).");
            }
        };
        if file_size == 0 || file_size as usize % hugepage_size != 0 {
            fail!(from self, with DynamicStorageOpenError::InternalError,
                "{msg} since the underlying file size {} is not aligned to the hugepage size {}.",
                file_size, hugepage_size);
        }

        let memory_mapping = match MemoryMappingBuilder::from_file_descriptor(fd)
            .mapping_behavior(MappingBehavior::Shared)
            .initial_mapping_permission(MappingPermission::ReadWrite)
            .size(file_size as usize)
            .create()
        {
            Ok(v) => v,
            Err(e) => {
                fail!(from self, with DynamicStorageOpenError::InternalError,
                        "{msg} since the memory could not be mapped into the process ({e:?}).");
            }
        };
        pre_touch_mapping(&memory_mapping, hugepage_size);

        let init_state = memory_mapping.base_address() as *const Data<T>;

        loop {
            // The mem-sync is actually not required since an uninitialized dynamic storage has
            // only write permissions and can be therefore not consumed.
            // This is only for the case that this strategy fails on an obscure POSIX platform.
            //
            //////////////////////////////////////////
            // SYNC POINT: read Data<T>::data
            //////////////////////////////////////////
            let package_version = unsafe { &(*init_state) }
                .version
                .load(core::sync::atomic::Ordering::SeqCst);

            let package_version = PackageVersion::from_u64(package_version);
            if package_version.to_u64() == 0 {
                if elapsed_time >= self.timeout {
                    fail!(from self, with DynamicStorageOpenError::InitializationNotYetFinalized,
                        "{} since the version number was not set - (it is not initialized after {:?}).",
                        msg, self.timeout);
                }
            } else if package_version != PackageVersion::get() {
                fail!(from self, with DynamicStorageOpenError::VersionMismatch,
                       "{} since the dynamic storage was created with version {} but this process requires version {}.",
                        msg, package_version, PackageVersion::get());
            } else {
                break;
            }

            elapsed_time = fail!(from self, when wait_for_read_write_access.wait(),
                                    with DynamicStorageOpenError::InternalError,
                                    "{} since the adaptive wait call failed.", msg);
        }

        Ok(Storage {
            file,
            memory_mapping,
            name: self.storage_name,
            _data: PhantomData,
        })
    }

    fn create_impl(&mut self) -> Result<Storage<T>, DynamicStorageCreateError> {
        let msg = "Failed to create dynamic_storage::hugetlbfs::DynamicStorage";
        let hugepage_size = fail!(from self,
            when resolve_hugepage_size(&self.config.path, self.config.hugepage_size_bytes),
            with DynamicStorageCreateError::InternalError,
            "{msg} since path \"{}\" is not on a hugetlbfs mount or the hugepage size could not be resolved.",
            self.config.path);

        let full_name = self.config.path_for(&self.storage_name);
        let mut file = match FileBuilder::new(&full_name)
            .has_ownership(self.has_ownership)
            .creation_mode(CreationMode::CreateExclusive)
            .permission(INIT_PERMISSIONS)
            .create()
        {
            Ok(v) => v,
            Err(FileCreationError::FileAlreadyExists) => {
                fail!(from self, with DynamicStorageCreateError::AlreadyExists,
                    "{} since a file with the name already exists.", msg);
            }
            Err(FileCreationError::InsufficientPermissions) => {
                fail!(from self, with DynamicStorageCreateError::InsufficientPermissions,
                    "{} due to insufficient permissions.", msg);
            }
            Err(_) => {
                fail!(from self, with DynamicStorageCreateError::InternalError,
                    "{} since the underlying file could not be created.", msg);
            }
        };

        let file_size_unaligned = core::mem::size_of::<Data<T>>() + self.supplementary_size;
        let file_size = fail!(from self,
            when round_up_to_multiple(file_size_unaligned, hugepage_size).ok_or(()),
            with DynamicStorageCreateError::InternalError,
            "{msg} since size {} could not be aligned to hugepage size {}.",
            file_size_unaligned, hugepage_size);

        if let Err(e) = file.truncate(file_size) {
            fail!(from self, with DynamicStorageCreateError::InternalError,
                "{msg} since the file could not be resized to {file_size} ({e:?}).");
        }

        let raw_fd = unsafe { file.file_descriptor().native_handle() };
        let fd = unsafe { FileDescriptor::non_owning_new_unchecked(raw_fd) };

        let memory_mapping = match MemoryMappingBuilder::from_file_descriptor(fd)
            .mapping_behavior(MappingBehavior::Shared)
            .initial_mapping_permission(MappingPermission::ReadWrite)
            .size(file_size)
            .create()
        {
            Ok(m) => m,
            Err(e) => {
                fail!(from self, with DynamicStorageCreateError::InternalError,
                        "{msg} since the file could not be mapped into the process space ({e:?}).");
            }
        };
        pre_touch_mapping(&memory_mapping, hugepage_size);

        Ok(Storage {
            file,
            memory_mapping,
            name: self.storage_name,
            _data: PhantomData,
        })
    }

    fn init_impl(
        &mut self,
        mut storage: Storage<T>,
        initial_value: T,
    ) -> Result<Storage<T>, DynamicStorageCreateError> {
        let msg = "Failed to init dynamic_storage::hugetlbfs::DynamicStorage";
        let value = storage.memory_mapping.base_address_mut() as *mut Data<T>;
        let version_ptr = unsafe { core::ptr::addr_of_mut!((*value).version) };
        unsafe { version_ptr.write(AtomicU64::new(0)) };

        unsafe { core::ptr::addr_of_mut!((*value).data).write(initial_value) };
        unsafe {
            core::ptr::addr_of_mut!((*value).call_drop_on_destruction)
                .write(self.call_drop_on_destruction)
        };

        let supplementary_start = (storage.memory_mapping.base_address() as usize
            + core::mem::size_of::<Data<T>>()) as *mut u8;
        let supplementary_len = storage.memory_mapping.size() - core::mem::size_of::<Data<T>>();

        let mut allocator = BumpAllocator::new(
            unsafe { NonNull::new_unchecked(supplementary_start) },
            supplementary_len,
        );

        let origin = format!("{self:?}");
        if !self
            .initializer
            .call(unsafe { &mut (*value).data }, &mut allocator)
        {
            storage.file.acquire_ownership();
            fail!(from origin, with DynamicStorageCreateError::InitializationFailed,
                "{} since the initialization of the underlying construct failed.", msg);
        }

        // The mem-sync is actually not required since an uninitialized dynamic storage has
        // only write permissions and can be therefore not consumed.
        // This is only for the case that this strategy fails on an obscure POSIX platform.
        //
        //////////////////////////////////////////
        // SYNC POINT: write Data<T>::data
        //////////////////////////////////////////
        unsafe { (*version_ptr).store(PackageVersion::get().to_u64(), Ordering::SeqCst) };

        if let Err(e) = storage.file.set_permission(FINAL_PERMISSIONS) {
            storage.file.acquire_ownership();
            fail!(from origin, with DynamicStorageCreateError::InternalError,
                "{} since the final permissions could not be applied to the underlying file ({:?}).",
                msg, e);
        }

        Ok(storage)
    }
}

impl<'builder, T: Send + Sync + Debug> DynamicStorageBuilder<'builder, T, Storage<T>>
    for Builder<'builder, T>
{
    fn call_drop_on_destruction(mut self, value: bool) -> Self {
        self.call_drop_on_destruction = value;
        self
    }

    fn has_ownership(mut self, value: bool) -> Self {
        self.has_ownership = value;
        self
    }

    fn initializer<F: FnMut(&mut T, &mut BumpAllocator) -> bool + 'builder>(
        mut self,
        value: F,
    ) -> Self {
        self.initializer = Initializer::new(value);
        self
    }

    fn timeout(mut self, value: Duration) -> Self {
        self.timeout = value;
        self
    }

    fn supplementary_size(mut self, value: usize) -> Self {
        self.supplementary_size = value;
        self
    }

    fn create(mut self, initial_value: T) -> Result<Storage<T>, DynamicStorageCreateError> {
        let shm = self.create_impl()?;
        self.init_impl(shm, initial_value)
    }

    fn open(self) -> Result<Storage<T>, DynamicStorageOpenError> {
        self.open_impl()
    }

    fn open_or_create(
        mut self,
        initial_value: T,
    ) -> Result<Storage<T>, DynamicStorageOpenOrCreateError> {
        loop {
            match self.open_impl() {
                Ok(storage) => return Ok(storage),
                Err(DynamicStorageOpenError::DoesNotExist) => match self.create_impl() {
                    Ok(shm) => {
                        return Ok(self.init_impl(shm, initial_value)?);
                    }
                    Err(DynamicStorageCreateError::AlreadyExists) => continue,
                    Err(e) => return Err(e.into()),
                },
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// Implements [`DynamicStorage`] based on a file on hugetlbfs. It is built by
/// [`Builder`].
#[derive(Debug)]
pub struct Storage<T: Debug + Send + Sync> {
    file: File,
    memory_mapping: MemoryMapping,
    name: FileName,
    _data: PhantomData<T>,
}

unsafe impl<T: Debug + Send + Sync> Send for Storage<T> {}
unsafe impl<T: Debug + Send + Sync> Sync for Storage<T> {}

impl<T: Debug + Send + Sync> Drop for Storage<T> {
    fn drop(&mut self) {
        if self.file.has_ownership() {
            let data = unsafe { &mut (*(self.memory_mapping.base_address_mut() as *mut Data<T>)) };
            if data.call_drop_on_destruction {
                let user_type = &mut data.data;
                unsafe { core::ptr::drop_in_place(user_type) };
            }
        }
    }
}

impl<T: Send + Sync + Debug> NamedConcept for Storage<T> {
    fn name(&self) -> &FileName {
        &self.name
    }
}

impl<T: Send + Sync + Debug> NamedConceptMgmt for Storage<T> {
    type Configuration = Configuration<T>;

    fn does_exist_cfg(
        name: &FileName,
        cfg: &Self::Configuration,
    ) -> Result<bool, NamedConceptDoesExistError> {
        let origin = "dynamic_storage::hugetlbfs::Storage::does_exist_cfg()";
        let msg = "Unable to determine if a dynamic storage exists";
        let full_name = cfg.path_for(name);
        match File::does_exist(&full_name) {
            Ok(v) => Ok(v),
            Err(FileAccessError::InsufficientPermissions) => {
                fail!(from origin, with NamedConceptDoesExistError::InsufficientPermissions,
                    "{msg} with the name {name} due to insufficient permissions.");
            }
            Err(e) => {
                fail!(from origin, with NamedConceptDoesExistError::InternalError,
                    "{msg} with the name {name} due to an internal error ({e:?}).");
            }
        }
    }

    fn list_cfg(cfg: &Self::Configuration) -> Result<Vec<FileName>, NamedConceptListError> {
        let origin = "dynamic_storage::hugetlbfs::Storage::list_cfg()";
        let msg = "Unable to list all dynamic storages";
        let directory = match Directory::new(&cfg.path) {
            Ok(d) => d,
            Err(DirectoryOpenError::InsufficientPermissions) => {
                fail!(from origin, with NamedConceptListError::InsufficientPermissions,
                    "{msg} due to insufficient permissions.");
            }
            Err(e) => {
                fail!(from origin, with NamedConceptListError::InternalError,
                    "{msg} due to an internal error ({e:?}).");
            }
        };

        let mut result = vec![];
        let contents = match directory.contents() {
            Ok(c) => c,
            Err(DirectoryReadError::InsufficientPermissions) => {
                fail!(from origin, with NamedConceptListError::InsufficientPermissions,
                    "{msg} since the directory content of {} could not be listed due to insufficient permissions.", cfg.path);
            }
            Err(e) => {
                fail!(from origin, with NamedConceptListError::InternalError,
                    "{msg} since the directory content of {} could not be listed due to an internal error ({e:?}).", cfg.path);
            }
        };

        for entry in contents {
            if let Some(entry_name) = cfg.extract_name_from_file(entry.name()) {
                result.push(entry_name);
            }
        }

        Ok(result)
    }

    unsafe fn remove_cfg(
        name: &FileName,
        cfg: &Self::Configuration,
    ) -> Result<bool, crate::static_storage::file::NamedConceptRemoveError> {
        let full_path = cfg.path_for(name);
        let msg = "Unable to remove dynamic_storage::hugetlbfs::Storage";
        let origin = "dynamic_storage::hugetlbfs::Storage::remove_cfg()";

        match Builder::<T>::new(name).config(cfg).open() {
            Ok(s) => {
                s.acquire_ownership();
                Ok(true)
            }
            Err(DynamicStorageOpenError::DoesNotExist) => Ok(false),
            Err(e) => {
                warn!(from origin,
                    "Removing DynamicStorage in broken state ({:?}) will not call drop of the underlying data type {:?}.",
                    e, core::any::type_name::<T>());

                match File::remove(&full_path) {
                    Ok(v) => Ok(v),
                    Err(FileRemoveError::InsufficientPermissions) => {
                        fail!(from origin, with NamedConceptRemoveError::InsufficientPermissions,
                                     "{} \"{}\" due to insufficient permissions.", msg, name);
                    }
                    Err(v) => {
                        fail!(from origin, with NamedConceptRemoveError::InternalError,
                                    "{} \"{}\" due to an internal failure ({:?}).", msg, name, v);
                    }
                }
            }
        }
    }

    fn remove_path_hint(
        value: &Path,
    ) -> Result<(), crate::named_concept::NamedConceptPathHintRemoveError> {
        crate::named_concept::remove_path_hint(value)
    }
}

impl<T: Send + Sync + Debug> DynamicStorage<T> for Storage<T> {
    type Builder<'builder> = Builder<'builder, T>;

    fn does_support_persistency() -> bool {
        SharedMemory::does_support_persistency()
    }

    fn acquire_ownership(&self) {
        self.file.acquire_ownership()
    }

    fn get(&self) -> &T {
        unsafe { &(*(self.memory_mapping.base_address() as *const Data<T>)).data }
    }

    fn has_ownership(&self) -> bool {
        self.file.has_ownership()
    }

    fn release_ownership(&self) {
        self.file.release_ownership()
    }

    unsafe fn __internal_set_type_name_in_config(
        config: &mut Self::Configuration,
        type_name: &str,
    ) {
        config.type_name = type_name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceoryx2_bb_testing::assert_that;

    #[test]
    fn parse_hugepage_size_to_bytes_works() {
        assert_that!(parse_hugepage_size_to_bytes("2048 kB"), eq Some(2 * 1024 * 1024));
        assert_that!(parse_hugepage_size_to_bytes("2M"), eq Some(2 * 1024 * 1024));
        assert_that!(
            parse_hugepage_size_to_bytes("1 GB"),
            eq Some(1024 * 1024 * 1024)
        );
        assert_that!(parse_hugepage_size_to_bytes(""), eq None);
        assert_that!(parse_hugepage_size_to_bytes("foo"), eq None);
    }

    #[test]
    fn round_up_to_multiple_works() {
        assert_that!(round_up_to_multiple(0, 2 * 1024 * 1024), eq Some(0));
        assert_that!(
            round_up_to_multiple(2 * 1024 * 1024, 2 * 1024 * 1024),
            eq Some(2 * 1024 * 1024)
        );
        assert_that!(
            round_up_to_multiple(2 * 1024 * 1024 + 1, 2 * 1024 * 1024),
            eq Some(4 * 1024 * 1024)
        );
    }

    #[test]
    fn find_hugetlbfs_mount_selects_longest_match() {
        let mounts = "\
none /dev/hugepages hugetlbfs rw,pagesize=2M 0 0\n\
none /dev/hugepages/app hugetlbfs rw,pagesize=1G 0 0\n\
tmpfs /tmp tmpfs rw 0 0\n";

        let result = find_hugetlbfs_mount_in_mounts("/dev/hugepages/app/data", mounts);
        assert_that!(result, is_ok);
        let (mount_path, hugepage_size) = result.unwrap();
        assert_that!(mount_path, eq "/dev/hugepages/app".to_string());
        assert_that!(hugepage_size, eq Some(1024 * 1024 * 1024));
    }

    #[test]
    fn find_hugetlbfs_mount_fails_for_non_hugetlbfs_path() {
        let mounts = "tmpfs /tmp tmpfs rw 0 0\n";
        let result = find_hugetlbfs_mount_in_mounts("/tmp/some/path", mounts);
        assert_that!(result, is_err);
    }
}
