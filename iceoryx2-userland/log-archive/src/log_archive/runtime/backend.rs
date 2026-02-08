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

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use super::common::{ArchiveRecorderError, AsyncIoBackend, EffectiveAsyncIoBackend};

#[derive(Debug)]
pub(super) struct BlockingIoBackend;

#[cfg(target_os = "linux")]
pub(super) struct IoUringBackend {
    ring: io_uring::IoUring,
}

#[cfg(target_os = "linux")]
impl core::fmt::Debug for IoUringBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IoUringBackend").finish_non_exhaustive()
    }
}

/// Unified recorder I/O backend abstraction.
#[derive(Debug)]
pub(super) enum RecorderIoBackend {
    Blocking(BlockingIoBackend),
    #[cfg(target_os = "linux")]
    IoUring(IoUringBackend),
}

impl RecorderIoBackend {
    pub(super) fn create(
        requested: AsyncIoBackend,
        io_uring_queue_depth: u32,
    ) -> (Self, EffectiveAsyncIoBackend) {
        match requested {
            AsyncIoBackend::Blocking => (
                Self::Blocking(BlockingIoBackend),
                EffectiveAsyncIoBackend::Blocking,
            ),
            AsyncIoBackend::IoUringPreferred => {
                #[cfg(target_os = "linux")]
                {
                    if let Ok(backend) = IoUringBackend::new(io_uring_queue_depth) {
                        return (Self::IoUring(backend), EffectiveAsyncIoBackend::IoUring);
                    }
                }

                (
                    Self::Blocking(BlockingIoBackend),
                    EffectiveAsyncIoBackend::Blocking,
                )
            }
        }
    }

    pub(super) fn write_all_at(
        &mut self,
        file: &mut File,
        path: &Path,
        offset: u64,
        bytes: &[u8],
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => {
                file.seek(SeekFrom::Start(offset))
                    .and_then(|_| file.write_all(bytes))
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation,
                        path: path.to_path_buf(),
                        source,
                    })
            }
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => {
                backend
                    .write_all_at(file, offset, bytes)
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation,
                        path: path.to_path_buf(),
                        source,
                    })
            }
        }
    }

    pub(super) fn flush(
        &mut self,
        file: &mut File,
        path: &Path,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        file.flush().map_err(|source| ArchiveRecorderError::Io {
            operation,
            path: path.to_path_buf(),
            source,
        })
    }

    pub(super) fn sync_data(
        &mut self,
        file: &mut File,
        path: &Path,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => file.sync_data().map_err(|source| ArchiveRecorderError::Io {
                operation,
                path: path.to_path_buf(),
                source,
            }),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => {
                backend
                    .sync_data(file)
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation,
                        path: path.to_path_buf(),
                        source,
                    })
            }
        }
    }

    pub(super) fn set_len(
        &mut self,
        file: &mut File,
        path: &Path,
        len: u64,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        file.set_len(len).map_err(|source| ArchiveRecorderError::Io {
            operation,
            path: path.to_path_buf(),
            source,
        })
    }
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    fn new(queue_depth: u32) -> std::io::Result<Self> {
        let depth = queue_depth.max(1);
        Ok(Self {
            ring: io_uring::IoUring::new(depth)?,
        })
    }

    fn write_all_at(
        &mut self,
        file: &File,
        offset: u64,
        bytes: &[u8],
    ) -> std::io::Result<()> {
        use std::io::{Error, ErrorKind};
        use std::os::fd::AsRawFd;

        if bytes.is_empty() {
            return Ok(());
        }

        let fd = io_uring::types::Fd(file.as_raw_fd());
        let mut written = 0usize;
        while written < bytes.len() {
            let chunk = &bytes[written..];
            let entry = io_uring::opcode::Write::new(fd, chunk.as_ptr(), chunk.len() as _)
                .offset((offset + written as u64) as _)
                .build()
                .user_data(0xA11CE);
            let result = unsafe { self.submit_and_wait_one(entry)? };
            if result <= 0 {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "io_uring write returned zero/negative bytes",
                ));
            }
            written += result as usize;
        }
        Ok(())
    }

    fn sync_data(&mut self, file: &File) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;

        let fd = io_uring::types::Fd(file.as_raw_fd());
        let entry = io_uring::opcode::Fsync::new(fd).build().user_data(0xF5);
        let _ = unsafe { self.submit_and_wait_one(entry)? };
        Ok(())
    }

    unsafe fn submit_and_wait_one(
        &mut self,
        entry: io_uring::squeue::Entry,
    ) -> std::io::Result<i32> {
        use std::io::{Error, ErrorKind};

        loop {
            let mut sq = self.ring.submission();
            match sq.push(&entry) {
                Ok(()) => break,
                Err(_) => {
                    drop(sq);
                    self.ring.submit()?;
                }
            }
        }

        self.ring.submit_and_wait(1)?;
        let mut cq = self.ring.completion();
        let Some(cqe) = cq.next() else {
            return Err(Error::new(
                ErrorKind::Other,
                "io_uring returned without completion",
            ));
        };
        let result = cqe.result();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(-result));
        }
        Ok(result)
    }
}
