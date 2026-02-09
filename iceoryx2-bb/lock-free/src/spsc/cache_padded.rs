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

use core::ops::{Deref, DerefMut};

/// Align to a full cache line to reduce producer/consumer false sharing on hot fields.
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: Default> Default for CachePadded<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
