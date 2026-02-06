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

#![allow(non_camel_case_types)]

use crate::iox2_type_detail_t;
use iceoryx2::service::static_config::pipeline::StaticConfig;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct iox2_static_config_pipeline_t {
    pub number_of_stages: usize,
    pub max_in_flight_samples: usize,
    pub max_nodes: usize,
    pub initial_max_slice_len: usize,
    pub payload_type_details: iox2_type_detail_t,
}

impl From<&StaticConfig> for iox2_static_config_pipeline_t {
    fn from(c: &StaticConfig) -> Self {
        Self {
            number_of_stages: c.number_of_stages(),
            max_in_flight_samples: c.max_in_flight_samples(),
            max_nodes: c.max_nodes(),
            initial_max_slice_len: c.initial_max_slice_len(),
            payload_type_details: c.payload_type_details().into(),
        }
    }
}
