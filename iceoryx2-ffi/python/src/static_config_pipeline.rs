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

use pyo3::prelude::*;

use crate::type_detail::TypeDetail;

#[pyclass]
/// The static configuration of a `MessagingPattern::Pipeline` based `Service`.
pub struct StaticConfigPipeline(pub(crate) iceoryx2::service::static_config::pipeline::StaticConfig);

#[pymethods]
impl StaticConfigPipeline {
    #[getter]
    /// Returns the amount of worker stages.
    pub fn number_of_stages(&self) -> usize {
        self.0.number_of_stages()
    }

    #[getter]
    /// Returns the bounded amount of in-flight samples per stage boundary.
    pub fn max_in_flight_samples(&self) -> usize {
        self.0.max_in_flight_samples()
    }

    #[getter]
    /// Returns the maximum amount of nodes that can open the service.
    pub fn max_nodes(&self) -> usize {
        self.0.max_nodes()
    }

    #[getter]
    /// Returns the default initial max slice length used by dynamic payload publishers.
    pub fn initial_max_slice_len(&self) -> usize {
        self.0.initial_max_slice_len()
    }

    #[getter]
    /// Returns payload type details of the service.
    pub fn payload_type_details(&self) -> TypeDetail {
        TypeDetail(*self.0.payload_type_details())
    }
}
