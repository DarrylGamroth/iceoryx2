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

#include "iox2/static_config_pipeline.hpp"

namespace iox2 {
StaticConfigPipeline::StaticConfigPipeline(iox2_static_config_pipeline_t value)
    : m_value { value } {
}

auto StaticConfigPipeline::number_of_stages() const -> size_t {
    return m_value.number_of_stages;
}

auto StaticConfigPipeline::max_in_flight_samples() const -> size_t {
    return m_value.max_in_flight_samples;
}

auto StaticConfigPipeline::max_nodes() const -> size_t {
    return m_value.max_nodes;
}

auto StaticConfigPipeline::initial_max_slice_len() const -> size_t {
    return m_value.initial_max_slice_len;
}

auto StaticConfigPipeline::payload_type_details() const -> TypeDetail {
    return TypeDetail(m_value.payload_type_details);
}
} // namespace iox2
