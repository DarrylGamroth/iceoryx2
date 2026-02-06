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

#ifndef IOX2_STATIC_CONFIG_PIPELINE_HPP
#define IOX2_STATIC_CONFIG_PIPELINE_HPP

#include "iox2/iceoryx2.h"
#include "iox2/message_type_details.hpp"

namespace iox2 {
/// The static configuration of a [`MessagingPattern::Pipeline`] based service.
class StaticConfigPipeline {
  public:
    /// Returns the configured amount of worker stages.
    auto number_of_stages() const -> size_t;

    /// Returns the configured maximum amount of in-flight samples per stage boundary.
    auto max_in_flight_samples() const -> size_t;

    /// Returns the maximum supported amount of [`Node`](crate::node::Node)s that can open the
    /// service in parallel.
    auto max_nodes() const -> size_t;

    /// Returns the default initial max slice length used by dynamic payload publishers.
    auto initial_max_slice_len() const -> size_t;

    /// Returns the payload type details of the service.
    auto payload_type_details() const -> TypeDetail;

  private:
    template <ServiceType, typename>
    friend class PortFactoryPipeline;

    explicit StaticConfigPipeline(iox2_static_config_pipeline_t value);

    iox2_static_config_pipeline_t m_value;
};
} // namespace iox2

#endif
