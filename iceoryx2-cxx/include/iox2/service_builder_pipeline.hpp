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

#ifndef IOX2_SERVICE_BUILDER_PIPELINE_HPP
#define IOX2_SERVICE_BUILDER_PIPELINE_HPP

#include "iox2/attribute_specifier.hpp"
#include "iox2/attribute_verifier.hpp"
#include "iox2/bb/detail/builder.hpp"
#include "iox2/bb/expected.hpp"
#include "iox2/internal/iceoryx2.hpp"
#include "iox2/internal/service_builder_internal.hpp"
#include "iox2/payload_info.hpp"
#include "iox2/port_factory_pipeline.hpp"
#include "iox2/service_builder_pipeline_error.hpp"
#include "iox2/service_type.hpp"

namespace iox2 {
/// Builder to create new [`MessagingPattern::Pipeline`] based [`Service`]s.
template <typename Payload, ServiceType S>
class ServiceBuilderPipeline {
  public:
    /// If the [`Service`] is created, defines how many worker stages are created.
    /// If an existing [`Service`] is opened it defines how many stages must be at least supported.
#ifdef DOXYGEN_MACRO_FIX
    auto number_of_stages(const uint64_t value) -> decltype(auto);
#else
    IOX2_BUILDER_OPTIONAL(uint64_t, number_of_stages);
#endif

    /// If the [`Service`] is created, defines how many in-flight samples are supported per stage
    /// boundary. If an existing [`Service`] is opened it defines the minimum required.
#ifdef DOXYGEN_MACRO_FIX
    auto max_in_flight_samples(const uint64_t value) -> decltype(auto);
#else
    IOX2_BUILDER_OPTIONAL(uint64_t, max_in_flight_samples);
#endif

    /// If the [`Service`] is created it defines how many [`Node`]s shall be able to open it in
    /// parallel. If an existing [`Service`] is opened it defines how many [`Node`]s must be at
    /// least supported.
#ifdef DOXYGEN_MACRO_FIX
    auto max_nodes(const uint64_t value) -> decltype(auto);
#else
    IOX2_BUILDER_OPTIONAL(uint64_t, max_nodes);
#endif

    /// If the [`Service`] is created it defines the default initial max slice length used by
    /// dynamic payload publishers.
#ifdef DOXYGEN_MACRO_FIX
    auto initial_max_slice_len(const uint64_t value) -> decltype(auto);
#else
    IOX2_BUILDER_OPTIONAL(uint64_t, initial_max_slice_len);
#endif

  public:
    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be created.
    auto open_or_create() && -> bb::Expected<PortFactoryPipeline<S, Payload>, PipelineOpenOrCreateError>;

    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be created
    /// with a set of required attributes.
    auto open_or_create_with_attributes(
        const AttributeVerifier& required_attributes) && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                         PipelineOpenOrCreateError>;

    /// Opens an existing [`Service`].
    auto open() && -> bb::Expected<PortFactoryPipeline<S, Payload>, PipelineOpenError>;

    /// Opens an existing [`Service`] with attribute requirements.
    auto open_with_attributes(
        const AttributeVerifier& required_attributes) && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                         PipelineOpenError>;

    /// Creates a new [`Service`].
    auto create() && -> bb::Expected<PortFactoryPipeline<S, Payload>, PipelineCreateError>;

    /// Creates a new [`Service`] with a set of attributes.
    auto create_with_attributes(const AttributeSpecifier& attributes) && -> bb::
        Expected<PortFactoryPipeline<S, Payload>, PipelineCreateError>;

  private:
    template <ServiceType>
    friend class ServiceBuilder;

    explicit ServiceBuilderPipeline(iox2_service_builder_h handle);
    void set_parameters();

    iox2_service_builder_pipeline_h m_handle = nullptr;
};

template <typename Payload, ServiceType S>
inline ServiceBuilderPipeline<Payload, S>::ServiceBuilderPipeline(iox2_service_builder_h handle)
    : m_handle { iox2_service_builder_pipeline(handle) } {
}

template <typename Payload, ServiceType S>
inline void ServiceBuilderPipeline<Payload, S>::set_parameters() {
    if (m_number_of_stages.has_value()) {
        iox2_service_builder_pipeline_set_number_of_stages(&m_handle, m_number_of_stages.value());
    }
    if (m_max_in_flight_samples.has_value()) {
        iox2_service_builder_pipeline_set_max_in_flight_samples(&m_handle, m_max_in_flight_samples.value());
    }
    if (m_max_nodes.has_value()) {
        iox2_service_builder_pipeline_set_max_nodes(&m_handle, m_max_nodes.value());
    }
    if (m_initial_max_slice_len.has_value()) {
        iox2_service_builder_pipeline_set_initial_max_slice_len(&m_handle, m_initial_max_slice_len.value());
    }

    using ValueType = typename PayloadInfo<Payload>::ValueType;
    auto type_variant = bb::IsSlice<Payload>::VALUE ? iox2_type_variant_e_DYNAMIC : iox2_type_variant_e_FIXED_SIZE;
    const auto payload_type_name = internal::get_type_name<Payload>();

    const auto payload_result =
        iox2_service_builder_pipeline_set_payload_type_details(&m_handle,
                                                               type_variant,
                                                               payload_type_name.unchecked_access().c_str(),
                                                               payload_type_name.size(),
                                                               sizeof(ValueType),
                                                               alignof(ValueType));

    if (payload_result != IOX2_OK) {
        IOX2_PANIC("This should never happen! Implementation failure while setting the Payload-Type.");
    }
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::open_or_create() && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                                     PipelineOpenOrCreateError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result = iox2_service_builder_pipeline_open_or_create(m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineOpenOrCreateError>(result));
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::open_or_create_with_attributes(
    const AttributeVerifier& required_attributes) && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                     PipelineOpenOrCreateError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result = iox2_service_builder_pipeline_open_or_create_with_attributes(
        m_handle, &required_attributes.m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineOpenOrCreateError>(result));
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::open() && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                          PipelineOpenError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result = iox2_service_builder_pipeline_open(m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineOpenError>(result));
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::open_with_attributes(
    const AttributeVerifier& required_attributes) && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                     PipelineOpenError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result = iox2_service_builder_pipeline_open_with_attributes(
        m_handle, &required_attributes.m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineOpenError>(result));
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::create() && -> bb::Expected<PortFactoryPipeline<S, Payload>,
                                                                            PipelineCreateError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result = iox2_service_builder_pipeline_create(m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineCreateError>(result));
}

template <typename Payload, ServiceType S>
inline auto ServiceBuilderPipeline<Payload, S>::create_with_attributes(
    const AttributeSpecifier& attributes) && -> bb::Expected<PortFactoryPipeline<S, Payload>, PipelineCreateError> {
    set_parameters();

    iox2_port_factory_pipeline_h port_factory_handle {};
    auto result =
        iox2_service_builder_pipeline_create_with_attributes(m_handle, &attributes.m_handle, nullptr, &port_factory_handle);

    if (result == IOX2_OK) {
        return PortFactoryPipeline<S, Payload>(port_factory_handle);
    }

    return bb::err(bb::into<PipelineCreateError>(result));
}
} // namespace iox2

#endif
