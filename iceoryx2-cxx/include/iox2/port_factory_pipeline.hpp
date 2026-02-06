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

#ifndef IOX2_PORTFACTORY_PIPELINE_HPP
#define IOX2_PORTFACTORY_PIPELINE_HPP

#include "iox2/attribute_set.hpp"
#include "iox2/bb/expected.hpp"
#include "iox2/bb/optional.hpp"
#include "iox2/bb/static_function.hpp"
#include "iox2/callback_progression.hpp"
#include "iox2/internal/callback_context.hpp"
#include "iox2/internal/iceoryx2.hpp"
#include "iox2/legacy/uninitialized_array.hpp"
#include "iox2/node_failure_enums.hpp"
#include "iox2/node_state.hpp"
#include "iox2/port_factory_publisher.hpp"
#include "iox2/port_factory_subscriber.hpp"
#include "iox2/publisher_details.hpp"
#include "iox2/service_id.hpp"
#include "iox2/service_name.hpp"
#include "iox2/service_type.hpp"
#include "iox2/static_config_pipeline.hpp"
#include "iox2/subscriber_details.hpp"

namespace iox2 {
/// The factory for [`MessagingPattern::Pipeline`].
template <ServiceType S, typename Payload, typename UserHeader>
class PortFactoryPipeline {
  public:
    PortFactoryPipeline(PortFactoryPipeline&& rhs) noexcept;
    auto operator=(PortFactoryPipeline&& rhs) noexcept -> PortFactoryPipeline&;
    ~PortFactoryPipeline();

    PortFactoryPipeline(const PortFactoryPipeline&) = delete;
    auto operator=(const PortFactoryPipeline&) -> PortFactoryPipeline& = delete;

    /// Returns the [`ServiceName`] of the service.
    auto name() const -> ServiceNameView;

    /// Returns the [`ServiceId`] of the [`Service`].
    auto service_id() const -> ServiceId;

    /// Returns the attributes defined in the [`Service`].
    auto attributes() const -> AttributeSetView;

    /// Returns the static configuration of the [`Service`].
    auto static_config() const -> StaticConfigPipeline;

    /// Iterates over all [`Node`]s of the [`Service`].
    auto nodes(const iox2::bb::StaticFunction<CallbackProgression(NodeState<S>)>& callback) const
        -> bb::Expected<void, NodeListFailure>;

    /// Returns the configured amount of worker stages.
    auto number_of_stages() const -> uint64_t;

    /// Returns the current amount of ingress ports.
    auto number_of_ingress_ports() const -> uint64_t;

    /// Returns the current amount of workers at a stage.
    auto number_of_workers(uint64_t stage_id) const -> bb::Optional<uint64_t>;

    /// Returns the current amount of egress ports.
    auto number_of_egress_ports() const -> uint64_t;

    /// Iterates over all ingress ports.
    void list_ingresses(const iox2::bb::StaticFunction<CallbackProgression(PublisherDetailsView)>& callback) const;

    /// Iterates over all worker ports at a stage.
    void list_workers(uint64_t stage_id,
                      const iox2::bb::StaticFunction<CallbackProgression(SubscriberDetailsView)>& callback) const;

    /// Iterates over all egress ports.
    void list_egresses(const iox2::bb::StaticFunction<CallbackProgression(SubscriberDetailsView)>& callback) const;

    /// Returns a builder for ingress endpoints.
    auto ingress_builder() const -> PortFactoryPublisher<S, Payload, UserHeader>;

    /// Returns a builder for worker input endpoints.
    auto worker_subscriber_builder(uint64_t stage_id) const -> bb::Optional<PortFactorySubscriber<S, Payload, UserHeader>>;

    /// Returns a builder for worker output endpoints.
    auto worker_publisher_builder(uint64_t stage_id) const -> bb::Optional<PortFactoryPublisher<S, Payload, UserHeader>>;

    /// Returns a builder for egress endpoints.
    auto egress_builder() const -> PortFactorySubscriber<S, Payload, UserHeader>;

  private:
    template <typename, typename, ServiceType>
    friend class ServiceBuilderPipeline;

    explicit PortFactoryPipeline(iox2_port_factory_pipeline_h handle);
    void drop();

    iox2_port_factory_pipeline_h m_handle = nullptr;
};

template <ServiceType S, typename Payload, typename UserHeader>
inline PortFactoryPipeline<S, Payload, UserHeader>::PortFactoryPipeline(iox2_port_factory_pipeline_h handle)
    : m_handle { handle } {
}

template <ServiceType S, typename Payload, typename UserHeader>
inline void PortFactoryPipeline<S, Payload, UserHeader>::drop() {
    if (m_handle != nullptr) {
        iox2_port_factory_pipeline_drop(m_handle);
        m_handle = nullptr;
    }
}

template <ServiceType S, typename Payload, typename UserHeader>
inline PortFactoryPipeline<S, Payload, UserHeader>::PortFactoryPipeline(PortFactoryPipeline&& rhs) noexcept {
    *this = std::move(rhs);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::operator=(PortFactoryPipeline&& rhs) noexcept
    -> PortFactoryPipeline& {
    if (this != &rhs) {
        drop();
        m_handle = std::move(rhs.m_handle);
        rhs.m_handle = nullptr;
    }

    return *this;
}

template <ServiceType S, typename Payload, typename UserHeader>
inline PortFactoryPipeline<S, Payload, UserHeader>::~PortFactoryPipeline() {
    drop();
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::name() const -> ServiceNameView {
    const auto* service_name_ptr = iox2_port_factory_pipeline_service_name(&m_handle);
    return ServiceNameView(service_name_ptr);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::service_id() const -> ServiceId {
    iox2::legacy::UninitializedArray<char, IOX2_SERVICE_ID_LENGTH> buffer;
    iox2_port_factory_pipeline_service_id(&m_handle, &buffer[0], IOX2_SERVICE_ID_LENGTH);

    return ServiceId(iox2::bb::StaticString<IOX2_SERVICE_ID_LENGTH>::from_utf8_null_terminated_unchecked_truncated(
        &buffer[0], IOX2_SERVICE_ID_LENGTH));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::attributes() const -> AttributeSetView {
    return AttributeSetView(iox2_port_factory_pipeline_attributes(&m_handle));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::static_config() const -> StaticConfigPipeline {
    iox2_static_config_pipeline_t static_config {};
    iox2_port_factory_pipeline_static_config(&m_handle, &static_config);

    return StaticConfigPipeline(static_config);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::nodes(
    const iox2::bb::StaticFunction<CallbackProgression(NodeState<S>)>& callback) const
    -> bb::Expected<void, NodeListFailure> {
    auto ctx = internal::ctx(callback);
    const auto ret_val =
        iox2_port_factory_pipeline_nodes(&m_handle, internal::list_callback<S>, static_cast<void*>(&ctx));

    if (ret_val == IOX2_OK) {
        return {};
    }

    return bb::err(bb::into<NodeListFailure>(ret_val));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::number_of_stages() const -> uint64_t {
    return iox2_port_factory_pipeline_number_of_stages(&m_handle);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::number_of_ingress_ports() const -> uint64_t {
    return iox2_port_factory_pipeline_dynamic_config_number_of_ingress_ports(&m_handle);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::number_of_workers(uint64_t stage_id) const
    -> bb::Optional<uint64_t> {
    bool has_value = false;
    const auto value = iox2_port_factory_pipeline_dynamic_config_number_of_workers(&m_handle, stage_id, &has_value);

    if (!has_value) {
        return bb::NULLOPT;
    }

    return value;
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::number_of_egress_ports() const -> uint64_t {
    return iox2_port_factory_pipeline_dynamic_config_number_of_egress_ports(&m_handle);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline void PortFactoryPipeline<S, Payload, UserHeader>::list_ingresses(
    const iox2::bb::StaticFunction<CallbackProgression(PublisherDetailsView)>& callback) const {
    auto ctx = internal::ctx(callback);
    iox2_port_factory_pipeline_dynamic_config_list_ingresses(
        &m_handle,
        internal::list_ports_callback<iox2_publisher_details_ptr, PublisherDetailsView>,
        static_cast<void*>(&ctx));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline void PortFactoryPipeline<S, Payload, UserHeader>::list_workers(
    uint64_t stage_id,
    const iox2::bb::StaticFunction<CallbackProgression(SubscriberDetailsView)>& callback) const {
    auto ctx = internal::ctx(callback);
    iox2_port_factory_pipeline_dynamic_config_list_workers(
        &m_handle,
        stage_id,
        internal::list_ports_callback<iox2_subscriber_details_ptr, SubscriberDetailsView>,
        static_cast<void*>(&ctx));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline void PortFactoryPipeline<S, Payload, UserHeader>::list_egresses(
    const iox2::bb::StaticFunction<CallbackProgression(SubscriberDetailsView)>& callback) const {
    auto ctx = internal::ctx(callback);
    iox2_port_factory_pipeline_dynamic_config_list_egresses(
        &m_handle,
        internal::list_ports_callback<iox2_subscriber_details_ptr, SubscriberDetailsView>,
        static_cast<void*>(&ctx));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::ingress_builder() const
    -> PortFactoryPublisher<S, Payload, UserHeader> {
    return PortFactoryPublisher<S, Payload, UserHeader>(iox2_port_factory_pipeline_ingress_builder(&m_handle, nullptr));
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::worker_subscriber_builder(uint64_t stage_id) const
    -> bb::Optional<PortFactorySubscriber<S, Payload, UserHeader>> {
    bool has_value = false;
    auto handle =
        iox2_port_factory_pipeline_worker_subscriber_builder(&m_handle, stage_id, nullptr, &has_value);
    if (!has_value) {
        return bb::NULLOPT;
    }

    return PortFactorySubscriber<S, Payload, UserHeader>(handle);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::worker_publisher_builder(uint64_t stage_id) const
    -> bb::Optional<PortFactoryPublisher<S, Payload, UserHeader>> {
    bool has_value = false;
    auto handle =
        iox2_port_factory_pipeline_worker_publisher_builder(&m_handle, stage_id, nullptr, &has_value);
    if (!has_value) {
        return bb::NULLOPT;
    }

    return PortFactoryPublisher<S, Payload, UserHeader>(handle);
}

template <ServiceType S, typename Payload, typename UserHeader>
inline auto PortFactoryPipeline<S, Payload, UserHeader>::egress_builder() const
    -> PortFactorySubscriber<S, Payload, UserHeader> {
    return PortFactorySubscriber<S, Payload, UserHeader>(iox2_port_factory_pipeline_egress_builder(&m_handle, nullptr));
}
} // namespace iox2

#endif
