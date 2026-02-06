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

#include "iox2/attribute_specifier.hpp"
#include "iox2/node.hpp"
#include "iox2/service.hpp"
#include "iox2/service_builder_pipeline_error.hpp"
#include "iox2/type_variant.hpp"

#include "test.hpp"

namespace {
using namespace iox2;

struct HeaderA {
    uint64_t value { 0 };
};

struct HeaderB {
    uint32_t value { 0 };
};

template <typename T>
class ServicePipelineTest : public ::testing::Test {
  public:
    static constexpr ServiceType TYPE = T::TYPE;
};

TYPED_TEST_SUITE(ServicePipelineTest, iox2_testing::ServiceTypes, );

TYPED_TEST(ServicePipelineTest, created_service_does_exist) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    ASSERT_FALSE(Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                     .value());

    auto node = NodeBuilder().create<SERVICE_TYPE>().value();

    {
        auto sut = node.service_builder(service_name).template pipeline<uint64_t>().create().value();
        ASSERT_TRUE(
            Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                .value());
    }

    ASSERT_FALSE(Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                     .value());
}

TYPED_TEST(ServicePipelineTest, service_name_works) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut = node.service_builder(service_name).template pipeline<uint64_t>().create().value();

    ASSERT_THAT(sut.name().to_string().unchecked_access().c_str(),
                StrEq(service_name.to_string().unchecked_access().c_str()));
}

TYPED_TEST(ServicePipelineTest, creating_existing_service_fails) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut = node.service_builder(service_name).template pipeline<uint64_t>().create().value();
    auto sut_2 = node.service_builder(service_name).template pipeline<uint64_t>().create();

    ASSERT_FALSE(sut_2.has_value());
    ASSERT_THAT(sut_2.error(), Eq(PipelineCreateError::AlreadyExists));
}

TYPED_TEST(ServicePipelineTest, open_or_create_service_does_exist) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();

    ASSERT_FALSE(Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                     .value());
    {
        auto sut =
            bb::Optional<PortFactoryPipeline<SERVICE_TYPE, uint64_t, void>>(node.service_builder(service_name)
                                                                          .template pipeline<uint64_t>()
                                                                          .open_or_create()
                                                                          .value());
        ASSERT_TRUE(
            Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                .value());

        auto sut_2 =
            bb::Optional<PortFactoryPipeline<SERVICE_TYPE, uint64_t, void>>(node.service_builder(service_name)
                                                                          .template pipeline<uint64_t>()
                                                                          .open_or_create()
                                                                          .value());
        ASSERT_TRUE(
            Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                .value());

        sut.reset();
        ASSERT_TRUE(
            Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                .value());
        sut_2.reset();
    }

    ASSERT_FALSE(Service<SERVICE_TYPE>::does_exist(service_name, Config::global_config(), MessagingPattern::Pipeline)
                     .value());
}

TYPED_TEST(ServicePipelineTest, opening_non_existing_service_fails) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();

    auto sut = node.service_builder(service_name).template pipeline<uint64_t>().open();
    ASSERT_FALSE(sut.has_value());
    ASSERT_THAT(sut.error(), Eq(PipelineOpenError::DoesNotExist));
}

TYPED_TEST(ServicePipelineTest, opening_existing_service_works) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut_create = node.service_builder(service_name).template pipeline<uint64_t>().create().value();
    auto sut = node.service_builder(service_name).template pipeline<uint64_t>().open();

    ASSERT_TRUE(sut.has_value());
}

TYPED_TEST(ServicePipelineTest, opening_existing_service_with_wrong_payload_type_fails) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut_create = node.service_builder(service_name).template pipeline<uint64_t>().create().value();
    auto sut = node.service_builder(service_name).template pipeline<double>().open();

    ASSERT_FALSE(sut.has_value());
    ASSERT_THAT(sut.error(), Eq(PipelineOpenError::IncompatiblePayloadType));
}

TYPED_TEST(ServicePipelineTest, opening_existing_service_with_wrong_user_header_type_fails) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut_create = node.service_builder(service_name)
                          .template pipeline<uint64_t>()
                          .template user_header<HeaderA>()
                          .create()
                          .value();
    auto sut = node.service_builder(service_name)
                   .template pipeline<uint64_t>()
                   .template user_header<HeaderB>()
                   .open();

    ASSERT_FALSE(sut.has_value());
    ASSERT_THAT(sut.error(), Eq(PipelineOpenError::IncompatibleUserHeaderType));
}

TYPED_TEST(ServicePipelineTest, service_builder_configuration_works) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;
    constexpr uint64_t NUMBER_OF_STAGES = 3U;
    constexpr uint64_t MAX_IN_FLIGHT_SAMPLES = 23U;
    constexpr uint64_t MAX_NODES = 7U;
    constexpr uint64_t INITIAL_MAX_SLICE_LEN = 42U;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut = node.service_builder(service_name)
                   .template pipeline<uint64_t>()
                   .number_of_stages(NUMBER_OF_STAGES)
                   .max_in_flight_samples(MAX_IN_FLIGHT_SAMPLES)
                   .max_nodes(MAX_NODES)
                   .initial_max_slice_len(INITIAL_MAX_SLICE_LEN)
                   .create()
                   .value();

    auto static_config = sut.static_config();
    ASSERT_THAT(static_config.number_of_stages(), Eq(NUMBER_OF_STAGES));
    ASSERT_THAT(static_config.max_in_flight_samples(), Eq(MAX_IN_FLIGHT_SAMPLES));
    ASSERT_THAT(static_config.max_nodes(), Eq(MAX_NODES));
    ASSERT_THAT(static_config.initial_max_slice_len(), Eq(INITIAL_MAX_SLICE_LEN));
    ASSERT_THAT(static_config.payload_type_details().variant(), Eq(TypeVariant::FixedSize));

    ASSERT_THAT(sut.number_of_stages(), Eq(NUMBER_OF_STAGES));
    ASSERT_THAT(sut.number_of_ingress_ports(), Eq(0));
    ASSERT_THAT(sut.number_of_workers(0).has_value(), Eq(true));
    ASSERT_THAT(sut.number_of_workers(0).value(), Eq(0));
    ASSERT_THAT(sut.number_of_workers(NUMBER_OF_STAGES).has_value(), Eq(false));
    ASSERT_THAT(sut.number_of_egress_ports(), Eq(0));
}

TYPED_TEST(ServicePipelineTest, runtime_role_builders_and_dynamic_lists_work) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;

    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();
    auto sut = node.service_builder(service_name)
                   .template pipeline<uint64_t>()
                   .number_of_stages(1)
                   .max_in_flight_samples(8)
                   .create()
                   .value();

    auto ingress = sut.ingress_builder().create().value();
    auto worker_subscriber = sut.worker_subscriber_builder(0).value().create().value();
    auto worker_publisher = sut.worker_publisher_builder(0).value().create().value();
    auto egress = sut.egress_builder().create().value();

    ASSERT_THAT(sut.worker_subscriber_builder(1).has_value(), Eq(false));
    ASSERT_THAT(sut.worker_publisher_builder(1).has_value(), Eq(false));

    ASSERT_THAT(sut.number_of_ingress_ports(), Eq(1));
    ASSERT_THAT(sut.number_of_workers(0).value(), Eq(1));
    ASSERT_THAT(sut.number_of_egress_ports(), Eq(1));

    uint64_t ingress_count = 0;
    sut.list_ingresses([&](auto) {
        ++ingress_count;
        return CallbackProgression::Continue;
    });
    ASSERT_THAT(ingress_count, Eq(1));

    uint64_t worker_count = 0;
    sut.list_workers(0, [&](auto) {
        ++worker_count;
        return CallbackProgression::Continue;
    });
    ASSERT_THAT(worker_count, Eq(1));

    uint64_t egress_count = 0;
    sut.list_egresses([&](auto) {
        ++egress_count;
        return CallbackProgression::Continue;
    });
    ASSERT_THAT(egress_count, Eq(1));

    ingress.update_connections().value();
    worker_publisher.update_connections().value();

    ingress.send_copy(11).value();
    auto work = worker_subscriber.receive().value();
    ASSERT_THAT(work.has_value(), Eq(true));
    ASSERT_THAT(work->payload(), Eq(11));

    worker_publisher.send_copy(22).value();
    auto final_sample = egress.receive().value();
    ASSERT_THAT(final_sample.has_value(), Eq(true));
    ASSERT_THAT(final_sample->payload(), Eq(22));
}

TYPED_TEST(ServicePipelineTest, open_or_create_with_attributes_works) {
    constexpr ServiceType SERVICE_TYPE = TestFixture::TYPE;
    const auto service_name = iox2_testing::generate_service_name();
    auto node = NodeBuilder().create<SERVICE_TYPE>().value();

    auto key = *Attribute::Key::from_utf8("pipeline");
    auto value = *Attribute::Value::from_utf8("enabled");

    auto attributes = AttributeSpecifier();
    attributes.define(key, value).value();

    auto verifier = AttributeVerifier();
    verifier.require(key, value).value();

    auto created =
        node.service_builder(service_name).template pipeline<uint64_t>().open_or_create_with_attributes(verifier);
    ASSERT_TRUE(created.has_value());

    auto opened = node.service_builder(service_name).template pipeline<uint64_t>().open();
    ASSERT_TRUE(opened.has_value());

    auto counter = 0;
    opened.value().attributes().iter_key_values(key, [&](auto& v) {
        EXPECT_THAT(v.unchecked_access().c_str(), StrEq(value.unchecked_access().c_str()));
        ++counter;
        return CallbackProgression::Continue;
    });
    EXPECT_THAT(counter, Eq(1));
}
} // namespace
