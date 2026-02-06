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

#include "custom_header.hpp"
#include "iox2/iceoryx2.hpp"

#include <cstdint>
#include <iostream>
#include <utility>

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_millis(100);

auto main() -> int {
    using namespace iox2;
    set_log_level_from_env_or(LogLevel::Info);
    auto node = NodeBuilder().create<ServiceType::Ipc>().value();

    auto pipeline = node.service_builder(ServiceName::create("Example/Pipeline/DynamicUserHeader").value())
                        .pipeline<bb::Slice<uint8_t>>()
                        .user_header<CustomHeader>()
                        .number_of_stages(1)
                        .max_in_flight_samples(16)
                        .initial_max_slice_len(64)
                        .open_or_create()
                        .value();

    auto worker_subscriber_builder = pipeline.worker_subscriber_builder(0);
    if (!worker_subscriber_builder.has_value()) {
        std::cout << "worker stage 0 subscriber builder not available" << std::endl;
        return 1;
    }

    auto worker_publisher_builder = pipeline.worker_publisher_builder(0);
    if (!worker_publisher_builder.has_value()) {
        std::cout << "worker stage 0 publisher builder not available" << std::endl;
        return 1;
    }

    auto worker_input = std::move(*worker_subscriber_builder).create().value();
    auto worker_output = std::move(*worker_publisher_builder)
                             .initial_max_slice_len(128)
                             .allocation_strategy(AllocationStrategy::PowerOfTwo)
                             .create()
                             .value();

    while (node.wait(CYCLE_TIME).has_value()) {
        auto sample = worker_input.receive().value();
        while (sample.has_value()) {
            auto payload = sample->payload();
            auto frame_len = payload.number_of_bytes();

            auto forwarded = worker_output.loan_slice_uninit(frame_len).value();
            forwarded.user_header_mut().version = sample->user_header().version + 1;
            forwarded.user_header_mut().timestamp = sample->user_header().timestamp + 1000000;

            auto initialized = forwarded.write_from_fn([&](auto byte_idx) {
                auto value = payload[byte_idx];
                if (byte_idx == 0) {
                    value = static_cast<uint8_t>(value + 1U);
                }
                return value;
            });

            send(std::move(initialized)).value();

            std::cout << "worker forwarded " << frame_len << " bytes, user_header: " << sample->user_header()
                      << std::endl;

            sample = worker_input.receive().value();
        }
    }

    std::cout << "exit" << std::endl;

    return 0;
}
