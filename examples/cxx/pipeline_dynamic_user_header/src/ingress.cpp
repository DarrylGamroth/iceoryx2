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

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_millis(500);

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

    auto ingress = pipeline
                       .ingress_builder()
                       .initial_max_slice_len(64)
                       .allocation_strategy(AllocationStrategy::PowerOfTwo)
                       .create()
                       .value();

    uint64_t frame_counter = 0;

    while (node.wait(CYCLE_TIME).has_value()) {
        frame_counter += 1;
        auto frame_len = 64U + static_cast<uint64_t>((frame_counter % 4U) * 32U);

        auto sample = ingress.loan_slice_uninit(frame_len).value();
        sample.user_header_mut().version = 1;
        sample.user_header_mut().timestamp = frame_counter;

        auto initialized = sample.write_from_fn(
            [&](auto byte_idx) { return static_cast<uint8_t>((byte_idx + frame_counter) % 255U); });

        send(std::move(initialized)).value();

        std::cout << "ingress sent frame " << frame_counter << " with " << frame_len << " bytes" << std::endl;
    }

    std::cout << "exit" << std::endl;

    return 0;
}
