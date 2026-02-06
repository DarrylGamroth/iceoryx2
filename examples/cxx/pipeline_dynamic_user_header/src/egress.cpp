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

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_millis(200);

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

    auto egress = pipeline.egress_builder().create().value();

    while (node.wait(CYCLE_TIME).has_value()) {
        auto sample = egress.receive().value();
        while (sample.has_value()) {
            auto payload = sample->payload();
            std::cout << "egress received " << payload.number_of_bytes() << " bytes, first_byte="
                      << static_cast<int>(payload[0]) << ", user_header=" << sample->user_header() << std::endl;

            sample = egress.receive().value();
        }
    }

    std::cout << "exit" << std::endl;

    return 0;
}
