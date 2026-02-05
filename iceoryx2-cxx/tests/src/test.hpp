// Copyright (c) 2024 Contributors to the Eclipse Foundation
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

#ifndef IOX2_CXX_TESTS_TEST_HPP
#define IOX2_CXX_TESTS_TEST_HPP

#include "iox2/bb/file_name.hpp"
#include "iox2/bb/path.hpp"
#include "iox2/bb/static_string.hpp"
#include "iox2/config.hpp"
#include "iox2/service_name.hpp"
#include "iox2/service_type.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

using namespace ::testing;

namespace iox2_testing {
using namespace iox2;
template <ServiceType T>
struct TypeServiceType {
    static constexpr ServiceType TYPE = T;
};
using ServiceTypeIpc = TypeServiceType<ServiceType::Ipc>;
using ServiceTypeLocal = TypeServiceType<ServiceType::Local>;

using ServiceTypes = ::testing::Types<ServiceTypeIpc, ServiceTypeLocal>;

inline auto generate_service_name() -> ServiceName {
    static std::atomic<uint64_t> COUNTER { 0 };
    const auto now = std::chrono::system_clock::now().time_since_epoch().count();
    const auto random_number = rand(); // NOLINT(cert-msc30-c,cert-msc50-cpp)
    return ServiceName::create((std::string("test_") + std::to_string(COUNTER.fetch_add(1)) + "_" + std::to_string(now)
                                + "_" + std::to_string(random_number))
                                   .c_str())
        .value();
}

inline auto test_directory() -> bb::Path {
#if defined(_WIN32)
    return bb::Path::create("C:\\Temp\\iceoryx2\\tests\\").value();
#elif defined(__QNXNTO__)
    return bb::Path::create("/data/iceoryx2/tests/").value();
#else
    return bb::Path::create("/tmp/iceoryx2/tests/").value();
#endif
}

inline auto generate_isolated_config() -> Config {
    const auto root_path = test_directory();
    const auto* root_path_cstr = root_path.as_string().unchecked_access().c_str();
    std::error_code ec;
    std::filesystem::create_directories(root_path_cstr, ec);
    if (ec) {
        ADD_FAILURE() << "Failed to create test directory \"" << root_path_cstr << "\": " << ec.message();
    }

    static std::atomic<uint64_t> COUNTER { 0 };
    const auto now = static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
    const auto random_number = static_cast<uint64_t>(rand()); // NOLINT(cert-msc30-c,cert-msc50-cpp)

    auto prefix = iox2::bb::FileName::create("test_prefix_").value();
    char suffix_buffer[64];
    const auto written = std::snprintf(suffix_buffer,
                                       sizeof(suffix_buffer),
                                       "%" PRIu64 "_%" PRIu64 "_%" PRIu64,
                                       COUNTER.fetch_add(1),
                                       now,
                                       random_number);
    if (written > 0) {
        using SuffixString = bb::StaticString<bb::platform::IOX2_MAX_FILENAME_LENGTH>;
        const auto suffix = SuffixString::from_utf8_null_terminated_unchecked_truncated(suffix_buffer,
                                                                                         sizeof(suffix_buffer));
        auto append_result = prefix.append(suffix);
        EXPECT_TRUE(append_result.has_value());
    } else {
        ADD_FAILURE() << "Failed to format test prefix suffix.";
    }

    auto config = Config();
    config.global().set_root_path(root_path);
    config.global().set_prefix(prefix);
    return config;
}
} // namespace iox2_testing

#endif // IOX2_CXX_TESTS_TEST_HPP
