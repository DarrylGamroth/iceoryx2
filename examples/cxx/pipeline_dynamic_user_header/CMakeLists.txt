# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache Software License 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
# which is available at https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: Apache-2.0 OR MIT

cmake_minimum_required(VERSION 3.22)
project(example_cxx_pipeline_dynamic_user_header LANGUAGES CXX)

find_package(iceoryx2-cxx 0.8.999 REQUIRED)

add_executable(example_cxx_pipeline_dynamic_user_header_ingress src/ingress.cpp)
target_link_libraries(example_cxx_pipeline_dynamic_user_header_ingress iceoryx2-cxx::static-lib-cxx)

add_executable(example_cxx_pipeline_dynamic_user_header_worker src/worker.cpp)
target_link_libraries(example_cxx_pipeline_dynamic_user_header_worker iceoryx2-cxx::static-lib-cxx)

add_executable(example_cxx_pipeline_dynamic_user_header_egress src/egress.cpp)
target_link_libraries(example_cxx_pipeline_dynamic_user_header_egress iceoryx2-cxx::static-lib-cxx)
