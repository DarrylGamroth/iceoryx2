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

"""Pipeline ingress example with dynamic payload and user header."""

import ctypes

import iceoryx2 as iox2
from custom_header import CustomHeader

cycle_time = iox2.Duration.from_millis(500)

iox2.set_log_level_from_env_or(iox2.LogLevel.Info)
node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)

pipeline = (
    node.service_builder(iox2.ServiceName.new("Example/Pipeline/DynamicUserHeader"))
    .pipeline(iox2.Slice[ctypes.c_uint8])
    .user_header(CustomHeader)
    .number_of_stages(1)
    .max_in_flight_samples(16)
    .initial_max_slice_len(64)
    .open_or_create()
)

ingress = (
    pipeline.ingress_builder()
    .initial_max_slice_len(64)
    .allocation_strategy(iox2.AllocationStrategy.PowerOfTwo)
    .create()
)

frame_counter = 0

try:
    while True:
        node.wait(cycle_time)
        frame_counter += 1

        frame_len = 64 + (frame_counter % 4) * 32
        sample = ingress.loan_slice_uninit(frame_len)

        header = sample.user_header().contents
        header.version = 1
        header.timestamp = frame_counter

        payload = sample.payload()
        for byte_idx in range(0, frame_len):
            payload[byte_idx] = (byte_idx + frame_counter) % 255

        sample.assume_init().send()
        print("ingress sent frame", frame_counter, "with", frame_len, "bytes")

except iox2.NodeWaitFailure:
    print("exit")
