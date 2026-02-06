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

"""Pipeline worker example with dynamic payload and user header mutation."""

import ctypes

import iceoryx2 as iox2
from custom_header import CustomHeader

cycle_time = iox2.Duration.from_millis(100)

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

worker_input_builder = pipeline.worker_subscriber_builder(0)
worker_output_builder = pipeline.worker_publisher_builder(0)

if worker_input_builder is None or worker_output_builder is None:
    raise RuntimeError("worker stage 0 is not available")

worker_input = worker_input_builder.create()
worker_output = (
    worker_output_builder.initial_max_slice_len(128)
    .allocation_strategy(iox2.AllocationStrategy.PowerOfTwo)
    .create()
)

try:
    while True:
        node.wait(cycle_time)

        while True:
            sample = worker_input.receive()
            if sample is None:
                break

            payload = sample.payload()
            frame_len = payload.len()

            forwarded = worker_output.loan_slice_uninit(frame_len)
            forwarded_payload = forwarded.payload()
            for byte_idx in range(0, frame_len):
                forwarded_payload[byte_idx] = payload[byte_idx]
            forwarded_payload[0] = (forwarded_payload[0] + 1) % 255

            in_header = sample.user_header().contents
            out_header = forwarded.user_header().contents
            out_header.version = in_header.version + 1
            out_header.timestamp = in_header.timestamp + 1000000

            forwarded.assume_init().send()
            print(
                "worker forwarded",
                frame_len,
                "bytes, header=",
                in_header,
            )

except iox2.NodeWaitFailure:
    print("exit")
