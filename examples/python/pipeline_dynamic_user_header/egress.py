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

"""Pipeline egress example with dynamic payload and user header."""

import ctypes

import iceoryx2 as iox2
from custom_header import CustomHeader

cycle_time = iox2.Duration.from_millis(200)

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

egress = pipeline.egress_builder().create()

try:
    while True:
        node.wait(cycle_time)

        while True:
            sample = egress.receive()
            if sample is None:
                break

            payload = sample.payload()
            first_byte = payload[0] if payload.len() > 0 else -1
            print(
                "egress received",
                payload.len(),
                "bytes, first_byte=",
                first_byte,
                ", user_header=",
                sample.user_header().contents,
            )

except iox2.NodeWaitFailure:
    print("exit")
