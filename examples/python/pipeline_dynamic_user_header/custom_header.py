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

"""Pipeline example custom user header type."""

import ctypes


class CustomHeader(ctypes.Structure):
    """The strongly typed custom pipeline user header."""

    _fields_ = [
        ("version", ctypes.c_uint32),
        ("timestamp", ctypes.c_uint64),
    ]

    def __str__(self) -> str:
        return (
            f"CustomHeader {{ version: {self.version}, timestamp: {self.timestamp} }}"
        )

    @staticmethod
    def type_name() -> str:
        return "CustomHeader"
