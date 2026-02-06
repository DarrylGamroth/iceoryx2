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

"""Strong type safe extensions for the pipeline messaging pattern."""

import ctypes
from typing import Type, TypeVar, get_args, get_origin

from ._iceoryx2 import *
from .slice import Slice
from .type_name import get_type_name

T = TypeVar("T", bound=ctypes.Structure)


def pipeline(self: ServiceBuilder, t: Type[T]) -> ServiceBuilderPipeline:
    """Returns the `ServiceBuilderPipeline` to create a new pipeline service."""
    type_name = t.__name__
    type_size = 0
    type_align = 0
    type_variant = TypeVariant.FixedSize

    if get_origin(t) is Slice:
        (contained_type,) = get_args(t)
        type_name = get_type_name(contained_type)
        type_variant = TypeVariant.Dynamic
        type_size = ctypes.sizeof(contained_type)
        type_align = ctypes.alignment(contained_type)
    else:
        type_name = get_type_name(t)
        type_size = ctypes.sizeof(t)
        type_align = ctypes.alignment(t)
        type_variant = TypeVariant.FixedSize

    result = self.__pipeline()
    result.__set_payload_type(t)
    return result.__payload_type_details(
        TypeDetail.new()
        .type_variant(type_variant)
        .type_name(TypeName.new(type_name))
        .size(type_size)
        .alignment(type_align)
    ).__user_header_type_details(
        TypeDetail.new()
        .type_variant(TypeVariant.FixedSize)
        .type_name(TypeName.new("()"))
        .size(0)
        .alignment(1)
    )


def set_user_header(self: ServiceBuilderPipeline, t: Type[T]) -> ServiceBuilderPipeline:
    """Sets the user header type for the service."""
    type_name = get_type_name(t)
    result = self.__user_header_type_details(
        TypeDetail.new()
        .type_variant(TypeVariant.FixedSize)
        .type_name(TypeName.new(type_name))
        .size(ctypes.sizeof(t))
        .alignment(ctypes.alignment(t))
    )
    result.__set_user_header_type(t)
    return result


ServiceBuilder.pipeline = pipeline
ServiceBuilderPipeline.user_header = set_user_header
