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

import ctypes

import iceoryx2 as iox2
import pytest

service_types = [iox2.ServiceType.Ipc, iox2.ServiceType.Local]


class Payload(ctypes.Structure):
    _fields_ = [("data", ctypes.c_ubyte)]


class HeaderA(ctypes.Structure):
    _fields_ = [("stage", ctypes.c_ubyte)]


class HeaderB(ctypes.Structure):
    _fields_ = [("stage", ctypes.c_uint)]


@pytest.mark.parametrize("service_type", service_types)
def test_non_existing_service_can_be_created(service_type: iox2.ServiceType) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    sut = node.service_builder(service_name).pipeline(Payload).create()
    assert sut.name == service_name


@pytest.mark.parametrize("service_type", service_types)
def test_existing_service_cannot_be_created(service_type: iox2.ServiceType) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    _existing = node.service_builder(service_name).pipeline(Payload).create()

    with pytest.raises(iox2.PipelineCreateError):
        node.service_builder(service_name).pipeline(Payload).create()


@pytest.mark.parametrize("service_type", service_types)
def test_existing_service_can_be_opened(service_type: iox2.ServiceType) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    _existing = node.service_builder(service_name).pipeline(Payload).create()

    sut = node.service_builder(service_name).pipeline(Payload).open()
    assert sut.name == service_name


@pytest.mark.parametrize("service_type", service_types)
def test_non_existing_service_cannot_be_opened(service_type: iox2.ServiceType) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    with pytest.raises(iox2.PipelineOpenError):
        node.service_builder(service_name).pipeline(Payload).open()


@pytest.mark.parametrize("service_type", service_types)
def test_service_builder_configuration_works(service_type: iox2.ServiceType) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    number_of_stages = 3
    max_in_flight_samples = 12
    max_nodes = 7
    initial_max_slice_len = 42

    sut = (
        node.service_builder(service_name)
        .pipeline(Payload)
        .number_of_stages(number_of_stages)
        .max_in_flight_samples(max_in_flight_samples)
        .max_nodes(max_nodes)
        .initial_max_slice_len(initial_max_slice_len)
        .create()
    )

    static_config = sut.static_config
    assert static_config.number_of_stages == number_of_stages
    assert static_config.max_in_flight_samples == max_in_flight_samples
    assert static_config.max_nodes == max_nodes
    assert static_config.initial_max_slice_len == initial_max_slice_len
    assert sut.number_of_stages() == number_of_stages
    assert sut.number_of_ingress_ports() == 0
    assert sut.number_of_egress_ports() == 0


@pytest.mark.parametrize("service_type", service_types)
def test_open_or_create_service_with_attributes_work(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    attribute_spec = iox2.AttributeSpecifier.new().define(
        iox2.AttributeKey.new("what"), iox2.AttributeValue.new("ever")
    )
    attribute_verifier = iox2.AttributeVerifier.new().require(
        iox2.AttributeKey.new("what"), iox2.AttributeValue.new("ever")
    )

    service_name = iox2.testing.generate_service_name()
    sut_create = (
        node.service_builder(service_name)
        .pipeline(Payload)
        .open_or_create_with_attributes(attribute_verifier)
    )

    sut_open = node.service_builder(service_name).pipeline(Payload).open()

    assert sut_create.attributes == attribute_spec.attributes
    assert sut_open.attributes == attribute_spec.attributes


@pytest.mark.parametrize("service_type", service_types)
def test_opening_existing_service_with_wrong_user_header_type_fails(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    _existing = (
        node.service_builder(service_name)
        .pipeline(Payload)
        .user_header(HeaderA)
        .create()
    )

    with pytest.raises(iox2.PipelineOpenError, match="IncompatibleUserHeaderType"):
        node.service_builder(service_name).pipeline(Payload).user_header(HeaderB).open()


@pytest.mark.parametrize("service_type", service_types)
def test_runtime_role_builders_and_dynamic_lists_work(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    sut = (
        node.service_builder(service_name)
        .pipeline(Payload)
        .number_of_stages(1)
        .max_in_flight_samples(4)
        .create()
    )

    ingress_builder = sut.ingress_builder()
    worker_sub_builder = sut.worker_subscriber_builder(0)
    worker_pub_builder = sut.worker_publisher_builder(0)
    egress_builder = sut.egress_builder()

    assert worker_sub_builder is not None
    assert worker_pub_builder is not None
    assert sut.worker_subscriber_builder(1) is None
    assert sut.worker_publisher_builder(1) is None
    assert sut.number_of_workers(1) is None

    ingress = ingress_builder.create()
    worker_sub = worker_sub_builder.create()
    worker_pub = worker_pub_builder.create()
    egress = egress_builder.create()

    assert sut.number_of_ingress_ports() == 1
    assert sut.number_of_workers(0) == 1
    assert sut.number_of_egress_ports() == 1
    assert len(sut.list_ingresses()) == 1
    assert len(sut.list_workers(0)) == 1
    assert sut.list_workers(1) is None
    assert len(sut.list_egresses()) == 1

    ingress.send_copy(Payload(7))

    worker_sample = None
    for _ in range(10_000):
        worker_sample = worker_sub.receive()
        if worker_sample is not None:
            break

    assert worker_sample is not None
    worker_pub.send_copy(Payload(worker_sample.payload().contents.data + 1))

    egress_sample = None
    for _ in range(10_000):
        egress_sample = egress.receive()
        if egress_sample is not None:
            break

    assert egress_sample is not None
    assert egress_sample.payload().contents.data == 8
