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

#include "custom_header.h"
#include "iox2/iceoryx2.h"

#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
#define alignof __alignof
#else
#include <stdalign.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const char* SERVICE_NAME = "Example/Pipeline/WithUserHeader";

int main(void) {
    iox2_set_log_level_from_env_or(iox2_log_level_e_INFO);

    iox2_node_builder_h node_builder = iox2_node_builder_new(NULL);
    iox2_node_h node = NULL;
    if (iox2_node_builder_create(node_builder, NULL, iox2_service_type_e_IPC, &node) != IOX2_OK) {
        printf("Failed to create node\n");
        return 1;
    }

    iox2_service_name_h service_name = NULL;
    if (iox2_service_name_new(NULL, SERVICE_NAME, strlen(SERVICE_NAME), &service_name) != IOX2_OK) {
        printf("Failed to create service name\n");
        iox2_node_drop(node);
        return 1;
    }

    iox2_service_builder_h service_builder =
        iox2_node_service_builder(&node, NULL, iox2_cast_service_name_ptr(service_name));
    iox2_service_name_drop(service_name);

    iox2_service_builder_pipeline_h pipeline_builder = iox2_service_builder_pipeline(service_builder);
    iox2_service_builder_pipeline_set_number_of_stages(&pipeline_builder, 1);
    iox2_service_builder_pipeline_set_max_in_flight_samples(&pipeline_builder, 16);

    const char* payload_type_name = "m";
    if (iox2_service_builder_pipeline_set_payload_type_details(&pipeline_builder,
                                                               iox2_type_variant_e_FIXED_SIZE,
                                                               payload_type_name,
                                                               strlen(payload_type_name),
                                                               sizeof(uint64_t),
                                                               alignof(uint64_t))
        != IOX2_OK) {
        printf("Failed to set payload type details\n");
        iox2_node_drop(node);
        return 1;
    }

    const char* header_type_name = "12CustomHeader";
    if (iox2_service_builder_pipeline_set_user_header_type_details(&pipeline_builder,
                                                                   iox2_type_variant_e_FIXED_SIZE,
                                                                   header_type_name,
                                                                   strlen(header_type_name),
                                                                   sizeof(struct CustomHeader),
                                                                   alignof(struct CustomHeader))
        != IOX2_OK) {
        printf("Failed to set user header type details\n");
        iox2_node_drop(node);
        return 1;
    }

    iox2_port_factory_pipeline_h pipeline = NULL;
    if (iox2_service_builder_pipeline_open_or_create(pipeline_builder, NULL, &pipeline) != IOX2_OK) {
        printf("Failed to open/create pipeline\n");
        iox2_node_drop(node);
        return 1;
    }

    bool has_value = false;
    iox2_port_factory_subscriber_builder_h worker_subscriber_builder =
        iox2_port_factory_pipeline_worker_subscriber_builder(&pipeline, 0, NULL, &has_value);
    if (!has_value || worker_subscriber_builder == NULL) {
        printf("Failed to acquire worker subscriber builder\n");
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    has_value = false;
    iox2_port_factory_publisher_builder_h worker_publisher_builder =
        iox2_port_factory_pipeline_worker_publisher_builder(&pipeline, 0, NULL, &has_value);
    if (!has_value || worker_publisher_builder == NULL) {
        printf("Failed to acquire worker publisher builder\n");
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    iox2_subscriber_h worker_input = NULL;
    if (iox2_port_factory_subscriber_builder_create(worker_subscriber_builder, NULL, &worker_input) != IOX2_OK) {
        printf("Failed to create worker subscriber\n");
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    iox2_publisher_h worker_output = NULL;
    if (iox2_port_factory_publisher_builder_create(worker_publisher_builder, NULL, &worker_output) != IOX2_OK) {
        printf("Failed to create worker publisher\n");
        iox2_subscriber_drop(worker_input);
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    if (iox2_publisher_update_connections(&worker_output) != IOX2_OK) {
        printf("Failed to update worker output connections\n");
    }

    while (iox2_node_wait(&node, 0, 100000000) == IOX2_OK) {
        iox2_sample_h input_sample = NULL;
        if (iox2_subscriber_receive(&worker_input, NULL, &input_sample) != IOX2_OK) {
            printf("Failed to receive worker input sample\n");
            continue;
        }

        if (input_sample == NULL) {
            continue;
        }

        const uint64_t* input_payload = NULL;
        size_t number_of_elements = 0;
        iox2_sample_payload(&input_sample, (const void**) &input_payload, &number_of_elements);

        const struct CustomHeader* input_header = NULL;
        iox2_sample_user_header(&input_sample, (const void**) &input_header);

        if (input_payload == NULL || input_header == NULL || number_of_elements == 0) {
            iox2_sample_drop(input_sample);
            continue;
        }

        iox2_sample_mut_h output_sample = NULL;
        if (iox2_publisher_loan_slice_uninit(&worker_output, NULL, &output_sample, number_of_elements) != IOX2_OK) {
            printf("Failed to loan worker output sample\n");
            iox2_sample_drop(input_sample);
            continue;
        }

        uint64_t* output_payload = NULL;
        size_t output_elements = 0;
        iox2_sample_mut_payload_mut(&output_sample, (void**) &output_payload, &output_elements);

        struct CustomHeader* output_header = NULL;
        iox2_sample_mut_user_header_mut(&output_sample, (void**) &output_header);

        if (output_payload == NULL || output_header == NULL || output_elements != number_of_elements) {
            iox2_sample_mut_drop(output_sample);
            iox2_sample_drop(input_sample);
            continue;
        }

        for (size_t i = 0; i < number_of_elements; ++i) {
            output_payload[i] = input_payload[i];
        }
        output_payload[0] += 1;

        output_header->stage = input_header->stage + 1;
        output_header->frame_counter = input_header->frame_counter;

        if (iox2_sample_mut_send(output_sample, NULL) != IOX2_OK) {
            printf("Failed to send worker output sample\n");
            iox2_sample_drop(input_sample);
            continue;
        }

        printf("worker forwarded frame %lu\n", (unsigned long) output_header->frame_counter);
        iox2_sample_drop(input_sample);
    }

    printf("exit\n");

    iox2_publisher_drop(worker_output);
    iox2_subscriber_drop(worker_input);
    iox2_port_factory_pipeline_drop(pipeline);
    iox2_node_drop(node);

    return 0;
}
