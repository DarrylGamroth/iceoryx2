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
    iox2_service_builder_pipeline_set_initial_max_slice_len(&pipeline_builder, 1);

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

    iox2_port_factory_publisher_builder_h ingress_builder =
        iox2_port_factory_pipeline_ingress_builder(&pipeline, NULL);
    iox2_publisher_h ingress = NULL;
    if (iox2_port_factory_publisher_builder_create(ingress_builder, NULL, &ingress) != IOX2_OK) {
        printf("Failed to create ingress publisher\n");
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    if (iox2_publisher_update_connections(&ingress) != IOX2_OK) {
        printf("Failed to update ingress connections\n");
    }

    uint64_t frame_counter = 0;

    while (iox2_node_wait(&node, 0, 500000000) == IOX2_OK) {
        frame_counter += 1;

        iox2_sample_mut_h sample = NULL;
        if (iox2_publisher_loan_slice_uninit(&ingress, NULL, &sample, 1) != IOX2_OK) {
            printf("Failed to loan sample\n");
            continue;
        }

        uint64_t* payload = NULL;
        size_t number_of_elements = 0;
        iox2_sample_mut_payload_mut(&sample, (void**) &payload, &number_of_elements);
        if (payload == NULL || number_of_elements != 1) {
            printf("Invalid payload buffer\n");
            iox2_sample_mut_drop(sample);
            continue;
        }
        payload[0] = frame_counter;

        struct CustomHeader* user_header = NULL;
        iox2_sample_mut_user_header_mut(&sample, (void**) &user_header);
        if (user_header == NULL) {
            printf("Invalid user header buffer\n");
            iox2_sample_mut_drop(sample);
            continue;
        }
        user_header->stage = 0;
        user_header->frame_counter = frame_counter;

        if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
            printf("Failed to send sample\n");
            continue;
        }

        printf("ingress sent frame %lu\n", (unsigned long) frame_counter);
    }

    printf("exit\n");

    iox2_publisher_drop(ingress);
    iox2_port_factory_pipeline_drop(pipeline);
    iox2_node_drop(node);

    return 0;
}
