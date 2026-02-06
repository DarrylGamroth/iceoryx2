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

    iox2_port_factory_subscriber_builder_h egress_builder =
        iox2_port_factory_pipeline_egress_builder(&pipeline, NULL);

    iox2_subscriber_h egress = NULL;
    if (iox2_port_factory_subscriber_builder_create(egress_builder, NULL, &egress) != IOX2_OK) {
        printf("Failed to create egress subscriber\n");
        iox2_port_factory_pipeline_drop(pipeline);
        iox2_node_drop(node);
        return 1;
    }

    while (iox2_node_wait(&node, 0, 200000000) == IOX2_OK) {
        iox2_sample_h sample = NULL;
        if (iox2_subscriber_receive(&egress, NULL, &sample) != IOX2_OK) {
            printf("Failed to receive egress sample\n");
            continue;
        }

        if (sample == NULL) {
            continue;
        }

        const uint64_t* payload = NULL;
        size_t number_of_elements = 0;
        iox2_sample_payload(&sample, (const void**) &payload, &number_of_elements);

        const struct CustomHeader* user_header = NULL;
        iox2_sample_user_header(&sample, (const void**) &user_header);

        if (payload != NULL && user_header != NULL && number_of_elements > 0) {
            printf("egress received value=%lu, stage=%u, frame=%lu\n",
                   (unsigned long) payload[0],
                   user_header->stage,
                   (unsigned long) user_header->frame_counter);
        }

        iox2_sample_drop(sample);
    }

    printf("exit\n");

    iox2_subscriber_drop(egress);
    iox2_port_factory_pipeline_drop(pipeline);
    iox2_node_drop(node);

    return 0;
}
