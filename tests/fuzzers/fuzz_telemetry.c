/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_telemetry_decode.h"
#include <stdlib.h>
#include <string.h>

static void dummy_decoded_string(void *opaque, const uint8_t *decoded) {}
static void dummy_decoded_NumberDataPoint(
    void *opaque,
    const opentelemetry_proto_metrics_v1_NumberDataPoint *decoded) {}
static void dummy_decoded_int64(void *opaque, int64_t decoded) {}
static void dummy_decoded_type(void *opaque,
                               rd_kafka_telemetry_metric_type_t type) {}
static void dummy_decode_error(void *opaque, const char *error, ...) {}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
        if (size == 0)
                return 0;

        rd_kafka_telemetry_decode_interface_t *decode_interface = 
            calloc(1, sizeof(*decode_interface));
        
        decode_interface->decoded_string          = dummy_decoded_string;
        decode_interface->decoded_NumberDataPoint = dummy_decoded_NumberDataPoint;
        decode_interface->decoded_int64           = dummy_decoded_int64;
        decode_interface->decoded_type            = dummy_decoded_type;
        decode_interface->decode_error            = dummy_decode_error;
        decode_interface->opaque                  = NULL;

        /* We need a non-const pointer for rd_kafka_telemetry_decode_metrics */
        void *buffer = malloc(size);
        memcpy(buffer, data, size);

        rd_kafka_telemetry_decode_metrics(decode_interface, buffer, size);

        free(buffer);
        free(decode_interface);

        return 0;
}
