/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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
#include "rdkafka_telemetry_decode.h"
#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "nanopb/pb_decode.h"
#include "opentelemetry/metrics.pb.h"

#define _NANOPB_STRING_DECODE_MAX_BUFFER_SIZE 1024

rd_bool_t decode_and_print_string(pb_istream_t *stream,
                                  const pb_field_t *field,
                                  void **arg) {
        uint8_t buffer[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE] = {0};

        if (stream->bytes_left > sizeof(buffer) - 1) {
                fprintf(stderr, "String too long for buffer\n");
                return rd_false;
        }

        if (!pb_read(stream, buffer, stream->bytes_left)) {
                fprintf(stderr, "Failed to read string\n");
                return rd_false;
        }
        fprintf(stderr, "String: %s\n", buffer);

        return rd_true;
}

rd_bool_t decode_and_print_key_value(pb_istream_t *stream,
                                     const pb_field_t *field,
                                     void **arg) {
        opentelemetry_proto_common_v1_KeyValue key_value =
            opentelemetry_proto_common_v1_KeyValue_init_zero;
        key_value.key.funcs.decode = &decode_and_print_string;
        key_value.value.value.string_value.funcs.decode =
            &decode_and_print_string;
        if (!pb_decode(stream, opentelemetry_proto_common_v1_KeyValue_fields,
                       &key_value)) {
                fprintf(stderr, "Failed to decode KeyValue: %s\n",
                        PB_GET_ERROR(stream));
                return rd_false;
        }

        return rd_true;
}

rd_bool_t decode_and_print_number_data_point(pb_istream_t *stream,
                                             const pb_field_t *field,
                                             void **arg) {
        opentelemetry_proto_metrics_v1_NumberDataPoint data_point =
            opentelemetry_proto_metrics_v1_NumberDataPoint_init_zero;
        data_point.attributes.funcs.decode = &decode_and_print_key_value;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_NumberDataPoint_fields,
                       &data_point)) {
                fprintf(stderr, "Failed to decode NumberDataPoint: %s\n",
                        PB_GET_ERROR(stream));
                return rd_false;
        }

        fprintf(stderr, "NumberDataPoint value: %lld time: %llu\n",
                data_point.value.as_int, data_point.time_unix_nano);
        return rd_true;
}

// TODO: add support for other data types
rd_bool_t
data_msg_callback(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        if (field->tag == opentelemetry_proto_metrics_v1_Metric_sum_tag) {
                opentelemetry_proto_metrics_v1_Sum *sum = field->pData;
                sum->data_points.funcs.decode =
                    &decode_and_print_number_data_point;
        } else if (field->tag ==
                   opentelemetry_proto_metrics_v1_Metric_gauge_tag) {
                opentelemetry_proto_metrics_v1_Gauge *gauge = field->pData;
                gauge->data_points.funcs.decode =
                    &decode_and_print_number_data_point;
        }
        return rd_true;
}


rd_bool_t decode_and_print_metric(pb_istream_t *stream,
                                  const pb_field_t *field,
                                  void **arg) {
        opentelemetry_proto_metrics_v1_Metric metric =
            opentelemetry_proto_metrics_v1_Metric_init_zero;
        metric.name.funcs.decode        = &decode_and_print_string;
        metric.description.funcs.decode = &decode_and_print_string;
        metric.unit.funcs.decode        = &decode_and_print_string;
        metric.cb_data.funcs.decode     = &data_msg_callback;

        if (!pb_decode(stream, opentelemetry_proto_metrics_v1_Metric_fields,
                       &metric)) {
                fprintf(stderr, "Failed to decode Metric: %s\n",
                        PB_GET_ERROR(stream));
                return rd_false;
        }

        return rd_true;
}

rd_bool_t decode_and_print_scope_metrics(pb_istream_t *stream,
                                         const pb_field_t *field,
                                         void **arg) {
        opentelemetry_proto_metrics_v1_ScopeMetrics scope_metrics =
            opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;
        scope_metrics.scope.name.funcs.decode    = &decode_and_print_string;
        scope_metrics.scope.version.funcs.decode = &decode_and_print_string;
        scope_metrics.metrics.funcs.decode       = &decode_and_print_metric;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
                       &scope_metrics)) {
                fprintf(stderr, "Failed to decode ScopeMetrics: %s\n",
                        PB_GET_ERROR(stream));
                return rd_false;
        }
        return rd_true;
}

rd_bool_t decode_and_print_resource_metrics(pb_istream_t *stream,
                                            const pb_field_t *field,
                                            void **arg) {
        opentelemetry_proto_metrics_v1_ResourceMetrics resource_metrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;
        resource_metrics.resource.attributes.funcs.decode =
            &decode_and_print_key_value;
        resource_metrics.scope_metrics.funcs.decode =
            &decode_and_print_scope_metrics;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_ResourceMetrics_fields,
                       &resource_metrics)) {
                fprintf(stderr, "Failed to decode ResourceMetrics: %s\n",
                        PB_GET_ERROR(stream));
                return rd_false;
        }
        return rd_true;
}

/**
 * Decode a metric from a buffer encoded with
 * opentelemetry_proto_metrics_v1_MetricsData datatype. Used for testing and
 * debugging.
 */
void decode_metric(void *buffer, size_t size) {
        opentelemetry_proto_metrics_v1_MetricsData metricsData =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        pb_istream_t stream = pb_istream_from_buffer(buffer, size);
        metricsData.resource_metrics.funcs.decode =
            &decode_and_print_resource_metrics;

        bool status = pb_decode(
            &stream, opentelemetry_proto_metrics_v1_MetricsData_fields,
            &metricsData);
        if (!status) {
                fprintf(stderr, "Failed to decode MetricsData: %s\n",
                        PB_GET_ERROR(&stream));
        }
}
