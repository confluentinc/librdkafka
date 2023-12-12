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
#include "rdkafka_int.h"
#include "rdkafka_telemetry_encode.h"
#include "rdunittest.h"

#define _NANOPB_STRING_DECODE_MAX_BUFFER_SIZE 1024

struct metric_unit_test_data {
        rd_kafka_telemetry_metric_type_t type;
        char metric_name[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        char metric_description[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        char metric_unit[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        int64_t metric_value_int;
        double metric_value_double;
        uint64_t metric_time;
};

static struct metric_unit_test_data unit_test_data;

bool (*decode_and_print_metric_ptr)(pb_istream_t *stream,
                                    const pb_field_t *field,
                                    void **arg) = NULL;

static bool decode_and_print_string(pb_istream_t *stream,
                                    const pb_field_t *field,
                                    void **arg) {
        uint8_t buffer[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE] = {0};

        if (stream->bytes_left > sizeof(buffer) - 1) {
                fprintf(stderr, "String too long for buffer\n");
                return false;
        }

        if (!pb_read(stream, buffer, stream->bytes_left)) {
                fprintf(stderr, "Failed to read string\n");
                return false;
        }
        fprintf(stderr, "String: %s\n", buffer);
        if (arg != NULL && *arg != NULL) {
                rd_strlcpy(*arg, (char *)buffer,
                           _NANOPB_STRING_DECODE_MAX_BUFFER_SIZE);
        }

        return true;
}

static bool decode_and_print_key_value(pb_istream_t *stream,
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
                return false;
        }

        return true;
}

static bool decode_and_print_number_data_point(pb_istream_t *stream,
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
                return false;
        }

        if (arg != NULL && *arg != NULL) {
                struct metric_unit_test_data *test_data = *arg;
                test_data->metric_value_int    = data_point.value.as_int;
                test_data->metric_value_double = data_point.value.as_double;
                test_data->metric_time         = data_point.time_unix_nano;
        }

        fprintf(stderr, "NumberDataPoint value: %ld time: %lu\n",
                data_point.value.as_int, data_point.time_unix_nano);
        return true;
}

// TODO: add support for other data types
static bool
data_msg_callback(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        if (field->tag == opentelemetry_proto_metrics_v1_Metric_sum_tag) {
                opentelemetry_proto_metrics_v1_Sum *sum = field->pData;
                sum->data_points.funcs.decode =
                    &decode_and_print_number_data_point;
                if (arg != NULL && *arg != NULL) {
                        sum->data_points.arg               = &unit_test_data;
                        struct metric_unit_test_data *data = *arg;
                        data->type = RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM;
                }
        } else if (field->tag ==
                   opentelemetry_proto_metrics_v1_Metric_gauge_tag) {
                opentelemetry_proto_metrics_v1_Gauge *gauge = field->pData;
                gauge->data_points.funcs.decode =
                    &decode_and_print_number_data_point;
                if (arg != NULL && *arg != NULL) {
                        gauge->data_points.arg             = &unit_test_data;
                        struct metric_unit_test_data *data = *arg;
                        data->type = RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE;
                }
        }
        return true;
}


static bool decode_and_print_metric(pb_istream_t *stream,
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
                return false;
        }

        return true;
}

static bool decode_and_print_metric_unittest(pb_istream_t *stream,
                                             const pb_field_t *field,
                                             void **arg) {
        opentelemetry_proto_metrics_v1_Metric metric =
            opentelemetry_proto_metrics_v1_Metric_init_zero;
        metric.name.funcs.decode        = &decode_and_print_string;
        metric.name.arg                 = &unit_test_data.metric_name;
        metric.description.funcs.decode = &decode_and_print_string;
        metric.description.arg          = &unit_test_data.metric_description;
        metric.unit.funcs.decode        = &decode_and_print_string;
        metric.unit.arg                 = &unit_test_data.metric_unit;
        metric.cb_data.funcs.decode     = &data_msg_callback;
        metric.cb_data.arg              = &unit_test_data;

        if (!pb_decode(stream, opentelemetry_proto_metrics_v1_Metric_fields,
                       &metric)) {
                fprintf(stderr, "Failed to decode Metric: %s\n",
                        PB_GET_ERROR(stream));
                return false;
        }

        return true;
}


static bool decode_and_print_scope_metrics(pb_istream_t *stream,
                                           const pb_field_t *field,
                                           void **arg) {
        opentelemetry_proto_metrics_v1_ScopeMetrics scope_metrics =
            opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;
        scope_metrics.scope.name.funcs.decode    = &decode_and_print_string;
        scope_metrics.scope.version.funcs.decode = &decode_and_print_string;
        scope_metrics.metrics.funcs.decode       = decode_and_print_metric_ptr;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
                       &scope_metrics)) {
                fprintf(stderr, "Failed to decode ScopeMetrics: %s\n",
                        PB_GET_ERROR(stream));
                return false;
        }
        return true;
}

static bool decode_and_print_resource_metrics(pb_istream_t *stream,
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
                return false;
        }
        return true;
}

/**
 * Decode a metric from a buffer encoded with
 * opentelemetry_proto_metrics_v1_MetricsData datatype. Used for testing and
 * debugging.
 */
int rd_kafka_telemetry_decode_metrics(void *buffer,
                                      size_t size,
                                      rd_bool_t is_unit_test) {
        opentelemetry_proto_metrics_v1_MetricsData metricsData =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        pb_istream_t stream = pb_istream_from_buffer(buffer, size);
        metricsData.resource_metrics.funcs.decode =
            &decode_and_print_resource_metrics;
        if (is_unit_test)
                decode_and_print_metric_ptr = &decode_and_print_metric_unittest;
        else
                decode_and_print_metric_ptr = &decode_and_print_metric;

        bool status = pb_decode(
            &stream, opentelemetry_proto_metrics_v1_MetricsData_fields,
            &metricsData);
        if (!status) {
                fprintf(stderr, "Failed to decode MetricsData: %s\n",
                        PB_GET_ERROR(&stream));
        }
        return status;
}

static void clear_unit_test_data(void) {
        unit_test_data.type           = RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE;
        unit_test_data.metric_name[0] = '\0';
        unit_test_data.metric_description[0] = '\0';
        unit_test_data.metric_unit[0]        = '\0';
        unit_test_data.metric_value_int      = 0;
        unit_test_data.metric_time           = 0;
}

bool unit_test_telemetry(rd_kafka_telemetry_producer_metric_name_t metric_name,
                         const char *expected_name,
                         const char *expected_description,
                         rd_kafka_telemetry_metric_type_t expected_type,
                         rd_bool_t is_double) {
        rd_kafka_t *rk                       = rd_calloc(1, sizeof(*rk));
        rk->rk_type                          = RD_KAFKA_PRODUCER;
        rk->rk_telemetry.matched_metrics_cnt = 1;
        rk->rk_telemetry.matched_metrics =
            rd_malloc(sizeof(rd_kafka_telemetry_producer_metric_name_t) *
                      rk->rk_telemetry.matched_metrics_cnt);
        rk->rk_telemetry.matched_metrics[0] = metric_name;
        rd_strlcpy(rk->rk_name, "unittest", sizeof(rk->rk_name));
        TAILQ_INIT(&rk->rk_brokers);

        rd_kafka_broker_t *rkb      = rd_calloc(1, sizeof(*rkb));
        rkb->rkb_c.connects.val     = 1;
        rkb->rkb_c_historic.ts_last = rd_uclock() * 1000;
        TAILQ_INSERT_HEAD(&rk->rk_brokers, rkb, rkb_link);

        size_t metrics_payload_size = 0;
        clear_unit_test_data();

        void *metrics_payload =
            rd_kafka_telemetry_encode_metrics(rk, &metrics_payload_size);
        RD_UT_SAY("metrics_payload_size: %zu", metrics_payload_size);

        RD_UT_ASSERT(metrics_payload_size != 0, "Metrics payload zero");

        bool decode_status = rd_kafka_telemetry_decode_metrics(
            metrics_payload, metrics_payload_size, rd_true);

        RD_UT_ASSERT(decode_status == 1, "Decoding failed");
        RD_UT_ASSERT(unit_test_data.type == expected_type,
                     "Metric type mismatch");
        RD_UT_ASSERT(strcmp(unit_test_data.metric_name, expected_name) == 0,
                     "Metric name mismatch");
        RD_UT_ASSERT(strcmp(unit_test_data.metric_description,
                            expected_description) == 0,
                     "Metric description mismatch");
        // RD_UT_ASSERT(strcmp(unit_test_data.metric_unit, "1") == 0,
        //              "Metric unit mismatch");
        if (is_double)
                RD_UT_ASSERT(unit_test_data.metric_value_double == 1.0,
                             "Metric value mismatch");
        else
                RD_UT_ASSERT(unit_test_data.metric_value_int == 1,
                             "Metric value mismatch");
        RD_UT_ASSERT(unit_test_data.metric_time != 0, "Metric time mismatch");

        rd_free(rk->rk_telemetry.matched_metrics);
        rd_free(metrics_payload);
        rd_free(rkb);
        rd_free(rk);
        RD_UT_PASS();
}

bool unit_test_telemetry_gauge(void) {
        return unit_test_telemetry(
            RD_KAFKA_TELEMETRY_METRIC_PRODUCER_CONNECTION_CREATION_RATE,
            RD_KAFKA_TELEMETRY_METRIC_PREFIX
            "producer.connection.creation.rate",
            "The rate of connections established per second.",
            RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE, rd_true);
}

bool unit_test_telemetry_sum(void) {
        return unit_test_telemetry(
            RD_KAFKA_TELEMETRY_METRIC_PRODUCER_CONNECTION_CREATION_TOTAL,
            RD_KAFKA_TELEMETRY_METRIC_PREFIX
            "producer.connection.creation.total",
            "The total number of connections established.",
            RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM, rd_false);
}

int unittest_telemetry_decode(void) {
        int fails = 0;
        fails += unit_test_telemetry_gauge();
        fails += unit_test_telemetry_sum();
        return fails;
}
