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
#include "rdkafka_int.h"
#include "rdkafka_telemetry_encode.h"
#include "pb.h"
#include "pb_encode.h"
#include "pb_decode.h"
#include "metrics.pb.h"

bool encode_string(pb_ostream_t *stream,
                   const pb_field_t *field,
                   void *const *arg) {
        if (!pb_encode_tag_for_field(stream, field))
                return false;
        return pb_encode_string(stream, (uint8_t *)(*arg), strlen(*arg));
}

bool encode_number_data_point(pb_ostream_t *stream,
                              const pb_field_t *field,
                              void *const *arg) {
        opentelemetry_proto_metrics_v1_NumberDataPoint *data_point =
            (opentelemetry_proto_metrics_v1_NumberDataPoint *)*arg;
        if (!pb_encode_tag_for_field(stream, field)) {
                return false;
        }
        bool status = pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_NumberDataPoint_fields,
            data_point);
        return status;
}

bool encode_metric(pb_ostream_t *stream,
                   const pb_field_t *field,
                   void *const *arg) {
        opentelemetry_proto_metrics_v1_Metric *metric =
            (opentelemetry_proto_metrics_v1_Metric *)*arg;
        if (!pb_encode_tag_for_field(stream, field)) {
                return false;
        }
        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_Metric_fields, metric);
}

bool encode_scope_metrics(pb_ostream_t *stream,
                          const pb_field_t *field,
                          void *const *arg) {
        opentelemetry_proto_metrics_v1_ScopeMetrics *scope_metrics =
            (opentelemetry_proto_metrics_v1_ScopeMetrics *)*arg;
        if (!pb_encode_tag_for_field(stream, field)) {
                return false;
        }
        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
            scope_metrics);
}

bool encode_resource_metrics(pb_ostream_t *stream,
                             const pb_field_t *field,
                             void *const *arg) {
        opentelemetry_proto_metrics_v1_ResourceMetrics *resource_metrics =
            (opentelemetry_proto_metrics_v1_ResourceMetrics *)*arg;
        if (!pb_encode_tag_for_field(stream, field)) {
                return false;
        }
        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_ResourceMetrics_fields,
            resource_metrics);
}

bool encode_key_value(pb_ostream_t *stream,
                      const pb_field_t *field,
                      void *const *arg) {
        if (!pb_encode_tag_for_field(stream, field)) {
                return false;
        }
        opentelemetry_proto_common_v1_KeyValue *key_value =
            (opentelemetry_proto_common_v1_KeyValue *)*arg;
        return pb_encode_submessage(
            stream, opentelemetry_proto_common_v1_KeyValue_fields, key_value);
}

static const char *rd_kafka_type2str(rd_kafka_type_t type) {
        static const char *types[] = {
            [RD_KAFKA_PRODUCER] = "producer",
            [RD_KAFKA_CONSUMER] = "consumer",
        };
        return types[type];
}

// TODO: Update
int calculate_connection_creation_total(rd_kafka_t *rk) {
        rd_atomic32_t total = {0};
        rd_kafka_broker_t *rkb;

        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_atomic32_add(&total, rkb->rkb_c.connects.val);
                rkb->rkb_c_historic.connects = rkb->rkb_c.connects;
        }

        rd_kafka_dbg(rk, TELEMETRY, "CONNECTIONS", "Total connections: %d",
                     total.val);

        return total.val;
}

/**
 * @brief Encodes the metrics to opentelemetry_proto_metrics_v1_MetricsData and
 * returns the serialized data. Currently only supports encoding of connection
 * creation total by default
 */
void *encode_metrics(rd_kafka_t *rk, size_t *size) {
        rd_kafka_dbg(rk, TELEMETRY, "METRICS", "Serializing metrics");

        opentelemetry_proto_metrics_v1_MetricsData metricsData =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        opentelemetry_proto_metrics_v1_ResourceMetrics resourceMetrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;

        //    TODO: Add resource attributes as needed
        //    opentelemetry_proto_common_v1_KeyValue resource_attribute =
        //    opentelemetry_proto_common_v1_KeyValue_init_zero;
        //    resource_attribute.key.funcs.encode = &encode_string;
        //    resource_attribute.key.arg = "type-resource_attribute";
        //    resource_attribute.has_value = true;
        //    resource_attribute.value.which_value =
        //    opentelemetry_proto_common_v1_AnyValue_string_value_tag;
        //    resource_attribute.value.value.string_value.funcs.encode =
        //    &encode_string; resource_attribute.value.value.string_value.arg =
        //    "heap-resource_attribute";
        //
        //    resourceMetrics.has_resource = true;
        //    resourceMetrics.resource.attributes.funcs.encode =
        //    &encode_key_value; resourceMetrics.resource.attributes.arg =
        //    &resource_attribute;

        opentelemetry_proto_metrics_v1_ScopeMetrics scopeMetrics =
            opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;

        opentelemetry_proto_common_v1_InstrumentationScope
            instrumentationScope =
                opentelemetry_proto_common_v1_InstrumentationScope_init_zero;
        // TODO: Update these with librdkafka client name and version
        instrumentationScope.name.funcs.encode    = &encode_string;
        instrumentationScope.name.arg             = &"Example scope name";
        instrumentationScope.version.funcs.encode = &encode_string;
        instrumentationScope.version.arg          = &"Example scope version";

        scopeMetrics.has_scope = true;
        scopeMetrics.scope     = instrumentationScope;

        opentelemetry_proto_metrics_v1_Metric metric =
            opentelemetry_proto_metrics_v1_Metric_init_zero;

        opentelemetry_proto_metrics_v1_Sum sum =
            opentelemetry_proto_metrics_v1_Sum_init_zero;

        opentelemetry_proto_metrics_v1_NumberDataPoint data_point =
            opentelemetry_proto_metrics_v1_NumberDataPoint_init_zero;
        data_point.which_value =
            opentelemetry_proto_metrics_v1_NumberDataPoint_as_int_tag;
        data_point.value.as_int   = calculate_connection_creation_total(rk);
        data_point.time_unix_nano = rd_clock();

        //    TODO: Add data point attributes as needed
        //    opentelemetry_proto_common_v1_KeyValue attribute =
        //    opentelemetry_proto_common_v1_KeyValue_init_zero;
        //    attribute.key.funcs.encode = &encode_string;
        //    attribute.key.arg = "type";
        //    attribute.has_value = true;
        //    attribute.value.which_value =
        //    opentelemetry_proto_common_v1_AnyValue_string_value_tag;
        //    attribute.value.value.string_value.funcs.encode = &encode_string;
        //    attribute.value.value.string_value.arg = "heap";
        //
        //    data_point.attributes.funcs.encode = &encode_key_value;
        //    data_point.attributes.arg = &attribute;

        sum.data_points.funcs.encode = &encode_number_data_point;
        sum.data_points.arg          = &data_point;
        sum.aggregation_temporality =
            opentelemetry_proto_metrics_v1_AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE;
        sum.is_monotonic = true;

        metric.which_data = opentelemetry_proto_metrics_v1_Metric_sum_tag;
        metric.data.sum   = sum;

        metric.description.funcs.encode = &encode_string;
        metric.description.arg =
            &"The total number of connections established.";

        char *metric_suffix = ".connection.creation.total";
        char *metric_name   = (char *)rd_malloc(
            strlen(rd_kafka_type2str(rk->rk_type)) + strlen(metric_suffix) + 1);
        strcpy(metric_name, rd_kafka_type2str(rk->rk_type));
        strcat(metric_name, metric_suffix);

        metric.name.funcs.encode = &encode_string;
        metric.name.arg          = metric_name;

        metric.unit.funcs.encode = &encode_string;
        metric.unit.arg          = &"1";

        scopeMetrics.metrics.funcs.encode = &encode_metric;
        scopeMetrics.metrics.arg          = &metric;

        resourceMetrics.scope_metrics.funcs.encode = &encode_scope_metrics;
        resourceMetrics.scope_metrics.arg          = &scopeMetrics;

        metricsData.resource_metrics.funcs.encode = &encode_resource_metrics;
        metricsData.resource_metrics.arg          = &resourceMetrics;

        size_t message_size;
        bool status = pb_get_encoded_size(
            &message_size, opentelemetry_proto_metrics_v1_MetricsData_fields,
            &metricsData);
        if (!status) {
                rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                             "Failed to get encoded size");
                rd_free(metric_name);
                return NULL;
        }

        uint8_t *buffer = rd_malloc(message_size);
        if (buffer == NULL) {
                rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                             "Failed to allocate memory for buffer");
                rd_free(metric_name);
                return NULL;
        }

        pb_ostream_t stream = pb_ostream_from_buffer(buffer, message_size);
        status              = pb_encode(&stream,
                           opentelemetry_proto_metrics_v1_MetricsData_fields,
                           &metricsData);

        if (!status) {
                rd_kafka_dbg(rk, TELEMETRY, "METRICS", "Encoding failed: %s",
                             PB_GET_ERROR(&stream));
                rd_free(buffer);
                rd_free(metric_name);
                return NULL;
        }
        rd_free(metric_name);

        rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                     "Push Telemetry metrics encoded, size: %ld",
                     stream.bytes_written);
        *size = message_size;
        return (void *)buffer;
}
