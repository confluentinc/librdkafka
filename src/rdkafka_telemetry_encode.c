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
#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "nanopb/pb_decode.h"
#include "opentelemetry/metrics.pb.h"

typedef union {
        int32_t intValue;
        double doubleValue;
} rd_kafka_telemetry_metric_value_t;

typedef rd_kafka_telemetry_metric_value_t (
    *rd_kafka_telemetry_metric_value_calculator_t)(rd_kafka_t *);

typedef struct {
        opentelemetry_proto_metrics_v1_Metric **metrics;
        size_t count;
} rd_kafka_telemetry_metrics_repeated_t;

typedef struct {
        const char *name;
        const char *description;
        const char *unit;
        const rd_bool_t is_int;
        rd_kafka_telemetry_metric_type_t type;
        rd_kafka_telemetry_metric_value_calculator_t calculate_value;
} rd_kafka_telemetry_metric_info_t;

static rd_kafka_telemetry_metric_value_t
calculate_connection_creation_total(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t total;
        rd_kafka_broker_t *rkb;

        total.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                total.intValue += rkb->rkb_c.connects.val;
        }

        return total;
}

static rd_kafka_telemetry_metric_value_t
calculate_connection_creation_rate(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t total;
        rd_kafka_broker_t *rkb;

        total.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                total.intValue +=
                    rkb->rkb_c.connects.val - rkb->rkb_c_historic.connects;
                rkb->rkb_c_historic.connects = rkb->rkb_c.connects.val;
        }
        return total;
}

static const rd_kafka_telemetry_metric_info_t
    RD_KAFKA_TELEMETRY_METRICS_INFO[RD_KAFKA_TELEMETRY_METRIC__CNT] = {
        [RD_KAFKA_TELEMETRY_METRIC_CONNECTION_CREATION_TOTAL] =
            {
                .name        = "connection.creation.total",
                .description = "The total number of connections established.",
                .unit        = "1",
                .is_int      = true,
                .type        = RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM,
                .calculate_value = &calculate_connection_creation_total,
            },
        [RD_KAFKA_TELEMETRY_METRIC_CONNECTION_CREATION_RATE] =
            {
                .name = "connection.creation.rate",
                .description =
                    "The rate of connections established per second.",
                .unit            = "1",
                .is_int          = true,
                .type            = RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE,
                .calculate_value = &calculate_connection_creation_rate,
            },
};


static bool
encode_string(pb_ostream_t *stream, const pb_field_t *field, void *const *arg) {
        if (!pb_encode_tag_for_field(stream, field))
                return false;
        return pb_encode_string(stream, (uint8_t *)(*arg), strlen(*arg));
}

// TODO: Update to handle multiple data points.
static bool encode_number_data_point(pb_ostream_t *stream,
                                     const pb_field_t *field,
                                     void *const *arg) {
        opentelemetry_proto_metrics_v1_NumberDataPoint *data_point =
            (opentelemetry_proto_metrics_v1_NumberDataPoint *)*arg;
        if (!pb_encode_tag_for_field(stream, field))
                return false;

        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_NumberDataPoint_fields,
            data_point);
}

static bool
encode_metric(pb_ostream_t *stream, const pb_field_t *field, void *const *arg) {
        rd_kafka_telemetry_metrics_repeated_t *metricArr =
            (rd_kafka_telemetry_metrics_repeated_t *)*arg;
        size_t i;

        for (i = 0; i < metricArr->count; i++) {

                opentelemetry_proto_metrics_v1_Metric *metric =
                    metricArr->metrics[i];
                if (!pb_encode_tag_for_field(stream, field))
                        return false;

                if (!pb_encode_submessage(
                        stream, opentelemetry_proto_metrics_v1_Metric_fields,
                        metric))
                        return false;
        }
        return true;
}

static bool encode_scope_metrics(pb_ostream_t *stream,
                                 const pb_field_t *field,
                                 void *const *arg) {
        opentelemetry_proto_metrics_v1_ScopeMetrics *scope_metrics =
            (opentelemetry_proto_metrics_v1_ScopeMetrics *)*arg;
        if (!pb_encode_tag_for_field(stream, field))
                return false;

        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
            scope_metrics);
}

static bool encode_resource_metrics(pb_ostream_t *stream,
                                    const pb_field_t *field,
                                    void *const *arg) {
        opentelemetry_proto_metrics_v1_ResourceMetrics *resource_metrics =
            (opentelemetry_proto_metrics_v1_ResourceMetrics *)*arg;
        if (!pb_encode_tag_for_field(stream, field))
                return false;

        return pb_encode_submessage(
            stream, opentelemetry_proto_metrics_v1_ResourceMetrics_fields,
            resource_metrics);
}

// TODO: Update to handle multiple KV pairs
static bool encode_key_value(pb_ostream_t *stream,
                             const pb_field_t *field,
                             void *const *arg) {
        if (!pb_encode_tag_for_field(stream, field))
                return false;

        opentelemetry_proto_common_v1_KeyValue *key_value =
            (opentelemetry_proto_common_v1_KeyValue *)*arg;
        return pb_encode_submessage(
            stream, opentelemetry_proto_common_v1_KeyValue_fields, key_value);
}

static void
free_metrics(opentelemetry_proto_metrics_v1_Metric **metrics,
             char **metric_names,
             opentelemetry_proto_metrics_v1_NumberDataPoint **data_points,
             size_t count) {
        size_t i;
        for (i = 0; i < count; i++) {
                rd_free(data_points[i]);
                rd_free(metric_names[i]);
                rd_free(metrics[i]);
        }
        rd_free(data_points);
        rd_free(metric_names);
        rd_free(metrics);
}

/**
 * @brief Encodes the metrics to opentelemetry_proto_metrics_v1_MetricsData and
 * returns the serialized data. Currently only supports encoding of connection
 * creation total by default
 */
void *rd_kafka_telemetry_encode_metrics(rd_kafka_t *rk, size_t *size) {
        size_t message_size;
        uint8_t *buffer;
        pb_ostream_t stream;
        bool status;
        const char *metric_type_str = rd_kafka_type2str(rk->rk_type);
        char **metric_names;
        const rd_kafka_telemetry_metric_name_t *metrics_to_encode =
            rk->rk_telemetry.matched_metrics;
        const size_t metrics_to_encode_count =
            rk->rk_telemetry.matched_metrics_cnt;
        size_t metric_name_len;

        rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                     "Serializing metrics");

        opentelemetry_proto_metrics_v1_MetricsData metrics_data =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        opentelemetry_proto_metrics_v1_ResourceMetrics resource_metrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;

        opentelemetry_proto_metrics_v1_Metric **metrics;
        opentelemetry_proto_metrics_v1_NumberDataPoint **data_points;
        rd_kafka_telemetry_metrics_repeated_t metrics_repeated;
        rd_ts_t now = rd_clock();

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
        //    resource_metrics.has_resource = true;
        //    resource_metrics.resource.attributes.funcs.encode =
        //    &encode_key_value; resource_metrics.resource.attributes.arg =
        //    &resource_attribute;

        opentelemetry_proto_metrics_v1_ScopeMetrics scopeMetrics =
            opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;

        opentelemetry_proto_common_v1_InstrumentationScope
            instrumentationScope =
                opentelemetry_proto_common_v1_InstrumentationScope_init_zero;
        instrumentationScope.name.funcs.encode    = &encode_string;
        instrumentationScope.name.arg             = (void *)rd_kafka_name(rk);
        instrumentationScope.version.funcs.encode = &encode_string;
        instrumentationScope.version.arg = (void *)rd_kafka_version_str();

        scopeMetrics.has_scope = true;
        scopeMetrics.scope     = instrumentationScope;

        metrics = rd_malloc(sizeof(opentelemetry_proto_metrics_v1_Metric *) *
                            metrics_to_encode_count);
        data_points =
            rd_malloc(sizeof(opentelemetry_proto_metrics_v1_NumberDataPoint *) *
                      metrics_to_encode_count);
        metric_names = rd_malloc(sizeof(char *) * metrics_to_encode_count);
        size_t i;

        for (i = 0; i < metrics_to_encode_count; i++) {
                metrics[i] =
                    rd_calloc(1, sizeof(opentelemetry_proto_metrics_v1_Metric));
                data_points[i] = rd_calloc(
                    1, sizeof(opentelemetry_proto_metrics_v1_NumberDataPoint));

                if (RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]]
                        .is_int) {
                        data_points[i]->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_int_tag;
                        data_points[i]->value.as_int =
                            RD_KAFKA_TELEMETRY_METRICS_INFO
                                [metrics_to_encode[i]]
                                    .calculate_value(rk)
                                    .intValue;

                } else {
                        data_points[i]->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_double_tag;
                        data_points[i]->value.as_double =
                            RD_KAFKA_TELEMETRY_METRICS_INFO
                                [metrics_to_encode[i]]
                                    .calculate_value(rk)
                                    .doubleValue;
                }

                data_points[i]->time_unix_nano = now;

                //    TODO: Add data point attributes as needed
                //    opentelemetry_proto_common_v1_KeyValue attribute =
                //    opentelemetry_proto_common_v1_KeyValue_init_zero;
                //    attribute.key.funcs.encode = &encode_string;
                //    attribute.key.arg = "type";
                //    attribute.has_value = true;
                //    attribute.value.which_value =
                //    opentelemetry_proto_common_v1_AnyValue_string_value_tag;
                //    attribute.value.value.string_value.funcs.encode =
                //    &encode_string; attribute.value.value.string_value.arg =
                //    "heap";
                //
                //    data_point.attributes.funcs.encode = &encode_key_value;
                //    data_point.attributes.arg = &attribute;


                switch (RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]]
                            .type) {

                case RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM: {
                        metrics[i]->which_data =
                            opentelemetry_proto_metrics_v1_Metric_sum_tag;
                        metrics[i]->data.sum.data_points.funcs.encode =
                            &encode_number_data_point;
                        metrics[i]->data.sum.data_points.arg = data_points[i];
                        metrics[i]->data.sum.aggregation_temporality =
                            opentelemetry_proto_metrics_v1_AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE;
                        metrics[i]->data.sum.is_monotonic = true;
                        break;
                }
                case RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE: {
                        metrics[i]->which_data =
                            opentelemetry_proto_metrics_v1_Metric_gauge_tag;
                        metrics[i]->data.gauge.data_points.funcs.encode =
                            &encode_number_data_point;
                        metrics[i]->data.gauge.data_points.arg = data_points[i];
                        break;
                }
                default:
                        rd_assert(!"Unknown metric type");
                        break;
                }

                metrics[i]->description.funcs.encode = &encode_string;
                metrics[i]->description.arg =
                    (void *)
                        RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]]
                            .description;

                metric_name_len =
                    strlen(metric_type_str) +
                    strlen(RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]]
                               .name) +
                    2;
                metric_names[i] = rd_calloc(1, metric_name_len);
                rd_snprintf(
                    metric_names[i], metric_name_len, "%s.%s", metric_type_str,
                    RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]].name);


                metrics[i]->name.funcs.encode = &encode_string;
                metrics[i]->name.arg          = metric_names[i];

                metrics[i]->unit.funcs.encode = &encode_string;
                metrics[i]->unit.arg =
                    (void *)
                        RD_KAFKA_TELEMETRY_METRICS_INFO[metrics_to_encode[i]]
                            .unit;
        }

        metrics_repeated.metrics = metrics;
        metrics_repeated.count   = metrics_to_encode_count;

        scopeMetrics.metrics.funcs.encode = &encode_metric;
        scopeMetrics.metrics.arg          = &metrics_repeated;


        resource_metrics.scope_metrics.funcs.encode = &encode_scope_metrics;
        resource_metrics.scope_metrics.arg          = &scopeMetrics;

        metrics_data.resource_metrics.funcs.encode = &encode_resource_metrics;
        metrics_data.resource_metrics.arg          = &resource_metrics;

        status = pb_get_encoded_size(
            &message_size, opentelemetry_proto_metrics_v1_MetricsData_fields,
            &metrics_data);
        if (!status) {
                rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                             "Failed to get encoded size");
                free_metrics(metrics, metric_names, data_points,
                             metrics_to_encode_count);
                return NULL;
        }

        buffer = rd_malloc(message_size);
        if (buffer == NULL) {
                rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                             "Failed to allocate memory for buffer");
                free_metrics(metrics, metric_names, data_points,
                             metrics_to_encode_count);
                return NULL;
        }

        stream = pb_ostream_from_buffer(buffer, message_size);
        status = pb_encode(&stream,
                           opentelemetry_proto_metrics_v1_MetricsData_fields,
                           &metrics_data);

        if (!status) {
                rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                             "Encoding failed: %s", PB_GET_ERROR(&stream));
                rd_free(buffer);
                free_metrics(metrics, metric_names, data_points,
                             metrics_to_encode_count);
                return NULL;
        }
        free_metrics(metrics, metric_names, data_points,
                     metrics_to_encode_count);

        rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                     "Push Telemetry metrics encoded, size: %ld",
                     stream.bytes_written);
        *size = message_size;

        return (void *)buffer;
}
