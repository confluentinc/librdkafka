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
#include "rdkafka_telemetry.h"
#include "rdkafka_telemetry_encode.h"
#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "nanopb/pb_decode.h"
#include "opentelemetry/metrics.pb.h"

typedef union {
        int32_t intValue;
        double doubleValue;
} rd_kafka_telemetry_metric_value_t;

typedef rd_kafka_telemetry_metric_value_t (*metric_value_calculator_t)(rd_kafka_t *);

typedef struct {
        void **metrics;
        size_t count;
} rd_kafka_telemetry_metrics_repeated_t;

typedef struct {
        const char *name;
        const char *description;
        const char *unit;
        const rd_bool_t is_int;
        rd_kafka_telemetry_metric_type_t type;
        metric_value_calculator_t calculate_value;
} rd_kafka_telemetry_metric_info_t;

static rd_kafka_telemetry_metric_value_t calculate_connection_creation_total(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t total;
        rd_kafka_broker_t *rkb;

        total.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                total.intValue += rkb->rkb_c.connects.val;
        }

        return total;
}

static rd_kafka_telemetry_metric_value_t calculate_connection_creation_rate(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t total;
        rd_kafka_broker_t *rkb;

        total.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                total.intValue += rkb->rkb_c.connects.val - rkb->rkb_c_historic.connects;
                rkb->rkb_c_historic.connects = rkb->rkb_c.connects.val;
        }
        return total;
}

rd_kafka_telemetry_metric_info_t METRICS[METRIC_COUNT] = {
    [METRIC_CONNECTION_CREATION_TOTAL] =
        {
            .name            = "connection.creation.total",
            .description     = "The total number of connections established.",
            .unit            = "1",
            .is_int          = true,
            .type            = METRIC_TYPE_SUM,
            .calculate_value = &calculate_connection_creation_total,
        },
    [METRIC_CONNECTION_CREATION_RATE] =
        {
            .name        = "connection.creation.rate",
            .description = "The rate of connections established per second.",
            .unit        = "1",
            .is_int      = true,
            .type        = METRIC_TYPE_GAUGE,
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
        rd_kafka_telemetry_metrics_repeated_t *metricArr = (rd_kafka_telemetry_metrics_repeated_t *)*arg;

        for (size_t i = 0; i < metricArr->count; i++) {

                opentelemetry_proto_metrics_v1_Metric *metric = ((opentelemetry_proto_metrics_v1_Metric **)metricArr->metrics)[i];
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

static void free_metrics(opentelemetry_proto_metrics_v1_Metric **metrics, char **metric_names, size_t count) {
        for (size_t i = 0; i < count; i++) {
                rd_free(metric_names[i]);
                rd_free(metrics[i]);
        }
        rd_free(metric_names);
        rd_free(metrics);
}

/**
 * @brief Encodes the metrics to opentelemetry_proto_metrics_v1_MetricsData and
 * returns the serialized data. Currently only supports encoding of connection
 * creation total by default
 */
void *rd_kafka_telemetry_encode_metrics(rd_kafka_t *rk,
                                        size_t *size) {
        size_t message_size;
        uint8_t *buffer;
        pb_ostream_t stream;
        bool status;
        const char *metric_type_str = rd_kafka_type2str(rk->rk_type);
        char **metric_names;
        const rd_kafka_telemetry_metric_name_t *metrics_to_encode = rk->rk_telemetry.matched_metrics;
        const size_t metrics_to_encode_count = rk->rk_telemetry.matched_metrics_cnt;
        size_t metric_name_len;

        rd_kafka_dbg(rk, TELEMETRY, "METRICS", "Serializing metrics");

        opentelemetry_proto_metrics_v1_MetricsData metrics_data =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        opentelemetry_proto_metrics_v1_ResourceMetrics resource_metrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;

        opentelemetry_proto_metrics_v1_Metric **metrics;
        opentelemetry_proto_metrics_v1_NumberDataPoint *data_point;
        opentelemetry_proto_metrics_v1_Sum *sum;
        opentelemetry_proto_metrics_v1_Gauge *gauge;
        rd_kafka_telemetry_metrics_repeated_t metrics_repeated;

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
        instrumentationScope.name.arg             = rd_kafka_name(rk);
        instrumentationScope.version.funcs.encode = &encode_string;
        instrumentationScope.version.arg          = rd_kafka_version_str();

        scopeMetrics.has_scope = true;
        scopeMetrics.scope     = instrumentationScope;

        metrics = rd_malloc(sizeof(opentelemetry_proto_metrics_v1_Metric*) * metrics_to_encode_count);
        metric_names = rd_malloc(sizeof(char*) * metrics_to_encode_count);

        for (size_t i = 0; i < metrics_to_encode_count; i++) {
                metrics[i] = rd_malloc(sizeof(opentelemetry_proto_metrics_v1_Metric));
                memset(metrics[i], 0, sizeof(opentelemetry_proto_metrics_v1_Metric));
                data_point =
                    (opentelemetry_proto_metrics_v1_NumberDataPoint*) rd_malloc(sizeof(opentelemetry_proto_metrics_v1_NumberDataPoint));
                memset(data_point, 0, sizeof(opentelemetry_proto_metrics_v1_NumberDataPoint));

                if (METRICS[metrics_to_encode[i]].is_int) {
                        data_point->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_int_tag;
                        data_point->value.as_int =
                            METRICS[metrics_to_encode[i]].calculate_value(rk).intValue;

                } else {
                        data_point->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_double_tag;
                        data_point->value.as_double =
                            METRICS[metrics_to_encode[i]].calculate_value(rk).doubleValue;
                }

                data_point->time_unix_nano = rd_clock();

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


                switch (METRICS[metrics_to_encode[i]].type) {

                case METRIC_TYPE_SUM: {
                        sum = rd_malloc(sizeof(opentelemetry_proto_metrics_v1_Sum));
                        memset(sum, 0, sizeof(opentelemetry_proto_metrics_v1_Sum));
                        sum->data_points.funcs.encode =
                            &encode_number_data_point;
                        sum->data_points.arg = data_point;
                        sum->aggregation_temporality =
                            opentelemetry_proto_metrics_v1_AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE;
                        sum->is_monotonic = true;
                        metrics[i]->which_data =
                            opentelemetry_proto_metrics_v1_Metric_sum_tag;
                        metrics[i]->data.sum = *sum;
                        rd_free(sum);
                        break;
                }
                case METRIC_TYPE_GAUGE: {
                        gauge = rd_malloc(sizeof(opentelemetry_proto_metrics_v1_Gauge));
                        memset(gauge, 0, sizeof(opentelemetry_proto_metrics_v1_Gauge));
                        gauge->data_points.funcs.encode =
                            &encode_number_data_point;
                        gauge->data_points.arg = data_point;
                        metrics[i]->which_data =
                            opentelemetry_proto_metrics_v1_Metric_gauge_tag;
                        metrics[i]->data.gauge = *gauge;
                        rd_free(gauge);
                        break;
                }
                default:
                        break;
                }

                metrics[i]->description.funcs.encode = &encode_string;
                metrics[i]->description.arg =
                    METRICS[metrics_to_encode[i]].description;

                metric_name_len = strlen(metric_type_str) +
                                  strlen(METRICS[metrics_to_encode[i]].name) +
                                  2;
                metric_names[i] = rd_malloc(metric_name_len);
                memset(metric_names[i], 0, metric_name_len);
                rd_snprintf(metric_names[i], metric_name_len, "%s.%s",
                            metric_type_str,
                            METRICS[metrics_to_encode[i]].name);


                metrics[i]->name.funcs.encode = &encode_string;
                metrics[i]->name.arg          = metric_names[i];

                metrics[i]->unit.funcs.encode = &encode_string;
                metrics[i]->unit.arg          = METRICS[metrics_to_encode[i]].unit;
                rd_free(data_point);
        }

        metrics_repeated.metrics = (void **)metrics;
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
                rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                             "Failed to get encoded size");
                free_metrics(metrics, metric_names, metrics_to_encode_count);
                return NULL;
        }

        buffer = rd_malloc(message_size);
        if (buffer == NULL) {
                rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                             "Failed to allocate memory for buffer");
                free_metrics(metrics, metric_names, metrics_to_encode_count);
                return NULL;
        }

        stream = pb_ostream_from_buffer(buffer, message_size);
        status = pb_encode(&stream,
                           opentelemetry_proto_metrics_v1_MetricsData_fields,
                           &metrics_data);

        if (!status) {
                rd_kafka_dbg(rk, TELEMETRY, "METRICS", "Encoding failed: %s",
                             PB_GET_ERROR(&stream));
                rd_free(buffer);
                free_metrics(metrics, metric_names, metrics_to_encode_count);
                return NULL;
        }
        free_metrics(metrics, metric_names, metrics_to_encode_count);

        rd_kafka_dbg(rk, TELEMETRY, "METRICS",
                     "Push Telemetry metrics encoded, size: %ld",
                     stream.bytes_written);
        *size = message_size;

        return (void *)buffer;
}
