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

#define RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR 1000000

typedef struct {
        opentelemetry_proto_metrics_v1_Metric **metrics;
        size_t count;
} rd_kafka_telemetry_metrics_repeated_t;


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
        }
        return total;
}

static rd_kafka_telemetry_metric_value_t
calculate_broker_avg_rtt(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t avg_rtt;
        int64_t sum_value = 0, broker_count = rk->rk_broker_cnt.val;
        rd_kafka_broker_t *rkb;

        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                int64_t current_cnt  = rkb->rkb_avg_rtt.ra_v.cnt;
                int64_t historic_cnt = rkb->rkb_c_historic.rkb_avg_rtt.ra_v.cnt;

                if (current_cnt > historic_cnt) {
                        int64_t current_sum = rkb->rkb_avg_rtt.ra_v.sum;
                        int64_t historic_sum =
                            rkb->rkb_c_historic.rkb_avg_rtt.ra_v.sum;
                        int64_t cnt_diff = current_cnt - historic_cnt;
                        int64_t sum_diff = current_sum - historic_sum;

                        sum_value +=
                            sum_diff /
                            (cnt_diff * RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR);
                }
        }
        avg_rtt.intValue = sum_value / broker_count;
        return avg_rtt;
}

static rd_kafka_telemetry_metric_value_t
calculate_broker_max_rtt(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t max_rtt;
        rd_kafka_broker_t *rkb;

        max_rtt.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                max_rtt.intValue = RD_MAX(max_rtt.intValue,
                                          rkb->rkb_avg_rtt.ra_v.maxv_interval);
        }
        max_rtt.intValue /= RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR;
        return max_rtt;
}

static rd_kafka_telemetry_metric_value_t
calculate_throttle_avg(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t avg_throttle;
        int64_t sum_value = 0, broker_count = rk->rk_broker_cnt.val;
        rd_kafka_broker_t *rkb;

        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                int64_t current_cnt = rkb->rkb_avg_throttle.ra_v.cnt;
                int64_t historic_cnt =
                    rkb->rkb_c_historic.rkb_avg_throttle.ra_v.cnt;

                if (current_cnt > historic_cnt) {
                        int64_t current_sum = rkb->rkb_avg_throttle.ra_v.sum;
                        int64_t historic_sum =
                            rkb->rkb_c_historic.rkb_avg_throttle.ra_v.sum;
                        int64_t cnt_diff = current_cnt - historic_cnt;
                        int64_t sum_diff = current_sum - historic_sum;

                        sum_value +=
                            sum_diff /
                            (cnt_diff * RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR);
                }
        }
        avg_throttle.intValue = sum_value / broker_count;
        return avg_throttle;
}


static rd_kafka_telemetry_metric_value_t
calculate_throttle_max(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t max_throttle;
        rd_kafka_broker_t *rkb;

        max_throttle.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                max_throttle.intValue =
                    RD_MAX(max_throttle.intValue,
                           rkb->rkb_avg_throttle.ra_v.maxv_interval);
        }
        max_throttle.intValue /= RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR;
        return max_throttle;
}

static rd_kafka_telemetry_metric_value_t
calculate_queue_time_avg(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t avg_queue_time;
        int64_t sum_value = 0, broker_count = rk->rk_broker_cnt.val;
        rd_kafka_broker_t *rkb;

        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                int64_t current_cnt = rkb->rkb_avg_outbuf_latency.ra_v.cnt;
                int64_t historic_cnt =
                    rkb->rkb_c_historic.rkb_avg_outbuf_latency.ra_v.cnt;

                if (current_cnt > historic_cnt) {
                        int64_t current_sum =
                            rkb->rkb_avg_outbuf_latency.ra_v.sum;
                        int64_t historic_sum =
                            rkb->rkb_c_historic.rkb_avg_outbuf_latency.ra_v.sum;
                        int64_t cnt_diff = current_cnt - historic_cnt;
                        int64_t sum_diff = current_sum - historic_sum;

                        sum_value +=
                            sum_diff /
                            (cnt_diff * RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR);
                }
        }
        avg_queue_time.intValue = sum_value / broker_count;
        return avg_queue_time;
}

static rd_kafka_telemetry_metric_value_t
calculate_queue_time_max(rd_kafka_t *rk) {
        rd_kafka_telemetry_metric_value_t max_queue_time;
        rd_kafka_broker_t *rkb;

        max_queue_time.intValue = 0;
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                max_queue_time.intValue =
                    RD_MAX(max_queue_time.intValue,
                           rkb->rkb_avg_outbuf_latency.ra_v.maxv_interval);
        }
        max_queue_time.intValue /= RDKAFKA_TELEMETRY_NS_TO_MS_FACTOR;
        return max_queue_time;
}


static void reset_historical_metrics(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb;

        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rkb->rkb_c_historic.connects    = rkb->rkb_c.connects.val;
                rkb->rkb_c_historic.rkb_avg_rtt = rkb->rkb_avg_rtt;
                rd_atomic32_set(&rkb->rkb_avg_rtt.ra_v.maxv_reset, 1);
                rkb->rkb_c_historic.rkb_avg_throttle = rkb->rkb_avg_throttle;
                rd_atomic32_set(&rkb->rkb_avg_throttle.ra_v.maxv_reset, 1);
                rkb->rkb_c_historic.rkb_avg_outbuf_latency =
                    rkb->rkb_avg_outbuf_latency;
                rd_atomic32_set(&rkb->rkb_avg_outbuf_latency.ra_v.maxv_reset,
                                1);
        }
}

static const rd_kafka_telemetry_metric_value_calculator_t
    PRODUCER_METRIC_VALUE_CALCULATORS[RD_KAFKA_TELEMETRY_PRODUCER_METRIC__CNT] =
        {
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_CONNECTION_CREATION_RATE] =
                &calculate_connection_creation_rate,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_CONNECTION_CREATION_TOTAL] =
                &calculate_connection_creation_total,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_NODE_REQUEST_LATENCY_AVG] =
                &calculate_broker_avg_rtt,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_NODE_REQUEST_LATENCY_MAX] =
                &calculate_broker_max_rtt,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_PRODUCE_THROTTLE_TIME_AVG] =
                &calculate_throttle_avg,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_PRODUCE_THROTTLE_TIME_MAX] =
                &calculate_throttle_max,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_RECORD_QUEUE_TIME_AVG] =
                &calculate_queue_time_avg,
            [RD_KAFKA_TELEMETRY_METRIC_PRODUCER_RECORD_QUEUE_TIME_MAX] =
                &calculate_queue_time_max,
};

static const rd_kafka_telemetry_metric_value_calculator_t
    CONSUMER_METRIC_VALUE_CALCULATORS[RD_KAFKA_TELEMETRY_CONSUMER_METRIC__CNT] =
        {
            [RD_KAFKA_TELEMETRY_METRIC_CONSUMER_CONNECTION_CREATION_RATE] =
                &calculate_connection_creation_rate,
            [RD_KAFKA_TELEMETRY_METRIC_CONSUMER_CONNECTION_CREATION_TOTAL] =
                &calculate_connection_creation_total,
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
        char **metric_names;
        const int *metrics_to_encode = rk->rk_telemetry.matched_metrics;
        const size_t metrics_to_encode_count =
            rk->rk_telemetry.matched_metrics_cnt;
        size_t metric_name_len;

        if (metrics_to_encode_count == 0) {
                rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                             "No metrics to encode.");
                *size = 0;
                return NULL;
        }

        rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                     "Serializing metrics");

        opentelemetry_proto_metrics_v1_MetricsData metrics_data =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        opentelemetry_proto_metrics_v1_ResourceMetrics resource_metrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;

        opentelemetry_proto_metrics_v1_Metric **metrics;
        opentelemetry_proto_metrics_v1_NumberDataPoint **data_points;
        rd_kafka_telemetry_metrics_repeated_t metrics_repeated;
        rd_ts_t now_ns = rd_clock() * 1000;

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

                rd_kafka_telemetry_metric_value_calculator_t
                    metricValueCalculator =
                        (rk->rk_type == RD_KAFKA_PRODUCER)
                            ? PRODUCER_METRIC_VALUE_CALCULATORS
                                  [metrics_to_encode[i]]
                            : CONSUMER_METRIC_VALUE_CALCULATORS
                                  [metrics_to_encode[i]];
                const rd_kafka_telemetry_metric_info_t *info =
                    TELEMETRY_METRIC_INFO(rk);

                if (info[metrics_to_encode[i]].is_int) {
                        data_points[i]->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_int_tag;
                        data_points[i]->value.as_int =
                            metricValueCalculator(rk).intValue;

                } else {
                        data_points[i]->which_value =
                            opentelemetry_proto_metrics_v1_NumberDataPoint_as_double_tag;
                        data_points[i]->value.as_double =
                            metricValueCalculator(rk).doubleValue;
                }

                data_points[i]->time_unix_nano = now_ns;
                /* TODO: For delta temporality do we needs to be rese when push
                 * fails? */
                data_points[i]->start_time_unix_nano = now_ns;

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


                switch (info[metrics_to_encode[i]].type) {

                case RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM: {
                        metrics[i]->which_data =
                            opentelemetry_proto_metrics_v1_Metric_sum_tag;
                        metrics[i]->data.sum.data_points.funcs.encode =
                            &encode_number_data_point;
                        metrics[i]->data.sum.data_points.arg = data_points[i];
                        /* TODO: Do we need sum metrics with Delta temporality?
                         */
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
                    (void *)info[metrics_to_encode[i]].description;

                metric_name_len = strlen(TELEMETRY_METRIC_PREFIX) +
                                  strlen(info[metrics_to_encode[i]].name) + 1;
                metric_names[i] = rd_calloc(1, metric_name_len);
                rd_snprintf(metric_names[i], metric_name_len, "%s%s",
                            TELEMETRY_METRIC_PREFIX,
                            info[metrics_to_encode[i]].name);


                metrics[i]->name.funcs.encode = &encode_string;
                metrics[i]->name.arg          = metric_names[i];

                metrics[i]->unit.funcs.encode = &encode_string;
                metrics[i]->unit.arg = (void *)info[metrics_to_encode[i]].unit;
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

        reset_historical_metrics(rk);

        return (void *)buffer;
}
