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
#include <math.h>
#include "rdkafka_telemetry_decode.h"
#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "nanopb/pb_decode.h"
#include "opentelemetry/metrics.pb.h"
#include "rdkafka_int.h"
#include "rdkafka_telemetry_encode.h"
#include "rdunittest.h"
#include "rdkafka_lz4.h"
#include "rdgz.h"
#include "rdkafka_zstd.h"
#include "snappy.h"


#define _NANOPB_STRING_DECODE_MAX_BUFFER_SIZE 1024

struct metric_unit_test_data {
        rd_kafka_telemetry_metric_type_t type;
        int current_field;
        char metric_name[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        char metric_description[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        char metric_unit[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE];
        int64_t metric_value_int;
        double metric_value_double;
        uint64_t metric_time;
};

static struct metric_unit_test_data unit_test_data;

bool (*decode_metric_ptr)(pb_istream_t *stream,
                          const pb_field_t *field,
                          void **arg) = NULL;

static bool
decode_string(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        rd_kafka_telemetry_decode_interface_t *it             = *arg;
        uint8_t buffer[_NANOPB_STRING_DECODE_MAX_BUFFER_SIZE] = {0};

        if (stream->bytes_left > sizeof(buffer) - 1) {
                RD_INTERFACE_CALL(it, error, "String too long for buffer");
                return false;
        }

        if (!pb_read(stream, buffer, stream->bytes_left)) {
                RD_INTERFACE_CALL(it, error, "Failed to read string");
                return false;
        }
        RD_INTERFACE_CALL(it, decoded_string, buffer);

        return true;
}

static bool
decode_key_value(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        opentelemetry_proto_common_v1_KeyValue key_value =
            opentelemetry_proto_common_v1_KeyValue_init_zero;
        key_value.key.funcs.decode                      = &decode_string;
        key_value.key.arg                               = it;
        key_value.value.value.string_value.funcs.decode = &decode_string;
        key_value.value.value.string_value.arg          = it;
        if (!pb_decode(stream, opentelemetry_proto_common_v1_KeyValue_fields,
                       &key_value)) {
                RD_INTERFACE_CALL(it, error, "Failed to decode KeyValue: %s",
                                  PB_GET_ERROR(stream));
                return false;
        }

        return true;
}

static bool decode_number_data_point(pb_istream_t *stream,
                                     const pb_field_t *field,
                                     void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        opentelemetry_proto_metrics_v1_NumberDataPoint data_point =
            opentelemetry_proto_metrics_v1_NumberDataPoint_init_zero;
        data_point.attributes.funcs.decode = &decode_key_value;
        data_point.attributes.arg          = it;

        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_NumberDataPoint_fields,
                       &data_point)) {
                RD_INTERFACE_CALL(it, error,
                                  "Failed to decode NumberDataPoint: %s",
                                  PB_GET_ERROR(stream));
                return false;
        }

        if (arg != NULL && *arg != NULL) {
                struct metric_unit_test_data *test_data = *arg;
                test_data->metric_value_int    = data_point.value.as_int;
                test_data->metric_value_double = data_point.value.as_double;
                test_data->metric_time         = data_point.time_unix_nano;
        }

        RD_INTERFACE_CALL(it, decoded_number, &data_point);
        return true;
}

// TODO: add support for other data types
static bool
data_msg_callback(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        if (field->tag == opentelemetry_proto_metrics_v1_Metric_sum_tag) {
                opentelemetry_proto_metrics_v1_Sum *sum = field->pData;
                sum->data_points.funcs.decode = &decode_number_data_point;
                sum->data_points.arg          = *arg;
                if (it->decoded_type) {
                        RD_INTERFACE_CALL(it, decoded_type,
                                          RD_KAFKA_TELEMETRY_METRIC_TYPE_SUM);
                }

        } else if (field->tag ==
                   opentelemetry_proto_metrics_v1_Metric_gauge_tag) {
                opentelemetry_proto_metrics_v1_Gauge *gauge = field->pData;
                gauge->data_points.funcs.decode = &decode_number_data_point;
                gauge->data_points.arg          = *arg;
                if (it->decoded_type) {
                        RD_INTERFACE_CALL(it, decoded_type,
                                          RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE);
                }
        }
        return true;
}


static bool
decode_metric(pb_istream_t *stream, const pb_field_t *field, void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        opentelemetry_proto_metrics_v1_Metric metric =
            opentelemetry_proto_metrics_v1_Metric_init_zero;
        metric.name.funcs.decode        = &decode_string;
        metric.name.arg                 = it;
        metric.description.funcs.decode = &decode_string;
        metric.description.arg          = it;
        metric.unit.funcs.decode        = &decode_string;
        metric.unit.arg                 = it;
        metric.cb_data.funcs.decode     = &data_msg_callback;
        metric.cb_data.arg              = it;

        if (!pb_decode(stream, opentelemetry_proto_metrics_v1_Metric_fields,
                       &metric)) {
                RD_INTERFACE_CALL(it, error, "Failed to decode Metric: %s",
                                  PB_GET_ERROR(stream));
                return false;
        }

        return true;
}

static bool decode_scope_metrics(pb_istream_t *stream,
                                 const pb_field_t *field,
                                 void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        opentelemetry_proto_metrics_v1_ScopeMetrics scope_metrics =
            opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;
        scope_metrics.scope.name.funcs.decode    = &decode_string;
        scope_metrics.scope.name.arg             = it;
        scope_metrics.scope.version.funcs.decode = &decode_string;
        scope_metrics.scope.version.arg          = it;
        scope_metrics.metrics.funcs.decode       = decode_metric_ptr;
        scope_metrics.metrics.arg                = it;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
                       &scope_metrics)) {
                RD_INTERFACE_CALL(it, error,
                                  "Failed to decode ScopeMetrics: %s",
                                  PB_GET_ERROR(stream));
                return false;
        }
        return true;
}

static bool decode_resource_metrics(pb_istream_t *stream,
                                    const pb_field_t *field,
                                    void **arg) {
        rd_kafka_telemetry_decode_interface_t *it = *arg;
        opentelemetry_proto_metrics_v1_ResourceMetrics resource_metrics =
            opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;
        resource_metrics.resource.attributes.funcs.decode = &decode_key_value;
        resource_metrics.resource.attributes.arg          = it;
        resource_metrics.scope_metrics.funcs.decode = &decode_scope_metrics;
        resource_metrics.scope_metrics.arg          = it;
        if (!pb_decode(stream,
                       opentelemetry_proto_metrics_v1_ResourceMetrics_fields,
                       &resource_metrics)) {
                RD_INTERFACE_CALL(it, error,
                                  "Failed to decode ResourceMetrics: %s",
                                  PB_GET_ERROR(stream));
                return false;
        }
        return true;
}
#ifdef WITH_SNAPPY

static int rd_kafka_snappy_decompress(rd_kafka_broker_t *rkb,
                                      const char *compressed,
                                      size_t compressed_size,
                                      void **outbuf,
                                      size_t *outbuf_len) {
        struct iovec iov = {.iov_base = NULL, .iov_len = 0};

        const char *inbuf = compressed;
        size_t inlen      = compressed_size;
        int r;
        static const unsigned char snappy_java_magic[] = {0x82, 'S', 'N', 'A',
                                                          'P',  'P', 'Y', 0};
        static const size_t snappy_java_hdrlen         = 8 + 4 + 4;

        /* snappy-java adds its own header (SnappyCodec)
         * which is not compatible with the official Snappy
         * implementation.
         *   8: magic, 4: version, 4: compatible
         * followed by any number of chunks:
         *   4: length
         * ...: snappy-compressed data. */
        if (likely(inlen > snappy_java_hdrlen + 4 &&
                   !memcmp(inbuf, snappy_java_magic, 8))) {
                /* snappy-java framing */
                char errstr[128];

                inbuf = inbuf + snappy_java_hdrlen;
                inlen -= snappy_java_hdrlen;
                iov.iov_base = rd_kafka_snappy_java_uncompress(
                    inbuf, inlen, &iov.iov_len, errstr, sizeof(errstr));

                if (unlikely(!iov.iov_base)) {
                        rd_rkb_dbg(
                            rkb, MSG, "SNAPPY",
                            "Snappy decompression for message failed: %s: "
                            "ignoring message",
                            errstr);
                        return -1;  // Indicates decompression error
                }


        } else {
                /* No framing */

                /* Acquire uncompressed length */
                if (unlikely(!rd_kafka_snappy_uncompressed_length(
                        inbuf, inlen, &iov.iov_len))) {
                        rd_rkb_dbg(
                            rkb, MSG, "SNAPPY",
                            "Failed to get length of Snappy compressed payload "
                            "for message (%" PRIusz
                            " bytes): "
                            "ignoring message",
                            inlen);
                        return -1;  // Indicates decompression error
                }

                /* Allocate output buffer for uncompressed data */
                iov.iov_base = rd_malloc(iov.iov_len);
                if (unlikely(!iov.iov_base)) {
                        rd_rkb_dbg(rkb, MSG, "SNAPPY",
                                   "Failed to allocate Snappy decompress "
                                   "buffer of size %" PRIusz
                                   " for message (%" PRIusz
                                   " bytes): %s: "
                                   "ignoring message",
                                   *outbuf_len, inlen, rd_strerror(errno));
                        return -1;  // Indicates memory allocation error
                }

                /* Uncompress to outbuf */
                if (unlikely((r = rd_kafka_snappy_uncompress(inbuf, inlen,
                                                             iov.iov_base)))) {
                        rd_rkb_dbg(
                            rkb, MSG, "SNAPPY",
                            "Failed to decompress Snappy payload for message "
                            "(%" PRIusz
                            " bytes): %s: "
                            "ignoring message",
                            inlen, rd_strerror(errno));
                        rd_free(iov.iov_base);
                        return -1;  // Indicates decompression error
                }
        }
        *outbuf     = iov.iov_base;
        *outbuf_len = iov.iov_len;
        return 0;
}
#endif

/*
 * Decompress a payload using the specified compression type. Allocates memory
 * for uncompressed payload.
 * @returns 0 on success, -1 on failure. Allocated memory in
 * uncompressed_payload and its size in uncompressed_payload_size.
 */
int rd_kafka_telemetry_uncompress_metrics_payload(
    rd_kafka_broker_t *rkb,
    rd_kafka_compression_t compression_type,
    void *compressed_payload,
    size_t compressed_payload_size,
    void **uncompressed_payload,
    size_t *uncompressed_payload_size) {
        int r = -1;
        switch (compression_type) {
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP:
                *uncompressed_payload = rd_gz_decompress(
                    compressed_payload, (int)compressed_payload_size,
                    (uint64_t *)uncompressed_payload_size);
                if (*uncompressed_payload == NULL)
                        r = -1;
                else
                        r = 0;
                break;
#endif
        case RD_KAFKA_COMPRESSION_LZ4:
                r = rd_kafka_lz4_decompress(
                    rkb, 0, 0, compressed_payload, compressed_payload_size,
                    uncompressed_payload, uncompressed_payload_size);
                break;
#if WITH_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD:
                r = rd_kafka_zstd_decompress(
                    rkb, compressed_payload, compressed_payload_size,
                    uncompressed_payload, uncompressed_payload_size);
                break;
#endif
#if WITH_SNAPPY
        case RD_KAFKA_COMPRESSION_SNAPPY:
                r = rd_kafka_snappy_decompress(
                    rkb, compressed_payload, compressed_payload_size,
                    uncompressed_payload, uncompressed_payload_size);
                break;
#endif
        default:
                break;
        }
        return r;
}

/**
 * Decode a metric from a buffer encoded with
 * opentelemetry_proto_metrics_v1_MetricsData datatype. Used for testing and
 * debugging.
 */
int rd_kafka_telemetry_decode_metrics(rd_kafka_telemetry_decode_interface_t *it,
                                      void *buffer,
                                      size_t size) {
        opentelemetry_proto_metrics_v1_MetricsData metricsData =
            opentelemetry_proto_metrics_v1_MetricsData_init_zero;

        pb_istream_t stream              = pb_istream_from_buffer(buffer, size);
        metricsData.resource_metrics.arg = it;
        metricsData.resource_metrics.funcs.decode = &decode_resource_metrics;
        decode_metric_ptr                         = &decode_metric;

        bool status = pb_decode(
            &stream, opentelemetry_proto_metrics_v1_MetricsData_fields,
            &metricsData);
        if (!status) {
                RD_INTERFACE_CALL(it, error, "Failed to decode MetricsData: %s",
                                  PB_GET_ERROR(&stream));
        }
        return status;
}

static void clear_unit_test_data(void) {
        unit_test_data.type           = RD_KAFKA_TELEMETRY_METRIC_TYPE_GAUGE;
        unit_test_data.current_field  = 0;
        unit_test_data.metric_name[0] = '\0';
        unit_test_data.metric_description[0] = '\0';
        unit_test_data.metric_unit[0]        = '\0';
        unit_test_data.metric_value_int      = 0;
        unit_test_data.metric_time           = 0;
}

static void unit_test_telemetry_decoded_string(void *opaque,
                                               const uint8_t *decoded) {
        switch (unit_test_data.current_field) {
        case 2:
                rd_snprintf(unit_test_data.metric_name,
                            sizeof(unit_test_data.metric_name), "%s", decoded);
                break;
        case 3:
                rd_snprintf(unit_test_data.metric_description,
                            sizeof(unit_test_data.metric_description), "%s",
                            decoded);
                break;
        default:
                break;
        }
        unit_test_data.current_field++;
}

static void unit_test_telemetry_decoded_number(
    void *opaque,
    const opentelemetry_proto_metrics_v1_NumberDataPoint *decoded) {
        unit_test_data.metric_value_int    = decoded->value.as_int;
        unit_test_data.metric_value_double = decoded->value.as_double;
        unit_test_data.metric_time         = decoded->time_unix_nano;
}

static void
unit_test_telemetry_decoded_type(void *opaque,
                                 rd_kafka_telemetry_metric_type_t type) {
        unit_test_data.type = type;
}

static void
unit_test_telemetry_decode_error(void *opaque, const char *fmt, ...) {
        char buffer[512];
        va_list ap;
        va_start(ap, fmt);
        rd_vsnprintf(buffer, sizeof(buffer), fmt, ap);
        va_end(ap);
        RD_UT_SAY("%s", buffer);
        rd_assert(rd_false);
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

        rd_kafka_telemetry_decode_interface_t cbs = {
            .error          = unit_test_telemetry_decode_error,
            .decoded_string = unit_test_telemetry_decoded_string,
            .decoded_number = unit_test_telemetry_decoded_number,
            .decoded_type   = unit_test_telemetry_decoded_type,
            .opaque         = &unit_test_data};

        TAILQ_INIT(&rk->rk_brokers);

        rk->rk_broker_cnt.val = 1;
        rk->rk_telemetry.rk_historic_c.ts_start =
            rd_uclock() * 1000 - 1000 * 1000 * 1000;
        rk->rk_telemetry.rk_historic_c.ts_last =
            rd_uclock() * 1000 - 1000 * 1000 * 1000;
        rd_kafka_broker_t rkb;
        rd_kafka_broker_keep(&rkb);
        rkb.rkb_nodeid                            = 0;
        rkb.rkb_c.connects.val                    = 1;
        rkb.rkb_telemetry.rkb_historic_c.connects = 0;
        rd_avg_init(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_rtt, RD_AVG_GAUGE,
                    0, 500 * 1000, 2, rd_true);
        rd_avg_init(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_outbuf_latency,
                    RD_AVG_GAUGE, 0, 500 * 1000, 2, rd_true);
        rd_avg_init(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_throttle,
                    RD_AVG_GAUGE, 0, 500 * 1000, 2, rd_true);
        TAILQ_INSERT_HEAD(&rk->rk_brokers, &rkb, rkb_link);

        clear_unit_test_data();

        rd_buf_t *metrics_payload = rd_kafka_telemetry_encode_metrics(rk);
        RD_UT_SAY("metrics_payload_size: %zu", metrics_payload->rbuf_len);

        RD_UT_ASSERT(metrics_payload->rbuf_len != 0, "Metrics payload zero");

        bool decode_status = rd_kafka_telemetry_decode_metrics(
            &cbs, metrics_payload->rbuf_wpos->seg_p, metrics_payload->rbuf_len);

        RD_UT_ASSERT(decode_status == 1, "Decoding failed");
        RD_UT_ASSERT(unit_test_data.type == expected_type,
                     "Metric type mismatch");
        RD_UT_ASSERT(strcmp(unit_test_data.metric_name, expected_name) == 0,
                     "Metric name mismatch");
        RD_UT_ASSERT(strcmp(unit_test_data.metric_description,
                            expected_description) == 0,
                     "Metric description mismatch");
        if (is_double)
                RD_UT_ASSERT(fabs(unit_test_data.metric_value_double - 1.0) <
                                 0.01,
                             "Metric value mismatch");
        else
                RD_UT_ASSERT(unit_test_data.metric_value_int == 1,
                             "Metric value mismatch");
        RD_UT_ASSERT(unit_test_data.metric_time != 0, "Metric time mismatch");

        rd_avg_destroy(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_rtt);
        rd_avg_destroy(
            &rkb.rkb_telemetry.rd_avg_current.rkb_avg_outbuf_latency);
        rd_avg_destroy(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_throttle);
        rd_free(rk->rk_telemetry.matched_metrics);
        rd_buf_destroy_free(metrics_payload);
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
