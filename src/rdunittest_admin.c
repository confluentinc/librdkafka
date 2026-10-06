/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_admin.h"
#include "rdkafka_aux.h"
#include "rdkafka_broker.h"
#include "rdkafka_buf.h"

/** Build a response with unknown tags at both nesting levels. */
static rd_kafka_buf_t *ut_describe_cluster_response(int16_t version,
                                                    int8_t endpoint_type,
                                                    const char *cluster_id,
                                                    const char *host,
                                                    int32_t port,
                                                    int32_t controller_id,
                                                    int32_t broker_cnt,
                                                    int32_t operations,
                                                    rd_kafka_resp_err_t error) {
        rd_kafka_buf_t *reply = rd_kafka_buf_new(1, 256);
        int32_t i;
        reply->rkbuf_flags |= RD_KAFKA_OP_F_FLEXVER;
        reply->rkbuf_reqhdr.ApiKey     = RD_KAFKAP_DescribeCluster;
        reply->rkbuf_reqhdr.ApiVersion = version;
        rd_kafka_buf_write_i32(reply, 0);
        rd_kafka_buf_write_i16(reply, error);
        rd_kafka_buf_write_str(reply, error ? "broker error" : NULL, -1);
        if (version >= 1)
                rd_kafka_buf_write_i8(reply, endpoint_type);
        rd_kafka_buf_write_str(reply, cluster_id, -1);
        rd_kafka_buf_write_i32(reply, controller_id);
        rd_kafka_buf_write_arraycnt(reply, broker_cnt);
        for (i = 0; i < broker_cnt; i++) {
                rd_kafka_buf_write_i32(reply, i + 1);
                rd_kafka_buf_write_str(reply, host, -1);
                rd_kafka_buf_write_i32(reply, port);
                rd_kafka_buf_write_str(reply, i == 0 ? NULL : "rack-2", -1);
                if (version >= 2)
                        rd_kafka_buf_write_bool(reply, rd_false);
                rd_kafka_buf_write_uvarint(reply, 1);
                rd_kafka_buf_write_uvarint(reply, 42);
                rd_kafka_buf_write_uvarint(reply, 2);
                rd_kafka_buf_write_i16(reply, 123);
        }
        rd_kafka_buf_write_i32(reply, operations);
        rd_kafka_buf_write_uvarint(reply, 1);
        rd_kafka_buf_write_uvarint(reply, 42);
        rd_kafka_buf_write_uvarint(reply, 2);
        rd_kafka_buf_write_i16(reply, 456);
        rd_slice_init_full(&reply->rkbuf_reader, &reply->rkbuf_buf);
        return reply;
}

static rd_kafka_resp_err_t ut_describe_cluster_parse(rd_kafka_t *rk,
                                                     rd_kafka_buf_t *reply,
                                                     rd_kafka_op_t **result,
                                                     char *errstr,
                                                     size_t errstr_size) {
        rd_kafka_op_t *request = rd_kafka_op_new(RD_KAFKA_OP_DESCRIBECLUSTER);
        rd_kafka_resp_err_t error;
        request->rko_rk = rk;
        rd_kafka_confval_init_ptr(&request->rko_u.admin_request.options.opaque,
                                  "opaque");
        reply->rkbuf_rkb = rd_kafka_broker_internal(rk);
        error = rd_kafka_DescribeClusterResponse_parse(request, result, reply,
                                                       errstr, errstr_size);
        rd_kafka_op_destroy(request);
        return error;
}

int unittest_DescribeClusterResponse_parse(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_buf_t *reply;
        rd_kafka_op_t *result = NULL;
        rd_kafka_ClusterDescription_t *description;
        rd_kafka_resp_err_t error;
        char errstr[512];
        char *data;
        size_t length, cut;
        size_t c;
        int16_t version;
        const struct {
                const char *cluster_id;
                const char *host;
                int32_t port;
                int32_t controller_id;
                int32_t broker_cnt;
                int32_t operations;
                rd_kafka_resp_err_t error;
                rd_kafka_resp_err_t expected;
        } cases[] = {
            {"cluster", "localhost", 9092, 2, 2,
             1 << RD_KAFKA_ACL_OPERATION_DESCRIBE, 0, 0},
            {"cluster", "localhost", 9092, -1, 2, INT32_MIN, 0, 0},
            {"cluster", "localhost", 9092, 99, 2, 0, 0, 0},
            {"cluster", "localhost", 9092, -1, 0, INT32_MIN, 0, 0},
            {NULL, "localhost", 9092, 2, 2, 0, 0, RD_KAFKA_RESP_ERR__BAD_MSG},
            {"cluster", NULL, 9092, 2, 2, 0, 0, RD_KAFKA_RESP_ERR__BAD_MSG},
            {"cluster", "localhost", -1, 2, 2, 0, 0,
             RD_KAFKA_RESP_ERR__BAD_MSG},
            {"cluster", "localhost", 65536, 2, 2, 0, 0,
             RD_KAFKA_RESP_ERR__BAD_MSG},
            {"cluster", "localhost", 9092, 2, -1, 0, 0,
             RD_KAFKA_RESP_ERR__BAD_MSG},
            {"cluster", "localhost", 9092, 2, 2, 0,
             RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
             RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED},
        };

        rd_kafka_conf_set(conf, "log_level", "0", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        RD_UT_ASSERT(rk, "Failed to create producer: %s", errstr);
        for (version = 0; version <= 2; version++) {
                for (c = 0; c < RD_ARRAYSIZE(cases); c++) {
                        reply = ut_describe_cluster_response(
                            version, 1, cases[c].cluster_id, cases[c].host,
                            cases[c].port, cases[c].controller_id,
                            cases[c].broker_cnt, cases[c].operations,
                            cases[c].error);
                        result = NULL;
                        error  = ut_describe_cluster_parse(
                            rk, reply, &result, errstr, sizeof(errstr));
                        rd_kafka_buf_destroy(reply);
                        RD_UT_ASSERT(error == cases[c].expected,
                                     "Case %" PRIusz
                                     ": expected %s, got %s: %s",
                                     c, rd_kafka_err2name(cases[c].expected),
                                     rd_kafka_err2name(error), errstr);
                        if (error) {
                                RD_UT_ASSERT(!result,
                                             "Parse failure returned a result");
                                if (cases[c].error)
                                        RD_UT_ASSERT(
                                            strstr(errstr, "broker error"),
                                            "Broker error message lost");
                                continue;
                        }
                        RD_UT_ASSERT(result, "Missing result");
                        description = rd_list_elem(
                            &result->rko_u.admin_result.results, 0);
                        /* Check ownership after the response buffer has been
                         * destroyed.
                         */
                        RD_UT_ASSERT(
                            !strcmp(description->cluster_id, "cluster"),
                            "Incorrect cluster id");
                        RD_UT_ASSERT(description->node_cnt ==
                                         (size_t)cases[c].broker_cnt,
                                     "Incorrect broker count");
                        if (cases[c].broker_cnt) {
                                RD_UT_ASSERT(
                                    description->nodes[0]->id == 1 &&
                                        !description->nodes[0]->rack &&
                                        !strcmp(description->nodes[0]->host,
                                                "localhost") &&
                                        description->nodes[0]->port == 9092,
                                    "Incorrect first broker");
                                RD_UT_ASSERT(
                                    !strcmp(description->nodes[1]->rack,
                                            "rack-2"),
                                    "Incorrect second broker rack");
                        }
                        if (cases[c].controller_id == 2)
                                RD_UT_ASSERT(
                                    description->controller &&
                                        description->controller->id == 2 &&
                                        !strcmp(description->controller->rack,
                                                "rack-2"),
                                    "Incorrect controller");
                        else if (cases[c].controller_id >= 0)
                                RD_UT_ASSERT(
                                    description->controller &&
                                        description->controller->id ==
                                            cases[c].controller_id &&
                                        !description->controller->host &&
                                        description->controller->port == 0 &&
                                        !description->controller->rack,
                                    "Incorrect controller without a known "
                                    "endpoint");
                        else
                                RD_UT_ASSERT(!description->controller,
                                             "Expected no controller");
                        if (cases[c].operations == INT32_MIN)
                                RD_UT_ASSERT(
                                    description->authorized_operations_cnt ==
                                            -1 &&
                                        !description->authorized_operations,
                                    "Incorrect authorization sentinel");
                        else if (cases[c].operations)
                                RD_UT_ASSERT(
                                    description->authorized_operations_cnt ==
                                            1 &&
                                        description->authorized_operations[0] ==
                                            RD_KAFKA_ACL_OPERATION_DESCRIBE,
                                    "Incorrect authorized operations");
                        else
                                RD_UT_ASSERT(
                                    description->authorized_operations_cnt ==
                                            0 &&
                                        description->authorized_operations,
                                    "Incorrect empty authorized operations");
                        rd_kafka_op_destroy(result);
                }

                reply = ut_describe_cluster_response(
                    version, 1, "cluster", "localhost", 9092, 2, 2,
                    1 << RD_KAFKA_ACL_OPERATION_DESCRIBE, 0);
                length = rd_buf_len(&reply->rkbuf_buf);
                data   = rd_malloc(length);
                RD_UT_ASSERT(
                    rd_slice_read(&reply->rkbuf_reader, data, length) == length,
                    "Failed to copy response");
                rd_kafka_buf_destroy(reply);
                /* Every incomplete prefix must fail, including after partial
                 * allocation. */
                for (cut = 0; cut < length; cut++) {
                        reply = rd_kafka_buf_new_shadow(data, length, NULL);
                        RD_UT_ASSERT(rd_slice_init(&reply->rkbuf_reader,
                                                   &reply->rkbuf_buf, 0,
                                                   cut) == 0,
                                     "Failed to limit response prefix");
                        reply->rkbuf_flags |= RD_KAFKA_OP_F_FLEXVER;
                        reply->rkbuf_reqhdr.ApiKey = RD_KAFKAP_DescribeCluster;
                        reply->rkbuf_reqhdr.ApiVersion = version;
                        result                         = NULL;
                        error = ut_describe_cluster_parse(
                            rk, reply, &result, errstr, sizeof(errstr));
                        rd_kafka_buf_destroy(reply);
                        RD_UT_ASSERT((error == RD_KAFKA_RESP_ERR__BAD_MSG ||
                                      error == RD_KAFKA_RESP_ERR__UNDERFLOW) &&
                                         !result,
                                     "Truncated response (%" PRIusz "/%" PRIusz
                                     ") did not fail: %s",
                                     cut, length, rd_kafka_err2name(error));
                }
                rd_free(data);
                if (version >= 1) {
                        reply = ut_describe_cluster_response(
                            version, 2, "cluster", "localhost", 9092, 2, 2, 0,
                            0);
                        result = NULL;
                        error  = ut_describe_cluster_parse(
                            rk, reply, &result, errstr, sizeof(errstr));
                        rd_kafka_buf_destroy(reply);
                        RD_UT_ASSERT(
                            error == RD_KAFKA_RESP_ERR__BAD_MSG && !result,
                            "Controller endpoint response was accepted");
                }
        }
        rd_kafka_destroy(rk);
        RD_UT_PASS();
}
