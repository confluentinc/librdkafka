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

#include "test.h"
#include "../src/rdkafka_proto.h"

/**
 * Verify the dedicated API with Metadata v13 only, and the Metadata fallback
 * for older brokers. Broker errors must not switch to the fallback.
 */
static void do_test_describe_cluster(void) {
        const struct {
                rd_bool_t supported;
                rd_bool_t include_authorized_operations;
                int32_t controller_id;
                rd_kafka_resp_err_t error;
                rd_bool_t timeout;
        } cases[] = {
            {rd_true, rd_false, 2, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_true, rd_true, 2, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_true, rd_true, -1, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_true, rd_false, 99, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_false, rd_false, 2, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_false, rd_true, 2, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_false, rd_true, -1, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_false, rd_false, 99, RD_KAFKA_RESP_ERR_NO_ERROR, rd_false},
            {rd_true, rd_true, 2,
             RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED, rd_false},
            {rd_true, rd_true, 2, RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION,
             rd_false},
            {rd_true, rd_true, 2, RD_KAFKA_RESP_ERR__TRANSPORT, rd_false},
            {rd_true, rd_true, 2, RD_KAFKA_RESP_ERR_NO_ERROR, rd_true},
        };
        size_t c;

        SUB_TEST_QUICK();
        for (c = 0; c < RD_ARRAYSIZE(cases); c++) {
                rd_kafka_mock_cluster_t *mcluster;
                rd_kafka_t *rk;
                rd_kafka_conf_t *conf;
                rd_kafka_AdminOptions_t *options;
                rd_kafka_queue_t *q;
                rd_kafka_event_t *event;
                const rd_kafka_DescribeCluster_result_t *result;
                const rd_kafka_metadata_t *metadata;
                rd_kafka_mock_request_t **requests;
                const char *bootstraps;
                char errstr[512];
                size_t request_cnt, i;
                int describe_requests = 0, metadata_requests = 0;
                rd_kafka_resp_err_t expected_error =
                    cases[c].timeout ? RD_KAFKA_RESP_ERR__TIMED_OUT
                                     : cases[c].error;

                TEST_SAY("DescribeCluster case %" PRIusz "\n", c);
                mcluster = test_mock_cluster_new(3, &bootstraps);
                TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(
                    mcluster, RD_KAFKAP_Metadata, cases[c].supported ? 13 : 10,
                    cases[c].supported ? 13 : 10));
                if (!cases[c].supported)
                        TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(
                            mcluster, RD_KAFKAP_DescribeCluster, -1, -1));
                rd_kafka_mock_set_controller_id(mcluster,
                                                cases[c].controller_id);
                TEST_CALL_ERR__(
                    rd_kafka_mock_broker_set_rack(mcluster, 2, "rack-2"));

                test_conf_init(&conf, NULL, 20);
                test_conf_set(conf, "bootstrap.servers", bootstraps);
                rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
                /* Complete broker discovery before tracking the admin request.
                 */
                TEST_CALL_ERR__(rd_kafka_metadata(rk, 0, NULL, &metadata,
                                                  tmout_multip(5000)));

                q       = rd_kafka_queue_new(rk);
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER);
                TEST_CALL_ERROR__(
                    rd_kafka_AdminOptions_set_include_authorized_operations(
                        options, cases[c].include_authorized_operations));
                /* Explicit targeting also lets us test a missing controller. */
                if (cases[c].controller_id != 2)
                        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_broker(
                            options, 1, errstr, sizeof(errstr)));
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, tmout_multip(cases[c].timeout ? 200 : 5000),
                    errstr, sizeof(errstr)));
                if (cases[c].timeout)
                        rd_kafka_mock_broker_push_request_error_rtts(
                            mcluster, 2, RD_KAFKAP_DescribeCluster, 1,
                            RD_KAFKA_RESP_ERR_NO_ERROR, tmout_multip(1000));
                else if (cases[c].error)
                        rd_kafka_mock_push_request_errors(
                            mcluster, RD_KAFKAP_DescribeCluster, 1,
                            cases[c].error);

                rd_kafka_mock_start_request_tracking(mcluster);
                rd_kafka_DescribeCluster(rk, options, q);
                rd_kafka_AdminOptions_destroy(options);
                event = test_wait_admin_result(
                    q, RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT,
                    tmout_multip(10000));
                TEST_ASSERT(event, "Missing DescribeCluster result");
                TEST_ASSERT(rd_kafka_event_error(event) == expected_error,
                            "Case %" PRIusz ": expected %s, got %s: %s", c,
                            rd_kafka_err2name(expected_error),
                            rd_kafka_err2name(rd_kafka_event_error(event)),
                            rd_kafka_event_error_string(event));
                result = rd_kafka_event_DescribeCluster_result(event);
                TEST_ASSERT(result, "Missing DescribeCluster result object");

                if (!expected_error) {
                        const rd_kafka_Node_t **nodes;
                        const rd_kafka_Node_t *controller;
                        const rd_kafka_AclOperation_t *operations;
                        size_t node_cnt;
                        size_t operation_cnt;
                        char *cluster_id =
                            rd_kafka_clusterid(rk, tmout_multip(5000));

                        TEST_ASSERT(
                            cluster_id &&
                                !strcmp(
                                    cluster_id,
                                    rd_kafka_DescribeCluster_result_cluster_id(
                                        result)),
                            "Cluster ids do not match");
                        rd_kafka_mem_free(rk, cluster_id);
                        nodes = rd_kafka_DescribeCluster_result_nodes(
                            result, &node_cnt);
                        TEST_ASSERT(node_cnt == 3,
                                    "Expected 3 brokers, got %" PRIusz,
                                    node_cnt);
                        for (i = 0; i < node_cnt; i++) {
                                int id = rd_kafka_Node_id(nodes[i]);
                                int j;
                                TEST_ASSERT(id >= 1 && id <= 3,
                                            "Invalid broker id %d", id);
                                for (j = 0; j < metadata->broker_cnt; j++)
                                        if (metadata->brokers[j].id == id)
                                                break;
                                TEST_ASSERT(j < metadata->broker_cnt,
                                            "Broker %d missing from Metadata",
                                            id);
                                TEST_ASSERT(
                                    !strcmp(rd_kafka_Node_host(nodes[i]),
                                            metadata->brokers[j].host) &&
                                        rd_kafka_Node_port(nodes[i]) ==
                                            metadata->brokers[j].port,
                                    "Incorrect endpoint for broker %d", id);
                                if (id == 2)
                                        TEST_ASSERT(
                                            rd_kafka_Node_rack(nodes[i]) &&
                                                !strcmp(rd_kafka_Node_rack(
                                                            nodes[i]),
                                                        "rack-2"),
                                            "Incorrect rack for broker 2");
                                else
                                        TEST_ASSERT(
                                            !rd_kafka_Node_rack(nodes[i]),
                                            "Expected null rack for broker %d",
                                            id);
                        }
                        controller =
                            rd_kafka_DescribeCluster_result_controller(result);
                        if (cases[c].controller_id == 2)
                                TEST_ASSERT(
                                    controller &&
                                        rd_kafka_Node_id(controller) == 2 &&
                                        rd_kafka_Node_rack(controller) &&
                                        !strcmp(rd_kafka_Node_rack(controller),
                                                "rack-2"),
                                    "Incorrect controller");
                        else if (cases[c].controller_id >= 0)
                                TEST_ASSERT(
                                    controller &&
                                        rd_kafka_Node_id(controller) ==
                                            cases[c].controller_id &&
                                        !rd_kafka_Node_host(controller) &&
                                        rd_kafka_Node_port(controller) == 0 &&
                                        !rd_kafka_Node_rack(controller),
                                    "Incorrect controller without a known "
                                    "endpoint");
                        else
                                TEST_ASSERT(!controller,
                                            "Expected no controller node");
                        operations =
                            rd_kafka_DescribeCluster_result_authorized_operations(
                                result, &operation_cnt);
                        if (cases[c].supported &&
                            cases[c].include_authorized_operations)
                                TEST_ASSERT(
                                    operation_cnt == 1 && operations &&
                                        operations[0] ==
                                            RD_KAFKA_ACL_OPERATION_DESCRIBE,
                                    "Incorrect cluster authorized operations");
                        else
                                TEST_ASSERT(
                                    operation_cnt == 0 && !operations,
                                    "Expected omitted authorized operations");
                } else if (cases[c].error ==
                           RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED)
                        TEST_ASSERT(strstr(rd_kafka_event_error_string(event),
                                           "Cluster authorization failed"),
                                    "Broker error message was lost");

                requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
                for (i = 0; i < request_cnt; i++) {
                        int16_t key =
                            rd_kafka_mock_request_api_key(requests[i]);
                        describe_requests += key == RD_KAFKAP_DescribeCluster;
                        metadata_requests += key == RD_KAFKAP_Metadata;
                }
                TEST_ASSERT(describe_requests == (cases[c].supported ? 1 : 0),
                            "Expected %d DescribeCluster requests, got %d",
                            cases[c].supported ? 1 : 0, describe_requests);
                if (!cases[c].supported)
                        TEST_ASSERT(metadata_requests >= 1,
                                    "Missing Metadata fallback request");
                rd_kafka_mock_request_destroy_array(requests, request_cnt);
                rd_kafka_metadata_destroy(metadata);
                rd_kafka_event_destroy(event);
                rd_kafka_queue_destroy(q);
                rd_kafka_destroy(rk);
                test_mock_cluster_destroy(mcluster);
        }
        SUB_TEST_PASS();
}

int main_0193_describe_cluster_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);
        do_test_describe_cluster();
        return 0;
}
