/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * @brief Verify the case where there are no bootstrap servers
 *        and the client is re-bootstrapped after brokers were added
 *        manually.
 */
static void
do_test_rebootstrap_local_no_bootstrap_servers(rd_kafka_type_t rk_type) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        SUB_TEST_QUICK("%s",
                       rk_type == RD_KAFKA_PRODUCER ? "producer" : "consumer");
        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(rk_type, conf);
        rd_kafka_brokers_add(rk, "localhost:9999");

        /* Give it time to trigger ALL_BROKERS_DOWN */
        rd_sleep(1);
        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

#define REBOOTSTRAP_ERROR_CNT 20

static rd_atomic32_t rebootstrap_cnt;

static void rebootstrap_log_cb(const rd_kafka_t *rk,
                               int level,
                               const char *fac,
                               const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence"))
                rd_atomic32_add(&rebootstrap_cnt, 1);
}

/**
 * @brief Matches ApiVersionRequests. If \p opaque is set it is a broker id
 *        and only requests to that broker match.
 */
static rd_bool_t is_ApiVersion_request(rd_kafka_mock_request_t *request,
                                       void *opaque) {
        const int32_t *broker_id = opaque;

        if (rd_kafka_mock_request_api_key(request) != RD_KAFKAP_ApiVersion)
                return rd_false;
        return !broker_id || rd_kafka_mock_request_id(request) == *broker_id;
}

static int32_t new_connection_cnt(rd_kafka_mock_cluster_t *mcluster,
                                  const int32_t *broker_id) {
        return (int32_t)test_mock_get_matching_request_cnt(
            mcluster, is_ApiVersion_request, (void *)broker_id);
}

static int32_t wait_new_connections(rd_kafka_mock_cluster_t *mcluster,
                                    const int32_t *broker_id,
                                    int32_t expected,
                                    int timeout_ms) {
        int64_t abs_timeout = test_clock() + timeout_ms * 1000;
        int32_t cnt;

        while ((cnt = new_connection_cnt(mcluster, broker_id)) < expected &&
               test_clock() < abs_timeout)
                rd_usleep(100 * 1000, 0);
        return cnt;
}

static void wait_connections_settled(rd_kafka_mock_cluster_t *mcluster) {
        int64_t abs_timeout = test_clock() + 10 * 1000 * 1000;
        int32_t prev;
        int32_t cnt = new_connection_cnt(mcluster, NULL);

        do {
                prev = cnt;
                rd_sleep(1);
                cnt = new_connection_cnt(mcluster, NULL);
        } while (cnt != prev && test_clock() < abs_timeout);
        TEST_ASSERT(cnt == prev, "client kept opening connections for 10s");
}

static void wait_rebootstrap(int timeout_ms) {
        int64_t abs_timeout = test_clock() + timeout_ms * 1000;

        while (rd_atomic32_get(&rebootstrap_cnt) < 1 &&
               test_clock() < abs_timeout)
                rd_usleep(100 * 1000, 0);
        TEST_ASSERT(rd_atomic32_get(&rebootstrap_cnt) >= 1,
                    "expected a re-bootstrap sequence to start");
}

/**
 * @brief KIP-1102: a top-level REBOOTSTRAP_REQUIRED Metadata error must make
 *        the client open a new connection to its bootstrap server.
 */
static void do_test_rebootstrap_required_reconnects(rd_kafka_type_t rk_type) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        char *bootstrap_server;
        const int32_t bootstrap_broker_id = 1;
        const char *debug_contexts[]      = {"generic", NULL};
        rd_kafka_conf_t *conf;
        test_conf_log_interceptor_t *log_interceptor;
        rd_kafka_t *rk;
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t err;
        rd_kafka_resp_err_t errors[REBOOTSTRAP_ERROR_CNT];
        int32_t connect_cnt;
        size_t i;

        SUB_TEST_QUICK("%s",
                       rk_type == RD_KAFKA_PRODUCER ? "producer" : "consumer");

        rd_atomic32_init(&rebootstrap_cnt, 0);
        for (i = 0; i < RD_ARRAY_SIZE(errors); i++)
                errors[i] = RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED;

        /* Only the first broker is a bootstrap server. */
        mcluster                       = test_mock_cluster_new(3, &bootstraps);
        bootstrap_server               = rd_strdup(bootstraps);
        *strchr(bootstrap_server, ',') = '\0';

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_server);
        log_interceptor = test_conf_set_log_interceptor(
            conf, rebootstrap_log_cb, debug_contexts);
        rk = test_create_handle(rk_type, conf);

        err = rd_kafka_metadata(rk, 1, NULL, &md, 5000);
        TEST_ASSERT(!err, "metadata() failed: %s", rd_kafka_err2str(err));
        rd_kafka_metadata_destroy(md);

        rd_kafka_mock_start_request_tracking(mcluster);
        wait_connections_settled(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        rd_kafka_mock_push_request_errors_array(mcluster, RD_KAFKAP_Metadata,
                                                RD_ARRAY_SIZE(errors), errors);

        err = rd_kafka_metadata(rk, 1, NULL, &md, 5000);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED,
                    "expected REBOOTSTRAP_REQUIRED, got %s",
                    rd_kafka_err2str(err));
        wait_rebootstrap(5000);

        connect_cnt =
            wait_new_connections(mcluster, &bootstrap_broker_id, 1, 5000);
        TEST_SAY("re-bootstrap sequences: %" PRId32
                 ", new connections to bootstrap server: %" PRId32 "\n",
                 rd_atomic32_get(&rebootstrap_cnt), connect_cnt);
        TEST_ASSERT(connect_cnt >= 1,
                    "client did not reconnect to its bootstrap server "
                    "after REBOOTSTRAP_REQUIRED");

        rd_kafka_mock_clear_request_errors(mcluster, RD_KAFKAP_Metadata);
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_destroy(rk);
        rd_free(log_interceptor);
        rd_free(bootstrap_server);
        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

int main_0152_rebootstrap_local(int argc, char **argv) {

        do_test_rebootstrap_local_no_bootstrap_servers(RD_KAFKA_PRODUCER);
        do_test_rebootstrap_local_no_bootstrap_servers(RD_KAFKA_CONSUMER);

        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_rebootstrap_required_reconnects(RD_KAFKA_PRODUCER);
        do_test_rebootstrap_required_reconnects(RD_KAFKA_CONSUMER);

        return 0;
}
