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

#include "../src/rdkafka_protocol.h"

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

static rd_atomic32_t all_brokers_down_cnt;
static rd_atomic32_t rebootstrap_sequence_cnt;
static rd_atomic32_t connect_attempt_cnt;

/**
 * @brief Error callback counting ERR__ALL_BROKERS_DOWN events.
 */
static void sustained_outage_error_cb(rd_kafka_t *rk,
                                      int err,
                                      const char *reason,
                                      void *opaque) {
        if ((rd_kafka_resp_err_t)err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN)
                rd_atomic32_add(&all_brokers_down_cnt, 1);
        TEST_SAY("error_cb: %s: %s\n",
                 rd_kafka_err2name((rd_kafka_resp_err_t)err), reason);
}

/**
 * @brief Log callback counting re-bootstrap sequences and
 *        connection attempts.
 */
static void sustained_outage_log_cb(const rd_kafka_t *rk,
                                    int level,
                                    const char *fac,
                                    const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence"))
                rd_atomic32_add(&rebootstrap_sequence_cnt, 1);
        if (strstr(buf, "broker in state ") && strstr(buf, "connecting"))
                rd_atomic32_add(&connect_attempt_cnt, 1);
}

/**
 * @brief Make the next \p cnt connections fail during the ApiVersion
 *        handshake: the mock broker closes the connection when returning
 *        RD_KAFKA_RESP_ERR__TRANSPORT.
 */
static void push_ApiVersion_transport_errors(rd_kafka_mock_cluster_t *mcluster,
                                             size_t cnt) {
        rd_kafka_resp_err_t errs[512];
        size_t i;

        TEST_ASSERT(cnt <= RD_ARRAY_SIZE(errs), "cnt too large");
        for (i = 0; i < cnt; i++)
                errs[i] = RD_KAFKA_RESP_ERR__TRANSPORT;
        rd_kafka_mock_push_request_errors_array(mcluster, RD_KAFKAP_ApiVersion,
                                                cnt, errs);
}

/**
 * @brief Create a consumer connecting to \p bootstraps with the outage
 *        counters reset and the counting error and log callbacks installed.
 */
static rd_kafka_t *
sustained_outage_client_new(const char *bootstraps,
                            const char *reconnect_backoff_ms,
                            const char *reconnect_backoff_max_ms,
                            test_conf_log_interceptor_t **interceptorp) {
        rd_kafka_conf_t *conf;
        const char *debug_contexts[2] = {"broker", NULL};

        rd_atomic32_init(&all_brokers_down_cnt, 0);
        rd_atomic32_init(&rebootstrap_sequence_cnt, 0);
        rd_atomic32_init(&connect_attempt_cnt, 0);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "reconnect.backoff.ms", reconnect_backoff_ms);
        test_conf_set(conf, "reconnect.backoff.max.ms",
                      reconnect_backoff_max_ms);
        test_conf_set(conf, "socket.connection.setup.timeout.ms", "2000");
        test_conf_set(conf, "group.id", "0152-outage");
        rd_kafka_conf_set_error_cb(conf, sustained_outage_error_cb);
        *interceptorp = test_conf_set_log_interceptor(
            conf, sustained_outage_log_cb, debug_contexts);

        return test_create_handle(RD_KAFKA_CONSUMER, conf);
}

/**
 * @brief Poll \p rk for \p duration_ms milliseconds.
 */
static void poll_for(rd_kafka_t *rk, int duration_ms) {
        rd_ts_t ts_end = test_clock() + (rd_ts_t)duration_ms * 1000;
        while (test_clock() < ts_end)
                rd_kafka_poll(rk, 100);
}

/**
 * @brief Poll \p rk until \p cnt reaches \p expected,
 *        failing after \p timeout_ms.
 */
static void poll_until_cnt(rd_kafka_t *rk,
                           rd_atomic32_t *cnt,
                           int32_t expected,
                           int timeout_ms,
                           const char *what) {
        rd_ts_t abs_timeout = test_clock() + (rd_ts_t)timeout_ms * 1000;
        while (rd_atomic32_get(cnt) < expected) {
                TEST_ASSERT(test_clock() < abs_timeout,
                            "Timed out waiting for %s to reach %d, got %d",
                            what, expected, rd_atomic32_get(cnt));
                rd_kafka_poll(rk, 100);
        }
}

/**
 * @brief Sustained outage: every connection attempt fails during ApiVersion
 *        negotiation (the peer closes the connection), for longer than
 *        several re-bootstrap cycles.
 *        ERR__ALL_BROKERS_DOWN must be reported only once for the whole
 *        outage while the re-bootstrap sequences keep cycling.
 */
static void do_test_sustained_outage_single_all_brokers_down(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        test_conf_log_interceptor_t *interceptor;
        int32_t errors, rebootstraps;

        SUB_TEST();

        mcluster = test_mock_cluster_new(3, &bootstraps);
        push_ApiVersion_transport_errors(mcluster, 500);

        rk =
            sustained_outage_client_new(bootstraps, "100", "500", &interceptor);

        poll_for(rk, 10000);

        errors       = rd_atomic32_get(&all_brokers_down_cnt);
        rebootstraps = rd_atomic32_get(&rebootstrap_sequence_cnt);
        TEST_SAY("Got %d ALL_BROKERS_DOWN errors, %d re-bootstrap sequences\n",
                 errors, rebootstraps);
        TEST_ASSERT(errors == 1,
                    "Expected exactly 1 ALL_BROKERS_DOWN error "
                    "during a sustained outage, got %d",
                    errors);
        TEST_ASSERT(rebootstraps >= 2,
                    "Expected re-bootstrap to keep cycling "
                    "(>= 2 sequences), got %d",
                    rebootstraps);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        rd_free(interceptor);
        SUB_TEST_PASS();
}

/**
 * @brief ERR__ALL_BROKERS_DOWN is reported once per distinct outage:
 *        after the cluster recovers and all connections are lost again
 *        a second error must be reported, exactly once, too.
 */
static void do_test_second_outage_second_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        test_conf_log_interceptor_t *interceptor;
        const struct rd_kafka_metadata *md;
        rd_ts_t abs_timeout;
        int32_t i, errors;
        rd_bool_t recovered = rd_false;

        SUB_TEST();

        mcluster = test_mock_cluster_new(3, &bootstraps);
        push_ApiVersion_transport_errors(mcluster, 500);

        rk =
            sustained_outage_client_new(bootstraps, "100", "500", &interceptor);

        TEST_SAY("Outage #1: awaiting the first ALL_BROKERS_DOWN\n");
        poll_until_cnt(rk, &all_brokers_down_cnt, 1, 15000,
                       "ALL_BROKERS_DOWN errors");
        poll_for(rk, 3000);
        errors = rd_atomic32_get(&all_brokers_down_cnt);
        TEST_ASSERT(errors == 1,
                    "Expected exactly 1 ALL_BROKERS_DOWN error "
                    "after the first outage, got %d",
                    errors);

        TEST_SAY("Healing the cluster\n");
        rd_kafka_mock_clear_request_errors(mcluster, RD_KAFKAP_ApiVersion);
        abs_timeout = test_clock() + 15 * 1000000;
        while (!recovered) {
                TEST_ASSERT(test_clock() < abs_timeout,
                            "Timed out waiting for cluster recovery");
                if (rd_kafka_metadata(rk, 0, NULL, &md, 1000) ==
                    RD_KAFKA_RESP_ERR_NO_ERROR) {
                        rd_kafka_metadata_destroy(md);
                        recovered = rd_true;
                } else {
                        rd_kafka_poll(rk, 100);
                }
        }

        TEST_SAY("Outage #2: dropping all connections\n");
        push_ApiVersion_transport_errors(mcluster, 500);
        for (i = 1; i <= 3; i++) {
                TEST_CALL_ERR__(rd_kafka_mock_broker_set_down(mcluster, i));
                TEST_CALL_ERR__(rd_kafka_mock_broker_set_up(mcluster, i));
        }

        poll_until_cnt(rk, &all_brokers_down_cnt, 2, 15000,
                       "ALL_BROKERS_DOWN errors");
        poll_for(rk, 3000);
        errors = rd_atomic32_get(&all_brokers_down_cnt);
        TEST_ASSERT(errors == 2,
                    "Expected exactly 2 ALL_BROKERS_DOWN errors "
                    "after the second outage, got %d",
                    errors);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        rd_free(interceptor);
        SUB_TEST_PASS();
}

/**
 * @brief The reconnect backoff must keep growing during a sustained outage
 *        until capped by reconnect.backoff.max.ms, pacing the connection
 *        attempts: neither the re-bootstrap sequences nor the periodic
 *        cluster connection maintenance may reset it.
 *        Each connection attempt fails during ApiVersion negotiation and
 *        doubles the reconnect backoff (once more when the broker address
 *        list is exhausted), so with reconnect.backoff.ms=1000 and
 *        reconnect.backoff.max.ms=5000 only attempts at roughly 0, 1.5-3,
 *        4.5-8 and 8.25-13 s fit in a 10 s window. If the backoff were
 *        reset on each cycle there would be an attempt roughly every
 *        0.5-1 s.
 */
static void do_test_backoff_grows_during_outage(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        test_conf_log_interceptor_t *interceptor;
        int32_t connects, errors;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        push_ApiVersion_transport_errors(mcluster, 500);

        rk = sustained_outage_client_new(bootstraps, "1000", "5000",
                                         &interceptor);

        poll_for(rk, 10000);

        connects = rd_atomic32_get(&connect_attempt_cnt);
        errors   = rd_atomic32_get(&all_brokers_down_cnt);
        TEST_SAY("Got %d connection attempts, %d ALL_BROKERS_DOWN errors\n",
                 connects, errors);
        TEST_ASSERT(connects >= 3 && connects <= 6,
                    "Expected 3..6 connection attempts in 10 s with a "
                    "growing reconnect backoff, got %d",
                    connects);
        TEST_ASSERT(errors == 1,
                    "Expected exactly 1 ALL_BROKERS_DOWN error, got %d",
                    errors);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        rd_free(interceptor);
        SUB_TEST_PASS();
}

int main_0152_rebootstrap_local(int argc, char **argv) {

        do_test_rebootstrap_local_no_bootstrap_servers(RD_KAFKA_PRODUCER);
        do_test_rebootstrap_local_no_bootstrap_servers(RD_KAFKA_CONSUMER);

        if (test_needs_auth()) {
                TEST_SAY(
                    "Skipping mock cluster subtests: "
                    "mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_sustained_outage_single_all_brokers_down();

        do_test_second_outage_second_error();

        do_test_backoff_grows_during_outage();

        return 0;
}
