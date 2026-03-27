/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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

/**
 * @brief Admin API local dry-run unit-tests for DescribeClientQuotas
 *        and AlterClientQuotas.
 */

#define MY_SOCKET_TIMEOUT_MS     100
#define MY_SOCKET_TIMEOUT_MS_STR "100"


static rd_kafka_t *create_quota_admin_client(rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 0);
        /* Remove brokers: this is a local test relying on timeout */
        test_conf_set(conf, "bootstrap.servers", "");
        test_conf_set(conf, "socket.timeout.ms", MY_SOCKET_TIMEOUT_MS_STR);

        rk = rd_kafka_new(cltype, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "kafka_new(%d): %s", cltype, errstr);

        return rk;
}


/**
 * @brief Test ClientQuotaFilter construction and input validation.
 */
static void do_test_ClientQuotaFilter(void) {
        rd_kafka_ClientQuotaFilter_t *filter;
        rd_kafka_resp_err_t err;
        char errstr[512];

        SUB_TEST_QUICK();

        filter = rd_kafka_ClientQuotaFilter_new(0 /*strict*/);
        TEST_ASSERT(filter != NULL, "expected non-NULL filter");

        /* NULL entity_type must be rejected */
        *errstr = '\0';
        err     = rd_kafka_ClientQuotaFilter_add_component(
            filter, NULL, RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for NULL entity_type, got %s",
                    rd_kafka_err2str(err));
        TEST_ASSERT(*errstr != '\0',
                    "expected non-empty errstr for NULL entity_type");

        /* EXACT match with a name */
        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, "alice", errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for EXACT component, got %s",
                    rd_kafka_err2str(err));

        /* DEFAULT match (NULL name) */
        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_DEFAULT, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for DEFAULT component, got %s",
                    rd_kafka_err2str(err));

        /* ANY match (NULL name) */
        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "client-id", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for ANY component, got %s",
                    rd_kafka_err2str(err));

        rd_kafka_ClientQuotaFilter_destroy(filter);

        /* Strict filter */
        filter = rd_kafka_ClientQuotaFilter_new(1 /*strict*/);
        TEST_ASSERT(filter != NULL, "expected non-NULL strict filter");
        rd_kafka_ClientQuotaFilter_destroy(filter);

        /* Empty filter (no components, matches all) */
        filter = rd_kafka_ClientQuotaFilter_new(0 /*strict*/);
        TEST_ASSERT(filter != NULL, "expected non-NULL empty filter");
        rd_kafka_ClientQuotaFilter_destroy(filter);

        SUB_TEST_PASS();
}


/**
 * @brief Test ClientQuotaEntry construction and input validation.
 */
static void do_test_ClientQuotaEntry(void) {
        rd_kafka_ClientQuotaEntry_t *entry;
        rd_kafka_resp_err_t err;
        char errstr[512];

        SUB_TEST_QUICK();

        entry = rd_kafka_ClientQuotaEntry_new();
        TEST_ASSERT(entry != NULL, "expected non-NULL entry");

        /* NULL entity type must be rejected */
        *errstr = '\0';
        err = rd_kafka_ClientQuotaEntry_add_entity(entry, NULL, "user1", errstr,
                                                   sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for NULL entity type, got %s",
                    rd_kafka_err2str(err));
        TEST_ASSERT(*errstr != '\0',
                    "expected non-empty errstr for NULL entity type");

        /* Valid entity with a name */
        err = rd_kafka_ClientQuotaEntry_add_entity(entry, "user", "alice",
                                                   errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for valid entity, got %s",
                    rd_kafka_err2str(err));

        /* Valid entity with NULL name (default entity) */
        err = rd_kafka_ClientQuotaEntry_add_entity(entry, "client-id", NULL,
                                                   errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for entity with NULL name, got %s",
                    rd_kafka_err2str(err));

        /* NULL quota key must be rejected */
        *errstr = '\0';
        err     = rd_kafka_ClientQuotaEntry_add_operation(
            entry, NULL, 1024.0, 0 /*remove*/, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for NULL quota key, got %s",
                    rd_kafka_err2str(err));
        TEST_ASSERT(*errstr != '\0',
                    "expected non-empty errstr for NULL quota key");

        /* Valid set operation */
        err = rd_kafka_ClientQuotaEntry_add_operation(
            entry, "producer_byte_rate", 1024.0, 0 /*remove*/, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for set operation, got %s",
                    rd_kafka_err2str(err));

        /* Valid remove operation */
        err = rd_kafka_ClientQuotaEntry_add_operation(
            entry, "consumer_byte_rate", 0.0, 1 /*remove*/, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for remove operation, got %s",
                    rd_kafka_err2str(err));

        rd_kafka_ClientQuotaEntry_destroy(entry);

        SUB_TEST_PASS();
}


/**
 * @brief Test DescribeClientQuotas local timeout (no broker).
 */
static void do_test_DescribeClientQuotas(const char *what,
                                         rd_kafka_t *rk,
                                         rd_kafka_queue_t *useq,
                                         int with_options) {
        rd_kafka_queue_t *q;
        rd_kafka_ClientQuotaFilter_t *filter;
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeClientQuotas_result_t *res;

        SUB_TEST_QUICK("%s DescribeClientQuotas with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        filter = rd_kafka_ClientQuotaFilter_new(0 /*strict*/);
        err    = rd_kafka_ClientQuotaFilter_add_component(
            filter, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(!err, "add_component: %s", errstr);

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBECLIENTQUOTAS);
                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
        }

        TIMING_START(&timing, "DescribeClientQuotas");
        TEST_SAY("Call DescribeClientQuotas, timeout is %dms\n", exp_timeout);
        rd_kafka_DescribeClientQuotas(rk, filter, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        rd_kafka_ClientQuotaFilter_destroy(filter);

        /* Poll result queue */
        TIMING_START(&timing, "DescribeClientQuotas.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DescribeClientQuotas: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Verify event type */
        res = rd_kafka_event_DescribeClientQuotas_result(rkev);
        TEST_ASSERT(res, "expected DescribeClientQuotas_result, not %s",
                    rd_kafka_event_name(rkev));

        /* Expecting timeout error since there is no broker */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DescribeClientQuotas to return %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        SUB_TEST_PASS();
}


/**
 * @brief Test AlterClientQuotas local timeout (no broker).
 */
static void do_test_AlterClientQuotas(const char *what,
                                      rd_kafka_t *rk,
                                      rd_kafka_queue_t *useq,
                                      int with_options) {
        rd_kafka_queue_t *q;
#define MY_ALTER_QUOTA_ENTRIES_CNT 2
        rd_kafka_ClientQuotaEntry_t *entries[MY_ALTER_QUOTA_ENTRIES_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_AlterClientQuotas_result_t *res;

        SUB_TEST_QUICK("%s AlterClientQuotas with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        /* First entry: set producer_byte_rate for user "alice" */
        entries[0] = rd_kafka_ClientQuotaEntry_new();
        err = rd_kafka_ClientQuotaEntry_add_entity(entries[0], "user", "alice",
                                                   errstr, sizeof(errstr));
        TEST_ASSERT(!err, "add_entity: %s", errstr);
        err = rd_kafka_ClientQuotaEntry_add_operation(
            entries[0], "producer_byte_rate", 1024.0, 0 /*remove*/, errstr,
            sizeof(errstr));
        TEST_ASSERT(!err, "add_operation: %s", errstr);

        /* Second entry: remove consumer_byte_rate for client-id "my-client" */
        entries[1] = rd_kafka_ClientQuotaEntry_new();
        err        = rd_kafka_ClientQuotaEntry_add_entity(
            entries[1], "client-id", "my-client", errstr, sizeof(errstr));
        TEST_ASSERT(!err, "add_entity: %s", errstr);
        err = rd_kafka_ClientQuotaEntry_add_operation(
            entries[1], "consumer_byte_rate", 0.0, 1 /*remove*/, errstr,
            sizeof(errstr));
        TEST_ASSERT(!err, "add_operation: %s", errstr);

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_ALTERCLIENTQUOTAS);
                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
        }

        TIMING_START(&timing, "AlterClientQuotas");
        TEST_SAY("Call AlterClientQuotas, timeout is %dms\n", exp_timeout);
        rd_kafka_AlterClientQuotas(rk, entries, MY_ALTER_QUOTA_ENTRIES_CNT,
                                   options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        rd_kafka_ClientQuotaEntry_destroy(entries[0]);
        rd_kafka_ClientQuotaEntry_destroy(entries[1]);

        /* Poll result queue */
        TIMING_START(&timing, "AlterClientQuotas.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("AlterClientQuotas: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Verify event type */
        res = rd_kafka_event_AlterClientQuotas_result(rkev);
        TEST_ASSERT(res, "expected AlterClientQuotas_result, not %s",
                    rd_kafka_event_name(rkev));

        /* Expecting timeout error since there is no broker */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected AlterClientQuotas to return %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_ALTER_QUOTA_ENTRIES_CNT

        SUB_TEST_PASS();
}


static void do_test_apis(rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        rd_kafka_queue_t *mainq;

        rk    = create_quota_admin_client(cltype);
        mainq = rd_kafka_queue_get_main(rk);

        do_test_ClientQuotaFilter();
        do_test_ClientQuotaEntry();

        do_test_DescribeClientQuotas("temp queue, no options", rk, NULL, 0);
        do_test_DescribeClientQuotas("temp queue, options", rk, NULL, 1);
        do_test_DescribeClientQuotas("main queue, options", rk, mainq, 1);

        do_test_AlterClientQuotas("temp queue, no options", rk, NULL, 0);
        do_test_AlterClientQuotas("temp queue, options", rk, NULL, 1);
        do_test_AlterClientQuotas("main queue, options", rk, mainq, 1);

        rd_kafka_queue_destroy(mainq);
        rd_kafka_destroy(rk);
}


int main_0154_admin_quota_ut(int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}
