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
            filter, "client-id", RD_KAFKA_CLIENT_QUOTA_MATCH_DEFAULT, NULL,
            errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for DEFAULT component, got %s",
                    rd_kafka_err2str(err));

        /* ANY match (NULL name) */
        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "ip", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected NO_ERROR for ANY component, got %s",
                    rd_kafka_err2str(err));

        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "bad-exact", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, NULL,
            errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for EXACT without match, got %s",
                    rd_kafka_err2str(err));

        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "bad-any", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, "value", errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for ANY with match, got %s",
                    rd_kafka_err2str(err));

        err = rd_kafka_ClientQuotaFilter_add_component(
            filter, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for duplicate entity type, got %s",
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

        err = rd_kafka_ClientQuotaEntry_add_entity(entry, "user", "bob", errstr,
                                                   sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for duplicate entity type, got %s",
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

        err = rd_kafka_ClientQuotaEntry_add_operation(
            entry, "producer_byte_rate", 2048.0, 0 /*remove*/, errstr,
            sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected INVALID_ARG for duplicate quota key, got %s",
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


static void do_test_AlterClientQuotas_broker(rd_kafka_t *rk,
                                             const char *user,
                                             const char *second_entity_type,
                                             const char *second_entity_name,
                                             const char *key,
                                             double value,
                                             rd_bool_t remove,
                                             rd_bool_t validate_only) {
        rd_kafka_ClientQuotaEntry_t *entry;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_queue_t *queue;
        rd_kafka_event_t *event;
        const rd_kafka_AlterClientQuotas_result_t *result;
        const rd_kafka_ClientQuotaEntry_t **results;
        const rd_kafka_error_t *error;
        rd_kafka_resp_err_t err;
        size_t result_cnt;
        char errstr[512];

        entry = rd_kafka_ClientQuotaEntry_new();
        TEST_CALL_ERR__(rd_kafka_ClientQuotaEntry_add_entity(
            entry, "user", user, errstr, sizeof(errstr)));
        if (second_entity_type)
                TEST_CALL_ERR__(rd_kafka_ClientQuotaEntry_add_entity(
                    entry, second_entity_type, second_entity_name, errstr,
                    sizeof(errstr)));
        TEST_CALL_ERR__(rd_kafka_ClientQuotaEntry_add_operation(
            entry, key, value, remove, errstr, sizeof(errstr)));

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCLIENTQUOTAS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 10 * 1000, errstr, sizeof(errstr)));
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_validate_only(
            options, validate_only, errstr, sizeof(errstr)));

        queue = rd_kafka_queue_new(rk);
        rd_kafka_AlterClientQuotas(rk, &entry, 1, options, queue);
        rd_kafka_ClientQuotaEntry_destroy(entry);
        rd_kafka_AdminOptions_destroy(options);

        event = rd_kafka_queue_poll(queue, 15 * 1000);
        TEST_ASSERT(event, "AlterClientQuotas result event missing");
        TEST_CALL_ERR__(rd_kafka_event_error(event));

        result = rd_kafka_event_AlterClientQuotas_result(event);
        TEST_ASSERT(result, "Expected AlterClientQuotasResult, got %s",
                    rd_kafka_event_name(event));
        results =
            rd_kafka_AlterClientQuotas_result_entries(result, &result_cnt);
        TEST_ASSERT(result_cnt == 1,
                    "Expected one AlterClientQuotas result, got %" PRIusz,
                    result_cnt);
        error = rd_kafka_ClientQuotaEntry_error(results[0]);
        err   = error ? rd_kafka_error_code(error) : RD_KAFKA_RESP_ERR_NO_ERROR;
        TEST_ASSERT(!err, "AlterClientQuotas entry failed: %s",
                    error ? rd_kafka_error_string(error)
                          : rd_kafka_err2str(err));

        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
}


static void
do_test_DescribeClientQuotas_broker(rd_kafka_t *rk,
                                    const char *filter_type,
                                    rd_kafka_ClientQuotaMatchType_t match_type,
                                    const char *match,
                                    rd_bool_t strict,
                                    const char *expected_user,
                                    const char *expected_key,
                                    double expected_value,
                                    rd_bool_t expected_found) {
        rd_kafka_ClientQuotaFilter_t *filter;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_queue_t *queue;
        rd_kafka_event_t *event;
        const rd_kafka_DescribeClientQuotas_result_t *result;
        const rd_kafka_DescribeClientQuotas_result_entry_t **entries;
        size_t entry_cnt, i;
        rd_bool_t found = rd_false;
        int retries     = 5;
        char errstr[512];

retry_describe:
        filter = rd_kafka_ClientQuotaFilter_new(strict);
        TEST_CALL_ERR__(rd_kafka_ClientQuotaFilter_add_component(
            filter, filter_type, match_type, match, errstr, sizeof(errstr)));
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBECLIENTQUOTAS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 10 * 1000, errstr, sizeof(errstr)));

        queue = rd_kafka_queue_new(rk);
        rd_kafka_DescribeClientQuotas(rk, filter, options, queue);
        rd_kafka_ClientQuotaFilter_destroy(filter);
        rd_kafka_AdminOptions_destroy(options);

        event = rd_kafka_queue_poll(queue, 15 * 1000);
        TEST_ASSERT(event, "DescribeClientQuotas result event missing");
        TEST_CALL_ERR__(rd_kafka_event_error(event));
        result = rd_kafka_event_DescribeClientQuotas_result(event);
        TEST_ASSERT(result, "Expected DescribeClientQuotasResult, got %s",
                    rd_kafka_event_name(event));
        entries =
            rd_kafka_DescribeClientQuotas_result_entries(result, &entry_cnt);

        for (i = 0; i < entry_cnt; i++) {
                const rd_kafka_ClientQuotaEntity_t **entities;
                const rd_kafka_ClientQuotaValue_t **values;
                double actual;
                size_t entity_cnt, value_cnt, j;
                rd_bool_t user_matches = rd_false;

                entities = rd_kafka_DescribeClientQuotas_result_entry_entities(
                    entries[i], &entity_cnt);
                for (j = 0; j < entity_cnt; j++) {
                        const char *type =
                            rd_kafka_ClientQuotaEntity_type(entities[j]);
                        const char *name =
                            rd_kafka_ClientQuotaEntity_name(entities[j]);
                        if (!strcmp(type, "user") && name &&
                            !strcmp(name, expected_user))
                                user_matches = rd_true;
                }
                if (!user_matches)
                        continue;

                values = rd_kafka_DescribeClientQuotas_result_entry_values(
                    entries[i], &value_cnt);
                for (j = 0; j < value_cnt; j++) {
                        if (!strcmp(rd_kafka_ClientQuotaValue_key(values[j]),
                                    expected_key)) {
                                actual =
                                    rd_kafka_ClientQuotaValue_value(values[j]);
                                TEST_ASSERT(actual > expected_value - 0.001 &&
                                                actual < expected_value + 0.001,
                                            "Expected %s=%f, got %f",
                                            expected_key, expected_value,
                                            actual);
                                found = rd_true;
                        }
                }
        }

        if (found != expected_found && retries-- > 0) {
                TEST_SAY("Quota update is still propagating; retrying\n");
                rd_kafka_event_destroy(event);
                rd_kafka_queue_destroy(queue);
                rd_usleep(200 * 1000, 0);
                found = rd_false;
                goto retry_describe;
        }

        TEST_ASSERT(found == expected_found,
                    "Expected quota %s for user %s to be %s", expected_key,
                    expected_user, expected_found ? "present" : "absent");
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
}


static void do_test_broker_roundtrip(void) {
        char *user          = rd_strdup(test_mk_topic_name("quota-user", 1));
        const char *brokers = test_getenv("BROKERS", NULL);
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        if (test_broker_version < TEST_BRKVER(2, 6, 0, 0)) {
                TEST_SAY("Skipping ClientQuota broker test for Kafka <2.6\n");
                return;
        }

        SUB_TEST_QUICK("KIP-546 broker roundtrip");
        test_conf_init(&conf, NULL, 30);
        if (brokers)
                test_conf_set(conf, "bootstrap.servers", brokers);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* validate_only must not persist the requested quota. */
        do_test_AlterClientQuotas_broker(rk, user, NULL, NULL,
                                         "producer_byte_rate", 111111.0,
                                         rd_false, rd_true);
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, user, rd_false, user,
            "producer_byte_rate", 111111.0, rd_false);

        do_test_AlterClientQuotas_broker(rk, user, NULL, NULL,
                                         "producer_byte_rate", 222222.0,
                                         rd_false, rd_false);
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, user, rd_false, user,
            "producer_byte_rate", 222222.0, rd_true);
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_ANY, NULL, rd_false, user,
            "producer_byte_rate", 222222.0, rd_true);

        /* A composite entity exercises DEFAULT without changing a cluster-wide
         * default quota. */
        do_test_AlterClientQuotas_broker(rk, user, "client-id", NULL,
                                         "consumer_byte_rate", 333333.0,
                                         rd_false, rd_false);
        do_test_DescribeClientQuotas_broker(
            rk, "client-id", RD_KAFKA_CLIENT_QUOTA_MATCH_DEFAULT, NULL,
            rd_false, user, "consumer_byte_rate", 333333.0, rd_true);

        /* Strict matching excludes the composite user+client-id entity when
         * the filter only specifies user. */
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, user, rd_true, user,
            "consumer_byte_rate", 333333.0, rd_false);

        do_test_AlterClientQuotas_broker(
            rk, user, NULL, NULL, "producer_byte_rate", 0.0, rd_true, rd_false);
        do_test_AlterClientQuotas_broker(rk, user, "client-id", NULL,
                                         "consumer_byte_rate", 0.0, rd_true,
                                         rd_false);
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, user, rd_false, user,
            "producer_byte_rate", 0.0, rd_false);
        do_test_DescribeClientQuotas_broker(
            rk, "user", RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT, user, rd_false, user,
            "consumer_byte_rate", 0.0, rd_false);

        rd_kafka_destroy(rk);
        rd_free(user);
        SUB_TEST_PASS();
}


int main_0154_admin_quota_ut(int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}


int main_0154_admin_quota_broker(int argc, char **argv) {
        do_test_broker_roundtrip();
        return 0;
}
