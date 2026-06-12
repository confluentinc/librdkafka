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

/**
 * @name ListTransactions Admin API tests (KIP-664)
 *
 * Requires Kafka 3.6+ for ListTransactions v2.
 */


/**
 * @brief Helper function to call ListTransactions and verify results.
 *
 * @param rk Client instance
 * @param options Admin options (or NULL)
 * @param exp_txnid Expected transactional ID to find (or NULL to skip check)
 * @param exp_found Expected number of transactions to find matching exp_txnid
 * @param exp_state Expected transaction state (or NULL to skip check)
 */
static void do_test_ListTransactions_helper(rd_kafka_t *rk,
                                            rd_kafka_AdminOptions_t *options,
                                            const char *exp_txnid,
                                            size_t exp_found,
                                            const char *exp_state) {
        rd_kafka_queue_t *q;
        rd_kafka_event_t *rkev;
        const rd_kafka_ListTransactions_result_t *result;
        const rd_kafka_TransactionListing_t **transactions;
        const rd_kafka_error_t **errors;
        size_t txn_cnt, error_cnt;
        size_t found = 0;
        size_t i;

        q = rd_kafka_queue_new(rk);

        TEST_SAY("Calling ListTransactions\n");
        rd_kafka_ListTransactions(rk, options, q);

        /* Poll for result */
        rkev = rd_kafka_queue_poll(q, tmout_multip(30 * 1000));
        TEST_ASSERT(rkev != NULL, "Expected ListTransactions result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("ListTransactions error: %s\n",
                         rd_kafka_event_error_string(rkev));
        }

        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_LISTTRANSACTIONS_RESULT,
                    "Expected LISTTRANSACTIONS_RESULT, got %s",
                    rd_kafka_event_name(rkev));

        result = rd_kafka_event_ListTransactions_result(rkev);
        TEST_ASSERT(result != NULL, "Expected ListTransactions result");

        transactions =
            rd_kafka_ListTransactions_result_transactions(result, &txn_cnt);
        errors = rd_kafka_ListTransactions_result_errors(result, &error_cnt);

        TEST_SAY("ListTransactions returned %zu transactions, %zu errors\n",
                 txn_cnt, error_cnt);

        /* Log any errors */
        for (i = 0; i < error_cnt; i++) {
                TEST_SAY("  Error[%zu]: %s\n", i,
                         rd_kafka_error_string(errors[i]));
        }

        /* Log and count transactions */
        for (i = 0; i < txn_cnt; i++) {
                const rd_kafka_TransactionListing_t *txn = transactions[i];
                const char *txnid =
                    rd_kafka_TransactionListing_transactional_id(txn);
                int64_t producer_id =
                    rd_kafka_TransactionListing_producer_id(txn);
                const char *state =
                    rd_kafka_TransactionListing_transaction_state(txn);

                TEST_SAY("  Transaction[%zu]: txnid=%s, producer_id=%" PRId64
                         ", state=%s\n",
                         i, txnid, producer_id, state);

                if (exp_txnid && strcmp(txnid, exp_txnid) == 0) {
                        found++;
                        if (exp_state) {
                                TEST_ASSERT(strcmp(state, exp_state) == 0,
                                            "Expected state %s, got %s",
                                            exp_state, state);
                        }
                }
        }

        if (exp_txnid) {
                TEST_ASSERT(found == exp_found,
                            "Expected to find %zu transactions with "
                            "txnid=%s, found %zu",
                            exp_found, exp_txnid, found);
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_queue_destroy(q);
}


/**
 * @brief Test ListTransactions with an ongoing transaction.
 */
static void do_test_list_ongoing_transaction(void) {
        char *topic;
        char *txnid;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_error_t *error;

        SUB_TEST();

        topic = rd_strdup(test_mk_topic_name("0154_list_txn_topic", 1));
        txnid = rd_strdup(test_mk_topic_name("0154_list_txn", 1));

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        test_conf_set(conf, "transactional.id", txnid);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create topic for the test */
        test_create_topic(rk, topic, 4, 1);

        /* Initialize transactions */
        TEST_SAY("Initializing transactions with txnid=%s\n", txnid);
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, 30 * 1000));

        /* Begin a transaction */
        TEST_SAY("Beginning transaction\n");
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

        /* Produce a message to make the transaction "ongoing" */
        TEST_SAY("Producing message in transaction\n");
        test_produce_msgs2(rk, topic, test_id_generate(), 0, 0, 1, NULL, 0);
        rd_kafka_flush(rk, 10000);

        /* Now list transactions - should find our ongoing transaction */
        TEST_SAY("Listing transactions (expecting to find %s)\n", txnid);
        {
                /* Use shorter timeout to handle unreachable brokers */
                char errstr[512];
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, 10000, errstr, sizeof(errstr)));
                do_test_ListTransactions_helper(rk, options, txnid, 1,
                                                "Ongoing");
                rd_kafka_AdminOptions_destroy(options);
        }

        /* Test state filtering - filter for Ongoing state */
        TEST_SAY("Testing state filter for 'Ongoing'\n");
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        {
                char errstr[512];
                const char *states[] = {"Ongoing"};
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, 10000, errstr, sizeof(errstr)));
                error = rd_kafka_AdminOptions_set_match_transaction_states(
                    options, states, 1);
                TEST_ASSERT(!error, "Failed to set state filter: %s",
                            error ? rd_kafka_error_string(error) : "");
        }
        do_test_ListTransactions_helper(rk, options, txnid, 1, "Ongoing");
        rd_kafka_AdminOptions_destroy(options);

        /* Test state filtering - filter for non-matching state */
        TEST_SAY(
            "Testing state filter for 'PrepareCommit' (should not find)\n");
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        {
                char errstr[512];
                const char *states[] = {"PrepareCommit"};
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, 10000, errstr, sizeof(errstr)));
                error = rd_kafka_AdminOptions_set_match_transaction_states(
                    options, states, 1);
                TEST_ASSERT(!error, "Failed to set state filter: %s",
                            error ? rd_kafka_error_string(error) : "");
        }
        do_test_ListTransactions_helper(rk, options, txnid, 0, NULL);
        rd_kafka_AdminOptions_destroy(options);

        /* Commit the transaction */
        TEST_SAY("Committing transaction\n");
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));

        /* After commit, transaction should no longer be ongoing */
        TEST_SAY(
            "Listing transactions after commit (should not find ongoing)\n");
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        {
                char errstr[512];
                const char *states[] = {"Ongoing"};
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, 10000, errstr, sizeof(errstr)));
                error = rd_kafka_AdminOptions_set_match_transaction_states(
                    options, states, 1);
                TEST_ASSERT(!error, "Failed to set state filter: %s",
                            error ? rd_kafka_error_string(error) : "");
        }
        do_test_ListTransactions_helper(rk, options, txnid, 0, NULL);
        rd_kafka_AdminOptions_destroy(options);

        rd_kafka_destroy(rk);

        rd_free(topic);
        rd_free(txnid);

        SUB_TEST_PASS();
}


/**
 * @brief Helper to check if ListTransactions result contains an unsupported
 *        feature error (expected when broker doesn't support required API
 *        version).
 *
 * @returns rd_true if unsupported feature error was found, rd_false otherwise.
 */
static rd_bool_t
do_test_ListTransactions_check_unsupported(rd_kafka_t *rk,
                                           rd_kafka_AdminOptions_t *options) {
        rd_kafka_queue_t *q;
        rd_kafka_event_t *rkev;
        const rd_kafka_ListTransactions_result_t *result;
        const rd_kafka_error_t **errors;
        size_t error_cnt;
        size_t i;
        rd_bool_t unsupported = rd_false;

        q = rd_kafka_queue_new(rk);

        rd_kafka_ListTransactions(rk, options, q);

        rkev = rd_kafka_queue_poll(q, tmout_multip(30 * 1000));
        TEST_ASSERT(rkev != NULL, "Expected ListTransactions result");
        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_LISTTRANSACTIONS_RESULT,
                    "Expected LISTTRANSACTIONS_RESULT, got %s",
                    rd_kafka_event_name(rkev));

        result = rd_kafka_event_ListTransactions_result(rkev);
        errors = rd_kafka_ListTransactions_result_errors(result, &error_cnt);

        for (i = 0; i < error_cnt; i++) {
                if (rd_kafka_error_code(errors[i]) ==
                    RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE) {
                        TEST_SAY("Got expected unsupported feature error: %s\n",
                                 rd_kafka_error_string(errors[i]));
                        unsupported = rd_true;
                }
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_queue_destroy(q);

        return unsupported;
}


/**
 * @brief Test ListTransactions with producer ID filter (API v0+).
 */
static void do_test_producer_id_filter(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_error_t *error;
        char errstr[512];
        int64_t producer_ids[] = {12345, 67890};

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 10000, errstr, sizeof(errstr)));

        /* Set producer ID filter - should work with API v0+ */
        error = rd_kafka_AdminOptions_set_match_producer_ids(options,
                                                             producer_ids, 2);
        TEST_ASSERT(!error, "Failed to set producer ID filter: %s",
                    error ? rd_kafka_error_string(error) : "");

        /* Call ListTransactions - should succeed (returns empty for these
         * IDs) */
        TEST_SAY("Testing producer ID filter with IDs 12345, 67890\n");
        do_test_ListTransactions_helper(rk, options, NULL, 0, NULL);

        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test ListTransactions with duration filter (API v1+).
 *
 * This test requires Kafka 3.6.0+ which supports ListTransactions v1.
 */
static void do_test_duration_filter(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_error_t *error;
        char errstr[512];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 10000, errstr, sizeof(errstr)));

        /* Set duration filter - requires API v1+ */
        error = rd_kafka_AdminOptions_set_transaction_duration_filter(options,
                                                                      60000);
        TEST_ASSERT(!error, "Failed to set duration filter: %s",
                    error ? rd_kafka_error_string(error) : "");

        TEST_SAY("Testing duration filter (60000ms) - requires API v1+\n");

        /* Call ListTransactions - may fail if broker doesn't support v1 */
        if (do_test_ListTransactions_check_unsupported(rk, options)) {
                TEST_SAY(
                    "Duration filter not supported by broker (requires v1+)\n");
        } else {
                TEST_SAY("Duration filter test passed\n");
        }

        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test ListTransactions with transactional ID pattern filter (API v2+).
 *
 * This test requires a Kafka version that supports ListTransactions v2.
 */
static void do_test_txnid_pattern_filter(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_error_t *error;
        char errstr[512];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 10000, errstr, sizeof(errstr)));

        /* Set transactional ID pattern filter - requires API v2+ */
        error = rd_kafka_AdminOptions_set_transactional_id_pattern(
            options, "rdkafkatest_.*");
        TEST_ASSERT(!error, "Failed to set txnid pattern: %s",
                    error ? rd_kafka_error_string(error) : "");

        TEST_SAY(
            "Testing transactional ID pattern filter "
            "(rdkafkatest_.*) - requires API v2+\n");

        /* Call ListTransactions - may fail if broker doesn't support v2 */
        if (do_test_ListTransactions_check_unsupported(rk, options)) {
                TEST_SAY(
                    "Transactional ID pattern filter not supported by "
                    "broker (requires v2+)\n");
        } else {
                TEST_SAY("Transactional ID pattern filter test passed\n");
        }

        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test ListTransactions with mock cluster.
 *
 * This test verifies the protocol encoding/decoding without
 * requiring a real Kafka cluster.
 */
static void do_test_list_transactions_mock(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_AdminOptions_t *options;
        char errstr[512];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create options with short timeout */
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        /* Call ListTransactions - mock returns empty list */
        TEST_SAY("Calling ListTransactions on mock cluster\n");
        do_test_ListTransactions_helper(rk, options, NULL, 0, NULL);

        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0154_list_transactions(int argc, char **argv) {
        /* Always run mock test - doesn't require real cluster */
        do_test_list_transactions_mock();

        /* Real cluster tests require Kafka 3.6.0+ */
        if (test_broker_version < TEST_BRKVER(3, 6, 0, 0)) {
                TEST_SKIP(
                    "Real cluster tests require Kafka 3.6.0+ (skipping)\n");
                return 0;
        }

        do_test_list_ongoing_transaction();

        /* Test individual filter types */
        do_test_producer_id_filter();   /* API v0+ */
        do_test_duration_filter();      /* API v1+ */
        do_test_txnid_pattern_filter(); /* API v2+ */

        return 0;
}
