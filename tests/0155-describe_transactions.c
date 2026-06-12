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
 * @name DescribeTransactions Admin API tests (KIP-664)
 *
 * Requires Kafka 3.0+ for DescribeTransactions.
 */


/**
 * @brief Helper function to call DescribeTransactions and return results.
 *
 * @param rk Client instance
 * @param transactional_ids Array of transactional IDs to describe
 * @param transactional_ids_cnt Count of transactional IDs
 * @param timeout_ms Request timeout in milliseconds
 * @param result_cnt Output: number of results
 *
 * @returns Array of transaction descriptions (caller must destroy event)
 */
static const rd_kafka_TransactionDescription_t **
do_test_DescribeTransactions(rd_kafka_t *rk,
                             const char **transactional_ids,
                             size_t transactional_ids_cnt,
                             int timeout_ms,
                             rd_kafka_event_t **eventp,
                             size_t *result_cnt) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeTransactions_result_t *result;
        const rd_kafka_TransactionDescription_t **transactions;
        char errstr[512];

        q = rd_kafka_queue_new(rk);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBETRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, timeout_ms, errstr, sizeof(errstr)));

        TEST_SAY("Calling DescribeTransactions for %zu transactional ID(s)\n",
                 transactional_ids_cnt);
        rd_kafka_DescribeTransactions(rk, transactional_ids,
                                      transactional_ids_cnt, options, q);
        rd_kafka_AdminOptions_destroy(options);

        /* Poll for result */
        rkev = rd_kafka_queue_poll(q, tmout_multip(timeout_ms + 10000));
        TEST_ASSERT(rkev != NULL, "Expected DescribeTransactions result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("DescribeTransactions error: %s\n",
                         rd_kafka_event_error_string(rkev));
        }

        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_DESCRIBETRANSACTIONS_RESULT,
                    "Expected DESCRIBETRANSACTIONS_RESULT, got %s",
                    rd_kafka_event_name(rkev));

        result = rd_kafka_event_DescribeTransactions_result(rkev);
        TEST_ASSERT(result != NULL, "Expected DescribeTransactions result");

        transactions = rd_kafka_DescribeTransactions_result_transactions(
            result, result_cnt);

        rd_kafka_queue_destroy(q);

        *eventp = rkev;
        return transactions;
}


/**
 * @brief Test describing an ongoing transaction.
 */
static void do_test_describe_ongoing_transaction(void) {
        char *topic;
        char *txnid;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_event_t *rkev = NULL;
        const rd_kafka_TransactionDescription_t **transactions;
        size_t txn_cnt;
        const char *transactional_ids[1];
        const rd_kafka_TransactionDescription_t *txn;
        const rd_kafka_error_t *error;
        const rd_kafka_topic_partition_list_t *topics;

        SUB_TEST();

        topic = rd_strdup(test_mk_topic_name("0155_desc_txn_topic", 1));
        txnid = rd_strdup(test_mk_topic_name("0155_desc_txn", 1));

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

        /* Now describe the transaction */
        TEST_SAY("Describing transaction %s\n", txnid);
        transactional_ids[0] = txnid;
        transactions = do_test_DescribeTransactions(rk, transactional_ids, 1,
                                                    10000, &rkev, &txn_cnt);

        TEST_ASSERT(txn_cnt == 1, "Expected 1 transaction result, got %zu",
                    txn_cnt);

        txn = transactions[0];

        /* Check transactional ID */
        TEST_ASSERT(
            strcmp(rd_kafka_TransactionDescription_transactional_id(txn),
                   txnid) == 0,
            "Expected transactional_id=%s, got %s", txnid,
            rd_kafka_TransactionDescription_transactional_id(txn));

        /* Check for errors */
        error = rd_kafka_TransactionDescription_error(txn);
        TEST_ASSERT(error == NULL, "Expected no error, got %s",
                    error ? rd_kafka_error_string(error) : "(null)");

        /* Check state is Ongoing */
        TEST_SAY("Transaction state: %s\n",
                 rd_kafka_TransactionDescription_transaction_state(txn));
        TEST_ASSERT(
            strcmp(rd_kafka_TransactionDescription_transaction_state(txn),
                   "Ongoing") == 0,
            "Expected state=Ongoing, got %s",
            rd_kafka_TransactionDescription_transaction_state(txn));

        /* Check producer ID is valid */
        TEST_SAY("Producer ID: %" PRId64 "\n",
                 rd_kafka_TransactionDescription_producer_id(txn));
        TEST_ASSERT(rd_kafka_TransactionDescription_producer_id(txn) >= 0,
                    "Expected valid producer ID");

        /* Check producer epoch */
        TEST_SAY("Producer epoch: %d\n",
                 rd_kafka_TransactionDescription_producer_epoch(txn));

        /* Check timeout */
        TEST_SAY("Transaction timeout: %" PRId32 " ms\n",
                 rd_kafka_TransactionDescription_transaction_timeout_ms(txn));
        TEST_ASSERT(
            rd_kafka_TransactionDescription_transaction_timeout_ms(txn) > 0,
            "Expected positive timeout");

        /* Check start time */
        TEST_SAY(
            "Transaction start time: %" PRId64 " ms\n",
            rd_kafka_TransactionDescription_transaction_start_time_ms(txn));
        TEST_ASSERT(
            rd_kafka_TransactionDescription_transaction_start_time_ms(txn) > 0,
            "Expected positive start time for ongoing transaction");

        /* Check topics/partitions */
        topics = rd_kafka_TransactionDescription_topics(txn);
        TEST_SAY("Topics/partitions count: %d\n", topics ? topics->cnt : 0);
        TEST_ASSERT(topics != NULL && topics->cnt > 0,
                    "Expected topics/partitions for ongoing transaction");

        rd_kafka_event_destroy(rkev);

        /* Commit the transaction */
        TEST_SAY("Committing transaction\n");
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));

        rd_kafka_destroy(rk);

        rd_free(topic);
        rd_free(txnid);

        SUB_TEST_PASS();
}


/**
 * @brief Test describing a non-existent transactional ID.
 */
static void do_test_describe_nonexistent_txnid(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_event_t *rkev = NULL;
        const rd_kafka_TransactionDescription_t **transactions;
        size_t txn_cnt;
        const char *transactional_ids[1];
        const rd_kafka_TransactionDescription_t *txn;
        const rd_kafka_error_t *error;

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Try to describe a non-existent transactional ID */
        transactional_ids[0] = "non-existent-txnid-12345";
        TEST_SAY("Describing non-existent transactional ID: %s\n",
                 transactional_ids[0]);

        transactions = do_test_DescribeTransactions(rk, transactional_ids, 1,
                                                    10000, &rkev, &txn_cnt);

        TEST_ASSERT(txn_cnt == 1, "Expected 1 transaction result, got %zu",
                    txn_cnt);

        txn = transactions[0];

        /* Check transactional ID */
        TEST_ASSERT(
            strcmp(rd_kafka_TransactionDescription_transactional_id(txn),
                   transactional_ids[0]) == 0,
            "Expected transactional_id=%s, got %s", transactional_ids[0],
            rd_kafka_TransactionDescription_transactional_id(txn));

        /* Check for error - should be TRANSACTIONAL_ID_NOT_FOUND or similar */
        error = rd_kafka_TransactionDescription_error(txn);
        TEST_SAY("Error for non-existent txnid: %s (%d)\n",
                 error ? rd_kafka_error_string(error) : "(null)",
                 error ? rd_kafka_error_code(error) : 0);
        TEST_ASSERT(error != NULL,
                    "Expected error for non-existent transactional ID");

        rd_kafka_event_destroy(rkev);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test describing multiple transactional IDs.
 */
static void do_test_describe_multiple_txnids(void) {
        char *topic;
        char *txnid1, *txnid2;
        rd_kafka_t *rk1, *rk2;
        rd_kafka_conf_t *conf1, *conf2;
        rd_kafka_event_t *rkev = NULL;
        const rd_kafka_TransactionDescription_t **transactions;
        size_t txn_cnt;
        const char *transactional_ids[2];
        size_t i;
        rd_bool_t found1 = rd_false, found2 = rd_false;

        SUB_TEST();

        topic  = rd_strdup(test_mk_topic_name("0155_desc_multi_topic", 1));
        txnid1 = rd_strdup(test_mk_topic_name("0155_desc_multi_txn1", 1));
        txnid2 = rd_strdup(test_mk_topic_name("0155_desc_multi_txn2", 1));

        /* Create first producer */
        test_conf_init(&conf1, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf1, test_dr_msg_cb);
        test_conf_set(conf1, "transactional.id", txnid1);
        rk1 = test_create_handle(RD_KAFKA_PRODUCER, conf1);

        /* Create second producer */
        test_conf_init(&conf2, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf2, test_dr_msg_cb);
        test_conf_set(conf2, "transactional.id", txnid2);
        rk2 = test_create_handle(RD_KAFKA_PRODUCER, conf2);

        /* Create topic */
        test_create_topic(rk1, topic, 4, 1);

        /* Initialize and begin transactions for both */
        TEST_SAY("Initializing transactions for %s and %s\n", txnid1, txnid2);
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk1, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk2, 30 * 1000));

        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk1));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk2));

        /* Produce messages */
        test_produce_msgs2(rk1, topic, test_id_generate(), 0, 0, 1, NULL, 0);
        test_produce_msgs2(rk2, topic, test_id_generate(), 0, 0, 1, NULL, 0);
        rd_kafka_flush(rk1, 10000);
        rd_kafka_flush(rk2, 10000);

        /* Describe both transactions */
        transactional_ids[0] = txnid1;
        transactional_ids[1] = txnid2;
        TEST_SAY("Describing transactions: %s, %s\n", txnid1, txnid2);

        transactions = do_test_DescribeTransactions(rk1, transactional_ids, 2,
                                                    10000, &rkev, &txn_cnt);

        TEST_ASSERT(txn_cnt == 2, "Expected 2 transaction results, got %zu",
                    txn_cnt);

        /* Verify both transactions are found */
        for (i = 0; i < txn_cnt; i++) {
                const rd_kafka_TransactionDescription_t *txn = transactions[i];
                const char *tid =
                    rd_kafka_TransactionDescription_transactional_id(txn);
                const rd_kafka_error_t *error =
                    rd_kafka_TransactionDescription_error(txn);

                TEST_SAY("  [%zu] transactional_id=%s, state=%s, error=%s\n", i,
                         tid,
                         rd_kafka_TransactionDescription_transaction_state(txn),
                         error ? rd_kafka_error_string(error) : "(none)");

                if (strcmp(tid, txnid1) == 0) {
                        found1 = rd_true;
                        TEST_ASSERT(error == NULL, "Expected no error for %s",
                                    txnid1);
                        TEST_ASSERT(
                            strcmp(
                                rd_kafka_TransactionDescription_transaction_state(
                                    txn),
                                "Ongoing") == 0,
                            "Expected state=Ongoing for %s", txnid1);
                } else if (strcmp(tid, txnid2) == 0) {
                        found2 = rd_true;
                        TEST_ASSERT(error == NULL, "Expected no error for %s",
                                    txnid2);
                        TEST_ASSERT(
                            strcmp(
                                rd_kafka_TransactionDescription_transaction_state(
                                    txn),
                                "Ongoing") == 0,
                            "Expected state=Ongoing for %s", txnid2);
                }
        }

        TEST_ASSERT(found1 && found2,
                    "Expected to find both transactions (found1=%d, found2=%d)",
                    found1, found2);

        rd_kafka_event_destroy(rkev);

        /* Commit both transactions */
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk1, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk2, 30 * 1000));

        rd_kafka_destroy(rk1);
        rd_kafka_destroy(rk2);

        rd_free(topic);
        rd_free(txnid1);
        rd_free(txnid2);

        SUB_TEST_PASS();
}


/**
 * @brief Test DescribeTransactions with mock cluster.
 */
static void do_test_describe_transactions_mock(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_event_t *rkev = NULL;
        const rd_kafka_TransactionDescription_t **transactions;
        size_t txn_cnt;
        const char *transactional_ids[1];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Describe a non-existent transaction on mock cluster */
        transactional_ids[0] = "mock-test-txnid";
        TEST_SAY("Calling DescribeTransactions on mock cluster for %s\n",
                 transactional_ids[0]);

        transactions = do_test_DescribeTransactions(rk, transactional_ids, 1,
                                                    5000, &rkev, &txn_cnt);

        /* Mock cluster should return the result (possibly with error) */
        TEST_SAY("DescribeTransactions on mock returned %zu results\n",
                 txn_cnt);

        if (txn_cnt > 0) {
                const rd_kafka_TransactionDescription_t *txn = transactions[0];
                const rd_kafka_error_t *error =
                    rd_kafka_TransactionDescription_error(txn);
                TEST_SAY("  transactional_id=%s, error=%s\n",
                         rd_kafka_TransactionDescription_transactional_id(txn),
                         error ? rd_kafka_error_string(error) : "(none)");
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Test input validation for DescribeTransactions.
 */
static void do_test_describe_transactions_input_validation(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        char errstr[512];
        const char *transactional_ids[2];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        q  = rd_kafka_queue_new(rk);

        /* Test with empty transactional ID */
        TEST_SAY("Testing with empty transactional ID\n");
        transactional_ids[0] = "";
        options              = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBETRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_DescribeTransactions(rk, transactional_ids, 1, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("Empty transactional ID error (expected): %s\n",
                         rd_kafka_event_error_string(rkev));
        } else {
                /* If no top-level error, the per-txn result should
                 * contain an error for the empty transactional ID. */
                const rd_kafka_DescribeTransactions_result_t *result =
                    rd_kafka_event_DescribeTransactions_result(rkev);
                size_t cnt;
                const rd_kafka_TransactionDescription_t **txns =
                    rd_kafka_DescribeTransactions_result_transactions(result,
                                                                      &cnt);
                TEST_ASSERT(cnt > 0,
                            "Expected at least one transaction result "
                            "for empty transactional ID");
                if (cnt > 0) {
                        const rd_kafka_error_t *error =
                            rd_kafka_TransactionDescription_error(txns[0]);
                        TEST_ASSERT(error != NULL,
                                    "Expected per-transaction error for "
                                    "empty transactional ID, got success");
                        TEST_SAY("Empty txnid per-transaction error: %s\n",
                                 rd_kafka_error_string(error));
                }
        }
        rd_kafka_event_destroy(rkev);

        /* Test with NULL transactional ID in array */
        TEST_SAY("Testing with NULL transactional ID in array\n");
        transactional_ids[0] = "valid-txnid";
        transactional_ids[1] = NULL;
        options              = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBETRANSACTIONS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_DescribeTransactions(rk, transactional_ids, 2, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        TEST_ASSERT(rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error for NULL transactional ID, got success");
        TEST_SAY("NULL transactional ID error (expected): %s\n",
                 rd_kafka_event_error_string(rkev));
        rd_kafka_event_destroy(rkev);

        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0155_describe_transactions(int argc, char **argv) {
        /* Always run mock test - doesn't require real cluster */
        do_test_describe_transactions_mock();

        /* Input validation test */
        do_test_describe_transactions_input_validation();

        /* Real cluster tests require Kafka 3.0.0+ */
        if (test_broker_version < TEST_BRKVER(3, 0, 0, 0)) {
                TEST_SKIP(
                    "Real cluster tests require Kafka 3.0.0+ (skipping)\n");
                return 0;
        }

        do_test_describe_ongoing_transaction();
        do_test_describe_nonexistent_txnid();
        do_test_describe_multiple_txnids();

        return 0;
}
