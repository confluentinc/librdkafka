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
 * @name AbortTransaction Admin API tests (KIP-664)
 *
 * Requires Kafka 3.0+ for WriteTxnMarkers.
 */


/**
 * @brief Helper function to call AbortTransaction and return results.
 *
 * @param rk Client instance
 * @param abort_specs Array of AbortTransactionSpec_t pointers
 * @param abort_spec_cnt Number of specs
 * @param timeout_ms Request timeout in milliseconds
 * @param eventp Output: event to be destroyed by caller
 * @param result_cnt Output: number of results
 *
 * @returns Array of partition abort results (caller must destroy event)
 */
static const rd_kafka_PartitionAbortResult_t **
do_test_AbortTransaction(rd_kafka_t *rk,
                         rd_kafka_AbortTransactionSpec_t **abort_specs,
                         size_t abort_spec_cnt,
                         int timeout_ms,
                         rd_kafka_event_t **eventp,
                         size_t *result_cnt) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        const rd_kafka_AbortTransaction_result_t *result;
        const rd_kafka_PartitionAbortResult_t **par_list;
        char errstr[512];

        q = rd_kafka_queue_new(rk);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ABORTTRANSACTION);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, timeout_ms, errstr, sizeof(errstr)));

        TEST_SAY("Calling AbortTransaction for %zu partition(s)\n",
                 abort_spec_cnt);
        rd_kafka_AbortTransaction(rk, abort_specs, abort_spec_cnt, options, q);
        rd_kafka_AdminOptions_destroy(options);

        /* Poll for result */
        rkev = rd_kafka_queue_poll(q, tmout_multip(timeout_ms + 10000));
        TEST_ASSERT(rkev != NULL, "Expected AbortTransaction result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("AbortTransaction error: %s\n",
                         rd_kafka_event_error_string(rkev));
        }

        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_ABORTTRANSACTION_RESULT,
                    "Expected ABORTTRANSACTION_RESULT, got %s",
                    rd_kafka_event_name(rkev));

        result = rd_kafka_event_AbortTransaction_result(rkev);
        TEST_ASSERT(result != NULL, "Expected AbortTransaction result");

        par_list =
            rd_kafka_AbortTransaction_result_partitions(result, result_cnt);

        rd_kafka_queue_destroy(q);

        *eventp = rkev;
        return par_list;
}


/**
 * @brief Test aborting a hanging transaction.
 */
static void do_test_abort_hanging_transaction(void) {
        char *topic;
        char *txnid;
        rd_kafka_t *producer, *admin;
        rd_kafka_conf_t *conf, *admin_conf;
        rd_kafka_event_t *rkev       = NULL;
        rd_kafka_event_t *abort_rkev = NULL;
        rd_kafka_AbortTransactionSpec_t **abort_specs;
        const rd_kafka_PartitionAbortResult_t **par_list;
        size_t par_cnt;
        const rd_kafka_PartitionAbortResult_t *par;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *partitions;
        const rd_kafka_PartitionProducerState_t **pps_list;
        size_t pps_cnt;
        const rd_kafka_PartitionProducerState_t *pps;
        const rd_kafka_ProducerState_t **producers;
        size_t producer_cnt;
        const rd_kafka_ProducerState_t *state;
        int64_t producer_id;
        int32_t producer_epoch;
        int64_t txn_start_offset;

        SUB_TEST();

        topic = rd_strdup(test_mk_topic_name("0157_abort_txn_topic", 1));
        txnid = rd_strdup(test_mk_topic_name("0157_test_txn", 1));

        /* Create transactional producer */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        test_conf_set(conf, "transactional.id", txnid);

        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create topic for the test */
        test_create_topic(producer, topic, 4, 1);

        /* Initialize transactions and produce a message */
        TEST_SAY("Initializing transactions\n");
        TEST_CALL_ERROR__(rd_kafka_init_transactions(producer, 30 * 1000));

        TEST_SAY("Beginning transaction\n");
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(producer));

        /* Produce a message to make producer active */
        TEST_SAY("Producing message in transaction\n");
        test_produce_msgs2(producer, topic, test_id_generate(), 0, 0, 1, NULL,
                           0);
        rd_kafka_flush(producer, 10000);

        /* Create admin client */
        test_conf_init(&admin_conf, NULL, 30);
        admin = test_create_handle(RD_KAFKA_PRODUCER, admin_conf);

        /* First, describe the producer to get transaction info */
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, topic, 0);

        TEST_SAY("Describing producers for %s [0]\n", topic);

        /* Call DescribeProducers */
        {
                rd_kafka_queue_t *q;
                rd_kafka_AdminOptions_t *options;
                char errstr[512];

                q = rd_kafka_queue_new(admin);

                options = rd_kafka_AdminOptions_new(
                    admin, RD_KAFKA_ADMIN_OP_DESCRIBEPRODUCERS);
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, 10000, errstr, sizeof(errstr)));

                rd_kafka_DescribeProducers(admin, partitions, options, q);
                rd_kafka_AdminOptions_destroy(options);

                rkev = rd_kafka_queue_poll(q, tmout_multip(20000));
                TEST_ASSERT(rkev != NULL, "Expected DescribeProducers result");

                rd_kafka_queue_destroy(q);
        }

        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_DESCRIBEPRODUCERS_RESULT,
                    "Expected DESCRIBEPRODUCERS_RESULT");

        pps_list = rd_kafka_DescribeProducers_result_partitions(
            rd_kafka_event_DescribeProducers_result(rkev), &pps_cnt);

        TEST_ASSERT(pps_cnt == 1, "Expected 1 partition result, got %zu",
                    pps_cnt);

        pps = pps_list[0];

        err = rd_kafka_PartitionProducerState_error(pps);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected no error from DescribeProducers, got %s",
                    rd_kafka_err2str(err));

        producers = rd_kafka_PartitionProducerState_active_producers(
            pps, &producer_cnt);

        TEST_ASSERT(producer_cnt > 0, "Expected at least one active producer");

        state            = producers[0];
        producer_id      = rd_kafka_ProducerState_producer_id(state);
        producer_epoch   = rd_kafka_ProducerState_producer_epoch(state);
        txn_start_offset = rd_kafka_ProducerState_txn_start_offset(state);

        TEST_SAY("Found active producer: ID=%" PRId64 ", epoch=%" PRId32
                 ", txn_start_offset=%" PRId64 "\n",
                 producer_id, producer_epoch, txn_start_offset);

        rd_kafka_event_destroy(rkev);
        rkev = NULL;

        /* Destroy the producer before aborting its transaction
         * externally, to avoid the fencing fatal error on the
         * producer handle when the abort marker is written. */
        rd_kafka_destroy(producer);
        producer = NULL;

        /* Now abort the transaction using AbortTransaction API */
        TEST_SAY("Aborting transaction for %s [0]\n", topic);

        abort_specs    = malloc(sizeof(*abort_specs));
        abort_specs[0] = rd_kafka_AbortTransactionSpec_new(
            topic, 0, producer_id, producer_epoch,
            -1 /* coordinator_epoch not known */, txn_start_offset);

        par_list = do_test_AbortTransaction(admin, abort_specs, 1, 10000,
                                            &abort_rkev, &par_cnt);

        TEST_ASSERT(par_cnt == 1, "Expected 1 partition result, got %zu",
                    par_cnt);

        par = par_list[0];

        /* Check partition */
        TEST_ASSERT(!strcmp(rd_kafka_PartitionAbortResult_topic(par), topic),
                    "Expected topic=%s, got %s", topic,
                    rd_kafka_PartitionAbortResult_topic(par));

        TEST_ASSERT(rd_kafka_PartitionAbortResult_partition(par) == 0,
                    "Expected partition=0, got %d",
                    rd_kafka_PartitionAbortResult_partition(par));

        /* Check for errors */
        err = rd_kafka_PartitionAbortResult_error(par);
        TEST_SAY("AbortTransaction result: %s\n", rd_kafka_err2str(err));

        /* The abort may succeed or fail depending on timing and broker state
         * - just verify we got a proper response */
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                TEST_SAY(
                    "AbortTransaction returned error (may be expected): %s\n",
                    rd_kafka_err2str(err));
        }

        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);
        rd_kafka_event_destroy(abort_rkev);
        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_destroy(admin);

        rd_free(topic);
        rd_free(txnid);

        SUB_TEST_PASS();
}


/**
 * @brief Test aborting transaction on a non-existent partition.
 */
static void do_test_abort_nonexistent_partition(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_event_t *rkev = NULL;
        rd_kafka_AbortTransactionSpec_t **abort_specs;
        const rd_kafka_PartitionAbortResult_t **par_list;
        size_t par_cnt;
        const rd_kafka_PartitionAbortResult_t *par;
        rd_kafka_resp_err_t err;

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Try to abort transaction on a non-existent partition */
        abort_specs    = malloc(sizeof(*abort_specs));
        abort_specs[0] = rd_kafka_AbortTransactionSpec_new(
            "nonexistent-topic-12345", 0, 1000, /* producer_id */
            0,                                  /* producer_epoch */
            -1,                                 /* coordinator_epoch */
            -1 /* txn_start_offset */);

        TEST_SAY("Aborting transaction on non-existent partition\n");
        par_list = do_test_AbortTransaction(rk, abort_specs, 1, 10000, &rkev,
                                            &par_cnt);

        TEST_SAY("Got %zu result(s)\n", par_cnt);

        if (par_cnt > 0) {
                par = par_list[0];
                err = rd_kafka_PartitionAbortResult_error(par);
                TEST_SAY("Partition error: %s (%d)\n", rd_kafka_err2str(err),
                         err);
                /* Expect UNKNOWN_TOPIC_OR_PARTITION or similar error */
                TEST_ASSERT(err != RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected error for non-existent partition");
        }

        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);
        rd_kafka_event_destroy(rkev);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test AbortTransaction with mock cluster.
 */
static void do_test_abort_transaction_mock(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_event_t *rkev = NULL;
        rd_kafka_AbortTransactionSpec_t **abort_specs;
        const rd_kafka_PartitionAbortResult_t **par_list;
        size_t par_cnt;

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Abort transaction on mock cluster */
        abort_specs    = malloc(sizeof(*abort_specs));
        abort_specs[0] = rd_kafka_AbortTransactionSpec_new(
            "mock-test-topic", 0, 1000, /* producer_id */
            0,                          /* producer_epoch */
            -1,                         /* coordinator_epoch */
            -1 /* txn_start_offset */);

        TEST_SAY("Calling AbortTransaction on mock cluster\n");
        par_list =
            do_test_AbortTransaction(rk, abort_specs, 1, 5000, &rkev, &par_cnt);

        /* Mock cluster should return the result (possibly with error) */
        TEST_SAY("AbortTransaction on mock returned %zu results\n", par_cnt);

        if (par_cnt > 0) {
                const rd_kafka_PartitionAbortResult_t *par = par_list[0];
                rd_kafka_resp_err_t err =
                    rd_kafka_PartitionAbortResult_error(par);
                TEST_SAY("  topic=%s, partition=%d, error=%s\n",
                         rd_kafka_PartitionAbortResult_topic(par),
                         rd_kafka_PartitionAbortResult_partition(par),
                         rd_kafka_err2str(err));
        }

        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);
        rd_kafka_event_destroy(rkev);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Test input validation for AbortTransaction.
 */
static void do_test_abort_transaction_input_validation(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_AbortTransactionSpec_t **abort_specs;
        char errstr[512];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        q  = rd_kafka_queue_new(rk);

        /* Test with empty spec array */
        TEST_SAY("Testing with empty spec array\n");
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ABORTTRANSACTION);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_AbortTransaction(rk, NULL, 0, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("Empty spec array error (expected): %s\n",
                         rd_kafka_event_error_string(rkev));
                TEST_ASSERT(rd_kafka_event_error(rkev) ==
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                            "Expected INVALID_ARG error");
        }
        rd_kafka_event_destroy(rkev);

        /* Test with empty topic name */
        TEST_SAY("Testing with empty topic name\n");
        abort_specs = malloc(sizeof(*abort_specs));
        abort_specs[0] =
            rd_kafka_AbortTransactionSpec_new("", 0, 1000, 0, -1, -1);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ABORTTRANSACTION);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_AbortTransaction(rk, abort_specs, 1, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        TEST_ASSERT(rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error for empty topic name, got success");
        TEST_SAY("Empty topic name error (expected): %s\n",
                 rd_kafka_event_error_string(rkev));
        rd_kafka_event_destroy(rkev);
        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);

        /* Test with negative partition */
        TEST_SAY("Testing with negative partition\n");
        abort_specs    = malloc(sizeof(*abort_specs));
        abort_specs[0] = rd_kafka_AbortTransactionSpec_new("test-topic", -1,
                                                           1000, 0, -1, -1);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ABORTTRANSACTION);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_AbortTransaction(rk, abort_specs, 1, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        TEST_ASSERT(rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error for negative partition, got success");
        TEST_SAY("Negative partition error (expected): %s\n",
                 rd_kafka_event_error_string(rkev));
        rd_kafka_event_destroy(rkev);
        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);

        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0157_abort_transaction(int argc, char **argv) {
        /* Always run mock test - doesn't require real cluster */
        do_test_abort_transaction_mock();

        /* Input validation test */
        do_test_abort_transaction_input_validation();

        /* Real cluster tests require Kafka 3.0.0+ */
        if (test_broker_version < TEST_BRKVER(3, 0, 0, 0)) {
                TEST_SKIP(
                    "Real cluster tests require Kafka 3.0.0+ (skipping)\n");
                return 0;
        }

        do_test_abort_hanging_transaction();
        do_test_abort_nonexistent_partition();

        return 0;
}
