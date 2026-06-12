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
 * @name DescribeProducers Admin API tests (KIP-664)
 *
 * Requires Kafka 3.0+ for DescribeProducers.
 */


/**
 * @brief Helper function to call DescribeProducers and return results.
 *
 * @param rk Client instance
 * @param partitions Topic-partitions to describe
 * @param timeout_ms Request timeout in milliseconds
 * @param result_cnt Output: number of results
 *
 * @returns Array of partition producer states (caller must destroy event)
 */
static const rd_kafka_PartitionProducerState_t **
do_test_DescribeProducers(rd_kafka_t *rk,
                          rd_kafka_topic_partition_list_t *partitions,
                          int timeout_ms,
                          rd_kafka_event_t **eventp,
                          size_t *result_cnt) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeProducers_result_t *result;
        const rd_kafka_PartitionProducerState_t **pps_list;
        char errstr[512];

        q = rd_kafka_queue_new(rk);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEPRODUCERS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, timeout_ms, errstr, sizeof(errstr)));

        TEST_SAY("Calling DescribeProducers for %d partition(s)\n",
                 partitions->cnt);
        rd_kafka_DescribeProducers(rk, partitions, options, q);
        rd_kafka_AdminOptions_destroy(options);

        /* Poll for result */
        rkev = rd_kafka_queue_poll(q, tmout_multip(timeout_ms + 10000));
        TEST_ASSERT(rkev != NULL, "Expected DescribeProducers result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("DescribeProducers error: %s\n",
                         rd_kafka_event_error_string(rkev));
        }

        TEST_ASSERT(rd_kafka_event_type(rkev) ==
                        RD_KAFKA_EVENT_DESCRIBEPRODUCERS_RESULT,
                    "Expected DESCRIBEPRODUCERS_RESULT, got %s",
                    rd_kafka_event_name(rkev));

        result = rd_kafka_event_DescribeProducers_result(rkev);
        TEST_ASSERT(result != NULL, "Expected DescribeProducers result");

        pps_list =
            rd_kafka_DescribeProducers_result_partitions(result, result_cnt);

        rd_kafka_queue_destroy(q);

        *eventp = rkev;
        return pps_list;
}


/**
 * @brief Test describing producers on a partition with active producers.
 */
static void do_test_describe_partition_with_producer(void) {
        char *topic;
        char *txnid;
        rd_kafka_t *producer, *admin;
        rd_kafka_conf_t *conf, *admin_conf;
        rd_kafka_event_t *rkev = NULL;
        rd_kafka_topic_partition_list_t *partitions;
        const rd_kafka_PartitionProducerState_t **pps_list;
        size_t pps_cnt;
        const rd_kafka_PartitionProducerState_t *pps;
        rd_kafka_resp_err_t err;

        SUB_TEST();

        topic = rd_strdup(test_mk_topic_name("0156_desc_prod_topic", 1));
        txnid = rd_strdup(test_mk_topic_name("0156_test_txn", 1));

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

        /* Now describe producers on the partition */
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, topic, 0);

        TEST_SAY("Describing producers for %s [0]\n", topic);
        pps_list = do_test_DescribeProducers(admin, partitions, 10000, &rkev,
                                             &pps_cnt);

        TEST_ASSERT(pps_cnt == 1, "Expected 1 partition result, got %zu",
                    pps_cnt);

        pps = pps_list[0];

        /* Check partition */
        TEST_ASSERT(rd_kafka_PartitionProducerState_partition(pps) == 0,
                    "Expected partition=0, got %d",
                    rd_kafka_PartitionProducerState_partition(pps));

        /* Check for errors */
        err = rd_kafka_PartitionProducerState_error(pps);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected no error, got %s", rd_kafka_err2str(err));

        /* Check for active producers */
        {
                const rd_kafka_ProducerState_t **producers;
                size_t producer_cnt;

                producers = rd_kafka_PartitionProducerState_active_producers(
                    pps, &producer_cnt);

                TEST_SAY("Found %zu active producer(s)\n", producer_cnt);
                TEST_ASSERT(producer_cnt > 0,
                            "Expected at least one active producer");

                if (producer_cnt > 0) {
                        const rd_kafka_ProducerState_t *state = producers[0];
                        TEST_SAY("  Producer ID: %" PRId64 "\n",
                                 rd_kafka_ProducerState_producer_id(state));
                        TEST_SAY("  Producer epoch: %" PRId32 "\n",
                                 rd_kafka_ProducerState_producer_epoch(state));
                        TEST_SAY("  Last sequence: %" PRId32 "\n",
                                 rd_kafka_ProducerState_last_sequence(state));
                        TEST_SAY(
                            "  Txn start offset: %" PRId64 "\n",
                            rd_kafka_ProducerState_txn_start_offset(state));

                        /* Verify producer ID is valid */
                        TEST_ASSERT(rd_kafka_ProducerState_producer_id(state) >=
                                        0,
                                    "Expected valid producer ID");
                }
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Commit the transaction */
        TEST_SAY("Committing transaction\n");
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(producer, 30 * 1000));

        rd_kafka_destroy(admin);
        rd_kafka_destroy(producer);

        rd_free(topic);
        rd_free(txnid);

        SUB_TEST_PASS();
}


/**
 * @brief Test describing producers on a non-existent partition.
 */
static void do_test_describe_nonexistent_partition(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_event_t *rkev = NULL;
        rd_kafka_topic_partition_list_t *partitions;
        const rd_kafka_PartitionProducerState_t **pps_list;
        size_t pps_cnt;
        const rd_kafka_PartitionProducerState_t *pps;
        rd_kafka_resp_err_t err;

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Try to describe a non-existent partition */
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, "nonexistent-topic-12345",
                                          0);

        TEST_SAY("Describing non-existent partition\n");
        pps_list =
            do_test_DescribeProducers(rk, partitions, 10000, &rkev, &pps_cnt);

        TEST_SAY("Got %zu result(s)\n", pps_cnt);

        if (pps_cnt > 0) {
                pps = pps_list[0];
                err = rd_kafka_PartitionProducerState_error(pps);
                TEST_SAY("Partition error: %s (%d)\n", rd_kafka_err2str(err),
                         err);
                /* Expect UNKNOWN_TOPIC_OR_PARTITION or similar error */
                TEST_ASSERT(err != RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected error for non-existent partition");
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Test DescribeProducers with mock cluster.
 */
static void do_test_describe_producers_mock(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_event_t *rkev = NULL;
        rd_kafka_topic_partition_list_t *partitions;
        const rd_kafka_PartitionProducerState_t **pps_list;
        size_t pps_cnt;

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Describe producers on mock cluster */
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, "mock-test-topic", 0);

        TEST_SAY("Calling DescribeProducers on mock cluster\n");
        pps_list =
            do_test_DescribeProducers(rk, partitions, 5000, &rkev, &pps_cnt);

        /* Mock cluster should return the result (possibly with error) */
        TEST_SAY("DescribeProducers on mock returned %zu results\n", pps_cnt);

        if (pps_cnt > 0) {
                const rd_kafka_PartitionProducerState_t *pps = pps_list[0];
                rd_kafka_resp_err_t err =
                    rd_kafka_PartitionProducerState_error(pps);
                TEST_SAY("  partition=%d, error=%s\n",
                         rd_kafka_PartitionProducerState_partition(pps),
                         rd_kafka_err2str(err));
        }

        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Test input validation for DescribeProducers.
 */
static void do_test_describe_producers_input_validation(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_topic_partition_list_t *partitions;
        char errstr[512];

        SUB_TEST();

        test_conf_init(&conf, NULL, 30);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        q  = rd_kafka_queue_new(rk);

        /* Test with empty partition list */
        TEST_SAY("Testing with empty partition list\n");
        partitions = rd_kafka_topic_partition_list_new(0);
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEPRODUCERS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_DescribeProducers(rk, partitions, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        if (rd_kafka_event_error(rkev)) {
                TEST_SAY("Empty partition list error (expected): %s\n",
                         rd_kafka_event_error_string(rkev));
                TEST_ASSERT(rd_kafka_event_error(rkev) ==
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                            "Expected INVALID_ARG error");
        }
        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Test with empty topic name */
        TEST_SAY("Testing with empty topic name\n");
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, "", 0);
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEPRODUCERS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_DescribeProducers(rk, partitions, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        TEST_ASSERT(rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error for empty topic name, got success");
        TEST_SAY("Empty topic name error (expected): %s\n",
                 rd_kafka_event_error_string(rkev));
        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Test with negative partition */
        TEST_SAY("Testing with negative partition\n");
        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, "test-topic", -1);
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEPRODUCERS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 5000, errstr, sizeof(errstr)));

        rd_kafka_DescribeProducers(rk, partitions, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = rd_kafka_queue_poll(q, tmout_multip(10000));
        TEST_ASSERT(rkev != NULL, "Expected result");

        TEST_ASSERT(rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error for negative partition, got success");
        TEST_SAY("Negative partition error (expected): %s\n",
                 rd_kafka_event_error_string(rkev));
        rd_kafka_event_destroy(rkev);
        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0156_describe_producers(int argc, char **argv) {
        /* Always run mock test - doesn't require real cluster */
        do_test_describe_producers_mock();

        /* Input validation test */
        do_test_describe_producers_input_validation();

        /* Real cluster tests require Kafka 3.0.0+ */
        if (test_broker_version < TEST_BRKVER(3, 0, 0, 0)) {
                TEST_SKIP(
                    "Real cluster tests require Kafka 3.0.0+ (skipping)\n");
                return 0;
        }

        do_test_describe_partition_with_producer();
        do_test_describe_nonexistent_partition();

        return 0;
}
