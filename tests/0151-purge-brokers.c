/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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


static void fetch_metadata(rd_kafka_t *rk,
                           size_t expected_broker_cnt,
                           int32_t *expected_brokers) {
        const rd_kafka_metadata_t *md = NULL;
        rd_kafka_resp_err_t err;
        size_t cnt   = 0;
        int32_t *ids = NULL;
        size_t i;
        int timeout_secs       = 10;
        int64_t abs_timeout_us = test_clock() + timeout_secs * 1000000;

        TEST_SAY("Waiting for up to %ds for metadata update\n", timeout_secs);

        /* Trigger Metadata request which will update learned brokers. */
        do {
                err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(100));
                if (md) {
                        rd_kafka_metadata_destroy(md);
                        md = NULL;
                }
                /* Can time out in case the request is sent to
                 * the broker that has been decommissioned. */
                else if (err != RD_KAFKA_RESP_ERR__TIMED_OUT &&
                         err != RD_KAFKA_RESP_ERR__TRANSPORT)
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                rd_usleep(100 * 1000, 0);
                RD_IF_FREE(ids, rd_free);
                ids = rd_kafka_brokers_learned_ids(rk, &cnt);
        } while (test_clock() < abs_timeout_us && cnt != expected_broker_cnt);

        TEST_ASSERT(cnt == expected_broker_cnt,
                    "expected %" PRIusz " brokers in cache, not %" PRIusz,
                    expected_broker_cnt, cnt);

        for (i = 0; i < cnt; i++) {
                TEST_ASSERT(ids[i] == expected_brokers[i],
                            "expected broker id[%" PRIusz
                            "] to be "
                            "%" PRId32 ", got %" PRId32,
                            i, expected_brokers[i], ids[i]);
        }

        RD_IF_FREE(ids, rd_free);
}

/**
 * @brief Test adding and removing brokers from the mock cluster.
 *        Verify that the client is updated with the new broker list.
 *
 *        All \p actions are executed in sequence.
 *
 *        After each action, the client is expected to have the broker
 *        ids in \p expected_broker_ids and the count to be
 *        \p expected_brokers_cnt .
 *        Brokers are added at the head of the list.
 *
 *        @param initial_cluster_size Initial number of brokers in the cluster.
 *        @param actions Array of actions to perform. Each action is a pair
 *                       (action,broker id). 0 to remove, 1 to add,
 *                       2 to set down, 3 to set up.
 *        @param expected_broker_ids Array of broker ids expected after each
 *                                   action.
 *        @param expected_broker_ids_cnt Number of elements in
 *                                       \p expected_broker_ids .
 *        @param expected_brokers_cnt Array of expected broker count after each
 *                                    action.
 */
static void do_test_add_remove_brokers(int32_t initial_cluster_size,
                                       int32_t actions[][2],
                                       int32_t expected_broker_ids[][5],
                                       size_t expected_broker_ids_cnt,
                                       int32_t expected_brokers_cnt[]) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        size_t action = 0;

        cluster = test_mock_cluster_new(initial_cluster_size, &bootstraps);

        test_conf_init(&conf, NULL, 10);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "1000");

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create a new topic to trigger partition reassignment */
        rd_kafka_mock_topic_create(cluster, "topic1", 3, initial_cluster_size);

        for (action = 0; action < expected_broker_ids_cnt; action++) {
                if (action > 0) {
                        int32_t action_type = actions[action - 1][0];
                        int32_t broker_id   = actions[action - 1][1];
                        TEST_SAY("Executing action %zu\n", action);
                        switch (action_type) {
                        case 0:
                                TEST_SAY("Removing broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(
                                    rd_kafka_mock_broker_decommission(
                                        cluster, broker_id) == 0,
                                    "Failed to remove broker from cluster");
                                break;

                        case 1:
                                TEST_SAY("Adding broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_add(
                                                cluster, broker_id) == 0,
                                            "Failed to add broker to cluster");
                                break;

                        case 2:
                                TEST_SAY("Setting down broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_set_down(
                                                cluster, broker_id) == 0,
                                            "Failed to set broker %" PRId32
                                            " down",
                                            broker_id);
                                break;

                        case 3:
                                TEST_SAY("Setting up broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_set_up(
                                                cluster, broker_id) == 0,
                                            "Failed to set broker %" PRId32
                                            " up",
                                            broker_id);
                                break;

                        default:
                                break;
                        }
                }

                fetch_metadata(rk, expected_brokers_cnt[action],
                               expected_broker_ids[action]);
        }

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);
}

/**
 * @brief Test replacing the brokers in the mock cluster with new ones.
 *        At each step a majority of brokers are returned by the Metadata call.
 *        At the end all brokers from the old cluster are removed.
 */
static void do_test_replace_with_new_cluster(void) {
        SUB_TEST_QUICK();

        int32_t expected_brokers_cnt[] = {3, 2, 3, 2, 3, 2, 3};

        int32_t expected_broker_ids[][5] = {
            {3, 2, 1}, {3, 2}, {4, 3, 2}, {4, 3}, {5, 4, 3}, {5, 4}, {6, 5, 4}};

        int32_t actions[][2] = {{0, 1}, {1, 4}, {0, 2}, {1, 5}, {0, 3}, {1, 6}};

        do_test_add_remove_brokers(3, actions, expected_broker_ids,
                                   RD_ARRAY_SIZE(expected_broker_ids),
                                   expected_brokers_cnt);

        SUB_TEST_PASS();
}

/**
 * @brief Test setting down all brokers from the mock cluster,
 *        simulating a correct cluster roll that never sets down the majority
 *        of brokers.
 *
 *        The effect is similar to decommissioning the brokers. Partition
 *        reassignment is not triggered in this case but they are not announced
 *        anymore by the Metadata response.
 */
static void do_test_cluster_roll(void) {
        SUB_TEST_QUICK();

        int32_t expected_brokers_cnt[] = {5, 4, 3, 4, 3, 4, 3, 4, 3, 4, 5};

        int32_t expected_broker_ids[][5] = {
            {5, 4, 3, 2, 1}, {5, 4, 3, 2}, {5, 4, 3},      {1, 5, 4, 3},
            {1, 5, 4},       {2, 1, 5, 4}, {2, 1, 5},      {3, 2, 1, 5},
            {3, 2, 1},       {4, 3, 2, 1}, {5, 4, 3, 2, 1}};

        int32_t actions[][2] = {{2, 1}, {2, 2}, {3, 1}, {2, 3}, {3, 2},
                                {2, 4}, {3, 3}, {2, 5}, {3, 4}, {3, 5}};

        do_test_add_remove_brokers(5, actions, expected_broker_ids,
                                   RD_ARRAY_SIZE(expected_broker_ids),
                                   expected_brokers_cnt);

        SUB_TEST_PASS();
}

int main_0151_purge_brokers_mock(int argc, char **argv) {

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_replace_with_new_cluster();

        do_test_cluster_roll();

        return 0;
}
