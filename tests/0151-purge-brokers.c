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

static rd_atomic32_t do_test_remove_then_add_received_terminate;
static rd_atomic32_t verification_complete;

/** @brief Verify that \p expected_broker_ids
 *         and \p actual_broker_ids correspond in
 *         count and value. Sorts \p actual_broker_ids .
 */
static rd_bool_t fetch_metadata_verify_brokers(int32_t *expected_broker_ids,
                                               size_t expected_broker_id_cnt,
                                               int32_t *actual_broker_ids,
                                               size_t actual_broker_id_cnt) {
        size_t i;
        if (actual_broker_id_cnt != expected_broker_id_cnt)
                return rd_false;

        for (i = 0; i < actual_broker_id_cnt; i++) {
                if (actual_broker_ids[i] != expected_broker_ids[i])
                        return rd_false;
        }
        return rd_true;
}

static void fetch_metadata(rd_kafka_t *rk,
                           int32_t *expected_broker_ids,
                           size_t expected_broker_id_cnt,
                           rd_bool_t (*await_after_action_cb)(int action),
                           int action) {
        const rd_kafka_metadata_t *md = NULL;
        rd_kafka_resp_err_t err;
        size_t actual_broker_id_cnt = 0;
        int32_t *actual_broker_ids  = NULL;
        size_t i;
        int timeout_usecs                      = 10000000;
        int64_t abs_timeout_us                 = test_clock() + timeout_usecs;
        rd_bool_t continue_requesting_metadata = rd_true;

        TEST_SAY("Waiting for up to 10s for metadata update\n");

        /* Trigger Metadata request which will update learned brokers. */
        do {
                err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000));
                if (md) {
                        rd_kafka_metadata_destroy(md);
                        md = NULL;
                } else if (err != RD_KAFKA_RESP_ERR__TRANSPORT)
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                rd_usleep(100 * 1000, 0);

                RD_IF_FREE(actual_broker_ids, rd_free);
                actual_broker_ids =
                    rd_kafka_brokers_learned_ids(rk, &actual_broker_id_cnt);
                continue_requesting_metadata = test_clock() <= abs_timeout_us;

                continue_requesting_metadata &= !fetch_metadata_verify_brokers(
                    expected_broker_ids, expected_broker_id_cnt,
                    actual_broker_ids, actual_broker_id_cnt);

                if (await_after_action_cb)
                        continue_requesting_metadata |=
                            await_after_action_cb(action);

        } while (continue_requesting_metadata);

        TEST_ASSERT(actual_broker_id_cnt == expected_broker_id_cnt,
                    "expected %" PRIusz " brokers in cache, got %" PRIusz,
                    expected_broker_id_cnt, actual_broker_id_cnt);

        for (i = 0; i < actual_broker_id_cnt; i++) {
                TEST_ASSERT(actual_broker_ids[i] == expected_broker_ids[i],
                            "expected broker id[%" PRIusz
                            "] to be "
                            "%" PRId32 ", got %" PRId32,
                            i, expected_broker_ids[i], actual_broker_ids[i]);
        }
        RD_IF_FREE(actual_broker_ids, rd_free);
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
 *        @param await_verification If `rd_false`, the verification is
 *                                  done only after last action.
 */
#define TEST_ACTION_REMOVE_BROKER         0
#define TEST_ACTION_ADD_BROKER            1
#define TEST_ACTION_SET_DOWN_BROKER       2
#define TEST_ACTION_SET_UP_BROKER         3
#define TEST_ACTION_SET_GROUP_COORDINATOR 4
#define TEST_GROUP                        "topic1"
static void
do_test_add_remove_brokers(int32_t initial_cluster_size,
                           int32_t actions[][2],
                           int32_t expected_broker_ids[][5],
                           size_t expected_broker_ids_cnt,
                           int32_t expected_brokers_cnt[],
                           void (*edit_configuration_cb)(rd_kafka_conf_t *conf),
                           rd_bool_t (*await_after_action_cb)(int action)) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        size_t action = 0;

        cluster = test_mock_cluster_new(initial_cluster_size, &bootstraps);

        test_conf_init(&conf, NULL, 100);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", TEST_GROUP);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "1000");
        if (edit_configuration_cb) {
                edit_configuration_cb(conf);
        }

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);
        test_consumer_subscribe(rk, TEST_GROUP);

        /* Create a new topic to trigger partition reassignment */
        rd_kafka_mock_topic_create(cluster, TEST_GROUP, 3,
                                   initial_cluster_size);

        for (action = 0; action < expected_broker_ids_cnt; action++) {
                if (action > 0) {
                        int32_t action_type = actions[action - 1][0];
                        int32_t broker_id   = actions[action - 1][1];
                        TEST_SAY("Executing action %zu\n", action);
                        switch (action_type) {
                        case TEST_ACTION_REMOVE_BROKER:
                                TEST_SAY("Removing broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(
                                    rd_kafka_mock_broker_decommission(
                                        cluster, broker_id) == 0,
                                    "Failed to remove broker from cluster");
                                break;

                        case TEST_ACTION_ADD_BROKER:
                                TEST_SAY("Adding broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_add(
                                                cluster, broker_id) == 0,
                                            "Failed to add broker to cluster");
                                break;

                        case TEST_ACTION_SET_DOWN_BROKER:
                                TEST_SAY("Setting down broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_set_down(
                                                cluster, broker_id) == 0,
                                            "Failed to set broker %" PRId32
                                            " down",
                                            broker_id);
                                break;

                        case TEST_ACTION_SET_UP_BROKER:
                                TEST_SAY("Setting up broker %" PRId32 "\n",
                                         broker_id);
                                TEST_ASSERT(rd_kafka_mock_broker_set_up(
                                                cluster, broker_id) == 0,
                                            "Failed to set broker %" PRId32
                                            " up",
                                            broker_id);
                                break;
                        case TEST_ACTION_SET_GROUP_COORDINATOR:
                                TEST_ASSERT(rd_kafka_mock_coordinator_set(
                                                cluster, "group", TEST_GROUP,
                                                broker_id) == 0,
                                            "Failed to set group coordinator "
                                            "to %" PRId32,
                                            broker_id);
                                break;
                        default:
                                break;
                        }
                }

                fetch_metadata(rk, expected_broker_ids[action],
                               expected_brokers_cnt[action],
                               await_after_action_cb, action);
        }
        TEST_SAY("Test verification complete\n");
        rd_atomic32_set(&verification_complete, 1);

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

        int32_t expected_brokers_cnt[] = {3, 3, 2, 3, 2, 3, 3, 2, 3};

        int32_t expected_broker_ids[][5] = {{1, 2, 3}, {1, 2, 3}, {2, 3},
                                            {2, 3, 4}, {3, 4},    {3, 4, 5},
                                            {3, 4, 5}, {4, 5},    {4, 5, 6}};

        int32_t actions[][2] = {
            {TEST_ACTION_SET_GROUP_COORDINATOR, 3},
            {TEST_ACTION_REMOVE_BROKER, 1},
            {TEST_ACTION_ADD_BROKER, 4},
            {TEST_ACTION_REMOVE_BROKER, 2},
            {TEST_ACTION_ADD_BROKER, 5},
            {TEST_ACTION_SET_GROUP_COORDINATOR, 5},
            {TEST_ACTION_REMOVE_BROKER, 3},
            {TEST_ACTION_ADD_BROKER, 6},
        };

        do_test_add_remove_brokers(3, actions, expected_broker_ids,
                                   RD_ARRAY_SIZE(expected_broker_ids),
                                   expected_brokers_cnt, NULL, NULL);

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

        int32_t expected_brokers_cnt[] = {5, 5, 4, 3, 4, 3, 4,
                                          3, 4, 4, 3, 4, 5};

        int32_t expected_broker_ids[][5] = {
            {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {2, 3, 4, 5}, {3, 4, 5},
            {1, 3, 4, 5},    {1, 4, 5},       {1, 2, 4, 5}, {1, 2, 5},
            {1, 2, 3, 5},    {1, 2, 3, 5},    {1, 2, 3},    {1, 2, 3, 4},
            {1, 2, 3, 4, 5}};

        int32_t actions[][2] = {
            {TEST_ACTION_SET_GROUP_COORDINATOR, 5},
            {TEST_ACTION_SET_DOWN_BROKER, 1},
            {TEST_ACTION_SET_DOWN_BROKER, 2},
            {TEST_ACTION_SET_UP_BROKER, 1},
            {TEST_ACTION_SET_DOWN_BROKER, 3},
            {TEST_ACTION_SET_UP_BROKER, 2},
            {TEST_ACTION_SET_DOWN_BROKER, 4},
            {TEST_ACTION_SET_UP_BROKER, 3},
            {TEST_ACTION_SET_GROUP_COORDINATOR, 1},
            {TEST_ACTION_SET_DOWN_BROKER, 5},
            {TEST_ACTION_SET_UP_BROKER, 4},
            {TEST_ACTION_SET_UP_BROKER, 5},
        };

        do_test_add_remove_brokers(5, actions, expected_broker_ids,
                                   RD_ARRAY_SIZE(expected_broker_ids),
                                   expected_brokers_cnt, NULL, NULL);

        SUB_TEST_PASS();
}

/**
 * @brief Log callback that waits for the TERMINATE op to be received
 */
static void do_test_remove_then_add_log_cb(const rd_kafka_t *rk,
                                           int level,
                                           const char *fac,
                                           const char *buf) {
        if (!rd_atomic32_get(&do_test_remove_then_add_received_terminate) &&
            strstr(buf, "/1: Handle terminates in state") > 0) {
                rd_atomic32_set(&do_test_remove_then_add_received_terminate, 1);
                while (!rd_atomic32_get(&verification_complete))
                        rd_usleep(100 * 1000, 0);
        }
}

/**
 * @brief Await for the TERMINATE op to be received after the action
 *        that removes the broker then proceed to
 *        add the broker again.
 */
static rd_bool_t do_test_remove_then_add_await_after_action_cb(int action) {
        if (action == 2) {
                /* Wait until TERMINATE is received */
                return !rd_atomic32_get(
                    &do_test_remove_then_add_received_terminate);
        }
        return rd_false;
}

/**
 * @brief Disable sparse connections to increase likely of problems
 *        when the decommisioned broker is re-connecting.
 *        Add a pause after receiving the TERMINATE op to allow to
 *        proceed with adding it again before it's decommissioned.
 */
static test_conf_log_interceptor_t *log_interceptor;
static void
do_test_remove_then_add_edit_configuration_cb(rd_kafka_conf_t *conf) {
        const char *debug_contexts[2] = {"broker", NULL};

        /* This timeout verifies that the correct brokers are returned
         * without duplicates as soon as possible. */
        test_timeout_set(6);
        /* Hidden property that forces connections to all brokers,
         * increasing likelyhood of wrong behaviour if the decommissioned broker
         * starts re-connecting. */
        test_conf_set(conf, "enable.sparse.connections", "false");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_remove_then_add_log_cb, debug_contexts);
}

/**
 * @brief Test setting down one broker and then adding it again
 *        while it's still being decommissioned.
 *
 *        This should not leave dangling references that prevent broker
 *        destruction.
 */
static void do_test_remove_then_add(void) {
        SUB_TEST_QUICK();
        rd_atomic32_init(&do_test_remove_then_add_received_terminate, 0);
        rd_atomic32_init(&verification_complete, 0);

        int32_t expected_brokers_cnt[] = {3, 3, 2, 3};

        int32_t expected_broker_ids[][5] = {
            {1, 2, 3}, {1, 2, 3}, {2, 3}, {1, 2, 3}};

        int32_t actions[][2] = {
            {TEST_ACTION_SET_GROUP_COORDINATOR, 3},
            {TEST_ACTION_REMOVE_BROKER, 1},
            {TEST_ACTION_ADD_BROKER, 1},
        };

        do_test_add_remove_brokers(
            3, actions, expected_broker_ids, RD_ARRAY_SIZE(expected_broker_ids),
            expected_brokers_cnt, do_test_remove_then_add_edit_configuration_cb,
            do_test_remove_then_add_await_after_action_cb);

        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

int main_0151_purge_brokers_mock(int argc, char **argv) {

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_replace_with_new_cluster();

        do_test_cluster_roll();

        do_test_remove_then_add();

        return 0;
}
