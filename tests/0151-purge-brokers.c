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
#include "../src/rdkafka_proto.h"

/**
 * @brief Test that is adding and removing brokers from the mock cluster, to
 * verify that the client is updated with the new broker list. This can trigger
 * re-bootstrapping of the client so it's also verifying that
 * the client is able to re-bootstrap itself correctly.
 */

/* Test verification is complete, thread safe variable */
static rd_atomic32_t verification_complete;
/* Mock cluster being used, for test customization */
static rd_kafka_mock_cluster_t *cluster;
/* Array of allowed errors in metadata requests,
 * terminated with `RD_KAFKA_RESP_ERR_NO_ERROR(0)` */
static rd_kafka_resp_err_t *allowed_errors = NULL;

/**
 * @brief Is \p err an allowed error in this test?
 */
static rd_bool_t fetch_metadata_allowed_error(rd_kafka_resp_err_t err) {
        if (!allowed_errors)
                return rd_false;
        rd_kafka_resp_err_t *allowed_error = allowed_errors;
        while (*allowed_error) {
                if (err == *allowed_error)
                        return rd_true;
                allowed_error++;
        }
        return rd_false;
}

/** @brief Verify that \p expected_broker_ids
 *         and \p actual_broker_ids correspond in
 *         count and value.
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

/**
 * @brief Wait for metadata update and verify that the
 *        \p expected_broker_ids are present in the
 *        response after \p action was executed.
 *
 *        It \p action is -1, no action was executed to reach this state
 *        (it's the initial state).
 *        so \p after_action_cb is not called and only the state is verified.
 *
 *        Until \p after_action_cb returns `rd_true`,
 *        continues with metadata requests and doesn't
 *        execute the next action even if brokers ids are
 *        verified or timeout has reached.
 *
 *        Checks metadata every 100ms for 20s max.
 */
static void fetch_metadata(rd_kafka_t *rk,
                           int32_t *expected_broker_ids,
                           size_t expected_broker_id_cnt,
                           rd_bool_t (*request_metadata_cb)(int action),
                           rd_bool_t (*after_action_cb)(rd_kafka_t **rkp,
                                                        int action),
                           int action) {
        const rd_kafka_metadata_t *md = NULL;
        rd_kafka_resp_err_t err;
        size_t actual_broker_id_cnt = 0;
        int32_t *actual_broker_ids  = NULL;
        size_t i;
        int timeout_usecs                      = 20000000;
        int64_t abs_timeout_us                 = test_clock() + timeout_usecs;
        rd_bool_t continue_requesting_metadata = rd_true;

        TEST_SAY("Waiting for up to 20s for metadata update\n");

        /* Trigger Metadata request which will update learned brokers. */
        do {
                if (!request_metadata_cb || request_metadata_cb(action)) {
                        /* We set a short timeout because an unavailable
                         * broker can be selected for the metadata request,
                         * While the client still doesn't know about it.
                         * In this case the request times out. */
                        err = rd_kafka_metadata(rk, 0, NULL, &md, 1000);
                        if (md) {
                                rd_kafka_metadata_destroy(md);
                                md = NULL;
                        } else if (err != RD_KAFKA_RESP_ERR__TRANSPORT &&
                                   err != RD_KAFKA_RESP_ERR__TIMED_OUT &&
                                   !fetch_metadata_allowed_error(err))
                                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }

                RD_IF_FREE(actual_broker_ids, rd_free);
                actual_broker_ids =
                    rd_kafka_brokers_learned_ids(rk, &actual_broker_id_cnt);
                continue_requesting_metadata = test_clock() <= abs_timeout_us;

                continue_requesting_metadata =
                    continue_requesting_metadata &&
                    !fetch_metadata_verify_brokers(
                        expected_broker_ids, expected_broker_id_cnt,
                        actual_broker_ids, actual_broker_id_cnt);

                if (after_action_cb)
                        continue_requesting_metadata =
                            continue_requesting_metadata ||
                            after_action_cb(&rk, action);

                if (continue_requesting_metadata)
                        rd_usleep(100 * 1000, 0);

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

#define do_test_add_remove_brokers(initial_cluster_size, actions, action_cnt,  \
                                   expected_broker_ids, expected_brokers_cnt)  \
        do_test_add_remove_brokers0(initial_cluster_size, actions, action_cnt, \
                                    expected_broker_ids, expected_brokers_cnt, \
                                    NULL, NULL, NULL);
#define TEST_ACTION_REMOVE_BROKER         0
#define TEST_ACTION_ADD_BROKER            1
#define TEST_ACTION_SET_DOWN_BROKER       2
#define TEST_ACTION_SET_UP_BROKER         3
#define TEST_ACTION_SET_GROUP_COORDINATOR 4
#define TEST_ACTION_NOOP                  5
#define TEST_GROUP                        "topic1"

/**
 * @brief Test adding and removing brokers from the mock cluster.
 *        Verify that the client is updated with the new broker list.
 *
 *        All \p actions are executed in sequence. \p expected_brokers_cnt
 *
 *        After each action, the client is expected to have the broker
 *        ids in \p expected_broker_ids and the count to be
 *        \p expected_brokers_cnt .
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
 *        @param edit_configuration_cb Callback to edit configuration before
 *                                     creating the consumer.
 *        @param after_action_cb Callback to be called after each action.
 *                               When returning `rd_true` it'll continue
 *                               requesting metadata, even if the expected
 *                               broker ids were found, before continuing
 *                               with the next action.
 *        @param request_metadata_cb Callback to be called before each action.
 *                                   When NULL or returning `rd_true` it'll
 * request metadata, when returning 'rd_false' it won't because the cluster
 * isn't reachable.
 */
static void do_test_add_remove_brokers0(
    int32_t initial_cluster_size,
    int32_t actions[][2],
    size_t action_cnt,
    int32_t expected_broker_ids[][5],
    int32_t expected_brokers_cnt[],
    rd_kafka_type_t (*edit_configuration_cb)(rd_kafka_conf_t *conf),
    rd_bool_t (*request_metadata_cb)(int action),
    rd_bool_t (*after_action_cb)(rd_kafka_t **rkp, int action)) {
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        size_t action     = 0;
        const char *group = test_mk_topic_name(__FUNCTION__, 1);

        cluster = test_mock_cluster_new(initial_cluster_size, &bootstraps);

        test_conf_init(&conf, NULL, 100);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "1000");
        rd_kafka_type_t type = RD_KAFKA_CONSUMER;
        if (edit_configuration_cb)
                type = edit_configuration_cb(conf);

        if (type == RD_KAFKA_CONSUMER)
                test_conf_set(conf, "group.id", group);

        rk = test_create_handle(type, conf);
        if (type == RD_KAFKA_CONSUMER)
                test_consumer_subscribe(rk, group);

        /* Create a new topic to trigger partition reassignment */
        rd_kafka_mock_topic_create(cluster, group, 3, initial_cluster_size);

        /* Verify state zero is reached */
        fetch_metadata(rk, expected_broker_ids[0], expected_brokers_cnt[0],
                       request_metadata_cb, after_action_cb, -1);

        for (action = 0; action < action_cnt; action++) {
                rd_kafka_message_t *rkmessage;

                /* action: N, state: N+1 */
                int next_state      = action + 1;
                int32_t action_type = actions[action][0];
                int32_t broker_id   = actions[action][1];
                TEST_SAY("Executing action %zu\n", action + 1);
                switch (action_type) {
                case TEST_ACTION_REMOVE_BROKER:
                        TEST_SAY("Removing broker %" PRId32 "\n", broker_id);
                        TEST_ASSERT(rd_kafka_mock_broker_decommission(
                                        cluster, broker_id) == 0,
                                    "Failed to remove broker from cluster");
                        break;

                case TEST_ACTION_ADD_BROKER:
                        TEST_SAY("Adding broker %" PRId32 "\n", broker_id);
                        TEST_ASSERT(
                            rd_kafka_mock_broker_add(cluster, broker_id) == 0,
                            "Failed to add broker to cluster");
                        break;

                case TEST_ACTION_SET_DOWN_BROKER:
                        TEST_SAY("Setting down broker %" PRId32 "\n",
                                 broker_id);
                        TEST_ASSERT(rd_kafka_mock_broker_set_down(
                                        cluster, broker_id) == 0,
                                    "Failed to set broker %" PRId32 " down",
                                    broker_id);
                        break;

                case TEST_ACTION_SET_UP_BROKER:
                        TEST_SAY("Setting up broker %" PRId32 "\n", broker_id);
                        TEST_ASSERT(rd_kafka_mock_broker_set_up(cluster,
                                                                broker_id) == 0,
                                    "Failed to set broker %" PRId32 " up",
                                    broker_id);
                        break;
                case TEST_ACTION_SET_GROUP_COORDINATOR:
                        TEST_SAY("Setting group coordinator to broker %" PRId32
                                 "\n",
                                 broker_id);
                        TEST_ASSERT(rd_kafka_mock_coordinator_set(
                                        cluster, "group", group, broker_id) ==
                                        0,
                                    "Failed to set group coordinator "
                                    "to %" PRId32,
                                    broker_id);
                        break;
                default:
                        break;
                }

                fetch_metadata(rk, expected_broker_ids[next_state],
                               expected_brokers_cnt[next_state],
                               request_metadata_cb, after_action_cb, action);

                /* Poll to get errors */
                rkmessage = rd_kafka_consumer_poll(rk, 0);
                RD_IF_FREE(rkmessage, rd_kafka_message_destroy);
                rkmessage = NULL;
        }
        TEST_SAY("Test verification complete\n");
        rd_atomic32_set(&verification_complete, 1);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);
        cluster = NULL;
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

        do_test_add_remove_brokers(3, actions, RD_ARRAY_SIZE(actions),
                                   expected_broker_ids, expected_brokers_cnt);

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

        do_test_add_remove_brokers(5, actions, RD_ARRAY_SIZE(actions),
                                   expected_broker_ids, expected_brokers_cnt);

        SUB_TEST_PASS();
}

static rd_atomic32_t do_test_remove_then_add_received_terminate;

/**
 * @brief Log callback that waits for the TERMINATE op to be received
 */
static void do_test_remove_then_add_log_cb(const rd_kafka_t *rk,
                                           int level,
                                           const char *fac,
                                           const char *buf) {
        if (!rd_atomic32_get(&do_test_remove_then_add_received_terminate) &&
            strstr(buf, "/1: Handle terminates in state")) {
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
static rd_bool_t do_test_remove_then_add_after_action_cb(rd_kafka_t **rkp,
                                                         int action) {
        /* Second action */
        if (action == 1) {
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
static rd_kafka_type_t
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

        return RD_KAFKA_CONSUMER;
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

        do_test_add_remove_brokers0(
            3, actions, RD_ARRAY_SIZE(actions), expected_broker_ids,
            expected_brokers_cnt, do_test_remove_then_add_edit_configuration_cb,
            NULL, do_test_remove_then_add_after_action_cb);

        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

static rd_atomic32_t
    do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt;

/**
 * @brief Log callback that counts numer of rebootstrap sequences received.
 */
static void
do_test_down_then_up_no_rebootstrap_loop_log_cb(const rd_kafka_t *rk,
                                                int level,
                                                const char *fac,
                                                const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence")) {
                rd_atomic32_add(
                    &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt,
                    1);
        }
}

/**
 * @brief Sets the logs callback to the log interceptor.
 */
static rd_kafka_type_t
do_test_down_then_up_no_rebootstrap_loop_edit_configuration_cb(
    rd_kafka_conf_t *conf) {
        const char *debug_contexts[2] = {"generic", NULL};

        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_down_then_up_no_rebootstrap_loop_log_cb,
            debug_contexts);
        return RD_KAFKA_PRODUCER;
}

/**
 * @brief After action 1 the broker is set down.
 *        Don't await for metadata update.
 */
static rd_bool_t
do_test_down_then_up_no_rebootstrap_loop_request_metadata_cb(int action) {
        return action != 1;
}

/**
 * @brief Await 5s after setting up the broker down
 *        to check for re-bootstrap sequences.
 */
static rd_bool_t
do_test_down_then_up_no_rebootstrap_loop_after_action_cb(rd_kafka_t **rkp,
                                                         int action) {
        if (action == 1) {
                rd_sleep(6);
        }
        return rd_false;
}

/**
 * @brief Test setting down a broker and then setting it up again.
 *        It shouldn't cause a loop of re-bootstrap sequences.
 */
static void do_test_down_then_up_no_rebootstrap_loop(void) {
        SUB_TEST_QUICK();
        rd_atomic32_init(
            &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt,
            0);

        int32_t expected_brokers_cnt[] = {1, 1, 1, 1};

        int32_t expected_broker_ids[][5] = {{1}, {1}, {1}, {1}};

        int32_t actions[][2] = {
            {TEST_ACTION_SET_UP_BROKER, 1},
            {TEST_ACTION_SET_DOWN_BROKER, 1},
            {TEST_ACTION_SET_UP_BROKER, 1},
        };

        do_test_add_remove_brokers0(
            1, actions, RD_ARRAY_SIZE(actions), expected_broker_ids,
            expected_brokers_cnt,
            do_test_down_then_up_no_rebootstrap_loop_edit_configuration_cb,
            do_test_down_then_up_no_rebootstrap_loop_request_metadata_cb,
            do_test_down_then_up_no_rebootstrap_loop_after_action_cb);

        /* With a rebootstrap every time the bootstrap brokers are removed
         * we get 6 re-bootstrap sequences.
         * With the fix we require connection to all learned brokers before
         * reaching all brokers down again.
         * In this case we have to connect to the bootstrap broker
         * and the learned broker, 2s in the slowest case as it depends
         * on periodic 10s brokers refresh timer too.
         * We expect 5 or less re-bootstrap sequences. */
        TEST_ASSERT(
            rd_atomic32_get(
                &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt) <=
                5,
            "Expected <= 5 re-bootstrap sequences, got %d",
            rd_atomic32_get(
                &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt));

        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

/**
 * @brief Test for the mock cluster to ensure there are no problems with
 *        other tests in case they're adding a broker with the same id
 *        as an existing one.
 */
static void do_test_add_same_broker_id(void) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK();

        cluster = test_mock_cluster_new(1, &bootstraps);
        TEST_SAY("Broker 1 was present from the start, should fail\n");
        err = rd_kafka_mock_broker_add(cluster, 1);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected error %s, got %s",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__INVALID_ARG),
                    rd_kafka_err2str(err));

        TEST_SAY("Broker 2 should be added\n");
        TEST_CALL_ERR__(rd_kafka_mock_broker_add(cluster, 2));

        TEST_SAY("Broker 2 cannot be added two times\n");
        err = rd_kafka_mock_broker_add(cluster, 2);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected error %s, got %s",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__INVALID_ARG),
                    rd_kafka_err2str(err));

        test_mock_cluster_destroy(cluster);

        SUB_TEST_PASS();
}

/**
 * @brief `do_test_kip899_rebootstrap_cases` test variation.
 */
static int do_test_kip899_rebootstrap_cases_variation;

/**
 * @brief Addition brokers to set in variation 2
 */
static char *do_test_kip899_rebootstrap_cases_additional_brokers;

/**
 * @brief Edit configuration by:
 *        - setting `metadata.recovery.strategy` to `none` to
 *          avoid re-bootstrapping when variation == 1
 *        - setting `bootstrap.servers` to the last two brokers
 *          when variation == 2 and adding first three after the first action
 */
static rd_kafka_type_t
do_test_kip899_rebootstrap_cases_edit_configuration_cb(rd_kafka_conf_t *conf) {
        char *bootstraps = test_conf_get(conf, "bootstrap.servers");
        switch (do_test_kip899_rebootstrap_cases_variation) {
        case 1:
                TEST_SAY("Disabling re-bootstrapping\n");
                test_conf_set(conf, "metadata.recovery.strategy", "none");
                break;
        case 2: {
                int i = 0;
                do_test_kip899_rebootstrap_cases_additional_brokers =
                    rd_strdup(bootstraps);
                char *comma =
                    do_test_kip899_rebootstrap_cases_additional_brokers;
                while (i++ < 3)
                        comma = strstr(comma + 1, ",");

                /* Add first three brokers after first action */
                *comma = '\0';
                TEST_SAY("First three brokers: %s\n",
                         do_test_kip899_rebootstrap_cases_additional_brokers);
                /* Set last two brokers as `bootstrap.servers` */
                test_conf_set(conf, "bootstrap.servers", comma + 1);
                TEST_SAY("Last two brokers: %s\n", comma + 1);
                break;
        }
        default:
                break;
        }
        return RD_KAFKA_CONSUMER;
}

/**
 * @brief Don't request metadata after setting all brokers down.
 */
static rd_bool_t
do_test_kip899_rebootstrap_cases_request_metadata_cb(int action) {
        if (action == 6) {
                return rd_false;
        }
        return rd_true;
}

/**
 * @brief After action callback for `do_test_kip899_rebootstrap_cases`.
 *        In case we need to add some additional brokers, add them after
 *        first action.
 */
static rd_bool_t
do_test_kip899_rebootstrap_cases_after_action_cb(rd_kafka_t **rkp, int action) {
        /* First action */
        if (action == 0 &&
            do_test_kip899_rebootstrap_cases_additional_brokers) {
                rd_kafka_brokers_add(
                    *rkp, do_test_kip899_rebootstrap_cases_additional_brokers);
        } else if (action == 6) {
                /* After setting all*/
                rd_sleep(1);
        }
        return rd_false;
}

/**
 * @brief KIP-899: Re-bootstrap test cases.
 *        In this test we set down 5 brokers one by one
 *        and when last two are set down, we set up the first three
 *        so there's no intersection between the two sets.
 *        This must trigger a re-bootstrapping of the client or a fatal
 *        error depending on the configuration.
 *
 *        Variations 2 and 3: the first three brokers aren't present
 *        in initial configuration but added afterwards.
 *
 *        - variation 0: re-bootstrap is enabled and triggered.
 *        - variation 1: re-bootstrap is disabled, no re-bootstrap is executed.
 *        - variation 2: same as #0 with brokers added after initial
 *                       configuration.
 */
static void do_test_kip899_rebootstrap_cases(int variation) {
        SUB_TEST_QUICK(
            "%s", variation == 0   ? "`metadata.recovery.strategy=rebootstrap`"
                  : variation == 1 ? "`metadata.recovery.strategy=none`"
                                   : "`metadata.recovery.strategy=rebootstrap` "
                                     "with additional brokers");

        do_test_kip899_rebootstrap_cases_variation          = variation;
        do_test_kip899_rebootstrap_cases_additional_brokers = NULL;
        int32_t expected_brokers_cnt[] = {5, 5, 4, 3, 2, 1, 1, 1, 1, 1, 2, 3};

        int32_t expected_broker_ids[][5] = {
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5},
            {2, 3, 4, 5},
            {3, 4, 5},
            {4, 5},
            {5},
            {5},
            {5},
            {1},
            {1},
            {1, 2},
            {1, 2, 3},
        };

        int32_t actions[][2] = {
            {TEST_ACTION_SET_GROUP_COORDINATOR, 5},
            {TEST_ACTION_SET_DOWN_BROKER, 1},
            {TEST_ACTION_SET_DOWN_BROKER, 2},
            {TEST_ACTION_SET_DOWN_BROKER, 3},
            {TEST_ACTION_SET_DOWN_BROKER, 4},
            {TEST_ACTION_SET_GROUP_COORDINATOR, -1},
            {TEST_ACTION_SET_DOWN_BROKER, 5},
            {TEST_ACTION_SET_UP_BROKER, 1},
            {TEST_ACTION_SET_GROUP_COORDINATOR, 1},
            {TEST_ACTION_SET_UP_BROKER, 2},
            {TEST_ACTION_SET_UP_BROKER, 3},
        };
        if (variation == 1) {
                /* If not re-bootstraping we've to start from the
                 * last broker seen */
                actions[7][1]              = 5;
                actions[8][1]              = 5;
                expected_broker_ids[8][0]  = 5;
                expected_broker_ids[9][0]  = 5;
                expected_broker_ids[10][0] = 2;
                expected_broker_ids[10][1] = 5;
                expected_broker_ids[11][0] = 2;
                expected_broker_ids[11][1] = 3;
                expected_broker_ids[11][2] = 5;
        }

        do_test_add_remove_brokers0(
            5, actions, RD_ARRAY_SIZE(actions), expected_broker_ids,
            expected_brokers_cnt,
            do_test_kip899_rebootstrap_cases_edit_configuration_cb,
            do_test_kip899_rebootstrap_cases_request_metadata_cb,
            do_test_kip899_rebootstrap_cases_after_action_cb);

        RD_IF_FREE(do_test_kip899_rebootstrap_cases_additional_brokers,
                   rd_free);
        SUB_TEST_PASS();
}

/**
 * @brief `do_test_kip1102_rebootstrap_cases` test variation.
 */
static int do_test_kip1102_rebootstrap_cases_variation;

/**
 * @brief Number of re-bootstrap sequences started.
 */
static rd_atomic32_t do_test_kip1102_rebootstrap_cases_rebootstrap_cnt;

static void do_test_kip1102_rebootstrap_cases_log_cb(const rd_kafka_t *rk,
                                                     int level,
                                                     const char *fac,
                                                     const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence")) {
                /* Count the number of re-bootstrap sequences started */
                rd_atomic32_add(
                    &do_test_kip1102_rebootstrap_cases_rebootstrap_cnt, 1);
        }
}

static rd_kafka_type_t
do_test_kip1102_rebootstrap_cases_edit_configuration_cb(rd_kafka_conf_t *conf) {
        const char *debug_contexts[2] = {"conf", NULL};
        /* This is 2 seconds less of the metadata refresh sequence expected
         * total duration.
         * ERR_UNKNOWN is returned as a top level error
         * so the rebootstrap timer isn't reset. */
        test_conf_set(conf, "metadata.recovery.rebootstrap.trigger.ms", "5000");
        /* Avoid Head Of Line blocking from fetch requests for predictable
         * timing */
        test_conf_set(conf, "fetch.wait.max.ms", "10");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_kip1102_rebootstrap_cases_log_cb, debug_contexts);
        return RD_KAFKA_CONSUMER;
}

static rd_kafka_resp_err_t
    do_test_kip1102_rebootstrap_cases_allowed_errors_unknown[] = {
        RD_KAFKA_RESP_ERR_UNKNOWN,
        RD_KAFKA_RESP_ERR_NO_ERROR,
};
static rd_kafka_resp_err_t
    do_test_kip1102_rebootstrap_cases_allowed_errors_rebootstrap_required[] = {
        RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED,
        RD_KAFKA_RESP_ERR_NO_ERROR,
};

/**
 * @brief After setting down one broker, we trigger a series of metadata
 *        error to cause a re-bootstrap because of
 *        `metadata.recovery.rebootstrap.trigger.ms` or directly with the
 *        dedicated error code.
 */
static rd_bool_t
do_test_kip1102_rebootstrap_cases_after_action_cb(rd_kafka_t **rkp,
                                                  int action) {
        if (action == 0) {
                /* First action: set the error codes */
                int i;
                TEST_ASSERT(cluster != NULL);
                allowed_errors =
                    do_test_kip1102_rebootstrap_cases_variation % 2 == 0
                        ? do_test_kip1102_rebootstrap_cases_allowed_errors_unknown
                        : do_test_kip1102_rebootstrap_cases_allowed_errors_rebootstrap_required;
                /* A request is made every 100 ms: 7s */
                for (i = 0; i < 70; i++)
                        rd_kafka_mock_push_request_errors(
                            cluster, RD_KAFKAP_Metadata, 1, allowed_errors[0]);

        } else if (action == 1) {
                /* Second action: in case there's no third action await
                 * enough re-bootstrap logs are seen. */
                int rebootstrap_cnt, min_rebootstrap_cnt = 0;
                switch (do_test_kip1102_rebootstrap_cases_variation) {
                case 2:
                        min_rebootstrap_cnt = 1;
                        break;
                case 3:
                        min_rebootstrap_cnt = 65;
                        break;
                default:
                        break;
                }
                rebootstrap_cnt = rd_atomic32_get(
                    &do_test_kip1102_rebootstrap_cases_rebootstrap_cnt);
                return rebootstrap_cnt < min_rebootstrap_cnt;
        }
        return rd_false;
}

/**
 * @brief KIP-1102: Re-bootstrap test cases.
 *        We set down one broker and we trigger a series of metadata request
 *        errors.
 *        When `metadata.recovery.rebootstrap.trigger.ms` is reached a
 *        single re-bootstrap should be triggered if the error is not
 *        `REBOOTSTRAP_REQUIRED`. In the latter case a re-bootstrap should
 *        be started on every returned error.
 *        We check the number of re-bootstrap sequences started from the log
 *        and the number of brokers returned by the metadata call should
 *        eventually be the initial one when broker is restarted.
 *
 *        - variation 0: an `UNKNOWN` error is returned from each metadata call.
 *        - variation 1: a `REBOOTSTRAP_REQUIRED` error is returned from
 *                       each metadata call.
 *        - variation 2: same as #0 but broker isn't restarted.
 *        - variation 3: same as #1 but broker isn't restarted.
 */
static void do_test_kip1102_rebootstrap_cases(int variation) {
        int rebootstrap_cnt = 0, expected_rebootstrap_cnt = 1,
            expected_min_rebootstrap_cnt = expected_rebootstrap_cnt;

        SUB_TEST_QUICK(
            "%s, %s",
            variation % 2 == 0 ? "metadata.recovery.rebootstrap.trigger.ms"
                               : "\"re-bootstrap required\" error code",
            variation / 2 == 0 ? "broker restarted" : "broker not restarted");

        do_test_kip1102_rebootstrap_cases_variation = variation;
        rd_atomic32_init(&do_test_kip1102_rebootstrap_cases_rebootstrap_cnt, 0);
        if (variation % 2 == 1) {
                /* It's possible multiple consecutive error responses cause a
                 * single re-bootstrap sequence because of the
                 * timer activation. */
                expected_min_rebootstrap_cnt = 65;
                expected_rebootstrap_cnt     = 70;
        }

        int32_t expected_brokers_cnt[] = {5, 5, 4, 5};

        int32_t expected_broker_ids[][5] = {
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5},
            {2, 3, 4, 5},
            {1, 2, 3, 4, 5},
        };

        int32_t actions[][2] = {{TEST_ACTION_SET_GROUP_COORDINATOR, 5},
                                {TEST_ACTION_SET_DOWN_BROKER, 1},
                                {TEST_ACTION_SET_UP_BROKER, 1}};

        do_test_add_remove_brokers0(
            5, actions,
            variation / 2 == 0 ? RD_ARRAY_SIZE(actions)
                               : RD_ARRAY_SIZE(actions) - 1,
            expected_broker_ids, expected_brokers_cnt,
            do_test_kip1102_rebootstrap_cases_edit_configuration_cb, NULL,
            do_test_kip1102_rebootstrap_cases_after_action_cb);

        rebootstrap_cnt =
            rd_atomic32_get(&do_test_kip1102_rebootstrap_cases_rebootstrap_cnt);
        TEST_ASSERT(expected_min_rebootstrap_cnt <= rebootstrap_cnt &&
                        rebootstrap_cnt <= expected_rebootstrap_cnt,
                    "Expected re-bootstrap count to be "
                    "between %d and %d, got %d",
                    expected_min_rebootstrap_cnt, expected_rebootstrap_cnt,
                    rebootstrap_cnt);

        rd_free(log_interceptor);
        allowed_errors = NULL;
        SUB_TEST_PASS();
}

int main_0151_purge_brokers_mock(int argc, char **argv) {
        int i;

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_add_same_broker_id();

        do_test_replace_with_new_cluster();

        do_test_cluster_roll();

        do_test_remove_then_add();

        do_test_down_then_up_no_rebootstrap_loop();

        for (i = 0; i < 3; i++) {
                do_test_kip899_rebootstrap_cases(i);
        }

        for (i = 0; i < 4; i++) {
                do_test_kip1102_rebootstrap_cases(i);
        }

        return 0;
}
