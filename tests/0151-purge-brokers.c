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

        /* A re-bootstrap sequence requires reaching the "all brokers down"
         * state again, that is a failed connection attempt to every broker
         * since the previous sequence. The learned broker is decommissioned
         * by the first sequence, so only the bootstrap broker remains and
         * each of its failed attempts starts a new sequence: those are paced
         * by `reconnect.backoff.ms` (100ms, doubling up to 10s, with
         * jitter), so about 7-9 sequences fit in 6s.
         * A loop not gated by connection attempts would give hundreds:
         * allow some slack over the expected count. */
        TEST_ASSERT(
            rd_atomic32_get(
                &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt) <=
                12,
            "Expected <= 12 re-bootstrap sequences, got %d",
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

typedef enum do_test_kip899_rebootstrap_cases_variation_t {
        /* re-bootstrap is enabled and triggered. */
        DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_ENABLED = 0,
        /* re-bootstrap is disabled, no re-bootstrap is executed. */
        DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_DISABLED = 1,
        /* same as #0 with brokers added after initial configuration. */
        DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_ENABLED_ADDITIONAL_BROKERS =
            2,
        DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION__CNT
} do_test_kip899_rebootstrap_cases_variation_t;

static const char *do_test_kip899_rebootstrap_cases_variation_name(
    do_test_kip899_rebootstrap_cases_variation_t variation) {
        rd_assert(
            variation >=
                DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_ENABLED &&
            variation < DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION__CNT);
        static const char *names[] = {
            "`metadata.recovery.strategy=rebootstrap`",
            "`metadata.recovery.strategy=none`",
            "`metadata.recovery.strategy=rebootstrap` "
            "with additional brokers"};
        return names[variation];
}

/**
 * @brief `do_test_kip899_rebootstrap_cases` test variation.
 */
static do_test_kip899_rebootstrap_cases_variation_t
    do_test_kip899_rebootstrap_cases_variation;

/**
 * @brief Addition brokers to set in variation
 * REBOOTSTRAP_ENABLED_ADDITIONAL_BROKERS
 */
static char *do_test_kip899_rebootstrap_cases_additional_brokers;

/**
 * @brief Edit configuration by:
 *        - setting `metadata.recovery.strategy` to `none` to
 *          avoid re-bootstrapping when variation == REBOOTSTRAP_DISABLED
 *        - setting `bootstrap.servers` to the last two brokers
 *          when variation == REBOOTSTRAP_ENABLED_ADDITIONAL_BROKERS and adding
 * first three after the first action
 */
static rd_kafka_type_t
do_test_kip899_rebootstrap_cases_edit_configuration_cb(rd_kafka_conf_t *conf) {
        char *bootstraps = test_conf_get(conf, "bootstrap.servers");
        switch (do_test_kip899_rebootstrap_cases_variation) {
        case DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_DISABLED:
                TEST_SAY("Disabling re-bootstrapping\n");
                test_conf_set(conf, "metadata.recovery.strategy", "none");
                break;
        case DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_ENABLED_ADDITIONAL_BROKERS: {
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
 * @sa `do_test_kip899_rebootstrap_cases_variation_t`
 */
static void do_test_kip899_rebootstrap_cases(
    do_test_kip899_rebootstrap_cases_variation_t variation) {
        SUB_TEST_QUICK(
            "%s", do_test_kip899_rebootstrap_cases_variation_name(variation));

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
        if (variation ==
            DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_DISABLED) {
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

typedef enum do_test_kip1102_rebootstrap_cases_variation_t {
        /* An `UNKNOWN` error is returned from each metadata call. */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR = 0,
        /* A `REBOOTSTRAP_REQUIRED` error is returned from each metadata call.
         */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED = 1,
        /* Same as TRANSPORT_ERROR but broker isn't restarted. */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR_NO_RESTART =
            2,
        /* Same as REBOOTSTRAP_REQUIRED but broker isn't restarted. */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED_NO_RESTART =
            3,
        /* A non-`REBOOTSTRAP_REQUIRED` top level error is returned from each
         * metadata call, leaving the connections up. The re-bootstrap is
         * triggered by `metadata.recovery.rebootstrap.trigger.ms`, like
         * TRANSPORT_ERROR, but with the learned brokers still connected. */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TOP_LEVEL_ERROR = 4,
        /* Same as TOP_LEVEL_ERROR but broker isn't restarted. */
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TOP_LEVEL_ERROR_NO_RESTART =
            5,
        DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION__CNT
} do_test_kip1102_rebootstrap_cases_variation_t;

/**
 * @brief Does \p variation inject the `REBOOTSTRAP_REQUIRED` error code, that
 *        is expected to start a re-bootstrap sequence on each response?
 */
static rd_bool_t do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        return variation ==
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED ||
               variation ==
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED_NO_RESTART;
}

/**
 * @brief Is the broker set up again as the last action of \p variation ?
 */
static rd_bool_t do_test_kip1102_rebootstrap_cases_restarts_broker(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        return variation ==
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR ||
               variation ==
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED ||
               variation ==
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TOP_LEVEL_ERROR;
}

/**
 * @brief Do the connections to the learned brokers stay up in \p variation ?
 *
 *        Only then is an inert re-bootstrap observable: with the learned
 *        brokers down the client reaches a bootstrap broker through the
 *        "all brokers down" path regardless.
 */
static rd_bool_t do_test_kip1102_rebootstrap_cases_connections_stay_up(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        return variation !=
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR &&
               variation !=
                   DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR_NO_RESTART;
}

/**
 * @brief `do_test_kip1102_rebootstrap_cases` test variation.
 */
static do_test_kip1102_rebootstrap_cases_variation_t
    do_test_kip1102_rebootstrap_cases_variation;

/**
 * @brief Number of re-bootstrap sequences started.
 */
static rd_atomic32_t do_test_kip1102_rebootstrap_cases_rebootstrap_cnt;

/**
 * @brief Id of a broker that is removed from Metadata responses while still
 *        listening and still present in `bootstrap.servers`, so that it can
 *        only ever be reached through a bootstrap (`RD_KAFKA_CONFIGURED`)
 *        broker object and never as a learned one. A Metadata request
 *        arriving there proves the re-bootstrap sequence didn't just start
 *        but actually resulted in a bootstrap broker being used.
 *
 *        -1 when the variation doesn't use one.
 */
static int32_t do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id = -1;

/**
 * @brief Highest number of Metadata requests seen on the bootstrap-only
 *        broker. Sampled while the mock cluster is still alive, as the
 *        test harness destroys it before returning.
 */
static rd_atomic32_t
    do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt;

/**
 * @brief Whether request tracking was already started for this variation.
 */
static rd_bool_t do_test_kip1102_rebootstrap_cases_tracking_started;

/**
 * @brief Returns the "host:port" listener of broker \p broker_id in
 *        \p mcluster , as a newly allocated string.
 *
 *        `rd_kafka_mock_cluster_bootstraps()` lists the brokers in id order,
 *        so entry `broker_id - 1` is the wanted one.
 */
static char *do_test_kip1102_rebootstrap_cases_broker_listener(
    rd_kafka_mock_cluster_t *mcluster,
    int32_t broker_id) {
        const char *bootstraps = rd_kafka_mock_cluster_bootstraps(mcluster);
        const char *start      = bootstraps;
        const char *end;
        int32_t i;

        for (i = 1; i < broker_id; i++) {
                start = strchr(start, ',');
                TEST_ASSERT(start,
                            "Broker %" PRId32 " not in bootstraps \"%s\"",
                            broker_id, bootstraps);
                start++;
        }

        end = strchr(start, ',');
        return end ? rd_strndup(start, (size_t)(end - start))
                   : rd_strdup(start);
}

static rd_bool_t
do_test_kip1102_rebootstrap_cases_is_metadata_to_bootstrap_only(
    rd_kafka_mock_request_t *request,
    void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Metadata &&
               rd_kafka_mock_request_id(request) ==
                   do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id;
}

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
         * ERR__TRANSPORT is returned
         * so the rebootstrap timer isn't reset. */
        test_conf_set(conf, "metadata.recovery.rebootstrap.trigger.ms", "5000");
        /* Avoid Head Of Line blocking from fetch requests for predictable
         * timing */
        test_conf_set(conf, "fetch.wait.max.ms", "10");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_kip1102_rebootstrap_cases_log_cb, debug_contexts);

        if (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id != -1) {
                int32_t id =
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id;
                char *listener;

                /* `cluster` is already created at this point. Hide this
                 * broker from Metadata responses: it keeps listening, so it
                 * becomes reachable only as a bootstrap broker and never as
                 * a learned one. Replica assignment already skips brokers
                 * that aren't in metadata and clamps the replication factor
                 * to the eligible count, so the expected learned broker set
                 * is unaffected. */
                rd_kafka_mock_broker_remove_from_metadata(cluster, id);

                /* Make it the *only* bootstrap server, so that any
                 * re-bootstrap has to reach it. Otherwise the client picks
                 * a random one of the cluster's bootstrap entries after
                 * decommissioning the learned brokers and the check would
                 * be flaky, in particular for the variations that trigger
                 * a single re-bootstrap sequence. */
                listener = do_test_kip1102_rebootstrap_cases_broker_listener(
                    cluster, id);
                TEST_SAY("Using broker %" PRId32
                         " (%s) as the only bootstrap server\n",
                         id, listener);
                test_conf_set(conf, "bootstrap.servers", listener);
                rd_free(listener);
        }
        return RD_KAFKA_CONSUMER;
}

static rd_kafka_resp_err_t
    do_test_kip1102_rebootstrap_cases_allowed_errors_transport[] = {
        RD_KAFKA_RESP_ERR__TRANSPORT,
        RD_KAFKA_RESP_ERR_NO_ERROR,
};
static rd_kafka_resp_err_t
    do_test_kip1102_rebootstrap_cases_allowed_errors_rebootstrap_required[] = {
        RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED,
        RD_KAFKA_RESP_ERR_NO_ERROR,
};
static rd_kafka_resp_err_t
    do_test_kip1102_rebootstrap_cases_allowed_errors_top_level[] = {
        RD_KAFKA_RESP_ERR_INVALID_REQUEST,
        RD_KAFKA_RESP_ERR_NO_ERROR,
};

/**
 * @brief The error injected in Metadata responses for \p variation .
 */
static rd_kafka_resp_err_t *do_test_kip1102_rebootstrap_cases_allowed_errors(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        if (do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(variation))
                return
                    do_test_kip1102_rebootstrap_cases_allowed_errors_rebootstrap_required;
        if (do_test_kip1102_rebootstrap_cases_connections_stay_up(variation))
                return do_test_kip1102_rebootstrap_cases_allowed_errors_top_level;
        return do_test_kip1102_rebootstrap_cases_allowed_errors_transport;
}

/**
 * @brief After setting down one broker, we trigger a series of metadata
 *        error to cause a re-bootstrap because of
 *        `metadata.recovery.rebootstrap.trigger.ms` or directly with the
 *        dedicated error code.
 */
static rd_bool_t
do_test_kip1102_rebootstrap_cases_after_action_cb(rd_kafka_t **rkp,
                                                  int action) {
        if (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id != -1 &&
            cluster) {
                /* Sample while the cluster is alive: the harness destroys
                 * it before returning to the test function. */
                size_t cnt = test_mock_get_matching_request_cnt(
                    cluster,
                    do_test_kip1102_rebootstrap_cases_is_metadata_to_bootstrap_only,
                    NULL);
                if ((int32_t)cnt >
                    rd_atomic32_get(
                        &do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt))
                        rd_atomic32_set(
                            &do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt,
                            (int32_t)cnt);
        }

        if (action == 0) {
                /* First action: set the error codes */
                int i;
                TEST_ASSERT(cluster != NULL);
                allowed_errors = do_test_kip1102_rebootstrap_cases_allowed_errors(
                    do_test_kip1102_rebootstrap_cases_variation);
                /* A request is made every 100 ms: 7s */
                for (i = 0; i < 70; i++)
                        rd_kafka_mock_push_request_errors(
                            cluster, RD_KAFKAP_Metadata, 1, allowed_errors[0]);

                if (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id !=
                        -1 &&
                    !do_test_kip1102_rebootstrap_cases_tracking_started) {
                        /* Start tracking only now: this clears the request
                         * list, discarding the initial bootstrap requests
                         * which legitimately may have gone to the
                         * bootstrap-only broker. */
                        rd_kafka_mock_start_request_tracking(cluster);
                        do_test_kip1102_rebootstrap_cases_tracking_started =
                            rd_true;
                }

        } else if (action == 1) {
                /* Second action: in case there's no third action await
                 * enough re-bootstrap logs are seen. */
                int rebootstrap_cnt, min_rebootstrap_cnt = 0;
                switch (do_test_kip1102_rebootstrap_cases_variation) {
                case DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR_NO_RESTART:
                        min_rebootstrap_cnt = 1;
                        break;
                case DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_REQUIRED_NO_RESTART:
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
 * @sa `do_test_kip1102_rebootstrap_cases_variation_t`
 */
static void do_test_kip1102_rebootstrap_cases(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        int rebootstrap_cnt = 0, expected_rebootstrap_cnt = 1,
            expected_min_rebootstrap_cnt = expected_rebootstrap_cnt;

        SUB_TEST_QUICK(
            "%s, %s",
            do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(variation)
                ? "\"re-bootstrap required\" error code"
                : do_test_kip1102_rebootstrap_cases_connections_stay_up(
                      variation)
                    ? "metadata.recovery.rebootstrap.trigger.ms, "
                      "connections up"
                    : "metadata.recovery.rebootstrap.trigger.ms",
            do_test_kip1102_rebootstrap_cases_restarts_broker(variation)
                ? "broker restarted"
                : "broker not restarted");

        do_test_kip1102_rebootstrap_cases_variation = variation;
        rd_atomic32_init(&do_test_kip1102_rebootstrap_cases_rebootstrap_cnt, 0);
        rd_atomic32_init(
            &do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt, 0);
        do_test_kip1102_rebootstrap_cases_tracking_started         = rd_false;
        do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id = -1;

        if (do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                variation)) {
                /* REBOOTSTRAP_REQUIRED error code cases:
                 * A re-bootstrap is expected for each error response.
                 * It's possible multiple consecutive error responses cause a
                 * single re-bootstrap sequence because of the
                 * timer activation. */
                expected_min_rebootstrap_cnt = 65;
                expected_rebootstrap_cnt     = 70;
        }

        if (do_test_kip1102_rebootstrap_cases_connections_stay_up(variation)) {
                /* The learned brokers stay connected here, so a re-bootstrap
                 * that starts but never actually reaches a bootstrap broker
                 * is observable. Add a 6th broker only reachable as a
                 * bootstrap server.
                 *
                 * Not done for the ERR__TRANSPORT variations, where the mock
                 * closes the connections and the client would reach a
                 * bootstrap broker via the "all brokers down" path anyway. */
                do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id = 6;
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
            do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id == -1
                ? 5
                : 6,
            actions,
            do_test_kip1102_rebootstrap_cases_restarts_broker(variation)
                ? RD_ARRAY_SIZE(actions)
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

        if (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id != -1) {
                /* Starting the sequence isn't enough: it must result in a
                 * bootstrap broker actually being queried, otherwise the
                 * client keeps asking the very brokers that reported its
                 * metadata as stale. */
                int32_t bootstrap_only_metadata_cnt = rd_atomic32_get(
                    &do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt);
                TEST_ASSERT(
                    bootstrap_only_metadata_cnt > 0,
                    "Expected at least one Metadata request to the "
                    "bootstrap-only broker %" PRId32
                    " after %d re-bootstrap sequence(s), got %" PRId32,
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id,
                    rebootstrap_cnt, bootstrap_only_metadata_cnt);
        }

        rd_free(log_interceptor);
        allowed_errors = NULL;
        SUB_TEST_PASS();
}

/**
 * @brief Run all KIP-899 re-bootstrap cases variations.
 */
static void do_test_kip899_rebootstrap_cases_variations(void) {
        int i;
        for (i = DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_ENABLED;
             i < DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION__CNT; i++) {
                do_test_kip899_rebootstrap_cases(i);
        }
}

/**
 * @brief Run all KIP-1102 re-bootstrap cases variations.
 */
static void do_test_kip1102_rebootstrap_cases_variations(void) {
        int i;
        for (i = DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION_TRANSPORT_ERROR;
             i < DO_TEST_KIP1102_REBOOTSTRAP_CASES_VARIATION__CNT; i++) {
                do_test_kip1102_rebootstrap_cases(i);
        }
}

int main_0151_purge_brokers_mock(int argc, char **argv) {

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_add_same_broker_id();

        do_test_replace_with_new_cluster();

        do_test_cluster_roll();

        do_test_remove_then_add();

        do_test_down_then_up_no_rebootstrap_loop();

        do_test_kip899_rebootstrap_cases_variations();

        do_test_kip1102_rebootstrap_cases_variations();

        return 0;
}
