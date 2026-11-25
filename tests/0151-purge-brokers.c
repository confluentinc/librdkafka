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

#ifndef _WIN32
#include <netdb.h>
#else
#define WIN32_MEAN_AND_LEAN
#include <winsock2.h>
#include <ws2ipdef.h>
#include <ws2tcpip.h>
#endif

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

static rd_atomic32_t
    do_test_down_then_up_no_rebootstrap_loop_all_brokers_down_cnt;

/** Duration in seconds the only broker is kept down. */
static int do_test_down_then_up_no_rebootstrap_loop_outage_s;

/**
 * @brief Error callback that counts the `ALL_BROKERS_DOWN` errors
 *        reported to the application.
 */
static void
do_test_down_then_up_no_rebootstrap_loop_error_cb(rd_kafka_t *rk,
                                                  int err,
                                                  const char *reason,
                                                  void *opaque) {
        if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                rd_atomic32_add(
                    &do_test_down_then_up_no_rebootstrap_loop_all_brokers_down_cnt,
                    1);
                TEST_SAY("Received ALL_BROKERS_DOWN: %s\n", reason);
        } else if (err != RD_KAFKA_RESP_ERR__TRANSPORT) {
                /* Connection failures are expected while the broker is down,
                 * anything else isn't. */
                TEST_FAIL("Unexpected error: %s: %s", rd_kafka_err2name(err),
                          reason);
        }
}

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

        /* Avoid it being changed through test.conf */
        test_conf_set(conf, "reconnect.backoff.max.ms", "10000");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_down_then_up_no_rebootstrap_loop_log_cb,
            debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_down_then_up_no_rebootstrap_loop_error_cb);
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
 * @brief Await the outage duration after setting the broker down
 *        to check for re-bootstrap sequences.
 *        Errors are only delivered to the error callback when the handle is
 *        polled, so poll while waiting and after every other action too.
 */
static rd_bool_t
do_test_down_then_up_no_rebootstrap_loop_after_action_cb(rd_kafka_t **rkp,
                                                         int action) {
        if (action == 1) {
                int64_t abs_timeout_us =
                    test_clock() +
                    do_test_down_then_up_no_rebootstrap_loop_outage_s *
                        1000000LL;
                while (test_clock() < abs_timeout_us)
                        rd_kafka_poll(*rkp, 100);
        } else {
                rd_kafka_poll(*rkp, 0);
        }
        return rd_false;
}

/**
 * @brief Test setting down a broker and then setting it up again.
 *        It shouldn't cause a loop of re-bootstrap sequences.
 *
 * @param outage_s Seconds the broker is kept down.
 */
static void do_test_down_then_up_no_rebootstrap_loop(int outage_s) {
        int32_t actual_rebootstrap_sequence_cnt, actual_all_brokers_down_cnt;
        int32_t max_rebootstrap_sequence_cnt, expected_all_brokers_down_cnt;
        SUB_TEST_QUICK("%ds outage", outage_s);
        do_test_down_then_up_no_rebootstrap_loop_outage_s = outage_s;
        rd_atomic32_init(
            &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt,
            0);
        rd_atomic32_init(
            &do_test_down_then_up_no_rebootstrap_loop_all_brokers_down_cnt, 0);

        /* Action 2 sets the only broker down, so the "all brokers down" state
         * is reached and a re-bootstrap sequence starts immediately,
         * decommissioning the learned broker. With every broker down no
         * Metadata response can succeed, so the learned broker list cannot be
         * rebuilt and stays empty until the broker is set up again. */
        int32_t expected_brokers_cnt[] = {1, 1, 0, 1};

        int32_t expected_broker_ids[][5] = {{1}, {1}, {0}, {1}};

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
         * each of its failed attempts starts a new sequence.
         *
         * The attempts are paced by the reconnect backoff, that keeps
         * growing across the re-bootstrap sequences while no broker is up,
         * see do_test_reconnect_backoff_all_brokers_down(). A looser bound
         * is checked here: `rd_kafka_connect_any()` is gated by
         * `sparse_connect_intvl` = max(11, min(reconnect.backoff.ms / 2,
         * 1000)), 50ms by default, so at most outage_ms / 50 sequences fit
         * in the outage window (100 for 5s). A loop not gated by connection
         * attempts would exceed that. */
        max_rebootstrap_sequence_cnt    = outage_s * 1000 / 50;
        actual_rebootstrap_sequence_cnt = rd_atomic32_get(
            &do_test_down_then_up_no_rebootstrap_loop_rebootstrap_sequence_cnt);
        TEST_SAY("Found %d re-bootstrap sequences\n",
                 actual_rebootstrap_sequence_cnt);
        TEST_ASSERT(
            actual_rebootstrap_sequence_cnt <= max_rebootstrap_sequence_cnt,
            "Expected at most %d re-bootstrap sequences, got %d",
            max_rebootstrap_sequence_cnt, actual_rebootstrap_sequence_cnt);

        /* Each of those sequences reaches the "all brokers down" state, but
         * the `ALL_BROKERS_DOWN` error is reported to the application at most
         * once every `reconnect.backoff.max.ms` (10s by default): one at the
         * start of the outage and at most one more every 10s it lasts, so at
         * most outage_s / 10 + 1 in total, not one per sequence. As the
         * sequences are paced by the reconnect backoff, the attempt that
         * would report it again can fall after the outage, so fewer can be
         * reported. */
        expected_all_brokers_down_cnt = outage_s / 10 + 1;
        actual_all_brokers_down_cnt   = rd_atomic32_get(
            &do_test_down_then_up_no_rebootstrap_loop_all_brokers_down_cnt);
        TEST_SAY("Found %d ALL_BROKERS_DOWN error(s)\n",
                 actual_all_brokers_down_cnt);
        TEST_ASSERT(
            actual_all_brokers_down_cnt >= 1 &&
                actual_all_brokers_down_cnt <= expected_all_brokers_down_cnt,
            "Expected 1 to %d ALL_BROKERS_DOWN error(s) over %d "
            "re-bootstrap sequences in a %ds outage, got %d",
            expected_all_brokers_down_cnt, actual_rebootstrap_sequence_cnt,
            outage_s, actual_all_brokers_down_cnt);

        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

/** Connection attempts recorded by
 *  do_test_reconnect_backoff_all_brokers_down_log_cb(). */
static struct {
        mtx_t lock;
        int cnt;
        struct {
                char addr[64]; /**< "ipv4#127.0.0.1:port" */
                int64_t ts;    /**< test_clock() */
        } attempts[2048];
} do_test_reconnect_backoff_attempts;

/**
 * @brief Log callback that records each connection attempt
 *        ("Connecting to <addr> (<proto>) with socket <fd>") and its address.
 */
static void
do_test_reconnect_backoff_all_brokers_down_log_cb(const rd_kafka_t *rk,
                                                  int level,
                                                  const char *fac,
                                                  const char *buf) {
        const char *addr = strstr(buf, "Connecting to ");
        const char *end;
        int i;

        if (!addr)
                return;
        addr += strlen("Connecting to ");
        if (!(end = strchr(addr, ' ')))
                return;

        mtx_lock(&do_test_reconnect_backoff_attempts.lock);
        i = do_test_reconnect_backoff_attempts.cnt;
        if (i <
            (int)RD_ARRAYSIZE(do_test_reconnect_backoff_attempts.attempts)) {
                rd_snprintf(
                    do_test_reconnect_backoff_attempts.attempts[i].addr,
                    sizeof(do_test_reconnect_backoff_attempts.attempts[i].addr),
                    "%.*s", (int)(end - addr), addr);
                do_test_reconnect_backoff_attempts.attempts[i].ts =
                    test_clock();
                do_test_reconnect_backoff_attempts.cnt++;
        }
        mtx_unlock(&do_test_reconnect_backoff_attempts.lock);
}

/**
 * @brief Connection failures are expected while all brokers are down,
 *        anything else isn't.
 */
static void
do_test_reconnect_backoff_all_brokers_down_error_cb(rd_kafka_t *rk,
                                                    int err,
                                                    const char *reason,
                                                    void *opaque) {
        if (err != RD_KAFKA_RESP_ERR__TRANSPORT &&
            err != RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN)
                TEST_FAIL("Unexpected error: %s: %s", rd_kafka_err2name(err),
                          reason);
}

/**
 * @name Connection attempts, state changes and backoff restores recorded from
 *       the broker debug logs, for the reconnect backoff tests below.
 * @{
 */

/** Kind of a recorded event. */
typedef enum {
        RECONNECT_EV_ATTEMPT, /**< "<broker>: Connecting to ..." */
        RECONNECT_EV_UP,      /**< "<broker>: Broker changed state ... -> UP" */
        RECONNECT_EV_RESTORE, /**< "<broker>: Continuing the reconnect
                               *   backoff ..." */
} reconnect_ev_kind_t;

static struct {
        mtx_t lock;
        int cnt;          /**< Recorded events */
        int rebootstraps; /**< "Starting re-bootstrap sequence" */
        struct {
                reconnect_ev_kind_t kind;
                char broker[128]; /**< Broker name, e.g. "127.0.0.1:1234/2" */
                int64_t ts;       /**< test_clock() */
        } ev[4096];
} reconnect_ev;

static void reconnect_ev_init(void) {
        mtx_init(&reconnect_ev.lock, mtx_plain);
        reconnect_ev.cnt          = 0;
        reconnect_ev.rebootstraps = 0;
}

static void reconnect_ev_destroy(void) {
        mtx_destroy(&reconnect_ev.lock);
}

/**
 * @brief Log interceptor callback recording the events above. The broker
 *        name is the one librdkafka prefixes its broker debug logs with,
 *        after the thread name: "[thrd:...]: <broker>: <message>".
 */
static void reconnect_ev_log_cb(const rd_kafka_t *rk,
                                int level,
                                const char *fac,
                                const char *buf) {
        static const char up[] = "-> UP";
        size_t len             = strlen(buf);
        reconnect_ev_kind_t kind;
        const char *msg, *start;

        if ((msg = strstr(buf, ": Connecting to ")))
                kind = RECONNECT_EV_ATTEMPT;
        else if ((msg = strstr(buf, ": Continuing the reconnect backoff")))
                kind = RECONNECT_EV_RESTORE;
        else if ((msg = strstr(buf, ": Broker changed state ")) &&
                 len >= sizeof(up) - 1 &&
                 !strcmp(buf + len - (sizeof(up) - 1), up))
                kind = RECONNECT_EV_UP;
        else {
                if (strstr(buf, "Starting re-bootstrap sequence")) {
                        mtx_lock(&reconnect_ev.lock);
                        reconnect_ev.rebootstraps++;
                        mtx_unlock(&reconnect_ev.lock);
                }
                return;
        }

        start = strstr(buf, "]: ");
        start = start && start < msg ? start + 3 : buf;

        mtx_lock(&reconnect_ev.lock);
        if (reconnect_ev.cnt < (int)RD_ARRAYSIZE(reconnect_ev.ev)) {
                int i                   = reconnect_ev.cnt++;
                reconnect_ev.ev[i].kind = kind;
                rd_snprintf(reconnect_ev.ev[i].broker,
                            sizeof(reconnect_ev.ev[i].broker), "%.*s",
                            (int)(msg - start), start);
                reconnect_ev.ev[i].ts = test_clock();
        }
        mtx_unlock(&reconnect_ev.lock);
}

/**
 * @brief Copies into \p ts, up to \p size, the times of the events of
 *        \p kind recorded in [\p since, \p until) (\p until 0: no end) for
 *        brokers whose name contains \p broker.
 *
 * @returns the number of such events, which can be more than \p size.
 */
static int reconnect_ev_get(reconnect_ev_kind_t kind,
                            const char *broker,
                            int64_t since,
                            int64_t until,
                            int64_t *ts,
                            int size) {
        int i, cnt = 0;

        mtx_lock(&reconnect_ev.lock);
        for (i = 0; i < reconnect_ev.cnt; i++) {
                if (reconnect_ev.ev[i].kind != kind ||
                    reconnect_ev.ev[i].ts < since ||
                    (until && reconnect_ev.ev[i].ts >= until) ||
                    !strstr(reconnect_ev.ev[i].broker, broker))
                        continue;
                if (cnt < size)
                        ts[cnt] = reconnect_ev.ev[i].ts;
                cnt++;
        }
        mtx_unlock(&reconnect_ev.lock);
        return cnt;
}

/** @returns the number of re-bootstrap sequences started so far. */
static int reconnect_ev_rebootstraps(void) {
        int cnt;
        mtx_lock(&reconnect_ev.lock);
        cnt = reconnect_ev.rebootstraps;
        mtx_unlock(&reconnect_ev.lock);
        return cnt;
}

/**
 * @brief Number of waits after the first connection attempt before the
 *        backoff, starting at \p backoff_ms and doubling at each attempt,
 *        reaches \p backoff_max_ms: later waits are at least 75% of
 *        \p backoff_max_ms (-25% jitter).
 */
static int reconnect_backoff_ramp_up_waits(int backoff_ms, int backoff_max_ms) {
        int waits = 0, step = backoff_ms;

        while (step < backoff_max_ms) {
                waits++;
                step *= 2;
        }
        return waits;
}

/**
 * @brief Upper bound of the connection attempts in \p duration_ms with the
 *        backoff of reconnect_backoff_ramp_up_waits(): one attempt per step
 *        until the maximum is reached, then one per minimum wait (75% of
 *        \p backoff_max_ms).
 */
static int reconnect_backoff_max_attempts(int backoff_ms,
                                          int backoff_max_ms,
                                          int duration_ms) {
        int attempts = 1, step = backoff_ms;

        while (step < backoff_max_ms) {
                attempts++;
                step *= 2;
        }
        return attempts + duration_ms / (int)(backoff_max_ms * 0.75) + 1;
}

/** @brief Sets `reconnect.backoff.ms` and `reconnect.backoff.max.ms`. */
static void reconnect_backoff_conf_set(rd_kafka_conf_t *conf,
                                       int backoff_ms,
                                       int backoff_max_ms) {
        char tmp[32];
        rd_snprintf(tmp, sizeof(tmp), "%d", backoff_ms);
        test_conf_set(conf, "reconnect.backoff.ms", tmp);
        rd_snprintf(tmp, sizeof(tmp), "%d", backoff_max_ms);
        test_conf_set(conf, "reconnect.backoff.max.ms", tmp);
}

/**
 * @brief Connect callback refusing every connection to the nodename in
 *        reconnect_refused_nodename, connecting as usual otherwise. The
 *        nodename is set before the client is created.
 */
static char reconnect_refused_nodename[128];

static int reconnect_refuse_nodename_connect_cb(int sockfd,
                                                const struct sockaddr *addr,
                                                int addrlen,
                                                const char *id,
                                                void *opaque) {
        if (!strcmp(id, reconnect_refused_nodename))
                return ECONNREFUSED;

        if (connect(sockfd, addr, addrlen) == RD_SOCKET_ERROR) {
                int err = rd_socket_errno;
                if (err != EINPROGRESS
#ifdef _WIN32
                    && err != WSAEWOULDBLOCK
#endif
                )
                        return err;
        }
        return 0;
}

/** @} */

/**
 * @brief While no broker is up, connection attempts must be paced by the
 *        reconnect backoff, growing up to `reconnect.backoff.max.ms`, across
 *        the re-bootstrap sequences that start each time all brokers are
 *        down. The bootstrap brokers are the addresses of a destroyed mock
 *        cluster, so connections are refused at once.
 *
 * @param type Client type.
 * @param sparse_connections `enable.sparse.connections` value.
 */
static void
do_test_reconnect_backoff_all_brokers_down(rd_kafka_type_t type,
                                           rd_bool_t sparse_connections) {
        const int reconnect_backoff_ms = 100;
        /* Above the 1s period of the sparse connection keepalive, so that
         * a flat reconnect rate driven by it can't pass for a backoff
         * that reached its maximum. */
        const int reconnect_backoff_max_ms = 2000;
        const int duration_ms              = 9000;
        /* The backoff doubles at each connection attempt: the waits after
         * the first ramp_up_waits ones are at its maximum. Counted in waits,
         * not in time: the re-bootstrap sequences can make the time between
         * attempts longer than the backoff. */
        const int ramp_up_waits = reconnect_backoff_ramp_up_waits(
            reconnect_backoff_ms, reconnect_backoff_max_ms);
        /* Each wait is at least 75% of the backoff (-25% jitter). */
        const int min_gap_ms =
            (int)(reconnect_backoff_max_ms * 0.75) - 250 /* timer slack */;
        const int max_attempts_per_broker = reconnect_backoff_max_attempts(
            reconnect_backoff_ms, reconnect_backoff_max_ms, duration_ms);
        const char *debug_contexts[2] = {"broker", NULL};
        const char *bootstraps;
        char *dead_bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char *addrs[2];
        int addr_cnt = 0, i, j;
        int64_t abs_timeout, first_ts = 0;

        SUB_TEST_QUICK("%s, enable.sparse.connections=%s",
                       type == RD_KAFKA_PRODUCER ? "producer" : "consumer",
                       sparse_connections ? "true" : "false");

        /* Bootstrap brokers that refuse connections: the addresses of a
         * destroyed mock cluster. */
        mcluster        = test_mock_cluster_new(2, &bootstraps);
        dead_bootstraps = rd_strdup(bootstraps);
        test_mock_cluster_destroy(mcluster);

        mtx_init(&do_test_reconnect_backoff_attempts.lock, mtx_plain);
        do_test_reconnect_backoff_attempts.cnt = 0;

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", dead_bootstraps);
        reconnect_backoff_conf_set(conf, reconnect_backoff_ms,
                                   reconnect_backoff_max_ms);
        test_conf_set(conf, "enable.sparse.connections",
                      sparse_connections ? "true" : "false");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_reconnect_backoff_all_brokers_down_log_cb,
            debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        if (type == RD_KAFKA_CONSUMER)
                test_conf_set(conf, "group.id", "mygroup");

        rk = test_create_handle(type, conf);
        if (type == RD_KAFKA_CONSUMER) {
                test_consumer_subscribe(rk, "mytopic");
        } else {
                /* A pending message keeps the producer asking for metadata. */
                TEST_CALL_ERR__(rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC("mytopic"), RD_KAFKA_V_VALUE("hi", 2),
                    RD_KAFKA_V_END));
        }

        abs_timeout = test_clock() + duration_ms * 1000LL;
        while (test_clock() < abs_timeout) {
                const struct rd_kafka_metadata *metadata;

                /* Each request asks for a cluster connection through
                 * rd_kafka_connect_any(). */
                if (!rd_kafka_metadata(rk, 0, NULL, &metadata, 100))
                        rd_kafka_metadata_destroy(metadata);

                if (type == RD_KAFKA_CONSUMER) {
                        rd_kafka_message_t *rkmessage =
                            rd_kafka_consumer_poll(rk, 0);
                        RD_IF_FREE(rkmessage, rd_kafka_message_destroy);
                } else {
                        rd_kafka_poll(rk, 0);
                }
        }

        if (type == RD_KAFKA_PRODUCER)
                rd_kafka_purge(rk, RD_KAFKA_PURGE_F_QUEUE);
        rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        mtx_lock(&do_test_reconnect_backoff_attempts.lock);
        TEST_ASSERT(do_test_reconnect_backoff_attempts.cnt > 0,
                    "Expected connection attempts");
        first_ts = do_test_reconnect_backoff_attempts.attempts[0].ts;

        /* Per broker address: the address is kept by the bootstrap brokers
         * that the re-bootstrap sequences re-create. */
        for (i = 0; i < do_test_reconnect_backoff_attempts.cnt; i++) {
                const char *addr =
                    do_test_reconnect_backoff_attempts.attempts[i].addr;
                for (j = 0; j < addr_cnt && strcmp(addrs[j], addr); j++)
                        ;
                if (j == addr_cnt) {
                        TEST_ASSERT(addr_cnt < (int)RD_ARRAYSIZE(addrs),
                                    "Unexpected broker address %s", addr);
                        addrs[addr_cnt++] = rd_strdup(addr);
                }
        }

        for (j = 0; j < addr_cnt; j++) {
                int cnt         = 0;
                int64_t prev_ts = 0;

                for (i = 0; i < do_test_reconnect_backoff_attempts.cnt; i++) {
                        int64_t ts =
                            do_test_reconnect_backoff_attempts.attempts[i].ts;

                        if (strcmp(addrs[j], do_test_reconnect_backoff_attempts
                                                 .attempts[i]
                                                 .addr))
                                continue;

                        cnt++;
                        /* This attempt follows wait number cnt - 1. */
                        if (prev_ts && cnt - 1 > ramp_up_waits)
                                TEST_ASSERT(
                                    (ts - prev_ts) / 1000 >= min_gap_ms,
                                    "%s: expected the reconnect backoff to "
                                    "keep its maximum after %d waits, got a "
                                    "%" PRId64
                                    "ms wait before the attempt at %" PRId64
                                    "ms",
                                    addrs[j], ramp_up_waits,
                                    (ts - prev_ts) / 1000,
                                    (ts - first_ts) / 1000);
                        prev_ts = ts;
                }

                TEST_SAY("%s: %d connection attempt(s) in %dms\n", addrs[j],
                         cnt, duration_ms);
                TEST_ASSERT(cnt <= max_attempts_per_broker,
                            "%s: expected at most %d connection attempts in "
                            "%dms, got %d",
                            addrs[j], max_attempts_per_broker, duration_ms,
                            cnt);
                rd_free(addrs[j]);
        }
        mtx_unlock(&do_test_reconnect_backoff_attempts.lock);
        mtx_destroy(&do_test_reconnect_backoff_attempts.lock);

        rd_free(dead_bootstraps);
        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

/** While set, do_test_reconnect_backoff_reset_connect_cb() refuses every
 *  connection: the mock broker is down. */
static rd_atomic32_t do_test_reconnect_backoff_reset_refuse;

/** Time of the first connection let through by
 *  do_test_reconnect_backoff_reset_connect_cb() once
 *  do_test_reconnect_backoff_reset_reconnect_armed is set, or 0.
 *  Protected by do_test_reconnect_backoff_attempts.lock. */
static int64_t do_test_reconnect_backoff_reset_reconnect_ts;
static rd_bool_t do_test_reconnect_backoff_reset_reconnect_armed;

/**
 * @brief Connect callback refusing every connection while the mock broker is
 *        down, as connections to a down mock broker hang on macOS.
 */
static int
do_test_reconnect_backoff_reset_connect_cb(int sockfd,
                                           const struct sockaddr *addr,
                                           int addrlen,
                                           const char *id,
                                           void *opaque) {
        if (rd_atomic32_get(&do_test_reconnect_backoff_reset_refuse))
                return ECONNREFUSED;

        mtx_lock(&do_test_reconnect_backoff_attempts.lock);
        if (do_test_reconnect_backoff_reset_reconnect_armed &&
            !do_test_reconnect_backoff_reset_reconnect_ts)
                do_test_reconnect_backoff_reset_reconnect_ts = test_clock();
        mtx_unlock(&do_test_reconnect_backoff_attempts.lock);

        if (connect(sockfd, addr, addrlen) == RD_SOCKET_ERROR) {
                int err = rd_socket_errno;
                if (err != EINPROGRESS
#ifdef _WIN32
                    && err != WSAEWOULDBLOCK
#endif
                )
                        return err;
        }
        return 0;
}

/**
 * @brief Calls rd_kafka_metadata() for \p duration_ms, or until it succeeds
 *        if \p until_success is true, like an application that keeps asking
 *        for metadata: while no broker is up, each request asks for a cluster
 *        connection through `rd_kafka_connect_any()`.
 *
 * @returns rd_true if a request succeeded.
 */
static rd_bool_t
do_test_reconnect_backoff_reset_metadata_loop(rd_kafka_t *rk,
                                              int duration_ms,
                                              rd_bool_t until_success) {
        int64_t abs_timeout = test_clock() + duration_ms * 1000LL;

        while (test_clock() < abs_timeout) {
                const struct rd_kafka_metadata *metadata;

                if (!rd_kafka_metadata(rk, 0, NULL, &metadata, 100)) {
                        rd_kafka_metadata_destroy(metadata);
                        if (until_success)
                                return rd_true;
                }
                rd_kafka_poll(rk, 0);
        }
        return rd_false;
}

/**
 * @brief The reconnect backoff restarts from `reconnect.backoff.ms` only once
 *        it's stale (see rd_kafka_broker_update_reconnect_backoff()), not
 *        when the connection reaches UP.
 *
 *        After an outage long enough to reach `reconnect.backoff.max.ms`, the
 *        client reconnects, then a second outage starts:
 *        - \p stable: after a long enough connection, the second outage's
 *          waits start again from `reconnect.backoff.ms`;
 *        - otherwise, right after reconnecting, they continue at
 *          `reconnect.backoff.max.ms`.
 */
static void do_test_reconnect_backoff_after_reconnection(rd_bool_t stable) {
        const int backoff_ms = 100, backoff_max_ms = 2000;
        /* The first outage lasts until a wait at the maximum backoff: one
         * attempt more than the waits it takes to reach it, plus one. */
        const int outage1_attempts =
            reconnect_backoff_ramp_up_waits(backoff_ms, backoff_max_ms) + 2;
        /* The staleness rule restarts the backoff once the reconnect time
         * set at the last attempt, at most backoff_max_ms after it, is more
         * than backoff_max_ms in the past. */
        const int stable_ms  = 2 * backoff_max_ms + 1000;
        const int outage2_ms = stable ? 3000 : 2 * backoff_max_ms + 1000;
        /* Starting again from backoff_ms, the first two waits are at most
         * 150% of backoff_ms and of its double; continuing at the maximum,
         * each wait is at least 75% of backoff_max_ms. */
        const int fresh_max_wait_ms   = (int)(backoff_ms * 1.5) * 2 + 250;
        const int capped_min_wait_ms  = (int)(backoff_max_ms * 0.75) - 250;
        const char *debug_contexts[2] = {"broker", NULL};
        char bootstrap_name[128];
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int64_t outage1_ts, recover_ts, outage2_ts, reconnect_ts, ts[64],
            abs_timeout;
        int cnt;

        SUB_TEST_QUICK("%s", stable ? "stable connection"
                                    : "connection lost right away");

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_snprintf(bootstrap_name, sizeof(bootstrap_name), "%s/bootstrap",
                    bootstraps);

        mtx_init(&do_test_reconnect_backoff_attempts.lock, mtx_plain);
        do_test_reconnect_backoff_attempts.cnt          = 0;
        do_test_reconnect_backoff_reset_reconnect_ts    = 0;
        do_test_reconnect_backoff_reset_reconnect_armed = rd_false;
        rd_atomic32_init(&do_test_reconnect_backoff_reset_refuse, 0);
        reconnect_ev_init();

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        rd_kafka_conf_set_connect_cb(
            conf, do_test_reconnect_backoff_reset_connect_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, reconnect_ev_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_ASSERT(
            do_test_reconnect_backoff_reset_metadata_loop(rk, 10000, rd_true),
            "Expected the client to connect to the mock cluster");

        TEST_SAY("First outage: until %d connection attempts\n",
                 outage1_attempts);
        rd_atomic32_set(&do_test_reconnect_backoff_reset_refuse, 1);
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_down(mcluster, 1));
        outage1_ts  = test_clock();
        abs_timeout = outage1_ts + 60 * 1000 * 1000;
        do {
                do_test_reconnect_backoff_reset_metadata_loop(rk, 100,
                                                              rd_false);
                cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, bootstrap_name,
                                       outage1_ts, 0, ts, RD_ARRAYSIZE(ts));
        } while (cnt < outage1_attempts && test_clock() < abs_timeout);

        TEST_SAY("Broker up again\n");
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_up(mcluster, 1));
        mtx_lock(&do_test_reconnect_backoff_attempts.lock);
        do_test_reconnect_backoff_reset_reconnect_armed = rd_true;
        mtx_unlock(&do_test_reconnect_backoff_attempts.lock);
        recover_ts = test_clock();
        rd_atomic32_set(&do_test_reconnect_backoff_reset_refuse, 0);
        TEST_ASSERT(do_test_reconnect_backoff_reset_metadata_loop(
                        rk, backoff_max_ms + 8000, rd_true),
                    "Expected the client to reconnect after the first outage");

        if (stable) {
                TEST_SAY("Connected for %dms\n", stable_ms);
                do_test_reconnect_backoff_reset_metadata_loop(rk, stable_ms,
                                                              rd_false);
        }

        TEST_SAY("Second outage: %dms\n", outage2_ms);
        rd_atomic32_set(&do_test_reconnect_backoff_reset_refuse, 1);
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_down(mcluster, 1));
        outage2_ts = test_clock();
        do_test_reconnect_backoff_reset_metadata_loop(rk, outage2_ms, rd_false);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        mtx_lock(&do_test_reconnect_backoff_attempts.lock);
        reconnect_ts = do_test_reconnect_backoff_reset_reconnect_ts;
        mtx_unlock(&do_test_reconnect_backoff_attempts.lock);
        mtx_destroy(&do_test_reconnect_backoff_attempts.lock);

        /* The backoff reached its maximum in the first outage. */
        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, bootstrap_name, outage1_ts,
                               recover_ts, ts, RD_ARRAYSIZE(ts));
        TEST_ASSERT(cnt >= 2 && cnt <= (int)RD_ARRAYSIZE(ts),
                    "Unexpected attempt count in the first outage: %d", cnt);
        TEST_SAY("First outage: %d attempt(s), last wait %" PRId64 "ms\n", cnt,
                 (ts[cnt - 1] - ts[cnt - 2]) / 1000);
        TEST_ASSERT((ts[cnt - 1] - ts[cnt - 2]) / 1000 >= capped_min_wait_ms,
                    "Expected the backoff to reach %dms in the first outage, "
                    "got a last wait of %" PRId64 "ms",
                    backoff_max_ms, (ts[cnt - 1] - ts[cnt - 2]) / 1000);

        TEST_ASSERT(reconnect_ts > 0,
                    "Expected a connection after the first outage");
        if (!stable)
                TEST_ASSERT((outage2_ts - reconnect_ts) / 1000 <
                                capped_min_wait_ms,
                            "The second outage started %" PRId64
                            "ms after the reconnection, too late to tell "
                            "whether the backoff is continued",
                            (outage2_ts - reconnect_ts) / 1000);

        /* The bootstrap brokers that the second outage's re-bootstrap
         * sequences re-create. */
        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, bootstrap_name, outage2_ts,
                               0, ts, RD_ARRAYSIZE(ts));
        TEST_ASSERT(cnt >= 2,
                    "Expected at least 2 connection attempts to %s in the "
                    "second outage, got %d",
                    bootstrap_name, cnt);
        TEST_SAY("Second outage, %" PRId64
                 "ms after the reconnection: %d attempt(s), first wait "
                 "%" PRId64 "ms\n",
                 (outage2_ts - reconnect_ts) / 1000, cnt,
                 (ts[1] - ts[0]) / 1000);
        if (stable)
                TEST_ASSERT((ts[1] - ts[0]) / 1000 <= fresh_max_wait_ms,
                            "Expected the backoff to start again from "
                            "%dms after a connection that lasted longer than "
                            "%dms, got a first wait of %" PRId64 "ms",
                            backoff_ms, 2 * backoff_max_ms,
                            (ts[1] - ts[0]) / 1000);
        else
                TEST_ASSERT((ts[1] - ts[0]) / 1000 >= capped_min_wait_ms,
                            "Expected the backoff to continue at %dms after "
                            "a connection lost right away, got a first wait "
                            "of %" PRId64 "ms",
                            backoff_max_ms, (ts[1] - ts[0]) / 1000);

        reconnect_ev_destroy();
        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}

/**
 * @brief A learned broker that leaves a Metadata response and is added back
 *        continues its reconnect backoff, although other brokers are up.
 *
 *        Broker 2, the partition leader, refuses connections while broker 1
 *        is up. After a few attempts it's removed and added back with the
 *        same address: the new broker object must wait for the removed one's
 *        reconnect time and continue its backoff.
 */
static void do_test_reconnect_backoff_readded_broker(void) {
        const int backoff_ms = 100, backoff_max_ms = 10000;
        /* Attempts to broker 2 before it's removed: the backoff is then
         * well above backoff_ms, but below backoff_max_ms. */
        const int attempts_before     = 5;
        const char *debug_contexts[2] = {"broker", NULL};
        char bootstrap_servers[128], broker2_name[128];
        const char *bootstraps, *broker2;
        const struct rd_kafka_metadata *metadata;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int64_t ts[64], abs_timeout, t_remove, t_readd, t_last, gap_last,
            first_after, second_after;
        int broker2_port, cnt;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(2, &bootstraps);
        broker2  = strchr(bootstraps, ',') + 1;
        rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers), "%.*s",
                    (int)(broker2 - 1 - bootstraps), bootstraps);
        rd_snprintf(reconnect_refused_nodename,
                    sizeof(reconnect_refused_nodename), "%s", broker2);
        rd_snprintf(broker2_name, sizeof(broker2_name), "%s/2", broker2);
        broker2_port = atoi(strrchr(broker2, ':') + 1);
        rd_kafka_mock_topic_create(mcluster, "readded", 1, 1);
        rd_kafka_mock_partition_set_leader(mcluster, "readded", 0, 2);

        reconnect_ev_init();
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        rd_kafka_conf_set_connect_cb(conf,
                                     reconnect_refuse_nodename_connect_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, reconnect_ev_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_CALL_ERR__(rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("readded"),
                                          RD_KAFKA_V_VALUE("1", 1),
                                          RD_KAFKA_V_END));

        abs_timeout = test_clock() + 20 * 1000 * 1000;
        do {
                rd_kafka_poll(rk, 10);
                cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name, 0, 0,
                                       ts, RD_ARRAYSIZE(ts));
        } while (cnt < attempts_before && test_clock() < abs_timeout);
        TEST_ASSERT(cnt >= attempts_before,
                    "Expected %d connection attempts to %s, got %d",
                    attempts_before, broker2_name, cnt);

        /* Remove broker 2: the client removes it on the first Metadata
         * response without it. Then add it back, with the same address. */
        t_remove = test_clock();
        TEST_CALL_ERR__(rd_kafka_mock_broker_decommission(mcluster, 2));
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, NULL, &metadata, 5000));
        rd_kafka_metadata_destroy(metadata);
        TEST_CALL_ERR__(rd_kafka_mock_broker_add(mcluster, 2));
        rd_kafka_mock_broker_set_host_port(mcluster, 2, "127.0.0.1",
                                           broker2_port);
        rd_kafka_mock_partition_set_leader(mcluster, "readded", 0, 2);
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, NULL, &metadata, 5000));
        rd_kafka_metadata_destroy(metadata);
        TEST_CALL_ERR__(rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("readded"),
                                          RD_KAFKA_V_VALUE("2", 1),
                                          RD_KAFKA_V_END));
        t_readd = test_clock();

        abs_timeout = test_clock() + 30 * 1000 * 1000;
        do {
                rd_kafka_poll(rk, 10);
                cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name,
                                       t_readd, 0, ts, RD_ARRAYSIZE(ts));
        } while (cnt < 2 && test_clock() < abs_timeout);

        rd_kafka_purge(rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        TEST_ASSERT(cnt >= 2,
                    "Expected 2 connection attempts to %s after it was added "
                    "back, got %d",
                    broker2_name, cnt);
        first_after  = ts[0];
        second_after = ts[1];

        /* The last two attempts before the removal: the removed broker's
         * reconnect time is at least the last wait after its last attempt,
         * the backoff doubling at each attempt (at least 75% of the doubled
         * step, against at most 150% of the step for the last wait). */
        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name, 0, t_remove,
                               ts, RD_ARRAYSIZE(ts));
        TEST_ASSERT(cnt >= 2 && cnt <= (int)RD_ARRAYSIZE(ts),
                    "Unexpected attempt count before the removal: %d", cnt);
        t_last   = ts[cnt - 1];
        gap_last = ts[cnt - 1] - ts[cnt - 2];

        TEST_SAY(
            "%s: last wait before the removal %" PRId64
            "ms, added back after %" PRId64 "ms, then attempts after %" PRId64
            "ms and %" PRId64 "ms more\n",
            broker2_name, gap_last / 1000, (t_readd - t_last) / 1000,
            (first_after - t_last) / 1000, (second_after - first_after) / 1000);
        TEST_ASSERT(reconnect_ev_get(RECONNECT_EV_RESTORE, broker2_name,
                                     t_remove, 0, ts, RD_ARRAYSIZE(ts)) >= 1,
                    "Expected %s's backoff to be restored when it was added "
                    "back (\"Continuing the reconnect backoff\" debug log)",
                    broker2_name);
        TEST_ASSERT((t_readd - t_last) < (int64_t)(gap_last * 0.75),
                    "Added back too late (%" PRId64
                    "ms after the last attempt) to tell whether the "
                    "backoff is continued",
                    (t_readd - t_last) / 1000);
        TEST_ASSERT(first_after - t_last >= (int64_t)(gap_last * 0.75),
                    "Expected the broker added back to wait for the "
                    "reconnect time of the removed one, at least %" PRId64
                    "ms after its last attempt, got %" PRId64 "ms",
                    (int64_t)(gap_last * 0.75) / 1000,
                    (first_after - t_last) / 1000);
        TEST_ASSERT(second_after - first_after >= (int64_t)(gap_last * 0.75),
                    "Expected the broker added back to continue the backoff, "
                    "waiting at least %" PRId64 "ms, got %" PRId64 "ms",
                    (int64_t)(gap_last * 0.75) / 1000,
                    (second_after - first_after) / 1000);

        reconnect_ev_destroy();
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
 * @brief Connections requested for a failing broker while another broker is
 *        up must respect its reconnect backoff.
 *
 *        Broker 2 refuses every connection while broker 1 stays up. Admin
 *        requests for broker 2 make rd_kafka_broker_get_async() send it an
 *        `RD_KAFKA_OP_CONNECT` on each state change: the connection attempts
 *        must still be paced, up to `reconnect.backoff.max.ms`.
 */
static void do_test_reconnect_backoff_broker_requests(void) {
        const int backoff_ms = 500, backoff_max_ms = 4000, duration_ms = 14000;
        const int request_timeout_ms = 3000;
        const int ramp_up_waits =
            reconnect_backoff_ramp_up_waits(backoff_ms, backoff_max_ms);
        const int max_attempts = reconnect_backoff_max_attempts(
            backoff_ms, backoff_max_ms, duration_ms);
        const int min_gap_ms          = (int)(backoff_max_ms * 0.75) - 250;
        const char *debug_contexts[2] = {"broker", NULL};
        char bootstrap_servers[128], broker2_name[128], waits[512];
        const char *bootstraps, *broker2;
        const struct rd_kafka_metadata *md;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_queue_t *q;
        int64_t ts[64], start, abs_timeout;
        size_t of = 0;
        int i, cnt, requests = 0;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(2, &bootstraps);
        broker2  = strchr(bootstraps, ',') + 1;
        /* Broker 1 is the only bootstrap server: broker 2 is learned from
         * the metadata, and only connected to when a request needs it. */
        rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers), "%.*s",
                    (int)(broker2 - 1 - bootstraps), bootstraps);
        rd_snprintf(reconnect_refused_nodename,
                    sizeof(reconnect_refused_nodename), "%s", broker2);
        rd_snprintf(broker2_name, sizeof(broker2_name), "%s/2", broker2);

        reconnect_ev_init();
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        rd_kafka_conf_set_connect_cb(conf,
                                     reconnect_refuse_nodename_connect_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, reconnect_ev_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        q  = rd_kafka_queue_new(rk);

        /* Connect to broker 1 and learn broker 2. */
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 0, NULL, &md, 5000));
        rd_kafka_metadata_destroy(md);

        start       = test_clock();
        abs_timeout = start + duration_ms * 1000LL;
        while (test_clock() < abs_timeout) {
                rd_kafka_ConfigResource_t *config =
                    rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_BROKER, "2");
                rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS);
                rd_kafka_event_t *rkev;
                char errstr[256];

                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, request_timeout_ms, errstr, sizeof(errstr)));
                rd_kafka_DescribeConfigs(rk, &config, 1, options, q);
                rd_kafka_AdminOptions_destroy(options);
                rd_kafka_ConfigResource_destroy(config);

                /* The request fails once it times out. */
                rkev = rd_kafka_queue_poll(q, request_timeout_ms + 5000);
                TEST_ASSERT(rkev, "Expected a DescribeConfigs result");
                rd_kafka_event_destroy(rkev);
                requests++;
        }

        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name, start, 0, ts,
                               RD_ARRAYSIZE(ts));
        waits[0] = '\0';
        for (i = 1; i < RD_MIN(cnt, 20) && of < sizeof(waits); i++)
                of +=
                    rd_snprintf(waits + of, sizeof(waits) - of, "%s%" PRId64,
                                i > 1 ? ", " : "", (ts[i] - ts[i - 1]) / 1000);
        TEST_SAY(
            "%s: %d connection attempt(s) in %dms for %d request(s) "
            "(at most %d expected), waits (ms): %s\n",
            broker2_name, cnt, duration_ms, requests, max_attempts, waits);
        reconnect_ev_destroy();

        TEST_ASSERT(cnt >= 2,
                    "Expected the requests to ask for connections to %s, got "
                    "%d connection attempt(s)",
                    broker2_name, cnt);
        TEST_ASSERT(cnt <= max_attempts,
                    "Expected at most %d connection attempts to %s in %dms, "
                    "paced by the reconnect backoff, got %d for %d "
                    "request(s): a request must not skip the backoff",
                    max_attempts, broker2_name, duration_ms, cnt, requests);
        /* Wait i is between attempts i - 1 and i. */
        for (i = ramp_up_waits + 1; i < RD_MIN(cnt, (int)RD_ARRAYSIZE(ts)); i++)
                TEST_ASSERT((ts[i] - ts[i - 1]) / 1000 >= min_gap_ms,
                            "Expected the reconnect backoff of %s to reach "
                            "%dms after %d waits, got a %" PRId64
                            "ms wait number %d",
                            broker2_name, backoff_max_ms, ramp_up_waits,
                            (ts[i] - ts[i - 1]) / 1000, i);

        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
}


/** Nodename changes of the logical group coordinator, recorded by
 *  do_test_reconnect_backoff_coordinator_log_cb() from the
 *  "Broker nodename changed" debug logs. */
static struct {
        mtx_t lock;
        char host2[128];     /**< Broker 2's "host:port" */
        char host3[128];     /**< Broker 3's "host:port" */
        int64_t cleared_ts;  /**< Last time it was cleared, or 0 */
        int64_t set_back_ts; /**< First time it was set to host2 after
                              *   being cleared, or 0 */
        int64_t moved_ts;    /**< First time it was set to host3, or 0 */
} do_test_coordinator_nodename;

static void do_test_reconnect_backoff_coordinator_log_cb(const rd_kafka_t *rk,
                                                         int level,
                                                         const char *fac,
                                                         const char *buf) {
        static const char changed[] = "Broker nodename changed from \"";
        const char *p, *to, *end;
        char nodename[128];

        reconnect_ev_log_cb(rk, level, fac, buf);

        if (!strstr(buf, "GroupCoordinator") || !(p = strstr(buf, changed)) ||
            !(to = strstr(p + sizeof(changed) - 1, "\" to \"")))
                return;
        to += strlen("\" to \"");
        if (!(end = strchr(to, '"')))
                return;
        rd_snprintf(nodename, sizeof(nodename), "%.*s", (int)(end - to), to);

        mtx_lock(&do_test_coordinator_nodename.lock);
        if (!*nodename)
                do_test_coordinator_nodename.cleared_ts = test_clock();
        else if (!strcmp(nodename, do_test_coordinator_nodename.host2) &&
                 do_test_coordinator_nodename.cleared_ts &&
                 !do_test_coordinator_nodename.set_back_ts)
                do_test_coordinator_nodename.set_back_ts = test_clock();
        else if (!strcmp(nodename, do_test_coordinator_nodename.host3) &&
                 !do_test_coordinator_nodename.moved_ts)
                do_test_coordinator_nodename.moved_ts = test_clock();
        mtx_unlock(&do_test_coordinator_nodename.lock);
}

/** @returns the field of do_test_coordinator_nodename at \p tsp, locked. */
static int64_t do_test_reconnect_backoff_coordinator_ts(const int64_t *tsp) {
        int64_t ts;
        mtx_lock(&do_test_coordinator_nodename.lock);
        ts = *tsp;
        mtx_unlock(&do_test_coordinator_nodename.lock);
        return ts;
}

/**
 * @brief The group coordinator broker starts its backoff afresh when the
 *        coordinator moves to a different host, but not when it's set back
 *        to the same host after being marked dead.
 *
 *        The coordinator, broker 2, refuses connections while broker 1 is up,
 *        so its backoff grows. Then:
 *        - \p different_host: it moves to broker 3, and its first attempt
 *          there must come sooner than the old backoff allows;
 *        - otherwise: broker 2 is left out of the metadata, so the
 *          coordinator is marked dead and then set back to broker 2, and its
 *          next attempt must still wait for the backoff.
 */
static void
do_test_reconnect_backoff_coordinator_moved(rd_bool_t different_host) {
        const int backoff_ms = 100, backoff_max_ms = 10000;
        /* Enough attempts for the last wait to be much longer than the
         * second between the coordinator queries that find the change. */
        const int attempts_before     = 7;
        const char *topic             = "coordinator_moved";
        const char *group             = "coordinator_moved";
        const char *debug_contexts[2] = {"broker", NULL};
        char bootstrap_servers[128], old_name[64], new_name[64];
        const char *bootstraps, *broker2, *broker3;
        const struct rd_kafka_metadata *metadata;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int64_t ts[64], abs_timeout, t_last, gap_last, t_change, first_after;
        int cnt;

        SUB_TEST_QUICK("%s", different_host
                                 ? "moved to a different host"
                                 : "cleared and set back to the same host");

        mcluster = test_mock_cluster_new(3, &bootstraps);
        broker2  = strchr(bootstraps, ',') + 1;
        broker3  = strchr(broker2, ',') + 1;
        rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers), "%.*s",
                    (int)(broker2 - 1 - bootstraps), bootstraps);
        mtx_init(&do_test_coordinator_nodename.lock, mtx_plain);
        rd_snprintf(do_test_coordinator_nodename.host2,
                    sizeof(do_test_coordinator_nodename.host2), "%.*s",
                    (int)(broker3 - 1 - broker2), broker2);
        rd_snprintf(do_test_coordinator_nodename.host3,
                    sizeof(do_test_coordinator_nodename.host3), "%s", broker3);
        do_test_coordinator_nodename.cleared_ts  = 0;
        do_test_coordinator_nodename.set_back_ts = 0;
        do_test_coordinator_nodename.moved_ts    = 0;
        /* Refuses broker 2's address, which the logical coordinator uses
         * while it points at broker 2. */
        rd_snprintf(reconnect_refused_nodename,
                    sizeof(reconnect_refused_nodename), "%s",
                    do_test_coordinator_nodename.host2);
        rd_snprintf(old_name, sizeof(old_name), "GroupCoordinator/2");
        rd_snprintf(new_name, sizeof(new_name), "GroupCoordinator/%d",
                    different_host ? 3 : 2);

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        TEST_CALL_ERR__(
            rd_kafka_mock_coordinator_set(mcluster, "group", group, 2));

        reconnect_ev_init();
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        test_conf_set(conf, "group.id", group);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        rd_kafka_conf_set_connect_cb(conf,
                                     reconnect_refuse_nodename_connect_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_reconnect_backoff_coordinator_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);
        test_consumer_subscribe(rk, topic);

        abs_timeout = test_clock() + 60 * 1000 * 1000;
        do {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 10);
                RD_IF_FREE(rkm, rd_kafka_message_destroy);
                cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, old_name, 0, 0, ts,
                                       RD_ARRAYSIZE(ts));
        } while (cnt < attempts_before && test_clock() < abs_timeout);
        TEST_ASSERT(cnt >= attempts_before,
                    "Expected %d connection attempts of %s, got %d",
                    attempts_before, old_name, cnt);

        if (different_host) {
                TEST_CALL_ERR__(
                    rd_kafka_mock_coordinator_set(mcluster, "group", group, 3));
        } else {
                /* Leaving broker 2 out of the metadata marks the
                 * coordinator dead, clearing its nodename, then the
                 * coordinator query sets it back to broker 2. */
                TEST_CALL_ERR__(
                    rd_kafka_mock_broker_remove_from_metadata(mcluster, 2));
                TEST_CALL_ERR__(
                    rd_kafka_metadata(rk, 1, NULL, &metadata, 5000));
                rd_kafka_metadata_destroy(metadata);
        }

        /* Until the coordinator's nodename changes, and its first attempt to
         * its (new or same) host after the change. */
        abs_timeout = test_clock() + 30 * 1000 * 1000;
        first_after = 0;
        do {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 10);
                RD_IF_FREE(rkm, rd_kafka_message_destroy);
                t_change = do_test_reconnect_backoff_coordinator_ts(
                    different_host ? &do_test_coordinator_nodename.moved_ts
                                   : &do_test_coordinator_nodename.set_back_ts);
                if (t_change &&
                    reconnect_ev_get(RECONNECT_EV_ATTEMPT, new_name, t_change,
                                     0, ts, RD_ARRAYSIZE(ts)) >= 1)
                        first_after = ts[0];
        } while (!first_after && test_clock() < abs_timeout);

        /* No consumer close: the group was never joined. */
        rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        test_mock_cluster_destroy(mcluster);

        TEST_ASSERT(t_change, "Expected the coordinator's nodename to be %s",
                    different_host ? "changed to broker 3's host"
                                   : "cleared and set back to broker 2's host");
        TEST_ASSERT(first_after,
                    "Expected a connection attempt of %s after the change",
                    new_name);

        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, old_name, 0, t_change, ts,
                               RD_ARRAYSIZE(ts));
        TEST_ASSERT(cnt >= 2 && cnt <= (int)RD_ARRAYSIZE(ts),
                    "Unexpected attempt count of %s before the change: %d",
                    old_name, cnt);
        t_last   = ts[cnt - 1];
        gap_last = ts[cnt - 1] - ts[cnt - 2];

        TEST_SAY("%s: last wait %" PRId64 "ms, nodename %s after %" PRId64
                 "ms, first attempt of %s after %" PRId64 "ms\n",
                 old_name, gap_last / 1000,
                 different_host ? "moved" : "set back",
                 (t_change - t_last) / 1000, new_name,
                 (first_after - t_last) / 1000);
        TEST_ASSERT((t_change - t_last) < (int64_t)(gap_last * 0.75),
                    "Changed too late (%" PRId64
                    "ms after the last attempt) to tell whether the backoff "
                    "applies",
                    (t_change - t_last) / 1000);
        if (different_host)
                TEST_ASSERT(first_after - t_last < (int64_t)(gap_last * 0.75),
                            "Expected the first attempt to the new "
                            "coordinator's host before the old host's "
                            "reconnect time (at least %" PRId64
                            "ms after its last attempt), got it after "
                            "%" PRId64 "ms",
                            (int64_t)(gap_last * 0.75) / 1000,
                            (first_after - t_last) / 1000);
        else
                TEST_ASSERT(first_after - t_last >= (int64_t)(gap_last * 0.75),
                            "Expected the coordinator set back to the same "
                            "host to continue its backoff (next attempt at "
                            "least %" PRId64
                            "ms after the last one), got an attempt after "
                            "%" PRId64 "ms",
                            (int64_t)(gap_last * 0.75) / 1000,
                            (first_after - t_last) / 1000);

        reconnect_ev_destroy();
        mtx_destroy(&do_test_coordinator_nodename.lock);
        rd_free(log_interceptor);
        log_interceptor = NULL;
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
        if (variation !=
            DO_TEST_KIP899_REBOOTSTRAP_CASES_VARIATION_REBOOTSTRAP_DISABLED) {
                /* Action 7 sets the last broker down, so the "all brokers
                 * down" state is reached and a re-bootstrap sequence starts
                 * immediately, decommissioning the learned brokers. With every
                 * broker down no Metadata response can succeed, so the learned
                 * broker list cannot be rebuilt and stays empty until a broker
                 * is set up again (action 8). */
                expected_brokers_cnt[7] = 0;
        } else {
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
 * @brief Whether the Metadata errors were already injected for this variation.
 *        Set by the test thread, read by the client main thread from
 *        the log callback.
 */
static rd_atomic32_t do_test_kip1102_rebootstrap_cases_errors_pushed;

/**
 * @brief Whether the injected Metadata errors were already cleared for this
 *        variation. Set by the client main thread from the log callback, read
 *        by the test thread.
 */
static rd_atomic32_t do_test_kip1102_rebootstrap_cases_errors_cleared;

/**
 * @brief Absolute time after which we stop waiting for the expected
 *        re-bootstrap sequences, 0 when not armed yet.
 */
static int64_t do_test_kip1102_rebootstrap_cases_wait_abs_timeout_us;

/**
 * @brief Host and port of the bootstrap-only broker listener, as passed to the
 *        resolve callback. Empty when the variation doesn't use one.
 */
static char do_test_kip1102_rebootstrap_cases_bootstrap_only_host[256];
static char do_test_kip1102_rebootstrap_cases_bootstrap_only_port[16];

/**
 * @brief Number of times the bootstrap-only broker listener was resolved
 *        since the Metadata errors were injected. Each re-bootstrap sequence
 *        re-adds the bootstrap servers as new broker objects, so each one
 *        should resolve it again.
 */
static rd_atomic32_t
    do_test_kip1102_rebootstrap_cases_bootstrap_only_resolve_cnt;

/**
 * @brief Number of re-bootstrap sequences started since the Metadata errors
 *        were injected, upper bound for the resolve count above.
 */
static rd_atomic32_t
    do_test_kip1102_rebootstrap_cases_rebootstrap_after_errors_cnt;

/**
 * @brief Number of Metadata errors injected in the error phase.
 */
#define DO_TEST_KIP1102_REBOOTSTRAP_CASES_PUSHED_ERROR_CNT 140

/**
 * @brief Number of re-bootstrap sequences \p variation must show before the
 *        injected error phase can be ended.
 */
static int do_test_kip1102_rebootstrap_cases_min_rebootstrap_cnt(
    do_test_kip1102_rebootstrap_cases_variation_t variation) {
        /* With the `REBOOTSTRAP_REQUIRED` error code a sequence is started on
         * each error response, otherwise a single one is started when
         * `metadata.recovery.rebootstrap.trigger.ms` is reached. */
        return do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                   variation)
                   ? 65
                   : 1;
}

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

/**
 * @brief Resolve callback delegating to getaddrinfo(3), counting the
 *        resolutions of the bootstrap-only broker listener once the Metadata
 *        errors were injected.
 */
static int
do_test_kip1102_rebootstrap_cases_resolve_cb(const char *node,
                                             const char *service,
                                             const struct addrinfo *hints,
                                             struct addrinfo **res,
                                             void *opaque) {
        if (!node) {
                freeaddrinfo(*res);
                return 0;
        }

        if (rd_atomic32_get(&do_test_kip1102_rebootstrap_cases_errors_pushed) &&
            !strcmp(node,
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_host) &&
            service &&
            !strcmp(service,
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_port))
                rd_atomic32_add(
                    &do_test_kip1102_rebootstrap_cases_bootstrap_only_resolve_cnt,
                    1);

        return getaddrinfo(node, service, hints, res);
}

static void do_test_kip1102_rebootstrap_cases_log_cb(const rd_kafka_t *rk,
                                                     int level,
                                                     const char *fac,
                                                     const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence")) {
                /* Count the number of re-bootstrap sequences started */
                int32_t cnt = rd_atomic32_add(
                    &do_test_kip1102_rebootstrap_cases_rebootstrap_cnt, 1);

                if (rd_atomic32_get(
                        &do_test_kip1102_rebootstrap_cases_errors_pushed))
                        rd_atomic32_add(
                            &do_test_kip1102_rebootstrap_cases_rebootstrap_after_errors_cnt,
                            1);

                if (rd_atomic32_get(
                        &do_test_kip1102_rebootstrap_cases_errors_pushed) &&
                    !rd_atomic32_get(
                        &do_test_kip1102_rebootstrap_cases_errors_cleared) &&
                    cnt >=
                        do_test_kip1102_rebootstrap_cases_min_rebootstrap_cnt(
                            do_test_kip1102_rebootstrap_cases_variation)) {
                        /* End the error phase as soon as the expected
                         * sequences have started: the next Metadata response
                         * succeeds and cancels the re-bootstrap timer, so no
                         * further sequence can start no matter how slow the
                         * machine is.
                         *
                         * Letting the remaining pushed errors drain instead
                         * re-arms the timer on each failed refresh and starts
                         * a new sequence every
                         * `metadata.recovery.rebootstrap.trigger.ms`, making
                         * the count depend on how long the error phase takes.
                         *
                         * Only the mock cluster lock is taken here, no
                         * client lock, so this is safe from the log callback.
                         */
                        rd_atomic32_set(
                            &do_test_kip1102_rebootstrap_cases_errors_cleared,
                            1);
                        rd_kafka_mock_clear_request_errors(cluster,
                                                           RD_KAFKAP_Metadata);
                }
        }
}

static rd_kafka_type_t
do_test_kip1102_rebootstrap_cases_edit_configuration_cb(rd_kafka_conf_t *conf) {
        const char *debug_contexts[2] = {"conf", NULL};
        /* Long enough that the first sequence is triggered only after the
         * error injection is well under way, short enough to keep the test
         * quick. The expected count doesn't depend on it: the error phase is
         * ended as soon as the expected sequences have started,
         * see `do_test_kip1102_rebootstrap_cases_log_cb()`. */
        test_conf_set(conf, "metadata.recovery.rebootstrap.trigger.ms", "5000");
        /* Avoid Head Of Line blocking from fetch requests for predictable
         * timing */
        test_conf_set(conf, "fetch.wait.max.ms", "10");
        if (do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                do_test_kip1102_rebootstrap_cases_variation))
                /* The re-bootstrap sequences are paced by the bootstrap
                 * brokers' reconnect backoff: cap it so that the expected
                 * sequences fit in the error phase. */
                test_conf_set(conf, "reconnect.backoff.max.ms", "100");
        log_interceptor = test_conf_set_log_interceptor(
            conf, do_test_kip1102_rebootstrap_cases_log_cb, debug_contexts);

        if (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id != -1) {
                int32_t id =
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id;
                char *listener;
                const char *colon;

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

                /* Count its resolutions: each re-bootstrap sequence must
                 * re-add it as a new broker object, that resolves and
                 * connects anew, instead of keeping a connected one. */
                colon = strrchr(listener, ':');
                TEST_ASSERT(colon, "No port in listener \"%s\"", listener);
                rd_snprintf(
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_host,
                    sizeof(
                        do_test_kip1102_rebootstrap_cases_bootstrap_only_host),
                    "%.*s", (int)(colon - listener), listener);
                rd_snprintf(
                    do_test_kip1102_rebootstrap_cases_bootstrap_only_port,
                    sizeof(
                        do_test_kip1102_rebootstrap_cases_bootstrap_only_port),
                    "%s", colon + 1);
                rd_kafka_conf_set_resolve_cb(
                    conf, do_test_kip1102_rebootstrap_cases_resolve_cb);
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
        if (do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                variation))
                return do_test_kip1102_rebootstrap_cases_allowed_errors_rebootstrap_required;
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
                allowed_errors =
                    do_test_kip1102_rebootstrap_cases_allowed_errors(
                        do_test_kip1102_rebootstrap_cases_variation);
                /* A request is made every 100 ms. More errors than the
                 * expected sequences are pushed: the error phase is ended
                 * from the log callback once the expected sequences are
                 * seen, and not every error consumed starts a sequence, see
                 * `do_test_kip1102_rebootstrap_cases()`. */
                for (i = 0;
                     i < DO_TEST_KIP1102_REBOOTSTRAP_CASES_PUSHED_ERROR_CNT;
                     i++)
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

                rd_atomic32_set(
                    &do_test_kip1102_rebootstrap_cases_errors_pushed, 1);
        }

        if (action == 1 &&
            (!rd_atomic32_get(
                 &do_test_kip1102_rebootstrap_cases_errors_cleared) ||
             (do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id !=
                  -1 &&
              rd_atomic32_get(
                  &do_test_kip1102_rebootstrap_cases_bootstrap_only_metadata_cnt) ==
                  0))) {
                /* Second action: in case there's no third action, keep waiting
                 * until the expected re-bootstrap sequences are seen, that is
                 * until the log callback ends the error phase. Bounded, so
                 * that a missing sequence fails the count assertion instead of
                 * waiting forever.
                 *
                 * Ending the error phase isn't enough when a bootstrap-only
                 * broker is expected to be queried: the log callback clears
                 * the errors on the "Starting re-bootstrap sequence" line,
                 * which is emitted before the bootstrap servers are re-added,
                 * so the Metadata request to the bootstrap-only broker is
                 * still ahead. Without waiting for it the loop stops and the
                 * cluster is destroyed while the client is still connecting,
                 * and the count assertion below fails on timing rather than
                 * on the property it checks. */
                if (!do_test_kip1102_rebootstrap_cases_wait_abs_timeout_us)
                        do_test_kip1102_rebootstrap_cases_wait_abs_timeout_us =
                            test_clock() + 20000000;
                return test_clock() <
                       do_test_kip1102_rebootstrap_cases_wait_abs_timeout_us;
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
            expected_min_rebootstrap_cnt = 1;

        SUB_TEST_QUICK(
            "%s, %s",
            do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(variation)
                ? "\"re-bootstrap required\" error code"
            : do_test_kip1102_rebootstrap_cases_connections_stay_up(variation)
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
        do_test_kip1102_rebootstrap_cases_tracking_started = rd_false;
        rd_atomic32_init(&do_test_kip1102_rebootstrap_cases_errors_pushed, 0);
        rd_atomic32_init(&do_test_kip1102_rebootstrap_cases_errors_cleared, 0);
        rd_atomic32_init(
            &do_test_kip1102_rebootstrap_cases_bootstrap_only_resolve_cnt, 0);
        rd_atomic32_init(
            &do_test_kip1102_rebootstrap_cases_rebootstrap_after_errors_cnt, 0);
        do_test_kip1102_rebootstrap_cases_bootstrap_only_host[0]   = '\0';
        do_test_kip1102_rebootstrap_cases_bootstrap_only_port[0]   = '\0';
        do_test_kip1102_rebootstrap_cases_wait_abs_timeout_us      = 0;
        do_test_kip1102_rebootstrap_cases_bootstrap_only_broker_id = -1;

        expected_min_rebootstrap_cnt =
            do_test_kip1102_rebootstrap_cases_min_rebootstrap_cnt(variation);

        if (do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                variation)) {
                /* REBOOTSTRAP_REQUIRED error code cases:
                 * A re-bootstrap is expected for each error response
                 * received. It's possible multiple consecutive error
                 * responses cause a single re-bootstrap sequence because of
                 * the timer activation, and an error consumed by a request
                 * in flight on a broker that the sequence decommissions is
                 * never received: pushed errors are an upper bound. */
                expected_rebootstrap_cnt =
                    DO_TEST_KIP1102_REBOOTSTRAP_CASES_PUSHED_ERROR_CNT;
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

                /* Each sequence must resolve the bootstrap server again:
                 * a kept, still connected, bootstrap broker would never
                 * re-resolve its address and, after a cluster switchover
                 * behind the same hostname, would keep asking the old
                 * cluster. Without the fix it resolves exactly once, on
                 * the first sequence that re-adds it.
                 *
                 * Not an exact match: a sequence can be superseded by the
                 * next one before its new bootstrap broker is selected
                 * for connection, in which case that object never
                 * resolves. */
                int32_t resolve_cnt = rd_atomic32_get(
                    &do_test_kip1102_rebootstrap_cases_bootstrap_only_resolve_cnt);
                int32_t rebootstrap_after_errors_cnt = rd_atomic32_get(
                    &do_test_kip1102_rebootstrap_cases_rebootstrap_after_errors_cnt);
                int32_t expected_min_resolve_cnt =
                    do_test_kip1102_rebootstrap_cases_is_rebootstrap_required(
                        variation)
                        ? 2
                        : 1;
                TEST_SAY("Bootstrap-only broker resolved %" PRId32
                         " time(s) during %" PRId32
                         " re-bootstrap sequence(s)\n",
                         resolve_cnt, rebootstrap_after_errors_cnt);
                TEST_ASSERT(resolve_cnt >= expected_min_resolve_cnt &&
                                resolve_cnt <= rebootstrap_after_errors_cnt,
                            "Expected the bootstrap-only broker to be "
                            "resolved between %" PRId32 " and %" PRId32
                            " time(s), got %" PRId32,
                            expected_min_resolve_cnt,
                            rebootstrap_after_errors_cnt, resolve_cnt);
        }

        rd_free(log_interceptor);
        allowed_errors = NULL;
        SUB_TEST_PASS();
}

/**
 * @brief A broker whose connection reaches UP and is then closed on the
 *        first request (e.g. a proxy that can't reach the broker) keeps
 *        reconnecting at the pace of the reconnect backoff: reaching UP
 *        doesn't reset it.
 *
 * @param healthy_broker If false, the only broker closes the connection on
 *        every Metadata request, so each re-bootstrap re-creates it and it
 *        must continue the backoff. If true, broker 1 stays up and broker 2,
 *        the partition leader, closes it on every Produce request.
 */
static void do_test_reconnect_backoff_up_then_close(rd_bool_t healthy_broker) {
        const int backoff_ms = 100, backoff_max_ms = 2000, duration_ms = 9000;
        const int ramp_up_waits =
            reconnect_backoff_ramp_up_waits(backoff_ms, backoff_max_ms);
        const int max_ups = reconnect_backoff_max_attempts(
            backoff_ms, backoff_max_ms, duration_ms);
        const int min_gap_ms          = (int)(backoff_max_ms * 0.75) - 250;
        const char *debug_contexts[2] = {"broker", NULL};
        rd_kafka_resp_err_t errs[2000];
        char bootstrap_servers[128], broker_name[128];
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char gaps[512];
        size_t of = 0;
        int64_t ups[64], abs_timeout;
        int i, up_cnt;

        SUB_TEST_QUICK("%s",
                       healthy_broker ? "another broker up" : "only broker");

        mcluster = test_mock_cluster_new(healthy_broker ? 2 : 1, &bootstraps);
        for (i = 0; i < (int)RD_ARRAYSIZE(errs); i++)
                errs[i] = RD_KAFKA_RESP_ERR__TRANSPORT;

        if (healthy_broker) {
                const char *broker2 = strchr(bootstraps, ',') + 1;

                rd_kafka_mock_topic_create(mcluster, "up_then_close", 1, 1);
                rd_kafka_mock_partition_set_leader(mcluster, "up_then_close", 0,
                                                   2);
                /* Only broker 2, the partition leader, receives Produce
                 * requests. */
                rd_kafka_mock_push_request_errors_array(
                    mcluster, RD_KAFKAP_Produce, RD_ARRAYSIZE(errs), errs);
                rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers),
                            "%.*s", (int)(broker2 - 1 - bootstraps),
                            bootstraps);
                rd_snprintf(broker_name, sizeof(broker_name), "%s/2", broker2);
        } else {
                rd_kafka_mock_push_request_errors_array(
                    mcluster, RD_KAFKAP_Metadata, RD_ARRAYSIZE(errs), errs);
                rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers), "%s",
                            bootstraps);
                rd_snprintf(broker_name, sizeof(broker_name), "%s/bootstrap",
                            bootstraps);
        }

        reconnect_ev_init();
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        log_interceptor = test_conf_set_log_interceptor(
            conf, reconnect_ev_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* A pending message keeps the producer asking for connections:
         * through rd_kafka_connect_any() for the Metadata it needs while no
         * broker is up, or to broker 2, the partition leader. */
        TEST_CALL_ERR__(rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("up_then_close"),
                                          RD_KAFKA_V_VALUE("hi", 2),
                                          RD_KAFKA_V_END));

        abs_timeout = test_clock() + duration_ms * 1000LL;
        while (test_clock() < abs_timeout)
                rd_kafka_poll(rk, 100);

        rd_kafka_purge(rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        up_cnt  = reconnect_ev_get(RECONNECT_EV_UP, broker_name, 0, 0, ups,
                                   RD_ARRAYSIZE(ups));
        gaps[0] = '\0';
        for (i = 1; i < RD_MIN(up_cnt, 20) && of < sizeof(gaps); i++)
                of += rd_snprintf(gaps + of, sizeof(gaps) - of, "%s%" PRId64,
                                  i > 1 ? ", " : "",
                                  (ups[i] - ups[i - 1]) / 1000);
        TEST_SAY(
            "%s: %d connection(s) reaching UP in %dms (at most %d "
            "expected), %d re-bootstrap sequence(s), waits (ms): %s\n",
            broker_name, up_cnt, duration_ms, max_ups,
            reconnect_ev_rebootstraps(), gaps);
        reconnect_ev_destroy();
        TEST_ASSERT(up_cnt >= 2,
                    "Expected %s to reach UP and be reconnected, got %d UP(s)",
                    broker_name, up_cnt);
        TEST_ASSERT(up_cnt <= max_ups,
                    "Expected at most %d connections of %s reaching UP in "
                    "%dms, paced by the reconnect backoff, got %d: reaching "
                    "UP must not reset the backoff",
                    max_ups, broker_name, duration_ms, up_cnt);
        /* Wait i is between connections i - 1 and i. */
        for (i = ramp_up_waits + 1; i < RD_MIN(up_cnt, (int)RD_ARRAYSIZE(ups));
             i++)
                TEST_ASSERT((ups[i] - ups[i - 1]) / 1000 >= min_gap_ms,
                            "Expected the reconnect backoff of %s to reach "
                            "%dms after %d waits, got a %" PRId64
                            "ms wait number %d",
                            broker_name, backoff_max_ms, ramp_up_waits,
                            (ups[i] - ups[i - 1]) / 1000, i);

        rd_free(log_interceptor);
        log_interceptor = NULL;
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

/** Host name that do_test_reconnect_backoff_renamed_broker_resolve_cb()
 *  resolves to the mock broker's address. */
#define DO_TEST_RENAMED_BROKER_HOST "renamed-broker.invalid"

/** Marks the address lists returned by
 *  do_test_reconnect_backoff_renamed_broker_resolve_cb(), so that it frees
 *  only those itself. */
static char do_test_reconnect_backoff_renamed_broker_ours[] = "renamed-broker";

/**
 * @brief Resolve callback: resolves DO_TEST_RENAMED_BROKER_HOST to 127.0.0.1
 *        and the requested port, and any other name as usual.
 */
static int do_test_reconnect_backoff_renamed_broker_resolve_cb(
    const char *node,
    const char *service,
    const struct addrinfo *hints,
    struct addrinfo **res,
    void *opaque) {
        struct addrinfo *a;
        struct sockaddr_in *sin;

        if (!node) {
                /* Free a list returned by a previous call. */
                if (*res && (*res)->ai_canonname ==
                                do_test_reconnect_backoff_renamed_broker_ours) {
                        while (*res) {
                                struct addrinfo *next = (*res)->ai_next;
                                free((*res)->ai_addr);
                                free(*res);
                                *res = next;
                        }
                } else if (*res) {
                        freeaddrinfo(*res);
                }
                return 0;
        }

        if (strcmp(node, DO_TEST_RENAMED_BROKER_HOST))
                return getaddrinfo(node, service, hints, res);

        a                    = calloc(1, sizeof(*a));
        sin                  = calloc(1, sizeof(*sin));
        sin->sin_family      = AF_INET;
        sin->sin_port        = htons((unsigned short)atoi(service));
        sin->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a->ai_family         = AF_INET;
        a->ai_socktype       = SOCK_STREAM;
        a->ai_protocol       = IPPROTO_TCP;
        a->ai_addrlen        = sizeof(*sin);
        a->ai_addr           = (struct sockaddr *)sin;
        a->ai_canonname      = do_test_reconnect_backoff_renamed_broker_ours;
        *res                 = a;
        return 0;
}

/**
 * @brief A learned broker whose advertised host changes isn't delayed by the
 *        backoff it built up against the old host.
 *
 *        Broker 2, the partition leader, refuses connections while broker 1
 *        is up, then is advertised as DO_TEST_RENAMED_BROKER_HOST: its first
 *        attempt to the new host must come sooner than the old backoff
 *        allows.
 */
static void do_test_reconnect_backoff_renamed_broker(void) {
        const int backoff_ms = 100, backoff_max_ms = 10000;
        const int attempts_before     = 5;
        const char *debug_contexts[2] = {"broker", NULL};
        char bootstrap_servers[128], broker2_name[128], renamed_name[128];
        const char *bootstraps, *broker2;
        const struct rd_kafka_metadata *metadata;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int64_t ts[64], abs_timeout, t_rename, t_last, gap_last, first_new;
        int broker2_port, cnt;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(2, &bootstraps);
        broker2  = strchr(bootstraps, ',') + 1;
        rd_snprintf(bootstrap_servers, sizeof(bootstrap_servers), "%.*s",
                    (int)(broker2 - 1 - bootstraps), bootstraps);
        rd_snprintf(reconnect_refused_nodename,
                    sizeof(reconnect_refused_nodename), "%s", broker2);
        rd_snprintf(broker2_name, sizeof(broker2_name), "%s/2", broker2);
        broker2_port = atoi(strrchr(broker2, ':') + 1);
        rd_snprintf(renamed_name, sizeof(renamed_name), "%s:%d/2",
                    DO_TEST_RENAMED_BROKER_HOST, broker2_port);
        rd_kafka_mock_topic_create(mcluster, "renamed", 1, 1);
        rd_kafka_mock_partition_set_leader(mcluster, "renamed", 0, 2);

        reconnect_ev_init();
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        reconnect_backoff_conf_set(conf, backoff_ms, backoff_max_ms);
        rd_kafka_conf_set_resolve_cb(
            conf, do_test_reconnect_backoff_renamed_broker_resolve_cb);
        rd_kafka_conf_set_connect_cb(conf,
                                     reconnect_refuse_nodename_connect_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, reconnect_ev_log_cb, debug_contexts);
        rd_kafka_conf_set_error_cb(
            conf, do_test_reconnect_backoff_all_brokers_down_error_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_CALL_ERR__(rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("renamed"),
                                          RD_KAFKA_V_VALUE("1", 1),
                                          RD_KAFKA_V_END));

        abs_timeout = test_clock() + 20 * 1000 * 1000;
        do {
                rd_kafka_poll(rk, 10);
                cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name, 0, 0,
                                       ts, RD_ARRAYSIZE(ts));
        } while (cnt < attempts_before && test_clock() < abs_timeout);
        TEST_ASSERT(cnt >= attempts_before,
                    "Expected %d connection attempts to %s, got %d",
                    attempts_before, broker2_name, cnt);

        t_rename = test_clock();
        rd_kafka_mock_broker_set_host_port(
            mcluster, 2, DO_TEST_RENAMED_BROKER_HOST, broker2_port);
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, NULL, &metadata, 5000));
        rd_kafka_metadata_destroy(metadata);

        abs_timeout = test_clock() + 20 * 1000 * 1000;
        do {
                rd_kafka_poll(rk, 10);
                cnt = reconnect_ev_get(RECONNECT_EV_UP, renamed_name, t_rename,
                                       0, ts, RD_ARRAYSIZE(ts));
        } while (cnt < 1 && test_clock() < abs_timeout);

        rd_kafka_purge(rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        TEST_ASSERT(cnt >= 1, "Expected %s to connect after the rename",
                    renamed_name);
        TEST_ASSERT(reconnect_ev_get(RECONNECT_EV_ATTEMPT, renamed_name,
                                     t_rename, 0, ts, RD_ARRAYSIZE(ts)) >= 1,
                    "Expected a connection attempt to %s", renamed_name);
        first_new = ts[0];

        cnt = reconnect_ev_get(RECONNECT_EV_ATTEMPT, broker2_name, 0, t_rename,
                               ts, RD_ARRAYSIZE(ts));
        TEST_ASSERT(cnt >= 2 && cnt <= (int)RD_ARRAYSIZE(ts),
                    "Unexpected attempt count before the rename: %d", cnt);
        t_last   = ts[cnt - 1];
        gap_last = ts[cnt - 1] - ts[cnt - 2];

        TEST_SAY("%s: last wait before the rename %" PRId64
                 "ms, renamed after %" PRId64
                 "ms, first attempt to the new host after %" PRId64 "ms\n",
                 broker2_name, gap_last / 1000, (t_rename - t_last) / 1000,
                 (first_new - t_last) / 1000);
        TEST_ASSERT((t_rename - t_last) < (int64_t)(gap_last * 0.75),
                    "Renamed too late (%" PRId64
                    "ms after the last attempt) to tell whether the old "
                    "backoff applies",
                    (t_rename - t_last) / 1000);
        TEST_ASSERT(first_new - t_last < (int64_t)(gap_last * 0.75),
                    "Expected the first attempt to the new host before the "
                    "old host's reconnect time (at least %" PRId64
                    "ms after its last attempt), got it after %" PRId64 "ms",
                    (int64_t)(gap_last * 0.75) / 1000,
                    (first_new - t_last) / 1000);

        reconnect_ev_destroy();
        rd_free(log_interceptor);
        log_interceptor = NULL;
        SUB_TEST_PASS();
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

        do_test_reconnect_backoff_renamed_broker();

        do_test_remove_then_add();

        do_test_reconnect_backoff_readded_broker();

        do_test_down_then_up_no_rebootstrap_loop(5);

        do_test_down_then_up_no_rebootstrap_loop(15);

        do_test_reconnect_backoff_broker_requests();

        do_test_reconnect_backoff_coordinator_moved(rd_true);
        do_test_reconnect_backoff_coordinator_moved(rd_false);

        do_test_reconnect_backoff_all_brokers_down(RD_KAFKA_PRODUCER, rd_true);
        do_test_reconnect_backoff_all_brokers_down(RD_KAFKA_PRODUCER, rd_false);
        do_test_reconnect_backoff_all_brokers_down(RD_KAFKA_CONSUMER, rd_true);
        do_test_reconnect_backoff_all_brokers_down(RD_KAFKA_CONSUMER, rd_false);

        do_test_reconnect_backoff_after_reconnection(rd_true);
        do_test_reconnect_backoff_after_reconnection(rd_false);

        do_test_reconnect_backoff_up_then_close(rd_false);
        do_test_reconnect_backoff_up_then_close(rd_true);

        do_test_kip899_rebootstrap_cases_variations();

        do_test_kip1102_rebootstrap_cases_variations();

        return 0;
}
