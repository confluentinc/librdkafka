/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#include "../src/rdkafka.h"
#include "../src/rdkafka_proto.h"

#define CONSUME_ARRAY        1000
#define TEST_MSGS            100
#define MAX_CONSUME_ATTEMPTS 30

/* Shared producer/admin handles for real-broker tests. Created in
 * main_0179_share_consumer_destroy and reused across sub-tests. */
static rd_kafka_t *common_producer;
static rd_kafka_t *common_admin;

/**
 * @brief Create a share consumer for the mock-broker tests in this file.
 *
 *        Points at the supplied mock bootstraps (test_create_share_consumer
 *        cannot, since it reads the real-broker bootstrap globals), sets
 *        a fast topic.metadata.refresh.interval.ms (500) so the client
 *        observes mock metadata changes quickly, and conditionally enables
 *        explicit ack mode.
 */
static rd_kafka_share_t *
new_share_consumer_for_mock_test(const char *bootstraps,
                                 const char *group_id,
                                 rd_bool_t explicit_ack,
                                 int socket_timeout_ms) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *consumer;
        char buf[32];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        if (socket_timeout_ms > 0) {
                rd_snprintf(buf, sizeof(buf), "%d", socket_timeout_ms);
                test_conf_set(conf, "socket.timeout.ms", buf);
        }
        if (explicit_ack)
                test_conf_set(conf, "share.acknowledgement.mode", "explicit");

        consumer = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(consumer != NULL, "Failed to create share consumer");
        return consumer;
}


/**
 * @brief Enable the three Share APIs (Heartbeat, Fetch, Acknowledge) on
 *        the given mock cluster. Every share-consumer mock test in this
 *        file needs all three.
 */
static void enable_share_apis(rd_kafka_mock_cluster_t *mcluster) {
        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareGroupHeartbeat");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareFetch,
                                                 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareFetch");
        TEST_ASSERT(
            rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareAcknowledge,
                                         1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR,
            "Failed to enable ShareAcknowledge");
}


/**
 * @brief Subscribe to topics.
 */
static void subscribe_topics(rd_kafka_share_t *consumer,
                             const char **topics,
                             int topic_cnt) {
        rd_kafka_topic_partition_list_t *tpl =
            rd_kafka_topic_partition_list_new(topic_cnt);
        for (int i = 0; i < topic_cnt; i++) {
                rd_kafka_topic_partition_list_add(tpl, topics[i],
                                                  RD_KAFKA_PARTITION_UA);
        }
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, tpl),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(tpl);
}


/**
 * @brief Acknowledge messages in [start, end) and log each result.
 */
static void ack_range_logged(rd_kafka_share_t *rkshare,
                             rd_kafka_message_t **rkmessages,
                             int start,
                             int end,
                             int rcvd) {
        int i;
        for (i = start; i < end && i < rcvd; i++) {
                rd_kafka_resp_err_t ack_err =
                    rd_kafka_share_acknowledge(rkshare, rkmessages[i]);
                TEST_SAY("  ack msg[%d] %s[%" PRId32 "]@%" PRId64 " -> %s\n", i,
                         rd_kafka_topic_name(rkmessages[i]->rkt),
                         rkmessages[i]->partition, rkmessages[i]->offset,
                         rd_kafka_err2str(ack_err));
        }
}

/**
 * @brief Destroy a share consumer, using flags variant when non-zero.
 */
static void destroy_share_consumer(rd_kafka_share_t *rkshare,
                                   int destroy_flags) {
        TEST_SAY("Calling destroy with flags 0x%x\n", destroy_flags);
        if (destroy_flags)
                rd_kafka_share_destroy_flags(rkshare, destroy_flags);
        else
                rd_kafka_share_destroy(rkshare);
        TEST_SAY("Successfully destroyed share consumer\n");
}

/**
 * @brief This test uses mock brokers to simulate delayed broker responses and
 * makes commit* calls causing acknowledgements to get cached. Eventually, calls
 * destroy() to validate that it does not hang.
 * @param destroy_flags 0 for normal destroy,
 * RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE to skip consumer close.
 *
 * In case of RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, a __DESTROY error is
 * expected to be returned from the broker thread having requests in-flight
 */
static void
test_destroy_with_cached_acks_and_delayed_broker(int destroy_flags) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_topic_partition_list_t *commit_result = NULL;
        const char *topic         = "0179-destroy-cached-acks-delayed-broker";
        const char *group         = "0179-destroy-cached-acks";
        const int broker_delay_ms = 5000;
        size_t rcvd               = 0;
        int attempts              = 0;
        int i;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST_QUICK("destroy_flags=0x%x", destroy_flags);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        TEST_SAY("Producing %d messages to topic %s\n", TEST_MSGS, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TEST_MSGS, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        TEST_SAY("Creating share consumer with explicit ack\n");
        rkshare =
            new_share_consumer_for_mock_test(bootstraps, group, rd_true, 0);
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Consuming messages (up to %d attempts)\n",
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < 10 && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                }
                attempts++;
        }

        TEST_ASSERT(rcvd >= 10,
                    "Expected at least 10 messages after %d attempts, got %d",
                    MAX_CONSUME_ATTEMPTS, (int)rcvd);
        TEST_SAY("Successfully consumed %d messages\n", (int)rcvd);

        /* Inject %dms delay on the next 3 ShareAcknowledge responses on
         * broker 1 (the only broker in this mock cluster). */
        TEST_SAY(
            "Injecting %dms delay on the next 3 ShareAcknowledge "
            "responses on broker 1\n",
            broker_delay_ms);
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        mcluster, 1, RD_KAFKAP_ShareAcknowledge, 3,
                        RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms,
                        RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms,
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                        broker_delay_ms) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to inject ShareAcknowledge delay");

        /* Step 1: Acknowledge first 2 messages and commit async */
        TEST_SAY(
            "Step 1: Acknowledging messages 0-1 and calling commit_async\n");
        ack_range_logged(rkshare, rkmessages, 0, 2, (int)rcvd);
        /* The below call should keep the broker busy */
        rd_kafka_share_commit_async(rkshare);

        /* Step 2: Acknowledge next 4 messages and commit async */
        TEST_SAY(
            "Step 2: Acknowledging messages 2-5 and calling commit_async "
            "(should cache)\n");
        ack_range_logged(rkshare, rkmessages, 2, 6, (int)rcvd);
        rd_kafka_share_commit_async(rkshare);

        /* Step 3: Acknowledge next 4 messages and commit sync */
        TEST_SAY(
            "Step 3: Acknowledging messages 6-9 and calling "
            "commit_sync (should cache)\n");
        ack_range_logged(rkshare, rkmessages, 6, 10, (int)rcvd);

        rd_kafka_share_commit_sync(rkshare, 1000, &commit_result);
        TEST_ASSERT(commit_result != NULL,
                    "Expected non-NULL commit_sync result");
        TEST_SAY("commit_sync returned %d partition result(s)\n",
                 commit_result->cnt);
        for (i = 0; i < commit_result->cnt; i++) {
                rd_kafka_topic_partition_t *p = &commit_result->elems[i];
                TEST_SAY("  result[%d] %s [%" PRId32 "] err=%s\n", i, p->topic,
                         p->partition, rd_kafka_err2str(p->err));
                TEST_ASSERT(p->err == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
                            "Expected REQUEST_TIMED_OUT for %s [%" PRId32
                            "], got %s",
                            p->topic, p->partition, rd_kafka_err2str(p->err));
        }
        rd_kafka_topic_partition_list_destroy(commit_result);

        /* Destroy all consumed messages */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        TEST_SAY("Calling destroy with flags 0x%x\n", destroy_flags);
        t_start = test_clock();
        destroy_share_consumer(rkshare, destroy_flags);
        t_elapsed_ms = (test_clock() - t_start) / 1000;
        TEST_SAY("Destroy completed in %" PRId64 " ms\n", t_elapsed_ms);

        if (destroy_flags & RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE) {
                /* NO_CONSUMER_CLOSE: destroy must NOT wait for the
                 * delayed broker. */
                TEST_ASSERT(t_elapsed_ms < 2000,
                            "Destroy(NO_CONSUMER_CLOSE) took %" PRId64
                            " ms, expected < 2000 ms",
                            t_elapsed_ms);
        } else {
                /* Full close: destroy waits for the existing commit
                 * async request and the session-leave request. Expect
                 * roughly 2 * broker_delay_ms plus small overhead. */
                int64_t expected_max_ms = 2 * broker_delay_ms + 2000;
                TEST_ASSERT(t_elapsed_ms <= expected_max_ms,
                            "Destroy took %" PRId64 " ms, expected <= %" PRId64
                            " ms",
                            t_elapsed_ms, expected_max_ms);
        }

        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

/**
 * @brief is_fatal_cb hook for test_broker_decommission_with_commit_sync.
 *
 * The decommission of a broker mid-flight produces __TRANSPORT and
 * __ALL_BROKERS_DOWN errors as the connection is dropped and the client
 * tries to reconnect. These are expected and should not fail the test.
 */
static int decommission_is_fatal_cb(rd_kafka_t *rk,
                                    rd_kafka_resp_err_t err,
                                    const char *reason) {
        if (err == RD_KAFKA_RESP_ERR__TRANSPORT ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                TEST_SAY("Ignoring expected error: %s: %s\n",
                         rd_kafka_err2name(err), reason);
                return 0;
        }
        return 1;
}

struct decommission_thread_args {
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic;
        int32_t broker_id;     /**< Broker to decommission. */
        int32_t new_leader_id; /**< Broker to migrate the partition to first. */
        int32_t partition;     /**< Partition whose leader to migrate. */
        int delay_ms;
};

/**
 * @brief Background thread: sleeps for delay_ms, then (1) migrates the
 *        partition leader off the target broker, (2) hides the target
 *        broker from metadata responses without dropping its TCP
 *        connections.
 *
 *        Step 2 uses rd_kafka_mock_broker_remove_from_metadata (instead of
 *        rd_kafka_mock_broker_decommission) so the in-flight ShareAck
 *        stays parked on the broker. The client's next metadata refresh
 *        (served by the surviving broker) sees the target broker is gone,
 *        triggers rd_kafka_broker_decommission() on the client side, and
 *        the OP_TERMINATE handler purges the parked ShareAck with
 *        __DESTROY_BROKER. This races deterministically in our favor —
 *        the connection drop never happens until the metadata-driven
 *        decommission has already fired.
 */
static int decommission_after_delay(void *arg) {
        struct decommission_thread_args *a = arg;
        rd_usleep(a->delay_ms * 1000, NULL);

        TEST_SAY("Background thread: moving partition %" PRId32
                 " leader from %" PRId32 " to %" PRId32 "\n",
                 a->partition, a->broker_id, a->new_leader_id);
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(
                        a->mcluster, a->topic, a->partition,
                        a->new_leader_id) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition leader");

        TEST_SAY("Background thread: removing broker %" PRId32
                 " from metadata (connections stay alive)\n",
                 a->broker_id);
        TEST_ASSERT(
            rd_kafka_mock_broker_remove_from_metadata(
                a->mcluster, a->broker_id) == RD_KAFKA_RESP_ERR_NO_ERROR,
            "Failed to remove broker %" PRId32 " from metadata", a->broker_id);
        return 0;
}

/**
 * @brief Test that commit_sync returns __DESTROY for partitions whose
 *        broker is decommissioned mid-flight
 *
 *        Setup: 2-broker mock cluster, 2-partition topic with partition 0
 *        led by broker 1 and partition 1 led by broker 2.
 *
 *        Flow:
 *          1. Consume from both partitions in implicit-ack mode. After
 *             the first batch arrives, pick the OTHER broker
 *             as the target and inject the 5s ShareAck delay on it.
 *             Continue consuming until all messages are received.
 *          2. Spawn a background thread that, ~200ms after commit_sync
 *             starts:
 *             a. migrates the target partition's leader to the
 *                surviving broker,
 *             b. removes the target broker from cluster metadata (its
 *                TCP connection stays up so its outbuf still holds the
 *                parked ShareAck).
 *          3. On the app thread, call commit_sync. The target partition's
 *             ShareAck is parked on the target broker (5s delay).
 *          4. Periodic metadata refresh — routed via the surviving
 *             broker — sees the target broker missing, runs
 *             rd_kafka_broker_decommission() on it. The OP_TERMINATE
 *             handler purges the parked ShareAck with __DESTROY_BROKER.
 *          5. Reply handler stamps the target-partition entry in
 *             commit_sync_request as __DESTROY_BROKER.
 *
 *        Assertion: commit_sync result has 1 entry for the target
 *        partition with err == __DESTROY_BROKER.
 */
static void test_broker_decommission_with_commit_sync(int destroy_flags,
                                                      rd_bool_t explicit_ack) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_topic_partition_list_t *result = NULL;
        const char *topic;
        const char *group               = "0179-decommission-commit-sync";
        int32_t target_broker_id        = -1;
        int32_t surviving_broker_id     = -1;
        int32_t target_partition        = -1;
        const int broker_delay_ms       = 300000;
        const int decommission_delay_ms = 200;
        size_t rcvd                     = 0;
        int attempts                    = 0;
        int i;
        struct decommission_thread_args args;
        thrd_t decommission_thrd;

        SUB_TEST_QUICK("destroy_flags=0x%x ack_mode=%s", destroy_flags,
                       explicit_ack ? "explicit" : "implicit");

        /* Suppress expected __TRANSPORT / __ALL_BROKERS_DOWN errors that
         * fire when the target broker connection is dropped. */
        test_curr->is_fatal_cb = decommission_is_fatal_cb;

        /* 2-broker mock cluster with all three Share APIs enabled. */
        mcluster = test_mock_cluster_new(2, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        /* 2-partition topic: partition 0 led by broker 1 (surviving),
         * partition 1 led by broker 2 (target). Consuming + acking from
         * BOTH partitions establishes UP connections to both brokers,
         * so when broker 2 is later blocked by the 5s ShareAck delay,
         * broker 1 is already up and idle and free to serve metadata
         * refresh requests. */
        topic = test_mk_topic_name("0179-decommission-commit-sync", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 2, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 0 leader to broker 1");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 1 leader to broker 2");

        /* Produce TEST_MSGS/2 messages to each partition (TEST_MSGS total). */
        TEST_SAY("Producing %d messages to each partition of topic %s\n",
                 TEST_MSGS / 2, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Share consumer with fast metadata refresh so the client
         * observes broker 2's removal from cluster metadata during
         * the 5s ShareAck delay. */
        TEST_SAY("Creating share consumer (%s ack)\n",
                 explicit_ack ? "explicit" : "implicit");
        /* Bump socket.timeout.ms so the intentionally-delayed
         * ShareAcknowledge (broker_delay_ms = 300000) doesn't fail
         * client-side via per-buffer timeout before the metadata-driven
         * broker decommission has a chance to surface __DESTROY_BROKER. */
        rkshare = new_share_consumer_for_mock_test(bootstraps, group,
                                                   explicit_ack, 300000);
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Consuming up to %d messages (max %d attempts)\n", TEST_MSGS,
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < TEST_MSGS && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;

                        if (explicit_ack) {
                                size_t k;
                                for (k = rcvd - batch_rcvd; k < rcvd; k++)
                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[k]);
                        }
                }
                attempts++;
        }
        TEST_ASSERT(rcvd >= TEST_MSGS,
                    "Expected %d messages, got %d after %d attempts", TEST_MSGS,
                    (int)rcvd, attempts);
        TEST_SAY("Consumed %d msgs total in %d attempts\n", (int)rcvd,
                 attempts);

        /* The last message of the last batch tells us which broker
         * answered the second (and last) consume_batch — that's the
         * broker holding the unacked records that commit_sync will ship
         * to. Decommission that broker; the other survives. */
        target_partition    = rkmessages[rcvd - 1]->partition;
        target_broker_id    = (target_partition == 0) ? 1 : 2;
        surviving_broker_id = (target_broker_id == 1) ? 2 : 1;
        TEST_SAY("Target partition = %" PRId32 ", target broker = %" PRId32
                 ", surviving broker = %" PRId32 "\n",
                 target_partition, target_broker_id, surviving_broker_id);

        TEST_SAY(
            "Injecting %dms delay on ShareAcknowledge for broker "
            "%" PRId32 "\n",
            broker_delay_ms, target_broker_id);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, target_broker_id, RD_KAFKAP_ShareAcknowledge, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms);

        /* Schedule the broker takedown ~200ms after we call commit_sync,
         * so the request is in flight when it happens. */
        args.mcluster      = mcluster;
        args.topic         = topic;
        args.broker_id     = target_broker_id;
        args.new_leader_id = surviving_broker_id;
        args.partition     = target_partition;
        args.delay_ms      = decommission_delay_ms;
        TEST_ASSERT(thrd_create(&decommission_thrd, decommission_after_delay,
                                &args) == thrd_success,
                    "thrd_create failed");

        /* Call commit_sync on the app thread; will block until the broker
         * decommission fails the in-flight ack or the timeout fires.
         * Use a long timeout (60s) so under CI load the commit_sync
         * timeout never wins the race against the metadata-driven
         * decommission, which is the path that surfaces __DESTROY_BROKER. */
        TEST_SAY("Calling commit_sync (timeout 60s)\n");
        error = rd_kafka_share_commit_sync(rkshare, 60000, &result);

        /* Wait for the decommission thread before inspecting state. */
        thrd_join(decommission_thrd, NULL);

        if (error) {
                TEST_SAY("commit_sync returned error: %s\n",
                         rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        }

        TEST_ASSERT(result != NULL,
                    "Expected commit_sync to return a non-NULL results list");
        TEST_SAY("commit_sync returned %d partition result(s)\n", result->cnt);

        /* Only the last batch's acks reach commit_sync.
         * So we expect commit_sync's
         * results to contain only the target partition, whose ShareAck
         * was sent to the target broker and failed with
         * __DESTROY_BROKER → stamped as __DESTROY_BROKER here. */
        TEST_ASSERT(result->cnt == 1,
                    "Expected 1 partition result (partition %" PRId32
                    " only), got %d",
                    target_partition, result->cnt);

        for (i = 0; i < result->cnt; i++) {
                rd_kafka_topic_partition_t *p = &result->elems[i];
                TEST_SAY("  result[%d] %s [%" PRId32 "] err=%s\n", i, p->topic,
                         p->partition, rd_kafka_err2str(p->err));
                TEST_ASSERT(p->partition == target_partition,
                            "Expected partition %" PRId32
                            " in result, got %" PRId32,
                            target_partition, p->partition);
                /* TODO KIP-932: Fix error code */
                TEST_ASSERT(p->err == RD_KAFKA_RESP_ERR__DESTROY_BROKER,
                            "Expected __DESTROY_BROKER for partition %" PRId32
                            " (broker %" PRId32 ", decommissioned), got %s",
                            target_partition, target_broker_id,
                            rd_kafka_err2str(p->err));
        }

        rd_kafka_topic_partition_list_destroy(result);

        /* Cleanup */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        destroy_share_consumer(rkshare, destroy_flags);
        test_mock_cluster_destroy(mcluster);

        /* Restore the default fatal-error handler. */
        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Test that a broker decommissioned mid-ShareFetch is handled
 *        gracefully: the in-flight ShareFetch fails internally with
 *        __DESTROY_BROKER, the consumer keeps polling the surviving
 *        broker, and the consumer continues to function for fetch +
 *        ack + destroy on the surviving broker.
 *
 *        Setup mirrors test_broker_decommission_with_commit_sync —
 *        2-broker mock cluster, 2-partition topic with RF=2, fast
 *        metadata refresh, dynamic target-broker selection based on
 *        which broker answered the last consume_batch. The only
 *        differences:
 *           - We delay ShareFetch (not ShareAcknowledge).
 *           - We assert at the consume_batch API boundary: no error
 *             surfaces and no messages are returned (target broker is
 *             dead, surviving broker is idle).
 *           - We then produce more messages to the surviving broker's
 *             partition and verify they can still be fetched.
 *
 *        Note: this test does NOT validate leader-migration behavior
 *        for the orphaned partition — that's punted to a later change.
 */
static void test_broker_decommission_with_consume_batch(int destroy_flags) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        const char *topic;
        const char *group                 = "0179-decommission-consume-batch";
        const int32_t target_broker_id    = 2;
        const int32_t surviving_broker_id = 1;
        const int32_t target_partition    = 1;
        const int32_t surviving_partition = 0;
        const int broker_delay_ms         = 5000;
        const int decommission_delay_ms   = 200;
        const int extra_msgs              = TEST_MSGS / 2;
        size_t rcvd                       = 0;
        size_t fetch_rcvd                 = 0;
        size_t extra_rcvd                 = 0;
        int attempts                      = 0;
        rd_bool_t flushed                 = rd_false;
        int i;
        struct decommission_thread_args args;
        thrd_t decommission_thrd;

        SUB_TEST_QUICK();

        /* Suppress expected __TRANSPORT / __ALL_BROKERS_DOWN errors that
         * fire when the target broker connection is dropped. */
        test_curr->is_fatal_cb = decommission_is_fatal_cb;

        /* 2-broker mock cluster with all three Share APIs enabled. */
        mcluster = test_mock_cluster_new(2, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        /* 2-partition topic, RF=2: p0 led by broker 1, p1 led by broker 2. */
        topic = test_mk_topic_name("0179-decommission-consume-batch", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 2, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 0 leader to broker 1");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 1 leader to broker 2");

        /* Produce TEST_MSGS/2 messages to each partition. */
        TEST_SAY("Producing %d messages to each partition of topic %s\n",
                 TEST_MSGS / 2, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Implicit-ack share consumer with fast metadata refresh so the
         * client picks up the target broker's removal during the 5s
         * ShareFetch delay. */
        TEST_SAY("Creating share consumer (implicit ack)\n");
        rkshare = new_share_consumer_for_mock_test(
            bootstraps, group, rd_false, 0 /* default socket tmout */);
        subscribe_topics(rkshare, &topic, 1);

        /* Drain both partitions. We loop one extra empty round after
         * rcvd reaches TEST_MSGS so the final batch's implicit acks get
         * piggybacked on a trailing ShareFetch — otherwise the last
         * batch's records stay ACQUIRED and could be replayed. */
        TEST_SAY("Consuming up to %d messages (max %d attempts)\n", TEST_MSGS,
                 MAX_CONSUME_ATTEMPTS);
        while (!flushed && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                } else if (rcvd >= TEST_MSGS) {
                        /* All messages consumed and a trailing empty
                         * batch fired the piggyback ack. */
                        flushed = rd_true;
                }
                attempts++;
        }
        TEST_ASSERT(rcvd >= TEST_MSGS && flushed,
                    "Expected %d messages and trailing empty flush after "
                    "%d attempts, got rcvd=%d flushed=%d",
                    TEST_MSGS, MAX_CONSUME_ATTEMPTS, (int)rcvd, flushed);
        TEST_SAY("Consumed %d msgs total in %d attempts\n", (int)rcvd,
                 attempts);

        /* Inject 5s delay on the target broker's next ShareFetch so the
         * consume_batch below has an in-flight ShareFetch parked when
         * the decommission fires. */
        TEST_SAY("Injecting %dms delay on ShareFetch for broker %" PRId32 "\n",
                 broker_delay_ms, target_broker_id);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, target_broker_id, RD_KAFKAP_ShareFetch, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms);

        /* Background thread: ~200ms later, migrate target partition's
         * leader to the surviving broker and remove the target broker
         * from mock metadata. The client's metadata refresh then runs
         * rd_kafka_broker_decommission() on the target broker
         * client-side, purging the parked ShareFetch with
         * __DESTROY_BROKER. */
        args.mcluster      = mcluster;
        args.topic         = topic;
        args.broker_id     = target_broker_id;
        args.new_leader_id = surviving_broker_id;
        args.partition     = target_partition;
        args.delay_ms      = decommission_delay_ms;
        TEST_ASSERT(thrd_create(&decommission_thrd, decommission_after_delay,
                                &args) == thrd_success,
                    "thrd_create failed");

        /* Call consume_batch with a 3s timeout — the background thread
         * fires the decommission ~200ms in, the in-flight ShareFetch on
         * the target broker is purged with __DESTROY_BROKER, and the
         * fanout retry skips the (now-terminating) target and polls the
         * surviving broker, which has no new messages. consume_batch
         * times out cleanly with 0 messages and no app-visible error. */
        TEST_SAY(
            "Calling consume_batch (timeout 3s) — expecting no "
            "messages and no app-visible error\n");
        fetch_rcvd = CONSUME_ARRAY;
        error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages + rcvd,
                                             &fetch_rcvd);

        thrd_join(decommission_thrd, NULL);

        TEST_ASSERT(error == NULL,
                    "Expected consume_batch to return NULL error, got %s",
                    error ? rd_kafka_error_string(error) : "(null)");
        TEST_ASSERT(fetch_rcvd == 0,
                    "Expected 0 messages from consume_batch (target broker "
                    "decommissioned, surviving broker has no new "
                    "messages), got %d",
                    (int)fetch_rcvd);

        /* Sanity check: the consumer must still be alive and able to
         * fetch from the surviving broker. Produce extra messages to
         * the surviving broker's partition and consume them. */
        TEST_SAY("Producing %d messages to surviving partition %" PRId32 "\n",
                 extra_msgs, surviving_partition);
        test_produce_msgs_easy_v(topic, 0, surviving_partition, 0, extra_msgs,
                                 16, "bootstrap.servers", bootstraps, NULL);

        TEST_SAY("Consuming up to %d additional messages (max %d attempts)\n",
                 extra_msgs, MAX_CONSUME_ATTEMPTS);
        attempts = 0;
        while (extra_rcvd < (size_t)extra_msgs &&
               attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd - extra_rcvd;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd + extra_rcvd, &batch_rcvd);

                TEST_ASSERT(error == NULL,
                            "Expected NULL error from sanity consume_batch, "
                            "got %s",
                            error ? rd_kafka_error_string(error) : "(null)");
                if (batch_rcvd > 0) {
                        size_t k;
                        TEST_SAY("Sanity attempt %d: consumed %d messages\n",
                                 attempts, (int)batch_rcvd);
                        for (k = 0; k < batch_rcvd; k++) {
                                rd_kafka_message_t *m =
                                    rkmessages[rcvd + extra_rcvd + k];
                                TEST_ASSERT(m->partition == surviving_partition,
                                            "Sanity batch contained a "
                                            "message from partition %" PRId32
                                            ", expected only %" PRId32,
                                            m->partition, surviving_partition);
                        }
                        extra_rcvd += batch_rcvd;
                }
                attempts++;
        }
        TEST_ASSERT(extra_rcvd >= (size_t)extra_msgs,
                    "Expected %d sanity messages from surviving broker, "
                    "got %d after %d attempts",
                    extra_msgs, (int)extra_rcvd, attempts);

        rcvd += extra_rcvd;

        /* Cleanup */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        destroy_share_consumer(rkshare, destroy_flags);
        test_mock_cluster_destroy(mcluster);

        /* Restore the default fatal-error handler. */
        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Test that rd_kafka_share_consumer_close() completes gracefully
 *        when one of the brokers it ships acks/leaves to is decommissioned
 *        mid-call.
 *
 *        Setup mirrors test_broker_decommission_with_commit_sync —
 *        2-broker mock cluster, 2-partition topic with RF=2, fast
 *        metadata refresh. close() will then ship a ShareAck
 *        for those records to whichever broker served them,
 *        plus session-leave requests to both brokers.
 *
 *        The target broker (the one holding the unacked batch) gets a
 *        5s ShareAck rtt delay. While that ShareAck is parked, the
 *        background thread removes the broker from mock metadata; the
 *        client decommissions it; the in-flight ShareAck is purged
 *        with __DESTROY_BROKER.
 *
 *        Expectation: rd_kafka_share_consumer_close() returns NULL (no
 *        error) and does not hang. destroy() afterwards also completes.
 */
static void test_broker_decommission_during_close(int destroy_flags,
                                                  rd_bool_t explicit_ack) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        const char *topic;
        const char *group               = "0179-decommission-close";
        int32_t target_broker_id        = -1;
        int32_t surviving_broker_id     = -1;
        int32_t target_partition        = -1;
        const int broker_delay_ms       = 5000;
        const int decommission_delay_ms = 200;
        size_t rcvd                     = 0;
        int attempts                    = 0;
        int i;
        struct decommission_thread_args args;
        thrd_t decommission_thrd;

        SUB_TEST_QUICK("destroy_flags=0x%x ack_mode=%s", destroy_flags,
                       explicit_ack ? "explicit" : "implicit");

        /* Suppress expected __TRANSPORT / __ALL_BROKERS_DOWN errors that
         * fire when the target broker connection is dropped. */
        test_curr->is_fatal_cb = decommission_is_fatal_cb;

        /* 2-broker mock cluster with all three Share APIs enabled. */
        mcluster = test_mock_cluster_new(2, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        /* 2-partition topic, RF=2: p0 led by broker 1, p1 led by broker 2. */
        topic = test_mk_topic_name("0179-decommission-close", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 2, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 0 leader to broker 1");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 1 leader to broker 2");

        /* Produce TEST_MSGS/2 messages to each partition. */
        TEST_SAY("Producing %d messages to each partition of topic %s\n",
                 TEST_MSGS / 2, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Implicit-ack share consumer with fast metadata refresh so the
         * client picks up the target broker's removal during the 5s
         * ShareAck delay. */
        TEST_SAY("Creating share consumer (%s ack)\n",
                 explicit_ack ? "explicit" : "implicit");
        rkshare = new_share_consumer_for_mock_test(bootstraps, group,
                                                   explicit_ack, 0);
        subscribe_topics(rkshare, &topic, 1);

        /* Drain both partitions WITHOUT a trailing empty flush — the
         * last batch's records remain ACQUIRED (unacked). close() will
         * ship a ShareAck for those records to whichever broker served
         * them. */
        TEST_SAY("Consuming up to %d messages (max %d attempts)\n", TEST_MSGS,
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < TEST_MSGS && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;

                        if (explicit_ack) {
                                size_t k;
                                for (k = rcvd - batch_rcvd; k < rcvd; k++)
                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[k]);
                        }
                }
                attempts++;
        }
        TEST_ASSERT(rcvd >= TEST_MSGS,
                    "Expected %d messages, got %d after %d attempts", TEST_MSGS,
                    (int)rcvd, attempts);
        TEST_SAY("Consumed %d msgs total in %d attempts\n", (int)rcvd,
                 attempts);

        /* The last message of the last batch tells us which broker
         * holds the unacked records — that's the broker close()'s
         * ShareAck will target. Decommission that broker; the other
         * survives. */
        target_partition    = rkmessages[rcvd - 1]->partition;
        target_broker_id    = (target_partition == 0) ? 1 : 2;
        surviving_broker_id = (target_broker_id == 1) ? 2 : 1;
        TEST_SAY("Target partition = %" PRId32 ", target broker = %" PRId32
                 ", surviving broker = %" PRId32 "\n",
                 target_partition, target_broker_id, surviving_broker_id);

        /* Inject 5s delay on the target broker's next ShareAcknowledge
         * so close()'s ShareAck stays in flight long enough for the
         * decommission to fire. */
        TEST_SAY(
            "Injecting %dms delay on ShareAcknowledge for broker "
            "%" PRId32 "\n",
            broker_delay_ms, target_broker_id);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, target_broker_id, RD_KAFKAP_ShareAcknowledge, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms);

        /* Background thread: ~200ms after close() starts, migrate
         * target partition's leader and remove the target broker from
         * mock metadata. The client's metadata refresh then runs
         * rd_kafka_broker_decommission() on the target broker
         * client-side, purging the parked ShareAck with
         * __DESTROY_BROKER. close() must absorb that gracefully. */
        args.mcluster      = mcluster;
        args.topic         = topic;
        args.broker_id     = target_broker_id;
        args.new_leader_id = surviving_broker_id;
        args.partition     = target_partition;
        args.delay_ms      = decommission_delay_ms;
        TEST_ASSERT(thrd_create(&decommission_thrd, decommission_after_delay,
                                &args) == thrd_success,
                    "thrd_create failed");

        /* Call close(). It will ship a ShareAck for the unacked batch
         * to the target broker (which is the one parking the request),
         * plus session-leave requests to both brokers. The target
         * broker's ShareAck is purged with __DESTROY_BROKER mid-flight;
         * close() must not surface this to the app. */
        TEST_SAY(
            "Calling rd_kafka_share_consumer_close() — expecting "
            "NULL error\n");
        error = rd_kafka_share_consumer_close(rkshare);

        thrd_join(decommission_thrd, NULL);

        TEST_ASSERT(error == NULL,
                    "Expected close() to return NULL error, got %s",
                    error ? rd_kafka_error_string(error) : "(null)");

        /* Cleanup */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        destroy_share_consumer(rkshare, destroy_flags);
        test_mock_cluster_destroy(mcluster);

        /* Restore the default fatal-error handler. */
        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Test that rd_kafka_share_commit_async() does not hang when the
 *        broker it ships acks to is decommissioned mid-flight.
 *
 *        Setup mirrors test_broker_decommission_during_close — drain both
 *        partitions without a trailing flush so the last batch's records
 *        stay ACQUIRED. commit_async() then ships a ShareAck for those
 *        records to whichever broker served them. The target broker has a
 *        5s ShareAck rtt delay; while the request is parked, the
 *        background thread removes the broker from mock metadata and the
 *        client decommissions it, purging the in-flight ShareAck with
 *        __DESTROY_BROKER.
 *
 *        Expectation: rd_kafka_share_commit_async() returns NULL (no
 *        error) and does not block on the in-flight ack.
 *        destroy() afterwards completes cleanly.
 *
 *        TODO KIP-932 destroy: add assertion for ack callback once it is
 *        implemented.
 */
static void test_broker_decommission_with_commit_async(int destroy_flags,
                                                       rd_bool_t explicit_ack) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        const char *topic;
        const char *group               = "0179-decommission-commit-async";
        int32_t target_broker_id        = -1;
        int32_t surviving_broker_id     = -1;
        int32_t target_partition        = -1;
        const int broker_delay_ms       = 5000;
        const int decommission_delay_ms = 200;
        size_t rcvd                     = 0;
        int attempts                    = 0;
        int i;
        struct decommission_thread_args args;
        thrd_t decommission_thrd;

        SUB_TEST_QUICK("destroy_flags=0x%x ack_mode=%s", destroy_flags,
                       explicit_ack ? "explicit" : "implicit");

        /* Suppress expected __TRANSPORT / __ALL_BROKERS_DOWN errors that
         * fire when the target broker connection is dropped. */
        test_curr->is_fatal_cb = decommission_is_fatal_cb;

        /* 2-broker mock cluster with all three Share APIs enabled. */
        mcluster = test_mock_cluster_new(2, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        /* 2-partition topic, RF=2: p0 led by broker 1, p1 led by broker 2. */
        topic = test_mk_topic_name("0179-decommission-commit-async", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 2, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 0 leader to broker 1");
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set partition 1 leader to broker 2");

        /* Produce TEST_MSGS/2 messages to each partition. */
        TEST_SAY("Producing %d messages to each partition of topic %s\n",
                 TEST_MSGS / 2, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, TEST_MSGS / 2, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Implicit-ack share consumer with fast metadata refresh so the
         * client picks up the target broker's removal during the 5s
         * ShareAck delay. */
        TEST_SAY("Creating share consumer (%s ack)\n",
                 explicit_ack ? "explicit" : "implicit");
        rkshare = new_share_consumer_for_mock_test(bootstraps, group,
                                                   explicit_ack, 0);
        subscribe_topics(rkshare, &topic, 1);

        /* Drain both partitions WITHOUT a trailing empty flush — the
         * last batch's records remain ACQUIRED (unacked).
         * commit_async() will ship a ShareAck for those records to
         * whichever broker served them. */
        TEST_SAY("Consuming up to %d messages (max %d attempts)\n", TEST_MSGS,
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < TEST_MSGS && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;

                        if (explicit_ack) {
                                size_t k;
                                for (k = rcvd - batch_rcvd; k < rcvd; k++)
                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[k]);
                        }
                }
                attempts++;
        }
        TEST_ASSERT(rcvd >= TEST_MSGS,
                    "Expected %d messages, got %d after %d attempts", TEST_MSGS,
                    (int)rcvd, attempts);
        TEST_SAY("Consumed %d msgs total in %d attempts\n", (int)rcvd,
                 attempts);

        /* The last message of the last batch tells us which broker
         * holds the unacked records — that's the broker the
         * commit_async ShareAck will target. Decommission that broker;
         * the other survives. */
        target_partition    = rkmessages[rcvd - 1]->partition;
        target_broker_id    = (target_partition == 0) ? 1 : 2;
        surviving_broker_id = (target_broker_id == 1) ? 2 : 1;
        TEST_SAY("Target partition = %" PRId32 ", target broker = %" PRId32
                 ", surviving broker = %" PRId32 "\n",
                 target_partition, target_broker_id, surviving_broker_id);

        /* Inject 5s delay on the target broker's next ShareAcknowledge
         * so the commit_async ShareAck stays in flight long enough for
         * the decommission to fire. */
        TEST_SAY(
            "Injecting %dms delay on ShareAcknowledge for broker "
            "%" PRId32 "\n",
            broker_delay_ms, target_broker_id);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, target_broker_id, RD_KAFKAP_ShareAcknowledge, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms);

        /* Background thread: ~200ms after commit_async starts, migrate
         * target partition's leader and remove the target broker from
         * mock metadata. The client's metadata refresh then runs
         * rd_kafka_broker_decommission() on the target broker
         * client-side, purging the parked ShareAck with
         * __DESTROY_BROKER. */
        args.mcluster      = mcluster;
        args.topic         = topic;
        args.broker_id     = target_broker_id;
        args.new_leader_id = surviving_broker_id;
        args.partition     = target_partition;
        args.delay_ms      = decommission_delay_ms;
        TEST_ASSERT(thrd_create(&decommission_thrd, decommission_after_delay,
                                &args) == thrd_success,
                    "thrd_create failed");

        /* Call commit_async(). It should return immediately without
         * blocking on the in-flight ack. */
        TEST_SAY(
            "Calling rd_kafka_share_commit_async() — expecting "
            "NULL error and no hang\n");
        error = rd_kafka_share_commit_async(rkshare);

        TEST_ASSERT(error == NULL,
                    "Expected commit_async() to return NULL error, got %s",
                    error ? rd_kafka_error_string(error) : "(null)");

        /* TODO KIP-932 destroy: add assertion for ack callback once it is
         *               implemented. */

        thrd_join(decommission_thrd, NULL);

        /* Cleanup */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        destroy_share_consumer(rkshare, destroy_flags);
        test_mock_cluster_destroy(mcluster);

        /* Restore the default fatal-error handler. */
        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


// static void test_leader_migration_mid_session_destroy(int destroy_flags) {
//         rd_kafka_mock_cluster_t *mcluster;
//         const char *bootstraps;
//         rd_kafka_share_t *rkshare;
//         rd_kafka_error_t *error;
//         rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
//         const char *topic = "0179-leader-migration-mid-session";
//         const char *group = "0179-leader-migration-mid-session";
//         const int msgs_per_round = 5;
//         size_t rcvd = 0;
//         int attempts = 0;
//         size_t i;
//
//         SUB_TEST_QUICK("destroy_flags=0x%x", destroy_flags);
//
//         mcluster = test_mock_cluster_new(2, &bootstraps);
//         enable_share_apis(mcluster);
//         rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);
//
//         /* 1 partition, RF=2 so both brokers know about it. Initial
//          * leader = broker 1. */
//         TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 2) ==
//                         RD_KAFKA_RESP_ERR_NO_ERROR,
//                     "Failed to create mock topic");
//         TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1)
//         ==
//                         RD_KAFKA_RESP_ERR_NO_ERROR,
//                     "Failed to set initial leader to broker 1");
//
//         /* Produce a first batch so the consumer can fetch from broker 1. */
//         TEST_SAY("Producing %d messages to broker 1\n", msgs_per_round);
//         test_produce_msgs_easy_v(topic, 0, 0, 0, msgs_per_round, 16,
//                                  "bootstrap.servers", bootstraps, NULL);
//
//         rkshare = new_share_consumer_for_mock_test(
//             bootstraps, group, rd_false /* implicit */);
//         subscribe_topics(rkshare, &topic, 1);
//
//         /* Round 1: consume from broker 1. After this, the toppar is in
//          * broker 1's toppars_in_session. */
//         TEST_SAY("Round 1: consume from broker 1 (initial leader)\n");
//         while (rcvd < (size_t)msgs_per_round &&
//                attempts++ < MAX_CONSUME_ATTEMPTS) {
//                 size_t batch_rcvd = CONSUME_ARRAY - rcvd;
//                 error = rd_kafka_share_consume_batch(
//                     rkshare, 3000, rkmessages + rcvd, &batch_rcvd);
//                 if (error)
//                         rd_kafka_error_destroy(error);
//                 else
//                         rcvd += batch_rcvd;
//         }
//         TEST_ASSERT(rcvd >= (size_t)msgs_per_round,
//                     "Round 1: expected %d msgs, got %d", msgs_per_round,
//                     (int)rcvd);
//         TEST_SAY("Round 1: consumed %d messages\n", (int)rcvd);
//
//         /* Migrate the partition leader to broker 2. */
//         TEST_SAY("Migrating partition leader from broker 1 to broker 2\n");
//         TEST_ASSERT(rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 2)
//         ==
//                         RD_KAFKA_RESP_ERR_NO_ERROR,
//                     "Failed to migrate leader to broker 2");
//
//         /* Produce a second batch so the consumer has something to fetch
//          * from the new leader. */
//         TEST_SAY("Producing %d more messages (now under broker 2)\n",
//                  msgs_per_round);
//         test_produce_msgs_easy_v(topic, 0, 0, 0, msgs_per_round, 16,
//                                  "bootstrap.servers", bootstraps, NULL);
//
//         /* Round 2: consume from broker 2. The first ShareFetch reply
//          * from broker 2 will TAILQ_INSERT_TAIL the toppar into broker
//          * 2's toppars_in_session, reusing the same rktp_rkb_session_link
//          * field that broker 1 was using. With the fix, broker 1's list
//          * is cleared by PARTITION_LEAVE before this happens. */
//         TEST_SAY("Round 2: consume from broker 2 (new leader)\n");
//         attempts = 0;
//         while (rcvd < (size_t)(2 * msgs_per_round) &&
//                attempts++ < MAX_CONSUME_ATTEMPTS) {
//                 size_t batch_rcvd = CONSUME_ARRAY - rcvd;
//                 error = rd_kafka_share_consume_batch(
//                     rkshare, 3000, rkmessages + rcvd, &batch_rcvd);
//                 if (error)
//                         rd_kafka_error_destroy(error);
//                 else
//                         rcvd += batch_rcvd;
//         }
//         TEST_ASSERT(rcvd >= (size_t)(2 * msgs_per_round),
//                     "Round 2: expected %d total msgs, got %d",
//                     2 * msgs_per_round, (int)rcvd);
//         TEST_SAY("Round 2: consumed %d total messages\n", (int)rcvd);
//
//         for (i = 0; i < rcvd; i++)
//                 rd_kafka_message_destroy(rkmessages[i]);
//
//         /* Close + destroy. Without the leader-migration fix, broker 1's
//          * destroy_final would assert on non-empty toppars_in_session. */
//         TEST_SAY("Calling destroy (flags 0x%x)\n", destroy_flags);
//         destroy_share_consumer(rkshare, destroy_flags);
//
//         test_mock_cluster_destroy(mcluster);
//         SUB_TEST_PASS();
// }


/**
 * @brief Destroy while the consumer's cgrp is mid-rebalance.
 *
 * Drives the cgrp into an intermediate join-state by:
 *   1. Subscribing and consuming at least one batch (cgrp now in steady
 *      state with a live assignment).
 *   2. Injecting a long RTT delay on ShareGroupHeartbeat so the next
 *      heartbeat sent by the cgrp will be stuck in-flight.
 *   3. Calling unsubscribe, which drives the cgrp into
 *      wait-unassign-call -> wait-incr-unassign-to-complete and ends up
 *      waiting on the (now-blocked) leave heartbeat.
 *   4. Calling destroy with the given flags. Destroy must NOT wait the
 *      full injected RTT.
 */
static void test_destroy_during_rebalance(int destroy_flags) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        const char *topic            = "0179-destroy-during-rebalance";
        const char *group            = "0179-destroy-during-rebalance";
        const int heartbeat_delay_ms = 10000;
        size_t rcvd                  = 0;
        int attempts                 = 0;
        rd_ts_t t_start, t_elapsed_ms;
        size_t i;

        SUB_TEST_QUICK("destroy_flags=0x%x", destroy_flags);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce so the consumer's first consume_batch returns a real
         * message, guaranteeing the cgrp reached up/steady with a live
         * assignment. */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 10, 16, "bootstrap.servers",
                                 bootstraps, NULL);

        rkshare = new_share_consumer_for_mock_test(
            bootstraps, group, rd_false /* implicit ack */, 0);
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Waiting for first batch (signals assignment is live)\n");
        while (rcvd == 0 && attempts++ < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &batch_rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
                else
                        rcvd = batch_rcvd;
        }
        TEST_ASSERT(rcvd > 0,
                    "Expected at least 1 msg before forcing rebalance; "
                    "got %d after %d attempts",
                    (int)rcvd, attempts);
        for (i = 0; i < rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        /* Block all subsequent ShareGroupHeartbeats. The next heartbeat
         * sent by the cgrp (including the leave-group heartbeat) will be
         * stuck in flight for heartbeat_delay_ms. */
        TEST_SAY("Injecting %dms RTT delay on ShareGroupHeartbeat\n",
                 heartbeat_delay_ms);
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                        heartbeat_delay_ms) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to inject heartbeat delay");

        /* Drive the cgrp into the leave-flow. */
        TEST_SAY("Calling unsubscribe to drive cgrp into wait-incr-unassign\n");
        TEST_ASSERT(!rd_kafka_share_unsubscribe(rkshare), "unsubscribe failed");

        /* Give the cgrp main thread time to process the unsubscribe op
         * and transition into the intermediate join-state. */
        rd_sleep(1);

        TEST_SAY("Calling destroy mid-rebalance (flags 0x%x)\n", destroy_flags);
        t_start = test_clock();
        destroy_share_consumer(rkshare, destroy_flags);
        t_elapsed_ms = (test_clock() - t_start) / 1000;
        TEST_SAY("Destroy completed in %" PRId64 " ms\n", t_elapsed_ms);

        if (destroy_flags & RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE) {
                /* No-close: destroy must skip the cgrp leave flow and
                 * return promptly, regardless of the heartbeat delay. */
                TEST_ASSERT(t_elapsed_ms < 2000,
                            "Destroy(NO_CONSUMER_CLOSE) took %" PRId64
                            " ms, expected < 2000 ms",
                            t_elapsed_ms);
        } else {
                /* Full close: destroy waits for the cgrp to finish its
                 * leave flow, which is blocked on the heartbeat. It must
                 * still complete in roughly heartbeat_delay_ms (plus
                 * small overhead) and never wait indefinitely. */
                TEST_ASSERT(t_elapsed_ms < heartbeat_delay_ms + 5000,
                            "Destroy took %" PRId64 " ms, expected < %d ms",
                            t_elapsed_ms, heartbeat_delay_ms + 5000);
        }

        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

/**
 * @brief Test destroying a share consumer after a fatal error.
 *
 * Drives the consumer to steady state, acks half the consumed batch,
 * then injects a 30s RTT delay on the next ShareAcknowledge so a real
 * close path would have to wait that long. Trigger a fatal error
 * (which causes destroy_app to promote flags to NO_CONSUMER_CLOSE,
 * then call destroy.
 *
 * Destroy must short-circuit the close path and complete promptly
 * regardless of the supplied destroy_flags — the delayed ShareAck
 * never gets sent, and broker threads must exit without waiting.
 *
 * @param destroy_flags Destroy flags (0 or
 *                      RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE). Both
 *                      must complete fast under fatal-error promotion.
 */
static void test_destroy_with_fatal_error(int destroy_flags) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_t *rk;
        rd_kafka_error_t *error;
        rd_kafka_resp_err_t err;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        const char *topic            = "0179-destroy-fatal-error";
        const char *group            = "0179-destroy-fatal-error";
        const int total_msgs         = 10;
        const int share_ack_delay_ms = 30000;
        size_t rcvd                  = 0;
        int attempts                 = 0;
        size_t i;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST_QUICK("destroy_flags=0x%x", destroy_flags);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        test_produce_msgs_easy_v(topic, 0, 0, 0, total_msgs, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        rkshare = new_share_consumer_for_mock_test(bootstraps, group,
                                                   rd_true /* explicit */, 0);
        rk      = test_share_consumer_get_rk(rkshare);
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Consuming %d msgs\n", total_msgs);
        while (rcvd < (size_t)total_msgs && attempts++ < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
                else
                        rcvd += batch_rcvd;
        }
        TEST_ASSERT(rcvd >= (size_t)total_msgs,
                    "Expected %d msgs, got %d after %d attempts", total_msgs,
                    (int)rcvd, attempts);

        TEST_SAY("Acknowledging %d/%d messages (no commit)\n", (int)(rcvd / 2),
                 (int)rcvd);
        for (i = 0; i < rcvd / 2; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        /* Inject a long delay on the next ShareAcknowledge so a normal
         * close path would block on it. The fatal-error path must NOT
         * wait. */
        TEST_SAY("Injecting %dms RTT delay on ShareAcknowledge\n",
                 share_ack_delay_ms);
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                        share_ack_delay_ms) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to inject ShareAcknowledge delay");

        TEST_SAY("Injecting fatal error\n");
        err = rd_kafka_test_fatal_error(
            rk, RD_KAFKA_RESP_ERR__FATAL,
            "0179: injected fatal error before destroy");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "test_fatal_error returned %s", rd_kafka_err2name(err));

        /* Destroy all consumed messages before share_destroy. */
        for (i = 0; i < rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        TEST_SAY("Calling destroy with flags 0x%x (fatal-error path)\n",
                 destroy_flags);
        t_start = test_clock();
        destroy_share_consumer(rkshare, destroy_flags);
        t_elapsed_ms = (test_clock() - t_start) / 1000;
        TEST_SAY("Destroy completed in %" PRId64 " ms\n", t_elapsed_ms);

        /* Fatal error short-circuits close regardless of supplied flag,
         * so destroy must be prompt and never wait the injected RTT. */
        TEST_ASSERT(t_elapsed_ms < 2000,
                    "Destroy took %" PRId64 " ms, expected < 2000 ms",
                    t_elapsed_ms);

        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}


/**
 * @brief Test destroying share consumer with explicit-ack mode.
 *
 * Consumes a batch of messages, explicitly acknowledges some/all of
 * them (no commit), then destroys the consumer. Verifies that destroy
 * handles pending acks correctly.
 *
 * @param destroy_flags Destroy flags (0 or
 *                      RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE).
 * @param ack_half      If true, ack only the first half of the
 *                      consumed batch (the rest are left unacked). If
 *                      false, ack the full batch.
 */
static void do_test_destroy_with_explicit_ack(int destroy_flags,
                                              rd_bool_t ack_half) {
        const char *topic;
        const char *group = "0179-destroy-explicit-ack";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd = 0;
        int i;
        int attempts = 0;
        int ack_cnt;

        SUB_TEST("destroy_flags=0x%x ack_half=%s", destroy_flags,
                 ack_half ? "true" : "false");

        topic = test_mk_topic_name("0179-destroy-explicit-ack", 1);
        TEST_SAY("Creating topic %s\n", topic);
        test_create_topic_wait_exists(common_admin, topic, 3, -1, 60 * 1000);

        TEST_SAY("Producing %d messages to topic %s\n", TEST_MSGS, topic);
        test_produce_msgs_simple(common_producer, topic, RD_KAFKA_PARTITION_UA,
                                 TEST_MSGS);

        test_share_set_auto_offset_reset(group, "earliest");

        TEST_SAY("Creating share consumer (explicit ack mode)\n");
        rkshare = test_create_share_consumer(group, "explicit");
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Consuming messages (up to %d attempts)\n",
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < 10 && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                }
                attempts++;
        }

        TEST_ASSERT(rcvd >= 10,
                    "Expected at least 10 messages after %d attempts, got %d",
                    MAX_CONSUME_ATTEMPTS, (int)rcvd);
        TEST_SAY("Successfully consumed %d messages\n", (int)rcvd);

        ack_cnt = ack_half ? (int)(rcvd / 2) : (int)rcvd;
        TEST_SAY("Acknowledging %d/%d messages (no commit)\n", ack_cnt,
                 (int)rcvd);
        for (i = 0; i < ack_cnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        /* Destroy all consumed messages (acked and unacked alike). */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        destroy_share_consumer(rkshare, destroy_flags);

        SUB_TEST_PASS();
}


/**
 * @brief Test destroying share consumer with implicit-ack mode.
 *
 * Consumes a batch of messages in implicit-ack mode but never makes a
 * follow-up poll (which is what would trigger the implicit ack), then
 * destroys the consumer directly. Verifies that destroy handles
 * un-acked records gracefully.
 *
 * @param destroy_flags Destroy flags (0 or
 *                      RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE).
 */
static void do_test_destroy_with_implicit_ack(int destroy_flags) {
        const char *topic;
        const char *group = "0179-destroy-implicit-ack";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd = 0;
        int i;
        int attempts = 0;

        SUB_TEST("destroy_flags=0x%x", destroy_flags);

        topic = test_mk_topic_name("0179-destroy-implicit-ack", 1);
        TEST_SAY("Creating topic %s\n", topic);
        test_create_topic_wait_exists(common_admin, topic, 3, -1, 60 * 1000);

        TEST_SAY("Producing %d messages to topic %s\n", TEST_MSGS, topic);
        test_produce_msgs_simple(common_producer, topic, RD_KAFKA_PARTITION_UA,
                                 TEST_MSGS);

        test_share_set_auto_offset_reset(group, "earliest");

        TEST_SAY("Creating share consumer (implicit ack mode)\n");
        rkshare = test_create_share_consumer(group, "implicit");
        subscribe_topics(rkshare, &topic, 1);

        TEST_SAY("Consuming messages (up to %d attempts)\n",
                 MAX_CONSUME_ATTEMPTS);
        while (rcvd < 10 && attempts < MAX_CONSUME_ATTEMPTS) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                }
                attempts++;
        }

        TEST_ASSERT(rcvd >= 10,
                    "Expected at least 10 messages after %d attempts, got %d",
                    MAX_CONSUME_ATTEMPTS, (int)rcvd);
        TEST_SAY(
            "Successfully consumed %d messages (no follow-up poll, "
            "so implicit ack never fires)\n",
            (int)rcvd);

        /* Destroy all consumed messages before share_destroy. */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        /* Destroy consumer directly with un-acked records still held by
         * the broker session. */
        destroy_share_consumer(rkshare, destroy_flags);

        SUB_TEST_PASS();
}


/**
 * @brief Test destroying share consumer after subscribe/unsubscribe.
 *
 * This test creates a share consumer, optionally subscribes to topics,
 * optionally unsubscribes, then destroys it without consuming any messages.
 * Tests various combinations similar to 0116-kafkaconsumer_close.
 *
 * @param do_subscribe Whether to subscribe to topics
 * @param do_unsubscribe Whether to unsubscribe before destroy
 * @param destroy_flags Destroy flags (0 or
 * RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE)
 */
static void do_test_destroy_with_subscribe_unsubscribe(int do_subscribe,
                                                       int do_unsubscribe,
                                                       int destroy_flags) {
        rd_kafka_share_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic = "0179-test-destroy-sub-unsub";
        rd_kafka_resp_err_t err;

        SUB_TEST("subscribe=%d, unsubscribe=%d, destroy_flags=0x%x",
                 do_subscribe, do_unsubscribe, destroy_flags);

        TEST_SAY("Creating share consumer\n");
        consumer = test_create_share_consumer("0179-sub-unsub-destroy-test",
                                              "explicit");

        if (do_subscribe) {
                TEST_SAY("Subscribing to topic: %s\n", topic);
                topics = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(topics, topic,
                                                  RD_KAFKA_PARTITION_UA);
                err = rd_kafka_share_subscribe(consumer, topics);
                TEST_ASSERT(!err, "Subscribe failed: %s",
                            rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(topics);
        }

        if (do_unsubscribe) {
                TEST_SAY("Unsubscribing from all topics\n");
                err = rd_kafka_share_unsubscribe(consumer);
                TEST_ASSERT(!err, "Unsubscribe failed: %s",
                            rd_kafka_err2str(err));
        }

        destroy_share_consumer(consumer, destroy_flags);
        SUB_TEST_PASS();
}


int main_0179_share_consumer_destroy(int argc, char **argv) {
        /* Real broker tests */
        test_timeout_set(120);

        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        do_test_destroy_with_subscribe_unsubscribe(0, 0, 0);
        do_test_destroy_with_subscribe_unsubscribe(
            1, 0, 0); /* subscribe, no unsubscribe */
        do_test_destroy_with_subscribe_unsubscribe(
            1, 1, 0); /* subscribe then unsubscribe */
        do_test_destroy_with_explicit_ack(0, rd_false /* ack all */);
        do_test_destroy_with_explicit_ack(0, rd_true /* ack half */);
        do_test_destroy_with_implicit_ack(0);

        do_test_destroy_with_subscribe_unsubscribe(
            1, 0, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        do_test_destroy_with_subscribe_unsubscribe(
            1, 1, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        do_test_destroy_with_subscribe_unsubscribe(
            0, 0, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        do_test_destroy_with_explicit_ack(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE,
                                          rd_false);
        do_test_destroy_with_explicit_ack(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE,
                                          rd_true);
        do_test_destroy_with_implicit_ack(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}

int main_0179_share_consumer_destroy_local(int argc, char **argv) {
        /* Mock broker tests only (no real broker needed) */
        TEST_SKIP_MOCK_CLUSTER(0);
        test_timeout_set(120);

        test_destroy_with_fatal_error(0);
        test_destroy_with_cached_acks_and_delayed_broker(0);
        test_broker_decommission_with_commit_sync(0, rd_false);
        test_broker_decommission_with_commit_sync(0, rd_true);
        /* TODO KIP-932 destroy: The below test case need to be debugged
         * Broker thread gets stuck until destroy is called */
        test_broker_decommission_with_consume_batch(0);
        test_broker_decommission_during_close(0, rd_false);
        test_broker_decommission_during_close(0, rd_true);
        test_broker_decommission_with_commit_async(0, rd_false);
        test_broker_decommission_with_commit_async(0, rd_true);
        test_destroy_during_rebalance(0);

        test_destroy_with_fatal_error(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        test_destroy_with_cached_acks_and_delayed_broker(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        test_broker_decommission_with_commit_sync(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_false);
        test_broker_decommission_with_commit_sync(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_true);
        /* TODO KIP-932 destroy: The below test case need to be debugged
         * Broker thread gets stuck until destroy is called */
        test_broker_decommission_with_consume_batch(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        test_broker_decommission_during_close(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_false);
        test_broker_decommission_during_close(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_true);
        test_broker_decommission_with_commit_async(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_false);
        test_broker_decommission_with_commit_async(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE, rd_true);
        test_destroy_during_rebalance(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        /* TODO KIP-932: The below test cases fail half the times
         * due to a negative refcount issue in destroy_final
         * Revisit them when working on leader migration */
        // test_leader_migration_mid_session_destroy(0);
        // test_leader_migration_mid_session_destroy(
        //     RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        return 0;
}