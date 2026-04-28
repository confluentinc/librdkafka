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
#include "rdkafka.h"
#include "rdkafka_protocol.h"

#define MAX_TOPICS           16
#define MAX_PARTITIONS       32
#define BATCH_SIZE           10000
#define MAX_CONSUME_ATTEMPTS 30

/** Common producer reused across all real-broker tests. */
static rd_kafka_t *common_producer;

/** Common admin client reused across all real-broker tests. */
static rd_kafka_t *common_admin;

/**
 * @brief Commit mode for test scenarios
 */
typedef enum {
        COMMIT_MODE_NONE,  /**< No commit before close */
        COMMIT_MODE_ASYNC, /**< Call commitAsync before close */
        COMMIT_MODE_SYNC   /**< Call commitSync before close */
} commit_mode_t;

/**
 * @brief Generic test context for topic and message tracking
 */
typedef struct {
        char *topic_names[MAX_TOPICS];
        int topic_cnt;
        char *group_id;
        int total_msgs_produced;
} test_context_t;

/**
 * @brief Tracked message info for verification
 */
typedef struct {
        char *topic;
        int32_t partition;
        int64_t offset;
} tracked_msg_t;

/**
 * @brief Get string representation of commit mode
 */
static const char *commit_mode_str(commit_mode_t mode) {
        switch (mode) {
        case COMMIT_MODE_NONE:
                return "no-commit";
        case COMMIT_MODE_ASYNC:
                return "commit-async";
        case COMMIT_MODE_SYNC:
                return "commit-sync";
        default:
                return "unknown";
        }
}

/**
 * @brief Setup topics and produce messages (modular helper)
 *
 * @param ctx Test context to populate
 * @param topic_cnt Number of topics to create
 * @param partitions Array of partition counts per topic
 * @param msgs_per_partition Messages to produce per partition
 *
 * @returns Total number of messages produced
 */
static int setup_topics_and_produce(test_context_t *ctx,
                                    int topic_cnt,
                                    const int *partitions,
                                    int msgs_per_partition) {
        int t, p;
        int total_msgs = 0;

        ctx->topic_cnt = topic_cnt;

        for (t = 0; t < topic_cnt; t++) {
                ctx->topic_names[t] =
                    rd_strdup(test_mk_topic_name("0178-close-test", 1));

                TEST_SAY("Creating topic %s with %d partition(s)\n",
                         ctx->topic_names[t], partitions[t]);

                test_create_topic_wait_exists(common_admin, ctx->topic_names[t],
                                              partitions[t], -1, 60 * 1000);

                for (p = 0; p < partitions[t]; p++) {
                        test_produce_msgs_simple(common_producer,
                                                 ctx->topic_names[t], p,
                                                 msgs_per_partition);
                        total_msgs += msgs_per_partition;
                }

                TEST_SAY(
                    "Topic %s: produced %d messages (%d partitions * %d"
                    "msgs)\n",
                    ctx->topic_names[t], partitions[t] * msgs_per_partition,
                    partitions[t], msgs_per_partition);
        }

        ctx->total_msgs_produced = total_msgs;
        TEST_SAY("Total messages produced: %d\n", total_msgs);
        return total_msgs;
}

/**
 * @brief Subscribe consumer to topics
 *
 * @param rkshare Consumer handle
 * @param topic_names Array of topic names to subscribe to
 * @param topic_cnt Number of topics
 */
static void subscribe_consumer(rd_kafka_share_t *rkshare,
                               char **topic_names,
                               int topic_cnt) {
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;
        int t;

        /* Build subscription list */
        subs = rd_kafka_topic_partition_list_new(topic_cnt);
        for (t = 0; t < topic_cnt; t++) {
                rd_kafka_topic_partition_list_add(subs, topic_names[t],
                                                  RD_KAFKA_PARTITION_UA);
        }

        /* Subscribe */
        err = rd_kafka_share_subscribe(rkshare, subs);
        if (err) {
                TEST_FAIL("Subscribe failed: %s", rd_kafka_err2str(err));
        }

        rd_kafka_topic_partition_list_destroy(subs);
}

/**
 * @brief Consume messages, track them, and optionally ACCEPT-acknowledge.
 *
 * In ack mode (do_acknowledge=true), messages are acknowledged with ACCEPT;
 * any remaining messages in a batch beyond target_count are RELEASE'd so they
 * can be redelivered. In no-ack mode (do_acknowledge=false), messages are
 * tracked and destroyed without acknowledging — close() should release them.
 *
 * @param rkshare Consumer handle
 * @param consumer_name Name for logging (e.g., "C1", "C2")
 * @param target_count Number of messages to track (and ACCEPT in ack mode)
 * @param do_acknowledge If true, ACCEPT each tracked message and RELEASE the
 *                      rest of the batch; if false, track-and-destroy only.
 * @param tracked_msgs If non-NULL, track messages here
 * @param tracked_cnt If non-NULL, store count of tracked messages here
 *
 * @returns Number of messages tracked
 */
static int consume_and_track(rd_kafka_share_t *rkshare,
                             const char *consumer_name,
                             int target_count,
                             rd_bool_t do_acknowledge,
                             tracked_msg_t *tracked_msgs,
                             int *tracked_cnt) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int attempt;
        rd_kafka_error_t *error;
        int tracked = 0;

        TEST_SAY("%s: Consuming and %s %d messages (max %d attempts)...\n",
                 consumer_name,
                 do_acknowledge ? "acknowledging" : "tracking (no ack)",
                 target_count, MAX_CONSUME_ATTEMPTS);

        for (attempt = 0;
             attempt < MAX_CONSUME_ATTEMPTS && tracked < target_count;
             attempt++) {
                size_t rcvd_msgs = 0;
                int i;

                error = rd_kafka_share_consume_batch(rkshare, 3000, batch,
                                                     &rcvd_msgs);

                if (error) {
                        TEST_SAY("%s: Attempt %d/%d: error: %s\n",
                                 consumer_name, attempt + 1,
                                 MAX_CONSUME_ATTEMPTS,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd_msgs == 0)
                        continue;

                TEST_SAY("%s: Attempt %d/%d: Received %d messages\n",
                         consumer_name, attempt + 1, MAX_CONSUME_ATTEMPTS,
                         (int)rcvd_msgs);

                for (i = 0; i < (int)rcvd_msgs && tracked < target_count; i++) {
                        rd_kafka_message_t *rkm = batch[i];

                        if (rkm->err) {
                                TEST_SAY(
                                    "%s: Skipping message %d with error: %s\n",
                                    consumer_name, i,
                                    rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        if (tracked_msgs && tracked < BATCH_SIZE) {
                                tracked_msgs[tracked].topic =
                                    rd_strdup(rd_kafka_topic_name(rkm->rkt));
                                tracked_msgs[tracked].partition =
                                    rkm->partition;
                                tracked_msgs[tracked].offset = rkm->offset;
                        }

                        TEST_SAY("%s: %s message %d: %s [%d] @ offset %" PRId64
                                 "\n",
                                 consumer_name,
                                 do_acknowledge ? "Acking" : "Tracking",
                                 tracked, rd_kafka_topic_name(rkm->rkt),
                                 rkm->partition, rkm->offset);

                        if (do_acknowledge)
                                rd_kafka_share_acknowledge(rkshare, rkm);
                        rd_kafka_message_destroy(rkm);
                        tracked++;
                }

                for (; i < (int)rcvd_msgs; i++) {
                        rd_kafka_message_destroy(batch[i]);
                }

                if (tracked >= target_count) {
                        TEST_SAY("%s: Reached target of %d tracked messages\n",
                                 consumer_name, target_count);
                        break;
                }
        }

        TEST_SAY("%s: Finished - tracked %d messages\n", consumer_name,
                 tracked);

        if (tracked < target_count) {
                TEST_FAIL("%s: Expected to track %d messages, got %d",
                          consumer_name, target_count, tracked);
        }

        if (tracked_cnt)
                *tracked_cnt = tracked;

        return tracked;
}

/**
 * @brief Execute commit based on mode (modular helper)
 *
 * @param rkshare Consumer handle
 * @param consumer_name Name for logging
 * @param mode Commit mode to execute
 */
static void perform_commit(rd_kafka_share_t *rkshare,
                           const char *consumer_name,
                           commit_mode_t mode) {
        rd_kafka_error_t *error;

        switch (mode) {
        case COMMIT_MODE_NONE:
                TEST_SAY("%s: Skipping commit (mode: %s)\n", consumer_name,
                         commit_mode_str(mode));
                break;

        case COMMIT_MODE_ASYNC:
                TEST_SAY("%s: Calling commitAsync\n", consumer_name);
                error = rd_kafka_share_commit_async(rkshare);
                if (error) {
                        TEST_FAIL("%s: commitAsync failed: %s", consumer_name,
                                  rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                }
                /* Give async commit some time to process */
                rd_sleep(1);
                break;

        case COMMIT_MODE_SYNC: {
                rd_kafka_topic_partition_list_t *partitions = NULL;
                TEST_SAY("%s: Calling commitSync\n", consumer_name);
                error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
                if (error) {
                        TEST_FAIL("%s: commitSync failed: %s", consumer_name,
                                  rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                }
                /* Free the partition list returned by commit_sync */
                if (partitions)
                        rd_kafka_topic_partition_list_destroy(partitions);
                TEST_SAY("%s: commitSync completed successfully\n",
                         consumer_name);
                break;
        }

        default:
                TEST_FAIL("Unknown commit mode: %d", mode);
        }
}

/**
 * @brief Verify whether tracked messages are (or are not) redelivered.
 *
 * Consumes up to @p max_total_msgs messages and compares each against the
 * @p tracked_msgs list. Behavior depends on @p expect_redelivery:
 *  - false: fails if any tracked message is received (post-ack scenario);
 *           also asserts the total received count equals @p max_total_msgs.
 *  - true:  fails unless every tracked message is received (post-release
 *           scenario); stops early once all tracked messages have been
 *           matched.
 *
 * @param rkshare Consumer handle to check
 * @param consumer_name Name for logging
 * @param max_total_msgs Number of messages to consume (also the exact-count
 *                      assertion in the not-redelivered case)
 * @param tracked_msgs Array of tracked messages to check against
 * @param tracked_cnt Number of tracked messages
 * @param expect_redelivery true = expect all tracked redelivered; false =
 *                          expect none redelivered
 * @param context_label Free-form label included in failure messages
 *                     (e.g., commit mode name)
 */
static void verify_tracked_messages(rd_kafka_share_t *rkshare,
                                    const char *consumer_name,
                                    int max_total_msgs,
                                    const tracked_msg_t *tracked_msgs,
                                    int tracked_cnt,
                                    rd_bool_t expect_redelivery,
                                    const char *context_label) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        size_t total_rcvd = 0;
        int attempt;
        rd_kafka_error_t *error;
        int matched_count = 0;
        rd_bool_t *tracked_seen =
            tracked_cnt > 0 ? rd_calloc(tracked_cnt, sizeof(*tracked_seen))
                            : NULL;

        TEST_SAY(
            "%s: Consuming for verification (up to %d messages, max %d "
            "attempts, expect_redelivery=%s)...\n",
            consumer_name, max_total_msgs, MAX_CONSUME_ATTEMPTS,
            expect_redelivery ? "true" : "false");

        for (attempt = 0; attempt < MAX_CONSUME_ATTEMPTS &&
                          (int)total_rcvd < max_total_msgs &&
                          (!expect_redelivery || matched_count < tracked_cnt);
             attempt++) {
                size_t rcvd_msgs = 0;
                size_t i;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, batch + total_rcvd, &rcvd_msgs);

                if (error) {
                        TEST_SAY("%s: Attempt %d/%d: error: %s\n",
                                 consumer_name, attempt + 1,
                                 MAX_CONSUME_ATTEMPTS,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd_msgs == 0)
                        continue;

                TEST_SAY(
                    "%s: Attempt %d/%d: Received %d messages (total: %d)\n",
                    consumer_name, attempt + 1, MAX_CONSUME_ATTEMPTS,
                    (int)rcvd_msgs, (int)(total_rcvd + rcvd_msgs));

                /* Match each new message against the tracked list */
                for (i = total_rcvd; i < total_rcvd + rcvd_msgs; i++) {
                        rd_kafka_message_t *rkm = batch[i];
                        const char *topic;
                        int32_t partition;
                        int64_t offset;
                        int j;

                        if (rkm->err)
                                continue;

                        topic     = rd_kafka_topic_name(rkm->rkt);
                        partition = rkm->partition;
                        offset    = rkm->offset;

                        for (j = 0; j < tracked_cnt; j++) {
                                if (tracked_seen && tracked_seen[j])
                                        continue;
                                if (strcmp(topic, tracked_msgs[j].topic) == 0 &&
                                    partition == tracked_msgs[j].partition &&
                                    offset == tracked_msgs[j].offset) {
                                        if (tracked_seen)
                                                tracked_seen[j] = rd_true;
                                        matched_count++;
                                        if (expect_redelivery) {
                                                TEST_SAY(
                                                    "%s: Matched tracked "
                                                    "message %d/%d: %s [%d] "
                                                    "@ offset %" PRId64 "\n",
                                                    consumer_name,
                                                    matched_count, tracked_cnt,
                                                    topic, partition, offset);
                                        } else {
                                                TEST_WARN(
                                                    "%s: Received previously "
                                                    "acknowledged message: "
                                                    "%s [%d] @ offset %" PRId64
                                                    "\n",
                                                    consumer_name, topic,
                                                    partition, offset);
                                        }
                                        break;
                                }
                        }
                }

                total_rcvd += rcvd_msgs;
        }

        TEST_SAY("%s: Received %d messages, matched %d/%d tracked\n",
                 consumer_name, (int)total_rcvd, matched_count, tracked_cnt);

        if (expect_redelivery) {
                int j;
                if (matched_count != tracked_cnt) {
                        for (j = 0; j < tracked_cnt; j++) {
                                if (!tracked_seen[j]) {
                                        TEST_FAIL(
                                            "%s: Did not receive tracked "
                                            "message: %s [%d] @ offset "
                                            "%" PRId64 " (context: %s)",
                                            consumer_name,
                                            tracked_msgs[j].topic,
                                            tracked_msgs[j].partition,
                                            tracked_msgs[j].offset,
                                            context_label);
                                }
                        }
                }
                TEST_SAY(
                    "%s: Verification passed - all %d tracked messages "
                    "redelivered\n",
                    consumer_name, matched_count);
        } else {
                if ((int)total_rcvd != max_total_msgs) {
                        TEST_FAIL("%s: Expected to receive %d messages, got %d",
                                  consumer_name, max_total_msgs,
                                  (int)total_rcvd);
                }
                if (matched_count > 0) {
                        TEST_FAIL(
                            "%s received %d messages that were acknowledged "
                            "(context: %s)",
                            consumer_name, matched_count, context_label);
                }
                TEST_SAY(
                    "%s: Verification passed - no acknowledged messages "
                    "were redelivered\n",
                    consumer_name);
        }

        /* Destroy all consumed messages */
        for (size_t i = 0; i < total_rcvd; i++)
                rd_kafka_message_destroy(batch[i]);

        if (tracked_seen)
                rd_free(tracked_seen);
}

/**
 * @brief Free tracked messages (modular helper)
 *
 * @param tracked_msgs Array of tracked messages
 * @param tracked_cnt Number of tracked messages
 */
static void free_tracked_messages(tracked_msg_t *tracked_msgs,
                                  int tracked_cnt) {
        for (int i = 0; i < tracked_cnt; i++) {
                if (tracked_msgs[i].topic)
                        rd_free(tracked_msgs[i].topic);
        }
}

/**
 * @brief Helper: assert the given rd_kafka_error_t* signals closed/closing.
 */
static void assert_state_error(rd_kafka_error_t *error, const char *api_name) {
        TEST_ASSERT(error != NULL, "%s: expected error, got NULL", api_name);
        TEST_ASSERT(rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__STATE,
                    "%s: expected __STATE, got %s", api_name,
                    rd_kafka_err2name(rd_kafka_error_code(error)));
        TEST_ASSERT(strstr(rd_kafka_error_string(error), "closed") != NULL,
                    "%s: expected error string to contain 'closed', got '%s'",
                    api_name, rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);
}

/**
 * @brief Invoke every share-consumer API and assert each returns a
 *        closed state error.
 *
 * @param consumer    Share consumer (already closed or closing).
 * @param topic       Topic name (for subscribe/acknowledge_offset).
 */
static void verify_all_apis_return_error(rd_kafka_share_t *consumer,
                                         const char *topic) {
        rd_kafka_error_t *error;
        rd_kafka_resp_err_t err;
        rd_kafka_message_t *batch[4];
        size_t rcvd = 0;
        rd_kafka_topic_partition_list_t *subs, *sub_result = NULL;
        rd_kafka_topic_partition_list_t *commit_results = NULL;
        rd_kafka_queue_t *queue                         = NULL;

        /* 1. consume_batch */
        error = rd_kafka_share_consume_batch(consumer, 100, batch, &rcvd);
        assert_state_error(error, "consume_batch");

        /* 2. commit_async */
        error = rd_kafka_share_commit_async(consumer);
        assert_state_error(error, "commit_async");

        /* 3. commit_sync */
        error = rd_kafka_share_commit_sync(consumer, 1000, &commit_results);
        assert_state_error(error, "commit_sync");
        TEST_ASSERT(commit_results == NULL,
                    "commit_sync: expected no partitions on error");

        /* 4. acknowledge_offset */
        err = rd_kafka_share_acknowledge_offset(
            consumer, topic, 0, 0, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                    "acknowledge_offset: expected __STATE, got %s",
                    rd_kafka_err2name(err));

        /* 5. subscribe */
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        err = rd_kafka_share_subscribe(consumer, subs);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                    "subscribe: expected __STATE, got %s",
                    rd_kafka_err2name(err));
        rd_kafka_topic_partition_list_destroy(subs);

        /* 6. unsubscribe */
        err = rd_kafka_share_unsubscribe(consumer);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                    "unsubscribe: expected __STATE, got %s",
                    rd_kafka_err2name(err));

        /* 7. subscription */
        err = rd_kafka_share_subscription(consumer, &sub_result);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                    "subscription: expected __STATE, got %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(sub_result == NULL, "subscription: expected NULL result");

        /* 8. close */
        error = rd_kafka_share_consumer_close(consumer);
        assert_state_error(error, "close");

        /* 9. async close */
        queue = rd_kafka_queue_new(rd_kafka_share_consumer_get_rk(consumer));
        error = rd_kafka_share_consumer_close_queue(consumer, queue);
        rd_kafka_queue_destroy(queue);
        assert_state_error(error, "close_queue");
}

/**
 * @brief Enable the three Share APIs (Heartbeat, Fetch, Acknowledge) on the
 *        given mock cluster. Every share-consumer mock test in this file
 *        needs all three.
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
            "Failed to enable ShareAck");
}

/**
 * @brief Common setup for 3-broker mock close-tests.
 *
 * Creates a 3-broker mock cluster, a topic with one partition per broker,
 * produces 10 messages to each partition, then creates a share consumer
 * (with optional socket.timeout.ms), subscribes, and consumes all
 * @p partition_cnt * @p msgs_per_partition messages so the share session
 * is established with every partition leader.
 *
 * @param test_name Used to derive unique topic and group names.
 * @param socket_timeout_ms Pass 0 to leave socket.timeout.ms at default,
 *                          or a positive value to override it.
 * @param out_mcluster Output: created cluster (caller destroys).
 * @param out_consumer Output: subscribed share consumer (caller destroys).
 */
static void setup_3broker_share_consumer(const char *test_name,
                                         int socket_timeout_ms,
                                         rd_kafka_mock_cluster_t **out_mcluster,
                                         rd_kafka_share_t **out_consumer) {
        const int partition_cnt      = 3;
        const int msgs_per_partition = 10;
        const int total_msgs         = partition_cnt * msgs_per_partition;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *error;
        char topic[64], group[64], errstr[512];
        int p, consumed = 0, attempts = 0;
        size_t i, rcvd;

        rd_snprintf(topic, sizeof(topic), "mock-%s", test_name);
        rd_snprintf(group, sizeof(group), "sg-%s", test_name);

        mcluster = test_mock_cluster_new(3, &bootstraps);
        enable_share_apis(mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce msgs_per_partition msgs to each partition via test helper. */
        for (p = 0; p < partition_cnt; p++) {
                test_produce_msgs_easy_v(topic, 0, p, 0, msgs_per_partition, 16,
                                         "bootstrap.servers", bootstraps, NULL);
        }

        /* Consumer */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        if (socket_timeout_ms > 0) {
                rd_snprintf(errstr, sizeof(errstr), "%d", socket_timeout_ms);
                test_conf_set(conf, "socket.timeout.ms", errstr);
        }

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume all messages so share session is established with every
         * partition leader (i.e. all 3 brokers). */
        TEST_SAY("Consuming %d messages across %d partitions...\n", total_msgs,
                 partition_cnt);
        while (consumed < total_msgs && attempts++ < 30) {
                rcvd = 0;
                error =
                    rd_kafka_share_consume_batch(consumer, 3000, batch, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        if (!batch[i]->err)
                                consumed++;
                        rd_kafka_message_destroy(batch[i]);
                }
        }

        TEST_ASSERT(consumed == total_msgs, "Expected %d messages, got %d",
                    total_msgs, consumed);
        TEST_SAY("Consumed all %d messages\n", consumed);

        *out_mcluster = mcluster;
        *out_consumer = consumer;
}

/** is_fatal_cb hook scoped to test_close_with_broker_down: ignores
 *  the transport error and cascading ALL_BROKERS_DOWN that result from
 *  taking the mock broker down mid-close. */
static int test_close_with_broker_down_is_fatal_cb(rd_kafka_t *rk,
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

/*#################################################################################*/

/**
 * @brief Close with acknowledge test scenarios
 *
 * Tests consumer close behavior with different commit modes and topologies.
 * Verifies that acknowledged messages are not redelivered to a second
 consumer.
 */
static void test_close_with_acknowledge(void) {
        /**
         * @brief Test configuration for close with acknowledge scenarios
         */
        typedef struct {
                const char *test_name;
                int topic_cnt;
                int partitions[MAX_TOPICS];
                int msgs_per_partition;
                commit_mode_t commit_mode;
                int ack_count;
        } close_ack_test_config_t;

        /* Test matrix: 3 commit modes × 4 topologies = 12 tests */
        close_ack_test_config_t tests[] = {
            /* 1 topic, 1 partition */
            {"close-1t1p-no-commit", 1, {1}, 20, COMMIT_MODE_NONE, 10},
            {"close-1t1p-commit-async", 1, {1}, 20, COMMIT_MODE_ASYNC, 10},
            {"close-1t1p-commit-sync", 1, {1}, 20, COMMIT_MODE_SYNC, 10},

            /* 1 topic, multiple partitions */
            {"close-1t3p-no-commit", 1, {3}, 10, COMMIT_MODE_NONE, 20},
            {"close-1t3p-commit-async", 1, {3}, 10, COMMIT_MODE_ASYNC, 20},
            {"close-1t3p-commit-sync", 1, {3}, 10, COMMIT_MODE_SYNC, 15},

            /* Multiple topics, 1 partition each */
            {"close-3t1p-no-commit", 3, {1, 1, 1}, 10, COMMIT_MODE_NONE, 20},
            {"close-3t1p-commit-async",
             3,
             {1, 1, 1},
             10,
             COMMIT_MODE_ASYNC,
             20},
            {"close-3t1p-commit-sync", 3, {1, 1, 1}, 10, COMMIT_MODE_SYNC, 20},

            /* Multiple topics, multiple partitions */
            {"close-2t2p-no-commit", 2, {2, 2}, 10, COMMIT_MODE_NONE, 20},
            {"close-2t2p-commit-async", 2, {2, 2}, 10, COMMIT_MODE_ASYNC, 20},
            {"close-2t2p-commit-sync", 2, {2, 2}, 10, COMMIT_MODE_SYNC, 20},
        };

        for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
                close_ack_test_config_t *config = &tests[i];
                test_context_t ctx              = {0};
                rd_kafka_share_t *c1, *c2;
                tracked_msg_t tracked_msgs[BATCH_SIZE];
                int tracked_cnt = 0;

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY("Topology: %d topic(s), partitions: [",
                         config->topic_cnt);
                for (int j = 0; j < config->topic_cnt; j++) {
                        TEST_SAY("%d%s", config->partitions[j],
                                 j < config->topic_cnt - 1 ? ", " : "");
                }
                TEST_SAY("]\n");
                TEST_SAY("Commit mode: %s\n",
                         commit_mode_str(config->commit_mode));
                TEST_SAY("========================================\n\n");

                ctx.group_id = "0178-group";
                setup_topics_and_produce(&ctx, config->topic_cnt,
                                         config->partitions,
                                         config->msgs_per_partition);

                /* Set group offset to earliest */
                test_share_set_auto_offset_reset(ctx.group_id, "earliest");

                /* Create C1 with explicit ack mode (will call acknowledge()
                 * explicitly). C2 will be created after C1 closes. */
                c1 = test_create_share_consumer(ctx.group_id, "explicit");

                /* Subscribe C1 only - C2 will subscribe after C1 closes */
                subscribe_consumer(c1, ctx.topic_names, ctx.topic_cnt);

                /* C1: Consume and acknowledge */
                consume_and_track(c1, "C1", config->ack_count, rd_true,
                                  tracked_msgs, &tracked_cnt);

                /* Execute commit */
                perform_commit(c1, "C1", config->commit_mode);

                /* Close C1 */
                TEST_SAY("C1: Closing consumer\n");
                rd_kafka_error_t *c1_close_err =
                    rd_kafka_share_consumer_close(c1);
                TEST_ASSERT(c1_close_err == NULL,
                            "C1: close returned error: %s",
                            rd_kafka_error_string(c1_close_err));
                TEST_SAY("C1: Closed successfully\n");

                /* Create C2 with implicit ack mode after C1 is fully destroyed
                 */
                TEST_SAY("C2: Creating and subscribing after C1 close\n");
                c2 = test_create_share_consumer(ctx.group_id, "implicit");
                subscribe_consumer(c2, ctx.topic_names, ctx.topic_cnt);

                /* C2: Consume and verify no redelivery */
                verify_tracked_messages(c2, "C2",
                                        ctx.total_msgs_produced - tracked_cnt,
                                        tracked_msgs, tracked_cnt, rd_false,
                                        commit_mode_str(config->commit_mode));

                /* Close C2 */
                TEST_SAY("C2: Closing consumer\n");
                rd_kafka_share_consumer_close(c2);
                TEST_SAY("C2: Closed successfully\n");
                rd_kafka_share_destroy(c2);


                /* Cleanup */
                free_tracked_messages(tracked_msgs, tracked_cnt);

                for (int t = 0; t < ctx.topic_cnt; t++) {
                        rd_free(ctx.topic_names[t]);
                }

                TEST_SAY("Test %s completed successfully\n\n",
                         config->test_name);
                rd_kafka_share_destroy(c1);
        }
}

/**
 * @brief Test close without acknowledge
 *
 * Verifies that when a consumer closes without acknowledging consumed records,
 * those records are released back to the share group and can be consumed by
 * another consumer.
 *
 * Test flow:
 * 1. C1 consumes records but does NOT acknowledge them
 * 2. C1 closes (should release the unacknowledged records)
 * 3. C2 should be able to consume those same records
 *
 * Tests multiple topologies: 1t1p, 1t3p, 3t1p, 2t2p
 */
static void test_close_without_acknowledge() {
        /**
         * @brief Test configuration for close without acknowledge scenarios
         */
        typedef struct {
                const char *test_name;
                int topic_cnt;
                int partitions[MAX_TOPICS];
                int msgs_per_partition;
                int track_count;
        } close_no_ack_test_config_t;

        /* Test matrix: various topologies */
        close_no_ack_test_config_t tests[] = {
            /* 1 topic, 1 partition */
            {"close-no-ack-1t1p", 1, {1}, 20, 5},

            /* 1 topic, multiple partitions */
            {"close-no-ack-1t3p", 1, {3}, 10, 5},

            /* Multiple topics, 1 partition each */
            {"close-no-ack-3t1p", 3, {1, 1, 1}, 10, 5},

            /* Multiple topics, multiple partitions */
            {"close-no-ack-2t2p", 2, {2, 2}, 10, 5},
        };

        for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
                close_no_ack_test_config_t *config = &tests[i];
                test_context_t ctx                 = {0};
                rd_kafka_share_t *c1, *c2;
                tracked_msg_t tracked_msgs[BATCH_SIZE];
                int tracked_cnt = 0;
                char group_id[64];

                TEST_SAY("\n========================================\n");
                TEST_SAY("Topology: %d topic(s), partitions: [",
                         config->topic_cnt);
                for (int j = 0; j < config->topic_cnt; j++) {
                        TEST_SAY("%d%s", config->partitions[j],
                                 j < config->topic_cnt - 1 ? ", " : "");
                }
                TEST_SAY("]\n");
                TEST_SAY("========================================\n\n");

                rd_snprintf(group_id, sizeof(group_id),
                            "0178-group-no-ack-%" PRIu64, test_id_generate());
                ctx.group_id = group_id;
                setup_topics_and_produce(&ctx, config->topic_cnt,
                                         config->partitions,
                                         config->msgs_per_partition);

                test_share_set_auto_offset_reset(ctx.group_id, "earliest");

                c1 = test_create_share_consumer(ctx.group_id, "implicit");
                subscribe_consumer(c1, ctx.topic_names, ctx.topic_cnt);

                /* C1: Consume and track without acknowledging. */
                consume_and_track(c1, "C1", config->track_count, rd_false,
                                  tracked_msgs, &tracked_cnt);

                /* Close C1 - unacked records must be released back to the
                 * share group. */
                TEST_SAY("C1: Closing without acknowledging\n");
                rd_kafka_error_t *c1_close_err =
                    rd_kafka_share_consumer_close(c1);
                TEST_ASSERT(c1_close_err == NULL,
                            "C1: close returned error: %s",
                            rd_kafka_error_string(c1_close_err));

                c2 = test_create_share_consumer(ctx.group_id, "implicit");
                subscribe_consumer(c2, ctx.topic_names, ctx.topic_cnt);

                /* C2: Verify every tracked message is redelivered. Cap the
                 * scan at total produced so we don't loop forever if redelivery
                 * is broken. */
                verify_tracked_messages(c2, "C2", ctx.total_msgs_produced,
                                        tracked_msgs, tracked_cnt, rd_true,
                                        "no-ack-close");

                TEST_SAY("C2: Closing consumer\n");
                rd_kafka_share_consumer_close(c2);
                rd_kafka_share_destroy(c2);

                free_tracked_messages(tracked_msgs, tracked_cnt);

                for (int t = 0; t < ctx.topic_cnt; t++) {
                        rd_free(ctx.topic_names[t]);
                }

                TEST_SAY("Test %s completed successfully\n\n",
                         config->test_name);
                rd_kafka_share_destroy(c1);
        }
}

/**
 * @brief Test close with slow broker response
 *
 * Verifies that close() waits for the broker to respond to the
 * session close request (sent to partition leader), even when
 * the broker is slow to respond.
 *
 * Tests 3 scenarios with a 3-broker setup:
 * 1. Only broker 1 has delayed response (10s)
 * 2. Brokers 1 and 2 have delayed responses (10s each)
 * 3. All 3 brokers have delayed responses (10s each)
 *
 * In each case, close() should wait for the response and complete
 * successfully after the delay.
 */
static void test_close_with_slow_broker_response(void) {
        typedef struct {
                const char *test_name;
                int delayed_broker_cnt;
                int32_t delayed_brokers[3];
        } delay_test_config_t;

        delay_test_config_t tests[] = {
            {"1-broker-delayed", 1, {1, 0, 0}},
            {"2-brokers-delayed", 2, {1, 2, 0}},
            {"3-brokers-delayed", 3, {1, 2, 3}},
        };

        for (size_t test_idx = 0; test_idx < sizeof(tests) / sizeof(tests[0]);
             test_idx++) {
                delay_test_config_t *config = &tests[test_idx];
                rd_kafka_mock_cluster_t *mcluster;
                rd_kafka_share_t *consumer;
                const int rtt_delay_ms = 10000; /* 10 seconds */
                rd_ts_t t_start, t_elapsed_ms;
                int i;

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY("Delayed brokers: %d\n", config->delayed_broker_cnt);
                TEST_SAY("========================================\n\n");

                SUB_TEST("%s", config->test_name);

                setup_3broker_share_consumer(config->test_name, 0, &mcluster,
                                             &consumer);

                /* Inject RTT delay on the specified brokers; this delays
                 * responses (including the session-close request). */
                for (i = 0; i < config->delayed_broker_cnt; i++) {
                        TEST_SAY("Injecting %dms RTT delay on broker %d\n",
                                 rtt_delay_ms, config->delayed_brokers[i]);
                        rd_kafka_mock_broker_set_rtt(
                            mcluster, config->delayed_brokers[i], rtt_delay_ms);
                }

                TEST_SAY(
                    "Calling close() with delayed broker response(s)...\n");
                t_start = test_clock();
                rd_kafka_share_consumer_close(consumer);
                t_elapsed_ms = (test_clock() - t_start) / 1000;

                TEST_SAY("Close completed after %" PRId64 " ms\n",
                         t_elapsed_ms);

                /* close() should wait ~rtt_delay_ms for the slowest delayed
                 * broker. Allow (rtt - 1s) to (rtt + 2s). */
                int64_t min_expected_ms = rtt_delay_ms - 1000;
                int64_t max_expected_ms = rtt_delay_ms + 2000;

                TEST_ASSERT(t_elapsed_ms >= min_expected_ms &&
                                t_elapsed_ms <= max_expected_ms,
                            "Close took %" PRId64
                            " ms, expected between %" PRId64 " and %" PRId64
                            " ms (with %d broker(s) delayed by %dms)",
                            t_elapsed_ms, min_expected_ms, max_expected_ms,
                            config->delayed_broker_cnt, rtt_delay_ms);

                TEST_SAY(
                    "SUCCESS: Close waited approximately %dms for "
                    "delayed broker response(s)\n",
                    rtt_delay_ms);

                rd_kafka_share_destroy(consumer);
                test_mock_cluster_destroy(mcluster);

                SUB_TEST_PASS();
        }
}

/**
 * @brief Test close respects socket timeout with very slow broker(s).
 *
 * Verifies that when broker(s) take longer than socket.timeout.ms to
 * respond to the session close request, close() times out at
 * socket.timeout.ms and does not wait for the full broker delay.
 *
 * Tests 3 scenarios with a 3-broker setup:
 * 1. Only broker 1 has delayed response (40s, exceeds 20s socket timeout)
 * 2. Brokers 1 and 2 have delayed responses
 * 3. All 3 brokers have delayed responses
 */
static void test_close_respects_socket_timeout(void) {
        typedef struct {
                const char *test_name;
                int delayed_broker_cnt;
                int32_t delayed_brokers[3];
        } delay_test_config_t;

        delay_test_config_t tests[] = {
            {"close-timeout-1-broker", 1, {1, 0, 0}},
            {"close-timeout-2-brokers", 2, {1, 2, 0}},
            {"close-timeout-3-brokers", 3, {1, 2, 3}},
        };

        const int rtt_delay_ms = 40000; /* 40s — exceeds socket.timeout */
        const int socket_timeout_ms = 20000; /* 20s */

        for (size_t test_idx = 0; test_idx < sizeof(tests) / sizeof(tests[0]);
             test_idx++) {
                delay_test_config_t *config = &tests[test_idx];
                rd_kafka_mock_cluster_t *mcluster;
                rd_kafka_share_t *consumer;
                rd_ts_t t_start, t_elapsed_ms;
                int i;

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY(
                    "Delayed brokers: %d, RTT: %dms, Socket timeout: %dms\n",
                    config->delayed_broker_cnt, rtt_delay_ms,
                    socket_timeout_ms);
                TEST_SAY("========================================\n\n");

                SUB_TEST("%s", config->test_name);

                setup_3broker_share_consumer(
                    config->test_name, socket_timeout_ms, &mcluster, &consumer);

                for (i = 0; i < config->delayed_broker_cnt; i++) {
                        TEST_SAY(
                            "Injecting %dms RTT delay on broker %d "
                            "(exceeds socket timeout %dms)\n",
                            rtt_delay_ms, config->delayed_brokers[i],
                            socket_timeout_ms);
                        rd_kafka_mock_broker_set_rtt(
                            mcluster, config->delayed_brokers[i], rtt_delay_ms);
                }

                TEST_SAY("Calling close() - should timeout around %dms...\n",
                         socket_timeout_ms);
                t_start = test_clock();
                rd_kafka_share_consumer_close(consumer);
                t_elapsed_ms = (test_clock() - t_start) / 1000;

                TEST_SAY("Close completed after %" PRId64 " ms\n",
                         t_elapsed_ms);

                /* close() should timeout around socket.timeout.ms.
                 * Allow ±5s margin. */
                int64_t min_expected_ms = socket_timeout_ms - 5000;
                int64_t max_expected_ms = socket_timeout_ms + 5000;

                TEST_ASSERT(t_elapsed_ms >= min_expected_ms &&
                                t_elapsed_ms <= max_expected_ms,
                            "Close took %" PRId64
                            " ms, expected ~%dms (between %" PRId64
                            " and %" PRId64
                            " ms). Should NOT wait for full broker delay "
                            "of %dms",
                            t_elapsed_ms, socket_timeout_ms, min_expected_ms,
                            max_expected_ms, rtt_delay_ms);

                /* Defensively verify it did not wait the full broker delay. */
                TEST_ASSERT(t_elapsed_ms < (rtt_delay_ms - 10000),
                            "Close took %" PRId64
                            " ms — too close to broker delay %dms. "
                            "Should have timed out at socket.timeout.ms=%dms",
                            t_elapsed_ms, rtt_delay_ms, socket_timeout_ms);

                TEST_SAY(
                    "SUCCESS: Close respected socket timeout (%dms) with "
                    "%d delayed broker(s)\n",
                    socket_timeout_ms, config->delayed_broker_cnt);

                rd_kafka_share_destroy(consumer);
                test_mock_cluster_destroy(mcluster);

                SUB_TEST_PASS();
        }
}

/**
 * @brief Test close with broker error response.
 *
 * Verifies that close() handles error responses from the broker
 * gracefully. When the broker(s) respond with an error to the
 * ShareAcknowledge request (session close request), close() should
 * complete without retrying.
 *
 * Tests 3 scenarios with a 3-broker setup:
 * 1. Only broker 1 returns LEADER_NOT_AVAILABLE
 * 2. Brokers 1 and 2 return the error
 * 3. All 3 brokers return the error
 */
static void test_close_with_broker_error_response(void) {
        typedef struct {
                const char *test_name;
                int erroring_broker_cnt;
                int32_t erroring_brokers[3];
        } error_test_config_t;

        error_test_config_t tests[] = {
            {"close-error-1-broker", 1, {1, 0, 0}},
            {"close-error-2-brokers", 2, {1, 2, 0}},
            {"close-error-3-brokers", 3, {1, 2, 3}},
        };

        const rd_kafka_resp_err_t error_code =
            RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;

        for (size_t test_idx = 0; test_idx < sizeof(tests) / sizeof(tests[0]);
             test_idx++) {
                error_test_config_t *config = &tests[test_idx];
                rd_kafka_mock_cluster_t *mcluster;
                rd_kafka_share_t *consumer;
                int i;

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY("Erroring brokers: %d, error code: %s\n",
                         config->erroring_broker_cnt,
                         rd_kafka_err2str(error_code));
                TEST_SAY("========================================\n\n");

                SUB_TEST("%s", config->test_name);

                setup_3broker_share_consumer(config->test_name, 0, &mcluster,
                                             &consumer);

                /* Push the error onto the specified brokers' next
                 * ShareAcknowledge request. */
                for (i = 0; i < config->erroring_broker_cnt; i++) {
                        TEST_SAY("Injecting %s error on broker %d\n",
                                 rd_kafka_err2str(error_code),
                                 config->erroring_brokers[i]);
                        rd_kafka_mock_broker_push_request_error_rtts(
                            mcluster, config->erroring_brokers[i],
                            RD_KAFKAP_ShareAcknowledge, 1, error_code, 0);
                }

                TEST_SAY(
                    "Calling close() - should complete despite error(s)...\n");
                rd_kafka_share_consumer_close(consumer);
                TEST_SAY("Close completed\n");

                TEST_SAY(
                    "SUCCESS: Close handled error %s gracefully on %d "
                    "broker(s) (no retry)\n",
                    rd_kafka_err2str(error_code), config->erroring_broker_cnt);

                rd_kafka_share_destroy(consumer);
                test_mock_cluster_destroy(mcluster);

                SUB_TEST_PASS();
        }
}


/**
 * @brief Test close() when brokers have pending acks queued.
 *
 * Setup:
 *  - 3-broker mock cluster, topic with 3 partitions (one leader per broker).
 *  - Inject 5s RTT delay on ShareAcknowledge for all brokers — this makes
 *    every ack the consumer sends take 5s broker-side.
 *  - Produce 30 msgs (10/partition), consume them all with an implicit-ack
 *    consumer. Implicit-ack piggybacks acks on the next ShareFetch, so by
 *    the end of the first round there are still acks in flight or queued.
 *  - Repeat: produce another 30, consume them all (same consumer).
 *  - Call close(). close() should drain the in-flight ShareAcknowledges
 *    and send the session-leave; with the 5s delay this should take
 *    roughly 5s (remaining ack RTT) + 5s (leave RTT) = ~10s.
 *  - Remove the RTT delay, start a second consumer (also implicit ack)
 *    and verify it receives 0 messages over up to 5 fetch attempts. If
 *    any message is delivered, close() failed to send the acks.
 */
static void test_close_with_broker_busy(void) {
        const char *test_name        = "close-broker-busy";
        const int partition_cnt      = 3;
        const int msgs_per_partition = 10;
        const int total_msgs         = partition_cnt * msgs_per_partition;
        const int rtt_delay_ms       = 5000;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *consumer, *c2;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *error, *close_err;
        char topic[64], group[64], errstr[512];
        int iter, p, i;
        size_t rcvd;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST("%s", test_name);

        rd_snprintf(topic, sizeof(topic), "mock-%s", test_name);
        rd_snprintf(group, sizeof(group), "sg-%s", test_name);

        TEST_SAY("\n========================================\n");
        TEST_SAY("Test: %s\n", test_name);
        TEST_SAY("ShareAck RTT: %dms on all brokers\n", rtt_delay_ms);
        TEST_SAY("========================================\n\n");

        /* Cluster + APIs + topic */
        mcluster = test_mock_cluster_new(3, &bootstraps);
        enable_share_apis(mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Producer */
        /* Implicit-ack consumer */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Inject 5s RTT delay on the next 20 ShareAcknowledge responses on
         * each broker (no error, just delay) */
        TEST_SAY(
            "Injecting %dms RTT delay on ShareAcknowledge "
            "(20 entries/broker)\n",
            rtt_delay_ms);
        for (i = 1; i <= 3; i++) {
                int j;
                for (j = 0; j < 20; j++) {
                        TEST_ASSERT(
                            rd_kafka_mock_broker_push_request_error_rtts(
                                mcluster, i, RD_KAFKAP_ShareAcknowledge, 1,
                                (rd_kafka_resp_err_t)0,
                                rtt_delay_ms) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Failed to push RTT delay on broker %d", i);
                }
        }

        /* Two rounds of produce+consume so that by the end there are acks
         * queued/in-flight on every broker. */
        for (iter = 1; iter <= 2; iter++) {
                int consumed = 0, attempts = 0;
                TEST_SAY(
                    "Iteration %d: producing %d msgs across %d "
                    "partitions\n",
                    iter, total_msgs, partition_cnt);
                for (p = 0; p < partition_cnt; p++) {
                        test_produce_msgs_easy_v(
                            topic, 0, p, iter * msgs_per_partition,
                            msgs_per_partition, 16, "bootstrap.servers",
                            bootstraps, NULL);
                }

                TEST_SAY("Iteration %d: consuming %d msgs\n", iter, total_msgs);
                while (consumed < total_msgs && attempts++ < 30) {
                        rcvd  = 0;
                        error = rd_kafka_share_consume_batch(consumer, 3000,
                                                             batch, &rcvd);
                        if (error) {
                                rd_kafka_error_destroy(error);
                                continue;
                        }
                        for (i = 0; i < (int)rcvd; i++) {
                                if (!batch[i]->err)
                                        consumed++;
                                rd_kafka_message_destroy(batch[i]);
                        }
                        TEST_ASSERT(rd_kafka_share_commit_async(consumer) ==
                                        NULL,
                                    "fail to call commit async");
                }
                TEST_ASSERT(consumed == total_msgs,
                            "Iteration %d: expected %d msgs, got %d", iter,
                            total_msgs, consumed);
                TEST_SAY("Iteration %d: consumed all %d msgs\n", iter,
                         consumed);
        }

        /* Call close. Expect ~10s: ~5s for in-flight ack to drain, then
         * ~5s for the session-leave ShareAcknowledge to come back. */
        TEST_SAY("Calling close() - expecting ~10s\n");
        t_start      = test_clock();
        close_err    = rd_kafka_share_consumer_close(consumer);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("Close completed after %" PRId64 " ms\n", t_elapsed_ms);
        TEST_ASSERT(close_err == NULL, "Close returned error: %s",
                    rd_kafka_error_string(close_err));
        TEST_ASSERT(t_elapsed_ms >= 8000 && t_elapsed_ms <= 12000,
                    "Close took %" PRId64 " ms, expected 8000-12000 ms",
                    t_elapsed_ms);

        rd_kafka_share_destroy(consumer);

        /* Verify nothing was redelivered: c2 should consume 0 messages
         * because c1's close() flushed all acks. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        c2 = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(c2, "Failed to create c2: %s", errstr);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(c2, subs), "c2 subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        TEST_SAY("c2: verifying no msgs redelivered (5 fetch attempts)\n");
        for (i = 0; i < 5; i++) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(c2, 2000, batch, &rcvd);
                if (error) {
                        TEST_SAY("c2 attempt %d: error: %s\n", i + 1,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }
                TEST_ASSERT(
                    rcvd == 0,
                    "c2 attempt %d: received %d msgs (expected 0 — close "
                    "did not flush acks)",
                    i + 1, (int)rcvd);
        }

        TEST_SAY("SUCCESS: close() flushed acks; c2 received no msgs\n");

        rd_kafka_share_destroy(c2);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test close() when the broker is down.
 *
 * Verifies that close() returns immediately without an error
 * when the broker is down
 *
 * Setup:
 * - Consumer subscribes, consumes at least one batch, then we set the
 *   broker down and call close().
 * - close() should return immediately and return NULL (no error).
 */
static void test_close_with_broker_down(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        const char *topic           = "mock-close-broker-down";
        const char *group           = "sg-close-broker-down";
        const int msgcnt            = 10;
        const int socket_timeout_ms = 20000;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *error;
        rd_kafka_error_t *close_err;
        size_t rcvd;
        int attempts       = 0;
        rd_bool_t got_msgs = rd_false;
        size_t i;
        rd_ts_t t_start, t_elapsed_ms;
        char errstr[512];

        SUB_TEST("close-with-broker-down");

        /* Suppress the transport-error / all-brokers-down events that the
         * test framework's default error_cb would otherwise fail on once we
         * set the broker down. Scoped to this test only. */
        test_curr->is_fatal_cb = test_close_with_broker_down_is_fatal_cb;

        TEST_SAY("\n========================================\n");
        TEST_SAY("Test: close with broker down\n");
        TEST_SAY("Socket timeout: %dms\n", socket_timeout_ms);
        TEST_SAY("========================================\n\n");

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce msgcnt messages via test helper. */
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Consumer with implicit ack mode and explicit socket timeout. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        test_conf_set(conf, "debug", "all");

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume until we get at least one batch of messages, then break.
         */
        TEST_SAY("Consuming until first non-empty batch...\n");
        while (!got_msgs && attempts++ < 30) {
                rcvd = 0;
                error =
                    rd_kafka_share_consume_batch(consumer, 3000, batch, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd > 0) {
                        TEST_SAY("Received %d messages\n", (int)rcvd);
                        for (i = 0; i < rcvd; i++)
                                rd_kafka_message_destroy(batch[i]);
                        got_msgs = rd_true;
                        break;
                }
        }

        TEST_ASSERT(got_msgs, "Expected to receive at least one message");

        /* Bring the broker down before calling close(). */
        TEST_SAY("Setting broker 1 down\n");
        TEST_ASSERT(rd_kafka_mock_broker_set_down(mcluster, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set broker down");

        /* close() should fail fast (broker connection was closed) */
        TEST_SAY("Calling close() with broker down\n");
        t_start      = test_clock();
        close_err    = rd_kafka_share_consumer_close(consumer);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("Close completed after %" PRId64 " ms (err=%s)\n",
                 t_elapsed_ms,
                 close_err ? rd_kafka_error_string(close_err) : "NULL");

        if (close_err)
                rd_kafka_error_destroy(close_err);

        TEST_SAY("SUCCESS: close() returned in %" PRId64
                 "ms after broker down\n",
                 t_elapsed_ms);

        /* Bring the broker back up so the consumer's network threads can
         * finish their shutdown handshake cleanly during destroy. */
        TEST_ASSERT(rd_kafka_mock_broker_set_up(mcluster, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to set broker up");

        rd_kafka_share_destroy(consumer);
        test_mock_cluster_destroy(mcluster);

        /* Restore default error-fatal behavior for subsequent tests. */
        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}

/**
 * @brief Test: calling share-consumer APIs after close() completes.
 *
 * Verifies every guarded API returns RD_KAFKA_RESP_ERR__STATE with
 * "closed" in the error string.
 */
static void test_api_calls_on_closed_consumer(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *consumer;
        const char *topic = "0178-api-after-close";
        const char *group = "grp-0178-api-after-close";
        char errstr[512];
        rd_kafka_error_t *close_error;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Create mock topic");

        /* Produce a single message via test helper (creates an internal
         * producer for the duration of the call). */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 3, "bootstrap.servers",
                                 bootstraps, NULL);

        /* Consumer */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "consumer: %s", errstr);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs), "subscribe");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Close (blocking), then verify all APIs reject further calls. */
        TEST_SAY("Calling close() and waiting for completion\n");
        close_error = rd_kafka_share_consumer_close(consumer);
        TEST_ASSERT(close_error == NULL, "initial close: %s",
                    close_error ? rd_kafka_error_string(close_error) : "");
        TEST_ASSERT(rd_kafka_share_consumer_closed(consumer),
                    "consumer should report closed");

        TEST_SAY("Exercising APIs on closed consumer\n");
        verify_all_apis_return_error(consumer, topic);

        rd_kafka_share_destroy(consumer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test: once close_queue() returns successfully, all subsequent API
 *        calls must be rejected with RD_KAFKA_RESP_ERR__STATE.
 *
 * A 5s RTT delay is injected on ShareAcknowledge so the consumer remains
 * in the "closing" state (close_queue returns immediately, actual close
 * completes later), giving us a deterministic window to exercise every
 * guarded API.
 */
static void test_api_calls_during_closing(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *consumer;
        const char *topic = "0178-api-during-closing";
        const char *group = "grp-0178-api-during-closing";
        char errstr[512];
        rd_kafka_queue_t *queue;
        rd_kafka_error_t *close_error;
        const int rtt_delay_ms = 5000;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        enable_share_apis(mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Create mock topic");

        /* Produce one message via test helper. */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 3, "bootstrap.servers",
                                 bootstraps, NULL);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "consumer: %s", errstr);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs), "subscribe");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume one batch so there's state to flush on close. */
        {
                rd_kafka_message_t *batch[4];
                size_t rcvd            = 0;
                int attempts           = 0;
                rd_kafka_error_t *cerr = NULL;
                while (attempts++ < 20 && rcvd == 0) {
                        cerr = rd_kafka_share_consume_batch(consumer, 500,
                                                            batch, &rcvd);
                        if (cerr)
                                rd_kafka_error_destroy(cerr);
                }
                for (size_t i = 0; i < rcvd; i++)
                        rd_kafka_message_destroy(batch[i]);
        }

        /* Inject 5s delay on ShareAcknowledge so close() stays in flight. */
        TEST_SAY("Injecting %dms RTT delay on broker\n", rtt_delay_ms);
        rd_kafka_mock_broker_set_rtt(mcluster, 1, rtt_delay_ms);

        queue = rd_kafka_queue_new(rd_kafka_share_consumer_get_rk(consumer));

        TEST_SAY("Calling close_queue() to initiate async close\n");
        close_error = rd_kafka_share_consumer_close_queue(consumer, queue);
        TEST_ASSERT(close_error == NULL, "close_queue: %s",
                    close_error ? rd_kafka_error_string(close_error) : "");

        /* close_queue has returned. Consumer must now be in closing state
         * (not fully closed yet, since ShareAcknowledge response is still
         * delayed). Every subsequent API call must be rejected. */
        TEST_ASSERT(!rd_kafka_share_consumer_closed(consumer),
                    "consumer should still be closing, not closed");

        TEST_SAY("Exercising APIs on closing consumer\n");
        verify_all_apis_return_error(consumer, topic);

        /* Wait for async close to complete before destroying. */
        TEST_SAY("Waiting for consumer to finish closing\n");
        while (!rd_kafka_share_consumer_closed(consumer)) {
                rd_usleep(500 * 1000, NULL); /* 500ms */
        }

        rd_kafka_queue_destroy(queue);
        rd_kafka_share_destroy(consumer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0178_share_consumer_close(int argc, char **argv) {
        /* Set overall timeout for all tests */
        test_timeout_set(600);

        /* Create common handles reused across all real-broker tests */
        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        /* Real broker tests */
        test_close_with_acknowledge();
        test_close_without_acknowledge();

        /* Cleanup common handles */
        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}

int main_0178_share_consumer_close_local(int argc, char **argv) {
        /* Mock broker tests only (no real broker needed) */
        TEST_SKIP_MOCK_CLUSTER(0);
        test_timeout_set(300);

        test_close_with_slow_broker_response();
        test_close_respects_socket_timeout();
        test_close_with_broker_error_response();
        test_close_with_broker_busy();
        test_close_with_broker_down();
        test_api_calls_on_closed_consumer();
        test_api_calls_during_closing();
        return 0;
}
