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

#define MAX_TOPICS     16
#define MAX_PARTITIONS 32
#define BATCH_SIZE     10000

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
 * @brief Set share.auto.offset.reset=earliest for a share group.
 */
static void set_group_offset_earliest(rd_kafka_share_t *rkshare,
                                      const char *group_name) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};

        test_IncrementalAlterConfigs_simple(
            rd_kafka_share_consumer_get_rk(rkshare), RD_KAFKA_RESOURCE_GROUP,
            group_name, cfg, 1);
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

                test_create_topic_wait_exists(NULL, ctx->topic_names[t],
                                              partitions[t], -1, 60 * 1000);

                for (p = 0; p < partitions[t]; p++) {
                        test_produce_msgs_easy(ctx->topic_names[t], 0, p,
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
 * @brief Consume and optionally acknowledge messages (modular helper)
 *
 * @param rkshare Consumer handle
 * @param consumer_name Name for logging (e.g., "C1", "C2")
 * @param max_attempts Maximum consume attempts
 * @param ack_count Number of messages to acknowledge and track (0 = consume
 * only)
 * @param tracked_msgs If non-NULL, track acknowledged messages here
 * @param tracked_cnt If non-NULL, store count of tracked messages here
 *
 * @returns Number of messages acknowledged
 */
static int consume_and_acknowledge(rd_kafka_share_t *rkshare,
                                   const char *consumer_name,
                                   int max_attempts,
                                   int ack_count,
                                   tracked_msg_t *tracked_msgs,
                                   int *tracked_cnt) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int attempt;
        rd_kafka_error_t *error;
        int acked = 0;

        TEST_SAY(
            "%s: Consuming and acknowledging %d messages (max %d "
            "attempts)...\n",
            consumer_name, ack_count, max_attempts);

        /* Consume and acknowledge messages until we have ack_count acked */
        for (attempt = 0; attempt < max_attempts && acked < ack_count;
             attempt++) {
                size_t rcvd_msgs = 0;
                int i;

                error = rd_kafka_share_consume_batch(rkshare, 3000, batch,
                                                     &rcvd_msgs);

                if (error) {
                        TEST_SAY("%s: Attempt %d/%d: error: %s\n",
                                 consumer_name, attempt + 1, max_attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd_msgs == 0)
                        continue;

                TEST_SAY("%s: Attempt %d/%d: Received %d messages\n",
                         consumer_name, attempt + 1, max_attempts,
                         (int)rcvd_msgs);

                /* Process each message in this batch */
                for (i = 0; i < (int)rcvd_msgs && acked < ack_count; i++) {
                        rd_kafka_message_t *rkm = batch[i];

                        if (rkm->err) {
                                TEST_SAY(
                                    "%s: Skipping message %d with error:"
                                    "%s\n",
                                    consumer_name, i,
                                    rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Track this message if tracking enabled */
                        if (tracked_msgs && acked < BATCH_SIZE) {
                                tracked_msgs[acked].topic =
                                    rd_strdup(rd_kafka_topic_name(rkm->rkt));
                                tracked_msgs[acked].partition = rkm->partition;
                                tracked_msgs[acked].offset    = rkm->offset;
                        }

                        TEST_SAY(
                            "%s: Acking message %d: %s [%d] @ offset "
                            "%" PRId64 "\n",
                            consumer_name, acked, rd_kafka_topic_name(rkm->rkt),
                            rkm->partition, rkm->offset);

                        rd_kafka_share_acknowledge(rkshare, rkm);
                        rd_kafka_message_destroy(rkm);
                        acked++;
                }

                /* Release any remaining messages from this batch that we
                 * didn't process - they will be redelivered to other consumers
                 */
                for (; i < (int)rcvd_msgs; i++) {
                        rd_kafka_share_acknowledge_type(
                            rkshare, batch[i],
                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
                        rd_kafka_message_destroy(batch[i]);
                }

                /* Exit early if we've acknowledged enough */
                if (acked >= ack_count) {
                        TEST_SAY(
                            "%s: Reached target of %d acknowledged "
                            "messages\n",
                            consumer_name, ack_count);
                        break;
                }
        }

        TEST_SAY("%s: Finished - acknowledged %d messages\n", consumer_name,
                 acked);

        if (acked < ack_count) {
                TEST_FAIL("%s: Expected to acknowledge %d messages, got %d",
                          consumer_name, ack_count, acked);
        }

        if (tracked_cnt)
                *tracked_cnt = acked;

        return acked;
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
 * @brief Verify no redelivery of tracked messages (modular helper)
 *
 * @param rkshare Consumer handle to check
 * @param consumer_name Name for logging
 * @param max_attempts Maximum consume attempts
 * @param expected_msgs Expected number of messages to receive (total -
 tracked)
 * @param tracked_msgs Array of tracked messages to check against
 * @param tracked_cnt Number of tracked messages
 * @param commit_mode Commit mode used (for error messages)
 */
static void verify_no_redelivery(rd_kafka_share_t *rkshare,
                                 const char *consumer_name,
                                 int max_attempts,
                                 int expected_msgs,
                                 const tracked_msg_t *tracked_msgs,
                                 int tracked_cnt,
                                 commit_mode_t commit_mode) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        size_t total_rcvd = 0;
        int attempt;
        rd_kafka_error_t *error;
        int redelivered_count = 0;

        TEST_SAY(
            "%s: Consuming messages for verification (expecting %d "
            "messages, max %d attempts)...\n",
            consumer_name, expected_msgs, max_attempts);

        /* Consume messages until we get expected count or max attempts */
        for (attempt = 0;
             attempt < max_attempts && (int)total_rcvd < expected_msgs;
             attempt++) {
                size_t rcvd_msgs = 0;

                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, batch + total_rcvd, &rcvd_msgs);

                if (error) {
                        TEST_SAY("%s: Attempt %d/%d: error: %s\n",
                                 consumer_name, attempt + 1, max_attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd_msgs > 0) {
                        TEST_SAY(
                            "%s: Attempt %d/%d: Received %d messages "
                            "(total: %d)\n",
                            consumer_name, attempt + 1, max_attempts,
                            (int)rcvd_msgs, (int)(total_rcvd + rcvd_msgs));
                        total_rcvd += rcvd_msgs;
                }
        }

        TEST_SAY("%s: Received %d messages\n", consumer_name, (int)total_rcvd);

        /* Verify we received the expected number of messages */
        if ((int)total_rcvd != expected_msgs) {
                TEST_FAIL("%s: Expected to receive %d messages, got %d",
                          consumer_name, expected_msgs, (int)total_rcvd);
        }

        /* Check if any received message was in tracked list */
        for (size_t i = 0; i < total_rcvd; i++) {
                rd_kafka_message_t *rkm = batch[i];

                if (rkm->err) {
                        rd_kafka_message_destroy(rkm);
                        continue;
                }

                const char *topic = rd_kafka_topic_name(rkm->rkt);
                int partition     = rkm->partition;
                int64_t offset    = rkm->offset;

                /* Check if this message was in tracked list */
                for (int j = 0; j < tracked_cnt; j++) {
                        if (strcmp(topic, tracked_msgs[j].topic) == 0 &&
                            partition == tracked_msgs[j].partition &&
                            offset == tracked_msgs[j].offset) {
                                TEST_WARN(
                                    "%s: Received previously acknowledged "
                                    "message: %s [%d] @ offset %ld\n",
                                    consumer_name, topic, partition, offset);
                                redelivered_count++;
                                break;
                        }
                }

                rd_kafka_message_destroy(rkm);
        }

        if (redelivered_count > 0) {
                TEST_FAIL(
                    "%s received %d messages that were acknowledged "
                    "(commit mode: %s)",
                    consumer_name, redelivered_count,
                    commit_mode_str(commit_mode));
        }

        TEST_SAY(
            "%s: Verification passed - no acknowledged messages were "
            "redelivered\n",
            consumer_name);
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

                /* Create C1 with explicit ack mode (will call acknowledge()
                 * explicitly). C2 will be created after C1 closes. */
                c1 = test_create_share_consumer(ctx.group_id, "explicit");

                /* Set group offset to earliest */
                set_group_offset_earliest(c1, ctx.group_id);

                /* Subscribe C1 only - C2 will subscribe after C1 closes */
                subscribe_consumer(c1, ctx.topic_names, ctx.topic_cnt);

                /* C1: Consume and acknowledge */
                consume_and_acknowledge(c1, "C1", 30, config->ack_count,
                                        tracked_msgs, &tracked_cnt);

                /* Execute commit */
                perform_commit(c1, "C1", config->commit_mode);

                /* Close C1 */
                TEST_SAY("C1: Closing consumer\n");
                rd_kafka_share_consumer_close(c1);
                TEST_SAY("C1: Closed successfully\n");
                rd_kafka_share_destroy(c1);

                /* Create C2 with implicit ack mode after C1 is fully destroyed
                 */
                TEST_SAY("C2: Creating and subscribing after C1 close\n");
                c2 = test_create_share_consumer(ctx.group_id, "implicit");
                subscribe_consumer(c2, ctx.topic_names, ctx.topic_cnt);

                /* C2: Consume and verify no redelivery */
                verify_no_redelivery(
                    c2, "C2", 30, ctx.total_msgs_produced - tracked_cnt,
                    tracked_msgs, tracked_cnt, config->commit_mode);

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
static void test_close_without_acknowledge(void) {
        /**
         * @brief Test configuration for close without acknowledge scenarios
         */
        typedef struct {
                const char *test_name;
                int topic_cnt;
                int partitions[MAX_TOPICS];
                int msgs_per_partition;
        } close_no_ack_test_config_t;

        /* Test matrix: various topologies */
        close_no_ack_test_config_t tests[] = {
            /* 1 topic, 1 partition */
            {"close-no-ack-1t1p", 1, {1}, 20},

            /* 1 topic, multiple partitions */
            {"close-no-ack-1t3p", 1, {3}, 10},

            /* Multiple topics, 1 partition each */
            {"close-no-ack-3t1p", 3, {1, 1, 1}, 10},

            /* Multiple topics, multiple partitions */
            {"close-no-ack-2t2p", 2, {2, 2}, 10},
        };

        for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
                close_no_ack_test_config_t *config = &tests[i];
                test_context_t ctx                 = {0};
                rd_kafka_share_t *c1, *c2;
                rd_kafka_message_t *batch[BATCH_SIZE];
                rd_kafka_error_t *error;
                size_t c1_rcvd_msgs = 0;
                int attempt;
                tracked_msg_t c1_tracked_msgs[BATCH_SIZE];
                int c1_tracked_cnt = 0;

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY("Topology: %d topic(s), partitions: [",
                         config->topic_cnt);
                for (int j = 0; j < config->topic_cnt; j++) {
                        TEST_SAY("%d%s", config->partitions[j],
                                 j < config->topic_cnt - 1 ? ", " : "");
                }
                TEST_SAY("]\n");
                TEST_SAY("========================================\n\n");

                ctx.group_id = "0178-group-no-ack";
                setup_topics_and_produce(&ctx, config->topic_cnt,
                                         config->partitions,
                                         config->msgs_per_partition);

                /* Create C1 with implicit ack mode */
                TEST_SAY("C1: Creating consumer\n");
                c1 = test_create_share_consumer(ctx.group_id, "implicit");

                /* Set group offset to earliest */
                set_group_offset_earliest(c1, ctx.group_id);

                /* Subscribe C1 */
                subscribe_consumer(c1, ctx.topic_names, ctx.topic_cnt);

                /* C1: Consume messages WITHOUT acknowledging */
                TEST_SAY("C1: Consuming messages WITHOUT acknowledging...\n");

                for (attempt = 0; attempt < 30 && c1_rcvd_msgs == 0;
                     attempt++) {
                        error = rd_kafka_share_consume_batch(c1, 3000, batch,
                                                             &c1_rcvd_msgs);

                        if (error) {
                                TEST_SAY("C1: Attempt %d: error: %s\n",
                                         attempt + 1,
                                         rd_kafka_error_string(error));
                                rd_kafka_error_destroy(error);
                                continue;
                        }

                        if (c1_rcvd_msgs > 0) {
                                TEST_SAY(
                                    "C1: Received %d messages (NOT "
                                    "acknowledging)\n",
                                    (int)c1_rcvd_msgs);

                                /* Track and destroy messages without
                                 * acknowledging them */
                                for (size_t i = 0; i < c1_rcvd_msgs; i++) {
                                        if (!batch[i]->err) {
                                                /* Track this message */
                                                c1_tracked_msgs[c1_tracked_cnt]
                                                    .topic = rd_strdup(
                                                    rd_kafka_topic_name(
                                                        batch[i]->rkt));
                                                c1_tracked_msgs[c1_tracked_cnt]
                                                    .partition =
                                                    batch[i]->partition;
                                                c1_tracked_msgs[c1_tracked_cnt]
                                                    .offset = batch[i]->offset;
                                                c1_tracked_cnt++;

                                                TEST_SAY(
                                                    "C1: Consumed (no ack): %s "
                                                    "[%d] @ "
                                                    "offset %" PRId64 "\n",
                                                    rd_kafka_topic_name(
                                                        batch[i]->rkt),
                                                    batch[i]->partition,
                                                    batch[i]->offset);
                                        }
                                        rd_kafka_message_destroy(batch[i]);
                                }
                                break;
                        }
                }

                TEST_ASSERT(
                    c1_rcvd_msgs > 0,
                    "C1: Expected to receive at least 1 message, got %d",
                    (int)c1_rcvd_msgs);

                TEST_ASSERT(c1_tracked_cnt == (int)c1_rcvd_msgs,
                            "C1: Tracked %d messages but received %d",
                            c1_tracked_cnt, (int)c1_rcvd_msgs);

                TEST_SAY(
                    "C1: Consumed and tracked %d messages that will be "
                    "released "
                    "on close\n",
                    c1_tracked_cnt);

                /* Close C1 without acknowledging - records should be released
                 */
                TEST_SAY(
                    "C1: Closing consumer WITHOUT acknowledging consumed "
                    "records\n");
                rd_kafka_share_consumer_close(c1);
                TEST_SAY("C1: Closed successfully\n");
                rd_kafka_share_destroy(c1);

                /* Create C2 after C1 is destroyed */
                TEST_SAY("C2: Creating consumer after C1 close\n");
                c2 = test_create_share_consumer(ctx.group_id, "implicit");
                subscribe_consumer(c2, ctx.topic_names, ctx.topic_cnt);

                /* C2: Should be able to consume all records that C1 released
                 * (C2 may receive additional messages beyond C1's released set)
                 */
                TEST_SAY(
                    "C2: Attempting to consume records released by C1 "
                    "(expecting at least %d messages)...\n",
                    c1_tracked_cnt);

                int c2_matched_cnt = 0;
                int c2_total_msgs  = 0;
                rd_bool_t *c1_msg_matched =
                    rd_calloc(c1_tracked_cnt, sizeof(*c1_msg_matched));

                for (attempt = 0;
                     attempt < 30 && c2_matched_cnt < c1_tracked_cnt;
                     attempt++) {
                        size_t batch_rcvd = 0;
                        size_t i;

                        error = rd_kafka_share_consume_batch(c2, 3000, batch,
                                                             &batch_rcvd);

                        if (error) {
                                TEST_SAY("C2: Attempt %d: error: %s\n",
                                         attempt + 1,
                                         rd_kafka_error_string(error));
                                rd_kafka_error_destroy(error);
                                continue;
                        }

                        if (batch_rcvd == 0)
                                continue;

                        TEST_SAY("C2: Attempt %d: Received %d messages\n",
                                 attempt + 1, (int)batch_rcvd);

                        /* Check each received message against C1's tracked
                         * messages */
                        for (i = 0; i < batch_rcvd; i++) {
                                rd_kafka_message_t *rkm = batch[i];
                                int j;
                                rd_bool_t found = rd_false;

                                if (rkm->err) {
                                        rd_kafka_message_destroy(rkm);
                                        continue;
                                }

                                c2_total_msgs++;

                                const char *topic =
                                    rd_kafka_topic_name(rkm->rkt);
                                int32_t partition = rkm->partition;
                                int64_t offset    = rkm->offset;

                                /* Check if this message matches any from C1's
                                 * tracked list */
                                for (j = 0; j < c1_tracked_cnt; j++) {
                                        if (!c1_msg_matched[j] &&
                                            strcmp(topic,
                                                   c1_tracked_msgs[j].topic) ==
                                                0 &&
                                            partition ==
                                                c1_tracked_msgs[j].partition &&
                                            offset ==
                                                c1_tracked_msgs[j].offset) {
                                                found             = rd_true;
                                                c1_msg_matched[j] = rd_true;
                                                c2_matched_cnt++;

                                                TEST_SAY(
                                                    "C2: Matched C1 message "
                                                    "%d/%d: "
                                                    "%s [%d] @ offset %" PRId64
                                                    "\n",
                                                    c2_matched_cnt,
                                                    c1_tracked_cnt, topic,
                                                    partition, offset);
                                                break;
                                        }
                                }

                                if (!found) {
                                        /* C2 received a message not in C1's
                                         * list - this is OK, just log it */
                                        TEST_SAY(
                                            "C2: Received additional message "
                                            "(not from C1): %s [%d] @ offset "
                                            "%" PRId64 "\n",
                                            topic, partition, offset);
                                }

                                rd_kafka_message_destroy(rkm);
                        }

                        /* Break early if we've matched all C1 messages */
                        if (c2_matched_cnt >= c1_tracked_cnt) {
                                TEST_SAY(
                                    "C2: All %d messages from C1 have been "
                                    "matched, "
                                    "breaking early (total received: %d)\n",
                                    c1_tracked_cnt, c2_total_msgs);
                                break;
                        }
                }

                TEST_SAY(
                    "C2: Total received %d messages, matched %d/%d from "
                    "C1\n",
                    c2_total_msgs, c2_matched_cnt, c1_tracked_cnt);

                /* Verify C2 received all the messages C1 released */
                TEST_ASSERT(
                    c2_matched_cnt == c1_tracked_cnt,
                    "C2: Expected to receive all %d messages released by C1, "
                    "matched only %d",
                    c1_tracked_cnt, c2_matched_cnt);

                /* Check if any C1 messages were not matched */
                for (int j = 0; j < c1_tracked_cnt; j++) {
                        if (!c1_msg_matched[j]) {
                                TEST_FAIL(
                                    "C2: Did not receive C1 message: "
                                    "%s [%d] @ offset %" PRId64,
                                    c1_tracked_msgs[j].topic,
                                    c1_tracked_msgs[j].partition,
                                    c1_tracked_msgs[j].offset);
                        }
                }

                TEST_SAY(
                    "C2: Successfully received all %d messages released "
                    "by C1 "
                    "(total messages received: %d)\n",
                    c2_matched_cnt, c2_total_msgs);

                rd_free(c1_msg_matched);

                /* Close C2 */
                TEST_SAY("C2: Closing consumer\n");
                rd_kafka_share_consumer_close(c2);
                TEST_SAY("C2: Closed successfully\n");
                rd_kafka_share_destroy(c2);

                /* Cleanup tracked messages */
                free_tracked_messages(c1_tracked_msgs, c1_tracked_cnt);

                /* Cleanup */
                for (int t = 0; t < ctx.topic_cnt; t++) {
                        rd_free(ctx.topic_names[t]);
                }

                TEST_SAY("Test %s completed successfully\n\n",
                         config->test_name);
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
                const char *bootstraps;
                rd_kafka_t *producer;
                rd_kafka_share_t *consumer;
                rd_kafka_conf_t *conf;
                const char *topic = "mock-close-delay";
                char group[64];
                const int msgcnt       = 10;
                const int rtt_delay_ms = 10000; /* 10 seconds */
                rd_kafka_message_t *batch[BATCH_SIZE];
                rd_kafka_error_t *error;
                size_t rcvd;
                int consumed = 0;
                int attempts = 0;
                size_t i;
                rd_ts_t t_start, t_elapsed_ms;
                char errstr[512];

                rd_snprintf(group, sizeof(group), "sg-close-delay-%s",
                            config->test_name);

                TEST_SAY("\n========================================\n");
                TEST_SAY("Test: %s\n", config->test_name);
                TEST_SAY("Delayed brokers: %d\n", config->delayed_broker_cnt);
                TEST_SAY("========================================\n\n");

                SUB_TEST("%s", config->test_name);

                /* Create mock cluster with 3 brokers */
                mcluster = test_mock_cluster_new(3, &bootstraps);

                TEST_ASSERT(rd_kafka_mock_set_apiversion(
                                mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1,
                                1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Failed to enable ShareGroupHeartbeat");
                TEST_ASSERT(rd_kafka_mock_set_apiversion(
                                mcluster, RD_KAFKAP_ShareFetch, 1, 1) ==
                                RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Failed to enable ShareFetch");
                TEST_ASSERT(rd_kafka_mock_set_apiversion(
                                mcluster, RD_KAFKAP_ShareAcknowledge, 1, 1) ==
                                RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Failed to enable ShareAck");

                /* Create topic */
                TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                                RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Failed to create mock topic");

                /* Create producer and produce messages */
                test_conf_init(&conf, NULL, 0);
                test_conf_set(conf, "bootstrap.servers", bootstraps);
                producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr,
                                        sizeof(errstr));
                TEST_ASSERT(producer, "Failed to create producer: %s", errstr);

                for (i = 0; i < (size_t)msgcnt; i++) {
                        char payload[64];
                        rd_snprintf(payload, sizeof(payload), "%s-%d", topic,
                                    (int)i);
                        TEST_ASSERT(
                            rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce failed");
                }
                rd_kafka_flush(producer, 5000);

                /* Create consumer */
                test_conf_init(&conf, NULL, 0);
                test_conf_set(conf, "bootstrap.servers", bootstraps);
                test_conf_set(conf, "group.id", group);

                consumer =
                    rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
                TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

                /* Subscribe */
                rd_kafka_topic_partition_list_t *subs;
                subs = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                            "Subscribe failed");
                rd_kafka_topic_partition_list_destroy(subs);

                /* Consume some messages */
                TEST_SAY("Consuming messages...\n");
                while (consumed < msgcnt && attempts++ < 30) {
                        rcvd  = 0;
                        error = rd_kafka_share_consume_batch(consumer, 3000,
                                                             batch, &rcvd);
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

                TEST_SAY("Consumed %d messages\n", consumed);
                TEST_ASSERT(consumed == msgcnt, "Expected %d messages, got %d",
                            msgcnt, consumed);

                /* Inject RTT delay on specified brokers
                 * This will delay responses from partition leaders including
                 * the session close request sent during close() */
                for (i = 0; i < (size_t)config->delayed_broker_cnt; i++) {
                        TEST_SAY("Injecting %dms RTT delay on broker %d\n",
                                 rtt_delay_ms, config->delayed_brokers[i]);
                        rd_kafka_mock_broker_set_rtt(
                            mcluster, config->delayed_brokers[i], rtt_delay_ms);
                }

                /* Call close and measure time
                 * close() should send session close request to partition
                 * leader and wait for the response */
                TEST_SAY(
                    "Calling close() with delayed broker response(s)...\n");
                t_start = test_clock();
                rd_kafka_share_consumer_close(consumer);
                t_elapsed_ms = (test_clock() - t_start) / 1000;

                TEST_SAY("Close completed after %" PRId64 " ms\n",
                         t_elapsed_ms);

                /* Verify close waited for the delayed response
                 * Should take approximately rtt_delay_ms
                 * Allow range: (rtt_delay_ms - 1000ms) to (rtt_delay_ms +
                 * 2000ms) */
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
                rd_kafka_destroy(producer);
                test_mock_cluster_destroy(mcluster);

                SUB_TEST_PASS();
        }
}

/**
 * @brief Test close respects socket timeout with very slow broker
 *
 * Verifies that when broker takes longer than socket.timeout.ms (60s)
 * to respond to the session close request, close() should timeout and
 * not wait indefinitely.
 *
 * Setup:
 * - Single broker with 80s RTT delay (exceeds 60s socket timeout)
 * - close() should timeout around 60s, not wait for full 80s
 */
static void test_close_respects_socket_timeout(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *producer;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        const char *topic = "mock-close-timeout";
        const char *group = "sg-close-timeout";
        const int msgcnt  = 10;
        const int rtt_delay_ms =
            40000; /* 40 seconds - exceeds socket timeout */
        const int socket_timeout_ms = 20000; /* 60 seconds */
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *error;
        size_t rcvd;
        int consumed = 0;
        int attempts = 0;
        size_t i;
        rd_ts_t t_start, t_elapsed_ms;
        char errstr[512];

        SUB_TEST("close-respects-socket-timeout");

        TEST_SAY("\n========================================\n");
        TEST_SAY("Test: close respects socket timeout\n");
        TEST_SAY("Broker delay: %dms, Socket timeout: %dms\n", rtt_delay_ms,
                 socket_timeout_ms);
        TEST_SAY("========================================\n\n");

        /* Create mock cluster with single broker */
        mcluster = test_mock_cluster_new(1, &bootstraps);

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

        /* Create topic */
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Create producer and produce messages */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(producer, "Failed to create producer: %s", errstr);

        for (i = 0; i < (size_t)msgcnt; i++) {
                char payload[64];
                rd_snprintf(payload, sizeof(payload), "%s-%d", topic, (int)i);
                TEST_ASSERT(rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce failed");
        }
        rd_kafka_flush(producer, 5000);

        /* Create consumer with explicit socket timeout */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        /* Set socket.timeout.ms to 60 seconds */
        rd_snprintf(errstr, sizeof(errstr), "%d", socket_timeout_ms);
        test_conf_set(conf, "socket.timeout.ms", errstr);

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe */
        rd_kafka_topic_partition_list_t *subs;
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume some messages */
        TEST_SAY("Consuming messages...\n");
        while (consumed < msgcnt && attempts++ < 30) {
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

        TEST_SAY("Consumed %d messages\n", consumed);
        TEST_ASSERT(consumed == msgcnt, "Expected %d messages, got %d", msgcnt,
                    consumed);

        /* Inject 80s RTT delay on broker (exceeds 60s socket timeout)
         * This will cause the session close request to timeout */
        TEST_SAY(
            "Injecting %dms RTT delay on broker 1 (exceeds socket "
            "timeout)\n",
            rtt_delay_ms);
        rd_kafka_mock_broker_set_rtt(mcluster, 1, rtt_delay_ms);

        /* Call close and measure time
         * close() should timeout around socket.timeout.ms (60s)
         * and NOT wait for the full 80s broker delay */
        TEST_SAY("Calling close() - should timeout around %dms...\n",
                 socket_timeout_ms);
        t_start = test_clock();
        rd_kafka_share_consumer_close(consumer);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("Close completed after %" PRId64 " ms\n", t_elapsed_ms);

        /* Verify close did NOT wait for full 80s delay
         * Should timeout around socket.timeout.ms (60s)
         * Allow range: 55s to 65s (socket timeout ± 5s margin) */
        int64_t min_expected_ms = socket_timeout_ms - 5000;
        int64_t max_expected_ms = socket_timeout_ms + 5000;

        TEST_ASSERT(t_elapsed_ms >= min_expected_ms &&
                        t_elapsed_ms <= max_expected_ms,
                    "Close took %" PRId64
                    " ms, expected timeout around %dms "
                    "(between %" PRId64 " and %" PRId64
                    " ms). "
                    "Should NOT wait for full broker delay of %dms",
                    t_elapsed_ms, socket_timeout_ms, min_expected_ms,
                    max_expected_ms, rtt_delay_ms);

        /* Also verify it definitely did NOT wait for the full 80s */
        TEST_ASSERT(t_elapsed_ms < 70000,
                    "Close took %" PRId64
                    " ms, which is too close to broker delay "
                    "of %dms. Should have timed out at socket.timeout.ms=%dms",
                    t_elapsed_ms, rtt_delay_ms, socket_timeout_ms);

        TEST_SAY(
            "SUCCESS: Close respected socket timeout (%dms) and did not "
            "wait for full broker delay (%dms)\n",
            socket_timeout_ms, rtt_delay_ms);

        rd_kafka_share_destroy(consumer);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test close with broker error response
 *
 * Verifies that close() handles error responses from the broker gracefully.
 * When the broker responds with an error to the ShareAcknowledge request
 * (session close request), close() should handle it.
 *
 * This test injects a COORDINATOR_NOT_AVAILABLE error response and verifies
 * that close() completes (errors during close are not retriable).
 */
static void test_close_with_broker_error_response(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *producer;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        const char *topic              = "mock-close-error";
        const char *group              = "sg-close-error";
        const int msgcnt               = 10;
        rd_kafka_resp_err_t error_code = RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *error;
        size_t rcvd;
        int consumed = 0;
        int attempts = 0;
        size_t i;
        char errstr[512];

        SUB_TEST("close-with-broker-error");

        TEST_SAY("\n========================================\n");
        TEST_SAY("Test: Close with broker error response\n");
        TEST_SAY("Error code: %s\n", rd_kafka_err2str(error_code));
        TEST_SAY("========================================\n\n");

        /* Create mock cluster with single broker */
        mcluster = test_mock_cluster_new(1, &bootstraps);

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

        /* Create topic */
        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Create producer and produce messages */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(producer, "Failed to create producer: %s", errstr);

        for (i = 0; i < (size_t)msgcnt; i++) {
                char payload[64];
                rd_snprintf(payload, sizeof(payload), "%s-%d", topic, (int)i);
                TEST_ASSERT(rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce failed");
        }
        rd_kafka_flush(producer, 5000);

        /* Create consumer */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe */
        rd_kafka_topic_partition_list_t *subs;
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume some messages */
        TEST_SAY("Consuming messages...\n");
        while (consumed < msgcnt && attempts++ < 30) {
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

        TEST_SAY("Consumed %d messages\n", consumed);
        TEST_ASSERT(consumed == msgcnt, "Expected %d messages, got %d", msgcnt,
                    consumed);

        /* Inject error response for ShareAcknowledge (session close request).
         */
        TEST_SAY("Injecting %s error for ShareAcknowledge\n",
                 rd_kafka_err2str(error_code));
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1, error_code, 0);

        TEST_SAY("Calling close() - should complete despite error...\n");
        rd_kafka_share_consumer_close(consumer);
        TEST_SAY("Close completed\n");

        TEST_SAY("SUCCESS: Close handled error %s gracefully (no retry)\n",
                 rd_kafka_err2str(error_code));

        rd_kafka_share_destroy(consumer);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Helper: assert the given rd_kafka_error_t* signals closed/closing.
 *
 * Expects non-NULL error with err code RD_KAFKA_RESP_ERR__STATE and a
 * string containing \p expect_substr ("closed" or "closing").
 * Destroys the error.
 */
static void assert_state_error(rd_kafka_error_t *error,
                               const char *api_name,
                               const char *expect_substr) {
        TEST_ASSERT(error != NULL, "%s: expected error, got NULL", api_name);
        TEST_ASSERT(rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__STATE,
                    "%s: expected __STATE, got %s", api_name,
                    rd_kafka_err2name(rd_kafka_error_code(error)));
        TEST_ASSERT(strstr(rd_kafka_error_string(error), expect_substr) != NULL,
                    "%s: expected error string to contain '%s', got '%s'",
                    api_name, expect_substr, rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);
}

/**
 * @brief Invoke every share-consumer API and assert each returns a
 *        closed/closing state error.
 *
 * @param consumer    Share consumer (already closed or closing).
 * @param topic       Topic name (for subscribe/acknowledge_offset).
 * @param expect_substr "closed" or "closing" (matched in error string).
 */
static void verify_all_apis_return_state_error(rd_kafka_share_t *consumer,
                                               const char *topic,
                                               const char *expect_substr) {
        rd_kafka_error_t *error;
        rd_kafka_resp_err_t err;
        rd_kafka_message_t *batch[4];
        size_t rcvd = 0;
        rd_kafka_topic_partition_list_t *subs, *sub_result = NULL;
        rd_kafka_topic_partition_list_t *commit_results = NULL;
        rd_kafka_queue_t *queue                         = NULL;

        /* 1. consume_batch */
        error = rd_kafka_share_consume_batch(consumer, 100, batch, &rcvd);
        assert_state_error(error, "consume_batch", expect_substr);

        /* 2. commit_async */
        error = rd_kafka_share_commit_async(consumer);
        assert_state_error(error, "commit_async", expect_substr);

        /* 3. commit_sync */
        error = rd_kafka_share_commit_sync(consumer, 1000, &commit_results);
        assert_state_error(error, "commit_sync", expect_substr);
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
        assert_state_error(error, "close", expect_substr);

        /* 9. async close */
        queue = rd_kafka_queue_new(rd_kafka_share_consumer_get_rk(consumer));
        error = rd_kafka_share_consumer_close_queue(consumer, queue);
        rd_kafka_queue_destroy(queue);
        assert_state_error(error, "close", expect_substr);
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
        rd_kafka_t *producer;
        rd_kafka_share_t *consumer;
        const char *topic = "0178-api-after-close";
        const char *group = "grp-0178-api-after-close";
        char errstr[512];
        rd_kafka_error_t *close_error;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "ShareGroupHeartbeat apiversion");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareFetch,
                                                 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "ShareFetch apiversion");
        TEST_ASSERT(
            rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareAcknowledge,
                                         1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR,
            "ShareAcknowledge apiversion");

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Create mock topic");

        /* Producer */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(producer, "producer: %s", errstr);
        TEST_ASSERT(rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic),
                                      RD_KAFKA_V_VALUE("msg", 3),
                                      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                      RD_KAFKA_V_END) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "produce");
        rd_kafka_flush(producer, 5000);

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
        verify_all_apis_return_state_error(consumer, topic, "closed");

        rd_kafka_share_destroy(consumer);
        rd_kafka_destroy(producer);
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
        rd_kafka_t *producer;
        rd_kafka_share_t *consumer;
        const char *topic = "0178-api-during-closing";
        const char *group = "grp-0178-api-during-closing";
        char errstr[512];
        rd_kafka_queue_t *queue;
        rd_kafka_error_t *close_error;
        const int rtt_delay_ms = 5000;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "ShareGroupHeartbeat apiversion");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareFetch,
                                                 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "ShareFetch apiversion");
        TEST_ASSERT(
            rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_ShareAcknowledge,
                                         1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR,
            "ShareAcknowledge apiversion");

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Create mock topic");

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(producer, "producer: %s", errstr);
        TEST_ASSERT(rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic),
                                      RD_KAFKA_V_VALUE("msg", 3),
                                      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                      RD_KAFKA_V_END) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "produce");
        rd_kafka_flush(producer, 5000);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "debug", "all");
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
        verify_all_apis_return_state_error(consumer, topic, "closing");

        /* Wait for async close to complete before destroying. */
        TEST_SAY("Waiting for consumer to finish closing\n");
        while (!rd_kafka_share_consumer_closed(consumer)) {
                rd_usleep(500 * 1000, NULL); /* 500ms */
        }

        rd_kafka_queue_destroy(queue);
        rd_kafka_share_destroy(consumer);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0178_share_consumer_close(int argc, char **argv) {
        /* Set overall timeout for all tests */
        test_timeout_set(600); /* 10 minutes */

        /* Real broker tests */
        test_close_with_acknowledge();
        test_close_without_acknowledge();

        return 0;
}

int main_0178_share_consumer_close_local(int argc, char **argv) {
        /* Mock broker tests only (no real broker needed) */
        TEST_SKIP_MOCK_CLUSTER(0);

        test_close_with_slow_broker_response();
        test_close_respects_socket_timeout();
        test_close_with_broker_error_response();
        test_api_calls_on_closed_consumer();
        test_api_calls_during_closing();

        return 0;
}
