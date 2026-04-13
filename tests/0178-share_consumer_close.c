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
//
#include "test.h"
//
// #define MAX_TOPICS 16
// #define MAX_PARTITIONS 32
// #define BATCH_SIZE 1000
//
// /**
//  * @brief Commit mode for test scenarios
//  */
// typedef enum {
//         COMMIT_MODE_NONE,  /**< No commit before close */
//         COMMIT_MODE_ASYNC, /**< Call commitAsync before close */
//         COMMIT_MODE_SYNC   /**< Call commitSync before close */
// } commit_mode_t;
//
// /**
//  * @brief Generic test context for topic and message tracking
//  */
// typedef struct {
//         char *topic_names[MAX_TOPICS];
//         int topic_cnt;
//         char *group_id;
//         int total_msgs_produced;
// } test_context_t;
//
// /**
//  * @brief Tracked message info for verification
//  */
// typedef struct {
//         char *topic;
//         int32_t partition;
//         int64_t offset;
// } tracked_msg_t;
//
// /**
//  * @brief Get string representation of commit mode
//  */
// static const char *commit_mode_str(commit_mode_t mode) {
//         switch (mode) {
//         case COMMIT_MODE_NONE:
//                 return "no-commit";
//         case COMMIT_MODE_ASYNC:
//                 return "commit-async";
//         case COMMIT_MODE_SYNC:
//                 return "commit-sync";
//         default:
//                 return "unknown";
//         }
// }
//
// /**
//  * @brief Create share consumer with explicit acknowledgement mode
//  */
// static rd_kafka_share_t *create_explicit_ack_consumer(const char *group_id) {
//         rd_kafka_share_t *rk;
//         rd_kafka_conf_t *conf;
//         char errstr[512];
//
//         test_conf_init(&conf, NULL, 60);
//
//         rd_kafka_conf_set(conf, "group.id", group_id, errstr,
//         sizeof(errstr)); rd_kafka_conf_set(conf, "enable.auto.commit",
//         "false", errstr,
//                           sizeof(errstr));
//         rd_kafka_conf_set(conf, "share.acknowledgement.mode", "explicit",
//                           errstr, sizeof(errstr));
//         rd_kafka_conf_set(conf, "debug", "all",
//                           errstr, sizeof(errstr));
//
//         rk = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
//         TEST_ASSERT(rk, "Failed to create explicit ack consumer: %s",
//         errstr);
//
//         return rk;
// }
//
// /**
//  * @brief Create share consumer with implicit acknowledgement mode (default)
//  */
// static rd_kafka_share_t *create_implicit_ack_consumer(const char *group_id) {
//         rd_kafka_share_t *rk;
//         rd_kafka_conf_t *conf;
//         char errstr[512];
//
//         test_conf_init(&conf, NULL, 60);
//
//         rd_kafka_conf_set(conf, "group.id", group_id, errstr,
//         sizeof(errstr)); rd_kafka_conf_set(conf, "enable.auto.commit",
//         "false", errstr,
//                           sizeof(errstr));
//         /* Don't set share.acknowledgement.mode - defaults to implicit */
//
//         rk = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
//         TEST_ASSERT(rk, "Failed to create implicit ack consumer: %s",
//         errstr);
//
//         return rk;
// }
//
// /**
//  * @brief Set group offset to earliest
//  */
// static void set_group_offset_earliest(rd_kafka_share_t *rkshare,
//                                       const char *group_name) {
//         const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
//         test_IncrementalAlterConfigs_simple(test_share_consumer_get_rk(rkshare),
//                                             RD_KAFKA_RESOURCE_GROUP,
//                                             group_name, cfg, 1);
// }
//
// /**
//  * @brief Initialize test context
//  */
// static void init_test_context(test_context_t *ctx) {
//         memset(ctx, 0, sizeof(*ctx));
//         ctx->group_id = "0174-group";
// }
//
// /**
//  * @brief Setup topics and produce messages (modular helper)
//  *
//  * @param ctx Test context to populate
//  * @param topic_cnt Number of topics to create
//  * @param partitions Array of partition counts per topic
//  * @param msgs_per_partition Messages to produce per partition
//  *
//  * @returns Total number of messages produced
//  */
// static int setup_topics_and_produce(test_context_t *ctx,
//                                     int topic_cnt,
//                                     const int *partitions,
//                                     int msgs_per_partition) {
//         int t, p;
//         int total_msgs = 0;
//
//         ctx->topic_cnt = topic_cnt;
//
//         for (t = 0; t < topic_cnt; t++) {
//                 ctx->topic_names[t] =
//                     rd_strdup(test_mk_topic_name("0174-close-test", 1));
//
//                 TEST_SAY("Creating topic %s with %d partition(s)\n",
//                          ctx->topic_names[t], partitions[t]);
//
//                 test_create_topic_wait_exists(NULL, ctx->topic_names[t],
//                                               partitions[t], -1, 60 * 1000);
//
//                 for (p = 0; p < partitions[t]; p++) {
//                         test_produce_msgs_easy(ctx->topic_names[t], 0, p,
//                                                msgs_per_partition);
//                         total_msgs += msgs_per_partition;
//                 }
//
//                 TEST_SAY("Topic %s: produced %d messages (%d partitions * %d
//                 "
//                          "msgs)\n",
//                          ctx->topic_names[t],
//                          partitions[t] * msgs_per_partition, partitions[t],
//                          msgs_per_partition);
//         }
//
//         ctx->total_msgs_produced = total_msgs;
//         TEST_SAY("Total messages produced: %d\n", total_msgs);
//         return total_msgs;
// }
//
// /**
//  * @brief Subscribe consumer to topics
//  *
//  * @param rkshare Consumer handle
//  * @param topic_names Array of topic names to subscribe to
//  * @param topic_cnt Number of topics
//  */
// static void subscribe_consumer(rd_kafka_share_t *rkshare,
//                                char **topic_names,
//                                int topic_cnt) {
//         rd_kafka_topic_partition_list_t *subs;
//         rd_kafka_resp_err_t err;
//         int t;
//
//         /* Build subscription list */
//         subs = rd_kafka_topic_partition_list_new(topic_cnt);
//         for (t = 0; t < topic_cnt; t++) {
//                 rd_kafka_topic_partition_list_add(subs, topic_names[t],
//                                                   RD_KAFKA_PARTITION_UA);
//         }
//
//         /* Subscribe */
//         err = rd_kafka_share_subscribe(rkshare, subs);
//         if (err) {
//                 TEST_FAIL("Subscribe failed: %s", rd_kafka_err2str(err));
//         }
//
//         rd_kafka_topic_partition_list_destroy(subs);
// }
//
// /**
//  * @brief Consume and optionally acknowledge messages (modular helper)
//  *
//  * @param rkshare Consumer handle
//  * @param consumer_name Name for logging (e.g., "C1", "C2")
//  * @param max_attempts Maximum consume attempts
//  * @param ack_count Number of messages to acknowledge and track (0 = consume
//  * only)
//  * @param tracked_msgs If non-NULL, track acknowledged messages here
//  * @param tracked_cnt If non-NULL, store count of tracked messages here
//  *
//  * @returns Number of messages acknowledged
//  */
// static int consume_and_acknowledge(rd_kafka_share_t *rkshare,
//                                    const char *consumer_name,
//                                    int max_attempts,
//                                    int ack_count,
//                                    tracked_msg_t *tracked_msgs,
//                                    int *tracked_cnt) {
//         rd_kafka_message_t *batch[BATCH_SIZE];
//         int attempt;
//         rd_kafka_error_t *error;
//         int acked = 0;
//
//         TEST_SAY("%s: Consuming and acknowledging %d messages (max %d "
//                  "attempts)...\n",
//                  consumer_name, ack_count, max_attempts);
//
//         /* Consume and acknowledge messages until we have ack_count acked */
//         for (attempt = 0; attempt < max_attempts && acked < ack_count;
//              attempt++) {
//                 size_t rcvd_msgs = 0;
//                 int i;
//
//                 error = rd_kafka_share_consume_batch(rkshare, 3000, batch,
//                                                      &rcvd_msgs);
//
//                 if (error) {
//                         TEST_SAY("%s: Attempt %d/%d: error: %s\n",
//                                  consumer_name, attempt + 1, max_attempts,
//                                  rd_kafka_error_string(error));
//                         rd_kafka_error_destroy(error);
//                         continue;
//                 }
//
//                 if (rcvd_msgs == 0)
//                         continue;
//
//                 TEST_SAY("%s: Attempt %d/%d: Received %d messages\n",
//                          consumer_name, attempt + 1, max_attempts,
//                          (int)rcvd_msgs);
//
//                 /* Process each message in this batch */
//                 for (i = 0; i < (int)rcvd_msgs && acked < ack_count; i++) {
//                         rd_kafka_message_t *rkm = batch[i];
//
//                         if (rkm->err) {
//                                 TEST_SAY("%s: Skipping message %d with error:
//                                 "
//                                          "%s\n",
//                                          consumer_name, i,
//                                          rd_kafka_message_errstr(rkm));
//                                 rd_kafka_message_destroy(rkm);
//                                 continue;
//                         }
//
//                         /* Track this message if tracking enabled */
//                         if (tracked_msgs && acked < BATCH_SIZE) {
//                                 tracked_msgs[acked].topic = rd_strdup(
//                                     rd_kafka_topic_name(rkm->rkt));
//                                 tracked_msgs[acked].partition =
//                                 rkm->partition; tracked_msgs[acked].offset =
//                                 rkm->offset;
//                         }
//
//                         TEST_SAY("%s: Acking message %d: %s [%d] @ offset "
//                                  "%llu\n",
//                                  consumer_name, acked,
//                                  rd_kafka_topic_name(rkm->rkt),
//                                  rkm->partition, rkm->offset);
//
//                         rd_kafka_share_acknowledge(rkshare, rkm);
//                         rd_kafka_message_destroy(rkm);
//                         acked++;
//                 }
//
//                 /* Destroy any remaining messages from this batch that we
//                  * didn't process */
//                 for (; i < (int)rcvd_msgs; i++) {
//                         rd_kafka_message_destroy(batch[i]);
//                 }
//
//                 /* Exit early if we've acknowledged enough */
//                 if (acked >= ack_count) {
//                         TEST_SAY("%s: Reached target of %d acknowledged "
//                                  "messages\n",
//                                  consumer_name, ack_count);
//                         break;
//                 }
//         }
//
//         TEST_SAY("%s: Finished - acknowledged %d messages\n", consumer_name,
//                  acked);
//
//         if (acked < ack_count) {
//                 TEST_FAIL("%s: Expected to acknowledge %d messages, got %d",
//                           consumer_name, ack_count, acked);
//         }
//
//         if (tracked_cnt)
//                 *tracked_cnt = acked;
//
//         return acked;
// }
//
// /**
//  * @brief Execute commit based on mode (modular helper)
//  *
//  * @param rkshare Consumer handle
//  * @param consumer_name Name for logging
//  * @param mode Commit mode to execute
//  */
// static void perform_commit(rd_kafka_share_t *rkshare,
//                            const char *consumer_name,
//                            commit_mode_t mode) {
//         switch (mode) {
//         case COMMIT_MODE_NONE:
//                 TEST_SAY("%s: Skipping commit (mode: %s)\n", consumer_name,
//                          commit_mode_str(mode));
//                 break;
//
//         case COMMIT_MODE_ASYNC:
//                 TEST_SAY("%s: Calling commitAsync\n", consumer_name);
//                 rd_kafka_share_commit_async(rkshare);
//                 /* Give async commit some time to process */
//                 rd_sleep(1);
//                 break;
//
//         // case COMMIT_MODE_SYNC:
//         //         TEST_SAY("%s: Calling commitSync\n", consumer_name);
//         //         error = rd_kafka_share_commit_sync(rkshare);
//         //         if (error) {
//         //                 TEST_FAIL("%s: commitSync failed: %s",
//         consumer_name,
//         //                           rd_kafka_error_string(error));
//         //                 rd_kafka_error_destroy(error);
//         //         }
//         //         TEST_SAY("%s: commitSync completed successfully\n",
//         //                  consumer_name);
//         //         break;
//
//         default:
//                 TEST_FAIL("Unknown commit mode: %d", mode);
//         }
// }
//
// /**
//  * @brief Verify no redelivery of tracked messages (modular helper)
//  *
//  * @param rkshare Consumer handle to check
//  * @param consumer_name Name for logging
//  * @param max_attempts Maximum consume attempts
//  * @param expected_msgs Expected number of messages to receive (total -
//  tracked)
//  * @param tracked_msgs Array of tracked messages to check against
//  * @param tracked_cnt Number of tracked messages
//  * @param commit_mode Commit mode used (for error messages)
//  */
// static void verify_no_redelivery(rd_kafka_share_t *rkshare,
//                                  const char *consumer_name,
//                                  int max_attempts,
//                                  int expected_msgs,
//                                  const tracked_msg_t *tracked_msgs,
//                                  int tracked_cnt,
//                                  commit_mode_t commit_mode) {
//         rd_kafka_message_t *batch[BATCH_SIZE];
//         size_t total_rcvd = 0;
//         int attempt;
//         rd_kafka_error_t *error;
//         int redelivered_count = 0;
//
//         TEST_SAY("%s: Consuming messages for verification (expecting %d "
//                  "messages, max %d attempts)...\n",
//                  consumer_name, expected_msgs, max_attempts);
//
//         /* Consume messages until we get expected count or max attempts */
//         for (attempt = 0; attempt < max_attempts && total_rcvd <
//         expected_msgs;
//              attempt++) {
//                 size_t rcvd_msgs = 0;
//
//                 error = rd_kafka_share_consume_batch(rkshare, 3000,
//                                                      batch + total_rcvd,
//                                                      &rcvd_msgs);
//
//                 if (error) {
//                         TEST_SAY("%s: Attempt %d/%d: error: %s\n",
//                                  consumer_name, attempt + 1, max_attempts,
//                                  rd_kafka_error_string(error));
//                         rd_kafka_error_destroy(error);
//                         continue;
//                 }
//
//                 if (rcvd_msgs > 0) {
//                         TEST_SAY("%s: Attempt %d/%d: Received %d messages "
//                                  "(total: %d)\n",
//                                  consumer_name, attempt + 1, max_attempts,
//                                  (int)rcvd_msgs, (int)(total_rcvd +
//                                  rcvd_msgs));
//                         total_rcvd += rcvd_msgs;
//                 }
//         }
//
//         TEST_SAY("%s: Received %d messages\n", consumer_name,
//         (int)total_rcvd);
//
//         /* Verify we received the expected number of messages */
//         if ((int)total_rcvd != expected_msgs) {
//                 TEST_FAIL("%s: Expected to receive %d messages, got %d",
//                           consumer_name, expected_msgs, (int)total_rcvd);
//         }
//
//         /* Check if any received message was in tracked list */
//         for (size_t i = 0; i < total_rcvd; i++) {
//                 rd_kafka_message_t *rkm = batch[i];
//
//                 if (rkm->err) {
//                         rd_kafka_message_destroy(rkm);
//                         continue;
//                 }
//
//                 const char *topic = rd_kafka_topic_name(rkm->rkt);
//                 int partition     = rkm->partition;
//                 int64_t offset    = rkm->offset;
//
//                 /* Check if this message was in tracked list */
//                 for (int j = 0; j < tracked_cnt; j++) {
//                         if (strcmp(topic, tracked_msgs[j].topic) == 0 &&
//                             partition == tracked_msgs[j].partition &&
//                             offset == tracked_msgs[j].offset) {
//                                 TEST_WARN(
//                                     "%s: Received previously acknowledged "
//                                     "message: %s [%d] @ offset %ld\n",
//                                     consumer_name, topic, partition, offset);
//                                 redelivered_count++;
//                                 break;
//                         }
//                 }
//
//                 rd_kafka_message_destroy(rkm);
//         }
//
//         if (redelivered_count > 0) {
//                 TEST_FAIL("%s received %d messages that were acknowledged "
//                           "(commit mode: %s)",
//                           consumer_name, redelivered_count,
//                           commit_mode_str(commit_mode));
//         }
//
//         TEST_SAY("%s: Verification passed - no acknowledged messages were "
//                  "redelivered\n",
//                  consumer_name);
// }
//
// /**
//  * @brief Free tracked messages (modular helper)
//  *
//  * @param tracked_msgs Array of tracked messages
//  * @param tracked_cnt Number of tracked messages
//  */
// static void free_tracked_messages(tracked_msg_t *tracked_msgs,
//                                   int tracked_cnt) {
//         for (int i = 0; i < tracked_cnt; i++) {
//                 if (tracked_msgs[i].topic)
//                         rd_free(tracked_msgs[i].topic);
//         }
// }
//
// /**
//  * @brief Close with acknowledge test scenarios
//  *
//  * Tests consumer close behavior with different commit modes and topologies.
//  * Verifies that acknowledged messages are not redelivered to a second
//  consumer.
//  */
// static void test_close_with_acknowledge(void) {
//         /**
//          * @brief Test configuration for close with acknowledge scenarios
//          */
//         typedef struct {
//                 const char *test_name;
//                 int topic_cnt;
//                 int partitions[MAX_TOPICS];
//                 int msgs_per_partition;
//                 commit_mode_t commit_mode;
//                 int ack_count;
//         } close_ack_test_config_t;
//
//         /* Test matrix: 3 commit modes × 4 topologies = 12 tests */
//         close_ack_test_config_t tests[] = {
//             /* 1 topic, 1 partition */
//             // {"close-1t1p-no-commit", 1, {1}, 20, COMMIT_MODE_NONE, 10},
//             // {"close-1t1p-commit-async", 1, {1}, 20, COMMIT_MODE_ASYNC,
//             10},
//             //{"close-1t1p-commit-sync", 1, {1}, 20, COMMIT_MODE_SYNC, 10},
//
//             /* 1 topic, multiple partitions */
//             {"close-1t3p-no-commit", 1, {3}, 10, COMMIT_MODE_NONE, 15},
//             //{"close-1t3p-commit-async", 1, {3}, 10, COMMIT_MODE_ASYNC, 15},
//              //{"close-1t3p-commit-sync", 1, {3}, 10, COMMIT_MODE_SYNC, 15},
//
//             /* Multiple topics, 1 partition each */
//             // {"close-3t1p-no-commit", 3, {1, 1, 1}, 10, COMMIT_MODE_NONE,
//             15},
//             //  {"close-3t1p-commit-async", 3, {1, 1, 1}, 10,
//             COMMIT_MODE_ASYNC,
//             // 15},
//             // {"close-3t1p-commit-sync", 3, {1, 1, 1}, 10, COMMIT_MODE_SYNC,
//             15},
//
//             /* Multiple topics, multiple partitions */
//             // {"close-2t2p-no-commit", 2, {2, 2}, 10, COMMIT_MODE_NONE, 20},
//             // {"close-2t2p-commit-async", 2, {2, 2}, 10, COMMIT_MODE_ASYNC,
//             20},
//             //{"close-2t2p-commit-sync", 2, {2, 2}, 10, COMMIT_MODE_SYNC,
//             20},
//         };
//
//         for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
//                 close_ack_test_config_t *config = &tests[i];
//                 test_context_t ctx              = {0};
//                 rd_kafka_share_t *c1, *c2;
//                 tracked_msg_t tracked_msgs[BATCH_SIZE];
//                 int tracked_cnt = 0;
//
//                 TEST_SAY("\n========================================\n");
//                 TEST_SAY("Test: %s\n", config->test_name);
//                 TEST_SAY("Topology: %d topic(s), partitions: [",
//                          config->topic_cnt);
//                 for (int j = 0; j < config->topic_cnt; j++) {
//                         TEST_SAY("%d%s", config->partitions[j],
//                                  j < config->topic_cnt - 1 ? ", " : "");
//                 }
//                 TEST_SAY("]\n");
//                 TEST_SAY("Commit mode: %s\n",
//                          commit_mode_str(config->commit_mode));
//                 TEST_SAY("========================================\n\n");
//
//                 /* Initialize and setup */
//                 init_test_context(&ctx);
//                 setup_topics_and_produce(&ctx, config->topic_cnt,
//                                          config->partitions,
//                                          config->msgs_per_partition);
//
//                 /* Create consumers:
//                  * C1 with explicit ack mode (will call acknowledge()
//                  explicitly)
//                  * C2 with implicit ack mode (default behavior) */
//                 c1 = create_explicit_ack_consumer("171");
//                 c2 = create_implicit_ack_consumer("171");
//
//                 /* Set group offset to earliest (only need to do once) */
//                 set_group_offset_earliest(c1, ctx.group_id);
//
//                 /* Subscribe both consumers */
//                 subscribe_consumer(c1, ctx.topic_names, ctx.topic_cnt);
//                 subscribe_consumer(c2, ctx.topic_names, ctx.topic_cnt);
//
//                 /* C1: Consume and acknowledge */O
//                 consume_and_acknowledge(c1, "C1", 30, config->ack_count,
//                                         tracked_msgs, &tracked_cnt);
//
//                 /* Execute commit */
//                 perform_commit(c1, "C1", config->commit_mode);
//
//                 /* Close C1 */
//                 TEST_SAY("C1: Closing consumer\n");
//                 rd_kafka_share_consumer_close(c1);
//                 TEST_SAY("C1: Closed successfully\n");
//                 rd_kafka_share_destroy(c1);
//
//                 /* C2: Consume and verify no redelivery */
//                 verify_no_redelivery(c2, "C2", 30,
//                                      ctx.total_msgs_produced - tracked_cnt,
//                                      tracked_msgs, tracked_cnt,
//                                      config->commit_mode);
//
//                 /* Close C2 */
//                 TEST_SAY("C2: Closing consumer\n");
//                 rd_kafka_share_consumer_close(c2);
//                 TEST_SAY("C2: Closed successfully\n");
//                 rd_kafka_share_destroy(c2);
//
//
//                 /* Cleanup */
//                 free_tracked_messages(tracked_msgs, tracked_cnt);
//
//                 TEST_SAY("Test %s completed successfully\n\n",
//                          config->test_name);
//         }
// }

int main_0178_share_consumer_close(int argc, char **argv) {
        /* Set overall timeout for all tests */
        test_timeout_set(600); /* 10 minutes */
        TEST_SKIP("Implementation is in progress");

        // test_close_with_acknowledge();

        // test_close_without_acknowledge();

        return 0;
}
