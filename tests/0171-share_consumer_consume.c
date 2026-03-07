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
 * @brief Maximum supported values for test configuration
 */
#define MAX_CONSUMERS  16
#define MAX_TOPICS     16
#define MAX_PARTITIONS 32
#define BATCH_SIZE     10000

/**
 * @brief Test configuration structure
 *
 * This structure defines all parameters for a share consumer test scenario.
 *
 * Example configurations:
 *
 * 1. Multiple consumers, single topic, multiple partitions:
 *    { .consumer_cnt = 2, .topic_cnt = 1, .partitions = {3},
 *      .msgs_per_partition = 100, .group_name = "test-group" }
 *
 * 2. Single consumer, multiple topics, single partition each:
 *    { .consumer_cnt = 1, .topic_cnt = 2, .partitions = {1, 1},
 *      .msgs_per_partition = 100, .group_name = "test-group" }
 *
 * 3. Multiple consumers, multiple topics, multiple partitions:
 *    { .consumer_cnt = 3, .topic_cnt = 2, .partitions = {3, 2},
 *      .msgs_per_partition = 50, .group_name = "test-group" }
 */
typedef struct {
        int consumer_cnt;           /**< Number of consumers to create */
        int topic_cnt;              /**< Number of topics to create */
        int partitions[MAX_TOPICS]; /**< Partition count for each topic */
        int msgs_per_partition;     /**< Messages to produce per partition */
        const char *group_name;     /**< Share group name */
        const char *test_name;      /**< Test description for logging */
        int poll_timeout_ms; /**< Timeout for each poll (default: 3000) */
        int max_attempts; /**< Max poll attempts (default: auto-calculated) */
} share_test_config_t;

/**
 * @brief Test state/results structure
 */
typedef struct {
        rd_kafka_share_t *consumers[MAX_CONSUMERS];
        char *topic_names[MAX_TOPICS];
        int per_consumer_count[MAX_CONSUMERS];
        int total_consumed;
        int total_expected;
} share_test_state_t;

/**
 * @brief Create share consumers
 */
static void create_share_consumers(share_test_config_t *config,
                                   share_test_state_t *state) {
        int i;

        for (i = 0; i < config->consumer_cnt; i++) {
                state->consumers[i] =
                    test_create_share_consumer(config->group_name);
        }

        TEST_SAY("Created %d share consumer(s)\n", config->consumer_cnt);
}

/**
 * @brief Create topics and produce messages
 */
static void setup_topics_and_produce(share_test_config_t *config,
                                     share_test_state_t *state) {
        int t, p;
        int total_partitions = 0;

        state->total_expected = 0;

        for (t = 0; t < config->topic_cnt; t++) {
                /* Generate unique topic name */
                state->topic_names[t] =
                    rd_strdup(test_mk_topic_name("0171-share-test", 1));

                /* Create topic with specified partitions */
                test_create_topic_wait_exists(NULL, state->topic_names[t],
                                              config->partitions[t], -1,
                                              60 * 1000);

                /* Produce messages to each partition */
                for (p = 0; p < config->partitions[t]; p++) {
                        test_produce_msgs_easy(state->topic_names[t], p, p,
                                               config->msgs_per_partition);
                        state->total_expected += config->msgs_per_partition;
                }

                total_partitions += config->partitions[t];
                TEST_SAY("Topic '%s': %d partition(s), %d msgs/partition\n",
                         state->topic_names[t], config->partitions[t],
                         config->msgs_per_partition);
        }

        TEST_SAY(
            "Setup complete: %d topic(s), %d total partition(s), "
            "%d total messages\n",
            config->topic_cnt, total_partitions, state->total_expected);
}

/**
 * @brief Subscribe consumers to topics
 */
static void subscribe_consumers(share_test_config_t *config,
                                share_test_state_t *state) {
        rd_kafka_topic_partition_list_t *subs;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        int t, i;

        /* Set group config using first consumer */
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(state->consumers[0]),
            RD_KAFKA_RESOURCE_GROUP, config->group_name, grp_conf, 1);

        /* Build subscription list */
        subs = rd_kafka_topic_partition_list_new(config->topic_cnt);
        for (t = 0; t < config->topic_cnt; t++) {
                rd_kafka_topic_partition_list_add(subs, state->topic_names[t],
                                                  RD_KAFKA_PARTITION_UA);
        }

        /* Subscribe all consumers */
        for (i = 0; i < config->consumer_cnt; i++) {
                rd_kafka_share_subscribe(state->consumers[i], subs);
        }

        rd_kafka_topic_partition_list_destroy(subs);

        TEST_SAY("Subscribed %d consumer(s) to %d topic(s)\n",
                 config->consumer_cnt, config->topic_cnt);
}

/**
 * @brief Consume messages function
 */
static void consume_messages(share_test_config_t *config,
                             share_test_state_t *state) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int attempts;
        int poll_timeout;
        int i;

        /* Initialize per-consumer counts */
        for (i = 0; i < config->consumer_cnt; i++) {
                state->per_consumer_count[i] = 0;
        }
        state->total_consumed = 0;

        /* Calculate defaults if not specified */
        poll_timeout =
            config->poll_timeout_ms > 0 ? config->poll_timeout_ms : 3000;

        /* Auto-calculate attempts based on expected messages and consumers */
        if (config->max_attempts > 0) {
                attempts = config->max_attempts;
        } else {
                /* Heuristic: at least 10 attempts, or based on message count */
                attempts = (state->total_expected / (BATCH_SIZE / 2)) + 10;
                if (attempts < 20)
                        attempts = 20;
        }

        TEST_SAY(
            "Starting consumption: expecting %d messages, "
            "max %d attempts, %dms timeout\n",
            state->total_expected, attempts, poll_timeout);

        while (state->total_consumed < state->total_expected &&
               attempts-- > 0) {
                /* Round-robin poll across all consumers */
                for (i = 0; i < config->consumer_cnt; i++) {
                        size_t rcvd = 0;
                        size_t m;
                        rd_kafka_error_t *err;

                        err = rd_kafka_share_consume_batch(
                            state->consumers[i], poll_timeout, batch, &rcvd);

                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }

                        for (m = 0; m < rcvd; m++) {
                                if (!batch[m]->err) {
                                        state->per_consumer_count[i]++;
                                        state->total_consumed++;
                                }
                                rd_kafka_message_destroy(batch[m]);
                        }

                        /* Early exit if we've consumed everything */
                        if (state->total_consumed >= state->total_expected)
                                break;
                }

                /* Progress logging */
                if (config->consumer_cnt == 1) {
                        TEST_SAY("Progress: %d/%d\n", state->total_consumed,
                                 state->total_expected);
                } else {
                        /* Build distribution string for multiple consumers */
                        char dist[256] = {0};
                        int pos        = 0;
                        for (i = 0; i < config->consumer_cnt && pos < 250;
                             i++) {
                                pos += rd_snprintf(
                                    dist + pos, sizeof(dist) - pos, "c%d=%d ",
                                    i, state->per_consumer_count[i]);
                        }
                        TEST_SAY("Progress: %d/%d (%s)\n",
                                 state->total_consumed, state->total_expected,
                                 dist);
                }
        }
}

/**
 * @brief Cleanup function
 */
static void cleanup_test(share_test_config_t *config,
                         share_test_state_t *state) {
        int t, i;

        /* Delete topics using first consumer */
        for (t = 0; t < config->topic_cnt; t++) {
                if (state->topic_names[t]) {
                        test_delete_topic(
                            test_share_consumer_get_rk(state->consumers[0]),
                            state->topic_names[t]);
                        rd_free(state->topic_names[t]);
                        state->topic_names[t] = NULL;
                }
        }

        /* Destroy consumers */
        for (i = 0; i < config->consumer_cnt; i++) {
                if (state->consumers[i]) {
                        rd_kafka_share_consumer_close(state->consumers[i]);
                        rd_kafka_share_destroy(state->consumers[i]);
                        state->consumers[i] = NULL;
                }
        }

        TEST_SAY("Cleanup complete\n");
}

/**
 * @brief Test Runner function
 *
 * This function handles:
 * 1. Create consumers
 * 2. Create topics and produce messages
 * 3. Subscribe consumers
 * 4. Consume and verify messages
 * 5. Cleanup
 *
 * @param config Test configuration
 * @returns 0 on success, -1 on failure
 */
static int run_share_consumer_test(share_test_config_t *config) {
        share_test_state_t state = {0};
        int i;
        char dist_str[512] = {0};
        int pos            = 0;

        /* Validate config */
        TEST_ASSERT(config->consumer_cnt > 0 &&
                        config->consumer_cnt <= MAX_CONSUMERS,
                    "consumer_cnt must be 1-%d", MAX_CONSUMERS);
        TEST_ASSERT(config->topic_cnt > 0 && config->topic_cnt <= MAX_TOPICS,
                    "topic_cnt must be 1-%d", MAX_TOPICS);
        TEST_ASSERT(config->group_name != NULL, "group_name is required");

        /* Print test header */
        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("=== %s ===\n",
                 config->test_name ? config->test_name : "Share Consumer Test");
        TEST_SAY("=== Consumers: %d, Topics: %d, Partitions: [",
                 config->consumer_cnt, config->topic_cnt);
        for (i = 0; i < config->topic_cnt; i++) {
                TEST_SAY("%d%s", config->partitions[i],
                         i < config->topic_cnt - 1 ? ", " : "");
        }
        TEST_SAY("], Msgs/Partition: %d ===\n", config->msgs_per_partition);
        TEST_SAY(
            "============================================================\n");

        /* Execute test phases */
        create_share_consumers(config, &state);
        setup_topics_and_produce(config, &state);
        subscribe_consumers(config, &state);
        consume_messages(config, &state);

        /* Verify results */
        TEST_ASSERT(state.total_consumed == state.total_expected,
                    "Expected %d messages, consumed %d", state.total_expected,
                    state.total_consumed);

        /* Build distribution string */
        for (i = 0; i < config->consumer_cnt && pos < 500; i++) {
                pos += rd_snprintf(dist_str + pos, sizeof(dist_str) - pos,
                                   "c%d=%d ", i, state.per_consumer_count[i]);
        }

        TEST_SAY("✓ SUCCESS: Consumed %d/%d messages (distribution: %s)\n",
                 state.total_consumed, state.total_expected, dist_str);

        /* Cleanup */
        cleanup_test(config, &state);

        return 0;
}


/**
 * @brief Single consumer, single topic, single partition
 */
static void test_single_consumer_single_topic_single_partition(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 1000,
            .group_name         = "share-1c-1t-1p",
            .test_name = "Single consumer, single topic, single partition"};
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, single topic, multiple partitions
 */
static void test_single_consumer_single_topic_multiple_partitions(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {3},
            .msgs_per_partition = 500,
            .group_name         = "share-1c-1t-3p",
            .test_name = "Single consumer, single topic, 3 partitions"};
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, multiple topics, single partition each
 */
static void test_single_consumer_multiple_topic_single_partition(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 2,
            .partitions         = {1, 1},
            .msgs_per_partition = 500,
            .group_name         = "share-1c-2t-1p",
            .test_name = "Single consumer, 2 topics, 1 partition each"};
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, multiple topics, multiple partitions
 */
static void test_single_consumer_multiple_topic_multiple_partitions(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 3,
            .partitions         = {2, 2, 2},
            .msgs_per_partition = 500,
            .group_name         = "share-1c-3t-2p",
            .test_name = "Single consumer, 3 topics, 2 partitions each"};
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, single topic, single partition
 */
static void test_multiple_consumers_single_topic_single_partition(void) {
        share_test_config_t config = {
            .consumer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 1000,
            .group_name         = "share-2c-1t-1p",
            .test_name = "2 consumers, single topic, single partition"};
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, single topic, multiple partitions
 */
static void test_multiple_consumers_single_topic_multiple_partitions(void) {
        share_test_config_t config = {
            .consumer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 500,
            .group_name         = "share-2c-1t-4p",
            .test_name          = "2 consumers, single topic, 4 partitions"};
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, multiple topics, multiple partitions
 */
static void test_multiple_consumers_multiple_topics_multiple_partitions(void) {
        share_test_config_t config = {
            .consumer_cnt       = 3,
            .topic_cnt          = 2,
            .partitions         = {3, 3},
            .msgs_per_partition = 500,
            .group_name         = "share-3c-2t-3p",
            .test_name          = "3 consumers, 2 topics, 3 partitions each"};
        run_share_consumer_test(&config);
}

/***************************************************************************
 * High Volume Tests
 ***************************************************************************/

/**
 * @brief High volume test with 10k messages on single partition
 */
static void test_high_volume_10k_messages(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 10000,
            .group_name         = "share-highvol-10k",
            .test_name          = "High volume: 10k messages, 1 partition",
            .max_attempts       = 100};
        run_share_consumer_test(&config);
}

/**
 * @brief High volume test with 50k messages across 5 partitions
 */
static void test_high_volume_50k_multi_partition(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {5},
            .msgs_per_partition = 10000,
            .group_name         = "share-highvol-50k",
            .test_name          = "High volume: 50k messages, 5 partitions",
            .max_attempts       = 150};
        run_share_consumer_test(&config);
}

/***************************************************************************
 * Multi-Topic Tests (Triggers Multiple Fetch Responses)
 ***************************************************************************/

/**
 * @brief 15 topics to trigger multiple ShareFetch responses
 */
static void test_many_topics_15(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 15,
            .partitions         = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            .msgs_per_partition = 100,
            .group_name         = "share-15topics",
            .test_name          = "15 topics, 1 partition each (1500 msgs)",
            .max_attempts       = 100};
        run_share_consumer_test(&config);
}

/**
 * @brief 10 topics with 2 partitions each
 */
static void test_many_topics_10_multi_partition(void) {
        share_test_config_t config = {
            .consumer_cnt       = 1,
            .topic_cnt          = 10,
            .partitions         = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
            .msgs_per_partition = 100,
            .group_name         = "share-10topics-2p",
            .test_name          = "10 topics, 2 partitions each (2000 msgs)",
            .max_attempts       = 100};
        run_share_consumer_test(&config);
}

/***************************************************************************
 * Rapid Produce/Consume Tests
 ***************************************************************************/

/**
 * @brief Rapid produce/consume cycles - 20 rounds of 500 messages each
 */
static void test_rapid_produce_consume_cycles(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const char *topic;
        const char *group = "share-rapid-cycles";
        rd_kafka_topic_partition_list_t *subs;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        int round, total_consumed = 0;
        const int rounds         = 20;
        const int msgs_per_round = 500;
        const int total_expected = rounds * msgs_per_round;

        TEST_SAY("\n");
        TEST_SAY("=== Rapid produce/consume cycles: %d rounds x %d msgs ===\n",
                 rounds, msgs_per_round);

        /* Create consumer and topic */
        consumer = test_create_share_consumer(group);
        topic    = test_mk_topic_name("0171-rapid-cycles", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Configure group */
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(consumer), RD_KAFKA_RESOURCE_GROUP,
            group, grp_conf, 1);

        /* Subscribe */
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Rapid cycles */
        for (round = 0; round < rounds; round++) {
                int round_consumed = 0;
                int attempts       = 100;

                /* Produce */
                test_produce_msgs_easy(topic, 0, 0, msgs_per_round);

                /* Consume */
                while (round_consumed < msgs_per_round && attempts-- > 0) {
                        size_t rcvd = 0;
                        size_t m;
                        rd_kafka_error_t *err;

                        err = rd_kafka_share_consume_batch(consumer, 1000,
                                                           batch, &rcvd);
                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }

                        for (m = 0; m < rcvd; m++) {
                                if (!batch[m]->err)
                                        round_consumed++;
                                rd_kafka_message_destroy(batch[m]);
                        }
                }

                total_consumed += round_consumed;
                TEST_SAY("Round %d: consumed %d/%d (total: %d/%d)\n", round + 1,
                         round_consumed, msgs_per_round, total_consumed,
                         (round + 1) * msgs_per_round);
        }

        TEST_ASSERT(total_consumed == total_expected,
                    "Expected %d messages, consumed %d", total_expected,
                    total_consumed);

        TEST_SAY("SUCCESS: Rapid cycles completed - %d messages\n",
                 total_consumed);

        /* Cleanup */
        test_delete_topic(test_share_consumer_get_rk(consumer), topic);
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
}

/**
 * @brief Empty topic then produce - verify consumer handles transition
 */
static void test_empty_then_produce(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const char *topic;
        const char *group = "share-empty-then-produce";
        rd_kafka_topic_partition_list_t *subs;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        int consumed           = 0, attempts;

        TEST_SAY("\n");
        TEST_SAY("=== Empty topic then produce test ===\n");

        /* Create consumer and empty topic */
        consumer = test_create_share_consumer(group);
        topic    = test_mk_topic_name("0171-empty-then-produce", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Configure and subscribe */
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(consumer), RD_KAFKA_RESOURCE_GROUP,
            group, grp_conf, 1);
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Poll empty topic - should return 0 */
        TEST_SAY("Polling empty topic...\n");
        for (attempts = 0; attempts < 5; attempts++) {
                size_t rcvd = 0;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 1000, batch, &rcvd);
                if (err)
                        rd_kafka_error_destroy(err);
                TEST_SAY("Empty poll %d: received %zu\n", attempts + 1, rcvd);
        }

        /* Now produce messages */
        TEST_SAY("Producing 100 messages...\n");
        test_produce_msgs_easy(topic, 0, 0, 100);

        /* Consume - should get messages now */
        attempts = 100;
        while (consumed < 100 && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 1000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err)
                                consumed++;
                        rd_kafka_message_destroy(batch[m]);
                }
        }

        TEST_ASSERT(consumed == 100, "Expected 100 messages, consumed %d",
                    consumed);
        TEST_SAY("SUCCESS: Empty then produce - consumed %d messages\n",
                 consumed);

        /* Cleanup */
        test_delete_topic(test_share_consumer_get_rk(consumer), topic);
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
}

/**
 * @brief Sparse partitions - produce only to some partitions
 */
static void test_sparse_partitions(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const char *topic;
        const char *group = "share-sparse-partitions";
        rd_kafka_topic_partition_list_t *subs;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        int consumed           = 0, attempts;
        const int msgs_per_partition = 100;
        const int expected = 3 * msgs_per_partition; /* partitions 0,2,4 */

        TEST_SAY("\n");
        TEST_SAY(
            "=== Sparse partitions test (5 partitions, produce to 0,2,4) "
            "===\n");

        /* Create consumer and topic with 5 partitions */
        consumer = test_create_share_consumer(group);
        topic    = test_mk_topic_name("0171-sparse-partitions", 1);
        test_create_topic_wait_exists(NULL, topic, 5, -1, 60 * 1000);

        /* Configure and subscribe */
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(consumer), RD_KAFKA_RESOURCE_GROUP,
            group, grp_conf, 1);
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Produce only to partitions 0, 2, 4 (skip 1, 3) */
        test_produce_msgs_easy(topic, 0, 0, msgs_per_partition);
        test_produce_msgs_easy(topic, 0, 2, msgs_per_partition);
        test_produce_msgs_easy(topic, 0, 4, msgs_per_partition);

        TEST_SAY("Produced %d messages to partitions 0, 2, 4\n", expected);

        /* Consume all */
        attempts = 100;
        while (consumed < expected && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 1000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err)
                                consumed++;
                        rd_kafka_message_destroy(batch[m]);
                }

                TEST_SAY("Progress: %d/%d\n", consumed, expected);
        }

        TEST_ASSERT(consumed == expected, "Expected %d messages, consumed %d",
                    expected, consumed);
        TEST_SAY("SUCCESS: Sparse partitions - consumed %d messages\n",
                 consumed);

        /* Cleanup */
        test_delete_topic(test_share_consumer_get_rk(consumer), topic);
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
}


int main_0171_share_consumer_consume(int argc, char **argv) {

        /* Single-consumer tests */
        test_single_consumer_single_topic_single_partition();      /* Single
                                                                      consumer,
                                                                      single topic,
                                                                      single
                                                                      partition */
        test_single_consumer_single_topic_multiple_partitions();   /* Single
                                                                      consumer,
                                                                      single
                                                                      topic, multi
                                                                      partitions
                                                                    */
        test_single_consumer_multiple_topic_single_partition();    /* Single
                                                                      consumer,
                                                                      multi topic,
                                                                      single
                                                                      partition
                                                                      each */
        test_single_consumer_multiple_topic_multiple_partitions(); /* Single
                                                                      consumer,
                                                                      multi
                                                                      topics,
                                                                      multi
                                                                      partitions
                                                                      each */

        // /* Multi-consumer tests */
        test_multiple_consumers_single_topic_single_partition();       /* Multi
                                                                          consumer
                                                                          sharing
                                                                          single
                                                                          partition */
        test_multiple_consumers_single_topic_multiple_partitions();    /* Multi
                                                                          consumer,
                                                                          multi
                                                                          partition
                                                                        */
        test_multiple_consumers_multiple_topics_multiple_partitions(); /* Full
                                                                          matrix:
                                                                          multi
                                                                          everything
                                                                        */

        // /* High volume tests */
        test_high_volume_10k_messages();
        test_high_volume_50k_multi_partition();

        // /* Multi-topic tests (triggers multiple fetch responses) */
        test_many_topics_15();
        test_many_topics_10_multi_partition();

        /* Rapid produce/consume tests */
        test_rapid_produce_consume_cycles();

        /* Edge case tests */
        test_empty_then_produce();
        test_sparse_partitions();

        return 0;
}
