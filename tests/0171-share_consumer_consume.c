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

/** Common producer reused across all tests. */
static rd_kafka_t *common_producer;

/** Common admin client reused across all tests. */
static rd_kafka_t *common_admin;

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
                    test_create_share_consumer(config->group_name, NULL);
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
                        test_produce_msgs_simple(common_producer,
                                                 state->topic_names[t], p,
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
        int t, i;

        /* Set group config using a dedicated admin client */
        test_share_set_auto_offset_reset(config->group_name, "earliest");

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

        for (t = 0; t < config->topic_cnt; t++) {
                if (state->topic_names[t]) {
                        rd_free(state->topic_names[t]);
                        state->topic_names[t] = NULL;
                }
        }

        /* Destroy consumers */
        for (i = 0; i < config->consumer_cnt; i++) {
                if (state->consumers[i]) {
                        test_share_consumer_close(state->consumers[i]);
                        test_share_destroy(state->consumers[i]);
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
        int round, total_consumed = 0;
        const int rounds         = 20;
        const int msgs_per_round = 500;
        const int total_expected = rounds * msgs_per_round;

        TEST_SAY("\n");
        TEST_SAY("=== Rapid produce/consume cycles: %d rounds x %d msgs ===\n",
                 rounds, msgs_per_round);

        /* Create consumer and topic */
        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-rapid-cycles", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Configure group */
        test_share_set_auto_offset_reset(group, "earliest");

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
                test_produce_msgs_simple(common_producer, topic, 0,
                                         msgs_per_round);

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

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
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
        int consumed = 0, attempts;

        TEST_SAY("\n");
        TEST_SAY("=== Empty topic then produce test ===\n");

        /* Create consumer and empty topic */
        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-empty-then-produce", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Configure and subscribe */
        test_share_set_auto_offset_reset(group, "earliest");
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
        test_produce_msgs_simple(common_producer, topic, 0, 100);

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

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
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
        int consumed                 = 0, attempts;
        const int msgs_per_partition = 100;
        const int expected = 3 * msgs_per_partition; /* partitions 0,2,4 */

        TEST_SAY("\n");
        TEST_SAY(
            "=== Sparse partitions test (5 partitions, produce to 0,2,4) "
            "===\n");

        /* Create consumer and topic with 5 partitions */
        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-sparse-partitions", 1);
        test_create_topic_wait_exists(NULL, topic, 5, -1, 60 * 1000);

        /* Configure and subscribe */
        test_share_set_auto_offset_reset(group, "earliest");
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Produce only to partitions 0, 2, 4 (skip 1, 3) */
        test_produce_msgs_simple(common_producer, topic, 0, msgs_per_partition);
        test_produce_msgs_simple(common_producer, topic, 2, msgs_per_partition);
        test_produce_msgs_simple(common_producer, topic, 4, msgs_per_partition);

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

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Poll callback - piggybacked acks on ShareFetch.
 *
 * In implicit mode, acks are automatically sent with the next poll().
 * This test verifies that the callback is invoked when acks are
 * piggybacked on ShareFetch responses.
 */
static void test_poll_callback_piggybacked_acks(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const char *topic;
        const char *group = "share-poll-callback";
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_conf_t *conf;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        char errstr[512];
        int consumed              = 0, attempts;
        test_ack_cb_state_t state = {0};

        TEST_SAY("\n");
        TEST_SAY(
            "=== Poll callback test (piggybacked acks on ShareFetch) ===\n");

        /* Create consumer with callback */
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "share.acknowledgement.mode", "implicit",
                          errstr, sizeof(errstr));
        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf,
                                                          test_share_ack_cb);
        rd_kafka_conf_set_opaque(conf, &state);

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create share consumer: %s", errstr);

        /* Create topic and produce messages */
        topic = test_mk_topic_name("0171-poll-cb", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_simple(common_producer, topic, 0, 100);

        /* Configure group */
        test_alter_group_configurations(group, grp_conf, 1);

        /* Subscribe */
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* First poll: consume messages (implicit mode auto-acks) */
        attempts = 50;
        while (consumed < 50 && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 2000, batch, &rcvd);
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

        TEST_SAY("Consumed %d messages\n", consumed);
        TEST_ASSERT(consumed > 0, "Expected to consume some messages");

        /* Second poll: this sends piggybacked acks from first batch */
        attempts = 10;
        while (attempts-- > 0 && state.callback_cnt == 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 2000, batch, &rcvd);
                if (err)
                        rd_kafka_error_destroy(err);

                for (m = 0; m < rcvd; m++)
                        rd_kafka_message_destroy(batch[m]);
        }

        TEST_SAY("Callback count=%d, total_offsets=%zu, last_err=%s\n",
                 state.callback_cnt, state.total_offsets,
                 rd_kafka_err2name(state.last_err));

        TEST_ASSERT(
            state.callback_cnt >= 1,
            "Expected at least 1 callback from piggybacked acks, got %d",
            state.callback_cnt);
        TEST_ASSERT(state.total_offsets > 0,
                    "Expected offsets in callback, got %zu",
                    state.total_offsets);

        TEST_SAY("SUCCESS: Poll callback received %d callbacks, %zu offsets\n",
                 state.callback_cnt, state.total_offsets);

        /* Cleanup */
        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Test that record headers are preserved end-to-end through the
 *        share consumer.
 *
 * Produce a single record with three headers and verify the share consumer
 * receives all of them with the correct values.
 */
static void test_headers_preserved(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[10];
        const char *topic;
        const char *group = "share-headers";
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;
        size_t rcvd = 0;
        size_t i;
        int attempts;
        rd_bool_t verified = rd_false;

        TEST_SAY("\n");
        TEST_SAY("=== Headers preserved through share consumer ===\n");

        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-headers", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Produce a single record with three headers */
        err = rd_kafka_producev(
            common_producer, RD_KAFKA_V_TOPIC(topic),
            RD_KAFKA_V_KEY("hdr-key", 7), RD_KAFKA_V_VALUE("hdr-value", 9),
            RD_KAFKA_V_HEADER("h1", "v1", 2), RD_KAFKA_V_HEADER("h2", "v2", 2),
            RD_KAFKA_V_HEADER("h3", "v3", 2), RD_KAFKA_V_END);
        TEST_ASSERT(!err, "producev failed: %s", rd_kafka_err2name(err));
        rd_kafka_flush(common_producer, 10 * 1000);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 30;
        while (rcvd < 1 && attempts-- > 0) {
                size_t batch_rcvd = 0;
                rd_kafka_error_t *e;

                e = rd_kafka_share_consume_batch(consumer, 2000, batch + rcvd,
                                                 &batch_rcvd);
                if (e)
                        rd_kafka_error_destroy(e);
                rcvd += batch_rcvd;
        }
        TEST_ASSERT(rcvd == 1, "Expected 1 record, got %zu", rcvd);

        for (i = 0; i < rcvd; i++) {
                rd_kafka_headers_t *hdrs = NULL;
                rd_kafka_resp_err_t herr;
                const void *val;
                size_t vsz;

                herr = rd_kafka_message_headers(batch[i], &hdrs);
                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR && hdrs,
                            "Failed to get headers: %s",
                            rd_kafka_err2name(herr));

                herr = rd_kafka_header_get_last(hdrs, "h1", &val, &vsz);
                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "h1 not present: %s", rd_kafka_err2name(herr));
                TEST_ASSERT(vsz == 2 && memcmp(val, "v1", 2) == 0,
                            "h1 value mismatch");

                herr = rd_kafka_header_get_last(hdrs, "h2", &val, &vsz);
                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "h2 not present: %s", rd_kafka_err2name(herr));
                TEST_ASSERT(vsz == 2 && memcmp(val, "v2", 2) == 0,
                            "h2 value mismatch");

                herr = rd_kafka_header_get_last(hdrs, "h3", &val, &vsz);
                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "h3 not present: %s", rd_kafka_err2name(herr));
                TEST_ASSERT(vsz == 2 && memcmp(val, "v3", 2) == 0,
                            "h3 value mismatch");

                verified = rd_true;
                rd_kafka_message_destroy(batch[i]);
        }

        TEST_ASSERT(verified, "No record was verified");

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);

        TEST_SAY("SUCCESS: Headers preserved through share consumer\n");
}


/**
 * @brief Test that a share consumer with a very small
 *        fetch.message.max.bytes still consumes all records.
 *
 * Mirrors 0036-partial_fetch for the share path: produce 100 records of
 * 1000 bytes each, set fetch.message.max.bytes=1500 (~1.5 messages per
 * fetch). Verify all 100 records are received.
 */
static void test_fetch_max_bytes_small(void) {
        const char *group = "share-small-fetch";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        char errstr[512];
        const int msgcnt  = 100;
        const int msgsize = 1000;
        int consumed      = 0;
        int attempts;

        TEST_SAY("\n");
        TEST_SAY(
            "=== Small fetch.message.max.bytes (1500) with 1000-byte msgs "
            "===\n");

        topic = test_mk_topic_name("0171-small-fetch", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Produce msgcnt messages of msgsize bytes each */
        test_produce_msgs2(common_producer, topic, 0, 0, 0, msgcnt, NULL,
                           msgsize);
        rd_kafka_flush(common_producer, 30 * 1000);

        /* Create share consumer with custom fetch.message.max.bytes */
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.message.max.bytes", "1500", errstr,
                          sizeof(errstr));

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create share consumer: %s", errstr);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 200;
        while (consumed < msgcnt && attempts-- > 0) {
                int batch_cnt = 0;
                test_share_consume_batch(consumer, 2000, NULL, 0, &batch_cnt);
                consumed += batch_cnt;
                if (batch_cnt > 0)
                        TEST_SAY("Progress: %d/%d\n", consumed, msgcnt);
        }

        TEST_ASSERT(consumed == msgcnt,
                    "Expected %d records with small fetch.max.bytes, "
                    "consumed %d",
                    msgcnt, consumed);

        TEST_SAY("SUCCESS: small fetch.max.bytes - consumed %d records\n",
                 consumed);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Test that a share consumer can consume a single record larger
 *        than fetch.message.max.bytes.
 *
 * Mirrors 0041-fetch_max_bytes for the share path: produce small + large
 * records, set fetch.message.max.bytes well below the large record size,
 * and verify all records are still received (the consumer must grow the
 * buffer to fit the oversized record).
 */
static void test_record_larger_than_fetch_max_bytes(void) {
        const char *group = "share-large-record";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        char errstr[512];
        const int MAX_BYTES = 100000;
        const int small_cnt = 50;
        const int large_cnt = 10;
        const int total     = small_cnt + large_cnt;
        int consumed        = 0;
        int attempts;

        TEST_SAY("\n");
        TEST_SAY("=== Records larger than fetch.message.max.bytes (%d) ===\n",
                 MAX_BYTES);

        topic = test_mk_topic_name("0171-large-record", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Produce small messages then large ones */
        test_produce_msgs2(common_producer, topic, 0, 0, 0, small_cnt, NULL,
                           MAX_BYTES / 10);
        test_produce_msgs2(common_producer, topic, 0, 0, small_cnt, large_cnt,
                           NULL, MAX_BYTES * 5);
        rd_kafka_flush(common_producer, 60 * 1000);

        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));

        char val[32];
        rd_snprintf(val, sizeof(val), "%d", MAX_BYTES);
        rd_kafka_conf_set(conf, "fetch.message.max.bytes", val, errstr,
                          sizeof(errstr));

        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create share consumer: %s", errstr);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 200;
        while (consumed < total && attempts-- > 0) {
                int batch_cnt = 0;
                test_share_consume_batch(consumer, 3000, NULL, 0, &batch_cnt);
                consumed += batch_cnt;
                if (batch_cnt > 0)
                        TEST_SAY("Progress: %d/%d\n", consumed, total);
        }

        TEST_ASSERT(consumed == total,
                    "Expected %d records (incl. oversized), consumed %d", total,
                    consumed);

        TEST_SAY("SUCCESS: oversized record - consumed %d records\n", consumed);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Test that records held by a consumer that closes mid-flight
 *        are released back to other consumers in the group.
 *
 * Create 4 share consumers in the same group, produce a known set of
 * records, have consumer 0 fetch a batch without acknowledging and then
 * close. The other consumers should eventually receive every record
 * (including the ones released by consumer 0).
 */
static void test_consumer_close_releases_to_group(void) {
        const char *group = "share-close-releases";
        const char *topic;
        rd_kafka_share_t *consumers[4];
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const int total_msgs = 200;
        int c0_fetched       = 0;
        int total_consumed   = 0;
        int per_consumer[4]  = {0};
        int attempts;
        int i;

        TEST_SAY("\n");
        TEST_SAY(
            "=== Failing consumer releases unacked records to group ===\n");

        topic = test_mk_topic_name("0171-close-release", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_simple(common_producer, topic, 0, total_msgs);

        test_share_set_auto_offset_reset(group, "earliest");

        for (i = 0; i < 4; i++)
                consumers[i] = test_create_share_consumer(group, NULL);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        for (i = 0; i < 4; i++)
                rd_kafka_share_subscribe(consumers[i], subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consumer 0 fetches a batch WITHOUT acknowledging, then closes. */
        attempts = 30;
        while (c0_fetched == 0 && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err = rd_kafka_share_consume_batch(consumers[0], 2000, batch,
                                                   &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err)
                                c0_fetched++;
                        rd_kafka_message_destroy(batch[m]);
                }
        }
        TEST_ASSERT(c0_fetched > 0,
                    "Consumer 0 should have fetched some records, got %d",
                    c0_fetched);

        TEST_SAY("Consumer 0 fetched %d records (unacked), closing now\n",
                 c0_fetched);
        /* Close without acknowledging - records should be released */
        test_share_consumer_close(consumers[0]);
        test_share_destroy(consumers[0]);
        consumers[0] = NULL;

        /* Drain the rest with consumers 1..3 */
        attempts = 200;
        while (total_consumed < total_msgs && attempts-- > 0) {
                for (i = 1; i < 4; i++) {
                        size_t rcvd = 0;
                        size_t m;
                        rd_kafka_error_t *err;

                        err = rd_kafka_share_consume_batch(consumers[i], 1000,
                                                           batch, &rcvd);
                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }

                        for (m = 0; m < rcvd; m++) {
                                if (!batch[m]->err) {
                                        per_consumer[i]++;
                                        total_consumed++;
                                }
                                rd_kafka_message_destroy(batch[m]);
                        }
                }
        }

        TEST_SAY(
            "Distribution: c0=%d (lost on close), c1=%d, c2=%d, c3=%d "
            "(total %d)\n",
            c0_fetched, per_consumer[1], per_consumer[2], per_consumer[3],
            total_consumed);

        TEST_ASSERT(total_consumed >= total_msgs,
                    "Expected at least %d records on the survivors after "
                    "consumer 0's records were released, got %d",
                    total_msgs, total_consumed);

        for (i = 1; i < 4; i++) {
                test_share_consumer_close(consumers[i]);
                test_share_destroy(consumers[i]);
        }

        TEST_SAY("SUCCESS: closed consumer's records redelivered to group\n");
}


/**
 * @brief Partition count increased during consume.
 *
 * 1 consumer, topic starts with 2 partitions, grown to 5.
 * Produce N records to {0,1} -> consume -> grow to 5 -> produce N to
 * {2,3,4} -> continue consuming -> expect 5*N total.
 */
static void test_partition_increase_during_consume(void) {
        const char *group = "share-partition-increase";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const int msgs_per_partition = 200;
        const int initial_partitions = 2;
        const int grown_partitions   = 5;
        const int initial_expected   = initial_partitions * msgs_per_partition;
        const int total_expected     = grown_partitions * msgs_per_partition;
        int consumed                 = 0;
        int attempts;
        int p;
        rd_kafka_resp_err_t cp_err;

        TEST_SAY("\n");
        TEST_SAY("=== Partition count increase during consume (%d -> %d) ===\n",
                 initial_partitions, grown_partitions);

        topic = test_mk_topic_name("0171-partition-increase", 1);
        test_create_topic_wait_exists(NULL, topic, initial_partitions, -1,
                                      60 * 1000);

        /* Produce to initial partitions only */
        for (p = 0; p < initial_partitions; p++)
                test_produce_msgs_simple(common_producer, topic, p,
                                         msgs_per_partition);

        consumer = test_create_share_consumer(group, NULL);
        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume from the initial partitions */
        attempts = 100;
        while (consumed < initial_expected && attempts-- > 0) {
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
        TEST_SAY("Pre-grow consumed: %d/%d\n", consumed, initial_expected);
        TEST_ASSERT(consumed >= initial_expected,
                    "Expected >= %d msgs from initial partitions, got %d",
                    initial_expected, consumed);

        /* Grow topic partitions from 2 -> 5 */
        TEST_SAY("Growing partitions to %d via CreatePartitions\n",
                 grown_partitions);
        cp_err = test_CreatePartitions_simple(common_admin, NULL, topic,
                                              grown_partitions, NULL);
        TEST_ASSERT(!cp_err, "CreatePartitions failed: %s",
                    rd_kafka_err2str(cp_err));

        /* Force common_producer's metadata cache to refresh and observe the
         * new partition count before producing to the new partitions —
         * otherwise rd_kafka_produce() to partition >= cached count fails
         * with UNKNOWN_PARTITION (producer's default metadata refresh
         * interval is 5 minutes). */
        rd_kafka_metadata_topic_t exp_md = {.topic         = (char *)topic,
                                            .partition_cnt = grown_partitions};
        test_wait_metadata_update(common_producer, &exp_md, 1, NULL, 0,
                                  30 * 1000);

        /* Give the consumer time to learn about the new partitions through
         * metadata refresh and ShareGroupHeartbeat assignment. */
        rd_sleep(5);

        /* Produce to the newly added partitions */
        for (p = initial_partitions; p < grown_partitions; p++)
                test_produce_msgs_simple(common_producer, topic, p,
                                         msgs_per_partition);

        /* Continue consuming, expecting records from new partitions */
        attempts = 200;
        while (consumed < total_expected && attempts-- > 0) {
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

        TEST_ASSERT(consumed >= total_expected,
                    "Expected >= %d total records after partition grow, "
                    "got %d",
                    total_expected, consumed);

        TEST_SAY("SUCCESS: partition count grow %d->%d, consumed %d records\n",
                 initial_partitions, grown_partitions, consumed);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Zero-byte payload and zero-byte key.
 *
 * Verifies that the share consumer correctly preserves zero-length
 * key/value buffers (len == 0, pointer non-NULL) and NULL key/value.
 */
static void test_zero_byte_payload_and_key(void) {
        const char *group = "share-zero-byte";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[16];
        rd_kafka_resp_err_t err;
        size_t rcvd = 0;
        int attempts;
        int saw_empty_kv   = 0;
        int saw_key_only   = 0;
        int saw_value_only = 0;
        int saw_null_kv    = 0;
        size_t i;

        TEST_SAY("\n");
        TEST_SAY("=== Zero-byte payload and key ===\n");

        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-zero-byte", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Empty key, empty value (zero-length, non-NULL pointers) */
        err = rd_kafka_producev(common_producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_KEY("", 0), RD_KAFKA_V_VALUE("", 0),
                                RD_KAFKA_V_END);
        TEST_ASSERT(!err, "produce empty kv failed: %s",
                    rd_kafka_err2name(err));

        /* Non-empty key, empty value */
        err = rd_kafka_producev(common_producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_KEY("k", 1), RD_KAFKA_V_VALUE("", 0),
                                RD_KAFKA_V_END);
        TEST_ASSERT(!err, "produce key-only failed: %s",
                    rd_kafka_err2name(err));

        /* Empty key, non-empty value */
        err = rd_kafka_producev(common_producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_KEY("", 0), RD_KAFKA_V_VALUE("v", 1),
                                RD_KAFKA_V_END);
        TEST_ASSERT(!err, "produce value-only failed: %s",
                    rd_kafka_err2name(err));

        /* NULL key, NULL value */
        err = rd_kafka_producev(common_producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_KEY(NULL, 0),
                                RD_KAFKA_V_VALUE(NULL, 0), RD_KAFKA_V_END);
        TEST_ASSERT(!err, "produce null kv failed: %s", rd_kafka_err2name(err));

        rd_kafka_flush(common_producer, 10 * 1000);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 30;
        while (rcvd < 4 && attempts-- > 0) {
                size_t batch_rcvd = 0;
                rd_kafka_error_t *e;

                e = rd_kafka_share_consume_batch(consumer, 2000, batch + rcvd,
                                                 &batch_rcvd);
                if (e)
                        rd_kafka_error_destroy(e);
                rcvd += batch_rcvd;
        }

        TEST_ASSERT(rcvd == 4, "Expected 4 records, got %zu", rcvd);

        for (i = 0; i < rcvd; i++) {
                rd_bool_t key_null  = batch[i]->key == NULL;
                rd_bool_t key_empty = batch[i]->key_len == 0 && !key_null;
                rd_bool_t val_null  = batch[i]->payload == NULL;
                rd_bool_t val_empty = batch[i]->len == 0 && !val_null;

                /* Brokers may normalize empty buffers to NULL; accept either.
                 */
                rd_bool_t key_is_empty_or_null = key_null || key_empty;
                rd_bool_t val_is_empty_or_null = val_null || val_empty;

                if (key_is_empty_or_null && val_is_empty_or_null) {
                        /* Could be "empty kv" or "null kv" record - count both
                         * as the "no content" class. */
                        if (key_null && val_null)
                                saw_null_kv++;
                        else
                                saw_empty_kv++;
                } else if (!key_is_empty_or_null && val_is_empty_or_null) {
                        TEST_ASSERT(batch[i]->key_len == 1 &&
                                        memcmp(batch[i]->key, "k", 1) == 0,
                                    "key-only record: key mismatch");
                        saw_key_only++;
                } else if (key_is_empty_or_null && !val_is_empty_or_null) {
                        TEST_ASSERT(batch[i]->len == 1 &&
                                        memcmp(batch[i]->payload, "v", 1) == 0,
                                    "value-only record: value mismatch");
                        saw_value_only++;
                }

                rd_kafka_message_destroy(batch[i]);
        }

        TEST_SAY("Counts: empty_kv=%d null_kv=%d key_only=%d value_only=%d\n",
                 saw_empty_kv, saw_null_kv, saw_key_only, saw_value_only);

        TEST_ASSERT(saw_empty_kv + saw_null_kv == 2,
                    "Expected 2 'no content' records, got %d",
                    saw_empty_kv + saw_null_kv);
        TEST_ASSERT(saw_key_only == 1, "Expected 1 key-only record, got %d",
                    saw_key_only);
        TEST_ASSERT(saw_value_only == 1, "Expected 1 value-only record, got %d",
                    saw_value_only);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);

        TEST_SAY("SUCCESS: zero-byte payload/key preserved\n");
}


/**
 * @brief Message at the topic's max.message.bytes boundary.
 *
 * Create a topic with max.message.bytes=8192, produce records of
 * increasing size leading up to the boundary, and verify all are
 * received via the share consumer.
 */
static void test_message_at_max_bytes_boundary(void) {
        const char *group = "share-max-bytes-boundary";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_t *producer;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[16];
        char errstr[512];
        const size_t sizes[] = {100, 1000, 4000, 7000, 7900};
        const int total      = sizeof(sizes) / sizeof(sizes[0]);
        size_t max_observed  = 0;
        int consumed         = 0;
        int attempts;
        int i;

        TEST_SAY("\n");
        TEST_SAY("=== Message at max.message.bytes (8192) boundary ===\n");

        topic = test_mk_topic_name("0171-max-bytes", 1);

        /* Topic with explicit max.message.bytes=8192. */
        test_admin_create_topic(
            NULL, topic, 1, -1,
            (const char *[]) {"max.message.bytes", "8192", NULL});
        test_wait_topic_exists(common_admin, topic, 60 * 1000);

        /* Producer with matching message.max.bytes so the client doesn't
         * pre-reject. */
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "message.max.bytes", "8192");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Produce one record per target size. */
        for (i = 0; i < total; i++) {
                char *payload = rd_malloc(sizes[i]);
                rd_kafka_resp_err_t err;
                size_t j;

                for (j = 0; j < sizes[i]; j++)
                        payload[j] = (char)('A' + (j % 26));

                err = rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic),
                                        RD_KAFKA_V_VALUE(payload, sizes[i]),
                                        RD_KAFKA_V_END);
                TEST_ASSERT(!err, "produce size=%zu failed: %s", sizes[i],
                            rd_kafka_err2name(err));
                rd_free(payload);
        }
        rd_kafka_flush(producer, 30 * 1000);
        rd_kafka_destroy(producer);

        /* Share consumer with fetch.message.max.bytes >= topic max so the
         * largest record fits a single fetch. */
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.message.max.bytes", "16384", errstr,
                          sizeof(errstr));
        consumer = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "share_consumer_new failed: %s", errstr);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 60;
        while (consumed < total && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err) {
                                if (batch[m]->len > max_observed)
                                        max_observed = batch[m]->len;
                                consumed++;
                        }
                        rd_kafka_message_destroy(batch[m]);
                }
        }

        TEST_ASSERT(consumed == total,
                    "Expected %d records up to boundary, consumed %d", total,
                    consumed);
        TEST_ASSERT(max_observed == sizes[total - 1],
                    "Largest record size mismatch: expected %zu, got %zu",
                    sizes[total - 1], max_observed);

        TEST_SAY(
            "SUCCESS: max.message.bytes boundary - %d records, max=%zu "
            "bytes\n",
            consumed, max_observed);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Consume + ack a round of messages produced with the given
 *        compression codec.
 */
static void do_test_compression_codec(const char *codec) {
        const char *group_prefix = "share-codec-";
        char group[128];
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_t *producer;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const int msg_cnt = 200;
        const int msg_sz  = 256;
        int consumed      = 0;
        int attempts;

        TEST_SAY("\n");
        TEST_SAY("=== Compression codec: %s ===\n", codec);

        rd_snprintf(group, sizeof(group), "%s%s", group_prefix, codec);
        topic = test_mk_topic_name(codec, 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Producer configured with the requested compression codec.
         * test_produce_msgs2 → test_wait_delivery uses a counter that's
         * decremented inside test_dr_msg_cb, so the dr callback must
         * be wired up explicitly when we create our own producer (only
         * test_create_producer() sets it for us). */
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "compression.type", codec);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        test_produce_msgs2(producer, topic, 0, 0, 0, msg_cnt, NULL, msg_sz);
        rd_kafka_flush(producer, 30 * 1000);
        rd_kafka_destroy(producer);

        consumer = test_create_share_consumer(group, NULL);
        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 100;
        while (consumed < msg_cnt && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err =
                    rd_kafka_share_consume_batch(consumer, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err) {
                                TEST_ASSERT(batch[m]->len == (size_t)msg_sz,
                                            "[%s] msg size %zu != expected %d",
                                            codec, batch[m]->len, msg_sz);
                                consumed++;
                        }
                        rd_kafka_message_destroy(batch[m]);
                }
        }

        TEST_ASSERT(consumed == msg_cnt,
                    "[%s] expected %d records, consumed %d", codec, msg_cnt,
                    consumed);

        TEST_SAY("SUCCESS: codec %s - consumed %d records\n", codec, consumed);

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);
}


/**
 * @brief Run consume+ack roundtrip for every supported producer
 *        compression codec.
 */
static void test_all_compression_codecs(void) {
        do_test_compression_codec("none");
        do_test_compression_codec("gzip");
        do_test_compression_codec("snappy");
        do_test_compression_codec("lz4");
        do_test_compression_codec("zstd");
}


/**
 * @brief Records with 100 headers and one very large header value.
 *
 * Verifies the share consumer preserves a large header count and large
 * per-header value sizes end-to-end.
 */
static void test_many_and_large_headers(void) {
        const char *group = "share-many-headers";
        const char *topic;
        rd_kafka_share_t *consumer;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[8];
        rd_kafka_resp_err_t err;
        const int hdr_cnt         = 100;
        const size_t big_hdr_size = 64 * 1024;
        char *big_hdr_value;
        size_t rcvd = 0;
        int attempts;
        size_t i;
        rd_bool_t verified_many = rd_false;
        rd_bool_t verified_big  = rd_false;

        TEST_SAY("\n");
        TEST_SAY(
            "=== Many headers (%d) and one large header value (%zu B) ===\n",
            hdr_cnt, big_hdr_size);

        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0171-many-headers", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Build the producev arg list for a record with `hdr_cnt` headers.
         * Each header name is "h-<i>" and each value is "v-<i>". */
        {
                rd_kafka_headers_t *hdrs = rd_kafka_headers_new(hdr_cnt);
                for (i = 0; i < (size_t)hdr_cnt; i++) {
                        char name[32];
                        char value[32];
                        rd_snprintf(name, sizeof(name), "h-%zu", i);
                        rd_snprintf(value, sizeof(value), "v-%zu", i);
                        rd_kafka_header_add(hdrs, name, -1, value, -1);
                }

                err =
                    rd_kafka_producev(common_producer, RD_KAFKA_V_TOPIC(topic),
                                      RD_KAFKA_V_KEY("many", 4),
                                      RD_KAFKA_V_VALUE("many-payload", 12),
                                      RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_END);
                TEST_ASSERT(!err, "producev many headers failed: %s",
                            rd_kafka_err2name(err));
        }

        /* Record with one very large header value. */
        big_hdr_value = rd_malloc(big_hdr_size);
        for (i = 0; i < big_hdr_size; i++)
                big_hdr_value[i] = (char)('A' + (i % 26));

        err = rd_kafka_producev(
            common_producer, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_KEY("big", 3),
            RD_KAFKA_V_VALUE("big-payload", 11),
            RD_KAFKA_V_HEADER("big-hdr", big_hdr_value, big_hdr_size),
            RD_KAFKA_V_END);
        TEST_ASSERT(!err, "producev big header failed: %s",
                    rd_kafka_err2name(err));
        rd_free(big_hdr_value);

        rd_kafka_flush(common_producer, 30 * 1000);

        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 30;
        while (rcvd < 2 && attempts-- > 0) {
                size_t batch_rcvd = 0;
                rd_kafka_error_t *e;

                e = rd_kafka_share_consume_batch(consumer, 2000, batch + rcvd,
                                                 &batch_rcvd);
                if (e)
                        rd_kafka_error_destroy(e);
                rcvd += batch_rcvd;
        }

        TEST_ASSERT(rcvd == 2, "Expected 2 records, got %zu", rcvd);

        for (i = 0; i < rcvd; i++) {
                rd_kafka_headers_t *hdrs = NULL;
                rd_kafka_resp_err_t herr;

                herr = rd_kafka_message_headers(batch[i], &hdrs);
                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR && hdrs,
                            "headers not available: %s",
                            rd_kafka_err2name(herr));

                if (batch[i]->key_len == 4 &&
                    memcmp(batch[i]->key, "many", 4) == 0) {
                        size_t n = rd_kafka_header_cnt(hdrs);
                        size_t j;

                        TEST_ASSERT(n == (size_t)hdr_cnt,
                                    "many-headers record: expected %d "
                                    "headers, got %zu",
                                    hdr_cnt, n);

                        /* Verify every header name+value present. */
                        for (j = 0; j < (size_t)hdr_cnt; j++) {
                                char name[32];
                                char expected[32];
                                const void *val;
                                size_t vsz;

                                rd_snprintf(name, sizeof(name), "h-%zu", j);
                                rd_snprintf(expected, sizeof(expected), "v-%zu",
                                            j);
                                herr = rd_kafka_header_get_last(hdrs, name,
                                                                &val, &vsz);
                                TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR,
                                            "%s missing: %s", name,
                                            rd_kafka_err2name(herr));
                                TEST_ASSERT(vsz == strlen(expected) &&
                                                memcmp(val, expected, vsz) == 0,
                                            "%s value mismatch", name);
                        }
                        verified_many = rd_true;
                } else if (batch[i]->key_len == 3 &&
                           memcmp(batch[i]->key, "big", 3) == 0) {
                        const void *val;
                        size_t vsz;
                        size_t k;
                        const char *bytes;

                        herr = rd_kafka_header_get_last(hdrs, "big-hdr", &val,
                                                        &vsz);
                        TEST_ASSERT(herr == RD_KAFKA_RESP_ERR_NO_ERROR,
                                    "big-hdr missing: %s",
                                    rd_kafka_err2name(herr));
                        TEST_ASSERT(vsz == big_hdr_size,
                                    "big-hdr size mismatch: expected %zu, "
                                    "got %zu",
                                    big_hdr_size, vsz);

                        /* Spot-check the content pattern at a few offsets. */
                        bytes = (const char *)val;
                        for (k = 0; k < vsz; k += vsz / 16) {
                                char expected = (char)('A' + (k % 26));
                                TEST_ASSERT(bytes[k] == expected,
                                            "big-hdr byte %zu: expected %c, "
                                            "got %c",
                                            k, expected, bytes[k]);
                        }
                        verified_big = rd_true;
                }

                rd_kafka_message_destroy(batch[i]);
        }

        TEST_ASSERT(verified_many, "many-headers record was not verified");
        TEST_ASSERT(verified_big, "big-header record was not verified");

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);

        TEST_SAY("SUCCESS: many headers + large header value preserved\n");
}


int main_0171_share_consumer_consume(int argc, char **argv) {
        /* Create common handles for all tests */
        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        test_timeout_set(600);

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

        /* Multi-consumer tests */
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

        /* High volume tests */
        test_high_volume_10k_messages();
        test_high_volume_50k_multi_partition();

        /* Multi-topic tests (triggers multiple fetch responses) */
        test_many_topics_15();
        test_many_topics_10_multi_partition();

        /* Rapid produce/consume tests */
        test_rapid_produce_consume_cycles();

        /* Edge case tests */
        test_empty_then_produce();
        test_sparse_partitions();

        /* Callback tests */
        test_poll_callback_piggybacked_acks();

        /* Headers and fetch byte-limit tests */
        test_headers_preserved();
        test_fetch_max_bytes_small();
        test_record_larger_than_fetch_max_bytes();

        /* Failing consumer releases to group */
        test_consumer_close_releases_to_group();

        /* Topology change: grow partitions while consuming */
        test_partition_increase_during_consume();

        /* Payload edge cases */
        test_zero_byte_payload_and_key();
        test_message_at_max_bytes_boundary();
        test_all_compression_codecs();
        test_many_and_large_headers();


        /* Cleanup common handles */
        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}
