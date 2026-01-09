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
#include "rdkafka_int.h"

/**
 * @brief Maximum supported values for test configuration
 */
#define MAX_CONSUMERS   16
#define MAX_TOPICS      16
#define MAX_PARTITIONS  32
#define BATCH_SIZE      500

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
        int consumer_cnt;                    /**< Number of consumers to create */
        int topic_cnt;                       /**< Number of topics to create */
        int partitions[MAX_TOPICS];          /**< Partition count for each topic */
        int msgs_per_partition;              /**< Messages to produce per partition */
        const char *group_name;              /**< Share group name */
        const char *test_name;               /**< Test description for logging */
        int poll_timeout_ms;                 /**< Timeout for each poll (default: 3000) */
        int max_attempts;                    /**< Max poll attempts (default: auto-calculated) */
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
                state->topic_names[t] = rd_strdup(
                    test_mk_topic_name("0171-share-test", 1));

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

        TEST_SAY("Setup complete: %d topic(s), %d total partition(s), "
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
        test_IncrementalAlterConfigs_simple(state->consumers[0]->rkshare_rk,
                                            RD_KAFKA_RESOURCE_GROUP,
                                            config->group_name, grp_conf, 1);

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
        poll_timeout = config->poll_timeout_ms > 0 ? 
                       config->poll_timeout_ms : 3000;
        
        /* Auto-calculate attempts based on expected messages and consumers */
        if (config->max_attempts > 0) {
                attempts = config->max_attempts;
        } else {
                /* Heuristic: at least 10 attempts, or based on message count */
                attempts = (state->total_expected / (BATCH_SIZE / 2)) + 10;
                if (attempts < 20)
                        attempts = 20;
        }

        TEST_SAY("Starting consumption: expecting %d messages, "
                 "max %d attempts, %dms timeout\n",
                 state->total_expected, attempts, poll_timeout);

        while (state->total_consumed < state->total_expected && attempts-- > 0) {
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
                        TEST_SAY("Progress: %d/%d\n",
                                 state->total_consumed, state->total_expected);
                } else {
                        /* Build distribution string for multiple consumers */
                        char dist[256] = {0};
                        int pos = 0;
                        for (i = 0; i < config->consumer_cnt && pos < 250; i++) {
                                pos += rd_snprintf(dist + pos, sizeof(dist) - pos,
                                                   "c%d=%d ", i, 
                                                   state->per_consumer_count[i]);
                        }
                        TEST_SAY("Progress: %d/%d (%s)\n",
                                 state->total_consumed, state->total_expected, dist);
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
                        test_delete_topic(state->consumers[0]->rkshare_rk,
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
        int pos = 0;

        /* Validate config */
        TEST_ASSERT(config->consumer_cnt > 0 && 
                    config->consumer_cnt <= MAX_CONSUMERS,
                    "consumer_cnt must be 1-%d", MAX_CONSUMERS);
        TEST_ASSERT(config->topic_cnt > 0 && 
                    config->topic_cnt <= MAX_TOPICS,
                    "topic_cnt must be 1-%d", MAX_TOPICS);
        TEST_ASSERT(config->group_name != NULL, 
                    "group_name is required");

        /* Print test header */
        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("=== %s ===\n", config->test_name ? config->test_name : "Share Consumer Test");
        TEST_SAY("=== Consumers: %d, Topics: %d, Partitions: [", 
                 config->consumer_cnt, config->topic_cnt);
        for (i = 0; i < config->topic_cnt; i++) {
                TEST_SAY("%d%s", config->partitions[i], 
                         i < config->topic_cnt - 1 ? ", " : "");
        }
        TEST_SAY("], Msgs/Partition: %d ===\n", config->msgs_per_partition);
        TEST_SAY("============================================================\n");

        /* Execute test phases */
        create_share_consumers(config, &state);
        setup_topics_and_produce(config, &state);
        subscribe_consumers(config, &state);
        consume_messages(config, &state);

        /* Verify results */
        TEST_ASSERT(state.total_consumed == state.total_expected,
                    "Expected %d messages, consumed %d",
                    state.total_expected, state.total_consumed);

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
                .consumer_cnt = 1,
                .topic_cnt = 1,
                .partitions = {1},
                .msgs_per_partition = 1000,
                .group_name = "share-1c-1t-1p",
                .test_name = "Single consumer, single topic, single partition"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, single topic, multiple partitions
 */
static void test_single_consumer_single_topic_multiple_partitions(void) {
        share_test_config_t config = {
                .consumer_cnt = 1,
                .topic_cnt = 1,
                .partitions = {3},
                .msgs_per_partition = 500,
                .group_name = "share-1c-1t-3p",
                .test_name = "Single consumer, single topic, 3 partitions"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, multiple topics, single partition each
 */
static void test_single_consumer_multiple_topic_single_partition(void) {
        share_test_config_t config = {
                .consumer_cnt = 1,
                .topic_cnt = 2,
                .partitions = {1, 1},
                .msgs_per_partition = 500,
                .group_name = "share-1c-2t-1p",
                .test_name = "Single consumer, 2 topics, 1 partition each"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Single consumer, multiple topics, multiple partitions
 */
static void test_single_consumer_multiple_topic_multiple_partitions(void) {
        share_test_config_t config = {
                .consumer_cnt = 1,
                .topic_cnt = 3,
                .partitions = {2, 2, 2},
                .msgs_per_partition = 500,
                .group_name = "share-1c-3t-2p",
                .test_name = "Single consumer, 3 topics, 2 partitions each"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, single topic, single partition
 */
static void test_multiple_consumers_single_topic_single_partition(void) {
        share_test_config_t config = {
                .consumer_cnt = 2,
                .topic_cnt = 1,
                .partitions = {1},
                .msgs_per_partition = 1000,
                .group_name = "share-2c-1t-1p",
                .test_name = "2 consumers, single topic, single partition"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, single topic, multiple partitions
 */
static void test_multiple_consumers_single_topic_multiple_partitions(void) {
        share_test_config_t config = {
                .consumer_cnt = 2,
                .topic_cnt = 1,
                .partitions = {4},
                .msgs_per_partition = 500,
                .group_name = "share-2c-1t-4p",
                .test_name = "2 consumers, single topic, 4 partitions"
        };
        run_share_consumer_test(&config);
}

/**
 * @brief Multiple consumers, multiple topics, multiple partitions
 */
static void test_multiple_consumers_multiple_topics_multiple_partitions(void) {
        share_test_config_t config = {
                .consumer_cnt = 3,
                .topic_cnt = 2,
                .partitions = {3, 3},
                .msgs_per_partition = 500,
                .group_name = "share-3c-2t-3p",
                .test_name = "3 consumers, 2 topics, 3 partitions each"
        };
        run_share_consumer_test(&config);
}


int main_0171_share_consumer_consume(int argc, char **argv) {
        
        /* Single-consumer tests */
        test_single_consumer_single_topic_single_partition();    /* Single consumer, single topic, single partition */
        test_single_consumer_single_topic_multiple_partitions();    /* Single consumer, single topic, multi partitions */
        test_single_consumer_multiple_topic_single_partition();    /* Single consumer, multi topic, single partition each */
        test_single_consumer_multiple_topic_multiple_partitions();    /* Single consumer, multi topics, multi partitions each */
        
        /* Multi-consumer tests */
        test_multiple_consumers_single_topic_single_partition();    /* Multi consumer sharing single partition */
        test_multiple_consumers_single_topic_multiple_partitions();    /* Multi consumer, multi partition */
        test_multiple_consumers_multiple_topics_multiple_partitions();    /* Full matrix: multi everything */

        return 0;
}
