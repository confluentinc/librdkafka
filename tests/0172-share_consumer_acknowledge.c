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
 * @brief Share consumer acknowledge API integration tests.
 *
 * Tests the acknowledge APIs (ACCEPT, REJECT, RELEASE) with real/mock broker.
 *
 * Expected Behavior:
 * - RELEASE: Records redelivered to the same or another consumer in the same
 * group.
 * - REJECT:  NOT redelivered
 * - ACCEPT:  Records committed, NOT redelivered
 *
 * All tests use share.acknowledgement.mode = "explicit"
 */

#define MAX_TOPICS        8
#define MAX_PARTITIONS    8
#define MAX_CONSUMERS     4
#define MAX_MSGS_PER_PART 100
#define BATCH_SIZE        10000

/**
 * @brief Test configuration structure
 */
typedef struct {
        const char *test_name;
        int topic_cnt;
        int partitions[MAX_TOPICS];
        int msgs_per_partition;
        int consumer_cnt;
        rd_kafka_share_AcknowledgeType_t actions[MAX_TOPICS][MAX_PARTITIONS]
                                                [MAX_MSGS_PER_PART];
        int expected_redelivered;
        int poll_timeout_ms;
        int max_attempts;
} ack_test_config_t;

/**
 * @brief Test state structure
 */
typedef struct {
        rd_kafka_share_t *consumers[MAX_CONSUMERS];
        char *topic_names[MAX_TOPICS];
        int64_t original_offsets[1000];
        int original_cnt;
        rd_kafka_topic_partition_list_t *released_msgs; /**< RELEASE'd msgs */
        rd_kafka_topic_partition_list_t
            *redelivered_msgs; /**< Redelivered msgs */
        int msgs_produced;
        int msgs_consumed;
        int msgs_redelivered;
        char group_name[128];
} ack_test_state_t;


/**
 * @brief Create share consumer with explicit acknowledgement mode
 */
static rd_kafka_share_t *create_explicit_ack_consumer(const char *group_id) {
        rd_kafka_share_t *rk;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 60);

        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "share.acknowledgement.mode", "explicit",
                          errstr, sizeof(errstr));

        rk = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "Failed to create share consumer: %s", errstr);

        return rk;
}

/**
 * @brief Set group offset to earliest
 */
static void set_group_offset_earliest(rd_kafka_share_t *rkshare,
                                      const char *group_name) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(test_share_consumer_get_rk(rkshare),
                                            RD_KAFKA_RESOURCE_GROUP, group_name,
                                            cfg, 1);
}

/**
 * @brief Create topics and produce messages
 */
static void setup_topics_and_produce(ack_test_config_t *config,
                                     ack_test_state_t *state) {
        int t, p;

        state->msgs_produced = 0;

        for (t = 0; t < config->topic_cnt; t++) {
                state->topic_names[t] =
                    rd_strdup(test_mk_topic_name("0172-ack-test", 1));

                test_create_topic_wait_exists(NULL, state->topic_names[t],
                                              config->partitions[t], -1,
                                              60 * 1000);

                for (p = 0; p < config->partitions[t]; p++) {
                        test_produce_msgs_easy(state->topic_names[t], 0, p,
                                               config->msgs_per_partition);
                        state->msgs_produced += config->msgs_per_partition;
                }

                TEST_SAY("Topic '%s': %d partition(s), %d msgs/partition\n",
                         state->topic_names[t], config->partitions[t],
                         config->msgs_per_partition);
        }

        TEST_SAY("Produced %d messages total\n", state->msgs_produced);
}

/**
 * @brief Subscribe consumers to topics
 */
static void subscribe_consumers(ack_test_config_t *config,
                                ack_test_state_t *state) {
        rd_kafka_topic_partition_list_t *subs;
        int t, i;

        set_group_offset_earliest(state->consumers[0], state->group_name);

        subs = rd_kafka_topic_partition_list_new(config->topic_cnt);
        for (t = 0; t < config->topic_cnt; t++) {
                rd_kafka_topic_partition_list_add(subs, state->topic_names[t],
                                                  RD_KAFKA_PARTITION_UA);
        }

        for (i = 0; i < config->consumer_cnt; i++) {
                rd_kafka_share_subscribe(state->consumers[i], subs);
        }

        rd_kafka_topic_partition_list_destroy(subs);

        TEST_SAY("Subscribed %d consumer(s) to %d topic(s)\n",
                 config->consumer_cnt, config->topic_cnt);
}

/**
 * @brief Get topic index from topic name
 */
static int get_topic_index(ack_test_config_t *config,
                           ack_test_state_t *state,
                           const char *topic_name) {
        int t;
        for (t = 0; t < config->topic_cnt; t++) {
                if (strcmp(state->topic_names[t], topic_name) == 0)
                        return t;
        }
        return -1;
}

/**
 * @brief Consume messages and apply acknowledgements based on config
 */
static void consume_and_acknowledge(ack_test_config_t *config,
                                    ack_test_state_t *state) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int poll_timeout =
            config->poll_timeout_ms > 0 ? config->poll_timeout_ms : 3000;
        int attempts = config->max_attempts > 0 ? config->max_attempts : 50;
        size_t total_consumed                   = 0;
        int msg_idx[MAX_TOPICS][MAX_PARTITIONS] = {{0}};

        state->original_cnt  = 0;
        state->msgs_consumed = 0;

        TEST_SAY("Consuming %d messages...\n", state->msgs_produced);

        while ((int)total_consumed < state->msgs_produced && attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err = rd_kafka_share_consume_batch(state->consumers[0],
                                                   poll_timeout, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err) {
                                int t_idx, p_idx, m_idx;
                                rd_kafka_share_AcknowledgeType_t ack_type;
                                rd_kafka_resp_err_t ack_err;

                                t_idx = get_topic_index(
                                    config, state,
                                    rd_kafka_topic_name(batch[m]->rkt));
                                p_idx = batch[m]->partition;

                                if (t_idx >= 0 &&
                                    p_idx < config->partitions[t_idx]) {
                                        m_idx = msg_idx[t_idx][p_idx]++;
                                        if (m_idx < MAX_MSGS_PER_PART)
                                                ack_type =
                                                    config
                                                        ->actions[t_idx][p_idx]
                                                                 [m_idx];
                                        else
                                                ack_type =
                                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                                } else {
                                        ack_type =
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                                }

                                ack_err = rd_kafka_share_acknowledge_type(
                                    state->consumers[0], batch[m], ack_type);
                                TEST_ASSERT(ack_err ==
                                                RD_KAFKA_RESP_ERR_NO_ERROR,
                                            "Acknowledge failed: %s",
                                            rd_kafka_err2str(ack_err));

                                /* Track RELEASE'd messages for verification */
                                if (ack_type ==
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE) {
                                        rd_kafka_topic_partition_t *rktpar;
                                        rktpar =
                                            rd_kafka_topic_partition_list_add(
                                                state->released_msgs,
                                                rd_kafka_topic_name(
                                                    batch[m]->rkt),
                                                batch[m]->partition);
                                        rktpar->offset = batch[m]->offset;
                                }

                                /* Verify delivery_count = 1 on first delivery
                                 */
                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(batch[m]) ==
                                        1,
                                    "Expected delivery_count=1 on first "
                                    "delivery, got %d",
                                    rd_kafka_message_delivery_count(batch[m]));

                                if (state->original_cnt < 1000) {
                                        state->original_offsets
                                            [state->original_cnt++] =
                                            batch[m]->offset;
                                }

                                total_consumed++;
                        }
                        rd_kafka_message_destroy(batch[m]);
                }

                TEST_SAY("Progress: %zu/%d\n", total_consumed,
                         state->msgs_produced);
        }

        state->msgs_consumed = (int)total_consumed;
        TEST_ASSERT(state->msgs_consumed == state->msgs_produced,
                    "Expected to consume %d messages, got %d",
                    state->msgs_produced, state->msgs_consumed);
}

/**
 * @brief Poll for redelivered messages after acknowledgements
 */
static void poll_for_redelivery(ack_test_config_t *config,
                                ack_test_state_t *state) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int poll_timeout =
            config->poll_timeout_ms > 0 ? config->poll_timeout_ms : 3000;
        int attempts = 10;

        state->msgs_redelivered = 0;

        TEST_SAY("Polling for redelivered messages (expecting %d)...\n",
                 config->expected_redelivered);

        while (attempts-- > 0) {
                size_t rcvd = 0;
                size_t m;
                rd_kafka_error_t *err;

                err = rd_kafka_share_consume_batch(state->consumers[0],
                                                   poll_timeout, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < rcvd; m++) {
                        if (!batch[m]->err) {
                                rd_kafka_topic_partition_t *rktpar;

                                /* Verify delivery_count = 2 on redelivery */
                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(batch[m]) ==
                                        2,
                                    "Expected delivery_count=2 on redelivery, "
                                    "got %d",
                                    rd_kafka_message_delivery_count(batch[m]));

                                /* Track redelivered message */
                                rktpar = rd_kafka_topic_partition_list_add(
                                    state->redelivered_msgs,
                                    rd_kafka_topic_name(batch[m]->rkt),
                                    batch[m]->partition);
                                rktpar->offset = batch[m]->offset;

                                state->msgs_redelivered++;

                                rd_kafka_share_acknowledge_type(
                                    state->consumers[0], batch[m],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                        }
                        rd_kafka_message_destroy(batch[m]);
                }

                if (rcvd > 0) {
                        TEST_SAY("Redelivered so far: %d\n",
                                 state->msgs_redelivered);
                }

                if (state->msgs_redelivered >= config->expected_redelivered &&
                    config->expected_redelivered > 0) {
                        break;
                }
        }
}

/**
 * @brief Verify redelivery results
 *
 * Checks that:
 * 1. The count of redelivered messages matches expected
 * 2. Each redelivered message was one that got RELEASE'd (same
 * topic+partition+offset)
 */
static void verify_results(ack_test_config_t *config, ack_test_state_t *state) {
        int i;

        TEST_SAY("Verifying: consumed=%d, redelivered=%d (expected=%d)\n",
                 state->msgs_consumed, state->msgs_redelivered,
                 config->expected_redelivered);

        TEST_ASSERT(state->msgs_redelivered == config->expected_redelivered,
                    "Expected %d redelivered messages, got %d",
                    config->expected_redelivered, state->msgs_redelivered);

        /* Verify each redelivered message was actually RELEASE'd */
        for (i = 0; i < state->redelivered_msgs->cnt; i++) {
                rd_kafka_topic_partition_t *redeliv =
                    &state->redelivered_msgs->elems[i];
                rd_kafka_topic_partition_t *found;
                rd_bool_t match_found = rd_false;
                int j;

                /* Search for matching released message */
                for (j = 0; j < state->released_msgs->cnt; j++) {
                        found = &state->released_msgs->elems[j];
                        if (strcmp(redeliv->topic, found->topic) == 0 &&
                            redeliv->partition == found->partition &&
                            redeliv->offset == found->offset) {
                                match_found = rd_true;
                                break;
                        }
                }

                TEST_ASSERT(match_found,
                            "Redelivered message (topic=%s, partition=%d, "
                            "offset=%" PRId64 ") was not in RELEASE'd list",
                            redeliv->topic, redeliv->partition,
                            redeliv->offset);
        }

        /* Verify all RELEASE'd messages were redelivered */
        for (i = 0; i < state->released_msgs->cnt; i++) {
                rd_kafka_topic_partition_t *released =
                    &state->released_msgs->elems[i];
                rd_kafka_topic_partition_t *found;
                rd_bool_t match_found = rd_false;
                int j;

                for (j = 0; j < state->redelivered_msgs->cnt; j++) {
                        found = &state->redelivered_msgs->elems[j];
                        if (strcmp(released->topic, found->topic) == 0 &&
                            released->partition == found->partition &&
                            released->offset == found->offset) {
                                match_found = rd_true;
                                break;
                        }
                }

                TEST_ASSERT(
                    match_found,
                    "RELEASE'd message (topic=%s, partition=%d, offset=%" PRId64
                    ") was not redelivered",
                    released->topic, released->partition, released->offset);
        }

        TEST_SAY("All %d redelivered messages verified correctly\n",
                 state->msgs_redelivered);
}

/**
 * @brief Cleanup test state
 */
static void cleanup_test(ack_test_config_t *config, ack_test_state_t *state) {
        int t, i;

        for (t = 0; t < config->topic_cnt; t++) {
                if (state->topic_names[t]) {
                        test_delete_topic(
                            test_share_consumer_get_rk(state->consumers[0]),
                            state->topic_names[t]);
                        rd_free(state->topic_names[t]);
                        state->topic_names[t] = NULL;
                }
        }

        for (i = 0; i < config->consumer_cnt; i++) {
                if (state->consumers[i]) {
                        rd_kafka_share_consumer_close(state->consumers[i]);
                        rd_kafka_share_destroy(state->consumers[i]);
                        state->consumers[i] = NULL;
                }
        }

        /* Destroy tracking lists */
        if (state->released_msgs)
                rd_kafka_topic_partition_list_destroy(state->released_msgs);
        if (state->redelivered_msgs)
                rd_kafka_topic_partition_list_destroy(state->redelivered_msgs);

        TEST_SAY("Cleanup complete\n");
}

/**
 * @brief Run a test scenario based on configuration
 */
static int run_ack_test(ack_test_config_t *config) {
        ack_test_state_t state = {0};
        int i;

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================"
            "\n");
        TEST_SAY("=== %s ===\n", config->test_name);
        TEST_SAY(
            "============================================================"
            "\n");

        rd_snprintf(state.group_name, sizeof(state.group_name), "share-%s",
                    config->test_name);

        /* Initialize tracking lists */
        state.released_msgs    = rd_kafka_topic_partition_list_new(100);
        state.redelivered_msgs = rd_kafka_topic_partition_list_new(100);

        for (i = 0; i < config->consumer_cnt; i++) {
                state.consumers[i] =
                    create_explicit_ack_consumer(state.group_name);
        }

        setup_topics_and_produce(config, &state);
        subscribe_consumers(config, &state);
        consume_and_acknowledge(config, &state);
        poll_for_redelivery(config, &state);
        verify_results(config, &state);
        cleanup_test(config, &state);

        TEST_SAY("=== %s: PASSED ===\n", config->test_name);
        return 0;
}


/***************************************************************************
 * Core Tests
 ***************************************************************************/

/**
 * @brief RELEASE causes redelivery
 */
static void test_release_redelivery(void) {
        ack_test_config_t config = {
            .test_name            = "release-redelivery",
            .topic_cnt            = 1,
            .partitions           = {1},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}}},
            .expected_redelivered = 5};
        run_ack_test(&config);
}

/**
 * @brief REJECT prevents redelivery
 */
static void test_reject_no_redelivery(void) {
        ack_test_config_t config = {
            .test_name            = "reject-no-redelivery",
            .topic_cnt            = 1,
            .partitions           = {1},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}}},
            .expected_redelivered = 0};
        run_ack_test(&config);
}

/**
 * @brief ACCEPT prevents redelivery
 */
static void test_accept_no_redelivery(void) {
        ack_test_config_t config = {
            .test_name            = "accept-no-redelivery",
            .topic_cnt            = 1,
            .partitions           = {1},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT}}},
            .expected_redelivered = 0};
        run_ack_test(&config);
}


/***************************************************************************
 * Extended Tests
 ***************************************************************************/

/**
 * @brief Mixed ACCEPT/REJECT/RELEASE in same batch
 *
 * Uses 3 partitions with different actions:
 * - Partition 0: ACCEPT (no redelivery)
 * - Partition 1: REJECT (no redelivery)
 * - Partition 2: RELEASE (redelivery)
 */
static void test_mixed_ack_types(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-ack-types",
            .topic_cnt            = 1,
            .partitions           = {3},
            .msgs_per_partition   = 3,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}}},
            .expected_redelivered = 3 /* Only partition 2 */
        };
        run_ack_test(&config);
}


/***************************************************************************
 * Multi-Partition Tests
 ***************************************************************************/

/**
 * @brief RELEASE works across multiple partitions
 *
 * 3 partitions with:
 * - Partition 0: RELEASE (redelivery)
 * - Partition 1: ACCEPT (no redelivery)
 * - Partition 2: REJECT (no redelivery)
 */
static void test_release_multiple_partitions(void) {
        ack_test_config_t config = {
            .test_name            = "release-multiple-partitions",
            .topic_cnt            = 1,
            .partitions           = {3},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}}},
            .expected_redelivered = 5 /* Only partition 0 */
        };
        run_ack_test(&config);
}

/**
 * @brief Mixed ack across partitions
 *
 * 2 partitions with mixed handling per offset (simplified by partition).
 */
static void test_mixed_ack_across_partitions(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-ack-across-partitions",
            .topic_cnt            = 1,
            .partitions           = {2},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT}}},
            .expected_redelivered = 5 /* Only partition 0 */
        };
        run_ack_test(&config);
}


/***************************************************************************
 * Multi-Topic Tests
 ***************************************************************************/

/**
 * @brief RELEASE works across multiple topics
 *
 * 2 topics with:
 * - Topic 0: ACCEPT (no redelivery)
 * - Topic 1: RELEASE (redelivery)
 */
static void test_release_multiple_topics(void) {
        ack_test_config_t config = {
            .test_name            = "release-multiple-topics",
            .topic_cnt            = 2,
            .partitions           = {1, 1},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}}},
            .expected_redelivered = 5 /* Only topic 1 */
        };
        run_ack_test(&config);
}

/**
 * @brief Mixed ack across topics
 *
 * 3 topics with:
 * - Topic 0: ACCEPT (no redelivery)
 * - Topic 1: REJECT (no redelivery)
 * - Topic 2: RELEASE (redelivery)
 */
static void test_mixed_ack_across_topics(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-ack-across-topics",
            .topic_cnt            = 3,
            .partitions           = {1, 1, 1},
            .msgs_per_partition   = 3,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}}},
            .expected_redelivered = 3 /* Only topic 2 */
        };
        run_ack_test(&config);
}


/***************************************************************************
 * Error Handling Tests (Standalone)
 ***************************************************************************/

/**
 * @brief Acknowledge with NULL message
 */
static void test_ack_null_message(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_resp_err_t err;
        const char *group = "share-null-msg-test";

        TEST_SAY("\n");
        TEST_SAY("=== test_ack_null_message ===\n");

        rkshare = create_explicit_ack_consumer(group);

        err = rd_kafka_share_acknowledge(rkshare, NULL);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG, got %s", rd_kafka_err2str(err));

        err = rd_kafka_share_acknowledge_type(
            rkshare, NULL, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG, got %s", rd_kafka_err2str(err));

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        TEST_SAY("=== test_ack_null_message: PASSED ===\n");
}

/**
 * @brief Acknowledge with NULL rkshare
 */
static void test_ack_null_rkshare(void) {
        rd_kafka_resp_err_t err;
        rd_kafka_message_t fake_msg = {0};

        TEST_SAY("\n");
        TEST_SAY("=== test_ack_null_rkshare ===\n");

        err = rd_kafka_share_acknowledge(NULL, &fake_msg);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG, got %s", rd_kafka_err2str(err));

        err = rd_kafka_share_acknowledge_type(
            NULL, &fake_msg, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG, got %s", rd_kafka_err2str(err));

        TEST_SAY("=== test_ack_null_rkshare: PASSED ===\n");
}

/**
 * @brief Acknowledge with invalid type
 */
static void test_ack_invalid_type(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *batch[10];
        rd_kafka_error_t *err;
        rd_kafka_resp_err_t ack_err;
        rd_kafka_topic_partition_list_t *subs;
        const char *group = "share-invalid-type-test";
        const char *topic;
        size_t rcvd = 0;
        int attempts;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};

        TEST_SAY("\n");
        TEST_SAY("=== test_ack_invalid_type ===\n");

        rkshare = create_explicit_ack_consumer(group);
        topic   = test_mk_topic_name("0172-invalid-type", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 1);

        test_IncrementalAlterConfigs_simple(test_share_consumer_get_rk(rkshare),
                                            RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        attempts = 20;
        while (rcvd == 0 && attempts-- > 0) {
                err = rd_kafka_share_consume_batch(rkshare, 2000, batch, &rcvd);
                if (err)
                        rd_kafka_error_destroy(err);
        }

        TEST_ASSERT(rcvd > 0, "Failed to consume message");

        /* Try invalid type (99) */
        ack_err = rd_kafka_share_acknowledge_type(
            rkshare, batch[0], (rd_kafka_share_AcknowledgeType_t)99);
        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG for type 99, got %s",
                    rd_kafka_err2str(ack_err));

        /* Try GAP type (0) - internal only */
        ack_err = rd_kafka_share_acknowledge_type(
            rkshare, batch[0], (rd_kafka_share_AcknowledgeType_t)0);
        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG for type 0 (GAP), got %s",
                    rd_kafka_err2str(ack_err));

        /* Clean up with valid acknowledge */
        rd_kafka_share_acknowledge_type(rkshare, batch[0],
                                        RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        rd_kafka_message_destroy(batch[0]);

        test_delete_topic(test_share_consumer_get_rk(rkshare), topic);
        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        TEST_SAY("=== test_ack_invalid_type: PASSED ===\n");
}

/**
 * @brief RELEASE then REJECT - final type is REJECT (no redelivery)
 */
static void test_release_then_reject_no_redelivery(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *batch[100];
        rd_kafka_error_t *err;
        rd_kafka_resp_err_t ack_err;
        rd_kafka_topic_partition_list_t *subs;
        const char *group = "share-release-then-reject";
        const char *topic;
        size_t rcvd = 0;
        size_t m;
        int attempts;
        int redelivered        = 0;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};

        TEST_SAY("\n");
        TEST_SAY("=== test_release_then_reject_no_redelivery ===\n");

        rkshare = create_explicit_ack_consumer(group);
        topic   = test_mk_topic_name("0172-release-reject", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        test_IncrementalAlterConfigs_simple(test_share_consumer_get_rk(rkshare),
                                            RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Consume all messages */
        attempts = 30;
        while (rcvd < 5 && attempts-- > 0) {
                size_t batch_rcvd = 0;
                err = rd_kafka_share_consume_batch(rkshare, 2000, batch + rcvd,
                                                   &batch_rcvd);
                if (err)
                        rd_kafka_error_destroy(err);
                rcvd += batch_rcvd;
        }

        TEST_ASSERT(rcvd == 5, "Expected 5 messages, got %zu", rcvd);

        /* First RELEASE offset 0, then override with REJECT */
        ack_err = rd_kafka_share_acknowledge_type(
            rkshare, batch[0], RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR_NO_ERROR, "RELEASE failed: %s",
                    rd_kafka_err2str(ack_err));

        ack_err = rd_kafka_share_acknowledge_type(
            rkshare, batch[0], RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT);
        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR_NO_ERROR, "REJECT failed: %s",
                    rd_kafka_err2str(ack_err));

        /* ACCEPT remaining messages */
        for (m = 1; m < rcvd; m++) {
                rd_kafka_share_acknowledge_type(
                    rkshare, batch[m], RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        }

        /* Destroy batch */
        for (m = 0; m < rcvd; m++) {
                rd_kafka_message_destroy(batch[m]);
        }

        /* Poll for redelivery - should get 0 */
        TEST_SAY("Polling for redelivery (expecting 0)...\n");
        attempts = 5;
        while (attempts-- > 0) {
                size_t redeliv_rcvd = 0;
                err = rd_kafka_share_consume_batch(rkshare, 2000, batch,
                                                   &redeliv_rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (m = 0; m < redeliv_rcvd; m++) {
                        if (!batch[m]->err)
                                redelivered++;
                        rd_kafka_share_acknowledge_type(
                            rkshare, batch[m],
                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                        rd_kafka_message_destroy(batch[m]);
                }
        }

        TEST_ASSERT(redelivered == 0,
                    "Expected 0 redelivered (REJECT overrides RELEASE), got %d",
                    redelivered);

        test_delete_topic(test_share_consumer_get_rk(rkshare), topic);
        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        TEST_SAY("=== test_release_then_reject_no_redelivery: PASSED ===\n");
}


/***************************************************************************
 * Max Delivery Attempts Tests
 ***************************************************************************/

/**
 * @brief Release a record 5 times (max attempts), verify no 6th delivery
 *
 * Default share.record.lock.partition.limit is 5. After 5 RELEASE attempts,
 * the broker should automatically reject the record (no more redeliveries).
 */
static void test_max_delivery_attempts(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *batch[100];
        rd_kafka_error_t *err;
        rd_kafka_topic_partition_list_t *subs;
        const char *group = "share-max-delivery-attempts";
        const char *topic;
        size_t rcvd;
        int delivery_attempt;
        int attempts;
        const int max_deliveries = 5;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};

        TEST_SAY("\n");
        TEST_SAY("=== test_max_delivery_attempts ===\n");
        TEST_SAY(
            "Testing that record is not redelivered after %d RELEASE "
            "attempts\n",
            max_deliveries);

        rkshare = create_explicit_ack_consumer(group);
        topic   = test_mk_topic_name("0172-max-delivery", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 1); /* Just 1 message */

        test_IncrementalAlterConfigs_simple(test_share_consumer_get_rk(rkshare),
                                            RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* RELEASE the same message max_deliveries times */
        for (delivery_attempt = 1; delivery_attempt <= max_deliveries;
             delivery_attempt++) {
                rcvd     = 0;
                attempts = 30;

                while (rcvd == 0 && attempts-- > 0) {
                        err = rd_kafka_share_consume_batch(rkshare, 2000, batch,
                                                           &rcvd);
                        if (err)
                                rd_kafka_error_destroy(err);
                }

                TEST_ASSERT(rcvd == 1,
                            "Delivery attempt %d: expected 1 message, got %zu",
                            delivery_attempt, rcvd);

                /* Verify delivery_count matches attempt number */
                TEST_ASSERT(
                    rd_kafka_message_delivery_count(batch[0]) ==
                        delivery_attempt,
                    "Delivery attempt %d: expected delivery_count=%d, got %d",
                    delivery_attempt, delivery_attempt,
                    rd_kafka_message_delivery_count(batch[0]));

                TEST_SAY(
                    "Delivery attempt %d: received message "
                    "(delivery_count=%d), sending RELEASE\n",
                    delivery_attempt,
                    rd_kafka_message_delivery_count(batch[0]));

                /* RELEASE to trigger redelivery */
                rd_kafka_share_acknowledge_type(
                    rkshare, batch[0], RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
                rd_kafka_message_destroy(batch[0]);
        }

        /* Now poll again - message should NOT be redelivered (max attempts
         * reached) */
        TEST_SAY("Polling for 6th delivery (should NOT receive message)...\n");
        rcvd     = 0;
        attempts = 5;
        while (attempts-- > 0) {
                size_t batch_rcvd = 0;
                err = rd_kafka_share_consume_batch(rkshare, 2000, batch,
                                                   &batch_rcvd);
                if (err)
                        rd_kafka_error_destroy(err);

                if (batch_rcvd > 0) {
                        size_t m;
                        for (m = 0; m < batch_rcvd; m++) {
                                if (!batch[m]->err)
                                        rcvd++;
                                rd_kafka_message_destroy(batch[m]);
                        }
                }
        }

        TEST_ASSERT(rcvd == 0,
                    "Expected 0 messages after %d RELEASE attempts, got %zu",
                    max_deliveries, rcvd);

        TEST_SAY("SUCCESS: Message not redelivered after %d RELEASE attempts\n",
                 max_deliveries);

        test_delete_topic(test_share_consumer_get_rk(rkshare), topic);
        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        TEST_SAY("=== test_max_delivery_attempts: PASSED ===\n");
}


/***************************************************************************
 * Mixed Per-Offset Acknowledgement Tests
 ***************************************************************************/

/**
 * @brief Mixed per-offset acks: single topic, single partition
 *
 * 10 messages with alternating ACCEPT/REJECT/RELEASE pattern:
 * Offset 0: ACCEPT  (no redeliver)
 * Offset 1: REJECT  (no redeliver)
 * Offset 2: RELEASE (redeliver)
 * Offset 3: ACCEPT  (no redeliver)
 * Offset 4: REJECT  (no redeliver)
 * Offset 5: RELEASE (redeliver)
 * ... and so on
 *
 * Expected redeliveries: offsets 2, 5, 8 = 3 messages
 */
static void test_mixed_per_offset_single_partition(void) {
        ack_test_config_t config = {
            .test_name          = "mixed-per-offset-1t-1p",
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 10,
            .consumer_cnt       = 1,
            .actions = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 0 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,  /* 1 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 2 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 3 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,  /* 4 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 5 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 6 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,  /* 7 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 8 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT /* 9 */}}},
            .expected_redelivered = 3 /* Offsets 2, 5, 8 */
        };
        run_ack_test(&config);
}

/**
 * @brief Mixed per-offset acks: single topic, multiple partitions
 *
 * 3 partitions, 6 messages each, different patterns per partition:
 * - Partition 0: ACCEPT, RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE (3
 * redeliver)
 * - Partition 1: RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE, ACCEPT (3
 * redeliver)
 * - Partition 2: REJECT, REJECT, REJECT, REJECT, REJECT, REJECT (0 redeliver)
 *
 * Expected redeliveries: 6
 */
static void test_mixed_per_offset_multiple_partitions(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-per-offset-1t-3p",
            .topic_cnt            = 1,
            .partitions           = {3},
            .msgs_per_partition   = 6,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}}},
            .expected_redelivered = 6 /* 3 from p0 + 3 from p1 */
        };
        run_ack_test(&config);
}

/**
 * @brief Mixed per-offset acks: multiple topics, single partition each
 *
 * 3 topics, 1 partition each, 5 messages per partition:
 * - Topic 0: RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE (3 redeliver)
 * - Topic 1: ACCEPT, REJECT, ACCEPT, REJECT, ACCEPT (0 redeliver)
 * - Topic 2: REJECT, RELEASE, REJECT, RELEASE, REJECT (2 redeliver)
 *
 * Expected redeliveries: 5
 */
static void test_mixed_per_offset_multiple_topics(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-per-offset-3t-1p",
            .topic_cnt            = 3,
            .partitions           = {1, 1, 1},
            .msgs_per_partition   = 5,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}}},
            .expected_redelivered = 5 /* 3 from t0 + 0 from t1 + 2 from t2 */
        };
        run_ack_test(&config);
}

/**
 * @brief Mixed per-offset acks: multiple topics, multiple partitions
 *
 * 2 topics, 2 partitions each, 4 messages per partition:
 * - Topic 0, Partition 0: RELEASE, RELEASE, ACCEPT, ACCEPT (2 redeliver)
 * - Topic 0, Partition 1: ACCEPT, ACCEPT, RELEASE, RELEASE (2 redeliver)
 * - Topic 1, Partition 0: REJECT, RELEASE, REJECT, RELEASE (2 redeliver)
 * - Topic 1, Partition 1: RELEASE, REJECT, RELEASE, REJECT (2 redeliver)
 *
 * Expected redeliveries: 8
 */
static void test_mixed_per_offset_full_matrix(void) {
        ack_test_config_t config = {
            .test_name            = "mixed-per-offset-2t-2p",
            .topic_cnt            = 2,
            .partitions           = {2, 2},
            .msgs_per_partition   = 4,
            .consumer_cnt         = 1,
            .actions              = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE}},
                                     {{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE},
                                      {RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
                                       RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT}}},
            .expected_redelivered = 8 /* 2+2 from t0 + 2+2 from t1 */
        };
        run_ack_test(&config);
}

/**
 * @brief Alternating pattern across all offsets - stress test
 *
 * Single topic, single partition, 20 messages:
 * Even offsets: RELEASE (10 redeliveries)
 * Odd offsets: ACCEPT (0 redeliveries)
 */
static void test_alternating_ack_pattern(void) {
        ack_test_config_t config = {
            .test_name          = "alternating-ack-pattern",
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 20,
            .consumer_cnt       = 1,
            .actions = {{{RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 0 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 1 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 2 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 3 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 4 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 5 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 6 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 7 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 8 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 9 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 10 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 11 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 12 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 13 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 14 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 15 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 16 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,  /* 17 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE, /* 18 */
                          RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT /* 19 */}}},
            .expected_redelivered = 10 /* Even offsets */
        };
        run_ack_test(&config);
}


int main_0172_share_consumer_acknowledge(int argc, char **argv) {

        /* Core tests */
        test_release_redelivery();
        test_reject_no_redelivery();
        test_accept_no_redelivery();

        /* Extended tests */
        test_mixed_ack_types();

        /* Multi-partition tests */
        test_release_multiple_partitions();
        test_mixed_ack_across_partitions();

        /* Multi-topic tests */
        test_release_multiple_topics();
        test_mixed_ack_across_topics();

        /* Error handling tests */
        test_ack_null_message();
        test_ack_null_rkshare();
        test_ack_invalid_type();
        test_release_then_reject_no_redelivery();

        /* Max delivery attempts test */
        test_max_delivery_attempts();

        /* Mixed per-offset acknowledgement tests */
        test_mixed_per_offset_single_partition();
        test_mixed_per_offset_multiple_partitions();
        test_mixed_per_offset_multiple_topics();
        test_mixed_per_offset_full_matrix();
        test_alternating_ack_pattern();

        return 0;
}
