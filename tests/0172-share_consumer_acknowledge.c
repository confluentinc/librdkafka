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

/** Common producer reused across all tests. */
static rd_kafka_t *common_producer;

/** Common admin client reused across all tests. */
static rd_kafka_t *common_admin;

/**
 * @brief Produce messages using the common producer.
 */
static void produce_to_topic(const char *topic, int32_t partition, int msgcnt) {
        rd_kafka_topic_t *rkt;
        rkt = test_create_producer_topic(common_producer, topic, NULL);
        test_produce_msgs(common_producer, rkt, 0, partition, 0, msgcnt, NULL,
                          0);
        rd_kafka_topic_destroy(rkt);
}

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

#define MAX_TOPICS        16
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
        int poll_timeout_ms;
        int max_attempts;
        int total_msgs;
        rd_bool_t use_random_acks; /**< Generate random acks at runtime */
        rd_kafka_share_AcknowledgeType_t
            single_ack_type; /**< Ack type when not using random */
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
        /* Random mode counters */
        int msgs_accepted;
        int msgs_rejected;
        int msgs_released;
} ack_test_state_t;


/**
 * @brief Generate random ack type with roughly equal distribution
 */
static rd_kafka_share_AcknowledgeType_t get_random_ack_type(void) {
        return (rd_kafka_share_AcknowledgeType_t)jitter(1, 3);
}

/**
 * @brief Set group offset to earliest
 */
static void set_group_offset_earliest(const char *group_name) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_alter_group_configurations(group_name, cfg, 1);
}

/**
 * @brief Create topics and produce messages
 */
static void setup_topics_and_produce(ack_test_config_t *config,
                                     ack_test_state_t *state) {
        int t, p;
        int msgs_per_partition;
        int total_partitions = 0;

        state->msgs_produced = 0;

        /* Calculate msgs_per_partition based on mode */
        if (config->use_random_acks && config->total_msgs > 0) {
                for (t = 0; t < config->topic_cnt; t++)
                        total_partitions += config->partitions[t];
                msgs_per_partition = config->total_msgs / total_partitions;
        } else {
                msgs_per_partition = config->msgs_per_partition;
        }

        for (t = 0; t < config->topic_cnt; t++) {
                state->topic_names[t] =
                    rd_strdup(test_mk_topic_name("0172-ack-test", 1));

                test_create_topic_wait_exists(NULL, state->topic_names[t],
                                              config->partitions[t], -1,
                                              60 * 1000);

                for (p = 0; p < config->partitions[t]; p++) {
                        produce_to_topic(state->topic_names[t], p,
                                         msgs_per_partition);
                        state->msgs_produced += msgs_per_partition;
                }

                TEST_SAY("Topic '%s': %d partition(s), %d msgs/partition\n",
                         state->topic_names[t], config->partitions[t],
                         msgs_per_partition);
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

        set_group_offset_earliest(state->group_name);

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
 * @brief Find message index in list by topic+partition+offset
 * @returns Index if found, -1 otherwise
 */
static int find_message_in_list(rd_kafka_topic_partition_list_t *list,
                                const char *topic,
                                int32_t partition,
                                int64_t offset) {
        int i;
        for (i = 0; i < list->cnt; i++) {
                rd_kafka_topic_partition_t *elem = &list->elems[i];
                if (strcmp(elem->topic, topic) == 0 &&
                    elem->partition == partition && elem->offset == offset)
                        return i;
        }
        return -1;
}

/**
 * @brief Remove message from list by index (swap with last element)
 */
static void remove_message_from_list_at(rd_kafka_topic_partition_list_t *list,
                                        int idx) {
        if (idx < 0 || idx >= list->cnt)
                return;

        /* Free the topic string being removed */
        rd_free(list->elems[idx].topic);

        /* Swap with last element and decrement count */
        if (idx < list->cnt - 1) {
                list->elems[idx] = list->elems[list->cnt - 1];
                /* Null out the moved element to prevent double-free */
                list->elems[list->cnt - 1].topic = NULL;
        }
        list->cnt--;
}

/**
 * @brief Handle a redelivered message (delivery_count == 2).
 *
 * Verifies the message was previously RELEASE'd (in released_msgs),
 * removes it from released_msgs, and acknowledges as ACCEPT.
 */
static void handle_redelivered_message(ack_test_state_t *state,
                                       rd_kafka_message_t *msg) {
        int released_idx;

        released_idx = find_message_in_list(state->released_msgs,
                                            rd_kafka_topic_name(msg->rkt),
                                            msg->partition, msg->offset);

        TEST_ASSERT(released_idx >= 0,
                    "Redelivered message (delivery_count=2) not found in "
                    "released_msgs: topic=%s, partition=%d, offset=%" PRId64,
                    rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset);

        remove_message_from_list_at(state->released_msgs, released_idx);
        state->msgs_redelivered++;

        rd_kafka_share_acknowledge_type(state->consumers[0], msg,
                                        RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
}

/**
 * @brief Determine ack type based on config.
 */
static rd_kafka_share_AcknowledgeType_t
determine_ack_type(ack_test_config_t *config) {
        if (config->use_random_acks)
                return get_random_ack_type();
        return config->single_ack_type;
}

/**
 * @brief Track ack type in state and add to released list if RELEASE.
 */
static void track_ack_type(ack_test_state_t *state,
                           rd_kafka_message_t *msg,
                           rd_kafka_share_AcknowledgeType_t ack_type) {
        if (ack_type == RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE) {
                rd_kafka_topic_partition_t *rktpar;
                rktpar = rd_kafka_topic_partition_list_add(
                    state->released_msgs, rd_kafka_topic_name(msg->rkt),
                    msg->partition);
                rktpar->offset = msg->offset;
                state->msgs_released++;
        } else if (ack_type == RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT) {
                state->msgs_accepted++;
        } else {
                state->msgs_rejected++;
        }
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
        size_t total_consumed = 0;

        if (config->use_random_acks) {
                poll_timeout = 5000;
                attempts     = 200 + (config->total_msgs / 1000) * 50;
        }

        state->original_cnt     = 0;
        state->msgs_consumed    = 0;
        state->msgs_accepted    = 0;
        state->msgs_rejected    = 0;
        state->msgs_released    = 0;
        state->msgs_redelivered = 0;

        TEST_SAY("Consuming %d messages%s...\n", state->msgs_produced,
                 config->use_random_acks ? " with random acks" : "");

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
                        rd_kafka_share_AcknowledgeType_t ack_type;
                        rd_kafka_resp_err_t ack_err;
                        int16_t delivery_count =
                            rd_kafka_message_delivery_count(batch[m]);

                        /* Error messages must use acknowledge_offset API */
                        if (batch[m]->err) {
                                ack_type = determine_ack_type(config);
                                ack_err  = rd_kafka_share_acknowledge_offset(
                                    state->consumers[0],
                                    rd_kafka_topic_name(batch[m]->rkt),
                                    batch[m]->partition, batch[m]->offset,
                                    ack_type);
                                TEST_ASSERT(
                                    ack_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                                    "acknowledge_offset failed for error msg: "
                                    "%s",
                                    rd_kafka_err2str(ack_err));
                                rd_kafka_message_destroy(batch[m]);
                                continue;
                        }

                        /* Redelivered message (delivery_count == 2) */
                        if (delivery_count == 2) {
                                handle_redelivered_message(state, batch[m]);
                                rd_kafka_message_destroy(batch[m]);
                                continue;
                        }

                        /* First delivery */
                        TEST_ASSERT(delivery_count == 1,
                                    "Expected delivery_count=1, got %d",
                                    delivery_count);

                        ack_type = determine_ack_type(config);
                        track_ack_type(state, batch[m], ack_type);

                        ack_err = rd_kafka_share_acknowledge_type(
                            state->consumers[0], batch[m], ack_type);
                        TEST_ASSERT(
                            ack_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Acknowledge failed: %s (topic=%s, partition=%d, "
                            "offset=%" PRId64 ", type=%d)",
                            rd_kafka_err2str(ack_err),
                            rd_kafka_topic_name(batch[m]->rkt),
                            batch[m]->partition, batch[m]->offset, ack_type);

                        if (state->original_cnt < 1000)
                                state->original_offsets[state->original_cnt++] =
                                    batch[m]->offset;

                        total_consumed++;
                        rd_kafka_message_destroy(batch[m]);
                }

                if (config->use_random_acks) {
                        if (total_consumed % 500 == 0 || rcvd > 0)
                                TEST_SAY(
                                    "Progress: %zu/%d (A:%d R:%d L:%d "
                                    "redeliv:%d)\n",
                                    total_consumed, state->msgs_produced,
                                    state->msgs_accepted, state->msgs_rejected,
                                    state->msgs_released,
                                    state->msgs_redelivered);
                } else {
                        TEST_SAY("Progress: %zu/%d\n", total_consumed,
                                 state->msgs_produced);
                }
        }

        state->msgs_consumed = (int)total_consumed;
        TEST_ASSERT(state->msgs_consumed == state->msgs_produced,
                    "Expected to consume %d messages, got %d",
                    state->msgs_produced, state->msgs_consumed);

        if (config->use_random_acks)
                TEST_SAY(
                    "Consumed %d: ACCEPT=%d, REJECT=%d, RELEASE=%d, "
                    "redelivered=%d\n",
                    state->msgs_consumed, state->msgs_accepted,
                    state->msgs_rejected, state->msgs_released,
                    state->msgs_redelivered);
}

/**
 * @brief Poll for redelivered messages after acknowledgements
 */
static void poll_for_redelivery(ack_test_config_t *config,
                                ack_test_state_t *state) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        int poll_timeout =
            config->poll_timeout_ms > 0 ? config->poll_timeout_ms : 3000;
        int attempts       = 10;
        int expected_count = state->msgs_released;

        /* Use higher timeout/attempts for random mode */
        if (config->use_random_acks) {
                poll_timeout = 5000;
                /* Scale attempts based on expected redeliveries */
                attempts = 100 + (expected_count / 500) * 50;
        }

        /*
         * In random mode, some redeliveries may have already been handled
         * during consume_and_acknowledge. Don't reset the counter.
         * For non-random mode, reset as before.
         */
        if (!config->use_random_acks)
                state->msgs_redelivered = 0;

        TEST_SAY(
            "Polling for redelivered messages (expecting %d, have %d)...\n",
            expected_count, state->msgs_redelivered);

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
                                int16_t delivery_count =
                                    rd_kafka_message_delivery_count(batch[m]);
                                const char *msg_topic =
                                    rd_kafka_topic_name(batch[m]->rkt);
                                int32_t msg_partition = batch[m]->partition;
                                int64_t msg_offset    = batch[m]->offset;
                                int released_idx;

                                /* Verify delivery_count >= 2 on redelivery */
                                TEST_ASSERT(
                                    delivery_count >= 2,
                                    "Expected delivery_count>=2 on redelivery, "
                                    "got %d",
                                    delivery_count);

                                /* Verify message was in released list */
                                released_idx = find_message_in_list(
                                    state->released_msgs, msg_topic,
                                    msg_partition, msg_offset);

                                TEST_ASSERT(released_idx >= 0,
                                            "Redelivered message (topic=%s, "
                                            "partition=%d, offset=%" PRId64
                                            ") was NOT in RELEASE'd list",
                                            msg_topic, msg_partition,
                                            msg_offset);

                                /* Remove from released list */
                                remove_message_from_list_at(
                                    state->released_msgs, released_idx);

                                /* Track redelivered message */
                                rktpar = rd_kafka_topic_partition_list_add(
                                    state->redelivered_msgs, msg_topic,
                                    msg_partition);
                                rktpar->offset = msg_offset;

                                state->msgs_redelivered++;

                                rd_kafka_share_acknowledge_type(
                                    state->consumers[0], batch[m],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                        }
                        rd_kafka_message_destroy(batch[m]);
                }

                if (rcvd > 0) {
                        TEST_SAY("Redelivered so far: %d/%d\n",
                                 state->msgs_redelivered, expected_count);
                }

                if (state->msgs_redelivered >= expected_count &&
                    expected_count > 0) {
                        break;
                }
        }
}

/**
 * @brief Verify redelivery results
 *
 * Verification approach:
 * - released_msgs is emptied as messages are redelivered
 * - Verify redelivered count matches expected count
 * - Verify released_msgs is empty (all RELEASE'd messages were redelivered)
 */
static void verify_results(ack_test_config_t *config, ack_test_state_t *state) {
        int expected_count = state->msgs_released;

        TEST_SAY("Verifying: consumed=%d, redelivered=%d (expected=%d)\n",
                 state->msgs_consumed, state->msgs_redelivered, expected_count);

        TEST_ASSERT(state->msgs_redelivered == expected_count,
                    "Expected %d redelivered messages, got %d", expected_count,
                    state->msgs_redelivered);

        /*
         * All RELEASE'd messages should have been redelivered and removed
         * from released_msgs. Verify the list is empty.
         */
        TEST_ASSERT(state->released_msgs->cnt == 0,
                    "Expected all RELEASE'd messages to be redelivered, "
                    "but %d remain in released list",
                    state->released_msgs->cnt);

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
        int list_capacity;

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

        /* Initialize tracking lists with larger capacity for random mode */
        list_capacity       = config->use_random_acks ? 6000 : 100;
        state.released_msgs = rd_kafka_topic_partition_list_new(list_capacity);
        state.redelivered_msgs =
            rd_kafka_topic_partition_list_new(list_capacity);

        for (i = 0; i < config->consumer_cnt; i++) {
                state.consumers[i] =
                    test_create_share_consumer(state.group_name, "explicit");
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
            .test_name          = "release-redelivery",
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 5,
            .consumer_cnt       = 1,
            .single_ack_type    = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE};
        run_ack_test(&config);
}

/**
 * @brief REJECT prevents redelivery
 */
static void test_reject_no_redelivery(void) {
        ack_test_config_t config = {.test_name  = "reject-no-redelivery",
                                    .topic_cnt  = 1,
                                    .partitions = {1},
                                    .msgs_per_partition = 5,
                                    .consumer_cnt       = 1,
                                    .single_ack_type =
                                        RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT};
        run_ack_test(&config);
}

/**
 * @brief ACCEPT prevents redelivery
 */
static void test_accept_no_redelivery(void) {
        ack_test_config_t config = {.test_name  = "accept-no-redelivery",
                                    .topic_cnt  = 1,
                                    .partitions = {1},
                                    .msgs_per_partition = 5,
                                    .consumer_cnt       = 1,
                                    .single_ack_type =
                                        RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT};
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

        rkshare = test_create_share_consumer(group, "explicit");

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

        /* fake_msg has NULL rkt, so returns STATE before checking rkshare */
        err = rd_kafka_share_acknowledge(NULL, &fake_msg);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE, "Expected STATE, got %s",
                    rd_kafka_err2str(err));

        err = rd_kafka_share_acknowledge_type(
            NULL, &fake_msg, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE, "Expected STATE, got %s",
                    rd_kafka_err2str(err));

        /* Test NULL rkshare directly via acknowledge_offset */
        err = rd_kafka_share_acknowledge_offset(
            NULL, "topic", 0, 0, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
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

        rkshare = test_create_share_consumer(group, "explicit");
        topic   = test_mk_topic_name("0172-invalid-type", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_to_topic(topic, 0, 1);

        test_alter_group_configurations(group, grp_conf, 1);

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

        rkshare = test_create_share_consumer(group, "explicit");
        topic   = test_mk_topic_name("0172-release-reject", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_to_topic(topic, 0, 5);

        test_alter_group_configurations(group, grp_conf, 1);

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
 * the broker should not attempt any redelivery.
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

        rkshare = test_create_share_consumer(group, "explicit");
        topic   = test_mk_topic_name("0172-max-delivery", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_to_topic(topic, 0, 1); /* Just 1 message */

        test_alter_group_configurations(group, grp_conf, 1);

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

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        TEST_SAY("=== test_max_delivery_attempts: PASSED ===\n");
}


/***************************************************************************
 * High-Intensity Random Acknowledgement Tests
 *
 * These tests produce large numbers of messages and apply random
 * acknowledgement types (ACCEPT/REJECT/RELEASE) to stress test the
 * acknowledgement infrastructure across different topologies.
 ***************************************************************************/

/**
 * @brief High-intensity random acks: Single topic, single partition
 *
 * 5000 messages on 1 topic with 1 partition.
 * Random ACCEPT/REJECT/RELEASE for each message.
 */
static void test_random_ack_single_topic_single_partition(void) {
        ack_test_config_t config = {.test_name    = "random-ack-1t-1p-5000msgs",
                                    .topic_cnt    = 1,
                                    .partitions   = {1},
                                    .consumer_cnt = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 5000};
        run_ack_test(&config);
}

/**
 * @brief High-intensity random acks: Multiple topics, single partition each
 *
 * 5000 messages across 4 topics, 1 partition each (~1250 msgs per topic).
 * Random ACCEPT/REJECT/RELEASE for each message.
 */
static void test_random_ack_multiple_topics_single_partition(void) {
        ack_test_config_t config = {.test_name    = "random-ack-4t-1p-5000msgs",
                                    .topic_cnt    = 4,
                                    .partitions   = {1, 1, 1, 1},
                                    .consumer_cnt = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 5000};
        run_ack_test(&config);
}

/**
 * @brief High-intensity random acks: Single topic, multiple partitions
 *
 * 5000 messages on 1 topic with 4 partitions (~1250 msgs per partition).
 * Random ACCEPT/REJECT/RELEASE for each message.
 */
static void test_random_ack_single_topic_multiple_partitions(void) {
        ack_test_config_t config = {.test_name    = "random-ack-1t-4p-5000msgs",
                                    .topic_cnt    = 1,
                                    .partitions   = {4},
                                    .consumer_cnt = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 5000};
        run_ack_test(&config);
}

/**
 * @brief High-intensity random acks: Multiple topics, multiple partitions
 *
 * 5000 messages across 2 topics, 2 partitions each (~1250 msgs per partition).
 * Random ACCEPT/REJECT/RELEASE for each message.
 */
static void test_random_ack_multiple_topics_multiple_partitions(void) {
        ack_test_config_t config = {.test_name    = "random-ack-2t-2p-5000msgs",
                                    .topic_cnt    = 2,
                                    .partitions   = {2, 2},
                                    .consumer_cnt = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 5000};
        run_ack_test(&config);
}

/**
 * @brief Scale test: 15 topics, 1 partition each, 10000 messages
 *
 * ~666 messages per topic. Tests handling many topics with random acks.
 */
static void test_scale_15_topics_single_partition(void) {
        ack_test_config_t config = {
            .test_name       = "scale-15t-1p-10000msgs",
            .topic_cnt       = 15,
            .partitions      = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            .consumer_cnt    = 1,
            .use_random_acks = rd_true,
            .total_msgs      = 10000};
        run_ack_test(&config);
}

/**
 * @brief Scale test: 15 topics, 2 partitions each, 10000 messages
 *
 * ~333 messages per partition (30 partitions total).
 * Tests handling many topics with multiple partitions.
 */
static void test_scale_15_topics_multiple_partitions(void) {
        ack_test_config_t config = {
            .test_name       = "scale-15t-2p-10000msgs",
            .topic_cnt       = 15,
            .partitions      = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
            .consumer_cnt    = 1,
            .use_random_acks = rd_true,
            .total_msgs      = 10000};
        run_ack_test(&config);
}

/**
 * @brief Scale test: 8 topics, 4 partitions each, 8000 messages
 *
 * ~250 messages per partition (32 partitions total).
 * Tests high partition count scenario.
 */
static void test_scale_8_topics_4_partitions(void) {
        ack_test_config_t config = {.test_name       = "scale-8t-4p-8000msgs",
                                    .topic_cnt       = 8,
                                    .partitions      = {4, 4, 4, 4, 4, 4, 4, 4},
                                    .consumer_cnt    = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 8000};
        run_ack_test(&config);
}

/**
 * @brief Scale test: Single topic, 8 partitions, 10000 messages
 *
 * 1250 messages per partition. Tests single topic with many partitions.
 */
static void test_scale_single_topic_8_partitions(void) {
        ack_test_config_t config = {.test_name       = "scale-1t-8p-10000msgs",
                                    .topic_cnt       = 1,
                                    .partitions      = {8},
                                    .consumer_cnt    = 1,
                                    .use_random_acks = rd_true,
                                    .total_msgs      = 10000};
        run_ack_test(&config);
}

/**
 * @brief Scale test: 10 topics, 3 partitions each, 15000 messages
 *
 * 500 messages per partition (30 partitions total).
 * Large scale test for acknowledgement infrastructure.
 */
static void test_scale_10_topics_3_partitions(void) {
        ack_test_config_t config = {
            .test_name       = "scale-10t-3p-15000msgs",
            .topic_cnt       = 10,
            .partitions      = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
            .consumer_cnt    = 1,
            .use_random_acks = rd_true,
            .total_msgs      = 15000};
        run_ack_test(&config);
}


int main_0172_share_consumer_acknowledge(int argc, char **argv) {

        test_timeout_set(600); /* 10 minutes for all tests */

        /* Create common handles for all tests */
        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        /* Core tests */
        test_release_redelivery();
        test_reject_no_redelivery();
        test_accept_no_redelivery();

        /* Error handling tests */
        test_ack_null_message();
        test_ack_null_rkshare();
        test_ack_invalid_type();
        test_release_then_reject_no_redelivery();

        /* Max delivery attempts test */
        test_max_delivery_attempts();

        /* High-intensity random acknowledgement tests */
        test_random_ack_single_topic_single_partition();
        test_random_ack_multiple_topics_single_partition();
        test_random_ack_single_topic_multiple_partitions();
        test_random_ack_multiple_topics_multiple_partitions();

        /* Scale tests - high topic/partition counts with many messages */
        test_scale_15_topics_single_partition();
        test_scale_15_topics_multiple_partitions();
        test_scale_8_topics_4_partitions();
        test_scale_single_topic_8_partitions();
        test_scale_10_topics_3_partitions();

        /* Cleanup common handles */
        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}
