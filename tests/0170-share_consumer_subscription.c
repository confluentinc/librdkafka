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
#include "testshared.h"
#include "rdkafka.h"

/**
 * @brief Share consumer subscription tests using operation-based framework.
 *
 * This test file uses a declarative, operation-based framework where tests
 * are defined as sequences of operations. The framework handles:
 * - Automatic topic name generation
 * - Topic creation and deletion
 * - Consumer creation and destruction
 * - Message production and consumption
 * - Subscription verification
 */


#define MAX_TOPICS    20
#define MAX_CONSUMERS 4
#define MAX_OPS       50

/**
 * @brief Operation types for subscription tests
 */
typedef enum {
        OP_END = 0,           /**< End of operations marker */
        OP_SUBSCRIBE,         /**< Subscribe to N new topics */
        OP_SUBSCRIBE_ADD,     /**< Add N topics to existing subscription */
        OP_SUBSCRIBE_MASK,    /**< Subscribe to topics by bitmask */
        OP_UNSUBSCRIBE,       /**< Unsubscribe from all topics */
        OP_RESUBSCRIBE,       /**< Replace subscription with N new topics */
        OP_PRODUCE,           /**< Produce to specified topic set */
        OP_PRODUCE_MASK,      /**< Produce to topics by bitmask */
        OP_CONSUME,           /**< Consume messages */
        OP_CONSUME_MASK,      /**< Consume from topics by bitmask */
        OP_VERIFY_SUB_CNT,    /**< Verify subscription count */
        OP_DELETE_TOPIC,      /**< Delete topic by index */
        OP_WAIT,              /**< Wait for specified milliseconds */
        OP_CREATE_CONSUMER,   /**< Create additional consumer */
        OP_POLL_NO_SUB,       /**< Poll without subscription (edge case) */
        OP_CREATE_TOPIC,      /**< Create subscribed topics that weren't created */
        OP_CREATE_TOPICS_ONLY,/**< Create N new topics (no subscription) */
        OP_SUBSCRIBE_EXISTING,/**< Subscribe to already created topics */
        OP_PRODUCE_TO_TOPIC,  /**< Produce to specific topic index */
} sub_op_type_t;

/**
 * @brief Flags for operations
 */
typedef enum {
        OP_FLAG_NONE = 0,
        OP_FLAG_SKIP_TOPIC_CREATE  = 1 << 0, /**< Don't create topics (subscribe before exists) */
        OP_FLAG_PRODUCE_TO_OLD     = 1 << 1, /**< Produce to previously subscribed topics */
        OP_FLAG_VERIFY_NO_OLD_MSGS = 1 << 2, /**< Verify no messages from old subscription */
} sub_op_flags_t;

/**
 * @brief Single operation in a test scenario
 */
typedef struct {
        sub_op_type_t op;       /**< Operation type */
        int topic_cnt;          /**< Number of topics (SUBSCRIBE/RESUBSCRIBE) */
        int msgs_per_topic;     /**< Messages per topic (PRODUCE) */
        int expected_msgs;      /**< Expected message count (CONSUME, -1=any) */
        int expected_sub_cnt;   /**< Expected subscription count (VERIFY_SUB_CNT) */
        int topic_idx;          /**< Topic index (DELETE_TOPIC) */
        int wait_ms;            /**< Wait time (WAIT) */
        int consumer_idx;       /**< Consumer index (multi-consumer) */
        int repeat_cnt;         /**< Repeat count (SUBSCRIBE/UNSUBSCRIBE) */
        sub_op_flags_t flags;   /**< Operation flags */
        unsigned int topic_mask;/**< Bitmask of topic indices (SUBSCRIBE_MASK) */
} sub_op_t;

/**
 * @brief Test scenario configuration
 */
typedef struct {
        const char *test_name;  /**< Test name for logging */
        int consumer_cnt;       /**< Number of consumers (default: 1) */
        sub_op_t ops[MAX_OPS];  /**< Operations, terminated by OP_END */
} sub_scenario_t;

/**
 * @brief Runtime state for test execution
 */
typedef struct {
        /* Consumers */
        rd_kafka_share_t *consumers[MAX_CONSUMERS];
        int consumer_cnt;

        /* Topics: all created topics */
        char *all_topics[MAX_TOPICS];
        int all_topic_cnt;
        int msgs_produced[MAX_TOPICS]; /**< Messages produced per topic */

        /* Current subscription tracking per consumer */
        int sub_start_idx[MAX_CONSUMERS]; /**< Start index in all_topics */
        int sub_count[MAX_CONSUMERS];     /**< Count of subscribed topics */

        /* Previous subscription (for RESUBSCRIBE verification) */
        int old_sub_start_idx;
        int old_sub_count;

        /* Group name */
        char group_name[128];
} sub_test_state_t;


#define SUBSCRIBE(n)         {.op = OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = 1}
#define SUBSCRIBE_REPEAT(n, r) {.op = OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = (r)}
#define SUBSCRIBE_ADD(n)     {.op = OP_SUBSCRIBE_ADD, .topic_cnt = (n)}
#define SUBSCRIBE_NO_CREATE(n) {.op = OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = 1, \
                                .flags = OP_FLAG_SKIP_TOPIC_CREATE}
#define UNSUBSCRIBE()        {.op = OP_UNSUBSCRIBE, .repeat_cnt = 1}
#define UNSUBSCRIBE_REPEAT(r) {.op = OP_UNSUBSCRIBE, .repeat_cnt = (r)}
#define RESUBSCRIBE(n)       {.op = OP_RESUBSCRIBE, .topic_cnt = (n)}
#define PRODUCE(msgs)        {.op = OP_PRODUCE, .msgs_per_topic = (msgs)}
#define PRODUCE_TO_OLD(msgs) {.op = OP_PRODUCE, .msgs_per_topic = (msgs), \
                              .flags = OP_FLAG_PRODUCE_TO_OLD}
#define PRODUCE_TO_TOPIC(idx, msgs) {.op = OP_PRODUCE_TO_TOPIC, .topic_idx = (idx), \
                                     .msgs_per_topic = (msgs)}
#define CONSUME(expected)    {.op = OP_CONSUME, .expected_msgs = (expected)}
#define CONSUME_VERIFY_NO_OLD(expected) {.op = OP_CONSUME, .expected_msgs = (expected), \
                                         .flags = OP_FLAG_VERIFY_NO_OLD_MSGS}
#define CONSUME_ANY()        {.op = OP_CONSUME, .expected_msgs = -1}
#define VERIFY_SUB(cnt)      {.op = OP_VERIFY_SUB_CNT, .expected_sub_cnt = (cnt)}
#define DELETE_TOPIC(idx)    {.op = OP_DELETE_TOPIC, .topic_idx = (idx)}
#define WAIT_MS(ms)          {.op = OP_WAIT, .wait_ms = (ms)}
#define CREATE_CONSUMER(idx) {.op = OP_CREATE_CONSUMER, .consumer_idx = (idx)}
#define CREATE_TOPIC(n)      {.op = OP_CREATE_TOPIC, .topic_cnt = (n)}
#define CREATE_TOPICS_ONLY(n) {.op = OP_CREATE_TOPICS_ONLY, .topic_cnt = (n)}
#define SUBSCRIBE_EXISTING() {.op = OP_SUBSCRIBE_EXISTING, .repeat_cnt = 1}
#define POLL_NO_SUB()        {.op = OP_POLL_NO_SUB}
#define END_OPS()            {.op = OP_END}

/* Bitmask-based operations for complex multi-consumer scenarios
 * topic_mask: bit N set = include topic N (e.g., 0x05 = topics 0 and 2) */
#define SUBSCRIBE_MASK(mask) {.op = OP_SUBSCRIBE_MASK, .topic_mask = (mask)}
#define PRODUCE_MASK(mask, msgs) {.op = OP_PRODUCE_MASK, .topic_mask = (mask), \
                                  .msgs_per_topic = (msgs)}
#define CONSUME_MASK(mask, expected) {.op = OP_CONSUME_MASK, .topic_mask = (mask), \
                                      .expected_msgs = (expected)}

/* Multi-consumer variants */
#define SUBSCRIBE_C(c, n)    {.op = OP_SUBSCRIBE, .topic_cnt = (n), .consumer_idx = (c), .repeat_cnt = 1}
#define SUBSCRIBE_ADD_C(c, n) {.op = OP_SUBSCRIBE_ADD, .topic_cnt = (n), .consumer_idx = (c)}
#define SUBSCRIBE_MASK_C(c, mask) {.op = OP_SUBSCRIBE_MASK, .consumer_idx = (c), .topic_mask = (mask)}
#define SUBSCRIBE_EXISTING_C(c) {.op = OP_SUBSCRIBE_EXISTING, .consumer_idx = (c), .repeat_cnt = 1}
#define UNSUBSCRIBE_C(c)     {.op = OP_UNSUBSCRIBE, .consumer_idx = (c), .repeat_cnt = 1}
#define PRODUCE_C(c, msgs)   {.op = OP_PRODUCE, .msgs_per_topic = (msgs), .consumer_idx = (c)}
#define PRODUCE_MASK_C(c, mask, msgs) {.op = OP_PRODUCE_MASK, .consumer_idx = (c), \
                                       .topic_mask = (mask), .msgs_per_topic = (msgs)}
#define CONSUME_C(c, expected) {.op = OP_CONSUME, .expected_msgs = (expected), .consumer_idx = (c)}
#define CONSUME_MASK_C(c, mask, expected) {.op = OP_CONSUME_MASK, .consumer_idx = (c), \
                                           .topic_mask = (mask), .expected_msgs = (expected)}
#define VERIFY_SUB_C(c, cnt) {.op = OP_VERIFY_SUB_CNT, .expected_sub_cnt = (cnt), .consumer_idx = (c)}


/**
 * @brief Set group config to earliest offset
 */
static void state_set_offset_earliest(sub_test_state_t *state) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(state->consumers[0]),
            RD_KAFKA_RESOURCE_GROUP, state->group_name, cfg, 1);
}

/**
 * @brief Create a new topic with auto-generated name
 */
static const char *state_create_topic(sub_test_state_t *state,
                                      rd_bool_t wait_exists) {
        char name[128];

        TEST_ASSERT(state->all_topic_cnt < MAX_TOPICS,
                    "Too many topics created");

        rd_snprintf(name, sizeof(name), "0170-t%d", state->all_topic_cnt);
        state->all_topics[state->all_topic_cnt] =
            rd_strdup(test_mk_topic_name(name, 1));

        if (wait_exists) {
                test_create_topic_wait_exists(
                    NULL, state->all_topics[state->all_topic_cnt], 1, -1, 30000);
        }

        state->msgs_produced[state->all_topic_cnt] = 0;
        return state->all_topics[state->all_topic_cnt++];
}

/**
 * @brief Execute OP_SUBSCRIBE
 */
static void exec_subscribe(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_topic_partition_list_t *tlist;
        int cidx = op->consumer_idx;
        int i, r;

        TEST_SAY("  SUBSCRIBE: %d topic(s), repeat=%d, consumer=%d\n",
                 op->topic_cnt, op->repeat_cnt, cidx);

        /* Save old subscription for RESUBSCRIBE verification */
        state->old_sub_start_idx = state->sub_start_idx[cidx];
        state->old_sub_count     = state->sub_count[cidx];

        /* Track new subscription */
        state->sub_start_idx[cidx] = state->all_topic_cnt;
        state->sub_count[cidx]     = op->topic_cnt;

        /* Create topics and build subscription list */
        tlist = rd_kafka_topic_partition_list_new(op->topic_cnt);
        for (i = 0; i < op->topic_cnt; i++) {
                const char *topic = state_create_topic(
                    state, !(op->flags & OP_FLAG_SKIP_TOPIC_CREATE));
                rd_kafka_topic_partition_list_add(tlist, topic,
                                                  RD_KAFKA_PARTITION_UA);
        }

        /* Subscribe (possibly multiple times) */
        for (r = 0; r < op->repeat_cnt; r++) {
                TEST_CALL_ERR__(
                    rd_kafka_share_subscribe(state->consumers[cidx], tlist));
        }

        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute OP_SUBSCRIBE_ADD (incremental - add to existing subscription)
 */
static void exec_subscribe_add(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_topic_partition_list_t *tlist;
        int cidx = op->consumer_idx;
        int i;
        int new_start = state->all_topic_cnt;

        TEST_SAY("  SUBSCRIBE_ADD: adding %d topic(s) to existing %d, consumer=%d\n",
                 op->topic_cnt, state->sub_count[cidx], cidx);

        /* Build subscription list including existing + new topics */
        tlist = rd_kafka_topic_partition_list_new(state->sub_count[cidx] + op->topic_cnt);

        /* Add existing subscribed topics */
        for (i = 0; i < state->sub_count[cidx]; i++) {
                int idx = state->sub_start_idx[cidx] + i;
                rd_kafka_topic_partition_list_add(tlist, state->all_topics[idx],
                                                  RD_KAFKA_PARTITION_UA);
        }

        /* Create and add new topics */
        for (i = 0; i < op->topic_cnt; i++) {
                const char *topic = state_create_topic(state, rd_true);
                rd_kafka_topic_partition_list_add(tlist, topic,
                                                  RD_KAFKA_PARTITION_UA);
        }

        TEST_CALL_ERR__(rd_kafka_share_subscribe(state->consumers[cidx], tlist));

        /* Update subscription tracking - topics are now spread across ranges */
        /* For simplicity, we track the new contiguous range */
        state->sub_start_idx[cidx] = new_start - state->sub_count[cidx];
        state->sub_count[cidx] += op->topic_cnt;

        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute OP_CREATE_TOPIC (create topics that exist in state but weren't created)
 */
static void exec_create_topic(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        int i;

        TEST_SAY("  CREATE_TOPIC: creating subscribed topics for consumer=%d\n", cidx);

        /* Create the topics that were subscribed to but not yet created */
        for (i = 0; i < state->sub_count[cidx]; i++) {
                int idx = state->sub_start_idx[cidx] + i;
                if (state->all_topics[idx]) {
                        test_create_topic_wait_exists(NULL, state->all_topics[idx],
                                                      1, -1, 30000);
                }
        }
}

/**
 * @brief Execute OP_CREATE_TOPICS_ONLY (create N new topics without subscribing)
 */
static void exec_create_topics_only(sub_test_state_t *state, const sub_op_t *op) {
        int i;

        TEST_SAY("  CREATE_TOPICS_ONLY: creating %d topic(s)\n", op->topic_cnt);

        for (i = 0; i < op->topic_cnt; i++) {
                state_create_topic(state, rd_true);
        }
}

/**
 * @brief Helper: count bits set in mask
 */
static int popcount(unsigned int mask) {
        int count = 0;
        while (mask) {
                count += mask & 1;
                mask >>= 1;
        }
        return count;
}

/**
 * @brief Execute OP_SUBSCRIBE_MASK (subscribe to topics by bitmask)
 */
static void exec_subscribe_mask(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_topic_partition_list_t *tlist;
        int cidx = op->consumer_idx;
        int i, cnt = 0;

        TEST_SAY("  SUBSCRIBE_MASK: mask=0x%x, consumer=%d\n",
                 op->topic_mask, cidx);

        tlist = rd_kafka_topic_partition_list_new(popcount(op->topic_mask));

        for (i = 0; i < state->all_topic_cnt && i < 32; i++) {
                if (op->topic_mask & (1u << i)) {
                        rd_kafka_topic_partition_list_add(tlist, state->all_topics[i],
                                                          RD_KAFKA_PARTITION_UA);
                        cnt++;
                }
        }

        TEST_CALL_ERR__(rd_kafka_share_subscribe(state->consumers[cidx], tlist));

        /* Update tracking - store mask info for this consumer */
        state->sub_start_idx[cidx] = 0;  /* Mask-based, starts from 0 */
        state->sub_count[cidx] = cnt;

        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute OP_PRODUCE_MASK (produce to topics by bitmask)
 */
static void exec_produce_mask(sub_test_state_t *state, const sub_op_t *op) {
        int i;

        TEST_SAY("  PRODUCE_MASK: mask=0x%x, %d msgs/topic\n",
                 op->topic_mask, op->msgs_per_topic);

        for (i = 0; i < state->all_topic_cnt && i < 32; i++) {
                if (op->topic_mask & (1u << i)) {
                        test_produce_msgs_easy(state->all_topics[i], 0, 0,
                                               op->msgs_per_topic);
                        state->msgs_produced[i] += op->msgs_per_topic;
                }
        }
}

/**
 * @brief Execute OP_CONSUME_MASK (consume from topics by bitmask)
 */
static void exec_consume_mask(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        const char *topics[MAX_TOPICS];
        int i, cnt = 0, consumed;

        /* Build topics array from mask */
        for (i = 0; i < state->all_topic_cnt && i < 32; i++) {
                if (op->topic_mask & (1u << i)) {
                        topics[cnt++] = state->all_topics[i];
                }
        }

        if (op->expected_msgs >= 0) {
                TEST_SAY("  CONSUME_MASK: mask=0x%x, expecting %d msgs\n",
                         op->topic_mask, op->expected_msgs);
                consumed = test_share_consume_msgs(
                    state->consumers[cidx], op->expected_msgs, 25, 3000,
                    cnt > 0 ? topics : NULL, cnt);
                TEST_ASSERT(consumed == op->expected_msgs,
                            "Expected %d messages, got %d",
                            op->expected_msgs, consumed);
        } else {
                TEST_SAY("  CONSUME_MASK: mask=0x%x, any available\n",
                         op->topic_mask);
                test_share_consume_msgs(state->consumers[cidx], 100, 15, 3000,
                                        cnt > 0 ? topics : NULL, cnt);
        }
}

/**
 * @brief Execute OP_SUBSCRIBE_EXISTING (subscribe to all created but unsubscribed topics)
 */
static void exec_subscribe_existing(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_topic_partition_list_t *tlist;
        int cidx = op->consumer_idx;
        int i;

        TEST_SAY("  SUBSCRIBE_EXISTING: %d topic(s), consumer=%d\n",
                 state->all_topic_cnt, cidx);

        tlist = rd_kafka_topic_partition_list_new(state->all_topic_cnt);

        for (i = 0; i < state->all_topic_cnt; i++) {
                rd_kafka_topic_partition_list_add(tlist, state->all_topics[i],
                                                  RD_KAFKA_PARTITION_UA);
        }

        TEST_CALL_ERR__(rd_kafka_share_subscribe(state->consumers[cidx], tlist));

        state->sub_start_idx[cidx] = 0;
        state->sub_count[cidx] = state->all_topic_cnt;

        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute OP_PRODUCE_TO_TOPIC (produce to specific topic by index)
 *
 * topic_idx is relative to current subscription of consumer 0
 */
static void exec_produce_to_topic(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        int idx = state->sub_start_idx[cidx] + op->topic_idx;

        TEST_ASSERT(op->topic_idx < state->sub_count[cidx],
                    "Topic index %d out of range (sub_count=%d)",
                    op->topic_idx, state->sub_count[cidx]);

        TEST_SAY("  PRODUCE_TO_TOPIC: %d msgs to topic[%d] (%s)\n",
                 op->msgs_per_topic, op->topic_idx, state->all_topics[idx]);

        test_produce_msgs_easy(state->all_topics[idx], 0, 0, op->msgs_per_topic);
        state->msgs_produced[idx] += op->msgs_per_topic;
}

/**
 * @brief Execute OP_RESUBSCRIBE (replace subscription with new topics)
 */
static void exec_resubscribe(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_topic_partition_list_t *tlist;
        int cidx = op->consumer_idx;
        int i;

        TEST_SAY("  RESUBSCRIBE: %d new topic(s), consumer=%d\n",
                 op->topic_cnt, cidx);

        /* Save old subscription */
        state->old_sub_start_idx = state->sub_start_idx[cidx];
        state->old_sub_count     = state->sub_count[cidx];

        /* Track new subscription */
        state->sub_start_idx[cidx] = state->all_topic_cnt;
        state->sub_count[cidx]     = op->topic_cnt;

        /* Create new topics */
        tlist = rd_kafka_topic_partition_list_new(op->topic_cnt);
        for (i = 0; i < op->topic_cnt; i++) {
                const char *topic = state_create_topic(state, rd_true);
                rd_kafka_topic_partition_list_add(tlist, topic,
                                                  RD_KAFKA_PARTITION_UA);
        }

        TEST_CALL_ERR__(rd_kafka_share_subscribe(state->consumers[cidx], tlist));
        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute OP_UNSUBSCRIBE
 */
static void exec_unsubscribe(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        int r;

        TEST_SAY("  UNSUBSCRIBE: repeat=%d, consumer=%d\n",
                 op->repeat_cnt, cidx);

        for (r = 0; r < op->repeat_cnt; r++) {
                TEST_CALL_ERR__(rd_kafka_share_unsubscribe(state->consumers[cidx]));
        }

        state->sub_count[cidx] = 0;
}

/**
 * @brief Execute OP_PRODUCE
 */
static void exec_produce(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        int start_idx, count, i;

        if (op->flags & OP_FLAG_PRODUCE_TO_OLD) {
                start_idx = state->old_sub_start_idx;
                count     = state->old_sub_count;
                TEST_SAY("  PRODUCE: %d msgs/topic to OLD %d topic(s)\n",
                         op->msgs_per_topic, count);
        } else {
                start_idx = state->sub_start_idx[cidx];
                count     = state->sub_count[cidx];
                TEST_SAY("  PRODUCE: %d msgs/topic to %d topic(s)\n",
                         op->msgs_per_topic, count);
        }

        for (i = 0; i < count; i++) {
                int idx = start_idx + i;
                test_produce_msgs_easy(state->all_topics[idx], 0, 0,
                                       op->msgs_per_topic);
                state->msgs_produced[idx] += op->msgs_per_topic;
        }
}

/**
 * @brief Execute OP_CONSUME
 */
static void exec_consume(sub_test_state_t *state, const sub_op_t *op) {
        int cidx      = op->consumer_idx;
        int start_idx = state->sub_start_idx[cidx];
        int count     = state->sub_count[cidx];
        const char *topics[MAX_TOPICS];
        int i, consumed;

        /* Build expected topics array */
        for (i = 0; i < count; i++) {
                topics[i] = state->all_topics[start_idx + i];
        }

        if (op->expected_msgs >= 0) {
                TEST_SAY("  CONSUME: expecting %d msgs from %d topic(s)\n",
                         op->expected_msgs, count);
                consumed = test_share_consume_msgs(
                    state->consumers[cidx], op->expected_msgs, 25, 3000,
                    count > 0 ? topics : NULL, count);

                if (op->flags & OP_FLAG_VERIFY_NO_OLD_MSGS) {
                        TEST_ASSERT(consumed >= 0,
                                    "Received message from old subscription!");
                }
                TEST_ASSERT(consumed == op->expected_msgs,
                            "Expected %d messages, got %d",
                            op->expected_msgs, consumed);
        } else {
                /* Consume any available */
                TEST_SAY("  CONSUME: any available from %d topic(s)\n", count);
                test_share_consume_msgs(state->consumers[cidx], 100, 10, 2000,
                                        count > 0 ? topics : NULL, count);
        }
}

/**
 * @brief Execute OP_VERIFY_SUB_CNT
 */
static void exec_verify_sub_cnt(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        rd_kafka_topic_partition_list_t *sub;

        TEST_SAY("  VERIFY_SUB_CNT: expecting %d, consumer=%d\n",
                 op->expected_sub_cnt, cidx);

        sub = test_get_subscription(state->consumers[cidx]);
        TEST_ASSERT(sub->cnt == op->expected_sub_cnt,
                    "Expected %d subscriptions, got %d",
                    op->expected_sub_cnt, sub->cnt);
        rd_kafka_topic_partition_list_destroy(sub);
}

/**
 * @brief Execute OP_DELETE_TOPIC
 *
 * topic_idx is relative to current subscription of consumer 0
 */
static void exec_delete_topic(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;
        int idx = state->sub_start_idx[cidx] + op->topic_idx;

        TEST_ASSERT(op->topic_idx < state->sub_count[cidx],
                    "Topic index %d out of range (sub_count=%d)",
                    op->topic_idx, state->sub_count[cidx]);

        TEST_SAY("  DELETE_TOPIC: index %d (%s)\n",
                 op->topic_idx, state->all_topics[idx]);

        test_delete_topic(test_share_consumer_get_rk(state->consumers[0]),
                          state->all_topics[idx]);
}

/**
 * @brief Execute OP_WAIT
 */
static void exec_wait(sub_test_state_t *state, const sub_op_t *op) {
        TEST_SAY("  WAIT: %d ms\n", op->wait_ms);
        rd_sleep(op->wait_ms / 1000);
        if (op->wait_ms % 1000)
                rd_usleep((op->wait_ms % 1000) * 1000, NULL);
}

/**
 * @brief Execute OP_CREATE_CONSUMER
 */
static void exec_create_consumer(sub_test_state_t *state, const sub_op_t *op) {
        int cidx = op->consumer_idx;

        TEST_SAY("  CREATE_CONSUMER: index %d\n", cidx);

        TEST_ASSERT(cidx < MAX_CONSUMERS, "Consumer index out of range");
        TEST_ASSERT(state->consumers[cidx] == NULL,
                    "Consumer %d already exists", cidx);

        state->consumers[cidx] = test_create_share_consumer(state->group_name);
        if (cidx >= state->consumer_cnt)
                state->consumer_cnt = cidx + 1;
}

/**
 * @brief Execute OP_POLL_NO_SUB
 */
static void exec_poll_no_sub(sub_test_state_t *state, const sub_op_t *op) {
        rd_kafka_message_t *batch[TEST_SHARE_BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0;
        int cidx = op->consumer_idx;

        TEST_SAY("  POLL_NO_SUB: consumer=%d\n", cidx);

        err = rd_kafka_share_consume_batch(state->consumers[cidx], 2000,
                                           batch, &rcvd);
        /* TODO KIP-932: Should return error */
        if (err)
                rd_kafka_error_destroy(err);
}

/**
 * @brief Initialize test state
 */
static void state_init(sub_test_state_t *state, const sub_scenario_t *scenario) {
        int i;

        memset(state, 0, sizeof(*state));

        rd_snprintf(state->group_name, sizeof(state->group_name),
                    "share-%s", scenario->test_name);

        state->consumer_cnt = scenario->consumer_cnt > 0 ? scenario->consumer_cnt : 1;

        /* Create initial consumers */
        for (i = 0; i < state->consumer_cnt; i++) {
                state->consumers[i] = test_create_share_consumer(state->group_name);
        }

        /* Set group offset to earliest */
        state_set_offset_earliest(state);
}

/**
 * @brief Cleanup test state
 */
static void state_cleanup(sub_test_state_t *state) {
        int i;

        /* Delete all created topics */
        for (i = 0; i < state->all_topic_cnt; i++) {
                if (state->all_topics[i]) {
                        test_delete_topic(
                            test_share_consumer_get_rk(state->consumers[0]),
                            state->all_topics[i]);
                        rd_free(state->all_topics[i]);
                }
        }

        /* Destroy all consumers */
        for (i = 0; i < MAX_CONSUMERS; i++) {
                if (state->consumers[i]) {
                        rd_kafka_share_consumer_close(state->consumers[i]);
                        rd_kafka_share_destroy(state->consumers[i]);
                }
        }
}

/**
 * @brief Run a test scenario
 */
static void run_scenario(const sub_scenario_t *scenario) {
        sub_test_state_t state;
        int op_idx;

        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("=== %s ===\n", scenario->test_name);
        TEST_SAY("============================================================\n");

        state_init(&state, scenario);

        /* Execute operations */
        for (op_idx = 0; scenario->ops[op_idx].op != OP_END; op_idx++) {
                const sub_op_t *op = &scenario->ops[op_idx];

                switch (op->op) {
                case OP_SUBSCRIBE:
                        exec_subscribe(&state, op);
                        break;
                case OP_SUBSCRIBE_ADD:
                        exec_subscribe_add(&state, op);
                        break;
                case OP_SUBSCRIBE_MASK:
                        exec_subscribe_mask(&state, op);
                        break;
                case OP_RESUBSCRIBE:
                        exec_resubscribe(&state, op);
                        break;
                case OP_UNSUBSCRIBE:
                        exec_unsubscribe(&state, op);
                        break;
                case OP_PRODUCE:
                        exec_produce(&state, op);
                        break;
                case OP_PRODUCE_MASK:
                        exec_produce_mask(&state, op);
                        break;
                case OP_PRODUCE_TO_TOPIC:
                        exec_produce_to_topic(&state, op);
                        break;
                case OP_CONSUME:
                        exec_consume(&state, op);
                        break;
                case OP_CONSUME_MASK:
                        exec_consume_mask(&state, op);
                        break;
                case OP_VERIFY_SUB_CNT:
                        exec_verify_sub_cnt(&state, op);
                        break;
                case OP_DELETE_TOPIC:
                        exec_delete_topic(&state, op);
                        break;
                case OP_WAIT:
                        exec_wait(&state, op);
                        break;
                case OP_CREATE_CONSUMER:
                        exec_create_consumer(&state, op);
                        break;
                case OP_CREATE_TOPIC:
                        exec_create_topic(&state, op);
                        break;
                case OP_CREATE_TOPICS_ONLY:
                        exec_create_topics_only(&state, op);
                        break;
                case OP_SUBSCRIBE_EXISTING:
                        exec_subscribe_existing(&state, op);
                        break;
                case OP_POLL_NO_SUB:
                        exec_poll_no_sub(&state, op);
                        break;
                default:
                        TEST_FAIL("Unknown operation: %d", op->op);
                }
        }

        state_cleanup(&state);

        TEST_SAY("=== %s: PASSED ===\n", scenario->test_name);
}

/**
 * Basic subscription tests
 */
static const sub_scenario_t test_single_subscribe = {
        .test_name = "single-subscribe",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                VERIFY_SUB(2),
                CONSUME(10),
                END_OPS()
        }
};

static const sub_scenario_t test_single_unsubscribe = {
        .test_name = "single-unsubscribe",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                CONSUME(10),
                UNSUBSCRIBE(),
                VERIFY_SUB(0),
                END_OPS()
        }
};

static const sub_scenario_t test_repeated_subscribe = {
        .test_name = "repeated-subscribe-no-duplicates",
        .ops = {
                SUBSCRIBE_REPEAT(2, 3),  /* Subscribe 3 times to same topics */
                PRODUCE(5),
                VERIFY_SUB(2),           /* Should still be 2, not 6 */
                CONSUME(10),
                END_OPS()
        }
};

static const sub_scenario_t test_repeated_unsubscribe = {
        .test_name = "repeated-unsubscribe-no-error",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                CONSUME(10),
                UNSUBSCRIBE_REPEAT(3),   /* Unsubscribe 3 times */
                VERIFY_SUB(0),
                END_OPS()
        }
};

/**
 * Subscription replacement tests
 */
static const sub_scenario_t test_topic_switch = {
        .test_name = "topic-switch",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(10),
                CONSUME_ANY(),
                RESUBSCRIBE(2),          /* Switch to 2 new topics */
                PRODUCE(10),             /* Produce to new topics */
                PRODUCE_TO_OLD(5),       /* Produce to old topics */
                CONSUME_VERIFY_NO_OLD(20), /* Should only get new topic msgs */
                END_OPS()
        }
};

static const sub_scenario_t test_incremental_subscription = {
        .test_name = "incremental-subscription",
        .ops = {
                /* Start with 1 topic */
                SUBSCRIBE(1),
                PRODUCE(10),
                VERIFY_SUB(1),
                CONSUME(10),
                /* Add 1 more topic (now 2 total) */
                SUBSCRIBE_ADD(1),
                PRODUCE(10),
                VERIFY_SUB(2),
                CONSUME(20),             /* 10 from each of 2 topics */
                /* Add 1 more topic (now 3 total) */
                SUBSCRIBE_ADD(1),
                PRODUCE(10),
                VERIFY_SUB(3),
                CONSUME(30),             /* 10 from each of 3 topics */
                END_OPS()
        }
};

/**
 * Edge case tests
 */
static const sub_scenario_t test_subscribe_before_topic_exists = {
        .test_name = "subscribe-before-topic-exists",
        .ops = {
                SUBSCRIBE_NO_CREATE(1),  /* Subscribe without creating topic */
                CREATE_TOPIC(0),         /* Now create the subscribed topic */
                PRODUCE(5),              /* Produce to the topic */
                CONSUME(5),              /* Should receive all messages */
                END_OPS()
        }
};

static const sub_scenario_t test_poll_empty_topic = {
        .test_name = "poll-empty-topic",
        .ops = {
                SUBSCRIBE(1),
                /* Don't produce - topic is empty */
                CONSUME(0),              /* Should return 0, not error */
                END_OPS()
        }
};

static const sub_scenario_t test_poll_no_subscription = {
        .test_name = "poll-no-subscription",
        .ops = {
                POLL_NO_SUB(),           /* Poll without subscribing */
                END_OPS()
        }
};

static const sub_scenario_t test_poll_after_unsubscribe = {
        .test_name = "poll-after-unsubscribe",
        .ops = {
                SUBSCRIBE(1),
                PRODUCE(5),
                CONSUME_ANY(),           /* Consume some */
                UNSUBSCRIBE(),
                POLL_NO_SUB(),           /* Poll after unsubscribe - should not return old msgs */
                END_OPS()
        }
};

/**
 * Topic deletion tests
 */
static const sub_scenario_t test_topic_deletion = {
        .test_name = "topic-deletion-while-subscribed",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(10),
                CONSUME_ANY(),
                DELETE_TOPIC(1),         /* Delete second topic */
                WAIT_MS(3000),
                PRODUCE_TO_TOPIC(0, 5),  /* Produce to remaining topic */
                CONSUME_ANY(),           /* Continue consuming from remaining */
                END_OPS()
        }
};

/**
 * Stress tests
 */
static const sub_scenario_t test_rapid_updates = {
        .test_name = "rapid-subscription-updates",
        .ops = {
                SUBSCRIBE(2),
                RESUBSCRIBE(1),
                RESUBSCRIBE(3),
                UNSUBSCRIBE(),
                SUBSCRIBE(2),
                RESUBSCRIBE(2),
                UNSUBSCRIBE(),
                SUBSCRIBE(3),
                VERIFY_SUB(3),
                END_OPS()
        }
};

/**
 * Multi-consumer overlap test (framework-based)
 *
 * Two consumers in same group with overlapping subscriptions:
 * - Topic 0: shared (subscribed by both)
 * - Topic 1: c0_only (only consumer 0)
 * - Topic 2: c1_only (only consumer 1)
 *
 * Consumer 0 subscribes to: topics 0, 1 (mask 0x03)
 * Consumer 1 subscribes to: topics 0, 2 (mask 0x05)
 */
static const sub_scenario_t test_multi_consumer_overlap = {
        .test_name = "multi-consumer-overlapping-subscriptions",
        .consumer_cnt = 2,
        .ops = {
                /* Create 3 topics: shared(0), c0_only(1), c1_only(2) */
                CREATE_TOPICS_ONLY(3),

                /* Produce to all topics */
                PRODUCE_MASK(0x01, 20),  /* 20 msgs to shared (topic 0) */
                PRODUCE_MASK(0x02, 10),  /* 10 msgs to c0_only (topic 1) */
                PRODUCE_MASK(0x04, 10),  /* 10 msgs to c1_only (topic 2) */

                /* Consumer 0: subscribe to topics 0,1 (shared + c0_only) */
                SUBSCRIBE_MASK_C(0, 0x03),

                /* Consumer 1: subscribe to topics 0,2 (shared + c1_only) */
                SUBSCRIBE_MASK_C(1, 0x05),

                /* Both consume from their subscriptions */
                CONSUME_MASK_C(0, 0x03, -1),  /* C0: any from topics 0,1 */
                CONSUME_MASK_C(1, 0x05, -1),  /* C1: any from topics 0,2 */

                VERIFY_SUB_C(0, 2),
                VERIFY_SUB_C(1, 2),
                END_OPS()
        }
};


/*============================================================================
 * Main Entry Point
 *============================================================================*/

int main_0170_share_consumer_subscription(int argc, char **argv) {

        /* Basic subscription tests */
        run_scenario(&test_single_subscribe);
        run_scenario(&test_single_unsubscribe);
        run_scenario(&test_repeated_subscribe);
        run_scenario(&test_repeated_unsubscribe);

        /* Subscription replacement tests */
        run_scenario(&test_topic_switch);
        run_scenario(&test_incremental_subscription);

        /* Edge case tests */
        run_scenario(&test_subscribe_before_topic_exists);
        run_scenario(&test_poll_empty_topic);
        run_scenario(&test_poll_no_subscription);
        run_scenario(&test_poll_after_unsubscribe);

        /* Topic deletion tests */
        run_scenario(&test_topic_deletion);

        /* Stress tests */
        run_scenario(&test_rapid_updates);

        /* Multi-consumer tests */
        run_scenario(&test_multi_consumer_overlap);

        return 0;
}
