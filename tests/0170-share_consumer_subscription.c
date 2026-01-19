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
        TEST_OP_END = 0,            /**< End of operations marker */
        TEST_OP_SUBSCRIBE,          /**< Subscribe to N new topics */
        TEST_OP_SUBSCRIBE_ADD,      /**< Add N topics to existing subscription */
        TEST_OP_UNSUBSCRIBE,        /**< Unsubscribe from all topics */
        TEST_OP_RESUBSCRIBE,        /**< Replace subscription with N new topics */
        TEST_OP_PRODUCE,            /**< Produce to specified topic set */
        TEST_OP_CONSUME,            /**< Consume messages */
        TEST_OP_VERIFY_SUB_CNT,     /**< Verify subscription count */
        TEST_OP_DELETE_TOPIC,       /**< Delete topic by index */
        TEST_OP_WAIT,               /**< Wait for specified milliseconds */
        TEST_OP_CREATE_CONSUMER,    /**< Create additional consumer */
        TEST_OP_POLL_NO_SUB,        /**< Poll without subscription (edge case) */
        TEST_OP_CREATE_TOPIC,       /**< Create subscribed topics that weren't created */
        TEST_OP_SUBSCRIBE_EXISTING, /**< Subscribe to already created topics */
        TEST_OP_PRODUCE_TO_TOPIC,   /**< Produce to specific topic index */
} test_op_type_t;

/**
 * @brief Flags for operations
 */
typedef enum {
        TEST_OP_F_NONE = 0,
        TEST_OP_F_SKIP_TOPIC_CREATE  = 1 << 0, /**< Don't create topics */
        TEST_OP_F_PRODUCE_TO_OLD     = 1 << 1, /**< Produce to old subscription */
        TEST_OP_F_VERIFY_NO_OLD_MSGS = 1 << 2, /**< Verify no old messages */
} test_op_flags_t;

/**
 * @brief Single operation in a test scenario
 */
typedef struct {
        test_op_type_t op;      /**< Operation type */
        int topic_cnt;          /**< Number of topics (SUBSCRIBE/RESUBSCRIBE) */
        int msgs_per_topic;     /**< Messages per topic (PRODUCE) */
        int expected_msgs;      /**< Expected message count (CONSUME, -1=any) */
        int expected_sub_cnt;   /**< Expected subscription count (VERIFY_SUB_CNT) */
        int topic_idx;          /**< Topic index (DELETE_TOPIC) */
        int wait_ms;            /**< Wait time (WAIT) */
        int consumer_idx;       /**< Consumer index (multi-consumer) */
        int repeat_cnt;         /**< Repeat count (SUBSCRIBE/UNSUBSCRIBE) */
        test_op_flags_t flags;  /**< Operation flags */
} test_op_t;

/**
 * @brief Test scenario configuration
 */
typedef struct {
        const char *name;       /**< Test name for logging */
        int consumer_cnt;       /**< Number of consumers (default: 1) */
        test_op_t ops[MAX_OPS]; /**< Operations, terminated by TEST_OP_END */
} test_scenario_t;

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


#define SUBSCRIBE(n)           {.op = TEST_OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = 1}
#define SUBSCRIBE_REPEAT(n, r) {.op = TEST_OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = (r)}
#define SUBSCRIBE_ADD(n)       {.op = TEST_OP_SUBSCRIBE_ADD, .topic_cnt = (n)}
#define SUBSCRIBE_NO_CREATE(n) {.op = TEST_OP_SUBSCRIBE, .topic_cnt = (n), .repeat_cnt = 1, \
                                .flags = TEST_OP_F_SKIP_TOPIC_CREATE}
#define UNSUBSCRIBE()          {.op = TEST_OP_UNSUBSCRIBE, .repeat_cnt = 1}
#define UNSUBSCRIBE_REPEAT(r)  {.op = TEST_OP_UNSUBSCRIBE, .repeat_cnt = (r)}
#define RESUBSCRIBE(n)         {.op = TEST_OP_RESUBSCRIBE, .topic_cnt = (n)}
#define PRODUCE(msgs)          {.op = TEST_OP_PRODUCE, .msgs_per_topic = (msgs)}
#define PRODUCE_TO_OLD(msgs)   {.op = TEST_OP_PRODUCE, .msgs_per_topic = (msgs), \
                                .flags = TEST_OP_F_PRODUCE_TO_OLD}
#define PRODUCE_TO_TOPIC(idx, msgs) {.op = TEST_OP_PRODUCE_TO_TOPIC, .topic_idx = (idx), \
                                     .msgs_per_topic = (msgs)}
#define CONSUME(expected)      {.op = TEST_OP_CONSUME, .expected_msgs = (expected)}
#define CONSUME_VERIFY_NO_OLD(expected) {.op = TEST_OP_CONSUME, .expected_msgs = (expected), \
                                         .flags = TEST_OP_F_VERIFY_NO_OLD_MSGS}
#define CONSUME_ANY()          {.op = TEST_OP_CONSUME, .expected_msgs = -1}
#define VERIFY_SUB(cnt)        {.op = TEST_OP_VERIFY_SUB_CNT, .expected_sub_cnt = (cnt)}
#define DELETE_TOPIC(idx)      {.op = TEST_OP_DELETE_TOPIC, .topic_idx = (idx)}
#define WAIT_MS(ms)            {.op = TEST_OP_WAIT, .wait_ms = (ms)}
#define CREATE_CONSUMER(idx)   {.op = TEST_OP_CREATE_CONSUMER, .consumer_idx = (idx)}
#define CREATE_TOPIC(n)        {.op = TEST_OP_CREATE_TOPIC, .topic_cnt = (n)}
#define SUBSCRIBE_EXISTING()   {.op = TEST_OP_SUBSCRIBE_EXISTING, .repeat_cnt = 1}
#define POLL_NO_SUB()          {.op = TEST_OP_POLL_NO_SUB}
#define TEST_OPS_END()         {.op = TEST_OP_END}


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
 * @brief Execute TEST_OP_SUBSCRIBE
 */
static void exec_subscribe(sub_test_state_t *state, const test_op_t *op) {
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
                    state, !(op->flags & TEST_OP_F_SKIP_TOPIC_CREATE));
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
 * @brief Execute TEST_OP_SUBSCRIBE_ADD (incremental - add to existing subscription)
 */
static void exec_subscribe_add(sub_test_state_t *state, const test_op_t *op) {
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
        state->sub_start_idx[cidx] = new_start - state->sub_count[cidx];
        state->sub_count[cidx] += op->topic_cnt;

        rd_kafka_topic_partition_list_destroy(tlist);
}

/**
 * @brief Execute TEST_OP_CREATE_TOPIC (create topics that weren't created)
 */
static void exec_create_topic(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_SUBSCRIBE_EXISTING (subscribe to all created topics)
 */
static void exec_subscribe_existing(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_PRODUCE_TO_TOPIC (produce to specific topic by index)
 */
static void exec_produce_to_topic(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_RESUBSCRIBE (replace subscription with new topics)
 */
static void exec_resubscribe(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_UNSUBSCRIBE
 */
static void exec_unsubscribe(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_PRODUCE
 */
static void exec_produce(sub_test_state_t *state, const test_op_t *op) {
        int cidx = op->consumer_idx;
        int start_idx, count, i;

        if (op->flags & TEST_OP_F_PRODUCE_TO_OLD) {
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
 * @brief Execute TEST_OP_CONSUME
 */
static void exec_consume(sub_test_state_t *state, const test_op_t *op) {
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

                if (op->flags & TEST_OP_F_VERIFY_NO_OLD_MSGS) {
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
 * @brief Execute TEST_OP_VERIFY_SUB_CNT
 */
static void exec_verify_sub_cnt(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_DELETE_TOPIC
 */
static void exec_delete_topic(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_WAIT
 */
static void exec_wait(sub_test_state_t *state, const test_op_t *op) {
        TEST_SAY("  WAIT: %d ms\n", op->wait_ms);
        rd_sleep(op->wait_ms / 1000);
        if (op->wait_ms % 1000)
                rd_usleep((op->wait_ms % 1000) * 1000, NULL);
}

/**
 * @brief Execute TEST_OP_CREATE_CONSUMER
 */
static void exec_create_consumer(sub_test_state_t *state, const test_op_t *op) {
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
 * @brief Execute TEST_OP_POLL_NO_SUB
 */
static void exec_poll_no_sub(sub_test_state_t *state, const test_op_t *op) {
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
static void state_init(sub_test_state_t *state, const test_scenario_t *scenario) {
        int i;

        memset(state, 0, sizeof(*state));

        rd_snprintf(state->group_name, sizeof(state->group_name),
                    "share-%s", scenario->name);

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
static void do_test_scenario(const test_scenario_t *scenario) {
        sub_test_state_t state;
        int op_idx;

        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("=== %s ===\n", scenario->name);
        TEST_SAY("============================================================\n");

        state_init(&state, scenario);

        /* Execute operations */
        for (op_idx = 0; scenario->ops[op_idx].op != TEST_OP_END; op_idx++) {
                const test_op_t *op = &scenario->ops[op_idx];

                switch (op->op) {
                case TEST_OP_SUBSCRIBE:
                        exec_subscribe(&state, op);
                        break;
                case TEST_OP_SUBSCRIBE_ADD:
                        exec_subscribe_add(&state, op);
                        break;
                case TEST_OP_RESUBSCRIBE:
                        exec_resubscribe(&state, op);
                        break;
                case TEST_OP_UNSUBSCRIBE:
                        exec_unsubscribe(&state, op);
                        break;
                case TEST_OP_PRODUCE:
                        exec_produce(&state, op);
                        break;
                case TEST_OP_PRODUCE_TO_TOPIC:
                        exec_produce_to_topic(&state, op);
                        break;
                case TEST_OP_CONSUME:
                        exec_consume(&state, op);
                        break;
                case TEST_OP_VERIFY_SUB_CNT:
                        exec_verify_sub_cnt(&state, op);
                        break;
                case TEST_OP_DELETE_TOPIC:
                        exec_delete_topic(&state, op);
                        break;
                case TEST_OP_WAIT:
                        exec_wait(&state, op);
                        break;
                case TEST_OP_CREATE_CONSUMER:
                        exec_create_consumer(&state, op);
                        break;
                case TEST_OP_CREATE_TOPIC:
                        exec_create_topic(&state, op);
                        break;
                case TEST_OP_SUBSCRIBE_EXISTING:
                        exec_subscribe_existing(&state, op);
                        break;
                case TEST_OP_POLL_NO_SUB:
                        exec_poll_no_sub(&state, op);
                        break;
                default:
                        TEST_FAIL("Unknown operation: %d", op->op);
                }
        }

        state_cleanup(&state);

        TEST_SAY("=== %s: PASSED ===\n", scenario->name);
}


/**
 * Basic subscription tests
 */
static const test_scenario_t test_single_subscribe = {
        .name = "single-subscribe",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                VERIFY_SUB(2),
                CONSUME(10),
                TEST_OPS_END()
        }
};

static const test_scenario_t test_single_unsubscribe = {
        .name = "single-unsubscribe",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                CONSUME(10),
                UNSUBSCRIBE(),
                VERIFY_SUB(0),
                TEST_OPS_END()
        }
};

static const test_scenario_t test_repeated_subscribe = {
        .name = "repeated-subscribe-no-duplicates",
        .ops = {
                SUBSCRIBE_REPEAT(2, 3),  /* Subscribe 3 times to same topics */
                PRODUCE(5),
                VERIFY_SUB(2),           /* Should still be 2, not 6 */
                CONSUME(10),
                TEST_OPS_END()
        }
};

static const test_scenario_t test_repeated_unsubscribe = {
        .name = "repeated-unsubscribe-no-error",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(5),
                CONSUME(10),
                UNSUBSCRIBE_REPEAT(3),   /* Unsubscribe 3 times */
                VERIFY_SUB(0),
                TEST_OPS_END()
        }
};

/**
 * Subscription replacement tests
 */
static const test_scenario_t test_topic_switch = {
        .name = "topic-switch",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(10),
                CONSUME_ANY(),
                RESUBSCRIBE(2),          /* Switch to 2 new topics */
                PRODUCE(10),             /* Produce to new topics */
                PRODUCE_TO_OLD(5),       /* Produce to old topics */
                CONSUME_VERIFY_NO_OLD(20), /* Should only get new topic msgs */
                TEST_OPS_END()
        }
};

static const test_scenario_t test_incremental_subscription = {
        .name = "incremental-subscription",
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
                TEST_OPS_END()
        }
};

/**
 * Edge case tests
 */
static const test_scenario_t test_subscribe_before_topic_exists = {
        .name = "subscribe-before-topic-exists",
        .ops = {
                SUBSCRIBE_NO_CREATE(1),  /* Subscribe without creating topic */
                CREATE_TOPIC(0),         /* Now create the subscribed topic */
                PRODUCE(5),              /* Produce to the topic */
                CONSUME(5),              /* Should receive all messages */
                TEST_OPS_END()
        }
};

static const test_scenario_t test_poll_empty_topic = {
        .name = "poll-empty-topic",
        .ops = {
                SUBSCRIBE(1),
                /* Don't produce - topic is empty */
                CONSUME(0),              /* Should return 0, not error */
                TEST_OPS_END()
        }
};

static const test_scenario_t test_poll_no_subscription = {
        .name = "poll-no-subscription",
        .ops = {
                POLL_NO_SUB(),           /* Poll without subscribing */
                TEST_OPS_END()
        }
};

static const test_scenario_t test_poll_after_unsubscribe = {
        .name = "poll-after-unsubscribe",
        .ops = {
                SUBSCRIBE(1),
                PRODUCE(5),
                CONSUME_ANY(),           /* Consume some */
                UNSUBSCRIBE(),
                POLL_NO_SUB(),           /* Poll after unsubscribe */
                TEST_OPS_END()
        }
};

/**
 * Topic deletion tests
 */
static const test_scenario_t test_topic_deletion = {
        .name = "topic-deletion-while-subscribed",
        .ops = {
                SUBSCRIBE(2),
                PRODUCE(10),
                CONSUME_ANY(),
                DELETE_TOPIC(1),         /* Delete second topic */
                WAIT_MS(3000),
                PRODUCE_TO_TOPIC(0, 5),  /* Produce to remaining topic */
                CONSUME_ANY(),           /* Continue consuming from remaining */
                TEST_OPS_END()
        }
};

/**
 * Stress tests
 */
static const test_scenario_t test_rapid_updates = {
        .name = "rapid-subscription-updates",
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
                TEST_OPS_END()
        }
};

/**
 * @brief Multi-consumer overlap test (standalone)
 *
 * Two consumers in same group with overlapping subscriptions:
 * - Consumer 0: [shared, c0_only]
 * - Consumer 1: [shared, c1_only]
 *
 * This test verifies share group consumers can have overlapping
 * subscriptions and both receive messages from shared topics.
 */
static void do_test_multi_consumer_overlap(void) {
        const char *group = test_mk_topic_name("share-overlap", 1);
        char *shared  = rd_strdup(test_mk_topic_name("0170-shared", 1));
        char *c0_only = rd_strdup(test_mk_topic_name("0170-c0only", 1));
        char *c1_only = rd_strdup(test_mk_topic_name("0170-c1only", 1));
        const char *c0_topics[] = {shared, c0_only};
        const char *c1_topics[] = {shared, c1_only};
        rd_kafka_share_t *rkshare0, *rkshare1;
        int c0_cnt = 0, c1_cnt = 0;
        int attempts;
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};

        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("=== multi-consumer-overlapping-subscriptions ===\n");
        TEST_SAY("============================================================\n");

        /* Create topics */
        test_create_topic_wait_exists(NULL, shared, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, c0_only, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, c1_only, 1, -1, 30000);

        /* Produce messages */
        test_produce_msgs_easy(shared, 0, 0, 20);
        test_produce_msgs_easy(c0_only, 0, 0, 10);
        test_produce_msgs_easy(c1_only, 0, 0, 10);

        /* Create consumers */
        rkshare0 = test_create_share_consumer(group);
        rkshare1 = test_create_share_consumer(group);

        /* Set group offset */
        test_IncrementalAlterConfigs_simple(
            test_share_consumer_get_rk(rkshare0),
            RD_KAFKA_RESOURCE_GROUP, group, cfg, 1);

        /* Subscribe with overlapping topics */
        test_share_consumer_subscribe_multi(rkshare0, 2, shared, c0_only);
        test_share_consumer_subscribe_multi(rkshare1, 2, shared, c1_only);

        /* Consume - alternate between consumers */
        attempts = 20;
        while ((c0_cnt + c1_cnt) < 10 && attempts-- > 0) {
                int batch_cnt = 0;
                int ret;

                ret = test_share_consume_batch(rkshare0, 2000, c0_topics, 2,
                                               &batch_cnt);
                TEST_ASSERT(ret >= 0, "C0 wrong topic");
                c0_cnt += batch_cnt;

                batch_cnt = 0;
                ret = test_share_consume_batch(rkshare1, 2000, c1_topics, 2,
                                               &batch_cnt);
                TEST_ASSERT(ret >= 0, "C1 wrong topic");
                c1_cnt += batch_cnt;
        }

        TEST_SAY("C0: %d, C1: %d (total: %d)\n", c0_cnt, c1_cnt, c0_cnt + c1_cnt);
        TEST_ASSERT(c0_cnt > 0 || c1_cnt > 0, "no messages received");

        /* Cleanup */
        rd_kafka_share_consumer_close(rkshare0);
        rd_kafka_share_consumer_close(rkshare1);
        rd_kafka_share_destroy(rkshare0);
        rd_kafka_share_destroy(rkshare1);

        rd_free(shared);
        rd_free(c0_only);
        rd_free(c1_only);

        TEST_SAY("=== multi-consumer-overlapping-subscriptions: PASSED ===\n");
}


int main_0170_share_consumer_subscription(int argc, char **argv) {

        /* Basic subscription tests */
        do_test_scenario(&test_single_subscribe);
        do_test_scenario(&test_single_unsubscribe);
        do_test_scenario(&test_repeated_subscribe);
        do_test_scenario(&test_repeated_unsubscribe);

        /* Subscription replacement tests */
        do_test_scenario(&test_topic_switch);
        do_test_scenario(&test_incremental_subscription);

        /* Edge case tests */
        do_test_scenario(&test_subscribe_before_topic_exists);
        do_test_scenario(&test_poll_empty_topic);
        do_test_scenario(&test_poll_no_subscription);
        do_test_scenario(&test_poll_after_unsubscribe);

        /* Topic deletion tests */
        do_test_scenario(&test_topic_deletion);

        /* Stress tests */
        do_test_scenario(&test_rapid_updates);

        /* Multi-consumer tests (standalone - requires shared topics) */
        do_test_multi_consumer_overlap();

        return 0;
}
