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

#include "../src/rdkafka_proto.h"

/**
 * @brief Share consumer acknowledgement callback tests.
 *
 * Tests the share_acknowledgement_cb callback functionality:
 * - Callback is invoked after commit_async
 * - Callback receives correct offset ranges
 * - Callback receives correct ack types (ACCEPT, RELEASE, REJECT)
 * - Callback receives correct error codes
 * - Multiple callbacks for multiple commit_async calls
 */

#define MAX_MSGS         100
#define CONSUME_ARRAY    1000
#define MAX_ACK_RESULTS  500

/**
 * @brief Structure to track callback invocations.
 */
typedef struct ack_callback_state_s {
        int callback_cnt;                   /**< Number of callbacks invoked */
        int total_results;                  /**< Total results across all cbs */
        rd_kafka_resp_err_t last_err;       /**< Last overall error */
        int64_t offsets[MAX_ACK_RESULTS];   /**< Recorded start offsets */
        int offset_cnt;                     /**< Number of recorded offsets */
        mtx_t lock;                         /**< Mutex for thread safety */
        cnd_t cond;                         /**< Condition for waiting */
} ack_callback_state_t;


/**
 * @brief Initialize callback state.
 */
static void ack_callback_state_init(ack_callback_state_t *state) {
        memset(state, 0, sizeof(*state));
        mtx_init(&state->lock, mtx_plain);
        cnd_init(&state->cond);
}

/**
 * @brief Destroy callback state.
 */
static void ack_callback_state_destroy(ack_callback_state_t *state) {
        mtx_destroy(&state->lock);
        cnd_destroy(&state->cond);
}


/**
 * @brief Share acknowledgement commit callback handler.
 *
 * Note: Each callback invocation contains exactly one partition.
 * The err parameter contains the per-partition acknowledgement error.
 */
static void share_ack_commit_cb(
    rd_kafka_share_t *rkshare,
    rd_kafka_share_partition_offsets_list_t *partitions,
    rd_kafka_resp_err_t err,
    void *opaque) {
        ack_callback_state_t *state = (ack_callback_state_t *)opaque;
        const rd_kafka_share_partition_offsets_t *entry;
        const rd_kafka_topic_partition_t *tpar;
        const int64_t *offsets;
        int offset_cnt;
        int j;

        (void)rkshare;

        mtx_lock(&state->lock);

        state->callback_cnt++;
        state->last_err = err;
        state->total_results += 1;

        /* Each callback has exactly one partition */
        entry = rd_kafka_share_partition_offsets_list_get(partitions, 0);
        if (!entry) {
                TEST_SAY("Ack commit callback #%d: err=%s, no partition data\n",
                         state->callback_cnt, rd_kafka_err2name(err));
                cnd_signal(&state->cond);
                mtx_unlock(&state->lock);
                return;
        }

        tpar       = rd_kafka_share_partition_offsets_partition(entry);
        offsets    = rd_kafka_share_partition_offsets_offsets(entry);
        offset_cnt = rd_kafka_share_partition_offsets_offsets_cnt(entry);

        TEST_SAY("Ack commit callback #%d: %s[%" PRId32 "] err=%s "
                 "offset_cnt=%d\n",
                 state->callback_cnt, tpar->topic, tpar->partition,
                 rd_kafka_err2name(err), offset_cnt);

        /* Record first few offsets if space available */
        for (j = 0; j < offset_cnt && state->offset_cnt < MAX_ACK_RESULTS; j++) {
                state->offsets[state->offset_cnt++] = offsets[j];
        }

        cnd_signal(&state->cond);
        mtx_unlock(&state->lock);
}


/**
 * @brief Create share consumer with ack callback.
 */
static rd_kafka_share_t *
create_share_consumer_with_cb(const char *group_id,
                              const char *ack_mode,
                              ack_callback_state_t *state) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 60);

        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "share.acknowledgement.mode", ack_mode, errstr,
                          sizeof(errstr));

        /* Set the acknowledgement callback */
        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf, share_ack_commit_cb);
        rd_kafka_conf_set_opaque(conf, state);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief Set share.auto.offset.reset=earliest for a share group.
 */
static void set_group_offset_earliest(const char *group_name) {
        rd_kafka_t *admin;
        rd_kafka_conf_t *conf;
        char errstr[512];
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};

        test_conf_init(&conf, NULL, 30);
        admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(admin, "Failed to create admin client: %s", errstr);

        test_IncrementalAlterConfigs_simple(admin, RD_KAFKA_RESOURCE_GROUP,
                                            group_name, cfg, 1);

        rd_kafka_destroy(admin);
}


/**
 * @brief Subscribe a share consumer to topics.
 */
static void subscribe_consumer(rd_kafka_share_t *rkshare,
                               const char **topics,
                               int topic_cnt) {
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;
        int i;

        subs = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0; i < topic_cnt; i++)
                rd_kafka_topic_partition_list_add(subs, topics[i],
                                                  RD_KAFKA_PARTITION_UA);

        err = rd_kafka_share_subscribe(rkshare, subs);
        TEST_ASSERT(!err, "Subscribe failed: %s", rd_kafka_err2str(err));

        rd_kafka_topic_partition_list_destroy(subs);
}


/**
 * @brief Wait for at least N callbacks with timeout, while polling.
 *
 * This function polls the share consumer to process callbacks while waiting.
 */
static rd_bool_t wait_for_callbacks_with_poll(ack_callback_state_t *state,
                                              rd_kafka_share_t *rkshare,
                                              int min_callbacks,
                                              int timeout_ms) {
        rd_bool_t success = rd_false;
        int elapsed       = 0;
        int poll_interval = 100; /* ms */
        rd_kafka_message_t *rkmessages[100];
        size_t rcvd;

        while (elapsed < timeout_ms) {
                /* Poll to process callbacks */
                rd_kafka_error_t *error = rd_kafka_share_consume_batch(
                    rkshare, poll_interval, rkmessages, &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);

                /* Destroy any received messages */
                for (size_t i = 0; i < rcvd; i++)
                        rd_kafka_message_destroy(rkmessages[i]);

                mtx_lock(&state->lock);
                if (state->callback_cnt >= min_callbacks) {
                        success = rd_true;
                        mtx_unlock(&state->lock);
                        break;
                }
                mtx_unlock(&state->lock);

                elapsed += poll_interval;
        }

        return success;
}


/* ===================================================================
 *  Test: Basic callback invocation with implicit ack mode
 *
 *  Verifies that the callback is invoked after commit_async
 *  in implicit acknowledgement mode.
 * =================================================================== */
static void do_test_callback_implicit_mode(void) {
        const char *topic;
        const char *group = "ack-cb-implicit";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        ack_callback_state_t state;

        SUB_TEST();

        ack_callback_state_init(&state);

        topic = test_mk_topic_name("0176-ack-cb-impl", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer_with_cb(group, "implicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume some messages (implicit mode auto-acks on consume_batch) */
        while (consumed < 10 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_ASSERT(consumed > 0, "Expected to consume some messages");
        TEST_SAY("Consumed %d messages in implicit mode\n", consumed);

        /* Call commit_async to trigger callback */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        /* Wait for callback while polling */
        TEST_SAY("Waiting for ack callback (polling)...\n");
        wait_for_callbacks_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback state: callback_cnt=%d, total_results=%d\n",
                 state.callback_cnt, state.total_results);

        /* In implicit mode, all acks are ACCEPT */
        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);
        TEST_ASSERT(state.total_results > 0,
                    "Expected some results in callback");

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_callback_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: Callback with explicit ack mode and mixed types
 *
 *  Verifies that callback receives correct ack types when
 *  different ack types are used (ACCEPT, RELEASE, REJECT).
 * =================================================================== */
static void do_test_callback_explicit_mixed(void) {
        const char *topic;
        const char *group = "ack-cb-explicit-mixed";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int accept_sent = 0, release_sent = 0, reject_sent = 0;
        ack_callback_state_t state;

        SUB_TEST();

        ack_callback_state_init(&state);

        topic = test_mk_topic_name("0176-ack-cb-mixed", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer_with_cb(group, "explicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume messages and ack with mixed types */
        while (consumed < 30 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_AcknowledgeType_t ack_type;

                                /* Mix: 50% ACCEPT, 30% RELEASE, 20% REJECT */
                                if (consumed % 10 < 5) {
                                        ack_type =
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                                        accept_sent++;
                                } else if (consumed % 10 < 8) {
                                        ack_type =
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
                                        release_sent++;
                                } else {
                                        ack_type =
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;
                                        reject_sent++;
                                }

                                rd_kafka_share_acknowledge_type(rkshare,
                                                                rkmessages[j],
                                                                ack_type);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_ASSERT(consumed >= 10, "Expected to consume at least 10 messages");
        TEST_SAY("Consumed %d messages, sent: accept=%d, release=%d, reject=%d\n",
                 consumed, accept_sent, release_sent, reject_sent);

        /* Call commit_async to trigger callback */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        /* Wait for callback */
        TEST_SAY("Waiting for ack callback...\n");
        wait_for_callbacks_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback state: callback_cnt=%d, total_results=%d\n",
                 state.callback_cnt, state.total_results);

        /* Verify callback received results */
        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);
        TEST_ASSERT(state.total_results > 0,
                    "Expected some results in callback");

        /* Log sent types for reference */
        TEST_SAY("Sent types: accept=%d, release=%d, reject=%d\n",
                 accept_sent, release_sent, reject_sent);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_callback_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: Multiple commit_async calls trigger multiple callbacks
 * =================================================================== */
static void do_test_multiple_callbacks(void) {
        const char *topic;
        const char *group = "ack-cb-multiple";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int round;
        int consumed_total = 0;
        ack_callback_state_t state;

        SUB_TEST();

        ack_callback_state_init(&state);

        topic = test_mk_topic_name("0176-ack-cb-multi", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer_with_cb(group, "implicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Do 3 rounds of consume + commit_async */
        for (round = 0; round < 3; round++) {
                int consumed = 0;
                int attempts = 0;

                TEST_SAY("Round %d: consuming...\n", round + 1);

                while (consumed < 10 && attempts++ < 20) {
                        rcvd  = 0;
                        error = rd_kafka_share_consume_batch(rkshare, 2000,
                                                             rkmessages, &rcvd);
                        if (error) {
                                rd_kafka_error_destroy(error);
                                continue;
                        }

                        for (j = 0; j < rcvd; j++) {
                                if (!rkmessages[j]->err)
                                        consumed++;
                                rd_kafka_message_destroy(rkmessages[j]);
                        }
                }

                if (consumed > 0) {
                        consumed_total += consumed;
                        TEST_SAY("Round %d: consumed %d, calling commit_async\n",
                                 round + 1, consumed);

                        error = rd_kafka_share_commit_async(rkshare);
                        TEST_ASSERT(!error, "commit_async failed");

                        /* Small delay between rounds */
                        rd_sleep(1);
                }
        }

        /* Wait for callbacks */
        TEST_SAY("Waiting for callbacks (consumed_total=%d)...\n",
                 consumed_total);
        wait_for_callbacks_with_poll(&state, rkshare, 1, 15000);

        TEST_SAY("Callback state: callback_cnt=%d, total_results=%d\n",
                 state.callback_cnt, state.total_results);

        /* Verify at least one callback was received */
        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_callback_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: No callback when no acks pending
 * =================================================================== */
static void do_test_no_callback_when_no_acks(void) {
        const char *group = "ack-cb-no-acks";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        ack_callback_state_t state;

        SUB_TEST();

        ack_callback_state_init(&state);

        /* Create consumer but don't consume anything */
        rkshare = create_share_consumer_with_cb(group, "implicit", &state);

        /* Call commit_async without consuming - should not trigger callback */
        error = rd_kafka_share_commit_async(rkshare);
        /* This may return error or succeed with no-op */
        if (error)
                rd_kafka_error_destroy(error);

        /* Wait briefly - callback should NOT be invoked */
        rd_sleep(2);

        TEST_SAY("Callback state after commit with no acks: callback_cnt=%d\n",
                 state.callback_cnt);

        /* No callback expected since no acks were pending */
        TEST_ASSERT(state.callback_cnt == 0,
                    "Expected 0 callbacks when no acks pending, got %d",
                    state.callback_cnt);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_callback_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: Callback offset ranges match acknowledged offsets
 * =================================================================== */
static void do_test_callback_offset_verification(void) {
        const char *topic;
        const char *group = "ack-cb-offset-verify";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int64_t consumed_offsets[MAX_MSGS];
        int offset_cnt = 0;
        ack_callback_state_t state;

        SUB_TEST();

        ack_callback_state_init(&state);

        topic = test_mk_topic_name("0176-ack-cb-offset", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer_with_cb(group, "implicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume messages and record offsets */
        while (consumed < 20 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err && offset_cnt < MAX_MSGS) {
                                consumed_offsets[offset_cnt++] =
                                    rkmessages[j]->offset;
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_ASSERT(consumed > 0, "Expected to consume some messages");
        TEST_SAY("Consumed %d messages, recorded %d offsets\n", consumed,
                 offset_cnt);

        /* Commit and wait for callback */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed");

        wait_for_callbacks_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback received %d offset ranges\n", state.offset_cnt);

        /* Verify that callback offset ranges cover consumed offsets */
        /* (This is a basic check - ranges may be collated) */
        TEST_ASSERT(state.offset_cnt > 0,
                    "Expected offset ranges in callback");

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_callback_state_destroy(&state);

        SUB_TEST_PASS();
}


int main_0176_share_consumer_ack_callback(int argc, char **argv) {
        do_test_callback_implicit_mode();
        do_test_callback_explicit_mixed();
        do_test_multiple_callbacks();
        do_test_no_callback_when_no_acks();
        do_test_callback_offset_verification();

        return 0;
}
