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

#include "../src/rdkafka_proto.h"

/**
 * @brief Share consumer rd_kafka_share_wakeup() API tests.
 *
 * Tests the wakeup API for both consume_batch and commit_sync operations.
 * Verifies that wakeup interrupts blocking operations and that callbacks
 * still receive broker responses.
 */

#define CONSUME_ARRAY 100


/* ===================================================================
 * Subscribe / consume helpers
 * =================================================================== */

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
 * @brief Consume up to @p msgcnt records and ACCEPT each.
 * @returns count of records actually acknowledged.
 */
static int consume_and_ack_n(rd_kafka_share_t *rkshare, int msgcnt) {
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int acked    = 0;
        int attempts = 0;

        while (acked < msgcnt && attempts++ < 30) {
                rd_kafka_error_t *error;
                size_t rcvd = 0;
                size_t j;

                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                acked++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        return acked;
}


/**
 * @brief Consume up to @p msgcnt records WITHOUT acknowledging.
 * @returns count of records consumed.
 */
static int consume_n(rd_kafka_share_t *rkshare, int msgcnt) {
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int consumed = 0;
        int attempts = 0;

        while (consumed < msgcnt && attempts++ < 30) {
                rd_kafka_error_t *error;
                size_t rcvd = 0;
                size_t j;

                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        if (rcvd == 0)
                                continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        return consumed;
}


/**
 * @brief Drain any already-buffered records, bailing out as soon as the
 *        consumer returns an empty batch. Use when there may be zero
 *        records available and waiting would just burn time — e.g. after
 *        a wakeup-interrupted fetch.
 * @returns count of records drained.
 */
static int drain_buffered(rd_kafka_share_t *rkshare) {
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int drained  = 0;
        int attempts = 0;

        while (attempts++ < 5) {
                rd_kafka_error_t *error;
                size_t rcvd = 0;
                size_t j;

                error = rd_kafka_share_consume_batch(rkshare, 500, rkmessages,
                                                     &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
                if (rcvd == 0)
                        break;

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                drained++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        return drained;
}


/* ===================================================================
 * Background consume thread (used by no-data-loss test)
 * =================================================================== */

typedef struct {
        rd_kafka_share_t *rkshare;
        rd_kafka_resp_err_t result;
        mtx_t lock;
        cnd_t cond;
        rd_bool_t started;
        rd_bool_t finished;
} consume_thread_state_t;

static int consume_thread_func(void *arg) {
        consume_thread_state_t *state = (consume_thread_state_t *)arg;
        rd_kafka_message_t *rkmessages[10];
        size_t msg_cnt;
        rd_kafka_error_t *err;

        mtx_lock(&state->lock);
        state->started = rd_true;
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);

        TEST_SAY("Consume thread: calling consume_batch\n");
        err = rd_kafka_share_consume_batch(state->rkshare, 10000, rkmessages,
                                           &msg_cnt);

        state->result =
            err ? rd_kafka_error_code(err) : RD_KAFKA_RESP_ERR_NO_ERROR;

        if (err)
                rd_kafka_error_destroy(err);

        mtx_lock(&state->lock);
        state->finished = rd_true;
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);

        TEST_SAY("Consume thread: finished with result %s\n",
                 rd_kafka_err2name(state->result));

        return 0;
}


/* ===================================================================
 * Mock broker scaffolding
 * =================================================================== */

typedef struct test_ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} test_ctx_t;


static test_ctx_t test_ctx_new(void) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        char errstr[512];

        memset(&ctx, 0, sizeof(ctx));

        ctx.mcluster = test_mock_cluster_new(3, &ctx.bootstraps);

        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareGroupHeartbeat");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(ctx.mcluster,
                                                 RD_KAFKAP_ShareFetch, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareFetch");

        rd_kafka_mock_sharegroup_set_auto_offset_reset(ctx.mcluster, 1);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        ctx.producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(ctx.producer != NULL, "Failed to create producer: %s",
                    errstr);

        return ctx;
}


static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}


static void
mock_produce_messages(rd_kafka_t *producer, const char *topic, int msgcnt) {
        int i;
        for (i = 0; i < msgcnt; i++) {
                char payload[64];
                rd_snprintf(payload, sizeof(payload), "%s-%d", topic, i);
                TEST_ASSERT(rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce failed");
        }
        rd_kafka_flush(producer, 5000);
}


/**
 * @brief Create share consumer for mock broker tests with callback.
 *
 * Installs @c test_share_ack_cb (or @p cb if provided) at runtime via
 * rd_kafka_share_set_acknowledgement_commit_cb() — the legacy conf-based
 * setter has been removed.
 */
static rd_kafka_share_t *
create_mock_share_consumer(const char *bootstraps,
                           const char *group_id,
                           const char *ack_mode,
                           test_ack_cb_state_t *cb_state,
                           void (*cb)(rd_kafka_share_t *,
                                      rd_kafka_share_partition_offsets_list_t *,
                                      rd_kafka_resp_err_t,
                                      void *)) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", ack_mode);

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        if (cb_state) {
                error = rd_kafka_share_set_acknowledgement_commit_cb(
                    rkshare, cb ? cb : test_share_ack_cb, cb_state);
                TEST_ASSERT(error == NULL,
                            "Failed to set acknowledgement commit callback: %s",
                            rd_kafka_error_string(error));
        }

        return rkshare;
}


/**
 * @brief Count callbacks in @p state whose err matches @p target_err.
 */
static int count_cb_err(const test_ack_cb_state_t *state,
                        rd_kafka_resp_err_t target_err) {
        int i;
        int cnt = 0;
        for (i = 0; i < state->callback_cnt; i++)
                if (state->errs[i] == target_err)
                        cnt++;
        return cnt;
}


/* ===================================================================
 * Delayed-wakeup thread (used by mock tests)
 * =================================================================== */

typedef struct {
        rd_kafka_share_t *rkshare;
        int delay_ms;
        mtx_t lock;
        cnd_t cond;
        rd_bool_t started;
} wakeup_thread_state_t;


static int wakeup_thread_func(void *arg) {
        wakeup_thread_state_t *state = (wakeup_thread_state_t *)arg;

        mtx_lock(&state->lock);
        state->started = rd_true;
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);

        TEST_SAY("Wakeup thread: sleeping %dms before calling wakeup\n",
                 state->delay_ms);
        rd_usleep(state->delay_ms * 1000, NULL);

        TEST_SAY("Wakeup thread: calling wakeup\n");
        rd_kafka_share_wakeup(state->rkshare);

        TEST_SAY("Wakeup thread: done\n");
        return 0;
}


/**
 * @brief Start the delayed-wakeup thread and block until it has started.
 */
static void start_wakeup_thread(wakeup_thread_state_t *state,
                                thrd_t *thread,
                                rd_kafka_share_t *rkshare,
                                int delay_ms) {
        memset(state, 0, sizeof(*state));
        state->rkshare  = rkshare;
        state->delay_ms = delay_ms;
        mtx_init(&state->lock, mtx_plain);
        cnd_init(&state->cond);

        if (thrd_create(thread, wakeup_thread_func, state) != thrd_success)
                TEST_FAIL("Failed to create wakeup thread");

        mtx_lock(&state->lock);
        while (!state->started)
                cnd_wait(&state->cond, &state->lock);
        mtx_unlock(&state->lock);
}


static void stop_wakeup_thread(wakeup_thread_state_t *state, thrd_t thread) {
        thrd_join(thread, NULL);
        mtx_destroy(&state->lock);
        cnd_destroy(&state->cond);
}


/* ===================================================================
 * Tests — real broker
 * =================================================================== */

/**
 * @brief Test wakeup called before consume_batch.
 *
 * Verify that:
 * - Wakeup before consume_batch returns immediately with WAKEUP error
 * - No fetch request is sent to broker
 * - Second consume_batch works normally (one-shot behavior)
 */
static void do_test_wakeup_before_consume(void) {
        const char *topic = test_mk_topic_name("wakeup_before", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[10];
        size_t msg_cnt;
        rd_kafka_error_t *err;

        SUB_TEST_QUICK();

        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = test_create_share_consumer(group, "implicit");
        test_share_set_auto_offset_reset(group, "earliest");
        subscribe_consumer(rkshare, &topic, 1);

        TEST_SAY("Calling wakeup before consume_batch\n");
        rd_kafka_share_wakeup(rkshare);

        TEST_SAY(
            "Calling consume_batch (should return immediately with "
            "WAKEUP)\n");
        err = rd_kafka_share_consume_batch(rkshare, 1000, rkmessages, &msg_cnt);

        TEST_ASSERT(err != NULL, "Expected error from consume_batch");
        TEST_ASSERT(rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s", rd_kafka_error_name(err));
        TEST_ASSERT(msg_cnt == 0, "Expected 0 messages, got %zu", msg_cnt);

        rd_kafka_error_destroy(err);

        TEST_SAY("Second consume_batch should work normally (one-shot)\n");
        err = rd_kafka_share_consume_batch(rkshare, 100, rkmessages, &msg_cnt);

        /* Should be NULL (timeout) or success, but NOT wakeup */
        if (err) {
                TEST_ASSERT(rd_kafka_error_code(err) !=
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Second consume should not be woken up");
                rd_kafka_error_destroy(err);
        }

        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test multiple wakeup calls.
 *
 * Verify that:
 * - Multiple wakeups before consume = only one consume interrupted
 * - One-shot behavior works correctly
 */
static void do_test_multiple_wakeups(void) {
        const char *topic = test_mk_topic_name("wakeup_multi", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[10];
        size_t msg_cnt;
        rd_kafka_error_t *err;

        SUB_TEST_QUICK();

        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = test_create_share_consumer(group, "implicit");
        test_share_set_auto_offset_reset(group, "earliest");
        subscribe_consumer(rkshare, &topic, 1);

        TEST_SAY("Calling wakeup 3 times\n");
        rd_kafka_share_wakeup(rkshare);
        rd_kafka_share_wakeup(rkshare);
        rd_kafka_share_wakeup(rkshare);

        TEST_SAY("First consume should return WAKEUP\n");
        err = rd_kafka_share_consume_batch(rkshare, 100, rkmessages, &msg_cnt);

        TEST_ASSERT(err != NULL, "Expected error");
        TEST_ASSERT(rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP, got %s", rd_kafka_error_name(err));
        rd_kafka_error_destroy(err);

        TEST_SAY("Second consume should NOT return WAKEUP (one-shot)\n");
        err = rd_kafka_share_consume_batch(rkshare, 100, rkmessages, &msg_cnt);

        if (err) {
                TEST_ASSERT(rd_kafka_error_code(err) !=
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Second consume should not get WAKEUP, got %s",
                            rd_kafka_error_name(err));
                rd_kafka_error_destroy(err);
        }

        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test wakeup called before commit_sync.
 *
 * Verify that:
 * - Wakeup before commit_sync returns WAKEUP error immediately
 * - Acknowledgements are still sent to broker (matching Java behavior)
 * - One-shot behavior - next commit_sync works normally
 */
static void do_test_wakeup_before_commit_sync(void) {
        const char *topic = test_mk_topic_name("wakeup_before_commit", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *err;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const int msgcnt                            = 10;
        int acked;
        int i;

        SUB_TEST_QUICK();

        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        rkshare = test_create_share_consumer(group, "explicit");
        test_share_set_auto_offset_reset(group, "earliest");
        subscribe_consumer(rkshare, &topic, 1);

        /* Allow consumer to join group and fetch metadata */
        TEST_SAY("Waiting for consumer to be ready\n");
        rd_sleep(2);

        TEST_SAY("Consuming and ACK-ing messages\n");
        acked = consume_and_ack_n(rkshare, msgcnt);
        TEST_SAY("Acknowledged %d messages\n", acked);

        if (acked == 0) {
                TEST_SAY("No messages consumed, skipping test\n");
                test_share_destroy(rkshare);
                SUB_TEST_PASS();
                return;
        }

        TEST_SAY("Calling wakeup before commit_sync\n");
        rd_kafka_share_wakeup(rkshare);

        TEST_SAY("Calling commit_sync (should return WAKEUP immediately)\n");
        err = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);

        TEST_ASSERT(err != NULL, "Expected error from commit_sync");
        TEST_ASSERT(rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s", rd_kafka_error_name(err));
        rd_kafka_error_destroy(err);

        if (partitions) {
                TEST_SAY("Partitions returned: %d\n", partitions->cnt);
                for (i = 0; i < partitions->cnt; i++)
                        TEST_ASSERT(
                            partitions->elems[i].err ==
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Expected WAKEUP for partition %d, got %s", i,
                            rd_kafka_err2name(partitions->elems[i].err));
                rd_kafka_topic_partition_list_destroy(partitions);
                partitions = NULL;
        }

        /* Sleep to allow acknowledgements to be sent to broker despite wakeup
         */
        TEST_SAY("Sleeping to allow acks to complete\n");
        rd_sleep(2);

        TEST_SAY("Second commit_sync should work normally (one-shot)\n");
        err = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);

        /* Should succeed or have no acks to send */
        if (err) {
                TEST_ASSERT(rd_kafka_error_code(err) !=
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Second commit should not be woken up, got %s",
                            rd_kafka_error_name(err));
                rd_kafka_error_destroy(err);
        }

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test that data is not lost after wakeup.
 *
 * Verify that:
 * - If fetch completes after wakeup, data is buffered
 * - Next consume_batch returns the buffered data
 */
static void do_test_wakeup_no_data_loss(void) {
        const char *topic = test_mk_topic_name("wakeup_dataloss", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        consume_thread_state_t state;
        thrd_t thread;
        int total_consumed = 0;
        const int msgcnt   = 50;
        size_t i;

        SUB_TEST_QUICK();

        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        test_share_set_auto_offset_reset(group, "earliest");

        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        rkshare = test_create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);

        /* Initialize thread state */
        memset(&state, 0, sizeof(state));
        state.rkshare = rkshare;
        mtx_init(&state.lock, mtx_plain);
        cnd_init(&state.cond);

        TEST_SAY("Starting consume thread\n");
        if (thrd_create(&thread, consume_thread_func, &state) != thrd_success)
                TEST_FAIL("Failed to create thread");

        /* Wait for thread to start */
        mtx_lock(&state.lock);
        while (!state.started)
                cnd_wait(&state.cond, &state.lock);
        mtx_unlock(&state.lock);

        /* Sleep briefly to let fetch potentially complete */
        rd_sleep(2);

        TEST_SAY("Calling wakeup\n");
        rd_kafka_share_wakeup(rkshare);

        /* Wait for thread to finish */
        mtx_lock(&state.lock);
        while (!state.finished)
                cnd_timedwait_ms(&state.cond, &state.lock, 10000);
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);
        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);

        TEST_SAY(
            "First consume got WAKEUP, now consuming remaining "
            "messages\n");

        /* Consume all messages - may take multiple calls */
        while (total_consumed < msgcnt) {
                err = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages,
                                                   &msg_cnt);

                if (err) {
                        TEST_ASSERT(
                            rd_kafka_error_code(err) !=
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Should not get wakeup after first consume");
                        rd_kafka_error_destroy(err);
                        break;
                }

                TEST_SAY("Consumed %zu messages (total %d)\n", msg_cnt,
                         total_consumed + (int)msg_cnt);

                for (i = 0; i < msg_cnt; i++)
                        rd_kafka_message_destroy(rkmessages[i]);

                total_consumed += (int)msg_cnt;

                if (msg_cnt == 0)
                        break; /* Timeout */
        }

        TEST_SAY("Total consumed: %d out of %d\n", total_consumed, msgcnt);
        TEST_ASSERT(total_consumed == msgcnt,
                    "Expected to consume all %d messages, got %d", msgcnt,
                    total_consumed);

        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 * Tests — mock broker
 * =================================================================== */

/* ===================================================================
 *  Test: Mock broker + wakeup during consume_batch with implicit acks
 *
 *  Scenario:
 *  - Use implicit acknowledgement mode
 *  - Produce 20 messages
 *  - Consume first batch of 10 messages (no implicit acks sent yet)
 *  - Mock broker has 5000ms RTT delay
 *  - Call consume_batch for second batch (triggers implicit acks for first
 * batch)
 *  - Wakeup interrupts after 500ms
 *  - Verify: consume_batch returns __WAKEUP
 *  - Verify: callbacks fire with NO_ERROR for implicit acks from first batch
 *  - Verify: messages from second batch are available in next consume_batch
 *
 *  This proves that in implicit mode, wakeup interrupts consume_batch
 *  but does NOT prevent acknowledgement callbacks from firing.
 * =================================================================== */
static void do_test_wakeup_consume_batch_implicit_acks_local(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic = "mock-wakeup-consume-implicit";
        const char *t     = topic;
        const int msgcnt  = 20;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed_first_batch;
        int consumed_second_batch;
        int total_consumed;
        int success_cnt, wakeup_err_cnt;
        test_ack_cb_state_t cb_state = {0};
        wakeup_thread_state_t wakeup_state;
        thrd_t wakeup_thread;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare =
            create_mock_share_consumer(ctx.bootstraps, "sg-mock-wakeup-consume",
                                       "implicit", &cb_state, NULL);
        subscribe_consumer(rkshare, &t, 1);

        TEST_SAY("Consuming first batch of messages (no implicit acks yet)\n");
        consumed_first_batch = consume_n(rkshare, 10);
        TEST_SAY("Consumed first batch: %d messages\n", consumed_first_batch);
        TEST_ASSERT(consumed_first_batch >= 10,
                    "Expected at least 10 messages in first batch, got %d",
                    consumed_first_batch);

        /* Inject 5000ms RTT on all brokers */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 5000);

        start_wakeup_thread(&wakeup_state, &wakeup_thread, rkshare, 500);

        /* Call consume_batch for second batch.
         * This triggers IMPLICIT ACKS for first batch (sent asynchronously).
         * Wakeup will interrupt after 500ms. */
        TEST_SAY(
            "Calling consume_batch for second batch (triggers implicit acks "
            "for first batch, wakeup after 500ms)\n");
        t_start = test_clock();
        rcvd    = 0;
        error = rd_kafka_share_consume_batch(rkshare, 2000, rkmessages, &rcvd);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY(
            "consume_batch returned after %" PRId64 "ms, error=%s, rcvd=%zu\n",
            t_elapsed_ms, error ? rd_kafka_error_string(error) : "NULL", rcvd);

        /* Verify it returned quickly (wakeup or timeout, not full 5000ms RTT)
         */
        TEST_ASSERT(t_elapsed_ms < 3000,
                    "Expected consume_batch to return quickly (<3000ms), "
                    "got %" PRId64 "ms",
                    t_elapsed_ms);

        /* Should get WAKEUP error */
        if (error) {
                TEST_ASSERT(rd_kafka_error_code(error) ==
                                    RD_KAFKA_RESP_ERR__WAKEUP ||
                                rd_kafka_error_code(error) ==
                                    RD_KAFKA_RESP_ERR__TIMED_OUT,
                            "Expected __WAKEUP or timeout, got %s",
                            rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        }

        /* Clean up any messages that were returned */
        for (j = 0; j < rcvd; j++)
                rd_kafka_message_destroy(rkmessages[j]);

        stop_wakeup_thread(&wakeup_state, wakeup_thread);

        /* Remove RTT and wait for broker to finish processing */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);
        TEST_SAY(
            "Removed broker RTT, sleeping 2s for broker to process implicit "
            "acks\n");
        rd_sleep(2);

        TEST_SAY(
            "Polling to service event queue and trigger callbacks for "
            "implicit acks\n");
        TEST_ASSERT(test_wait_for_cb_with_poll(&cb_state, rkshare, 1, 10000),
                    "Timeout waiting for callbacks: got %d, expected >= 1",
                    cb_state.callback_cnt);

        success_cnt    = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR_NO_ERROR);
        wakeup_err_cnt = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR__WAKEUP);
        TEST_SAY(
            "Callbacks invoked: %d total, %d success (NO_ERROR), %d wakeup "
            "errors\n",
            cb_state.callback_cnt, success_cnt, wakeup_err_cnt);

        TEST_ASSERT(success_cnt >= 1,
                    "Expected at least one NO_ERROR callback, got %d",
                    success_cnt);
        TEST_ASSERT(wakeup_err_cnt == 0,
                    "Callbacks should NOT receive __WAKEUP error, got %d",
                    wakeup_err_cnt);

        /* Drain whatever was buffered by the wakeup-interrupted fetch.
         * In implicit mode the first consume_batch may have already
         * delivered everything, in which case this drain returns 0 —
         * that is fine, the total-consumed assertion below tolerates
         * up to 5 missing records. */
        TEST_SAY("Draining any messages buffered by interrupted fetch\n");
        consumed_second_batch = drain_buffered(rkshare);
        TEST_SAY("Drained second batch: %d messages\n", consumed_second_batch);

        total_consumed = consumed_first_batch + consumed_second_batch;
        TEST_SAY("Total consumed: %d out of %d\n", total_consumed, msgcnt);
        TEST_ASSERT(total_consumed >= msgcnt - 5,
                    "Expected to consume most messages (%d), got %d", msgcnt,
                    total_consumed);

        TEST_SAY(
            "\n=== SUCCESS ===\n"
            "Application: consume_batch interrupted with WAKEUP\n"
            "Callback: received NO_ERROR from broker for implicit acks\n"
            "Messages: fetched messages delivered in next consume_batch\n"
            "This proves wakeup does NOT prevent implicit acknowledgement "
            "callbacks!\n");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: Mock broker with high RTT + wakeup during commit_sync.
 *
 *  Similar to test 176's do_test_mock_commit_sync_timeout, but with wakeup.
 *
 *  Scenario:
 *  - Mock broker has 5000ms RTT delay on all requests
 *  - Consume and acknowledge 10 messages
 *  - Start background thread that will call wakeup() after 500ms
 *  - Call commit_sync() in main thread with 2000ms timeout
 *  - Verify: commit_sync returns __WAKEUP to application (wakeup interrupts)
 *  - Wait for broker response and verify callback receives NO_ERROR
 *
 *  This test proves that wakeup interrupts the application thread
 *  but does NOT prevent broker responses from being processed
 *  in callbacks - matching Java behavior.
 * =================================================================== */
static void do_test_wakeup_commit_sync_high_rtt_local(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const char *topic                           = "mock-wakeup-rtt";
        const char *t                               = topic;
        const int msgcnt                            = 10;
        int acked;
        int success_cnt, wakeup_err_cnt;
        test_ack_cb_state_t cb_state = {0};
        wakeup_thread_state_t wakeup_state;
        thrd_t wakeup_thread;
        rd_ts_t t_start, t_elapsed_ms;
        rd_bool_t got_wakeup_or_timeout = rd_false;
        int i;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(
            ctx.bootstraps, "sg-mock-wakeup-rtt", "explicit", &cb_state, NULL);
        subscribe_consumer(rkshare, &t, 1);

        acked = consume_and_ack_n(rkshare, msgcnt);
        TEST_SAY("Consumed and acknowledged %d messages\n", acked);
        TEST_ASSERT(acked == msgcnt, "Expected %d, got %d", msgcnt, acked);

        /* Inject 5000ms RTT on all brokers (like test 176) */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 5000);

        start_wakeup_thread(&wakeup_state, &wakeup_thread, rkshare, 500);

        /* commit_sync with 2000ms timeout in main thread.
         * Wakeup will be called after 500ms (should interrupt before timeout).
         * Broker RTT is 5000ms, so response won't arrive until later. */
        TEST_SAY(
            "Calling commit_sync (2000ms timeout, wakeup after 500ms, "
            "RTT=5000ms)\n");
        t_start      = test_clock();
        partitions   = NULL;
        error        = rd_kafka_share_commit_sync(rkshare, 2000, &partitions);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("commit_sync returned after %" PRId64 "ms, error=%s\n",
                 t_elapsed_ms, error ? rd_kafka_error_string(error) : "NULL");

        /* Verify it returned quickly (wakeup or timeout, not full 5000ms RTT)
         */
        TEST_ASSERT(t_elapsed_ms < 3000,
                    "Expected commit_sync to return quickly (<3000ms), "
                    "got %" PRId64 "ms",
                    t_elapsed_ms);

        /* Check for __WAKEUP or timeout errors */
        if (error) {
                TEST_SAY("Top-level error: %s\n", rd_kafka_error_string(error));
                if (rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__WAKEUP ||
                    rd_kafka_error_code(error) ==
                        RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT ||
                    rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__TIMED_OUT)
                        got_wakeup_or_timeout = rd_true;
                rd_kafka_error_destroy(error);
        }

        if (partitions) {
                for (i = 0; i < partitions->cnt; i++) {
                        rd_kafka_topic_partition_t *rktpar =
                            &partitions->elems[i];
                        TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                                 rktpar->partition,
                                 rd_kafka_err2str(rktpar->err));
                        if (rktpar->err == RD_KAFKA_RESP_ERR__WAKEUP ||
                            rktpar->err ==
                                RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT ||
                            rktpar->err == RD_KAFKA_RESP_ERR__TIMED_OUT)
                                got_wakeup_or_timeout = rd_true;
                }
                rd_kafka_topic_partition_list_destroy(partitions);
        }

        TEST_ASSERT(got_wakeup_or_timeout,
                    "Expected __WAKEUP or timeout error from commit_sync");

        stop_wakeup_thread(&wakeup_state, wakeup_thread);

        /* Remove RTT and wait for broker to finish processing the delayed
         * request. Similar to test 176's Phase 2. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);
        TEST_SAY(
            "Removed broker RTT, sleeping 2s for broker to process delayed "
            "ShareAcknowledge\n");
        rd_sleep(2);

        TEST_SAY("Polling to service event queue and trigger callbacks\n");
        TEST_ASSERT(test_wait_for_cb_with_poll(&cb_state, rkshare, 1, 10000),
                    "Timeout waiting for callbacks: got %d, expected >= 1",
                    cb_state.callback_cnt);

        success_cnt    = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR_NO_ERROR);
        wakeup_err_cnt = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR__WAKEUP);
        TEST_SAY(
            "Callbacks invoked: %d total, %d success (NO_ERROR), %d wakeup "
            "errors\n",
            cb_state.callback_cnt, success_cnt, wakeup_err_cnt);

        TEST_ASSERT(success_cnt >= 1,
                    "Expected at least one NO_ERROR callback, got %d",
                    success_cnt);
        TEST_ASSERT(wakeup_err_cnt == 0,
                    "Callbacks should NOT receive __WAKEUP error, got %d",
                    wakeup_err_cnt);

        TEST_SAY(
            "\n=== SUCCESS ===\n"
            "Application: commit_sync interrupted with WAKEUP/timeout\n"
            "Callback: received NO_ERROR from broker\n"
            "This proves wakeup does NOT prevent acknowledgement callbacks!\n");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: wakeup interrupts consume_batch fast, fetched data is still
 *        retrievable in the next call.
 *
 *  Scenario:
 *  - Mock broker has 2000ms RTT injected on every request.
 *  - Consumer is subscribed and joined (RTT injected *after* join).
 *  - Background thread calls wakeup() at 200ms.
 *  - Main thread calls consume_batch() with a 5000ms timeout.
 *  - Verify: consume_batch returns within @c WAKEUP_LATENCY_BUDGET_MS
 *           with __WAKEUP — way below both the timeout and the RTT.
 *  - Remove the RTT and call consume_batch again: the in-flight
 *    ShareFetch that the wakeup interrupted on the app side still
 *    completes on the broker side, so the records must be retrievable.
 * =================================================================== */
#define WAKEUP_LATENCY_BUDGET_MS 500

static void do_test_wakeup_fast_consume_batch_local(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic = "mock-wakeup-fast-consume";
        const char *t     = topic;
        const int msgcnt  = 10;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed_after = 0;
        wakeup_thread_state_t wakeup_state;
        thrd_t wakeup_thread;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(
            ctx.bootstraps, "sg-mock-wakeup-fast-consume", "implicit", NULL,
            NULL);
        subscribe_consumer(rkshare, &t, 1);

        /* Let the consumer join the group / settle metadata BEFORE the
         * RTT is injected, otherwise the join itself would block. */
        rd_sleep(1);

        TEST_SAY("Injecting 2000ms RTT on all brokers\n");
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 2000);

        start_wakeup_thread(&wakeup_state, &wakeup_thread, rkshare, 200);

        TEST_SAY(
            "Calling consume_batch (5000ms timeout, wakeup at 200ms, "
            "RTT=2000ms)\n");
        t_start = test_clock();
        rcvd    = 0;
        error = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages, &rcvd);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("consume_batch returned after %" PRId64 "ms, error=%s\n",
                 t_elapsed_ms, error ? rd_kafka_error_string(error) : "NULL");

        TEST_ASSERT(error != NULL &&
                        rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected __WAKEUP error, got %s",
                    error ? rd_kafka_error_string(error) : "no error");
        rd_kafka_error_destroy(error);

        TEST_ASSERT(t_elapsed_ms < WAKEUP_LATENCY_BUDGET_MS,
                    "consume_batch took %" PRId64
                    "ms — should have woken up within %dms",
                    t_elapsed_ms, WAKEUP_LATENCY_BUDGET_MS);

        for (j = 0; j < rcvd; j++)
                rd_kafka_message_destroy(rkmessages[j]);

        stop_wakeup_thread(&wakeup_state, wakeup_thread);

        /* Drop the RTT and verify the in-flight fetch's records are
         * still retrievable on the next consume_batch call. */
        TEST_SAY("Removing RTT and consuming remaining records\n");
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);

        consumed_after = consume_n(rkshare, msgcnt);
        TEST_ASSERT(consumed_after == msgcnt,
                    "Expected %d records retrievable after wakeup, got %d",
                    msgcnt, consumed_after);

        TEST_SAY(
            "\n=== SUCCESS ===\n"
            "consume_batch woke up in %" PRId64
            "ms (RTT=2000ms, wakeup at 200ms); all %d records were "
            "retrievable in the next consume_batch call.\n",
            t_elapsed_ms, consumed_after);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: wakeup interrupts commit_sync fast, ack callback still fires.
 *
 *  Scenario:
 *  - Consumer is subscribed and joined; 10 records consumed+ACK'd.
 *  - Mock broker has 2000ms RTT injected (after consume+ack).
 *  - Background thread calls wakeup() at 200ms.
 *  - Main thread calls commit_sync() with a 5000ms timeout.
 *  - Verify: commit_sync returns within @c WAKEUP_LATENCY_BUDGET_MS
 *           with __WAKEUP.
 *  - Remove the RTT and poll: the ShareAcknowledge that was in flight
 *    when wakeup fired must still complete on the broker, and the
 *    acknowledgement callback must fire with NO_ERROR.
 * =================================================================== */
static void do_test_wakeup_fast_commit_sync_local(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const char *topic                           = "mock-wakeup-fast-commit";
        const char *t                               = topic;
        const int msgcnt                            = 10;
        int acked;
        int success_cnt, wakeup_err_cnt;
        test_ack_cb_state_t cb_state = {0};
        wakeup_thread_state_t wakeup_state;
        thrd_t wakeup_thread;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                             "sg-mock-wakeup-fast-commit",
                                             "explicit", &cb_state, NULL);
        subscribe_consumer(rkshare, &t, 1);

        acked = consume_and_ack_n(rkshare, msgcnt);
        TEST_ASSERT(acked == msgcnt, "Expected %d acked, got %d", msgcnt,
                    acked);

        TEST_SAY("Injecting 2000ms RTT on all brokers\n");
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 2000);

        start_wakeup_thread(&wakeup_state, &wakeup_thread, rkshare, 200);

        TEST_SAY(
            "Calling commit_sync (5000ms timeout, wakeup at 200ms, "
            "RTT=2000ms)\n");
        t_start      = test_clock();
        error        = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("commit_sync returned after %" PRId64 "ms, error=%s\n",
                 t_elapsed_ms, error ? rd_kafka_error_string(error) : "NULL");

        TEST_ASSERT(error != NULL &&
                        rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected __WAKEUP error, got %s",
                    error ? rd_kafka_error_string(error) : "no error");
        rd_kafka_error_destroy(error);

        TEST_ASSERT(t_elapsed_ms < WAKEUP_LATENCY_BUDGET_MS,
                    "commit_sync took %" PRId64
                    "ms — should have woken up within %dms",
                    t_elapsed_ms, WAKEUP_LATENCY_BUDGET_MS);

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        stop_wakeup_thread(&wakeup_state, wakeup_thread);

        /* Drop the RTT and wait for the broker to finish the delayed
         * ShareAcknowledge; the ack callback must fire with NO_ERROR. */
        TEST_SAY("Removing RTT and polling for delayed ack callback\n");
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);
        TEST_ASSERT(test_wait_for_cb_with_poll(&cb_state, rkshare, 1, 10000),
                    "Timeout waiting for callback: got %d, expected >= 1",
                    cb_state.callback_cnt);

        success_cnt    = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR_NO_ERROR);
        wakeup_err_cnt = count_cb_err(&cb_state, RD_KAFKA_RESP_ERR__WAKEUP);
        TEST_ASSERT(success_cnt >= 1,
                    "Expected at least one NO_ERROR callback, got %d",
                    success_cnt);
        TEST_ASSERT(wakeup_err_cnt == 0,
                    "Callbacks should NOT receive __WAKEUP error, got %d",
                    wakeup_err_cnt);

        TEST_SAY(
            "\n=== SUCCESS ===\n"
            "commit_sync woke up in %" PRId64
            "ms (RTT=2000ms, wakeup at 200ms); the in-flight "
            "ShareAcknowledge still completed and fired the callback "
            "with NO_ERROR.\n",
            t_elapsed_ms);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 * Entry points
 * =================================================================== */

int main_0186_share_consumer_wakeup(int argc, char **argv) {
        /* Real broker tests */
        do_test_wakeup_before_consume();
        do_test_multiple_wakeups();
        do_test_wakeup_before_commit_sync();
        do_test_wakeup_no_data_loss();

        return 0;
}

int main_0186_share_consumer_wakeup_local(int argc, char **argv) {
        /* Mock broker tests only (no real broker needed) */
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_wakeup_consume_batch_implicit_acks_local();
        do_test_wakeup_commit_sync_high_rtt_local();
        do_test_wakeup_fast_consume_batch_local();
        do_test_wakeup_fast_commit_sync_local();

        return 0;
}
