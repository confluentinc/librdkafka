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
 * @brief Test wakeup called before consume_batch.
 *
 * Verify that:
 * - Wakeup before consume_batch returns immediately with WAKEUP error
 * - No fetch request is sent to broker
 * - Second consume_batch works normally (one-shot behavior)
 */
static void test_wakeup_before_consume(void) {
        const char *topic = test_mk_topic_name("wakeup_before", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[10];
        size_t msg_cnt;
        rd_kafka_error_t *err;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        rkshare = test_create_share_consumer(group, "implicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        TEST_SAY("Calling wakeup before consume_batch\n");
        rd_kafka_share_wakeup(rkshare);

        TEST_SAY("Calling consume_batch (should return immediately with "
                 "WAKEUP)\n");
        err = rd_kafka_share_consume_batch(rkshare, 1000, rkmessages,
                                           &msg_cnt);

        TEST_ASSERT(err != NULL, "Expected error from consume_batch");
        TEST_ASSERT(rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_error_name(err));
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

        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Thread function for consume_batch in background.
 */
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

        state->result = err ? rd_kafka_error_code(err) : RD_KAFKA_RESP_ERR_NO_ERROR;

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


/**
 * @brief Test wakeup called during consume_batch.
 *
 * Verify that:
 * - Wakeup during blocking consume_batch interrupts the wait
 * - Returns WAKEUP error
 * - Fetch request may have been sent, data buffered for next consume
 */
static void test_wakeup_during_consume(void) {
        const char *topic = test_mk_topic_name("wakeup_during", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        consume_thread_state_t state;
        thrd_t thread;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        rkshare = test_create_share_consumer(group, "implicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        /* Initialize thread state */
        memset(&state, 0, sizeof(state));
        state.rkshare = rkshare;
        mtx_init(&state.lock, mtx_plain);
        cnd_init(&state.cond);

        TEST_SAY("Starting consume thread\n");
        if (thrd_create(&thread, consume_thread_func, &state) != thrd_success)
                TEST_FAIL("Failed to create thread");

        /* Wait for thread to start consuming */
        mtx_lock(&state.lock);
        while (!state.started)
                cnd_wait(&state.cond, &state.lock);
        mtx_unlock(&state.lock);

        TEST_SAY("Thread started, sleeping briefly then calling wakeup\n");
        rd_sleep(1);

        TEST_SAY("Calling wakeup during consume_batch\n");
        rd_kafka_share_wakeup(rkshare);

        /* Wait for thread to finish */
        TEST_SAY("Waiting for thread to finish\n");
        mtx_lock(&state.lock);
        while (!state.finished) {
                if (cnd_timedwait_ms(&state.cond, &state.lock, 10000) ==
                    thrd_timedout) {
                        mtx_unlock(&state.lock);
                        TEST_FAIL("Timeout waiting for consume thread");
                }
        }
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);

        TEST_SAY("Thread finished with result: %s\n",
                 rd_kafka_err2name(state.result));

        TEST_ASSERT(state.result == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_err2name(state.result));

        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test that data is not lost after wakeup.
 *
 * Verify that:
 * - If fetch completes after wakeup, data is buffered
 * - Next consume_batch returns the buffered data
 */
static void test_wakeup_no_data_loss(void) {
        const char *topic = test_mk_topic_name("wakeup_dataloss", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        consume_thread_state_t state;
        thrd_t thread;
        int total_consumed = 0;
        int msgcnt         = 50;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        /* Produce messages */
        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        rkshare = test_create_share_consumer(group, "implicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

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
        while (!state.finished) {
                cnd_timedwait_ms(&state.cond, &state.lock, 10000);
        }
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);
        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);

        TEST_SAY("First consume got WAKEUP, now consuming remaining "
                 "messages\n");

        /* Consume all messages - may take multiple calls */
        while (total_consumed < msgcnt) {
                err = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages,
                                                   &msg_cnt);

                if (err) {
                        TEST_ASSERT(rd_kafka_error_code(err) !=
                                        RD_KAFKA_RESP_ERR__WAKEUP,
                                    "Should not get wakeup after first consume");
                        rd_kafka_error_destroy(err);
                        break;
                }

                TEST_SAY("Consumed %zu messages (total %d)\n", msg_cnt,
                         total_consumed + (int)msg_cnt);

                for (size_t i = 0; i < msg_cnt; i++) {
                        rd_kafka_message_destroy(rkmessages[i]);
                }

                total_consumed += (int)msg_cnt;

                if (msg_cnt == 0)
                        break; /* Timeout */
        }

        TEST_SAY("Total consumed: %d out of %d\n", total_consumed, msgcnt);
        TEST_ASSERT(total_consumed == msgcnt,
                    "Expected to consume all %d messages, got %d", msgcnt,
                    total_consumed);

        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test multiple wakeup calls.
 *
 * Verify that:
 * - Multiple wakeups before consume = only one consume interrupted
 * - One-shot behavior works correctly
 */
static void test_multiple_wakeups(void) {
        const char *topic = test_mk_topic_name("wakeup_multi", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[10];
        size_t msg_cnt;
        rd_kafka_error_t *err;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        rkshare = test_create_share_consumer(group, "implicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

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

        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Thread function for commit_sync in background.
 */
typedef struct {
        rd_kafka_share_t *rkshare;
        rd_kafka_resp_err_t result;
        mtx_t lock;
        cnd_t cond;
        rd_bool_t started;
        rd_bool_t finished;
} commit_thread_state_t;

static int commit_thread_func(void *arg) {
        commit_thread_state_t *state = (commit_thread_state_t *)arg;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *err;

        mtx_lock(&state->lock);
        state->started = rd_true;
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);

        TEST_SAY("Commit thread: calling commit_sync\n");
        err = rd_kafka_share_commit_sync(state->rkshare, 10000, &partitions);

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        state->result =
            err ? rd_kafka_error_code(err) : RD_KAFKA_RESP_ERR_NO_ERROR;

        if (err)
                rd_kafka_error_destroy(err);

        mtx_lock(&state->lock);
        state->finished = rd_true;
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);

        TEST_SAY("Commit thread: finished with result %s\n",
                 rd_kafka_err2name(state->result));

        return 0;
}


/**
 * @brief Test wakeup called before commit_sync.
 *
 * Verify that:
 * - Wakeup before commit_sync returns WAKEUP error immediately
 * - Acknowledgements are still sent to broker (matching Java behavior)
 * - One-shot behavior - next commit_sync works normally
 */
static void test_wakeup_before_commit_sync(void) {
        const char *topic = test_mk_topic_name("wakeup_before_commit", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        int msgcnt = 10;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        /* Produce messages */
        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        rkshare = test_create_share_consumer(group, "explicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        /* Consume messages */
        TEST_SAY("Consuming messages\n");
        err = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages, &msg_cnt);
        if (err)
                rd_kafka_error_destroy(err);

        TEST_SAY("Consumed %zu messages\n", msg_cnt);
        for (size_t i = 0; i < msg_cnt; i++) {
                rd_kafka_message_destroy(rkmessages[i]);
        }

        if (msg_cnt == 0) {
                TEST_SAY("No messages consumed, skipping test\n");
                rd_kafka_share_destroy(rkshare);
                SUB_TEST_PASS();
                return;
        }

        TEST_SAY("Calling wakeup before commit_sync\n");
        rd_kafka_share_wakeup(rkshare);

        TEST_SAY("Calling commit_sync (should return WAKEUP immediately)\n");
        err = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);

        TEST_ASSERT(err != NULL, "Expected error from commit_sync");
        TEST_ASSERT(rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_error_name(err));
        rd_kafka_error_destroy(err);

        if (partitions) {
                TEST_SAY("Partitions returned: %d\n", partitions->cnt);
                /* Verify all partitions have WAKEUP error */
                for (int i = 0; i < partitions->cnt; i++) {
                        TEST_ASSERT(
                            partitions->elems[i].err ==
                                RD_KAFKA_RESP_ERR__WAKEUP,
                            "Expected WAKEUP for partition %d, got %s", i,
                            rd_kafka_err2name(partitions->elems[i].err));
                }
                rd_kafka_topic_partition_list_destroy(partitions);
                partitions = NULL;
        }

        /* Sleep to allow acknowledgements to be sent to broker despite wakeup */
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

        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test wakeup during commit_sync.
 *
 * Verify that:
 * - Wakeup during blocking commit_sync interrupts the wait
 * - Returns WAKEUP error
 * - Commit request was already sent (transactional behavior)
 */
static void test_wakeup_commit_sync(void) {
        const char *topic = test_mk_topic_name("wakeup_commit", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        commit_thread_state_t state;
        thrd_t thread;
        int msgcnt = 10;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        /* Produce messages */
        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        rkshare = test_create_share_consumer(group, "explicit");

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        /* Consume some messages to have acknowledgements to commit */
        TEST_SAY("Consuming messages\n");
        err = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages, &msg_cnt);
        TEST_ASSERT(!err ||
                        rd_kafka_error_code(err) ==
                            RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Expected success or timeout, got %s",
                    err ? rd_kafka_error_name(err) : "");
        if (err)
                rd_kafka_error_destroy(err);

        TEST_SAY("Consumed %zu messages\n", msg_cnt);
        for (size_t i = 0; i < msg_cnt; i++) {
                rd_kafka_message_destroy(rkmessages[i]);
        }

        if (msg_cnt == 0) {
                TEST_SAY("No messages consumed, skipping commit test\n");
                rd_kafka_share_destroy(rkshare);
                SUB_TEST_PASS();
                return;
        }

        /* Initialize thread state */
        memset(&state, 0, sizeof(state));
        state.rkshare = rkshare;
        mtx_init(&state.lock, mtx_plain);
        cnd_init(&state.cond);

        TEST_SAY("Starting commit thread\n");
        if (thrd_create(&thread, commit_thread_func, &state) != thrd_success)
                TEST_FAIL("Failed to create thread");

        /* Wait for thread to start */
        mtx_lock(&state.lock);
        while (!state.started)
                cnd_wait(&state.cond, &state.lock);
        mtx_unlock(&state.lock);

        /* Sleep briefly to let commit start processing */
        rd_sleep(1);

        TEST_SAY("Calling wakeup during commit_sync\n");
        rd_kafka_share_wakeup(rkshare);

        /* Wait for thread to finish */
        TEST_SAY("Waiting for thread to finish\n");
        mtx_lock(&state.lock);
        while (!state.finished) {
                if (cnd_timedwait_ms(&state.cond, &state.lock, 10000) ==
                    thrd_timedout) {
                        mtx_unlock(&state.lock);
                        TEST_FAIL("Timeout waiting for commit thread");
                }
        }
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);

        TEST_SAY("Thread finished with result: %s\n",
                 rd_kafka_err2name(state.result));

        TEST_ASSERT(state.result == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_err2name(state.result));

        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief State for acknowledgement callback tracking.
 */
typedef struct {
        mtx_t lock;
        int callback_count;
        int expected_count;
        cnd_t cond;
} ack_callback_state_t;


/**
 * @brief Acknowledgement callback function.
 */
static void ack_commit_callback(
    rd_kafka_share_t *rkshare,
    rd_kafka_share_partition_offsets_list_t *partitions,
    rd_kafka_resp_err_t err,
    void *opaque) {
        ack_callback_state_t *state = (ack_callback_state_t *)opaque;
        size_t partition_cnt;

        (void)rkshare;
        (void)err;

        partition_cnt = rd_kafka_share_partition_offsets_list_count(partitions);

        mtx_lock(&state->lock);
        state->callback_count += (int)partition_cnt;
        TEST_SAY("Ack callback invoked: %zu partitions (total %d)\n",
                 partition_cnt, state->callback_count);
        cnd_broadcast(&state->cond);
        mtx_unlock(&state->lock);
}


/**
 * @brief Test wakeup with acknowledgement callback.
 *
 * Verify that:
 * - When wakeup interrupts commit_sync, acks are still sent to broker
 * - Acknowledgement callbacks are invoked when broker responses arrive
 * - Matches Java behavior where background thread continues processing
 */
static void test_wakeup_commit_sync_with_callback(void) {
        const char *topic =
            test_mk_topic_name("wakeup_commit_callback", 1);
        const char *group = topic;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        commit_thread_state_t state;
        ack_callback_state_t cb_state;
        thrd_t thread;
        int msgcnt = 10;
        rd_kafka_conf_t *conf;

        SUB_TEST_QUICK();

        test_create_topic(NULL, topic, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        /* Produce messages */
        TEST_SAY("Producing %d messages\n", msgcnt);
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

        /* Initialize callback state */
        memset(&cb_state, 0, sizeof(cb_state));
        mtx_init(&cb_state.lock, mtx_plain);
        cnd_init(&cb_state.cond);
        cb_state.expected_count = 0;

        /* Create share consumer with acknowledgement callback */
        conf = rd_kafka_conf_new();
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf,
                                                          ack_commit_callback);
        rd_kafka_conf_set_opaque(conf, &cb_state);

        {
                char errstr[512];
                rkshare = rd_kafka_share_consumer_new(conf, errstr,
                                                      sizeof(errstr));
                TEST_ASSERT(rkshare, "Failed to create share consumer: %s",
                            errstr);
        }

        /* Subscribe to topic */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        /* Consume messages */
        TEST_SAY("Consuming messages\n");
        err = rd_kafka_share_consume_batch(rkshare, 5000, rkmessages, &msg_cnt);
        if (err)
                rd_kafka_error_destroy(err);

        TEST_SAY("Consumed %zu messages\n", msg_cnt);
        cb_state.expected_count = (int)msg_cnt;
        for (size_t i = 0; i < msg_cnt; i++) {
                rd_kafka_message_destroy(rkmessages[i]);
        }

        if (msg_cnt == 0) {
                TEST_SAY("No messages consumed, skipping test\n");
                mtx_destroy(&cb_state.lock);
                cnd_destroy(&cb_state.cond);
                rd_kafka_share_destroy(rkshare);
                SUB_TEST_PASS();
                return;
        }

        /* Initialize commit thread state */
        memset(&state, 0, sizeof(state));
        state.rkshare = rkshare;
        mtx_init(&state.lock, mtx_plain);
        cnd_init(&state.cond);

        TEST_SAY("Starting commit thread\n");
        if (thrd_create(&thread, commit_thread_func, &state) != thrd_success)
                TEST_FAIL("Failed to create thread");

        /* Wait for thread to start */
        mtx_lock(&state.lock);
        while (!state.started)
                cnd_wait(&state.cond, &state.lock);
        mtx_unlock(&state.lock);

        /* Sleep briefly to let commit start processing */
        rd_sleep(1);

        TEST_SAY("Calling wakeup during commit_sync\n");
        rd_kafka_share_wakeup(rkshare);

        /* Wait for commit thread to finish */
        mtx_lock(&state.lock);
        while (!state.finished) {
                cnd_timedwait_ms(&state.cond, &state.lock, 10000);
        }
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);

        TEST_SAY("Commit thread finished with WAKEUP\n");
        TEST_ASSERT(state.result == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_err2name(state.result));

        /* Now wait for acknowledgement callbacks to be invoked.
         * This verifies that even though commit_sync returned with WAKEUP,
         * the acknowledgements were still sent to broker and callbacks
         * are invoked when responses arrive (matching Java behavior). */
        TEST_SAY("Waiting for acknowledgement callbacks (expected %d)\n",
                 cb_state.expected_count);

        mtx_lock(&cb_state.lock);
        rd_ts_t deadline = test_clock() + (10 * 1000000); /* 10 seconds */
        while (cb_state.callback_count < cb_state.expected_count) {
                rd_ts_t now = test_clock();
                if (now >= deadline) {
                        mtx_unlock(&cb_state.lock);
                        TEST_FAIL(
                            "Timeout waiting for callbacks: got %d, expected %d",
                            cb_state.callback_count, cb_state.expected_count);
                }
                int64_t timeout_ms = (deadline - now) / 1000;
                cnd_timedwait_ms(&cb_state.cond, &cb_state.lock,
                                 (int)timeout_ms);
        }
        mtx_unlock(&cb_state.lock);

        TEST_SAY("All acknowledgement callbacks invoked: %d\n",
                 cb_state.callback_count);
        TEST_ASSERT(cb_state.callback_count == cb_state.expected_count,
                    "Expected %d callbacks, got %d", cb_state.expected_count,
                    cb_state.callback_count);

        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);
        mtx_destroy(&cb_state.lock);
        cnd_destroy(&cb_state.cond);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Test wakeup with multiple brokers (pending acks dispatched).
 *
 * Verify that:
 * - When wakeup is called, ALL pending acks are dispatched immediately
 * - This includes acks waiting for busy brokers
 * - Matches Java behavior where network thread continues sending all acks
 */
static void test_wakeup_commit_sync_pending_acks(void) {
        const char *topic_pfx = test_mk_topic_name("wakeup_pending", 1);
        char topic1[128], topic2[128];
        const char *group = topic_pfx;
        rd_kafka_share_t *rkshare;
        rd_kafka_message_t *rkmessages[100];
        size_t msg_cnt;
        rd_kafka_error_t *err;
        commit_thread_state_t state;
        ack_callback_state_t cb_state;
        thrd_t thread;
        int msgcnt       = 5;
        int total_msgs   = 0;
        rd_kafka_conf_t *conf;

        SUB_TEST_QUICK();

        /* Create two topics to potentially have different leader brokers */
        rd_snprintf(topic1, sizeof(topic1), "%s_1", topic_pfx);
        rd_snprintf(topic2, sizeof(topic2), "%s_2", topic_pfx);

        test_create_topic(NULL, topic1, 1, 1);
        test_create_topic(NULL, topic2, 1, 1);

        /* Set share group auto offset reset to earliest */
        {
                const char *cfg[] = {"share.auto.offset.reset", "SET",
                                     "earliest"};
                test_alter_group_configurations(group, cfg, 1);
        }

        /* Produce messages to both topics */
        TEST_SAY("Producing %d messages to each topic\n", msgcnt);
        test_produce_msgs_easy(topic1, 0, 0, msgcnt);
        test_produce_msgs_easy(topic2, 0, 0, msgcnt);

        /* Initialize callback state */
        memset(&cb_state, 0, sizeof(cb_state));
        mtx_init(&cb_state.lock, mtx_plain);
        cnd_init(&cb_state.cond);

        /* Create share consumer with callback */
        conf = rd_kafka_conf_new();
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf,
                                                          ack_commit_callback);
        rd_kafka_conf_set_opaque(conf, &cb_state);

        {
                char errstr[512];
                rkshare = rd_kafka_share_consumer_new(conf, errstr,
                                                      sizeof(errstr));
                TEST_ASSERT(rkshare, "Failed to create share consumer: %s",
                            errstr);
        }

        /* Subscribe to both topics */
        {
                rd_kafka_topic_partition_list_t *subs =
                    rd_kafka_topic_partition_list_new(2);
                rd_kafka_resp_err_t sub_err;
                rd_kafka_topic_partition_list_add(subs, topic1,
                                                  RD_KAFKA_PARTITION_UA);
                rd_kafka_topic_partition_list_add(subs, topic2,
                                                  RD_KAFKA_PARTITION_UA);
                sub_err = rd_kafka_share_subscribe(rkshare, subs);
                TEST_ASSERT(!sub_err, "Failed to subscribe: %s",
                            rd_kafka_err2str(sub_err));
                rd_kafka_topic_partition_list_destroy(subs);
        }

        /* Consume messages from both topics */
        TEST_SAY("Consuming messages from both topics\n");
        for (int i = 0; i < 5; i++) { /* Multiple consume calls */
                err = rd_kafka_share_consume_batch(rkshare, 2000, rkmessages,
                                                   &msg_cnt);
                if (err)
                        rd_kafka_error_destroy(err);

                for (size_t j = 0; j < msg_cnt; j++) {
                        rd_kafka_message_destroy(rkmessages[j]);
                }
                total_msgs += (int)msg_cnt;

                if (total_msgs >= msgcnt * 2)
                        break;
        }

        TEST_SAY("Consumed %d total messages\n", total_msgs);
        cb_state.expected_count = total_msgs;

        if (total_msgs == 0) {
                TEST_SAY("No messages consumed, skipping test\n");
                mtx_destroy(&cb_state.lock);
                cnd_destroy(&cb_state.cond);
                rd_kafka_share_destroy(rkshare);
                SUB_TEST_PASS();
                return;
        }

        /* Initialize commit thread state */
        memset(&state, 0, sizeof(state));
        state.rkshare = rkshare;
        mtx_init(&state.lock, mtx_plain);
        cnd_init(&state.cond);

        TEST_SAY("Starting commit thread\n");
        if (thrd_create(&thread, commit_thread_func, &state) != thrd_success)
                TEST_FAIL("Failed to create thread");

        /* Wait for thread to start */
        mtx_lock(&state.lock);
        while (!state.started)
                cnd_wait(&state.cond, &state.lock);
        mtx_unlock(&state.lock);

        /* Sleep briefly */
        rd_sleep(1);

        TEST_SAY("Calling wakeup - should dispatch ALL pending acks\n");
        rd_kafka_share_wakeup(rkshare);

        /* Wait for commit thread */
        mtx_lock(&state.lock);
        while (!state.finished) {
                cnd_timedwait_ms(&state.cond, &state.lock, 10000);
        }
        mtx_unlock(&state.lock);

        thrd_join(thread, NULL);

        TEST_ASSERT(state.result == RD_KAFKA_RESP_ERR__WAKEUP,
                    "Expected WAKEUP error, got %s",
                    rd_kafka_err2name(state.result));

        /* Wait for all callbacks */
        TEST_SAY("Waiting for all acknowledgement callbacks\n");
        mtx_lock(&cb_state.lock);
        rd_ts_t deadline = test_clock() + (15 * 1000000);
        while (cb_state.callback_count < cb_state.expected_count) {
                rd_ts_t now = test_clock();
                if (now >= deadline) {
                        mtx_unlock(&cb_state.lock);
                        TEST_FAIL(
                            "Timeout: got %d callbacks, expected %d",
                            cb_state.callback_count, cb_state.expected_count);
                }
                int64_t timeout_ms = (deadline - now) / 1000;
                cnd_timedwait_ms(&cb_state.cond, &cb_state.lock,
                                 (int)timeout_ms);
        }
        mtx_unlock(&cb_state.lock);

        TEST_SAY("All callbacks invoked successfully\n");

        mtx_destroy(&state.lock);
        cnd_destroy(&state.cond);
        mtx_destroy(&cb_state.lock);
        cnd_destroy(&cb_state.cond);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


int main_0179_share_consumer_wakeup(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        /* Wakeup tests for consume_batch */
        test_wakeup_before_consume();
        test_wakeup_during_consume();
        test_wakeup_no_data_loss();
        test_multiple_wakeups();

        /* Wakeup tests for commit_sync */
        test_wakeup_before_commit_sync();
        test_wakeup_commit_sync();
        test_wakeup_commit_sync_with_callback();
        test_wakeup_commit_sync_pending_acks();

        return 0;
}
