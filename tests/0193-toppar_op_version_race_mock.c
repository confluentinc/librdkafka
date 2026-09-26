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
#include "rdkafka.h"
#include "../src/rdkafka_proto.h"

/**
 * @name Partition ops must not be dropped as outdated when issued
 *       concurrently from the application and the main thread (#5591).
 *
 * Every partition op (FETCH_START, FETCH_STOP, SEEK, PAUSE) is stamped with
 * a new version from the partition's version counter and then enqueued.
 * The op handler drops any op whose version is lower than the last one it
 * served. If two threads take versions vN and vN+1 but enqueue them in the
 * opposite order, the vN op is dropped:
 *  - a dropped FETCH_START leaves the partition unstarted forever,
 *  - a dropped PAUSE (or resume) is silently lost.
 *
 * The first two sub-tests reproduce the reported interleavings
 * deterministically: the window between taking the version and enqueueing
 * the op is only a few instructions wide, so the hidden `ut_toppar_op_enq`
 * hook is used to hold one op inside it, while the other op is issued from
 * another thread.
 *
 * The third sub-test uses no hook: several application threads issue
 * pause, resume and seek on the same partition for a while, and no op may
 * be dropped as outdated. It also fails if the version is taken outside
 * the section that enqueues the op, a window the hook can't hold open.
 *
 * Timing: neither test relies on a sleep to line the two ops up. The held op
 * waits for a signal that the other op has passed through the same window.
 * Once versions are taken and ops enqueued atomically, the other op cannot
 * reach the window while one is held there, so the held op's wait times out
 * after HOOK_WAIT_MS and the ops are enqueued in version order.
 * The only fixed delay is HOOK_SETTLE_MS, which covers the few instructions
 * between the other op leaving the hook and it being enqueued. The second
 * sub-test also delays the mock OffsetFetch response, so its pause is
 * issued before the FETCH_START even on a loaded host; its precondition
 * check fails the test if not.
 */

#define HOOK_WAIT_MS   2000
#define HOOK_SETTLE_MS 100
#define MSGCNT         10
#define RACE_THREADS   4
#define RACE_MS        1000

static mtx_t hook_lock;
static cnd_t hook_cnd;

static struct {
        const char *topic;
        const char *held_op;  /**< Op held in the window */
        const char *other_op; /**< Op that should pass while held_op waits */
        rd_bool_t held_entered;
        rd_bool_t other_done;
        rd_bool_t other_done_in_window; /**< other_op passed the hook while
                                         *   held_op was waiting */
        char first_op[32];              /**< First of the two ops to enter */
} hook_state;

static rd_atomic32_t outdated_fetch_start_cnt;
static rd_atomic32_t outdated_pause_cnt;
static rd_atomic32_t outdated_cnt;


static void
hook_state_reset(const char *topic, const char *held_op, const char *other_op) {
        mtx_lock(&hook_lock);
        memset(&hook_state, 0, sizeof(hook_state));
        hook_state.topic    = topic;
        hook_state.held_op  = held_op;
        hook_state.other_op = other_op;
        mtx_unlock(&hook_lock);

        rd_atomic32_set(&outdated_fetch_start_cnt, 0);
        rd_atomic32_set(&outdated_pause_cnt, 0);
        rd_atomic32_set(&outdated_cnt, 0);
}


/**
 * @brief Called by librdkafka after an op's version is taken and before
 *        the op is enqueued. Holds the first \c held_op until the first
 *        \c other_op has passed, or HOOK_WAIT_MS has elapsed.
 */
static void toppar_op_enq_hook(rd_kafka_t *rk,
                               const char *topic,
                               int32_t partition,
                               const char *op_name,
                               int32_t version) {
        mtx_lock(&hook_lock);

        if (!hook_state.topic || strcmp(topic, hook_state.topic)) {
                mtx_unlock(&hook_lock);
                return;
        }

        if (!hook_state.held_entered && !strcmp(op_name, hook_state.held_op)) {
                int timeout_ms = HOOK_WAIT_MS;

                TEST_SAY("Holding %s (v%" PRId32 ") before enqueue\n", op_name,
                         version);
                hook_state.held_entered = rd_true;
                if (!*hook_state.first_op)
                        rd_snprintf(hook_state.first_op,
                                    sizeof(hook_state.first_op), "%s", op_name);
                cnd_broadcast(&hook_cnd);

                while (!hook_state.other_done &&
                       cnd_timedwait_msp(&hook_cnd, &hook_lock, &timeout_ms) !=
                           thrd_timedout)
                        ;

                hook_state.other_done_in_window = hook_state.other_done;
                TEST_SAY("Releasing %s (v%" PRId32 "): %s passed %s\n", op_name,
                         version, hook_state.other_op,
                         hook_state.other_done ? "while it was held"
                                               : "only after it was released");
                mtx_unlock(&hook_lock);

                if (hook_state.other_done_in_window)
                        rd_usleep(HOOK_SETTLE_MS * 1000, NULL);
                return;
        }

        if (!hook_state.other_done && !strcmp(op_name, hook_state.other_op)) {
                TEST_SAY("%s (v%" PRId32 ") passing\n", op_name, version);
                hook_state.other_done = rd_true;
                if (!*hook_state.first_op)
                        rd_snprintf(hook_state.first_op,
                                    sizeof(hook_state.first_op), "%s", op_name);
                cnd_broadcast(&hook_cnd);
        }

        mtx_unlock(&hook_lock);
}


/**
 * @brief Counts partition ops dropped as outdated.
 */
static void
log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
        if (!strstr(buf, "received outdated op"))
                return;

        rd_atomic32_add(&outdated_cnt, 1);
        if (strstr(buf, "received outdated op FETCH_START"))
                rd_atomic32_add(&outdated_fetch_start_cnt, 1);
        else if (strstr(buf, "received outdated op PAUSE"))
                rd_atomic32_add(&outdated_pause_cnt, 1);
}


static rd_kafka_t *create_consumer(const char *bootstraps,
                                   const char *topic,
                                   rd_bool_t with_hook,
                                   test_conf_log_interceptor_t **interceptorp) {
        rd_kafka_conf_t *conf;
        const char *debug_contexts[] = {"topic", NULL};

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        if (with_hook)
                test_conf_set(conf, "ut_toppar_op_enq",
                              (char *)toppar_op_enq_hook);
        *interceptorp =
            test_conf_set_log_interceptor(conf, log_cb, debug_contexts);

        return test_create_consumer(topic, NULL, conf, NULL);
}


/**
 * @brief The FETCH_START issued from the main thread when the committed
 *        offset arrives must not be dropped by an application pause
 *        that takes a later version but is enqueued first.
 *        A dropped FETCH_START leaves the partition in fetch state `none`,
 *        so nothing is consumed even after resuming.
 */
static void do_test_fetch_start_vs_pause(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        uint64_t testid   = test_id_generate();
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *parts;
        test_conf_log_interceptor_t *interceptor;
        test_msgver_t mv;
        int timeout_ms = 10 * 1000;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, testid, 0, 0, MSGCNT, 100,
                                 "bootstrap.servers", bootstraps, NULL);

        hook_state_reset(topic, "FETCH_START", "PAUSE");
        c = create_consumer(bootstraps, topic, rd_true, &interceptor);

        /* No start offset: the committed offset is fetched first and
         * the FETCH_START is issued from the main thread. */
        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0);
        test_consumer_assign("assign", c, parts);

        mtx_lock(&hook_lock);
        while (!hook_state.held_entered &&
               cnd_timedwait_msp(&hook_cnd, &hook_lock, &timeout_ms) !=
                   thrd_timedout)
                ;
        TEST_ASSERT(hook_state.held_entered,
                    "FETCH_START was not issued within 10s");
        mtx_unlock(&hook_lock);

        /* FETCH_START has its version and is held before its enqueue:
         * pause and resume from this thread. */
        TEST_CALL_ERR__(rd_kafka_pause_partitions(c, parts));
        TEST_CALL_ERR__(rd_kafka_resume_partitions(c, parts));

        /* Ops are served in queue order and the resume was served, so
         * FETCH_START has been served too. */
        TEST_ASSERT(rd_atomic32_get(&outdated_fetch_start_cnt) == 0,
                    "FETCH_START was dropped as outdated %d time(s): "
                    "the partition is never started",
                    rd_atomic32_get(&outdated_fetch_start_cnt));

        test_msgver_init(&mv, testid);
        test_consumer_poll("consume", c, testid, -1, 0, MSGCNT, &mv);
        test_msgver_verify("consume", &mv, TEST_MSGVER_ORDER | TEST_MSGVER_DUP,
                           0, MSGCNT);
        test_msgver_clear(&mv);

        rd_kafka_topic_partition_list_destroy(parts);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        rd_free(interceptor);
        hook_state_reset(NULL, "", "");
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief An application pause must not be dropped by the FETCH_START
 *        issued from the main thread, when the pause takes the earlier
 *        version but is enqueued last.
 *        A dropped pause is not reported to the application and
 *        messages are consumed while the partition is paused.
 */
static void do_test_pause_vs_fetch_start(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        uint64_t testid   = test_id_generate();
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *parts;
        test_conf_log_interceptor_t *interceptor;
        test_msgver_t mv;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, testid, 0, 0, MSGCNT, 100,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Delay the committed offset, so the pause below takes its version
         * before the FETCH_START is issued, even on a loaded host. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetFetch, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            1000);

        hook_state_reset(topic, "PAUSE", "FETCH_START");
        c = create_consumer(bootstraps, topic, rd_true, &interceptor);

        /* No start offset: the committed offset is fetched first and
         * the FETCH_START is issued from the main thread, while the
         * pause below is held before its enqueue. */
        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0);
        test_consumer_assign("assign", c, parts);
        TEST_CALL_ERR__(rd_kafka_pause_partitions(c, parts));

        mtx_lock(&hook_lock);
        TEST_ASSERT(!strcmp(hook_state.first_op, "PAUSE"),
                    "Expected PAUSE to take its version before FETCH_START, "
                    "not %s: the test did not exercise the race",
                    hook_state.first_op);
        mtx_unlock(&hook_lock);

        /* pause_partitions() waits for the pause to be served. */
        TEST_ASSERT(rd_atomic32_get(&outdated_pause_cnt) == 0,
                    "PAUSE was dropped as outdated %d time(s): "
                    "the partition is not paused",
                    rd_atomic32_get(&outdated_pause_cnt));

        test_consumer_poll_no_msgs("paused", c, testid, 3000);

        TEST_CALL_ERR__(rd_kafka_resume_partitions(c, parts));

        test_msgver_init(&mv, testid);
        test_consumer_poll("consume", c, testid, -1, 0, MSGCNT, &mv);
        test_msgver_verify("consume", &mv, TEST_MSGVER_ORDER | TEST_MSGVER_DUP,
                           0, MSGCNT);
        test_msgver_clear(&mv);

        rd_kafka_topic_partition_list_destroy(parts);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        rd_free(interceptor);
        hook_state_reset(NULL, "", "");
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


struct race_thrd_arg {
        rd_kafka_t *c;
        const char *topic;
        int idx;
        rd_atomic32_t *stop;
};

/**
 * @brief Issues pause, resume and seek on partition 0 until stopped.
 */
static int race_thrd_main(void *arg) {
        struct race_thrd_arg *rarg = arg;
        rd_kafka_topic_partition_list_t *parts;
        int i;

        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, rarg->topic, 0)->offset = 0;

        for (i = rarg->idx; !rd_atomic32_get(rarg->stop); i++) {
                rd_kafka_error_t *error;

                switch (i % 3) {
                /* Not TEST_CALL_ERR__(): its per-call output would
                 * serialise the threads and hide the race. */
                case 0:
                        TEST_ASSERT(!rd_kafka_pause_partitions(rarg->c, parts));
                        break;
                case 1:
                        TEST_ASSERT(
                            !rd_kafka_resume_partitions(rarg->c, parts));
                        break;
                default:
                        error = rd_kafka_seek_partitions(rarg->c, parts, 5000);
                        if (error)
                                rd_kafka_error_destroy(error);
                        break;
                }
        }

        rd_kafka_topic_partition_list_destroy(parts);
        return 0;
}


/**
 * @brief No partition op may be dropped as outdated while several
 *        application threads pause, resume and seek the same partition.
 */
static void do_test_concurrent_app_ops(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        uint64_t testid   = test_id_generate();
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *parts;
        test_conf_log_interceptor_t *interceptor;
        struct race_thrd_arg args[RACE_THREADS];
        thrd_t thrds[RACE_THREADS];
        rd_atomic32_t stop;
        rd_ts_t end;
        int i;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, testid, 0, 0, MSGCNT, 100,
                                 "bootstrap.servers", bootstraps, NULL);

        hook_state_reset(NULL, "", "");
        c = create_consumer(bootstraps, topic, rd_false, &interceptor);

        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0)->offset =
            RD_KAFKA_OFFSET_BEGINNING;
        test_consumer_assign("assign", c, parts);

        rd_atomic32_init(&stop, 0);
        for (i = 0; i < RACE_THREADS; i++) {
                args[i].c     = c;
                args[i].topic = topic;
                args[i].idx   = i;
                args[i].stop  = &stop;
                if (thrd_create(&thrds[i], race_thrd_main, &args[i]) !=
                    thrd_success)
                        TEST_FAIL("Failed to create thread %d", i);
        }

        end = test_clock() + RACE_MS * 1000;
        while (test_clock() < end) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 10);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        rd_atomic32_set(&stop, 1);
        for (i = 0; i < RACE_THREADS; i++)
                thrd_join(thrds[i], NULL);

        TEST_ASSERT(rd_atomic32_get(&outdated_cnt) == 0,
                    "%d partition op(s) were dropped as outdated",
                    rd_atomic32_get(&outdated_cnt));

        rd_kafka_topic_partition_list_destroy(parts);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        rd_free(interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_toppar_op_version_race_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        mtx_init(&hook_lock, mtx_plain);
        cnd_init(&hook_cnd);
        rd_atomic32_init(&outdated_fetch_start_cnt, 0);
        rd_atomic32_init(&outdated_pause_cnt, 0);
        rd_atomic32_init(&outdated_cnt, 0);

        do_test_fetch_start_vs_pause();

        do_test_pause_vs_fetch_start();

        do_test_concurrent_app_ops();

        cnd_destroy(&hook_cnd);
        mtx_destroy(&hook_lock);

        return 0;
}
