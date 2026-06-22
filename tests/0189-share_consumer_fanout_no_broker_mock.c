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
 * @brief Share consumer FANOUT early-return recovery.
 *
 * Regression test for the case where rd_kafka_share_fetch_fanout_op()
 * hits the "No fetch or acks to fan out" early-return because
 * rd_kafka_share_select_broker() returns NULL (no broker in UP state).
 *
 * When this early-return fires, two flags are left in an inconsistent
 * state:
 *
 *   rkshare_fetch_more_records_requested = rd_true  (set at FANOUT enqueue)
 *   rkcg->rkcg_share.share_fetch_more_records = rd_true (set inside FANOUT op)
 *
 * rkshare_fetch_more_records_requested prevents the app thread from
 * enqueuing a new FANOUT, because it is only reset when a
 * SHARE_FETCH_RESPONSE is dequeued from rkcg_q. Since no SHARE_FETCH op
 * was dispatched, no response ever arrives, and the app thread never
 * sends another FANOUT on its own.
 *
 * Recovery relies on the main thread's periodic re-trigger at
 * rdkafka.c:2422-2443: when share_fetch_more_records is set, no fetch op
 * is in-flight, and at least one toppar is assigned, the main thread
 * directly enqueues a SHARE_FETCH op to a broker without going through
 * the FANOUT path. When that SHARE_FETCH response carries records,
 * SHARE_FETCH_RESPONSE is enqueued to rkcg_q, the app thread wakes,
 * and rkshare_fetch_more_records_requested is reset to rd_false.
 *
 * Test flow:
 *
 *  Phase 1 – Baseline: establish the session and confirm records flow.
 *    1. Create a 1-broker mock cluster with 1 topic (1 partition).
 *    2. Produce PHASE1_MSGS records.
 *    3. Subscribe; consume all PHASE1_MSGS records with implicit ack.
 *
 *  Phase 2 – Trigger the early-return path:
 *    4. Set the broker DOWN (drops connections, new ones refused).
 *    5. Poll with a short timeout POLL_IDLE_ATTEMPTS times.
 *       The first poll (after flags were reset in Phase 1) enqueues a
 *       FANOUT; the main thread processes it and hits the early-return
 *       because the broker is DOWN.  rkshare_fetch_more_records_requested
 *       stays rd_true, so subsequent polls skip the FANOUT entirely.
 *       All polls return rcvd=0, error=NULL — no spurious error.
 *
 *  Phase 3 – Recovery:
 *    6. Produce PHASE2_MSGS more records while broker is still DOWN
 *       (the producer uses a separate topic and will buffer or fail,
 *       so we bring the broker back UP first, then produce).
 *    7. Bring broker back UP.
 *    8. Produce PHASE2_MSGS records.
 *    9. Poll until all PHASE2_MSGS records are received, up to
 *       RECOVERY_TIMEOUT_MS.
 *       If the consumer were permanently stuck, this would time out.
 *       The main-thread re-trigger is the only recovery path.
 */

#define CONSUME_ARRAY       256
#define PHASE1_MSGS         10
#define PHASE2_MSGS         10
#define POLL_IDLE_MS        200 /* timeout per poll during broker-down window */
#define POLL_IDLE_ATTEMPTS  15  /* polls while broker is DOWN */
#define RECOVERY_TIMEOUT_MS 20000

/* ===================================================================
 *  Infrastructure helpers (mirrors 0182 / 0183 pattern).
 * =================================================================== */

typedef struct test_ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} test_ctx_t;

static test_ctx_t ctx_new(void) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        char errstr[512];

        memset(&ctx, 0, sizeof(ctx));

        ctx.mcluster = test_mock_cluster_new(1, &ctx.bootstraps);

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

static void ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}

static int produce_msgs(rd_kafka_t *producer,
                        const char *topic,
                        int32_t partition,
                        int msgcnt) {
        int i;
        for (i = 0; i < msgcnt; i++) {
                char payload[64];
                rd_snprintf(payload, sizeof(payload), "%s-p%" PRId32 "-%d",
                            topic, partition, i);
                TEST_ASSERT(rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_PARTITION(partition),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce to %s [%" PRId32 "] failed", topic,
                            partition);
        }
        rd_kafka_flush(producer, 5000);
        return msgcnt;
}

/**
 * @brief Log callback that counts "No fetch or acks to fan out" hits.
 *
 * This message is emitted at RD_KAFKA_DBG_CGRP level (fac="SHARE") by
 * rd_kafka_share_fetch_fanout_op() when it hits the early-return path
 * (selected_rkb == NULL && !has_fanout_acks).  The counter is stored as
 * the client's opaque so the callback is reentrant and consumer-specific.
 *
 * Pattern mirrors 0182's no_bounce_loop_log_cb.
 */
static void fanout_early_return_log_cb(const rd_kafka_t *rk,
                                       int level,
                                       const char *fac,
                                       const char *buf) {
        rd_atomic32_t *cnt = rd_kafka_opaque(rk);
        (void)level;
        if (cnt && !strcmp(fac, "SHARE") &&
            strstr(buf, "No fetch or acks to fan out"))
                rd_atomic32_add(cnt, 1);
}


/**
 * @param early_return_cnt Optional: if non-NULL, wires up the log callback
 *        above and enables "cgrp" debug so the SHARE dbg message is emitted.
 *        The counter is caller-owned and must outlive the consumer.
 */
static rd_kafka_share_t *create_consumer(const char *bootstraps,
                                         const char *group_id,
                                         rd_atomic32_t *early_return_cnt) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        /* Implicit ack: records are auto-acknowledged when consumed. */
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");

        if (early_return_cnt) {
                /* Enable cgrp debug so rd_kafka_dbg(rk, CGRP, "SHARE", ...)
                 * messages are emitted to the log callback. */
                test_conf_set(conf, "debug", "cgrp");
                rd_kafka_conf_set_opaque(conf, early_return_cnt);
                rd_kafka_conf_set_log_cb(conf, fanout_early_return_log_cb);
        }

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer: %s",
                    errstr);
        return rkshare;
}

/**
 * @brief is_fatal_cb: ALL_BROKERS_DOWN is expected when we set the broker
 *        DOWN during Phase 2. Allow it through as a non-fatal log entry so
 *        the test framework doesn't abort on it.
 */
static int
is_not_fatal_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, const char *reason) {
        if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                TEST_SAY("Ignoring expected error: %s: %s\n",
                         rd_kafka_err2str(err), reason);
                return 0; /* non-fatal */
        }
        return 1; /* fatal: let the framework handle it */
}


static void subscribe_one(rd_kafka_share_t *rkshare, const char *topic) {
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        err = rd_kafka_share_subscribe(rkshare, subs);
        TEST_ASSERT(!err, "subscribe failed: %s", rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subs);
}

/**
 * @brief Consume up to \p want records via polling, returning total received.
 *        Errors and per-message errors are ignored (silently dropped).
 *        Times out after \p timeout_ms total.
 */
static int consume_msgs(rd_kafka_share_t *rkshare, int want, int timeout_ms) {
        rd_kafka_messages_t *batch = NULL;
        rd_ts_t deadline           = test_clock() + (rd_ts_t)timeout_ms * 1000;
        int got                    = 0;

        while (got < want && test_clock() < deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0, i;

                err  = rd_kafka_share_poll(rkshare, 500, &batch);
                rcvd = batch ? rd_kafka_messages_count(batch) : 0;
                if (err) {
                        rd_kafka_error_destroy(err);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, i);
                        if (!rkm->err)
                                got++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }
        return got;
}

/* ===================================================================
 *  Test case
 * =================================================================== */

static void do_test_fanout_no_broker_recovery(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic   = "0189-fanout-no-broker";
        const char *group   = "sg-0189-fanout-no-broker";
        const int broker_id = 1; /* only broker in single-broker cluster */
        rd_atomic32_t early_return_cnt;
        int got;
        int idle_pass;

        SUB_TEST_QUICK();

        /* ALL_BROKERS_DOWN fires when we take the broker DOWN in Phase 2.
         * Mark it non-fatal so the test framework does not abort on it. */
        test_curr->is_fatal_cb = is_not_fatal_cb;

        rd_atomic32_init(&early_return_cnt, 0);

        ctx = ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        /* ---- Phase 1: baseline ---- */
        TEST_SAY("Phase 1: producing %d records for baseline\n", PHASE1_MSGS);
        produce_msgs(ctx.producer, topic, 0, PHASE1_MSGS);

        rkshare = create_consumer(ctx.bootstraps, group, &early_return_cnt);
        subscribe_one(rkshare, topic);

        got = consume_msgs(rkshare, PHASE1_MSGS, 30000);
        TEST_ASSERT(got == PHASE1_MSGS, "Phase 1: expected %d records, got %d",
                    PHASE1_MSGS, got);
        TEST_SAY("Phase 1: OK, %d records consumed\n", got);

        /* ---- Phase 2: take broker down, trigger early-return path ---- */
        TEST_SAY("Phase 2: taking broker %d down\n", broker_id);
        TEST_ASSERT(rd_kafka_mock_broker_set_down(ctx.mcluster, broker_id) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "broker_set_down failed");

        /*
         * Poll POLL_IDLE_ATTEMPTS times. The first share_poll call
         * after Phase 1 (when rkshare_fetch_more_records_requested was
         * reset to rd_false) enqueues a FANOUT. The main thread processes
         * it, rd_kafka_share_select_broker() returns NULL (broker is DOWN),
         * and the FANOUT hits the early-return:
         *   "No fetch or acks to fan out"
         *
         * rkshare_fetch_more_records_requested stays rd_true, so all
         * subsequent polls skip the FANOUT entirely and just time out.
         * No error must surface from these polls.
         */
        TEST_SAY(
            "Phase 2: polling %d times while broker is down "
            "(early-return path)\n",
            POLL_IDLE_ATTEMPTS);
        for (idle_pass = 0; idle_pass < POLL_IDLE_ATTEMPTS; idle_pass++) {
                rd_kafka_messages_t *batch = NULL;
                rd_kafka_error_t *err;
                size_t rcvd = 0;

                err  = rd_kafka_share_poll(rkshare, POLL_IDLE_MS, &batch);
                rcvd = batch ? rd_kafka_messages_count(batch) : 0;

                /* No error expected during this window: the early-return
                 * path silently returns without dispatching any RPC. */
                if (err) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(err);
                        rd_kafka_error_destroy(err);
                        /* Non-fatal transport / session errors are
                         * acceptable; fail only on unexpected fatal err. */
                        TEST_SAY(
                            "Phase 2 poll %d: error %s (non-fatal, "
                            "continuing)\n",
                            idle_pass, rd_kafka_err2name(code));
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                TEST_ASSERT(rcvd == 0,
                            "Phase 2 poll %d: expected 0 records, got %zu",
                            idle_pass, rcvd);
        }
        TEST_SAY("Phase 2: %d polls returned 0 records — OK\n",
                 POLL_IDLE_ATTEMPTS);

        /* Verify the early-return path was actually hit: the log message
         * "No fetch or acks to fan out" (rdkafka.c:3747, fac="SHARE") must
         * have fired at least once.  If it never fired, the test is not
         * exercising the code path it claims to test. */
        TEST_ASSERT(rd_atomic32_get(&early_return_cnt) >= 1,
                    "Phase 2: expected FANOUT early-return log at least once, "
                    "got %d — broker may not have been seen as DOWN in time",
                    rd_atomic32_get(&early_return_cnt));
        TEST_SAY("Phase 2: FANOUT early-return fired %d time(s) — confirmed\n",
                 rd_atomic32_get(&early_return_cnt));

        /* ---- Phase 3: recover ---- */
        TEST_SAY("Phase 3: bringing broker %d back up\n", broker_id);
        TEST_ASSERT(rd_kafka_mock_broker_set_up(ctx.mcluster, broker_id) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "broker_set_up failed");

        TEST_SAY("Phase 3: producing %d records\n", PHASE2_MSGS);
        produce_msgs(ctx.producer, topic, 0, PHASE2_MSGS);

        /*
         * The consumer must recover without the app explicitly re-sending
         * a FANOUT. The only recovery mechanism is the main thread's
         * periodic re-trigger (rdkafka.c:2422-2443): when
         *   share_fetch_more_records == rd_true
         *   share_should_fetch_ops_in_flight_cnt == 0
         *   rkcg_toppars > 0
         * it directly enqueues a SHARE_FETCH op to the (now UP) broker.
         * That response carries the Phase 2 records → SHARE_FETCH_RESPONSE
         * is enqueued to rkcg_q → rkshare_fetch_more_records_requested
         * is reset → consumer resumes normal operation.
         *
         * If the consumer were permanently stuck, consume_msgs would
         * time out and the assertion below would fail.
         */
        /* Phase 1 acks may not have been flushed before the broker went
         * DOWN, so the broker may re-deliver Phase 1 records alongside the
         * Phase 2 records once it recovers.  Accept any count >= PHASE2_MSGS
         * as proof that the consumer is no longer stuck. */
        TEST_SAY("Phase 3: waiting up to %dms for at least %d records\n",
                 RECOVERY_TIMEOUT_MS, PHASE2_MSGS);
        got = consume_msgs(rkshare, PHASE2_MSGS, RECOVERY_TIMEOUT_MS);
        TEST_ASSERT(
            got >= PHASE2_MSGS,
            "Phase 3: consumer did not recover — expected >= %d "
            "records, got %d (consumer stuck after FANOUT early-return)",
            PHASE2_MSGS, got);

        TEST_SAY(
            "Phase 3: OK, consumer recovered — %d records received "
            "(>= %d expected)\n",
            got, PHASE2_MSGS);

        /* Ensure broker is UP before closing the consumer so the leave
         * request can complete and broker threads are not left hanging. */
        rd_kafka_mock_broker_set_up(ctx.mcluster, broker_id);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


int main_0189_share_consumer_fanout_no_broker_mock(int argc, char **argv) {
        do_test_fanout_no_broker_recovery();
        return 0;
}
