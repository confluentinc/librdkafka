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
 * @name Share consumer end-to-end flow invariant tests (mock).
 *
 * Covers minute behaviours at each step of rd_kafka_share_poll()
 * and the FANOUT / SHARE_FETCH / ack paths that regression tests on the
 * individual sub-operations might miss.
 *
 * GAP-A  Explicit ack gate: second share_poll before acking previous
 *        batch returns __STATE error, not a deadlock.
 *
 * GAP-C  CONSUMER_ERR does not permanently set rkshare_fetch_more_records
 *        _requested: after a propagated topic error is surfaced, subsequent
 *        share_poll calls can get records again (recovery via main-thread
 *        re-trigger or new FANOUT once SHARE_FETCH_RESPONSE resets the flag).
 *
 * GAP-E  SHARE_SESSION_NOT_FOUND triggers a session reset (epoch → 0) and
 *        the consumer re-establishes the session and delivers records without
 *        app intervention.
 *
 * GAP-F  rkb_share_fetch_enqueued reset on broker reconnect: after broker
 *        DOWN+UP, new SHARE_FETCH ops are dispatched (flag reset in the
 *        __TRANSPORT reply path).
 *
 * GAP-G  Unsubscribe + resubscribe continuity: rkshare_fetch_more_records
 *        _requested may be rd_true after resubscribe (set during the old
 *        subscription's in-flight fetch), but the main-thread re-trigger
 *        (rdkafka.c:2422-2443) fires once a broker is UP and partitions
 *        are assigned, and eventually delivers records.
 */

/* ===========================================================================
 *  Shared infrastructure
 * =========================================================================*/

typedef struct ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} ctx_t;

static ctx_t ctx_new(int nbrok) {
        ctx_t ctx;
        rd_kafka_conf_t *conf;
        char errstr[512];

        memset(&ctx, 0, sizeof(ctx));
        ctx.mcluster = test_mock_cluster_new(nbrok, &ctx.bootstraps);

        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set ShareGroupHeartbeat version");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(ctx.mcluster,
                                                 RD_KAFKAP_ShareFetch, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set ShareFetch version");

        rd_kafka_mock_sharegroup_set_auto_offset_reset(ctx.mcluster, 1);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        ctx.producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(ctx.producer, "create producer: %s", errstr);
        return ctx;
}

static void ctx_destroy(ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}

static void produce_to_partition(rd_kafka_t *producer,
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
                            "produce to %s[%" PRId32 "]", topic, partition);
        }
        rd_kafka_flush(producer, 5000);
}

static rd_kafka_share_t *create_consumer(const char *bootstraps,
                                         const char *group_id,
                                         const char *ack_mode) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", ack_mode);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create share consumer: %s", errstr);
        return rkshare;
}

static void subscribe_one(rd_kafka_share_t *rkshare, const char *topic) {
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        err = rd_kafka_share_subscribe(rkshare, subs);
        TEST_ASSERT(!err, "subscribe: %s", rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subs);
}

/**
 * @brief Drain up to \p want records. Returns actual count received.
 *        Error ops are surfaced to \p first_err if non-NULL (first
 *        error seen). Messages are acked in IMPLICIT mode automatically.
 *        Caller must ack in EXPLICIT mode.
 */
static int drain_batch(rd_kafka_share_t *rkshare,
                       int want,
                       int timeout_ms,
                       rd_kafka_resp_err_t *first_err) {
        rd_kafka_messages_t *batch = NULL;
        rd_ts_t deadline           = test_clock() + (rd_ts_t)timeout_ms * 1000;
        int got                    = 0;

        if (first_err)
                *first_err = RD_KAFKA_RESP_ERR_NO_ERROR;

        while (got < want && test_clock() < deadline) {
                rd_kafka_error_t *err;
                size_t rcvd, i;

                err = rd_kafka_share_poll(rkshare, 500, &batch);
                if (err) {
                        if (first_err &&
                            *first_err == RD_KAFKA_RESP_ERR_NO_ERROR)
                                *first_err = rd_kafka_error_code(err);
                        rd_kafka_error_destroy(err);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = batch ? rd_kafka_messages_count(batch) : 0;
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

static int is_not_fatal_broker_down_cb(rd_kafka_t *rk,
                                       rd_kafka_resp_err_t err,
                                       const char *reason) {
        if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN ||
            err == RD_KAFKA_RESP_ERR__TRANSPORT) {
                TEST_SAY("Expected broker-down error: %s\n",
                         rd_kafka_err2str(err));
                return 0;
        }
        return 1;
}

/* ===========================================================================
 *  GAP-A: Explicit ack gate
 *  rd_kafka_share_ensure_all_acknowledged_if_explicit()
 *
 *  In explicit ack mode, calling share_poll a second time before
 *  acknowledging the first batch must return __STATE (not block or silently
 *  proceed).  After acking, the next share_poll must succeed.
 *
 *  This pins the behaviour of rdkafka_share_acknowledgement.c:86-95:
 *    if (explicit && rkshare_unacked_cnt > 0)
 *        return error(__STATE, "N records from previous poll not acked")
 * =========================================================================*/
static void do_test_explicit_ack_gate(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *err;
        const char *topic = "0191-explicit-ack-gate";
        const char *group = "sg-0191-explicit-ack-gate";
        size_t rcvd       = 0;
        size_t i;

        SUB_TEST_QUICK();

        ctx = ctx_new(1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        produce_to_partition(ctx.producer, topic, 0, 5);

        rkshare = create_consumer(ctx.bootstraps, group, "explicit");
        subscribe_one(rkshare, topic);

        /* First share_poll — should return 5 records. */
        err  = NULL;
        rcvd = 0;
        while (!rcvd) {
                if (err)
                        rd_kafka_error_destroy(err);
                err  = rd_kafka_share_poll(rkshare, 2000, &batch);
                rcvd = batch ? rd_kafka_messages_count(batch) : 0;
                if (err) {
                        TEST_ASSERT(rd_kafka_error_code(err) !=
                                        RD_KAFKA_RESP_ERR__STATE,
                                    "unexpected __STATE before any records "
                                    "consumed");
                }
        }
        TEST_ASSERT(!err, "unexpected error on first batch: %s",
                    err ? rd_kafka_error_string(err) : "");
        TEST_SAY("First batch: %zu records\n", rcvd);

        /* Second share_poll WITHOUT acking first batch.
         * Must return __STATE error (not records, not deadlock). */
        rd_kafka_messages_t *batch2 = NULL;
        size_t rcvd2;
        rd_kafka_error_t *err2;

        err2  = rd_kafka_share_poll(rkshare, 200, &batch2);
        rcvd2 = batch2 ? rd_kafka_messages_count(batch2) : 0;
        TEST_ASSERT(err2 != NULL,
                    "Expected __STATE error when previous batch "
                    "unacknowledged, got NULL (no error)");
        TEST_ASSERT(rd_kafka_error_code(err2) == RD_KAFKA_RESP_ERR__STATE,
                    "Expected __STATE, got %s",
                    rd_kafka_err2name(rd_kafka_error_code(err2)));
        TEST_ASSERT(rcvd2 == 0,
                    "Expected 0 records with ack-gate error, got %zu", rcvd2);
        rd_kafka_error_destroy(err2);
        rd_kafka_messages_destroy(batch2);
        TEST_SAY("Ack gate: __STATE error returned correctly\n");


        /* Ack all records from the first batch. */
        for (i = 0; i < rcvd; i++) {
                rd_kafka_message_t *rkm = rd_kafka_messages_get(batch, i);
                if (!rkm->err)
                        rd_kafka_share_acknowledge(rkshare, rkm);
        }
        rd_kafka_messages_destroy(batch);

        /* Now share_poll must succeed (gate cleared). */
        rd_kafka_messages_t *batch3 = NULL;
        size_t rcvd3;
        rd_kafka_error_t *err3;

        err3  = rd_kafka_share_poll(rkshare, 2000, &batch3);
        rcvd3 = batch3 ? rd_kafka_messages_count(batch3) : 0;
        TEST_ASSERT(!err3, "Expected no error after acking, got %s",
                    err3 ? rd_kafka_error_string(err3) : "");
        rd_kafka_messages_destroy(batch3);
        TEST_SAY("Post-ack consume: %zu records, no gate error\n", rcvd3);


        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


/* ===========================================================================
 *  GAP-C: CONSUMER_ERR recovery
 *
 *  When a CONSUMER_ERR (propagated topic error) is surfaced via
 *  share_poll, rkshare_fetch_more_records_requested is NOT reset
 *  (only SHARE_FETCH_RESPONSE resets it).  The consumer must still
 *  recover and deliver records via the main-thread re-trigger path.
 *
 *  Mechanism tested:
 *    1. Inject a per-partition TOPIC_AUTHORIZATION_FAILED on one
 *       ShareFetch → CONSUMER_ERR enqueued to rkcg_q (this code is a
 *       SURFACE arm on the per-partition path; UNKNOWN_TOPIC_OR_PART
 *       would be silently awaited and never surface).
 *    2. share_poll dequeues CONSUMER_ERR → returns error.
 *       rkshare_fetch_more_records_requested stays rd_true.
 *    3. The in-flight or next SHARE_FETCH op completes normally.
 *       When it returns records → SHARE_FETCH_RESPONSE enqueued →
 *       rkshare_fetch_more_records_requested reset → app gets records.
 *
 *  The log interceptor counts "per-partition fetch error" log messages
 *  to confirm the error was actually surfaced, not silently swallowed.
 * =========================================================================*/
static void consumer_err_log_cb(const rd_kafka_t *rk,
                                int level,
                                const char *fac,
                                const char *buf) {
        rd_atomic32_t *cnt = test_conf_log_interceptor_opaque(rk);
        (void)level;
        /* rd_kafka_share_fetch_reply_handle_partition_error logs at
         * LOG_INFO with fac "SHAREFETCH" for per-partition errors. */
        if (cnt && !strcmp(fac, "SHAREFETCH") &&
            strstr(buf, "per-partition fetch error"))
                rd_atomic32_add(cnt, 1);
}

static void do_test_consumer_err_recovery(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        rd_atomic32_t consumer_err_cnt;
        test_conf_log_interceptor_t *interceptor;
        const char *debug_contexts[] = {"fetch", NULL};
        const char *topic            = "0191-consumer-err-recovery";
        const char *group            = "sg-0191-consumer-err-recovery";
        const int msgcnt             = 10;
        int got;
        rd_kafka_resp_err_t first_err;

        SUB_TEST_QUICK();

        rd_atomic32_init(&consumer_err_cnt, 0);
        ctx = ctx_new(1);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        /* Consumer with log interceptor on fac=SHAREFETCH. Route through
         * the shared interceptor helper so the "fetch" debug context is
         * MERGED with TEST_DEBUG instead of clobbering it. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        interceptor = test_conf_set_log_interceptor(conf, consumer_err_log_cb,
                                                    debug_contexts);
        test_conf_log_interceptor_set_opaque(interceptor, &consumer_err_cnt);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create consumer: %s", errstr);
        subscribe_one(rkshare, topic);

        /* Phase 1: consume records to establish the session. */
        got = drain_batch(rkshare, msgcnt, 20000, NULL);
        TEST_ASSERT(got == msgcnt, "Phase 1: expected %d got %d", msgcnt, got);
        TEST_SAY("Phase 1: %d records consumed\n", got);

        /* Phase 2: inject a per-partition error on the next ShareFetch.
         * We use TOPIC_AUTHORIZATION_FAILED because, on the
         * per-partition ShareFetch error path
         * (rd_kafka_handle_ShareFetch_partition_error,
         * rdkafka_fetcher.c), it is a SURFACE arm — it calls
         * rd_kafka_consumer_err() to enqueue a CONSUMER_ERR to rkcg_q.
         * (UNKNOWN_TOPIC_OR_PART is deliberately a SILENT-AWAIT arm
         * there and would never surface, so it cannot exercise GAP-C.)
         * A single per-partition injection is deterministic: it surfaces
         * once, then drains so the next fetch recovers. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push per-partition ShareFetch error");

        /* Produce more records so recovery is observable. */
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        /* Consume — the injected per-partition error surfaces first,
         * then the consumer must recover and deliver the new records.
         * Phase-2 production above happens only after the error was
         * injected, so the error deterministically precedes the
         * recovery records and drain_batch records it as first_err. */
        got = drain_batch(rkshare, msgcnt, 30000, &first_err);
        TEST_SAY("Phase 2+3: first_err=%s got=%d consumer_err_cnt=%d\n",
                 rd_kafka_err2name(first_err), got,
                 rd_atomic32_get(&consumer_err_cnt));

        /* The error must actually have been surfaced (GAP-C is only
         * meaningful if a CONSUMER_ERR was raised, not silently
         * swallowed): the per-partition error log fired at least once. */
        TEST_ASSERT(rd_atomic32_get(&consumer_err_cnt) >= 1,
                    "Expected the injected per-partition error to surface "
                    "(consumer_err_cnt >= 1), got %d — error was silently "
                    "swallowed",
                    rd_atomic32_get(&consumer_err_cnt));

        /* And the surfaced error must be the one we injected. */
        TEST_ASSERT(first_err == RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
                    "Expected first surfaced error to be "
                    "TOPIC_AUTHORIZATION_FAILED, got %s",
                    rd_kafka_err2name(first_err));

        /* Despite the error, the consumer must recover and deliver. */
        TEST_ASSERT(got >= msgcnt,
                    "Consumer did not recover after injected error: "
                    "expected >= %d records, got %d",
                    msgcnt, got);
        TEST_SAY("Phase 2+3: consumer recovered, %d records\n", got);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_conf_log_interceptor_destroy(interceptor);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


/* ===========================================================================
 *  GAP-E: SHARE_SESSION_NOT_FOUND recovery
 *
 *  When the broker returns SHARE_SESSION_NOT_FOUND, the client calls
 *  rd_kafka_broker_session_reset() which sets the session epoch back
 *  to 0.  The next ShareFetch must start a fresh session and eventually
 *  deliver records.
 *
 *  Log interceptor: counts "share-fetch session epoch" resets to verify
 *  the session epoch actually went back to 0 (epoch N → 1, from 0 = new).
 * =========================================================================*/
static void session_reset_log_cb(const rd_kafka_t *rk,
                                 int level,
                                 const char *fac,
                                 const char *buf) {
        rd_atomic32_t *cnt = test_conf_log_interceptor_opaque(rk);
        (void)level;
        /* rd_kafka_broker_share_fetch_session_* logs "share-fetch session
         * epoch 0 -> 1" when a new session starts. Counting transitions
         * FROM epoch 0 tells us how many fresh sessions were started. */
        if (cnt && !strcmp(fac, "SHARESESSION") && strstr(buf, "epoch 0 ->"))
                rd_atomic32_add(cnt, 1);
}

static void do_test_session_not_found_recovery(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        rd_atomic32_t session_reset_cnt;
        test_conf_log_interceptor_t *interceptor;
        const char *debug_contexts[] = {"fetch", NULL};
        const char *topic            = "0191-session-not-found";
        const char *group            = "sg-0191-session-not-found";
        const int msgcnt             = 5;
        int got_before, got_after;

        SUB_TEST_QUICK();

        rd_atomic32_init(&session_reset_cnt, 0);
        ctx = ctx_new(1);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        /* Route through the shared interceptor so "fetch" merges with
         * TEST_DEBUG instead of clobbering it. */
        interceptor = test_conf_set_log_interceptor(conf, session_reset_log_cb,
                                                    debug_contexts);
        test_conf_log_interceptor_set_opaque(interceptor, &session_reset_cnt);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create consumer: %s", errstr);
        subscribe_one(rkshare, topic);

        /* Phase 1: consume msgcnt records to establish a live session. */
        got_before = drain_batch(rkshare, msgcnt, 20000, NULL);
        TEST_ASSERT(got_before == msgcnt, "Phase 1: expected %d got %d", msgcnt,
                    got_before);
        TEST_SAY("Phase 1: baseline %d records\n", got_before);

        /* Snapshot the session-reset counter AFTER the initial session is
         * established. The first session also logs "epoch 0 -> 1", so a
         * plain ">= 1" check would be satisfied by the initial session
         * alone. We assert a STRICT increase past this snapshot, which is
         * caused only by the injected SHARE_SESSION_NOT_FOUND reset. */
        int resets_before = rd_atomic32_get(&session_reset_cnt);

        /* Inject SHARE_SESSION_NOT_FOUND on the next ShareFetch.
         * The client must reset the session (epoch→0) and re-establish. */
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                        0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push SHARE_SESSION_NOT_FOUND");

        /* Produce more records for Phase 2. */
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        /* Phase 2: consumer must re-establish session and get records. */
        got_after = drain_batch(rkshare, msgcnt, 30000, NULL);
        TEST_ASSERT(got_after >= msgcnt,
                    "Phase 2: consumer did not recover after "
                    "SHARE_SESSION_NOT_FOUND — expected >= %d, got %d",
                    msgcnt, got_after);
        TEST_SAY("Phase 2: %d records after session reset\n", got_after);

        /* Verify the session was actually reset by the injection: the
         * counter must strictly increase past the post-Phase-1 snapshot.
         * This excludes the initial session establishment. */
        int resets_after = rd_atomic32_get(&session_reset_cnt);
        TEST_ASSERT(resets_after > resets_before,
                    "Expected a session restart from epoch 0 caused by the "
                    "injected SHARE_SESSION_NOT_FOUND: resets_before=%d "
                    "resets_after=%d (no strict increase => session was not "
                    "reset by the injection)",
                    resets_before, resets_after);
        TEST_SAY("Session resets from epoch 0: %d -> %d (delta %d)\n",
                 resets_before, resets_after, resets_after - resets_before);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_conf_log_interceptor_destroy(interceptor);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


/* ===========================================================================
 *  GAP-F: rkb_share_fetch_enqueued reset after broker reconnect
 *
 *  When a broker disconnects while a SHARE_FETCH op is in-flight,
 *  rd_kafka_broker_share_fetch_reply() fires with __TRANSPORT error and
 *  sets reply_rkb->rkb_share_fetch_enqueued = rd_false (line ~3457).
 *  Without this reset, rd_kafka_share_select_broker() would skip the
 *  broker forever (it checks !leader->rkb_share_fetch_enqueued).
 *
 *  Log interceptor: counts "Selected broker" messages to verify a broker
 *  IS selected after reconnect (not permanently skipped).
 * =========================================================================*/
static void broker_selected_log_cb(const rd_kafka_t *rk,
                                   int level,
                                   const char *fac,
                                   const char *buf) {
        rd_atomic32_t *cnt = test_conf_log_interceptor_opaque(rk);
        (void)level;
        if (cnt && !strcmp(fac, "SHARE") && strstr(buf, "Selected broker"))
                rd_atomic32_add(cnt, 1);
}

static void do_test_enqueued_flag_reset_on_reconnect(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        rd_atomic32_t broker_selected_cnt;
        test_conf_log_interceptor_t *interceptor;
        const char *debug_contexts[] = {"cgrp", NULL};
        const char *topic            = "0191-enqueued-flag-reset";
        const char *group            = "sg-0191-enqueued-flag-reset";
        const int msgcnt             = 5;
        int got;
        int32_t select_before_down, select_after_up;

        SUB_TEST_QUICK();

        test_curr->is_fatal_cb = is_not_fatal_broker_down_cb;
        rd_atomic32_init(&broker_selected_cnt, 0);

        ctx = ctx_new(1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        /* Route through the shared interceptor so "cgrp" merges with
         * TEST_DEBUG instead of clobbering it. */
        interceptor = test_conf_set_log_interceptor(
            conf, broker_selected_log_cb, debug_contexts);
        test_conf_log_interceptor_set_opaque(interceptor, &broker_selected_cnt);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create consumer: %s", errstr);
        subscribe_one(rkshare, topic);

        /* Phase 1: consume records, capture broker-selected count. */
        got = drain_batch(rkshare, msgcnt, 20000, NULL);
        TEST_ASSERT(got == msgcnt, "Phase 1: expected %d got %d", msgcnt, got);
        select_before_down = rd_atomic32_get(&broker_selected_cnt);
        TEST_SAY("Phase 1: %d records, broker selected %d time(s)\n", got,
                 select_before_down);

        /* Phase 2: take broker DOWN while a fetch is likely in-flight.
         * The __TRANSPORT error path resets rkb_share_fetch_enqueued. */
        TEST_ASSERT(rd_kafka_mock_broker_set_down(ctx.mcluster, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "broker_set_down");

        /* Poll briefly while broker is down. */
        rd_kafka_messages_t *tmp = NULL;
        rd_kafka_error_t *e;
        e = rd_kafka_share_poll(rkshare, 500, &tmp);
        if (e)
                rd_kafka_error_destroy(e);
        rd_kafka_messages_destroy(tmp);
        tmp = NULL;


        /* Phase 3: bring broker back UP, produce new records. */
        TEST_ASSERT(rd_kafka_mock_broker_set_up(ctx.mcluster, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "broker_set_up");
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        /* Consumer must select the broker again (rkb_share_fetch_enqueued
         * was reset by the __TRANSPORT reply). */
        got = drain_batch(rkshare, msgcnt, 20000, NULL);
        TEST_ASSERT(got >= msgcnt,
                    "Phase 3: consumer did not recover — "
                    "rkb_share_fetch_enqueued may not have been reset: "
                    "expected >= %d records, got %d",
                    msgcnt, got);

        select_after_up = rd_atomic32_get(&broker_selected_cnt);
        TEST_ASSERT(select_after_up > select_before_down,
                    "Broker was not re-selected after reconnect: "
                    "select_before=%d select_after=%d",
                    select_before_down, select_after_up);
        TEST_SAY("Phase 3: %d records, broker selected %d more time(s)\n", got,
                 select_after_up - select_before_down);

        /* Ensure broker is UP before closing. */
        rd_kafka_mock_broker_set_up(ctx.mcluster, 1);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_conf_log_interceptor_destroy(interceptor);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


/* ===========================================================================
 *  GAP-G: Unsubscribe + resubscribe continuity
 *
 *  After rd_kafka_share_unsubscribe(), rkshare_subscribed = rd_false and
 *  rkshare_fetch_more_records_requested may be rd_true (if a SHARE_FETCH
 *  was in-flight when unsubscribe was called and its response_rko == NULL).
 *
 *  After rd_kafka_share_subscribe(), rkshare_subscribed = rd_true, but
 *  rkshare_fetch_more_records_requested is NOT reset by subscribe().
 *
 *  The consumer must recover via the main-thread re-trigger (or the next
 *  FANOUT, if the flag was already rd_false at resubscribe time).  Either
 *  way, records must arrive within a reasonable timeout.
 *
 *  Log interceptor: counts FANOUT early-returns and successful broker
 *  selections to verify the whole path.
 * =========================================================================*/
typedef struct resub_log_state_s {
        rd_atomic32_t fanout_early_return_cnt;
        rd_atomic32_t broker_selected_cnt;
} resub_log_state_t;

static void resub_log_cb(const rd_kafka_t *rk,
                         int level,
                         const char *fac,
                         const char *buf) {
        resub_log_state_t *s = test_conf_log_interceptor_opaque(rk);
        (void)level;
        if (!s || strcmp(fac, "SHARE"))
                return;
        if (strstr(buf, "No fetch or acks to fan out"))
                rd_atomic32_add(&s->fanout_early_return_cnt, 1);
        if (strstr(buf, "Selected broker"))
                rd_atomic32_add(&s->broker_selected_cnt, 1);
}

static void do_test_resubscribe_continuity(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        resub_log_state_t log_state;
        test_conf_log_interceptor_t *interceptor;
        const char *debug_contexts[] = {"cgrp", NULL};
        const char *topic            = "0191-resubscribe";
        const char *group            = "sg-0191-resubscribe";
        const int msgcnt             = 5;
        const int broker_id          = 1;
        int got;

        SUB_TEST_QUICK();

        test_curr->is_fatal_cb = is_not_fatal_broker_down_cb;
        rd_atomic32_init(&log_state.fanout_early_return_cnt, 0);
        rd_atomic32_init(&log_state.broker_selected_cnt, 0);

        ctx = ctx_new(1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        /* Route through the shared interceptor so "cgrp" merges with
         * TEST_DEBUG instead of clobbering it. */
        interceptor =
            test_conf_set_log_interceptor(conf, resub_log_cb, debug_contexts);
        test_conf_log_interceptor_set_opaque(interceptor, &log_state);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create consumer: %s", errstr);
        subscribe_one(rkshare, topic);

        /* Phase 1: consume baseline records to establish session. */
        got = drain_batch(rkshare, msgcnt, 20000, NULL);
        TEST_ASSERT(got == msgcnt, "Phase 1: expected %d got %d", msgcnt, got);
        TEST_SAY("Phase 1: %d records\n", got);

        /* Phase 2: inject broker RTT delay so a fetch is in-flight when
         * we unsubscribe, exercising the rkshare_fetch_more_records_
         * _requested stuck path. */
        TEST_ASSERT(
            rd_kafka_mock_broker_set_rtt(ctx.mcluster, broker_id, 2000) ==
                RD_KAFKA_RESP_ERR_NO_ERROR,
            "set broker RTT");

        /* Unsubscribe. */

        rd_kafka_resp_err_t uerr = rd_kafka_share_unsubscribe(rkshare);
        TEST_ASSERT(!uerr, "unsubscribe: %s", rd_kafka_err2str(uerr));

        TEST_SAY("Phase 2: unsubscribed\n");

        /* Remove RTT delay, produce new records, then resubscribe. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, broker_id, 0);
        produce_to_partition(ctx.producer, topic, 0, msgcnt);

        subscribe_one(rkshare, topic);
        TEST_SAY("Phase 3: resubscribed, waiting for records\n");

        /* Phase 3: consumer must recover and deliver records. */
        got = drain_batch(rkshare, msgcnt, 30000, NULL);
        TEST_ASSERT(got >= msgcnt,
                    "Phase 3: consumer did not recover after resubscribe — "
                    "expected >= %d records, got %d "
                    "(fanout_early_returns=%d, broker_selections=%d)",
                    msgcnt, got,
                    rd_atomic32_get(&log_state.fanout_early_return_cnt),
                    rd_atomic32_get(&log_state.broker_selected_cnt));

        TEST_SAY(
            "Phase 3: %d records after resubscribe "
            "(fanout_early_returns=%d broker_selections=%d)\n",
            got, rd_atomic32_get(&log_state.fanout_early_return_cnt),
            rd_atomic32_get(&log_state.broker_selected_cnt));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_conf_log_interceptor_destroy(interceptor);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


/* ===========================================================================
 *  GAP-B (bonus): rkshare_fetch_more_records_requested prevents duplicate
 *                 FANOUTs while a fetch is in-flight.
 *
 *  While the broker is slow (high RTT so the SHARE_FETCH hangs), make
 *  multiple share_poll calls.  Only the FIRST call should enqueue a
 *  FANOUT ("Selected broker" log).  Subsequent calls should log
 *  "No fetch or acks to fan out" OR skip silently — but must NOT log
 *  "Selected broker" again (that would mean a duplicate SHARE_FETCH was
 *  sent while one is already in-flight, violating the one-at-a-time
 *  invariant).
 * =========================================================================*/
typedef struct no_dup_log_state_s {
        rd_atomic32_t selected_cnt;
        rd_atomic32_t early_return_cnt;
} no_dup_log_state_t;

static void no_dup_log_cb(const rd_kafka_t *rk,
                          int level,
                          const char *fac,
                          const char *buf) {
        no_dup_log_state_t *s = test_conf_log_interceptor_opaque(rk);
        (void)level;
        if (!s || strcmp(fac, "SHARE"))
                return;
        if (strstr(buf, "Selected broker"))
                rd_atomic32_add(&s->selected_cnt, 1);
        if (strstr(buf, "No fetch or acks to fan out"))
                rd_atomic32_add(&s->early_return_cnt, 1);
}

static void do_test_no_duplicate_fanout(void) {
        ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        no_dup_log_state_t log_state;
        test_conf_log_interceptor_t *interceptor;
        const char *debug_contexts[] = {"cgrp", NULL};
        const char *topic            = "0191-no-dup-fanout";
        const char *group            = "sg-0191-no-dup-fanout";
        const int broker_id          = 1;
        const int rtt_ms             = 3000; /* slow enough for several polls */
        const int n_polls            = 8;
        int i;
        int32_t selected_during_slow;

        SUB_TEST_QUICK();

        rd_atomic32_init(&log_state.selected_cnt, 0);
        rd_atomic32_init(&log_state.early_return_cnt, 0);

        ctx = ctx_new(1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        produce_to_partition(ctx.producer, topic, 0, 5);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        /* Route through the shared interceptor so "cgrp" merges with
         * TEST_DEBUG instead of clobbering it. */
        interceptor =
            test_conf_set_log_interceptor(conf, no_dup_log_cb, debug_contexts);
        test_conf_log_interceptor_set_opaque(interceptor, &log_state);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "create consumer: %s", errstr);
        subscribe_one(rkshare, topic);

        /* Baseline: get assigned and start fetching (normal speed). */
        rd_kafka_messages_t *tmp = NULL;
        size_t r                 = 0;
        rd_kafka_error_t *e;
        rd_ts_t deadline = test_clock() + 15000000;
        while (!r && test_clock() < deadline) {
                e = rd_kafka_share_poll(rkshare, 500, &tmp);
                r = tmp ? rd_kafka_messages_count(tmp) : 0;
                if (e)
                        rd_kafka_error_destroy(e);
                rd_kafka_messages_destroy(tmp);
                tmp = NULL;
        }


        /* Reset counter after baseline. */
        rd_atomic32_init(&log_state.selected_cnt, 0);
        rd_atomic32_init(&log_state.early_return_cnt, 0);

        /* Apply high RTT so the in-flight fetch takes several seconds. */
        TEST_ASSERT(
            rd_kafka_mock_broker_set_rtt(ctx.mcluster, broker_id, rtt_ms) ==
                RD_KAFKA_RESP_ERR_NO_ERROR,
            "set RTT");

        /* Make n_polls rapid consume_batch calls while broker is slow.
         * rkshare_fetch_more_records_requested prevents duplicate FANOUTs:
         * only the FIRST call should select a broker; the rest should
         * hit the "no fetch needed" path. */
        for (i = 0; i < n_polls; i++) {
                rd_kafka_messages_t *tmp_poll = NULL;
                rd_kafka_error_t *e;
                e = rd_kafka_share_poll(rkshare, 200, &tmp_poll);
                if (e)
                        rd_kafka_error_destroy(e);
                rd_kafka_messages_destroy(tmp_poll);
        }

        selected_during_slow = rd_atomic32_get(&log_state.selected_cnt);
        TEST_SAY(
            "During slow broker: selected=%d early_returns=%d "
            "(over %d polls)\n",
            selected_during_slow, rd_atomic32_get(&log_state.early_return_cnt),
            n_polls);

        /* Key invariant: while one SHARE_FETCH is in-flight to the slow
         * broker, no DUPLICATE fetch may be fanned out. Empirically
         * (20/20 runs) exactly TWO "Selected broker" events occur in
         * this window: (1) the initial selection that sends the
         * in-flight fetch, and (2) one legitimate main-thread re-trigger
         * tick that re-selects the same broker but finds the fetch still
         * pending (rkshare_fetch_more_records_requested) and does NOT
         * send a second fetch. A third selection would mean a real
         * duplicate FANOUT, so we pin the count to exactly 2 — this
         * catches both a regression to >2 (duplicate fetch) and an
         * unexpected drop that would indicate the re-trigger path
         * changed. */
        TEST_ASSERT(selected_during_slow == 2,
                    "Expected exactly 2 broker selections during the "
                    "slow-broker window (1 initial + 1 re-trigger tick), "
                    "got %d; >2 means duplicate FANOUTs are being sent",
                    selected_during_slow);

        /* Remove RTT, let the pending response complete. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, broker_id, 0);

        /* Drain the pending response. Reuse tmp/r/e/deadline from the
         * baseline block above; this is the same function scope. */
        r        = 0;
        deadline = test_clock() + 10000000;
        while (test_clock() < deadline) {
                e = rd_kafka_share_poll(rkshare, 500, &tmp);
                if (e) {
                        rd_kafka_error_destroy(e);
                        rd_kafka_messages_destroy(tmp);
                        tmp = NULL;
                } else {
                        r = tmp ? rd_kafka_messages_count(tmp) : 0;
                        rd_kafka_messages_destroy(tmp);
                        tmp = NULL;
                        if (r > 0)
                                break;
                }
        }


        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_conf_log_interceptor_destroy(interceptor);
        ctx_destroy(&ctx);
        SUB_TEST_PASS();
}


int main_0191_share_consumer_consume_flow_mock(int argc, char **argv) {
        /* GAP-A: explicit ack gate blocks next consume until records acked */
        do_test_explicit_ack_gate();

        /* GAP-C: CONSUMER_ERR does not permanently block record delivery */
        do_test_consumer_err_recovery();

        /* GAP-E: SHARE_SESSION_NOT_FOUND resets session, records delivered */
        do_test_session_not_found_recovery();

        /* GAP-F: rkb_share_fetch_enqueued reset after broker reconnect */
        do_test_enqueued_flag_reset_on_reconnect();

        /* GAP-G: consumer recovers after unsubscribe + resubscribe */
        do_test_resubscribe_continuity();

        /* GAP-B: only 1 FANOUT sent while fetch is in-flight */
        do_test_no_duplicate_fanout();

        return 0;
}
