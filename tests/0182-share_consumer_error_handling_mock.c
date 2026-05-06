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
 * @brief Share consumer top-level error propagation through ShareAcknowledge.
 *
 * Verifies that a top-level err on a ShareAcknowledge response reaches
 * the per-partition rktpar->err that commit_sync reads, with no
 * _IN_PROGRESS sentinel leaking to the caller. The broker-thread helper
 * (rd_kafka_share_fetch_op_reply_with_err) sets err on each batch in
 * ack_details; the main reply handler copies batch->rktpar->err into
 * rkcg_commit_sync_request.results.
 *
 * Out of scope here (need mock enhancements not yet available):
 *   - Per-partition AcknowledgementErrorCode injection (mock only supports
 *     top-level err push)
 *   - Partition missing from ShareAcknowledge response (no mock API to
 *     drop a partition from response)
 */

#define CONSUME_ARRAY 1024

/* ===================================================================
 *  Mock broker infrastructure (same pattern as 0176).
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

static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}

static rd_kafka_share_t *create_mock_share_consumer(const char *bootstraps,
                                                    const char *group_id,
                                                    const char *ack_mode) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", ack_mode);

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");
        return rkshare;
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

static void mock_produce(rd_kafka_t *producer, const char *topic, int msgcnt) {
        int i;
        for (i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "%s-%d", topic, i);
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
 * @brief Consume up to msgcnt records and ACCEPT each, returning count
 *        actually acknowledged.
 */
static int consume_and_ack_all(rd_kafka_share_t *rkshare, int msgcnt) {
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int acked    = 0;
        int attempts = 0;

        while (acked < msgcnt && attempts++ < 30) {
                size_t rcvd = 0;
                size_t j;
                rd_kafka_error_t *error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages, &rcvd);
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

/* ===================================================================
 *  Parameterized helper: inject one top-level err on next
 *  ShareAcknowledge response and verify commit_sync result carries
 *  that err on every partition.
 *
 *  This exercises the broker-thread helper
 *  (rd_kafka_share_fetch_op_reply_with_err) + the main reply handler
 *  defensive _IN_PROGRESS sentinel together: regardless of which
 *  layer writes batch->rktpar->err, the commit_sync caller must see
 *  the top-level err for every partition that was sent to the broker.
 * =================================================================== */
static void
do_test_commit_sync_top_level_err(const char *test_name,
                                  rd_kafka_resp_err_t injected_err) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        char topic[64];
        char group[64];
        const int msgcnt = 10;
        int acked;
        int i;

        SUB_TEST_QUICK("%s -> %s", test_name, rd_kafka_err2name(injected_err));

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-%s", test_name);
        rd_snprintf(group, sizeof(group), "sg-0182-%s", test_name);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit");
        subscribe_one(rkshare, topic);

        acked = consume_and_ack_all(rkshare, msgcnt);
        TEST_ASSERT(acked == msgcnt, "expected %d acked, got %d", msgcnt,
                    acked);

        /* Inject the top-level err on the next ShareAcknowledge
         * response from broker 1 (only broker in cluster). */
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err, 0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);

        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "expected non-NULL partition results "
                    "(top-level err must surface per-partition, "
                    "not as _IN_PROGRESS leak)");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == injected_err, "expected %s, got %s",
                            rd_kafka_err2name(injected_err),
                            rd_kafka_err2name(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* Top-level session error: SHARE_SESSION_NOT_FOUND on ShareAcknowledge
 * is propagated to every partition in commit_sync results (no
 * _IN_PROGRESS leak). */
static void test_commit_sync_share_session_not_found(void) {
        do_test_commit_sync_top_level_err(
            "session-not-found", RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND);
}

/* Same path with a different session error code — confirms
 * propagation isn't tied to a specific err. */
static void test_commit_sync_invalid_share_session_epoch(void) {
        do_test_commit_sync_top_level_err(
            "invalid-session-epoch",
            RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH);
}

/* SHARE_SESSION_LIMIT_REACHED on ShareAcknowledge falls to the
 * default branch (no session reset, no special handling). The
 * top-level err is just propagated to commit_sync results. Matches
 * Java which only checks SHARE_SESSION_LIMIT_REACHED on ShareFetch. */
static void test_commit_sync_share_session_limit_reached(void) {
        do_test_commit_sync_top_level_err(
            "session-limit-reached",
            RD_KAFKA_RESP_ERR_SHARE_SESSION_LIMIT_REACHED);
}

/* GROUP_AUTHORIZATION_FAILED on ShareAcknowledge propagates through
 * the default-case path (previously hit `default: break` with no ack
 * propagation). */
static void test_commit_sync_group_authorization_failed(void) {
        do_test_commit_sync_top_level_err(
            "group-auth-failed", RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
}

/* Same default-case path as group-auth-failed with a different err. */
static void test_commit_sync_topic_authorization_failed(void) {
        do_test_commit_sync_top_level_err(
            "topic-auth-failed", RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
}

/* Generic protocol error to confirm unknown / fatal codes also
 * propagate through the default-case path. */
static void test_commit_sync_invalid_request(void) {
        do_test_commit_sync_top_level_err("invalid-request",
                                          RD_KAFKA_RESP_ERR_INVALID_REQUEST);
}

/* ===================================================================
 *  Test — commit_sync at session epoch 0 returns
 *         INVALID_SHARE_SESSION_EPOCH without sending the
 *         ShareAcknowledge request.
 *
 *  Rationale: when the broker session epoch is 0 (new consumer or
 *  post-reset) the broker has no session state to acknowledge
 *  against. The client must fail acks locally — matches Java's
 *  ShareConsumeRequestManager which raises
 *  InvalidShareSessionEpochException for the same condition.
 *
 *  Two-phase test:
 *    Phase 1: Trigger session reset by injecting
 *             SHARE_SESSION_NOT_FOUND on the first ShareAcknowledge.
 *             commit_sync surfaces SHARE_SESSION_NOT_FOUND for the
 *             partition; broker thread resets epoch to 0.
 *    Phase 2: Acknowledge remaining records and call commit_sync
 *             again. With epoch 0 the client fails acks locally: no
 *             ShareAcknowledge is sent, commit_sync returns
 *             INVALID_SHARE_SESSION_EPOCH for the partition.
 * =================================================================== */
static rd_bool_t is_share_ack_request(rd_kafka_mock_request_t *request,
                                      void *opaque) {
        return rd_kafka_mock_request_api_key(request) ==
               RD_KAFKAP_ShareAcknowledge;
}

static void
test_commit_sync_at_epoch_zero_returns_invalid_session_epoch_error(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic = "0182-epoch-zero-ack";
        const char *group = "sg-0182-epoch-zero-ack";
        const int msgcnt  = 10;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int total_consumed = 0;
        int attempts       = 0;
        size_t share_ack_cnt;
        int i;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit");
        subscribe_one(rkshare, topic);

        /* Phase 0: consume all 10 records. Hold message handles for
         * acknowledge in phase 1 and phase 2. */
        while (total_consumed < msgcnt && attempts++ < 30) {
                size_t rcvd = 0;
                error       = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + total_consumed, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                total_consumed += (int)rcvd;
        }
        TEST_ASSERT(total_consumed == msgcnt,
                    "Phase 0: expected %d records, got %d", msgcnt,
                    total_consumed);

        /* Phase 1: ACCEPT first 5 records, inject SHARE_SESSION_NOT_FOUND
         * on next ShareAcknowledge, call commit_sync and verify the err
         * propagates. The buf reply handler resets the broker session
         * epoch to 0 on this error. */
        for (i = 0; i < 5; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                        0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push SHARE_SESSION_NOT_FOUND");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 1000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *p = &partitions->elems[i];
                TEST_SAY("Phase 1 %s [%" PRId32 "]: %s\n", p->topic,
                         p->partition, rd_kafka_err2name(p->err));
                TEST_ASSERT(p->err == RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                            "Phase 1: expected SHARE_SESSION_NOT_FOUND, "
                            "got %s",
                            rd_kafka_err2name(p->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);
        TEST_ASSERT(share_ack_cnt == 1,
                    "Phase 1: expected 1 ShareAck request, got %" PRIusz,
                    share_ack_cnt);

        /* Phase 2: ACCEPT remaining 5 records and call commit_sync
         * again. Broker epoch is 0 (session reset by phase 1). B4a
         * must fire: no ShareAck request sent, commit_sync returns
         * INVALID_SHARE_SESSION_EPOCH per partition. */
        rd_kafka_mock_clear_requests(ctx.mcluster);

        for (i = 5; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 1000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 2: expected non-NULL partition results");
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *p = &partitions->elems[i];
                TEST_SAY("Phase 2 %s [%" PRId32 "]: %s\n", p->topic,
                         p->partition, rd_kafka_err2name(p->err));
                TEST_ASSERT(p->err ==
                                RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                            "Phase 2: expected INVALID_SHARE_SESSION_EPOCH, "
                            "got %s",
                            rd_kafka_err2name(p->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);
        TEST_ASSERT(share_ack_cnt == 0,
                    "Phase 2: expected 0 ShareAck requests "
                    "(local epoch-0 fail should have prevented send), "
                    "got %" PRIusz,
                    share_ack_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        for (i = 0; i < msgcnt; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Test — consume_batch with session epoch 0 strips piggybacked
 *         acks from the ShareFetch wire request.
 *
 *  Same shape as test_commit_sync_at_epoch_zero_..._error but Phase
 *  2 calls consume_batch (which triggers a FANOUT -> ShareFetch
 *  with should_fetch=true) instead of commit_sync. With session
 *  epoch == 0 and any cached piggyback acks attached, the strip
 *  path in broker_share_rpc fires: acks are pre-set with
 *  INVALID_SHARE_SESSION_EPOCH and detached from rko before
 *  ShareFetch is built, so the wire request goes out with no ack
 *  data section.
 *
 *  To verify the strip path manually, run with:
 *    TEST_DEBUG=fetch,broker,cgrp TESTS=0182 \
 *        SUBTESTS=test_consume_batch_at_epoch_zero make
 *  and look for the "Stripping N piggybacked ack batches" SHAREFETCH
 *  log line in stderr.
 *
 *  TODO: When share_acknowledgement_commit_cb is wired, add an
 *  assertion that the callback is invoked with
 *  INVALID_SHARE_SESSION_EPOCH for each stripped batch. Today the
 *  per-partition err set on the batch by the strip path lives on
 *  ack_details but has no app-facing surface for piggybacked acks.
 * =================================================================== */
static rd_bool_t is_share_fetch_request(rd_kafka_mock_request_t *request,
                                        void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_ShareFetch;
}

static void test_consume_batch_at_epoch_zero_strips_piggyback_acks(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic = "0182-epoch-zero-piggyback";
        const char *group = "sg-0182-epoch-zero-piggyback";
        const int msgcnt  = 10;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_message_t *phase2_msgs[CONSUME_ARRAY];
        int total_consumed = 0;
        int attempts       = 0;
        size_t share_ack_cnt;
        size_t share_fetch_cnt;
        int i;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit");
        subscribe_one(rkshare, topic);

        /* Phase 0: consume all 10 records. */
        while (total_consumed < msgcnt && attempts++ < 30) {
                size_t rcvd = 0;
                error       = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + total_consumed, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                total_consumed += (int)rcvd;
        }
        TEST_ASSERT(total_consumed == msgcnt,
                    "Phase 0: expected %d records, got %d", msgcnt,
                    total_consumed);

        /* Phase 1: ACCEPT first 5 records, inject SHARE_SESSION_NOT_FOUND
         * on next ShareAcknowledge, call commit_sync to trigger session
         * reset on the broker (epoch -> 0). */
        for (i = 0; i < 5; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                        0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push SHARE_SESSION_NOT_FOUND");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 1000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *p = &partitions->elems[i];
                TEST_SAY("Phase 1 %s [%" PRId32 "]: %s\n", p->topic,
                         p->partition, rd_kafka_err2name(p->err));
                TEST_ASSERT(p->err == RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                            "Phase 1: expected SHARE_SESSION_NOT_FOUND, "
                            "got %s",
                            rd_kafka_err2name(p->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);
        TEST_ASSERT(share_ack_cnt == 1,
                    "Phase 1: expected 1 ShareAck request, got %" PRIusz,
                    share_ack_cnt);

        /* Phase 2: ACCEPT remaining 5 records and call consume_batch
         * (NOT commit_sync). This triggers a FANOUT -> ShareFetch with
         * should_fetch=true. Broker epoch is 0 (reset in phase 1). If
         * any piggyback acks are attached when broker_share_rpc runs,
         * the strip path fires and pre-sets each batch's err to
         * INVALID_SHARE_SESSION_EPOCH. ShareFetch goes out with no ack
         * data section.
         *
         * Wire-level assertions:
         *   - At least 1 new ShareFetch request was sent (session
         *     re-establishment).
         *   - 0 new ShareAck requests (strip prevented send if any
         *     piggyback acks were present; ack-only path also doesn't
         *     fire because we did not call commit_sync). */
        rd_kafka_mock_clear_requests(ctx.mcluster);

        for (i = 5; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        attempts = 0;
        while (attempts++ < 5) {
                size_t rcvd = 0;
                error = rd_kafka_share_consume_batch(rkshare, 1000, phase2_msgs,
                                                     &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
                /* phase2_msgs may receive nothing or duplicates after
                 * lock expiry — we don't assert on contents here, only
                 * on wire-level requests counted via the mock. */
                {
                        size_t j;
                        for (j = 0; j < rcvd; j++)
                                rd_kafka_message_destroy(phase2_msgs[j]);
                }
        }

        share_fetch_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_fetch_request, NULL);
        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);

        TEST_SAY("Phase 2 wire counts: ShareFetch=%" PRIusz
                 ", ShareAck=%" PRIusz "\n",
                 share_fetch_cnt, share_ack_cnt);

        TEST_ASSERT(share_fetch_cnt >= 1,
                    "Phase 2: expected >= 1 ShareFetch (session "
                    "re-establish), got %" PRIusz,
                    share_fetch_cnt);
        TEST_ASSERT(share_ack_cnt == 0,
                    "Phase 2: expected 0 ShareAck "
                    "(strip should have prevented any piggyback ack "
                    "send and no ack-only path fired), got %" PRIusz,
                    share_ack_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        for (i = 0; i < msgcnt; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

int main_0182_share_consumer_error_handling_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        test_commit_sync_share_session_not_found();
        test_commit_sync_invalid_share_session_epoch();
        test_commit_sync_share_session_limit_reached();
        test_commit_sync_group_authorization_failed();
        test_commit_sync_topic_authorization_failed();
        test_commit_sync_invalid_request();
        test_commit_sync_at_epoch_zero_returns_invalid_session_epoch_error();
        test_consume_batch_at_epoch_zero_strips_piggyback_acks();

        return 0;
}