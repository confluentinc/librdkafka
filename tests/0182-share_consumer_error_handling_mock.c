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

static test_ctx_t test_ctx_new_n(int nbrok) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        char errstr[512];

        memset(&ctx, 0, sizeof(ctx));

        ctx.mcluster = test_mock_cluster_new(nbrok, &ctx.bootstraps);

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

static test_ctx_t test_ctx_new(void) {
        return test_ctx_new_n(1);
}

static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}

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

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", ack_mode);

        if (cb && cb_state) {
                rd_kafka_conf_set_share_acknowledgement_commit_cb(conf, cb);
                rd_kafka_conf_set_opaque(conf, cb_state);
        }

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

static void mock_produce_partition(rd_kafka_t *producer,
                                   const char *topic,
                                   int32_t partition,
                                   int msgcnt) {
        int i;
        for (i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "%s-p%d-%d", topic,
                         (int)partition, i);
                TEST_ASSERT(rd_kafka_producev(
                                producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_PARTITION(partition),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce to partition %d failed", (int)partition);
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
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK("%s -> %s", test_name, rd_kafka_err2name(injected_err));

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-%s", test_name);
        rd_snprintf(group, sizeof(group), "sg-0182-%s", test_name);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
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

        /* Verify callback was invoked with the error */
        TEST_ASSERT(cb_state.callback_cnt == 1, "expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == msgcnt,
                    "expected callback total_offsets %d, got %zu", msgcnt,
                    cb_state.total_offsets);

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
 *  Test — top-level error is propagated to all partitions in
 *         multi-partition acknowledgement.
 *
 *  Creates a topic with multiple partitions, consumes and acknowledges
 *  records from all partitions, injects a top-level error on
 *  ShareAcknowledge, and verifies that commit_sync returns the error
 *  for EVERY partition that was part of the acknowledgement request.
 *
 *  This ensures the error propagation logic correctly applies the
 *  top-level error to all partitions in the ack_details list, not
 *  just the first one.
 * =================================================================== */
static void test_commit_sync_multi_partition_top_level_error(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic            = "0182-multi-partition-error";
        const char *group            = "sg-0182-multi-partition-error";
        const int partition_cnt      = 3;
        const int msgs_per_partition = 5;
        const int total_msgs         = partition_cnt * msgs_per_partition;
        rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int total_consumed = 0;
        int acked          = 0;
        int attempts       = 0;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        /* Create topic with multiple partitions */
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic with %d partitions", partition_cnt);

        /* Produce messages to all partitions explicitly */
        for (i = 0; i < partition_cnt; i++)
                mock_produce_partition(ctx.producer, topic, i,
                                       msgs_per_partition);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        subscribe_one(rkshare, topic);

        /* Consume messages from all partitions */
        while (total_consumed < total_msgs && attempts++ < 50) {
                size_t rcvd = 0;
                error       = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + total_consumed, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                total_consumed += (int)rcvd;
        }

        TEST_ASSERT(total_consumed == total_msgs,
                    "expected to consume %d messages from %d partitions, "
                    "got %d",
                    total_msgs, partition_cnt, total_consumed);

        /* Acknowledge all messages */
        for (i = 0; i < total_consumed; i++) {
                if (!rkmessages[i]->err) {
                        rd_kafka_share_acknowledge(rkshare, rkmessages[i]);
                        acked++;
                }
        }

        TEST_ASSERT(acked == total_msgs, "expected to ack %d messages, got %d",
                    total_msgs, acked);

        /* Inject top-level error on next ShareAcknowledge */
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err, 0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);

        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL, "expected non-NULL partition results");

        /* Verify ALL partitions have the injected error */
        TEST_ASSERT(partitions->cnt == partition_cnt,
                    "expected results for %d partitions, got %d", partition_cnt,
                    partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == injected_err,
                            "partition [%" PRId32 "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(injected_err),
                            rd_kafka_err2name(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        /* Verify callback was invoked with the error.
         * Callback is invoked once per partition, so we expect
         * partition_cnt callbacks. */
        TEST_ASSERT(cb_state.callback_cnt == partition_cnt,
                    "expected %d callbacks (one per partition), got %d",
                    partition_cnt, cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == total_msgs,
                    "expected callback total_offsets %d, got %zu", total_msgs,
                    cb_state.total_offsets);

        for (i = 0; i < total_consumed; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Test — top-level error on ShareFetch with piggybacked acks is
 *         propagated to callback for all partitions.
 *
 *  Uses implicit mode where acks are piggybacked on ShareFetch.
 *  Creates a topic with multiple partitions, consumes messages in
 *  implicit mode (auto-acknowledge), then injects a top-level error
 *  on the next ShareFetch request (which carries piggybacked acks).
 *
 *  Verifies that the acknowledgement callback is invoked with the
 *  error for all partitions that had piggybacked acks.
 * =================================================================== */
static void test_consume_batch_multi_partition_top_level_error(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic            = "0182-consume-multi-partition-error";
        const char *group            = "sg-0182-consume-multi-partition-error";
        const int partition_cnt      = 3;
        const int msgs_per_partition = 5;
        const int total_msgs         = partition_cnt * msgs_per_partition;
        rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int total_consumed = 0;
        int attempts       = 0;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        /* Create topic with multiple partitions */
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic with %d partitions", partition_cnt);

        /* Produce messages to all partitions explicitly */
        for (i = 0; i < partition_cnt; i++)
                mock_produce_partition(ctx.producer, topic, i,
                                       msgs_per_partition);

        /* Use implicit mode - acks are piggybacked on ShareFetch */
        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "implicit",
                                             &cb_state, test_share_ack_cb);
        subscribe_one(rkshare, topic);

        /* First consume batch - establishes session, consumes messages */
        while (total_consumed < total_msgs && attempts++ < 50) {
                size_t rcvd = 0;
                error       = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + total_consumed, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                total_consumed += (int)rcvd;
        }

        TEST_ASSERT(total_consumed == total_msgs,
                    "expected to consume %d messages from %d partitions, "
                    "got %d",
                    total_msgs, partition_cnt, total_consumed);

        /* Destroy messages - in implicit mode they're auto-acknowledged */
        for (i = 0; i < total_consumed; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        /* Produce more messages to trigger another ShareFetch with
         * piggybacked acks from the previous consume */
        for (i = 0; i < partition_cnt; i++)
                mock_produce_partition(ctx.producer, topic, i,
                                       msgs_per_partition);

        /* Inject top-level error on next ShareFetch (which will carry
         * piggybacked acks from the previous consume_batch) */
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareFetch, 1, injected_err,
                        0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push error on ShareFetch");

        /* Next consume_batch triggers ShareFetch with piggybacked acks.
         * The error should trigger the callback for the piggybacked acks. */
        total_consumed = 0;
        attempts       = 0;
        while (total_consumed < total_msgs && attempts++ < 50) {
                size_t rcvd = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        /* May get errors due to injected error */
                }
                for (i = 0; i < (int)rcvd; i++) {
                        if (!rkmessages[i]->err)
                                total_consumed++;
                        rd_kafka_message_destroy(rkmessages[i]);
                }
                /* Break after we've given the callback a chance to fire */
                if (cb_state.callback_cnt > 0)
                        break;
        }

        /* Verify callback was invoked with the error for piggybacked acks.
         * The callback should have been invoked for the acks that were
         * piggybacked on the ShareFetch that got the error. */
        TEST_ASSERT(cb_state.callback_cnt >= 1,
                    "expected at least 1 callback for piggybacked acks, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(cb_state.last_err));
        /* In implicit mode, we expect the callback to be invoked for the
         * first batch of messages that were piggybacked */
        TEST_ASSERT(cb_state.total_offsets > 0,
                    "expected callback total_offsets > 0, got %zu",
                    cb_state.total_offsets);

        TEST_SAY(
            "Callback invoked %d times with %zu total offsets, last_err=%s\n",
            cb_state.callback_cnt, cb_state.total_offsets,
            rd_kafka_err2name(cb_state.last_err));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
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
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
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

        /* Verify Phase 1 callback was invoked with SHARE_SESSION_NOT_FOUND */
        TEST_ASSERT(cb_state.callback_cnt == 1,
                    "Phase 1: expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(
            cb_state.last_err == RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
            "Phase 1: expected callback err SHARE_SESSION_NOT_FOUND, got %s",
            rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 1: expected 5 offsets in callback, got %zu",
                    cb_state.total_offsets);

        /* Reset callback state for Phase 2 */
        cb_state.callback_cnt  = 0;
        cb_state.total_offsets = 0;
        cb_state.last_err      = RD_KAFKA_RESP_ERR_NO_ERROR;

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

        /* Verify Phase 2 callback was invoked with INVALID_SHARE_SESSION_EPOCH
         * (local fail because session epoch is 0) */
        TEST_ASSERT(cb_state.callback_cnt == 1,
                    "Phase 2: expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH, got %s",
                    rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 offsets in callback, got %zu",
                    cb_state.total_offsets);

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
 *  Asserts (per-partition):
 *    - Wire-level: ShareFetch is sent (>= 1), no extra ShareAck
 *      requests fire.
 *    - Acknowledgement callback fires with
 *      INVALID_SHARE_SESSION_EPOCH for each stripped batch (the
 *      session-establish ShareFetch's response would normally cause
 *      the parser to write the broker's per-partition
 *      AcknowledgementErrorCode, but the parser conditional preserves
 *      our pre-set).
 *
 *  To verify the strip path manually, run with:
 *    TEST_DEBUG=fetch,broker,cgrp TESTS=0182 \
 *        SUBTESTS=test_consume_batch_at_epoch_zero make
 *  and look for the "Stripping N piggybacked ack batches" SHAREFETCH
 *  log line in stderr.
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
        int attempts = 0;
        size_t rcvd  = 0;
        size_t share_ack_cnt;
        size_t share_fetch_cnt;
        size_t j;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        subscribe_one(rkshare, topic);

        /* Phase 0: consume all msgcnt records in a single consume_batch
         * call (with retry-on-empty for transient cases). Explicit-mode
         * contract requires every record from a previous consume_batch
         * to be acknowledged before the next call, so we must not loop
         * and accumulate without acking each batch first. msgcnt is far
         * below max.poll.records (default 500), so a non-empty call
         * returns the full set. */
        while (rcvd == 0 && attempts++ < 30) {
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
        }
        TEST_ASSERT(rcvd == (size_t)msgcnt,
                    "Phase 0: expected %d records in single batch, "
                    "got %" PRIusz,
                    msgcnt, rcvd);

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

        /* Verify Phase 1 callback was invoked with SHARE_SESSION_NOT_FOUND */
        TEST_ASSERT(cb_state.callback_cnt == 1,
                    "Phase 1: expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(
            cb_state.last_err == RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
            "Phase 1: expected callback err SHARE_SESSION_NOT_FOUND, got %s",
            rd_kafka_err2name(cb_state.last_err));

        /* Reset callback state for Phase 2 */
        cb_state.callback_cnt  = 0;
        cb_state.total_offsets = 0;
        cb_state.last_err      = RD_KAFKA_RESP_ERR_NO_ERROR;

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

        /* Single consume_batch triggers the strip-mode FANOUT.
         * Records here may be re-deliveries after lock expiry — we
         * don't assert on contents, only on wire-level counts.
         * test_wait_for_cb_with_poll below tolerates the explicit-mode
         * __STATE the next consume_batch may return for these
         * un-acknowledged records (rcvd stays 0). */
        error = rd_kafka_share_consume_batch(rkshare, 1000, phase2_msgs, &rcvd);
        if (error)
                rd_kafka_error_destroy(error);
        for (j = 0; j < rcvd; j++)
                rd_kafka_message_destroy(phase2_msgs[j]);

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

        /* Wait for the per-partition acknowledgement callback to fire
         * (dispatched on rk_rep when the SHARE_FETCH op reply reaches
         * the main thread). */
        TEST_ASSERT(test_wait_for_cb_with_poll(&cb_state, rkshare, 1, 5000),
                    "Phase 2: timed out waiting for ack callback");

        /* The strip path pre-set INVALID_SHARE_SESSION_EPOCH on each
         * batch in ack_details. The session-establish ShareFetch
         * succeeds and the broker echoes the added partition in its
         * response, so the parser would normally write the broker's
         * AcknowledgementErrorCode (NO_ERROR) onto the batch — the
         * parser conditional (override only _IN_PROGRESS) preserves
         * the pre-set instead. The callback fires with the pre-set
         * err. */
        TEST_ASSERT(cb_state.callback_cnt == 1,
                    "Phase 2: expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH, got %s",
                    rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 acked offsets in callback, "
                    "got %" PRIusz,
                    cb_state.total_offsets);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        for (i = 0; i < msgcnt; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Test — strip + ShareFetch response top-level err: callback err
 *         remains INVALID_SHARE_SESSION_EPOCH (not the response err).
 *
 *  Same flow as test_consume_batch_at_epoch_zero_strips_piggyback_acks
 *  but in Phase 2 we ALSO inject a top-level err on the
 *  session-establish ShareFetch response. The buf-callback's helper
 *  (rd_kafka_share_fetch_op_reply_with_err) will be called with that
 *  err — its conditional override (only _IN_PROGRESS) must NOT
 *  clobber the pre-set INVALID_SHARE_SESSION_EPOCH on the stripped
 *  batches.
 *
 *  Asserts (per-partition via callback):
 *    - Wire-level: ShareFetch is sent, no ShareAck.
 *    - Acknowledgement callback fires with
 *      INVALID_SHARE_SESSION_EPOCH (NOT the injected response err)
 *      for each stripped batch.
 * =================================================================== */
static void test_strip_pre_set_survives_sharefetch_err(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic = "0182-epoch-zero-piggyback-fetch-err";
        const char *group = "sg-0182-epoch-zero-piggyback-fetch-err";
        const int msgcnt  = 10;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_message_t *phase2_msgs[CONSUME_ARRAY];
        int attempts = 0;
        size_t rcvd  = 0;
        size_t share_ack_cnt;
        size_t share_fetch_cnt;
        size_t j;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        subscribe_one(rkshare, topic);

        /* Phase 0: consume all msgcnt records in a single consume_batch
         * call (with retry-on-empty for transient cases). Explicit-mode
         * contract requires every record from a previous consume_batch
         * to be acknowledged before the next call, so we must not loop
         * and accumulate without acking each batch first. msgcnt is far
         * below max.poll.records (default 500), so a non-empty call
         * returns the full set. */
        while (rcvd == 0 && attempts++ < 30) {
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);
        }
        TEST_ASSERT(rcvd == (size_t)msgcnt,
                    "Phase 0: expected %d records in single batch, "
                    "got %" PRIusz,
                    msgcnt, rcvd);

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
                TEST_ASSERT(p->err == RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
                            "Phase 1: expected SHARE_SESSION_NOT_FOUND, "
                            "got %s",
                            rd_kafka_err2name(p->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Reset callback state for Phase 2 */
        cb_state.callback_cnt  = 0;
        cb_state.total_offsets = 0;
        cb_state.last_err      = RD_KAFKA_RESP_ERR_NO_ERROR;

        /* Phase 2: ACCEPT remaining 5 records. Inject
         * SHARE_SESSION_LIMIT_REACHED on the next ShareFetch — this is
         * the strip-mode ShareFetch (epoch 0, with stripped piggyback
         * acks) that goes out from consume_batch. The buf-callback's
         * helper is then called with SHARE_SESSION_LIMIT_REACHED on a
         * batch already pre-set to INVALID_SHARE_SESSION_EPOCH; the
         * helper conditional (only override _IN_PROGRESS) must
         * preserve the pre-set. */
        rd_kafka_mock_clear_requests(ctx.mcluster);

        for (i = 5; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        ctx.mcluster, 1, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_SHARE_SESSION_LIMIT_REACHED,
                        0) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push SHARE_SESSION_LIMIT_REACHED on next ShareFetch");

        /* Single consume_batch triggers the strip-mode FANOUT.
         * Records here may be re-deliveries after lock expiry — we
         * don't assert on contents, only on wire-level counts.
         * test_wait_for_cb_with_poll below tolerates the explicit-mode
         * __STATE the next consume_batch may return for these
         * un-acknowledged records (rcvd stays 0). */
        error = rd_kafka_share_consume_batch(rkshare, 1000, phase2_msgs, &rcvd);
        if (error)
                rd_kafka_error_destroy(error);
        for (j = 0; j < rcvd; j++)
                rd_kafka_message_destroy(phase2_msgs[j]);

        share_fetch_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_fetch_request, NULL);
        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);

        TEST_SAY("Phase 2 wire counts: ShareFetch=%" PRIusz
                 ", ShareAck=%" PRIusz "\n",
                 share_fetch_cnt, share_ack_cnt);

        TEST_ASSERT(share_fetch_cnt >= 1,
                    "Phase 2: expected >= 1 ShareFetch, got %" PRIusz,
                    share_fetch_cnt);
        TEST_ASSERT(share_ack_cnt == 0,
                    "Phase 2: expected 0 ShareAck (strip), got %" PRIusz,
                    share_ack_cnt);

        /* Wait for the per-partition acknowledgement callback. */
        TEST_ASSERT(test_wait_for_cb_with_poll(&cb_state, rkshare, 1, 5000),
                    "Phase 2: timed out waiting for ack callback");

        /* The injected ShareFetch top-level err
         * (SHARE_SESSION_LIMIT_REACHED) reached the
         * rd_kafka_share_fetch_op_reply_with_err helper. The helper's
         * conditional override (only _IN_PROGRESS) must NOT clobber
         * the pre-set INVALID_SHARE_SESSION_EPOCH. */
        TEST_ASSERT(cb_state.callback_cnt == 1,
                    "Phase 2: expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(cb_state.last_err ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH (pre-set preserved), "
                    "got %s",
                    rd_kafka_err2name(cb_state.last_err));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 acked offsets in callback, "
                    "got %" PRIusz,
                    cb_state.total_offsets);

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

        test_timeout_set(120);

        test_commit_sync_share_session_not_found();
        test_commit_sync_invalid_share_session_epoch();
        test_commit_sync_share_session_limit_reached();
        test_commit_sync_group_authorization_failed();
        test_commit_sync_topic_authorization_failed();
        test_commit_sync_invalid_request();
        test_commit_sync_multi_partition_top_level_error();
        test_consume_batch_multi_partition_top_level_error();
        test_commit_sync_at_epoch_zero_returns_invalid_session_epoch_error();
        test_consume_batch_at_epoch_zero_strips_piggyback_acks();
        test_strip_pre_set_survives_sharefetch_err();

        return 0;
}