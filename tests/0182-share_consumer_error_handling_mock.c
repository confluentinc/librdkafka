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

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        /* Register acknowledgement callback at runtime */
        if (cb && cb_state) {
                rd_kafka_error_t *error =
                    rd_kafka_share_set_acknowledgement_commit_cb(rkshare, cb,
                                                                 cb_state);
                TEST_ASSERT(error == NULL,
                            "Failed to set acknowledgement commit callback: "
                            "%s",
                            rd_kafka_error_string(error));
        }
        return rkshare;
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
        rd_kafka_messages_t *batch = NULL;
        int acked                  = 0;
        int attempts               = 0;

        while (acked < msgcnt && attempts++ < 30) {
                size_t rcvd = 0;
                size_t j;
                rd_kafka_error_t *error =
                    rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err) {
                                rd_kafka_share_acknowledge(rkshare, rkm);
                                acked++;
                        }
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == msgcnt,
                    "expected callback total_offsets %d, got %zu", msgcnt,
                    cb_state.total_offsets);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
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

/* commit_sync surfaces a top-level SHARE_SESSION_LIMIT_REACHED from
 * ShareAcknowledge without any client-side special handling. */
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
        rd_kafka_messages_t *batches[CONSUME_ARRAY] = {0};
        int batch_cnt                               = 0;
        int total_consumed                          = 0;
        int acked                                   = 0;
        int attempts                                = 0;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Consume messages from all partitions */
        while (total_consumed < total_msgs && attempts++ < 50) {
                rd_kafka_messages_t *batch = NULL;
                size_t rcvd                = 0;
                error = rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                if (rcvd == 0) {
                        rd_kafka_messages_destroy(batch);
                        continue;
                }
                TEST_ASSERT(batch_cnt < CONSUME_ARRAY, "batch buffer overflow");
                batches[batch_cnt++] = batch;
                total_consumed += (int)rcvd;
        }

        TEST_ASSERT(total_consumed == total_msgs,
                    "expected to consume %d messages from %d partitions, "
                    "got %d",
                    total_msgs, partition_cnt, total_consumed);

        /* Acknowledge all messages */
        for (i = 0; i < batch_cnt; i++) {
                size_t j;
                size_t rcvd = rd_kafka_messages_count(batches[i]);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batches[i], j);
                        if (!rkm->err) {
                                rd_kafka_share_acknowledge(rkshare, rkm);
                                acked++;
                        }
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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == total_msgs,
                    "expected callback total_offsets %d, got %zu", total_msgs,
                    cb_state.total_offsets);

        for (i = 0; i < batch_cnt; i++)
                rd_kafka_messages_destroy(batches[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
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
        rd_kafka_messages_t *batch = NULL;
        int total_consumed         = 0;
        int attempts               = 0;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* First consume batch - establishes session, consumes messages */
        while (total_consumed < total_msgs && attempts++ < 50) {
                size_t rcvd = 0;
                error       = rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                total_consumed += (int)rcvd;
                /* Destroy messages - in implicit mode they're
                 * auto-acknowledged */
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(total_consumed == total_msgs,
                    "expected to consume %d messages from %d partitions, "
                    "got %d",
                    total_msgs, partition_cnt, total_consumed);

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
                size_t j;
                error = rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        /* May get errors due to injected error */
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err)
                                total_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) == injected_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        /* In implicit mode, we expect the callback to be invoked for the
         * first batch of messages that were piggybacked */
        TEST_ASSERT(cb_state.total_offsets > 0,
                    "expected callback total_offsets > 0, got %zu",
                    cb_state.total_offsets);

        TEST_SAY(
            "Callback invoked %d times with %zu total offsets, first_err=%s\n",
            cb_state.callback_cnt, cb_state.total_offsets,
            rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Test — commit_sync at session epoch 0 returns
 *         INVALID_SHARE_SESSION_EPOCH without sending a
 *         ShareAcknowledge request.
 *
 *  When the broker session epoch is 0 (new consumer or post-reset)
 *  there is no session state to acknowledge against, so the client
 *  must fail acks locally.
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
        rd_kafka_messages_t *batches[CONSUME_ARRAY] = {0};
        int batch_cnt                               = 0;
        int total_consumed                          = 0;
        int attempts                                = 0;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 0: consume all 10 records. Hold message handles for
         * acknowledge in phase 1 and phase 2. */
        while (total_consumed < msgcnt && attempts++ < 30) {
                rd_kafka_messages_t *batch = NULL;
                size_t rcvd                = 0;
                size_t j;
                error = rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                if (rcvd == 0) {
                        rd_kafka_messages_destroy(batch);
                        continue;
                }
                /* Flatten message handles into rkmessages for the
                 * phase-1/phase-2 partial-ack indexing pattern. */
                for (j = 0; j < rcvd && total_consumed < CONSUME_ARRAY; j++)
                        rkmessages[total_consumed++] =
                            rd_kafka_messages_get(batch, j);
                TEST_ASSERT(batch_cnt < CONSUME_ARRAY, "batch buffer overflow");
                batches[batch_cnt++] = batch;
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
            test_ack_cb_state_first_err(&cb_state) ==
                RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
            "Phase 1: expected callback err SHARE_SESSION_NOT_FOUND, got %s",
            rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 1: expected 5 offsets in callback, got %zu",
                    cb_state.total_offsets);

        /* Reset callback state for Phase 2 */
        test_ack_cb_state_destroy(&cb_state);

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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH, got %s",
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 offsets in callback, got %zu",
                    cb_state.total_offsets);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        for (i = 0; i < batch_cnt; i++)
                rd_kafka_messages_destroy(batches[i]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
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
        rd_kafka_messages_t *phase0_batch = NULL;
        rd_kafka_messages_t *phase2_batch = NULL;
        int attempts                      = 0;
        size_t rcvd                       = 0;
        size_t share_ack_cnt;
        size_t share_fetch_cnt;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 0: consume all msgcnt records in a single consume_batch
         * call (with retry-on-empty for transient cases). Explicit-mode
         * contract requires every record from a previous consume_batch
         * to be acknowledged before the next call, so we must not loop
         * and accumulate without acking each batch first. msgcnt is far
         * below max.poll.records (default 500), so a non-empty call
         * returns the full set. */
        while (rcvd == 0 && attempts++ < 30) {
                rd_kafka_messages_destroy(phase0_batch);
                phase0_batch = NULL;
                error = rd_kafka_share_poll(rkshare, 3000, &phase0_batch);
                if (error)
                        rd_kafka_error_destroy(error);
                rcvd = rd_kafka_messages_count(phase0_batch);
        }
        TEST_ASSERT(rcvd == (size_t)msgcnt,
                    "Phase 0: expected %d records in single batch, "
                    "got %" PRIusz,
                    msgcnt, rcvd);
        for (i = 0; i < msgcnt; i++)
                rkmessages[i] = rd_kafka_messages_get(phase0_batch, i);

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
            test_ack_cb_state_first_err(&cb_state) ==
                RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND,
            "Phase 1: expected callback err SHARE_SESSION_NOT_FOUND, got %s",
            rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));

        /* Reset callback state for Phase 2 */
        test_ack_cb_state_destroy(&cb_state);

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
        error = rd_kafka_share_poll(rkshare, 1000, &phase2_batch);
        if (error)
                rd_kafka_error_destroy(error);
        rd_kafka_messages_destroy(phase2_batch);
        phase2_batch = NULL;

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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH, got %s",
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 acked offsets in callback, "
                    "got %" PRIusz,
                    cb_state.total_offsets);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        rd_kafka_messages_destroy(phase0_batch);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
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
        rd_kafka_messages_t *phase0_batch = NULL;
        rd_kafka_messages_t *phase2_batch = NULL;
        int attempts                      = 0;
        size_t rcvd                       = 0;
        size_t share_ack_cnt;
        size_t share_fetch_cnt;
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
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 0: consume all msgcnt records in a single consume_batch
         * call (with retry-on-empty for transient cases). Explicit-mode
         * contract requires every record from a previous consume_batch
         * to be acknowledged before the next call, so we must not loop
         * and accumulate without acking each batch first. msgcnt is far
         * below max.poll.records (default 500), so a non-empty call
         * returns the full set. */
        while (rcvd == 0 && attempts++ < 30) {
                rd_kafka_messages_destroy(phase0_batch);
                phase0_batch = NULL;
                error = rd_kafka_share_poll(rkshare, 3000, &phase0_batch);
                if (error)
                        rd_kafka_error_destroy(error);
                rcvd = rd_kafka_messages_count(phase0_batch);
        }
        TEST_ASSERT(rcvd == (size_t)msgcnt,
                    "Phase 0: expected %d records in single batch, "
                    "got %" PRIusz,
                    msgcnt, rcvd);
        for (i = 0; i < msgcnt; i++)
                rkmessages[i] = rd_kafka_messages_get(phase0_batch, i);

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
        test_ack_cb_state_destroy(&cb_state);

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
        error = rd_kafka_share_poll(rkshare, 1000, &phase2_batch);
        if (error)
                rd_kafka_error_destroy(error);
        rd_kafka_messages_destroy(phase2_batch);
        phase2_batch = NULL;

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
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) ==
                        RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH,
                    "Phase 2: expected callback err "
                    "INVALID_SHARE_SESSION_EPOCH (pre-set preserved), "
                    "got %s",
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));
        TEST_ASSERT(cb_state.total_offsets == 5,
                    "Phase 2: expected 5 acked offsets in callback, "
                    "got %" PRIusz,
                    cb_state.total_offsets);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        rd_kafka_messages_destroy(phase0_batch);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Wire-level socket timeout matrix.
 *
 *  Exercises commit_sync under a socket-timeout-induced connection
 *  teardown. The two timer layers in the share-acknowledge path are
 *  intentionally decoupled:
 *
 *    - The app-visible commit_sync(timeout_ms) deadline drives the
 *      timeout cb that stamps REQUEST_TIMED_OUT on the per-partition
 *      results.
 *    - The wire RPC carries the broker connection's socket.timeout.ms
 *      and tears the connection down (default socket.max.fails = 1)
 *      when it fires.
 *
 *  Three (api_timeout_ms, socket_timeout_ms, rtt_ms) triples cover
 *  the cases where api > socket, api == socket, and api < socket. In
 *  all three, rtt_ms > socket_timeout_ms so the wire eventually times
 *  out and the connection is torn down (broker drops the session as
 *  a result of the TCP close).
 *
 *  Two flow variants build on the same setup:
 *
 *    full_ack_then_more   — Phase 1 commits all consumed records.
 *      Phase 2 produces and consumes new records on a fresh session
 *      and expects NO_ERROR on the second commit_sync.
 *
 *    partial_ack_then_remaining — Phase 1 commits only half. The
 *      records left in the local inflight map were ACQUIRED on the
 *      now-dropped session, so Phase 2's commit_sync surfaces an
 *      INVALID_RECORD_STATE / SHARE_SESSION_NOT_FOUND /
 *      INVALID_SHARE_SESSION_EPOCH-class error (when session-err
 *      translation lands at the app-facing boundary this narrows
 *      to INVALID_RECORD_STATE).
 *
 *  TODO KIP-932: add multi-broker variants once the single-broker
 *  matrix is stable. Multi-broker exercises the case where only one
 *  broker is slow — partitions on other brokers should commit
 *  immediately without waiting for the slow broker's socket timer.
 * =================================================================== */

#define SOCKET_TIMEOUT_MATRIX_RECORD_LOCK_MS     600000
#define SOCKET_TIMEOUT_MATRIX_PARTITIONS         3
#define SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION 10

/**
 * @brief Create a share consumer with a custom socket.timeout.ms.
 */
static rd_kafka_share_t *create_share_consumer_socket_timeout(
    const char *bootstraps,
    const char *group_id,
    const char *ack_mode,
    int socket_timeout_ms,
    test_ack_cb_state_t *cb_state,
    void (*cb)(rd_kafka_share_t *,
               rd_kafka_share_partition_offsets_list_t *,
               rd_kafka_resp_err_t,
               void *)) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char buf[32];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", ack_mode);

        rd_snprintf(buf, sizeof(buf), "%d", socket_timeout_ms);
        test_conf_set(conf, "socket.timeout.ms", buf);

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        if (cb && cb_state) {
                rd_kafka_error_t *error =
                    rd_kafka_share_set_acknowledgement_commit_cb(rkshare, cb,
                                                                 cb_state);
                TEST_ASSERT(error == NULL,
                            "Failed to set acknowledgement commit callback: "
                            "%s",
                            rd_kafka_error_string(error));
        }
        return rkshare;
}

/**
 * @brief Consume up to \p expected records (across one or more
 *        share_poll calls), acknowledging the first \p ack_first.
 *
 * Caller owns the returned batch via \p *out_batch and must
 * rd_kafka_messages_destroy() it. The flattened \p rkmessages array
 * is populated with pointers borrowed from the batch (lifetime tied
 * to the batch). Asserts that exactly \p expected records were
 * received within the polling budget. Used both for the pre-stage
 * Phase 1 consume (broker has all records ready, typically arrives
 * in a single batch) and for the post-teardown Phase 2 consume
 * (records may trickle in across multiple batches as the new
 * session warms up).
 */
static void consume_first_batch(rd_kafka_share_t *rkshare,
                                rd_kafka_message_t **rkmessages,
                                int expected,
                                rd_kafka_messages_t **out_batch) {
        size_t rcvd  = 0;
        int attempts = 0;
        rd_kafka_error_t *error;
        rd_kafka_messages_t *batch = NULL;
        int j;

        /* Spin until the first non-empty batch arrives. The test
         * pre-stages the broker with all \p expected records so the
         * batch we receive will contain exactly that many. */
        while (rcvd == 0 && attempts++ < 30) {
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                error = rd_kafka_share_poll(rkshare, 3000, &batch);
                if (error) {
                        TEST_SAY("consume_first_batch: err=%s\n",
                                 rd_kafka_err2name(rd_kafka_error_code(error)));
                        rd_kafka_error_destroy(error);
                }
                rcvd = rd_kafka_messages_count(batch);
        }

        TEST_ASSERT((int)rcvd == expected,
                    "Expected %d records in first batch, got %zu", expected,
                    rcvd);

        for (j = 0; j < (int)rcvd; j++) {
                rkmessages[j] = rd_kafka_messages_get(batch, j);
                TEST_ASSERT(!rkmessages[j]->err,
                            "Unexpected per-record err: %s",
                            rd_kafka_err2str(rkmessages[j]->err));
        }

        *out_batch = batch;
}

/**
 * @brief Drain pending ack callbacks via commit_async until at least
 *        \p min_callbacks have been observed, or \p timeout_ms elapses.
 *
 * commit_async's first step drains rk_rep for callbacks. With no
 * pending acks to commit, the remainder is a no-op.
 *
 * @returns rd_true if min_callbacks observed within timeout.
 */
static rd_bool_t drain_callbacks_via_commit_async(rd_kafka_share_t *rkshare,
                                                  test_ack_cb_state_t *cb_state,
                                                  int min_callbacks,
                                                  int timeout_ms) {
        int elapsed_ms = 0;

        while (elapsed_ms < timeout_ms) {
                if (cb_state->callback_cnt >= min_callbacks)
                        return rd_true;
                rd_kafka_error_t *err = rd_kafka_share_commit_async(rkshare);
                if (err)
                        rd_kafka_error_destroy(err);
                rd_usleep(50 * 1000, NULL);
                elapsed_ms += 50;
        }
        return cb_state->callback_cnt >= min_callbacks;
}

/**
 * @brief Test commit_sync under a wire-level socket timeout, then
 *        consume + commit a fresh batch.
 *
 *  The three timer layers (api timeout A, RTT R, socket.timeout.ms S)
 *  determine which event wins:
 *    - A smallest: commit_sync timeout cb stamps REQUEST_TIMED_OUT
 *      on results and returns at t=A. Wire request continues in
 *      flight until either R completes or S fires; late callback
 *      reflects the actual wire outcome (NO_ERROR if R < S,
 *      __TIMED_OUT if S < R).
 *    - S smallest: wire torn down at t=S. Reply handler stamps
 *      __TIMED_OUT on batches and results; commit_sync returns at
 *      ~t=S with __TIMED_OUT and callbacks fire with __TIMED_OUT.
 *    - R smallest: broker reply arrives normally; commit_sync
 *      returns NO_ERROR. Not exercised by this matrix.
 *
 *  Phase 1 measures the wait between commit_sync return and all ack
 *  callbacks landing. That wait equals max(0, min(R, S) -
 *  min(A, R, S)) and is asserted with tolerance.
 *
 *  Phase 2 produces N new records, consumes + acks + commits on the
 *  (possibly reconnected) consumer. Expects NO_ERROR for every
 *  partition and a fresh set of NO_ERROR callbacks.
 *
 *  TODO KIP-932: add multi-broker variant once single-broker matrix
 *  is stable. Multi-broker exercises the case where only one broker
 *  is slow.
 *
 *  TODO KIP-932: add partial-ack variant — Phase 1 acks only half
 *  the records, Phase 2 acks the remaining. With session drop on
 *  connection tear-down, the remaining records reference a dropped
 *  session and Phase 2's commit_sync surfaces INVALID_RECORD_STATE.
 */
static void do_test_socket_timeout_full_ack_then_more(int api_timeout_ms,
                                                      int socket_timeout_ms,
                                                      int rtt_ms) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        char topic[128];
        char group[128];
        const int partitions_total = SOCKET_TIMEOUT_MATRIX_PARTITIONS;
        const int msgcnt =
            partitions_total * SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION;
        rd_kafka_message_t **rkmessages;
        rd_kafka_messages_t *phase1_batch = NULL;
        rd_kafka_messages_t *phase2_batch = NULL;
        test_ack_cb_state_t cb_state      = {0};
        rd_ts_t t_p1_end_us, t_callbacks_done_us;
        int actual_wait_ms, expected_wait_ms;
        int min_between_rtt_ms_socket_timeout_ms;
        int min_between_api_timeout_ms_rtt_ms_socket_timeout_ms;
        rd_kafka_resp_err_t expected_phase1_commit_err;
        rd_kafka_resp_err_t expected_phase1_callback_err;
        int prev_callback_cnt;
        int i;
        /* Valgrind serializes threads and slows wall-clock-bound paths;
         * widen the tolerance to avoid flaky over-budget failures. */
        const int wait_tolerance_ms =
            !strcmp(test_mode, "valgrind") ? 2000 : 500;

        SUB_TEST_QUICK("api_timeout_ms=%d socket_timeout_ms=%d rtt_ms=%d",
                       api_timeout_ms, socket_timeout_ms, rtt_ms);

        /* Derive expected outcomes from the ordering of three timers:
         *
         *   api_timeout_ms    -> commit_sync API's deadline parameter;
         *                        upper bound on how long commit_sync
         *                        can block before returning
         *                        REQUEST_TIMED_OUT.
         *   rtt_ms            -> mock-injected broker round-trip-time;
         *                        time after which the broker's
         *                        ShareAcknowledge response is allowed
         *                        to leave the broker side (records are
         *                        applied broker-side immediately on
         *                        request receive — see
         *                        rdkafka_mock_handlers.c:3811-3840).
         *   socket_timeout_ms -> wire-level socket.timeout.ms; after
         *                        this the connection is torn down and
         *                        any in-flight request fails with
         *                        __TIMED_OUT (default socket.max.fails
         *                        of 1 makes a single failure terminal).
         *
         * The expected wait between commit_sync returning and the last
         * ack callback landing equals
         *   max(0, min_between(rtt_ms, socket_timeout_ms)
         *          - min_between(api_timeout_ms, rtt_ms,
         *                        socket_timeout_ms))
         *
         * The reasoning:
         *
         *   - commit_sync returns at
         *     t_p1_end = min_between(api_timeout_ms, rtt_ms,
         *                            socket_timeout_ms):
         *       api_timeout_ms smallest: api timer cb fires, stamps
         *         REQUEST_TIMED_OUT on the per-partition results;
         *         send_response wakes commit_sync at ~t=api_timeout_ms.
         *       socket_timeout_ms smallest: wire socket.timeout.ms
         *         fires, broker thread reply path runs with
         *         rko_err=__TIMED_OUT, callbacks dispatched, results
         *         stamped via apply_result, commit_sync returns at
         *         ~t=socket_timeout_ms.
         *       rtt_ms smallest: broker reply arrives normally,
         *         callbacks dispatched with broker's per-partition err,
         *         results stamped, commit_sync returns at ~t=rtt_ms
         *         (NO_ERROR for this happy path — not in our matrix).
         *
         *   - All Phase 1 callbacks have landed by
         *     t_callbacks_done = min_between(rtt_ms, socket_timeout_ms):
         *       When the wire request resolves the broker-thread reply
         *       handler dispatches the per-partition callbacks. That
         *       happens when broker actually replies (t=rtt_ms) OR
         *       when socket.timeout.ms fires (t=socket_timeout_ms),
         *       whichever comes first. For
         *       api_timeout_ms < (rtt_ms, socket_timeout_ms) the
         *       callbacks fire AFTER commit_sync returned; for
         *       (rtt_ms or socket_timeout_ms) < api_timeout_ms the
         *       callbacks already fired BEFORE commit_sync returned
         *       and the wait is 0.
         *
         *   - Wait = t_callbacks_done - t_p1_end
         *          = min_between(rtt_ms, socket_timeout_ms)
         *            - min_between(api_timeout_ms, rtt_ms,
         *                          socket_timeout_ms).
         *
         * The expected callback err depends on which layer resolved
         * the wire:
         *   - rtt_ms < socket_timeout_ms: broker actually replied —
         *     callback gets broker's per-partition result (NO_ERROR,
         *     the records were applied).
         *   - socket_timeout_ms < rtt_ms: wire torn down before broker
         *     could reply — helper stamps __TIMED_OUT on batches; the
         *     app-facing funnel translates it to REQUEST_TIMED_OUT
         *     before the callback fires.
         *
         * The expected commit_sync result err comes from which layer
         * stamped results first:
         *   - api_timeout_ms < min_between(rtt_ms, socket_timeout_ms):
         *     api timer cb wrote REQUEST_TIMED_OUT into results.
         *   - socket_timeout_ms <= api_timeout_ms: broker-thread reply
         *     path wrote __TIMED_OUT into results via apply_result;
         *     the funnel translates it to REQUEST_TIMED_OUT. */
        min_between_rtt_ms_socket_timeout_ms =
            socket_timeout_ms < rtt_ms ? socket_timeout_ms : rtt_ms;
        min_between_api_timeout_ms_rtt_ms_socket_timeout_ms = api_timeout_ms;
        if (rtt_ms < min_between_api_timeout_ms_rtt_ms_socket_timeout_ms)
                min_between_api_timeout_ms_rtt_ms_socket_timeout_ms = rtt_ms;
        if (socket_timeout_ms <
            min_between_api_timeout_ms_rtt_ms_socket_timeout_ms)
                min_between_api_timeout_ms_rtt_ms_socket_timeout_ms =
                    socket_timeout_ms;
        expected_wait_ms = min_between_rtt_ms_socket_timeout_ms -
                           min_between_api_timeout_ms_rtt_ms_socket_timeout_ms;

        if (rtt_ms < api_timeout_ms && rtt_ms < socket_timeout_ms) {
                /* rtt_ms smaller than both other timers: broker reply
                 * lands before either timer fires. commit_sync sees
                 * the broker's per-partition success and callbacks
                 * fire with NO_ERROR. Happy path — included for
                 * matrix completeness, not exercising any timeout
                 * layer. */
                expected_phase1_commit_err   = RD_KAFKA_RESP_ERR_NO_ERROR;
                expected_phase1_callback_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        } else if (socket_timeout_ms < api_timeout_ms) {
                /* socket_timeout_ms < api_timeout_ms (and rtt_ms is
                 * NOT strictly smallest by the first branch): socket
                 * fires before api can. (At api == socket the race
                 * outcome is non-deterministic across runs; that
                 * boundary case is not in the matrix.)
                 *
                 * __TIMED_OUT from the broker-thread socket timer is
                 * translated to REQUEST_TIMED_OUT at the app-facing
                 * funnel. */
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
        } else if (rtt_ms < socket_timeout_ms) {
                /* api wins (api <= socket AND rtt < socket; api
                 * fires no later than broker reply at rtt). Late
                 * broker reply at rtt brings actual per-partition
                 * outcome (NO_ERROR — broker did apply the ack) to
                 * the callback. */
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        } else {
                /* api wins (api <= socket AND rtt >= socket). Socket
                 * fires before broker can reply, wire torn down; the
                 * late wire-failure err is __TIMED_OUT, translated to
                 * REQUEST_TIMED_OUT at the app-facing funnel. */
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
        }

        ctx = test_ctx_new();
        rd_kafka_mock_sharegroup_set_record_lock_duration(
            ctx.mcluster, SOCKET_TIMEOUT_MATRIX_RECORD_LOCK_MS);

        rd_snprintf(topic, sizeof(topic), "0182-fullack-a%d-s%d",
                    api_timeout_ms, socket_timeout_ms);
        rd_snprintf(group, sizeof(group), "sg-0182-fullack-a%d-s%d",
                    api_timeout_ms, socket_timeout_ms);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partitions_total,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        for (i = 0; i < partitions_total; i++)
                mock_produce_partition(
                    ctx.producer, topic, i,
                    SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION);

        rkshare = create_share_consumer_socket_timeout(
            ctx.bootstraps, group, "explicit", socket_timeout_ms, &cb_state,
            test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        rkmessages = rd_calloc(msgcnt, sizeof(*rkmessages));

        /* Phase 1: consume the first batch (all msgcnt records
         * given the small partition count and broker readiness),
         * acknowledge everyone, then inject RTT and commit_sync. */
        consume_first_batch(rkshare, rkmessages, msgcnt, &phase1_batch);
        for (i = 0; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, rtt_ms);

        partitions = NULL;
        error =
            rd_kafka_share_commit_sync(rkshare, api_timeout_ms, &partitions);
        t_p1_end_us = test_clock();

        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == partitions_total,
                    "Phase 1: expected %d partition results, got %d",
                    partitions_total, partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("Phase 1 commit_sync: %s [%" PRId32 "]: %s\n",
                         rktpar->topic, rktpar->partition,
                         rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == expected_phase1_commit_err,
                            "Phase 1: expected %s, got %s",
                            rd_kafka_err2name(expected_phase1_commit_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Drain ack callbacks via commit_async until we observe one
         * per partition. For socket_timeout_ms-smallest orderings the
         * callbacks fired before commit_sync returned so this returns
         * immediately. For api_timeout_ms-smallest orderings the
         * callbacks fire when the wire resolves (broker reply at
         * t=rtt_ms OR socket timer at t=socket_timeout_ms). */
        TEST_ASSERT(drain_callbacks_via_commit_async(rkshare, &cb_state,
                                                     partitions_total, 30000),
                    "Phase 1: expected %d ack callbacks within 30s, got %d",
                    partitions_total, cb_state.callback_cnt);
        t_callbacks_done_us = test_clock();

        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) ==
                        expected_phase1_callback_err,
                    "Phase 1: expected callback err %s, got %s",
                    rd_kafka_err2name(expected_phase1_callback_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));

        actual_wait_ms = (int)((t_callbacks_done_us - t_p1_end_us) / 1000);
        TEST_SAY(
            "Phase 1 wait t_callbacks_done - t_p1_end = %dms "
            "(expected ~%dms, tolerance %dms)\n",
            actual_wait_ms, expected_wait_ms, wait_tolerance_ms);
        TEST_ASSERT(actual_wait_ms >= expected_wait_ms - wait_tolerance_ms &&
                        actual_wait_ms <= expected_wait_ms + wait_tolerance_ms,
                    "Phase 1 wait %dms outside expected %dms +/- %dms",
                    actual_wait_ms, expected_wait_ms, wait_tolerance_ms);

        prev_callback_cnt = cb_state.callback_cnt;

        /* Phase 2: clear RTT (so Phase 2's broker response isn't
         * delayed), produce more, consume + ack + commit. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);

        for (i = 0; i < partitions_total; i++)
                mock_produce_partition(
                    ctx.producer, topic, i,
                    SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION);

        /* Free phase-1 messages. */
        rd_kafka_messages_destroy(phase1_batch);
        phase1_batch = NULL;
        memset(rkmessages, 0, msgcnt * sizeof(*rkmessages));

        consume_first_batch(rkshare, rkmessages, msgcnt, &phase2_batch);
        for (i = 0; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 2: expected non-NULL partition results");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("Phase 2: %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Phase 2: expected NO_ERROR on a fresh "
                            "session, got %s",
                            rd_kafka_err2name(rktpar->err));
        }
        TEST_ASSERT(partitions->cnt == partitions_total,
                    "Phase 2: expected %d partition results, got %d",
                    partitions_total, partitions->cnt);
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(cb_state.callback_cnt > prev_callback_cnt,
                    "Phase 2: expected new ack callbacks; before=%d "
                    "after=%d",
                    prev_callback_cnt, cb_state.callback_cnt);

        rd_kafka_messages_destroy(phase2_batch);
        rd_free(rkmessages);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);
        test_ack_cb_state_destroy(&cb_state);

        SUB_TEST_PASS();
}

/**
 * @brief Same Phase 1 as do_test_socket_timeout_full_ack_then_more, but
 *        Phase 1 acks only half the consumed records. Phase 2 then
 *        acks the remaining half (still in the client's local inflight
 *        map) and runs commit_sync on those.
 *
 *  After the Phase 1 drain-callbacks step we clear RTT, so Phase 2's
 *  commit_sync gets a prompt broker response (no second wire-level
 *  timer to race).
 *
 *  Phase 2 outcome depends on whether the connection was torn down
 *  during Phase 1, which is equivalent to socket_timeout_ms < rtt_ms:
 *
 *    Connection alive (rtt_ms < socket_timeout_ms): broker session
 *      preserved; the remaining 15 records are still ACQUIRED for
 *      this member on the same session. Phase 2's ShareAck advances
 *      the session epoch normally; broker returns NO_ERROR per
 *      partition.
 *
 *    Connection killed (socket_timeout_ms < rtt_ms): broker dropped
 *      the session on TCP close. The remaining 15 records' ACQUIRED
 *      state was released to AVAILABLE by `release_member_locks`.
 *      Phase 2's broker-thread reconnects and sends ShareAck with
 *      session epoch reset to 0 client-side. Broker rejects with
 *      INVALID_SHARE_SESSION_EPOCH at top-level (no active session
 *      for this member at epoch 0 outside of a ShareFetch). The
 *      top-level err propagates to all partitions via the existing
 *      `rd_kafka_share_fetch_op_reply_and_update_ack_details_with_err`
 *      helper.
 *
 *  TODO KIP-932: when SHARE_SESSION_NOT_FOUND /
 *  INVALID_SHARE_SESSION_EPOCH are translated to
 *  INVALID_RECORD_STATE at the app-facing boundary,
 *  expected_phase2_commit_err for the killed branch becomes
 *  INVALID_RECORD_STATE.
 */
static void
do_test_socket_timeout_partial_ack_then_remaining(int api_timeout_ms,
                                                  int socket_timeout_ms,
                                                  int rtt_ms) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        char topic[128];
        char group[128];
        const int partitions_total = SOCKET_TIMEOUT_MATRIX_PARTITIONS;
        const int msgcnt =
            partitions_total * SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION;
        rd_kafka_message_t **rkmessages;
        rd_kafka_messages_t *phase1_batch = NULL;
        test_ack_cb_state_t cb_state      = {0};
        rd_ts_t t_p1_end_us, t_callbacks_done_us;
        int actual_wait_ms, expected_wait_ms;
        int min_between_rtt_ms_socket_timeout_ms;
        int min_between_api_timeout_ms_rtt_ms_socket_timeout_ms;
        rd_kafka_resp_err_t expected_phase1_commit_err;
        rd_kafka_resp_err_t expected_phase1_callback_err;
        rd_kafka_resp_err_t expected_phase2_commit_err;
        rd_bool_t connection_killed;
        int prev_callback_cnt;
        int phase1_partition_cnt;
        int i;
        /* See do_test_socket_timeout_full_ack_then_more for the Valgrind
         * tolerance rationale. */
        const int wait_tolerance_ms =
            !strcmp(test_mode, "valgrind") ? 2000 : 500;

        SUB_TEST_QUICK("api_timeout_ms=%d socket_timeout_ms=%d rtt_ms=%d",
                       api_timeout_ms, socket_timeout_ms, rtt_ms);

        /* Phase 1 expected outcomes — same derivation as
         * do_test_socket_timeout_full_ack_then_more. See the comment
         * there for the full reasoning. */
        min_between_rtt_ms_socket_timeout_ms =
            socket_timeout_ms < rtt_ms ? socket_timeout_ms : rtt_ms;
        min_between_api_timeout_ms_rtt_ms_socket_timeout_ms = api_timeout_ms;
        if (rtt_ms < min_between_api_timeout_ms_rtt_ms_socket_timeout_ms)
                min_between_api_timeout_ms_rtt_ms_socket_timeout_ms = rtt_ms;
        if (socket_timeout_ms <
            min_between_api_timeout_ms_rtt_ms_socket_timeout_ms)
                min_between_api_timeout_ms_rtt_ms_socket_timeout_ms =
                    socket_timeout_ms;
        expected_wait_ms = min_between_rtt_ms_socket_timeout_ms -
                           min_between_api_timeout_ms_rtt_ms_socket_timeout_ms;

        if (rtt_ms < api_timeout_ms && rtt_ms < socket_timeout_ms) {
                expected_phase1_commit_err   = RD_KAFKA_RESP_ERR_NO_ERROR;
                expected_phase1_callback_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        } else if (socket_timeout_ms < api_timeout_ms) {
                /* __TIMED_OUT from the broker-thread socket timer is
                 * translated to REQUEST_TIMED_OUT at the app-facing
                 * funnel. */
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
        } else if (rtt_ms < socket_timeout_ms) {
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        } else {
                /* Late wire-failure callback err __TIMED_OUT is
                 * translated to REQUEST_TIMED_OUT at the app-facing
                 * funnel. */
                expected_phase1_commit_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
                expected_phase1_callback_err =
                    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT;
        }

        /* Connection torn down iff socket fires before broker reply.
         *
         * TODO KIP-932: when SHARE_SESSION_NOT_FOUND /
         * INVALID_SHARE_SESSION_EPOCH are translated to
         * INVALID_RECORD_STATE at the app-facing boundary, the
         * killed-branch expected err becomes INVALID_RECORD_STATE. */
        connection_killed = socket_timeout_ms < rtt_ms;
        expected_phase2_commit_err =
            connection_killed ? RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH
                              : RD_KAFKA_RESP_ERR_NO_ERROR;

        ctx = test_ctx_new();
        rd_kafka_mock_sharegroup_set_record_lock_duration(
            ctx.mcluster, SOCKET_TIMEOUT_MATRIX_RECORD_LOCK_MS);

        rd_snprintf(topic, sizeof(topic), "0182-partial-a%d-s%d-r%d",
                    api_timeout_ms, socket_timeout_ms, rtt_ms);
        rd_snprintf(group, sizeof(group), "sg-0182-partial-a%d-s%d-r%d",
                    api_timeout_ms, socket_timeout_ms, rtt_ms);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partitions_total,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        for (i = 0; i < partitions_total; i++)
                mock_produce_partition(
                    ctx.producer, topic, i,
                    SOCKET_TIMEOUT_MATRIX_MSGS_PER_PARTITION);

        rkshare = create_share_consumer_socket_timeout(
            ctx.bootstraps, group, "explicit", socket_timeout_ms, &cb_state,
            test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        rkmessages = rd_calloc(msgcnt, sizeof(*rkmessages));

        /* Phase 1: consume the first batch (all msgcnt records),
         * then ack the first half of the batch in receive order.
         * The exact set of partitions covered depends on broker-side
         * record ordering; the assertions below adapt to whatever
         * partitions appear in commit_sync's results. */
        consume_first_batch(rkshare, rkmessages, msgcnt, &phase1_batch);
        for (i = 0; i < msgcnt / 2; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, rtt_ms);

        partitions = NULL;
        error =
            rd_kafka_share_commit_sync(rkshare, api_timeout_ms, &partitions);
        t_p1_end_us = test_clock();

        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");

        /* The number of partitions reflects whichever ones had records
         * in the first half of the batch — depends on broker-side
         * record ordering. We assert each partition's err matches the
         * expected code, and drain that many callbacks. */
        phase1_partition_cnt = partitions->cnt;
        TEST_ASSERT(phase1_partition_cnt > 0,
                    "Phase 1: expected at least one partition result");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("Phase 1 commit_sync: %s [%" PRId32 "]: %s\n",
                         rktpar->topic, rktpar->partition,
                         rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == expected_phase1_commit_err,
                            "Phase 1: expected %s, got %s",
                            rd_kafka_err2name(expected_phase1_commit_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(drain_callbacks_via_commit_async(
                        rkshare, &cb_state, phase1_partition_cnt, 30000),
                    "Phase 1: expected %d ack callbacks within 30s, got %d",
                    phase1_partition_cnt, cb_state.callback_cnt);
        t_callbacks_done_us = test_clock();

        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) ==
                        expected_phase1_callback_err,
                    "Phase 1: expected callback err %s, got %s",
                    rd_kafka_err2name(expected_phase1_callback_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));

        actual_wait_ms = (int)((t_callbacks_done_us - t_p1_end_us) / 1000);
        TEST_SAY(
            "Phase 1 wait t_callbacks_done - t_p1_end = %dms "
            "(expected ~%dms, tolerance %dms)\n",
            actual_wait_ms, expected_wait_ms, wait_tolerance_ms);
        TEST_ASSERT(actual_wait_ms >= expected_wait_ms - wait_tolerance_ms &&
                        actual_wait_ms <= expected_wait_ms + wait_tolerance_ms,
                    "Phase 1 wait %dms outside expected %dms +/- %dms",
                    actual_wait_ms, expected_wait_ms, wait_tolerance_ms);

        prev_callback_cnt = cb_state.callback_cnt;

        /* Phase 2: clear RTT and ack the remaining records (still in
         * the local inflight map from Phase 1's consume — the second
         * half per partition). No new consume_batch in this variant.
         *
         * Small settle delay so the consumer's broker thread can
         * finish post-teardown bookkeeping (reconnect, session
         * reset, broker decommission cleanup) before Phase 2's
         * commit_sync probes the state. Without this perm 5/6/boundary
         * cases racing the post-teardown handling can return
         * __STATE from the FANOUT op instead of the expected
         * INVALID_SHARE_SESSION_EPOCH from the local epoch-0 check. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);
        rd_sleep(1);

        /* Ack the remaining half of the batch in receive order. */
        for (i = msgcnt / 2; i < msgcnt; i++)
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 2: expected non-NULL partition results");

        /* As in Phase 1, the partition count reflects whichever
         * partitions had records in the second half. Some partitions
         * may appear in both phases (e.g., partition that had its
         * first 5 records acked in Phase 1 and last 5 in Phase 2). */
        TEST_ASSERT(partitions->cnt > 0,
                    "Phase 2: expected at least one partition result");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("Phase 2 commit_sync: %s [%" PRId32 "]: %s\n",
                         rktpar->topic, rktpar->partition,
                         rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == expected_phase2_commit_err,
                            "Phase 2: expected %s, got %s",
                            rd_kafka_err2name(expected_phase2_commit_err),
                            rd_kafka_err2name(rktpar->err));
        }
        TEST_ASSERT(
            drain_callbacks_via_commit_async(
                rkshare, &cb_state, prev_callback_cnt + partitions->cnt, 10000),
            "Phase 2: expected %d new ack callbacks; before=%d "
            "after=%d",
            partitions->cnt, prev_callback_cnt, cb_state.callback_cnt);
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(
            cb_state.errs[cb_state.callback_cnt - 1] ==
                expected_phase2_commit_err,
            "Phase 2: expected last callback err %s, got %s",
            rd_kafka_err2name(expected_phase2_commit_err),
            rd_kafka_err2name(cb_state.errs[cb_state.callback_cnt - 1]));

        rd_kafka_messages_destroy(phase1_batch);
        rd_free(rkmessages);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);
        test_ack_cb_state_destroy(&cb_state);

        SUB_TEST_PASS();
}

/* ===================================================================
 *  Topic-level metadata error tests.
 *
 *  Verify that a share consumer surfaces topic-level errors from
 *  metadata responses to the application as rd_kafka_error_t via
 *  consume_batch. Only TOPIC_EXCEPTION and TOPIC_AUTHORIZATION_FAILED
 *  reach the app; transient codes (UNKNOWN_TOPIC, UNKNOWN_TOPIC_OR_PART,
 *  UNKNOWN_TOPIC_ID, UNKNOWN_PARTITION) are not delivered. Repeats of
 *  the same (topic, err) are deduped; recovery, unsubscribe, and
 *  re-subscribe each have well-defined behaviour exercised below.
 *
 *  These scenarios are hard to reproduce on a real broker — the mock
 *  cluster lets us inject the exact per-topic error byte in a metadata
 *  response on demand.
 * =================================================================== */

/* Drive at least one successful consume_batch so the share assignment
 * is fully materialised before the test injects an error. Records are
 * ACKed inline because the consumer is in explicit-ack mode. */
static void share_topic_err_prime_assignment(rd_kafka_share_t *rkshare) {
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        size_t rcvd;
        size_t j;
        int attempts;
        rd_bool_t got_any = rd_false;

        for (attempts = 0; attempts < 20; attempts++) {
                error = rd_kafka_share_poll(rkshare, 1000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err) {
                                rd_kafka_share_acknowledge(rkshare, rkm);
                                got_any = rd_true;
                        }
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                if (got_any)
                        return;
        }
        TEST_FAIL(
            "Pre-condition: expected to consume a batch before "
            "injecting the metadata error");
}

/* Force a metadata refresh on the share consumer's underlying rk so
 * the injected per-topic err is observed. */
static void share_topic_err_force_metadata(rd_kafka_share_t *rkshare) {
        const rd_kafka_metadata_t *md = NULL;
        rd_kafka_t *rk;

        rk = test_share_consumer_get_rk(rkshare);
        (void)rd_kafka_metadata(rk, 1 /*all_topics*/, NULL, &md, 5000);
        if (md)
                rd_kafka_metadata_destroy(md);
}

/* Drain consume_batch until either the expected err code surfaces (then
 * return rd_true) or `max_attempts` calls go by without it (return
 * rd_false). Records that arrive are destroyed. */
static rd_bool_t share_topic_err_wait_for_err(rd_kafka_share_t *rkshare,
                                              rd_kafka_resp_err_t expected,
                                              int max_attempts) {
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        size_t rcvd;
        size_t j;
        int attempts;

        for (attempts = 0; attempts < max_attempts; attempts++) {
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        TEST_SAY("share_poll returned %s: %s\n",
                                 rd_kafka_err2name(code),
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        if (code == expected)
                                return rd_true;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                /* Ack received records so the next share_poll can
                 * proceed past the explicit-mode "previous poll
                 * unacked" gate. */
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err)
                                rd_kafka_share_acknowledge(rkshare, rkm);
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }
        return rd_false;
}

/* Run `n_attempts` consume_batch calls and fail the test if any of them
 * returns an rd_kafka_error_t — used for the negative-assertion tests
 * (transient-code log-only paths must not surface). */
static void share_topic_err_assert_no_err(rd_kafka_share_t *rkshare,
                                          int n_attempts,
                                          const char *context) {
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        size_t rcvd;
        size_t j;
        int attempts;

        for (attempts = 0; attempts < n_attempts; attempts++) {
                error = rd_kafka_share_poll(rkshare, 200, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        TEST_FAIL(
                            "[%s] unexpected error from share_poll: "
                            "%s",
                            context, rd_kafka_err2name(code));
                }
                rcvd = rd_kafka_messages_count(batch);
                /* Ack received records so the next share_poll can
                 * proceed past the explicit-mode "previous poll
                 * unacked" gate. */
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err)
                                rd_kafka_share_acknowledge(rkshare, rkm);
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }
}

/* Run one share-topic-err scenario: assign, inject `inject_err`,
 * verify consume_batch surfaces `expect_err` (or fails). */
static void do_test_share_topic_err_surfaces(const char *topic_suffix,
                                             rd_kafka_resp_err_t inject_err,
                                             rd_kafka_resp_err_t expect_err) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        char topic[64];
        char group[64];

        SUB_TEST_QUICK("inject=%s expect=%s", rd_kafka_err2name(inject_err),
                       rd_kafka_err2name(expect_err));

        ctx = test_ctx_new();
        rd_snprintf(topic, sizeof(topic), "0182-%s", topic_suffix);
        rd_snprintf(group, sizeof(group), "sg-0182-%s", topic_suffix);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        rd_kafka_mock_topic_set_error(ctx.mcluster, topic, inject_err);
        share_topic_err_force_metadata(rkshare);

        TEST_ASSERT(share_topic_err_wait_for_err(rkshare, expect_err, 30),
                    "Expected consume_batch to surface %s within 30 attempts",
                    rd_kafka_err2name(expect_err));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


static void test_share_consumer_surfaces_topic_exception(void) {
        do_test_share_topic_err_surfaces("surfaces-topic-exception",
                                         RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION,
                                         RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION);
}


static void test_share_consumer_surfaces_topic_authorization_failed(void) {
        do_test_share_topic_err_surfaces(
            "surfaces-topic-auth-failed",
            RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
            RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
}


/* A topic with N partitions failing in a single metadata cycle must
 * surface exactly one op for that topic, not one per partition.
 *
 * share_toppar_enq_error keys its accumulator on topic_id and
 * dedups-on-add, so the per-rktp calls from partition_cnt_update and
 * propagate_notexists (2N total) collapse to a single entry in
 * rkcg_errored_topics — and hence a single rd_kafka_consumer_err. */
static void test_share_consumer_multi_partition_single_op_per_cycle(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic          = "0182-multipart-single-op";
        const char *group          = "sg-0182-multipart-single-op";
        const int partition_cnt    = 5;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        size_t rcvd, j;
        int err_count = 0;
        int attempts;

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic with %d partitions", partition_cnt);
        mock_produce(ctx.producer, topic, partition_cnt * 3);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        /* Inject AUTH_FAILED on the topic; partition_cnt_update +
         * propagate_notexists will each fire enq_error per rktp. */
        rd_kafka_mock_topic_set_error(
            ctx.mcluster, topic, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        share_topic_err_force_metadata(rkshare);

        /* Drain a short window immediately after the synchronous
         * force_metadata. By the time it returns, propagate has emitted
         * the (single) op for this cycle. The window is intentionally
         * short to keep it within one heartbeat interval and count just
         * what this cycle produced. */
        for (attempts = 0; attempts < 3; attempts++) {
                error = rd_kafka_share_poll(rkshare, 200, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        const char *errstr       = rd_kafka_error_string(error);
                        if (code ==
                                RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED &&
                            errstr && strstr(errstr, topic))
                                err_count++;
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err)
                                rd_kafka_share_acknowledge(rkshare, rkm);
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(err_count == 1,
                    "expected exactly 1 AUTH_FAILED op for a %d-partition "
                    "topic in a single metadata cycle, got %d",
                    partition_cnt, err_count);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* The same topic failing with a different error code must surface a
 * fresh op for the new code rather than being deduped against the
 * previous one. */
static void test_share_consumer_re_emits_when_err_code_changes(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-err-code-change";
        const char *group = "sg-0182-err-code-change";

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        /* First err: AUTH_FAILED. */
        rd_kafka_mock_topic_set_error(
            ctx.mcluster, topic, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(
            share_topic_err_wait_for_err(
                rkshare, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 30),
            "First AUTH_FAILED must surface");

        /* Second err for the same topic: TOPIC_EXCEPTION (different
         * code) — must surface, dedup must NOT swallow it. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(share_topic_err_wait_for_err(
                        rkshare, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, 30),
                    "TOPIC_EXCEPTION must surface after AUTH_FAILED "
                    "(err code change must bypass dedup)");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* A topic that surfaces AUTH_FAILED, then recovers, then fails again
 * with the same code must surface the error a second time. */
static void test_share_consumer_re_surfaces_after_recovery(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-re-surface-after-recovery";
        const char *group = "sg-0182-re-surface-after-recovery";

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        /* Phase 1: fail — first surface. */
        rd_kafka_mock_topic_set_error(
            ctx.mcluster, topic, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(
            share_topic_err_wait_for_err(
                rkshare, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 30),
            "First AUTH_FAILED must surface");

        /* Phase 2: recover — no error must reach the app. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_NO_ERROR);
        share_topic_err_force_metadata(rkshare);
        share_topic_err_assert_no_err(rkshare, 5,
                                      "no error must surface while recovered");

        /* Phase 3: fail again with the same code — must surface a
         * second time. */
        rd_kafka_mock_topic_set_error(
            ctx.mcluster, topic, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(
            share_topic_err_wait_for_err(
                rkshare, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 30),
            "AUTH_FAILED must surface a second time after recovery");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* A topic that surfaces TOPIC_EXCEPTION, then recovers, then fails
 * again with the same code must surface the error a second time. */
static void
test_share_consumer_re_surfaces_after_recovery_topic_exception(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-re-surface-topic-exception";
        const char *group = "sg-0182-re-surface-topic-exception";

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        /* Phase 1: fail. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(share_topic_err_wait_for_err(
                        rkshare, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, 30),
                    "first TOPIC_EXCEPTION must surface");

        /* Phase 2: recover. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_NO_ERROR);
        share_topic_err_force_metadata(rkshare);
        share_topic_err_assert_no_err(
            rkshare, 5, "no error must surface while topic is recovered");

        /* Phase 3: re-fail with the same code — must surface again. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(share_topic_err_wait_for_err(
                        rkshare, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, 30),
                    "TOPIC_EXCEPTION must surface a second time after "
                    "recovery");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* A topic surfaces TOPIC_EXCEPTION, gets unsubscribed (no surface),
 * and is then re-subscribed while still failing. The error must
 * surface again on re-subscribe — not be permanently suppressed. */
static void test_share_consumer_resubscribe_re_emits_persistent_failure(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-resubscribe-re-emit";
        const char *group = "sg-0182-resubscribe-re-emit";

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        /* Phase 1: subscribe + fail + surface. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION);
        share_topic_err_force_metadata(rkshare);
        TEST_ASSERT(share_topic_err_wait_for_err(
                        rkshare, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, 30),
                    "first TOPIC_EXCEPTION must surface");

        /* Phase 2: unsubscribe. */
        TEST_ASSERT(rd_kafka_share_unsubscribe(rkshare) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "unsubscribe");

        /* Phase 3: re-subscribe to the same still-failing topic; the
         * error must surface again. Re-force metadata across the wait
         * loop so the request happens after the share-assignment
         * heartbeat re-populates the partition list. */
        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        rd_bool_t saw_err = rd_false;
        int outer;
        for (outer = 0; outer < 10 && !saw_err; outer++) {
                share_topic_err_force_metadata(rkshare);
                if (share_topic_err_wait_for_err(
                        rkshare, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, 3))
                        saw_err = rd_true;
        }
        TEST_ASSERT(saw_err,
                    "TOPIC_EXCEPTION must surface again after "
                    "re-subscribing to a still-failing topic");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* UNKNOWN_TOPIC_OR_PART is debug-logged only and must not reach the
 * app via consume_batch. */
static void test_share_consumer_does_not_surface_unknown_topic_or_part(void) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-no-surface-unknown-tp";
        const char *group = "sg-0182-no-surface-unknown-tp";

        SUB_TEST();

        ctx = test_ctx_new();
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        mock_produce(ctx.producer, topic, 5);

        /* Custom consumer with the metadata propagation defer window
         * disabled so the no-surface path is exercised on the first
         * metadata refresh rather than 30 s later. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.propagation.max.ms", "0");
        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        test_share_consumer_subscribe_multi(rkshare, 1, topic);
        share_topic_err_prime_assignment(rkshare);

        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        share_topic_err_force_metadata(rkshare);

        share_topic_err_assert_no_err(
            rkshare, 20,
            "UNKNOWN_TOPIC_OR_PART must be logged only, not surfaced");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Log callback shared by the two select_broker STATE_UP guard tests
 *  below. Counts the broker-thread short-circuit log emitted at
 *  src/rdkafka_broker.c when a SHARE_FETCH op is served against a
 *  broker whose rkb_state is not STATE_UP.
 * =================================================================== */
static void no_bounce_loop_log_cb(const rd_kafka_t *rk,
                                  int level,
                                  const char *fac,
                                  const char *buf) {
        rd_atomic32_t *cnt = rd_kafka_opaque(rk);
        if (cnt && !strcmp(fac, "SHAREFETCH") && strstr(buf, "broker not up"))
                rd_atomic32_add(cnt, 1);
}

/* ===================================================================
 *  do_test_no_bounce_loop_on_down_broker
 *
 *  Steady-state DOWN: the consumer is idle when the broker is taken
 *  down, sleeps long enough for the broker thread to settle rkb_state
 *  to !UP, then drives consume_batch for ~1s. Every FANOUT must skip
 *  the DOWN leader via the select_broker STATE_UP guard, so no
 *  "broker not up" log line should fire.
 *
 *  Wire never sees a ShareFetch on the DOWN broker (broker thread
 *  rejects the internal op before any RPC is built), so we assert on
 *  the count of the broker-thread short-circuit debug log instead of
 *  on mock_get_requests.
 * =================================================================== */
static void do_test_no_bounce_loop_on_down_broker(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        rd_atomic32_t broker_not_up_cnt;
        rd_kafka_error_t *error;
        rd_kafka_messages_t *batch = NULL;
        const char *topic          = "0182-no_bounce_loop";
        const char *group          = "sg-0182-no-bounce-loop";
        const int msgcnt_phase1    = 5;
        const int msgcnt_phase2    = 5;
        int acked, cnt;
        size_t rcvd;
        size_t share_fetch_cnt_before_drain;
        size_t share_fetch_cnt_after_drain;
        rd_ts_t end_ts;

        SUB_TEST_QUICK();

        /* Taking the only broker down legitimately raises
         * __ALL_BROKERS_DOWN and __TRANSPORT on producer + consumer
         * error callbacks. None of these should fail the test. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        ctx = test_ctx_new();
        rd_kafka_mock_start_request_tracking(ctx.mcluster);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce(ctx.producer, topic, msgcnt_phase1);

        rd_atomic32_init(&broker_not_up_cnt, 0);
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "debug", "broker");
        /* Cap reconnect backoff so the consumer recovers quickly after
         * set_up. The default max (10s) extends past the recovery
         * sleep below, causing the post-recovery ShareFetch not to
         * land in time. */
        test_conf_set(conf, "reconnect.backoff.ms", "100");
        test_conf_set(conf, "reconnect.backoff.max.ms", "500");
        rd_kafka_conf_set_log_cb(conf, no_bounce_loop_log_cb);
        rd_kafka_conf_set_opaque(conf, &broker_not_up_cnt);

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        acked = consume_and_ack_all(rkshare, msgcnt_phase1);
        TEST_ASSERT(acked == msgcnt_phase1, "phase1: expected %d acked, got %d",
                    msgcnt_phase1, acked);

        /* Flush any acks still cached in rkb_share_async_ack_details
         * before taking the broker down. Otherwise the next FANOUT
         * after set_down would dispatch an ack-only op to the DOWN
         * broker (the FANOUT iteration must always deliver cached
         * acks so the broker thread can surface the error), and the
         * broker thread would legitimately log "broker not up" once.
         * That log is unrelated to the select_broker guard. */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async error: %s",
                    error ? rd_kafka_error_string(error) : "NULL");

        /* Phase-2 records sit in the partition log; the post-recovery
         * consume below drains them. */
        mock_produce(ctx.producer, topic, msgcnt_phase2);

        TEST_SAY("Taking broker 1 down\n");
        rd_kafka_mock_broker_set_down(ctx.mcluster, 1);

        /* Settle: let the client's broker thread detect TCP close and
         * transition rkb_state to !UP before we start counting. After
         * this point every select_broker reads DOWN — no race window. */
        rd_sleep(1);
        rd_atomic32_set(&broker_not_up_cnt, 0);

        end_ts = test_clock() + 1000 * 1000;
        while (test_clock() < end_ts) {
                error = rd_kafka_share_poll(rkshare, 100, &batch);
                TEST_ASSERT(!error,
                            "unexpected error from share_poll while "
                            "broker is down: %s",
                            error ? rd_kafka_error_string(error) : "NULL");
                rcvd = rd_kafka_messages_count(batch);
                TEST_ASSERT(rcvd == 0,
                            "expected 0 records while broker is down, "
                            "got %zu",
                            rcvd);
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        cnt = rd_atomic32_get(&broker_not_up_cnt);
        TEST_SAY("\"broker not up\" log count: %d (expected 0)\n", cnt);
        TEST_ASSERT(cnt == 0,
                    "select_broker should skip the DOWN leader on every "
                    "FANOUT once rkb_state has settled; got %d "
                    "\"broker not up\" log lines",
                    cnt);

        TEST_SAY("Bringing broker 1 back up\n");
        rd_kafka_mock_broker_set_up(ctx.mcluster, 1);

        /* The main-thread retrigger keeps calling select_broker. As
         * soon as broker 1 reaches STATE_UP the next select_broker
         * returns it and a ShareFetch fires automatically — records
         * land on the consumer queue without any consume_batch call
         * driving the fetch. */
        rd_sleep(3);

        share_fetch_cnt_before_drain = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_fetch_request, NULL);
        TEST_SAY("ShareFetch count after recovery (pre-drain): %" PRIusz "\n",
                 share_fetch_cnt_before_drain);
        TEST_ASSERT(share_fetch_cnt_before_drain >= 1,
                    "expected >= 1 ShareFetch via internal retry after "
                    "set_up, got %" PRIusz,
                    share_fetch_cnt_before_drain);

        /* Drain the pre-fetched records. They're already on the
         * consumer queue, so share_poll returns them directly
         * without enqueueing a FANOUT and no new ShareFetch fires. */
        error                       = rd_kafka_share_poll(rkshare, 100, &batch);
        rcvd                        = rd_kafka_messages_count(batch);
        share_fetch_cnt_after_drain = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_fetch_request, NULL);
        /* Destroy the batch before asserting so a failed assert can't
         * leak it (TEST_FAIL longjmps past any later destroy). */
        rd_kafka_messages_destroy(batch);
        batch = NULL;

        TEST_ASSERT(!error, "post-recovery share_poll error: %s",
                    error ? rd_kafka_error_string(error) : "NULL");
        TEST_ASSERT(rcvd == (size_t)msgcnt_phase2,
                    "expected %d records from queue, got %" PRIusz,
                    msgcnt_phase2, rcvd);
        TEST_ASSERT(share_fetch_cnt_after_drain == share_fetch_cnt_before_drain,
                    "share_poll should drain the queue without firing "
                    "a new ShareFetch; pre=%" PRIusz " post=%" PRIusz,
                    share_fetch_cnt_before_drain, share_fetch_cnt_after_drain);

        rd_kafka_mock_clear_requests(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}

/* ===================================================================
 *  do_test_one_log_on_broker_down_during_active_empty_poll
 *
 *  Race-window companion to do_test_no_bounce_loop_on_down_broker.
 *
 *  With an empty topic the consumer's FANOUT->ShareFetch->Step 6
 *  empty-poll loop fires continuously. We rapidly flip the broker
 *  up/down across N cycles while that loop is hot. Each set_down
 *  has a narrow window between the mock TCP close and the client
 *  broker thread updating rkb_state, in which select_broker can
 *  read stale STATE_UP and enqueue one more op — the broker thread
 *  (rkb_state has caught up by then) logs "broker not up" at most
 *  once and replies ERR__STATE. The next Step 6 re-select reads
 *  DOWN and skips, closing the loop until the next set_down.
 *
 *  Total log count is therefore bounded by N (one slip per
 *  set_down). Without the select_broker guard, each set_down would
 *  produce hundreds of logs per second for the duration of the
 *  down window — unbounded across cycles.
 * =================================================================== */
static void do_test_one_log_on_broker_down_during_active_empty_poll(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        rd_atomic32_t broker_not_up_cnt;
        const char *topic             = "0182-race_bounce";
        const char *group             = "sg-0182-race-bounce";
        const int msgcnt_recovery     = 5;
        const int n_cycles            = 10;
        const int max_allowed_log_cnt = n_cycles;
        rd_kafka_messages_t *batch    = NULL;
        size_t rcvd;
        rd_kafka_error_t *error;
        int i, cnt, acked;

        SUB_TEST_QUICK();

        /* Taking the only broker down legitimately raises
         * __ALL_BROKERS_DOWN and __TRANSPORT on producer + consumer
         * error callbacks. None of these should fail the test. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        rd_atomic32_init(&broker_not_up_cnt, 0);
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "debug", "broker");
        /* Cap reconnect backoff so each set_up reconnects quickly
         * across the chaos cycles. */
        test_conf_set(conf, "reconnect.backoff.ms", "100");
        test_conf_set(conf, "reconnect.backoff.max.ms", "500");
        rd_kafka_conf_set_log_cb(conf, no_bounce_loop_log_cb);
        rd_kafka_conf_set_opaque(conf, &broker_not_up_cnt);

        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Prime with one record so the share group has joined and the
         * partition is assigned by the time we take the broker down.
         * Without this guarantee, select_broker would return NULL
         * (no toppars) and the bounce loop wouldn't fire at all,
         * making the test pass for the wrong reason. */
        mock_produce(ctx.producer, topic, 1);
        acked = consume_and_ack_all(rkshare, 1);
        TEST_ASSERT(acked == 1, "prime: expected 1 acked, got %d", acked);

        /* Flush the prime ack still cached in rkb_share_async_ack_details
         * before taking the broker down. Otherwise the next FANOUT
         * iteration would dispatch an ack-only op to the DOWN broker
         * and produce a "broker not up" log unrelated to the
         * select_broker race we're measuring. */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async error: %s",
                    error ? rd_kafka_error_string(error) : "NULL");

        /* Reset to drop any noise from cgrp/connection bring-up. */
        rd_atomic32_set(&broker_not_up_cnt, 0);

        /* Kickstart the empty-poll loop: one share_poll on the now
         * empty topic starts the FANOUT->Step 6 cycle which keeps
         * firing on the main thread until share_poll returns. */
        error = rd_kafka_share_poll(rkshare, 500, &batch);
        TEST_ASSERT(!error, "kickstart share_poll error: %s",
                    error ? rd_kafka_error_string(error) : "NULL");
        rcvd = rd_kafka_messages_count(batch);
        TEST_ASSERT(rcvd == 0, "expected 0 records, got %zu", rcvd);
        rd_kafka_messages_destroy(batch);
        batch = NULL;

        /* Chaos: rapidly flip the broker up/down across n_cycles
         * while the consumer's empty-poll loop is hot. Each set_down
         * may admit one race-window slip; total log count must stay
         * bounded by n_cycles. */
        for (i = 0; i < n_cycles; i++) {
                TEST_SAY("Cycle %d/%d: taking broker 1 down\n", i + 1,
                         n_cycles);
                rd_kafka_mock_broker_set_down(ctx.mcluster, 1);
                error = rd_kafka_share_poll(rkshare, 200, &batch);
                TEST_ASSERT(!error, "cycle %d down: share_poll error: %s",
                            i + 1,
                            error ? rd_kafka_error_string(error) : "NULL");
                rcvd = rd_kafka_messages_count(batch);
                TEST_ASSERT(rcvd == 0,
                            "cycle %d down: expected 0 records, got %zu", i + 1,
                            rcvd);
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                TEST_SAY("Cycle %d/%d: bringing broker 1 back up\n", i + 1,
                         n_cycles);
                rd_kafka_mock_broker_set_up(ctx.mcluster, 1);
                error = rd_kafka_share_poll(rkshare, 200, &batch);
                TEST_ASSERT(!error, "cycle %d up: share_poll error: %s", i + 1,
                            error ? rd_kafka_error_string(error) : "NULL");
                rcvd = rd_kafka_messages_count(batch);
                TEST_ASSERT(rcvd == 0,
                            "cycle %d up: expected 0 records, got %zu", i + 1,
                            rcvd);
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        cnt = rd_atomic32_get(&broker_not_up_cnt);
        TEST_SAY("\"broker not up\" log count after %d cycles: %d (max %d)\n",
                 n_cycles, cnt, max_allowed_log_cnt);
        TEST_ASSERT(cnt <= max_allowed_log_cnt,
                    "race-window slips must be bounded to %d (one per "
                    "set_down across %d cycles); got %d log lines",
                    max_allowed_log_cnt, n_cycles, cnt);

        /* Recovery: broker is left UP at the end of the chaos loop.
         * Produce records and verify the consumer drains them. */
        mock_produce(ctx.producer, topic, msgcnt_recovery);

        acked = consume_and_ack_all(rkshare, msgcnt_recovery);
        TEST_ASSERT(acked == msgcnt_recovery,
                    "post-recovery: expected %d acked, got %d", msgcnt_recovery,
                    acked);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Partition-level error injection: API basics.
 * =================================================================== */
static void test_partition_error_injection_general(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        char topic[64];
        char group[64];
        const int msgcnt = 3;
        int acked;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-part_err_general");
        rd_snprintf(group, sizeof(group), "sg-0182-part_err_general");

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        /* Unknown topics are auto-created. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, "0182-no-such-topic", 0,
                        RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push to unknown topic");

        /* Out-of-range partition is rejected. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 99, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR) ==
                        RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
                    "push to unknown partition");

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        mock_produce_partition(ctx.producer, topic, 0, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* The injected error is transient: all records are delivered
         * once it has been consumed off the stack. */
        acked = consume_and_ack_all(rkshare, msgcnt);
        TEST_ASSERT(acked == msgcnt, "expected %d acked, got %d", msgcnt,
                    acked);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  ShareFetch partition error injection: errors on one partition
 *  must not affect the other, and the partition recovers once the
 *  error stack drains.
 * =================================================================== */
static void test_partition_error_injection_share_fetch(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        char topic[64];
        char group[64];
        const int msgs_per_part = 5;
        int acked;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-part_err_sharefetch");
        rd_snprintf(group, sizeof(group), "sg-0182-part_err_sharefetch");

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 1, RD_KAFKAP_ShareFetch, 2,
                        RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER,
                        RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition errors");

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Partition 0 is unaffected and partition 1 recovers after
         * the two errored fetches: nothing is lost. */
        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "expected %d acked, got %d",
                    2 * msgs_per_part, acked);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  ShareAcknowledge partition error injection: the injected error
 *  surfaces in commit_sync results for that partition only.
 * =================================================================== */
static void test_partition_error_injection_share_ack(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        char topic[64];
        char group[64];
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR;
        int acked;
        int i;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-part_err_shareack");
        rd_snprintf(group, sizeof(group), "sg-0182-part_err_shareack");

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "expected %d acked, got %d",
                    2 * msgs_per_part, acked);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL, "expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 2,
                    "expected results for 2 partitions, got %d",
                    partitions->cnt);

        /* Partition 0 carries the injected error, partition 1 is
         * unaffected. */
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                rd_kafka_resp_err_t exp_err        = rktpar->partition == 0
                                                         ? injected_err
                                                         : RD_KAFKA_RESP_ERR_NO_ERROR;

                TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == exp_err,
                            "partition [%" PRId32 "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(exp_err),
                            rd_kafka_err2name(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


static int test_ack_cb_state_count_err(const test_ack_cb_state_t *state,
                                       rd_kafka_resp_err_t err) {
        int n = 0;
        int i;
        for (i = 0; i < state->callback_cnt; i++) {
                if (state->errs[i] == err)
                        n++;
        }
        return n;
}

/**
 * @brief Inject a per-partition err on ShareAcknowledge for partition
 *        0 of a 2-partition topic and verify the err propagates to
 *        commit_sync results and the ack callback for the affected
 *        partition only; partition 1 remains NO_ERROR.
 */
static void do_test_partition_error_injection_share_ack_code(
    rd_kafka_resp_err_t injected_err) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        char topic[64];
        char group[64];
        const int msgs_per_part = 5;
        int acked;
        int i;
        int err_cb_cnt;
        int ok_cb_cnt;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK("%s", rd_kafka_err2name(injected_err));

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-part_ack_err_%s",
                    rd_kafka_err2name(injected_err));
        rd_snprintf(group, sizeof(group), "sg-0182-part_ack_err_%s",
                    rd_kafka_err2name(injected_err));

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "expected %d acked, got %d",
                    2 * msgs_per_part, acked);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL, "expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 2,
                    "expected results for 2 partitions, got %d",
                    partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                rd_kafka_resp_err_t exp_err        = rktpar->partition == 0
                                                         ? injected_err
                                                         : RD_KAFKA_RESP_ERR_NO_ERROR;

                TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == exp_err,
                            "partition [%" PRId32 "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(exp_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* One callback per partition; one carries injected_err, one
         * carries NO_ERROR. Callback order is not deterministic so
         * count rather than indexing. */
        TEST_ASSERT(cb_state.callback_cnt == 2,
                    "expected 2 callbacks (one per partition), got %d",
                    cb_state.callback_cnt);
        err_cb_cnt = test_ack_cb_state_count_err(&cb_state, injected_err);
        ok_cb_cnt =
            test_ack_cb_state_count_err(&cb_state, RD_KAFKA_RESP_ERR_NO_ERROR);
        TEST_ASSERT(err_cb_cnt == 1, "expected 1 callback with %s, got %d",
                    rd_kafka_err2name(injected_err), err_cb_cnt);
        TEST_ASSERT(ok_cb_cnt == 1, "expected 1 callback with NO_ERROR, got %d",
                    ok_cb_cnt);
        TEST_ASSERT(cb_state.total_offsets == (size_t)(2 * msgs_per_part),
                    "expected callback total_offsets %d, got %zu",
                    2 * msgs_per_part, cb_state.total_offsets);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

static void test_partition_error_injection_share_ack_matrix(void) {
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH);
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_INVALID_RECORD_STATE);
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR);
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_INVALID_REQUEST);
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        do_test_partition_error_injection_share_ack_code(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID);
}


/**
 * @brief Verify a per-partition ShareFetch err on the surface arm
 *        (TOPIC_AUTHORIZATION_FAILED) reaches the app via share_poll
 *        while records from unaffected partitions are still
 *        delivered.
 */
static void test_partition_error_injection_share_fetch_surfaces_err(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic       = "0182-part_fetch_err_surface";
        const char *group       = "sg-0182-part_fetch_err_surface";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        rd_bool_t saw_err = rd_false;
        int p0_consumed   = 0;
        int attempts      = 0;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        /* Stack TOPIC_AUTHORIZATION_FAILED on partition 1 so the
         * client sees it on every ShareFetch reply until the test
         * times out — partition 0 is unaffected. The depth covers
         * all retries the client may issue in the polling window. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 1, RD_KAFKAP_ShareFetch, 5,
                        injected_err, injected_err, injected_err, injected_err,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition errors");

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Drain: collect all partition-0 records and watch for the
         * partition-1 surface err. Both paths must observe within
         * a bounded number of polls. */
        while (attempts++ < 40 && (!saw_err || p0_consumed < msgs_per_part)) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        TEST_SAY("share_poll returned %s: %s\n",
                                 rd_kafka_err2name(code),
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        if (code == injected_err)
                                saw_err = rd_true;
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        /* Partition-0 records: ack inline so the
                         * next share_poll passes the explicit-mode
                         * unacked gate. */
                        TEST_ASSERT(rkm->partition == 0,
                                    "unexpected record from partition %" PRId32
                                    " (partition 1 has only error in stack)",
                                    rkm->partition);
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        p0_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0, got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(saw_err,
                    "expected %s to surface from share_poll within %d polls",
                    rd_kafka_err2name(injected_err), attempts);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify per-partition error stacks are scoped per ApiKey:
 *        a ShareFetch err and a ShareAcknowledge err on the same
 *        partition each drain on their own path without
 *        cross-contamination.
 */
static void test_partition_error_injection_per_apikey_stack_isolation(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic       = "0182-part_err_apikey_isolation";
        const char *group       = "sg-0182-part_err_apikey_isolation";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t fetch_err =
            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER;
        const rd_kafka_resp_err_t ack_err = RD_KAFKA_RESP_ERR_INVALID_REQUEST;
        int acked;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);

        /* One transient ShareFetch err: drained on the first fetch,
         * the second fetch succeeds and records are delivered. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        fetch_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push fetch error");

        /* One ShareAck err: drained on commit_sync. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        ack_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push ack error");

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Fetch err drains transparently: all records delivered. */
        acked = consume_and_ack_all(rkshare, msgs_per_part);
        TEST_ASSERT(acked == msgs_per_part, "expected %d acked, got %d",
                    msgs_per_part, acked);

        /* Ack err drains on commit_sync. */
        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL, "expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 1, "expected 1 partition result, got %d",
                    partitions->cnt);
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_ASSERT(rktpar->err == ack_err,
                            "partition [%" PRId32 "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(ack_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(cb_state.callback_cnt == 1, "expected 1 callback, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(test_ack_cb_state_first_err(&cb_state) == ack_err,
                    "expected callback err %s, got %s",
                    rd_kafka_err2name(ack_err),
                    rd_kafka_err2name(test_ack_cb_state_first_err(&cb_state)));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify distinct per-partition ShareAcknowledge errs on
 *        different partitions in the same commit_sync are each
 *        propagated to the matching partition's result and ack
 *        callback.
 */
static void
test_partition_error_injection_share_ack_heterogeneous_multi_partition(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic       = "0182-part_ack_err_heterogeneous";
        const char *group       = "sg-0182-part_ack_err_heterogeneous";
        const int partition_cnt = 3;
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t p0_err =
            RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH;
        const rd_kafka_resp_err_t p2_err =
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR;
        int acked;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic,
                                               partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic with %d partitions", partition_cnt);

        for (i = 0; i < partition_cnt; i++)
                mock_produce_partition(ctx.producer, topic, i, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        acked = consume_and_ack_all(rkshare, partition_cnt * msgs_per_part);
        TEST_ASSERT(acked == partition_cnt * msgs_per_part,
                    "expected %d acked, got %d", partition_cnt * msgs_per_part,
                    acked);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        p0_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition 0 error");
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 2, RD_KAFKAP_ShareAcknowledge, 1,
                        p2_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition 2 error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL, "expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == partition_cnt,
                    "expected results for %d partitions, got %d", partition_cnt,
                    partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                rd_kafka_resp_err_t exp_err;

                switch (rktpar->partition) {
                case 0:
                        exp_err = p0_err;
                        break;
                case 2:
                        exp_err = p2_err;
                        break;
                default:
                        exp_err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        break;
                }

                TEST_SAY("%s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2name(rktpar->err));
                TEST_ASSERT(rktpar->err == exp_err,
                            "partition [%" PRId32 "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(exp_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* One callback per partition; one each carries p0_err, p2_err,
         * NO_ERROR. */
        TEST_ASSERT(cb_state.callback_cnt == partition_cnt,
                    "expected %d callbacks (one per partition), got %d",
                    partition_cnt, cb_state.callback_cnt);
        TEST_ASSERT(test_ack_cb_state_count_err(&cb_state, p0_err) == 1,
                    "expected 1 callback with %s", rd_kafka_err2name(p0_err));
        TEST_ASSERT(test_ack_cb_state_count_err(&cb_state, p2_err) == 1,
                    "expected 1 callback with %s", rd_kafka_err2name(p2_err));
        TEST_ASSERT(test_ack_cb_state_count_err(
                        &cb_state, RD_KAFKA_RESP_ERR_NO_ERROR) == 1,
                    "expected 1 callback with NO_ERROR");
        TEST_ASSERT(cb_state.total_offsets ==
                        (size_t)(partition_cnt * msgs_per_part),
                    "expected callback total_offsets %d, got %zu",
                    partition_cnt * msgs_per_part, cb_state.total_offsets);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


static rd_bool_t is_metadata_request(rd_kafka_mock_request_t *request,
                                     void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Metadata;
}

static rd_bool_t
is_share_group_heartbeat_request(rd_kafka_mock_request_t *request,
                                 void *opaque) {
        return rd_kafka_mock_request_api_key(request) ==
               RD_KAFKAP_ShareGroupHeartbeat;
}

/**
 * @brief Verify a leader-unavailable-class err
 *        (NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH,
 *        KAFKA_STORAGE_ERROR, OFFSET_NOT_AVAILABLE,
 *        REPLICA_NOT_AVAILABLE) on a per-partition ShareFetch
 *        triggers a Metadata refresh and recovers transparently:
 *        records arrive without the err surfacing to share_poll.
 */
static void do_test_share_fetch_partition_err_triggers_metadata_refresh(
    rd_kafka_resp_err_t injected_err) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        char topic[64];
        char group[64];
        const int msgs_per_part    = 5;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        int p0_consumed = 0;
        int p1_consumed = 0;
        int attempts    = 0;
        size_t metadata_cnt;
        rd_bool_t saw_surface = rd_false;

        SUB_TEST_QUICK("%s", rd_kafka_err2name(injected_err));

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-fetch_refresh_%s",
                    rd_kafka_err2name(injected_err));
        rd_snprintf(group, sizeof(group), "sg-0182-fetch_refresh_%s",
                    rd_kafka_err2name(injected_err));

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Start request tracking AFTER subscribe so we don't count
         * the initial subscription-driven metadata fetch. Clear
         * accumulated requests so the count window starts empty. */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Stack 3 errs to keep the err arm armed across multiple
         * fetch attempts. Each fetch pops one off the stack until
         * the partition recovers. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 3,
                        injected_err, injected_err,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition errors");

        /* Drain until all records arrive from both partitions (recovery
         * must succeed). Bound the attempts to keep the test quick. */
        while (attempts++ < 40 &&
               (p0_consumed < msgs_per_part || p1_consumed < msgs_per_part)) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        if (code == injected_err)
                                saw_surface = rd_true;
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (rkm->partition == 0)
                                p0_consumed++;
                        else if (rkm->partition == 1)
                                p1_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0, got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(p1_consumed == msgs_per_part,
                    "expected %d records from partition 1, got %d",
                    msgs_per_part, p1_consumed);
        TEST_ASSERT(!saw_surface,
                    "leader-unavailable arm must NOT surface %s to app",
                    rd_kafka_err2name(injected_err));

        metadata_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_metadata_request, NULL);
        TEST_SAY("metadata requests during recovery window: %" PRIusz "\n",
                 metadata_cnt);
        TEST_ASSERT(metadata_cnt >= 1,
                    "leader-unavailable arm must trigger >= 1 Metadata "
                    "request, got %" PRIusz,
                    metadata_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

static void
test_share_fetch_partition_err_triggers_metadata_refresh_matrix(void) {
        do_test_share_fetch_partition_err_triggers_metadata_refresh(
            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER);
        do_test_share_fetch_partition_err_triggers_metadata_refresh(
            RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH);
        do_test_share_fetch_partition_err_triggers_metadata_refresh(
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR);
        do_test_share_fetch_partition_err_triggers_metadata_refresh(
            RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE);
        do_test_share_fetch_partition_err_triggers_metadata_refresh(
            RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE);
}


/**
 * @brief Verify a silent-await-class err (UNKNOWN_TOPIC_OR_PART,
 *        UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID) on a per-partition
 *        ShareFetch does not surface to share_poll; records are
 *        delivered once the err stack drains.
 */
static void do_test_share_fetch_partition_err_silent_await(
    rd_kafka_resp_err_t injected_err) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        char topic[64];
        char group[64];
        const int msgs_per_part    = 5;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        int p0_consumed       = 0;
        int p1_consumed       = 0;
        int attempts          = 0;
        rd_bool_t saw_surface = rd_false;

        SUB_TEST_QUICK("%s", rd_kafka_err2name(injected_err));

        ctx = test_ctx_new();

        rd_snprintf(topic, sizeof(topic), "0182-fetch_silent_%s",
                    rd_kafka_err2name(injected_err));
        rd_snprintf(group, sizeof(group), "sg-0182-fetch_silent_%s",
                    rd_kafka_err2name(injected_err));

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Single err on partition 0; recovery requires no client
         * action — the err just drains and the next fetch succeeds. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        while (attempts++ < 40 &&
               (p0_consumed < msgs_per_part || p1_consumed < msgs_per_part)) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        if (code == injected_err)
                                saw_surface = rd_true;
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (rkm->partition == 0)
                                p0_consumed++;
                        else if (rkm->partition == 1)
                                p1_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0 after stack "
                    "drain, got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(p1_consumed == msgs_per_part,
                    "expected %d records from partition 1, got %d",
                    msgs_per_part, p1_consumed);
        TEST_ASSERT(!saw_surface, "silent-await arm must NOT surface %s to app",
                    rd_kafka_err2name(injected_err));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

static void test_share_fetch_partition_err_silent_await_matrix(void) {
        do_test_share_fetch_partition_err_silent_await(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        do_test_share_fetch_partition_err_silent_await(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID);
        do_test_share_fetch_partition_err_silent_await(
            RD_KAFKA_RESP_ERR_INCONSISTENT_TOPIC_ID);
}


/**
 * @brief Verify the default arm of the per-partition ShareFetch
 *        error handler translates an unmapped err code to __STATE
 *        before surfacing it to share_poll; the original broker
 *        code is never visible to the app.
 */
static void test_share_fetch_partition_err_default_translates_to_state(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic       = "0182-fetch_default_state";
        const char *group       = "sg-0182-fetch_default_state";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_LOG_DIR_NOT_FOUND;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        rd_bool_t saw_state    = rd_false;
        rd_bool_t saw_original = rd_false;
        int p0_consumed        = 0;
        int attempts           = 0;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        /* Stack on partition 1 so partition 0 is unaffected. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 1, RD_KAFKAP_ShareFetch, 5,
                        injected_err, injected_err, injected_err, injected_err,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition errors");

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        while (attempts++ < 40 && (!saw_state || p0_consumed < msgs_per_part)) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        TEST_SAY("share_poll returned %s: %s\n",
                                 rd_kafka_err2name(code),
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        if (code == RD_KAFKA_RESP_ERR__STATE)
                                saw_state = rd_true;
                        else if (code == injected_err)
                                saw_original = rd_true;
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        TEST_ASSERT(rkm->partition == 0,
                                    "unexpected record from partition %" PRId32,
                                    rkm->partition);
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        p0_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0, got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(saw_state,
                    "expected __STATE to surface from default arm "
                    "translation");
        TEST_ASSERT(!saw_original,
                    "default arm must translate %s to __STATE, not "
                    "surface the original code",
                    rd_kafka_err2name(injected_err));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify the UNKNOWN_LEADER_EPOCH arm of the per-partition
 *        ShareFetch error handler does not surface the err to
 *        share_poll; records are delivered once the err stack drains.
 */
static void test_share_fetch_partition_err_unknown_leader_epoch_log_only(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic       = "0182-fetch_unknown_leader_epoch";
        const char *group       = "sg-0182-fetch_unknown_leader_epoch";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        int p0_consumed       = 0;
        int p1_consumed       = 0;
        int attempts          = 0;
        rd_bool_t saw_surface = rd_false;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        while (attempts++ < 40 &&
               (p0_consumed < msgs_per_part || p1_consumed < msgs_per_part)) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        if (code == injected_err)
                                saw_surface = rd_true;
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (rkm->partition == 0)
                                p0_consumed++;
                        else if (rkm->partition == 1)
                                p1_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0, got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(p1_consumed == msgs_per_part,
                    "expected %d records from partition 1, got %d",
                    msgs_per_part, p1_consumed);
        TEST_ASSERT(!saw_surface,
                    "UNKNOWN_LEADER_EPOCH arm must NOT surface to app");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify a per-partition ShareAcknowledge err does not reset
 *        the share session: after the failed ack, a subsequent
 *        fetch + ack + commit_sync cycle succeeds with NO_ERROR for
 *        every partition and no extra ShareGroupHeartbeat rejoin
 *        fires.
 */
static void test_share_ack_partition_err_preserves_session(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic       = "0182-ack_err_preserves_session";
        const char *group       = "sg-0182-ack_err_preserves_session";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_INVALID_REQUEST;
        int acked;
        int i;
        size_t heartbeat_cnt;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 1: consume + ack all, inject per-partition err on
         * partition 0 only, commit_sync. */
        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "Phase 1: expected %d acked",
                    2 * msgs_per_part);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 2,
                    "Phase 1: expected 2 partition results, got %d",
                    partitions->cnt);
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                rd_kafka_resp_err_t exp_err        = rktpar->partition == 0
                                                         ? injected_err
                                                         : RD_KAFKA_RESP_ERR_NO_ERROR;
                TEST_ASSERT(rktpar->err == exp_err,
                            "Phase 1 partition [%" PRId32
                            "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(exp_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Start tracking AFTER Phase 1 to count only Phase-2 wire
         * activity. Session reset would force a member rejoin via an
         * extra ShareGroupHeartbeat; in the happy preserved-session
         * path the consumer continues with its existing member id
         * and no rejoin heartbeat is needed in this short window. */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Phase 2: fresh records on partition 1. If session was
         * reset, commit_sync here would surface
         * INVALID_SHARE_SESSION_EPOCH for every partition (epoch-0
         * strip). NO_ERROR means the session is intact. The lower
         * bound on acked count is msgs_per_part (the new p1 records);
         * p0 may also contribute if its lock has expired by the time
         * we poll. */
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        acked = consume_and_ack_all(rkshare, msgs_per_part);
        TEST_ASSERT(acked >= msgs_per_part,
                    "Phase 2: expected >= %d acked, got %d", msgs_per_part,
                    acked);

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 2: expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt >= 1,
                    "Phase 2: expected at least 1 partition result, got %d",
                    partitions->cnt);
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Phase 2 partition [%" PRId32
                            "]: session should be intact, expected "
                            "NO_ERROR, got %s",
                            rktpar->partition, rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Heartbeat count check: a session reset would push the
         * client to rejoin (extra heartbeat with new member id /
         * reset epoch). Allow up to 1 for a natural-interval
         * heartbeat that may fire during the Phase 2 window; any
         * additional heartbeats here suggest a forced rejoin. */
        heartbeat_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_group_heartbeat_request, NULL);
        TEST_SAY("Phase 2 ShareGroupHeartbeat count: %" PRIusz "\n",
                 heartbeat_cnt);
        TEST_ASSERT(heartbeat_cnt <= 1,
                    "Phase 2: ShareGroupHeartbeat count > 1 suggests a "
                    "rejoin (per-partition ack err should not reset "
                    "session); got %" PRIusz,
                    heartbeat_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify the client does not silently retry a failed
 *        per-partition ack: after a commit_sync surfaces the
 *        injected err, an immediate follow-up commit_sync with no
 *        new acks must send zero ShareAcknowledge requests on the
 *        wire.
 */
static void test_share_ack_partition_err_not_auto_retried(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic       = "0182-ack_err_no_auto_retry";
        const char *group       = "sg-0182-ack_err_no_auto_retry";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_INVALID_REQUEST;
        int acked;
        size_t share_ack_cnt;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        acked = consume_and_ack_all(rkshare, msgs_per_part);
        TEST_ASSERT(acked == msgs_per_part, "expected %d acked", msgs_per_part);

        /* Inject only one err: if the client retried, the second
         * commit_sync would consume the still-injected err — but
         * after this single err drains, the stack is empty. So the
         * test result is unambiguous regardless of whether the err
         * arm is "transient" or "permanent". */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        /* First commit_sync: surfaces the err. */
        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "1st commit_sync: expected non-NULL results");
        TEST_ASSERT(partitions->cnt == 1,
                    "1st commit_sync: expected 1 partition, got %d",
                    partitions->cnt);
        TEST_ASSERT(partitions->elems[0].err == injected_err,
                    "1st commit_sync: expected %s, got %s",
                    rd_kafka_err2name(injected_err),
                    rd_kafka_err2name(partitions->elems[0].err));
        rd_kafka_topic_partition_list_destroy(partitions);

        /* Start tracking AFTER the 1st commit_sync so we count only
         * wire traffic from the 2nd commit_sync window. */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Second commit_sync immediately, with no new acks. If the
         * client auto-retried the failed ack, a ShareAcknowledge
         * would fire here to deliver the re-queued ack. Zero
         * ShareAck requests confirms no retry. The partitions
         * pointer may be NULL or point to an empty list — both
         * mean "nothing to commit", which is the success signal. */
        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);
        TEST_SAY("2nd commit_sync wire counts: ShareAck=%" PRIusz "\n",
                 share_ack_cnt);
        TEST_ASSERT(share_ack_cnt == 0,
                    "2nd commit_sync: expected 0 ShareAck requests (no "
                    "auto-retry of failed ack), got %" PRIusz,
                    share_ack_cnt);

        if (partitions) {
                TEST_ASSERT(partitions->cnt == 0,
                            "2nd commit_sync: expected empty partitions "
                            "list when present, got %d entries",
                            partitions->cnt);
                rd_kafka_topic_partition_list_destroy(partitions);
        }

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify a leader-unavailable err on a ShareFetch issued
 *        after a clean fetch + ack cycle (mid-session) still
 *        triggers a Metadata refresh and recovers transparently.
 */
static void test_share_fetch_partition_err_on_subsequent_fetch_recovers(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic       = "0182-fetch_err_subsequent_fetch";
        const char *group       = "sg-0182-fetch_err_subsequent_fetch";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        int p0_consumed = 0;
        int attempts    = 0;
        int acked;
        size_t metadata_cnt;
        rd_bool_t saw_surface = rd_false;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 1: clean fetch + ack establishes the session. */
        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "Phase 1: expected %d acked",
                    2 * msgs_per_part);

        /* Phase 2: produce more on partition 0, inject err on its
         * next ShareFetch, verify recovery. Tracking starts here so
         * only Phase 2 metadata fanout is counted. */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 3,
                        injected_err, injected_err,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition errors");

        while (attempts++ < 40 && p0_consumed < msgs_per_part) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
                        if (code == injected_err)
                                saw_surface = rd_true;
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (rkm->partition == 0)
                                p0_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(p0_consumed == msgs_per_part,
                    "expected %d records from partition 0 after recovery, "
                    "got %d",
                    msgs_per_part, p0_consumed);
        TEST_ASSERT(!saw_surface,
                    "leader-unavailable arm must NOT surface to app on a "
                    "subsequent fetch");

        metadata_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_metadata_request, NULL);
        TEST_SAY("metadata requests during recovery window: %" PRIusz "\n",
                 metadata_cnt);
        TEST_ASSERT(metadata_cnt >= 1,
                    "leader-unavailable arm on subsequent fetch must "
                    "trigger >= 1 Metadata request, got %" PRIusz,
                    metadata_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify a per-partition ShareAck err on a second commit_sync
 *        (after a clean prior cycle) propagates to the matching
 *        partition's result and ack callback, while unaffected
 *        partitions remain NO_ERROR.
 */
static void test_share_ack_partition_err_after_clean_ack_surfaces(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_error_t *error;
        const char *topic       = "0182-ack_err_after_clean";
        const char *group       = "sg-0182-ack_err_after_clean";
        const int msgs_per_part = 5;
        const rd_kafka_resp_err_t injected_err =
            RD_KAFKA_RESP_ERR_INVALID_REQUEST;
        int acked;
        int i;
        test_ack_cb_state_t cb_state = {0};

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             &cb_state, test_share_ack_cb);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Phase 1: clean ack cycle. */
        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "Phase 1: expected %d acked",
                    2 * msgs_per_part);

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 1: expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 2,
                    "Phase 1: expected 2 partition results, got %d",
                    partitions->cnt);
        for (i = 0; i < partitions->cnt; i++) {
                TEST_ASSERT(partitions->elems[i].err ==
                                RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Phase 1 partition [%" PRId32
                            "]: expected NO_ERROR, got %s",
                            partitions->elems[i].partition,
                            rd_kafka_err2name(partitions->elems[i].err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(cb_state.callback_cnt == 2,
                    "Phase 1: expected 2 callbacks, got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(test_ack_cb_state_count_err(
                        &cb_state, RD_KAFKA_RESP_ERR_NO_ERROR) == 2,
                    "Phase 1: expected 2 NO_ERROR callbacks");

        /* Phase 2: fresh batch + err on partition 0 ShareAck. */
        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);
        mock_produce_partition(ctx.producer, topic, 1, msgs_per_part);

        acked = consume_and_ack_all(rkshare, 2 * msgs_per_part);
        TEST_ASSERT(acked == 2 * msgs_per_part, "Phase 2: expected %d acked",
                    2 * msgs_per_part);

        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareAcknowledge, 1,
                        injected_err) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push partition error");

        partitions = NULL;
        error      = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        if (error)
                rd_kafka_error_destroy(error);

        TEST_ASSERT(partitions != NULL,
                    "Phase 2: expected non-NULL partition results");
        TEST_ASSERT(partitions->cnt == 2,
                    "Phase 2: expected 2 partition results, got %d",
                    partitions->cnt);
        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                rd_kafka_resp_err_t exp_err        = rktpar->partition == 0
                                                         ? injected_err
                                                         : RD_KAFKA_RESP_ERR_NO_ERROR;
                TEST_ASSERT(rktpar->err == exp_err,
                            "Phase 2 partition [%" PRId32
                            "]: expected %s, got %s",
                            rktpar->partition, rd_kafka_err2name(exp_err),
                            rd_kafka_err2name(rktpar->err));
        }
        rd_kafka_topic_partition_list_destroy(partitions);

        TEST_ASSERT(cb_state.callback_cnt == 4,
                    "expected 4 callbacks total (2 per phase), got %d",
                    cb_state.callback_cnt);
        TEST_ASSERT(test_ack_cb_state_count_err(
                        &cb_state, RD_KAFKA_RESP_ERR_NO_ERROR) == 3,
                    "expected 3 NO_ERROR callbacks total (2 Phase 1 + 1 "
                    "Phase 2 partition 1)");
        TEST_ASSERT(test_ack_cb_state_count_err(&cb_state, injected_err) == 1,
                    "expected 1 callback with the injected err");

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ack_cb_state_destroy(&cb_state);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify the share consumer fires ShareGroupHeartbeat
 *        requests at the configured interval: with a 1000ms
 *        interval, expect 3-5 heartbeats over ~2s.
 */
static void test_share_group_adherence_to_hb_interval(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic = "0182-hb_interval_adherence";
        const char *group = "sg-0182-hb_interval_adherence";
        size_t hb_cnt;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        rd_kafka_mock_sharegroup_set_heartbeat_interval(ctx.mcluster, 1000);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 3, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);

        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        rd_sleep(2);

        hb_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_group_heartbeat_request, NULL);
        TEST_SAY("ShareGroupHeartbeat count over ~2s: %" PRIusz "\n", hb_cnt);
        TEST_ASSERT(hb_cnt >= 3 && hb_cnt <= 5,
                    "Expected 3–5 ShareGroupHeartbeats at 1000ms interval "
                    "over ~2s, got %" PRIusz,
                    hb_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Verify the share consumer cannot fetch from a topic while
 *        its Metadata response carries UNKNOWN_TOPIC_ID, and that
 *        records arrive once the err clears. With two topics, a
 *        clean topic must still deliver records while the other is
 *        blocked.
 */
static void
do_test_share_group_metadata_unknown_topic_id(rd_bool_t two_topics) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic          = "0182-meta_unk_topic_id";
        const char *topic2         = "0182-meta_unk_topic_id_2";
        const int msgs_per_topic   = 5;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        int attempts;
        int blocked_topic_consumed = 0;
        int other_topic_consumed   = 0;
        char group[64];

        SUB_TEST_QUICK("%s", two_topics ? "two topics" : "one topic");

        rd_snprintf(group, sizeof(group), "sg-0182-meta_unk_topic_id_%s",
                    two_topics ? "two" : "one");

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        if (two_topics) {
                TEST_ASSERT(
                    rd_kafka_mock_topic_create(ctx.mcluster, topic2, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic2");
        }

        mock_produce(ctx.producer, topic, msgs_per_topic);
        if (two_topics)
                mock_produce(ctx.producer, topic2, msgs_per_topic);

        /* Inject UNKNOWN_TOPIC_ID on the blocked topic's Metadata. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        if (two_topics)
                test_share_consumer_subscribe_multi(rkshare, 2, topic, topic2);
        else
                test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* While the err is injected the blocked topic delivers nothing.
         * In the two-topic case the clean topic should still deliver. */
        for (attempts = 0; attempts < 4; attempts++) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (!strcmp(rd_kafka_topic_name(rkm->rkt), topic))
                                blocked_topic_consumed++;
                        else
                                other_topic_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(blocked_topic_consumed == 0,
                    "Expected 0 records from blocked topic while err "
                    "injected, got %d",
                    blocked_topic_consumed);
        if (two_topics)
                TEST_ASSERT(other_topic_consumed == msgs_per_topic,
                            "Expected %d records from the clean topic, got %d",
                            msgs_per_topic, other_topic_consumed);

        /* Clear the err. Reconciliation should now complete and the
         * blocked topic's records should arrive. */
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic,
                                      RD_KAFKA_RESP_ERR_NO_ERROR);

        attempts = 0;
        while (attempts++ < 40 && blocked_topic_consumed < msgs_per_topic) {
                size_t rcvd, j;
                error = rd_kafka_share_poll(rkshare, 500, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                rcvd = rd_kafka_messages_count(batch);
                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (rkm->err)
                                continue;
                        rd_kafka_share_acknowledge(rkshare, rkm);
                        if (!strcmp(rd_kafka_topic_name(rkm->rkt), topic))
                                blocked_topic_consumed++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_ASSERT(blocked_topic_consumed == msgs_per_topic,
                    "After clearing err: expected %d records from "
                    "blocked topic, got %d",
                    msgs_per_topic, blocked_topic_consumed);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}

static void test_share_group_metadata_unknown_topic_id_tests(void) {
        do_test_share_group_metadata_unknown_topic_id(rd_false /* one topic */);
        do_test_share_group_metadata_unknown_topic_id(rd_true /* two topics */);
}


/**
 * @brief Verify rapid rd_kafka_share_subscribe /
 *        rd_kafka_share_unsubscribe cycles do not trip any
 *        assertion and the consumer can be cleanly destroyed
 *        afterwards, both with a ready mock cluster and against an
 *        unreachable bootstrap.
 */
static void do_test_share_group_quick_unsubscribe(rd_bool_t cluster_ready) {
        test_ctx_t ctx                    = {0};
        rd_kafka_mock_cluster_t *mcluster = NULL;
        const char *bootstraps            = "localhost:9999";
        const char *topic                 = "0182-quick_unsub";
        const char *group                 = "sg-0182-quick_unsub";
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_share_t *rkshare;
        rd_kafka_resp_err_t err;
        int i;

        SUB_TEST_QUICK("%s",
                       cluster_ready ? "mock cluster ready" : "no cluster");

        /* The no-cluster variant points at an unreachable bootstrap, so the
         * consumer's background connect legitimately raises __TRANSPORT /
         * __ALL_BROKERS_DOWN on the error callback. Those must not fail the
         * test — only the subscribe/unsubscribe return codes matter here. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        if (cluster_ready) {
                ctx = test_ctx_new();
                TEST_ASSERT(
                    rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
                bootstraps = ctx.bootstraps;
                mcluster   = ctx.mcluster;
        }

        rkshare = create_mock_share_consumer(bootstraps, group, "explicit",
                                             NULL, NULL);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        for (i = 0; i < 2; i++) {
                err = rd_kafka_share_subscribe(rkshare, subscription);
                TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "iter %d: subscribe failed: %s", i,
                            rd_kafka_err2name(err));
                err = rd_kafka_share_unsubscribe(rkshare);
                TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "iter %d: unsubscribe failed: %s", i,
                            rd_kafka_err2name(err));
        }

        rd_kafka_topic_partition_list_destroy(subscription);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        if (cluster_ready)
                test_ctx_destroy(&ctx);
        else
                RD_IF_FREE(mcluster, test_mock_cluster_destroy);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}

static void test_share_group_quick_unsubscribe_tests(void) {
        do_test_share_group_quick_unsubscribe(rd_false /* no cluster */);
        do_test_share_group_quick_unsubscribe(rd_true /* cluster ready */);
}


/**
 * @brief Verify a NOT_LEADER_OR_FOLLOWER err on ShareFetch triggers
 *        a fast-leader-query Metadata request after the failing
 *        ShareFetch.
 */
static void test_share_fetch_fast_leader_query_backoff(void) {
        test_ctx_t ctx;
        rd_kafka_share_t *rkshare;
        const char *topic          = "0182-fast_leader_query_backoff";
        const char *group          = "sg-0182-fast_leader_query_backoff";
        const int msgs_per_part    = 5;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *error;
        rd_kafka_mock_request_t **requests  = NULL;
        size_t request_cnt                  = 0;
        rd_bool_t previous_was_ShareFetch   = rd_false;
        rd_bool_t metadata_after_ShareFetch = rd_false;
        size_t i;
        int acked;

        SUB_TEST_QUICK();

        ctx = test_ctx_new();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        mock_produce_partition(ctx.producer, topic, 0, msgs_per_part);

        rkshare = create_mock_share_consumer(ctx.bootstraps, group, "explicit",
                                             NULL, NULL);
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Drive a clean consume cycle so the share session is fully
         * established and any subscription-driven metadata traffic is
         * out of the way before we start tracking. */
        acked = consume_and_ack_all(rkshare, msgs_per_part);
        TEST_ASSERT(acked == msgs_per_part,
                    "Phase 1: expected %d acked, got %d", msgs_per_part, acked);

        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Inject one NOT_LEADER_OR_FOLLOWER on the next ShareFetch.
         * The arm triggers rd_kafka_toppar_leader_unavailable →
         * topic_fast_leader_query → Metadata refresh. */
        TEST_ASSERT(rd_kafka_mock_partition_push_request_errors(
                        ctx.mcluster, topic, 0, RD_KAFKAP_ShareFetch, 1,
                        RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "push fetch error");

        /* Drive a poll so the ShareFetch goes out and consumes the
         * injected err. */
        error = rd_kafka_share_poll(rkshare, 500, &batch);
        if (error)
                rd_kafka_error_destroy(error);
        rd_kafka_messages_destroy(batch);

        /* Give the fast-leader-query Metadata request a chance to
         * fire. */
        rd_sleep(3);

        requests = rd_kafka_mock_get_requests(ctx.mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                int16_t api = rd_kafka_mock_request_api_key(requests[i]);
                TEST_SAY("Request: api=%d ts=%" PRId64 "\n", (int)api,
                         rd_kafka_mock_request_timestamp(requests[i]));

                if (api == RD_KAFKAP_ShareFetch) {
                        previous_was_ShareFetch = rd_true;
                } else if (api == RD_KAFKAP_Metadata &&
                           previous_was_ShareFetch) {
                        metadata_after_ShareFetch = rd_true;
                        break;
                } else if (api != RD_KAFKAP_ShareGroupHeartbeat) {
                        previous_was_ShareFetch = rd_false;
                }
        }
        rd_kafka_mock_request_destroy_array(requests, request_cnt);

        TEST_ASSERT(metadata_after_ShareFetch,
                    "expected a Metadata request after a failing "
                    "ShareFetch");

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


int main_0182_share_consumer_error_handling_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        test_timeout_set(300);

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

        /* Topic-level metadata err surface */
        test_share_consumer_surfaces_topic_exception();
        test_share_consumer_surfaces_topic_authorization_failed();
        test_share_consumer_multi_partition_single_op_per_cycle();
        test_share_consumer_re_emits_when_err_code_changes();
        test_share_consumer_re_surfaces_after_recovery();
        test_share_consumer_re_surfaces_after_recovery_topic_exception();
        test_share_consumer_resubscribe_re_emits_persistent_failure();
        test_share_consumer_does_not_surface_unknown_topic_or_part();

        /* Socket timeout matrix (single broker).
         *
         * Each call corresponds to a different ordering of the three
         * timer layers (api_timeout_ms, socket_timeout_ms, rtt_ms).
         * See the comment in do_test_socket_timeout_full_ack_then_more
         * for the full analysis of what each ordering exercises.
         * Arguments are (api_timeout_ms, socket_timeout_ms, rtt_ms). */

        /* All 6 strict-inequality permutations. */
        do_test_socket_timeout_full_ack_then_more(
            1000, 5000, 3000); /* api < rtt < socket  */
        do_test_socket_timeout_full_ack_then_more(
            1000, 3000, 5000); /* api < socket < rtt  */
        do_test_socket_timeout_full_ack_then_more(
            3000, 5000, 1000); /* rtt < api < socket  */
        do_test_socket_timeout_full_ack_then_more(
            5000, 3000, 1000); /* rtt < socket < api  */
        do_test_socket_timeout_full_ack_then_more(
            3000, 1000, 5000); /* socket < api < rtt  */
        do_test_socket_timeout_full_ack_then_more(
            5000, 1000, 3000); /* socket < rtt < api  */
        /* Boundary api == socket is intentionally skipped: the race
         * between the api timer cb and the wire socket timer is
         * non-deterministic in practice — both outcomes are
         * observed across runs. */

        /* Partial-ack variant of the matrix. Phase 1 acks only half
         * the consumed records; Phase 2 acks the remaining half (still
         * in client's inflight map). Phase 2 commit_sync surfaces
         * INVALID_SHARE_SESSION_EPOCH when the session was dropped
         * by the Phase 1 socket teardown (socket < rtt), NO_ERROR
         * otherwise. */
        do_test_socket_timeout_partial_ack_then_remaining(
            1000, 5000, 3000); /* api < rtt < socket  */
        do_test_socket_timeout_partial_ack_then_remaining(
            1000, 3000, 5000); /* api < socket < rtt  */
        do_test_socket_timeout_partial_ack_then_remaining(
            3000, 5000, 1000); /* rtt < api < socket  */
        do_test_socket_timeout_partial_ack_then_remaining(
            5000, 3000, 1000); /* rtt < socket < api  */
        do_test_socket_timeout_partial_ack_then_remaining(
            3000, 1000, 5000); /* socket < api < rtt  */
        do_test_socket_timeout_partial_ack_then_remaining(
            5000, 1000, 3000); /* socket < rtt < api  */
        /* Boundary api == socket skipped — see comment above. */

        /* select_broker STATE_UP guard: steady-state DOWN and
         * race-window flavours. */
        do_test_no_bounce_loop_on_down_broker();
        do_test_one_log_on_broker_down_during_active_empty_poll();

        /* Partition-level error injection. */
        test_partition_error_injection_general();
        test_partition_error_injection_share_fetch();
        test_partition_error_injection_share_ack();

        test_partition_error_injection_share_ack_matrix();
        test_partition_error_injection_share_fetch_surfaces_err();
        test_partition_error_injection_per_apikey_stack_isolation();
        test_partition_error_injection_share_ack_heterogeneous_multi_partition();

        test_share_fetch_partition_err_triggers_metadata_refresh_matrix();
        test_share_fetch_partition_err_silent_await_matrix();
        test_share_fetch_partition_err_default_translates_to_state();
        test_share_fetch_partition_err_unknown_leader_epoch_log_only();
        test_share_ack_partition_err_preserves_session();
        test_share_ack_partition_err_not_auto_retried();

        test_share_fetch_partition_err_on_subsequent_fetch_recovers();
        test_share_ack_partition_err_after_clean_ack_surfaces();

        test_share_group_adherence_to_hb_interval();
        test_share_group_metadata_unknown_topic_id_tests();
        test_share_group_quick_unsubscribe_tests();

        test_share_fetch_fast_leader_query_backoff();

        return 0;
}
