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
 * Verifies the broker-thread helper + main reply handler defensive sentinel
 * path:
 *   ShareAcknowledge response top-level err
 *     -> rd_kafka_share_fetch_op_reply_with_err sets err on each batch
 *        in ack_details
 *     -> main reply handler copies batch->rktpar->err into
 *        rkcg_commit_sync_request.results
 *     -> commit_sync caller sees per-partition err equal to top-level err
 *
 * Covers:
 *   - Gap #1, #5, #10, #39 — top-level err propagated to all partitions
 *     in the request (no _IN_PROGRESS leak to caller)
 *   - Gap #3 — SHARE_SESSION_LIMIT_REACHED on ShareAck handled via default
 *     case (no session reset)
 *   - Gap #4/#9 — auth/fatal errors propagated through default case
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
                                                    const char *group_id) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");

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

        rkshare = create_mock_share_consumer(ctx.bootstraps, group);
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

/* ===================================================================
 *  T1 — Top-level SHARE_SESSION_NOT_FOUND on ShareAcknowledge
 *
 *  Covers Gap #1, #5, #10, #39: session-error variant of top-level
 *  err propagation. Without the helper + defensive sentinel the
 *  commit_sync caller would see _IN_PROGRESS instead.
 * =================================================================== */
static void test_commit_sync_share_session_not_found(void) {
        do_test_commit_sync_top_level_err(
            "session-not-found", RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND);
}

/* ===================================================================
 *  T-2 — Top-level INVALID_SHARE_SESSION_EPOCH on ShareAcknowledge
 *
 *  Same path as T1 but exercises a different session error to
 *  confirm propagation isn't tied to a specific err code.
 * =================================================================== */
static void test_commit_sync_invalid_share_session_epoch(void) {
        do_test_commit_sync_top_level_err(
            "invalid-session-epoch",
            RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH);
}

/* ===================================================================
 *  T5 — Top-level SHARE_SESSION_LIMIT_REACHED on ShareAcknowledge
 *
 *  Covers Gap #3: previously librdkafka treated this as a session
 *  error and reset the session epoch. Now ShareAcknowledge handler
 *  has no special case for SHARE_SESSION_LIMIT_REACHED — it falls
 *  to the default branch and the top-level err is just propagated
 *  to commit_sync results. (Java does the same: this error is
 *  ShareFetch-only.)
 * =================================================================== */
static void test_commit_sync_share_session_limit_reached(void) {
        do_test_commit_sync_top_level_err(
            "session-limit-reached",
            RD_KAFKA_RESP_ERR_SHARE_SESSION_LIMIT_REACHED);
}

/* ===================================================================
 *  T6a — Top-level GROUP_AUTHORIZATION_FAILED on ShareAcknowledge
 *
 *  Covers Gap #4/#9 (ShareAck side). Previously hit `default: break`
 *  with no ack propagation. Now propagates through helper +
 *  defensive sentinel to commit_sync results.
 * =================================================================== */
static void test_commit_sync_group_authorization_failed(void) {
        do_test_commit_sync_top_level_err(
            "group-auth-failed", RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
}

/* ===================================================================
 *  T6b — Top-level TOPIC_AUTHORIZATION_FAILED on ShareAcknowledge
 *
 *  Same default-case path as T6a with a different err.
 * =================================================================== */
static void test_commit_sync_topic_authorization_failed(void) {
        do_test_commit_sync_top_level_err(
            "topic-auth-failed", RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
}

/* ===================================================================
 *  T-Default — Top-level INVALID_REQUEST on ShareAcknowledge
 *
 *  Covers default-case path with a generic protocol error to confirm
 *  unknown / fatal codes also propagate.
 * =================================================================== */
static void test_commit_sync_invalid_request(void) {
        do_test_commit_sync_top_level_err("invalid-request",
                                          RD_KAFKA_RESP_ERR_INVALID_REQUEST);
}

int main_0182_share_consumer_error_handling_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        test_commit_sync_share_session_not_found();
        test_commit_sync_invalid_share_session_epoch();
        test_commit_sync_share_session_limit_reached();
        test_commit_sync_group_authorization_failed();
        test_commit_sync_topic_authorization_failed();
        test_commit_sync_invalid_request();

        return 0;
}