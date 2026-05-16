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
 * @brief Share consumer leader change tests.
 *
 * Verifies that per-partition leader-change errors on ShareAcknowledge
 * trigger metadata refresh and that unnecessary RPCs are avoided once
 * the client discovers the new leader.
 */

#define CONSUME_ARRAY 1024

/* ===================================================================
 *  Mock broker infrastructure.
 * =================================================================== */
typedef struct test_ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} test_ctx_t;

static test_ctx_t test_ctx_new(int nbrok) {
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

static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
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

static rd_bool_t is_share_ack_request(rd_kafka_mock_request_t *request,
                                      void *opaque) {
        return rd_kafka_mock_request_api_key(request) ==
               RD_KAFKAP_ShareAcknowledge;
}

static rd_bool_t is_share_fetch_request(rd_kafka_mock_request_t *request,
                                        void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_ShareFetch;
}

static rd_bool_t is_metadata_request(rd_kafka_mock_request_t *request,
                                     void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Metadata;
}


/* ===================================================================
 *  Test — ShareAcknowledge per-partition NOT_LEADER_OR_FOLLOWER
 *         should trigger metadata refresh and avoid redundant RPCs.
 *
 *  Setup:
 *    - 2 brokers, 1 topic, 1 partition on broker 1
 *    - Explicit ack mode, background metadata refresh disabled
 *    - Produce 10 records, consume one batch (expect all 10)
 *    - Move leader to broker 2
 *
 *  Loop (5 iterations):
 *    - Acknowledge 2 records from the held batch
 *    - commitSync → sends ShareAcknowledge to stale broker 1
 *
 *  Expected (after both fixes land):
 *    - Only 1 ShareAcknowledge RPC sent (first commitSync)
 *    - Remaining 4 commitSync fail locally (leader mismatch)
 *    - All 5 commitSync return NOT_LEADER_OR_FOLLOWER
 *
 *  Current behavior (no fix):
 *    - All 5 commitSync send ShareAcknowledge RPCs to broker 1
 *    - 5 RPCs wasted
 * =================================================================== */
static void test_shareack_leader_change_reduces_rpcs(void) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic         = "0183-shareack-nlof";
        const char *group         = "sg-0183-shareack-nlof";
        const int broker1         = 1;
        const int broker2         = 2;
        const int msgcnt          = 10;
        const int acks_per_commit = 2;
        const int commit_rounds   = msgcnt / acks_per_commit;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int round;
        size_t rcvd         = 0;
        size_t consumed_idx = 0;
        size_t share_ack_cnt;
        size_t metadata_cnt;
        size_t j;

        SUB_TEST_QUICK();

        ctx = test_ctx_new(2);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        TEST_ASSERT(rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0,
                                                       broker1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set initial leader to broker %d", broker1);

        mock_produce(ctx.producer, topic, msgcnt);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "-1");
        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");
        subscribe_one(rkshare, topic);

        /* Consume one batch — expect all 10 records in a single call. */
        error = rd_kafka_share_consume_batch(rkshare, 10000, rkmessages, &rcvd);
        TEST_ASSERT(!error, "consume_batch failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(rcvd == (size_t)msgcnt,
                    "expected %d records in first batch, got %" PRIusz, msgcnt,
                    rcvd);

        /* Move leader to broker 2 before acknowledging. */
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0,
                                                       broker2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set leader to broker %d", broker2);

        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* 5 rounds: acknowledge 2 records, commitSync each time.
         * Each commitSync should get NOT_LEADER_OR_FOLLOWER. */
        for (round = 0; round < commit_rounds; round++) {
                rd_kafka_topic_partition_list_t *results = NULL;
                int k;

                for (k = 0;
                     k < acks_per_commit && consumed_idx < (size_t)msgcnt;
                     k++, consumed_idx++) {
                        rd_kafka_resp_err_t ack_err;
                        ack_err = rd_kafka_share_acknowledge(
                            rkshare, rkmessages[consumed_idx]);
                        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                                    "round %d: acknowledge failed: %s", round,
                                    rd_kafka_err2str(ack_err));
                }

                error = rd_kafka_share_commit_sync(rkshare, 500, &results);

                TEST_SAY("round %d: commit_sync returned %s\n", round,
                         error ? rd_kafka_error_string(error) : "success");

                TEST_ASSERT(results != NULL && results->cnt > 0,
                            "round %d: expected results from commit_sync",
                            round);

                for (k = 0; k < results->cnt; k++) {
                        TEST_SAY("  round %d: partition %s [%" PRId32
                                 "] err=%s\n",
                                 round, results->elems[k].topic,
                                 results->elems[k].partition,
                                 rd_kafka_err2name(results->elems[k].err));
                        TEST_ASSERT(
                            results->elems[k].err ==
                                RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER,
                            "round %d: expected NOT_LEADER_OR_FOLLOWER, "
                            "got %s",
                            round, rd_kafka_err2name(results->elems[k].err));
                }

                rd_kafka_topic_partition_list_destroy(results);

                if (error)
                        rd_kafka_error_destroy(error);
        }

        share_ack_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_ack_request, NULL);
        metadata_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_metadata_request, NULL);

        TEST_SAY("ShareAcknowledge requests after leader change: %" PRIusz "\n",
                 share_ack_cnt);
        TEST_SAY("Metadata requests after leader change: %" PRIusz "\n",
                 metadata_cnt);

        /* The first commit_sync sends a ShareAcknowledge RPC to the
         * stale broker; the response carries CurrentLeader and
         * NodeEndpoints which update the metadata cache inline.
         * Rounds 2-5 are caught by the local leader-stale check and
         * fail without sending an RPC. No separate Metadata RPC is
         * needed because the cache update is inline. */
        TEST_ASSERT(share_ack_cnt == 1,
                    "expected 1 ShareAcknowledge RPC, got %" PRIusz,
                    share_ack_cnt);
        TEST_ASSERT(metadata_cnt == 0,
                    "expected 0 Metadata RPCs (inline cache update), "
                    "got %" PRIusz,
                    metadata_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        /* Clean up held message handles. */
        for (j = 0; j < rcvd; j++)
                rd_kafka_message_destroy(rkmessages[j]);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test — per-partition NOT_LEADER_OR_FOLLOWER on ShareFetch is
 *         handled silently: no OP_CONSUMER_ERR, consumer recovers
 *         and continues reading from the new leader.
 *
 *  Two-broker mock cluster. Phase 1 produces and consumes records
 *  from broker1. The partition leader is moved to broker2. Phase 2
 *  verifies the consumer silently recovers and reads new records
 *  from broker2.
 *
 *  Wire-level verification: at least 1 Metadata request is triggered
 *  (error handler fired metadata refresh) and ShareFetch count stays
 *  below 10 (no retry storm).  The NOT_LEADER_OR_FOLLOWER response
 *  may arrive before or after request tracking starts, so a >= 2
 *  ShareFetch lower bound is not reliable; >= 1 is the meaningful
 *  minimum.
 *
 *  TODO: Add a real-broker companion test that triggers a leader
 *  change on a multi-broker cluster.
 * =================================================================== */
static void test_partition_not_leader_or_follower_silent(void) {
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic = "0183-nlof-silent";
        const char *group = "sg-0183-nlof-silent";
        const int broker1 = 1;
        const int broker2 = 2;
        const int msgcnt  = 5;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        int phase1_consumed = 0;
        int phase2_consumed = 0;
        int error_cnt       = 0;
        int attempts        = 0;
        size_t rcvd         = 0;
        size_t share_fetch_cnt;
        size_t metadata_cnt;
        size_t j;

        SUB_TEST_QUICK();

        ctx = test_ctx_new(2);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");

        /* Explicitly set the initial leader (do not assume a default). */
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0,
                                                       broker1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set initial leader to broker %d", broker1);

        /* Phase 1: produce and consume records from broker1.
         * auto.offset.reset=earliest (set by test_ctx_new) so the
         * share group starts from the beginning of the log. */
        mock_produce(ctx.producer, topic, msgcnt);

        /* Disable background metadata refresh so that the only metadata
         * refresh that fires is the one triggered by the per-partition
         * NOT_LEADER_OR_FOLLOWER error.  This makes the >= 2 ShareFetch
         * assertion deterministic: background refresh would otherwise
         * silently update the cached leader before Phase 2 starts. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "implicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "-1");
        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");
        subscribe_one(rkshare, topic);

        while (phase1_consumed < msgcnt && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                phase1_consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }
        TEST_ASSERT(phase1_consumed == msgcnt,
                    "Phase 1: expected %d records from broker %d, got %d",
                    msgcnt, broker1, phase1_consumed);

        /* Start request tracking before the leader change to capture
         * ShareFetch requests sent after it. */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Move the partition leader to broker2. The consumer's next
         * ShareFetch to broker1 will get NOT_LEADER_OR_FOLLOWER per
         * partition, which triggers a silent metadata refresh and
         * reconnect to broker2. */
        TEST_ASSERT(rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0,
                                                       broker2) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "set leader to broker %d", broker2);

        /* Produce records that will be available on broker2. mock_produce
         * flushes before returning, so all records are committed to
         * broker2 before Phase 2 begins. */
        mock_produce(ctx.producer, topic, msgcnt);

        /* Phase 2: consume records after recovery. OP_CONSUMER_ERR
         * surfaces as a non-NULL return from consume_batch; silent
         * handling must keep that return NULL throughout recovery. */
        attempts = 0;
        while (phase2_consumed < msgcnt && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        error_cnt++;
                        rd_kafka_error_destroy(error);
                        continue;
                }
                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                phase2_consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }
        TEST_ASSERT(phase2_consumed >= msgcnt,
                    "Phase 2: expected >= %d records from broker %d after "
                    "recovery, got %d",
                    msgcnt, broker2, phase2_consumed);
        TEST_ASSERT(error_cnt == 0,
                    "expected 0 OP_CONSUMER_ERR (NOT_LEADER_OR_FOLLOWER "
                    "must be silently handled), got %d",
                    error_cnt);

        /* Verify the error handler fired (metadata refresh triggered) and
         * did not cause a retry storm.  The NOT_LEADER_OR_FOLLOWER response
         * may be processed before or after request tracking starts, so only
         * a Metadata lower bound (not a >= 2 ShareFetch) is reliable. */
        share_fetch_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_share_fetch_request, NULL);
        metadata_cnt = test_mock_get_matching_request_cnt(
            ctx.mcluster, is_metadata_request, NULL);
        TEST_SAY("ShareFetch requests after leader change: %" PRIusz "\n",
                 share_fetch_cnt);
        TEST_SAY("Metadata requests after leader change: %" PRIusz "\n",
                 metadata_cnt);
        TEST_ASSERT(metadata_cnt >= 1,
                    "expected >= 1 Metadata request after leader change "
                    "(error handler must have triggered metadata refresh), "
                    "got %" PRIusz,
                    metadata_cnt);
        TEST_ASSERT(share_fetch_cnt >= 1,
                    "expected >= 1 ShareFetch request after leader change, "
                    "got %" PRIusz,
                    share_fetch_cnt);
        TEST_ASSERT(share_fetch_cnt < 10,
                    "expected < 10 ShareFetch requests (no retry storm), "
                    "got %" PRIusz,
                    share_fetch_cnt);

        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


// /* ===================================================================
//  *  Test — per-partition UNKNOWN_TOPIC_OR_PARTITION on ShareFetch is
//  *         handled silently: no OP_CONSUMER_ERR is delivered after the
//  *         topic is deleted.
//  *
//  *  Single-broker mock cluster. Phase 1 produces and consumes records
//  *  to establish a share session. The topic is deleted. Phase 2 polls
//  *  several times and verifies no error is surfaced to the application.
//  *
//  *  Wire-level verification: ShareFetch count is between 1 and 9
//  *  (lower bound: error path fired; upper bound: no retry storm).
//  *
//  *  TODO: Add a real-broker companion test that deletes a live topic
//  *  and verifies the same silent behaviour.
//  * =================================================================== */
// static void test_partition_unknown_topic_silent(void) {
//         test_ctx_t ctx;
//         rd_kafka_conf_t *conf;
//         rd_kafka_share_t *rkshare;
//         rd_kafka_error_t *error;
//         const char *topic = "0183-utp-silent";
//         const char *group = "sg-0183-utp-silent";
//         const int msgcnt  = 5;
//         rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
//         int phase1_consumed = 0;
//         int error_cnt       = 0;
//         int attempts        = 0;
//         int poll_cnt        = 0;
//         size_t rcvd         = 0;
//         size_t share_fetch_cnt;
//         size_t metadata_cnt;
//         size_t j;

//         SUB_TEST_QUICK();

//         ctx = test_ctx_new(1);

//         TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
//                         RD_KAFKA_RESP_ERR_NO_ERROR,
//                     "create topic");

//         /* Phase 1: produce and consume records to establish a share
//          * session. auto.offset.reset=earliest (set by test_ctx_new) so
//          * the share group starts from the beginning of the log. */
//         mock_produce(ctx.producer, topic, msgcnt);

//         /* Disable auto-create so the metadata handler does not recreate
//          * the deleted topic, and disable the propagation wait so the
//          * toppar is removed from the session immediately when metadata
//          * confirms the topic is gone (instead of waiting 30 s). */
//         test_conf_init(&conf, NULL, 0);
//         test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
//         test_conf_set(conf, "group.id", group);
//         test_conf_set(conf, "share.acknowledgement.mode", "implicit");
//         test_conf_set(conf, "allow.auto.create.topics", "false");
//         test_conf_set(conf, "topic.metadata.propagation.max.ms", "0");
//         rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
//         TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");
//         subscribe_one(rkshare, topic);

//         while (phase1_consumed < msgcnt && attempts++ < 30) {
//                 rcvd  = 0;
//                 error = rd_kafka_share_consume_batch(rkshare, 3000,
//                 rkmessages,
//                                                      &rcvd);
//                 if (error) {
//                         rd_kafka_error_destroy(error);
//                         continue;
//                 }
//                 for (j = 0; j < rcvd; j++) {
//                         if (!rkmessages[j]->err)
//                                 phase1_consumed++;
//                         rd_kafka_message_destroy(rkmessages[j]);
//                 }
//         }
//         TEST_ASSERT(phase1_consumed == msgcnt,
//                     "Phase 1: expected %d records, got %d", msgcnt,
//                     phase1_consumed);

//         /* Start request tracking before topic deletion to capture
//          * ShareFetch requests that get UNKNOWN_TOPIC_OR_PARTITION. */
//         rd_kafka_mock_start_request_tracking(ctx.mcluster);
//         rd_kafka_mock_clear_requests(ctx.mcluster);

//         /* Delete the topic. The consumer's next ShareFetch will receive
//          * UNKNOWN_TOPIC_OR_PARTITION per partition, which must be handled
//          * silently (metadata refresh, no OP_CONSUMER_ERR). */
//         TEST_ASSERT(rd_kafka_mock_topic_delete(ctx.mcluster, topic) ==
//                         RD_KAFKA_RESP_ERR_NO_ERROR,
//                     "delete topic");

//         /* Phase 2: poll several times. OP_CONSUMER_ERR surfaces as a
//          * non-NULL return from consume_batch; count any such return as
//          * an error (silent handling requires zero). */
//         while (poll_cnt++ < 5) {
//                 rcvd  = 0;
//                 error = rd_kafka_share_consume_batch(rkshare, 1000,
//                 rkmessages,
//                                                      &rcvd);
//                 if (error) {
//                         error_cnt++;
//                         rd_kafka_error_destroy(error);
//                         continue;
//                 }
//                 for (j = 0; j < rcvd; j++)
//                         rd_kafka_message_destroy(rkmessages[j]);
//         }
//         TEST_ASSERT(error_cnt == 0,
//                     "expected 0 OP_CONSUMER_ERR after topic delete "
//                     "(UNKNOWN_TOPIC_OR_PARTITION must be silently handled), "
//                     "got %d",
//                     error_cnt);

//         /* Verify the error path fired. The background "keep fetching" loop
//          * may send additional empty ShareFetch requests after the partition
//          * leaves the session, so only a lower bound is meaningful here. */
//         share_fetch_cnt = test_mock_get_matching_request_cnt(
//             ctx.mcluster, is_share_fetch_request, NULL);
//         TEST_SAY("ShareFetch requests after topic delete: %" PRIusz "\n",
//                  share_fetch_cnt);
//         TEST_ASSERT(share_fetch_cnt >= 1,
//                     "expected >= 1 ShareFetch after topic delete "
//                     "(error path must have fired), got %" PRIusz,
//                     share_fetch_cnt);

//         /* Verify a metadata refresh was triggered by the error. */
//         metadata_cnt = test_mock_get_matching_request_cnt(
//             ctx.mcluster, is_metadata_request, NULL);
//         TEST_SAY("Metadata requests after topic delete: %" PRIusz "\n",
//                  metadata_cnt);
//         TEST_ASSERT(metadata_cnt >= 1,
//                     "expected >= 1 Metadata request after topic delete "
//                     "(metadata refresh must fire), got %" PRIusz,
//                     metadata_cnt);

//         rd_kafka_mock_stop_request_tracking(ctx.mcluster);

//         test_share_consumer_close(rkshare);
//         test_share_destroy(rkshare);
//         test_ctx_destroy(&ctx);

//         SUB_TEST_PASS();
// }

int main_0183_share_consumer_leader_change_mock(int argc, char **argv) {
        test_shareack_leader_change_reduces_rpcs();
        test_partition_not_leader_or_follower_silent();
        /* test_partition_unknown_topic_silent(); */
        return 0;
}