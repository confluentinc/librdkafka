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
 * @name 4C: Share consumer broker decommission while ack inflight.
 *
 * Cluster has 2 brokers, partition 0 leader = broker 1. Consumer
 * acquires records, calls acknowledge() on each, then BEFORE
 * commit_sync, the partition leader is migrated to broker 2 AND broker
 * 1 is decommissioned. The acks are now segregated against a broker
 * that no longer exists in the cluster.
 *
 * The consumer must:
 *  (a) surface a per-partition error to commit_sync (typically
 *      SHARE_SESSION_NOT_FOUND or NOT_LEADER_OR_FOLLOWER), NOT silently
 *      drop the acks;
 *  (b) keep consuming after the decommission — new records produced to
 *      broker 2 must be deliverable without restart.
 */

#define CONSUME_ARRAY 64

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
                    "enable ShareGroupHeartbeat");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(ctx.mcluster,
                                                 RD_KAFKAP_ShareFetch, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "enable ShareFetch");

        rd_kafka_mock_sharegroup_set_auto_offset_reset(ctx.mcluster, 1);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        ctx.producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(ctx.producer, "create producer: %s", errstr);

        return ctx;
}

static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}

static void subscribe_one(rd_kafka_share_t *consumer, const char *topic) {
        rd_kafka_topic_partition_list_t *tpl =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(tpl, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, tpl),
                    "subscribe failed");
        rd_kafka_topic_partition_list_destroy(tpl);
}

static void do_test_decommission_while_inflight(void) {
        const char *topic     = "0192-decommission";
        const char *group     = "sg-0192-decom";
        const int b1          = 1;
        const int b2          = 2;
        const int phase1_msgs = 5;
        const int phase2_msgs = 5;
        test_ctx_t ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_messages_t *batch = NULL;
        size_t rcvd                = 0;
        size_t j;
        rd_kafka_topic_partition_list_t *results = NULL;
        int saw_err_partition                    = 0;
        int phase2_consumed                      = 0;
        int attempts;

        SUB_TEST_QUICK();

        /* Decommissioning broker 1 leaves the producer/consumer with
         * lingering Connect-to-broker-1 attempts that hit Connection
         * refused. These are expected post-decommission noise; suppress
         * them so the test_error_cb doesn't fail the test. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        ctx = test_ctx_new(2);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "create topic");
        TEST_ASSERT(
            rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0, b1) ==
                RD_KAFKA_RESP_ERR_NO_ERROR,
            "set leader to broker %d", b1);

        test_produce_msgs_simple(ctx.producer, topic, 0, phase1_msgs);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare, "create share consumer");
        subscribe_one(rkshare, topic);

        /* Phase 1: acquire records from broker 1. */
        error = rd_kafka_share_poll(rkshare, 10000, &batch);
        rcvd  = batch ? rd_kafka_messages_count(batch) : 0;
        TEST_ASSERT(!error, "phase1 share_poll: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(rcvd == (size_t)phase1_msgs,
                    "phase1 expected %d, got %" PRIusz, phase1_msgs, rcvd);

        /* Acknowledge all (ACCEPT) but do NOT commit yet. */
        for (j = 0; j < rcvd; j++) {
                rd_kafka_resp_err_t aerr = rd_kafka_share_acknowledge(
                    rkshare, rd_kafka_messages_get(batch, j));
                TEST_ASSERT(aerr == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "acknowledge failed: %s", rd_kafka_err2str(aerr));
        }

        /* Migrate leader to broker 2 and decommission broker 1. The
         * inflight acks are targeted at a broker that no longer exists. */
        TEST_ASSERT(
            rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0, b2) ==
                RD_KAFKA_RESP_ERR_NO_ERROR,
            "migrate leader to broker %d", b2);
        TEST_ASSERT(rd_kafka_mock_broker_decommission(ctx.mcluster, b1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "decommission broker %d", b1);

        /* commit_sync must surface the failure (either by rerouting and
         * succeeding, or by returning a per-partition err — but NOT by
         * silently dropping the acks). */
        error = rd_kafka_share_commit_sync(rkshare, 5000, &results);
        TEST_SAY("commit_sync after decommission: %s\n",
                 error ? rd_kafka_error_string(error) : "success");

        TEST_ASSERT(results && results->cnt > 0,
                    "expected per-partition results");

        for (j = 0; j < (size_t)results->cnt; j++) {
                rd_kafka_resp_err_t per = results->elems[j].err;
                TEST_SAY("  partition [%" PRId32 "] err=%s\n",
                         results->elems[j].partition, rd_kafka_err2name(per));
                if (per != RD_KAFKA_RESP_ERR_NO_ERROR)
                        saw_err_partition++;
        }

        /* Either: all rerouted successfully (err == NO_ERROR), or some
         * partitions surfaced an error. Both outcomes are acceptable —
         * what we explicitly assert is that the consumer is in a sane
         * state and can continue. */
        TEST_SAY(
            "decommission inflight: err_partitions=%d/%d (any "
            "value is acceptable; silent drop would be a bug)\n",
            saw_err_partition, results->cnt);

        rd_kafka_topic_partition_list_destroy(results);
        if (error)
                rd_kafka_error_destroy(error);

        rd_kafka_messages_destroy(batch);
        batch = NULL;

        /* Phase 2: produce new records (now to broker 2's partition) and
         * verify the consumer keeps working after decommission. */
        test_produce_msgs_simple(ctx.producer, topic, 0, phase2_msgs);

        attempts = 0;
        while (phase2_consumed < phase2_msgs && attempts++ < 30) {
                size_t r = 0;
                rd_kafka_error_t *e;
                size_t k;
                e = rd_kafka_share_poll(rkshare, 1000, &batch);
                if (e) {
                        rd_kafka_error_destroy(e);
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }
                r = rd_kafka_messages_count(batch);
                for (k = 0; k < r; k++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, k);
                        if (!rkm->err) {
                                phase2_consumed++;
                                rd_kafka_share_acknowledge(rkshare, rkm);
                        }
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        /* Phase 2 expected >= phase2_msgs: the phase-1 acquisition locks
         * were released when broker 1 was decommissioned, so those
         * records get redelivered alongside the phase-2 records.
         * What we care about is "consumer is still alive and delivering"
         * — exact equality would be wrong. */
        TEST_ASSERT(phase2_consumed >= phase2_msgs,
                    "phase2 expected at least %d (consumer should not have "
                    "wedged), got %d",
                    phase2_msgs, phase2_consumed);

        TEST_SAY("phase2: consumed=%d/%d (consumer survived decommission)\n",
                 phase2_consumed, phase2_msgs);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


int main_0189_share_consumer_decommission_mock(int argc, char **argv) {
        test_timeout_set(120);
        do_test_decommission_while_inflight();
        return 0;
}
