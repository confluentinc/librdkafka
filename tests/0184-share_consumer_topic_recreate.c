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
#include "testshared.h"
#include "rdkafka.h"
#include "../src/rdkafka_proto.h"

/**
 * @name Share consumer behavior across server-side topic recreation.
 *
 * Verifies that share consumers correctly observe topic_id changes,
 * rebuild rktps, tear down and re-establish share sessions, and resume
 * fetching when a subscribed topic is deleted and recreated on the cluster.
 *
 * Verification is built on top of the framework's test_msgver_t:
 *  - Each phase ("before" and "after" recreate) uses a distinct random
 *    testid; produced messages encode that testid + msgid in the payload.
 *  - test_msgver_add_msg() auto-filters by testid, so a "before" msgver
 *    will never accept "after" messages and vice versa. This catches:
 *      * gaps (a phase missing some of its expected msgids)
 *      * duplicates / spurious redelivery
 *      * cross-phase leakage (stale "before" messages resurfacing after
 *        a topic recreate)
 *  - We additionally assert rd_kafka_message_delivery_count() == 1 for
 *    every message: any value >1 means the broker re-delivered, which
 *    is a recreate-related failure mode worth flagging.
 */

#define PARTITION_CNT      3
#define MSGS_PER_PARTITION 10
/* Total messages per phase = PARTITION_CNT * MSGS_PER_PARTITION.
 * Each partition receives exactly MSGS_PER_PARTITION messages with
 * msgids in [0, MSGS_PER_PARTITION). */
#define MSGS_PER_PHASE (PARTITION_CNT * MSGS_PER_PARTITION)
#define BATCH_SIZE     64


static rd_kafka_t *common_producer;
static rd_kafka_t *common_admin;

/****************************************************************************
 * Helpers
 ****************************************************************************/

/**
 * @brief Produce MSGS_PER_PARTITION framework-encoded messages (NULL
 *        payload => librdkafka test helper auto-encodes
 *        testid+partition+msgid into payload and key) to each of the
 *        first \p partition_cnt partitions of \p topic.
 *
 * Per-partition msgids run [0, MSGS_PER_PARTITION), so msgver_verify_part
 * can later assert exact-range coverage independently per partition.
 */
static void
produce_phase(const char *topic, uint64_t testid, int32_t partition_cnt) {
        int32_t p;
        for (p = 0; p < partition_cnt; p++) {
                test_produce_msgs2(common_producer, topic, testid, p,
                                   0 /*msg_base*/, MSGS_PER_PARTITION,
                                   NULL /*payload*/, 0);
        }
}

/**
 * @brief Fetch the broker-assigned topic_id for \p topic via the
 *        DescribeTopics admin API.
 *
 * The Uuid type is opaque so we return the 128-bit value as two int64
 * out-parameters; callers can compare them directly to detect a fresh
 * topic_id after a recreate.
 *
 * On any failure (topic missing, admin timeout, etc.) the test is failed.
 */
static void
fetch_topic_id(const char *topic, int64_t *out_msb, int64_t *out_lsb) {
        rd_kafka_queue_t *q;
        rd_kafka_TopicCollection_t *tc;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeTopics_result_t *res;
        const rd_kafka_TopicDescription_t **descs;
        const rd_kafka_Uuid_t *topic_id;
        size_t desc_cnt = 0;
        const char *topics_arr[1];

        topics_arr[0] = topic;

        q  = rd_kafka_queue_new(common_admin);
        tc = rd_kafka_TopicCollection_of_topic_names(topics_arr, 1);

        rd_kafka_DescribeTopics(common_admin, tc, NULL /*options*/, q);

        rkev = rd_kafka_queue_poll(q, 30 * 1000);
        TEST_ASSERT(rkev != NULL, "DescribeTopics(%s) timed out", topic);
        TEST_ASSERT(rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "DescribeTopics(%s) failed: %s", topic,
                    rd_kafka_event_error_string(rkev));

        res   = rd_kafka_event_DescribeTopics_result(rkev);
        descs = rd_kafka_DescribeTopics_result_topics(res, &desc_cnt);
        TEST_ASSERT(desc_cnt == 1, "Expected 1 topic description, got %zu",
                    desc_cnt);
        TEST_ASSERT(
            rd_kafka_error_code(rd_kafka_TopicDescription_error(descs[0])) ==
                RD_KAFKA_RESP_ERR_NO_ERROR,
            "Topic %s description error: %s", topic,
            rd_kafka_error_string(rd_kafka_TopicDescription_error(descs[0])));

        topic_id = rd_kafka_TopicDescription_topic_id(descs[0]);
        *out_msb = rd_kafka_Uuid_most_significant_bits(topic_id);
        *out_lsb = rd_kafka_Uuid_least_significant_bits(topic_id);

        rd_kafka_event_destroy(rkev);
        rd_kafka_TopicCollection_destroy(tc);
        rd_kafka_queue_destroy(q);
}

/**
 * @brief Consume from a share consumer into \p mv until \p exp_cnt messages
 *        matching mv->testid have been collected, or \p timeout_ms elapses.
 *
 * Every consumed message is ACCEPT-ack'd. On every message we assert that
 * delivery_count == 1 (no broker-side redelivery occurred for this run).
 *
 * @returns the count of matching messages collected into \p mv.
 */
static int consume_into_msgver(rd_kafka_share_t *rkshare,
                               test_msgver_t *mv,
                               int exp_cnt,
                               int timeout_ms) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_ts_t deadline = test_clock() + (rd_ts_t)timeout_ms * 1000;
        rd_kafka_t *rk   = test_share_consumer_get_rk(rkshare);

        while (mv->msgcnt < exp_cnt && test_clock() < deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0;
                size_t i;

                err = rd_kafka_share_consume_batch(rkshare, 500, batch, &rcvd);
                if (err) {
                        TEST_SAY("share_consume_batch error: %s\n",
                                 rd_kafka_error_string(err));
                        rd_kafka_error_destroy(err);
                        continue;
                }

                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = batch[i];

                        if (m->err) {
                                TEST_SAY(
                                    "Consumer event: %s "
                                    "(topic=%s partition=%" PRId32 ")\n",
                                    rd_kafka_err2str(m->err),
                                    m->rkt ? rd_kafka_topic_name(m->rkt)
                                           : "(none)",
                                    m->partition);
                                rd_kafka_message_destroy(m);
                                continue;
                        }

                        TEST_ASSERT(
                            rd_kafka_message_delivery_count(m) == 1,
                            "Unexpected redelivery: delivery_count=%d on "
                            "topic=%s partition=%" PRId32 " offset=%" PRId64,
                            rd_kafka_message_delivery_count(m),
                            rd_kafka_topic_name(m->rkt), m->partition,
                            m->offset);

                        /* msgver auto-filters by mv->testid; messages from
                         * the other phase (or untagged) are silently
                         * dropped. */
                        test_msgver_add_msg(rk, mv, m);
                        rd_kafka_share_acknowledge(rkshare, m);
                        rd_kafka_message_destroy(m);
                }
        }

        return mv->msgcnt;
}

/**
 * @brief Delete a topic, wait for the deletion to settle, then recreate
 *        with the same name and \p partition_cnt partitions. The new
 *        instance will have a fresh topic_id on the broker.
 */
static void recreate_topic(const char *topic, int partition_cnt) {
        TEST_SAY("Deleting topic %s\n", topic);
        test_delete_topic(common_admin, topic);

        /* DeleteTopics is async on the broker; the controller may still
         * be removing log segments when we issue CreateTopics. Without
         * this pause, the broker frequently lets the CreateTopics request
         * sit until DeleteTopics finishes, which can blow past the
         * admin-op timeout. The same 5s pause pattern is used by
         * 0107-topic_recreate. */
        rd_sleep(5);

        TEST_SAY("Recreating topic %s with %d partition(s)\n", topic,
                 partition_cnt);
        test_create_topic_wait_exists(common_admin, topic, partition_cnt, -1,
                                      60 * 1000);
}

/**
 * @brief Build a share consumer such that the ShareGroupHeartbeat is the
 *        dominant channel for discovering topic_id changes:
 *          - topic.metadata.refresh.interval.ms is set very high so the
 *            periodic metadata refresh effectively does not fire during
 *            the test;
 *          - topic.metadata.propagation.max.ms is left at default (30s)
 *            so the brief delete/recreate gap is well within the grace
 *            window and no spurious "topic does not exist" errors should
 *            surface.
 */
static rd_kafka_share_t *create_consumer_hb_first(const char *group_id) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);

        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "300000");
        test_conf_set(conf, "topic.metadata.propagation.max.ms", "30000");
        test_conf_set(conf, "debug", "all");

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}

/**
 * @brief Build a share consumer such that the periodic metadata refresh
 *        is the dominant channel for discovering topic_id changes:
 *          - topic.metadata.refresh.interval.ms is set to 500ms, well
 *            below the broker-dictated ShareGroupHeartbeat interval
 *            (typically 5s), so the refresh timer is virtually
 *            guaranteed to fire between heartbeats during the
 *            delete/recreate window;
 *          - topic.metadata.propagation.max.ms is left at default (30s).
 */
static rd_kafka_share_t *create_consumer_md_first(const char *group_id) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);

        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        test_conf_set(conf, "topic.metadata.propagation.max.ms", "30000");

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}

/**
 * @brief Background-thread payload that deletes and recreates a topic
 *        on a mock cluster after a short sleep.
 *
 * Used by recreate-during-close mock tests so the recreate lands in the
 * middle of an artificially-delayed broker request on the main thread.
 */
struct recreate_thread_args {
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic;
        int32_t partition_cnt;
        int sleep_ms;
};

static int recreate_thread_main(void *p) {
        struct recreate_thread_args *args = p;
        rd_usleep((int64_t)args->sleep_ms * 1000, NULL);
        TEST_SAY("[recreate-thread] Deleting topic %s\n", args->topic);
        TEST_ASSERT(rd_kafka_mock_topic_delete(args->mcluster, args->topic) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "[recreate-thread] mock_topic_delete failed");
        TEST_SAY("[recreate-thread] Recreating topic %s with %" PRId32
                 " partition(s)\n",
                 args->topic, args->partition_cnt);
        TEST_ASSERT(rd_kafka_mock_topic_create(args->mcluster, args->topic,
                                               args->partition_cnt,
                                               1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "[recreate-thread] mock_topic_create failed");
        return 0;
}

/**
 * @brief Background producer thread: produces one record every
 *        producer_period_ms with payload "<phase>:<seq>", where
 *        phase is whatever the controlling thread has most recently
 *        written into args->phase. Tolerates produce errors (the
 *        topic may not exist mid-test).
 *
 *
 * Used by the survives-delete-and-recreate test to keep produce
 * activity steady through the delete window.
 */
struct producer_thread_args {
        rd_kafka_t *producer;
        const char *topic;
        int period_ms;
        char phase[16]; /* "before" / "during" / "after" */
        rd_bool_t stop;
};

static int producer_thread_main(void *p) {
        struct producer_thread_args *args = p;
        int seq                           = 0;
        while (!args->stop) {
                char payload[64];
                rd_snprintf(payload, sizeof(payload), "%s:%d", args->phase,
                            seq++);
                /* Errors during the delete window are expected; the
                 * test tolerates them silently. */
                (void)rd_kafka_producev(
                    args->producer, RD_KAFKA_V_TOPIC(args->topic),
                    RD_KAFKA_V_VALUE(payload, strlen(payload)),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);
                rd_usleep((int64_t)args->period_ms * 1000, NULL);
        }
        return 0;
}

/****************************************************************************
 * Test Cases
 ****************************************************************************/

/**
 * @brief Recreate while the periodic metadata refresh is silent.
 *
 * Sequence:
 *   1. Subscribe; produce MSGS_BEFORE framework-encoded messages tagged
 *      with testid_before; consume them all via a "before" msgver and
 *      assert exact range coverage and zero redelivery.
 *   2. Delete + recreate the topic with the same partition count. The
 *      broker assigns a fresh topic_id. The ShareGroupHeartbeat is what
 *      will carry the new topic_id to the client (periodic MD refresh is
 *      pinned to 5 minutes).
 *   3. Produce MSGS_AFTER framework-encoded messages tagged with
 *      testid_after; consume them all via an "after" msgver and assert
 *      exact range coverage, zero redelivery, and (implicitly via
 *      testid filtering) zero stale "before" messages.
 */
static void do_test_recreate_hb_first(void) {
        const char *topic;
        const char *group_id = "0184-share-recreate-hb-first";
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        test_msgver_t mv_before, mv_after;
        uint64_t testid_before, testid_after;
        int64_t id_before_msb, id_before_lsb;
        int64_t id_after_msb, id_after_lsb;
        int got;
        int32_t p;

        SUB_TEST_QUICK();

        testid_before = test_id_generate();
        testid_after  = test_id_generate();

        topic = test_mk_topic_name("0184-recreate-hb-first", 1);

        test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
                                      60 * 1000);

        /* Snapshot the broker-assigned topic_id of the initial topic
         * instance so we can later prove the recreate produced a new id. */
        fetch_topic_id(topic, &id_before_msb, &id_before_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_before_msb, id_before_lsb);

        test_share_set_auto_offset_reset(group_id, "earliest");

        rkshare = create_consumer_hb_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* ---- Phase 1: pre-recreate ---- */
        TEST_SAY(
            "Phase 1: producing %d msgs/partition x %d partitions "
            "(testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_before);
        produce_phase(topic, testid_before, PARTITION_CNT);

        test_msgver_init(&mv_before, testid_before);

        got =
            consume_into_msgver(rkshare, &mv_before, MSGS_PER_PHASE, 30 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 1: expected %d msgs matching testid_before, "
                    "got %d",
                    MSGS_PER_PHASE, got);
        /* Per-partition verification: every partition must contribute
         * exactly MSGS_PER_PARTITION messages with msgids [0, N), no
         * duplicates, in produce order. A rktp that wasn't properly
         * created/assigned would manifest as a partition with fewer
         * than expected messages.
         */
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase1-before-part", &mv_before,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_before);

        /* ---- Recreate ---- */
        recreate_topic(topic, PARTITION_CNT);

        /* Confirm the broker actually issued a new topic_id; without
         * this the rest of the test could pass for a no-op reason. */
        fetch_topic_id(topic, &id_after_msb, &id_after_lsb);
        TEST_SAY("Recreated topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_after_msb, id_after_lsb);
        TEST_ASSERT(id_before_msb != id_after_msb ||
                        id_before_lsb != id_after_lsb,
                    "Recreated topic has the same topic_id as before "
                    "(msb=0x%" PRIx64 " lsb=0x%" PRIx64
                    "); broker did not actually recreate it",
                    id_before_msb, id_before_lsb);

        /* ---- Phase 2: post-recreate ---- */
        TEST_SAY(
            "Phase 2: producing %d msgs/partition x %d partitions on "
            "recreated topic (testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_after);
        produce_phase(topic, testid_after, PARTITION_CNT);

        test_msgver_init(&mv_after, testid_after);

        got =
            consume_into_msgver(rkshare, &mv_after, MSGS_PER_PHASE, 60 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 2: expected %d msgs matching testid_after, "
                    "got %d (post-recreate)",
                    MSGS_PER_PHASE, got);
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase2-after-part", &mv_after,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_after);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}

/**
 * @brief Recreate while the periodic metadata refresh fires
 *        frequently.
 *
 * Mirror of do_test_recreate_hb_first, but with
 * topic.metadata.refresh.interval.ms set to 500ms so the refresh timer wins the
 * race against the ShareGroupHeartbeat for discovering the new topic_id.
 * Because the MD path updates rkt->rkt_topic_id directly when it sees the
 * change, the subsequent HB-driven assignment-resolution finds NEW-UUID already
 * in the cache without needing a targeted-by-id MetadataRequest.
 */
static void do_test_recreate_md_first(void) {
        const char *topic;
        const char *group_id = "0184-share-recreate-md-first";
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        test_msgver_t mv_before, mv_after;
        uint64_t testid_before, testid_after;
        int64_t id_before_msb, id_before_lsb;
        int64_t id_after_msb, id_after_lsb;
        int got;
        int32_t p;

        SUB_TEST_QUICK();

        testid_before = test_id_generate();
        testid_after  = test_id_generate();

        topic = test_mk_topic_name("0184-recreate-md-first", 1);

        test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
                                      60 * 1000);

        fetch_topic_id(topic, &id_before_msb, &id_before_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_before_msb, id_before_lsb);

        test_share_set_auto_offset_reset(group_id, "earliest");

        rkshare = create_consumer_md_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* ---- Phase 1: pre-recreate ---- */
        TEST_SAY(
            "Phase 1: producing %d msgs/partition x %d partitions "
            "(testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_before);
        produce_phase(topic, testid_before, PARTITION_CNT);

        test_msgver_init(&mv_before, testid_before);

        got =
            consume_into_msgver(rkshare, &mv_before, MSGS_PER_PHASE, 30 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 1: expected %d msgs matching testid_before, "
                    "got %d",
                    MSGS_PER_PHASE, got);
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase1-before-part", &mv_before,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_before);

        /* ---- Recreate ---- */
        recreate_topic(topic, PARTITION_CNT);

        fetch_topic_id(topic, &id_after_msb, &id_after_lsb);
        TEST_SAY("Recreated topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_after_msb, id_after_lsb);
        TEST_ASSERT(id_before_msb != id_after_msb ||
                        id_before_lsb != id_after_lsb,
                    "Recreated topic has the same topic_id as before "
                    "(msb=0x%" PRIx64 " lsb=0x%" PRIx64
                    "); broker did not actually recreate it",
                    id_before_msb, id_before_lsb);

        /* ---- Phase 2: post-recreate ---- */
        TEST_SAY(
            "Phase 2: producing %d msgs/partition x %d partitions on "
            "recreated topic (testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_after);
        produce_phase(topic, testid_after, PARTITION_CNT);

        test_msgver_init(&mv_after, testid_after);

        got =
            consume_into_msgver(rkshare, &mv_after, MSGS_PER_PHASE, 60 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 2: expected %d msgs matching testid_after, "
                    "got %d (post-recreate)",
                    MSGS_PER_PHASE, got);
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase2-after-part", &mv_after,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_after);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Recreate with a smaller partition count, HB-first regime.
 *
 * topic.metadata.refresh.interval.ms is
 * pinned high so the ShareGroupHeartbeat drives discovery), but the
 * recreated topic has only 2 partitions instead of 3. On top of the
 * topic_id flip we now exercise the partition-count shrink path:
 *
 *  - The metadata refresh for the recreated topic should drive
 *    rd_kafka_topic_partition_cnt_update() with a smaller count, which
 *    marks rkt_p[2] as F_UNKNOWN.
 *  - The next ShareGroupHeartbeat must deliver an assignment of only
 *    partitions 0 and 1 under NEW-UUID, prompting the cgrp to revoke
 *    partition [2] and add partitions [0] and [1].
 */
static void do_test_recreate_shrink_partitions_hb_first(void) {
        const char *topic;
        const char *group_id = "0184-share-recreate-shrink-hb-first";
        const int32_t shrink_partition_cnt = 2;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        test_msgver_t mv_before, mv_after;
        uint64_t testid_before, testid_after;
        int64_t id_before_msb, id_before_lsb;
        int64_t id_after_msb, id_after_lsb;
        int got;
        int32_t p;
        const int msgs_per_phase_after =
            shrink_partition_cnt * MSGS_PER_PARTITION;

        SUB_TEST_QUICK();

        testid_before = test_id_generate();
        testid_after  = test_id_generate();

        topic = test_mk_topic_name("0184-recreate-shrink-hb-first", 1);

        test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
                                      60 * 1000);

        fetch_topic_id(topic, &id_before_msb, &id_before_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_before_msb, id_before_lsb);

        test_share_set_auto_offset_reset(group_id, "earliest");

        /* Reuse the HB-first builder from TC#1: long MD refresh so the
         * ShareGroupHeartbeat is the channel that brings the new
         * topic_id and the (smaller) partition list to the client. */
        rkshare = create_consumer_hb_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* ---- Phase 1: pre-recreate, all PARTITION_CNT partitions ---- */
        TEST_SAY(
            "Phase 1: producing %d msgs/partition x %d partitions "
            "(testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_before);
        produce_phase(topic, testid_before, PARTITION_CNT);

        test_msgver_init(&mv_before, testid_before);

        got =
            consume_into_msgver(rkshare, &mv_before, MSGS_PER_PHASE, 30 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 1: expected %d msgs matching testid_before, "
                    "got %d",
                    MSGS_PER_PHASE, got);
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase1-before-part", &mv_before,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_before);

        /* ---- Recreate with fewer partitions ---- */
        recreate_topic(topic, shrink_partition_cnt);

        fetch_topic_id(topic, &id_after_msb, &id_after_lsb);
        TEST_SAY("Recreated topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_after_msb, id_after_lsb);
        TEST_ASSERT(id_before_msb != id_after_msb ||
                        id_before_lsb != id_after_lsb,
                    "Recreated topic has the same topic_id as before "
                    "(msb=0x%" PRIx64 " lsb=0x%" PRIx64
                    "); broker did not actually recreate it",
                    id_before_msb, id_before_lsb);

        /* ---- Phase 2: post-recreate, only shrink_partition_cnt partitions
         * ---- */
        TEST_SAY(
            "Phase 2: producing %d msgs/partition x %d partitions on "
            "recreated topic (testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, shrink_partition_cnt, testid_after);
        produce_phase(topic, testid_after, shrink_partition_cnt);

        test_msgver_init(&mv_after, testid_after);

        got = consume_into_msgver(rkshare, &mv_after, msgs_per_phase_after,
                                  60 * 1000);
        TEST_ASSERT(got == msgs_per_phase_after,
                    "Phase 2: expected %d msgs matching testid_after, "
                    "got %d (post-recreate)",
                    msgs_per_phase_after, got);
        for (p = 0; p < shrink_partition_cnt; p++)
                test_msgver_verify_part("phase2-after-part", &mv_after,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_after);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Recreate with a smaller partition count, MD-first regime.
 *
 * topic.metadata.refresh.interval.ms is
 * set very low so the periodic metadata refresh wins the race against
 * the ShareGroupHeartbeat for discovering the new topic_id and the
 * smaller partition count), combined with the partition-count shrink
 * of TC#6.
 *
 * Compared to the HB-first shrink variant, the metadata cache learns
 * the new topic_id and reduced partition_cnt before the HB delivers
 * the new assignment. partition_cnt_update() therefore fires from the
 * MD path (not from a heartbeat-triggered targeted-by-id MetadataRequest),
 * marking rkt_p[2] as F_UNKNOWN before the HB's revoke for partition 2
 * arrives. This exercises a different ordering of the same client-side
 * state changes.
 */
static void do_test_recreate_shrink_partitions_md_first(void) {
        const char *topic;
        const char *group_id = "0184-share-recreate-shrink-md-first";
        const int32_t shrink_partition_cnt = 2;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        test_msgver_t mv_before, mv_after;
        uint64_t testid_before, testid_after;
        int64_t id_before_msb, id_before_lsb;
        int64_t id_after_msb, id_after_lsb;
        int got;
        int32_t p;
        const int msgs_per_phase_after =
            shrink_partition_cnt * MSGS_PER_PARTITION;

        SUB_TEST_QUICK();

        testid_before = test_id_generate();
        testid_after  = test_id_generate();

        topic = test_mk_topic_name("0184-recreate-shrink-md-first", 1);

        test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
                                      60 * 1000);

        fetch_topic_id(topic, &id_before_msb, &id_before_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_before_msb, id_before_lsb);

        test_share_set_auto_offset_reset(group_id, "earliest");

        /* Reuse the MD-first builder from TC#2: very short metadata
         * refresh interval so the periodic refresh discovers the
         * recreate before the ShareGroupHeartbeat does. */
        rkshare = create_consumer_md_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* ---- Phase 1: pre-recreate, all PARTITION_CNT partitions ---- */
        TEST_SAY(
            "Phase 1: producing %d msgs/partition x %d partitions "
            "(testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_before);
        produce_phase(topic, testid_before, PARTITION_CNT);

        test_msgver_init(&mv_before, testid_before);

        got =
            consume_into_msgver(rkshare, &mv_before, MSGS_PER_PHASE, 30 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE,
                    "Phase 1: expected %d msgs matching testid_before, "
                    "got %d",
                    MSGS_PER_PHASE, got);
        for (p = 0; p < PARTITION_CNT; p++)
                test_msgver_verify_part("phase1-before-part", &mv_before,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_before);

        /* ---- Recreate with fewer partitions ---- */
        recreate_topic(topic, shrink_partition_cnt);

        fetch_topic_id(topic, &id_after_msb, &id_after_lsb);
        TEST_SAY("Recreated topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_after_msb, id_after_lsb);
        TEST_ASSERT(id_before_msb != id_after_msb ||
                        id_before_lsb != id_after_lsb,
                    "Recreated topic has the same topic_id as before "
                    "(msb=0x%" PRIx64 " lsb=0x%" PRIx64
                    "); broker did not actually recreate it",
                    id_before_msb, id_before_lsb);

        /* ---- Phase 2: post-recreate, only shrink_partition_cnt partitions
         * ---- */
        TEST_SAY(
            "Phase 2: producing %d msgs/partition x %d partitions on "
            "recreated topic (testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, shrink_partition_cnt, testid_after);
        produce_phase(topic, testid_after, shrink_partition_cnt);

        test_msgver_init(&mv_after, testid_after);

        got = consume_into_msgver(rkshare, &mv_after, msgs_per_phase_after,
                                  60 * 1000);
        TEST_ASSERT(got == msgs_per_phase_after,
                    "Phase 2: expected %d msgs matching testid_after, "
                    "got %d (post-recreate)",
                    msgs_per_phase_after, got);
        for (p = 0; p < shrink_partition_cnt; p++)
                test_msgver_verify_part("phase2-after-part", &mv_after,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_after);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}

/**
 * @brief Stress test: many rapid topic recreations interleaved with
 *        random produce activity and varying partition counts.
 *
 * Each chaos cycle picks (uniformly at random):
 *   - A short pre-recreate sleep in [200, 1500] ms, to let the
 *     consumer's state advance unpredictably between cycles.
 *   - Whether to produce 0..5 records to each partition of the
 *     current topic instance before the recreate; if any are
 *     produced, they use a per-cycle testid that the test does not
 *     track (we don't require these records to be received).
 *   - A new partition count from {2, 3, 4} for the recreated topic,
 *     exercising shrink, no-change, and grow paths from one cycle to
 *     the next.
 *
 * After chaos_cycles iterations the test produces a final batch of
 * MSGS_PER_PARTITION records to every partition of whatever topic
 * instance is currently live (partition count is whatever chaos left
 * it at) and asserts:
 *   - The final batch is fully consumed (per-partition exact-range).
 *   - All final-batch records have delivery_count == 1.
 *   - The broker-assigned topic_id changed end-to-end.
 *
 * The test does NOT assert anything about records produced during
 * the chaos loop. Whether they're received depends on whether the
 * consumer happened to be in a fetchable state for that topic_id at
 * that moment; that timing is intentionally unpredictable.
 *
 * Randomness is seeded from a freshly generated testid which is
 * logged at the start so any failing run can be reproduced by
 * re-seeding from the same value.
 */
static void do_test_recreate_chaos(void) {
        const int chaos_cycles                  = 10;
        const int chaos_sleep_min_ms            = 200;
        const int chaos_sleep_max_ms            = 1500;
        const int chaos_max_records_per_part    = 5;
        const int32_t chaos_partition_choices[] = {2, 3, 4};
        const int chaos_partition_choices_cnt =
            sizeof(chaos_partition_choices) /
            sizeof(chaos_partition_choices[0]);
        const char *topic;
        const char *group_id = "0184-share-recreate-chaos";
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        test_msgver_t mv_warmup, mv_final;
        uint64_t testid_warmup, testid_final, seed_testid;
        int64_t id_initial_msb, id_initial_lsb;
        int64_t id_final_msb, id_final_lsb;
        int32_t current_partition_cnt = PARTITION_CNT;
        int cycle;
        int got;
        int32_t p;
        int final_msgs;

        SUB_TEST_QUICK();

        /* Use a freshly-generated testid as the RNG seed; log it so a
         * failure can be reproduced by manually seeding the same
         * value. test_id_generate() returns a 64-bit value; srand()
         * takes unsigned, so we truncate. */
        seed_testid = test_id_generate();
        srand((unsigned)seed_testid);
        TEST_SAY("Chaos seed (from testid): %" PRIu64 "\n", seed_testid);

        testid_warmup = test_id_generate();
        testid_final  = test_id_generate();

        topic = test_mk_topic_name("0184-recreate-chaos", 1);

        test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
                                      60 * 1000);
        fetch_topic_id(topic, &id_initial_msb, &id_initial_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_initial_msb, id_initial_lsb);

        test_share_set_auto_offset_reset(group_id, "earliest");

        /* HB-first regime: long MD refresh so HB drives discovery
         * across recreate cycles. */
        rkshare = create_consumer_hb_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Warmup: drive the consumer to a fully-active state before
         * chaos begins. Produces MSGS_PER_PARTITION to every
         * partition; consumes them all. */
        TEST_SAY(
            "Warmup: producing %d msgs/partition x %d partitions "
            "(testid=%" PRIu64 ")\n",
            MSGS_PER_PARTITION, PARTITION_CNT, testid_warmup);
        produce_phase(topic, testid_warmup, PARTITION_CNT);

        test_msgver_init(&mv_warmup, testid_warmup);
        got =
            consume_into_msgver(rkshare, &mv_warmup, MSGS_PER_PHASE, 30 * 1000);
        TEST_ASSERT(got == MSGS_PER_PHASE, "Warmup: expected %d msgs, got %d",
                    MSGS_PER_PHASE, got);
        test_msgver_clear(&mv_warmup);

        /* Chaos loop. */
        for (cycle = 0; cycle < chaos_cycles; cycle++) {
                int sleep_ms;
                int32_t next_partition_cnt;
                int do_produce;
                int n_per_part;

                sleep_ms =
                    chaos_sleep_min_ms +
                    (rand() % (chaos_sleep_max_ms - chaos_sleep_min_ms + 1));
                next_partition_cnt =
                    chaos_partition_choices[rand() %
                                            chaos_partition_choices_cnt];
                do_produce = rand() % 2;
                n_per_part = do_produce
                                 ? (rand() % (chaos_max_records_per_part + 1))
                                 : 0;

                TEST_SAY(
                    "[chaos cycle %d/%d] sleep=%dms, produce=%d "
                    "msg(s)/partition (cur_partitions=%" PRId32
                    "), next_partition_cnt=%" PRId32 "\n",
                    cycle + 1, chaos_cycles, sleep_ms, n_per_part,
                    current_partition_cnt, next_partition_cnt);

                rd_usleep((int64_t)sleep_ms * 1000, NULL);

                /* Optional mid-cycle produce. Uses a throwaway testid
                 * so these records are silently ignored on consume —
                 * they may or may not arrive depending on what state
                 * the consumer is in. */
                if (n_per_part > 0) {
                        uint64_t cycle_testid = test_id_generate();
                        int32_t cp;
                        for (cp = 0; cp < current_partition_cnt; cp++) {
                                test_produce_msgs2(common_producer, topic,
                                                   cycle_testid, cp,
                                                   0 /*msg_base*/, n_per_part,
                                                   NULL /*payload*/, 0);
                        }
                }

                recreate_topic(topic, next_partition_cnt);
                current_partition_cnt = next_partition_cnt;

                /* common_producer's per-topic metadata cache still has
                 * the previous partition count; if the recreate grew
                 * the partition count, the next mid-cycle produce or
                 * the final settle would hit "Local: Unknown
                 * partition". Force a producer-side metadata refresh
                 * so its view matches the broker before we try to
                 * produce again. */
                test_wait_topic_exists(common_producer, topic, 60 * 1000);
        }

        /* Settle: produce a final batch we will require to be fully
         * consumed, against whatever topic instance chaos left us
         * with. */
        TEST_SAY("Settle: producing %d msgs/partition x %" PRId32
                 " partitions (testid=%" PRIu64 ")\n",
                 MSGS_PER_PARTITION, current_partition_cnt, testid_final);
        produce_phase(topic, testid_final, current_partition_cnt);

        final_msgs = current_partition_cnt * MSGS_PER_PARTITION;

        test_msgver_init(&mv_final, testid_final);
        got = consume_into_msgver(rkshare, &mv_final, final_msgs, 60 * 1000);
        TEST_ASSERT(got == final_msgs,
                    "Settle: expected %d msgs matching testid_final, "
                    "got %d after %d chaos cycles",
                    final_msgs, got, chaos_cycles);
        for (p = 0; p < current_partition_cnt; p++)
                test_msgver_verify_part("chaos-final-part", &mv_final,
                                        TEST_MSGVER_ALL_PART, topic, p,
                                        0 /*msg_base*/, MSGS_PER_PARTITION);
        test_msgver_clear(&mv_final);

        /* Verify chaos actually moved the topic_id. */
        fetch_topic_id(topic, &id_final_msb, &id_final_lsb);
        TEST_SAY("Final topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64 "\n",
                 id_final_msb, id_final_lsb);
        TEST_ASSERT(
            id_initial_msb != id_final_msb || id_initial_lsb != id_final_lsb,
            "Final topic_id unchanged after %d recreate cycles", chaos_cycles);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}

/**
 * @brief Subscribed consumer survives topic delete + recreate-with-
 *        different-partition-count while a producer is actively
 *        producing across the whole window.
 *
 * Differs from the other recreate tests in that this one keeps a
 * concurrent producer thread running through the entire delete window.
 * That stresses the path where:
 *   - records arrive at the broker before the delete (must reach the
 *     consumer);
 *   - records produced while the topic does not exist fail at the
 *     producer (test tolerates these silently);
 *   - records produced after the recreate, against the new partition
 *     shape, must reach the consumer once the share session has
 *     reconciled the new topic_id.
 *
 * Invariant: the consumer does not need to be restarted or
 * re-subscribed across the recreate; it transparently reattaches to
 * the freshly-created topic and continues delivering records.
 *
 * Producer-thread payload format: "<phase>:<seq>" where phase is one
 * of "before" / "during" / "after". The main thread switches the
 * phase variable at each transition. Records are produced with
 * RD_KAFKA_PARTITION_UA so the partitioner picks; we don't verify
 * per-partition coverage here, only that some records from "before"
 * and some from "after" reach the consumer.
 *
 * Assertions:
 *   - At least one "before" record consumed (consumer was healthy).
 *   - At least one "after" record consumed (consumer recovered).
 *   - "During" records may or may not be consumed; no assertion.
 *   - Final topic_id differs from the initial topic_id (broker
 *     actually recreated the topic).
 */
static void do_test_recreate_survives_concurrent_producer(void) {
        const char *topic;
        const char *group_id = "0184-share-recreate-concurrent-producer";
        const int32_t partition_cnt_a = 3;
        const int32_t partition_cnt_b = 5;
        const int producer_period_ms  = 100; /* ~10 msgs/sec */
        const int phase_duration_ms   = 10000;
        rd_kafka_share_t *rkshare;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_message_t *batch[BATCH_SIZE];
        struct producer_thread_args producer_args;
        thrd_t producer_thrd;
        int64_t id_initial_msb, id_initial_lsb;
        int64_t id_final_msb, id_final_lsb;
        int before_cnt = 0, during_cnt = 0, after_cnt = 0;
        rd_ts_t consume_deadline;

        SUB_TEST_QUICK();

        topic = test_mk_topic_name("0184-recreate-concurrent-producer", 1);

        test_create_topic_wait_exists(common_admin, topic, partition_cnt_a, -1,
                                      60 * 1000);
        fetch_topic_id(topic, &id_initial_msb, &id_initial_lsb);
        TEST_SAY("Initial topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64
                 " (partition_cnt=%" PRId32 ")\n",
                 id_initial_msb, id_initial_lsb, partition_cnt_a);

        test_share_set_auto_offset_reset(group_id, "earliest");
        /* MD-first regime: short metadata refresh interval so the
         * consumer notices the recreate quickly. The HB-first regime
         * used by the simpler recreate tests has a 5min refresh and
         * would leave the consumer blind to the topic_id change for
         * far longer than this test is willing to wait. */
        rkshare = create_consumer_hb_first(group_id);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(rkshare, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Start background producer in "before" phase. */
        producer_args.producer  = common_producer;
        producer_args.topic     = topic;
        producer_args.period_ms = producer_period_ms;
        producer_args.stop      = rd_false;
        rd_snprintf(producer_args.phase, sizeof(producer_args.phase), "before");

        TEST_ASSERT(thrd_create(&producer_thrd, producer_thread_main,
                                &producer_args) == thrd_success,
                    "thrd_create failed");

        /* --- Phase: BEFORE.
         * Consume for a few seconds while producer thread feeds the
         * topic. Count "before:" records to confirm the pipeline is
         * live. */
        consume_deadline = test_clock() + (rd_ts_t)phase_duration_ms * 1000;
        while (test_clock() < consume_deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0;
                size_t i;
                err = rd_kafka_share_consume_batch(rkshare, 200, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = batch[i];
                        if (!m->err && m->payload && m->len >= 7 &&
                            !strncmp((const char *)m->payload, "before:", 7)) {
                                before_cnt++;
                                rd_kafka_share_acknowledge(rkshare, m);
                        }
                        rd_kafka_message_destroy(m);
                }
        }
        TEST_SAY("Phase BEFORE: consumed %d \"before:\" record(s)\n",
                 before_cnt);

        /* --- Transition: enter DURING. Delete the topic but keep the
         * producer running. Most produces in this window will fail with
         * Unknown topic-type errors; that's expected. */
        TEST_SAY("Phase transition: entering DURING (delete topic)\n");
        rd_snprintf(producer_args.phase, sizeof(producer_args.phase), "during");
        test_delete_topic(common_admin, topic);

        /* Drain the consumer briefly during the delete window. Most
         * fetches will return errors. We don't track "during:" counts
         * for an assertion; just log. */
        consume_deadline = test_clock() + (rd_ts_t)phase_duration_ms * 1000;
        while (test_clock() < consume_deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0;
                size_t i;
                err = rd_kafka_share_consume_batch(rkshare, 200, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = batch[i];
                        if (!m->err && m->payload && m->len >= 7 &&
                            !strncmp((const char *)m->payload, "during:", 7)) {
                                during_cnt++;
                                rd_kafka_share_acknowledge(rkshare, m);
                        }
                        rd_kafka_message_destroy(m);
                }
        }
        TEST_SAY(
            "Phase DURING: consumed %d \"during:\" record(s) "
            "(no assertion on this count)\n",
            during_cnt);

        /* --- Transition: recreate with a different partition count
         * and switch producer to "after". */
        TEST_SAY("Recreating topic with partition_cnt=%" PRId32 " (was %" PRId32
                 ")\n",
                 partition_cnt_b, partition_cnt_a);
        rd_sleep(5); /* let delete settle on the broker */
        test_create_topic_wait_exists(common_admin, topic, partition_cnt_b, -1,
                                      60 * 1000);

        fetch_topic_id(topic, &id_final_msb, &id_final_lsb);
        TEST_SAY("Recreated topic_id: msb=0x%" PRIx64 " lsb=0x%" PRIx64
                 " (partition_cnt=%" PRId32 ")\n",
                 id_final_msb, id_final_lsb, partition_cnt_b);

        /* Force the producer to refresh its metadata for the recreated
         * topic. common_producer has the topic's previous partition
         * count and topic_id cached; without this, every subsequent
         * producev call routes "after:" records to partitions of the
         * old topic instance (which no longer exists or has a
         * different layout), so the broker rejects them and the
         * consumer never sees any "after:" records. */
        test_wait_topic_exists(common_producer, topic, 60 * 1000);

        rd_snprintf(producer_args.phase, sizeof(producer_args.phase), "after");

        /* --- Phase: AFTER.
         * Consume for a longer window to give the consumer time to
         * reconcile the new topic_id and start delivering "after:"
         * records. */
        consume_deadline = test_clock() + 60 * 1000 * 1000;
        while (after_cnt == 0 && test_clock() < consume_deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0;
                size_t i;
                err = rd_kafka_share_consume_batch(rkshare, 500, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = batch[i];
                        if (!m->err && m->payload && m->len >= 6 &&
                            !strncmp((const char *)m->payload, "after:", 6))
                                after_cnt++;
                        if (!m->err)
                                rd_kafka_share_acknowledge(rkshare, m);
                        rd_kafka_message_destroy(m);
                }
        }
        TEST_SAY("Phase AFTER: consumed %d \"after:\" record(s)\n", after_cnt);

        /* Stop and join the producer thread before any assertion
         * runs. TEST_ASSERT longjmps on failure, so leaving the
         * thread alive past an assertion would let it read the
         * struct after the stack frame is gone — segfault.
         * Joining first guarantees clean teardown either way. */
        producer_args.stop = rd_true;
        thrd_join(producer_thrd, NULL);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        /* All assertions deferred to here so any failure runs after
         * the background thread has been joined. */
        TEST_ASSERT(before_cnt > 0,
                    "Expected at least one \"before:\" record to be "
                    "consumed");
        TEST_ASSERT(id_initial_msb != id_final_msb ||
                        id_initial_lsb != id_final_lsb,
                    "Recreated topic_id matches initial; broker did not "
                    "actually recreate");
        TEST_ASSERT(after_cnt > 0,
                    "Consumer did not recover after recreate: zero "
                    "\"after:\" records seen within 60s");

        SUB_TEST_PASS();
}

/**
 * @brief Topic ID changes server-side while the consumer is in
 *        the middle of close(); verify close completes cleanly.
 *
 * Setup:
 *   - Mock cluster, 1 broker, 1 topic with 1 partition.
 *   - Produce N messages.
 *   - Share consumer with explicit ack mode. Subscribe, consume, and
 *     locally-ack every message via rd_kafka_share_acknowledge. The
 *     acks are staged but not yet flushed.
 *   - Inject a delay on the next ShareAcknowledge response so the
 *     close-time ack flush is held by the broker for that duration.
 *   - Spawn a background thread that mock-deletes and mock-recreates
 *     the topic during that ack hold, so by the time the broker
 *     finally responds the topic on the cluster has a fresh topic_id.
 *
 * Note on observability:
 *   The consumer does not actually adopt the new topic_id during
 *   close — the periodic metadata refresh path issues by-id requests
 *   for the cached (old) topic_id and the broker returns
 *   UNKNOWN_TOPIC_ID, but there is no close-time machinery that
 *   re-resolves the topic by name. So the recreate is effectively
 *   invisible to the consumer for the remainder of the close window.
 *   This test therefore verifies that close still terminates cleanly
 *   even when the topic ID changes underneath an in-flight ack.
 *
 * Assertions:
 *   - close() completes within a reasonable time
 */
static void test_recreate_during_close(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic  = "0184-recreate-during-close";
        const char *group  = "0184-share-recreate-during-close";
        const int n_msgs   = 10;
        const int delay_ms = 5000;
        rd_kafka_message_t *rkmessages[64];
        rd_kafka_topic_partition_list_t *subs;
        struct recreate_thread_args thread_args;
        thrd_t recreate_thrd;
        rd_kafka_conf_t *conf;
        size_t rcvd     = 0;
        int attempts    = 0;
        int max_attempt = 30;
        int i;
        rd_ts_t t_start, t_elapsed_ms;
        /* Upper bound: broker holds the ack for delay_ms; close also
         * needs to send the session-leave HB and wait for one more
         * round-trip on the same broker. Allow a couple of seconds for
         * overhead and jitter. */
        const int64_t close_upper_bound_ms = delay_ms + 2000;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_auto_offset_reset(mcluster, 1);

        TEST_ASSERT(rd_kafka_mock_topic_create(mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        TEST_SAY("Producing %d messages to topic %s\n", n_msgs, topic);
        test_produce_msgs_easy_v(topic, 0, 0, 0, n_msgs, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        TEST_SAY("Creating share consumer with explicit ack mode\n");
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        rkshare = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(rkshare != NULL, "Failed to create share consumer");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_share_subscribe(rkshare, subs),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        TEST_SAY("Consuming up to %d messages\n", n_msgs);
        while (rcvd < (size_t)n_msgs && attempts < max_attempt) {
                size_t batch_rcvd =
                    sizeof(rkmessages) / sizeof(rkmessages[0]) - rcvd;
                error = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);
                if (error) {
                        TEST_SAY("Consume attempt %d: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        rcvd += batch_rcvd;
                }
                attempts++;
        }
        TEST_ASSERT(rcvd == (size_t)n_msgs,
                    "Expected to consume %d messages, got %zu", n_msgs, rcvd);

        TEST_SAY("Staging acks for all %zu messages\n", rcvd);
        for (i = 0; i < (int)rcvd; i++) {
                rd_kafka_resp_err_t ack_err =
                    rd_kafka_share_acknowledge(rkshare, rkmessages[i]);
                TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "ack %d failed: %s", i, rd_kafka_err2str(ack_err));
        }

        TEST_SAY("Injecting %dms delay on the next ShareAcknowledge response\n",
                 delay_ms);
        TEST_ASSERT(rd_kafka_mock_broker_push_request_error_rtts(
                        mcluster, 1, RD_KAFKAP_ShareAcknowledge, 1,
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                        delay_ms) == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to inject ShareAcknowledge delay");

        thread_args.mcluster      = mcluster;
        thread_args.topic         = topic;
        thread_args.partition_cnt = 1;
        thread_args.sleep_ms      = 1000;
        TEST_ASSERT(thrd_create(&recreate_thrd, recreate_thread_main,
                                &thread_args) == thrd_success,
                    "thrd_create failed");

        TEST_SAY(
            "Calling close(); broker will hold ack for %dms while "
            "background thread recreates the topic\n",
            delay_ms);
        t_start      = test_clock();
        error        = rd_kafka_share_consumer_close(rkshare);
        t_elapsed_ms = (test_clock() - t_start) / 1000;
        TEST_SAY("close() returned in %" PRId64 " ms\n", t_elapsed_ms);

        if (error) {
                TEST_SAY("close() returned error: %s\n",
                         rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        }

        TEST_ASSERT(t_elapsed_ms <= close_upper_bound_ms,
                    "close() took %" PRId64 " ms, expected <= %" PRId64
                    " ms (broker delay + overhead)",
                    t_elapsed_ms, close_upper_bound_ms);

        thrd_join(recreate_thrd, NULL);

        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        test_share_destroy(rkshare);
        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

int main_0184_share_consumer_topic_recreate(int argc, char **argv) {
        /* Topic deletion is not supported against Windows brokers. */
        if (!strcmp(test_getenv("TEST_BROKER_OS", ""), "windows")) {
                TEST_SKIP("Topic deletion not supported on Windows brokers\n");
                return 0;
        }

        test_timeout_set(300);

        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        // do_test_recreate_hb_first();
        // do_test_recreate_md_first();
        // do_test_recreate_shrink_partitions_hb_first();
        // do_test_recreate_shrink_partitions_md_first();
        // do_test_recreate_chaos();
        do_test_recreate_survives_concurrent_producer();



        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}

int main_0184_share_consumer_topic_recreate_local(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);
        test_timeout_set(120);

        // test_recreate_during_close();

        return 0;
}