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
        // test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "300000");
        test_conf_set(conf, "topic.metadata.propagation.max.ms", "30000");

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief TC#1 — Recreate while the periodic metadata refresh is silent.
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
        // test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        test_conf_set(conf, "topic.metadata.propagation.max.ms", "30000");

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief TC#2 — Recreate while the periodic metadata refresh fires
 *        frequently.
 *
 * Mirror of TC#1, but with topic.metadata.refresh.interval.ms set to
 * 500ms so the refresh timer wins the race against the ShareGroupHeartbeat
 * for discovering the new topic_id. Because the MD path updates
 * rkt->rkt_topic_id directly when it sees the change, the subsequent
 * HB-driven assignment-resolution finds NEW-UUID already in the cache
 * without needing a targeted-by-id MetadataRequest. Otherwise the test
 * shape is identical to TC#1.
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
 * @brief TC#6 — Recreate with a smaller partition count, HB-first regime.
 *
 * Same HB-first regime as TC#1 (topic.metadata.refresh.interval.ms is
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
 * @brief TC#6 — Recreate with a smaller partition count, MD-first regime.
 *
 * Same MD-first regime as TC#2 (topic.metadata.refresh.interval.ms is
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
 * @brief Build a share consumer with configurable
 *        topic.metadata.propagation.max.ms.
 *
 * Other settings mirror create_consumer_md_first: short metadata refresh
 * interval (500ms) so the periodic refresh observes
 * UNKNOWN_TOPIC_OR_PART promptly after the delete — the propagation
 * grace window then determines how long that observation is suppressed
 * before being surfaced to the application.
 */
static rd_kafka_share_t *
create_consumer_with_propagation(const char *group_id, int propagation_max_ms) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];
        char propagation_str[32];

        rd_snprintf(propagation_str, sizeof(propagation_str), "%d",
                    propagation_max_ms);

        test_conf_init(&conf, NULL, 0);

        test_conf_set(conf, "group.id", group_id);
        // test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        test_conf_set(conf, "topic.metadata.propagation.max.ms",
                      propagation_str);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief Poll the share consumer for up to \p timeout_ms and return the
 *        timestamp (test_clock()) of the first UNKNOWN_TOPIC_OR_PART
 *        event observed — or 0 if none was seen.
 *
 * Note: consumer-level error events for share consumers (those produced
 * by rd_kafka_propagate_consumer_topic_errors() and friends) are
 * surfaced by rd_kafka_share_consume_batch() as the returned
 * rd_kafka_error_t*, NOT inside the per-message batch with m->err set
 * (rdkafka_queue.c:936-945 — RD_KAFKA_OP_CONSUMER_ERR is converted into
 * an rd_kafka_error_t). So we check the return value, not the batch.
 *
 * Any non-error messages in the same call are ACCEPT-ack'd and dropped.
 *
 * \p topic is currently unused (the consumer-err return value doesn't
 * carry topic context), but is kept in the signature for clarity at
 * call sites about which topic we expect the error for.
 */
static rd_ts_t wait_for_unknown_topic_event(rd_kafka_share_t *rkshare,
                                            const char *topic,
                                            int timeout_ms) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_ts_t deadline = test_clock() + (rd_ts_t)timeout_ms * 1000;

        (void)topic;

        while (test_clock() < deadline) {
                rd_kafka_error_t *err;
                size_t rcvd = 0;
                size_t i;

                err = rd_kafka_share_consume_batch(rkshare, 200, batch, &rcvd);

                if (err) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(err);
                        if (code == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
                                rd_ts_t now = test_clock();
                                TEST_SAY(
                                    "Observed UNKNOWN_TOPIC_OR_PART from "
                                    "share_consume_batch: %s\n",
                                    rd_kafka_error_string(err));
                                rd_kafka_error_destroy(err);
                                for (i = 0; i < rcvd; i++)
                                        rd_kafka_message_destroy(batch[i]);
                                return now;
                        }
                        TEST_SAY(
                            "Event ignored while waiting for "
                            "UNKNOWN_TOPIC_OR_PART: %s (%s)\n",
                            rd_kafka_err2name(code),
                            rd_kafka_error_string(err));
                        rd_kafka_error_destroy(err);
                }

                for (i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = batch[i];
                        if (!m->err)
                                rd_kafka_share_acknowledge(rkshare, m);
                        rd_kafka_message_destroy(m);
                }
        }

        return 0;
}


/**
 * @brief TC#7 — Topic deleted server-side without recreate; verify the
 *        topic.metadata.propagation.max.ms grace window suppresses
 *        UNKNOWN_TOPIC_OR_PART for that duration and then surfaces it.
 *
 * Timing semantics caveat (rdkafka_topic.c:1218-1264):
 *
 *   rd_kafka_topic_set_notexists() compares
 *       remains_us = (rkt_ts_state + propagation_max_ms) - rkt_ts_metadata
 *   against zero. `rkt_ts_state` is the timestamp of the LAST topic-state
 *   transition (e.g. UNKNOWN -> EXISTS). Once `remains_us` reaches zero,
 *   the next metadata response that says UNKNOWN flips the rkt to
 *   S_NOTEXISTS and the error is propagated.
 *
 *   So the grace window is measured from when the rkt last transitioned
 *   to S_EXISTS — NOT from when the delete happened. To make the test's
 *   measurement meaningful, we keep the gap between rkt_ts_state and
 *   the topic delete as small as possible: we only do enough work
 *   between consumer-create and topic-delete to confirm the rkt is in
 *   S_EXISTS (one metadata round-trip), then immediately delete. That
 *   way "time since rkt_ts_state" ≈ "time since delete" within tens
 *   of milliseconds.
 *
 * Flow:
 *   1. Create topic.
 *   2. Create share consumer; subscribe.
 *   3. Poll briefly so the consumer issues a MetadataRequest and the
 *      rkt transitions to S_EXISTS — this stamps rkt_ts_state.
 *   4. Delete the topic. t_delete ≈ rkt_ts_state.
 *   5. Poll, waiting for UNKNOWN_TOPIC_OR_PART. Measure elapsed.
 *   6. Assert elapsed is in [propagation_max_ms - slack,
 *      propagation_max_ms + slack].
 *
 * @param propagation_max_ms  topic.metadata.propagation.max.ms value
 *                            for the consumer under test.
 * @param slack_ms            tolerance on both sides of the expected
 *                            event time. Must accommodate at least one
 *                            metadata refresh tick (500ms) plus broker
 *                            delete latency.
 * @param label               short slug used for topic naming.
 */
// static void do_test_delete_within_propagation_window(int propagation_max_ms,
//                                                      int slack_ms,
//                                                      const char *label) {
//         const char *topic;
//         const char *group_id = "0184-share-delete-within-propagation";
//         rd_kafka_share_t *rkshare;
//         rd_kafka_topic_partition_list_t *subs;
//         rd_ts_t t_delete, t_event;
//         int64_t elapsed_ms;
//         int upper_bound_ms;
//
//         SUB_TEST_QUICK("%s (propagation_max_ms=%d)", label,
//                        propagation_max_ms);
//
//         topic = test_mk_topic_name(label, 1);
//
//         test_create_topic_wait_exists(common_admin, topic, PARTITION_CNT, -1,
//                                       60 * 1000);
//
//         test_share_set_auto_offset_reset(group_id, "earliest");
//
//         rkshare =
//             create_consumer_with_propagation(group_id, propagation_max_ms);
//
//         subs = rd_kafka_topic_partition_list_new(1);
//         rd_kafka_topic_partition_list_add(subs, topic,
//         RD_KAFKA_PARTITION_UA); rd_kafka_share_subscribe(rkshare, subs);
//         rd_kafka_topic_partition_list_destroy(subs);
//
//         /* Bring the consumer to a fully-active state before deleting
//          * the topic. "Fully active" means: the ShareGroupHeartbeat has
//          * delivered an assignment, rkts have been created for the
//          * assigned partitions, and at least one ShareFetch round-trip
//          * has completed (which is what guarantees the rkt has been
//          * marked S_EXISTS — stamping rkt_ts_state).
//          *
//          * Without this, the broker's share-group assignor may not have
//          * even assigned partitions to us yet, so no rkt exists, the
//          * propagation grace window never engages, and the test will
//          * time out without ever seeing the error event.
//          *
//          * The cleanest "fully active" signal is receiving an actual
//          * record. We produce one warmup message per partition (with a
//          * disposable testid so it doesn't pollute later state) and
//          * poll until we see at least one come back.
//          *
//          * The interval [first record received .. topic delete] is
//          * small but non-zero — typically a few hundred ms. The
//          * propagation grace window is measured from rkt_ts_state, so
//          * this small offset is implicitly absorbed by the slack in the
//          * assertion bounds. */
//         {
//                 uint64_t warmup_testid = test_id_generate();
//                 rd_kafka_message_t *batch[BATCH_SIZE];
//                 rd_ts_t warmup_deadline;
//                 rd_bool_t got_warmup_record = rd_false;
//
//                 TEST_SAY("Warmup: producing 1 msg/partition to drive "
//                          "assignment and rkt creation\n");
//                 produce_phase(topic, warmup_testid, PARTITION_CNT);
//
//                 warmup_deadline = test_clock() + 30 * 1000 * 1000;
//                 while (!got_warmup_record && test_clock() < warmup_deadline)
//                 {
//                         rd_kafka_error_t *err;
//                         size_t rcvd = 0;
//                         size_t i;
//                         err = rd_kafka_share_consume_batch(rkshare, 500,
//                         batch,
//                                                            &rcvd);
//                         if (err) {
//                                 TEST_SAY("Warmup: share_consume_batch "
//                                          "returned %s\n",
//                                          rd_kafka_error_string(err));
//                                 rd_kafka_error_destroy(err);
//                         }
//                         for (i = 0; i < rcvd; i++) {
//                                 rd_kafka_message_t *m = batch[i];
//                                 if (!m->err) {
//                                         got_warmup_record = rd_true;
//                                         rd_kafka_share_acknowledge(rkshare,
//                                         m);
//                                 }
//                                 rd_kafka_message_destroy(m);
//                         }
//                 }
//                 TEST_ASSERT(got_warmup_record,
//                             "Warmup: did not receive any record within 30s; "
//                             "share consumer never became active");
//         }
//
//         /* Delete the topic. After this point, every metadata refresh
//          * (every 500ms) will return UNKNOWN_TOPIC_OR_PART. The rkt will
//          * not transition to S_NOTEXISTS — and thus no error will be
//          * propagated to the application — until propagation_max_ms has
//          * elapsed since rkt_ts_state. */
//         TEST_SAY("Deleting topic %s (no recreate)\n", topic);
//         t_delete = test_clock();
//         test_delete_topic(common_admin, topic);
//
//         /* Wait for the UNKNOWN_TOPIC_OR_PART event with a generous
//          * upper bound. We'll measure when it actually arrives and
//          * assert it lands in [propagation - slack, propagation + slack]. */
//         upper_bound_ms = propagation_max_ms + slack_ms + 5000;
//         TEST_SAY("Waiting up to %d ms for UNKNOWN_TOPIC_OR_PART "
//                  "(expecting around T+%d ms)\n",
//                  upper_bound_ms, propagation_max_ms);
//         t_event = wait_for_unknown_topic_event(rkshare, topic,
//         upper_bound_ms); TEST_ASSERT(t_event != 0,
//                     "UNKNOWN_TOPIC_OR_PART for %s did not surface within "
//                     "%d ms after delete",
//                     topic, upper_bound_ms);
//
//         elapsed_ms = (t_event - t_delete) / 1000;
//         TEST_SAY("UNKNOWN_TOPIC_OR_PART surfaced at T+%" PRId64
//                  " ms (delete at T+0, propagation_max_ms=%d, slack=%d)\n",
//                  elapsed_ms, propagation_max_ms, slack_ms);
//
//         TEST_ASSERT(elapsed_ms >= propagation_max_ms - slack_ms,
//                     "UNKNOWN_TOPIC_OR_PART surfaced at T+%" PRId64
//                     " ms, more than %d ms before propagation_max_ms (%d)",
//                     elapsed_ms, slack_ms, propagation_max_ms);
//         TEST_ASSERT(elapsed_ms <= propagation_max_ms + slack_ms,
//                     "UNKNOWN_TOPIC_OR_PART surfaced at T+%" PRId64
//                     " ms, more than %d ms after propagation_max_ms (%d)",
//                     elapsed_ms, slack_ms, propagation_max_ms);
//
//         test_share_consumer_close(rkshare);
//         test_share_destroy(rkshare);
//
//         SUB_TEST_PASS();
// }


int main_0184_share_consumer_topic_recreate(int argc, char **argv) {
        test_timeout_set(300);

        common_producer = test_create_producer();
        common_admin    = test_create_producer();

        do_test_recreate_hb_first();
        do_test_recreate_md_first();
        do_test_recreate_shrink_partitions_hb_first();
        do_test_recreate_shrink_partitions_md_first();

        /* TC#7: topic deletion within propagation window. */
        // do_test_delete_within_propagation_window(
        //     10000 /* propagation_max_ms */, 3000 /* post-window slack */,
        //     "0184-delete-within-propagation-10s");

        rd_kafka_destroy(common_admin);
        rd_kafka_destroy(common_producer);

        return 0;
}