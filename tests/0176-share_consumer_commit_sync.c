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
 * @brief Share consumer rd_kafka_share_commit_sync() API tests.
 *
 * Tests the commit_sync API in both implicit and explicit ack modes.
 * Verifies that commit_sync synchronously commits acknowledged records
 * and returns per-partition results.
 */

#define MAX_MSGS      500
#define CONSUME_ARRAY 10001


/**
 * @brief Create share consumer with specified ack mode.
 * @param ack_mode "implicit" or "explicit"
 */
static rd_kafka_share_t *create_share_consumer(const char *group_id,
                                               const char *ack_mode) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 60);

        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "share.acknowledgement.mode", ack_mode, errstr,
                          sizeof(errstr));

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief Set share.auto.offset.reset=earliest for a share group.
 *        Uses a new admin client (not the share consumer handle).
 */
static void set_group_offset_earliest(const char *group_name) {
        rd_kafka_t *admin;
        rd_kafka_conf_t *conf;
        char errstr[512];
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};

        test_conf_init(&conf, NULL, 30);
        admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(admin, "Failed to create admin client: %s", errstr);

        test_IncrementalAlterConfigs_simple(admin, RD_KAFKA_RESOURCE_GROUP,
                                            group_name, cfg, 1);

        rd_kafka_destroy(admin);
}


/**
 * @brief Subscribe a share consumer to topics.
 */
static void subscribe_consumer(rd_kafka_share_t *rkshare,
                               const char **topics,
                               int topic_cnt) {
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_resp_err_t err;
        int i;

        subs = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0; i < topic_cnt; i++)
                rd_kafka_topic_partition_list_add(subs, topics[i],
                                                  RD_KAFKA_PARTITION_UA);

        err = rd_kafka_share_subscribe(rkshare, subs);
        TEST_ASSERT(!err, "Subscribe failed: %s", rd_kafka_err2str(err));

        rd_kafka_topic_partition_list_destroy(subs);
}


/* ===================================================================
 *  Test 1: Basic implicit ack mode commit_sync.
 *
 *  Implicit mode auto-ACCEPTs all records from previous poll.
 *  Consumer consumes records, calls commit_sync, verifies
 *  per-partition results show NO_ERROR.
 * =================================================================== */
static void do_test_basic_implicit_commit_sync(void) {
        const char *topic;
        const char *group = "commit-sync-implicit-basic";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int i;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-impl-basic", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume records */
        while (consumed == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumed %d messages\n", consumed);
        TEST_ASSERT(consumed == 5, "Expected 5 messages, got %d", consumed);

        /* commit_sync should flush implicit acks */
        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(partitions != NULL,
                    "Expected per-partition results, got NULL");

        TEST_SAY("commit_sync returned %d partition(s)\n", partitions->cnt);
        TEST_ASSERT(partitions->cnt >= 1,
                    "Expected at least 1 partition result, got %d",
                    partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("  %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2str(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected NO_ERROR for %s [%" PRId32
                            "], got %s",
                            rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 2: Basic explicit ack mode commit_sync.
 *
 *  Explicit mode requires the app to ACCEPT each record via
 *  rd_kafka_share_acknowledge(). Then commit_sync flushes acks
 *  and returns per-partition results.
 * =================================================================== */
static void do_test_basic_explicit_commit_sync(void) {
        const char *topic;
        const char *group = "commit-sync-explicit-basic";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int i;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-expl-basic", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume records and explicitly ACCEPT each */
        while (consumed == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumed and acknowledged %d messages\n", consumed);
        TEST_ASSERT(consumed == 5, "Expected 5 messages, got %d", consumed);

        /* commit_sync should flush explicit acks */
        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(partitions != NULL,
                    "Expected per-partition results, got NULL");

        TEST_SAY("commit_sync returned %d partition(s)\n", partitions->cnt);
        TEST_ASSERT(partitions->cnt >= 1,
                    "Expected at least 1 partition result, got %d",
                    partitions->cnt);

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("  %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2str(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected NO_ERROR for %s [%" PRId32
                            "], got %s",
                            rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 3: No pending acks — commit_sync with nothing to commit.
 *
 *  Subscribe but do not consume any records. commit_sync should
 *  return NULL error and NULL partitions.
 * =================================================================== */
static void do_test_no_pending_acks(void) {
        const char *topic;
        const char *group = "commit-sync-no-pending";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-no-pending", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* commit_sync with no consumed records */
        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(partitions == NULL,
                    "Expected NULL partitions when no acks pending, "
                    "got %d partition(s)",
                    partitions ? partitions->cnt : -1);

        TEST_SAY("commit_sync with no pending acks returned NULL as expected\n");

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 4: commit_sync prevents redelivery.
 *
 *  Consumer A consumes and commit_sync's all records, then closes.
 *  Consumer B (same group) should see 0 records since they were
 *  all committed by A.
 * =================================================================== */
static void do_test_commit_sync_prevents_redelivery(void) {
        const char *topic;
        const char *group = "commit-sync-no-redeliver";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-no-redeliver", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        /* Consumer A: consume all and commit_sync */
        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        while (consumed == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer A consumed %d messages\n", consumed);
        TEST_ASSERT(consumed == 5, "Expected 5, got %d", consumed);

        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        RD_IF_FREE(partitions, rd_kafka_topic_partition_list_destroy);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Consumer B: same group, should get 0 records */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 5) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer B consumed %d messages (expected 0)\n", consumed);
        TEST_ASSERT(consumed == 0,
                    "Consumer B got %d records, expected 0 after commit_sync",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 5: Mixed ack types — ACCEPT, RELEASE, REJECT.
 *
 *  Consumer A: ACCEPT first 5, RELEASE next 3, REJECT last 2,
 *  then commit_sync. Consumer B should only receive the 3
 *  RELEASE'd records with delivery_count >= 2.
 * =================================================================== */
static void do_test_mixed_ack_types(void) {
        const char *topic;
        const char *group = "commit-sync-mixed-acks";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int i;
        int64_t released_offsets[3];
        int released_cnt = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-mixed-acks", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 10);

        /* Consumer A: consume all 10 in a single batch, apply mixed ack
         * types */
        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        while (consumed == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                /* Skip partial batches — wait for all 10 in one call */
                if (rcvd < 10) {
                        for (j = 0; j < rcvd; j++)
                                rd_kafka_message_destroy(rkmessages[j]);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        rd_kafka_resp_err_t err;

                        if (rkmessages[j]->err) {
                                rd_kafka_message_destroy(rkmessages[j]);
                                continue;
                        }

                        if (consumed < 5) {
                                /* ACCEPT first 5 */
                                err = rd_kafka_share_acknowledge_type(
                                    rkshare, rkmessages[j],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                        } else if (consumed < 8) {
                                /* RELEASE next 3 */
                                released_offsets[released_cnt++] =
                                    rkmessages[j]->offset;
                                err = rd_kafka_share_acknowledge_type(
                                    rkshare, rkmessages[j],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
                        } else {
                                /* REJECT last 2 */
                                err = rd_kafka_share_acknowledge_type(
                                    rkshare, rkmessages[j],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT);
                        }

                        TEST_ASSERT(!err, "acknowledge_type failed: %s",
                                    rd_kafka_err2str(err));
                        consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer A consumed %d messages "
                 "(5 ACCEPT, 3 RELEASE, 2 REJECT)\n",
                 consumed);
        TEST_ASSERT(consumed == 10, "Expected 10, got %d", consumed);
        TEST_ASSERT(released_cnt == 3, "Expected 3 released, got %d",
                    released_cnt);

        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(partitions != NULL,
                    "Expected per-partition results, got NULL");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("  %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2str(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected NO_ERROR for %s [%" PRId32 "], got %s",
                            rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Consumer B: should only get the 3 RELEASE'd records */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 10) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                int16_t dc;
                                int k;
                                rd_bool_t found = rd_false;

                                dc = rd_kafka_message_delivery_count(
                                    rkmessages[j]);
                                TEST_ASSERT(
                                    dc >= 2,
                                    "Consumer B got record at offset %" PRId64
                                    " with delivery_count=%d, expected >= 2",
                                    rkmessages[j]->offset, (int)dc);

                                /* Verify it is one of the released offsets */
                                for (k = 0; k < released_cnt; k++) {
                                        if (rkmessages[j]->offset ==
                                            released_offsets[k]) {
                                                found = rd_true;
                                                break;
                                        }
                                }
                                TEST_ASSERT(
                                    found,
                                    "Consumer B got offset %" PRId64
                                    " which was not RELEASE'd",
                                    rkmessages[j]->offset);

                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer B consumed %d messages (expected 3 RELEASE'd)\n",
                 consumed);
        TEST_ASSERT(consumed == 3,
                    "Consumer B got %d records, expected 3 RELEASE'd",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 6: Multiple commit_sync calls.
 *
 *  Consume 50 records, ACCEPT 10 at a time and commit_sync after
 *  each batch of 10 (5 commit_sync calls total). After each
 *  commit_sync, immediately call commit_sync again to verify
 *  a no-op commit (no pending acks) returns NULL/NULL cleanly.
 *  Consumer B should get 0 records.
 * =================================================================== */
static void do_test_multiple_commit_sync_calls(void) {
        const char *topic;
        const char *group = "commit-sync-multi-calls";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int commit_cnt = 0;
        int acked_since_last_commit = 0;
        int i;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-multi-calls", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 50);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume all 50, commit_sync every 10 records */
        while (consumed < 50 && attempts++ < 60) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                                acked_since_last_commit++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);

                        /* commit_sync every 10 records */
                        if (acked_since_last_commit == 10) {
                                partitions = NULL;
                                error = rd_kafka_share_commit_sync(
                                    rkshare, 30000, &partitions);
                                commit_cnt++;
                                TEST_ASSERT(
                                    !error,
                                    "commit_sync #%d failed: %s", commit_cnt,
                                    error ? rd_kafka_error_string(error) : "");
                                TEST_ASSERT(
                                    partitions != NULL,
                                    "commit_sync #%d: expected results, "
                                    "got NULL",
                                    commit_cnt);

                                for (i = 0; i < partitions->cnt; i++) {
                                        rd_kafka_topic_partition_t *rktpar =
                                            &partitions->elems[i];
                                        TEST_ASSERT(
                                            rktpar->err ==
                                                RD_KAFKA_RESP_ERR_NO_ERROR,
                                            "commit_sync #%d: %s [%" PRId32
                                            "] got %s",
                                            commit_cnt, rktpar->topic,
                                            rktpar->partition,
                                            rd_kafka_err2str(rktpar->err));
                                }

                                rd_kafka_topic_partition_list_destroy(
                                    partitions);
                                TEST_SAY("commit_sync #%d OK "
                                         "(consumed %d so far)\n",
                                         commit_cnt, consumed);
                                acked_since_last_commit = 0;

                                /* Immediately call commit_sync again —
                                 * no pending acks, should return
                                 * NULL/NULL */
                                partitions = NULL;
                                error = rd_kafka_share_commit_sync(
                                    rkshare, 30000, &partitions);
                                TEST_ASSERT(
                                    !error,
                                    "back-to-back commit_sync after #%d "
                                    "failed: %s",
                                    commit_cnt,
                                    error ? rd_kafka_error_string(error) : "");
                                TEST_ASSERT(
                                    partitions == NULL,
                                    "back-to-back commit_sync after #%d: "
                                    "expected NULL partitions, got %d",
                                    commit_cnt,
                                    partitions ? partitions->cnt : -1);
                                TEST_SAY("back-to-back commit_sync after "
                                         "#%d returned NULL as expected\n",
                                         commit_cnt);
                        }
                }
        }

        TEST_SAY("Consumer A consumed %d messages, %d commit_sync calls\n",
                 consumed, commit_cnt);
        TEST_ASSERT(consumed == 50, "Expected 50, got %d", consumed);
        TEST_ASSERT(commit_cnt == 5, "Expected 5 commit_sync calls, got %d",
                    commit_cnt);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Consumer B: same group, should get 0 records */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 5) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer B consumed %d messages (expected 0)\n", consumed);
        TEST_ASSERT(consumed == 0,
                    "Consumer B got %d records, expected 0 after 5 "
                    "commit_sync calls",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 7: Multi-topic multi-partition commit_sync.
 *
 *  10 topics with 6 partitions each, 10 messages per partition
 *  (600 total). Consume in batches, apply mixed ack types
 *  (ACCEPT ~50%, RELEASE ~30%, REJECT ~20%) and commit_sync
 *  after each consume_batch call.
 *
 *  RELEASE'd records are redelivered. Continue consuming until
 *  accepted + rejected == total_msgs (all records settled).
 *  On delivery_count >= MAX_REDELIVERY_ROUNDS, force ACCEPT.
 *  Verify commit_sync returns NO_ERROR each time and that
 *  delivery_count stays within bounds.
 * =================================================================== */
#define MULTI_TP_TOPICS             10
#define MULTI_TP_PARTITIONS         6
#define MULTI_TP_MSGS_PER_PARTITION 10
#define MAX_REDELIVERY_ROUNDS       4

static rd_kafka_share_AcknowledgeType_t get_ack_type(int index) {
        int r = index % 10;
        if (r < 5)
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
        else if (r < 8)
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
        else
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;
}

static void do_test_multi_topic_partition(void) {
        const char *group = "commit-sync-multi-tp";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        char *topics[MULTI_TP_TOPICS];
        size_t rcvd;
        size_t j;
        int total_consumed = 0;
        int accepted = 0, released = 0, rejected = 0;
        int attempts = 0;
        int commit_cnt = 0;
        int total_msgs =
            MULTI_TP_TOPICS * MULTI_TP_PARTITIONS * MULTI_TP_MSGS_PER_PARTITION;
        int i, p;
        int16_t max_dc_seen = 0;

        SUB_TEST();

        /* Create topics and produce 10 messages per partition */
        for (i = 0; i < MULTI_TP_TOPICS; i++) {
                topics[i] = rd_strdup(test_mk_topic_name("0176-cs-mtp", 1));
                test_create_topic_wait_exists(NULL, topics[i],
                                              MULTI_TP_PARTITIONS, -1,
                                              60 * 1000);
                for (p = 0; p < MULTI_TP_PARTITIONS; p++)
                        test_produce_msgs_easy(topics[i], p, p,
                                               MULTI_TP_MSGS_PER_PARTITION);
        }

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, (const char **)topics, MULTI_TP_TOPICS);

        /* Consume until all records are settled
         * (accepted + rejected == total_msgs) */
        while (accepted + rejected < total_msgs && attempts++ < 200) {
                int batch_cnt = 0;

                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        rd_kafka_share_AcknowledgeType_t ack_type;
                        rd_kafka_resp_err_t err;
                        int16_t dc;

                        if (rkmessages[j]->err) {
                                rd_kafka_message_destroy(rkmessages[j]);
                                continue;
                        }

                        dc = rd_kafka_message_delivery_count(rkmessages[j]);
                        if (dc > max_dc_seen)
                                max_dc_seen = dc;

                        /* On final delivery attempt, force ACCEPT */
                        if (dc >= MAX_REDELIVERY_ROUNDS)
                                ack_type =
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                        else
                                ack_type = get_ack_type(total_consumed);

                        err = rd_kafka_share_acknowledge_type(
                            rkshare, rkmessages[j], ack_type);
                        TEST_ASSERT(!err, "acknowledge_type failed: %s",
                                    rd_kafka_err2str(err));

                        if (ack_type ==
                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT)
                                accepted++;
                        else if (ack_type ==
                                 RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE)
                                released++;
                        else
                                rejected++;

                        total_consumed++;
                        batch_cnt++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }

                if (batch_cnt == 0)
                        continue;

                /* commit_sync after each batch */
                partitions = NULL;
                error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
                commit_cnt++;
                TEST_ASSERT(!error, "commit_sync #%d failed: %s", commit_cnt,
                            error ? rd_kafka_error_string(error) : "");
                TEST_ASSERT(partitions != NULL,
                            "commit_sync #%d: expected results, got NULL",
                            commit_cnt);

                for (i = 0; i < partitions->cnt; i++) {
                        rd_kafka_topic_partition_t *rktpar =
                            &partitions->elems[i];
                        TEST_ASSERT(
                            rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "commit_sync #%d: %s [%" PRId32 "] got %s",
                            commit_cnt, rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
                }

                TEST_SAY("commit_sync #%d OK (%d in batch, %d total, "
                         "settled=%d/%d, %d partition results)\n",
                         commit_cnt, batch_cnt, total_consumed,
                         accepted + rejected, total_msgs, partitions->cnt);

                rd_kafka_topic_partition_list_destroy(partitions);
        }

        TEST_SAY("Total consumed=%d (original=%d), commit_sync calls=%d, "
                 "accepted=%d, released=%d, rejected=%d, "
                 "max delivery_count=%d\n",
                 total_consumed, total_msgs, commit_cnt, accepted, released,
                 rejected, (int)max_dc_seen);

        TEST_ASSERT(accepted + rejected == total_msgs,
                    "Expected accepted(%d) + rejected(%d) == %d",
                    accepted, rejected, total_msgs);

        /* Verify redelivery happened */
        TEST_ASSERT(max_dc_seen > 1,
                    "Expected redeliveries (max delivery_count > 1), "
                    "got max=%d",
                    (int)max_dc_seen);

        TEST_ASSERT(max_dc_seen <= MAX_REDELIVERY_ROUNDS,
                    "Max delivery_count=%d exceeds limit=%d",
                    (int)max_dc_seen, MAX_REDELIVERY_ROUNDS);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        for (i = 0; i < MULTI_TP_TOPICS; i++)
                rd_free(topics[i]);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Mock broker infrastructure (same pattern as 0173).
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

        ctx.mcluster = test_mock_cluster_new(3, &ctx.bootstraps);

        TEST_ASSERT(rd_kafka_mock_set_apiversion(
                        ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareGroupHeartbeat");
        TEST_ASSERT(rd_kafka_mock_set_apiversion(ctx.mcluster,
                                                 RD_KAFKAP_ShareFetch, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to enable ShareFetch");

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", ctx.bootstraps);

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

static void
mock_produce_messages(rd_kafka_t *producer, const char *topic, int msgcnt) {
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
 * @brief Create share consumer for mock broker tests.
 *
 * Unlike create_share_consumer() which uses test_conf_init with real
 * broker settings, this uses mock cluster bootstraps.
 */
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

static int count_share_ack_requests(rd_kafka_mock_cluster_t *mcluster) {
        size_t cnt;
        size_t i;
        rd_kafka_mock_request_t **requests;
        int share_ack_cnt = 0;

        requests = rd_kafka_mock_get_requests(mcluster, &cnt);

        for (i = 0; i < cnt; i++) {
                int16_t api_key = rd_kafka_mock_request_api_key(requests[i]);
                if (api_key == RD_KAFKAP_ShareAcknowledge)
                        share_ack_cnt++;
        }

        rd_kafka_mock_request_destroy_array(requests, cnt);
        return share_ack_cnt;
}


/* ===================================================================
 *  Test 8: Mock — verify commit_sync uses ShareAcknowledge RPC.
 *
 *  Consume 50 records, ACCEPT 10 at a time, commit_sync after
 *  each batch. Track ShareAcknowledge requests and verify the
 *  count matches the number of commit_sync calls that returned
 *  at least 1 partition result.
 * =================================================================== */
static void do_test_mock_uses_share_acknowledge(void) {
        test_ctx_t ctx = test_ctx_new();
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const char *topic = "mock-cs-share-ack";
        const int msgcnt  = 50;
        int consumed = 0;
        int attempts = 0;
        int commit_cnt = 0;
        int commit_with_partitions = 0;
        int acked_since_last_commit = 0;
        int share_ack_cnt;
        int i;

        SUB_TEST();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-ack", "explicit");

        {
                const char *t = topic;
                subscribe_consumer(rkshare, &t, 1);
        }

        /* Clear and start tracking requests */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Consume all records, ACCEPT each, commit_sync every 10 */
        while (consumed < msgcnt && attempts++ < 30) {
                rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
                size_t rcvd = 0;
                size_t j;

                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                                acked_since_last_commit++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);

                        if (acked_since_last_commit == 10) {
                                partitions = NULL;
                                error = rd_kafka_share_commit_sync(
                                    rkshare, 30000, &partitions);
                                commit_cnt++;
                                TEST_ASSERT(
                                    !error,
                                    "commit_sync #%d failed: %s", commit_cnt,
                                    error ? rd_kafka_error_string(error) : "");

                                if (partitions != NULL) {
                                        commit_with_partitions++;
                                        for (i = 0; i < partitions->cnt; i++) {
                                                rd_kafka_topic_partition_t
                                                    *rktpar =
                                                        &partitions->elems[i];
                                                TEST_ASSERT(
                                                    rktpar->err ==
                                                        RD_KAFKA_RESP_ERR_NO_ERROR,
                                                    "commit_sync #%d: "
                                                    "%s [%" PRId32 "] got %s",
                                                    commit_cnt, rktpar->topic,
                                                    rktpar->partition,
                                                    rd_kafka_err2str(
                                                        rktpar->err));
                                        }
                                        rd_kafka_topic_partition_list_destroy(
                                            partitions);
                                }

                                TEST_SAY("commit_sync #%d OK "
                                         "(consumed %d so far)\n",
                                         commit_cnt, consumed);
                                acked_since_last_commit = 0;
                        }
                }
        }

        TEST_SAY("Mock: consumed %d/%d, %d commit_sync calls, "
                 "%d with partition results\n",
                 consumed, msgcnt, commit_cnt, commit_with_partitions);
        TEST_ASSERT(consumed == msgcnt, "Expected %d, got %d", msgcnt,
                    consumed);

        /* Wait for async ops to complete before counting requests */
        rd_sleep(3);

        share_ack_cnt = count_share_ack_requests(ctx.mcluster);
        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        TEST_SAY("Mock: ShareAcknowledge requests=%d, "
                 "commit_sync with partitions=%d\n",
                 share_ack_cnt, commit_with_partitions);
        TEST_ASSERT(
            share_ack_cnt == commit_with_partitions,
            "Expected ShareAcknowledge count (%d) == commit_sync with "
            "partitions count (%d)",
            share_ack_cnt, commit_with_partitions);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 9: Mock — commit_sync timeout and recovery.
 *
 *  Phase 1: Consume 10 records, ACCEPT all. Set broker RTT to
 *  5000ms and call commit_sync with timeout_ms=2000. The call
 *  should block ~2000ms then return _TIMED_OUT. The broker still
 *  processes the ack when the delayed response arrives.
 *
 *  Phase 2: Remove RTT, wait 5s for broker to finish processing.
 *  Second consumer should get 0 records (acks were processed).
 *
 *  Phase 3: Produce 10 more, consume and ACCEPT, commit_sync
 *  with normal timeout → succeeds. Verifies recovery.
 * =================================================================== */
static void do_test_mock_commit_sync_timeout(void) {
        test_ctx_t ctx = test_ctx_new();
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const char *topic = "mock-cs-timeout";
        const char *t     = topic;
        const int msgcnt  = 10;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed;
        int attempts;
        int i;
        rd_bool_t got_timed_out = rd_false;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-timeout", "explicit");
        subscribe_consumer(rkshare, &t, 1);

        /* Phase 1: Consume all 10 records and ACCEPT */
        consumed = 0;
        attempts = 0;
        while (consumed < msgcnt && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Phase 1: consumed and acknowledged %d messages\n", consumed);
        TEST_ASSERT(consumed == msgcnt, "Expected %d, got %d", msgcnt,
                    consumed);

        /* Inject 5000ms RTT on all brokers */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 5000);

        /* commit_sync with 2000ms timeout — should block ~2000ms
         * then time out */
        t_start = test_clock();
        partitions = NULL;
        error = rd_kafka_share_commit_sync(rkshare, 2000, &partitions);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("Phase 1: commit_sync returned after %" PRId64
                 "ms, error=%s, partitions=%s\n",
                 t_elapsed_ms,
                 error ? rd_kafka_error_string(error) : "NULL",
                 partitions ? "non-NULL" : "NULL");

        /* Verify commit_sync blocked for ~2000ms (allow 1500-3000ms) */
        TEST_ASSERT(t_elapsed_ms >= 1500 && t_elapsed_ms <= 3000,
                    "Expected commit_sync to block ~2000ms, "
                    "got %" PRId64 "ms",
                    t_elapsed_ms);

        /* May return top-level error or per-partition _TIMED_OUT */
        if (error) {
                TEST_SAY("Phase 1: top-level error: %s\n",
                         rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                got_timed_out = rd_true;
        }

        if (partitions) {
                for (i = 0; i < partitions->cnt; i++) {
                        rd_kafka_topic_partition_t *rktpar =
                            &partitions->elems[i];
                        TEST_SAY("Phase 1: %s [%" PRId32 "]: %s\n",
                                 rktpar->topic, rktpar->partition,
                                 rd_kafka_err2str(rktpar->err));
                        if (rktpar->err == RD_KAFKA_RESP_ERR__TIMED_OUT)
                                got_timed_out = rd_true;
                }
                rd_kafka_topic_partition_list_destroy(partitions);
        }

        TEST_ASSERT(got_timed_out, "Expected _TIMED_OUT error from "
                     "commit_sync with short timeout");

        /* Phase 2: Remove RTT, wait 5s for broker to finish
         * processing the delayed response */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, -1, 0);
        rd_sleep(5);

        /* Close first consumer */
        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Second consumer: should get 0 records because the broker
         * processed the acks despite client-side timeout */
        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-timeout", "implicit");
        subscribe_consumer(rkshare, &t, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 5) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Phase 2: second consumer got %d records (expected 0)\n",
                 consumed);
        TEST_ASSERT(consumed == 0,
                    "Second consumer got %d records, expected 0 "
                    "(broker should have processed acks despite "
                    "client-side timeout)",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Phase 3: Produce more, consume, commit_sync normally —
         * verify recovery after timeout */
        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-timeout", "explicit");
        subscribe_consumer(rkshare, &t, 1);

        consumed = 0;
        attempts = 0;
        while (consumed < msgcnt && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Phase 3: consumed %d messages\n", consumed);
        TEST_ASSERT(consumed == msgcnt, "Expected %d, got %d", msgcnt,
                    consumed);

        partitions = NULL;
        error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
        TEST_ASSERT(!error, "Phase 3: commit_sync failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(partitions != NULL,
                    "Phase 3: expected partition results, got NULL");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("Phase 3: %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2str(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Phase 3: expected NO_ERROR for %s [%" PRId32
                            "], got %s",
                            rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 10: Mixed commit types — commit_async and commit_sync.
 *
 *  Produce 50 records. Consume all, ACCEPT each record
 *  individually. For the first 10 records call commit_async,
 *  then call commit_sync for the 11th. Repeat this pattern
 *  (10 async + 1 sync) until all records are committed.
 *
 *  Verify that commit_sync after async commits completes very
 *  quickly (acks already sent by async, so sync has little or
 *  no work to do). Second consumer should get 0 records.
 * =================================================================== */
static void do_test_mixed_commit_types(void) {
        const char *topic;
        const char *group = "commit-sync-mixed-types";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        int async_cnt = 0;
        int sync_cnt = 0;
        int acked_since_last_sync = 0;
        int i;
        rd_ts_t t_start, t_elapsed_ms;
        rd_ts_t max_sync_elapsed_ms = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0176-cs-mixed-types", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 50);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume all 50 records, ACCEPT each, alternate between
         * commit_async (10 times) and commit_sync (1 time) */
        while (consumed < 50 && attempts++ < 60) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                                acked_since_last_sync++;

                                if (acked_since_last_sync <= 10) {
                                        /* commit_async for first 10 */
                                        error = rd_kafka_share_commit_async(
                                            rkshare);
                                        TEST_ASSERT(
                                            !error,
                                            "commit_async #%d failed: %s",
                                            async_cnt + 1,
                                            error ? rd_kafka_error_string(error)
                                                  : "");
                                        async_cnt++;
                                }

                                if (acked_since_last_sync == 11) {
                                        /* commit_sync on the 11th —
                                         * should be fast since most acks
                                         * were already sent by async */
                                        partitions = NULL;
                                        t_start    = test_clock();
                                        error = rd_kafka_share_commit_sync(
                                            rkshare, 30000, &partitions);
                                        t_elapsed_ms =
                                            (test_clock() - t_start) / 1000;
                                        sync_cnt++;

                                        if (t_elapsed_ms > max_sync_elapsed_ms)
                                                max_sync_elapsed_ms =
                                                    t_elapsed_ms;

                                        TEST_ASSERT(
                                            !error,
                                            "commit_sync #%d failed: %s",
                                            sync_cnt,
                                            error ? rd_kafka_error_string(error)
                                                  : "");

                                        if (partitions) {
                                                for (i = 0;
                                                     i < partitions->cnt;
                                                     i++) {
                                                        rd_kafka_topic_partition_t
                                                            *rktpar =
                                                                &partitions
                                                                     ->elems[i];
                                                        TEST_ASSERT(
                                                            rktpar->err ==
                                                                RD_KAFKA_RESP_ERR_NO_ERROR,
                                                            "commit_sync #%d: "
                                                            "%s [%" PRId32
                                                            "] got %s",
                                                            sync_cnt,
                                                            rktpar->topic,
                                                            rktpar->partition,
                                                            rd_kafka_err2str(
                                                                rktpar->err));
                                                }
                                                rd_kafka_topic_partition_list_destroy(
                                                    partitions);
                                        }

                                        TEST_SAY("commit_sync #%d OK "
                                                 "in %" PRId64
                                                 "ms (consumed %d, "
                                                 "%d async calls)\n",
                                                 sync_cnt, t_elapsed_ms,
                                                 consumed, async_cnt);
                                        acked_since_last_sync = 0;
                                }
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        /* Final commit_sync for any remaining acks */
        if (acked_since_last_sync > 0) {
                partitions = NULL;
                error = rd_kafka_share_commit_sync(rkshare, 30000, &partitions);
                sync_cnt++;
                TEST_ASSERT(!error, "final commit_sync failed: %s",
                            error ? rd_kafka_error_string(error) : "");
                RD_IF_FREE(partitions, rd_kafka_topic_partition_list_destroy);
                TEST_SAY("Final commit_sync #%d OK\n", sync_cnt);
        }

        TEST_SAY("Consumed %d, async commits=%d, sync commits=%d, "
                 "max sync elapsed=%" PRId64 "ms\n",
                 consumed, async_cnt, sync_cnt, max_sync_elapsed_ms);
        TEST_ASSERT(consumed == 50, "Expected 50, got %d", consumed);

        /* Verify commit_sync completed quickly — acks were mostly
         * already sent by commit_async, so sync should not wait long.
         * Allow up to 5000ms to account for broker round-trip. */
        TEST_ASSERT(max_sync_elapsed_ms < 5000,
                    "commit_sync took too long (%" PRId64
                    "ms), expected < 5000ms since acks were "
                    "mostly sent by commit_async",
                    max_sync_elapsed_ms);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Second consumer: should get 0 records */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 5) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Second consumer got %d records (expected 0)\n", consumed);
        TEST_ASSERT(consumed == 0,
                    "Second consumer got %d records, expected 0 after "
                    "mixed async+sync commits",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 11: Mock — broker dispatch priority (pending_commit_sync
 *  dispatched before async_ack_details).
 *
 *  Produce 30 messages. Consume all 30 in one batch. Then:
 *  - ACCEPT first 10, commit_async → inflight (push entry 1: 2s RTT)
 *  - ACCEPT next 10, commit_async → cached in async_ack_details
 *  - ACCEPT last 10, commit_sync(5000) → pending_commit_sync
 *
 *  When inflight completes at ~2s, broker dispatches next request.
 *  If pending_commit_sync has priority (correct): sync dispatched
 *  next (push entry 2: 2s RTT), completes at ~4s < 5s → NO_ERROR.
 *  If async_ack_details has priority (wrong): async dispatched
 *  next (2s RTT), then sync (2s RTT), completes at ~6s > 5s →
 *  TIMED_OUT.
 *
 *  Verification:
 *  1. commit_sync returns NO_ERROR and completes in ~4s (3500-4500ms)
 *  2. Per-partition results show NO_ERROR
 *  3. Second consumer gets 0 records (all acks processed)
 * =================================================================== */
static void do_test_mock_broker_dispatch_priority(void) {
        test_ctx_t ctx = test_ctx_new();
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        const char *topic = "mock-cs-dispatch-prio";
        const char *t     = topic;
        const int msgcnt  = 30;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_message_t *msg_handles[30];
        size_t rcvd;
        size_t j;
        int consumed;
        int attempts;
        int i;
        int32_t leader_broker_id = 1;
        rd_ts_t t_start, t_elapsed_ms;

        SUB_TEST();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        rd_kafka_mock_partition_set_leader(ctx.mcluster, topic, 0,
                                           leader_broker_id);

        mock_produce_messages(ctx.producer, topic, msgcnt);

        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-prio", "explicit");
        subscribe_consumer(rkshare, &t, 1);

        /* Consume all 30 records in one batch, keep handles */
        consumed = 0;
        attempts = 0;
        while (consumed == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err && consumed < msgcnt)
                                msg_handles[consumed++] = rkmessages[j];
                        else
                                rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumed %d/%d messages\n", consumed, msgcnt);
        TEST_ASSERT(consumed == msgcnt, "Expected %d, got %d", msgcnt,
                    consumed);

        /* Push 3 ShareAcknowledge entries with 2000ms RTT each on
         * the leader broker. These are consumed in order:
         *  Entry 1: first commit_async inflight request (~2s)
         *  Entry 2: pending_commit_sync (if priority correct) (~2s)
         *  Entry 3: async_ack_details (dispatched last) (~2s) */
        rd_kafka_mock_broker_push_request_error_rtts(
            ctx.mcluster, leader_broker_id, RD_KAFKAP_ShareAcknowledge, 3,
            RD_KAFKA_RESP_ERR_NO_ERROR, 2000,
            RD_KAFKA_RESP_ERR_NO_ERROR, 2000,
            RD_KAFKA_RESP_ERR_NO_ERROR, 2000);

        /* ACCEPT first 10, commit_async → sends first
         * ShareAcknowledge (inflight, push entry 1: 2s RTT) */
        for (i = 0; i < 10; i++)
                rd_kafka_share_acknowledge(rkshare, msg_handles[i]);

        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async #1 failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_SAY("commit_async #1 sent (inflight, 2s RTT)\n");

        /* Small delay to ensure async request is dispatched to
         * broker thread before next commit */
        rd_usleep(200 * 1000, NULL);

        /* ACCEPT next 10, commit_async → broker busy,
         * cached in async_ack_details */
        for (i = 10; i < 20; i++)
                rd_kafka_share_acknowledge(rkshare, msg_handles[i]);

        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async #2 failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_SAY("commit_async #2 sent (cached in async_ack_details)\n");

        /* ACCEPT last 10, commit_sync(5000ms) → broker still busy,
         * stored in pending_commit_sync.
         *
         * Timeline if sync has dispatch priority (correct):
         *   t=0s:  inflight request sent
         *   t=2s:  inflight completes, sync dispatched (entry 2)
         *   t=4s:  sync completes → NO_ERROR (4s < 5s timeout)
         *
         * Timeline if async has priority (wrong):
         *   t=0s:  inflight request sent
         *   t=2s:  inflight completes, async dispatched (entry 2)
         *   t=4s:  async completes, sync dispatched (entry 3)
         *   t=5s:  sync timeout fires → TIMED_OUT (5s < 6s) */
        for (i = 20; i < 30; i++)
                rd_kafka_share_acknowledge(rkshare, msg_handles[i]);

        TEST_SAY("Calling commit_sync(5000ms)\n");
        t_start    = test_clock();
        partitions = NULL;
        error = rd_kafka_share_commit_sync(rkshare, 5000, &partitions);
        t_elapsed_ms = (test_clock() - t_start) / 1000;

        TEST_SAY("commit_sync returned after %" PRId64 "ms, error=%s\n",
                 t_elapsed_ms,
                 error ? rd_kafka_error_string(error) : "NO_ERROR");

        /* Verify no error */
        if (error) {
                TEST_ASSERT(rd_kafka_error_code(error) !=
                                RD_KAFKA_RESP_ERR__TIMED_OUT,
                            "commit_sync TIMED_OUT after %" PRId64
                            "ms — pending_commit_sync was NOT dispatched "
                            "before async_ack_details. "
                            "Dispatch priority is broken.",
                            t_elapsed_ms);
                rd_kafka_error_destroy(error);
        } else {
                TEST_SAY("commit_sync returned NO_ERROR — dispatch "
                         "priority is correct\n");
        }

        /* Verify timing: should complete in ~4s (2s inflight + 2s sync),
         * not ~6s (2s inflight + 2s async + 2s sync). */
        TEST_ASSERT(t_elapsed_ms >= 3500 && t_elapsed_ms <= 4500,
                    "Expected commit_sync to complete in ~4s (3500-4500ms), "
                    "got %" PRId64 "ms. If >5s, dispatch priority is wrong.",
                    t_elapsed_ms);

        /* Verify per-partition results show NO_ERROR */
        TEST_ASSERT(partitions != NULL,
                    "Expected per-partition results, got NULL");

        for (i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                TEST_SAY("  %s [%" PRId32 "]: %s\n", rktpar->topic,
                         rktpar->partition, rd_kafka_err2str(rktpar->err));
                TEST_ASSERT(rktpar->err == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected NO_ERROR for %s [%" PRId32 "], got %s",
                            rktpar->topic, rktpar->partition,
                            rd_kafka_err2str(rktpar->err));
        }

        rd_kafka_topic_partition_list_destroy(partitions);

        /* Destroy message handles */
        for (i = 0; i < msgcnt; i++)
                rd_kafka_message_destroy(msg_handles[i]);

        /* Wait for remaining async to complete */
        rd_sleep(3);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Second consumer: should get 0 records since all acks
         * were processed successfully */
        rkshare = create_mock_share_consumer(ctx.bootstraps,
                                              "sg-mock-cs-prio", "implicit");
        subscribe_consumer(rkshare, &t, 1);

        consumed = 0;
        attempts = 0;
        while (attempts++ < 5) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Second consumer got %d records (expected 0)\n", consumed);
        TEST_ASSERT(consumed == 0,
                    "Second consumer got %d records, expected 0 "
                    "(all acks should have been processed)",
                    consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


int main_0176_share_consumer_commit_sync(int argc, char **argv) {
        /* Real broker tests */
        do_test_basic_implicit_commit_sync();
        do_test_basic_explicit_commit_sync();
        do_test_no_pending_acks();
        do_test_commit_sync_prevents_redelivery();
        do_test_mixed_ack_types();
        do_test_multiple_commit_sync_calls();
        do_test_multi_topic_partition();
        do_test_mixed_commit_types();

        /* Mock broker tests */
        do_test_mock_uses_share_acknowledge();
        do_test_mock_commit_sync_timeout();
        do_test_mock_broker_dispatch_priority();

        return 0;
}