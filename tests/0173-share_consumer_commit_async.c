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
 * @brief Share consumer rd_kafka_share_commit_async() API tests.
 *
 * Tests the commit_async API in both implicit and explicit ack modes.
 * Verifies that commit_async commits acknowledged records so they are
 * not redelivered, and that it properly extracts acks from the inflight
 * map (preventing consume_batch from re-sending the same acks).
 *
 * Mixed ack types: ACCEPT (~50%), RELEASE (~40%), REJECT (~10%)
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



/**
 * @brief Determine random ack type with distribution:
 *        ACCEPT ~50%, RELEASE ~40%, REJECT ~10%
 */
static rd_kafka_share_AcknowledgeType_t get_random_ack_type(void) {
        int r = jitter(0, 99);
        if (r < 50)
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
        else if (r < 90)
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
        else
                return RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;
}


/* ===================================================================
 *  Implicit ack mode — commit_async commits acks independently of
 *  consume_batch piggybacking.
 *
 *  Consumer 1 (implicit) consumes first batch, calls commit_async,
 *  waits, closes. Consumer 2 consumes remaining and verifies no
 *  offset overlap with consumer 1.
 * =================================================================== */
static void do_test_implicit_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-implicit-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed1 = 0, consumed2 = 0;
        int attempts = 0;
        int64_t *c1_offsets;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-impl-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        c1_offsets = rd_calloc(MAX_MSGS, sizeof(*c1_offsets));

        /* Wait for first batch of records */
        while (consumed1 == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                c1_offsets[consumed1++] = rkmessages[j]->offset;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer 1 consumed %d messages in first batch\n", consumed1);
        TEST_ASSERT(consumed1 > 0, "Consumer 1 got no records");

        /* commit_async without any subsequent consume_batch */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        rd_sleep(3);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Second consumer: consume remaining records and verify none
         * overlap with consumer 1's committed records.
         * No lock wait needed — implicit mode close tears down the
         * connection and broker releases records immediately. */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);
        rd_sleep(3);

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
                        if (!rkmessages[j]->err) {
                                int k;

                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]) == 1,
                                    "Consumer 2 got redelivered record at "
                                    "offset %" PRId64 " (delivery_count=%d)",
                                    rkmessages[j]->offset,
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]));

                                for (k = 0; k < consumed1; k++) {
                                        TEST_ASSERT(
                                            rkmessages[j]->offset !=
                                                c1_offsets[k],
                                            "Consumer 2 got offset %" PRId64
                                            " which was committed by "
                                            "consumer 1",
                                            rkmessages[j]->offset);
                                }
                                consumed2++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY(
            "Consumer 2 got %d remaining messages, "
            "none overlapped with consumer 1's %d\n",
            consumed2, consumed1);

        rd_free(c1_offsets);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Explicit ack mode — commit_async commits explicit ACCEPT acks.
 *
 *  Consumer 1 (explicit) consumes first batch, ACCEPTs each record,
 *  calls commit_async, waits, closes. Consumer 2 consumes remaining
 *  and verifies no offset overlap with consumer 1.
 * =================================================================== */
static void do_test_explicit_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-explicit-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed1 = 0, consumed2 = 0;
        int attempts = 0;
        int64_t *c1_offsets;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-expl-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        c1_offsets = rd_calloc(MAX_MSGS, sizeof(*c1_offsets));

        /* Wait for first batch of records */
        while (consumed1 == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                c1_offsets[consumed1++] = rkmessages[j]->offset;
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer 1 consumed %d messages in first batch\n", consumed1);
        TEST_ASSERT(consumed1 > 0, "Consumer 1 got no records");

        /* commit_async without any subsequent consume_batch */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        rd_sleep(3);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Records are either committed by the last commit_async or
         * released on the broker side when the connection is closed.
         * No lock wait needed.
         * TODO KIP-932: When share consumer close is fully implemented,
         * these tests may need to wait for the acquisition lock
         * timeout before closing as close will commit acknowledged
         * records in explicit acknowledgement mode. */

        /* Second consumer: consume remaining and verify no overlap */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);
        rd_sleep(3);

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
                        if (!rkmessages[j]->err) {
                                int k;

                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]) == 1,
                                    "Consumer 2 got redelivered record at "
                                    "offset %" PRId64 " (delivery_count=%d)",
                                    rkmessages[j]->offset,
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]));

                                for (k = 0; k < consumed1; k++) {
                                        TEST_ASSERT(
                                            rkmessages[j]->offset !=
                                                c1_offsets[k],
                                            "Consumer 2 got offset %" PRId64
                                            " which was committed by "
                                            "consumer 1",
                                            rkmessages[j]->offset);
                                }
                                consumed2++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY(
            "Consumer 2 got %d remaining messages, "
            "none overlapped with consumer 1's %d\n",
            consumed2, consumed1);

        rd_free(c1_offsets);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Mixed ack types — commit_async with ACCEPT/RELEASE/REJECT.
 *
 *  Consumer 1 (explicit) acks each record randomly (ACCEPT ~50%,
 *  RELEASE ~40%, REJECT ~10%), calls commit_async after each batch.
 *  Redeliveries (delivery_count > 1) are ACCEPT'd. Drain loop
 *  flushes remaining redeliveries. Consumer 2 gets unredelivered
 *  RELEASE'd records.
 * =================================================================== */
static void do_test_mixed_acks_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-mixed-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0, redelivered = 0;
        int released_cnt = 0;
        int attempts     = 0;
        int64_t *released_offsets;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-mixed-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        released_offsets = rd_calloc(MAX_MSGS, sizeof(*released_offsets));

        /* Consume all records and handle redeliveries in the same loop */
        while ((consumed < MAX_MSGS || redelivered < released_cnt) &&
               attempts++ < 100) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                if (rd_kafka_message_delivery_count(
                                        rkmessages[j]) > 1) {
                                        /* Redelivered — verify it was
                                         * RELEASE'd and ACCEPT it */
                                        int k;
                                        rd_bool_t found = rd_false;

                                        for (k = 0; k < released_cnt; k++) {
                                                if (rkmessages[j]->offset ==
                                                    released_offsets[k]) {
                                                        found = rd_true;
                                                        released_offsets[k] =
                                                            -1;
                                                        break;
                                                }
                                        }
                                        TEST_ASSERT(
                                            found,
                                            "Redelivered offset %" PRId64
                                            " was not RELEASE'd",
                                            rkmessages[j]->offset);

                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[j]);
                                        redelivered++;
                                } else {
                                        /* New record — ack with random type */
                                        rd_kafka_share_AcknowledgeType_t
                                            ack_type = get_random_ack_type();

                                        if (ack_type ==
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT) {
                                                rd_kafka_share_acknowledge(
                                                    rkshare, rkmessages[j]);
                                        } else {
                                                if (ack_type ==
                                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE)
                                                        released_offsets
                                                            [released_cnt++] =
                                                                rkmessages[j]
                                                                    ->offset;
                                                rd_kafka_share_acknowledge_type(
                                                    rkshare, rkmessages[j],
                                                    ack_type);
                                        }
                                        consumed++;
                                }
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }

                error = rd_kafka_share_commit_async(rkshare);
                TEST_ASSERT(!error, "commit_async failed: %s",
                            error ? rd_kafka_error_string(error) : "");
        }

        TEST_SAY("Consumed %d/%d, released %d, redelivered %d/%d\n", consumed,
                 MAX_MSGS, released_cnt, redelivered, released_cnt);
        TEST_ASSERT(consumed == MAX_MSGS, "Expected %d consumed, got %d",
                    MAX_MSGS, consumed);
        TEST_ASSERT(redelivered == released_cnt,
                    "Expected %d redelivered, got %d", released_cnt,
                    redelivered);

        rd_free(released_offsets);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}



/* ===================================================================
 *  Multiple topics x partitions — commit_async across many
 *  topic-partitions.
 *
 *  Creates 10 topics x 5 partitions. Runs 20 rounds: each round
 *  produces messages, consumes them, and calls commit_async.
 * =================================================================== */
static void do_test_multi_topic_partition(void) {
        const int topic_cnt      = 10;
        const int part_cnt       = 5;
        const int total_parts    = topic_cnt * part_cnt;
        const int rounds         = 20;
        const int msgs_per_part  = 10;
        const int msgs_per_round = msgs_per_part * total_parts;
        const char *group        = "commit-async-multi-tp";
        const char *topics[10];
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int t, p, round;
        int total_consumed = 0;

        SUB_TEST();

        for (t = 0; t < topic_cnt; t++) {
                topics[t] =
                    rd_strdup(test_mk_topic_name("0173-ca-multi-tp", 1));
                test_create_topic_wait_exists(NULL, topics[t], part_cnt, -1,
                                              60 * 1000);
        }

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, topics, topic_cnt);

        for (round = 0; round < rounds; round++) {
                int consumed = 0;
                int attempts = 0;

                for (t = 0; t < topic_cnt; t++) {
                        for (p = 0; p < part_cnt; p++)
                                test_produce_msgs_easy(topics[t], 0, p,
                                                       msgs_per_part);
                }

                while (consumed < msgs_per_round && attempts++ < 100) {
                        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
                        size_t rcvd = 0;
                        size_t j;

                        error = rd_kafka_share_consume_batch(rkshare, 5000,
                                                             rkmessages, &rcvd);
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

                TEST_SAY("Round %d: consumed %d/%d messages\n", round, consumed,
                         msgs_per_round);
                TEST_ASSERT(consumed == msgs_per_round,
                            "Round %d: Expected %d, got %d", round,
                            msgs_per_round, consumed);

                error = rd_kafka_share_commit_async(rkshare);
                TEST_ASSERT(!error, "Round %d: commit_async failed: %s", round,
                            error ? rd_kafka_error_string(error) : "");

                total_consumed += consumed;
        }

        TEST_SAY("Total consumed across %d rounds: %d\n", rounds,
                 total_consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        for (t = 0; t < topic_cnt; t++)
                rd_free((void *)topics[t]);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Produce-consume loop — mixing commit_async and consume_batch
 *  piggybacking.
 *
 *  5 rounds: each round produces N, consumes N. Even rounds use
 *  commit_async, odd rounds let consume_batch handle acks on the
 *  next poll.
 * =================================================================== */
static void do_test_produce_consume_loop(void) {
        const char *topic;
        const char *group = "commit-async-loop";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int round;
        const int rounds         = 5;
        const int msgs_per_round = MAX_MSGS / rounds;
        int total_consumed       = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-loop", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        for (round = 0; round < rounds; round++) {
                int consumed = 0;
                int attempts = 0;

                test_produce_msgs_easy(topic, 0, 0, msgs_per_round);
                TEST_SAY("Round %d: produced %d messages\n", round,
                         msgs_per_round);

                while (consumed < msgs_per_round && attempts++ < 100) {
                        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
                        size_t rcvd = 0;
                        size_t j;

                        error = rd_kafka_share_consume_batch(rkshare, 5000,
                                                             rkmessages, &rcvd);
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

                TEST_SAY("Round %d: consumed %d/%d messages\n", round, consumed,
                         msgs_per_round);
                TEST_ASSERT(consumed == msgs_per_round,
                            "Round %d: Expected %d, got %d", round,
                            msgs_per_round, consumed);

                if (round % 2 == 0) {
                        error = rd_kafka_share_commit_async(rkshare);
                        TEST_ASSERT(!error, "Round %d: commit_async failed: %s",
                                    round,
                                    error ? rd_kafka_error_string(error) : "");
                        TEST_SAY("Round %d: used commit_async\n", round);
                } else {
                        TEST_SAY(
                            "Round %d: letting consume_batch handle acks\n",
                            round);
                }

                total_consumed += consumed;
        }

        TEST_SAY("Total consumed across %d rounds: %d\n", rounds,
                 total_consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Multi-round mixed acks — per-round produce, consume, and
 *  commit_async with same-consumer redelivery handling.
 *
 *  3 rounds: each round produces msgs_per_round, consumes them,
 *  acks randomly (ACCEPT/RELEASE/REJECT), calls commit_async.
 *  Redeliveries (delivery_count > 1) are ACCEPT'd inline within
 *  the same round. Verifies consumed + redelivered counts per round.
 * =================================================================== */
static void do_test_multi_round_mixed_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-multi-round-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int round;
        const int rounds         = 3;
        const int msgs_per_round = MAX_MSGS / rounds;
        int total_consumed       = 0;
        int total_released       = 0;
        int total_redelivered    = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-mr-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        for (round = 0; round < rounds; round++) {
                test_produce_msgs_easy(topic, 0, 0, msgs_per_round);
                int consumed     = 0;
                int released_cnt = 0;
                int redelivered  = 0;
                int attempts     = 0;

                /* Consume msgs_per_round new records, handling redeliveries
                 * from previous rounds/batches inline */
                while (
                    (consumed < msgs_per_round || redelivered < released_cnt) &&
                    attempts++ < 100) {
                        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
                        size_t rcvd = 0;
                        size_t j;

                        error = rd_kafka_share_consume_batch(rkshare, 5000,
                                                             rkmessages, &rcvd);
                        if (error) {
                                rd_kafka_error_destroy(error);
                                continue;
                        }

                        for (j = 0; j < rcvd; j++) {
                                if (!rkmessages[j]->err) {
                                        if (rd_kafka_message_delivery_count(
                                                rkmessages[j]) > 1) {
                                                rd_kafka_share_acknowledge(
                                                    rkshare, rkmessages[j]);
                                                redelivered++;
                                        } else {
                                                rd_kafka_share_AcknowledgeType_t
                                                    ack_type =
                                                        get_random_ack_type();

                                                if (ack_type ==
                                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT) {
                                                        rd_kafka_share_acknowledge(
                                                            rkshare,
                                                            rkmessages[j]);
                                                } else {
                                                        if (ack_type ==
                                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE)
                                                                released_cnt++;
                                                        rd_kafka_share_acknowledge_type(
                                                            rkshare,
                                                            rkmessages[j],
                                                            ack_type);
                                                }
                                                consumed++;
                                        }
                                }
                                rd_kafka_message_destroy(rkmessages[j]);
                        }

                        error = rd_kafka_share_commit_async(rkshare);
                        TEST_ASSERT(!error, "Round %d: commit_async failed: %s",
                                    round,
                                    error ? rd_kafka_error_string(error) : "");
                }

                TEST_SAY(
                    "Round %d: consumed %d/%d, released %d, "
                    "redelivered %d/%d\n",
                    round, consumed, msgs_per_round, released_cnt, redelivered,
                    released_cnt);
                TEST_ASSERT(consumed == msgs_per_round,
                            "Round %d: expected %d consumed, got %d", round,
                            msgs_per_round, consumed);
                TEST_ASSERT(redelivered == released_cnt,
                            "Round %d: expected %d redelivered, got %d", round,
                            released_cnt, redelivered);

                total_consumed += consumed;
                total_released += released_cnt;
                total_redelivered += redelivered;
        }

        TEST_SAY("Total: consumed %d, released %d, redelivered %d\n",
                 total_consumed, total_released, total_redelivered);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  commit_async with no pending acks — returns NULL (no error).
 * =================================================================== */
static void do_test_no_pending_acks(void) {
        const char *group = "commit-async-no-pending";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;

        SUB_TEST();

        rkshare = create_share_consumer(group, "implicit");

        error = rd_kafka_share_commit_async(rkshare);
        TEST_SAY("commit_async with no acks: error=%s\n",
                 error ? rd_kafka_error_string(error) : "NULL");
        TEST_ASSERT(!error, "Expected NULL when no pending acks, got error: %s",
                    error ? rd_kafka_error_string(error) : "");

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Multiple consecutive commit_async calls — verifies no ack
 *  duplication or loss.
 *
 *  Produce first set, consume, produce second set, call commit_async
 *  3 times, then consume and verify count equals second produce amount.
 * =================================================================== */
static void do_test_multiple_commit_async_calls(void) {
        const char *topic;
        const char *group = "commit-async-multiple-calls";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const int first_produce  = MAX_MSGS / 2;
        const int second_produce = MAX_MSGS / 2;
        int consumed = 0, consumed2 = 0, call;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-multi-call", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        test_produce_msgs_easy(topic, 0, 0, first_produce);

        while (consumed < first_produce && attempts++ < 100) {
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
                        if (!rkmessages[j]->err)
                                consumed++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("First set: consumed %d/%d messages\n", consumed,
                 first_produce);
        TEST_ASSERT(consumed == first_produce, "Expected %d, got %d",
                    first_produce, consumed);

        test_produce_msgs_easy(topic, 0, 0, second_produce);

        for (call = 0; call < 10; call++) {
                error = rd_kafka_share_commit_async(rkshare);
                TEST_SAY("commit_async call %d: error=%s\n", call,
                         error ? rd_kafka_error_string(error) : "NULL");
                TEST_ASSERT(!error, "commit_async call %d failed: %s", call,
                            error ? rd_kafka_error_string(error) : "");
        }

        attempts = 0;
        while (consumed2 < second_produce && attempts++ < 100) {
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
                        if (!rkmessages[j]->err)
                                consumed2++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("After multiple commits: consumed %d/%d messages\n", consumed2,
                 second_produce);
        TEST_ASSERT(consumed2 == second_produce,
                    "Expected %d (second produce), got %d", second_produce,
                    consumed2);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  commit_async between produce rounds — correctly separates acks.
 *
 *  Produce N, consume N, commit_async, wait for lock timeout,
 *  produce N more, consume N, commit_async. Second consumer verifies
 *  nothing left.
 * =================================================================== */
static void do_test_commit_between_produces(void) {
        const char *topic;
        const char *group = "commit-async-between-produces";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        const int half = MAX_MSGS / 2;
        int consumed1 = 0, consumed2 = 0, redelivered = 0;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-between", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        rkshare = create_share_consumer(group, "implicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* First half: produce, wait for records, commit_async */
        test_produce_msgs_easy(topic, 0, 0, half);

        while (consumed1 == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed1++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("First half: consumed %d messages\n", consumed1);
        TEST_ASSERT(consumed1 == half, "First half: expected %d, got %d", half,
                    consumed1);

        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        /* Wait for acquisition lock timeout so first half's acks
         * are fully committed or released before producing the second
         * half */
        rd_sleep(35);

        /* Second half: produce more, wait for records, commit_async */
        test_produce_msgs_easy(topic, 0, 0, half);

        attempts = 0;
        while (consumed2 == 0 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                consumed2++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Second half: consumed %d messages\n", consumed2);
        TEST_ASSERT(consumed2 == half, "Second half: expected %d, got %d", half,
                    consumed2);

        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        rd_sleep(3);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* No lock wait needed — implicit mode close tears down the
         * connection and broker releases records immediately. */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);
        rd_sleep(3);

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
                        if (!rkmessages[j]->err) {
                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]) >= 2,
                                    "Consumer 2 got non-redelivered record at "
                                    "offset %" PRId64 " (delivery_count=%d)",
                                    rkmessages[j]->offset,
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]));
                                redelivered++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer 2 got %d messages (expected 0)\n", redelivered);
        TEST_ASSERT(redelivered == 0,
                    "Expected 0 redelivered for consumer 2, got %d",
                    redelivered);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  All RELEASE — commit_async with all-RELEASE acks causes full
 *  redelivery within the same consumer.
 *
 *  Consumer (explicit) RELEASEs every new record and ACCEPT's
 *  redeliveries (delivery_count > 1). Verifies all original records
 *  are consumed and all redeliveries are received.
 * =================================================================== */
static void do_test_all_release_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-all-release-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int consumed = 0, redelivered = 0;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-allrel-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume all records: RELEASE new records, ACCEPT redeliveries.
         * Loop until all original records are consumed AND all
         * redeliveries are received. */
        while ((consumed < MAX_MSGS || redelivered < consumed) &&
               attempts++ < 200) {
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
                                if (rd_kafka_message_delivery_count(
                                        rkmessages[j]) > 1) {
                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[j]);
                                        redelivered++;
                                } else {
                                        rd_kafka_share_acknowledge_type(
                                            rkshare, rkmessages[j],
                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
                                        consumed++;
                                }
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }

                error = rd_kafka_share_commit_async(rkshare);
                TEST_ASSERT(!error, "commit_async failed: %s",
                            error ? rd_kafka_error_string(error) : "");
        }

        TEST_SAY("Consumed %d/%d, redelivered %d/%d\n", consumed, MAX_MSGS,
                 redelivered, consumed);
        TEST_ASSERT(consumed == MAX_MSGS, "Expected %d consumed, got %d",
                    MAX_MSGS, consumed);
        TEST_ASSERT(redelivered == consumed, "Expected %d redelivered, got %d",
                    consumed, redelivered);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  All REJECT — commit_async with all-REJECT acks. Records are not
 *  redelivered.
 *
 *  Consumer 1 (explicit) REJECTs every record, calls commit_async.
 *  Consumer 2 verifies 0 messages (REJECT'd records are archived).
 * =================================================================== */
static void do_test_all_reject_second_consumer(void) {
        const char *topic;
        const char *group = "commit-async-all-reject-second";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int consumed = 0, redelivered;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-allrej-2nd", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        while (consumed < MAX_MSGS && attempts++ < 100) {
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
                                rd_kafka_share_acknowledge_type(
                                    rkshare, rkmessages[j],
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }

                error = rd_kafka_share_commit_async(rkshare);
                TEST_ASSERT(!error, "commit_async failed: %s",
                            error ? rd_kafka_error_string(error) : "");
        }

        TEST_SAY("Consumer 1 consumed %d/%d messages\n", consumed, MAX_MSGS);
        TEST_ASSERT(consumed == MAX_MSGS, "Expected %d consumed, got %d",
                    MAX_MSGS, consumed);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Records are either committed by the last commit_async or
         * released on the broker side when the connection is closed.
         * No lock wait needed.
         * TODO KIP-932: When share consumer close is fully implemented,
         * these tests may need to wait for the acquisition lock
         * timeout before closing as close will commit acknowledged
         * records in explicit acknowledgement mode. */

        /* Second consumer verifies no redeliveries (REJECT is final) */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);
        rd_sleep(3);

        redelivered = 0;
        attempts    = 0;
        while (attempts++ < 5) {
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
                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]) >= 2,
                                    "Consumer 2 got non-redelivered record at "
                                    "offset %" PRId64 " (delivery_count=%d)",
                                    rkmessages[j]->offset,
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]));
                                redelivered++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer 2 got %d messages (expected 0)\n", redelivered);
        TEST_ASSERT(redelivered == 0,
                    "Expected 0 redelivered for consumer 2, got %d",
                    redelivered);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Per-record commit_async — acknowledges each record individually
 *  and calls commit_async after each acknowledgement.
 *
 *  Waits for acquisition lock timeout, then consumer 2 verifies all
 *  records were committed (0 redelivered).
 * =================================================================== */
static void do_test_per_record_commit_async(void) {
        const char *topic;
        const char *group = "commit-async-per-record";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        int consumed = 0, redelivered;
        int attempts = 0;

        SUB_TEST();

        topic = test_mk_topic_name("0173-ca-per-rec", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, MAX_MSGS);

        rkshare = create_share_consumer(group, "explicit");
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume all records, ACCEPT each individually with
         * commit_async after every record */
        while (consumed < MAX_MSGS && attempts++ < 100) {
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

                                error = rd_kafka_share_commit_async(rkshare);
                                TEST_ASSERT(
                                    !error, "commit_async at msg %d failed: %s",
                                    consumed,
                                    error ? rd_kafka_error_string(error) : "");
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumed %d/%d messages with per-record commit_async\n",
                 consumed, MAX_MSGS);
        TEST_ASSERT(consumed == MAX_MSGS, "Expected %d, got %d", MAX_MSGS,
                    consumed);

        /* Wait for async commits to propagate */
        rd_sleep(3);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        /* Records are either committed by the last commit_async or
         * released on the broker side when the connection is closed.
         * No lock wait needed.
         * TODO KIP-932: When share consumer close is fully implemented,
         * these tests may need to wait for the acquisition lock
         * timeout before closing as close will commit acknowledged
         * records in explicit acknowledgement mode. */

        /* Second consumer verifies no redeliveries */
        rkshare = create_share_consumer(group, "implicit");
        subscribe_consumer(rkshare, &topic, 1);
        rd_sleep(3);

        redelivered = 0;
        attempts    = 0;
        while (attempts++ < 5) {
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
                                TEST_ASSERT(
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]) >= 2,
                                    "Consumer 2 got non-redelivered record at "
                                    "offset %" PRId64 " (delivery_count=%d)",
                                    rkmessages[j]->offset,
                                    rd_kafka_message_delivery_count(
                                        rkmessages[j]));
                                redelivered++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumer 2 got %d messages (expected 0)\n", redelivered);
        TEST_ASSERT(redelivered == 0,
                    "Expected 0 redelivered for consumer 2, got %d",
                    redelivered);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Mock broker helpers.
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
produce_messages(rd_kafka_t *producer, const char *topic, int msgcnt) {
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

static rd_kafka_share_t *new_share_consumer(const char *bootstraps,
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

static void count_share_requests(rd_kafka_mock_cluster_t *mcluster,
                                 int *share_fetch_cntp,
                                 int *share_ack_cntp) {
        size_t cnt;
        size_t i;
        rd_kafka_mock_request_t **requests;

        *share_fetch_cntp = 0;
        *share_ack_cntp   = 0;

        requests = rd_kafka_mock_get_requests(mcluster, &cnt);

        for (i = 0; i < cnt; i++) {
                int16_t api_key = rd_kafka_mock_request_api_key(requests[i]);
                if (api_key == RD_KAFKAP_ShareFetch)
                        (*share_fetch_cntp)++;
                else if (api_key == RD_KAFKAP_ShareAcknowledge)
                        (*share_ack_cntp)++;
        }

        rd_kafka_mock_request_destroy_array(requests, cnt);
}


/* ===================================================================
 *  Mock broker — inflight request caching.
 *
 *  Consumes records, acknowledges each individually and calls
 *  commit_async after each acknowledgement. Verifies ShareFetch
 *  request count < commit_async call count, proving acks are cached
 *  when a request is already inflight (rkb_share_fetch_enqueued=true).
 * =================================================================== */
static void do_test_mock_inflight_caching(void) {
        test_ctx_t ctx = test_ctx_new();
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        const char *topic = "mock-inflight-cache";
        const int msgcnt  = 100;
        int consumed = 0, i = 0;
        int share_fetch_cnt, share_ack_cnt;
        int commit_cnt = 0;

        SUB_TEST();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        produce_messages(ctx.producer, topic, msgcnt);

        rkshare =
            new_share_consumer(ctx.bootstraps, "sg-mock-inflight", "explicit");

        {
                const char *t = topic;
                subscribe_consumer(rkshare, &t, 1);
        }

        /* Clear and start tracking requests before the consume+ack+commit
         * loop */
        rd_kafka_mock_start_request_tracking(ctx.mcluster);
        rd_kafka_mock_clear_requests(ctx.mcluster);

        /* Consume records batch-by-batch, acknowledging each record
         * individually and calling commit_async after each acknowledgement.
         * Most commit_async calls will find an inflight request and
         * cache the acks in rkb_share_async_ack_details instead of
         * sending a new ShareFetch request. */
        while (consumed < msgcnt && i < 30) {
                rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
                size_t rcvd = 0;
                size_t j;

                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                i++;
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;

                                error = rd_kafka_share_commit_async(rkshare);
                                TEST_ASSERT(
                                    !error, "commit_async at msg %d failed: %s",
                                    consumed,
                                    error ? rd_kafka_error_string(error) : "");
                                commit_cnt++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }
        TEST_SAY("Mock: consumed %d/%d\n", consumed, msgcnt);
        TEST_ASSERT(consumed == msgcnt, "Expected %d, got %d", msgcnt,
                    consumed);

        /* Wait for async ops to complete before counting requests */
        rd_sleep(3);

        count_share_requests(ctx.mcluster, &share_fetch_cnt, &share_ack_cnt);
        rd_kafka_mock_stop_request_tracking(ctx.mcluster);

        /* We called commit_async 100 times but the total number of
         * ShareFetch + ShareAcknowledge requests should be much fewer
         * due to inflight caching. Ack-only requests use ShareAcknowledge
         * RPC instead of ShareFetch. */
        TEST_SAY(
            "Mock: commit_async calls=%d, ShareFetch requests=%d, "
            "ShareAcknowledge requests=%d\n",
            commit_cnt, share_fetch_cnt, share_ack_cnt);
        TEST_ASSERT(share_ack_cnt > 0,
                    "Expected at least one ShareAcknowledge request, got %d",
                    share_ack_cnt);
        TEST_ASSERT(share_fetch_cnt + share_ack_cnt < commit_cnt,
                    "Expected fewer total share requests (%d + %d = %d) than "
                    "commit_async calls (%d) due to inflight caching",
                    share_fetch_cnt, share_ack_cnt,
                    share_fetch_cnt + share_ack_cnt, commit_cnt);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Acknowledgement callback helpers.
 * =================================================================== */

#define MAX_CB_OFFSETS 500

typedef struct ack_cb_state_s {
        int callback_cnt;
        int total_offsets;
        rd_kafka_resp_err_t last_err;
        mtx_t lock;
        cnd_t cond;
} ack_cb_state_t;

static void ack_cb_state_init(ack_cb_state_t *state) {
        memset(state, 0, sizeof(*state));
        mtx_init(&state->lock, mtx_plain);
        cnd_init(&state->cond);
}

static void ack_cb_state_destroy(ack_cb_state_t *state) {
        mtx_destroy(&state->lock);
        cnd_destroy(&state->cond);
}

static void share_ack_cb(rd_kafka_share_t *rkshare,
                         rd_kafka_share_partition_offsets_list_t *partitions,
                         rd_kafka_resp_err_t err,
                         void *opaque) {
        ack_cb_state_t *state = (ack_cb_state_t *)opaque;
        const rd_kafka_share_partition_offsets_t *entry;

        (void)rkshare;

        mtx_lock(&state->lock);
        state->callback_cnt++;
        state->last_err = err;

        entry = rd_kafka_share_partition_offsets_list_get(partitions, 0);
        if (entry)
                state->total_offsets +=
                    rd_kafka_share_partition_offsets_offsets_cnt(entry);

        cnd_signal(&state->cond);
        mtx_unlock(&state->lock);
}

static rd_kafka_share_t *create_share_consumer_with_cb(const char *group_id,
                                                       const char *ack_mode,
                                                       ack_cb_state_t *state) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "share.acknowledgement.mode", ack_mode, errstr,
                          sizeof(errstr));
        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf, share_ack_cb);
        rd_kafka_conf_set_opaque(conf, state);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);
        return rkshare;
}

static rd_bool_t wait_for_cb_with_poll(ack_cb_state_t *state,
                                       rd_kafka_share_t *rkshare,
                                       int min_callbacks,
                                       int timeout_ms) {
        rd_bool_t success = rd_false;
        int elapsed       = 0;
        int poll_interval = 100;
        rd_kafka_message_t *rkmessages[100];
        size_t rcvd;

        while (elapsed < timeout_ms) {
                rd_kafka_error_t *error = rd_kafka_share_consume_batch(
                    rkshare, poll_interval, rkmessages, &rcvd);
                if (error)
                        rd_kafka_error_destroy(error);

                for (size_t i = 0; i < rcvd; i++)
                        rd_kafka_message_destroy(rkmessages[i]);

                mtx_lock(&state->lock);
                if (state->callback_cnt >= min_callbacks) {
                        success = rd_true;
                        mtx_unlock(&state->lock);
                        break;
                }
                mtx_unlock(&state->lock);
                elapsed += poll_interval;
        }
        return success;
}


/* ===================================================================
 *  Test: commit_async callback invocation.
 *
 *  Verifies that share_acknowledgement_commit_cb is invoked after
 *  commit_async when acks are piggybacked on ShareFetch.
 * =================================================================== */
static void do_test_commit_async_callback(void) {
        const char *topic;
        const char *group = "commit-async-callback";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        ack_cb_state_t state;

        SUB_TEST();

        ack_cb_state_init(&state);

        topic = test_mk_topic_name("0173-ca-callback", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 50);

        rkshare = create_share_consumer_with_cb(group, "implicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume some messages */
        while (consumed < 20 && attempts++ < 30) {
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
        TEST_ASSERT(consumed > 0, "Expected to consume some messages");

        /* Call commit_async to trigger callback */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        /* Wait for callback */
        wait_for_cb_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback count=%d, total_offsets=%d, last_err=%s\n",
                 state.callback_cnt, state.total_offsets,
                 rd_kafka_err2name(state.last_err));

        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);
        TEST_ASSERT(state.total_offsets > 0,
                    "Expected offsets in callback, got %d",
                    state.total_offsets);

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_cb_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Negative Test: Acknowledge after commit_async.
 *
 *  After commit_async completes and acks are sent, the acknowledged
 *  messages are removed from inflight_acks map. Trying to acknowledge
 *  the same message again should return _STATE error.
 * =================================================================== */
static void do_test_ack_after_commit_async(void) {
        const char *topic;
        const char *group = "commit-async-ack-after-commit";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        rd_kafka_resp_err_t ack_err;
        ack_cb_state_t state;
        /* Store message info for re-ack attempt after commit */
        const char *saved_topic = NULL;
        int32_t saved_partition = -1;
        int64_t saved_offset    = -1;

        SUB_TEST();

        ack_cb_state_init(&state);

        topic = test_mk_topic_name("0173-ca-ack-after-commit", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 10);

        rkshare = create_share_consumer_with_cb(group, "explicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume messages, acknowledge all, save first message info */
        while (consumed < 5 && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err) {
                                /* Save first message info for later */
                                if (saved_offset < 0) {
                                        saved_topic = topic;
                                        saved_partition =
                                            rkmessages[j]->partition;
                                        saved_offset = rkmessages[j]->offset;
                                        TEST_SAY("Saved msg info: %s [%" PRId32
                                                 "] @ %" PRId64 "\n",
                                                 saved_topic, saved_partition,
                                                 saved_offset);
                                }
                                rd_kafka_share_acknowledge(rkshare,
                                                           rkmessages[j]);
                                consumed++;
                        }
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }

        TEST_SAY("Consumed and acknowledged %d messages\n", consumed);
        TEST_ASSERT(consumed >= 5, "Expected at least 5, got %d", consumed);
        TEST_ASSERT(saved_offset >= 0, "Expected to save at least 1 message");

        /* commit_async triggers sending acknowledgements */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_SAY("commit_async succeeded\n");

        /* Wait for callback to confirm acks were sent */
        wait_for_cb_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback: count=%d, last_err=%s\n", state.callback_cnt,
                 rd_kafka_err2name(state.last_err));
        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);

        /* Now try to acknowledge the same message again using the
         * offset-based API. This should fail with _STATE because the
         * message is no longer in inflight_acks. */
        ack_err = rd_kafka_share_acknowledge_offset(
            rkshare, saved_topic, saved_partition, saved_offset,
            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);

        TEST_SAY("Ack after commit returned: %s\n", rd_kafka_err2str(ack_err));
        TEST_ASSERT(ack_err == RD_KAFKA_RESP_ERR__STATE,
                    "Expected _STATE error when acknowledging after commit, "
                    "got %s",
                    rd_kafka_err2str(ack_err));

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_cb_state_destroy(&state);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test: Change ack type before commit_async.
 *
 *  Before commit_async is called, user can change their acknowledgement
 *  decision (e.g., RELEASE then ACCEPT). This should work and the
 *  final ack type should be committed.
 * =================================================================== */
static void do_test_change_ack_type_before_commit_async(void) {
        const char *topic;
        const char *group = "commit-async-change-ack-type";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        rd_kafka_message_t *test_msg = NULL;
        size_t rcvd;
        size_t j;
        int consumed = 0;
        int attempts = 0;
        rd_kafka_resp_err_t release_err, accept_err;
        ack_cb_state_t state;

        SUB_TEST();

        ack_cb_state_init(&state);

        topic = test_mk_topic_name("0173-ca-change-ack-type", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 10);

        rkshare = create_share_consumer_with_cb(group, "explicit", &state);
        set_group_offset_earliest(group);
        subscribe_consumer(rkshare, &topic, 1);

        /* Consume one message and keep handle */
        while (test_msg == NULL && attempts++ < 30) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err && test_msg == NULL) {
                                test_msg = rkmessages[j];
                                consumed++;
                        } else {
                                if (!rkmessages[j]->err) {
                                        rd_kafka_share_acknowledge(
                                            rkshare, rkmessages[j]);
                                        consumed++;
                                }
                                rd_kafka_message_destroy(rkmessages[j]);
                        }
                }
        }

        TEST_ASSERT(test_msg != NULL, "Expected to consume at least 1 message");
        TEST_SAY("Consumed %d messages, kept one for ack type change test\n",
                 consumed);

        /* First: RELEASE the message (user initially decides to release) */
        release_err = rd_kafka_share_acknowledge_type(
            rkshare, test_msg, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
        TEST_SAY("RELEASE returned: %s\n", rd_kafka_err2str(release_err));
        TEST_ASSERT(release_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected RELEASE to succeed, got %s",
                    rd_kafka_err2str(release_err));

        /* Second: Change mind and ACCEPT the same message before commit */
        accept_err = rd_kafka_share_acknowledge_type(
            rkshare, test_msg, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
        TEST_SAY("ACCEPT (changing from RELEASE) returned: %s\n",
                 rd_kafka_err2str(accept_err));
        TEST_ASSERT(accept_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected ACCEPT to succeed (changing ack type before "
                    "commit is allowed), got %s",
                    rd_kafka_err2str(accept_err));

        rd_kafka_message_destroy(test_msg);

        /* Commit async - should succeed with ACCEPT as final ack type */
        error = rd_kafka_share_commit_async(rkshare);
        TEST_ASSERT(!error, "commit_async failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_SAY("commit_async succeeded\n");

        /* Wait for callback */
        wait_for_cb_with_poll(&state, rkshare, 1, 10000);

        TEST_SAY("Callback: count=%d, last_err=%s\n", state.callback_cnt,
                 rd_kafka_err2name(state.last_err));

        TEST_ASSERT(state.callback_cnt >= 1,
                    "Expected at least 1 callback, got %d", state.callback_cnt);
        TEST_ASSERT(state.last_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected NO_ERROR in callback, got %s",
                    rd_kafka_err2name(state.last_err));

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        ack_cb_state_destroy(&state);

        SUB_TEST_PASS();
}


int main_0173_share_consumer_commit_async(int argc, char **argv) {
        /* Real broker tests */
        do_test_implicit_second_consumer();
        do_test_explicit_second_consumer();
        do_test_mixed_acks_second_consumer();
        do_test_multi_topic_partition();
        do_test_produce_consume_loop();
        do_test_multi_round_mixed_second_consumer();
        do_test_no_pending_acks();
        do_test_multiple_commit_async_calls();
        do_test_commit_between_produces();
        do_test_all_release_second_consumer();
        do_test_all_reject_second_consumer();
        do_test_per_record_commit_async();

        /* Callback test */
        do_test_commit_async_callback();

        /* Negative tests - explicit acknowledgement edge cases */
        do_test_ack_after_commit_async();
        do_test_change_ack_type_before_commit_async();

        /* Mock broker test */
        do_test_mock_inflight_caching();

        return 0;
}
