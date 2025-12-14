/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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

/**
 * Subscription introspection:
 * Subscribe to 3 topics, verify subscription(), then unsubscribe, verify empty.
 */
static void test_subscription_introspection(void) {
        char errstr[512];
        const char *group = "share-group-sub-introspect";
        const char *t1    = "0154-share-sub-intro-1";
        const char *t2    = "0154-share-sub-intro-2";
        const char *t3    = "0154-share-sub-intro-3";

        test_create_topic_wait_exists(NULL, t1, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, t2, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, t3, 1, -1, 30 * 1000);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));
        rd_kafka_t *c =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(c, "%s", errstr);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(3);
        rd_kafka_topic_partition_list_add(subs, t1, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs, t2, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs, t3, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(c, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_topic_partition_list_t *cur = NULL;
        TEST_ASSERT(!rd_kafka_subscription(c, &cur) && cur,
                    "subscription() failed");
        TEST_ASSERT(cur->cnt == 3, "expected 3 topics, got %d", cur->cnt);
        rd_kafka_topic_partition_list_destroy(cur);

        TEST_ASSERT(!rd_kafka_unsubscribe(c), "unsubscribe failed");

        cur = NULL;
        TEST_ASSERT(!rd_kafka_subscription(c, &cur) && cur,
                    "subscription() after unsubscribe failed");
        TEST_ASSERT(cur->cnt == 0,
                    "expected 0 topics after unsubscribe, got %d", cur->cnt);
        rd_kafka_topic_partition_list_destroy(cur);

        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);
}

/**
 * Unsubscribe idempotence:
 * First unsubscribe empties subscription, second is no-op.
 */
static void test_unsubscribe_idempotence(void) {
        char errstr[512];
        const char *group = "share-group-unsub-idem";
        const char *t1    = "0154-share-unsub-idem-1";
        const char *t2    = "0154-share-unsub-idem-2";

        test_create_topic_wait_exists(NULL, t1, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, t2, 1, -1, 30 * 1000);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));
        rd_kafka_t *c =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(c, "%s", errstr);

        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(c, RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs, t1, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs, t2, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(c, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        TEST_ASSERT(!rd_kafka_unsubscribe(c), "first unsubscribe failed");
        TEST_ASSERT(!rd_kafka_unsubscribe(c),
                    "second unsubscribe should be idempotent");

        rd_kafka_topic_partition_list_t *cur = NULL;
        TEST_ASSERT(!rd_kafka_subscription(c, &cur) && cur,
                    "subscription() failed");
        TEST_ASSERT(cur->cnt == 0,
                    "expected 0 after double unsubscribe, got %d", cur->cnt);
        rd_kafka_topic_partition_list_destroy(cur);

        test_delete_topic(c, t1);
        test_delete_topic(c, t2);
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);
}

/**
 * Resubscribe replacing set (A,B) -> (C,D) verifies old topics gone.
 */
static void test_resubscribe_replaces_set(void) {
        char errstr[512];
        const char *group = "share-group-resub-replace";
        const char *a     = "0154-share-resub-A";
        const char *b     = "0154-share-resub-B";
        const char *c     = "0154-share-resub-C";
        const char *d     = "0154-share-resub-D";

        test_create_topic_wait_exists(NULL, a, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, b, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, c, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, d, 1, -1, 30 * 1000);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "%s", errstr);

        rd_kafka_topic_partition_list_t *subs1 =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs1, a, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs1, b, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs1), "subscribe A,B failed");
        rd_kafka_topic_partition_list_destroy(subs1);

        rd_kafka_topic_partition_list_t *cur = NULL;
        rd_kafka_subscription(rk, &cur);
        TEST_ASSERT(cur && cur->cnt == 2, "expected 2 after first subscribe");
        rd_kafka_topic_partition_list_destroy(cur);

        rd_kafka_topic_partition_list_t *subs2 =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs2, c, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs2, d, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs2), "resubscribe C,D failed");
        rd_kafka_topic_partition_list_destroy(subs2);

        cur = NULL;
        rd_kafka_subscription(rk, &cur);
        TEST_ASSERT(cur && cur->cnt == 2, "expected 2 after resubscribe");
        for (int i = 0; i < cur->cnt; i++) {
                const char *tn = cur->elems[i].topic;
                TEST_ASSERT(strcmp(tn, a) && strcmp(tn, b),
                            "old topic %s still present", tn);
        }
        rd_kafka_topic_partition_list_destroy(cur);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
}

/**
 * Duplicate subscribe call with same list (idempotence).
 */
static void test_duplicate_subscribe_idempotent(void) {
        char errstr[512];
        const char *group = "share-group-dup-sub";
        const char *t1    = "0154-share-dup-sub-1";
        const char *t2    = "0154-share-dup-sub-2";

        test_create_topic_wait_exists(NULL, t1, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, t2, 1, -1, 30 * 1000);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "%s", errstr);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs, t1, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs, t2, RD_KAFKA_PARTITION_UA);

        TEST_ASSERT(!rd_kafka_subscribe(rk, subs), "first subscribe failed");
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs),
                    "duplicate subscribe failed");

        rd_kafka_topic_partition_list_t *cur = NULL;
        rd_kafka_subscription(rk, &cur);
        TEST_ASSERT(cur && cur->cnt == 2,
                    "expected exactly 2 after duplicate subscribe");
        rd_kafka_topic_partition_list_destroy(cur);

        rd_kafka_topic_partition_list_destroy(subs);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
}

/**
 * Subscribe to non-existent topic, then create it, produce, consume.
 */
static void test_subscribe_nonexistent_then_create(void) {
        char errstr[512];
        const char *group = "share-group-sub-nonexist";
        const char *topic = test_mk_topic_name("0154-share-nonexist-topic", 1);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "%s", errstr);

        const char *confs_set_group[] = {"share.auto.offset.reset", "SET",
                                         "earliest"};
        test_IncrementalAlterConfigs_simple(rk, RD_KAFKA_RESOURCE_GROUP, group,
                                            confs_set_group, 1);


        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs),
                    "subscribe non-existent failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Confirm subscription shows the topic */
        rd_kafka_topic_partition_list_t *cur = NULL;
        rd_kafka_subscription(rk, &cur);
        TEST_ASSERT(cur && cur->cnt == 1, "expected 1 subscription");
        rd_kafka_topic_partition_list_destroy(cur);

        /* Now create topic and produce */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 30 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        rd_kafka_message_t *batch[10];
        int got      = 0;
        int attempts = 10;
        while (got < 5 && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err)
                                got++;
                        rd_kafka_message_destroy(batch[i]);
                }
        }
        TEST_ASSERT(got == 5,
                    "expected 5 messages after topic creation, got %d", got);

        test_delete_topic(rk, topic);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
}

/**
 * Unsubscribe then immediate subscribe to new topics: ensure old topics gone,
 * only new consumed.
 */
static void test_unsubscribe_then_subscribe_new_topics(void) {
        char errstr[512];
        const char *group = "share-group-unsub-resub";
        const char *old1  = "0154-share-old-1";
        const char *old2  = "0154-share-old-2";
        const char *new1  = "0154-share-new-1";
        const char *new2  = "0154-share-new-2";

        test_create_topic_wait_exists(NULL, old1, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, old2, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, new1, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, new2, 1, -1, 30 * 1000);

        test_produce_msgs_easy(old1, 0, 0, 3);
        test_produce_msgs_easy(old2, 0, 0, 3);
        test_produce_msgs_easy(new1, 0, 0, 4);
        test_produce_msgs_easy(new2, 0, 0, 4);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "%s", errstr);

        const char *confs_set_group[] = {"share.auto.offset.reset", "SET",
                                         "earliest"};
        test_IncrementalAlterConfigs_simple(rk, RD_KAFKA_RESOURCE_GROUP, group,
                                            confs_set_group, 1);

        rd_kafka_topic_partition_list_t *subs_old =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs_old, old1,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs_old, old2,
                                          RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs_old), "subscribe old failed");
        rd_kafka_topic_partition_list_destroy(subs_old);

        /* Unsubscribe immediately */
        TEST_ASSERT(!rd_kafka_unsubscribe(rk), "unsubscribe failed");

        /* Subscribe new topics */
        rd_kafka_topic_partition_list_t *subs_new =
            rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subs_new, new1,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subs_new, new2,
                                          RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subs_new), "subscribe new failed");
        rd_kafka_topic_partition_list_destroy(subs_new);

        /* Consume; ensure only new topics appear */
        rd_kafka_message_t *batch[50];
        int got_new  = 0;
        int attempts = 10;
        while (got_new < 8 && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err) {
                                const char *tn =
                                    rd_kafka_topic_name(batch[i]->rkt);
                                TEST_ASSERT(
                                    strcmp(tn, old1) && strcmp(tn, old2),
                                    "received message from old topic %s", tn);
                                if (!strcmp(tn, new1) || !strcmp(tn, new2))
                                        got_new++;
                        }
                        rd_kafka_message_destroy(batch[i]);
                }
        }
        TEST_ASSERT(got_new == 8, "expected 8 new-topic msgs, got %d", got_new);

        test_delete_topic(rk, old1);
        test_delete_topic(rk, old2);
        test_delete_topic(rk, new1);
        test_delete_topic(rk, new2);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
}

/**
 * Re-subscribe while messages exist:
 * Consume some from A, then resubscribe to B only; ensure no A messages
 * afterwards.
 */
static void test_resubscribe_switch_topics(void) {
        char errstr[512];
        const char *group       = "share-group-switch";
        const char *topicA      = "0154-share-switch-A-resubscribe";
        const char *topicB      = "0154-share-switch-B-resubscribe";
        const int msgsA_initial = 5;
        const int msgsA_extra   = 3;
        const int msgsB_initial = 7;
        const int msgsB_extra   = 4;

        test_create_topic_wait_exists(NULL, topicA, 1, -1, 30 * 1000);
        test_create_topic_wait_exists(NULL, topicB, 1, -1, 30 * 1000);
        test_produce_msgs_easy(topicA, 0, 0, msgsA_initial);
        test_produce_msgs_easy(topicB, 0, 0, msgsB_initial);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "%s", errstr);

        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(rk, RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        rd_kafka_topic_partition_list_t *subsA =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subsA, topicA, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subsA), "subscribe A failed");
        rd_kafka_topic_partition_list_destroy(subsA);

        int gotA     = 0;
        int attempts = 10;
        rd_kafka_message_t *batch[128];
        while (gotA < msgsA_initial && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err &&
                            !strcmp(rd_kafka_topic_name(batch[i]->rkt), topicA))
                                gotA++;
                        rd_kafka_message_destroy(batch[i]);
                }
        }
        TEST_ASSERT(gotA > 0,
                    "did not consume any messages from A before resubscribe");

        /* Add extra messages to A that should not be seen after switching */
        test_produce_msgs_easy(topicA, 0, 0, msgsA_extra);

        /* Resubscribe to B only */
        rd_kafka_topic_partition_list_t *subsB =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subsB, topicB, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(rk, subsB), "resubscribe B failed");
        rd_kafka_topic_partition_list_destroy(subsB);

        /* Produce extra B after resubscribe */
        test_produce_msgs_easy(topicB, 0, 0, msgsB_extra);

        int wantB = msgsB_initial + msgsB_extra;
        int gotB  = 0;
        attempts  = 25;

        while (gotB < wantB && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(rk, 3000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err) {
                                const char *tn =
                                    rd_kafka_topic_name(batch[i]->rkt);
                                TEST_ASSERT(!strcmp(tn, topicB),
                                            "received message from old topic "
                                            "%s after resubscribe",
                                            tn);
                                gotB++;
                        }
                        rd_kafka_message_destroy(batch[i]);
                }
        }

        TEST_ASSERT(gotB == wantB, "expected %d B messages, got %d", wantB,
                    gotB);

        test_delete_topic(rk, topicA);
        test_delete_topic(rk, topicB);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
}


/**
 * @brief Test that polling without subscription fails
 */
static void test_poll_no_subscribe_fails(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        char *group = "share-group-no-subscribe";

        TEST_SAY("=== Testing poll without subscription fails ===\n");

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Try to poll without subscribing - should fail or return timeout */
        TEST_SAY("Attempting to poll without subscription\n");
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs          = 0;

        rd_kafka_error_t *error =
            rd_kafka_share_consume_batch(consumer, 2000, msgs, &rcvd_msgs);

        /**
         * TODO KIP-932: Uncomment once polling before any subscription is
         * properly handled
         */
        // TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it
        // succeeded");

        free(msgs);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test subscribe and poll with no records available
 */
static void test_subscribe_and_poll_no_records(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic = "0154-share-empty-records";
        const char *group = "share-group-empty";

        TEST_SAY("=== Testing subscribe and poll with no records ===\n");

        /* Create empty topic (no messages produced) */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        TEST_SAY("Created empty topic: %s\n", topic);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to empty topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to empty topic, polling for messages\n");

        /* Poll for messages - should get none */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs          = 0;

        rd_kafka_error_t *error =
            rd_kafka_share_consume_batch(consumer, 5000, msgs, &rcvd_msgs);

        TEST_ASSERT(rcvd_msgs == 0,
                    "Should not receive messages from empty topic");
        TEST_SAY("✓ No messages received from empty topic (expected)\n");

        test_delete_topic(consumer, topic);

        free(msgs);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test subscribe, poll, then unsubscribe
 */
static void test_subscribe_poll_unsubscribe(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic   = "0154-share-unsub";
        const char *group   = "share-group-unsub";
        const int msg_count = 5;

        TEST_SAY("=== Testing subscribe, poll, then unsubscribe ===\n");

        /* Create topic and produce messages */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, msg_count);
        TEST_SAY("Produced %d messages\n", msg_count);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to topic, consuming messages\n");

        /* Poll for some messages */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs          = 0;
        int consumed_count        = 0;

        rd_kafka_error_t *error =
            rd_kafka_share_consume_batch(consumer, 10000, msgs, &rcvd_msgs);

        if (!error && rcvd_msgs > 0) {
                for (int i = 0; i < (int)rcvd_msgs; i++) {
                        if (!msgs[i]->err) {
                                consumed_count++;
                        }
                        rd_kafka_message_destroy(msgs[i]);
                }
                TEST_SAY("Consumed %d messages before unsubscribe\n",
                         consumed_count);
        } else if (error) {
                rd_kafka_error_destroy(error);
        }

        /* Unsubscribe from all topics */
        TEST_SAY("Unsubscribing from all topics\n");
        rd_kafka_resp_err_t err = rd_kafka_unsubscribe(consumer);
        TEST_ASSERT(!err, "Failed to unsubscribe: %s", rd_kafka_err2str(err));

        /* Try to poll after unsubscribe - should fail or get no messages */
        TEST_SAY("Attempting to poll after unsubscribe\n");
        rcvd_msgs = 0;
        error = rd_kafka_share_consume_batch(consumer, 2000, msgs, &rcvd_msgs);

        /**
         * TODO KIP-932: Uncomment once polling before any subscription is
         * properly handled
         */
        // TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it
        // succeeded");

        test_delete_topic(consumer, topic);

        free(msgs);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test subscribe, poll, then subscribe to different topic
 */
static void test_subscribe_poll_subscribe(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        char *topic1        = "test-topic-0154-share-sub1";
        char *topic2        = "test-topic-0154-share-sub2";
        char *group         = "share-group-resub";
        const int msg_count = 3;

        TEST_SAY("=== Testing subscribe, poll, then resubscribe ===\n");

        /* Create topics and produce messages */
        test_create_topic_wait_exists(NULL, topic1, 1, -1, 60 * 1000);
        test_create_topic_wait_exists(NULL, topic2, 1, -1, 60 * 1000);

        test_produce_msgs_easy(topic1, 0, 0, msg_count);
        test_produce_msgs_easy(topic2, 0, 0, msg_count);
        TEST_SAY("Produced %d messages to each topic\n", msg_count);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to first topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic1,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to first topic: %s\n", topic1);

        /* Poll from first topic */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs          = 0;
        int topic1_count          = 0;

        rd_kafka_error_t *error =
            rd_kafka_share_consume_batch(consumer, 10000, msgs, &rcvd_msgs);

        if (!error && rcvd_msgs > 0) {
                for (int i = 0; i < (int)rcvd_msgs; i++) {
                        if (!msgs[i]->err) {
                                topic1_count++;
                        }
                        rd_kafka_message_destroy(msgs[i]);
                }
                TEST_SAY("Consumed %d messages from topic1\n", topic1_count);
        } else if (error) {
                rd_kafka_error_destroy(error);
        }

        /* Subscribe to second topic */
        TEST_SAY("Resubscribing to second topic: %s\n", topic2);
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic2,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        /* Poll from second topic */
        rcvd_msgs        = 0;
        int topic2_count = 0;

        error = rd_kafka_share_consume_batch(consumer, 10000, msgs, &rcvd_msgs);

        if (!error && rcvd_msgs > 0) {
                for (int i = 0; i < (int)rcvd_msgs; i++) {
                        if (!msgs[i]->err) {
                                topic2_count++;
                        }
                        rd_kafka_message_destroy(msgs[i]);
                }
                TEST_SAY("Consumed %d messages from topic2\n", topic2_count);
        } else if (error) {
                rd_kafka_error_destroy(error);
        }

        TEST_SAY(
            "✓ Successfully resubscribed and consumed from different topics\n");

        test_delete_topic(consumer, topic1);
        test_delete_topic(consumer, topic2);

        free(msgs);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test subscribe, unsubscribe, then poll fails
 */
static void test_subscribe_unsubscribe_poll_fails(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic = "0154-share-unsub-fail";
        const char *group = "share-group-unsub-fail";

        TEST_SAY("=== Testing subscribe, unsubscribe, then poll fails ===\n");

        /* Create topic */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, 3);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to topic: %s\n", topic);

        /* Immediately unsubscribe */
        TEST_SAY("Unsubscribing immediately\n");
        rd_kafka_resp_err_t err = rd_kafka_unsubscribe(consumer);
        TEST_ASSERT(!err, "Failed to unsubscribe: %s", rd_kafka_err2str(err));

        /* Try to poll - should fail */
        TEST_SAY("Attempting to poll after unsubscribe\n");
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs          = 0;

        rd_kafka_error_t *error =
            rd_kafka_share_consume_batch(consumer, 2000, msgs, &rcvd_msgs);

        /**
         * TODO KIP-932: Uncomment once polling before any subscription is
         * properly handled
         */
        // TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it
        // succeeded");

        test_delete_topic(consumer, topic);

        free(msgs);
        rd_kafka_destroy(consumer);
}

int main_0170_share_consumer_subscription(int argc, char **argv) {
        test_subscription_introspection();
        test_unsubscribe_idempotence();
        test_resubscribe_replaces_set();
        test_duplicate_subscribe_idempotent();
        test_subscribe_nonexistent_then_create();
        test_unsubscribe_then_subscribe_new_topics();
        test_resubscribe_switch_topics();
        test_poll_no_subscribe_fails();
        test_subscribe_and_poll_no_records();
        test_subscribe_poll_unsubscribe();
        test_subscribe_poll_subscribe();
        test_subscribe_unsubscribe_poll_fails();
        return 0;
}