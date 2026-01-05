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
#include "testshared.h"

/*
 * Share consumer subscription tests.
 *
 * Tests subscription/unsubscription mechanics including:
 * - Subscribed topics via rd_kafka_subscription()
 * - Repeated subscribe/unsubscribe calls
 * - Resubscription behavior (topic replacement)
 * - Subscribing to non-existent topics
 * - Polling without active subscription
 */

#define MAX_TOPICS 10

/**
 * Configuration for parameterized subscription tests.
 * Consolidates tests for: single/repeated subscribe and unsubscribe,
 * and incremental subscription.
 */
typedef struct {
        const char *name;
        int topic_cnt;
        int msgs_per_topic;

        /* Subscribe behavior */
        rd_bool_t subscribe_incrementally; /* Add topics one by one */
        int subscribe_repeat_count;        /* Times to call subscribe */

        /* Unsubscribe behavior */
        int unsubscribe_repeat_count; /* 0=don't, N=call N times */

        /* Verification */
        rd_bool_t verify_empty_after_unsub;
} sub_test_config_t;


/**
 * Helper: Set group config to earliest offset.
 */
static void set_group_offset_earliest(rd_kafka_t *rk, const char *group) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(rk, RD_KAFKA_RESOURCE_GROUP, group,
                                            cfg, 1);
}


/**
 * Generic subscription test runner.
 * Handles: single/repeated subscribe and unsubscribe, incremental subscription.
 */
static void run_subscription_test(const sub_test_config_t *cfg) {
        char group_name[128];
        const char *topics[MAX_TOPICS];
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *tlist, *sub;
        int i, j, total_consumed = 0;
        int expected_msgs;

        rd_snprintf(group_name, sizeof(group_name), "share-%s",
                    cfg->name);
        TEST_SAY("=== %s ===\n", cfg->name);

        TEST_ASSERT(cfg->topic_cnt <= MAX_TOPICS, "topic_cnt exceeds MAX_TOPICS");

        /* Create topics and produce messages */
        for (i = 0; i < cfg->topic_cnt; i++) {
                char topic_name[128];
                rd_snprintf(topic_name, sizeof(topic_name), "0170-%s-t%d",
                            cfg->name, i);
                topics[i] = rd_strdup(test_mk_topic_name(topic_name, 1));
                test_create_topic_wait_exists(NULL, topics[i], 1, -1, 30000);
                test_produce_msgs_easy(topics[i], 0, 0, cfg->msgs_per_topic);
        }

        rk = test_create_share_consumer(group_name);
        set_group_offset_earliest(rk, group_name);

        tlist = rd_kafka_topic_partition_list_new(cfg->topic_cnt);

        if (cfg->subscribe_incrementally) {
                /* Incremental: add topics one by one */
                for (i = 0; i < cfg->topic_cnt; i++) {
                        int consumed;

                        rd_kafka_topic_partition_list_add(
                            tlist, topics[i], RD_KAFKA_PARTITION_UA);
                        TEST_SAY("Subscribing to %d topic(s)\n", tlist->cnt);
                        TEST_CALL_ERR__(rd_kafka_subscribe(rk, tlist));

                        sub = test_get_subscription(rk);
                        TEST_ASSERT(sub->cnt == i + 1,
                                    "expected %d subscriptions, got %d", i + 1,
                                    sub->cnt);
                        rd_kafka_topic_partition_list_destroy(sub);

                        /* Consume some from currently subscribed topics */
                        consumed = test_share_consume_msgs(
                            rk, cfg->msgs_per_topic, 15, 2000, topics, i + 1);
                        TEST_ASSERT(consumed >= 0,
                                    "message from unexpected topic");
                        total_consumed += consumed;
                }
        } else {
                /* Normal: add all topics at once, possibly multiple times */
                for (i = 0; i < cfg->topic_cnt; i++) {
                        rd_kafka_topic_partition_list_add(
                            tlist, topics[i], RD_KAFKA_PARTITION_UA);
                }

                for (j = 0; j < cfg->subscribe_repeat_count; j++) {
                        TEST_CALL_ERR__(rd_kafka_subscribe(rk, tlist));
                }

                sub = test_get_subscription(rk);
                TEST_ASSERT(sub->cnt == cfg->topic_cnt,
                            "expected %d subscriptions, got %d", cfg->topic_cnt,
                            sub->cnt);
                rd_kafka_topic_partition_list_destroy(sub);
        }

        /* Consume all expected messages */
        expected_msgs = cfg->topic_cnt * cfg->msgs_per_topic;
        if (total_consumed < expected_msgs) {
                int remaining = test_share_consume_msgs(
                    rk, expected_msgs - total_consumed, 20, 2000, topics,
                    cfg->topic_cnt);
                TEST_ASSERT(remaining >= 0, "message from unexpected topic");
                total_consumed += remaining;
        }
        TEST_SAY("Consumed %d/%d messages\n", total_consumed, expected_msgs);
        TEST_ASSERT(total_consumed == expected_msgs,
                    "expected %d messages, got %d", expected_msgs,
                    total_consumed);

        /* Unsubscribe (if configured) */
        for (j = 0; j < cfg->unsubscribe_repeat_count; j++) {
                TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));
        }

        if (cfg->verify_empty_after_unsub && cfg->unsubscribe_repeat_count > 0) {
                sub = test_get_subscription(rk);
                TEST_ASSERT(sub->cnt == 0,
                            "expected 0 after unsubscribe, got %d", sub->cnt);
                rd_kafka_topic_partition_list_destroy(sub);
        }

        rd_kafka_topic_partition_list_destroy(tlist);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== %s: PASSED ===\n", cfg->name);
}


/*
 * Subscription replacement test: switch from old topics to new topics.
 * Verifies messages produced to old topics AFTER switch are not received.
 */
static void do_test_topic_switch(void) {
        const char *group = test_mk_topic_name("share-topic-switch", 1);
        const char *old_topics[2], *new_topics[2];
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *sub;
        int i, consumed;

        TEST_SAY("=== Topic switch test ===\n");

        for (i = 0; i < 2; i++) {
                char name[64];
                rd_snprintf(name, sizeof(name), "0170-old-%d", i);
                old_topics[i] = rd_strdup(test_mk_topic_name(name, 1));
                rd_snprintf(name, sizeof(name), "0170-new-%d", i);
                new_topics[i] = rd_strdup(test_mk_topic_name(name, 1));

                test_create_topic_wait_exists(NULL, old_topics[i], 1, -1, 30000);
                test_create_topic_wait_exists(NULL, new_topics[i], 1, -1, 30000);
                test_produce_msgs_easy(old_topics[i], 0, 0, 10);
                test_produce_msgs_easy(new_topics[i], 0, 0, 10);
        }

        rk = test_create_share_consumer(group);
        set_group_offset_earliest(rk, group);

        /* Subscribe to old topics, consume some */
        test_consumer_subscribe_multi(rk, 2, old_topics[0], old_topics[1]);
        consumed = test_share_consume_msgs(rk, 10, 15, 2000, old_topics, 2);
        TEST_ASSERT(consumed >= 0, "wrong topic before switch");

        /* Switch to new topics */
        test_consumer_subscribe_multi(rk, 2, new_topics[0], new_topics[1]);
        sub = test_get_subscription(rk);
        for (i = 0; i < sub->cnt; i++) {
                TEST_ASSERT(strcmp(sub->elems[i].topic, old_topics[0]) &&
                                strcmp(sub->elems[i].topic, old_topics[1]),
                            "old topic still in subscription");
        }
        rd_kafka_topic_partition_list_destroy(sub);

        /* Produce MORE to old topics - should NOT be received */
        for (i = 0; i < 2; i++)
                test_produce_msgs_easy(old_topics[i], 0, 0, 5);

        /* Consume from new topics only */
        consumed = test_share_consume_msgs(rk, 20, 20, 2000, new_topics, 2);
        TEST_ASSERT(consumed >= 0, "got message from old topic after switch");
        TEST_ASSERT(consumed == 20, "expected 20, got %d", consumed);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Topic switch: PASSED ===\n");
}


/*
 * Subscribe to topic before it exists, then create and consume.
 */
static void do_test_subscribe_before_topic_exists(void) {
        const char *group = test_mk_topic_name("share-presubscribe", 1);
        const char *topic = test_mk_topic_name("0170-late-create", 1);
        const char *topics[] = {topic};
        rd_kafka_t *rk;
        int consumed;

        TEST_SAY("=== Subscribe before topic exists ===\n");

        rk = test_create_share_consumer(group);
        set_group_offset_earliest(rk, group);

        /* Subscribe before topic exists */
        test_consumer_subscribe_multi(rk, 1, topic);

        /* Now create and produce */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        consumed = test_share_consume_msgs(rk, 5, 15, 2000, topics, 1);
        TEST_ASSERT(consumed >= 0 && consumed == 5,
                    "expected 5, got %d", consumed);

        test_delete_topic(rk, topic);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Subscribe before topic exists: PASSED ===\n");
}


/*
 * Poll empty topic - should return 0 messages, not error.
 */
static void do_test_poll_empty_topic(void) {
        const char *group = test_mk_topic_name("share-empty-poll", 1);
        const char *topic = test_mk_topic_name("0170-empty", 1);
        rd_kafka_t *rk;
        int consumed = 0;

        TEST_SAY("=== Poll empty topic ===\n");

        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);

        rk = test_create_share_consumer(group);
        test_consumer_subscribe_multi(rk, 1, topic);

        test_share_consume_batch(rk, 3000, NULL, 0, &consumed);
        TEST_ASSERT(consumed == 0, "expected 0, got %d", consumed);

        test_delete_topic(rk, topic);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Poll empty topic: PASSED ===\n");
}


/*
 * Poll without subscription - should handle gracefully.
 */
static void do_test_poll_no_subscription(void) {
        const char *group = test_mk_topic_name("share-no-sub", 1);
        rd_kafka_t *rk;
        rd_kafka_message_t *batch[TEST_SHARE_BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0;

        TEST_SAY("=== Poll without subscription ===\n");

        rk = test_create_share_consumer(group);

        err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
        /* TODO KIP-932: Should return error, currently returns no msgs */
        if (err)
                rd_kafka_error_destroy(err);

        rd_kafka_destroy(rk);

        TEST_SAY("=== Poll without subscription: PASSED ===\n");
}


/*
 * Poll after unsubscribe - should not return old messages.
 */
static void do_test_poll_after_unsubscribe(void) {
        const char *group = test_mk_topic_name("share-post-unsub", 1);
        const char *topic = test_mk_topic_name("0170-post-unsub", 1);
        rd_kafka_t *rk;
        rd_kafka_message_t *batch[TEST_SHARE_BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0, i;

        TEST_SAY("=== Poll after unsubscribe ===\n");

        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        rk = test_create_share_consumer(group);
        set_group_offset_earliest(rk, group);
        test_consumer_subscribe_multi(rk, 1, topic);

        /* Consume at least one */
        test_share_consume_msgs(rk, 1, 10, 2000, NULL, 0);

        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));

        err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
        if (err)
                rd_kafka_error_destroy(err);
        for (i = 0; i < rcvd; i++)
                rd_kafka_message_destroy(batch[i]);

        test_delete_topic(rk, topic);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Poll after unsubscribe: PASSED ===\n");
}


/*
 * Topic deleted while subscribed - consumer should continue with remaining.
 */
static void do_test_topic_deletion(void) {
        const char *group = test_mk_topic_name("share-topic-del", 1);
        const char *topic_keep =
            rd_strdup(test_mk_topic_name("0170-keep", 1));
        const char *topic_delete =
            rd_strdup(test_mk_topic_name("0170-delete", 1));
        const char *both[] = {topic_keep, topic_delete};
        const char *keep_only[] = {topic_keep};
        rd_kafka_t *rk;
        int consumed;

        TEST_SAY("=== Topic deletion while subscribed ===\n");

        test_create_topic_wait_exists(NULL, topic_keep, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, topic_delete, 1, -1, 30000);
        test_produce_msgs_easy(topic_keep, 0, 0, 10);
        test_produce_msgs_easy(topic_delete, 0, 0, 10);

        rk = test_create_share_consumer(group);
        set_group_offset_earliest(rk, group);
        test_consumer_subscribe_multi(rk, 2, topic_keep, topic_delete);

        consumed = test_share_consume_msgs(rk, 10, 15, 2000, both, 2);
        TEST_ASSERT(consumed >= 0, "wrong topic");

        test_delete_topic(rk, topic_delete);
        rd_sleep(3);

        test_produce_msgs_easy(topic_keep, 0, 0, 5);
        consumed = test_share_consume_msgs(rk, 5, 15, 3000, keep_only, 1);
        TEST_ASSERT(consumed >= 0, "wrong topic after deletion");

        test_delete_topic(rk, topic_keep);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Topic deletion while subscribed: PASSED ===\n");
}


/*
 * Rapid subscription updates - stress test.
 */
static void do_test_rapid_updates(void) {
        const char *group = test_mk_topic_name("share-rapid", 1);
#define RAPID_CNT 10
        const char *topics[RAPID_CNT];
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *tlist, *sub;
        int i, j;

        TEST_SAY("=== Rapid subscription updates ===\n");

        for (i = 0; i < RAPID_CNT; i++) {
                char name[64];
                rd_snprintf(name, sizeof(name), "0170-rapid-%d", i);
                topics[i] = rd_strdup(test_mk_topic_name(name, 1));
                test_create_topic_wait_exists(NULL, topics[i], 1, -1, 30000);
        }

        rk = test_create_share_consumer(group);

        /* Many rapid changes */
        for (i = 0; i < RAPID_CNT; i++) {
                int sub_size = (i % 3) + 1;

                tlist = rd_kafka_topic_partition_list_new(sub_size);
                for (j = 0; j < sub_size && (i + j) < RAPID_CNT; j++) {
                        rd_kafka_topic_partition_list_add(
                            tlist, topics[(i + j) % RAPID_CNT],
                            RD_KAFKA_PARTITION_UA);
                }
                TEST_CALL_ERR__(rd_kafka_subscribe(rk, tlist));
                rd_kafka_topic_partition_list_destroy(tlist);

                if (i % 5 == 4)
                        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));
        }

        /* Final: last 3 topics */
        test_consumer_subscribe_multi(rk, 3, topics[RAPID_CNT - 3],
                                      topics[RAPID_CNT - 2],
                                      topics[RAPID_CNT - 1]);
        sub = test_get_subscription(rk);
        TEST_ASSERT(sub->cnt == 3, "expected 3, got %d", sub->cnt);
        rd_kafka_topic_partition_list_destroy(sub);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("=== Rapid subscription updates: PASSED ===\n");
#undef RAPID_CNT
}


/*
 * Multiple consumers with overlapping subscriptions.
 */
static void do_test_multi_consumer_overlap(void) {
        const char *group = test_mk_topic_name("share-overlap", 1);
        const char *shared = rd_strdup(test_mk_topic_name("0170-shared", 1));
        const char *c1_only = rd_strdup(test_mk_topic_name("0170-c1only", 1));
        const char *c2_only = rd_strdup(test_mk_topic_name("0170-c2only", 1));
        const char *c1_topics[] = {shared, c1_only};
        const char *c2_topics[] = {shared, c2_only};
        rd_kafka_t *c1, *c2;
        int c1_cnt = 0, c2_cnt = 0;
        int attempts;

        TEST_SAY("=== Multiple consumers overlapping ===\n");

        test_create_topic_wait_exists(NULL, shared, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, c1_only, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, c2_only, 1, -1, 30000);

        test_produce_msgs_easy(shared, 0, 0, 20);
        test_produce_msgs_easy(c1_only, 0, 0, 10);
        test_produce_msgs_easy(c2_only, 0, 0, 10);

        c1 = test_create_share_consumer(group);
        c2 = test_create_share_consumer(group);
        set_group_offset_earliest(c1, group);

        test_consumer_subscribe_multi(c1, 2, shared, c1_only);
        test_consumer_subscribe_multi(c2, 2, shared, c2_only);

        /* Give consumers time to join group and receive assignments.
         * Alternate between consumers to allow both to make progress. */
        attempts = 20;
        while ((c1_cnt + c2_cnt) < 10 && attempts-- > 0) {
                int batch_cnt = 0;
                int ret;

                /* C1 batch */
                ret = test_share_consume_batch(c1, 2000, c1_topics, 2,
                                               &batch_cnt);
                TEST_ASSERT(ret >= 0, "C1 wrong topic");
                c1_cnt += batch_cnt;

                /* C2 batch */
                batch_cnt = 0;
                ret = test_share_consume_batch(c2, 2000, c2_topics, 2,
                                               &batch_cnt);
                TEST_ASSERT(ret >= 0, "C2 wrong topic");
                c2_cnt += batch_cnt;
        }

        TEST_SAY("C1: %d, C2: %d (total: %d)\n", c1_cnt, c2_cnt,
                 c1_cnt + c2_cnt);
        TEST_ASSERT(c1_cnt > 0 || c2_cnt > 0, "no messages received");

        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        TEST_SAY("=== Multiple consumers overlapping: PASSED ===\n");
}


int main_0170_share_consumer_subscription(int argc, char **argv) {
        static const sub_test_config_t configs[] = {
            /* Single unsubscribe clears subscription */
            {.name                    = "single-unsubscribe",
             .topic_cnt               = 2,
             .msgs_per_topic          = 5,
             .subscribe_incrementally = rd_false,
             .subscribe_repeat_count  = 1,
             .unsubscribe_repeat_count = 1,
             .verify_empty_after_unsub = rd_true},

            /* Multiple unsubscribe calls should not error */
            {.name                    = "repeated-unsubscribe-no-error",
             .topic_cnt               = 2,
             .msgs_per_topic          = 5,
             .subscribe_incrementally = rd_false,
             .subscribe_repeat_count  = 1,
             .unsubscribe_repeat_count = 3,
             .verify_empty_after_unsub = rd_true},

            /* Multiple subscribe calls to same topics should not duplicate */
            {.name                    = "repeated-subscribe-no-duplicates",
             .topic_cnt               = 2,
             .msgs_per_topic          = 5,
             .subscribe_incrementally = rd_false,
             .subscribe_repeat_count  = 3,
             .unsubscribe_repeat_count = 0,
             .verify_empty_after_unsub = rd_false},

            /* Incremental subscription */
            {.name                    = "incremental-subscription",
             .topic_cnt               = 3,
             .msgs_per_topic          = 10,
             .subscribe_incrementally = rd_true,
             .subscribe_repeat_count  = 1,
             .unsubscribe_repeat_count = 0,
             .verify_empty_after_unsub = rd_false},
        };
        size_t i;

        /* Run parameterized tests */
        for (i = 0; i < sizeof(configs) / sizeof(configs[0]); i++) {
                run_subscription_test(&configs[i]);
        }
        do_test_topic_switch();
        do_test_subscribe_before_topic_exists();
        do_test_poll_empty_topic();
        do_test_poll_no_subscription();
        do_test_poll_after_unsubscribe();
        do_test_topic_deletion();
        do_test_rapid_updates();
        do_test_multi_consumer_overlap();

        return 0;
}
