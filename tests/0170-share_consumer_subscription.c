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

/*
 * Share consumer subscription tests.
 *
 * Tests subscription/unsubscription mechanics including:
 * - Subscribed topics via rd_kafka_subscription()
 * - Unsubscribe idempotence
 * - Resubscription behavior (topic replacement)
 * - Subscribing to non-existent topics
 * - Polling without active subscription
 */

#define BATCH_SIZE 128


/**
 * Create a share consumer with standard configuration.
 */
static rd_kafka_t *create_share_consumer(const char *group) {
        char errstr[512];
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "Failed to create share consumer: %s", errstr);
        return rk;
}

/**
 * Subscribe to a list of topics.
 */
static void subscribe_topics(rd_kafka_t *rk, const char **topics, int cnt) {
        rd_kafka_topic_partition_list_t *subs;
        int i;

        subs = rd_kafka_topic_partition_list_new(cnt);
        for (i = 0; i < cnt; i++)
                rd_kafka_topic_partition_list_add(subs, topics[i], RD_KAFKA_PARTITION_UA);

        TEST_CALL_ERR__(rd_kafka_subscribe(rk, subs));
        rd_kafka_topic_partition_list_destroy(subs);
}

/**
 * Get current subscription count.
 */
static int get_subscription_count(rd_kafka_t *rk) {
        rd_kafka_topic_partition_list_t *cur = NULL;
        int cnt;

        TEST_CALL_ERR__(rd_kafka_subscription(rk, &cur));
        TEST_ASSERT(cur != NULL, "subscription() returned NULL list");
        cnt = cur->cnt;
        rd_kafka_topic_partition_list_destroy(cur);
        return cnt;
}

/**
 * Consume messages, return count of valid messages received.
 */
static int consume_batch(rd_kafka_t *rk, int timeout_ms) {
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0;
        int valid = 0;
        size_t i;

        err = rd_kafka_share_consume_batch(rk, timeout_ms, batch, &rcvd);
        if (err) {
                rd_kafka_error_destroy(err);
                return 0;
        }

        for (i = 0; i < rcvd; i++) {
                if (!batch[i]->err)
                        valid++;
                rd_kafka_message_destroy(batch[i]);
        }
        return valid;
}

/**
 * Consume until expected count reached or max attempts exhausted.
 */
static int consume_until(rd_kafka_t *rk, int expected, int max_attempts) {
        int total = 0;

        while (total < expected && max_attempts-- > 0)
                total += consume_batch(rk, 2000);

        return total;
}

/**
 * Helper: Set group config to earliest offset.
 */
static void set_group_offset_earliest(rd_kafka_t *rk, const char *group) {
        const char *cfg[] = {"share.auto.offset.reset", "SET", "earliest"};
        test_IncrementalAlterConfigs_simple(rk, RD_KAFKA_RESOURCE_GROUP, group, cfg, 1);
}


/*
 * Verify rd_kafka_subscription() correctly reflects subscribed topics
 * before and after unsubscribe.
 */
static void do_test_subscription_count(void) {
        const char *group = "share-sub-query";
        const char *topics[] = {
                "0170-share-query-t1",
                "0170-share-query-t2",
                "0170-share-query-t3"
        };
        const int topic_cnt = 3;
        rd_kafka_t *rk;
        int cnt, i;

        TEST_SAY("Subscription introspection test\n");

        for (i = 0; i < topic_cnt; i++)
                test_create_topic_wait_exists(NULL, topics[i], 1, -1, 30000);

        rk = create_share_consumer(group);
        subscribe_topics(rk, topics, topic_cnt);

        /* Verify subscription shows all topics */
        cnt = get_subscription_count(rk);
        TEST_ASSERT(cnt == topic_cnt, "expected %d subscriptions, got %d",
                    topic_cnt, cnt);

        /* Unsubscribe and verify empty */
        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));
        cnt = get_subscription_count(rk);
        TEST_ASSERT(cnt == 0, "expected 0 after unsubscribe, got %d", cnt);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Subscription introspection: PASSED\n");
}


/*
 * Calling unsubscribe multiple times should not error.
 */
static void do_test_multiple_unsubscribe(void) {
        const char *group = "share-unsub-idem";
        const char *topics[] = {"0170-share-idem-t1", "0170-share-idem-t2"};
        rd_kafka_t *rk;
        int i;

        TEST_SAY("Unsubscribe idempotence test\n");

        for (i = 0; i < 2; i++)
                test_create_topic_wait_exists(NULL, topics[i], 1, -1, 30000);

        rk = create_share_consumer(group);
        subscribe_topics(rk, topics, 2);

        /* Multiple unsubscribe calls should succeed */
        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));
        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));
        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));

        TEST_ASSERT(get_subscription_count(rk) == 0,
                    "subscription should be empty after unsubscribe");

        for (i = 0; i < 2; i++)
                test_delete_topic(rk, topics[i]);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Unsubscribe idempotence: PASSED\n");
}


/*
 * Subscribing to the same topics multiple times should be a no-op.
 */
static void do_test_duplicate_subscribe(void) {
        const char *group = "share-dup-sub";
        const char *topics[] = {"0170-share-dup-t1", "0170-share-dup-t2"};
        rd_kafka_t *rk;
        int i;

        TEST_SAY("Duplicate subscribe idempotence test\n");

        for (i = 0; i < 2; i++)
                test_create_topic_wait_exists(NULL, topics[i], 1, -1, 30000);

        rk = create_share_consumer(group);

        /* Subscribe multiple times to same topics */
        subscribe_topics(rk, topics, 2);
        subscribe_topics(rk, topics, 2);
        subscribe_topics(rk, topics, 2);

        TEST_ASSERT(get_subscription_count(rk) == 2,
                    "should have exactly 2 topics after duplicate subscribes");

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Duplicate subscribe idempotence: PASSED\n");
}


/*

 * A new subscribe() call should completely replace the previous subscription,
 * not add to it.
 */
static void do_test_resubscribe_replaces_old_subscription(void) {
        const char *group = "share-resub-replace";
        const char *set1[] = {"0170-share-old-a", "0170-share-old-b"};
        const char *set2[] = {"0170-share-new-c", "0170-share-new-d"};
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *cur = NULL;
        int i;

        TEST_SAY("Resubscribe replacement test\n");

        for (i = 0; i < 2; i++) {
                test_create_topic_wait_exists(NULL, set1[i], 1, -1, 30000);
                test_create_topic_wait_exists(NULL, set2[i], 1, -1, 30000);
        }

        rk = create_share_consumer(group);

        /* Subscribe to first set */
        subscribe_topics(rk, set1, 2);
        TEST_ASSERT(get_subscription_count(rk) == 2, "expected 2 topics");

        /* Resubscribe to second set */
        subscribe_topics(rk, set2, 2);
        TEST_ASSERT(get_subscription_count(rk) == 2, "expected 2 topics after resubscribe");

        /* Verify old topics are gone */
        rd_kafka_subscription(rk, &cur);
        for (i = 0; i < cur->cnt; i++) {
                const char *name = cur->elems[i].topic;
                TEST_ASSERT(strcmp(name, set1[0]) && strcmp(name, set1[1]),
                            "old topic '%s' still present after resubscribe", name);
        }
        rd_kafka_topic_partition_list_destroy(cur);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Resubscribe replacement: PASSED\n");
}


/*
 * Subscribing to a topic that doesn't exist yet should work,
 * and messages should be consumable after topic creation.
 */
static void do_test_subscribe_before_topic_exists(void) {
        const char *group = "share-presubscribe";
        char *topic = test_mk_topic_name("0170-share-late-create", 1);
        rd_kafka_t *rk;
        int consumed;

        TEST_SAY("Subscribe before topic exists test\n");

        rk = create_share_consumer(group);
        set_group_offset_earliest(rk, group);

        /* Subscribe before topic exists */
        subscribe_topics(rk, (const char *[]){topic}, 1);
        TEST_ASSERT(get_subscription_count(rk) == 1, "expected 1 subscription");

        /* Now create topic and produce */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        /* Should be able to consume */
        consumed = consume_until(rk, 5, 15);
        TEST_ASSERT(consumed == 5, "expected 5 messages, got %d", consumed);

        test_delete_topic(rk, topic);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Subscribe before topic exists: PASSED\n");
}


/*
 * After resubscribing to different topics, only messages from new topics
 * should be received.
 */
static void do_test_topic_switch(void) {
        const char *group = "share-topic-switch";
        const char *topicA = "0170-share-switch-a";
        const char *topicB = "0170-share-switch-b";
        rd_kafka_t *rk;
        rd_kafka_message_t *batch[BATCH_SIZE];
        int got_a = 0, got_b = 0;
        int attempts;
        size_t rcvd, i;
        rd_kafka_error_t *err;

        TEST_SAY("Topic switch verification test\n");

        test_create_topic_wait_exists(NULL, topicA, 1, -1, 30000);
        test_create_topic_wait_exists(NULL, topicB, 1, -1, 30000);

        test_produce_msgs_easy(topicA, 0, 0, 10);
        test_produce_msgs_easy(topicB, 0, 0, 10);

        rk = create_share_consumer(group);
        set_group_offset_earliest(rk, group);

        /* Subscribe to A, consume some messages */
        subscribe_topics(rk, (const char *[]){topicA}, 1);

        attempts = 10;
        while (got_a < 5 && attempts-- > 0) {
                rcvd = 0;
                err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        if (!batch[i]->err)
                                got_a++;
                        rd_kafka_message_destroy(batch[i]);
                }
        }
        TEST_SAY("Consumed %d from topic A before switch\n", got_a);

        /* Switch to B only */
        subscribe_topics(rk, (const char *[]){topicB}, 1);

        /* Produce more to A (should NOT be received) */
        test_produce_msgs_easy(topicA, 0, 0, 5);

        /* Consume from B, verify no A messages */
        attempts = 15;
        while (got_b < 10 && attempts-- > 0) {
                rcvd = 0;
                err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (i = 0; i < rcvd; i++) {
                        if (!batch[i]->err) {
                                const char *name = rd_kafka_topic_name(batch[i]->rkt);
                                TEST_ASSERT(strcmp(name, topicA) != 0,
                                            "received from old topic A after switch");
                                got_b++;
                        }
                        rd_kafka_message_destroy(batch[i]);
                }
        }
        TEST_ASSERT(got_b == 10, "expected 10 from B, got %d", got_b);

        test_delete_topic(rk, topicA);
        test_delete_topic(rk, topicB);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Topic switch verification: PASSED\n");
}


/*
 * Polling a topic with no messages should return zero messages (not error).
 */
static void do_test_poll_empty_topic(void) {
        const char *group = "share-empty-poll";
        const char *topic = "0170-share-empty";
        rd_kafka_t *rk;
        int consumed;

        TEST_SAY("Poll empty topic test\n");

        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);
        /* Don't produce any messages */

        rk = create_share_consumer(group);
        subscribe_topics(rk, (const char *[]){topic}, 1);

        /* Should get zero messages, not an error */
        consumed = consume_batch(rk, 3000);
        TEST_ASSERT(consumed == 0, "expected 0 from empty topic, got %d", consumed);

        test_delete_topic(rk, topic);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        TEST_SAY("Poll empty topic: PASSED\n");
}


/*
 * Polling without subscribing first should handle gracefully.
 */
static void do_test_poll_no_subscription(void) {
        const char *group = "share-no-sub";
        rd_kafka_t *rk;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0;

        TEST_SAY("Poll without subscription test\n");

        rk = create_share_consumer(group);

        /* Poll without subscribing */
        err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);

        /*
         * TODO KIP-932: Polling without any subscription should 
         * return error. Currently it is just sending out No msgs.
         */
        if (err)
                rd_kafka_error_destroy(err);

        TEST_SAY("Poll without subscription completed (rcvd=%zu)\n", rcvd);

        rd_kafka_destroy(rk);

        TEST_SAY("Poll without subscription: PASSED\n");
}


/*
 * After unsubscribing, polling should not return messages from old topics.
 */
static void do_test_poll_after_unsubscribe(void) {
        const char *group = "share-post-unsub";
        const char *topic = "0170-share-post-unsub";
        rd_kafka_t *rk;
        rd_kafka_message_t *batch[BATCH_SIZE];
        rd_kafka_error_t *err;
        size_t rcvd = 0;
        int consumed;

        TEST_SAY("Poll after unsubscribe test\n");

        test_create_topic_wait_exists(NULL, topic, 1, -1, 30000);
        test_produce_msgs_easy(topic, 0, 0, 5);

        rk = create_share_consumer(group);
        set_group_offset_earliest(rk, group);
        subscribe_topics(rk, (const char *[]){topic}, 1);

        /* Consume at least one message */
        consumed = consume_until(rk, 1, 10);
        TEST_SAY("Consumed %d before unsubscribe\n", consumed);

        /* Unsubscribe */
        TEST_CALL_ERR__(rd_kafka_unsubscribe(rk));

        /* Poll after unsubscribe */
        err = rd_kafka_share_consume_batch(rk, 2000, batch, &rcvd);

        /*
         * TODO KIP-932: Polling without any subscription should 
         * return error. Currently it is just sending out No msgs.
         */
        if (err)
                rd_kafka_error_destroy(err);

        /* Clean up any returned messages */
        for (size_t i = 0; i < rcvd; i++)
                rd_kafka_message_destroy(batch[i]);

        TEST_SAY("Poll after unsubscribe completed (rcvd=%zu)\n", rcvd);

        test_delete_topic(rk, topic);
        rd_kafka_destroy(rk);

        TEST_SAY("Poll after unsubscribe: PASSED\n");
}


int main_0170_share_consumer_subscription(int argc, char **argv) {

        do_test_subscription_count();
        do_test_multiple_unsubscribe();
        do_test_duplicate_subscribe();
        do_test_resubscribe_replaces_old_subscription();
        do_test_subscribe_before_topic_exists();
        do_test_topic_switch();
        do_test_poll_empty_topic();
        do_test_poll_no_subscription();
        do_test_poll_after_unsubscribe();

        return 0;
}
