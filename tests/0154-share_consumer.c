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
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Try to poll without subscribing - should fail or return timeout */
        TEST_SAY("Attempting to poll without subscription\n");
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs = 0;
        
        rd_kafka_error_t *error = rd_kafka_share_consume_batch(consumer, 2000, msgs, &rcvd_msgs);
        
        /**
         * TODO KIP-932: Uncomment once polling before any subscription is properly handled
         */
        //TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it succeeded");

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
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to empty topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to empty topic, polling for messages\n");

        /* Poll for messages - should get none */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs = 0;
        
        rd_kafka_error_t *error = rd_kafka_share_consume_batch(consumer, 5000, msgs, &rcvd_msgs);

        TEST_ASSERT(rcvd_msgs == 0, "Should not receive messages from empty topic");
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
        const char *topic = "0154-share-unsub";
        const char *group = "share-group-unsub";
        const int msg_count = 5;

        TEST_SAY("=== Testing subscribe, poll, then unsubscribe ===\n");

        /* Create topic and produce messages */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, msg_count);
        TEST_SAY("Produced %d messages\n", msg_count);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to topic, consuming messages\n");

        /* Poll for some messages */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs = 0;
        int consumed_count = 0;
        
        rd_kafka_error_t *error = rd_kafka_share_consume_batch(consumer, 10000, msgs, &rcvd_msgs);
        
        if (!error && rcvd_msgs > 0) {
                for (int i = 0; i < (int)rcvd_msgs; i++) {
                        if (!msgs[i]->err) {
                                consumed_count++;
                        }
                        rd_kafka_message_destroy(msgs[i]);
                }
                TEST_SAY("Consumed %d messages before unsubscribe\n", consumed_count);
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
         * TODO KIP-932: Uncomment once polling before any subscription is properly handled
         */
        //TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it succeeded");

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
        char *topic1 = "test-topic-0154-share-sub1";
        char *topic2 = "test-topic-0154-share-sub2";
        char *group = "share-group-resub";
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
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Subscribe to first topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic1, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Subscribed to first topic: %s\n", topic1);

        /* Poll from first topic */
        rd_kafka_message_t **msgs = malloc(sizeof(rd_kafka_message_t *) * 10);
        size_t rcvd_msgs = 0;
        int topic1_count = 0;
        
        rd_kafka_error_t *error = rd_kafka_share_consume_batch(consumer, 10000, msgs, &rcvd_msgs);
        
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
        rd_kafka_topic_partition_list_add(topics, topic2, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        /* Poll from second topic */
        rcvd_msgs = 0;
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

        TEST_SAY("✓ Successfully resubscribed and consumed from different topics\n");

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
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
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
        size_t rcvd_msgs = 0;
        
        rd_kafka_error_t *error = rd_kafka_share_consume_batch(consumer, 2000, msgs, &rcvd_msgs);
        
        /**
         * TODO KIP-932: Uncomment once polling before any subscription is properly handled
         */
        //TEST_ASSERT(error, "Expected poll to fail after unsubscribe, but it succeeded");

        test_delete_topic(consumer, topic);

        free(msgs);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test producing and consuming 10 messages
 */
static void test_share_consumer_messages(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic = "0154-share-test";
        char *group = "share-group-10msg";
        const int msg_count = 10;
        int consumed_count = 0;
        int attempts = 10; // Number of attempts to poll so the test doesn't run indefinitely

        const char *confs_set_group[] = {"share.auto.offset.reset",
                                                 "SET", "earliest"};

        TEST_SAY("=== Testing share consumer with 10 messages ===\n");

        /* Create topic */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        rd_sleep(5);

        test_produce_msgs_easy(topic, 0, 0, msg_count);
        TEST_SAY("Successfully produced %d messages\n", msg_count);

        /* Create share consumer */
        TEST_SAY("Creating share consumer for group %s\n", group);
        test_conf_init(&cons_conf, NULL, 60);
        
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        if (!consumer) {
                TEST_FAIL("Failed to create share consumer: %s", errstr);
        }

        /* Subscribe to topic */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        
        TEST_SAY("Subscribing to topic %s\n", topic);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP, group, confs_set_group, 1);

        /* Allocate message array for batch consumption */
        rd_kafka_message_t **rkmessages = malloc(sizeof(rd_kafka_message_t *) * 100);

        /* Consume messages until we get all 10 */
        while (consumed_count < msg_count && attempts > 0) {
                size_t rcvd_msgs = 0;
                rd_kafka_error_t *error;
                
                error = rd_kafka_share_consume_batch(consumer, 5000, rkmessages, &rcvd_msgs);

                if (error) {
                        TEST_SAY("Consume error: %s\n", rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        attempts--;
                        continue;
                }

                TEST_SAY("Received %zu messages in batch\n", rcvd_msgs);
                
                /* Process each message in the batch */
                for (int i = 0; i < (int)rcvd_msgs; i++) {
                        rd_kafka_message_t *rkm = rkmessages[i];

                        if (rkm->err) {
                                TEST_SAY("Message error: %s\n", rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Count valid messages only */
                        consumed_count++;
                        TEST_SAY("Consumed message %d/%d\n", consumed_count, msg_count);

                        /* Clean up message */
                        rd_kafka_message_destroy(rkm);
                }

                /* Break if we've consumed all expected messages */
                if (consumed_count >= msg_count) {
                        TEST_SAY("Consumed all %d messages, stopping\n", msg_count);
                        break;
                }
                
                attempts--;
        }

        free(rkmessages);

        /* Verify we got exactly the expected number of messages */
        TEST_ASSERT(consumed_count == msg_count, 
                   "Expected to consume %d messages, but consumed %d", 
                   msg_count, consumed_count);

        TEST_SAY("✓ Successfully consumed exactly %d messages\n", consumed_count);

        test_delete_topic(consumer, topic);

        /* Clean up */
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test subscribing to multiple topics
 */
static void test_share_consumer_multiple_topics(void) {
        char errstr[512];
        rd_kafka_conf_t *cons_conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        char *topic1 = "0154-share-topic-multi-1";
        char *topic2 = "0154-share-topic-multi-2";
        char *group = "share-group-multitopic";
        const int msgs_per_topic = 5;
        int consumed_count = 0;

        const char *confs_set_group[] = {"share.auto.offset.reset",
                                                 "SET", "earliest"};

        TEST_SAY("=== Testing share consumer with multiple topics ===\n");

        /* Create topics and produce messages */
        test_create_topic_wait_exists(NULL, topic1, 1, -1, 60 * 1000);
        test_create_topic_wait_exists(NULL, topic2, 1, -1, 60 * 1000);
        
        test_produce_msgs_easy(topic1, 0, 0, msgs_per_topic);
        test_produce_msgs_easy(topic2, 0, 0, msgs_per_topic);
        TEST_SAY("Produced %d messages to each topic\n", msgs_per_topic);

        /* Create share consumer */
        test_conf_init(&cons_conf, NULL, 60);
        rd_kafka_conf_set(cons_conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cons_conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        //rd_kafka_conf_set(cons_conf, "debug", "all", errstr, sizeof(errstr));

        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP, group, confs_set_group, 1);

        /* Subscribe to both topics */
        topics = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(topics, topic1, RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(topics, topic2, RD_KAFKA_PARTITION_UA);

        TEST_SAY("Subscribing to topics: %s, %s\n", topic1, topic2);
        rd_kafka_subscribe(consumer, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        /* Consume messages from both topics */
        rd_kafka_message_t **rkmessages = malloc(sizeof(rd_kafka_message_t *) * 20);
        int attempts = 10; // Number of attempts to poll so the test doesn't run indefinitely

        while (consumed_count < (msgs_per_topic * 2) && attempts > 0) {
                size_t rcvd_msgs = 0;
                rd_kafka_error_t *error;
                int i;

                printf("Polling for messages, consumed so far: %d/%d\n", consumed_count, msgs_per_topic * 2);
                error = rd_kafka_share_consume_batch(consumer, 3000, rkmessages, &rcvd_msgs);

                if (error) {
                        TEST_SAY("Consume error: %s\n", rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        attempts--;
                        continue;
                }

                for (i = 0; i < (int)rcvd_msgs; i++) {
                        rd_kafka_message_t *rkm = rkmessages[i];
                        if (!rkm->err) {
                                consumed_count++;
                                TEST_SAY("Consumed from topic %s: %d/%d total\n",
                                        rd_kafka_topic_name(rkm->rkt), consumed_count, msgs_per_topic * 2);
                        }
                        rd_kafka_message_destroy(rkm);
                }
                attempts--;
        }

        free(rkmessages);

        TEST_ASSERT(consumed_count == (msgs_per_topic * 2),
                   "Expected %d messages from both topics, got %d",
                   msgs_per_topic * 2, consumed_count);

        TEST_SAY("✓ Successfully consumed from multiple topics: %d messages\n", consumed_count);

        test_delete_topic(consumer, topic1);
        test_delete_topic(consumer, topic2);

        /* Clean up */
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Test multiple share consumers on the same topic.
 * Verifies that messages are divided (each delivered once overall).
 */
static void test_share_consumer_multi_members_same_topic(void) {
        char errstr[512];
        const char *group = "share-group-multi-member";
        char *topic = "0154-share-multi-member";
        const int total_msgs = 20;
        int consumed_total = 0;
        int c1_count = 0;
        int c2_count = 0;
        int attempts = 15;
        const char *group_conf[] = {"share.auto.offset.reset","SET","earliest"};

        TEST_SAY("=== Testing multiple share consumers on same topic ===\n");

        /* Create topic and produce messages */
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        test_produce_msgs_easy(topic, 0, 0, total_msgs);
        TEST_SAY("Produced %d messages to %s\n", total_msgs, topic);

        /* Common subscription list */
        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);

        /* Create first consumer */
        rd_kafka_conf_t *conf1;
        test_conf_init(&conf1, NULL, 60);
        rd_kafka_conf_set(conf1, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf1, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf1, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf1, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_t *c1 = rd_kafka_new(RD_KAFKA_CONSUMER, conf1, errstr, sizeof(errstr));
        TEST_ASSERT(c1, "Failed to create consumer1: %s", errstr);

        /* Create second consumer */
        rd_kafka_conf_t *conf2;
        test_conf_init(&conf2, NULL, 60);
        rd_kafka_conf_set(conf2, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf2, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf2, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf2, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_t *c2 = rd_kafka_new(RD_KAFKA_CONSUMER, conf2, errstr, sizeof(errstr));
        TEST_ASSERT(c2, "Failed to create consumer2: %s", errstr);

        /* Set group config (offset reset) to earliest */
        test_IncrementalAlterConfigs_simple(c1, RD_KAFKA_RESOURCE_GROUP,
                                            group, group_conf, 1);

        /* Subscribe both */
        rd_kafka_subscribe(c1, subs);
        rd_kafka_subscribe(c2, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Poll loop: alternate polling both consumers */
        rd_kafka_message_t *batch[32];

        while (consumed_total < total_msgs && attempts-- > 0) {
                size_t rcvd1 = 0, rcvd2 = 0;
                rd_kafka_error_t *err1 =
                    rd_kafka_share_consume_batch(c1, 2000, batch, &rcvd1);
                if (!err1) {
                        for (size_t i = 0; i < rcvd1; i++) {
                                if (!batch[i]->err) {
                                        c1_count++;
                                        consumed_total++;
                                }
                                rd_kafka_message_destroy(batch[i]);
                        }
                } else {
                        rd_kafka_error_destroy(err1);
                }

                if (consumed_total >= total_msgs)
                        break;

                rd_kafka_error_t *err2 =
                    rd_kafka_share_consume_batch(c2, 2000, batch, &rcvd2);
                if (!err2) {
                        for (size_t i = 0; i < rcvd2; i++) {
                                if (!batch[i]->err) {
                                        c2_count++;
                                        consumed_total++;
                                }
                                rd_kafka_message_destroy(batch[i]);
                        }
                } else {
                        rd_kafka_error_destroy(err2);
                }

                TEST_SAY("Progress: total=%d/%d (c1=%d, c2=%d)\n",
                         consumed_total, total_msgs, c1_count, c2_count);
        }

        TEST_ASSERT(consumed_total == total_msgs,
                    "Expected %d total messages, got %d", total_msgs,
                    consumed_total);

        TEST_SAY("✓ Multi-member share consumption complete: total=%d "
                 "(c1=%d, c2=%d)\n",
                 consumed_total, c1_count, c2_count);

        test_delete_topic(c1, topic);

        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);
}

/**
 * Single share consumer, one topic with multiple partitions.
 */
static void test_share_single_consumer_multi_partitions_one_topic(void) {
        char errstr[512];
        const char *group = "share-group-single-one-topic-mparts";
        const char *topic = "0154-share-one-topic-mparts";
        const int partition_cnt = 3;
        const int msgs_per_partition = 7;
        const int total_msgs = partition_cnt * msgs_per_partition;
        int consumed = 0;
        int attempts = 30;
        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};

        TEST_SAY("=== Single consumer, one topic (%d partitions) ===\n",
                 partition_cnt);

        test_create_topic_wait_exists(NULL, topic, partition_cnt, -1,
                                      60 * 1000);

        for (int p = 0; p < partition_cnt; p++)
                test_produce_msgs_easy(topic, p, p,
                                       msgs_per_partition);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_t *consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "create failed: %s", errstr);

        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP,
                                            group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *batch[64];

        while (consumed < total_msgs && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(consumer, 3000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err)
                                consumed++;
                        rd_kafka_message_destroy(batch[i]);
                }
                TEST_SAY("Progress: %d/%d\n", consumed, total_msgs);
        }

        TEST_ASSERT(consumed == total_msgs,
                    "Expected %d, got %d", total_msgs, consumed);

        TEST_SAY("✓ Consumed all %d messages across %d partitions\n",
                 consumed, partition_cnt);

        test_delete_topic(consumer, topic);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

/**
 * Single share consumer, multiple topics each with multiple partitions.
 */
static void test_share_single_consumer_multi_partitions_multi_topics(void) {
        char errstr[512];
        const char *group = "share-group-single-multi-topic-mparts";
        const int topic_cnt = 3;
        const int partition_cnt = 2;
        const int msgs_per_partition = 5;
        char *topics[topic_cnt];
        int total_msgs = topic_cnt * partition_cnt * msgs_per_partition;
        int consumed = 0;
        int attempts = 40;
        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};

        TEST_SAY("=== Single consumer, %d topics x %d partitions ===\n",
                 topic_cnt, partition_cnt);

        for (int t = 0; t < topic_cnt; t++) {
                topics[t] = test_mk_topic_name("0154-share-multiT-mparts", t);
                test_create_topic_wait_exists(NULL, topics[t], partition_cnt,
                                              -1, 60 * 1000);
                for (int p = 0; p < partition_cnt; p++)
                        test_produce_msgs_easy(topics[t], p,
                                               p,
                                               msgs_per_partition);
        }

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_t *consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "create failed: %s", errstr);

        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP,
                                            group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(topic_cnt);
        for (int t = 0; t < topic_cnt; t++)
                rd_kafka_topic_partition_list_add(subs, topics[t],
                                                  RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *batch[128];

        while (consumed < total_msgs && attempts-- > 0) {
                size_t rcvd = 0;
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(consumer, 3000, batch, &rcvd);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }
                for (size_t i = 0; i < rcvd; i++) {
                        if (!batch[i]->err)
                                consumed++;
                        rd_kafka_message_destroy(batch[i]);
                }
                TEST_SAY("Progress: %d/%d\n", consumed, total_msgs);
        }

        TEST_ASSERT(consumed == total_msgs,
                    "Expected %d, got %d", total_msgs, consumed);
        TEST_SAY("✓ Consumed all %d messages from %d topics\n",
                 consumed, topic_cnt);

        for (int t = 0; t < topic_cnt; t++)
                test_delete_topic(consumer, topics[t]);

        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

/**
 * Multiple share consumers, one topic with multiple partitions.
 */
static void test_share_multi_consumers_multi_partitions_one_topic(void) {
        char errstr[512];
        const char *group = "share-group-multi-cons-one-topic-mparts";
        const char *topic = "0154-share-cons-oneT-mparts";
        const int partition_cnt = 4;
        const int msgs_per_partition = 6;
        const int total_msgs = partition_cnt * msgs_per_partition;
        int consumed_total = 0;
        int c_counts[4] = {0};
        int attempts = 50;
        const int consumer_cnt = 2;
        rd_kafka_t *consumers[consumer_cnt];
        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};

        TEST_SAY("=== %d consumers, one topic, %d partitions ===\n",
                 consumer_cnt, partition_cnt);

        test_create_topic_wait_exists(NULL, topic, partition_cnt, -1,
                                      60 * 1000);
        for (int p = 0; p < partition_cnt; p++)
                test_produce_msgs_easy(topic, p, p ,
                                       msgs_per_partition);

        for (int i = 0; i < consumer_cnt; i++) {
            rd_kafka_conf_t *conf;
            test_conf_init(&conf, NULL, 60);
            rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
            rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
            rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
            rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
            consumers[i] =
                rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
            TEST_ASSERT(consumers[i], "create failed (%d): %s", i, errstr);
        }

        test_IncrementalAlterConfigs_simple(consumers[0], RD_KAFKA_RESOURCE_GROUP,
                                            group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        for (int i = 0; i < consumer_cnt; i++)
                rd_kafka_subscribe(consumers[i], subs);
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *batch[64];

        while (consumed_total < total_msgs && attempts-- > 0) {
                for (int i = 0; i < consumer_cnt; i++) {
                        size_t rcvd = 0;
                        rd_kafka_error_t *err =
                            rd_kafka_share_consume_batch(consumers[i], 1000,
                                                         batch, &rcvd);
                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }
                        for (size_t m = 0; m < rcvd; m++) {
                                if (!batch[m]->err) {
                                        c_counts[i]++;
                                        consumed_total++;
                                }
                                rd_kafka_message_destroy(batch[m]);
                        }
                }
                TEST_SAY("Progress: total=%d/%d c0=%d c1=%d\n",
                         consumed_total, total_msgs,
                         c_counts[0], c_counts[1]);
        }

        TEST_ASSERT(consumed_total == total_msgs,
                    "Expected %d total, got %d", total_msgs, consumed_total);

        TEST_SAY("✓ All %d messages consumed by %d consumers "
                 "(dist: c0=%d c1=%d)\n",
                 consumed_total, consumer_cnt, c_counts[0], c_counts[1]);

        test_delete_topic(consumers[0], topic);
        for (int i = 0; i < consumer_cnt; i++) {
                rd_kafka_consumer_close(consumers[i]);
                rd_kafka_destroy(consumers[i]);
        }
}

/**
 * Multiple consumers, multiple topics each with multiple partitions.
 */
static void test_share_multi_consumers_multi_partitions_multi_topics(void) {
        char errstr[512];
        const char *group = "share-group-multi-cons-multiT-mparts";
        const int topic_cnt = 2;
        const int partition_cnt = 3;
        const int msgs_per_partition = 5;
        const int consumer_cnt = 3;
        char *topics[topic_cnt];
        int total_msgs = topic_cnt * partition_cnt * msgs_per_partition;
        int consumed_total = 0;
        int per_cons[consumer_cnt];
        memset(per_cons, 0, sizeof(per_cons));
        int attempts = 80;
        rd_kafka_t *consumers[consumer_cnt];
        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};

        TEST_SAY("=== %d consumers, %d topics x %d partitions ===\n",
                 consumer_cnt, topic_cnt, partition_cnt);

        for (int t = 0; t < topic_cnt; t++) {
                topics[t] = test_mk_topic_name("0154-share-multiT", t);
                test_create_topic_wait_exists(NULL, topics[t], partition_cnt,
                                              -1, 60 * 1000);
                for (int p = 0; p < partition_cnt; p++)
                        test_produce_msgs_easy(topics[t], p,
                                               p,
                                               msgs_per_partition);
        }

        for (int i = 0; i < consumer_cnt; i++) {
                rd_kafka_conf_t *conf;
                test_conf_init(&conf, NULL, 60);
                rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
                rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
                rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
                rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
                consumers[i] =
                    rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
                TEST_ASSERT(consumers[i], "create failed (%d): %s", i, errstr);
        }

        test_IncrementalAlterConfigs_simple(consumers[0], RD_KAFKA_RESOURCE_GROUP,
                                            group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(topic_cnt);
        for (int t = 0; t < topic_cnt; t++)
                rd_kafka_topic_partition_list_add(subs, topics[t],
                                                  RD_KAFKA_PARTITION_UA);
        for (int i = 0; i < consumer_cnt; i++)
                rd_kafka_subscribe(consumers[i], subs);
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *batch[128];

        while (consumed_total < total_msgs && attempts-- > 0) {
                for (int i = 0; i < consumer_cnt; i++) {
                        size_t rcvd = 0;
                        rd_kafka_error_t *err =
                            rd_kafka_share_consume_batch(consumers[i], 1000,
                                                         batch, &rcvd);
                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }
                        for (size_t m = 0; m < rcvd; m++) {
                                if (!batch[m]->err) {
                                        per_cons[i]++;
                                        consumed_total++;
                                }
                                rd_kafka_message_destroy(batch[m]);
                        }
                }
                TEST_SAY("Progress: total=%d/%d c0=%d c1=%d c2=%d\n",
                         consumed_total, total_msgs,
                         per_cons[0], per_cons[1], per_cons[2]);
        }

        TEST_ASSERT(consumed_total == total_msgs,
                    "Expected %d total, got %d", total_msgs, consumed_total);

        TEST_SAY("✓ All %d messages consumed across %d consumers "
                 "(dist: c0=%d c1=%d c2=%d)\n",
                 consumed_total, consumer_cnt,
                 per_cons[0], per_cons[1], per_cons[2]);

        for (int t = 0; t < topic_cnt; t++)
                test_delete_topic(consumers[0], topics[t]);

        for (int i = 0; i < consumer_cnt; i++) {
                rd_kafka_consumer_close(consumers[i]);
                rd_kafka_destroy(consumers[i]);
        }
}


int main_0154_share_consumer(int argc, char **argv) {

        test_poll_no_subscribe_fails();
        test_subscribe_and_poll_no_records();
        test_subscribe_poll_unsubscribe();
        test_subscribe_poll_subscribe();
        test_subscribe_unsubscribe_poll_fails();
        test_share_consumer_messages();
        test_share_consumer_multiple_topics();
        test_share_consumer_multi_members_same_topic();
        test_share_single_consumer_multi_partitions_one_topic();
        test_share_single_consumer_multi_partitions_multi_topics();
        test_share_multi_consumers_multi_partitions_one_topic();
        test_share_multi_consumers_multi_partitions_multi_topics();
        return 0;
}
