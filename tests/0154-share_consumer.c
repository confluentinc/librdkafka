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
        char *topic = test_mk_topic_name("0154-share-empty-records", 0);
        char *group = "share-group-empty";

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
        char *topic = test_mk_topic_name("0154-share-unsub", 0);
        char *group = "share-group-unsub";
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
        char *topic = test_mk_topic_name("0154-share-unsub-fail", 0);
        char *group = "share-group-unsub-fail";

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
        char *topic = test_mk_topic_name("0154-share-test", 0);
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
        char *topic1 = test_mk_topic_name("0154-share-topic1", 0);
        char *topic2 = test_mk_topic_name("0154-share-topic2", 0);
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


int main_0154_share_consumer(int argc, char **argv) {

        test_poll_no_subscribe_fails();
        test_subscribe_and_poll_no_records();
        test_subscribe_poll_unsubscribe();
        test_subscribe_poll_subscribe();
        test_subscribe_unsubscribe_poll_fails();
        test_share_consumer_messages();
        //test_share_consumer_multiple_topics();
        return 0;
}
