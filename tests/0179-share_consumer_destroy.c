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
#include "../src/rdkafka.h"
#include "../src/rdkafka_proto.h"

#define CONSUME_ARRAY 1000
#define TEST_MSGS     100

typedef struct test_ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} test_ctx_t;


/**
 * @brief Initialize test context with mock cluster.
 */
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


/**
 * @brief Destroy test context.
 */
static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        if (ctx->mcluster)
                test_mock_cluster_destroy(ctx->mcluster);
        memset(ctx, 0, sizeof(*ctx));
}


/**
 * @brief Produce messages to topic.
 */
static void
produce_messages(rd_kafka_t *producer, const char *topic, int msgcnt) {
        for (int i = 0; i < msgcnt; i++) {
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
 * @brief Create share consumer.
 */
static rd_kafka_share_t *new_share_consumer(const char *bootstraps,
                                            const char *group_id,
                                            rd_bool_t explicit_mode) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *consumer;

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "debug", "all");

        if (explicit_mode) {
                test_conf_set(conf, "share.acknowledgement.mode", "explicit");
        }

        consumer = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(consumer != NULL, "Failed to create share consumer");
        return consumer;
}


/**
 * @brief Subscribe to topics.
 */
static void subscribe_topics(rd_kafka_share_t *consumer,
                             const char **topics,
                             int topic_cnt) {
        rd_kafka_topic_partition_list_t *tpl =
            rd_kafka_topic_partition_list_new(topic_cnt);
        for (int i = 0; i < topic_cnt; i++) {
                rd_kafka_topic_partition_list_add(tpl, topics[i],
                                                  RD_KAFKA_PARTITION_UA);
        }
        TEST_ASSERT(!rd_kafka_share_subscribe(consumer, tpl),
                    "Subscribe failed");
        rd_kafka_topic_partition_list_destroy(tpl);
}


/**
 * @brief Acknowledge messages in [start, end) and log each result.
 */
static void ack_range_logged(rd_kafka_share_t *rkshare,
                             rd_kafka_message_t **rkmessages,
                             int start,
                             int end,
                             int rcvd) {
        int i;
        for (i = start; i < end && i < rcvd; i++) {
                rd_kafka_resp_err_t ack_err =
                    rd_kafka_share_acknowledge(rkshare, rkmessages[i]);
                TEST_SAY("  ack msg[%d] %s[%" PRId32 "]@%" PRId64 " -> %s\n", i,
                         rd_kafka_topic_name(rkmessages[i]->rkt),
                         rkmessages[i]->partition, rkmessages[i]->offset,
                         rd_kafka_err2str(ack_err));
        }
}


/**
 * @brief This test uses mock brokers to simulate delayed broker responses and
 * makes commit* calls causing acknowledgements to get cached. Eventually, calls
 * destroy() to validate that it does not hang.
 * @param destroy_flags 0 for normal destroy,
 * RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE to skip consumer close.
 */
static void
do_test_destroy_with_cached_acks_and_delayed_broker(int destroy_flags) {
        test_ctx_t ctx;
        const char *topic;
        const char *group = "0179-destroy-cached-acks";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd = 0;
        int i;
        int attempts        = 0;
        int max_attempts    = 30;
        int broker_delay_ms = 5000; /* 5 seconds */
        test_timing_t t_destroy;

        SUB_TEST("destroy_flags=0x%x", destroy_flags);

        /* Initialize test context */
        ctx = test_ctx_new();

        topic =
            test_mk_topic_name("0179-destroy-cached-acks-delayed-broker", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce test messages */
        TEST_SAY("Producing %d messages to topic %s\n", TEST_MSGS, topic);
        produce_messages(ctx.producer, topic, TEST_MSGS);

        /* Create share consumer with explicit ack mode */
        TEST_SAY("Creating share consumer with explicit ack\n");
        rkshare = new_share_consumer(ctx.bootstraps, group, rd_true);
        subscribe_topics(rkshare, &topic, 1);

        /* Consume at least 10 messages with retry */
        TEST_SAY("Consuming messages (up to %d attempts)\n", max_attempts);
        while (rcvd < 10 && attempts < max_attempts) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                }
                attempts++;
                rd_usleep(100 * 1000, NULL);
        }

        TEST_ASSERT(rcvd >= 10,
                    "Expected at least 10 messages after %d attempts, got %d",
                    max_attempts, (int)rcvd);
        TEST_SAY("Successfully consumed %d messages\n", (int)rcvd);

        /* Setup broker delays for ShareAcknowledge responses */
        TEST_SAY(
            "Setting up %dms delays for ShareAcknowledge responses on all "
            "brokers\n",
            broker_delay_ms);
        for (i = 1; i <= 3; i++) {
                rd_kafka_mock_broker_push_request_error_rtts(
                    ctx.mcluster, i, RD_KAFKAP_ShareAcknowledge, 3,
                    RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms,
                    RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms,
                    RD_KAFKA_RESP_ERR_NO_ERROR, broker_delay_ms);
        }

        /* Step 1: Acknowledge first 2 messages and commit async */
        TEST_SAY(
            "Step 1: Acknowledging messages 0-1 and calling commit_async\n");
        ack_range_logged(rkshare, rkmessages, 0, 2, (int)rcvd);
        /* The below call should keep the broker busy */
        rd_kafka_share_commit_async(rkshare);

        /* Step 2: Acknowledge next 4 messages and commit async */
        TEST_SAY(
            "Step 2: Acknowledging messages 2-5 and calling commit_async "
            "(should cache)\n");
        ack_range_logged(rkshare, rkmessages, 2, 6, (int)rcvd);
        rd_kafka_share_commit_async(rkshare);

        /* Step 3: Acknowledge next 4 messages and commit sync */
        TEST_SAY(
            "Step 3: Acknowledging messages 6-9 and calling "
            "commit_sync (should cache)\n");
        ack_range_logged(rkshare, rkmessages, 6, 10, (int)rcvd);

        rd_kafka_topic_partition_list_t *commit_result = NULL;
        rd_kafka_share_commit_sync(rkshare, 5000, &commit_result);
        if (commit_result)
                rd_kafka_topic_partition_list_destroy(commit_result);

        /* Destroy all consumed messages */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        /* Now test destroy behavior with timing */
        TEST_SAY("Calling destroy with flags 0x%x\n", destroy_flags);
        TIMING_START(&t_destroy, "destroy");

        if (destroy_flags)
                rd_kafka_share_destroy_flags(rkshare, destroy_flags);
        else
                rd_kafka_share_destroy(rkshare);

        TIMING_STOP(&t_destroy);

        /* Verify timing expectations */
        if (destroy_flags & RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE) {
                /* With NO_CONSUMER_CLOSE, destroy should return quickly
                 * without waiting for broker reply) */
                TEST_ASSERT(
                    TIMING_DURATION(&t_destroy) < 100 * 1000, /* 100 ms */
                    "destroy(NO_CONSUMER_CLOSE) took %dms, expected < 100ms "
                    "(should not wait for broker)",
                    (int)(TIMING_DURATION(&t_destroy) / 1000));
                TEST_SAY(
                    "destroy(NO_CONSUMER_CLOSE) completed in %dms (expected "
                    "fast)\n",
                    (int)(TIMING_DURATION(&t_destroy) / 1000));
        } else {
                /* Without NO_CONSUMER_CLOSE, destroy calls close() internally
                 * which should wait for broker responses. Destroy should
                 * wait for the existing commit async request to complete and
                 * the session leave request initiated by close() */
                int expected_max_ms =
                    2 * broker_delay_ms + 2000; /* +2 sec overhead */

                TEST_ASSERT(
                    TIMING_DURATION(&t_destroy) <= (expected_max_ms * 1000),
                    "destroy() took %dms, expected <= %dms",
                    (int)(TIMING_DURATION(&t_destroy) / 1000), expected_max_ms);

                TEST_SAY(
                    "destroy() completed in %dms (expected < %dms for broker "
                    "delay)\n",
                    (int)(TIMING_DURATION(&t_destroy) / 1000), expected_max_ms);
        }

        TEST_SAY("Destroy completed successfully\n");

        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Test destroying share consumer after acknowledge without commit.
 *
 * This test consumes messages, acknowledges them, but destroys the consumer
 * before explicitly committing. Tests that destroy handles pending acks
 * correctly.
 *
 * @param destroy_flags Destroy flags (0 or
 * RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE)
 */
static void do_test_destroy_after_acknowledge(int destroy_flags) {
        test_ctx_t ctx;
        const char *topic;
        const char *group = "0179-destroy-after-ack";
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_message_t *rkmessages[CONSUME_ARRAY];
        size_t rcvd = 0;
        int i;
        int attempts     = 0;
        int max_attempts = 30;

        SUB_TEST("destroy_flags=0x%x", destroy_flags);

        /* Initialize test context */
        ctx = test_ctx_new();

        topic = test_mk_topic_name("0179-destroy-after-ack", 1);
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 3, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce test messages */
        TEST_SAY("Producing %d messages to topic %s\n", TEST_MSGS, topic);
        produce_messages(ctx.producer, topic, TEST_MSGS);

        /* Create share consumer with explicit ack mode */
        TEST_SAY("Creating share consumer with explicit ack\n");
        rkshare = new_share_consumer(ctx.bootstraps, group, rd_false);
        subscribe_topics(rkshare, &topic, 1);

        /* Consume messages */
        TEST_SAY("Consuming messages (up to %d attempts)\n", max_attempts);
        while (rcvd < 10 && attempts < max_attempts) {
                size_t batch_rcvd = CONSUME_ARRAY - rcvd;
                error             = rd_kafka_share_consume_batch(
                    rkshare, 3000, rkmessages + rcvd, &batch_rcvd);

                if (error) {
                        TEST_SAY("Attempt %d: consume error: %s\n", attempts,
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                } else if (batch_rcvd > 0) {
                        TEST_SAY("Attempt %d: consumed %d messages\n", attempts,
                                 (int)batch_rcvd);
                        rcvd += batch_rcvd;
                }
                attempts++;
                /* 100 ms */
                rd_usleep(100 * 1000, NULL);
        }

        TEST_ASSERT(rcvd >= 10,
                    "Expected at least 10 messages after %d attempts, got %d",
                    max_attempts, (int)rcvd);
        TEST_SAY("Successfully consumed %d messages\n", (int)rcvd);

        /* Acknowledge messages but DO NOT commit */
        TEST_SAY("Acknowledging %d messages without commit\n", (int)rcvd);
        for (i = 0; i < (int)rcvd; i++) {
                rd_kafka_share_acknowledge(rkshare, rkmessages[i]);
        }

        /* Destroy all consumed messages */
        for (i = 0; i < (int)rcvd; i++)
                rd_kafka_message_destroy(rkmessages[i]);

        /* Now destroy consumer with pending acknowledged messages */
        TEST_SAY("Calling destroy with flags 0x%x (with pending acks)\n",
                 destroy_flags);
        if (destroy_flags)
                rd_kafka_share_destroy_flags(rkshare, destroy_flags);
        else
                rd_kafka_share_destroy(rkshare);

        TEST_SAY("Destroy completed successfully\n");

        test_ctx_destroy(&ctx);

        SUB_TEST_PASS();
}


/**
 * @brief Test destroying share consumer after subscribe/unsubscribe.
 *
 * This test creates a share consumer, optionally subscribes to topics,
 * optionally unsubscribes, then destroys it without consuming any messages.
 * Tests various combinations similar to 0116-kafkaconsumer_close.
 *
 * @param do_subscribe Whether to subscribe to topics
 * @param do_unsubscribe Whether to unsubscribe before destroy
 * @param destroy_flags Destroy flags (0 or
 * RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE)
 */
static void do_test_destroy_with_subscribe_unsubscribe(int do_subscribe,
                                                       int do_unsubscribe,
                                                       int destroy_flags) {
        rd_kafka_share_t *consumer;
        rd_kafka_topic_partition_list_t *topics;
        const char *topic = "0179-test-destroy-sub-unsub";
        rd_kafka_resp_err_t err;

        SUB_TEST("subscribe=%d, unsubscribe=%d, destroy_flags=0x%x",
                 do_subscribe, do_unsubscribe, destroy_flags);

        TEST_SAY("Creating share consumer\n");
        consumer = test_create_share_consumer("0179-sub-unsub-destroy-test",
                                              "explicit");

        if (do_subscribe) {
                TEST_SAY("Subscribing to topic: %s\n", topic);
                topics = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(topics, topic,
                                                  RD_KAFKA_PARTITION_UA);
                err = rd_kafka_share_subscribe(consumer, topics);
                TEST_ASSERT(!err, "Subscribe failed: %s",
                            rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(topics);
        }

        if (do_unsubscribe) {
                TEST_SAY("Unsubscribing from all topics\n");
                err = rd_kafka_share_unsubscribe(consumer);
                TEST_ASSERT(!err, "Unsubscribe failed: %s",
                            rd_kafka_err2str(err));
        }

        TEST_SAY("Calling destroy with flags 0x%x\n", destroy_flags);
        if (destroy_flags)
                rd_kafka_share_destroy_flags(consumer, destroy_flags);
        else
                rd_kafka_share_destroy(consumer);

        TEST_SAY("Successfully destroyed share consumer\n");

        SUB_TEST_PASS();
}


int main_0179_share_consumer_destroy(int argc, char **argv) {
        /* Set overall timeout */
        test_timeout_set(120);

        /* Test destroy with subscribe/unsubscribe combinations
         * Similar to 0116-kafkaconsumer_close, test all combinations */
        do_test_destroy_with_subscribe_unsubscribe(0, 0, 0);
        do_test_destroy_with_subscribe_unsubscribe(
            1, 0, 0); /* subscribe, no unsubscribe */
        do_test_destroy_with_subscribe_unsubscribe(
            1, 1, 0); /* subscribe then unsubscribe */
        do_test_destroy_with_subscribe_unsubscribe(
            1, 0, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        do_test_destroy_with_subscribe_unsubscribe(
            1, 1, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        do_test_destroy_with_subscribe_unsubscribe(
            0, 0, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        // /* Test destroy after acknowledge without commit */
        do_test_destroy_after_acknowledge(0);
        do_test_destroy_after_acknowledge(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        /* Test destroy with inflight operations and broker delays */
        do_test_destroy_with_cached_acks_and_delayed_broker(0);
        do_test_destroy_with_cached_acks_and_delayed_broker(
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        return 0;
}