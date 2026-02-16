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

#include "../src/rdkafka_proto.h"

/**
 * @name KIP-932 ShareFetch mock broker tests using the share consumer API.
 *
 * Exercises the ShareFetch path via mock broker.  There is no coordinator
 * or ShareAcknowledge support in the mock broker, so group management and
 * ack-based state transitions are not validated here.
 */

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

        /* Create a producer targeting the mock cluster */
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

static rd_kafka_share_t *new_share_consumer(const char *bootstraps,
                                            const char *group_id) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *consumer;

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);

        consumer = rd_kafka_share_consumer_new(conf, NULL, 0);
        TEST_ASSERT(consumer != NULL, "Failed to create share consumer");
        rd_kafka_share_poll_set_consumer(consumer);
        return consumer;
}

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

static int
consume_n(rd_kafka_share_t *consumer, int expected, int max_attempts) {
        int consumed = 0;
        int attempts = 0;

        while (consumed < expected && attempts < max_attempts) {
                rd_kafka_message_t *rkmessages[100];
                size_t rcvd_msgs = 0;
                rd_kafka_error_t *error;

                error = rd_kafka_share_consume_batch(consumer, 500, rkmessages,
                                                     &rcvd_msgs);
                attempts++;

                if (error) {
                        TEST_SAY("consume error: %s\n",
                                 rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (size_t i = 0; i < rcvd_msgs; i++) {
                        rd_kafka_message_t *rkmsg = rkmessages[i];
                        if (rkmsg->err) {
                                TEST_SAY("consume error: %s\n",
                                         rd_kafka_message_errstr(rkmsg));
                                rd_kafka_message_destroy(rkmsg);
                                continue;
                        }
                        TEST_SAY("Consumed: %.*s\n", (int)rkmsg->len,
                                 (const char *)rkmsg->payload);
                        consumed++;
                        rd_kafka_message_destroy(rkmsg);
                }
        }

        return consumed;
}


static void do_test_basic_consume(void) {
        const char *topic = "kip932_pos_basic";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-basic");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 50);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == msgcnt, "Expected %d consumed, got %d", msgcnt,
                    consumed);
        SUB_TEST_PASS();
}

static void do_test_followup_fetch(void) {
        const char *topic = "kip932_pos_followup";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, 5);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-followup");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 3, 30);
        consumed += consume_n(consumer, 2, 30);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 5, "Expected 5 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

static void do_test_multi_partition(void) {
        const char *topic = "kip932_pos_multi_part";
        const int msgcnt  = 6;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-multipart");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 60);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == msgcnt, "Expected %d consumed, got %d", msgcnt,
                    consumed);
        SUB_TEST_PASS();
}

static void do_test_multi_topic(void) {
        const char *topic_a  = "kip932_pos_topic_a";
        const char *topic_b  = "kip932_pos_topic_b";
        const char *topics[] = {topic_a, topic_b};
        test_ctx_t ctx       = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic A");
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic B");

        produce_messages(ctx.producer, topic_a, 2);
        produce_messages(ctx.producer, topic_b, 2);
        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-multitopic");
        subscribe_topics(consumer, topics, 2);
        consumed = consume_n(consumer, 4, 40);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 4, "Expected 4 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

static void do_test_empty_topic_no_records(void) {
        const char *topic = "kip932_pos_empty";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-empty");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

static void do_test_negative_sharefetch_error(rd_kafka_resp_err_t err) {
        const char *topic = "kip932_neg_sharefetch_error";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);

        rd_kafka_mock_push_request_errors(ctx.mcluster, RD_KAFKAP_ShareFetch, 1,
                                          err);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-sharefetch");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
}

static void do_test_sharefetch_invalid_session_epoch(void) {
        SUB_TEST_QUICK();
        do_test_negative_sharefetch_error(
            RD_KAFKA_RESP_ERR_INVALID_FETCH_SESSION_EPOCH);
        SUB_TEST_PASS();
}

static void do_test_sharefetch_unknown_topic_or_part(void) {
        SUB_TEST_QUICK();
        do_test_negative_sharefetch_error(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        SUB_TEST_PASS();
}

static void do_test_sghb_error(rd_kafka_resp_err_t err, int count) {
        const char *topic = "kip932_neg_sghb";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;
        rd_kafka_resp_err_t *errs;
        int i;

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);

        /* Build an array of 'count' identical errors and push them all.
         * Using the array variant avoids UB from mismatched varargs count. */
        errs = malloc(sizeof(*errs) * count);
        TEST_ASSERT(errs != NULL, "malloc failed");
        for (i = 0; i < count; i++)
                errs[i] = err;
        rd_kafka_mock_push_request_errors_array(
            ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, (size_t)count, errs);
        free(errs);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-sghb");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
}

static void do_test_sghb_coord_unavailable(void) {
        SUB_TEST_QUICK();
        do_test_sghb_error(RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE, 50);
        SUB_TEST_PASS();
}

static void do_test_topic_error(rd_kafka_resp_err_t err) {
        const char *topic = "kip932_neg_topic_error";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic, err);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-topicerr");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
}

static void do_test_topic_error_unknown_topic_or_part(void) {
        SUB_TEST_QUICK();
        do_test_topic_error(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        SUB_TEST_PASS();
}

static void do_test_unknown_topic_subscription(void) {
        const char *topic = "kip932_neg_unknown_topic";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-unknown-topic");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

static void do_test_empty_fetch_no_records(void) {
        const char *topic = "kip932_neg_empty_fetch";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-empty");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 0, "Expected 0 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

/**
 * @brief Verify that ShareFetch rejects requests from an unregistered member
 *        (UNKNOWN_MEMBER_ID), and that after the member re-joins it can
 *        consume again.
 *
 *  Phase 1: Consumer joins normally via SGHB -> consumes messages OK.
 *  Phase 2: Push SGHB errors -> heartbeats fail -> member expires -> broker
 *           rejects ShareFetch with UNKNOWN_MEMBER_ID.
 *  Phase 3: SGHB errors drain -> member re-joins -> consumes again.
 */
static void do_test_member_validation(void) {
        const char *topic = "kip932_member_validation";
        const int msgcnt  = 4;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_p1, consumed_p3;

        SUB_TEST();

        /* Short session timeout so the member is evicted quickly once
         * heartbeats stop succeeding. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-member-val");
        subscribe_topics(consumer, &topic, 1);

        /* Phase 1: Consume normally -- member is registered via SGHB. */
        consumed_p1 = consume_n(consumer, 2, 30);
        TEST_SAY("member_validation: phase1 consumed %d/2\n", consumed_p1);

        /* Phase 2: Block SGHB so heartbeats fail.
         * Push enough errors to cover the window while we wait for the
         * member to be evicted. */
        rd_kafka_mock_push_request_errors(
            ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 20,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE);

        /* Wait for the member to be evicted (500ms session timeout + margin).
         */
        usleep(1500 * 1000);

        /* Phase 3: SGHB errors will eventually drain. Once a SGHB
         * succeeds, the member re-joins and the remaining records
         * become fetchable again. */
        consumed_p3 = consume_n(consumer, 2, 50);
        TEST_SAY("member_validation: phase3 consumed %d/2\n", consumed_p3);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed_p1 >= 2 && (consumed_p1 + consumed_p3) >= msgcnt,
                    "Expected at least 2+2, got %d+%d", consumed_p1,
                    consumed_p3);
        SUB_TEST_PASS();
}

static void do_test_sharefetch_session_expiry_rtt(void) {
        const char *topic = "kip932_rtt_expiry";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST();

        /* Session timeout must be long enough for normal requests
         * to complete, but short enough to expire during high RTT. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 1000);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, 2);

        consumer = new_share_consumer(ctx.bootstraps, "sg-rtt-expiry");
        subscribe_topics(consumer, &topic, 1);

        /* Phase 1: consume one message with normal RTT (no injection). */
        consumed = consume_n(consumer, 1, 20);
        TEST_SAY("rtt_expiry: phase1 consumed %d/1\n", consumed);

        /* Phase 2: inject RTT >> session timeout to force session expiry.
         * All requests to broker 1 now take 3s, but the session
         * expires after 1s of inactivity. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 3000);
        usleep(2000 * 1000); /* wait for session to expire */

        /* Phase 3: clear RTT and let the consumer recover. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 0);
        consumed += consume_n(consumer, 1, 30);
        TEST_SAY("rtt_expiry: phase3 consumed %d/2 total\n", consumed);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == 2, "Expected 2 consumed, got %d", consumed);
        SUB_TEST_PASS();
}

static void do_test_forgotten_topics(void) {
        const char *topic_a = "kip932_forgotten_a";
        const char *topic_b = "kip932_forgotten_b";
        const char *both[]  = {topic_a, topic_b};
        test_ctx_t ctx      = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic A");
        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic B");

        /* Produce 2 messages to each topic */
        produce_messages(ctx.producer, topic_a, 2);
        produce_messages(ctx.producer, topic_b, 2);

        /* Subscribe to both topics and consume all 4 messages */
        consumer = new_share_consumer(ctx.bootstraps, "sg-forgotten");
        subscribe_topics(consumer, both, 2);
        consumed = consume_n(consumer, 4, 40);
        TEST_SAY("forgotten_topics: consumed %d/4 from both topics\n",
                 consumed);

        /* Re-subscribe to only topic_a (topic_b becomes forgotten) */
        subscribe_topics(consumer, &topic_a, 1);

        /* Produce 2 more messages to topic_a */
        produce_messages(ctx.producer, topic_a, 2);

        /* Consume the 2 new messages -- only topic_a should deliver */
        consumed += consume_n(consumer, 2, 30);
        TEST_SAY("forgotten_topics: consumed %d/6 total after forget\n",
                 consumed);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        /* We expect at least the 4 initial + 2 from topic_a = 6.
         * Depending on timing the consumer may or may not have already
         * received all messages from the first round, so we accept >= 4. */
        TEST_ASSERT(consumed >= 4, "Expected at least 4 consumed, got %d",
                    consumed);
        SUB_TEST_PASS();
}

/**
 * @brief Produce messages one-at-a-time (each flush creates a separate
 *        msgset on the mock partition), then consume and verify all are
 *        received.  This validates that the ShareFetch response includes
 *        records from *all* acquired msgsets, not just the first one.
 */
static void do_test_multi_batch_consume(void) {
        const char *topic = "kip932_multi_batch";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        SUB_TEST_QUICK();

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");

        /* Produce each message individually with a flush in between,
         * guaranteeing separate msgsets on the mock partition. */
        for (int i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "batch-%d", i);
                TEST_ASSERT(rd_kafka_producev(
                                ctx.producer, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Produce failed");
                rd_kafka_flush(ctx.producer, 5000);
        }

        consumer = new_share_consumer(ctx.bootstraps, "sg-multi-batch");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 50);
        TEST_SAY("multi_batch: consumed %d/%d\n", consumed, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed == msgcnt, "Expected %d consumed, got %d", msgcnt,
                    consumed);
        SUB_TEST_PASS();
}

/**
 * @brief Verify that max_delivery_attempts causes records to be archived
 *        after the limit is exceeded.  Consumer A acquires all records, then
 *        its session times out (releasing locks).  Consumer B acquires them
 *        again, and its session also times out.  After the delivery limit is
 *        exhausted, Consumer C should see 0 available records.
 */
static void do_test_max_delivery_attempts(void) {
        const char *topic = "kip932_max_delivery";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, consumed_c;

        SUB_TEST();

        /* Set max delivery attempts to 2 and a short session timeout
         * so locks expire quickly after consumer destruction. */
        rd_kafka_mock_sharegroup_set_max_delivery_attempts(ctx.mcluster, 2);
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Delivery 1: Consumer A acquires and "crashes" (no ack). */
        consumer = new_share_consumer(ctx.bootstraps, "sg-max-delivery");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        TEST_SAY("max_delivery: A consumed %d/%d (delivery 1)\n", consumed_a,
                 msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Delivery 2: Consumer B acquires same records again (delivery_count
         * reaches 2 = limit) and "crashes". */
        consumer = new_share_consumer(ctx.bootstraps, "sg-max-delivery");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        TEST_SAY("max_delivery: B consumed %d/%d (delivery 2)\n", consumed_b,
                 msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Delivery 3 attempt: Consumer C should get 0 records because
         * all records have been archived (delivery_count >= max). */
        consumer = new_share_consumer(ctx.bootstraps, "sg-max-delivery");
        subscribe_topics(consumer, &topic, 1);
        consumed_c = consume_n(consumer, 1, 10);
        TEST_SAY("max_delivery: C consumed %d/0 (should be archived)\n",
                 consumed_c);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed_a == msgcnt && consumed_b == msgcnt &&
                        consumed_c == 0,
                    "Expected A=%d B=%d C=0, got A=%d B=%d C=%d", msgcnt,
                    msgcnt, consumed_a, consumed_b, consumed_c);
        SUB_TEST_PASS();
}

/**
 * @brief Verify that record_lock_duration_ms controls how long acquired
 *        records stay locked, independently of session_timeout_ms.
 *        Sets a short lock duration (300ms) with a longer session timeout
 *        (10s).  Consumer A acquires records and "crashes".  After the short
 *        lock duration expires, Consumer B should be able to acquire them
 *        even though A's session hasn't timed out yet.
 */
static void do_test_record_lock_duration(void) {
        const char *topic = "kip932_lock_duration";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b;

        SUB_TEST();

        /* Long session timeout, short record lock duration. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 10000);
        rd_kafka_mock_sharegroup_set_record_lock_duration(ctx.mcluster, 300);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A acquires records, then crashes (no close). */
        consumer = new_share_consumer(ctx.bootstraps, "sg-lock-duration");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        TEST_SAY("lock_duration: A consumed %d/%d\n", consumed_a, msgcnt);
        rd_kafka_share_destroy(consumer);

        /* Wait for record lock to expire (300ms + margin),
         * but NOT session timeout (10s). */
        usleep(800 * 1000);

        /* Consumer B should get the records because locks have expired
         * even though A's session is still technically alive. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-lock-duration");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        TEST_SAY("lock_duration: B consumed %d/%d\n", consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed_a == msgcnt && consumed_b == msgcnt,
                    "Expected A=%d B=%d, got A=%d B=%d", msgcnt, msgcnt,
                    consumed_a, consumed_b);
        SUB_TEST_PASS();
}

/**
 * @brief Multi-consumer lock expiry test.
 *
 * Consumer A acquires records, then crashes (destroyed without close).
 * After the lock expiry timeout, consumer B should be able to pick up
 * the same records because the proactive lock-expiry scan releases them.
 */
static void do_test_multi_consumer_lock_expiry(void) {
        const char *topic = "kip932_multi_consumer_lock";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer_a, *consumer_b;
        int consumed_a, consumed_b;

        SUB_TEST();

        /* Use a short session/lock timeout so the test runs quickly. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        TEST_ASSERT(rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: subscribe and consume all records (acquires locks). */
        consumer_a =
            new_share_consumer(ctx.bootstraps, "sg-multi-consumer-lock");
        subscribe_topics(consumer_a, &topic, 1);
        consumed_a = consume_n(consumer_a, msgcnt, 50);
        TEST_SAY("multi_consumer: A consumed %d/%d\n", consumed_a, msgcnt);

        /* Simulate crash: destroy consumer A without calling close.
         * The session will time out and the proactive lock-expiry
         * timer will release A's locks. */
        rd_kafka_share_destroy(consumer_a);

        /* Wait for locks to expire (session_timeout=500ms, add margin). */
        usleep(1500 * 1000);

        /* Consumer B: joins the same share group, should get the same
         * records once the locks have been released. */
        consumer_b =
            new_share_consumer(ctx.bootstraps, "sg-multi-consumer-lock");
        subscribe_topics(consumer_b, &topic, 1);
        consumed_b = consume_n(consumer_b, msgcnt, 50);
        TEST_SAY("multi_consumer: B consumed %d/%d\n", consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer_b);
        rd_kafka_share_destroy(consumer_b);
        test_ctx_destroy(&ctx);

        TEST_ASSERT(consumed_a == msgcnt && consumed_b == msgcnt,
                    "Expected A=%d B=%d, got A=%d B=%d", msgcnt, msgcnt,
                    consumed_a, consumed_b);
        SUB_TEST_PASS();
}


int main_0156_kip932_sharefetch_mockbroker(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        /* Positive scenarios */
        do_test_basic_consume();
        do_test_followup_fetch();
        do_test_multi_partition();
        do_test_multi_topic();
        do_test_empty_topic_no_records();
        do_test_sharefetch_session_expiry_rtt();
        do_test_forgotten_topics();
        do_test_multi_batch_consume();
        do_test_max_delivery_attempts();
        do_test_record_lock_duration();
        do_test_multi_consumer_lock_expiry();

        /* Negative scenarios */
        do_test_sharefetch_invalid_session_epoch();
        do_test_sharefetch_unknown_topic_or_part();
        do_test_sghb_coord_unavailable();
        do_test_topic_error_unknown_topic_or_part();
        do_test_unknown_topic_subscription();
        do_test_empty_fetch_no_records();
        do_test_member_validation();

        return 0;
}
