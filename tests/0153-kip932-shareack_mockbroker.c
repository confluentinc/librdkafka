/*
 * KIP-932 Share Acknowledgement (implicit ack) mock broker tests.
 *
 */

#include "rdkafka.h"
#include "rdkafka_mock.h"
#include "rdkafka_protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ===================================================================
 *  Utility helpers 
 * =================================================================== */

static void die(const char *msg) {
        fprintf(stderr, "error: %s\n", msg);
        exit(1);
}

static void conf_set(rd_kafka_conf_t *conf, const char *name,
                     const char *value) {
        char errstr[512];
        if (rd_kafka_conf_set(conf, name, value, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                die(errstr);
}

typedef struct test_ctx_s {
        rd_kafka_t *producer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
} test_ctx_t;

static test_ctx_t test_ctx_new(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        test_ctx_t ctx;

        memset(&ctx, 0, sizeof(ctx));
        conf_set(conf, "test.mock.num.brokers", "3");
        ctx.producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!ctx.producer)
                die("Failed to create producer");

        ctx.mcluster = rd_kafka_handle_mock_cluster(ctx.producer);
        if (!ctx.mcluster)
                die("Failed to get mock cluster handle");
        ctx.bootstraps = rd_kafka_mock_cluster_bootstraps(ctx.mcluster);
        if (!ctx.bootstraps)
                die("Failed to get mock bootstraps");

        if (rd_kafka_mock_set_apiversion(ctx.mcluster,
                                         RD_KAFKAP_ShareGroupHeartbeat, 1,
                                         1) != RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to enable ShareGroupHeartbeat");
        if (rd_kafka_mock_set_apiversion(ctx.mcluster, RD_KAFKAP_ShareFetch, 1,
                                         1) != RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to enable ShareFetch");

        return ctx;
}

static void test_ctx_destroy(test_ctx_t *ctx) {
        if (ctx->producer)
                rd_kafka_destroy(ctx->producer);
        memset(ctx, 0, sizeof(*ctx));
}

static void produce_messages(rd_kafka_t *producer, const char *topic,
                             int msgcnt) {
        for (int i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "%s-%d", topic, i);
                if (rd_kafka_producev(
                        producer, RD_KAFKA_V_TOPIC(topic),
                        RD_KAFKA_V_VALUE(payload, strlen(payload)),
                        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                        RD_KAFKA_V_END) != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        die("Produce failed");
                }
        }
        rd_kafka_flush(producer, 5000);
}

static rd_kafka_share_t *new_share_consumer(const char *bootstraps,
                                            const char *group_id) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_share_t *consumer;

        conf_set(conf, "bootstrap.servers", bootstraps);
        conf_set(conf, "group.id", group_id);
        consumer = rd_kafka_share_consumer_new(conf, NULL, 0);
        if (!consumer)
                die("Failed to create consumer");
        rd_kafka_share_poll_set_consumer(consumer);
        return consumer;
}

static void subscribe_topics(rd_kafka_share_t *consumer,
                             const char **topics, int topic_cnt) {
        rd_kafka_topic_partition_list_t *tpl =
            rd_kafka_topic_partition_list_new(topic_cnt);
        for (int i = 0; i < topic_cnt; i++) {
                rd_kafka_topic_partition_list_add(tpl, topics[i],
                                                 RD_KAFKA_PARTITION_UA);
        }
        if (rd_kafka_share_subscribe(consumer, tpl))
                die("Subscribe failed");
        rd_kafka_topic_partition_list_destroy(tpl);
}

static int consume_n(rd_kafka_share_t *consumer, int expected,
                     int max_attempts) {
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
                        fprintf(stderr, "consume error: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                for (size_t i = 0; i < rcvd_msgs; i++) {
                        rd_kafka_message_t *rkmsg = rkmessages[i];
                        if (rkmsg->err) {
                                fprintf(stderr, "consume error: %s\n",
                                        rd_kafka_message_errstr(rkmsg));
                                rd_kafka_message_destroy(rkmsg);
                                continue;
                        }
                        printf("  Consumed: %.*s\n", (int)rkmsg->len,
                               (const char *)rkmsg->payload);
                        consumed++;
                        rd_kafka_message_destroy(rkmsg);
                }
        }

        return consumed;
}

/* ===================================================================
 *  Positive test scenarios
 * =================================================================== */

/**
 * @brief Basic implicit ack prevents re-delivery.
 *
 * 1. Produce 5 messages.
 * 2. Consume all 5 (first ShareFetch acquires them).
 * 3. Poll again — the next ShareFetch carries AcknowledgementBatches
 *    (ACCEPT) for the 5 records.  The mock broker archives them and
 *    advances SPSO.
 * 4. Subsequent polls should return 0 records (no re-delivery).
 */
static int run_implicit_ack_no_redelivery(void) {
        const char *topic = "kip932_ack_no_redeliver";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-noredeliver");
        subscribe_topics(consumer, &topic, 1);

        /* Consume all records. */
        consumed = consume_n(consumer, msgcnt, 50);
        printf("  ack_no_redelivery: consumed %d/%d\n", consumed, msgcnt);

        /* Poll again to trigger the implicit ack (piggybacked on the
         * next ShareFetch).  Expect 0 new records. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_no_redelivery: extra %d/0 (should be 0)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt && extra == 0;
}

/**
 * @brief After implicit ack, only newly-produced records are delivered.
 *
 * 1. Produce batch A (3 messages).
 * 2. Consumer A consumes batch A and triggers implicit ack.
 * 3. Consumer A closes.
 * 4. Produce batch B (3 messages).
 * 5. Consumer B (same group) consumes — should receive only batch B
 *    because batch A was acked (ARCHIVED, SPSO advanced).
 */
static int run_implicit_ack_with_new_records(void) {
        const char *topic = "kip932_ack_new_records";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        /* Batch A */
        produce_messages(ctx.producer, topic, 3);

        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-newrecords");
        subscribe_topics(consumer, &topic, 1);

        consumed_a = consume_n(consumer, 3, 40);
        printf("  ack_new_records: A consumed %d/3\n", consumed_a);

        /* Trigger implicit ack for batch A. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_new_records: A extra %d/0 (ack sent)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Batch B */
        produce_messages(ctx.producer, topic, 3);

        /* Consumer B: same group — batch A is archived, should get
         * only batch B. */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-newrecords");
        subscribe_topics(consumer, &topic, 1);

        consumed_b = consume_n(consumer, 3, 40);
        printf("  ack_new_records: B consumed %d/3 (batch B only)\n",
               consumed_b);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == 3 && extra == 0 && consumed_b == 3;
}

/**
 * @brief Acked records are not visible to a different consumer that
 *        joins the same share group afterwards.
 *
 * 1. Consumer A consumes all records and triggers implicit ack.
 * 2. Consumer A closes.
 * 3. Consumer B joins the same group — should see 0 records because
 *    the records are ARCHIVED and SPSO has advanced.
 */
static int run_implicit_ack_cross_consumer(void) {
        const char *topic = "kip932_ack_cross_consumer";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer_a, *consumer_b;
        int consumed_a, consumed_b, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: consume and ack. */
        consumer_a =
            new_share_consumer(ctx.bootstraps, "sg-ack-cross");
        subscribe_topics(consumer_a, &topic, 1);

        consumed_a = consume_n(consumer_a, msgcnt, 50);
        printf("  ack_cross_consumer: A consumed %d/%d\n", consumed_a, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer_a, 1, 10);
        printf("  ack_cross_consumer: A extra %d/0\n", extra);

        rd_kafka_share_consumer_close(consumer_a);
        rd_kafka_share_destroy(consumer_a);

        /* Consumer B: same group, should see nothing. */
        consumer_b =
            new_share_consumer(ctx.bootstraps, "sg-ack-cross");
        subscribe_topics(consumer_b, &topic, 1);

        consumed_b = consume_n(consumer_b, 1, 15);
        printf("  ack_cross_consumer: B consumed %d/0 (should be 0)\n",
               consumed_b);

        rd_kafka_share_consumer_close(consumer_b);
        rd_kafka_share_destroy(consumer_b);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && extra == 0 && consumed_b == 0;
}

/**
 * @brief Implicit ack works across multiple partitions.
 *
 * 1. Create a 2-partition topic.
 * 2. Produce messages (distributed across partitions by the partitioner).
 * 3. Consume all messages.
 * 4. Trigger implicit ack.
 * 5. Poll again — should get 0 (acked from all partitions).
 */
static int run_implicit_ack_multi_partition(void) {
        const char *topic = "kip932_ack_multi_part";
        const int msgcnt  = 6;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-multipart");
        subscribe_topics(consumer, &topic, 1);

        consumed = consume_n(consumer, msgcnt, 60);
        printf("  ack_multi_partition: consumed %d/%d\n", consumed, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_multi_partition: extra %d/0\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt && extra == 0;
}

/**
 * @brief Multiple rounds of produce → consume → ack with new consumers.
 *
 * Each round:
 *   1. Produce N messages.
 *   2. New consumer joins the same share group.
 *   3. Consumer should get exactly the N new records (records from
 *      previous rounds were acked and SPSO advanced).
 *   4. Consumer triggers implicit ack and closes.
 *
 * This verifies that SPSO advancement from acks in earlier rounds
 * persists correctly across consumer lifetimes.
 */
static int run_implicit_ack_multiple_rounds(void) {
        const char *topic    = "kip932_ack_multi_round";
        const int per_round  = 2;
        const int rounds     = 3;
        test_ctx_t ctx       = test_ctx_new();
        int total_consumed = 0;
        int round_ok       = 1;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        for (int r = 0; r < rounds; r++) {
                rd_kafka_share_t *consumer;
                int got, extra;

                produce_messages(ctx.producer, topic, per_round);

                consumer = new_share_consumer(ctx.bootstraps,
                                              "sg-ack-multiround");
                subscribe_topics(consumer, &topic, 1);

                got = consume_n(consumer, per_round, 40);
                printf("  ack_multiple_rounds: round %d consumed %d/%d\n",
                       r + 1, got, per_round);
                total_consumed += got;
                if (got != per_round)
                        round_ok = 0;

                /* Trigger implicit ack. */
                extra = consume_n(consumer, 1, 5);
                if (extra != 0) {
                        printf("  ack_multiple_rounds: round %d extra %d "
                               "(expected 0)\n",
                               r + 1, extra);
                        round_ok = 0;
                }

                rd_kafka_share_consumer_close(consumer);
                rd_kafka_share_destroy(consumer);
        }

        printf("  ack_multiple_rounds: total %d/%d\n", total_consumed,
               per_round * rounds);

        test_ctx_destroy(&ctx);

        return round_ok && total_consumed == per_round * rounds;
}

/**
 * @brief Implicit ack with a single record (boundary case).
 *
 * Produce exactly 1 message, consume it, trigger implicit ack,
 * verify no re-delivery.  Tests the minimum-batch-size edge case for
 * AcquiredRecords ranges and SPSO advancement.
 */
static int run_implicit_ack_single_record(void) {
        const char *topic = "kip932_ack_single";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);

        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-single");
        subscribe_topics(consumer, &topic, 1);

        consumed = consume_n(consumer, 1, 40);
        printf("  ack_single_record: consumed %d/1\n", consumed);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_single_record: extra %d/0 (should be 0)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 1 && extra == 0;
}

/**
 * @brief Implicit ack with a large batch of records.
 *
 * Produce 100 messages, consume all, trigger implicit ack, verify 0
 * on subsequent polls.  Tests ack handling at scale — many records
 * in a single AcquiredRecords range and SPSO advancement over a
 * large offset span.
 */
static int run_implicit_ack_large_batch(void) {
        const char *topic  = "kip932_ack_large";
        const int msgcnt   = 100;
        test_ctx_t ctx     = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-large");
        subscribe_topics(consumer, &topic, 1);

        consumed = consume_n(consumer, msgcnt, 80);
        printf("  ack_large_batch: consumed %d/%d\n", consumed, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_large_batch: extra %d/0 (should be 0)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt && extra == 0;
}

/**
 * @brief Implicit ack across multiple topics in the same share group.
 *
 * 1. Create 2 topics, produce messages to both.
 * 2. Subscribe a single consumer to both topics via the same group.
 * 3. Consume all messages from both topics.
 * 4. Trigger implicit ack.
 * 5. Consumer B joins the same group — should see 0 records from
 *    either topic (both acked independently).
 */
static int run_implicit_ack_multi_topic(void) {
        const char *topic_a = "kip932_ack_mtopic_a";
        const char *topic_b = "kip932_ack_mtopic_b";
        const char *both[]  = {topic_a, topic_b};
        test_ctx_t ctx      = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra, consumed_b;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic A");
        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic B");

        produce_messages(ctx.producer, topic_a, 3);
        produce_messages(ctx.producer, topic_b, 3);

        /* Consumer A: subscribe to both, consume all 6, ack. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-mtopic");
        subscribe_topics(consumer, both, 2);

        consumed = consume_n(consumer, 6, 60);
        printf("  ack_multi_topic: consumed %d/6 from both\n", consumed);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_multi_topic: extra %d/0\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Consumer B: same group — should see 0 from either topic. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-mtopic");
        subscribe_topics(consumer, both, 2);

        consumed_b = consume_n(consumer, 1, 15);
        printf("  ack_multi_topic: B consumed %d/0 (should be 0)\n",
               consumed_b);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 6 && extra == 0 && consumed_b == 0;
}

/**
 * @brief Implicit ack with records from separate message sets.
 *
 * Produce records one at a time with a flush in between, creating
 * separate msgsets on the mock partition.  Then consume all, ack,
 * and verify 0 on subsequent polls.
 *
 * This validates that the ack machinery handles records spanning
 * multiple RecordBatch wire objects.
 */
static int run_implicit_ack_multi_msgset(void) {
        const char *topic = "kip932_ack_multi_msgset";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        /* Produce each message individually with a flush in between,
         * guaranteeing separate msgsets on the mock partition. */
        for (int i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "msgset-%d", i);
                if (rd_kafka_producev(
                        ctx.producer, RD_KAFKA_V_TOPIC(topic),
                        RD_KAFKA_V_VALUE(payload, strlen(payload)),
                        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                        RD_KAFKA_V_END) != RD_KAFKA_RESP_ERR_NO_ERROR)
                        die("Produce failed");
                rd_kafka_flush(ctx.producer, 5000);
        }

        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-multimsgset");
        subscribe_topics(consumer, &topic, 1);

        consumed = consume_n(consumer, msgcnt, 60);
        printf("  ack_multi_msgset: consumed %d/%d\n", consumed, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  ack_multi_msgset: extra %d/0 (should be 0)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt && extra == 0;
}

/* ===================================================================
 *  Negative test scenarios
 * =================================================================== */

/**
 * @brief Crash (no ack) → records re-delivered after lock expiry.
 *
 * Consumer A consumes records but is destroyed without closing and
 * without sending a subsequent ShareFetch that would carry the ack.
 * After the acquisition lock expires, Consumer B should receive the
 * same records.
 */
static int run_crash_before_ack_redelivery(void) {
        const char *topic = "kip932_ack_crash_redeliver";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b;

        /* Short lock so the test doesn't wait too long. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: acquire records, then crash (no close, no ack). */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-crash-redeliver");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  crash_before_ack: A consumed %d/%d\n", consumed_a, msgcnt);
        rd_kafka_share_destroy(consumer); /* crash — no close */

        /* Wait for lock expiry (session_timeout=500ms + margin). */
        usleep(1500 * 1000);

        /* Consumer B: should get the same records (re-delivered). */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-crash-redeliver");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  crash_before_ack: B consumed %d/%d (re-delivered)\n",
               consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt;
}

/**
 * @brief Crash → re-delivery → ack stops further re-delivery.
 *
 * 1. Consumer A consumes records but crashes (no ack).
 * 2. Locks expire, records become AVAILABLE again.
 * 3. Consumer B consumes the re-delivered records and triggers an
 *    implicit ack via the next poll.
 * 4. Consumer C joins — should see 0 records (acked by B).
 */
static int run_crash_then_ack_stops_redelivery(void) {
        const char *topic = "kip932_ack_crash_then_ack";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, consumed_c, extra;

        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: acquire and crash. */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-crash-then-ack");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  crash_then_ack: A consumed %d/%d (will crash)\n",
               consumed_a, msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Consumer B: re-delivery, then ack via next poll. */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-crash-then-ack");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  crash_then_ack: B consumed %d/%d (re-delivered)\n",
               consumed_b, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  crash_then_ack: B extra %d/0\n", extra);
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Consumer C: should see 0 — records were acked by B. */
        consumer =
            new_share_consumer(ctx.bootstraps, "sg-ack-crash-then-ack");
        subscribe_topics(consumer, &topic, 1);
        consumed_c = consume_n(consumer, 1, 15);
        printf("  crash_then_ack: C consumed %d/0 (should be 0)\n",
               consumed_c);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt &&
               extra == 0 && consumed_c == 0;
}

/**
 * @brief Session expiry via heartbeat failure causes pending ack to be lost.
 *
 * 1. Consumer A acquires records normally.
 * 2. Push many SGHB errors → heartbeats fail → member is evicted.
 * 3. Consumer A is destroyed (crash, no ack delivered).
 * 4. The broker releases A's locks upon eviction.
 * 5. Consumer B joins → gets the same records (re-delivered).
 *
 * This is distinct from crash_before_ack because here the session is
 * proactively killed via heartbeat failure, not passive lock timeout.
 */
static int run_session_expiry_invalidates_ack(void) {
        const char *topic = "kip932_ack_session_expire";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b;

        /* Short session timeout so eviction happens quickly. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: consume records (acquires them). */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-session-expire");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  session_expiry: A consumed %d/%d\n", consumed_a, msgcnt);

        /* Block SGHB so heartbeats fail → member evicted. */
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

        /* Wait for member eviction (session_timeout=500ms + margin). */
        usleep(1500 * 1000);

        /* Crash consumer A without acking. */
        rd_kafka_share_destroy(consumer);

        /* Consumer B: records should be re-delivered (locks released
         * when A's membership was evicted). */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-session-expire");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  session_expiry: B consumed %d/%d (re-delivered)\n",
               consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt;
}

/**
 * @brief Records archived by max_delivery_count without any ack.
 *
 * 1. Set max_delivery_attempts=2.
 * 2. Consumer A acquires records and crashes (delivery 1).
 * 3. Consumer B acquires same records and crashes (delivery 2 = max).
 * 4. Consumer C joins — should see 0 records because the broker
 *    archived them after the delivery count was exhausted.
 *
 * Validates that records can be force-archived by the broker even
 * without an explicit/implicit ack.
 */
static int run_max_delivery_without_ack(void) {
        const char *topic = "kip932_ack_maxdel_noack";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, consumed_c;

        rd_kafka_mock_sharegroup_set_max_delivery_attempts(ctx.mcluster, 2);
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Delivery 1: Consumer A acquires and crashes. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-maxdel-noack");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  max_delivery_no_ack: A consumed %d/%d (delivery 1)\n",
               consumed_a, msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Delivery 2 = max: Consumer B acquires and crashes. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-maxdel-noack");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  max_delivery_no_ack: B consumed %d/%d (delivery 2)\n",
               consumed_b, msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry + archival */

        /* Delivery 3 attempt: records should be archived (delivery
         * count exhausted). Consumer C sees 0. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-maxdel-noack");
        subscribe_topics(consumer, &topic, 1);
        consumed_c = consume_n(consumer, 1, 10);
        printf("  max_delivery_no_ack: C consumed %d/0 (archived)\n",
               consumed_c);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt &&
               consumed_c == 0;
}

/**
 * @brief ShareFetch error injection prevents ack from being processed.
 *
 * 1. Consumer A acquires records.
 * 2. Inject ShareFetch transport errors on all brokers so the next
 *    ShareFetch (carrying the ack) causes a disconnect.
 * 3. Consumer A is destroyed (crash — ack was never processed).
 * 4. Wait for locks to expire.
 * 5. Consumer B joins — should get the same records (re-delivered).
 *
 * Validates that server/network errors during ack delivery don't
 * silently archive records.
 */
static int run_sharefetch_error_drops_ack(void) {
        const char *topic = "kip932_ack_sf_error";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b;

        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A acquires records. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-sf-error");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  sf_error_drops_ack: A consumed %d/%d\n",
               consumed_a, msgcnt);

        /* Inject ShareFetch transport errors — the next ShareFetch
         * (which would carry acks) will cause a disconnect. */
        rd_kafka_mock_push_request_errors(
            ctx.mcluster, RD_KAFKAP_ShareFetch, 10,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT,
            RD_KAFKA_RESP_ERR__TRANSPORT);

        /* Crash consumer A (ack never delivered). */
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Clear any remaining errors. */
        rd_kafka_mock_clear_request_errors(ctx.mcluster,
                                           RD_KAFKAP_ShareFetch);

        /* Consumer B should get re-delivered records. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-sf-error");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  sf_error_drops_ack: B consumed %d/%d (re-delivered)\n",
               consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt;
}

/**
 * @brief Un-acked records from a forgotten topic remain available.
 *
 * 1. Subscribe to topic_a and topic_b, consume from both.
 * 2. Crash consumer immediately (no ack for either topic).
 * 3. Wait for lock expiry so records become AVAILABLE.
 * 4. Consumer B subscribes to topic_a only, consumes and acks topic_a.
 * 5. Consumer C subscribes to topic_b only — should see topic_b's
 *    records (they were never acked, only released by lock expiry).
 *
 * This validates that records whose topic was not acked remain
 * available for re-delivery even while records from other topics
 * in the same group are acked normally.
 */
static int run_forgotten_topic_releases_not_acks(void) {
        const char *topic_a = "kip932_ack_forget_a";
        const char *topic_b = "kip932_ack_forget_b";
        const char *both[]  = {topic_a, topic_b};
        test_ctx_t ctx      = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_both, consumed_a, consumed_b, extra;

        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic A");
        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic B");

        produce_messages(ctx.producer, topic_a, 2);
        produce_messages(ctx.producer, topic_b, 2);

        /* Consumer A: subscribe to both, consume all 4, then crash. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-forget");
        subscribe_topics(consumer, both, 2);

        consumed_both = consume_n(consumer, 4, 60);
        printf("  forgotten_releases: consumed %d/4 from both\n",
               consumed_both);

        /* Crash — no ack for either topic. */
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000); /* wait for lock expiry */

        /* Consumer B: subscribe to topic_a only, consume and ack. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-forget");
        subscribe_topics(consumer, &topic_a, 1);

        consumed_a = consume_n(consumer, 2, 40);
        printf("  forgotten_releases: B consumed %d/2 from topic_a\n",
               consumed_a);

        /* Trigger implicit ack for topic_a. */
        extra = consume_n(consumer, 1, 10);
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Consumer C: subscribe to topic_b only.
         * Topic_b's records were never acked — should be available. */
        consumer = new_share_consumer(ctx.bootstraps, "sg-ack-forget");
        subscribe_topics(consumer, &topic_b, 1);

        consumed_b = consume_n(consumer, 2, 40);
        printf("  forgotten_releases: C consumed %d/2 from topic_b "
               "(should be re-delivered)\n", consumed_b);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_both >= 4 && consumed_a == 2 && consumed_b == 2;
}

/**
 * @brief Multiple consumers crash sequentially without acking.
 *
 * A, B, C each acquire the same records and crash without acking.
 * Each re-delivery round hands the same records to the next consumer
 * after lock expiry.  This validates that without any ack, records
 * cycle through ACQUIRED→AVAILABLE indefinitely (bounded by
 * max_delivery_count, which we set high here).
 */
static int run_multi_consumer_cascade_crash(void) {
        const char *topic = "kip932_ack_cascade_crash";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, consumed_c;

        /* High max delivery so records don't get archived. */
        rd_kafka_mock_sharegroup_set_max_delivery_attempts(ctx.mcluster, 10);
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: acquire and crash. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-cascade-crash");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  cascade_crash: A consumed %d/%d\n", consumed_a, msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000);

        /* Consumer B: re-acquire and crash. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-cascade-crash");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  cascade_crash: B consumed %d/%d\n", consumed_b, msgcnt);
        rd_kafka_share_destroy(consumer);
        usleep(1500 * 1000);

        /* Consumer C: re-acquire and crash. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-cascade-crash");
        subscribe_topics(consumer, &topic, 1);
        consumed_c = consume_n(consumer, msgcnt, 50);
        printf("  cascade_crash: C consumed %d/%d\n", consumed_c, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt &&
               consumed_c == msgcnt;
}

/**
 * @brief Lock expires before the ack-carrying ShareFetch arrives.
 *
 * 1. Set a very short record lock duration (200ms).
 * 2. Consumer A acquires records.
 * 3. Inject high RTT on broker 1 so the next ShareFetch (carrying
 *    the ack) is delayed beyond the lock duration.
 * 4. Lock expires → records become AVAILABLE.
 * 5. Consumer A is destroyed (crash — ack was delayed and lock
 *    already expired, so ack for those records is a no-op or
 *    the records may have been re-acquired).
 * 6. Consumer B should be able to get the same records.
 */
static int run_lock_expiry_before_ack(void) {
        const char *topic = "kip932_ack_lock_expire";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b;

        /* Very short lock, long session timeout. */
        rd_kafka_mock_sharegroup_set_record_lock_duration(ctx.mcluster, 200);
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 10000);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A acquires records. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-lockexpire");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  lock_expiry_before_ack: A consumed %d/%d\n",
               consumed_a, msgcnt);

        /* Inject high RTT on all brokers so the next ShareFetch
         * (carrying the ack) is delayed well past lock expiry. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 3000);
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 2, 3000);
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 3, 3000);

        /* Wait for lock to expire (200ms + margin), then crash. */
        usleep(800 * 1000);
        rd_kafka_share_destroy(consumer);

        /* Clear RTT so consumer B can operate normally. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 0);
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 2, 0);
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 3, 0);

        /* Consumer B should get the records — locks expired before
         * A's ack could be delivered. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-lockexpire");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 50);
        printf("  lock_expiry_before_ack: B consumed %d/%d\n",
               consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt;
}

/**
 * @brief Empty topic produces no ack side effects.
 *
 * 1. Subscribe to an empty topic, poll several times (no records,
 *    no AcquiredRecords, no acks sent).
 * 2. Produce messages.
 * 3. Consumer should consume them normally.
 * 4. Ack them normally (implicit ack via next poll).
 * 5. Verify 0 after ack.
 *
 * Validates that empty ack batches don't corrupt share-group state.
 */
static int run_empty_topic_no_ack_side_effects(void) {
        const char *topic = "kip932_ack_empty_topic";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_empty, consumed, extra;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-empty-topic");
        subscribe_topics(consumer, &topic, 1);

        /* Phase 1: Poll empty topic — no records, no acks. */
        consumed_empty = consume_n(consumer, 1, 5);
        printf("  empty_topic: phase1 consumed %d/0 (should be 0)\n",
               consumed_empty);

        /* Phase 2: Produce messages, then close this consumer and
         * open a new one to avoid incremental session issues. */
        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-empty-topic");
        subscribe_topics(consumer, &topic, 1);

        consumed = consume_n(consumer, msgcnt, 50);
        printf("  empty_topic: phase2 consumed %d/%d\n", consumed, msgcnt);

        /* Phase 3: Trigger implicit ack, verify 0. */
        extra = consume_n(consumer, 1, 10);
        printf("  empty_topic: phase3 extra %d/0 (should be 0)\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_empty == 0 && consumed == msgcnt && extra == 0;
}

/**
 * @brief Coordinator failover — consumer recovers and acks new records.
 *
 * 1. Consumer A consumes and acks records (happy path).
 * 2. Push SGHB errors → member evicted.
 * 3. Clear errors → member re-joins.
 * 4. Produce new records.
 * 5. Consumer (new instance, same group) consumes and acks new records.
 * 6. Consumer C joins — should see 0 (everything acked).
 *
 * Validates graceful recovery from coordinator flap; stale state
 * doesn't corrupt the share group.
 */
static int run_coordinator_failover_ack_recovery(void) {
        const char *topic = "kip932_ack_coord_failover";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed_a, consumed_b, consumed_c, extra;

        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        /* Phase 1: Consumer A consumes and acks normally. */
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-coord-failover");
        subscribe_topics(consumer, &topic, 1);
        consumed_a = consume_n(consumer, msgcnt, 50);
        printf("  coord_failover: A consumed %d/%d\n", consumed_a, msgcnt);

        /* Trigger implicit ack. */
        extra = consume_n(consumer, 1, 10);
        printf("  coord_failover: A extra %d/0\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Phase 2: Temporarily push SGHB errors (simulating coordinator
         * failover). Any consumer joining now will fail heartbeats. */
        rd_kafka_mock_push_request_errors(
            ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 10,
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

        /* Wait for errors to start draining, then produce new records. */
        usleep(1000 * 1000);
        produce_messages(ctx.producer, topic, msgcnt);

        /* Phase 3: Errors drain, new consumer B joins and consumes
         * the newly produced records (A's records are already acked). */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-coord-failover");
        subscribe_topics(consumer, &topic, 1);
        consumed_b = consume_n(consumer, msgcnt, 60);
        printf("  coord_failover: B consumed %d/%d\n", consumed_b, msgcnt);

        /* Trigger implicit ack for B's records. */
        extra = consume_n(consumer, 1, 10);
        printf("  coord_failover: B extra %d/0\n", extra);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);

        /* Phase 4: Consumer C — everything is acked, should see 0. */
        consumer = new_share_consumer(ctx.bootstraps,
                                      "sg-ack-coord-failover");
        subscribe_topics(consumer, &topic, 1);
        consumed_c = consume_n(consumer, 1, 15);
        printf("  coord_failover: C consumed %d/0 (should be 0)\n",
               consumed_c);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt &&
               consumed_c == 0;
}

/* ===================================================================
 *  Test runner
 * =================================================================== */

static int run_test(const char *name, int (*fn)(void)) {
        int ok = fn();
        printf("[%s] %s\n", ok ? "OK" : "FAIL", name);
        printf("=========================================\n\n");
        return ok ? 0 : 1;
}

int main(void) {
        int failures = 0;

        printf("Share Acknowledgement (implicit ack) test scenarios\n");
        printf("=========================================\n\n");

        /* Positive scenarios */
        failures += run_test("implicit_ack_no_redelivery",
                             run_implicit_ack_no_redelivery);
        failures += run_test("implicit_ack_with_new_records",
                             run_implicit_ack_with_new_records);
        failures += run_test("implicit_ack_cross_consumer",
                             run_implicit_ack_cross_consumer);
        failures += run_test("implicit_ack_multi_partition",
                             run_implicit_ack_multi_partition);
        failures += run_test("implicit_ack_multiple_rounds",
                             run_implicit_ack_multiple_rounds);
        failures += run_test("implicit_ack_single_record",
                             run_implicit_ack_single_record);
        failures += run_test("implicit_ack_large_batch",
                             run_implicit_ack_large_batch);
        failures += run_test("implicit_ack_multi_topic",
                             run_implicit_ack_multi_topic);
        failures += run_test("implicit_ack_multi_msgset",
                             run_implicit_ack_multi_msgset);

        /* Negative scenarios */
        failures += run_test("crash_before_ack_redelivery",
                             run_crash_before_ack_redelivery);
        failures += run_test("crash_then_ack_stops_redelivery",
                             run_crash_then_ack_stops_redelivery);
        failures += run_test("session_expiry_invalidates_ack",
                             run_session_expiry_invalidates_ack);
        failures += run_test("max_delivery_without_ack",
                             run_max_delivery_without_ack);
        failures += run_test("sharefetch_error_drops_ack",
                             run_sharefetch_error_drops_ack);
        failures += run_test("forgotten_topic_releases_not_acks",
                             run_forgotten_topic_releases_not_acks);
        failures += run_test("multi_consumer_cascade_crash",
                             run_multi_consumer_cascade_crash);
        failures += run_test("lock_expiry_before_ack",
                             run_lock_expiry_before_ack);
        failures += run_test("empty_topic_no_ack_side_effects",
                             run_empty_topic_no_ack_side_effects);
        failures += run_test("coordinator_failover_ack_recovery",
                             run_coordinator_failover_ack_recovery);

        printf("Failures: %d\n", failures);
        return failures ? 1 : 0;
}

