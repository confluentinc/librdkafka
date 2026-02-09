/*
 * KIP-932 ShareFetch mock broker demo using the librdkafka share consumer API.
 *
 * This test exercises the ShareFetch path only. There is no coordinator
 * or ShareAcknowledge support in the mock broker, so group management and
 * ack-based state transitions are not validated here.
 *
 * Build (from repo root):
 *   cc -Isrc -o /tmp/kip932_share_consumer_mock \
 *      tests/012x-kip932-sharefetch_mockbroker.c src/librdkafka.a -lz -lpthread
 *
 * Run:
 *   /tmp/kip932_share_consumer_mock
 */

#include "rdkafka.h"
#include "rdkafka_mock.h"
#include "rdkafka_protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

        if (rd_kafka_mock_set_apiversion(
                ctx.mcluster, RD_KAFKAP_ShareGroupHeartbeat, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to enable ShareGroupHeartbeat");
        if (rd_kafka_mock_set_apiversion(
                ctx.mcluster, RD_KAFKAP_ShareFetch, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
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
                rd_kafka_topic_partition_list_add(
                    tpl, topics[i], RD_KAFKA_PARTITION_UA);
        }
        if (rd_kafka_share_subscribe(consumer, tpl))
                die("Subscribe failed");
        rd_kafka_topic_partition_list_destroy(tpl);
}

static int consume_n(rd_kafka_share_t *consumer, int expected, int max_attempts) {
        int consumed = 0;
        int attempts = 0;

        while (consumed < expected && attempts < max_attempts) {
                rd_kafka_message_t *rkmessages[100];
                size_t rcvd_msgs = 0;
                rd_kafka_error_t *error;

                error = rd_kafka_share_consume_batch(
                    consumer, 500, rkmessages, &rcvd_msgs);
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
                        printf("Consumed: %.*s\n", (int)rkmsg->len,
                               (const char *)rkmsg->payload);
                        consumed++;
                        rd_kafka_message_destroy(rkmsg);
                }
        }

        return consumed;
}

static int run_basic_consume(void) {
        const char *topic = "kip932_pos_basic";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-basic");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 50);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt;
}

static int run_followup_fetch(void) {
        const char *topic = "kip932_pos_followup";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 5);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-followup");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 3, 30);
        consumed += consume_n(consumer, 2, 30);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 5;
}

static int run_multi_partition(void) {
        const char *topic = "kip932_pos_multi_part";
        const int msgcnt  = 6;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 2, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-multipart");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 60);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt;
}

static int run_multi_topic(void) {
        const char *topic_a = "kip932_pos_topic_a";
        const char *topic_b = "kip932_pos_topic_b";
        const char *topics[] = {topic_a, topic_b};
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic A");
        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic B");

        produce_messages(ctx.producer, topic_a, 2);
        produce_messages(ctx.producer, topic_b, 2);
        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-multitopic");
        subscribe_topics(consumer, topics, 2);
        consumed = consume_n(consumer, 4, 40);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 4;
}

static int run_empty_topic_no_records(void) {
        const char *topic = "kip932_pos_empty";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        consumer = new_share_consumer(ctx.bootstraps, "sg-pos-empty");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_negative_sharefetch_error(rd_kafka_resp_err_t err) {
        const char *topic = "kip932_neg_sharefetch_error";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);

        rd_kafka_mock_push_request_errors(ctx.mcluster, RD_KAFKAP_ShareFetch,
                                          1, err);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-sharefetch");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_sharefetch_invalid_session_epoch(void) {
        return run_negative_sharefetch_error(
            RD_KAFKA_RESP_ERR_INVALID_FETCH_SESSION_EPOCH);
}

static int run_sharefetch_unknown_topic_or_part(void) {
        return run_negative_sharefetch_error(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
}

static int run_sghb_error(rd_kafka_resp_err_t err, int count) {
        const char *topic = "kip932_neg_sghb";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);

        rd_kafka_mock_push_request_errors(ctx.mcluster,
                                          RD_KAFKAP_ShareGroupHeartbeat, count,
                                          err);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-sghb");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_sghb_coord_unavailable(void) {
        return run_sghb_error(
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE, 50);
}

static int run_topic_error(rd_kafka_resp_err_t err) {
        const char *topic = "kip932_neg_topic_error";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 1);
        rd_kafka_mock_topic_set_error(ctx.mcluster, topic, err);

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-topicerr");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_topic_error_unknown_topic_or_part(void) {
        return run_topic_error(
            RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
}

static int run_unknown_topic_subscription(void) {
        const char *topic = "kip932_neg_unknown_topic";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-unknown-topic");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_empty_fetch_no_records(void) {
        const char *topic = "kip932_neg_empty_fetch";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        consumer = new_share_consumer(ctx.bootstraps, "sg-neg-empty");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, 1, 5);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 0;
}

static int run_sharefetch_session_expiry_rtt(void) {
        const char *topic = "kip932_rtt_expiry";
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        /* Session timeout must be long enough for normal requests
         * to complete, but short enough to expire during high RTT. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 1000);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, 2);

        consumer = new_share_consumer(ctx.bootstraps, "sg-rtt-expiry");
        subscribe_topics(consumer, &topic, 1);

        /* Phase 1: consume one message with normal RTT (no injection). */
        consumed = consume_n(consumer, 1, 20);
        printf("  rtt_expiry: phase1 consumed %d/1\n", consumed);

        /* Phase 2: inject RTT >> session timeout to force session expiry.
         * All requests to broker 1 now take 3s, but the session
         * expires after 1s of inactivity. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 3000);
        usleep(2000 * 1000); /* wait for session to expire */

        /* Phase 3: clear RTT and let the consumer recover.
         * It should re-create the session (epoch=0) and consume
         * the remaining message. */
        rd_kafka_mock_broker_set_rtt(ctx.mcluster, 1, 0);
        consumed += consume_n(consumer, 1, 30);
        printf("  rtt_expiry: phase3 consumed %d/2 total\n", consumed);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == 2;
}

static int run_forgotten_topics(void) {
        const char *topic_a  = "kip932_forgotten_a";
        const char *topic_b  = "kip932_forgotten_b";
        const char *both[]   = {topic_a, topic_b};
        test_ctx_t ctx       = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_a, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic A");
        if (rd_kafka_mock_topic_create(ctx.mcluster, topic_b, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic B");

        /* Produce 2 messages to each topic */
        produce_messages(ctx.producer, topic_a, 2);
        produce_messages(ctx.producer, topic_b, 2);

        /* Subscribe to both topics and consume all 4 messages */
        consumer = new_share_consumer(ctx.bootstraps, "sg-forgotten");
        subscribe_topics(consumer, both, 2);
        consumed = consume_n(consumer, 4, 40);
        printf("  forgotten_topics: consumed %d/4 from both topics\n",
               consumed);

        /* Re-subscribe to only topic_a (topic_b becomes forgotten) */
        subscribe_topics(consumer, &topic_a, 1);

        /* Produce 2 more messages to topic_a */
        produce_messages(ctx.producer, topic_a, 2);

        /* Consume the 2 new messages — only topic_a should deliver */
        consumed += consume_n(consumer, 2, 30);
        printf("  forgotten_topics: consumed %d/6 total after forget\n",
               consumed);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        /* We expect at least the 4 initial + 2 from topic_a = 6.
         * Depending on timing the consumer may or may not have already
         * received all messages from the first round, so we accept >= 4. */
        return consumed >= 4;
}

/**
 * @brief Multi-consumer lock expiry test.
 *
 * Consumer A acquires records, then crashes (destroyed without close).
 * After the lock expiry timeout, consumer B should be able to pick up
 * the same records because the proactive lock-expiry scan releases them.
 */
/**
 * @brief Produce messages one-at-a-time (each flush creates a separate
 *        msgset on the mock partition), then consume and verify all are
 *        received. This validates that the ShareFetch response includes
 *        records from *all* acquired msgsets, not just the first one.
 */
static int run_multi_batch_consume(void) {
        const char *topic = "kip932_multi_batch";
        const int msgcnt  = 5;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer;
        int consumed;

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");

        /* Produce each message individually with a flush in between,
         * guaranteeing separate msgsets on the mock partition. */
        for (int i = 0; i < msgcnt; i++) {
                char payload[64];
                snprintf(payload, sizeof(payload), "batch-%d", i);
                if (rd_kafka_producev(
                        ctx.producer, RD_KAFKA_V_TOPIC(topic),
                        RD_KAFKA_V_VALUE(payload, strlen(payload)),
                        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                        RD_KAFKA_V_END) != RD_KAFKA_RESP_ERR_NO_ERROR)
                        die("Produce failed");
                rd_kafka_flush(ctx.producer, 5000);
        }

        consumer = new_share_consumer(ctx.bootstraps, "sg-multi-batch");
        subscribe_topics(consumer, &topic, 1);
        consumed = consume_n(consumer, msgcnt, 50);
        printf("  multi_batch: consumed %d/%d\n", consumed, msgcnt);

        rd_kafka_share_consumer_close(consumer);
        rd_kafka_share_destroy(consumer);
        test_ctx_destroy(&ctx);

        return consumed == msgcnt;
}

static int run_multi_consumer_lock_expiry(void) {
        const char *topic = "kip932_multi_consumer_lock";
        const int msgcnt  = 3;
        test_ctx_t ctx    = test_ctx_new();
        rd_kafka_share_t *consumer_a, *consumer_b;
        int consumed_a, consumed_b;

        /* Use a short session/lock timeout so the test runs quickly. */
        rd_kafka_mock_sharegroup_set_session_timeout(ctx.mcluster, 500);

        if (rd_kafka_mock_topic_create(ctx.mcluster, topic, 1, 1) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                die("Failed to create mock topic");
        produce_messages(ctx.producer, topic, msgcnt);

        /* Consumer A: subscribe and consume all records (acquires locks). */
        consumer_a =
            new_share_consumer(ctx.bootstraps, "sg-multi-consumer-lock");
        subscribe_topics(consumer_a, &topic, 1);
        consumed_a = consume_n(consumer_a, msgcnt, 50);
        printf("  multi_consumer: A consumed %d/%d\n", consumed_a, msgcnt);

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
        printf("  multi_consumer: B consumed %d/%d\n", consumed_b, msgcnt);

        rd_kafka_share_consumer_close(consumer_b);
        rd_kafka_share_destroy(consumer_b);
        test_ctx_destroy(&ctx);

        return consumed_a == msgcnt && consumed_b == msgcnt;
}

static int run_test(const char *name, int (*fn)(void)) {
        int ok = fn();
        printf("[%s] %s\n", ok ? "OK" : "FAIL", name);
        return ok ? 0 : 1;
}

int main(void) {
        int failures = 0;

        printf("ShareFetch test scenarios\n");

        /* Positive scenarios */
        failures += run_test("basic_consume", run_basic_consume);
        failures += run_test("followup_fetch", run_followup_fetch);
        failures += run_test("multi_partition", run_multi_partition);
        failures += run_test("multi_topic", run_multi_topic);
        failures += run_test("empty_topic_no_records", run_empty_topic_no_records);
        failures += run_test("sharefetch_session_expiry_rtt",
                             run_sharefetch_session_expiry_rtt);
        failures += run_test("forgotten_topics", run_forgotten_topics);
        failures += run_test("multi_batch_consume", run_multi_batch_consume);
        failures += run_test("multi_consumer_lock_expiry",
                             run_multi_consumer_lock_expiry);

        /* Negative scenarios */
        failures += run_test("sharefetch_invalid_session_epoch",
                             run_sharefetch_invalid_session_epoch);
        failures += run_test("sharefetch_unknown_topic_or_part",
                             run_sharefetch_unknown_topic_or_part);
        failures += run_test("sghb_coord_unavailable",
                             run_sghb_coord_unavailable);
        failures += run_test("topic_error_unknown_topic_or_part",
                             run_topic_error_unknown_topic_or_part);
        failures += run_test("unknown_topic_subscription",
                             run_unknown_topic_subscription);
        failures += run_test("empty_fetch_no_records",
                             run_empty_fetch_no_records);

        printf("Failures: %d\n", failures);
        return failures ? 1 : 0;
}

