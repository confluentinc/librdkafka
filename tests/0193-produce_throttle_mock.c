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

#include <stdarg.h>

#include "test.h"
#include "../src/rdkafka_proto.h"

/**
 * @name KIP-219: a producer does not send ProduceRequests to a broker while
 *       the broker throttles the connection, and the other requests sent
 *       to it do not time out behind the mute.
 *
 * The implementation is partial: for a producer only the ProduceRequests are
 * held back, the other request types are still sent to the connection that
 * the broker has muted. Their timeouts are extended while the broker throttles,
 * instead of the requests being held back as well.
 *
 * A mock broker answers a throttled request right away and then stops
 * reading from the connection for the throttle time, as a Kafka broker does,
 * see rd_kafka_mock_broker_push_request_throttle().
 *
 * The mock broker mutes the connection itself, so the time at which a request
 * arrives at it can't tell whether the client held it back or sent it right
 * away. The tests look at what the client does instead: the time at which it
 * logs that a ProduceRequest was sent, and the delivery reports.
 */

#define EVENTS_MAX 64

/** Margin for the clocks of the log callback and of the test. */
#define SLACK_US (150 * 1000)

/**
 * The log callback is called by the thread that logs, the broker threads and
 * the main thread, so events is only accessed with events_lock held.
 */
static mtx_t events_lock;

/** What the client logged */
static struct {
        int64_t sent_ts[EVENTS_MAX]; /**< When each ProduceRequest was sent */
        int sent_ver[EVENTS_MAX];    /**< and its version */
        int sent_broker[EVENTS_MAX]; /**< and the id of the broker */
        int sent_cnt;
        int reqtmout_cnt; /**< Requests that timed out */
        int bump_cnt;     /**< Epoch bumps of the idempotent producer */
} events;

/** Result of a produced message */
struct msgres {
        volatile int done;
        rd_kafka_resp_err_t err;
        int64_t offset;
        int64_t produced_ts; /**< When it was produced */
        int64_t dr_ts;       /**< When its delivery report was served */
};


static void events_reset(void) {
        mtx_lock(&events_lock);
        memset(&events, 0, sizeof(events));
        mtx_unlock(&events_lock);
}

static void events_log_cb(const rd_kafka_t *rk,
                          int level,
                          const char *fac,
                          const char *buf) {
        const char *sent = "Sent ProduceRequest (v";
        const char *p;

        mtx_lock(&events_lock);
        if (!strcmp(fac, "REQTMOUT")) {
                events.reqtmout_cnt++;
        } else if (!strcmp(fac, "DRAIN") && strstr(buf, "bumped epoch")) {
                events.bump_cnt++;
        } else if ((p = strstr(buf, sent)) && events.sent_cnt < EVENTS_MAX) {
                const char *slash = NULL, *q;

                /* The broker name, "host:port/id: ", precedes the message */
                for (q = buf; q < p; q++)
                        if (*q == '/')
                                slash = q;
                events.sent_broker[events.sent_cnt] =
                    slash ? atoi(slash + 1) : -1;
                events.sent_ver[events.sent_cnt] = atoi(p + strlen(sent));
                events.sent_ts[events.sent_cnt]  = test_clock();
                events.sent_cnt++;
        }
        mtx_unlock(&events_lock);
}

/**
 * @returns the number of ProduceRequests sent so far.
 */
static int events_sent_cnt(void) {
        int cnt;
        mtx_lock(&events_lock);
        cnt = events.sent_cnt;
        mtx_unlock(&events_lock);
        return cnt;
}

/**
 * @returns the time between the sending of ProduceRequests \p a and \p b.
 */
static int64_t events_sent_gap(int a, int b) {
        int64_t gap;
        mtx_lock(&events_lock);
        TEST_ASSERT(a < events.sent_cnt && b < events.sent_cnt,
                    "ProduceRequest %d or %d was not sent, %d were", a, b,
                    events.sent_cnt);
        gap = events.sent_ts[b] - events.sent_ts[a];
        mtx_unlock(&events_lock);
        return gap;
}

/**
 * @returns the time at which the \p nth (from 0) ProduceRequest was sent to
 *          \p broker.
 */
static int64_t events_sent_ts(int broker, int nth) {
        int64_t ts = -1;
        int i, n = 0;

        mtx_lock(&events_lock);
        for (i = 0; i < events.sent_cnt; i++) {
                if (events.sent_broker[i] == broker && n++ == nth) {
                        ts = events.sent_ts[i];
                        break;
                }
        }
        mtx_unlock(&events_lock);

        TEST_ASSERT(ts != -1, "ProduceRequest %d to broker %d was not sent",
                    nth, broker);
        return ts;
}

/**
 * @returns the time at which, and the version with which, the \p idx'th
 *          (from 0) ProduceRequest was sent, whatever the broker.
 */
static void events_sent_get(int idx, int64_t *tsp, int *verp) {
        mtx_lock(&events_lock);
        TEST_ASSERT(idx < events.sent_cnt,
                    "ProduceRequest %d was not sent, %d were", idx,
                    events.sent_cnt);
        *tsp  = events.sent_ts[idx];
        *verp = events.sent_ver[idx];
        mtx_unlock(&events_lock);
}

static int events_bump_cnt(void) {
        int cnt;
        mtx_lock(&events_lock);
        cnt = events.bump_cnt;
        mtx_unlock(&events_lock);
        return cnt;
}

static int events_reqtmout_cnt(void) {
        int cnt;
        mtx_lock(&events_lock);
        cnt = events.reqtmout_cnt;
        mtx_unlock(&events_lock);
        return cnt;
}


static void
dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *o) {
        struct msgres *res = rkmessage->_private;

        res->err    = rkmessage->err;
        res->offset = rkmessage->offset;
        res->dr_ts  = test_clock();
        res->done   = 1;
}

/**
 * @brief Create a producer with the \p ... configuration, a NULL terminated
 *        list of property names and values.
 *
 * It logs the protocol requests and reports the delivery of the messages
 * produced with produce1().
 * Destroy it with producer_destroy().
 */
static rd_kafka_t *producer_new(const char *bootstraps,
                                test_conf_log_interceptor_t **interceptorp,
                                ...) {
        const char *debug_contexts[] = {"protocol", "eos", NULL};
        rd_kafka_conf_t *conf;
        va_list ap;
        const char *name;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "linger.ms", "0");
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

        va_start(ap, interceptorp);
        while ((name = va_arg(ap, const char *)))
                test_conf_set(conf, name, va_arg(ap, const char *));
        va_end(ap);

        *interceptorp =
            test_conf_set_log_interceptor(conf, events_log_cb, debug_contexts);

        return test_create_handle(RD_KAFKA_PRODUCER, conf);
}

static void producer_destroy(rd_kafka_t *rk,
                             test_conf_log_interceptor_t *interceptor) {
        rd_kafka_destroy(rk);
        rd_free(interceptor);
}

/**
 * @brief Produce a message, its result is set in \p res.
 */
static void produce1(rd_kafka_t *rk,
                     const char *topic,
                     int32_t partition,
                     struct msgres *res) {
        rd_kafka_resp_err_t err;

        memset(res, 0, sizeof(*res));
        res->produced_ts = test_clock();
        err              = rd_kafka_producev(
            rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(partition),
            RD_KAFKA_V_VALUE("msg", 3), RD_KAFKA_V_OPAQUE(res), RD_KAFKA_V_END);
        TEST_ASSERT(!err, "rd_kafka_producev() failed: %s",
                    rd_kafka_err2str(err));
}

/**
 * @brief Wait for the delivery report of \p res.
 */
static void wait_dr(rd_kafka_t *rk, struct msgres *res, int timeout_ms) {
        int64_t until = test_clock() + (int64_t)timeout_ms * 1000;

        while (!res->done && test_clock() < until)
                rd_kafka_poll(rk, 50);

        TEST_ASSERT(res->done, "No delivery report within %dms", timeout_ms);
}

/**
 * @brief Produce a message and wait for it to be delivered.
 */
static void produce_ok(rd_kafka_t *rk,
                       const char *topic,
                       int32_t partition,
                       struct msgres *res) {
        produce1(rk, topic, partition, res);
        wait_dr(rk, res, 30 * 1000);
        TEST_ASSERT(!res->err, "Message not delivered: %s",
                    rd_kafka_err2str(res->err));
}

static void sleep_until(int64_t ts) {
        int64_t remains = ts - test_clock();
        if (remains > 0)
                rd_usleep(remains, NULL);
}

/**
 * @brief Assert that \p gap_us is at least \p min_ms, with a margin for
 *        the clocks.
 *
 * @param what The gap that is measured, as a noun phrase, such as
 *             "The gap between A and B". It is followed by "was Nms,
 *             expected at least Mms" when the assertion fails.
 */
#define ASSERT_GAP_AT_LEAST(gap_us, min_ms, what)                              \
        TEST_ASSERT((gap_us) + SLACK_US >= (int64_t)(min_ms) * 1000,           \
                    "%s was %" PRId64 "ms, expected at least %dms", what,      \
                    (int64_t)(gap_us) / 1000, (int)(min_ms))


/**
 *  @brief The next ProduceRequests are not sent until the throttle has passed,
 *        and the messages produced meanwhile are sent together.
 */
static void do_test_held_back(void) {
        const int throttle_ms = 2000;
        const char *topic     = test_mk_topic_name("0193_held_back", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, batch[10];
        int i;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        /* The response to the first ProduceRequest throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, NULL);

        produce_ok(rk, topic, 0, &first);

        for (i = 0; i < 10; i++)
                produce1(rk, topic, 0, &batch[i]);
        for (i = 0; i < 10; i++) {
                wait_dr(rk, &batch[i], 30 * 1000);
                TEST_ASSERT(!batch[i].err, "Message %d not delivered: %s", i,
                            rd_kafka_err2str(batch[i].err));
        }

        TEST_ASSERT(events_sent_cnt() == 2,
                    "Expected the first message and then a batch with the "
                    "others, that is 2 ProduceRequests, got %d",
                    events_sent_cnt());
        ASSERT_GAP_AT_LEAST(
            events_sent_gap(0, 1), throttle_ms,
            "The gap between the first and the second ProduceRequest");

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The throttle time is taken from the response of any request type,
 *        as the broker mutes the whole connection.
 */
static void do_test_held_back_by_other_request(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_other_request", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        const struct rd_kafka_metadata *md;
        struct msgres warmup, res;
        int64_t ts, sent_ts;
        int sent_ver;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        rk = producer_new(bootstraps, &interceptor, NULL);

        /* Connect, and let the client be idle. */
        produce_ok(rk, topic, 0, &warmup);

        /* The response to the next Metadata request throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Metadata, 1, throttle_ms));
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 0, NULL, &md, 5000));
        rd_kafka_metadata_destroy(md);
        ts = test_clock();

        produce_ok(rk, topic, 0, &res);

        TEST_ASSERT(events_sent_cnt() == 2,
                    "Expected 2 ProduceRequests, got %d", events_sent_cnt());
        events_sent_get(1, &sent_ts, &sent_ver);
        ASSERT_GAP_AT_LEAST(
            sent_ts - ts, throttle_ms,
            "The gap between the throttled Metadata response and the "
            "ProduceRequest");

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Only the throttled broker is not sent ProduceRequests.
 */
static void do_test_per_broker(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_per_broker", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres warmup[2], throttled, held, other;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(2, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 2, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2));

        rk = producer_new(bootstraps, &interceptor, NULL);

        /* Connect to both brokers */
        produce_ok(rk, topic, 0, &warmup[0]);
        produce_ok(rk, topic, 1, &warmup[1]);

        /* The first broker throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));
        produce_ok(rk, topic, 0, &throttled);

        produce1(rk, topic, 0, &held);
        produce1(rk, topic, 1, &other);

        wait_dr(rk, &other, 30 * 1000);
        TEST_ASSERT(!other.err, "Message not delivered: %s",
                    rd_kafka_err2str(other.err));
        TEST_ASSERT(other.dr_ts - throttled.dr_ts < 1000 * 1000,
                    "The message for the broker that is not throttled took "
                    "%" PRId64 "ms to be delivered",
                    (other.dr_ts - throttled.dr_ts) / 1000);

        wait_dr(rk, &held, 30 * 1000);
        TEST_ASSERT(!held.err, "Message not delivered: %s",
                    rd_kafka_err2str(held.err));

        /* The first broker was sent the ProduceRequests of the first message
         * to connect, of the one that was throttled, and of the one that was
         * held back. The second one those of the first message to connect,
         * and of the one that was not held back. */
        ASSERT_GAP_AT_LEAST(events_sent_ts(1, 2) - events_sent_ts(1, 1),
                            throttle_ms,
                            "The gap between the throttled and the held back "
                            "ProduceRequest");
        TEST_ASSERT(events_sent_ts(2, 1) - other.produced_ts < 1000 * 1000,
                    "The ProduceRequest to the broker that is not throttled "
                    "was sent %" PRId64 "ms after the message was produced",
                    (events_sent_ts(2, 1) - other.produced_ts) / 1000);

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A throttle time is only honored for the versions of the request
 *        that have the KIP-219 semantics.
 */
static void do_test_version_not_honored(void) {
        const int throttle_ms = 2000;
        const char *topic     = test_mk_topic_name("0193_version", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, second;
        int64_t sent_ts;
        int sent_ver[2];

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        /* Produce v6 is the first version with KIP-219. */
        TEST_CALL_ERR__(
            rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_Produce, 0, 5));
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, NULL);

        produce_ok(rk, topic, 0, &first);
        /* The mock broker has muted the connection, it is delivered when that
         * is over. */
        produce_ok(rk, topic, 0, &second);

        TEST_ASSERT(events_sent_cnt() == 2,
                    "Expected 2 ProduceRequests, got %d", events_sent_cnt());
        events_sent_get(0, &sent_ts, &sent_ver[0]);
        events_sent_get(1, &sent_ts, &sent_ver[1]);
        TEST_ASSERT(sent_ver[0] == 5 && sent_ver[1] == 5,
                    "Expected Produce v5, got v%d and v%d", sent_ver[0],
                    sent_ver[1]);
        TEST_ASSERT(events_sent_gap(0, 1) < 500 * 1000,
                    "The second ProduceRequest was sent %" PRId64
                    "ms after the first, it must not be held back",
                    events_sent_gap(0, 1) / 1000);

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The messages that are held back still time out.
 */
static void do_test_held_back_message_timeout(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_msg_timeout", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, held, after;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, "message.timeout.ms",
                          "1000", NULL);

        produce_ok(rk, topic, 0, &first);

        produce1(rk, topic, 0, &held);
        wait_dr(rk, &held, 30 * 1000);
        TEST_ASSERT(held.err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
                    "Expected the message to time out, got %s",
                    rd_kafka_err2str(held.err));
        TEST_ASSERT(held.dr_ts - held.produced_ts < 2500 * 1000,
                    "The message timed out after %" PRId64
                    "ms, before the throttle ends",
                    (held.dr_ts - held.produced_ts) / 1000);
        TEST_ASSERT(events_sent_cnt() == 1,
                    "The message that timed out must not have been sent, "
                    "%d ProduceRequests were",
                    events_sent_cnt());

        /* The next one is sent when the throttle has passed. */
        sleep_until(first.dr_ts + (int64_t)throttle_ms * 1000 + 300 * 1000);
        produce_ok(rk, topic, 0, &after);
        TEST_ASSERT(events_sent_cnt() == 2,
                    "Expected 2 ProduceRequests, got %d", events_sent_cnt());

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The client can be destroyed while messages are held back. Like any
 *        message that is still queued, they are dropped: they are not sent and
 *        they don't get a delivery report.
 */
static void do_test_destroy_while_held_back(void) {
        const int throttle_ms = 10000;
        const char *topic     = test_mk_topic_name("0193_destroy", 1);
        struct msgres first, held;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int64_t ts;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, NULL);

        produce_ok(rk, topic, 0, &first);
        produce1(rk, topic, 0, &held);
        /* Let the broker thread see it, and hold it back. */
        rd_usleep(500 * 1000, NULL);

        ts = test_clock();
        rd_kafka_destroy(rk);
        TEST_ASSERT(test_clock() - ts < 3000 * 1000,
                    "rd_kafka_destroy() took %" PRId64
                    "ms with a throttle of %dms",
                    (test_clock() - ts) / 1000, throttle_ms);
        TEST_ASSERT(events_sent_cnt() == 1,
                    "Expected only the first message to be sent, that is 1 "
                    "ProduceRequest, got %d",
                    events_sent_cnt());
        TEST_ASSERT(!held.done,
                    "The held back message got a delivery report: %s",
                    rd_kafka_err2str(held.err));
        rd_free(interceptor);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The requests that the broker has not read yet when it throttles the
 *        connection do not time out while it is muted.
 *
 * rd_kafka_query_watermark_offsets() sends two ListOffsetsRequests, one
 * right after the other, and nothing else sends them.
 */
static void do_test_pipelined_requests_timeout(void) {
        const int throttle_ms = 4000;
        const char *topic     = test_mk_topic_name("0193_pipelined", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int64_t low, high, ts;
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        /* The response to the first ListOffsetsRequest throttles the client,
         * the second one is not read by the broker until it has passed, which
         * is much longer than socket.timeout.ms. */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_ListOffsets, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, "socket.timeout.ms", "1000",
                          NULL);

        ts  = test_clock();
        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &low, &high,
                                               30 * 1000);
        TEST_SAY("Query took %" PRId64 "ms: %s\n", (test_clock() - ts) / 1000,
                 rd_kafka_err2str(err));
        TEST_ASSERT(!err, "Query failed: %s", rd_kafka_err2str(err));
        ASSERT_GAP_AT_LEAST(test_clock() - ts, throttle_ms,
                            "The duration of the query");
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A request that is sent to a connection that is muted by the broker
 *        does not time out.
 *
 * Metadata requests are not held back by the client, only Produce and
 * Fetch are.
 */
static void do_test_request_sent_while_muted(void) {
        const int throttle_ms = 4000;
        const char *topic     = test_mk_topic_name("0193_muted", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        const struct rd_kafka_metadata *md;
        struct msgres warmup;
        rd_kafka_resp_err_t err;
        int64_t ts;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        rk = producer_new(bootstraps, &interceptor, "socket.timeout.ms", "1000",
                          NULL);

        /* Connect, and let the client be idle. */
        produce_ok(rk, topic, 0, &warmup);

        /* The response to the next Metadata request throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Metadata, 1, throttle_ms));
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 0, NULL, &md, 5000));
        rd_kafka_metadata_destroy(md);

        /* This one is not read by the broker until the throttle has passed,
         * which is much longer than socket.timeout.ms. */
        ts  = test_clock();
        err = rd_kafka_metadata(rk, 0, NULL, &md, 30 * 1000);
        TEST_SAY("Metadata request took %" PRId64 "ms: %s\n",
                 (test_clock() - ts) / 1000, rd_kafka_err2str(err));
        TEST_ASSERT(!err, "Metadata request failed: %s", rd_kafka_err2str(err));
        rd_kafka_metadata_destroy(md);
        ASSERT_GAP_AT_LEAST(test_clock() - ts, throttle_ms - 500,
                            "The duration of the Metadata request");
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A response with an error and a throttle time: the messages are
 *        retried when the throttle has passed, and delivered once and in order.
 */
static void do_test_idempotent_retry_held_back(void) {
        const int throttle_ms = 2000;
        const char *topic     = test_mk_topic_name("0193_retry", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, second;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_Produce, 1,
            RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, "enable.idempotence",
                          "true", NULL);

        produce_ok(rk, topic, 0, &first);
        produce_ok(rk, topic, 0, &second);

        TEST_ASSERT(first.offset == 0 && second.offset == 1,
                    "Expected the messages at offsets 0 and 1, got %" PRId64
                    " and %" PRId64,
                    first.offset, second.offset);
        TEST_ASSERT(events_sent_cnt() == 3,
                    "Expected the failed ProduceRequest, its retry and the "
                    "one of the second message, got %d",
                    events_sent_cnt());
        ASSERT_GAP_AT_LEAST(
            events_sent_gap(0, 1), throttle_ms,
            "The gap between the failed ProduceRequest and its retry");

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The idempotent producer bumps its epoch when a message times out,
 *        also if it was held back, and carries on when the throttle has
 *        passed.
 */
static void do_test_idempotent_held_back_timeout(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_idemp_timeout", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, held, after;
        char errstr[256];

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));

        rk = producer_new(bootstraps, &interceptor, "enable.idempotence",
                          "true", "message.timeout.ms", "1000", NULL);

        produce_ok(rk, topic, 0, &first);

        produce1(rk, topic, 0, &held);
        wait_dr(rk, &held, 30 * 1000);
        TEST_ASSERT(held.err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
                    "Expected the message to time out, got %s",
                    rd_kafka_err2str(held.err));
        TEST_ASSERT(events_sent_cnt() == 1,
                    "The message that timed out must not have been sent, "
                    "%d ProduceRequests were",
                    events_sent_cnt());

        /* The next one is sent when the throttle has passed. */
        sleep_until(first.dr_ts + (int64_t)throttle_ms * 1000 + 300 * 1000);
        produce_ok(rk, topic, 0, &after);

        TEST_ASSERT(events_bump_cnt() == 1,
                    "Expected 1 epoch bump after the message timeout, got %d",
                    events_bump_cnt());
        TEST_ASSERT(!rd_kafka_fatal_error(rk, errstr, sizeof(errstr)),
                    "Fatal error: %s", errstr);

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The transactional producer with its messages held back by the
 *        throttle: commit_transaction() times out and can be called again.
 */
static void do_test_txn_commit_timeout(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_txn_commit", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, held;
        rd_kafka_error_t *error;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        rk = producer_new(bootstraps, &interceptor, "transactional.id",
                          "0193_txn_commit", NULL);
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));
        produce_ok(rk, topic, 0, &first);
        produce1(rk, topic, 0, &held);

        error = rd_kafka_commit_transaction(rk, 1000);
        TEST_ASSERT(error, "Expected commit_transaction() to time out");
        TEST_ASSERT(rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Expected a timeout, got %s", rd_kafka_error_string(error));
        TEST_ASSERT(rd_kafka_error_is_retriable(error),
                    "Expected a retriable error: %s",
                    rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);
        TEST_ASSERT(events_sent_cnt() == 1,
                    "The message held back must not have been sent, "
                    "%d ProduceRequests were",
                    events_sent_cnt());

        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));
        wait_dr(rk, &held, 1000);
        TEST_ASSERT(!held.err, "Message not delivered: %s",
                    rd_kafka_err2str(held.err));
        ASSERT_GAP_AT_LEAST(
            held.dr_ts - first.dr_ts, throttle_ms,
            "The gap between the delivery of the first and the held "
            "back message");

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


static rd_bool_t is_init_producer_id_request(rd_kafka_mock_request_t *request,
                                             void *opaque) {
        return rd_kafka_mock_request_api_key(request) ==
               RD_KAFKAP_InitProducerId;
}


/**
 * @brief A message of a transaction that times out while it is held back
 *        makes the transaction abortable. After the abort the epoch is bumped
 *        and the next transaction works.
 */
static void do_test_txn_held_back_timeout(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_txn_timeout", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, held, next;
        rd_kafka_error_t *error;
        size_t init_pid_cnt;
        char errstr[256];

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        rd_kafka_mock_start_request_tracking(mcluster);

        rk = producer_new(bootstraps, &interceptor, "transactional.id",
                          "0193_txn_timeout", "message.timeout.ms", "1000",
                          "socket.timeout.ms", "1000", NULL);
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));
        produce_ok(rk, topic, 0, &first);

        produce1(rk, topic, 0, &held);
        wait_dr(rk, &held, 30 * 1000);
        TEST_ASSERT(held.err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
                    "Expected the message to time out, got %s",
                    rd_kafka_err2str(held.err));

        error = rd_kafka_commit_transaction(rk, 30 * 1000);
        TEST_ASSERT(error, "Expected commit_transaction() to fail");
        TEST_ASSERT(rd_kafka_error_txn_requires_abort(error),
                    "Expected an abortable error, got %s",
                    rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);

        TEST_CALL_ERROR__(rd_kafka_abort_transaction(rk, 30 * 1000));

        /* The next message is held back until the throttle has passed, which
         * is later than its message.timeout.ms. */
        sleep_until(first.dr_ts + (int64_t)throttle_ms * 1000 + 500 * 1000);
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));
        produce_ok(rk, topic, 0, &next);
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));

        TEST_ASSERT(!rd_kafka_fatal_error(rk, errstr, sizeof(errstr)),
                    "Fatal error: %s", errstr);
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());

        init_pid_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_init_producer_id_request, NULL);
        rd_kafka_mock_stop_request_tracking(mcluster);
        TEST_ASSERT(init_pid_cnt == 2,
                    "Expected an InitProducerId for the first PID and one "
                    "for the epoch bump, got %" PRIusz,
                    init_pid_cnt);

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Aborting a transaction purges its messages that are held back,
 *        it does not wait for the throttle.
 *
 * The transaction coordinator has its own connection to the broker, the
 * throttle of the connection that is used to produce does not mute it.
 */
static void do_test_txn_abort_held_back(void) {
        const int throttle_ms = 3000;
        const char *topic     = test_mk_topic_name("0193_txn_abort", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres first, held[2], next;
        int64_t ts;
        int i;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        rk = producer_new(bootstraps, &interceptor, "transactional.id",
                          "0193_txn_abort", "socket.timeout.ms", "1000", NULL);
        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Produce, 1, throttle_ms));
        produce_ok(rk, topic, 0, &first);
        produce1(rk, topic, 0, &held[0]);
        produce1(rk, topic, 0, &held[1]);

        ts = test_clock();
        TEST_CALL_ERROR__(rd_kafka_abort_transaction(rk, 30 * 1000));
        TEST_SAY("Abort took %" PRId64 "ms\n", (test_clock() - ts) / 1000);
        TEST_ASSERT(test_clock() - ts < (int64_t)throttle_ms * 1000 / 2,
                    "The abort took %" PRId64 "ms, with a throttle of %dms",
                    (test_clock() - ts) / 1000, throttle_ms);

        for (i = 0; i < 2; i++) {
                wait_dr(rk, &held[i], 5000);
                TEST_ASSERT(held[i].err == RD_KAFKA_RESP_ERR__PURGE_QUEUE,
                            "Expected the held back message to be purged, "
                            "got %s",
                            rd_kafka_err2str(held[i].err));
        }
        TEST_ASSERT(events_sent_cnt() == 1,
                    "The messages held back must not have been sent, "
                    "%d ProduceRequests were",
                    events_sent_cnt());

        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));
        produce_ok(rk, topic, 0, &next);
        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());

        producer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_produce_throttle_mock(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        mtx_init(&events_lock, mtx_plain);

        do_test_held_back();
        do_test_held_back_by_other_request();
        do_test_per_broker();
        do_test_version_not_honored();
        do_test_held_back_message_timeout();
        do_test_destroy_while_held_back();
        do_test_pipelined_requests_timeout();
        do_test_request_sent_while_muted();
        do_test_idempotent_retry_held_back();
        do_test_idempotent_held_back_timeout();
        do_test_txn_commit_timeout();
        do_test_txn_held_back_timeout();
        do_test_txn_abort_held_back();

        mtx_destroy(&events_lock);

        return 0;
}
