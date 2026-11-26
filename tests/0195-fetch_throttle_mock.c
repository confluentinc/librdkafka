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
 * @name KIP-219: a consumer does not send FetchRequests to a broker while
 *       the broker throttles the connection, and the other requests sent
 *       to it do not time out behind the mute.
 *
 * The implementation is partial: for a consumer only the FetchRequests are
 * held back, the other request types are still sent to the connection that
 * the broker has muted. Their timeouts are extended while the broker throttles,
 * instead of the requests being held back as well.
 *
 * A mock broker answers a throttled request right away and then stops
 * reading from the connection for the throttle time, as a Kafka broker does,
 * see rd_kafka_mock_broker_push_request_throttle(). Its answer to a throttled
 * Fetch has no data.
 *
 * The mock broker mutes the connection itself, so the time at which a request
 * arrives at it can't tell whether the client held it back or sent it right
 * away. The tests look at what the client does instead: the time at which it
 * logs that a FetchRequest was sent, and what the consumer returns.
 */

#define EVENTS_MAX 256
#define STATS_MAX  512

/** Margin for the clocks of the log callback and of the test. */
#define SLACK_US (150 * 1000)

/**
 * How long after the end of a throttle the next FetchRequest can be sent: the
 * broker thread is woken up when the throttle ends, it does not wait for its
 * next scheduled wakeup, that is up to a second away.
 */
#define LATE_MAX_US (500 * 1000)

/**
 * The log and the stats callbacks are called by the thread that logs, the
 * broker threads and the main thread, so events is only accessed with
 * events_lock held.
 */
static mtx_t events_lock;

/** What the client logged */
static struct {
        int64_t sent_ts[EVENTS_MAX]; /**< When each FetchRequest was sent */
        int sent_ver[EVENTS_MAX];    /**< and its version */
        int sent_broker[EVENTS_MAX]; /**< and the id of the broker */
        int sent_cnt;

        /** The responses that throttled the client */
        int64_t throttle_ts[EVENTS_MAX];
        int throttle_ms[EVENTS_MAX];
        int throttle_broker[EVENTS_MAX];
        char throttle_api[EVENTS_MAX][32]; /**< request type, as "Fetch" */
        int throttle_cnt;

        int reqtmout_cnt; /**< Requests that timed out */

        /** The sum of the wakeups of the broker threads, at each stats */
        int64_t stats_ts[STATS_MAX];
        int64_t stats_wakeups[STATS_MAX];
        int stats_cnt;
} events;

/** What a consumer returned */
struct consumed {
        int cnt;            /**< messages */
        int part_cnt[4];    /**< by partition */
        int64_t min_offset; /**< the lowest offset, -1 if there is none */
        int64_t first_ts;   /**< when the first message was returned, or 0 */
        int64_t part_first_ts[4]; /**< and the first of each partition */
};


static void events_reset(void) {
        mtx_lock(&events_lock);
        memset(&events, 0, sizeof(events));
        mtx_unlock(&events_lock);
}

/**
 * @returns the id of the broker in the log message \p buf, that starts with
 *          "host:port/id: ", the message is at \p msg.
 */
static int log_broker_id(const char *buf, const char *msg) {
        const char *slash = NULL, *p;

        for (p = buf; p < msg; p++)
                if (*p == '/')
                        slash = p;

        return slash ? atoi(slash + 1) : -1;
}

static void events_log_cb(const rd_kafka_t *rk,
                          int level,
                          const char *fac,
                          const char *buf) {
        const char *sent      = "Sent FetchRequest (v";
        const char *throttled = "Throttled for ";
        const char *p;

        mtx_lock(&events_lock);
        if (!strcmp(fac, "REQTMOUT")) {
                events.reqtmout_cnt++;
        } else if (!strcmp(fac, "THROTTLE") && (p = strstr(buf, throttled)) &&
                   events.throttle_cnt < EVENTS_MAX) {
                int i          = events.throttle_cnt++;
                const char *by = strstr(p, " by "), *end;

                events.throttle_broker[i] = log_broker_id(buf, p);
                events.throttle_ms[i]     = atoi(p + strlen(throttled));
                events.throttle_ts[i]     = test_clock();
                events.throttle_api[i][0] = '\0';
                if (by && (end = strstr(by, "Response"))) {
                        size_t len = (size_t)(end - (by + 4));

                        if (len >= sizeof(events.throttle_api[i]))
                                len = sizeof(events.throttle_api[i]) - 1;
                        memcpy(events.throttle_api[i], by + 4, len);
                        events.throttle_api[i][len] = '\0';
                }
        } else if ((p = strstr(buf, sent)) && events.sent_cnt < EVENTS_MAX) {
                events.sent_broker[events.sent_cnt] = log_broker_id(buf, p);
                events.sent_ver[events.sent_cnt]    = atoi(p + strlen(sent));
                events.sent_ts[events.sent_cnt]     = test_clock();
                events.sent_cnt++;
        }
        mtx_unlock(&events_lock);
}

/**
 * The sum of the wakeups of all the broker threads of the client: a thread
 * that has no reason to wake up sleeps, one that spins counts thousands.
 */
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        const char *key = "\"wakeups\":";
        const char *p   = json;
        int64_t total   = 0;

        while ((p = strstr(p, key))) {
                p += strlen(key);
                total += strtoll(p, NULL, 10);
        }

        mtx_lock(&events_lock);
        if (events.stats_cnt < STATS_MAX) {
                events.stats_ts[events.stats_cnt]      = test_clock();
                events.stats_wakeups[events.stats_cnt] = total;
                events.stats_cnt++;
        }
        mtx_unlock(&events_lock);

        return 0;
}

/**
 * @returns the number of FetchRequests sent so far.
 */
static int events_sent_cnt(void) {
        int cnt;
        mtx_lock(&events_lock);
        cnt = events.sent_cnt;
        mtx_unlock(&events_lock);
        return cnt;
}

/**
 * @returns the time at which the \p idx'th (from 0) FetchRequest was sent,
 *          whatever the broker.
 */
static int64_t events_sent_ts(int idx) {
        int64_t ts;

        mtx_lock(&events_lock);
        TEST_ASSERT(idx < events.sent_cnt,
                    "FetchRequest %d was not sent, %d were", idx,
                    events.sent_cnt);
        ts = events.sent_ts[idx];
        mtx_unlock(&events_lock);

        return ts;
}

/**
 * @returns the time between the sending of FetchRequests \p a and \p b.
 */
static int64_t events_sent_gap(int a, int b) {
        return events_sent_ts(b) - events_sent_ts(a);
}

/**
 * @returns the version of the \p idx'th (from 0) FetchRequest.
 */
static int events_sent_ver(int idx) {
        int ver;

        mtx_lock(&events_lock);
        TEST_ASSERT(idx < events.sent_cnt,
                    "FetchRequest %d was not sent, %d were", idx,
                    events.sent_cnt);
        ver = events.sent_ver[idx];
        mtx_unlock(&events_lock);

        return ver;
}

/**
 * @returns the time at which the first FetchRequest to \p broker, or to any if
 *          it is -1, was sent at or after \p ts, or -1 if there is none.
 *
 * Only one FetchRequest is in flight per broker, so the first one that is sent
 * after a throttle is received is the one that must be held back: a client
 * that does not hold it back sends it at once, and then nothing until its
 * response, that the broker has muted, comes. Looking for requests in the
 * middle of the throttle would not see it.
 */
static int64_t events_first_sent_after(int broker, int64_t ts) {
        int64_t first = -1;
        int i;

        mtx_lock(&events_lock);
        for (i = 0; i < events.sent_cnt; i++) {
                if ((broker != -1 && events.sent_broker[i] != broker) ||
                    events.sent_ts[i] < ts)
                        continue;
                first = events.sent_ts[i];
                break;
        }
        mtx_unlock(&events_lock);

        return first;
}

/**
 * @returns the number of FetchRequests sent to \p broker in [from, to].
 */
static int events_sent_between(int broker, int64_t from, int64_t to) {
        int cnt = 0, i;

        mtx_lock(&events_lock);
        for (i = 0; i < events.sent_cnt; i++)
                if (events.sent_broker[i] == broker &&
                    events.sent_ts[i] >= from && events.sent_ts[i] <= to)
                        cnt++;
        mtx_unlock(&events_lock);

        return cnt;
}

/**
 * @brief Find the first response of type \p api that throttled the client.
 *
 * @param broker only look at this broker, or -1 for any.
 *
 * @returns rd_true if there is one, and sets \p ts to the time at which it
 *          was received, \p ms to the throttle time and \p broker_out to the
 *          id of the broker.
 */
static rd_bool_t events_find_throttle(const char *api,
                                      int broker,
                                      int64_t *ts,
                                      int *ms,
                                      int *broker_out) {
        rd_bool_t found = rd_false;
        int i;

        mtx_lock(&events_lock);
        for (i = 0; i < events.throttle_cnt; i++) {
                if (strcmp(events.throttle_api[i], api) ||
                    (broker != -1 && events.throttle_broker[i] != broker))
                        continue;
                *ts         = events.throttle_ts[i];
                *ms         = events.throttle_ms[i];
                *broker_out = events.throttle_broker[i];
                found       = rd_true;
                break;
        }
        mtx_unlock(&events_lock);

        return found;
}

static int events_reqtmout_cnt(void) {
        int cnt;
        mtx_lock(&events_lock);
        cnt = events.reqtmout_cnt;
        mtx_unlock(&events_lock);
        return cnt;
}

/**
 * @brief The number of wakeups of the broker threads between the first stats
 *        after \p from and the last before \p to.
 *
 * @returns rd_false if there are no two stats in the interval.
 */
static rd_bool_t
events_wakeups_between(int64_t from, int64_t to, int64_t *deltap) {
        int64_t first = -1, last = -1;
        int i;

        mtx_lock(&events_lock);
        for (i = 0; i < events.stats_cnt; i++) {
                if (first == -1 && events.stats_ts[i] >= from)
                        first = events.stats_wakeups[i];
                if (events.stats_ts[i] <= to)
                        last = events.stats_wakeups[i];
        }
        mtx_unlock(&events_lock);

        if (first == -1 || last == -1 || last < first)
                return rd_false;

        *deltap = last - first;
        return rd_true;
}


/**
 * @brief Create a mock cluster of \p broker_cnt brokers that do not support
 *        telemetry.
 *
 * The requests that the client enqueues to a broker that is not connected
 * connect to it, and the answers of the broker to the telemetry requests wake
 * up its thread, which would hide a missing wakeup or a missing connection.
 * The client does not set enable.metrics.push=false instead, rd_kafka_destroy()
 * of such a client fails the assertion of rd_kafka_telemetry_set_terminated()
 * on the thread when asserts are enabled.
 */
static rd_kafka_mock_cluster_t *mock_cluster_new(int broker_cnt,
                                                 const char **bootstrapsp) {
        rd_kafka_mock_cluster_t *mcluster =
            test_mock_cluster_new(broker_cnt, bootstrapsp);

        TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(
            mcluster, RD_KAFKAP_GetTelemetrySubscriptions, -1, -1));

        return mcluster;
}

/**
 * @brief Create a consumer with the \p ... configuration, a NULL terminated
 *        list of property names and values.
 *
 * It logs the protocol requests and the throttles, and has its stats served.
 * Destroy it with consumer_destroy().
 */
static rd_kafka_t *consumer_new(const char *bootstraps,
                                const char *group_id,
                                test_conf_log_interceptor_t **interceptorp,
                                ...) {
        const char *debug_contexts[] = {"protocol", "broker", NULL};
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        va_list ap;
        const char *name;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "fetch.wait.max.ms", "100");
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "statistics.interval.ms", "200");
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        va_start(ap, interceptorp);
        while ((name = va_arg(ap, const char *)))
                test_conf_set(conf, name, va_arg(ap, const char *));
        va_end(ap);

        *interceptorp =
            test_conf_set_log_interceptor(conf, events_log_cb, debug_contexts);

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);
        rd_kafka_poll_set_consumer(rk);

        return rk;
}

static void consumer_destroy(rd_kafka_t *rk,
                             test_conf_log_interceptor_t *interceptor) {
        rd_kafka_resp_err_t err = rd_kafka_consumer_close(rk);

        TEST_ASSERT(!err, "rd_kafka_consumer_close() failed: %s",
                    rd_kafka_err2str(err));
        rd_kafka_destroy(rk);
        rd_free(interceptor);
}

/**
 * @brief Assign the first \p partition_cnt partitions of \p topic, from the
 *        beginning.
 */
static void
consumer_assign(rd_kafka_t *rk, const char *topic, int partition_cnt) {
        rd_kafka_topic_partition_list_t *parts =
            rd_kafka_topic_partition_list_new(partition_cnt);
        rd_kafka_resp_err_t err;
        int i;

        for (i = 0; i < partition_cnt; i++)
                rd_kafka_topic_partition_list_add(parts, topic, i)->offset =
                    RD_KAFKA_OFFSET_BEGINNING;
        err = rd_kafka_assign(rk, parts);
        TEST_ASSERT(!err, "rd_kafka_assign() failed: %s",
                    rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(parts);
}

static void consumed_init(struct consumed *c) {
        memset(c, 0, sizeof(*c));
        c->min_offset = -1;
}

/**
 * @brief Poll the consumer until \p until, and count what it returns in \p c,
 *        if it is not NULL. A consumer error fails the test.
 */
static void
consumer_poll_until(rd_kafka_t *rk, int64_t until, struct consumed *c) {
        while (test_clock() < until) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 50);

                if (!rkm)
                        continue;
                TEST_ASSERT(!rkm->err, "Consumer error: %s",
                            rd_kafka_message_errstr(rkm));
                if (c) {
                        int64_t now = test_clock();

                        c->cnt++;
                        if (!c->first_ts)
                                c->first_ts = now;
                        if (rkm->partition >= 0 && rkm->partition < 4) {
                                c->part_cnt[rkm->partition]++;
                                if (!c->part_first_ts[rkm->partition])
                                        c->part_first_ts[rkm->partition] = now;
                        }
                        if (c->min_offset == -1 || rkm->offset < c->min_offset)
                                c->min_offset = rkm->offset;
                }
                rd_kafka_message_destroy(rkm);
        }
}

static void consumer_poll_for(rd_kafka_t *rk, int ms, struct consumed *c) {
        consumer_poll_until(rk, test_clock() + (int64_t)ms * 1000, c);
}

/**
 * @brief Poll the consumer until a response of type \p api throttled it, from
 *        \p broker or -1 for any.
 *
 * @param ts is set to the time at which the response was received, \p ms to
 *           the throttle time and \p broker_out to the id of the broker.
 */
static void consumer_poll_until_throttled(rd_kafka_t *rk,
                                          const char *api,
                                          int broker,
                                          struct consumed *c,
                                          int64_t *ts,
                                          int *ms,
                                          int *broker_out) {
        int64_t until = test_clock() + 30 * 1000 * 1000;

        while (test_clock() < until) {
                if (events_find_throttle(api, broker, ts, ms, broker_out))
                        return;
                consumer_poll_for(rk, 50, c);
        }

        TEST_FAIL("The consumer was not throttled by a %s response", api);
}

/**
 * @brief Poll the consumer until \p c has \p cnt messages, for \p ms at most.
 */
static void
consumer_poll_until_cnt(rd_kafka_t *rk, int cnt, int ms, struct consumed *c) {
        int64_t until = test_clock() + (int64_t)ms * 1000;

        while (test_clock() < until && c->cnt < cnt)
                consumer_poll_for(rk, 50, c);
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
 * @brief The next FetchRequest is not sent until the throttle has passed, the
 *        throttled response has no data, and the broker thread sleeps meanwhile
 *        instead of spinning.
 */
static void do_test_held_back(void) {
        const int throttle_ms = 3300;
        const int msgcnt      = 100;
        const char *topic     = test_mk_topic_name("0195_held_back", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct consumed c;
        int64_t t0, deadline, wakeups = 0, first;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);

        /* The response to the first FetchRequest throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        consumer_poll_until_throttled(rk, "Fetch", -1, &c, &t0, &ms, &broker);
        TEST_ASSERT(ms == throttle_ms, "Expected a throttle of %dms, got %dms",
                    throttle_ms, ms);
        deadline = t0 + (int64_t)throttle_ms * 1000;

        /* The broker thread has nothing to do while the FetchRequest is held
         * back, it must not spin. */
        consumer_poll_until(rk, deadline - 300 * 1000, &c);
        TEST_ASSERT(
            events_wakeups_between(t0 + 500 * 1000, t0 + 2500 * 1000, &wakeups),
            "No stats in the throttle");
        TEST_SAY("The broker threads woke up %" PRId64 " times in 2s\n",
                 wakeups);
        TEST_ASSERT(wakeups < 300,
                    "The broker threads woke up %" PRId64
                    " times in 2s of a throttle, are they spinning?",
                    wakeups);

        /* When it has passed the consumer fetches the messages. */
        consumer_poll_until_cnt(rk, msgcnt, 15 * 1000, &c);
        TEST_ASSERT(c.cnt == msgcnt, "Expected %d messages, got %d", msgcnt,
                    c.cnt);

        /* The next FetchRequest is sent when the throttle ends, not before and
         * not much later, and the messages are only returned then: the
         * throttled response had none. */
        first = events_first_sent_after(-1, t0);
        TEST_ASSERT(first != -1, "No FetchRequest was sent after the throttle");
        ASSERT_GAP_AT_LEAST(first - t0, throttle_ms,
                            "The time between the throttle and the next "
                            "FetchRequest");
        TEST_ASSERT(first - deadline < LATE_MAX_US,
                    "The FetchRequest was sent %" PRId64
                    "ms after the end of the throttle, the broker thread must "
                    "be woken up when it ends",
                    (first - deadline) / 1000);
        ASSERT_GAP_AT_LEAST(c.first_ts - t0, throttle_ms,
                            "The time between the throttle and the first "
                            "message");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The throttle time is taken from the response of any request type,
 *        as the broker mutes the whole connection.
 */
static void do_test_held_back_by_other_request(void) {
        const int throttle_ms = 3000;
        const int msgcnt      = 10;
        const char *topic     = test_mk_topic_name("0195_other_request", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct consumed c;
        int64_t ts;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);

        /* The response to the ListOffsetsRequest that finds the first offset
         * throttles the client, before the first FetchRequest. */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_ListOffsets, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        consumer_poll_until_cnt(rk, msgcnt, 30 * 1000, &c);
        TEST_ASSERT(c.cnt == msgcnt, "Expected %d messages, got %d", msgcnt,
                    c.cnt);

        TEST_ASSERT(
            events_find_throttle("ListOffsets", -1, &ts, &ms, &broker) &&
                ms == throttle_ms,
            "Expected a ListOffsets response to throttle the client for %dms",
            throttle_ms);
        ASSERT_GAP_AT_LEAST(
            events_sent_ts(0) - ts, throttle_ms,
            "The gap between the throttled ListOffsets response and the "
            "FetchRequest");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Only the throttled broker is not sent FetchRequests, the partitions
 *        of the other one are consumed meanwhile.
 */
static void do_test_per_broker(void) {
        const int throttle_ms = 4000;
        const int msgcnt      = 50;
        const int more_cnt    = 10;
        const char *topic     = test_mk_topic_name("0195_per_broker", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct consumed c;
        int64_t t0, deadline, first;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(2, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 2, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);

        /* The first broker throttles the client */
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 2);

        consumer_poll_until_throttled(rk, "Fetch", 1, &c, &t0, &ms, &broker);
        deadline = t0 + (int64_t)throttle_ms * 1000;

        /* The second broker is not throttled: its messages are consumed, and
         * so are those that are produced meanwhile. */
        consumer_poll_until(rk, t0 + 1000 * 1000, &c);
        TEST_ASSERT(c.part_cnt[1] == msgcnt,
                    "Expected the %d messages of the partition of the broker "
                    "that is not throttled, got %d",
                    msgcnt, c.part_cnt[1]);
        test_produce_msgs_easy_v(topic, 0, 1, msgcnt, more_cnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        consumer_poll_until(rk, deadline - 500 * 1000, &c);
        TEST_ASSERT(c.part_cnt[1] == msgcnt + more_cnt,
                    "Expected %d messages of the partition of the broker "
                    "that is not throttled, got %d",
                    msgcnt + more_cnt, c.part_cnt[1]);

        TEST_ASSERT(
            events_sent_between(2, t0 + 100 * 1000, deadline - SLACK_US) >= 3,
            "Expected FetchRequests to be sent to the broker that is "
            "not throttled meanwhile, got %d",
            events_sent_between(2, t0 + 100 * 1000, deadline - SLACK_US));

        /* The throttled broker is fetched from when the throttle has passed. */
        consumer_poll_until_cnt(rk, 2 * msgcnt + more_cnt, 15 * 1000, &c);
        TEST_ASSERT(c.part_cnt[0] == msgcnt,
                    "Expected the %d messages of the partition of the broker "
                    "that was throttled, got %d",
                    msgcnt, c.part_cnt[0]);

        /* The first FetchRequest to it after the throttle is sent when the
         * throttle has ended, and the first message of its partition is only
         * returned then: the throttled response had none. */
        first = events_first_sent_after(1, t0);
        TEST_ASSERT(first != -1,
                    "No FetchRequest was sent to the broker after the "
                    "throttle");
        ASSERT_GAP_AT_LEAST(first - t0, throttle_ms,
                            "The time between the throttle and the next "
                            "FetchRequest to the broker");
        ASSERT_GAP_AT_LEAST(c.part_first_ts[0] - t0, throttle_ms,
                            "The time between the throttle and the first "
                            "message of its partition");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A throttle time is only honored for the versions of the request
 *        that have the KIP-219 semantics.
 */
static void do_test_version_not_honored(void) {
        const int throttle_ms = 2000;
        const int msgcnt      = 100;
        const char *topic     = test_mk_topic_name("0195_version", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct consumed c;
        int64_t until;

        SUB_TEST_QUICK();

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Fetch v8 is the first version with KIP-219. */
        TEST_CALL_ERR__(
            rd_kafka_mock_set_apiversion(mcluster, RD_KAFKAP_Fetch, 0, 7));
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        /* The second FetchRequest is sent at once. The mock broker has muted
         * the connection, the messages are returned when that is over. */
        until = test_clock() + 15 * 1000 * 1000;
        while (test_clock() < until && events_sent_cnt() < 2)
                consumer_poll_for(rk, 50, &c);
        TEST_ASSERT(events_sent_cnt() >= 2, "Expected 2 FetchRequests, got %d",
                    events_sent_cnt());
        TEST_ASSERT(events_sent_ver(0) == 7 && events_sent_ver(1) == 7,
                    "Expected Fetch v7, got v%d and v%d", events_sent_ver(0),
                    events_sent_ver(1));
        TEST_ASSERT(events_sent_gap(0, 1) < 500 * 1000,
                    "The second FetchRequest was sent %" PRId64
                    "ms after the first, it must not be held back",
                    events_sent_gap(0, 1) / 1000);

        consumer_poll_until_cnt(rk, msgcnt, 15 * 1000, &c);
        TEST_ASSERT(c.cnt == msgcnt, "Expected %d messages, got %d", msgcnt,
                    c.cnt);

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Pausing, resuming and seeking a partition while the FetchRequests
 *        are held back does not send one, and the consumer continues from
 *        the offset that it was moved to when the throttle has passed.
 */
static void do_test_partition_ops_while_held_back(void) {
        const int throttle_ms = 4000;
        const int msgcnt      = 100;
        const int seek_to     = 50;
        const char *topic     = test_mk_topic_name("0195_part_ops", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_error_t *error;
        struct consumed c;
        int64_t t0, first;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        consumer_poll_until_throttled(rk, "Fetch", -1, &c, &t0, &ms, &broker);

        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0);

        consumer_poll_until(rk, t0 + 500 * 1000, &c);
        TEST_CALL_ERR__(rd_kafka_pause_partitions(rk, parts));
        consumer_poll_until(rk, t0 + 1000 * 1000, &c);
        TEST_CALL_ERR__(rd_kafka_resume_partitions(rk, parts));
        consumer_poll_until(rk, t0 + 1500 * 1000, &c);
        parts->elems[0].offset = seek_to;
        error                  = rd_kafka_seek_partitions(rk, parts, 5000);
        TEST_ASSERT(!error, "rd_kafka_seek_partitions() failed: %s",
                    error ? rd_kafka_error_string(error) : "");
        rd_kafka_topic_partition_list_destroy(parts);

        consumer_poll_until_cnt(rk, msgcnt - seek_to, 15 * 1000, &c);
        TEST_ASSERT(c.cnt == msgcnt - seek_to && c.min_offset == seek_to,
                    "Expected the %d messages from offset %d, got %d from "
                    "offset %" PRId64,
                    msgcnt - seek_to, seek_to, c.cnt, c.min_offset);

        /* Whatever was done to the partition meanwhile, no FetchRequest was
         * sent, and no message returned, until the throttle ended. */
        first = events_first_sent_after(-1, t0);
        TEST_ASSERT(first != -1, "No FetchRequest was sent after the throttle");
        ASSERT_GAP_AT_LEAST(first - t0, throttle_ms,
                            "The time between the throttle and the next "
                            "FetchRequest");
        ASSERT_GAP_AT_LEAST(c.first_ts - t0, throttle_ms,
                            "The time between the throttle and the first "
                            "message");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The consumer can be closed and destroyed while its FetchRequests are
 *        held back, without waiting for the throttle to pass.
 */
static void do_test_close_while_held_back(void) {
        const int throttle_ms = 20000;
        const char *topic     = test_mk_topic_name("0195_close", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int64_t t0, start, took;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, 10, 10, "bootstrap.servers",
                                 bootstraps, NULL);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        consumer_poll_until_throttled(rk, "Fetch", -1, NULL, &t0, &ms, &broker);
        consumer_poll_for(rk, 300, NULL);

        /* The throttle is still far from its end, or it could not be what the
         * close does not wait for. */
        start = test_clock();
        TEST_ASSERT(t0 + (int64_t)throttle_ms * 1000 - start > 10 * 1000 * 1000,
                    "Only %" PRId64 "ms of the throttle are left",
                    (t0 + (int64_t)throttle_ms * 1000 - start) / 1000);
        consumer_destroy(rk, interceptor);
        took = test_clock() - start;
        TEST_SAY("Closing the consumer took %" PRId64 "ms\n", took / 1000);
        TEST_ASSERT(took < 5 * 1000 * 1000,
                    "Closing the consumer took %" PRId64
                    "ms, it must not wait for the throttle of %dms",
                    took / 1000, throttle_ms);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A request that is sent to a connection that is muted by the broker
 *        does not time out.
 *
 * Only FetchRequests are held back by the client.
 * rd_kafka_query_watermark_offsets() sends two ListOffsetsRequests, one
 * right after the other, to the leader of the partition.
 */
static void do_test_request_sent_while_muted(void) {
        const int throttle_ms = 4000;
        const int msgcnt      = 10;
        const char *topic     = test_mk_topic_name("0195_muted", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int64_t t0, start, end, low, high;
        rd_kafka_resp_err_t err;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        rk = consumer_new(bootstraps, topic, &interceptor, "socket.timeout.ms",
                          "1000", NULL);
        consumer_assign(rk, topic, 1);

        consumer_poll_until_throttled(rk, "Fetch", -1, NULL, &t0, &ms, &broker);

        /* The first one is not read by the broker until the throttle has
         * passed, which is much longer than socket.timeout.ms, and the
         * second is behind it. */
        start = test_clock();
        err   = rd_kafka_query_watermark_offsets(rk, topic, 0, &low, &high,
                                                 30 * 1000);
        end   = test_clock();
        TEST_SAY("The query took %" PRId64 "ms: %s\n", (end - start) / 1000,
                 rd_kafka_err2str(err));
        TEST_ASSERT(!err, "The query failed: %s", rd_kafka_err2str(err));
        TEST_ASSERT(high == msgcnt,
                    "Expected the high watermark %d, got %" PRId64, msgcnt,
                    high);
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());
        ASSERT_GAP_AT_LEAST(end - t0, throttle_ms,
                            "The time between the throttle and the end of the "
                            "query");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


static rd_bool_t is_api_version_request_to(rd_kafka_mock_request_t *request,
                                           void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_ApiVersion &&
               rd_kafka_mock_request_id(request) == *(const int32_t *)opaque;
}

/**
 * @brief The connection attempts to a broker that is down fail, and are
 *        reported to the application when the connection is refused, as it
 *        is on Linux.
 */
static int ignore_connection_errors_cb(rd_kafka_t *rk,
                                       rd_kafka_resp_err_t err,
                                       const char *reason) {
        if (err == RD_KAFKA_RESP_ERR__TRANSPORT ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                TEST_SAY("Ignoring allowed error: %s: %s\n",
                         rd_kafka_err2name(err), reason);
                return 0;
        }
        return 1;
}

/**
 * @brief The connection to a broker that throttles the consumer is
 *        established again when it is closed while the FetchRequests are held
 *        back, to be ready when the throttle ends.
 *
 * Nothing else must request the connection, or it hides a failure:
 * - The client bootstraps from the other broker, that stays connected.
 * - The group coordinator is that other broker, its connection is separate and
 *   it would connect to the broker too if it were the coordinator.
 * - The brokers do not support telemetry, see mock_cluster_new().
 *
 * It does not test that the FetchRequests are still held back when the
 * connection is back: a broker that is down is not in the metadata that the
 * client requests when it goes down, and when it is again the client has a new
 * broker, that did not throttle it.
 */
static void do_test_reconnect_while_throttled(void) {
        const int throttle_ms = 4000;
        const int msgcnt      = 10;
        int32_t broker_id     = 1;
        const char *topic     = test_mk_topic_name("0195_reconnect", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int64_t t0, window_end;
        size_t cnt = 0;
        int ms, broker;

        SUB_TEST_QUICK();

        events_reset();
        mcluster = mock_cluster_new(2, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 0, broker_id));
        TEST_CALL_ERR__(rd_kafka_mock_coordinator_set(mcluster, "group", topic,
                                                      broker_id + 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, broker_id, RD_KAFKAP_Fetch, 1, throttle_ms));
        rd_kafka_mock_start_request_tracking(mcluster);

        /* Only the address of the second broker. */
        bootstraps = strchr(bootstraps, ',');
        TEST_ASSERT(bootstraps, "Expected the addresses of two brokers");
        bootstraps++;

        test_curr->is_fatal_cb = ignore_connection_errors_cb;
        rk = consumer_new(bootstraps, topic, &interceptor, NULL);
        consumer_assign(rk, topic, 1);

        /* The first broker connects, and throttles the client. */
        consumer_poll_until_throttled(rk, "Fetch", broker_id, NULL, &t0, &ms,
                                      &broker);
        TEST_ASSERT(ms == throttle_ms, "Expected a throttle of %dms, got %dms",
                    throttle_ms, ms);
        consumer_poll_for(rk, 300, NULL);

        /* Close the connection, and let the client see it before the broker
         * can be connected to again. */
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_down(mcluster, broker_id));
        consumer_poll_for(rk, 300, NULL);
        rd_kafka_mock_clear_requests(mcluster);
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_up(mcluster, broker_id));

        /* Look for a new connection until well before the throttle ends. */
        window_end = t0 + (int64_t)(throttle_ms - 1000) * 1000;
        while (test_clock() < window_end) {
                consumer_poll_for(rk, 50, NULL);
                cnt = test_mock_get_matching_request_cnt(
                    mcluster, is_api_version_request_to, &broker_id);
                if (cnt > 0)
                        break;
        }
        TEST_ASSERT(cnt > 0,
                    "Expected the client to connect to broker %" PRId32
                    " again while it throttles the client, it did not in the "
                    "%" PRId64 "ms since the throttle of %dms started",
                    broker_id, (test_clock() - t0) / 1000, throttle_ms);

        consumer_destroy(rk, interceptor);
        test_curr->is_fatal_cb = NULL;
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief A consumer of a group, with the group protocol of the test run,
 *        keeps its assignment and does not exceed max.poll.interval.ms while
 *        its FetchRequests are held back for longer than that, as long as the
 *        application polls, and then consumes the messages once.
 */
static void do_test_subscribed(void) {
        const int throttle_ms = 7000;
        const int msgcnt      = 100;
        const char *topic     = test_mk_topic_name("0195_subscribed", 1);
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *topics;
        struct consumed c;
        int64_t t0, first;
        int ms, broker;

        SUB_TEST_QUICK("group.protocol=%s",
                       test_consumer_group_protocol_classic() ? "classic"
                                                              : "consumer");

        events_reset();
        consumed_init(&c);
        mcluster = mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps, NULL);
        TEST_CALL_ERR__(rd_kafka_mock_broker_push_request_throttle(
            mcluster, 1, RD_KAFKAP_Fetch, 1, throttle_ms));

        /* The session timeout is only for the classic protocol, where it must
         * not be longer than max.poll.interval.ms. */
        rk = consumer_new(bootstraps, topic, &interceptor,
                          "max.poll.interval.ms", "5000", "session.timeout.ms",
                          "5000", NULL);

        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        TEST_CALL_ERR__(rd_kafka_subscribe(rk, topics));
        rd_kafka_topic_partition_list_destroy(topics);

        consumer_poll_until_throttled(rk, "Fetch", -1, &c, &t0, &ms, &broker);
        TEST_ASSERT(ms == throttle_ms, "Expected a throttle of %dms, got %dms",
                    throttle_ms, ms);

        /* The application polls all the time, max.poll.interval.ms is not
         * exceeded, which would be an error here. */
        consumer_poll_until_cnt(rk, msgcnt, 15 * 1000, &c);
        consumer_poll_for(rk, 500, &c);
        TEST_ASSERT(c.cnt == msgcnt, "Expected the %d messages once, got %d",
                    msgcnt, c.cnt);

        /* No FetchRequest was sent, and no message returned, until the
         * throttle ended. */
        first = events_first_sent_after(-1, t0);
        TEST_ASSERT(first != -1, "No FetchRequest was sent after the throttle");
        ASSERT_GAP_AT_LEAST(first - t0, throttle_ms,
                            "The time between the throttle and the next "
                            "FetchRequest");
        ASSERT_GAP_AT_LEAST(c.first_ts - t0, throttle_ms,
                            "The time between the throttle and the first "
                            "message");

        consumer_destroy(rk, interceptor);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0195_fetch_throttle_mock(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        mtx_init(&events_lock, mtx_plain);

        do_test_held_back();
        do_test_held_back_by_other_request();
        do_test_per_broker();
        do_test_version_not_honored();
        do_test_partition_ops_while_held_back();
        do_test_close_while_held_back();
        do_test_request_sent_while_muted();
        do_test_reconnect_while_throttled();
        do_test_subscribed();

        mtx_destroy(&events_lock);

        return 0;
}
