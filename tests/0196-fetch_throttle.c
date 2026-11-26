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

/**
 * @name KIP-219: a consumer does not send FetchRequests to a broker while
 *       the broker throttles the connection, with a real broker.
 *
 * The broker throttles a consumer that exceeds its client quota: it answers
 * with an empty response and the throttle time, and then stops reading from
 * the connection for that time.
 *
 * The consumers of these tests have the client id QUOTA_CLIENT_ID, and the
 * consumer_byte_rate quota of that client id is set with kafka-configs.sh
 * while the tests run.
 *
 * The broker computes the throttle time. The tests consume the messages of a
 * topic, FETCH_MAX_BYTES at a time, until it is long enough, whatever the
 * quota window has accumulated so far, and look at what the client does from
 * its debug log: when it sent each FetchRequest, and when the broker
 * throttled it.
 *
 * The implementation is partial: for a consumer only the FetchRequests are
 * held back, the other request types are still sent to the connection that
 * the broker has muted. Their timeouts are extended while the broker throttles,
 * instead of the requests being held back as well.
 */

/** The consumers of the tests, and the quota are set for this client id. */
#define QUOTA_CLIENT_ID "librdkafka-0196"

/** The consumer_byte_rate quota */
#define QUOTA_BYTES_PER_SEC 5000

#define MSG_SIZE 1000

/** The messages of a batch, a batch is about as large as FETCH_MAX_BYTES. */
#define BATCH_MSGS 10

/** The messages of the topic, many more than the consumer can read before the
 *  broker throttles it. */
#define TOPIC_MSGS 400

/** What a FetchRequest asks for at most: about one batch, so that the broker
 *  throttles the consumer after a few of them, and not after one for the whole
 *  topic that would take it much longer. */
#define FETCH_MAX_BYTES 10000

/** The throttle time that the tests wait for. */
#define THROTTLE_MIN_MS 1500

/** A throttle that is much longer than socket.timeout.ms */
#define THROTTLE_LONG_MS  2500
#define SOCKET_TIMEOUT_MS 1000

#define EVENTS_MAX 1024

/** Margin for the clocks of the log callback and of the test. */
#define SLACK_US (150 * 1000)

/** What the client logged */
struct events {
        int64_t sent_ts[EVENTS_MAX]; /**< When each FetchRequest was sent */
        int sent_broker[EVENTS_MAX]; /**< and the id of the broker */
        int sent_cnt;
        int64_t throttle_ts[EVENTS_MAX]; /**< When a FetchResponse throttled
                                          *   the consumer */
        int throttle_ms[EVENTS_MAX];     /**< and for how long */
        int throttle_broker[EVENTS_MAX]; /**< and the id of the broker */
        int throttle_cnt;
        int reqtmout_cnt; /**< Requests that timed out */
};

/**
 * The log callback is called by the thread that logs, the broker threads and
 * the main thread, so events is only accessed with events_lock held.
 */
static mtx_t events_lock;
static struct events events;


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
        } else if ((p = strstr(buf, sent)) && events.sent_cnt < EVENTS_MAX) {
                events.sent_broker[events.sent_cnt] = log_broker_id(buf, p);
                events.sent_ts[events.sent_cnt]     = test_clock();
                events.sent_cnt++;
        } else if ((p = strstr(buf, throttled)) && strstr(p, "FetchResponse") &&
                   events.throttle_cnt < EVENTS_MAX) {
                int i = events.throttle_cnt++;

                events.throttle_broker[i] = log_broker_id(buf, p);
                events.throttle_ms[i]     = atoi(p + strlen(throttled));
                events.throttle_ts[i]     = test_clock();
        }
        mtx_unlock(&events_lock);
}

/**
 * @returns a copy of the events, to be freed by the caller.
 */
static struct events *events_copy(void) {
        struct events *ev = rd_malloc(sizeof(*ev));

        mtx_lock(&events_lock);
        memcpy(ev, &events, sizeof(*ev));
        mtx_unlock(&events_lock);

        return ev;
}

/**
 * @brief Find a FetchResponse that throttled the consumer for at least
 *        \p min_ms.
 *
 * @returns rd_true if there is one, and sets \p ts to the time at which it
 *          was received, \p ms to the throttle time and \p broker to the id
 *          of the broker.
 */
static rd_bool_t
events_find_throttle(int min_ms, int64_t *ts, int *ms, int *broker) {
        rd_bool_t found = rd_false;
        int i;

        mtx_lock(&events_lock);
        for (i = 0; i < events.throttle_cnt; i++) {
                if (events.throttle_ms[i] < min_ms)
                        continue;
                *ts     = events.throttle_ts[i];
                *ms     = events.throttle_ms[i];
                *broker = events.throttle_broker[i];
                found   = rd_true;
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
 * @brief Create a topic with TOPIC_MSGS messages, in batches of BATCH_MSGS.
 */
static void topic_create(const char *topic) {
        test_create_topic_wait_exists(NULL, topic, 1, 1, 30 * 1000);
        test_produce_msgs_easy_v(topic, 0, 0, 0, TOPIC_MSGS, MSG_SIZE,
                                 "batch.num.messages", "10", "linger.ms", "200",
                                 NULL);
}

/**
 * @brief Create a consumer of \p topic with the \p ... configuration, a NULL
 *        terminated list of property names and values.
 *
 * It has the client id QUOTA_CLIENT_ID, reads about FETCH_MAX_BYTES with each
 * FetchRequest, and logs the requests and the throttles. Destroy it with
 * consumer_destroy().
 */
static rd_kafka_t *consumer_new(const char *topic,
                                test_conf_log_interceptor_t **interceptorp,
                                ...) {
        const char *debug_contexts[] = {"protocol", "broker", NULL};
        char fetch_max_bytes[16];
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        va_list ap;
        const char *name;

        rd_snprintf(fetch_max_bytes, sizeof(fetch_max_bytes), "%d",
                    FETCH_MAX_BYTES);

        test_conf_init(&conf, NULL, 180);
        test_conf_set(conf, "client.id", QUOTA_CLIENT_ID);
        test_conf_set(conf, "group.id", topic);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "message.max.bytes", fetch_max_bytes);
        test_conf_set(conf, "fetch.max.bytes", fetch_max_bytes);
        test_conf_set(conf, "fetch.message.max.bytes", fetch_max_bytes);

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
        test_consumer_close(rk);
        rd_kafka_destroy(rk);
        rd_free(interceptor);
}

/**
 * @brief Poll the consumer until \p until, and count the messages that it
 *        returns in \p cntp. A consumer error fails the test.
 */
static void consumer_poll_until(rd_kafka_t *rk, int64_t until, int *cntp) {
        while (test_clock() < until) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 100);

                if (!rkm)
                        continue;
                TEST_ASSERT(!rkm->err, "Consumer error: %s",
                            rd_kafka_message_errstr(rkm));
                (*cntp)++;
                rd_kafka_message_destroy(rkm);
        }
}

/**
 * @brief Consume messages until the broker throttles the consumer for at least
 *        \p min_ms.
 *
 * The consumer is throttled when it returns.
 *
 * @param cntp The number of messages consumed so far, is updated.
 * @param t0 is set to the time at which the throttle was received, \p ms to
 *           the throttle time and \p broker to the id of the broker.
 */
static void consume_until_throttled(rd_kafka_t *rk,
                                    int *cntp,
                                    int min_ms,
                                    int64_t *t0,
                                    int *ms,
                                    int *broker) {
        int64_t until = test_clock() + 180 * 1000 * 1000;

        while (test_clock() < until) {
                TEST_ASSERT(*cntp < TOPIC_MSGS,
                            "The consumer read all the %d messages and was "
                            "not throttled for %dms, is the quota of client "
                            "id %s set?",
                            TOPIC_MSGS, min_ms, QUOTA_CLIENT_ID);

                if (events_find_throttle(min_ms, t0, ms, broker)) {
                        TEST_SAY("Broker %d throttled the consumer for %dms\n",
                                 *broker, *ms);
                        return;
                }
                consumer_poll_until(rk, test_clock() + 100 * 1000, cntp);
        }

        TEST_FAIL("The consumer was not throttled for %dms", min_ms);
}

/**
 * @brief Check that the next FetchRequest to a broker after it throttled the
 *        consumer was not sent before the throttle ended, for every throttle,
 *        and that one was sent to \p broker after the throttle received at
 *        \p t0 for \p ms had ended, the consumer is still reading.
 *
 * Only one FetchRequest is in flight per broker, so the first one that is
 * sent after a throttle is received is the one to look at: a client that does
 * not hold it back sends it at once, and then nothing until its response,
 * that the broker has muted, comes. Looking for requests in the middle of the
 * throttle would not see it.
 */
static void check_held_back(int64_t t0, int ms, int broker) {
        struct events *ev    = events_copy();
        int64_t end          = t0 + (int64_t)ms * 1000;
        rd_bool_t sent_after = rd_false;
        int i, j;

        for (i = 0; i < ev->throttle_cnt; i++) {
                int64_t throttle_end =
                    ev->throttle_ts[i] + (int64_t)ev->throttle_ms[i] * 1000;

                for (j = 0; j < ev->sent_cnt; j++) {
                        if (ev->sent_broker[j] != ev->throttle_broker[i] ||
                            ev->sent_ts[j] < ev->throttle_ts[i])
                                continue;

                        /* The first FetchRequest after the throttle */
                        TEST_ASSERT(
                            ev->sent_ts[j] + SLACK_US >= throttle_end,
                            "The first FetchRequest to broker %d after it "
                            "throttled the consumer for %dms was sent "
                            "%" PRId64 "ms later",
                            ev->sent_broker[j], ev->throttle_ms[i],
                            (ev->sent_ts[j] - ev->throttle_ts[i]) / 1000);
                        break;
                }
        }

        for (j = 0; j < ev->sent_cnt; j++) {
                if (ev->sent_broker[j] == broker &&
                    ev->sent_ts[j] + SLACK_US >= end) {
                        sent_after = rd_true;
                        break;
                }
        }
        TEST_ASSERT(sent_after,
                    "No FetchRequest was sent to broker %d after the "
                    "throttle of %dms, the consumer was held back for longer",
                    broker, ms);

        rd_free(ev);
}


/**
 * @brief Set the consumer_byte_rate quota of QUOTA_CLIENT_ID, or remove it.
 *
 * A failed test leaves the quota set, the next run replaces it.
 */
static void quota_set(rd_bool_t set) {
        const char *brokers = test_getenv("BROKERS", NULL);

        if (set)
                test_kafka_cmd(
                    "kafka-configs.sh --bootstrap-server %s --alter "
                    "--add-config consumer_byte_rate=%d "
                    "--entity-type clients --entity-name %s",
                    brokers, QUOTA_BYTES_PER_SEC, QUOTA_CLIENT_ID);
        else
                test_kafka_cmd(
                    "kafka-configs.sh --bootstrap-server %s --alter "
                    "--delete-config consumer_byte_rate "
                    "--entity-type clients --entity-name %s",
                    brokers, QUOTA_CLIENT_ID);
}


/**
 * @brief The consumer does not send FetchRequests while it is throttled, and
 *        sends them again when the throttle has passed.
 */
static void do_test_held_back(void) {
        const char *topic = test_mk_topic_name("0196_held_back", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int cnt = 0, ms, broker;
        int64_t t0;

        SUB_TEST();

        topic_create(topic);
        events_reset();
        rk = consumer_new(topic, &interceptor, NULL);
        test_consumer_assign_partition("consume", rk, topic, 0,
                                       RD_KAFKA_OFFSET_BEGINNING);

        consume_until_throttled(rk, &cnt, THROTTLE_MIN_MS, &t0, &ms, &broker);

        /* Until after the throttle, and some time more to see the consumer
         * fetch again. */
        consumer_poll_until(rk, t0 + (int64_t)ms * 1000 + 1500 * 1000, &cnt);

        check_held_back(t0, ms, broker);

        consumer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


/**
 * @brief The requests that are not held back are sent to the connection that
 *        the broker has muted, and don't time out while it is.
 *
 * rd_kafka_query_watermark_offsets() sends two ListOffsetsRequests, one
 * right after the other, to the leader of the partition.
 */
static void do_test_other_requests_dont_time_out(void) {
        const char *topic = test_mk_topic_name("0196_other_requests", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int cnt = 0, ms, broker;
        int64_t t0, start, end, lo, hi;
        char socket_timeout[16];
        rd_kafka_resp_err_t err;

        SUB_TEST();

        topic_create(topic);
        events_reset();
        rd_snprintf(socket_timeout, sizeof(socket_timeout), "%d",
                    SOCKET_TIMEOUT_MS);
        rk = consumer_new(topic, &interceptor, "socket.timeout.ms",
                          socket_timeout, NULL);
        test_consumer_assign_partition("consume", rk, topic, 0,
                                       RD_KAFKA_OFFSET_BEGINNING);

        consume_until_throttled(rk, &cnt, THROTTLE_LONG_MS, &t0, &ms, &broker);

        /* The throttle must outlast socket.timeout.ms by far */
        start = test_clock();
        TEST_ASSERT(t0 + (int64_t)ms * 1000 - start >
                        (int64_t)(SOCKET_TIMEOUT_MS + 500) * 1000,
                    "Only %" PRId64 "ms of the throttle are left",
                    (t0 + (int64_t)ms * 1000 - start) / 1000);

        err =
            rd_kafka_query_watermark_offsets(rk, topic, 0, &lo, &hi, 60 * 1000);
        end = test_clock();
        TEST_SAY("The query took %" PRId64 "ms: %s\n", (end - start) / 1000,
                 rd_kafka_err2str(err));
        TEST_ASSERT(!err, "The query failed: %s", rd_kafka_err2str(err));
        TEST_ASSERT(hi == TOPIC_MSGS,
                    "Expected the high watermark %d, got %" PRId64, TOPIC_MSGS,
                    hi);
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());
        TEST_ASSERT(end + SLACK_US >= t0 + (int64_t)ms * 1000,
                    "The query returned %" PRId64
                    "ms before the end of the throttle of %dms, the broker "
                    "has not muted the connection",
                    (t0 + (int64_t)ms * 1000 - end) / 1000, ms);

        consumer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


/**
 * @brief A consumer of a group, with the group protocol of the test run, is
 *        held back as well, the group requests go to the group coordinator.
 */
static void do_test_subscribed(void) {
        const char *topic = test_mk_topic_name("0196_subscribed", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        int cnt = 0, ms, broker;
        int64_t t0;

        SUB_TEST("group.protocol=%s", test_consumer_group_protocol_classic()
                                          ? "classic"
                                          : "consumer");

        topic_create(topic);
        events_reset();
        rk = consumer_new(topic, &interceptor, NULL);
        test_consumer_subscribe(rk, topic);

        consume_until_throttled(rk, &cnt, THROTTLE_MIN_MS, &t0, &ms, &broker);
        consumer_poll_until(rk, t0 + (int64_t)ms * 1000 + 1500 * 1000, &cnt);

        check_held_back(t0, ms, broker);

        consumer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


int main_0196_fetch_throttle(int argc, char **argv) {
#ifdef _WIN32
        TEST_SKIP("The Kafka scripts can't be run on Windows\n");
        return 0;
#endif
        if (test_needs_auth()) {
                TEST_SKIP("The Kafka scripts need a PLAINTEXT cluster\n");
                return 0;
        }

        if (!test_getenv("KAFKA_PATH", NULL) || !test_getenv("BROKERS", NULL)) {
                TEST_SKIP(
                    "KAFKA_PATH and BROKERS are needed to set a client "
                    "quota\n");
                return 0;
        }

        mtx_init(&events_lock, mtx_plain);

        quota_set(rd_true);

        do_test_held_back();
        do_test_other_requests_dont_time_out();
        do_test_subscribed();

        quota_set(rd_false);

        return 0;
}
