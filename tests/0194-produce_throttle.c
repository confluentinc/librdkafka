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
 * @name KIP-219: a producer does not send ProduceRequests to a broker while
 *       the broker throttles the connection, with a real broker.
 *
 * The broker throttles a producer that exceeds its client quota: it answers
 * right away with the throttle time and then stops reading from the
 * connection for that time.
 *
 * The producers of these tests have the client id QUOTA_CLIENT_ID, and the
 * producer_byte_rate quota of that client id is set with kafka-configs.sh
 * while the tests run. That is why KAFKA_PATH and BROKERS are needed, trivup
 * sets them.
 *
 * The broker computes the throttle time. The tests produce bursts of messages
 * until it is long enough, whatever the quota window has accumulated so far,
 * and look at what the client does from its debug log: when it sent each
 * ProduceRequest, and when the broker throttled it.
 *
 * The implementation is partial: for a producer only the ProduceRequests are
 * held back, the other request types are still sent to the connection that
 * the broker has muted. Their timeouts are extended while the broker throttles,
 * instead of the requests being held back as well.
 */

/** The producers of the tests, and the quota are set for this client id. */
#define QUOTA_CLIENT_ID "librdkafka-0194"

/** The producer_byte_rate quota */
#define QUOTA_BYTES_PER_SEC 5000

#define MSG_SIZE 1000

/** Messages that are produced at once, together they exceed the quota. */
#define BURST_MSGS 10

/** Messages that are produced while the producer is throttled. */
#define HELD_MSGS 5

/** The throttle time that the tests wait for. */
#define THROTTLE_MIN_MS 1500

/** A throttle that is much longer than socket.timeout.ms */
#define THROTTLE_LONG_MS  2500
#define SOCKET_TIMEOUT_MS 1000

#define MSGS_MAX   512
#define EVENTS_MAX 1024

/** Margin for the clocks of the log callback and of the test. */
#define SLACK_US (150 * 1000)

/**
 * A ProduceRequest that was created just before the throttle was received
 * can be sent after it.
 */
#define START_MARGIN_US (100 * 1000)

/** What the client logged */
struct events {
        int64_t sent_ts[EVENTS_MAX]; /**< When each ProduceRequest was sent */
        int sent_broker[EVENTS_MAX]; /**< and the id of the broker */
        int sent_cnt;
        int64_t throttle_ts[EVENTS_MAX]; /**< When a ProduceResponse throttled
                                          *   the producer */
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

/** The value of the messages */
static char payload[MSG_SIZE];

/** Result of a produced message */
struct msgres {
        volatile int done;
        rd_kafka_resp_err_t err;
        int64_t offset;
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
        const char *sent      = "Sent ProduceRequest (v";
        const char *throttled = "Throttled for ";
        const char *p;

        mtx_lock(&events_lock);
        if (!strcmp(fac, "REQTMOUT")) {
                events.reqtmout_cnt++;
        } else if ((p = strstr(buf, sent)) && events.sent_cnt < EVENTS_MAX) {
                events.sent_broker[events.sent_cnt] = log_broker_id(buf, p);
                events.sent_ts[events.sent_cnt]     = test_clock();
                events.sent_cnt++;
        } else if ((p = strstr(buf, throttled)) &&
                   strstr(p, "ProduceResponse") &&
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
 * @brief Find a ProduceResponse that throttled the producer for at least
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


static void
dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        struct msgres *res = rkmessage->_private;

        res->err    = rkmessage->err;
        res->offset = rkmessage->offset;
        res->done   = 1;
}

/**
 * @brief Create a producer with the \p ... configuration, a NULL terminated
 *        list of property names and values.
 *
 * It has the client id QUOTA_CLIENT_ID, and logs the requests and the
 * throttles. Destroy it with producer_destroy().
 */
static rd_kafka_t *producer_new(test_conf_log_interceptor_t **interceptorp,
                                ...) {
        const char *debug_contexts[] = {"protocol", "broker", NULL};
        rd_kafka_conf_t *conf;
        va_list ap;
        const char *name;

        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "client.id", QUOTA_CLIENT_ID);
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
static void produce_msg(rd_kafka_t *rk, const char *topic, struct msgres *res) {
        memset(res, 0, sizeof(*res));
        TEST_CALL_ERR__(rd_kafka_producev(
            rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(0),
            RD_KAFKA_V_VALUE(payload, sizeof(payload)), RD_KAFKA_V_OPAQUE(res),
            RD_KAFKA_V_END));
}

/**
 * @brief Produce bursts of messages, and wait for their delivery, until the
 *        broker throttles the producer for at least \p min_ms.
 *
 * The producer is throttled when it returns.
 *
 * @param cntp The number of messages produced so far, with the results in
 *             \p res, is updated.
 * @param t0 is set to the time at which the throttle was received, \p ms to
 *           the throttle time and \p broker to the id of the broker.
 */
static void produce_until_throttled(rd_kafka_t *rk,
                                    const char *topic,
                                    struct msgres *res,
                                    int *cntp,
                                    int min_ms,
                                    int64_t *t0,
                                    int *ms,
                                    int *broker) {
        int64_t until = test_clock() + 120 * 1000 * 1000;

        while (test_clock() < until) {
                int i;

                TEST_ASSERT(*cntp + BURST_MSGS <= MSGS_MAX,
                            "More than %d messages are needed", MSGS_MAX);

                for (i = 0; i < BURST_MSGS; i++)
                        produce_msg(rk, topic, &res[(*cntp)++]);
                test_flush(rk, 120 * 1000);

                if (events_find_throttle(min_ms, t0, ms, broker)) {
                        TEST_SAY("Broker %d throttled the producer for %dms\n",
                                 *broker, *ms);
                        return;
                }
        }

        TEST_FAIL(
            "The producer was not throttled for %dms, is the quota of "
            "client id %s set?",
            min_ms, QUOTA_CLIENT_ID);
}

/**
 * @brief Produce HELD_MSGS messages while the producer is throttled, since the
 *        throttle was received at \p t0.
 *
 * They are produced after START_MARGIN_US, otherwise a ProduceRequest that is
 * sent for them right away could not be told from one that was created
 * before the throttle was received.
 */
static void produce_while_throttled(rd_kafka_t *rk,
                                    const char *topic,
                                    struct msgres *res,
                                    int *cntp,
                                    int64_t t0) {
        int64_t remains = t0 + 2 * START_MARGIN_US - test_clock();
        int i;

        if (remains > 0)
                rd_usleep(remains, NULL);

        for (i = 0; i < HELD_MSGS; i++)
                produce_msg(rk, topic, &res[(*cntp)++]);
}

/**
 * @brief Check that the first \p cnt messages were delivered.
 *
 * @param contiguous If set, also that they have the offsets 0 to \p cnt - 1,
 *                   so none was lost, duplicated or reordered.
 */
static void
check_delivered(const struct msgres *res, int cnt, rd_bool_t contiguous) {
        int i;

        for (i = 0; i < cnt; i++) {
                TEST_ASSERT(res[i].done, "Message %d has no delivery report",
                            i);
                TEST_ASSERT(!res[i].err, "Message %d was not delivered: %s", i,
                            rd_kafka_err2str(res[i].err));
                TEST_ASSERT(!contiguous || res[i].offset == i,
                            "Message %d was delivered with offset %" PRId64, i,
                            res[i].offset);
        }
}

/**
 * @brief Check that no ProduceRequest was sent to a broker while it throttled
 *        the producer, and that some were sent when the throttle of
 *        \p broker received at \p t0 for \p ms had ended, the messages that
 *        were held back.
 */
static void check_held_back(int64_t t0, int ms, int broker) {
        struct events *ev = events_copy();
        int64_t end       = t0 + (int64_t)ms * 1000;
        int sent_after    = 0;
        int i, j;

        for (i = 0; i < ev->throttle_cnt; i++) {
                int64_t from = ev->throttle_ts[i] + START_MARGIN_US;
                int64_t to   = ev->throttle_ts[i] +
                             (int64_t)ev->throttle_ms[i] * 1000 - SLACK_US;

                for (j = 0; j < ev->sent_cnt; j++) {
                        if (ev->sent_broker[j] != ev->throttle_broker[i])
                                continue;
                        TEST_ASSERT(
                            ev->sent_ts[j] <= from || ev->sent_ts[j] >= to,
                            "A ProduceRequest was sent to broker %d "
                            "%" PRId64
                            "ms after it throttled the producer for "
                            "%dms",
                            ev->sent_broker[j],
                            (ev->sent_ts[j] - ev->throttle_ts[i]) / 1000,
                            ev->throttle_ms[i]);
                }
        }

        for (j = 0; j < ev->sent_cnt; j++)
                if (ev->sent_broker[j] == broker &&
                    ev->sent_ts[j] + SLACK_US >= end)
                        sent_after++;
        TEST_ASSERT(sent_after > 0,
                    "No ProduceRequest was sent to broker %d after the "
                    "throttle of %dms, the messages produced while it lasted "
                    "were not held back",
                    broker, ms);

        rd_free(ev);
}


/**
 * @brief Set the producer_byte_rate quota of QUOTA_CLIENT_ID, or remove it.
 *
 * A failed test leaves the quota set, the next run replaces it.
 */
static void quota_set(rd_bool_t set) {
        const char *brokers = test_getenv("BROKERS", NULL);

        if (set)
                test_kafka_cmd(
                    "kafka-configs.sh --bootstrap-server %s --alter "
                    "--add-config producer_byte_rate=%d "
                    "--entity-type clients --entity-name %s",
                    brokers, QUOTA_BYTES_PER_SEC, QUOTA_CLIENT_ID);
        else
                test_kafka_cmd(
                    "kafka-configs.sh --bootstrap-server %s --alter "
                    "--delete-config producer_byte_rate "
                    "--entity-type clients --entity-name %s",
                    brokers, QUOTA_CLIENT_ID);
}


/**
 * @brief The producer does not send ProduceRequests while it is throttled.
 */
static void do_test_held_back(void) {
        const char *topic = test_mk_topic_name("0194_held_back", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres res[MSGS_MAX];
        int cnt = 0, ms, broker;
        int64_t t0;

        SUB_TEST();

        test_create_topic_wait_exists(NULL, topic, 1, 1, 30 * 1000);
        events_reset();
        rk = producer_new(&interceptor, NULL);

        produce_until_throttled(rk, topic, res, &cnt, THROTTLE_MIN_MS, &t0, &ms,
                                &broker);

        produce_while_throttled(rk, topic, res, &cnt, t0);
        test_flush(rk, 120 * 1000);

        check_delivered(res, cnt, rd_false);
        check_held_back(t0, ms, broker);

        producer_destroy(rk, interceptor);

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
        const char *topic = test_mk_topic_name("0194_other_requests", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres res[MSGS_MAX];
        int cnt = 0, ms, broker;
        int64_t t0, start, end, lo, hi;
        char socket_timeout[16];
        rd_kafka_resp_err_t err;

        SUB_TEST();

        test_create_topic_wait_exists(NULL, topic, 1, 1, 30 * 1000);
        events_reset();
        rd_snprintf(socket_timeout, sizeof(socket_timeout), "%d",
                    SOCKET_TIMEOUT_MS);
        rk = producer_new(&interceptor, "socket.timeout.ms", socket_timeout,
                          NULL);

        produce_until_throttled(rk, topic, res, &cnt, THROTTLE_LONG_MS, &t0,
                                &ms, &broker);

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
        TEST_ASSERT(hi == cnt, "Expected the high watermark %d, got %" PRId64,
                    cnt, hi);
        TEST_ASSERT(events_reqtmout_cnt() == 0, "%d request(s) timed out",
                    events_reqtmout_cnt());
        TEST_ASSERT(end + SLACK_US >= t0 + (int64_t)ms * 1000,
                    "The query returned %" PRId64
                    "ms before the end of the throttle of %dms, the broker "
                    "has not muted the connection",
                    (t0 + (int64_t)ms * 1000 - end) / 1000, ms);

        producer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


/**
 * @brief An idempotent producer delivers all messages, in order and without
 *        duplicates, when it is throttled. The broker checks the sequence
 *        numbers.
 */
static void do_test_idempotent(void) {
        const char *topic = test_mk_topic_name("0194_idempotent", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres res[MSGS_MAX];
        int cnt = 0, ms, broker;
        int64_t t0;

        SUB_TEST();

        test_create_topic_wait_exists(NULL, topic, 1, 1, 30 * 1000);
        events_reset();
        rk = producer_new(&interceptor, "enable.idempotence", "true", NULL);

        produce_until_throttled(rk, topic, res, &cnt, THROTTLE_MIN_MS, &t0, &ms,
                                &broker);
        produce_while_throttled(rk, topic, res, &cnt, t0);
        test_flush(rk, 120 * 1000);

        check_delivered(res, cnt, rd_true);
        check_held_back(t0, ms, broker);

        producer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


/**
 * @returns the number of messages that a read_committed consumer reads from
 *          partition 0 of \p topic.
 */
static int consume_committed(const char *topic) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int cnt       = 0;
        int64_t until = test_clock() + 30 * 1000 * 1000;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "isolation.level", "read_committed");
        test_conf_set(conf, "enable.partition.eof", "true");
        rk = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_assign_partition("consume", rk, topic, 0,
                                       RD_KAFKA_OFFSET_BEGINNING);

        while (test_clock() < until) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 1000);
                rd_bool_t eof;

                if (!rkm)
                        continue;
                eof = rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF;
                TEST_ASSERT(eof || !rkm->err, "Consume error: %s",
                            rd_kafka_message_errstr(rkm));
                if (!eof)
                        cnt++;
                rd_kafka_message_destroy(rkm);
                if (eof)
                        break;
        }

        test_consumer_close(rk);
        rd_kafka_destroy(rk);

        return cnt;
}

/**
 * @brief A transactional producer can commit a transaction whose messages
 *        were held back, and a read_committed consumer reads all of them.
 */
static void do_test_transactional(void) {
        const char *topic = test_mk_topic_name("0194_transactional", 1);
        test_conf_log_interceptor_t *interceptor;
        rd_kafka_t *rk;
        struct msgres res[MSGS_MAX];
        int cnt = 0, ms, broker, consumed;
        int64_t t0;

        SUB_TEST();

        test_create_topic_wait_exists(NULL, topic, 1, 1, 30 * 1000);
        events_reset();
        rk = producer_new(&interceptor, "transactional.id", topic, NULL);

        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, 30 * 1000));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

        produce_until_throttled(rk, topic, res, &cnt, THROTTLE_MIN_MS, &t0, &ms,
                                &broker);
        produce_while_throttled(rk, topic, res, &cnt, t0);

        TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 120 * 1000));

        check_delivered(res, cnt, rd_true);
        check_held_back(t0, ms, broker);

        consumed = consume_committed(topic);
        TEST_ASSERT(consumed == cnt,
                    "Expected a read_committed consumer to read the %d "
                    "messages of the transaction, got %d",
                    cnt, consumed);

        producer_destroy(rk, interceptor);

        SUB_TEST_PASS();
}


int main_0194_produce_throttle(int argc, char **argv) {
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
        memset(payload, 'x', sizeof(payload));

        quota_set(rd_true);

        do_test_held_back();
        do_test_other_requests_dont_time_out();
        do_test_idempotent();
        do_test_transactional();

        quota_set(rd_false);

        return 0;
}
