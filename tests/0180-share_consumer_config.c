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

/**
 * @brief Verify that a particular conf shape is rejected by
 *        rd_kafka_share_consumer_new.
 *
 * The setter callback is applied to a fresh conf to produce the
 * case-under-test (e.g. set rebalance_cb, set events). The helper
 * then calls rd_kafka_share_consumer_new and asserts the call returns
 * NULL with an errstr containing expected_substr.
 */
static void
verify_share_consumer_conf_set_rejected(const char *case_name,
                                        void (*conf_setter)(rd_kafka_conf_t *),
                                        const char *expected_substr) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];

        conf = rd_kafka_conf_new();
        conf_setter(conf);
        errstr[0] = '\0';
        rkshare   = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare == NULL,
                    "[%s] expected NULL share consumer, got non-NULL",
                    case_name);
        TEST_ASSERT(strstr(errstr, expected_substr) != NULL,
                    "[%s] errstr should mention '%s', got: %s", case_name,
                    expected_substr, errstr);
        TEST_SAY("[%s] rejected with: %s\n", case_name, errstr);
        rd_kafka_conf_destroy(conf);
}

/**
 * @brief Verify that setting prop_name to value (via the generic
 *        rd_kafka_conf_set string interface) causes
 *        rd_kafka_share_consumer_new to fail with an errstr that
 *        mentions prop_name.
 */
static void verify_share_consumer_conf_prop_rejected(const char *prop_name,
                                                     const char *value) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];
        rd_kafka_conf_res_t res;

        conf = rd_kafka_conf_new();
        res = rd_kafka_conf_set(conf, prop_name, value, errstr, sizeof(errstr));
        TEST_ASSERT(res == RD_KAFKA_CONF_OK,
                    "[%s=%s] precondition rd_kafka_conf_set failed: %s",
                    prop_name, value, errstr);
        errstr[0] = '\0';
        rkshare   = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare == NULL,
                    "[%s=%s] expected NULL share consumer, got non-NULL",
                    prop_name, value);
        TEST_ASSERT(strstr(errstr, prop_name) != NULL,
                    "[%s=%s] errstr should mention '%s', got: %s", prop_name,
                    value, prop_name, errstr);
        TEST_SAY("[%s=%s] rejected with: %s\n", prop_name, value, errstr);
        rd_kafka_conf_destroy(conf);
}

/* Unused stub used only as a non-NULL function-pointer value for
 * rd_kafka_conf_set_rebalance_cb in the rejection test. */
static void unused_rebalance_cb(rd_kafka_t *rk,
                                rd_kafka_resp_err_t err,
                                rd_kafka_topic_partition_list_t *parts,
                                void *opaque) {
}

static void setter_rebalance_cb(rd_kafka_conf_t *conf) {
        rd_kafka_conf_set_rebalance_cb(conf, unused_rebalance_cb);
}

static void setter_event_rebalance(rd_kafka_conf_t *conf) {
        rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_REBALANCE);
}

/**
 * @brief Share consumer has no rebalance callback semantics; the
 *        factory rejects rebalance_cb at construction so an app's
 *        handler can never silently never-fire.
 */
static void test_rebalance_cb_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_set_rejected(
            "rebalance_cb set", setter_rebalance_cb, "rebalance_cb");

        SUB_TEST_PASS();
}

/**
 * @brief Same reasoning as rebalance_cb — the event-mask form of
 *        opting into rebalance delivery is also rejected.
 */
static void test_event_rebalance_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_set_rejected("RD_KAFKA_EVENT_REBALANCE set",
                                                setter_event_rebalance,
                                                "RD_KAFKA_EVENT_REBALANCE");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer does not emit periodic stats; the
 *        statistics.interval.ms property is rejected so the user
 *        cannot enable stats emission. (stats_cb alone is inert with
 *        interval=0 and is not rejected — see the TODO in
 *        rdkafka_conf.c.)
 */
static void test_statistics_interval_ms_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("statistics.interval.ms",
                                                 "1000");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer locks enable.auto.commit to false internally.
 *        Any explicit set by the app (true or false) is rejected so
 *        the value can only come from the library default.
 */
static void test_enable_auto_commit_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("enable.auto.commit", "true");
        verify_share_consumer_conf_prop_rejected("enable.auto.commit", "false");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer requires group.protocol=consumer (forced
 *        internally); any explicit set by the app — even to the
 *        same value — is rejected so the value can only come from
 *        the library.
 */
static void test_group_protocol_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("group.protocol", "consumer");
        verify_share_consumer_conf_prop_rejected("group.protocol", "classic");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer forces socket.max.fails=1 to keep the broker
 *        share session in sync; any explicit set by the app is
 *        rejected.
 */
static void test_socket_max_fails_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("socket.max.fails", "1");
        verify_share_consumer_conf_prop_rejected("socket.max.fails", "5");

        SUB_TEST_PASS();
}

/**
 * @brief Offset-reset for share consumer is a broker-side share-
 *        group property (`share.auto.offset.reset`); the client
 *        `auto.offset.reset` is not used. Any explicit set on the
 *        client is rejected.
 */
static void test_auto_offset_reset_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("auto.offset.reset",
                                                 "earliest");
        verify_share_consumer_conf_prop_rejected("auto.offset.reset", "latest");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer forces group.protocol=consumer, so the
 *        downstream consumer-protocol validation rejects
 *        session.timeout.ms (defined broker side). Verify the
 *        rejection surfaces for share consumer too.
 */
static void test_session_timeout_ms_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("session.timeout.ms", "30000");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer forces group.protocol=consumer, so the
 *        downstream consumer-protocol validation rejects
 *        partition.assignment.strategy. Verify the rejection
 *        surfaces for share consumer too.
 */
static void test_partition_assignment_strategy_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected(
            "partition.assignment.strategy", "range");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer forces group.protocol=consumer, so the
 *        downstream consumer-protocol validation rejects
 *        group.protocol.type. Verify the rejection surfaces for
 *        share consumer too.
 */
static void test_group_protocol_type_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("group.protocol.type",
                                                 "consumer");

        SUB_TEST_PASS();
}

/**
 * @brief Share consumer forces group.protocol=consumer, so the
 *        downstream consumer-protocol validation rejects
 *        heartbeat.interval.ms (defined broker side). Verify the
 *        rejection surfaces for share consumer too.
 */
static void test_heartbeat_interval_ms_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("heartbeat.interval.ms",
                                                 "3000");

        SUB_TEST_PASS();
}


/**
 * @brief Build a producer with linger.ms=0 so each rd_kafka_produce()
 *        flushes as its own broker batch.
 */
static rd_kafka_t *create_no_linger_producer(void) {
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        TEST_ASSERT(rd_kafka_conf_set(conf, "linger.ms", "0", errstr,
                                      sizeof(errstr)) == RD_KAFKA_CONF_OK,
                    "linger.ms=0: %s", errstr);
        return test_create_handle(RD_KAFKA_PRODUCER, conf);
}


/**
 * @brief Build a share consumer with the given max.poll.records value.
 *
 * Consumer uses implicit ack mode (default) and auto-offset-reset is
 * left to be set group-side by the caller.
 */
static rd_kafka_share_t *
create_share_consumer_with_max_poll(const char *group_id, int max_poll) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];
        char val[32];

        test_conf_init(&conf, NULL, 0);
        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_snprintf(val, sizeof(val), "%d", max_poll);
        TEST_ASSERT(rd_kafka_conf_set(conf, "max.poll.records", val, errstr,
                                      sizeof(errstr)) == RD_KAFKA_CONF_OK,
                    "max.poll.records=%d: %s", max_poll, errstr);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);
        return rkshare;
}


/**
 * @brief Produce \p msgcnt records to \p topic, partition 0, with
 *        \p gap_ms between each produce call. With linger.ms=0 on the
 *        producer this lands each record as its own broker batch.
 */
static void produce_one_per_batch(rd_kafka_t *producer,
                                  const char *topic,
                                  int msgcnt,
                                  int gap_ms) {
        rd_kafka_topic_t *rkt;
        rd_kafka_resp_err_t err;
        char payload[64];
        int i;

        rkt = rd_kafka_topic_new(producer, topic, NULL);
        TEST_ASSERT(rkt, "topic_new(%s) failed: %s", topic,
                    rd_kafka_err2str(rd_kafka_last_error()));

        for (i = 0; i < msgcnt; i++) {
                rd_snprintf(payload, sizeof(payload), "msg-%d", i);
                if (rd_kafka_produce(rkt, 0, RD_KAFKA_MSG_F_COPY, payload,
                                     strlen(payload), NULL, 0, NULL) == -1)
                        TEST_FAIL("produce #%d failed: %s", i,
                                  rd_kafka_err2str(rd_kafka_last_error()));

                err = rd_kafka_flush(producer, 30 * 1000);
                TEST_ASSERT(!err, "flush after produce #%d: %s", i,
                            rd_kafka_err2str(err));

                if (i + 1 < msgcnt)
                        rd_usleep(gap_ms * 1000, NULL);
        }

        rd_kafka_topic_destroy(rkt);
}


/**
 * @brief Consume \p target records and record how many records came in
 *        each batch returned by rd_kafka_share_consume_batch().
 *
 * @returns Number of batches it took to reach \p target records.
 *          Out-param \p batch_sizes is filled with each batch's
 *          record count, up to \p batch_sizes_cap entries.
 */
static int consume_record_batches(rd_kafka_share_t *rkshare,
                                  int target,
                                  int *batch_sizes,
                                  int batch_sizes_cap,
                                  int per_call_timeout_ms,
                                  int max_calls) {
        rd_kafka_message_t *rkmessages[1024];
        size_t rcvd;
        size_t j;
        rd_kafka_error_t *error;
        int batches = 0;
        int got     = 0;
        int call;

        for (call = 0; call < max_calls && got < target; call++) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(
                    rkshare, per_call_timeout_ms, rkmessages, &rcvd);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd == 0)
                        continue;

                if (batches < batch_sizes_cap)
                        batch_sizes[batches] = (int)rcvd;
                batches++;

                for (j = 0; j < rcvd; j++) {
                        if (!rkmessages[j]->err)
                                got++;
                        rd_kafka_message_destroy(rkmessages[j]);
                }
        }
        return batches;
}


/**
 * @brief Verify max.poll.records=5 splits 10 single-record broker
 *        batches into 2 consume_batch() returns of ~5 records each.
 *
 * Setup: producer with linger.ms=0 emits 10 records with 500ms gap, so
 * each lands as its own broker batch. Consumer with
 * max.poll.records=5: across all consume_batch() calls, no single call
 * returns more than 5 records and the 10 records are drained in 2 or
 * more batches (the cap is the upper bound the lib must respect).
 */
static void test_max_poll_records_caps_batch_at_5(void) {
        const char *topic;
        const char *group = "0180-max-poll-records-5";
        rd_kafka_t *producer;
        rd_kafka_share_t *rkshare;
        const int msgcnt    = 10;
        const int max_poll  = 5;
        int batch_sizes[32] = {0};
        int batches;
        int i;

        SUB_TEST();

        producer = create_no_linger_producer();

        topic = test_mk_topic_name("0180-max-poll-5", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        produce_one_per_batch(producer, topic, msgcnt, 500);

        rkshare = create_share_consumer_with_max_poll(group, max_poll);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        batches =
            consume_record_batches(rkshare, msgcnt, batch_sizes,
                                   (int)RD_ARRAY_SIZE(batch_sizes), 3000, 30);

        TEST_SAY("max.poll.records=%d, msgcnt=%d -> %d batch(es):", max_poll,
                 msgcnt, batches);
        for (i = 0; i < batches && i < (int)RD_ARRAY_SIZE(batch_sizes); i++)
                TEST_SAY0(" %d", batch_sizes[i]);
        TEST_SAY0("\n");

        for (i = 0; i < batches && i < (int)RD_ARRAY_SIZE(batch_sizes); i++)
                TEST_ASSERT(batch_sizes[i] <= max_poll,
                            "Batch #%d returned %d records, exceeds "
                            "max.poll.records=%d",
                            i, batch_sizes[i], max_poll);

        TEST_ASSERT(batches >= 2,
                    "Expected at least 2 batches with max.poll.records=%d "
                    "for %d records, got %d",
                    max_poll, msgcnt, batches);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        rd_kafka_destroy(producer);

        SUB_TEST_PASS();
}


/**
 * @brief Verify max.poll.records=10 drains 10 single-record broker
 *        batches in a single consume_batch() call.
 */
static void test_max_poll_records_allows_full_drain_at_10(void) {
        const char *topic;
        const char *group = "0180-max-poll-records-10";
        rd_kafka_t *producer;
        rd_kafka_share_t *rkshare;
        const int msgcnt    = 10;
        const int max_poll  = 10;
        int batch_sizes[32] = {0};
        int batches;
        int i;

        SUB_TEST();

        producer = create_no_linger_producer();

        topic = test_mk_topic_name("0180-max-poll-10", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        produce_one_per_batch(producer, topic, msgcnt, 500);

        rkshare = create_share_consumer_with_max_poll(group, max_poll);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        batches =
            consume_record_batches(rkshare, msgcnt, batch_sizes,
                                   (int)RD_ARRAY_SIZE(batch_sizes), 5000, 30);

        TEST_SAY("max.poll.records=%d, msgcnt=%d -> %d batch(es):", max_poll,
                 msgcnt, batches);
        for (i = 0; i < batches && i < (int)RD_ARRAY_SIZE(batch_sizes); i++)
                TEST_SAY0(" %d", batch_sizes[i]);
        TEST_SAY0("\n");

        {
                int sum = 0;
                for (i = 0; i < batches && i < (int)RD_ARRAY_SIZE(batch_sizes);
                     i++) {
                        TEST_ASSERT(batch_sizes[i] <= max_poll,
                                    "Batch #%d returned %d records, "
                                    "exceeds max.poll.records=%d",
                                    i, batch_sizes[i], max_poll);
                        sum += batch_sizes[i];
                }
                TEST_ASSERT(sum == msgcnt,
                            "Expected %d total records across batches, "
                            "got %d",
                            msgcnt, sum);
        }

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        rd_kafka_destroy(producer);

        SUB_TEST_PASS();
}


/* Behavioural tests that require a real broker. */
int main_0180_share_consumer_config(int argc, char **argv) {
        test_max_poll_records_caps_batch_at_5();
        test_max_poll_records_allows_full_drain_at_10();
        return 0;
}


/* Construction-time conf-rejection tests; no broker required. */
int main_0180_share_consumer_config_local(int argc, char **argv) {
        test_rebalance_cb_rejected_at_construction();
        test_event_rebalance_rejected_at_construction();
        test_statistics_interval_ms_rejected_at_construction();
        test_enable_auto_commit_rejected_at_construction();
        test_group_protocol_rejected_at_construction();
        test_socket_max_fails_rejected_at_construction();
        test_auto_offset_reset_rejected_at_construction();
        test_session_timeout_ms_rejected_at_construction();
        test_partition_assignment_strategy_rejected_at_construction();
        test_group_protocol_type_rejected_at_construction();
        test_heartbeat_interval_ms_rejected_at_construction();
        return 0;
}
