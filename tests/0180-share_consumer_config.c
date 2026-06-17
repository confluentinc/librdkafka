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

/**
 * @brief Verify that creating a REGULAR (non-share) consumer with
 *        prop_name=value fails at construction (rd_kafka_new returns
 *        NULL) with an errstr that mentions prop_name.
 *
 * group.id is set so the consumer reaches the consumer-specific
 * finalize validation.
 */
static void verify_regular_consumer_conf_prop_rejected(const char *prop_name,
                                                       const char *value) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];

        conf = rd_kafka_conf_new();
        TEST_ASSERT(rd_kafka_conf_set(conf, "group.id", "0180-regular", errstr,
                                      sizeof(errstr)) == RD_KAFKA_CONF_OK,
                    "group.id: %s", errstr);
        TEST_ASSERT(rd_kafka_conf_set(conf, prop_name, value, errstr,
                                      sizeof(errstr)) == RD_KAFKA_CONF_OK,
                    "[%s=%s] precondition rd_kafka_conf_set failed: %s",
                    prop_name, value, errstr);

        errstr[0] = '\0';
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk == NULL,
                    "[%s=%s] expected regular consumer creation to fail, "
                    "got non-NULL",
                    prop_name, value);
        TEST_ASSERT(strstr(errstr, prop_name) != NULL,
                    "[%s=%s] errstr should mention '%s', got: %s", prop_name,
                    value, prop_name, errstr);
        TEST_SAY("[%s=%s] regular consumer rejected with: %s\n", prop_name,
                 value, errstr);
        /* rd_kafka_new failed, so it did not take ownership of conf. */
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
 * @brief For non-share (regular) consumers, fetch.min.bytes must stay in
 *        the historical range 1..100000000. The property bounds were
 *        widened to 0..INT_MAX for share-consumer parity with the Java
 *        client; regular consumers are re-restricted at finalize, so 0
 *        and values above 100000000 (e.g. INT_MAX) are rejected.
 */
static void test_fetch_min_bytes_regular_consumer_range_rejected(void) {
        SUB_TEST_QUICK();

        /* 0 is below the regular-consumer minimum of 1. */
        verify_regular_consumer_conf_prop_rejected("fetch.min.bytes", "0");
        /* INT_MAX exceeds the regular-consumer maximum of 100000000. */
        verify_regular_consumer_conf_prop_rejected("fetch.min.bytes",
                                                   "2147483647");

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
 * @brief `receive.message.max.bytes` is forced to INT_MAX for share
 *        consumers (a ShareFetch response can exceed fetch.max.bytes,
 *        which is a soft limit); any explicit set by the app is
 *        rejected.
 */
static void test_receive_message_max_bytes_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("receive.message.max.bytes",
                                                 "100000000");

        SUB_TEST_PASS();
}

/**
 * @brief Static membership (`group.instance.id`) is not supported for
 *        share groups; the property is rejected for share consumers.
 */
static void test_group_instance_id_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("group.instance.id",
                                                 "share-instance-1");

        SUB_TEST_PASS();
}

/**
 * @brief `isolation.level` is defined broker-side for share groups; the
 *        client property is rejected for share consumers.
 */
static void test_isolation_level_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("isolation.level",
                                                 "read_committed");

        SUB_TEST_PASS();
}

/**
 * @brief Share-group assignment is broker-driven; the client
 *        `group.remote.assignor` property is rejected for share
 *        consumers.
 */
static void test_group_remote_assignor_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("group.remote.assignor",
                                                 "uniform");

        SUB_TEST_PASS();
}

/**
 * @brief `queued.min.messages` tunes the per-partition prefetch queue,
 *        which share consumers (broker-driven via max.poll.records) do
 *        not use; it is rejected for share consumers.
 */
static void test_queued_min_messages_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("queued.min.messages", "1000");

        SUB_TEST_PASS();
}

/**
 * @brief `queued.max.messages.kbytes` tunes the per-partition prefetch
 *        queue, which share consumers do not use; it is rejected for
 *        share consumers.
 */
static void test_queued_max_messages_kbytes_rejected_at_construction(void) {
        SUB_TEST_QUICK();

        verify_share_consumer_conf_prop_rejected("queued.max.messages.kbytes",
                                                 "1024");

        SUB_TEST_PASS();
}

struct idle_reconnect_counters {
        rd_atomic32_t idle_closes;
        rd_atomic32_t reconnects_after_idle;
};

/* ===================================================================
 *  Log callback for the fetch-connection idle test. Same as
 *  idle_reconnect_log_cb but filtered to the FETCH (leader) connection
 *  only: the broker name is prefixed into buf, so coordinator lines
 *  contain "GroupCoordinator" and fetch-broker lines do not.
 * =================================================================== */
static void fetch_idle_log_cb(const rd_kafka_t *rk,
                              int level,
                              const char *fac,
                              const char *buf) {
        struct idle_reconnect_counters *c = rd_kafka_opaque(rk);
        if (!c || strstr(buf, "GroupCoordinator"))
                return; /* ignore the coordinator connection */
        if (!strcmp(fac, "FAIL") &&
            strstr(buf, "Connection max idle time exceeded"))
                rd_atomic32_add(&c->idle_closes, 1);
        else if (!strcmp(fac, "CONNECT") && strstr(buf, "connecting") &&
                 rd_atomic32_get(&c->idle_closes) > 0)
                rd_atomic32_add(&c->reconnects_after_idle, 1);
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
 * @brief Build a share consumer for \p group_id, optionally setting one
 *        extra config property \p prop to \p value.
 */
static rd_kafka_share_t *create_share_consumer_with_prop(const char *group_id,
                                                         const char *prop,
                                                         int value) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        char errstr[512];
        char val[32];

        test_conf_init(&conf, NULL, 0);
        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));

        if (prop) {
                rd_snprintf(val, sizeof(val), "%d", value);
                TEST_ASSERT(rd_kafka_conf_set(conf, prop, val, errstr,
                                              sizeof(errstr)) ==
                                RD_KAFKA_CONF_OK,
                            "%s=%d: %s", prop, value, errstr);
        }

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
 *        each batch returned by rd_kafka_share_poll().
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
        rd_kafka_messages_t *batch = NULL;
        size_t rcvd;
        size_t j;
        rd_kafka_error_t *error;
        int batches = 0;
        int got     = 0;
        int call;

        for (call = 0; call < max_calls && got < target; call++) {
                error =
                    rd_kafka_share_poll(rkshare, per_call_timeout_ms, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }

                rcvd = rd_kafka_messages_count(batch);
                if (rcvd == 0) {
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                        continue;
                }

                if (batches < batch_sizes_cap)
                        batch_sizes[batches] = (int)rcvd;
                batches++;

                for (j = 0; j < rcvd; j++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, j);
                        if (!rkm->err)
                                got++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }
        return batches;
}

/**
 * @brief Drain \p msgcnt records from \p topic with a share consumer
 *        configured by \p set_fetch_max_bytes / \p fetch_max_bytes, and
 *        return how many non-empty poll batches it took.
 *
 * max.poll.records is left at the library default so that the only
 * thing capping a single poll batch is the byte limit under test.
 */
static int drain_count_with_fetch_max_bytes(const char *group,
                                            const char *topic,
                                            int msgcnt,
                                            rd_bool_t set_fetch_max_bytes,
                                            int fetch_max_bytes) {
        rd_kafka_share_t *rkshare;
        int batch_sizes[256] = {0};
        int batches;

        rkshare = create_share_consumer_with_prop(
            group, set_fetch_max_bytes ? "fetch.max.bytes" : NULL,
            fetch_max_bytes);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        batches =
            consume_record_batches(rkshare, msgcnt, batch_sizes,
                                   (int)RD_ARRAY_SIZE(batch_sizes), 3000, 200);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
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

        rkshare = create_share_consumer_with_prop(group, "max.poll.records",
                                                  max_poll);
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

        rkshare = create_share_consumer_with_prop(group, "max.poll.records",
                                                  max_poll);
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

/**
 * @brief Verify fetch.max.bytes behaves as a soft per-fetch byte limit
 *        for share consumers.
 *
 * Produces msgcnt single-record broker batches (linger.ms=0 producer
 * with a gap between produces). Then:
 *
 *  - With fetch.max.bytes UNSET (library default 50 MB): the broker can
 *    pack many batches into one ShareFetch response, so the records
 *    drain in far fewer poll calls than there are batches.
 *
 *  - With fetch.max.bytes=1: the limit is a soft cap and the broker's
 *    "return at least one batch" guarantee yields
 *    exactly one broker batch per ShareFetch response, so draining
 *    takes one poll per produced batch.
 * The two phases use distinct topics/groups so their share sessions and
 * offsets do not interfere.
 */
static void test_fetch_max_bytes_one_is_soft_limit(void) {
        const char *topic_default;
        const char *topic_one;
        const int msgcnt = 10;
        rd_kafka_t *producer;
        int drains_default;
        int drains_one;

        SUB_TEST();

        producer = create_no_linger_producer();

        /* Phase 1: default fetch.max.bytes. */
        topic_default = test_mk_topic_name("0180-fmb-default", 1);
        test_create_topic_wait_exists(NULL, topic_default, 1, -1, 60 * 1000);
        produce_one_per_batch(producer, topic_default, msgcnt, 200);

        drains_default = drain_count_with_fetch_max_bytes(
            "0180-fetch-max-bytes-default", topic_default, msgcnt,
            rd_false /*unset*/, 0);
        TEST_SAY(
            "fetch.max.bytes default: %d record(s) drained in %d poll "
            "batch(es)\n",
            msgcnt, drains_default);

        /* Phase 2: fetch.max.bytes=1. */
        topic_one = test_mk_topic_name("0180-fmb-one", 1);
        test_create_topic_wait_exists(NULL, topic_one, 1, -1, 60 * 1000);
        produce_one_per_batch(producer, topic_one, msgcnt, 200);

        drains_one = drain_count_with_fetch_max_bytes(
            "0180-fetch-max-bytes-one", topic_one, msgcnt, rd_true /*set*/, 1);
        TEST_SAY(
            "fetch.max.bytes=1: %d record(s) drained in %d poll "
            "batch(es)\n",
            msgcnt, drains_one);

        /* With fetch.max.bytes=1 the broker returns one batch per fetch,
         * so the number of poll batches equals the number of produced
         * broker batches (msgcnt single-record batches). */
        TEST_ASSERT(drains_one == msgcnt,
                    "fetch.max.bytes=1 should drain one batch per fetch: "
                    "expected %d poll batches, got %d",
                    msgcnt, drains_one);

        /* The default (large) limit must drain in strictly fewer poll
         * batches than the one-per-fetch case. */
        TEST_ASSERT(drains_default < drains_one,
                    "default fetch.max.bytes (%d poll batches) should drain "
                    "in fewer batches than fetch.max.bytes=1 (%d poll "
                    "batches)",
                    drains_default, drains_one);

        rd_kafka_destroy(producer);

        SUB_TEST_PASS();
}


/**
 * @brief Drain msgcnt records from a fresh topic with a share consumer
 *        configured with fetch.min.bytes=\p value, and assert all
 *        records are received.
 *
 * Confirms fetch.min.bytes is functional end-to-end for share consumers
 * across its range:
 *  - 0       : broker responds immediately (no long-poll).
 *  - INT_MAX : broker can never satisfy the threshold, so it responds
 *              when fetch.max.wait.ms elapses.
 * In both cases all records must still drain (matches the Java client).
 */
static void verify_share_fetch_min_bytes_drains_all(const char *group,
                                                    const char *topic_suffix,
                                                    int value) {
        const char *topic;
        const int msgcnt = 10;
        rd_kafka_t *producer;
        rd_kafka_share_t *rkshare;
        int batch_sizes[64] = {0};
        int batches;
        int i;
        int total = 0;

        producer = create_no_linger_producer();

        topic = test_mk_topic_name(topic_suffix, 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_one_per_batch(producer, topic, msgcnt, 200);

        rkshare =
            create_share_consumer_with_prop(group, "fetch.min.bytes", value);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        batches =
            consume_record_batches(rkshare, msgcnt, batch_sizes,
                                   (int)RD_ARRAY_SIZE(batch_sizes), 3000, 60);

        for (i = 0; i < batches && i < (int)RD_ARRAY_SIZE(batch_sizes); i++)
                total += batch_sizes[i];

        TEST_SAY(
            "fetch.min.bytes=%d: drained %d/%d record(s) in %d poll "
            "batch(es)\n",
            value, total, msgcnt, batches);

        TEST_ASSERT(total == msgcnt,
                    "fetch.min.bytes=%d should drain all records: expected "
                    "%d, got %d",
                    value, msgcnt, total);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        rd_kafka_destroy(producer);
}

/**
 * @brief fetch.min.bytes=0 is functional for share consumers (broker
 *        responds immediately, no long-poll). Matches the Java client.
 */
static void test_fetch_min_bytes_zero_drains_all(void) {
        SUB_TEST();
        verify_share_fetch_min_bytes_drains_all("0180-fetch-min-bytes-zero",
                                                "0180-fmin-zero", 0);
        SUB_TEST_PASS();
}

/**
 * @brief fetch.min.bytes=INT_MAX is functional for share consumers: the
 *        broker can't satisfy the threshold, so it responds when
 *        fetch.max.wait.ms elapses and all records still drain. Confirms
 *        the INT_MAX upper bound (raised for Java parity) works
 *        end-to-end.
 */
static void test_fetch_min_bytes_max_drains_all(void) {
        SUB_TEST();
        verify_share_fetch_min_bytes_drains_all("0180-fetch-min-bytes-max",
                                                "0180-fmin-max", 2147483647);
        SUB_TEST_PASS();
}


/**
 * @brief Verify fetch.max.bytes=0 returns no records for a share consumer.
 *
 * fetch.max.bytes=0 is accepted, but a ShareFetch with MaxBytes=0 returns
 * no records. This matches the Java client.
 * This test pins the resulting behavior so it does not silently change.
 */
static void test_fetch_max_bytes_zero_returns_no_records(void) {
        const char *topic;
        const char *group = "0180-fetch-max-bytes-zero";
        const int msgcnt  = 10;
        rd_kafka_t *producer;
        rd_kafka_share_t *rkshare;
        rd_kafka_messages_t *batch = NULL;
        int received               = 0;
        int call;

        SUB_TEST();

        producer = create_no_linger_producer();

        topic = test_mk_topic_name("0180-fmax-zero", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_one_per_batch(producer, topic, msgcnt, 200);

        rkshare = create_share_consumer_with_prop(group, "fetch.max.bytes", 0);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Poll a bounded number of times; with fetch.max.bytes=0 the broker
         * returns no records, so every poll must be empty. */
        for (call = 0; call < 5; call++) {
                rd_kafka_error_t *error =
                    rd_kafka_share_poll(rkshare, 1000, &batch);
                TEST_ASSERT(!error, "fetch.max.bytes=0 poll #%d failed: %s",
                            call, error ? rd_kafka_error_string(error) : "");
                received += (int)rd_kafka_messages_count(batch);
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        TEST_SAY("fetch.max.bytes=0: received %d record(s) over 5 polls\n",
                 received);

        TEST_ASSERT(received == 0,
                    "fetch.max.bytes=0 should return no records, got %d",
                    received);

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        rd_kafka_destroy(producer);

        SUB_TEST_PASS();
}

/**
 * @brief Characterize the FETCH connection's idle behavior after the
 *        consumer has drained all records and stopped polling.
 *
 * While records are available the share fetch loop self-perpetuates and
 * the fetch connection stays busy. Once all records are consumed,
 * share_fetch_more_records clears and no further ShareFetch is enqueued,
 * so the fetch (leader) connection goes idle. This test produces a few
 * records, drains them, then stops polling and sleeps past
 * connections.max.idle.ms, and asserts the fetch connection idle-closed
 * and then reconnected (current behavior: a persistent connection is
 * requested while the broker has an assignment and is not UP).
 */
static void test_share_consumer_fetch_conn_idle_after_drain(void) {
        const char *topic;
        const char *group = "0180-fetch-idle";
        const int msgcnt  = 5;
        const int idle_ms = 5000;
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rkshare;
        rd_kafka_t *producer;
        struct idle_reconnect_counters counters;
        rd_kafka_messages_t *batch = NULL;
        char errstr[512];
        int drained = 0;
        int i;

        SUB_TEST();

        rd_atomic32_init(&counters.idle_closes, 0);
        rd_atomic32_init(&counters.reconnects_after_idle, 0);

        producer = create_no_linger_producer();
        topic    = test_mk_topic_name("0180-fetch-idle", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);
        produce_one_per_batch(producer, topic, msgcnt, 100);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "debug", "all");
        /* Short idle timeout so the test runs fast (overrides the 9-min
         * share-consumer default). */
        test_conf_set(conf, "connections.max.idle.ms", "5000");
        rd_kafka_conf_set_log_cb(conf, fetch_idle_log_cb);
        rd_kafka_conf_set_opaque(conf, &counters);

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);
        test_share_set_auto_offset_reset(group, "earliest");
        test_share_consumer_subscribe_multi(rkshare, 1, topic);

        /* Drain all records so share_fetch_more_records clears and the
         * fetch loop quiesces. */
        for (i = 0; i < 30 && drained < msgcnt; i++) {
                rd_kafka_error_t *error =
                    rd_kafka_share_poll(rkshare, 1000, &batch);
                if (error) {
                        rd_kafka_error_destroy(error);
                        continue;
                }
                drained += (int)rd_kafka_messages_count(batch);
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }
        TEST_ASSERT(drained >= msgcnt, "expected to drain %d records, got %d",
                    msgcnt, drained);

        /* Stop polling and idle past the timeout (~2x) so the fetch
         * connection can be closed by connections.max.idle.ms. */
        rd_sleep((idle_ms * 2) / 1000 + 2);

        TEST_SAY("fetch-conn idle closes=%d, reconnects-after-idle=%d\n",
                 rd_atomic32_get(&counters.idle_closes),
                 rd_atomic32_get(&counters.reconnects_after_idle));

        TEST_ASSERT(rd_atomic32_get(&counters.idle_closes) >= 1,
                    "expected the idle fetch connection to be closed by "
                    "connections.max.idle.ms, got %d closes",
                    rd_atomic32_get(&counters.idle_closes));
        TEST_ASSERT(rd_atomic32_get(&counters.reconnects_after_idle) >= 1,
                    "expected the fetch connection to reconnect after the "
                    "idle close (persistent connection requested while the "
                    "broker has an assignment), got %d",
                    rd_atomic32_get(&counters.reconnects_after_idle));

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        rd_kafka_destroy(producer);
}


/* Behavioural tests that require a real broker. */
int main_0180_share_consumer_config(int argc, char **argv) {
        test_max_poll_records_caps_batch_at_5();
        test_max_poll_records_allows_full_drain_at_10();
        test_fetch_max_bytes_one_is_soft_limit();
        test_fetch_min_bytes_zero_drains_all();
        test_fetch_min_bytes_max_drains_all();
        test_fetch_max_bytes_zero_returns_no_records();
        test_share_consumer_fetch_conn_idle_after_drain();
        return 0;
}


/* Construction-time conf-rejection tests; no broker required. */
int main_0180_share_consumer_config_local(int argc, char **argv) {
        test_rebalance_cb_rejected_at_construction();
        test_event_rebalance_rejected_at_construction();
        test_enable_auto_commit_rejected_at_construction();
        test_group_protocol_rejected_at_construction();
        test_socket_max_fails_rejected_at_construction();
        test_auto_offset_reset_rejected_at_construction();
        test_session_timeout_ms_rejected_at_construction();
        test_partition_assignment_strategy_rejected_at_construction();
        test_group_protocol_type_rejected_at_construction();
        test_heartbeat_interval_ms_rejected_at_construction();
        test_receive_message_max_bytes_rejected_at_construction();
        test_group_instance_id_rejected_at_construction();
        test_isolation_level_rejected_at_construction();
        test_group_remote_assignor_rejected_at_construction();
        test_queued_min_messages_rejected_at_construction();
        test_queued_max_messages_kbytes_rejected_at_construction();
        test_fetch_min_bytes_regular_consumer_range_rejected();
        return 0;
}


/*
 * 1. receive.message.max.bytes - using normal consumer, we produce the record
 * with 5KB size and set this to 1KB and try to see if we are getting the record
 * or not
 */