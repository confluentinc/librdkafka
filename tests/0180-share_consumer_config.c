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

int main_0180_share_consumer_config(int argc, char **argv) {
        test_rebalance_cb_rejected_at_construction();
        test_event_rebalance_rejected_at_construction();
        test_enable_auto_commit_rejected_at_construction();
        test_group_protocol_rejected_at_construction();
        test_socket_max_fails_rejected_at_construction();
        test_auto_offset_reset_rejected_at_construction();
        return 0;
}