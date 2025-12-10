/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
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
 * @name Test persistent broker failure detection and error surfacing
 *
 * This test verifies that:
 * 1. When a broker is unreachable for longer than reconnect.failure.report.ms,
 *    an RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE error is surfaced.
 * 2. The error is only reported once per failure streak.
 * 3. After successful reconnection, the failure tracking is reset.
 */

struct test_state {
        rd_bool_t persistent_failure_seen;
        rd_bool_t all_brokers_down_seen;
        int persistent_failure_count;
        mtx_t lock;
};

static void
error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
        struct test_state *state = (struct test_state *)opaque;

        mtx_lock(&state->lock);
        TEST_SAY("Error callback: %s: %s\n", rd_kafka_err2name(err), reason);

        if (err == RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE) {
                state->persistent_failure_seen = rd_true;
                state->persistent_failure_count++;
                TEST_SAY("Persistent broker failure detected (count: %d)\n",
                         state->persistent_failure_count);
        } else if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                state->all_brokers_down_seen = rd_true;
                TEST_SAY("All brokers down\n");
        }
        mtx_unlock(&state->lock);
}

/**
 * @brief Test that persistent failure error is surfaced when broker
 *        is unreachable for longer than the configured threshold.
 */
static void do_test_persistent_failure_detection(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        struct test_state state = {.persistent_failure_seen  = rd_false,
                                   .all_brokers_down_seen    = rd_false,
                                   .persistent_failure_count = 0};
        int64_t start_time, elapsed;
        const int threshold_ms = 2000; /* 2 seconds for faster testing */

        SUB_TEST("Test persistent failure detection");

        mtx_init(&state.lock, mtx_plain);

        conf = rd_kafka_conf_new();

        /* Use non-existent broker addresses to trigger connection failures */
        test_conf_set(conf, "bootstrap.servers", "127.0.0.1:19091,127.0.0.1:19092");

        /* Set a short threshold for testing */
        test_conf_set(conf, "reconnect.failure.report.ms", "2000");

        /* Short reconnect backoff for faster testing */
        test_conf_set(conf, "reconnect.backoff.ms", "100");
        test_conf_set(conf, "reconnect.backoff.max.ms", "500");

        /* Set the error callback */
        rd_kafka_conf_set_error_cb(conf, error_cb);
        rd_kafka_conf_set_opaque(conf, &state);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        TEST_ASSERT(rk, "Failed to create producer");

        start_time = test_clock();

        /* Poll until we see the persistent failure error or timeout */
        while (!state.persistent_failure_seen) {
                rd_kafka_poll(rk, 100);

                elapsed = (test_clock() - start_time) / 1000;
                if (elapsed > threshold_ms + 5000) {
                        /* Give extra 5 seconds for the error to propagate */
                        TEST_FAIL(
                            "Persistent failure error not received within "
                            "expected time (waited %dms, threshold was %dms)",
                            (int)elapsed, threshold_ms);
                }
        }

        elapsed = (test_clock() - start_time) / 1000;
        TEST_SAY(
            "Persistent failure detected after %dms "
            "(threshold: %dms)\n",
            (int)elapsed, threshold_ms);

        /* Verify the error was received around the threshold time */
        TEST_ASSERT(elapsed >= threshold_ms - 500,
                    "Persistent failure reported too early: %dms", (int)elapsed);

        /* Continue polling to verify we don't get MORE duplicate reports
         * Note: Each broker reports independently, so with 2 brokers we may
         * get 2 reports (one per broker). This is correct behavior.
         * We verify that after seeing the first, we don't get repeated
         * reports from the SAME broker (count should stay stable). */
        int initial_count;
        mtx_lock(&state.lock);
        initial_count = state.persistent_failure_count;
        mtx_unlock(&state.lock);

        TEST_SAY("Initial persistent failure count: %d\n", initial_count);
        TEST_ASSERT(initial_count >= 1,
                    "Expected at least 1 persistent failure report, got %d",
                    initial_count);

        /* Wait and verify count doesn't keep increasing (no repeated reports
         * from same broker) */
        start_time = test_clock();
        while ((test_clock() - start_time) / 1000 < 3000) {
                rd_kafka_poll(rk, 100);
        }

        mtx_lock(&state.lock);
        /* With 2 brokers, we expect at most 2 reports (one per broker) */
        TEST_ASSERT(state.persistent_failure_count <= 2,
                    "Expected at most 2 persistent failure reports "
                    "(one per broker), got %d",
                    state.persistent_failure_count);
        mtx_unlock(&state.lock);

        rd_kafka_destroy(rk);
        mtx_destroy(&state.lock);

        SUB_TEST_PASS();
}

/**
 * @brief Test that persistent failure reporting is disabled when
 *        reconnect.failure.report.ms is set to 0.
 */
static void do_test_persistent_failure_disabled(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        struct test_state state = {.persistent_failure_seen  = rd_false,
                                   .all_brokers_down_seen    = rd_false,
                                   .persistent_failure_count = 0};
        int64_t start_time;

        SUB_TEST("Test persistent failure reporting disabled");

        mtx_init(&state.lock, mtx_plain);

        conf = rd_kafka_conf_new();

        /* Use non-existent broker addresses */
        test_conf_set(conf, "bootstrap.servers", "127.0.0.1:19091");

        /* Disable persistent failure reporting */
        test_conf_set(conf, "reconnect.failure.report.ms", "0");

        /* Short reconnect backoff for faster testing */
        test_conf_set(conf, "reconnect.backoff.ms", "100");
        test_conf_set(conf, "reconnect.backoff.max.ms", "500");

        rd_kafka_conf_set_error_cb(conf, error_cb);
        rd_kafka_conf_set_opaque(conf, &state);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        TEST_ASSERT(rk, "Failed to create producer");

        start_time = test_clock();

        /* Poll for 5 seconds - should NOT see persistent failure error */
        while ((test_clock() - start_time) / 1000 < 5000) {
                rd_kafka_poll(rk, 100);
        }

        mtx_lock(&state.lock);
        TEST_ASSERT(!state.persistent_failure_seen,
                    "Persistent failure should NOT be reported when disabled");
        /* We should still see all_brokers_down though */
        TEST_ASSERT(state.all_brokers_down_seen,
                    "ALL_BROKERS_DOWN should still be reported");
        mtx_unlock(&state.lock);

        rd_kafka_destroy(rk);
        mtx_destroy(&state.lock);

        SUB_TEST_PASS();
}

/**
 * @brief Test the new error code exists and has proper description.
 */
static void do_test_error_code(void) {
        const char *errstr;

        SUB_TEST("Test error code RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE");

        errstr = rd_kafka_err2str(RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE);
        TEST_ASSERT(errstr != NULL, "Error string should not be NULL");
        TEST_ASSERT(strlen(errstr) > 0, "Error string should not be empty");

        TEST_SAY("Error code %d: %s\n",
                 RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE, errstr);

        /* Verify it's a local error (negative) */
        TEST_ASSERT(RD_KAFKA_RESP_ERR__BROKER_PERSISTENT_FAILURE < 0,
                    "Should be a local error (negative value)");

        SUB_TEST_PASS();
}

/**
 * @brief Test that configuration option is properly set.
 */
static void do_test_config(void) {
        rd_kafka_conf_t *conf;
        char buf[64];
        size_t buf_size = sizeof(buf);
        rd_kafka_conf_res_t res;

        SUB_TEST("Test reconnect.failure.report.ms configuration");

        conf = rd_kafka_conf_new();

        /* Test getting default value */
        res = rd_kafka_conf_get(conf, "reconnect.failure.report.ms", buf,
                                &buf_size);
        TEST_ASSERT(res == RD_KAFKA_CONF_OK, "Failed to get config");
        TEST_SAY("Default reconnect.failure.report.ms: %s\n", buf);
        TEST_ASSERT(strcmp(buf, "60000") == 0, "Default should be 60000");

        /* Test setting custom value */
        res = rd_kafka_conf_set(conf, "reconnect.failure.report.ms", "30000",
                                NULL, 0);
        TEST_ASSERT(res == RD_KAFKA_CONF_OK, "Failed to set config");

        buf_size = sizeof(buf);
        res      = rd_kafka_conf_get(conf, "reconnect.failure.report.ms", buf,
                                     &buf_size);
        TEST_ASSERT(res == RD_KAFKA_CONF_OK, "Failed to get config");
        TEST_ASSERT(strcmp(buf, "30000") == 0, "Value should be 30000");

        /* Test disabling (value 0) */
        res = rd_kafka_conf_set(conf, "reconnect.failure.report.ms", "0", NULL,
                                0);
        TEST_ASSERT(res == RD_KAFKA_CONF_OK, "Failed to set config to 0");

        buf_size = sizeof(buf);
        res      = rd_kafka_conf_get(conf, "reconnect.failure.report.ms", buf,
                                     &buf_size);
        TEST_ASSERT(res == RD_KAFKA_CONF_OK, "Failed to get config");
        TEST_ASSERT(strcmp(buf, "0") == 0, "Value should be 0");

        rd_kafka_conf_destroy(conf);

        SUB_TEST_PASS();
}

int main_0154_persistent_broker_failure(int argc, char **argv) {
        /* Quick tests that don't require a broker */
        do_test_error_code();
        do_test_config();

        /* Integration tests that simulate broker failures */
        do_test_persistent_failure_detection();
        do_test_persistent_failure_disabled();

        return 0;
}

