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

#include "../src/rdkafka_proto.h"

/**
 * @brief Share consumer rd_kafka_share_client_instance_id() API tests.
 *
 * Verifies that the share consumer correctly retrieves the client
 * instance UUID assigned by the broker via GetTelemetrySubscriptions
 * (KIP-714) and that the API surfaces the expected errors when
 * telemetry is disabled, the consumer is closed, the timeout
 * expires, or invalid arguments are supplied.
 *
 * Subtests are split across two entry points so that the telemetry-dependent
 * happy paths can be skipped on brokers that do not expose KIP-714 APIs
 * without affecting the broker-agnostic error-path coverage:
 *
 *   main_0189_share_client_instance_id         (needs broker telemetry)
 *     - basic happy path (UUID returned, non-zero)
 *     - stability (second call returns the same UUID)
 *     - distinct consumers receive distinct UUIDs
 *
 *   main_0189_share_client_instance_id_errors  (no broker telemetry needed)
 *     - invalid (negative) timeout returns __INVALID_ARG
 *     - telemetry disabled (enable.metrics.push=false) returns __STATE
 *     - call after rd_kafka_share_consumer_close() returns __STATE
 */

/**
 * @brief Create a share consumer with telemetry enabled or disabled.
 *
 * Mirrors create_share_consumer() in 0176 but exposes telemetry
 * control instead of acknowledgement mode (this test does not produce
 * or consume records, so the ack mode is left at its default).
 */
static rd_kafka_share_t *create_share_consumer(const char *group_id,
                                               rd_bool_t enable_telemetry) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);

        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.metrics.push",
                          enable_telemetry ? "true" : "false", errstr,
                          sizeof(errstr));

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rkshare, "Failed to create share consumer: %s", errstr);

        return rkshare;
}


/**
 * @brief True if two UUIDs are bitwise identical.
 */
static rd_bool_t uuid_eq(const rd_kafka_Uuid_t *a, const rd_kafka_Uuid_t *b) {
        return rd_kafka_Uuid_most_significant_bits(a) ==
                   rd_kafka_Uuid_most_significant_bits(b) &&
               rd_kafka_Uuid_least_significant_bits(a) ==
                   rd_kafka_Uuid_least_significant_bits(b);
}


/**
 * @brief Probe whether the broker exposes the KIP-714 telemetry APIs.
 *
 * A stock Apache Kafka broker only advertises GetTelemetrySubscriptions
 * (API 71) and PushTelemetry (API 72) when at least one class in
 * metric.reporters implements ClientTelemetryReceiver. Without that,
 * librdkafka's telemetry FSM never sends GetTelemetrySubscriptions, the
 * client instance UUID is never populated, and every call to
 * rd_kafka_share_client_instance_id() times out.
 *
 * @returns true when a non-zero UUID was returned within the probe
 *          timeout; false on __TIMED_OUT (i.e. broker telemetry is not
 *          set up — broker-dependent subtests should be skipped).
 */
static rd_bool_t telemetry_available(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid = NULL;
        rd_bool_t available;

        rkshare = create_share_consumer("0189-iid-probe", rd_true);

        error = rd_kafka_share_client_instance_id(rkshare, 15 * 1000, &uuid);

        if (!error) {
                available = rd_true;
                rd_kafka_Uuid_destroy(uuid);
        } else {
                TEST_ASSERT(
                    rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Probe failed with unexpected error: %s",
                    rd_kafka_error_string(error));
                available = rd_false;
                rd_kafka_error_destroy(error);
        }

        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);
        return available;
}


/* ===================================================================
 *  Test 1: basic happy path.
 *
 *  Create a share consumer with telemetry enabled, call
 *  rd_kafka_share_client_instance_id() with a generous timeout and
 *  verify a non-zero UUID is returned.
 * =================================================================== */
static void do_test_basic_get_instance_id(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid = NULL;

        SUB_TEST();

        rkshare = create_share_consumer("0189-iid-basic", rd_true);

        error = rd_kafka_share_client_instance_id(rkshare, 30 * 1000, &uuid);
        TEST_ASSERT(!error, "Expected success, got: %s",
                    error ? rd_kafka_error_string(error) : "");
        TEST_ASSERT(uuid != NULL, "Expected non-NULL UUID out-param");
        TEST_ASSERT(!RD_KAFKA_UUID_IS_ZERO(*uuid),
                    "Expected non-zero UUID from broker");

        TEST_SAY("Client instance ID: %s\n", rd_kafka_Uuid_base64str(uuid));

        rd_kafka_Uuid_destroy(uuid);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 2: UUID is stable across calls.
 *
 *  The UUID is assigned once per client instance and must not change
 *  between successive calls. The second call should also return
 *  promptly because the UUID is already cached.
 * =================================================================== */
static void do_test_instance_id_is_stable(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid1 = NULL;
        rd_kafka_Uuid_t *uuid2 = NULL;

        SUB_TEST();

        rkshare = create_share_consumer("0189-iid-stable", rd_true);

        error = rd_kafka_share_client_instance_id(rkshare, 10 * 1000, &uuid1);
        TEST_ASSERT(!error && uuid1, "Initial call failed: %s",
                    error ? rd_kafka_error_string(error)
                          : "(no error, NULL uuid)");

        /* Second call should return the same UUID without blocking. */
        error = rd_kafka_share_client_instance_id(rkshare, 5 * 1000, &uuid2);
        TEST_ASSERT(!error && uuid2, "Second call failed: %s",
                    error ? rd_kafka_error_string(error)
                          : "(no error, NULL uuid)");

        TEST_ASSERT(uuid_eq(uuid1, uuid2),
                    "Client instance ID changed between calls: %s -> %s",
                    rd_kafka_Uuid_base64str(uuid1),
                    rd_kafka_Uuid_base64str(uuid2));

        rd_kafka_Uuid_destroy(uuid1);
        rd_kafka_Uuid_destroy(uuid2);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 3: distinct consumers, distinct UUIDs.
 *
 *  The broker assigns a fresh UUID per client connection (KIP-714).
 *  Two share consumers in the same process must receive different
 *  client instance IDs.
 * =================================================================== */
static void do_test_different_consumers_different_ids(void) {
        rd_kafka_share_t *rkshare1;
        rd_kafka_share_t *rkshare2;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid1 = NULL;
        rd_kafka_Uuid_t *uuid2 = NULL;

        SUB_TEST();

        rkshare1 = create_share_consumer("0189-iid-c1", rd_true);
        rkshare2 = create_share_consumer("0189-iid-c2", rd_true);

        error = rd_kafka_share_client_instance_id(rkshare1, 10 * 1000, &uuid1);
        TEST_ASSERT(!error && uuid1, "Consumer 1 failed: %s",
                    error ? rd_kafka_error_string(error) : "(NULL uuid)");

        error = rd_kafka_share_client_instance_id(rkshare2, 10 * 1000, &uuid2);
        TEST_ASSERT(!error && uuid2, "Consumer 2 failed: %s",
                    error ? rd_kafka_error_string(error) : "(NULL uuid)");

        TEST_ASSERT(!uuid_eq(uuid1, uuid2),
                    "Two share consumers got the same client instance ID: %s",
                    rd_kafka_Uuid_base64str(uuid1));

        rd_kafka_Uuid_destroy(uuid1);
        rd_kafka_Uuid_destroy(uuid2);
        test_share_consumer_close(rkshare1);
        test_share_destroy(rkshare1);
        test_share_consumer_close(rkshare2);
        test_share_destroy(rkshare2);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 4: negative timeout -> __INVALID_ARG.
 *
 *  Pure client-side argument validation; the broker is never queried.
 * =================================================================== */
static void do_test_invalid_timeout(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid = NULL;

        SUB_TEST();

        rkshare = create_share_consumer("0189-iid-badtmo", rd_true);

        error = rd_kafka_share_client_instance_id(rkshare, -1, &uuid);
        TEST_ASSERT(error, "Expected __INVALID_ARG, got NULL (success)");
        TEST_ASSERT(rd_kafka_error_code(error) ==
                        RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected __INVALID_ARG, got %s: %s",
                    rd_kafka_error_name(error), rd_kafka_error_string(error));
        TEST_ASSERT(uuid == NULL,
                    "Out-param should remain NULL on error, got %p",
                    (void *)uuid);

        rd_kafka_error_destroy(error);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 5: telemetry disabled -> __STATE.
 *
 *  With enable.metrics.push=false the function must short-circuit
 *  without contacting the broker.
 * =================================================================== */
static void do_test_telemetry_disabled(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid = NULL;

        SUB_TEST();

        rkshare = create_share_consumer("0189-iid-disabled", rd_false);

        error = rd_kafka_share_client_instance_id(rkshare, 1000, &uuid);
        TEST_ASSERT(error, "Expected __STATE, got NULL (success)");
        TEST_ASSERT(rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__STATE,
                    "Expected __STATE, got %s: %s",
                    rd_kafka_error_name(error), rd_kafka_error_string(error));
        TEST_ASSERT(uuid == NULL,
                    "Out-param should remain NULL on error, got %p",
                    (void *)uuid);

        rd_kafka_error_destroy(error);
        test_share_consumer_close(rkshare);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/* ===================================================================
 *  Test 6: call after rd_kafka_share_consumer_close() -> __STATE.
 * =================================================================== */
static void do_test_after_close(void) {
        rd_kafka_share_t *rkshare;
        rd_kafka_error_t *error;
        rd_kafka_Uuid_t *uuid = NULL;

        SUB_TEST();

        rkshare = create_share_consumer("0189-iid-closed", rd_true);

        test_share_consumer_close(rkshare);

        error = rd_kafka_share_client_instance_id(rkshare, 1000, &uuid);
        TEST_ASSERT(error,
                    "Expected __STATE on closed consumer, got NULL (success)");
        TEST_ASSERT(rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__STATE,
                    "Expected __STATE, got %s: %s",
                    rd_kafka_error_name(error), rd_kafka_error_string(error));
        TEST_ASSERT(uuid == NULL,
                    "Out-param should remain NULL on error, got %p",
                    (void *)uuid);

        rd_kafka_error_destroy(error);
        test_share_destroy(rkshare);

        SUB_TEST_PASS();
}


/**
 * @brief Telemetry-dependent subtests.
 *
 * These exercise the actual GetTelemetrySubscriptions round-trip and
 * therefore require a broker that has at least one ClientTelemetryReceiver
 * registered via metric.reporters. The probe at the top skips the rest
 * when the broker does not expose KIP-714 telemetry APIs.
 */
int main_0189_share_client_instance_id(int argc, char **argv) {
        test_timeout_set(120);

        if (!telemetry_available()) {
                TEST_SKIP(
                    "Broker does not expose KIP-714 telemetry APIs "
                    "(no ClientTelemetryReceiver registered via "
                    "metric.reporters)\n");
                return 0;
        }

        do_test_basic_get_instance_id();
        do_test_instance_id_is_stable();
        do_test_different_consumers_different_ids();

        return 0;
}


/**
 * @brief Client-side error-path subtests.
 *
 * These do not depend on the broker exposing KIP-714 telemetry APIs —
 * each assertion is short-circuited inside librdkafka before any
 * GetTelemetrySubscriptions round-trip would be issued.
 */
int main_0189_share_client_instance_id_errors(int argc, char **argv) {
        test_timeout_set(60);

        do_test_invalid_timeout();
        do_test_telemetry_disabled();
        do_test_after_close();

        return 0;
}
