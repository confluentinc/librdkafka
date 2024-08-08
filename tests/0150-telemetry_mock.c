/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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

typedef struct {
        int16_t ApiKey;
        int64_t
            expected_diff_ms /* Expected time difference from last request */;
        int64_t jitter_percent; /* Jitter to be accounted for while checking
                                   expected diff*/
        int broker_id;          /* Broker id of request. */
} rd_kafka_telemetry_expected_request_t;

static void test_telemetry_check_protocol_request_times(
    rd_kafka_mock_request_t **requests_actual,
    size_t actual_cnt,
    rd_kafka_telemetry_expected_request_t *requests_expected,
    size_t expected_cnt) {
        int64_t prev_timestamp = -1;
        int64_t curr_timestamp = -1;
        size_t expected_idx    = 0;
        size_t actual_idx      = 0;
        const int buffer       = 200 /* constant buffer time. */;

        if (expected_cnt < 1)
                return;

        TEST_ASSERT(actual_cnt >= expected_cnt,
                    "Expected at least %" PRIusz " requests, have %" PRIusz,
                    expected_cnt, actual_cnt);

        for (expected_idx = 0, actual_idx = 0;
             expected_idx < expected_cnt && actual_idx < actual_cnt;
             actual_idx++) {
                rd_kafka_mock_request_t *request_actual =
                    requests_actual[actual_idx];
                int16_t actual_ApiKey =
                    rd_kafka_mock_request_api_key(request_actual);
                int actual_broker_id = rd_kafka_mock_request_id(request_actual);
                rd_kafka_telemetry_expected_request_t request_expected =
                    requests_expected[expected_idx];

                if (actual_ApiKey != RD_KAFKAP_GetTelemetrySubscriptions &&
                    actual_ApiKey != RD_KAFKAP_PushTelemetry)
                        continue;

                TEST_ASSERT(actual_ApiKey == request_expected.ApiKey,
                            "Expected ApiKey %s, got ApiKey %s",
                            rd_kafka_ApiKey2str(request_expected.ApiKey),
                            rd_kafka_ApiKey2str(actual_ApiKey));

                if (request_expected.broker_id != -1)
                        TEST_ASSERT(
                            request_expected.broker_id == actual_broker_id,
                            "Expected request to be sent to broker %d, "
                            "was sent to %d",
                            request_expected.broker_id, actual_broker_id);

                prev_timestamp = curr_timestamp;
                curr_timestamp =
                    rd_kafka_mock_request_timestamp(request_actual);
                if (prev_timestamp != -1 &&
                    request_expected.expected_diff_ms != -1) {
                        int64_t diff_ms =
                            (curr_timestamp - prev_timestamp) / 1000;
                        int64_t expected_diff_low =
                            request_expected.expected_diff_ms *
                                (100 - request_expected.jitter_percent) / 100 -
                            buffer;
                        int64_t expected_diff_hi =
                            request_expected.expected_diff_ms *
                                (100 + request_expected.jitter_percent) / 100 +
                            buffer;

                        TEST_ASSERT(
                            diff_ms > expected_diff_low,
                            "Expected difference to be more than %" PRId64
                            ", was "
                            "%" PRId64,
                            expected_diff_low, diff_ms);
                        TEST_ASSERT(
                            diff_ms < expected_diff_hi,
                            "Expected difference to be less than %" PRId64
                            ", was "
                            "%" PRId64,
                            expected_diff_hi, diff_ms);
                }
                expected_idx++;
        }
}

static void test_clear_request_list(rd_kafka_mock_request_t **requests,
                                    size_t request_cnt) {
        size_t i;
        for (i = 0; i < request_cnt; i++) {
                rd_kafka_mock_request_destroy(requests[i]);
        }
        rd_free(requests);
}

static void test_poll_timeout(rd_kafka_t *rk, int64_t duration_ms) {
        int64_t start_time = test_clock();
        while ((test_clock() - start_time) / 1000 < duration_ms)
                rd_kafka_poll(rk, 500);
}

/**
 * @brief Tests the 'happy path' of GetTelemetrySubscriptions, followed by
 *        successful PushTelemetry requests.
 *        See `requests_expected` for detailed expected flow.
 */
void do_test_telemetry_get_subscription_push_telemetry(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *producer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t push_interval = 5000;

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = push_interval + jitter : The first PushTelemetry request */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
            /* T = push_interval*2 + jitter : The second PushTelemetry request.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
        };

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "debug", "telemetry");
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Poll for enough time for two pushes to be triggered, and a little
         * extra, so 2.5 x push interval. */
        test_poll_timeout(producer, push_interval * 2.5);

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);

        test_telemetry_check_protocol_request_times(
            requests, request_cnt, requests_expected,
            RD_ARRAY_SIZE(requests_expected));

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Tests the 'happy path' of GetTelemetrySubscriptions, followed by
 *        successful PushTelemetry requests.
 *        See `requests_expected` for detailed expected flow.
 */
void do_test_telemetry_get_subscription_push_telemetry_consumer(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *consumer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t push_interval = 5000;

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = push_interval + jitter : The first PushTelemetry request */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
            /* T = push_interval*2 + jitter : The second PushTelemetry request.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
        };

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "debug", "telemetry");
        consumer = test_create_handle(RD_KAFKA_CONSUMER, conf);

        /* Poll for enough time for two pushes to be triggered, and a little
         * extra, so 2.5 x push interval. */
        test_poll_timeout(consumer, push_interval * 2.5);

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);

        test_telemetry_check_protocol_request_times(
            requests, request_cnt, requests_expected,
            RD_ARRAY_SIZE(requests_expected));

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        rd_kafka_destroy(consumer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief When there are no subscriptions, GetTelemetrySubscriptions should be
 *        resent after the push interval until there are subscriptions.
 *        See `requests_expected` for detailed expected flow.
 */
void do_test_telemetry_empty_subscriptions_list(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *producer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t push_interval = 5000;

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request, returns
             * empty subscription. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = push_interval : The second GetTelemetrySubscriptions request,
             * returns non-empty subscription */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
            /* T = push_interval*2 + jitter : The first PushTelemetry request.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
        };


        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster, NULL, 0);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Poll for enough time so that the first GetTelemetrySubscription
         * request is triggered. */
        test_poll_timeout(producer, (push_interval * 0.5));

        /* Set expected_metrics before the second GetTelemetrySubscription is
         * triggered. */
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);

        /* Poll for enough time so that the second GetTelemetrySubscriptions and
         * subsequent PushTelemetry request is triggered. */
        test_poll_timeout(producer, (push_interval * 2));

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        test_telemetry_check_protocol_request_times(requests, request_cnt,
                                                    requests_expected, 3);

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief When a client is terminating, PushIntervalMs is overriden and a final
 *        push telemetry request should be sent immediately.
 *        See `requests_expected` for detailed expected flow.
 */
void do_test_telemetry_terminating_push(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *producer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t wait_before_termination = 2000;
        const int64_t push_interval = 5000; /* Needs to be comfortably larger
                                               than wait_before_termination. */

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = wait_before_termination : The second PushTelemetry request is
             * sent immediately (terminating).
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = wait_before_termination,
             .jitter_percent   = 0},
        };
        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Poll for enough time so that the initial GetTelemetrySubscriptions
         * can be sent and handled, and keep polling till it's time to
         * terminate. */
        test_poll_timeout(producer, wait_before_termination);

        /* Destroy the client to trigger a terminating push request
         * immediately. */
        rd_kafka_destroy(producer);

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        test_telemetry_check_protocol_request_times(requests, request_cnt,
                                                    requests_expected, 2);

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Preferred broker should be 'sticky' and should not change unless the
 *        old preferred broker goes down.
 *        See `requests_expected` for detailed expected flow.
 */
void do_test_telemetry_preferred_broker_change(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *producer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t push_interval = 5000;

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = 1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = push_interval + jitter : The first PushTelemetry request,
             * sent to the preferred broker 1.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = 1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
            /* T = 2*push_interval + jitter : The second PushTelemetry request,
             * sent to the preferred broker 1.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = 1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
            /* T = 3*push_interval + jitter: The old preferred broker is set
             * down, and this is the first PushTelemetry request to the new
             * preferred broker.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = 2,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
            /* T = 4*push_interval + jitter + arbitraryT + jitter2 : The second
             * PushTelemetry request to the new preferred broker. The old
             * broker will be up, but the preferred broker will not chnage.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = 2,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 0},
        };
        SUB_TEST();

        mcluster = test_mock_cluster_new(2, &bootstraps);
        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        /* Set broker 2 down, to make sure broker 1 is the first preferred
         * broker. */
        rd_kafka_mock_broker_set_down(mcluster, 2);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "debug", "telemetry");
        // rd_kafka_conf_set_error_cb(conf, test_error_is_not_fatal_cb);
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        producer               = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Poll for enough time that the initial GetTelemetrySubscription can be
         * sent and the first PushTelemetry request can be scheduled. */
        test_poll_timeout(producer, 0.5 * push_interval);

        /* Poll for enough time that 2 PushTelemetry requests can be sent. Set
         * the all brokers up during this time, but the preferred broker (1)
         * should remain sticky. */
        rd_kafka_mock_broker_set_up(mcluster, 2);
        test_poll_timeout(producer, 2 * push_interval);

        /* Set the preferred broker (1) down. */
        rd_kafka_mock_broker_set_down(mcluster, 1);

        /* Poll for enough time that 1 PushTelemetry request can be sent. */
        test_poll_timeout(producer, 1.25 * push_interval);

        /* Poll for enough time that 1 PushTelemetry request can be sent.  Set
         * the all brokers up during this time, but the preferred broker (2)
         * should remain sticky. */
        rd_kafka_mock_broker_set_up(mcluster, 1);
        test_poll_timeout(producer, 1.25 * push_interval);

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        test_telemetry_check_protocol_request_times(requests, request_cnt,
                                                    requests_expected, 5);

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Subscription Id change at the broker should trigger a new
 *       GetTelemetrySubscriptions request.
 */
void do_test_subscription_id_change(void) {
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        char *expected_metrics[]           = {"*"};
        rd_kafka_t *producer               = NULL;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt;
        const int64_t push_interval = 1000;

        rd_kafka_telemetry_expected_request_t requests_expected[] = {
            /* T= 0 : The initial GetTelemetrySubscriptions request. */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = -1,
             .jitter_percent   = 0},
            /* T = push_interval + jitter : The first PushTelemetry request,
             * sent to the preferred broker 1.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
            /* T = 2*push_interval + jitter : The second PushTelemetry request,
             * which will fail with unknown subscription id.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
            /* New GetTelemetrySubscriptions request will be sent immediately.
             */
            {.ApiKey           = RD_KAFKAP_GetTelemetrySubscriptions,
             .broker_id        = -1,
             .expected_diff_ms = 0,
             .jitter_percent   = 0},
            /* T = 3*push_interval + jitter : The third PushTelemetry request,
             * sent to the preferred broker 1 with new subscription id.
             */
            {.ApiKey           = RD_KAFKAP_PushTelemetry,
             .broker_id        = -1,
             .expected_diff_ms = push_interval,
             .jitter_percent   = 20},
        };
        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_telemetry_set_requested_metrics(mcluster,
                                                      expected_metrics, 1);
        rd_kafka_mock_telemetry_set_push_interval(mcluster, push_interval);
        rd_kafka_mock_start_request_tracking(mcluster);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "debug", "telemetry");
        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_poll_timeout(producer, push_interval * 1.2);

        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_PushTelemetry, 1,
            RD_KAFKA_RESP_ERR_UNKNOWN_SUBSCRIPTION_ID);

        test_poll_timeout(producer, push_interval * 2.5);

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);

        test_telemetry_check_protocol_request_times(
            requests, request_cnt, requests_expected,
            RD_ARRAY_SIZE(requests_expected));

        /* Clean up. */
        rd_kafka_mock_stop_request_tracking(mcluster);
        test_clear_request_list(requests, request_cnt);
        rd_kafka_destroy(producer);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0150_telemetry_mock(int argc, char **argv) {

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_telemetry_get_subscription_push_telemetry_consumer();

        // do_test_telemetry_empty_subscriptions_list();

        // do_test_telemetry_terminating_push();

        // do_test_telemetry_preferred_broker_change();

        // do_test_subscription_id_change();

        return 0;
}
