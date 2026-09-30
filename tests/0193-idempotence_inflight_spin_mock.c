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
 * @name Idempotent producer must not busy-loop the broker thread while a
 *       partition is at its in-flight limit (#5617).
 *
 * With a high-latency link and a steady produce rate the partition's
 * transmit queue always holds messages whose linger.ms has expired, while
 * the in-flight limit prevents sending them until a ProduceResponse arrives.
 * The broker thread must then wait for that response instead of repeatedly
 * polling the connection with a zero timeout.
 *
 * The number of broker thread wakeups (from the statistics) is compared to
 * the number of requests sent to the broker over a fixed window.
 */

/** Latest statistics for broker 1, updated from the stats callback. */
static int64_t stats_wakeups = -1;
static int64_t stats_tx      = -1;

static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        const char *p, *s;
        int64_t v;

        if (!(p = strstr(json, "\"nodeid\":1,")))
                return 0;

        if ((s = strstr(p, "\"tx\":")) && sscanf(s, "\"tx\":%" SCNd64, &v) == 1)
                stats_tx = v;

        if ((s = strstr(p, "\"wakeups\":")) &&
            sscanf(s, "\"wakeups\":%" SCNd64, &v) == 1)
                stats_wakeups = v;

        return 0;
}


/**
 * @brief Produce at approximately \p msgs_per_sec for \p duration_ms,
 *        serving the stats callback in between.
 */
static void produce_for(rd_kafka_t *rk,
                        const char *topic,
                        int msgs_per_sec,
                        int duration_ms) {
        int64_t end      = test_clock() + (int64_t)duration_ms * 1000;
        int interval_ms  = 1000 / msgs_per_sec;
        const char *key  = "constant-key";
        const char *data = "payload";

        while (test_clock() < end) {
                TEST_CALL_ERR__(rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(0),
                    RD_KAFKA_V_KEY(key, strlen(key)),
                    RD_KAFKA_V_VALUE((void *)data, strlen(data)),
                    RD_KAFKA_V_END));
                rd_kafka_poll(rk, interval_ms);
        }
}


static void do_test_idempotence_inflight_no_spin(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int64_t wakeups_start, tx_start, wakeups, tx;
        double wakeups_per_req;
        /* Healthy runs see a handful of wakeups per request
         * (send, response, ops). A busy-looping broker thread sees
         * thousands. */
        const double max_wakeups_per_req = 100.0;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        rd_kafka_mock_broker_set_rtt(mcluster, 1, 200);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.idempotence", "true");
        test_conf_set(conf, "acks", "all");
        test_conf_set(conf, "linger.ms", "5");
        test_conf_set(conf, "statistics.interval.ms", "500");
        rd_kafka_conf_set_stats_cb(conf, stats_cb);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Warm up: acquire PID, connect, reach a steady state. */
        produce_for(rk, topic, 500, 2000);
        TEST_ASSERT(stats_wakeups >= 0 && stats_tx >= 0,
                    "No statistics received for broker 1");
        wakeups_start = stats_wakeups;
        tx_start      = stats_tx;

        /* Measurement window */
        produce_for(rk, topic, 500, 4000);
        wakeups = stats_wakeups - wakeups_start;
        tx      = stats_tx - tx_start;

        TEST_ASSERT(tx > 0, "No requests sent during the measurement window");
        wakeups_per_req = (double)wakeups / (double)tx;

        TEST_SAY("Broker 1: %" PRId64 " wakeups for %" PRId64
                 " requests during the measurement window: "
                 "%.1f wakeups/request\n",
                 wakeups, tx, wakeups_per_req);

        TEST_ASSERT(wakeups_per_req < max_wakeups_per_req,
                    "Broker thread is busy-looping while the partition is "
                    "at its in-flight limit: %.1f wakeups/request "
                    "(%" PRId64 " wakeups, %" PRId64
                    " requests), expected < %.1f",
                    wakeups_per_req, wakeups, tx, max_wakeups_per_req);

        TEST_CALL_ERR__(rd_kafka_flush(rk, 30 * 1000));

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_idempotence_inflight_spin_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_idempotence_inflight_no_spin();

        return 0;
}
