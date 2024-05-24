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


static void fetch_metadata(rd_kafka_t *rk, size_t expected_num_brokers) {
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t err;
        size_t cnt = 0;
        int32_t *ids;
        size_t i;
        int timeout_secs = 5;
        int64_t abs_timeout_us = test_clock() + timeout_secs * 1000000;

        TEST_SAY("Waiting for up to %ds for metadata update\n", timeout_secs);

        /* Trigger Metadata request which will update learned brokers. */
        do {
                err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                rd_kafka_metadata_destroy(md);

                rd_sleep(1);

                ids = rd_kafka_broker_get_learned_ids(rk, &cnt);
        } while (test_clock() < abs_timeout_us && cnt < expected_num_brokers);

        TEST_ASSERT(cnt == expected_num_brokers,
                    "expected %" PRIusz " brokers in cache, not %" PRIusz,
                    expected_num_brokers, cnt);

        for (i = 0; i < cnt; i++) {
                /* Brokers are added at the head of the list. */
                int32_t expected_id = cnt - i;

                TEST_ASSERT(ids[i] == expected_id,
                            "expected broker %d in cache, not %d", expected_id,
                            ids[i]);
        }

        if (ids)
                free(ids);
}


int main_0146_purge_brokers(int argc, char **argv) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const size_t num_brokers = 3;

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        cluster = test_mock_cluster_new(num_brokers, &bootstraps);

        test_conf_init(&conf, NULL, 10);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "debug", "mock,broker,metadata");

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        fetch_metadata(rk, num_brokers);

        /* Decommission a broker. */
        TEST_ASSERT(rd_kafka_mock_broker_decommission(cluster, 3) == 0,
                    "Failed to remove broker from cluster");

        /*
         * Wait for:
         * - the broker to decommission
         * - rd_kafka_1s_tmr_cb() to fire and establish a new broker connection
         */
        rd_sleep(3);

        fetch_metadata(rk, num_brokers - 1);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);

        return 0;
}
