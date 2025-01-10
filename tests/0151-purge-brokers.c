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


static void fetch_metadata(rd_kafka_t *rk,
                           size_t expected_broker_cnt,
                           int32_t *expected_brokers) {
        const rd_kafka_metadata_t *md = NULL;
        rd_kafka_resp_err_t err;
        size_t cnt   = 0;
        int32_t *ids = NULL;
        size_t i;
        int timeout_secs       = 5;
        int64_t abs_timeout_us = test_clock() + timeout_secs * 1000000;

        TEST_SAY("Waiting for up to %ds for metadata update\n", timeout_secs);

        /* Trigger Metadata request which will update learned brokers. */
        do {
                err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(100));
                if (md)
                        rd_kafka_metadata_destroy(md);
                /* Can time out in case the request is sent to
                 * the broker that has been decommissioned. */
                else if (err != RD_KAFKA_RESP_ERR__TIMED_OUT &&
                         err != RD_KAFKA_RESP_ERR__TRANSPORT)
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                rd_usleep(100 * 1000, 0);
                RD_IF_FREE(ids, rd_free);
                ids = rd_kafka_brokers_learned_ids(rk, &cnt);
        } while (test_clock() < abs_timeout_us && cnt != expected_broker_cnt);

        TEST_ASSERT(cnt == expected_broker_cnt,
                    "expected %" PRIusz " brokers in cache, not %" PRIusz,
                    expected_broker_cnt, cnt);

        for (i = 0; i < cnt; i++) {
                TEST_ASSERT(ids[i] == expected_brokers[i],
                            "expected broker id[%" PRIusz
                            "] to be "
                            "%" PRId32 ", got %" PRId32,
                            i, expected_brokers[i], ids[i]);
        }

        RD_IF_FREE(ids, rd_free);
}

/**
 * @brief Purge brokers from the mock cluster,
 *        verifying that brokers are decommissioned in the client.
 *        Then new ones are added and verified.
 */
int main_0151_purge_brokers_mock(int argc, char **argv) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const size_t num_brokers       = 3;
        size_t action                  = 0;
        int32_t expected_broker_cnt[5] = {3, 2, 1, 2, 3};

        /* Brokers are added at the head of the list. */
        int32_t expected_brokers[5][3] = {
            {3, 2, 1}, {3, 2}, {3}, {4, 3}, {5, 4, 3}};

        /* (action,broker id). 0 to remove, 1 to add */
        int32_t actions[4][2] = {{0, 1}, {0, 2}, {1, 4}, {1, 5}};

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        cluster = test_mock_cluster_new(num_brokers, &bootstraps);

        test_conf_init(&conf, NULL, 10);

        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create a new topic to trigger partition reassignment */
        rd_kafka_mock_topic_create(cluster, "topic1", 3, num_brokers);

        for (action = 0; action < RD_ARRAY_SIZE(expected_brokers); action++) {
                if (action > 0) {
                        if (actions[action - 1][0] == 0) {
                                TEST_ASSERT(
                                    rd_kafka_mock_broker_decommission(
                                        cluster, actions[action - 1][1]) == 0,
                                    "Failed to remove broker from cluster");
                        } else {
                                TEST_ASSERT(
                                    rd_kafka_mock_broker_add(
                                        cluster, actions[action - 1][1]) == 0,
                                    "Failed to add broker from cluster");
                        }
                }

                fetch_metadata(rk, expected_broker_cnt[action],
                               expected_brokers[action]);
        }

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);

        return 0;
}
