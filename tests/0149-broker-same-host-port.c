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


int main_0149_broker_same_host_port_mock(int argc, char **argv) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t err;
        const size_t num_brokers = 3;
        size_t cnt               = 0;
        int32_t *ids;
        size_t i;

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        cluster = test_mock_cluster_new(num_brokers, &bootstraps);

        for (i = 1; i <= num_brokers; i++) {
                rd_kafka_mock_broker_set_host_port(cluster, i, "localhost",
                                                   9092);
        }

        test_conf_init(&conf, NULL, 10);

        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Trigger Metadata request which will update learned brokers. */
        err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000));
        rd_kafka_metadata_destroy(md);
        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

        ids = rd_kafka_brokers_learned_ids(rk, &cnt);

        TEST_ASSERT(cnt == num_brokers,
                    "expected %" PRIusz " brokers in cache, not %" PRIusz,
                    num_brokers, cnt);

        for (i = 0; i < cnt; i++) {
                /* Brokers are added at the head of the list. */
                int32_t expected_id = cnt - i;

                TEST_ASSERT(ids[i] == expected_id,
                            "expected broker %d in cache, not %d", expected_id,
                            ids[i]);
        }

        if (ids)
                free(ids);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);

        return 0;
}
