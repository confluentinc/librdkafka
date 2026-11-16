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
 * @name A FetchResponse received after its partition was unassigned,
 *       using the mock broker.
 *
 * Stopping a partition's fetcher unforwards its fetch queue from the
 * consumer queue. The messages of a FetchResponse that was in flight at
 * that time were still enqueued on the fetch queue, where no reader
 * serves them: they stayed allocated until the partition was assigned to
 * this consumer again or the consumer was destroyed.
 */

int main_0193_fetch_after_unassign_mock(int argc, char **argv) {
        const char *topic    = "fetch_after_unassign";
        const int msgset_cnt = 4;
        const int msgcnt     = 100;
        const int rtt_ms     = 500;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        rd_kafka_queue_t *partq;
        uint64_t testid = test_id_generate();
        size_t retained;
        int i;

        TEST_SKIP_MOCK_CLUSTER(0);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* The mock broker returns a single MessageSet per FetchRequest:
         * seed several, so that the consumer sends another FetchRequest
         * after the first one was served. */
        for (i = 0; i < msgset_cnt; i++)
                test_produce_msgs_easy_v(topic, testid, 0, i * msgcnt, msgcnt,
                                         100, "bootstrap.servers", bootstraps,
                                         NULL);

        TEST_CALL_ERR__(rd_kafka_mock_broker_set_rtt(mcluster, 1, rtt_ms));

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");

        c = test_create_consumer("fetch_after_unassign_group", NULL, conf,
                                 NULL);

        test_consumer_assign_partition("assign", c, topic, 0, 0);

        /* The first FetchResponse was served. */
        test_consumer_poll("consume.first", c, testid, -1, 0, 1, NULL);

        /* Let the next FetchRequest go out, its response is still
         * most of a round trip away. */
        rd_usleep(rtt_ms * 1000 / 5, NULL);

        test_consumer_unassign("unassign", c);

        /* Wait for that response: the consumer queue only holds
         * outdated messages, which the poll discards. */
        test_consumer_poll_no_msgs("after.unassign", c, testid, rtt_ms * 3);

        partq = rd_kafka_queue_get_partition(c, topic, 0);
        TEST_ASSERT(partq != NULL, "expected a partition queue");
        retained = rd_kafka_queue_length(partq);
        rd_kafka_queue_destroy(partq);

        TEST_ASSERT(retained == 0,
                    "expected no messages on the unassigned partition's "
                    "queue, not %" PRIusz,
                    retained);

        test_consumer_close(c);
        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        return 0;
}
