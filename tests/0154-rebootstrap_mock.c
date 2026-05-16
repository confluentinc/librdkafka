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

struct log_hits_s {
        rd_atomic32_t metadata;
        rd_atomic32_t rebootstrap;
};

static void
log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
        struct log_hits_s *log_hits = rd_kafka_opaque(rk);

        if (!strcmp(fac, "METADATA") &&
            strstr(buf, ": 0 brokers, 1 topics")) {
                rd_atomic32_add(&log_hits->metadata, 1);
        } else if (!strcmp(fac, "REBOOTSTRAP") &&
                 strstr(buf, "Starting re-bootstrap sequence")) {
                rd_atomic32_add(&log_hits->rebootstrap, 1);
        }

        TEST_SAY("%s [%d] [%s] %s\n", rd_kafka_name(rk), level, fac, buf);
}

/**
 * @brief Rebootstrap should not be cancelled if a metadata request
 *        returned no brokers.
 */
static void do_test_rebootstrap_after_metadata_no_brokers(void) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        struct log_hits_s log_hits;

        SUB_TEST_QUICK();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_follower(mcluster, topic, 0, 2);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "metadata.recovery.strategy", "rebootstrap");
        /* Should re-bootstrap immediately regardless of this config */
        test_conf_set(conf, "metadata.recovery.rebootstrap.trigger.ms", "3600000");
        test_conf_set(conf, "debug", "metadata");

        rd_atomic32_init(&log_hits.metadata, 0);
        rd_atomic32_init(&log_hits.rebootstrap, 0);
        rd_kafka_conf_set_log_cb(conf, log_cb);
        rd_kafka_conf_set_opaque(conf, &log_hits);

        rk = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_assign_partition("assign", rk, topic, 0,
                                       RD_KAFKA_OFFSET_INVALID);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 1,
                                 "bootstrap.servers", bootstraps, NULL);
        test_consumer_poll_timeout("read 1", rk, 0, -1, -1, 1,
                                   NULL, 1000);

        TEST_ASSERT(rd_atomic32_get(&log_hits.metadata) == 0,
                    "Expected no empty metadata responses, got %d",
                    rd_atomic32_get(&log_hits.metadata));

        TEST_ASSERT(rd_atomic32_get(&log_hits.rebootstrap) == 0,
                    "Expected no re-bootstraps, got %d",
                    rd_atomic32_get(&log_hits.rebootstrap));

        /* Return no brokers in Metadata responses */
        rd_kafka_mock_broker_count_override(mcluster, 0);

        /* Trigger refresh */
        rd_kafka_mock_broker_set_down(mcluster, 2);
        rd_kafka_mock_partition_set_follower(mcluster, topic, 0, 3);

        /* Wait some time for seeing the re-bootstrap */
        rd_usleep(200 * 1000, NULL);

        TEST_ASSERT(rd_atomic32_get(&log_hits.metadata) > 0,
                    "Expected at least 1 empty metadata response");

        TEST_ASSERT(rd_atomic32_get(&log_hits.rebootstrap) > 0,
                    "Expected at least 1 re-bootstrap");

        /* Restore brokers for Metadata responses */
        rd_kafka_mock_broker_count_override(mcluster, -1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 0,
                                 "bootstrap.servers", bootstraps, NULL);
        test_consumer_poll_timeout("read 2", rk, 0, -1, -1, 1,
                                   NULL, 2000);

        test_consumer_close(rk);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0154_rebootstrap_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_rebootstrap_after_metadata_no_brokers();

        return 0;
}
