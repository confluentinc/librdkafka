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
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
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
 * @name Verify that librdkafka can complete a classic consumer group
 *       session (FindCoordinator, JoinGroup, SyncGroup, Heartbeat,
 *       LeaveGroup) when the mock coordinator only accepts exactly the
 *       raised protocol version ceiling for each of those APIs.
 *
 * These ceilings were raised to bring each API's max version up to the
 * Apache Kafka Java client's max version, which has used KIP-482
 * flexible-version framing for these APIs since the 2.8.0 release.
 * Pinning the mock broker to accept only the new version (rather than the
 * usual 0..max range) proves librdkafka actually emits, and correctly
 * parses the response of, that exact version end-to-end rather than just
 * negotiating down to a version it already spoke before.
 */
static void do_test_group_protocol_version(const char *what,
                                           int16_t ApiKey,
                                           int16_t ApiVersion) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *topics;

        SUB_TEST_QUICK("%s (ApiKey %" PRId16 " pinned to v%" PRId16 ")", what,
                       ApiKey, ApiVersion);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, "topic", 1, 1);

        TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(mcluster, ApiKey,
                                                     ApiVersion, ApiVersion));

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");

        rk = test_create_consumer(what, NULL, conf, NULL);

        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, "topic",
                                          RD_KAFKA_PARTITION_UA);
        TEST_CALL_ERR__(rd_kafka_subscribe(rk, topics));
        rd_kafka_topic_partition_list_destroy(topics);

        test_consumer_wait_assignment(rk, rd_true);

        /* Let a couple of heartbeats go through before leaving. */
        rd_sleep(2);

        TEST_CALL_ERR__(rd_kafka_consumer_close(rk));
        rd_kafka_destroy(rk);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0191_flexver_group_protocol_mock(int argc, char **argv) {
        do_test_group_protocol_version("Heartbeat flexible version",
                                       RD_KAFKAP_Heartbeat, 4);
        do_test_group_protocol_version("FindCoordinator flexible version",
                                       RD_KAFKAP_FindCoordinator, 3);
        do_test_group_protocol_version("LeaveGroup flexible version",
                                       RD_KAFKAP_LeaveGroup, 4);
        do_test_group_protocol_version("SyncGroup KIP-559 version",
                                       RD_KAFKAP_SyncGroup, 5);
        do_test_group_protocol_version("JoinGroup flexible version",
                                       RD_KAFKAP_JoinGroup, 7);
        return 0;
}
