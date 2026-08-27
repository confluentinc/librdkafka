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
 * @name KIP-394: keep the member id assigned by the broker
 *
 * A JoinGroup response that arrives after the client has left the join state
 * it was sent in is discarded. A MEMBER_ID_REQUIRED response carries the
 * member id the broker assigned to us, so discarding it made the client join
 * again without a member id and be assigned a second one, leaving the broker
 * holding the first as a pending member that blocks the group's rebalance
 * until its session timeout expires.
 *
 * The subscription is changed while the initial JoinGroup response is delayed
 * by the mock broker, which is what makes that response outdated. What is
 * verified is that the client asks the broker for a member id only once.
 */

#define JOINGROUP_RTT_MS 1000

static size_t joingroup_request_cnt(rd_kafka_mock_cluster_t *mcluster) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt, i, cnt = 0;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                if (rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_JoinGroup)
                        cnt++;
        }
        rd_kafka_mock_request_destroy_array(requests, request_cnt);

        return cnt;
}

static void do_test_member_id_required(const char *assignor) {
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const char *bootstraps;
        const char *topic1 = "rdkafkatest_0191_first";
        const char *topic2 = "rdkafkatest_0191_second";
        rd_kafka_topic_partition_list_t *subscription, *expected;
        size_t joingroup_cnt;

        SUB_TEST_QUICK("%s", assignor);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        /* Elect as soon as the member has joined. */
        rd_kafka_mock_group_initial_rebalance_delay_ms(mcluster, 0);

        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic1, 1, 1));
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic2, 1, 1));

        /* Delay the response to the initial JoinGroup, the one the broker
         * answers with MEMBER_ID_REQUIRED and a new member id, so that the
         * subscription can change while it is in flight. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_JoinGroup, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            JOINGROUP_RTT_MS);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", "0191_member_id_required");
        test_conf_set(conf, "partition.assignment.strategy", assignor);

        rk = test_create_consumer(NULL, NULL, conf, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);

        test_consumer_subscribe(rk, topic1);

        /* Wait for the JoinGroup to be in flight. */
        while (joingroup_request_cnt(mcluster) < 1)
                test_consumer_poll_no_msgs("wait JoinGroup", rk, 0, 100);

        TEST_SAY("Changing the subscription while JoinGroup is in flight\n");
        subscription = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subscription, topic1,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subscription, topic2,
                                          RD_KAFKA_PARTITION_UA);
        TEST_CALL_ERR__(rd_kafka_subscribe(rk, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        expected = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(expected, topic1, 0);
        rd_kafka_topic_partition_list_add(expected, topic2, 0);
        test_consumer_wait_assignment_topic_partition_list(rk, rd_true,
                                                           expected, 10000);
        rd_kafka_topic_partition_list_destroy(expected);

        /* One JoinGroup without a member id, and one with the member id the
         * broker assigned in response to it. A third means the assigned
         * member id was discarded along with the outdated response. */
        joingroup_cnt = joingroup_request_cnt(mcluster);
        TEST_ASSERT(joingroup_cnt == 2,
                    "Expected 2 JoinGroup requests, not %" PRIusz
                    ": the member id assigned by the broker was discarded "
                    "with the outdated response",
                    joingroup_cnt);

        rd_kafka_mock_stop_request_tracking(mcluster);

        test_consumer_close(rk);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0191_joingroup_member_id_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        if (!test_consumer_group_protocol_classic()) {
                TEST_SKIP("Requires the classic group protocol\n");
                return 0;
        }

        do_test_member_id_required("range");
        do_test_member_id_required("cooperative-sticky");

        return 0;
}
