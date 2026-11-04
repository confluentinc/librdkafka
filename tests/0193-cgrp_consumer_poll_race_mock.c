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
 * @name The application thread must not access the consumer group state
 *       when polling a `group.protocol=consumer` (KIP-848) consumer.
 *
 * The cgrp is owned by the main thread, which updates `rkcg_flags` without
 * a lock on every ConsumerGroupHeartbeat response. Polling used to read
 * those flags, and expedite the next heartbeat after an exceeded
 * `max.poll.interval.ms`, from the application thread.
 *
 * The test polls while heartbeats are frequent, then exceeds
 * `max.poll.interval.ms` and polls again until the member is assigned the
 * partition again. Run with TSAN (`./dev-conf.sh tsan`) and
 * TEST_CONSUMER_GROUP_PROTOCOL=consumer to detect the data race: there is
 * no functional symptom to assert on.
 */


/**
 * @brief Poll \p c until it has a non-empty assignment or \p timeout_ms
 *        elapses.
 *
 * @returns rd_true if the consumer has an assignment.
 */
static rd_bool_t poll_until_assigned(rd_kafka_t *c, int timeout_ms) {
        rd_kafka_topic_partition_list_t *assignment;
        rd_kafka_message_t *rkm;
        int64_t deadline   = test_clock() + (int64_t)timeout_ms * 1000;
        rd_bool_t assigned = rd_false;

        while (!assigned && test_clock() < deadline) {
                rkm = rd_kafka_consumer_poll(c, 10);
                if (rkm)
                        rd_kafka_message_destroy(rkm);

                assignment = NULL;
                if (!rd_kafka_assignment(c, &assignment))
                        assigned = assignment->cnt > 0;
                RD_IF_FREE(assignment, rd_kafka_topic_partition_list_destroy);
        }

        return assigned;
}


static void do_test_poll_during_heartbeats(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        rd_kafka_message_t *rkm;
        int64_t deadline;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_set_group_consumer_heartbeat_interval_ms(mcluster, 100);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "max.poll.interval.ms", "2000");
        c = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(c, topic);

        TEST_ASSERT(poll_until_assigned(c, tmout_multip(10000)),
                    "Timed out waiting for the initial assignment");

        TEST_SAY("Polling during frequent heartbeats\n");
        deadline = test_clock() + 2 * 1000000;
        while (test_clock() < deadline) {
                rkm = rd_kafka_consumer_poll(c, 10);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        TEST_SAY("Exceeding max.poll.interval.ms\n");
        rd_sleep(3);

        TEST_SAY("Polling until the member is assigned again\n");
        TEST_ASSERT(poll_until_assigned(c, tmout_multip(10000)),
                    "Timed out waiting for the assignment after polling "
                    "again following an exceeded max.poll.interval.ms");

        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_cgrp_consumer_poll_race_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        if (test_consumer_group_protocol_classic()) {
                TEST_SKIP("Test only for group.protocol=consumer\n");
                return 0;
        }

        do_test_poll_during_heartbeats();

        return 0;
}
