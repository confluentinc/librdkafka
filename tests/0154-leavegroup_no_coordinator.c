/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * @name Test LeaveGroup handling when coordinator becomes unavailable
 *
 * This test verifies that rd_kafka_cgrp_handle_LeaveGroup() correctly handles
 * the case where the coordinator broker (rkb) is NULL when the consumer group
 * initiates LeaveGroup during shutdown.
 *
 * Previously, rd_kafka_dbg() calls in the error path would dereference
 * rkb->rkb_rk when rkb was NULL, causing a SIGSEGV crash.
 *
 * The fix uses the always-valid `rk` parameter instead of `rkb->rkb_rk`.
 */

static int allowed_error;

static int
error_is_fatal_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, const char *reason) {
        if (err == allowed_error || err == RD_KAFKA_RESP_ERR__TRANSPORT ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN ||
            err == RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE ||
            err == RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP) {
                TEST_SAY("Ignoring allowed error: %s: %s\n",
                         rd_kafka_err2name(err), reason);
                return 0;
        }
        return 1;
}

/**
 * @brief Test that consumer close/destroy handles missing coordinator
 * gracefully.
 *
 * Scenario:
 * 1. Create a consumer and subscribe to a topic
 * 2. Wait for consumer to join group and establish coordinator connection
 * 3. Make coordinator unavailable (broker goes down)
 * 4. Close/destroy the consumer, which triggers LeaveGroup
 * 5. Verify no crash occurs (the bug would cause SIGSEGV here)
 */
static void do_test_leavegroup_no_coordinator(void) {
        rd_kafka_t *consumer;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_message_t *rkm;

        SUB_TEST();

        test_curr->is_fatal_cb = error_is_fatal_cb;
        allowed_error          = RD_KAFKA_RESP_ERR__TRANSPORT;

        mcluster = test_mock_cluster_new(2, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 2);
        rd_kafka_mock_group_initial_rebalance_delay_ms(mcluster, 0);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 1);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", topic);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");

        consumer = test_create_consumer(topic, NULL, conf, NULL);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);
        TEST_CALL_ERR__(rd_kafka_subscribe(consumer, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        TEST_SAY("Waiting for consumer to join group and get assignment\n");
        rkm = rd_kafka_consumer_poll(consumer, 10000);
        if (rkm)
                rd_kafka_message_destroy(rkm);

        TEST_SAY(
            "Simulating coordinator failure by making broker unavailable\n");
        rd_kafka_mock_broker_set_down(mcluster, 1);

        rd_sleep(1);

        TEST_SAY(
            "Destroying consumer - this triggers LeaveGroup "
            "with coordinator unavailable\n");
        rd_kafka_destroy(consumer);

        TEST_SAY("Consumer destroyed successfully (no crash)\n");

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;
        allowed_error          = RD_KAFKA_RESP_ERR_NO_ERROR;

        SUB_TEST_PASS();
}

int main_0154_leavegroup_no_coordinator(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_leavegroup_no_coordinator();

        return 0;
}
