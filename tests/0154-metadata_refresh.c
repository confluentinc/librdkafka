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

#include <stdio.h>
#include <string.h>
#include "test.h"
#include "rdkafka.h"

/**
 * Integration test for KIP-848 partition metadata refresh:
 * - Create topic with 2 partitions
 * - Start consumer group and verify initial assignment
 * - Increase partition count to 4
 * - Reset log tracking variables after partition creation
 * - Wait for HeartbeatRequest, HeartbeatResponse, and metadata refresh logs
 * - Assert that metadata refresh is triggered for new partitions
 */

// Globals to track log sequence
static volatile int seen_heartbeat_req  = 0;
static volatile int seen_heartbeat_resp = 0;
static volatile int seen_metadata_log   = 0;

static void reset_log_tracking(void) {
        seen_heartbeat_req  = 0;
        seen_heartbeat_resp = 0;
        seen_metadata_log   = 0;
}

static void wait_for_metadata_refresh_log(int timeout_ms) {
        int elapsed = 0;
        while (elapsed < timeout_ms && !seen_metadata_log) {
                rd_usleep(500 * 1000, NULL);  // 500 ms
                elapsed += 500;
        }
        TEST_ASSERT(
            seen_heartbeat_req,
            "Expected HeartbeatRequest log not seen after partition creation");
        TEST_ASSERT(
            seen_heartbeat_resp,
            "Expected HeartbeatResponse log not seen after partition creation");
        TEST_ASSERT(seen_metadata_log,
                    "Expected metadata refresh log not seen after partition "
                    "creation and heartbeat");
}

// Custom log callback to capture and process librdkafka logs
static void test_metadata_log_cb(const rd_kafka_t *rk,
                                 int level,
                                 const char *fac,
                                 const char *buf) {
        if (strstr(buf, "Sent ConsumerGroupHeartbeatRequest")) {
                seen_heartbeat_req = 1;
        }
        if (seen_heartbeat_req &&
            strstr(buf, "Received ConsumerGroupHeartbeatResponse")) {
                seen_heartbeat_resp = 1;
        }
        if (seen_heartbeat_resp &&
            strstr(buf,
                   "Partition assigned to this consumer is not present in "
                   "cached metadata")) {
                seen_metadata_log = 1;
        }
}

static rd_kafka_t *create_consumer(
    const char *topic,
    const char *group) {
        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "debug", "cgrp, protocol");
        rd_kafka_conf_set_log_cb(conf, test_metadata_log_cb);
        rd_kafka_t *consumer = test_create_consumer(topic, NULL, conf, NULL);
        return consumer;
}

static void setup_and_run_metadata_refresh_test(void) {
        const char *topic      = test_mk_topic_name("cgrp_metadata", 1);
        int initial_partitions = 2;
        int new_partitions     = 4;
        rd_kafka_t *c1, *c2, *rk;
        const char *group = "grp_metadata";

        SUB_TEST_QUICK();

        TEST_SAY("Creating topic %s with %d partitions\n", topic,
                 initial_partitions);
        test_create_topic(NULL, topic, initial_partitions, 1);

        TEST_SAY("Creating consumers\n");
        c1 = create_consumer(topic, group);
        c2 = create_consumer(topic, group);

        rk = test_create_handle(RD_KAFKA_PRODUCER, NULL);

        TEST_SAY("Subscribing to topic %s\n", topic);
        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);

        // Wait for initial assignment
        test_consumer_wait_assignment(c1, rd_false);
        test_consumer_wait_assignment(c2, rd_false);

        // Create new partitions
        TEST_SAY("Increasing partition count to %d\n", new_partitions);
        test_create_partitions(rk, topic, new_partitions);

        // Reset log tracking variables to only consider logs after partition
        // creation
        reset_log_tracking();

        // Wait for expected logs for up to 10 seconds
        wait_for_metadata_refresh_log(10000);

        TEST_SAY("Closing consumers\n");
        test_consumer_close(c1);
        test_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        SUB_TEST_PASS();
}

int main_0154_metadata_refresh(int argc, char **argv) {
        if (!test_consumer_group_protocol_classic())
                setup_and_run_metadata_refresh_test();
        return 0;
}