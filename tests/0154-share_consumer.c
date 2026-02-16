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

int main_0154_share_consumer(int argc, char **argv) {
        char errstr[512];
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_partition_list_t *topics;
        char *topic =
            "test-topic";  // test_mk_topic_name("0154-share-consumer", 0);
        char *group = "test-group-0";

        test_create_topic_wait_exists(NULL, topic, 3, -1, 60 * 1000);
        rd_sleep(5);

        test_produce_msgs_easy(topic, 0, 0, 2);

        TEST_SAY("Creating share consumer\n");
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", NULL, 0);
        rd_kafka_conf_set(conf, "group.protocol", "consumer", NULL, 0);
        rd_kafka_conf_set(conf, "group.id", group, NULL, 0);
        rd_kafka_conf_set(conf, "debug", "cgrp,protocol,conf", NULL, 0);

        // rk = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                TEST_FAIL("Failed to create share consumer: %s\n", errstr);
        }

        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(rk, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        TEST_SAY("Share consumer created successfully\n");

        rd_kafka_consumer_poll(rk, 65000);

        TEST_SAY("Destroying consumer\n");

        /* Clean up */
        rd_kafka_destroy(rk);
        return 0;
}
