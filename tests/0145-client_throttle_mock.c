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
#include "../src/rdkafka_proto.h"
#include "../src/rdkafka_mock.h"

#include <rdtime.h>


static void free_mock_requests(rd_kafka_mock_request_t **requests,
                               size_t request_cnt) {
        size_t i;
        for (i = 0; i < request_cnt; i++)
                rd_kafka_mock_request_destroy(requests[i]);
        rd_free(requests);
}

static void test_produce(rd_kafka_mock_cluster_t *mcluster,
                         const char *topic,
                         rd_kafka_conf_t *conf) {
        rd_kafka_t *producer;
        rd_kafka_topic_t *rkt;
        SUB_TEST();
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt      = test_create_producer_topic(producer, topic, NULL);

        rd_ts_t start_throttle_time = rd_clock();
        rd_kafka_mock_broker_set_throttle_ms(mcluster, -1, 3000);
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1,
                          "hello", 5);
        rd_sleep(1);

        rd_kafka_mock_start_request_tracking(mcluster);
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 20,
                          "hello", 5);
        size_t req_cnt;
        rd_kafka_mock_request_t** requests =
        rd_kafka_mock_get_requests(mcluster, &req_cnt);
        rd_kafka_mock_stop_request_tracking(mcluster);
        for (size_t i=0; i<req_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         " TimeDiff: %" PRId64 "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time);
                TEST_ASSERT(rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time > 2500*1000,
                         "Expect to handle request all delay more than 2500ms, but now %" PRId64,
                         rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time);
        }
        free_mock_requests(requests, req_cnt);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(producer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

static void test_fetch(rd_kafka_mock_cluster_t *mcluster,
                                         const char *topic,
                                         rd_kafka_conf_t *conf) {
        rd_kafka_t *consumer;
        rd_kafka_message_t *rkm;
        SUB_TEST();
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, conf, NULL);

        test_consumer_subscribe(consumer, topic);

        rd_ts_t start_throttle_time = rd_clock();
        rd_kafka_mock_broker_set_throttle_ms(mcluster, -1, 3000);
        rkm = rd_kafka_consumer_poll(consumer, 1 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_kafka_mock_clear_requests(mcluster);

        rd_kafka_mock_start_request_tracking(mcluster);

        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);

        size_t req_cnt;
        rd_kafka_mock_request_t** requests =
        rd_kafka_mock_get_requests(mcluster, &req_cnt);
        rd_kafka_mock_stop_request_tracking(mcluster);
        for (size_t i=0; i<req_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         " TimeDiff: %" PRId64 "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time);
                TEST_ASSERT(rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time > 2500*1000,
                         "Expect to handle request all delay more than 2500ms, but now %" PRId64,
                         rd_kafka_mock_request_timestamp(requests[i]) - start_throttle_time);
        }
        free_mock_requests(requests, req_cnt);

        rd_kafka_destroy(consumer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/**
 * @brief KIP219 client throttle
 * test produce&fetch request in this case
 */
int main_0145_client_throttle_mock(int argc, char **argv) {
        const char *topic = test_mk_topic_name("topic", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL.\n");
                return 0;
        }
        mcluster = test_mock_cluster_new(1, &bootstraps);
        // rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "-1");
        // flush for every message, so produce request will be send immediately
        test_conf_set(conf, "batch.num.messages", "1");

        test_produce(mcluster, topic, rd_kafka_conf_dup(conf));
        test_fetch(mcluster, topic, rd_kafka_conf_dup(conf));

        test_mock_cluster_destroy(mcluster);
        rd_kafka_conf_destroy(conf);
        return 0;
}
