/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019-2022, Magnus Edenhill
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
 * @name Verify that the builtin mock cluster works by producing to a topic
 *       and then consuming from it.
 */



int main_0009_mock_cluster(int argc, char **argv) {
        const char *topic = test_mk_topic_name("0009_mock_cluster", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *p, *c;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        mcluster = test_mock_cluster_new(1, &bootstraps);

        test_conf_init(&conf, NULL, 30);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        TEST_SAY("TEST has Started!!!!\n");
        /* Producer */
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        p = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));
        rkt = test_create_producer_topic(p, topic, NULL);
        
        // First test the produce Retry Error
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Produce, 2,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);
        test_produce_msgs(p, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1, "hello",
                          5);
        rd_sleep(5000);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        TEST_SAY(request_cnt);
        for(int i=0; i < request_cnt;i++){
            TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
        }

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(p);

        test_mock_cluster_destroy(mcluster);
        return 0;

        /* Initial Test 0009 */
        // const char *topic = test_mk_topic_name("0009_mock_cluster", 1);
        // rd_kafka_mock_cluster_t *mcluster;
        // rd_kafka_t *p, *c;
        // rd_kafka_topic_t *rkt;
        // rd_kafka_conf_t *conf;
        // const int msgcnt = 100;
        // const char *bootstraps;
        // rd_kafka_topic_partition_list_t *parts;

        // if (test_needs_auth()) {
        //         TEST_SKIP("Mock cluster does not support SSL/SASL\n");
        //         return 0;
        // }

        // mcluster = test_mock_cluster_new(3, &bootstraps);

        // test_conf_init(&conf, NULL, 30);

        // test_conf_set(conf, "bootstrap.servers", bootstraps);

        // /* Producer */
        // rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        // p = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));

        // /* Consumer */
        // test_conf_set(conf, "auto.offset.reset", "earliest");
        // c = test_create_consumer(topic, NULL, conf, NULL);

        // rkt = test_create_producer_topic(p, topic, NULL);

        // /* Produce */
        // test_produce_msgs(p, rkt, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, NULL, 0);
        // /* Produce tiny messages */
        // test_produce_msgs(p, rkt, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, "hello",
        //                   5);

        // rd_kafka_topic_destroy(rkt);

        // /* Assign */
        // parts = rd_kafka_topic_partition_list_new(1);
        // rd_kafka_topic_partition_list_add(parts, topic, 0);
        // rd_kafka_topic_partition_list_add(parts, topic, 1);
        // rd_kafka_topic_partition_list_add(parts, topic, 2);
        // rd_kafka_topic_partition_list_add(parts, topic, 3);
        // test_consumer_assign("CONSUME", c, parts);
        // rd_kafka_topic_partition_list_destroy(parts);


        // /* Consume */
        // test_consumer_poll("CONSUME", c, 0, -1, 0, msgcnt, NULL);

        // rd_kafka_destroy(c);
        // rd_kafka_destroy(p);

        // test_mock_cluster_destroy(mcluster);

        // return 0;
}
