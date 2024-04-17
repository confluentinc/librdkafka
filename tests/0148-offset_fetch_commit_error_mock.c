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

#include <stdarg.h>

/**
 * @brief TODO
 */
void do_test_OffsetFetch_StaleMemberError(void) {
        const char *topic = test_mk_topic_name("test_of", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *producer, *first_consumer, *second_consumer;
        rd_kafka_conf_t *conf, *producer_conf;
        uint64_t test_id = test_id_generate();
        const int msgcnt = 5;
        const char *bootstraps;
        int i;
        rd_kafka_message_t *rkmessage;

        mcluster = test_mock_cluster_new(3, &bootstraps);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        /* Producer */
        producer_conf = rd_kafka_conf_dup(conf);
        rd_kafka_conf_set_dr_msg_cb(producer_conf, test_dr_msg_cb);
        producer = test_create_handle(RD_KAFKA_PRODUCER, producer_conf);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 2);
        test_produce_msgs2(producer, topic, test_id, 0, 0, msgcnt, NULL, 0);
        rd_kafka_flush(producer, -1);

        /* Consumer */
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "group.protocol", "consumer");
        first_consumer =
            test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        test_consumer_subscribe(first_consumer, topic);
        test_consumer_poll("BEFORE_ERROR_CONSUME", first_consumer, test_id, -1,
                           0, msgcnt, NULL);
        test_consumer_close(first_consumer);

        /* Produce again */
        test_produce_msgs2(producer, topic, test_id, 0, 0, msgcnt, NULL, 0);
        rd_kafka_flush(producer, -1);

        /* Set OffsetFetch Error */
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetFetch, 5,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH);

        /* Consume again*/
        second_consumer = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(second_consumer, topic);
        for (i = 0; i < msgcnt; i++) {
                rkmessage = rd_kafka_consumer_poll(second_consumer,
                                                   tmout_multip(10 * 1000));
                TEST_ASSERT(rkmessage->offset == msgcnt + i,
                            "Incorrect Offset for the consumed message.");
                rd_kafka_message_destroy(rkmessage);
        }
        test_consumer_close(second_consumer);

        /* Destroy */
        rd_kafka_destroy(first_consumer);
        rd_kafka_destroy(second_consumer);
        rd_kafka_destroy(producer);

        test_mock_cluster_destroy(mcluster);
}

int main_0148_offset_fetch_commit_error_mock(int argc, char **argv) {

        TEST_SKIP_MOCK_CLUSTER(0);

        if (!test_consumer_group_protocol_consumer()) {
                TEST_SKIP(
                    "Test meaningful only with Consumer Group 'Consumer' "
                    "protocol\n");
                return 0;
        }

        do_test_OffsetFetch_StaleMemberError();

        return 0;
}