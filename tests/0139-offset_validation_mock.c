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


struct _produce_args {
        const char *topic;
        int sleep;
        rd_kafka_conf_t *conf;
};

static int produce_concurrent_thread(void *args) {
        rd_kafka_t *p1;
        test_curr->exp_dr_err    = RD_KAFKA_RESP_ERR_NO_ERROR;
        test_curr->exp_dr_status = RD_KAFKA_MSG_STATUS_PERSISTED;

        struct _produce_args *produce_args = args;
        rd_sleep(produce_args->sleep);

        p1 = test_create_handle(RD_KAFKA_PRODUCER, produce_args->conf);
        TEST_CALL_ERR__(
            rd_kafka_producev(p1, RD_KAFKA_V_TOPIC(produce_args->topic),
                              RD_KAFKA_V_VALUE("hi", 2), RD_KAFKA_V_END));
        rd_kafka_flush(p1, -1);
        rd_kafka_destroy(p1);
        return 0;
}

/**
 * @brief Send a produce request in the middle of an offset validation
 *        and expect that the fetched message is discarded, don't producing
 *        a duplicate when state becomes active again. See #4249.
 */
static void do_test_no_duplicates_during_offset_validation(void) {
        const char *topic      = test_mk_topic_name(__FUNCTION__, 1);
        const char *c1_groupid = topic;
        rd_kafka_t *c1;
        rd_kafka_conf_t *conf, *conf_producer;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        int initial_msg_count = 5;
        thrd_t thrd;
        struct _produce_args args = RD_ZERO_INIT;
        uint64_t testid           = test_id_generate();

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* Slow down OffsetForLeaderEpoch so a produce and
         * subsequent fetch can happen while it's in-flight */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetForLeaderEpoch, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, 5000);

        test_conf_init(&conf_producer, NULL, 60);
        test_conf_set(conf_producer, "bootstrap.servers", bootstraps);


        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, testid, 0, 0, initial_msg_count, 10,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "1", NULL);

        args.topic = topic;
        /* Makes that the message is produced while an offset validation
         * is ongoing */
        args.sleep = 5;
        args.conf  = conf_producer;
        /* Spin up concurrent thread */
        if (thrd_create(&thrd, produce_concurrent_thread, (void *)&args) !=
            thrd_success)
                TEST_FAIL("Failed to create thread");

        test_conf_init(&conf, NULL, 60);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        /* Makes that an offset validation happens at the same
         * time a new message is being produced */
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "5000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "enable.auto.offset.store", "false");
        test_conf_set(conf, "enable.partition.eof", "true");

        c1 = test_create_consumer(c1_groupid, NULL, conf, NULL);
        test_consumer_subscribe(c1, topic);

        /* Consume initial messages */
        test_consumer_poll("MSG_INIT", c1, testid, 0, 0, initial_msg_count,
                           NULL);
        /* EOF after initial messages */
        test_consumer_poll("MSG_EOF", c1, testid, 1, initial_msg_count, 0,
                           NULL);
        /* Concurrent producer message and EOF */
        test_consumer_poll("MSG_AND_EOF", c1, testid, 1, initial_msg_count, 1,
                           NULL);
        /* Only an EOF, not a duplicate message */
        test_consumer_poll("MSG_EOF2", c1, testid, 1, initial_msg_count, 0,
                           NULL);

        thrd_join(thrd, NULL);

        rd_kafka_destroy(c1);

        test_mock_cluster_destroy(mcluster);

        TEST_LATER_CHECK();
        SUB_TEST_PASS();
}

int main_0139_offset_validation_mock(int argc, char **argv) {

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_no_duplicates_during_offset_validation();

        return 0;
}
