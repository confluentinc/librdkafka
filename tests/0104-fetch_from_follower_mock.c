/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019, Magnus Edenhill
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
 * @name Fetch from follower tests using the mock broker.
 */


/**
 * @brief Test offset reset / ListOffsets when fetching from replica.
 *        The ListOffsets request needs to be sent to the leader.
 */
static int do_test_offset_reset (const char *auto_offset_reset) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *topic = "test";
        const int msgcnt = 1000;
        const size_t msgsize = 10000;

        TEST_SAY(_C_MAG "[ Test FFF auto.offset.reset=%s ]\n",
                 auto_offset_reset);

        mcluster = test_mock_cluster_new(3, &bootstraps);

        /* Seed the topic with messages */
        test_produce_msgs_easy2(bootstraps, topic, 0, 0, 0, msgcnt, msgsize);

        /* Set partition leader to broker 1, follower to broker 2 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_follower(mcluster, topic, 0, 2);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", auto_offset_reset);
        /* Make sure we don't consume the entire partition in one Fetch */
        test_conf_set(conf, "fetch.message.max.bytes", "100");

        c = test_create_consumer("mygroup", NULL, conf, NULL);

        /* The first fetch will go to the leader which will redirect
         * the consumer to the follower, the second and sub-sequent fetches
         * will go to the follower. We want the third fetch, second one on
         * the follower, to fail and trigger an offset reset. */
        rd_kafka_mock_push_request_errors(
                mcluster,
                1/*FetchRequest*/,
                3,
                RD_KAFKA_RESP_ERR_NO_ERROR /*leader*/,
                RD_KAFKA_RESP_ERR_NO_ERROR /*follower*/,
                RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE /*follower: fail*/);

        test_consumer_assign_partition(auto_offset_reset, c, topic, 0,
                                       RD_KAFKA_OFFSET_INVALID);

        if (!strcmp(auto_offset_reset, "latest"))
                test_consumer_poll_no_msgs(auto_offset_reset, c, 0, 5000);
        else
                test_consumer_poll(auto_offset_reset, c, 0, 1, 0,
                                   msgcnt, NULL);

        test_consumer_close(c);

        test_mock_cluster_destroy(mcluster);

        return 0;

}



int main_0104_fetch_from_follower_mock (int argc, char **argv) {
        do_test_offset_reset("earliest");
        do_test_offset_reset("latest");

        return 0;
}
