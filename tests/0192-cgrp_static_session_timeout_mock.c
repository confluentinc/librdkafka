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
 * @name Static group member (KIP-345) hitting a client-side session timeout.
 *
 * When no Heartbeat response arrives within `session.timeout.ms` the classic
 * consumer times itself out in rd_kafka_cgrp_session_timeout_check(), which
 * resets the member id to "" and revokes the assignment as lost.
 *
 * The offsets of a lost assignment must not be committed: the member no
 * longer owns those partitions. On top of that, for a static member the
 * commit is actively harmful, since an OffsetCommit carrying a
 * GroupInstanceId with an empty MemberId is answered with
 * ERR_FENCED_INSTANCE_ID by a real broker, which librdkafka raises as a
 * fatal (unrecoverable) consumer error.
 */

static rd_bool_t is_offset_commit_request(rd_kafka_mock_request_t *request,
                                          void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_OffsetCommit;
}


/**
 * @brief A static member whose Heartbeat response is stalled past
 *        `session.timeout.ms` must not commit the offsets of the assignment
 *        it just lost.
 */
static void do_test_no_commit_of_lost_assignment(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t offset_commit_cnt;
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        /* Static group membership */
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        /* No rebalance callback: the revoke is then handled by the internal
         * unassign, which is the path that clears the assignment-lost flag
         * before the removed partitions are served. */
        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Consume a couple of messages so that there is a stored offset,
         * without it there is nothing to commit on revoke. */
        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Stall a single Heartbeat response for longer than
         * session.timeout.ms. The mock broker marks the member as active
         * when it handles the request and only the response is delayed, so
         * the member stays alive on the broker while the client times the
         * session out on its own. */
        TEST_SAY(
            "Stalling Heartbeat response for %dms "
            "(> session.timeout.ms %dms)\n",
            session_timeout_ms + 3000, session_timeout_ms);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_Heartbeat, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            session_timeout_ms + 3000);

        /* Poll past the session timeout so that the client revokes the
         * assignment as lost and the internal unassign serves the removed
         * partitions. */
        TEST_SAY("Polling past the session timeout\n");
        tmout = test_clock() + ((session_timeout_ms + 3000) * 1000);
        while (test_clock() < tmout)
                test_consumer_poll_once(c, NULL, 1000);

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        TEST_SAY("%" PRIusz
                 " OffsetCommit request(s) sent after the "
                 "session timeout\n",
                 offset_commit_cnt);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, "
                    "but %" PRIusz
                    " were sent. "
                    "The member id is reset to \"\" by the session timeout "
                    "while group.instance.id is still set, so a real broker "
                    "answers FENCED_INSTANCE_ID and the consumer goes fatal.",
                    offset_commit_cnt);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


int main_0192_cgrp_static_session_timeout_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        if (!test_consumer_group_protocol_classic()) {
                /* rkcg_ts_session_timeout is only maintained by the classic
                 * protocol, so the client-side session timeout cannot
                 * trigger under group.protocol=consumer. */
                TEST_SKIP("Test only for group.protocol=classic\n");
                return 0;
        }

        do_test_no_commit_of_lost_assignment();

        return 0;
}
