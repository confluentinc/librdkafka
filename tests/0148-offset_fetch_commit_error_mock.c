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
 * @brief A stale member error during an OffsetFetch should cause
 *        to retry the operation just after next ConsumerGroupHeartbeat
 *        response.
 *        The offset fetch eventually succeeds and the consumer can
 *        start from the committed offset.
 */
void do_test_OffsetFetch_stale_member_error(rd_kafka_mock_cluster_t *mcluster,
                                            const char *bootstraps) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_t *producer, *first_consumer, *second_consumer;
        rd_kafka_conf_t *conf, *producer_conf;
        uint64_t testid  = test_id_generate();
        const int msgcnt = 5;
        test_msgver_t mv;

        SUB_TEST_QUICK();

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        /* Producer */
        producer_conf = rd_kafka_conf_dup(conf);
        rd_kafka_conf_set_dr_msg_cb(producer_conf, test_dr_msg_cb);
        producer = test_create_handle(RD_KAFKA_PRODUCER, producer_conf);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 2);
        test_produce_msgs2(producer, topic, testid, 0, 0, msgcnt, NULL, 0);
        rd_kafka_flush(producer, -1);

        /* Consumer */
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "group.protocol", "consumer");
        first_consumer =
            test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        test_consumer_subscribe(first_consumer, topic);
        test_consumer_poll("before consume error", first_consumer, testid, -1,
                           0, msgcnt, NULL);
        test_consumer_close(first_consumer);

        /* Produce again */
        test_produce_msgs2(producer, topic, testid, 0, msgcnt, msgcnt, NULL, 0);
        rd_kafka_flush(producer, -1);

        /* Set OffsetFetch errors */
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetFetch, 5,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH);

        /* Consume again*/
        test_msgver_init(&mv, testid);
        second_consumer = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(second_consumer, topic);
        test_consumer_poll("receive second batch", second_consumer, testid, -1,
                           msgcnt, msgcnt, &mv);
        test_msgver_verify("verify second batch", &mv, TEST_MSGVER_ALL, msgcnt,
                           msgcnt);
        test_msgver_clear(&mv);
        test_consumer_close(second_consumer);

        /* Destroy */
        rd_kafka_destroy(first_consumer);
        rd_kafka_destroy(second_consumer);
        rd_kafka_destroy(producer);

        SUB_TEST_PASS();
}

/**
 * @brief Doing a manual commits that returns error \p expected_err
 *        should return the error to the caller, even if the error
 *        is a partition level error.
 *        These errors aren't retried.
 *
 *        Variations:
 *
 *        - variation 0: commit stored offsets passing NULL
 *        - variation 1: commit passed offsets
 */
void do_test_OffsetCommit_manual_error(rd_kafka_mock_cluster_t *mcluster,
                                       const char *bootstraps,
                                       rd_kafka_resp_err_t expected_err,
                                       int variation) {
        rd_kafka_t *consumer;
        test_msgver_t mv;
        rd_kafka_conf_t *conf;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        uint64_t testid   = test_id_generate();
        const int msgcnt  = 5;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *to_commit = NULL;

        SUB_TEST_QUICK();

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "group.protocol", "consumer");

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, testid, 0, 0, msgcnt, 0,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Consume same messages */
        consumer = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(consumer, topic);
        test_msgver_init(&mv, testid);
        test_consumer_poll("receive first batch", consumer, testid, -1, 0,
                           msgcnt, &mv);
        test_msgver_verify("verify first batch", &mv, TEST_MSGVER_ALL, 0,
                           msgcnt);
        test_msgver_clear(&mv);

        /* Set OffsetCommit errors */
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 1,
                                          expected_err);

        if (variation == 1) {
                /* Variation 1: pass offsets to commit */
                to_commit = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(to_commit, topic, 0)->offset =
                    msgcnt;
        }

        /* Sync commit */
        err = rd_kafka_commit(consumer, to_commit, rd_false);
        TEST_ASSERT(err == expected_err, "Expected error %s, got %s",
                    rd_kafka_err2name(expected_err), rd_kafka_err2name(err));
        if (to_commit) {
                TEST_ASSERT(to_commit->elems[0].err == expected_err,
                            "Expected error %s for partition 0, got %s",
                            rd_kafka_err2name(expected_err),
                            rd_kafka_err2name(to_commit->elems[0].err));
                /* Reset error code to retry */
                to_commit->elems[0].err = RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        /* Retry it, this time it should work */
        err = rd_kafka_commit(consumer, to_commit, rd_false);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected error %s, got %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR_NO_ERROR),
                    rd_kafka_err2name(err));
        if (to_commit) {
                TEST_ASSERT(to_commit->elems[0].err ==
                                RD_KAFKA_RESP_ERR_NO_ERROR,
                            "Expected error %s for partition 0, got %s",
                            rd_kafka_err2name(RD_KAFKA_RESP_ERR_NO_ERROR),
                            rd_kafka_err2name(to_commit->elems[0].err));
        }

        RD_IF_FREE(to_commit, rd_kafka_topic_partition_list_destroy);

        rd_kafka_destroy(consumer);

        SUB_TEST_PASS();
}

/**
 * @brief When a partition is revoked, with auto-commit enabled,
 *        if the RPC returns STALE_MEMBER_EPOCH for one of the
 *        partitions, it should be retried until the member is
 *        fenced.
 *
 *        Variations:
 *
 *        during_revocation: the auto-commit is triggered by a revocation,
 *                           otherwise it's triggered by the consumer close.
 *        session_times_out: Session times out giving UNKNOWN_MEMBER_ID,
 *                           otherwise commit succeeds after
 *                           last STALE_MEMBER_EPOCH.
 *                           When session times out the auto-commit fails
 *                           and messages are consumed again.
 *
 *        - variation 0: during_revocation=false, session_times_out=false
 *        - variation 1: during_revocation=false, session_times_out=true
 *        - variation 2: during_revocation=true, session_times_out=false
 *        - variation 3: during_revocation=true, session_times_out=true
 */
void do_test_OffsetCommit_automatic_stale_member(
    rd_kafka_mock_cluster_t *mcluster,
    const char *bootstraps,
    int variation) {
        rd_kafka_t *consumer;
        test_msgver_t mv;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *target_assignment_partitions;
        const char *topic           = test_mk_topic_name(__FUNCTION__, 1);
        uint64_t testid             = test_id_generate();
        const int msgcnt            = 5;
        rd_bool_t during_revocation = (variation / 2) == 1;
        rd_bool_t session_times_out = (variation % 2) == 1;

        SUB_TEST_QUICK("during_revocation=%s, session_times_out=%s",
                       RD_STR_ToF(during_revocation),
                       RD_STR_ToF(session_times_out));

        rd_kafka_mock_topic_create(mcluster, topic, 1, 2);
        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 1);
        rd_kafka_mock_set_default_session_timeout(mcluster, 10000);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "group.protocol", "consumer");

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, testid, 0, 0, msgcnt, 0,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Consume same messages */
        consumer =
            test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        test_consumer_subscribe(consumer, topic);
        test_msgver_init(&mv, testid);
        test_consumer_poll_exact("receive first batch", consumer, testid, -1, 0,
                                 msgcnt, rd_true, &mv);
        test_msgver_verify("verify first batch", &mv, TEST_MSGVER_ALL, 0,
                           msgcnt);
        test_msgver_clear(&mv);


        rd_kafka_mock_clear_request_errors(mcluster, RD_KAFKAP_OffsetCommit);

        /* First sequence of stale member epoch for 2 s */
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 4,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                                          RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH);

        if (session_times_out) {
                /* Simulate a session timeout after that */
                rd_kafka_mock_push_request_errors(
                    mcluster, RD_KAFKAP_OffsetCommit, 4,
                    RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
                    RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
                    RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
                    RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID);
        }

        if (during_revocation) {
                /* Changing target assignment to partition 0 only,
                 * partition revoked and automatically committed,
                 * but the commit fails. */
                target_assignment_partitions =
                    rd_kafka_topic_partition_list_new(2);
                rd_kafka_topic_partition_list_add(target_assignment_partitions,
                                                  topic, 0);
                test_mock_cluster_member_assignment(
                    mcluster, 1, consumer, target_assignment_partitions);
                rd_kafka_topic_partition_list_destroy(
                    target_assignment_partitions);
                rd_sleep(1);
        }

        /* Otherwise partition is committed before leaving the group */
        rd_kafka_destroy(consumer);


        /* Reset mock assignor to automatic */
        rd_kafka_mock_cgrp_consumer_target_assignment(mcluster, topic, NULL);

        consumer = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(consumer, topic);

        if (session_times_out) {
                /* Messages are consumed again because the commit failed */
                test_msgver_init(&mv, testid);
                test_consumer_poll_exact("messages consumed again", consumer,
                                         testid, -1, 0, msgcnt, rd_true, &mv);
                test_msgver_verify("messages consumed again", &mv,
                                   TEST_MSGVER_ALL, 0, msgcnt);
                test_msgver_clear(&mv);
        } else {
                /* No message should be consumer after the autocommit */
                test_consumer_poll_no_msgs("no messages", consumer, testid,
                                           200);
        }
        rd_kafka_destroy(consumer);

        SUB_TEST_PASS();
}

int main_0148_offset_fetch_commit_error_mock(int argc, char **argv) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        int i;

        TEST_SKIP_MOCK_CLUSTER(0);

        if (!test_consumer_group_protocol_consumer()) {
                TEST_SKIP(
                    "Test meaningful only with Consumer Group 'Consumer' "
                    "protocol\n");
                return 0;
        }

        mcluster = test_mock_cluster_new(3, &bootstraps);

        do_test_OffsetFetch_stale_member_error(mcluster, bootstraps);

        for (i = 0; i < 2; i++) {
                do_test_OffsetCommit_manual_error(
                    mcluster, bootstraps, RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH,
                    i);
                do_test_OffsetCommit_manual_error(
                    mcluster, bootstraps, RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
                    i);
        }

        for (i = 0; i < 4; i++)
                do_test_OffsetCommit_automatic_stale_member(mcluster,
                                                            bootstraps, i);

        test_mock_cluster_destroy(mcluster);

        return 0;
}
