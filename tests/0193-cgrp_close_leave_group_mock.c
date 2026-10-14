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
 * @name A consumer that holds no partitions must still leave the group on
 *       rd_kafka_consumer_close().
 *
 * With the classic protocol and a cooperative assignor there is nothing to
 * revoke for such a member, so the close path reaches
 * rd_kafka_cgrp_revoke_all_rejoin() with an empty assignment. That used to
 * return early when terminating without ever acting on
 * RD_KAFKA_CGRP_F_LEAVE_ON_UNASSIGN_DONE, so no LeaveGroupRequest was sent
 * and the coordinator only evicted the member after `session.timeout.ms`,
 * stalling every rebalance in the group until then.
 *
 * With an eager assignor the same close enqueues an (absolute) revoke of the
 * empty assignment, which ends up in rd_kafka_cgrp_unassign_done() and does
 * send the leave; that path is covered here too as a regression guard and to
 * show parity between the two assignors.
 */

static rd_bool_t is_leavegroup_request(rd_kafka_mock_request_t *request,
                                       void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_LeaveGroup;
}


/**
 * @brief Whether \p c has joined the group, i.e. the coordinator has handed
 *        it a member id. An empty assignment on its own does not tell the two
 *        apart: a consumer that has not joined yet has one as well.
 */
static rd_bool_t member_has_joined(rd_kafka_t *c) {
        char *memberid = rd_kafka_memberid(c);
        rd_bool_t joined;

        joined = memberid && *memberid;

        if (memberid)
                rd_free(memberid);

        return joined;
}


/**
 * @brief Poll \p c1 and \p c2 until both have joined the group, exactly one
 *        of them owns the single partition of the subscribed topic and the
 *        other one owns nothing, and return the one with the empty
 *        assignment.
 */
static rd_kafka_t *
poll_until_one_member_is_idle(rd_kafka_t *c1, rd_kafka_t *c2, int timeout_ms) {
        int64_t tmout = test_clock() + ((int64_t)timeout_ms * 1000);

        while (test_clock() < tmout) {
                rd_kafka_topic_partition_list_t *a1, *a2;
                rd_kafka_t *idle = NULL;

                test_consumer_poll_once(c1, NULL, 100);
                test_consumer_poll_once(c2, NULL, 100);

                if (!member_has_joined(c1) || !member_has_joined(c2))
                        continue;

                TEST_CALL_ERR__(rd_kafka_assignment(c1, &a1));
                TEST_CALL_ERR__(rd_kafka_assignment(c2, &a2));

                if (a1->cnt == 1 && a2->cnt == 0)
                        idle = c2;
                else if (a1->cnt == 0 && a2->cnt == 1)
                        idle = c1;

                rd_kafka_topic_partition_list_destroy(a1);
                rd_kafka_topic_partition_list_destroy(a2);

                if (idle)
                        return idle;
        }

        TEST_FAIL(
            "Timed out waiting for both members to join with one of them "
            "owning the partition and the other one idle");
        return NULL;
}


/**
 * @brief Two members share a single-partition topic, so one of them ends up
 *        with an empty assignment. Closing that idle member must send a
 *        LeaveGroupRequest, and must do so without waiting out
 *        `session.timeout.ms`.
 */
static void do_test_leave_group_on_close_of_idle_member(const char *assignor) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c1, *c2, *idle, *survivor;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t leave_cnt;
        int64_t close_start, close_duration_ms;

        SUB_TEST("%s", assignor);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "partition.assignment.strategy", assignor);
        test_conf_set(conf, "session.timeout.ms",
                      tsprintf("%d", session_timeout_ms));
        test_conf_set(conf, "heartbeat.interval.ms", "500");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        c1 = test_create_consumer(groupid, NULL, rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);

        idle     = poll_until_one_member_is_idle(c1, c2, 30 * 1000);
        survivor = idle == c1 ? c2 : c1;

        TEST_SAY("Member %s has an empty assignment, closing it\n",
                 rd_kafka_name(idle));

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        close_start = test_clock();
        TEST_CALL_ERR__(rd_kafka_consumer_close(idle));
        close_duration_ms = (test_clock() - close_start) / 1000;

        leave_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_leavegroup_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_SAY("close() of the idle member took %" PRId64
                 "ms and sent %" PRIusz " LeaveGroupRequest(s)\n",
                 close_duration_ms, leave_cnt);

        TEST_ASSERT(leave_cnt == 1,
                    "Expected the idle member to send exactly one "
                    "LeaveGroupRequest on close, got %" PRIusz
                    ". Without it the coordinator only evicts the member "
                    "after session.timeout.ms (%dms), stalling every "
                    "rebalance in the group until then.",
                    leave_cnt, session_timeout_ms);

        TEST_ASSERT(close_duration_ms < session_timeout_ms,
                    "Expected close() to return well within "
                    "session.timeout.ms (%dms), took %" PRId64 "ms",
                    session_timeout_ms, close_duration_ms);

        rd_kafka_destroy(idle);

        test_consumer_close(survivor);
        rd_kafka_destroy(survivor);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Same, but the member never had any partition to begin with: it
 *        subscribes to a topic that is already fully owned by the first
 *        member and is closed right after it has joined.
 *
 * This covers the close of a member whose assignment is empty from the very
 * first SyncGroup, rather than one that was assigned partitions earlier.
 */
static void do_test_leave_group_on_close_of_never_assigned_member(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c1, *c2;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t leave_cnt;
        int64_t close_start, close_duration_ms;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms",
                      tsprintf("%d", session_timeout_ms));
        test_conf_set(conf, "heartbeat.interval.ms", "500");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        c1 = test_create_consumer(groupid, NULL, rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(groupid, NULL, conf, NULL);

        /* Let c1 take the only partition before c2 joins. */
        test_consumer_subscribe(c1, topic);
        test_consumer_wait_assignment(c1, rd_true /*poll*/);

        test_consumer_subscribe(c2, topic);
        (void)poll_until_one_member_is_idle(c1, c2, 30 * 1000);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        close_start = test_clock();
        TEST_CALL_ERR__(rd_kafka_consumer_close(c2));
        close_duration_ms = (test_clock() - close_start) / 1000;

        leave_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_leavegroup_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_SAY("close() took %" PRId64 "ms and sent %" PRIusz
                 " LeaveGroupRequest(s)\n",
                 close_duration_ms, leave_cnt);

        TEST_ASSERT(leave_cnt == 1,
                    "Expected exactly one LeaveGroupRequest on close, "
                    "got %" PRIusz,
                    leave_cnt);

        TEST_ASSERT(close_duration_ms < session_timeout_ms,
                    "Expected close() to return well within "
                    "session.timeout.ms (%dms), took %" PRId64 "ms",
                    session_timeout_ms, close_duration_ms);

        rd_kafka_destroy(c2);

        test_consumer_close(c1);
        rd_kafka_destroy(c1);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_cgrp_close_leave_group_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        if (!test_consumer_group_protocol_classic()) {
                TEST_SKIP(
                    "Tests are only for group.protocol=classic: "
                    "`partition.assignment.strategy` does not apply to "
                    "group.protocol=consumer\n");
                return 0;
        }

        do_test_leave_group_on_close_of_idle_member("cooperative-sticky");
        do_test_leave_group_on_close_of_idle_member("range");
        do_test_leave_group_on_close_of_never_assigned_member();

        return 0;
}
