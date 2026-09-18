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
 * @name Static group member (KIP-345) losing its assignment on the classic
 *       protocol, via either client-side trigger that resets the member id
 *       and revokes the assignment as lost: a session timeout
 *       (rd_kafka_cgrp_session_timeout_check()) or an exceeded
 *       `max.poll.interval.ms`
 * (rd_kafka_cgrp_max_poll_interval_check_tmr_cb()).
 *
 * The offsets of a lost assignment must not be committed: the member no
 * longer owns those partitions. On top of that, for a static member the
 * commit is actively harmful, since an OffsetCommit carrying a
 * GroupInstanceId with an empty MemberId is answered with
 * ERR_FENCED_INSTANCE_ID by a real broker, which librdkafka raises as a
 * fatal (unrecoverable) consumer error.
 *
 * Covered here, for both the incremental (cooperative-sticky) and the
 * absolute (eager) unassign path:
 *  - no OffsetCommit is sent for the lost assignment, under either trigger;
 *  - losing the assignment is not permanent: once the member rejoins, a
 *    subsequent commit succeeds again.
 *
 * Also covered below: a subscribed topic disappearing from the cluster's
 * metadata as a third, protocol-agnostic trigger (rd_kafka_cgrp_metadata_
 * update_check()); the same guarantees from inside an application
 * rebalance callback; for the periodic auto-commit timer; on
 * rd_kafka_consumer_close() / rd_kafka_unsubscribe(); for a dynamic
 * (non-static) member; for an explicit rd_kafka_commit(); for redelivery
 * after rejoin; for a poisoned FENCED_INSTANCE_ID response; and for a
 * genuine broker-side eviction rather than a merely delayed response.
 */

static rd_bool_t is_offset_commit_request(rd_kafka_mock_request_t *request,
                                          void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_OffsetCommit;
}


static rd_bool_t is_leavegroup_request(rd_kafka_mock_request_t *request,
                                       void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_LeaveGroup;
}


/**
 * @brief Verify that losing the assignment is not a permanent condition:
 *        once the member has rejoined, the assignment must no longer be
 *        seen as lost, and a manual commit must succeed again.
 *
 * Whatever is left of the seed messages after the trigger and the rejoin
 * above is not a reliable base for this check: depending on timing, the
 * discard loops that poll through the trigger may have already drained
 * all of it, so a fresh message is produced here rather than relying on
 * one still being unconsumed.
 */
static void verify_recovery_after_lost_assignment(rd_kafka_t *c,
                                                  const char *bootstraps,
                                                  const char *topic) {
        int64_t tmout;
        rd_kafka_resp_err_t err;

        TEST_SAY("Waiting for the member to rejoin\n");
        tmout = test_clock() + (15 * 1000000);
        while (test_clock() < tmout && rd_kafka_assignment_lost(c))
                test_consumer_poll_once(c, NULL, 1000);

        TEST_ASSERT(!rd_kafka_assignment_lost(c),
                    "Expected the assignment to no longer be lost "
                    "after the member rejoined");

        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 10, "bootstrap.servers",
                                 bootstraps, NULL);

        test_consumer_poll("post-recovery", c, 0, -1, 0, 1, NULL);

        err = rd_kafka_commit(c, NULL, 0 /*sync*/);
        TEST_ASSERT(!err,
                    "Expected commit to succeed once the member has "
                    "rejoined, got: %s",
                    rd_kafka_err2str(err));
}


/**
 * @brief Stall a single Heartbeat response for longer than
 *        session.timeout.ms and poll for long enough for the client to
 *        time the session out on its own and revoke the assignment as
 *        lost. Shared by the subtests below that don't need any finer
 *        control over the two steps than do_test_no_commit_of_lost_
 *        assignment() (and its siblings above, left as-is) take inline.
 *
 * Deliberately does not try to catch rd_kafka_assignment_lost() as true:
 * with no rebalance callback installed, the internal default unassign that
 * follows the revoke runs synchronously on the background thread, with
 * nothing left to block on, so the flag can already be cleared again
 * (rd_kafka_cgrp_incr_unassign_done()) before a poll-and-check loop ever
 * gets scheduled to observe it - the set and the clear can land in the same
 * internal pass. That is not a bug: it is exactly what "clear the lost flag
 * once the unassign completes" means when there is nothing asynchronous
 * left to wait for. Callers below only need the trigger to have fired, not
 * to catch it in the act, and check that through its effects instead - no
 * OffsetCommit sent, and a successful recovery once the member rejoins.
 * The one subtest that does need to observe the flag as true,
 * do_test_manual_commit_returns_assignment_lost(), uses a rebalance
 * callback and its own wait loop instead (see the comment there).
 */
static void trigger_session_timeout(rd_kafka_mock_cluster_t *mcluster,
                                    rd_kafka_t *c,
                                    int session_timeout_ms) {
        int64_t tmout;

        TEST_SAY(
            "Stalling Heartbeat response for %dms "
            "(> session.timeout.ms %dms)\n",
            session_timeout_ms + 3000, session_timeout_ms);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_Heartbeat, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            session_timeout_ms + 3000);

        TEST_SAY("Polling past the session timeout\n");
        tmout = test_clock() + ((session_timeout_ms + 3000) * 1000);
        while (test_clock() < tmout)
                test_consumer_poll_once(c, NULL, 1000);
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
        /* Cooperative rebalancing: the revoke is an incremental unassign.
         * The assignment-lost flag is kept set across the unassign so that the
         * revoke-time commit of the removed partitions is skipped, and is
         * cleared only once the removal completes
         * (rd_kafka_cgrp_incr_unassign_done, or
         * rd_kafka_cgrp_consumer_incr_unassign_done under KIP-848). */
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        /* No rebalance callback: the revoke is then handled by the internal
         * unassign, which keeps the assignment-lost flag set while the removed
         * partitions are served (so their offsets are not committed) and
         * clears it once the removal completes. */
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

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Same as do_test_no_commit_of_lost_assignment() but with an eager
 *        assignor: the revoke this triggers is an absolute unassign
 *        (rd_kafka_cgrp_unassign() / rd_kafka_cgrp_unassign_done()) rather
 *        than the incremental one. With no rebalance callback and no
 *        EVENT_REBALANCE handler, rd_kafka_rebalance_op() calls
 *        rd_kafka_cgrp_unassign() directly for the EAGER protocol.
 */
static void do_test_no_commit_of_lost_assignment_eager(void) {
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
        /* Eager rebalancing: the revoke is an absolute unassign, unlike
         * do_test_no_commit_of_lost_assignment()'s cooperative-sticky. */
        test_conf_set(conf, "partition.assignment.strategy", "range");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        /* No rebalance callback: the revoke is then handled by the internal
         * unassign (rd_kafka_cgrp_unassign()), which keeps the
         * assignment-lost flag set while the removed partitions are served
         * (so their offsets are not committed) and clears it once the
         * removal completes (rd_kafka_cgrp_unassign_done()). */
        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Consume a couple of messages so that there is a stored offset,
         * without it there is nothing to commit on revoke. */
        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Stall a single Heartbeat response for longer than
         * session.timeout.ms, same as do_test_no_commit_of_lost_assignment().
         */
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

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Same as do_test_no_commit_of_lost_assignment() but the assignment
 *        is lost by exceeding `max.poll.interval.ms` instead of a session
 *        timeout. rd_kafka_cgrp_max_poll_interval_check_tmr_cb() resets the
 *        member id and revokes the assignment as lost the same way
 *        rd_kafka_cgrp_session_timeout_check() does, and, per KIP-345, does
 *        so without sending a LeaveGroupRequest since this is a static
 *        member.
 */
static void do_test_no_commit_of_lost_assignment_max_poll_interval(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid            = "mygroup";
        const char *topic              = "test";
        const int max_poll_interval_ms = 6000;
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
        /* Cooperative rebalancing, same as
         * do_test_no_commit_of_lost_assignment(): only the trigger differs
         * here. */
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        /* Keep the session timeout well clear of the time this test spends
         * not polling: Heartbeats keep flowing on the internal thread
         * regardless of whether the application calls poll, so only
         * max.poll.interval.ms is exercised here. */
        test_conf_set(conf, "session.timeout.ms", "30000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "max.poll.interval.ms", "6000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        /* No rebalance callback: the revoke is then handled by the internal
         * unassign, which keeps the assignment-lost flag set while the
         * removed partitions are served (so their offsets are not
         * committed) and clears it once the removal completes. */
        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Consume a couple of messages so that there is a stored offset,
         * without it there is nothing to commit on revoke. */
        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Simulate the application getting stuck: just don't call poll.
         * max.poll.interval.ms enforcement is purely client-side/timer
         * driven, so no mock broker trickery is needed to trigger it. */
        TEST_SAY("Not polling for %dms (> max.poll.interval.ms %dms)\n",
                 max_poll_interval_ms + 2000, max_poll_interval_ms);
        rd_sleep((max_poll_interval_ms / 1000) + 2);

        /* Poll, discarding everything (including the app-visible
         * ERR__MAX_POLL_EXCEEDED notification), long enough for the
         * internal revoke to complete. Plain test_consumer_poll_once()
         * can't be used here since it fails the test on any message error,
         * and ERR__MAX_POLL_EXCEEDED is expected. */
        TEST_SAY("Polling past max.poll.interval.ms\n");
        tmout = test_clock() + (6 * 1000000);
        while (test_clock() < tmout) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        TEST_SAY("%" PRIusz
                 " OffsetCommit request(s) sent after max.poll.interval.ms "
                 "was exceeded\n",
                 offset_commit_cnt);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, "
                    "but %" PRIusz
                    " were sent. "
                    "max.poll.interval.ms resets the member id the same way "
                    "a session timeout does, while group.instance.id is "
                    "still set, so a real broker answers FENCED_INSTANCE_ID "
                    "and the consumer goes fatal.",
                    offset_commit_cnt);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief A subscribed topic disappearing from the cluster's metadata is a
 *        third, separate trigger for losing (part of) the assignment,
 *        independent of session.timeout.ms or max.poll.interval.ms:
 *        rd_kafka_cgrp_metadata_update_check() marks the now-nonexistent
 *        topic's partitions as lost and incrementally revokes just those,
 *        while the rest of the assignment is left untouched.
 *
 * Unlike every other trigger in this file, the two call sites of
 * rd_kafka_cgrp_metadata_update_check() are not gated to the classic
 * protocol, so this is the only trigger that also applies under
 * group.protocol=consumer (KIP-848), and this subtest is run under
 * whichever protocol the test invocation is using, before the rest of
 * main() below skips everything else for group.protocol=consumer.
 */
static void do_test_no_commit_of_lost_assignment_deleted_topic(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid = "mygroup";
        const char *topic_a = "topic_a";
        const char *topic_b = "topic_b";
        size_t offset_commit_cnt;
        int64_t tmout;
        rd_kafka_topic_partition_list_t *assignment;
        rd_bool_t saw_topic_b = rd_false;
        int i;

        SUB_TEST("group.protocol=%s", test_consumer_group_protocol()
                                          ? test_consumer_group_protocol()
                                          : "classic");

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic_a, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);
        test_produce_msgs_easy_v(topic_b, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        /* Static group membership, same as every other subtest in this
         * file, though this particular trigger does not depend on it. */
        test_conf_set(conf, "group.instance.id", "my-instance");
        if (test_consumer_group_protocol_classic()) {
                /* Cooperative rebalancing: rd_kafka_cgrp_metadata_update_
                 * check() only marks the deleted topic's partitions as
                 * lost under the COOPERATIVE protocol.
                 * group.protocol=consumer has no eager/cooperative choice
                 * of its own (its assignment is always incremental), and
                 * forbids setting this and the confs below anyway, so
                 * this is unconditional there. */
                test_conf_set(conf, "partition.assignment.strategy",
                              "cooperative-sticky");
                test_conf_set(conf, "session.timeout.ms", "30000");
                test_conf_set(conf, "heartbeat.interval.ms", "1000");
        }
        /* Short enough that the deleted topic is noticed quickly, without
         * relying on a specific refresh being forced. */
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "3000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe_multi(c, 2, topic_a, topic_b);

        /* Consume from both topics so that there is a stored offset for
         * each. */
        test_consumer_poll("consume", c, 0, -1, 0, 20, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        TEST_SAY("Deleting topic %s\n", topic_a);
        TEST_ASSERT(!rd_kafka_mock_topic_delete(mcluster, topic_a),
                    "Failed to delete topic %s", topic_a);

        /* Poll long enough for the client to pick up the topic's removal
         * on a subsequent metadata refresh, revoke its partitions as
         * lost, and clear the flag again once that revoke completes -
         * all without any application-visible rebalance event, since no
         * rebalance callback is installed. */
        TEST_SAY("Polling past the topic deletion\n");
        tmout = test_clock() + (20 * 1000000);
        while (test_clock() < tmout) {
                /* Not test_consumer_poll_once(): the deleted topic
                 * surfaces as a non-fatal "Unknown topic or partition"
                 * consumer error, which that helper would treat as a
                 * hard test failure. */
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        TEST_ASSERT(!rd_kafka_assignment_lost(c),
                    "Expected the assignment to no longer be lost");

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        TEST_SAY("%" PRIusz
                 " OffsetCommit request(s) sent after the topic deletion\n",
                 offset_commit_cnt);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        for (i = 0; i < assignment->cnt; i++) {
                TEST_ASSERT(strcmp(assignment->elems[i].topic, topic_a) != 0,
                            "Did not expect %s to still be assigned after "
                            "it was deleted",
                            topic_a);
                if (!strcmp(assignment->elems[i].topic, topic_b))
                        saw_topic_b = rd_true;
        }
        TEST_ASSERT(saw_topic_b, "Expected %s to still be assigned", topic_b);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* topic_b was never touched: its consumption and ability to
         * commit must be unaffected throughout. Verify it still works. */
        verify_recovery_after_lost_assignment(c, bootstraps, topic_b);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


static int test2_rebalance_cnt;

/**
 * @brief Rebalance callback for
 *        do_test_rebalance_cb_sees_lost_cleared_before_assign(): performs
 *        the incremental unassign/assign itself and asserts
 *        rd_kafka_assignment_lost() inside both the lost revoke and the
 *        following assign.
 */
static void test2_rebalance_cb(rd_kafka_t *rk,
                               rd_kafka_resp_err_t err,
                               rd_kafka_topic_partition_list_t *parts,
                               void *opaque) {
        test2_rebalance_cnt++;

        TEST_SAY("Rebalance #%d: %s: %d partition(s)\n", test2_rebalance_cnt,
                 rd_kafka_err2name(err), parts->cnt);

        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                /* The very first assign precedes any loss; every
                 * following assign is the recovery from the lost revoke
                 * below and must see the flag already cleared. This is
                 * exactly the property the first version of this fix
                 * broke: it cleared the flag on the *next*
                 * rd_kafka_assign() rather than when the unassign
                 * completed. */
                if (test2_rebalance_cnt > 1)
                        TEST_ASSERT(!rd_kafka_assignment_lost(rk),
                                    "Expected assignment_lost() to already "
                                    "be false inside the recovery assign");
                TEST_CALL_ERROR__(rd_kafka_incremental_assign(rk, parts));
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                /* Rebalance #2 is the session-timeout revoke this test
                 * targets. Any later revoke - such as the ordinary
                 * teardown revoke rd_kafka_consumer_close() triggers below -
                 * is not a lost assignment and must not be asserted as
                 * one. */
                if (test2_rebalance_cnt == 2)
                        TEST_ASSERT(
                            rd_kafka_assignment_lost(rk),
                            "Expected assignment_lost() to be true inside "
                            "the revoke triggered by the session timeout");
                else
                        TEST_ASSERT(!rd_kafka_assignment_lost(rk),
                                    "Expected assignment_lost() to be false "
                                    "inside a revoke that was not caused by "
                                    "the session timeout");
                TEST_CALL_ERROR__(rd_kafka_incremental_unassign(rk, parts));
                break;

        default:
                TEST_FAIL("Unexpected rebalance event: %s",
                          rd_kafka_err2name(err));
        }
}

/**
 * @brief assignment_lost() must be true inside the rebalance callback's
 *        lost REVOKE and false again inside the ASSIGN that follows the
 *        rejoin - even though, unlike every other subtest in this file,
 *        it is the application (not the internal auto-unassign) that
 *        performs the incremental unassign/assign here. This is the
 *        property the first version of this fix broke.
 */
static void do_test_rebalance_cb_sees_lost_cleared_before_assign(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t offset_commit_cnt;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        test2_rebalance_cnt    = 0;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, test2_rebalance_cb, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Polls unconditionally for a fixed window past session.timeout.ms,
         * which is what actually drives the queued revoke to
         * test2_rebalance_cb() - its REVOKE_PARTITIONS assertion runs as a
         * side effect. (A "while lost" loop doesn't work here: right after
         * the stall is armed the flag is still false, so it would exit
         * without polling at all.) */
        trigger_session_timeout(mcluster, c, session_timeout_ms);

        /* Now wait for the recovery ASSIGN_PARTITIONS side of the callback -
         * and its own assertion - to run too, before checking the
         * OffsetCommit count. Waiting on the lost flag itself doesn't work
         * here: the revoke callback's own incremental_unassign() call just
         * cleared it synchronously, before this loop even started, the same
         * way the internal default unassign does for the no-callback tests
         * (see trigger_session_timeout() above) - so "while lost" would exit
         * without polling at all and the queued assign would never get
         * dispatched. Wait for the side effect that actually matters
         * instead: a third rebalance event. Request tracking must stop right
         * after, before verify_recovery_after_lost_assignment() below runs
         * its own legitimate post-recovery commit - otherwise that commit
         * would be counted against the "no commit while lost" assertion
         * too. */
        TEST_SAY("Waiting for the recovery assign\n");
        {
                int64_t tmout = test_clock() + (15 * 1000000);
                while (test_clock() < tmout && test2_rebalance_cnt < 3)
                        test_consumer_poll_once(c, NULL, 1000);
                TEST_ASSERT(!rd_kafka_assignment_lost(c),
                            "Expected the assignment to no longer be lost "
                            "after the member rejoined");
        }

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);
        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        TEST_ASSERT(test2_rebalance_cnt >= 3,
                    "Expected at least 3 rebalance events (initial assign, "
                    "lost revoke, recovery assign), saw %d",
                    test2_rebalance_cnt);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief The periodic auto-commit timer must also be suppressed while the
 *        assignment is lost - not just the one-shot revoke-time commit -
 *        and must resume firing on its own, unprompted, once the member
 *        has rejoined.
 */
static void do_test_no_auto_commit_timer_while_lost(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid               = "mygroup";
        const char *topic                 = "test";
        const int session_timeout_ms      = 6000;
        const int auto_commit_interval_ms = 2000;
        size_t offset_commit_cnt;
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Short enough that, without the fix, the timer would get more
         * than one chance to misfire during the windows checked below. */
        test_conf_set(conf, "auto.commit.interval.ms", "2000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        /* Tracking starts only now, once the assignment is already known
         * to be lost: any commit seen from here on, whether the one-shot
         * revoke-time commit or a misfiring timer tick, must be
         * suppressed. Commits from before this point, while the
         * assignment was still healthy, are expected and irrelevant. */
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        TEST_SAY("Polling for %dms while the assignment is lost\n",
                 2 * auto_commit_interval_ms);
        tmout = test_clock() + (2 * auto_commit_interval_ms * 1000);
        while (test_clock() < tmout)
                test_consumer_poll_once(c, NULL, 1000);

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);
        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit while the assignment is "
                    "lost, including from the auto-commit timer "
                    "(auto.commit.interval.ms=%dms), but %" PRIusz " were sent",
                    auto_commit_interval_ms, offset_commit_cnt);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        /* Prove the timer itself has resumed on its own, unprompted -
         * verify_recovery_after_lost_assignment() only proved a manual
         * commit works again. That manual commit is also why a fresh
         * message needs to be produced and consumed here first: it already
         * committed the current position, and with nothing new stored past
         * the last commit, rd_kafka_cgrp_offsets_commit() (see set_offsets
         * in rd_kafka_topic_partition_list_set_offsets(), which leaves an
         * unadvanced partition's offset invalid) has nothing valid left to
         * commit and silently skips sending a request - the timer would
         * still be firing every auto.commit.interval.ms, just with nothing
         * to do. */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 10, "bootstrap.servers",
                                 bootstraps, NULL);
        test_consumer_poll("post-recovery-2", c, 0, -1, 0, 1, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);
        offset_commit_cnt = 0;

        TEST_SAY("Waiting for the auto-commit timer to fire on its own\n");
        tmout = test_clock() + (3 * auto_commit_interval_ms * 1000);
        while (test_clock() < tmout && !offset_commit_cnt) {
                test_consumer_poll_once(c, NULL, 1000);
                offset_commit_cnt = test_mock_get_matching_request_cnt(
                    mcluster, is_offset_commit_request, NULL);
        }
        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt > 0,
                    "Expected the auto-commit timer to resume firing on "
                    "its own after the member rejoined, saw none within "
                    "%dms",
                    3 * auto_commit_interval_ms);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief rd_kafka_consumer_close() while the assignment is lost must not
 *        hang, must not send a commit with the empty member id, and must
 *        not raise a fatal error.
 */
static void do_test_close_while_lost(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t offset_commit_cnt;
        char errstr[512];

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        TEST_SAY("Closing the consumer while the assignment is lost\n");
        test_consumer_close(c);

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit on close for the lost "
                    "assignment, but %" PRIusz " were sent",
                    offset_commit_cnt);

        TEST_ASSERT(!rd_kafka_fatal_error(c, errstr, sizeof(errstr)),
                    "Expected no fatal error, got: %s", errstr);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief rd_kafka_unsubscribe() while the assignment is lost must not send
 *        a commit with the empty member id, and - since the session timeout
 *        already reset the member id to "" precisely to avoid an
 *        ERR_UNKNOWN_MEMBER_ID on the next join
 *        (rd_kafka_cgrp_session_timeout_check(), src/rdkafka_cgrp.c) - must
 *        not send a LeaveGroupRequest either: rd_kafka_cgrp_unsubscribe()
 *        only requests a leave when RD_KAFKA_CGRP_HAS_JOINED() is true,
 *        which for the classic protocol requires a non-empty member id.
 *        There is nothing valid to leave with until the member rejoins.
 */
static void do_test_unsubscribe_while_lost(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t offset_commit_cnt;
        size_t leavegroup_cnt;
        char errstr[512];
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        TEST_SAY("Unsubscribing while the assignment is lost\n");
        TEST_CALL_ERR__(rd_kafka_unsubscribe(c));

        tmout = test_clock() + (10 * 1000000);
        while (test_clock() < tmout) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);
        leavegroup_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_leavegroup_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_SAY("%" PRIusz " OffsetCommit, %" PRIusz
                 " LeaveGroup request(s) sent after unsubscribing\n",
                 offset_commit_cnt, leavegroup_cnt);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        /* No LeaveGroupRequest either: the session timeout already reset
         * the member id to "" (to avoid an ERR_UNKNOWN_MEMBER_ID on the
         * next join), so RD_KAFKA_CGRP_HAS_JOINED() is false and
         * rd_kafka_cgrp_unsubscribe() has nothing valid to leave with. */
        TEST_ASSERT(leavegroup_cnt == 0,
                    "Expected no LeaveGroupRequest while the member id is "
                    "still invalidated from the lost assignment, but "
                    "%" PRIusz " were sent",
                    leavegroup_cnt);

        TEST_ASSERT(!rd_kafka_fatal_error(c, errstr, sizeof(errstr)),
                    "Expected no fatal error, got: %s", errstr);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Same as do_test_no_commit_of_lost_assignment() but for a dynamic
 *        member (no group.instance.id): the commit must be suppressed
 *        regardless of static membership, even though only a static
 *        member is fatally fenced by a real broker for it.
 */
static void do_test_no_commit_of_lost_assignment_dynamic_member(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        size_t offset_commit_cnt;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        /* No group.instance.id, unlike every other subtest in this file:
         * this is a dynamic member. */
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);
        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief With enable.auto.commit=false, a manual rd_kafka_commit(NULL)
 *        while the assignment is lost must return _ASSIGNMENT_LOST
 *        synchronously and non-fatally, then succeed again after rejoin.
 *        Static variant of 0106-cgrp_sess_timeout.c's
 *        do_test_commit_on_lost(), pinned to the exact error code rather
 *        than just any error.
 *
 * Needs a rebalance callback and its own wait loop, unlike every other
 * subtest in this file: with no callback, the internal default unassign
 * runs synchronously on the background thread the moment the session times
 * out, clearing the lost flag again before application code gets a chance
 * to observe it (see the comment on trigger_session_timeout() above). With
 * a callback, the revoke instead waits as a queued event until the
 * application dispatches it via poll() - the incremental_unassign() call
 * that clears the flag only happens inside that dispatch. So as long as
 * this test avoids polling while it waits, the flag stays observably true
 * for as long as needed, the same way 0106's do_test_commit_on_lost()
 * relies on an rd_sleep()-only wait loop rather than one that polls.
 */
static void do_test_manual_commit_returns_assignment_lost(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        rd_kafka_resp_err_t err;
        char errstr[512];
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        c = test_create_consumer(groupid, test_rebalance_cb, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        TEST_SAY(
            "Stalling Heartbeat response for %dms "
            "(> session.timeout.ms %dms)\n",
            session_timeout_ms + 3000, session_timeout_ms);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_Heartbeat, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            session_timeout_ms + 3000);

        /* Not test_consumer_poll_once() / rd_kafka_consumer_poll(): polling
         * would dispatch the queued revoke to the rebalance callback above,
         * whose incremental_unassign() call clears the lost flag as soon as
         * it completes - before this test gets to see it set. rd_sleep()
         * lets the session time out and the flag get set on the background
         * thread without ever giving the callback a chance to run. */
        TEST_SAY("Waiting for the assignment to be lost, without polling\n");
        tmout = test_clock() + ((session_timeout_ms + 5000) * 1000);
        while (test_clock() < tmout && !rd_kafka_assignment_lost(c))
                rd_sleep(1);

        TEST_ASSERT(rd_kafka_assignment_lost(c),
                    "Expected the assignment to be lost after the session "
                    "timeout");

        err = rd_kafka_commit(c, NULL, 0 /*sync*/);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST,
                    "Expected rd_kafka_commit() to return %s while the "
                    "assignment is lost, got %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST),
                    rd_kafka_err2name(err));

        TEST_ASSERT(!rd_kafka_fatal_error(c, errstr, sizeof(errstr)),
                    "Expected _ASSIGNMENT_LOST to not be fatal, got: %s",
                    errstr);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Same as do_test_no_commit_of_lost_assignment_max_poll_interval()
 *        but with an eager assignor: the last missing cell of the
 *        trigger (session timeout / max.poll.interval.ms) x assignor
 *        (eager / cooperative) matrix - the other three cells are covered
 *        by the tests above.
 */
static void do_test_no_commit_of_lost_assignment_eager_max_poll_interval(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid            = "mygroup";
        const char *topic              = "test";
        const int max_poll_interval_ms = 6000;
        size_t offset_commit_cnt;
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        /* Eager rebalancing: the revoke is an absolute unassign, unlike
         * do_test_no_commit_of_lost_assignment_max_poll_interval()'s
         * cooperative-sticky. */
        test_conf_set(conf, "partition.assignment.strategy", "range");
        test_conf_set(conf, "session.timeout.ms", "30000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "max.poll.interval.ms", "6000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        TEST_SAY("Not polling for %dms (> max.poll.interval.ms %dms)\n",
                 max_poll_interval_ms + 2000, max_poll_interval_ms);
        rd_sleep((max_poll_interval_ms / 1000) + 2);

        TEST_SAY("Polling past max.poll.interval.ms\n");
        tmout = test_clock() + (6 * 1000000);
        while (test_clock() < tmout) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


static int test3_rebalance_cnt;

/**
 * @brief Rebalance callback for
 *        do_test_max_poll_interval_lost_during_pending_rebalance():
 *        performs the incremental unassign/assign itself and asserts
 *        rd_kafka_assignment_lost() inside the revoke that carries the
 *        partition given up to the second member.
 */
static void test3_rebalance_cb(rd_kafka_t *rk,
                               rd_kafka_resp_err_t err,
                               rd_kafka_topic_partition_list_t *parts,
                               void *opaque) {
        test3_rebalance_cnt++;

        TEST_SAY("Rebalance #%d: %s: %d partition(s)\n", test3_rebalance_cnt,
                 rd_kafka_err2name(err), parts->cnt);

        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                TEST_CALL_ERROR__(rd_kafka_incremental_assign(rk, parts));
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                /* Rebalance #2 is the one this test targets: the revoke
                 * triggered by consumer B joining, still queued and
                 * undelivered when max.poll.interval.ms separately expired
                 * on A. Any other revoke - such as the ordinary teardown
                 * revoke rd_kafka_consumer_close() triggers below - is not
                 * the one under test. */
                if (test3_rebalance_cnt == 2)
                        TEST_ASSERT(
                            rd_kafka_assignment_lost(rk),
                            "Expected assignment_lost() to be true inside "
                            "the revoke that was pending when "
                            "max.poll.interval.ms expired");
                TEST_CALL_ERROR__(rd_kafka_incremental_unassign(rk, parts));
                break;

        default:
                TEST_FAIL("Unexpected rebalance event: %s",
                          rd_kafka_err2name(err));
        }
}

/**
 * @brief The two max.poll.interval.ms / session.timeout.ms triggers above
 *        only ever fire against an idle member: nothing else is under way
 *        when the trigger runs, so rd_kafka_cgrp_revoke_all_rejoin_maybe()
 *        falls straight through to rd_kafka_cgrp_revoke_all_rejoin() and
 *        sets the lost flag itself.
 *
 *        The far more common shape in production is the trigger landing
 *        while an ordinary rebalance - caused by a second member joining
 *        the group, not by any loss - is already under way and still
 *        waiting on this application to call poll(). The early return in
 *        rd_kafka_cgrp_revoke_all_rejoin_maybe() for that case ("a
 *        rebalance is already in progress, don't start a second one")
 *        drops the assignment_lost=true it was called with on the floor:
 *        the revoke that was already queued is delivered to the
 *        application - and to rd_kafka_assignment_serve_removals()'s
 *        revoke-time commit - as an ordinary, not-lost one.
 *
 *        Reproduced here with a genuine second member: consumer A starts
 *        out owning both partitions of a 2-partition topic; consumer B
 *        then joins the same group, which is what leaves A with a
 *        REVOKE_PARTITIONS op queued and its join-state inside
 *        RD_KAFKA_CGRP_REBALANCING() - without A itself ever being polled,
 *        the same way the other max.poll.interval.ms subtests above rely
 *        on Heartbeats flowing on the internal thread regardless of
 *        polling. While A is still not polled, A's own
 *        max.poll.interval.ms separately elapses on top of that: the
 *        exact overlap this test targets.
 */
static void do_test_max_poll_interval_lost_during_pending_rebalance(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c, *c2;
        const char *groupid            = "mygroup";
        const char *topic              = "test";
        const int max_poll_interval_ms = 6000;
        size_t offset_commit_cnt;
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        test3_rebalance_cnt    = 0;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        /* Two partitions so that consumer B joining actually requires A to
         * give one up, rather than A keeping everything to itself. */
        rd_kafka_mock_topic_create(mcluster, topic, 2, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 50, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, 50, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "consumer-a");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        /* Keep the session timeout well clear of the time this test spends
         * not polling A, the same way the other max.poll.interval.ms
         * subtests above do: only max.poll.interval.ms is exercised here. */
        test_conf_set(conf, "session.timeout.ms", "30000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "max.poll.interval.ms", "6000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        /* Keep the auto commit interval out of the way so that the only
         * commit that can be observed is the one triggered by the revoke. */
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, test3_rebalance_cb, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Consume from both partitions so that there is a stored offset on
         * the one A is about to give up. */
        test_consumer_poll("consume", c, 0, -1, 0, 20, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        TEST_SAY("Starting consumer B in the same group\n");
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "30000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        c2 = test_create_consumer(groupid, NULL, conf, NULL);
        test_consumer_subscribe(c2, topic);

        /* Not polling A at all: B's own background thread sends its
         * JoinGroup regardless, which is all A needs to hear about on its
         * next Heartbeat - also background-thread-driven - to queue its
         * revoke and land in RD_KAFKA_CGRP_REBALANCING(), where it then
         * stays stuck (a real rebalance callback is registered above, so
         * only a poll of A can dispatch the queued op and move it further).
         * Poll only B here, for long enough to span max.poll.interval.ms on
         * A: B never gets its own share of the partitions until A's revoke
         * actually completes, which can't happen while A isn't being
         * polled, so there is nothing for B's callback-less internal
         * handling to do beyond keep rejoining. */
        TEST_SAY("Not polling A for %dms (> max.poll.interval.ms %dms), "
                 "polling only B\n",
                 max_poll_interval_ms + 2000, max_poll_interval_ms);
        tmout = test_clock() + ((max_poll_interval_ms + 2000) * 1000);
        while (test_clock() < tmout)
                test_consumer_poll_once(c2, NULL, 500);

        /* Poll A past max.poll.interval.ms, discarding everything
         * (including the app-visible ERR__MAX_POLL_EXCEEDED notification):
         * this is what finally dispatches the queued revoke to
         * test3_rebalance_cb() above, whose assertion runs as a side
         * effect. */
        TEST_SAY("Polling A\n");
        tmout = test_clock() + (6 * 1000000);
        while (test_clock() < tmout && test3_rebalance_cnt < 2) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        TEST_ASSERT(test3_rebalance_cnt >= 2,
                    "Expected at least 2 rebalance events (initial assign, "
                    "the revoke pending when max.poll.interval.ms expired), "
                    "saw %d",
                    test3_rebalance_cnt);

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        test_consumer_close(c2);
        rd_kafka_destroy(c2);

        test_consumer_close(c);
        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief The offsets of a lost assignment must never reach the broker as
 *        committed: after losing the assignment and rejoining, the
 *        consumer must resume from the last real commit, not from
 *        whatever was consumed (but not committed) right before the loss.
 */
static void do_test_redelivery_after_lost_assignment(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        int64_t committed_offset     = -1;
        rd_kafka_topic_partition_list_t *committed;
        int i;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        /* Manual commits only, so the committed offset checked below is
         * exactly and only what this test explicitly commits. */
        test_conf_set(conf, "enable.auto.commit", "false");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        for (i = 0; i < 10; i++) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 10 * 1000);
                TEST_ASSERT(rkm, "Expected a message");
                TEST_ASSERT(!rkm->err, "Expected no error, got: %s",
                            rd_kafka_message_errstr(rkm));

                if (i == 4) {
                        /* Commit up to and including the 5th message: this,
                         * and only this, is the offset the consumer must
                         * still be at after the loss below. */
                        TEST_CALL_ERR__(rd_kafka_commit_message(c, rkm, 0));
                        committed_offset = rkm->offset + 1;
                }

                rd_kafka_message_destroy(rkm);
        }

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        TEST_SAY("Waiting for the member to rejoin\n");
        {
                int64_t tmout = test_clock() + (15 * 1000000);
                while (test_clock() < tmout && rd_kafka_assignment_lost(c))
                        test_consumer_poll_once(c, NULL, 1000);
                TEST_ASSERT(!rd_kafka_assignment_lost(c),
                            "Expected the assignment to no longer be lost "
                            "after the member rejoined");
        }

        committed = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(committed, topic, 0);
        TEST_CALL_ERR__(rd_kafka_committed(c, committed, 10 * 1000));

        TEST_ASSERT(committed->cnt == 1 &&
                        committed->elems[0].offset == committed_offset,
                    "Expected the committed offset to still be %" PRId64
                    " after the lost assignment and rejoin, got %" PRId64,
                    committed_offset,
                    committed->cnt == 1 ? committed->elems[0].offset : -1);

        rd_kafka_topic_partition_list_destroy(committed);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief If the lost-assignment guard ever regresses and a commit leaks
 *        out during the lost window, it comes back as FENCED_INSTANCE_ID
 *        for a static member against a real broker. Poison every
 *        OffsetCommit response with exactly that error so a leak would be
 *        caught at the symptom level (a fatal error), not just as a
 *        wire-level request count like the other subtests in this file.
 *        auto.commit.interval.ms is set past this test's entire duration:
 *        the guard rejects a lost-window commit locally before any
 *        request is built, so there is no "tick fires but gets a
 *        poisoned response" moment to observe - a short interval would
 *        just race a legitimate pre-loss or post-recovery tick against
 *        the poison and fail for a reason unrelated to the lost-assignment
 *        fix (a real FENCED_INSTANCE_ID on an ordinary commit is correctly
 *        fatal).
 */
static void do_test_no_fatal_error_on_lost_assignment_commit_attempt(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid          = "mygroup";
        const char *topic            = "test";
        const int session_timeout_ms = 6000;
        char errstr[512];
        int64_t tmout;

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 10,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                                          RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID);

        trigger_session_timeout(mcluster, c, session_timeout_ms);

        /* Keep polling a bit longer to let the rejoin fully settle while
         * the poisoned error is still armed. */
        tmout = test_clock() + (4 * 1000000);
        while (test_clock() < tmout)
                test_consumer_poll_once(c, NULL, 1000);

        TEST_ASSERT(!rd_kafka_fatal_error(c, errstr, sizeof(errstr)),
                    "Expected no fatal error, got: %s", errstr);

        rd_kafka_mock_clear_request_errors(mcluster, RD_KAFKAP_OffsetCommit);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief Same trigger shape as do_test_no_commit_of_lost_assignment() but
 *        with a short session.timeout.ms: once max.poll.interval.ms trips
 *        and stops the internal Heartbeats, the mock broker's own session
 *        timer gets a full cycle to evict the member before the
 *        application resumes polling, so the eventual rejoin faces a
 *        broker that has genuinely forgotten the member, not merely one
 *        that is still waiting on a delayed response like every other
 *        subtest above.
 */
static void do_test_no_commit_after_broker_side_eviction(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid            = "mygroup";
        const char *topic              = "test";
        const int max_poll_interval_ms = 6000;
        const int session_timeout_ms   = 3000;
        size_t offset_commit_cnt;
        int64_t tmout;
        char errstr[512];

        SUB_TEST();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10, "bootstrap.servers",
                                 bootstraps, "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "group.instance.id", "my-instance");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        test_conf_set(conf, "session.timeout.ms", "3000");
        test_conf_set(conf, "heartbeat.interval.ms", "500");
        test_conf_set(conf, "max.poll.interval.ms", "6000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.commit.interval.ms", "60000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        test_consumer_poll("consume", c, 0, -1, 0, 10, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Don't poll for long enough that max.poll.interval.ms trips
         * (stopping the internal Heartbeats) and then session.timeout.ms
         * also elapses on top of that, giving the mock broker's 1s-period
         * session timer a full cycle to notice and evict the member. */
        TEST_SAY(
            "Not polling for %dms "
            "(> max.poll.interval.ms %dms + session.timeout.ms %dms)\n",
            max_poll_interval_ms + session_timeout_ms + 3000,
            max_poll_interval_ms, session_timeout_ms);
        rd_sleep((max_poll_interval_ms + session_timeout_ms) / 1000 + 3);

        TEST_SAY(
            "Polling past max.poll.interval.ms and the broker-side "
            "eviction\n");
        tmout = test_clock() + (10 * 1000000);
        while (test_clock() < tmout) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        offset_commit_cnt = test_mock_get_matching_request_cnt(
            mcluster, is_offset_commit_request, NULL);

        rd_kafka_mock_stop_request_tracking(mcluster);

        TEST_SAY("%" PRIusz
                 " OffsetCommit request(s) sent after max.poll.interval.ms "
                 "was exceeded and the broker evicted the member\n",
                 offset_commit_cnt);

        TEST_ASSERT(offset_commit_cnt == 0,
                    "Expected no OffsetCommit for the lost assignment, but "
                    "%" PRIusz " were sent",
                    offset_commit_cnt);

        TEST_ASSERT(!rd_kafka_fatal_error(c, errstr, sizeof(errstr)),
                    "Expected no fatal error even though the broker "
                    "genuinely evicted the member, got: %s",
                    errstr);

        verify_recovery_after_lost_assignment(c, bootstraps, topic);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


int main_0192_cgrp_static_session_timeout_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        /* The only trigger in this file that is not gated to the classic
         * protocol: run it under whichever group.protocol this invocation
         * is testing before the rest of the triggers below are skipped
         * for group.protocol=consumer. */
        do_test_no_commit_of_lost_assignment_deleted_topic();

        if (!test_consumer_group_protocol_classic()) {
                /* rkcg_ts_session_timeout is only maintained by the classic
                 * protocol, and the classic branch of
                 * rd_kafka_cgrp_max_poll_interval_check_tmr_cb() is what
                 * resets the member id and revokes as lost, so none of
                 * the remaining triggers apply under
                 * group.protocol=consumer. */
                TEST_SKIP(
                    "Remaining tests are only for group.protocol=classic\n");
                return 0;
        }

        do_test_no_commit_of_lost_assignment();
        do_test_no_commit_of_lost_assignment_eager();
        do_test_no_commit_of_lost_assignment_max_poll_interval();
        do_test_no_commit_of_lost_assignment_eager_max_poll_interval();
        do_test_max_poll_interval_lost_during_pending_rebalance();
        do_test_no_commit_of_lost_assignment_dynamic_member();
        do_test_rebalance_cb_sees_lost_cleared_before_assign();
        do_test_no_auto_commit_timer_while_lost();
        do_test_close_while_lost();
        do_test_unsubscribe_while_lost();
        do_test_manual_commit_returns_assignment_lost();
        do_test_redelivery_after_lost_assignment();
        do_test_no_fatal_error_on_lost_assignment_commit_attempt();
        do_test_no_commit_after_broker_side_eviction();

        return 0;
}
