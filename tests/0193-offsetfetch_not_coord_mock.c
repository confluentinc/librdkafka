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
 * @name Request-level coordinator errors on OffsetFetch (#5586).
 *
 * A consumer that assign()s a partition without an offset fetches the
 * group's committed offset before it starts fetching. Since OffsetFetch v2 a
 * broker that is no longer (or not yet) the group coordinator replies with a
 * top-level NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE and an empty topics
 * array, and a new coordinator replies the same way with
 * COORDINATOR_LOAD_IN_PROGRESS while it loads the group's offsets. The
 * partition stayed on the assignment's queried list forever, and a non-empty
 * queried list blocks every later offset query, so the partition was never
 * consumed. A request that got no reply while the assignment changed left the
 * partition there too.
 */

#define MSGCNT           10
#define COMMITTED_OFFSET 4


/**
 * @brief Commit \p offset for every partition of \p topic in \p group.
 */
static void commit_offsets(const char *bootstraps,
                           const char *group,
                           const char *topic,
                           int partition_cnt,
                           int64_t offset) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *offsets;
        int i;

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");
        c = test_create_consumer(group, NULL, conf, NULL);

        offsets = rd_kafka_topic_partition_list_new(partition_cnt);
        for (i = 0; i < partition_cnt; i++)
                rd_kafka_topic_partition_list_add(offsets, topic, i)->offset =
                    offset;
        TEST_CALL_ERR__(rd_kafka_commit(c, offsets, 0 /*sync*/));
        rd_kafka_topic_partition_list_destroy(offsets);

        test_consumer_close(c);
        rd_kafka_destroy(c);
}


/**
 * @brief Create a consumer in \p group, with \p socket_timeout_ms as
 *        socket.timeout.ms unless it's NULL.
 */
static rd_kafka_t *create_consumer(const char *bootstraps,
                                   const char *group,
                                   const char *socket_timeout_ms) {
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");
        /* Fail instead of resetting if the committed offset isn't used. */
        test_conf_set(conf, "auto.offset.reset", "error");
        /* The backoff check in do_test_coord_err_retried() relies on it. */
        test_conf_set(conf, "retry.backoff.ms", "100");
        if (socket_timeout_ms)
                test_conf_set(conf, "socket.timeout.ms", socket_timeout_ms);
        return test_create_consumer(group, NULL, conf, NULL);
}


/**
 * @brief Consume the records after the committed offset from each of the
 *        \p partition_cnt partitions, checking that each partition starts at
 *        the committed offset and that no error reaches the application.
 */
static void consume_from_committed(rd_kafka_t *c, int partition_cnt) {
        int64_t next_offset[2];
        int exp_cnt = partition_cnt * (MSGCNT - COMMITTED_OFFSET);
        int cnt     = 0;
        int i;
        test_timing_t t_consume;

        TEST_ASSERT(partition_cnt <= (int)RD_ARRAYSIZE(next_offset));
        for (i = 0; i < partition_cnt; i++)
                next_offset[i] = COMMITTED_OFFSET;

        TIMING_START(&t_consume, "CONSUME");
        while (cnt < exp_cnt) {
                rd_kafka_message_t *rkm;

                TEST_ASSERT(TIMING_DURATION(&t_consume) < 10 * 1000000,
                            "Consumed %d/%d records in 10s: a partition is "
                            "still waiting for its committed offset",
                            cnt, exp_cnt);

                rkm = rd_kafka_consumer_poll(c, 100);
                if (!rkm)
                        continue;

                TEST_ASSERT(!rkm->err, "Unexpected consumer error: %s",
                            rd_kafka_message_errstr(rkm));
                TEST_ASSERT(rkm->partition >= 0 &&
                                rkm->partition < partition_cnt,
                            "Unexpected partition %" PRId32, rkm->partition);
                TEST_ASSERT(rkm->offset == next_offset[rkm->partition],
                            "Partition %" PRId32 ": expected offset %" PRId64
                            ", got %" PRId64,
                            rkm->partition, next_offset[rkm->partition],
                            rkm->offset);

                next_offset[rkm->partition]++;
                cnt++;
                rd_kafka_message_destroy(rkm);
        }
        TIMING_STOP(&t_consume);
}


/**
 * @returns the number of tracked \p api_key requests, storing up to \p size
 *          of their timestamps (in microseconds) in \p ts and of the ids of
 *          the brokers they were sent to in \p broker_ids, each unless NULL.
 */
static size_t get_requests(rd_kafka_mock_cluster_t *mcluster,
                           int16_t api_key,
                           int64_t *ts,
                           int32_t *broker_ids,
                           size_t size) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt, i, cnt = 0;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                if (rd_kafka_mock_request_api_key(requests[i]) != api_key)
                        continue;
                if (ts && cnt < size)
                        ts[cnt] = rd_kafka_mock_request_timestamp(requests[i]);
                if (broker_ids && cnt < size)
                        broker_ids[cnt] = rd_kafka_mock_request_id(requests[i]);
                cnt++;
        }
        rd_kafka_mock_request_destroy_array(requests, request_cnt);

        return cnt;
}


/**
 * @brief Wait for \p cnt \p api_key requests to reach the cluster.
 */
static void
wait_requests(rd_kafka_mock_cluster_t *mcluster, int16_t api_key, size_t cnt) {
        test_timing_t t_wait;

        TIMING_START(&t_wait, "WAIT_REQUESTS");
        while (get_requests(mcluster, api_key, NULL, NULL, 0) < cnt) {
                TEST_ASSERT(TIMING_DURATION(&t_wait) < 5 * 1000000,
                            "Fewer than %" PRIusz " %s request(s) sent in 5s",
                            cnt, rd_kafka_ApiKey2str(api_key));
                rd_usleep(10 * 1000, 0);
        }
        TIMING_STOP(&t_wait);
}


/**
 * @brief Incrementally assign partition \p partition of \p topic, starting
 *        from its committed offset, or unassign it if \p unassign is true.
 */
static void incremental_assign(rd_kafka_t *c,
                               const char *topic,
                               int32_t partition,
                               rd_bool_t unassign) {
        rd_kafka_topic_partition_list_t *parts =
            rd_kafka_topic_partition_list_new(1);

        rd_kafka_topic_partition_list_add(parts, topic, partition)->offset =
            RD_KAFKA_OFFSET_STORED;
        if (unassign)
                test_consumer_incremental_unassign("unassign", c, parts);
        else
                test_consumer_incremental_assign("assign", c, parts);
        rd_kafka_topic_partition_list_destroy(parts);
}


/**
 * @brief Check that the first tracked OffsetFetch request went to broker 1
 *        and the last one to broker 2.
 */
static void check_offsetfetch_moved(rd_kafka_mock_cluster_t *mcluster) {
        int32_t broker_ids[16];
        size_t cnt, i;

        cnt = get_requests(mcluster, RD_KAFKAP_OffsetFetch, NULL, broker_ids,
                           RD_ARRAYSIZE(broker_ids));
        TEST_ASSERT(cnt >= 2 && cnt <= RD_ARRAYSIZE(broker_ids),
                    "Expected 2 to %" PRIusz
                    " OffsetFetch requests, got %" PRIusz,
                    RD_ARRAYSIZE(broker_ids), cnt);
        for (i = 0; i < cnt; i++)
                TEST_SAY("OffsetFetch #%" PRIusz " sent to broker %" PRId32
                         "\n",
                         i, broker_ids[i]);
        TEST_ASSERT(broker_ids[0] == 1 && broker_ids[cnt - 1] == 2,
                    "Expected the first OffsetFetch request on broker 1 and "
                    "the last one on broker 2, got %" PRId32 " and %" PRId32,
                    broker_ids[0], broker_ids[cnt - 1]);
}


/**
 * @brief Wait for the coordinator queries of a consumer that just started:
 *        the first one finds the coordinator and setting it triggers a
 *        second one. The next one is only sent after
 *        coordinator.query.interval.ms, so after these two only an error
 *        makes the consumer look for a new coordinator.
 */
static void wait_startup_coord_queries(rd_kafka_mock_cluster_t *mcluster) {
        wait_requests(mcluster, RD_KAFKAP_FindCoordinator, 2);
}


/**
 * @brief The first \p err_cnt OffsetFetch requests fail with a request-level
 *        \p err: the consumer must retry with a backoff, without raising the
 *        error, and start from the committed offset.
 */
static void do_test_coord_err_retried(rd_kafka_resp_err_t err, int err_cnt) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *c;
        int64_t ts[16];
        size_t req_cnt, i;
        int j;

        SUB_TEST("%s x %d", rd_kafka_err2name(err), err_cnt);

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 1, COMMITTED_OFFSET);

        for (j = 0; j < err_cnt; j++)
                rd_kafka_mock_push_request_errors(
                    mcluster, RD_KAFKAP_OffsetFetch, 1, err);

        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_consumer(bootstraps, topic, NULL);
        test_consumer_assign_partition("assign", c, topic, 0,
                                       RD_KAFKA_OFFSET_STORED);

        consume_from_committed(c, 1);

        req_cnt = get_requests(mcluster, RD_KAFKAP_OffsetFetch, ts, NULL,
                               RD_ARRAYSIZE(ts));
        TEST_ASSERT(req_cnt == (size_t)err_cnt + 1,
                    "Expected %d OffsetFetch requests, got %" PRIusz,
                    err_cnt + 1, req_cnt);

        /* retry.backoff.ms is 100 with up to 20% jitter: a request sent at
         * once means the error was not retried but the partition was
         * queried again without a backoff. */
        for (i = 1; i < req_cnt; i++) {
                int64_t gap_ms = (ts[i] - ts[i - 1]) / 1000;
                TEST_SAY("OffsetFetch #%" PRIusz " sent %" PRId64
                         "ms after the previous one\n",
                         i, gap_ms);
                TEST_ASSERT(gap_ms >= 50,
                            "OffsetFetch #%" PRIusz " was sent %" PRId64
                            "ms after the previous one, expected a backoff",
                            i, gap_ms);
        }

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The first OffsetFetch request fails with \p err after the assignment
 *        changed, so the OffsetFetch handler can't retry the request: the
 *        queried partition must go back to the pending list and be queried
 *        again, together with the partition added meanwhile.
 *
 * @param err is a request-level error, or RD_KAFKA_RESP_ERR__TIMED_OUT for a
 *            request whose reply comes after it timed out.
 */
static void do_test_assignment_changed(rd_kafka_resp_err_t err) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *c;
        rd_bool_t timed_out = err == RD_KAFKA_RESP_ERR__TIMED_OUT;

        SUB_TEST("%s", rd_kafka_err2name(err));

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 2, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 2, COMMITTED_OFFSET);

        if (timed_out)
                /* Reply after socket.timeout.ms (1500 below), so the request
                 * times out after the assignment changed. */
                rd_kafka_mock_broker_push_request_error_rtts(
                    mcluster, 1, RD_KAFKAP_OffsetFetch, 1,
                    RD_KAFKA_RESP_ERR_NO_ERROR, 4000);
        else
                /* Delay the failed reply so the assignment can change
                 * meanwhile. */
                rd_kafka_mock_broker_push_request_error_rtts(
                    mcluster, 1, RD_KAFKAP_OffsetFetch, 1, err, 1000);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* 1500 is the lowest socket.timeout.ms that doesn't warn with the
         * default fetch.wait.max.ms. */
        c = create_consumer(bootstraps, topic, timed_out ? "1500" : NULL);

        incremental_assign(c, topic, 0, rd_false);
        wait_requests(mcluster, RD_KAFKAP_OffsetFetch, 1);

        /* Changes the assignment version while the request is in flight. */
        incremental_assign(c, topic, 1, rd_false);

        consume_from_committed(c, 2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief While the OffsetFetch request of partition 1 is in flight, the
 *        partition is unassigned and partition 0 assigned, which sends a
 *        second request. When the first one then fails with a request-level
 *        error, partition 0 must not be queried again: the second request,
 *        still in flight, queries it.
 */
static void do_test_assignment_replaced(void) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *c;
        size_t req_cnt;

        SUB_TEST();

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 2, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 2, COMMITTED_OFFSET);

        /* The first request fails while the second one is in flight. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetFetch, 2,
            RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 500, RD_KAFKA_RESP_ERR_NO_ERROR,
            1500);

        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_consumer(bootstraps, topic, NULL);

        incremental_assign(c, topic, 1, rd_false);
        wait_requests(mcluster, RD_KAFKAP_OffsetFetch, 1);
        incremental_assign(c, topic, 1, rd_true);
        incremental_assign(c, topic, 0, rd_false);
        wait_requests(mcluster, RD_KAFKAP_OffsetFetch, 2);

        consume_from_committed(c, 1);

        req_cnt = get_requests(mcluster, RD_KAFKAP_OffsetFetch, NULL, NULL, 0);
        TEST_ASSERT(req_cnt == 2,
                    "Expected 2 OffsetFetch requests, got %" PRIusz, req_cnt);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The group coordinator moves from broker 1 to broker 2 before the
 *        first OffsetFetch request: broker 1 replies NOT_COORDINATOR, so the
 *        consumer must query the coordinator again and retry the request on
 *        broker 2.
 */
static void do_test_coord_moves(void) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *c;

        SUB_TEST();

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(2, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 1, COMMITTED_OFFSET);

        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_consumer(bootstraps, topic, NULL);
        wait_startup_coord_queries(mcluster);

        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 2);

        test_consumer_assign_partition("assign", c, topic, 0,
                                       RD_KAFKA_OFFSET_STORED);

        consume_from_committed(c, 1);
        check_offsetfetch_moved(mcluster);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief The group coordinator moves to a broker that only supports older
 *        OffsetFetch versions while the first request is in flight. Its
 *        retry, built for the previous coordinator, can't be sent, so the
 *        partition must be queried again with a new request.
 */
static void do_test_coord_moves_older_version(void) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *c;

        SUB_TEST();

        /* The old coordinator closes the connection if a retry reaches it
         * after the version change. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(2, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 1, COMMITTED_OFFSET);

        /* Delay the reply until the coordinator moved. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetFetch, 1,
            RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 500);

        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_consumer(bootstraps, topic, NULL);
        wait_startup_coord_queries(mcluster);
        test_consumer_assign_partition("assign", c, topic, 0,
                                       RD_KAFKA_OFFSET_STORED);
        wait_requests(mcluster, RD_KAFKAP_OffsetFetch, 1);

        /* The request, in flight, uses OffsetFetch v6: new connections, like
         * the one to broker 2, now negotiate v5. */
        TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(
            mcluster, RD_KAFKAP_OffsetFetch, 0, 5));
        rd_kafka_mock_coordinator_set(mcluster, "group", topic, 2);

        consume_from_committed(c, 1);
        check_offsetfetch_moved(mcluster);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}


/**
 * @brief rd_kafka_committed() retries a request-level \p err until it gets
 *        the committed offset, or returns RD_KAFKA_RESP_ERR__TIMED_OUT once
 *        its timeout expires.
 */
static void do_test_committed(rd_kafka_resp_err_t err) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_t *c;
        rd_kafka_resp_err_t ret;
        int j;

        SUB_TEST("%s", rd_kafka_err2name(err));

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        commit_offsets(bootstraps, topic, topic, 1, COMMITTED_OFFSET);

        c     = create_consumer(bootstraps, topic, NULL);
        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0);

        for (j = 0; j < 3; j++)
                rd_kafka_mock_push_request_errors(
                    mcluster, RD_KAFKAP_OffsetFetch, 1, err);
        TEST_CALL_ERR__(rd_kafka_committed(c, parts, tmout_multip(10000)));
        TEST_ASSERT(parts->elems[0].offset == COMMITTED_OFFSET,
                    "Expected committed offset %d, got %" PRId64,
                    COMMITTED_OFFSET, parts->elems[0].offset);

        /* More errors than requests in the timeout: the backoff starts at
         * retry.backoff.ms (100) and doubles. */
        for (j = 0; j < 20; j++)
                rd_kafka_mock_push_request_errors(
                    mcluster, RD_KAFKAP_OffsetFetch, 1, err);
        ret = rd_kafka_committed(c, parts, 1000);
        TEST_ASSERT(ret == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Expected rd_kafka_committed() to time out, got %s",
                    rd_kafka_err2name(ret));

        rd_kafka_topic_partition_list_destroy(parts);
        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0193_offsetfetch_not_coord_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_coord_err_retried(RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 1);
        do_test_coord_err_retried(RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 3);
        do_test_coord_err_retried(RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
                                  3);
        do_test_coord_err_retried(
            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS, 3);

        do_test_assignment_changed(RD_KAFKA_RESP_ERR_NOT_COORDINATOR);
        do_test_assignment_changed(RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE);
        do_test_assignment_changed(
            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);
        do_test_assignment_changed(RD_KAFKA_RESP_ERR__TIMED_OUT);
        do_test_assignment_replaced();

        do_test_coord_moves();
        do_test_coord_moves_older_version();

        do_test_committed(RD_KAFKA_RESP_ERR_NOT_COORDINATOR);

        return 0;
}
