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
 * array. The partition stayed on the assignment's queried list forever, and
 * a non-empty queried list blocks every later offset query, so the
 * partition was never consumed.
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


static rd_kafka_t *create_consumer(const char *bootstraps, const char *group) {
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");
        /* Fail instead of resetting if the committed offset isn't used. */
        test_conf_set(conf, "auto.offset.reset", "error");
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
 * @returns the number of tracked OffsetFetch requests, storing up to
 *          \p ts_size of their timestamps (in microseconds) in \p ts.
 */
static size_t get_offsetfetch_requests(rd_kafka_mock_cluster_t *mcluster,
                                       int64_t *ts,
                                       size_t ts_size) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt, i, cnt = 0;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                if (rd_kafka_mock_request_api_key(requests[i]) !=
                    RD_KAFKAP_OffsetFetch)
                        continue;
                if (ts && cnt < ts_size)
                        ts[cnt] = rd_kafka_mock_request_timestamp(requests[i]);
                cnt++;
        }
        rd_kafka_mock_request_destroy_array(requests, request_cnt);

        return cnt;
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

        c = create_consumer(bootstraps, topic);
        test_consumer_assign_partition("assign", c, topic, 0,
                                       RD_KAFKA_OFFSET_STORED);

        consume_from_committed(c, 1);

        req_cnt = get_offsetfetch_requests(mcluster, ts, RD_ARRAYSIZE(ts));
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
 * @brief A request-level \p err arrives after the assignment changed, so the
 *        OffsetFetch handler can't retry the request: the queried partition
 *        must go back to the pending list and be queried again, together
 *        with the partition added meanwhile.
 */
static void do_test_coord_err_assignment_changed(rd_kafka_resp_err_t err) {
        const char *topic;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_t *c;
        test_timing_t t_wait;

        SUB_TEST("%s", rd_kafka_err2name(err));

        topic    = test_mk_topic_name(__FUNCTION__, 1);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 2, 1);
        test_produce_msgs_easy_v(topic, 0, 0, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        test_produce_msgs_easy_v(topic, 0, 1, 0, MSGCNT, 16,
                                 "bootstrap.servers", bootstraps, NULL);
        commit_offsets(bootstraps, topic, topic, 2, COMMITTED_OFFSET);

        /* Delay the failed reply so the assignment can change meanwhile. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetFetch, 1, err, 1000);

        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_consumer(bootstraps, topic);

        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 0)->offset =
            RD_KAFKA_OFFSET_STORED;
        test_consumer_incremental_assign("assign p0", c, parts);
        rd_kafka_topic_partition_list_destroy(parts);

        /* Wait for the OffsetFetch of partition 0 to reach the broker. */
        TIMING_START(&t_wait, "WAIT_OFFSETFETCH");
        while (get_offsetfetch_requests(mcluster, NULL, 0) == 0) {
                TEST_ASSERT(TIMING_DURATION(&t_wait) < 5 * 1000000,
                            "No OffsetFetch request sent in 5s");
                rd_usleep(10 * 1000, 0);
        }
        TIMING_STOP(&t_wait);

        /* Changes the assignment version while the request is in flight. */
        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, 1)->offset =
            RD_KAFKA_OFFSET_STORED;
        test_consumer_incremental_assign("assign p1", c, parts);
        rd_kafka_topic_partition_list_destroy(parts);

        consume_from_committed(c, 2);

        rd_kafka_mock_stop_request_tracking(mcluster);
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

        do_test_coord_err_assignment_changed(RD_KAFKA_RESP_ERR_NOT_COORDINATOR);
        do_test_coord_err_assignment_changed(
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE);

        return 0;
}
