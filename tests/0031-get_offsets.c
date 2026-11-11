
/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */
#include "../src/rdkafka_proto.h"


/**
 * @brief Verify that rd_kafka_query_watermark_offsets times out in case we're
 * unable to fetch offsets within the timeout (Issue #2588).
 */
void test_query_watermark_offsets_timeout(void) {
        int64_t qry_low, qry_high;
        rd_kafka_resp_err_t err;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        const int timeout_ms = 1000;

        TEST_SKIP_MOCK_CLUSTER();

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ListOffsets, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            (int)(timeout_ms * 1.2));

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);


        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &qry_low,
                                               &qry_high, timeout_ms);

        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Querying watermark offsets should fail with %s when RTT > "
                    "timeout, instead got %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2name(err));

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Query watermark offsets should be able to query the correct
 *        leader immediately after a leader change.
 */
void test_query_watermark_offsets_leader_change(void) {
        int64_t qry_low, qry_high;
        rd_kafka_resp_err_t err;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        const int timeout_ms = 1000;

        TEST_SKIP_MOCK_CLUSTER();

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(2, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 2);

        /* Leader is broker 1 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &qry_low,
                                               &qry_high, timeout_ms);

        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Querying watermark offsets succeed on the first broker"
                    "and cache the leader, got %s",
                    rd_kafka_err2name(err));

        /* Leader is broker 2 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 2);

        /* First call returns NOT_LEADER_FOR_PARTITION, second one should go to
         * the second broker and return NO_ERROR instead of
         * NOT_LEADER_FOR_PARTITION. */
        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &qry_low,
                                               &qry_high, timeout_ms);

        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION,
                    "Querying watermark offsets should fail with "
                    "NOT_LEADER_FOR_PARTITION, got %s",
                    rd_kafka_err2name(err));

        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &qry_low,
                                               &qry_high, timeout_ms);

        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Querying watermark offsets should succeed by "
                    "querying the second broker, got %s",
                    rd_kafka_err2name(err));

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


struct leader_query_thread_arg {
        rd_kafka_t *rk;
        const char *topic;
        rd_bool_t offsets_for_times;
        rd_atomic32_t started;
        rd_atomic32_t done;
        rd_kafka_resp_err_t err;
        int64_t elapsed_us;
};

static int leader_query_thread_main(void *p) {
        struct leader_query_thread_arg *arg = p;
        int64_t ts_start                    = test_clock();

        rd_atomic32_set(&arg->started, 1);
        if (arg->offsets_for_times) {
                rd_kafka_topic_partition_list_t *offsets =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(offsets, arg->topic, 0)
                    ->offset = 0;
                arg->err =
                    rd_kafka_offsets_for_times(arg->rk, offsets, 60 * 1000);
                rd_kafka_topic_partition_list_destroy(offsets);
        } else {
                int64_t low, high;
                arg->err = rd_kafka_query_watermark_offsets(
                    arg->rk, arg->topic, 0, &low, &high, 60 * 1000);
        }
        arg->elapsed_us = test_clock() - ts_start;
        rd_atomic32_set(&arg->done, 1);
        return 0;
}

/**
 * @brief Destroying the client while rd_kafka_query_watermark_offsets() or
 *        rd_kafka_offsets_for_times() is waiting for the partition leaders
 *        must make it fail with RD_KAFKA_RESP_ERR__DESTROY immediately.
 */
static void test_destroy_during_leader_query(rd_bool_t offsets_for_times) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        struct leader_query_thread_arg arg = {0};
        thrd_t thrd;
        int64_t ts_deadline;
        int ret;

        TEST_SKIP_MOCK_CLUSTER();

        SUB_TEST_QUICK("%s", offsets_for_times ? "offsets_for_times"
                                               : "query_watermark_offsets");

        rd_atomic32_init(&arg.started, 0);
        rd_atomic32_init(&arg.done, 0);
        arg.topic             = topic;
        arg.offsets_for_times = offsets_for_times;

        /* Broker down: no metadata is received, so the leader query
         * keeps waiting for the metadata cache to change. */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);
        rd_kafka_mock_broker_set_down(mcluster, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        arg.rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        if (thrd_create(&thrd, leader_query_thread_main, &arg) != thrd_success)
                TEST_FAIL("Failed to create thread");

        ts_deadline = test_clock() + 10 * 1000 * 1000;
        while (!rd_atomic32_get(&arg.started)) {
                TEST_ASSERT(test_clock() < ts_deadline,
                            "leader query thread did not start");
                rd_usleep(10 * 1000, NULL);
        }

        /* Let the thread block waiting for the leaders, then make sure
         * it's still blocked when destroying, so destroy is what
         * interrupts it. */
        rd_sleep(1);
        TEST_ASSERT(!rd_atomic32_get(&arg.done),
                    "Leader query returned before rd_kafka_destroy(): %s",
                    rd_kafka_err2name(arg.err));
        rd_kafka_destroy(arg.rk);

        if (thrd_join(thrd, &ret) != thrd_success)
                TEST_FAIL("thrd_join failed");

        TEST_SAY("Leader query returned %s after %.3fs\n",
                 rd_kafka_err2name(arg.err), (double)arg.elapsed_us / 1e6);
        TEST_ASSERT(arg.err == RD_KAFKA_RESP_ERR__DESTROY,
                    "Expected %s, got %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__DESTROY),
                    rd_kafka_err2name(arg.err));
        TEST_ASSERT(arg.elapsed_us < 5 * 1000 * 1000,
                    "Expected the leader query to return right after "
                    "rd_kafka_destroy(), took %.3fs",
                    (double)arg.elapsed_us / 1e6);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * Verify that rd_kafka_(query|get)_watermark_offsets() works.
 */
int main_0031_get_offsets(int argc, char **argv) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        const int msgcnt  = test_quick ? 10 : 100;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        int64_t qry_low = -1234, qry_high = -1235;
        int64_t get_low = -1234, get_high = -1235;
        rd_kafka_resp_err_t err;
        test_timing_t t_qry, t_get;
        uint64_t testid;

        /* Produce messages */
        testid = test_produce_msgs_easy(topic, 0, 0, msgcnt);

        /* Get offsets */
        rk = test_create_consumer(NULL, NULL, NULL, NULL);

        TIMING_START(&t_qry, "query_watermark_offsets");
        err = rd_kafka_query_watermark_offsets(
            rk, topic, 0, &qry_low, &qry_high, tmout_multip(10 * 1000));
        TIMING_STOP(&t_qry);
        if (err)
                TEST_FAIL("query_watermark_offsets failed: %s\n",
                          rd_kafka_err2str(err));

        if (qry_low != 0 && qry_high != msgcnt)
                TEST_FAIL(
                    "Expected low,high %d,%d, but got "
                    "%" PRId64 ",%" PRId64,
                    0, msgcnt, qry_low, qry_high);

        TEST_SAY(
            "query_watermark_offsets: "
            "offsets %" PRId64 ", %" PRId64 "\n",
            qry_low, qry_high);

        /* Now start consuming to update the offset cache, then query it
         * with the get_ API. */
        rkt = test_create_topic_object(rk, topic, NULL);

        test_consumer_start("get", rkt, 0, RD_KAFKA_OFFSET_BEGINNING);
        test_consume_msgs("get", rkt, testid, 0, TEST_NO_SEEK, 0, msgcnt, 0);
        /* After at least one message has been consumed the
         * watermarks are cached. */

        TIMING_START(&t_get, "get_watermark_offsets");
        err = rd_kafka_get_watermark_offsets(rk, topic, 0, &get_low, &get_high);
        TIMING_STOP(&t_get);
        if (err)
                TEST_FAIL("get_watermark_offsets failed: %s\n",
                          rd_kafka_err2str(err));

        TEST_SAY(
            "get_watermark_offsets: "
            "offsets %" PRId64 ", %" PRId64 "\n",
            get_low, get_high);

        if (get_high != qry_high)
                TEST_FAIL(
                    "query/get discrepancies: "
                    "low: %" PRId64 "/%" PRId64 ", high: %" PRId64 "/%" PRId64,
                    qry_low, get_low, qry_high, get_high);
        if (get_low >= get_high)
                TEST_FAIL(
                    "get_watermark_offsets: "
                    "low %" PRId64 " >= high %" PRId64,
                    get_low, get_high);

        /* FIXME: We currently dont bother checking the get_low offset
         *        since it requires stats to be enabled. */

        test_consumer_stop("get", rkt, 0);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        return 0;
}

int main_0031_get_offsets_mock(int argc, char **argv) {

        test_query_watermark_offsets_timeout();

        test_query_watermark_offsets_leader_change();

        test_destroy_during_leader_query(rd_false);

        test_destroy_during_leader_query(rd_true);

        return 0;
}
