/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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
#include "rdkafka.h"

/**
 * @brief Admin API local dry-run unit-tests.
 */

#define MY_SOCKET_TIMEOUT_MS 1500
#define MY_SOCKET_TIMEOUT_MS_STR "1500"





/**
 * @brief CreateTopics tests
 *
 *
 *
 */
static void do_test_CreateTopics (const char *what,
                                  rd_kafka_t *rk, rd_kafka_queue_t *useq,
                                  int with_options) {
        rd_kafka_queue_t *q = useq ? useq : rd_kafka_queue_new(rk);
#define MY_NEW_TOPICS_CNT 6
        rd_kafka_NewTopic_t *new_topics[MY_NEW_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_CreateTopics_result_t *res;
        const rd_kafka_topic_result_t **restopics;
        size_t restopic_cnt;
        void *my_opaque = NULL, *opaque;

        TEST_SAY(_C_MAG "[ %s CreateTopics with %s, timeout %dms ]\n",
                 rd_kafka_name(rk), what, exp_timeout);

        /**
         * Construct NewTopic array with different properties for
         * different partitions.
         */
        for (i = 0 ; i < MY_NEW_TOPICS_CNT ; i++) {
                const char *topic = test_mk_topic_name(__FUNCTION__, 1);
                int num_parts = i * 51 + 1;
                int num_replicas = jitter(1, MY_NEW_TOPICS_CNT-1);
                int add_config = (i & 2);
                int set_replicas = !(i % 1);

                new_topics[i] = rd_kafka_NewTopic_new(topic,
                                                      num_parts,
                                                      set_replicas ? -1 :
                                                      num_replicas);

                if (add_config) {
                        /*
                         * Add various (unverified) configuration properties
                         */
                        err = rd_kafka_NewTopic_add_config(new_topics[i],
                                                           "dummy.doesntexist",
                                                           "butThere'sNothing "
                                                           "to verify that");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                        err = rd_kafka_NewTopic_add_config(new_topics[i],
                                                           "try.a.null.value",
                                                           NULL);
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                        err = rd_kafka_NewTopic_add_config(new_topics[i],
                                                           "or.empty", "");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }


                if (set_replicas) {
                        int32_t p;

                        /*
                         * Set valid replica assignments
                         */
                        for (p = 0 ; p < num_parts ; p++) {
                                int32_t replicas[MY_NEW_TOPICS_CNT];
                                int j;

                                /* Skip second partition */
                                if (p == 1)
                                        continue;

                                for (j = 0 ; j < num_replicas ; j++)
                                        replicas[j] = j;

                                err = rd_kafka_NewTopic_set_replica_assignment(
                                        new_topics[i], p,
                                        replicas, num_replicas);
                                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                        }
                } else {
                        int32_t dummy_replicas[1] = {1};

                        /* Test invalid partition */
                        err = rd_kafka_NewTopic_set_replica_assignment(
                                new_topics[i], num_parts+1, dummy_replicas, 1);
                        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                                    "%s", rd_kafka_err2str(err));

                        /* Setting replicas with with default replicas != -1
                         * is an error. */
                        err = rd_kafka_NewTopic_set_replica_assignment(
                                new_topics[i], 0, dummy_replicas, 1);
                        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                                    "%s", rd_kafka_err2str(err));
                }
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err = rd_kafka_AdminOptions_set_request_timeout(
                        options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                my_opaque = (void *)123;
                rd_kafka_AdminOptions_set_opaque(options, my_opaque);
        }

        TIMING_START(&timing, "CreateTopics");
        TEST_SAY("Call CreateTopics, timeout is %dms\n", exp_timeout);
        rd_kafka_admin_CreateTopics(rk, new_topics, MY_NEW_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue */
        TIMING_START(&timing, "CreateTopics.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout-100, exp_timeout+100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("CreateTopics: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_CreateTopics_result(rkev);
        TEST_ASSERT(res, "expected CreateTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        errstr2 = (const char *)0x1;
        err = rd_kafka_CreateTopics_result_error(res, &errstr2);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected CreateTopics to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err),
                    err ? errstr2 : "n/a");

        /* Attempt to extract topics anyway, should return NULL. */
        restopics = rd_kafka_CreateTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(!restopics && restopic_cnt == 0,
                    "expected no result_topics, got %p cnt %"PRIusz,
                    restopics, restopic_cnt);

        rd_kafka_event_destroy(rkev);

        for (i = 0 ; i < MY_NEW_TOPICS_CNT ; i++)
                rd_kafka_NewTopic_destroy(new_topics[i]);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
}






/**
 * @brief DeleteTopics tests
 *
 *
 *
 */
static void do_test_DeleteTopics (const char *what,
                                  rd_kafka_t *rk, rd_kafka_queue_t *useq,
                                  int with_options) {
        rd_kafka_queue_t *q = useq ? useq : rd_kafka_queue_new(rk);
#define MY_DEL_TOPICS_CNT 4
        rd_kafka_DeleteTopic_t *del_topics[MY_DEL_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DeleteTopics_result_t *res;
        const rd_kafka_topic_result_t **restopics;
        size_t restopic_cnt;
        void *my_opaque = NULL, *opaque;

        TEST_SAY(_C_MAG "[ %s DeleteTopics with %s, timeout %dms ]\n",
                 rd_kafka_name(rk), what, exp_timeout);

        for (i = 0 ; i < MY_DEL_TOPICS_CNT ; i++)
                del_topics[i] = rd_kafka_DeleteTopic_new(test_mk_topic_name(__FUNCTION__, 1));

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err = rd_kafka_AdminOptions_set_request_timeout(
                        options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DeleteTopics");
        TEST_SAY("Call DeleteTopics, timeout is %dms\n", exp_timeout);
        rd_kafka_admin_DeleteTopics(rk, del_topics, MY_DEL_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue */
        TIMING_START(&timing, "DeleteTopics.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout-100, exp_timeout+100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteTopics: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteTopics_result(rkev);
        TEST_ASSERT(res, "expected DeleteTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        errstr2 = (const char *)0x1;
        err = rd_kafka_DeleteTopics_result_error(res, &errstr2);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DeleteTopics to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err),
                    err ? errstr2 : "n/a");

        /* Attempt to extract topics anyway, should return NULL. */
        restopics = rd_kafka_DeleteTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(!restopics && restopic_cnt == 0,
                    "expected no result_topics, got %p cnt %"PRIusz,
                    restopics, restopic_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_DeleteTopic_destroy_array(del_topics, MY_DEL_TOPICS_CNT);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
}


/**
 * @brief Test a mix of APIs using the same replyq.
 *
 *  - Create topics A,B
 *  - Delete topic B
 *  - Create topic C
 *  - Create extra partitions for topic D
 */
static void do_test_mix (rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
        char *topics[] = { "topicA", "topicB", "topicC" };
        int cnt = 0;
        struct waiting {
                rd_kafka_event_type_t evtype;
                int seen;
        };
        struct waiting id1 = {RD_KAFKA_EVENT_CREATETOPICS_RESULT};
        struct waiting id2 = {RD_KAFKA_EVENT_DELETETOPICS_RESULT};
        struct waiting id3 = {RD_KAFKA_EVENT_CREATETOPICS_RESULT};
        struct waiting id4 = {RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT};

        TEST_SAY(_C_MAG "[ Mixed mode test on %s]\n", rd_kafka_name(rk));

        test_CreateTopics_simple(rk, rkqu, topics, 2, 1, &id1);
        test_DeleteTopics_simple(rk, rkqu, &topics[1], 1, &id2);
        test_CreateTopics_simple(rk, rkqu, &topics[2], 1, 1, &id3);
        test_CreatePartitions_simple(rk, rkqu, "topicD", 15, &id4);

        while (cnt < 4) {
                rd_kafka_event_t *rkev;
                struct waiting *w;

                rkev = rd_kafka_queue_poll(rkqu, -1);
                TEST_ASSERT(rkev);

                TEST_SAY("Got event %s: %s\n",
                         rd_kafka_event_name(rkev),
                         rd_kafka_event_error_string(rkev));

                w = rd_kafka_event_opaque(rkev);
                TEST_ASSERT(w);

                TEST_ASSERT(w->evtype == rd_kafka_event_type(rkev),
                            "Expected evtype %d, not %d (%s)",
                            w->evtype, rd_kafka_event_type(rkev),
                            rd_kafka_event_name(rkev));

                TEST_ASSERT(w->seen == 0, "Duplicate results");

                w->seen++;
                cnt++;

                rd_kafka_event_destroy(rkev);
        }
}

static void do_test_apis (rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_queue_t *mainq;
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 0);
        /* Remove brokers, if any, since this is a local test and we
         * rely on the controller not being found. */
        test_conf_set(conf, "bootstrap.servers", "");
        test_conf_set(conf, "socket.timeout.ms", MY_SOCKET_TIMEOUT_MS_STR);

        rk = rd_kafka_new(cltype, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "kafka_new(%d): %s", cltype, errstr);

        mainq = rd_kafka_queue_get_main(rk);

        do_test_CreateTopics("temp queue, no options", rk, NULL, 0);
        do_test_CreateTopics("temp queue, options", rk, NULL, 1);
        do_test_CreateTopics("main queue, options", rk, mainq, 1);

        do_test_DeleteTopics("temp queue, no options", rk, NULL, 0);
        do_test_DeleteTopics("temp queue, options", rk, NULL, 1);
        do_test_DeleteTopics("main queue, options", rk, mainq, 1);

        do_test_mix(rk, mainq);

        rd_kafka_queue_destroy(mainq);

        rd_kafka_destroy(rk);
}


int main_0080_admin_ut (int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}
