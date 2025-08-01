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
#include "rdkafka.h"

/**
 * @brief Admin API local dry-run unit-tests.
 */

#define MY_SOCKET_TIMEOUT_MS     100
#define MY_SOCKET_TIMEOUT_MS_STR "100"



static mtx_t last_event_lock;
static cnd_t last_event_cnd;
static rd_kafka_event_t *last_event = NULL;

/**
 * @brief The background event callback is called automatically
 *        by librdkafka from a background thread.
 */
static void
background_event_cb(rd_kafka_t *rk, rd_kafka_event_t *rkev, void *opaque) {
        mtx_lock(&last_event_lock);
        TEST_ASSERT(!last_event,
                    "Multiple events seen in background_event_cb "
                    "(existing %s, new %s)",
                    rd_kafka_event_name(last_event), rd_kafka_event_name(rkev));
        last_event = rkev;
        mtx_unlock(&last_event_lock);
        cnd_broadcast(&last_event_cnd);
        rd_sleep(1);
}

static rd_kafka_event_t *wait_background_event_cb(void) {
        rd_kafka_event_t *rkev;
        mtx_lock(&last_event_lock);
        while (!(rkev = last_event))
                cnd_wait(&last_event_cnd, &last_event_lock);
        last_event = NULL;
        mtx_unlock(&last_event_lock);

        return rkev;
}


/**
 * @brief CreateTopics tests
 *
 *
 *
 */
static void do_test_CreateTopics(const char *what,
                                 rd_kafka_t *rk,
                                 rd_kafka_queue_t *useq,
                                 int with_background_event_cb,
                                 int with_options) {
        rd_kafka_queue_t *q;
#define MY_NEW_TOPICS_CNT 6
        rd_kafka_NewTopic_t *new_topics[MY_NEW_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
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

        SUB_TEST_QUICK("%s CreateTopics with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        /**
         * Construct NewTopic array with different properties for
         * different partitions.
         */
        for (i = 0; i < MY_NEW_TOPICS_CNT; i++) {
                const char *topic = test_mk_topic_name(__FUNCTION__, 1);
                int num_parts     = i * 51 + 1;
                int num_replicas  = jitter(1, MY_NEW_TOPICS_CNT - 1);
                int set_config    = (i & 2);
                int set_replicas  = !(i % 1);

                new_topics[i] = rd_kafka_NewTopic_new(
                    topic, num_parts, set_replicas ? -1 : num_replicas, NULL,
                    0);

                if (set_config) {
                        /*
                         * Add various (unverified) configuration properties
                         */
                        err = rd_kafka_NewTopic_set_config(new_topics[i],
                                                           "dummy.doesntexist",
                                                           "butThere'sNothing "
                                                           "to verify that");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                        err = rd_kafka_NewTopic_set_config(
                            new_topics[i], "try.a.null.value", NULL);
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                        err = rd_kafka_NewTopic_set_config(new_topics[i],
                                                           "or.empty", "");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }


                if (set_replicas) {
                        int32_t p;
                        int32_t replicas[MY_NEW_TOPICS_CNT];
                        int j;

                        for (j = 0; j < num_replicas; j++)
                                replicas[j] = j;

                        /*
                         * Set valid replica assignments
                         */
                        for (p = 0; p < num_parts; p++) {
                                /* Try adding an existing out of order,
                                 * should fail */
                                if (p == 1) {
                                        err =
                                            rd_kafka_NewTopic_set_replica_assignment(
                                                new_topics[i], p + 1, replicas,
                                                num_replicas, errstr,
                                                sizeof(errstr));
                                        TEST_ASSERT(
                                            err ==
                                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                            "%s", rd_kafka_err2str(err));
                                }

                                err = rd_kafka_NewTopic_set_replica_assignment(
                                    new_topics[i], p, replicas, num_replicas,
                                    errstr, sizeof(errstr));
                                TEST_ASSERT(!err, "%s", errstr);
                        }

                        /* Try to add an existing partition, should fail */
                        err = rd_kafka_NewTopic_set_replica_assignment(
                            new_topics[i], 0, replicas, num_replicas, NULL, 0);
                        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG, "%s",
                                    rd_kafka_err2str(err));

                } else {
                        int32_t dummy_replicas[1] = {1};

                        /* Test invalid partition */
                        err = rd_kafka_NewTopic_set_replica_assignment(
                            new_topics[i], num_parts + 1, dummy_replicas, 1,
                            errstr, sizeof(errstr));
                        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                                    "%s: %s", rd_kafka_err2str(err),
                                    err == RD_KAFKA_RESP_ERR_NO_ERROR ? ""
                                                                      : errstr);

                        /* Setting replicas with with default replicas != -1
                         * is an error. */
                        err = rd_kafka_NewTopic_set_replica_assignment(
                            new_topics[i], 0, dummy_replicas, 1, errstr,
                            sizeof(errstr));
                        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                                    "%s: %s", rd_kafka_err2str(err),
                                    err == RD_KAFKA_RESP_ERR_NO_ERROR ? ""
                                                                      : errstr);
                }
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                my_opaque = (void *)123;
                rd_kafka_AdminOptions_set_opaque(options, my_opaque);
        }

        TIMING_START(&timing, "CreateTopics");
        TEST_SAY("Call CreateTopics, timeout is %dms\n", exp_timeout);
        rd_kafka_CreateTopics(rk, new_topics, MY_NEW_TOPICS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (with_background_event_cb) {
                /* Result event will be triggered by callback from
                 * librdkafka background queue thread. */
                TIMING_START(&timing, "CreateTopics.wait_background_event_cb");
                rkev = wait_background_event_cb();
        } else {
                /* Poll result queue */
                TIMING_START(&timing, "CreateTopics.queue_poll");
                rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        }

        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("CreateTopics: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_CreateTopics_result(rkev);
        TEST_ASSERT(res, "expected CreateTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected CreateTopics to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Attempt to extract topics anyway, should return NULL. */
        restopics = rd_kafka_CreateTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(!restopics && restopic_cnt == 0,
                    "expected no result_topics, got %p cnt %" PRIusz, restopics,
                    restopic_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_NewTopic_destroy_array(new_topics, MY_NEW_TOPICS_CNT);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        SUB_TEST_PASS();
}



/**
 * @brief DeleteTopics tests
 *
 *
 *
 */
static void do_test_DeleteTopics(const char *what,
                                 rd_kafka_t *rk,
                                 rd_kafka_queue_t *useq,
                                 int with_options) {
        rd_kafka_queue_t *q;
#define MY_DEL_TOPICS_CNT 4
        rd_kafka_DeleteTopic_t *del_topics[MY_DEL_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
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

        SUB_TEST_QUICK("%s DeleteTopics with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_DEL_TOPICS_CNT; i++)
                del_topics[i] = rd_kafka_DeleteTopic_new(
                    test_mk_topic_name(__FUNCTION__, 1));

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DELETETOPICS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DeleteTopics");
        TEST_SAY("Call DeleteTopics, timeout is %dms\n", exp_timeout);
        rd_kafka_DeleteTopics(rk, del_topics, MY_DEL_TOPICS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue */
        TIMING_START(&timing, "DeleteTopics.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteTopics: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteTopics_result(rkev);
        TEST_ASSERT(res, "expected DeleteTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DeleteTopics to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Attempt to extract topics anyway, should return NULL. */
        restopics = rd_kafka_DeleteTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(!restopics && restopic_cnt == 0,
                    "expected no result_topics, got %p cnt %" PRIusz, restopics,
                    restopic_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_DeleteTopic_destroy_array(del_topics, MY_DEL_TOPICS_CNT);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
#undef MY_DEL_TOPICS_CNT

        SUB_TEST_QUICK();
}

/**
 * @brief DeleteGroups tests
 *
 *
 *
 */
static void do_test_DeleteGroups(const char *what,
                                 rd_kafka_t *rk,
                                 rd_kafka_queue_t *useq,
                                 int with_options,
                                 rd_bool_t destroy) {
        rd_kafka_queue_t *q;
#define MY_DEL_GROUPS_CNT 4
        char *group_names[MY_DEL_GROUPS_CNT];
        rd_kafka_DeleteGroup_t *del_groups[MY_DEL_GROUPS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DeleteGroups_result_t *res;
        const rd_kafka_group_result_t **resgroups;
        size_t resgroup_cnt;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DeleteGroups with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_DEL_GROUPS_CNT; i++) {
                group_names[i] = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
                del_groups[i]  = rd_kafka_DeleteGroup_new(group_names[i]);
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DELETEGROUPS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DeleteGroups");
        TEST_SAY("Call DeleteGroups, timeout is %dms\n", exp_timeout);
        rd_kafka_DeleteGroups(rk, del_groups, MY_DEL_GROUPS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (destroy)
                goto destroy;

        /* Poll result queue */
        TIMING_START(&timing, "DeleteGroups.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteGroups: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteGroups_result(rkev);
        TEST_ASSERT(res, "expected DeleteGroups_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting no error (errors will be per-group) */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "expected DeleteGroups to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR_NO_ERROR),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Extract groups, should return MY_DEL_GROUPS_CNT groups. */
        resgroups = rd_kafka_DeleteGroups_result_groups(res, &resgroup_cnt);
        TEST_ASSERT(resgroups && resgroup_cnt == MY_DEL_GROUPS_CNT,
                    "expected %d result_groups, got %p cnt %" PRIusz,
                    MY_DEL_GROUPS_CNT, resgroups, resgroup_cnt);

        /* The returned groups should be in the original order, and
         * should all have timed out. */
        for (i = 0; i < MY_DEL_GROUPS_CNT; i++) {
                TEST_ASSERT(!strcmp(group_names[i],
                                    rd_kafka_group_result_name(resgroups[i])),
                            "expected group '%s' at position %d, not '%s'",
                            group_names[i], i,
                            rd_kafka_group_result_name(resgroups[i]));
                TEST_ASSERT(rd_kafka_error_code(rd_kafka_group_result_error(
                                resgroups[i])) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                            "expected group '%s' to have timed out, got %s",
                            group_names[i],
                            rd_kafka_error_string(
                                rd_kafka_group_result_error(resgroups[i])));
        }

        rd_kafka_event_destroy(rkev);

destroy:
        for (i = 0; i < MY_DEL_GROUPS_CNT; i++) {
                rd_kafka_DeleteGroup_destroy(del_groups[i]);
                rd_free(group_names[i]);
        }

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
#undef MY_DEL_GROUPS_CNT

        SUB_TEST_QUICK();
}

/**
 * @brief ListConsumerGroups tests
 *
 *
 *
 */
static void do_test_ListConsumerGroups(const char *what,
                                       rd_kafka_t *rk,
                                       rd_kafka_queue_t *useq,
                                       int with_options,
                                       rd_bool_t destroy) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_ListConsumerGroups_result_t *res;
        const rd_kafka_error_t **errors;
        size_t errors_cnt, valid_cnt;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s ListConsumerGroups with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        if (with_options) {
                rd_kafka_error_t *error;
                rd_kafka_consumer_group_state_t duplicate_states[2] = {
                    RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY,
                    RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY};
                rd_kafka_consumer_group_type_t duplicate_types[2] = {
                    RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC,
                    RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC};
                rd_kafka_consumer_group_type_t unknown_type[1] = {
                    RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN};

                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS);

                /* Test duplicate error on match states */
                error = rd_kafka_AdminOptions_set_match_consumer_group_states(
                    options, duplicate_states, 2);
                TEST_ASSERT(error && rd_kafka_error_code(error), "%s",
                            "Expected error on duplicate states,"
                            " got no error");
                rd_kafka_error_destroy(error);

                /* Test duplicate error on match group types */
                error = rd_kafka_AdminOptions_set_match_consumer_group_types(
                    options, duplicate_types, 2);
                TEST_ASSERT(error && rd_kafka_error_code(error), "%s",
                            "Expected error on duplicate group types,"
                            " got no error");
                rd_kafka_error_destroy(error);

                /* Test invalid args error on setting UNKNOWN group type in
                 * match group types */
                error = rd_kafka_AdminOptions_set_match_consumer_group_types(
                    options, unknown_type, 1);
                TEST_ASSERT(error && rd_kafka_error_code(error), "%s",
                            "Expected error on Unknown group type,"
                            " got no error");
                rd_kafka_error_destroy(error);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr)));

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "ListConsumerGroups");
        TEST_SAY("Call ListConsumerGroups, timeout is %dms\n", exp_timeout);
        rd_kafka_ListConsumerGroups(rk, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (destroy)
                goto destroy;

        /* Poll result queue */
        TIMING_START(&timing, "ListConsumerGroups.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("ListConsumerGroups: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_ListConsumerGroups_result(rkev);
        TEST_ASSERT(res, "expected ListConsumerGroups_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting no error here, the real error will be in the error array */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(
            err == RD_KAFKA_RESP_ERR_NO_ERROR,
            "expected ListConsumerGroups to return error %s, not %s (%s)",
            rd_kafka_err2str(RD_KAFKA_RESP_ERR_NO_ERROR), rd_kafka_err2str(err),
            err ? errstr2 : "n/a");

        errors = rd_kafka_ListConsumerGroups_result_errors(rkev, &errors_cnt);
        TEST_ASSERT(errors_cnt == 1, "expected one error, got %" PRIusz,
                    errors_cnt);
        rd_kafka_ListConsumerGroups_result_valid(rkev, &valid_cnt);
        TEST_ASSERT(valid_cnt == 0, "expected zero valid groups, got %" PRIusz,
                    valid_cnt);

        err     = rd_kafka_error_code(errors[0]);
        errstr2 = rd_kafka_error_string(errors[0]);
        TEST_ASSERT(
            err == RD_KAFKA_RESP_ERR__TIMED_OUT,
            "expected ListConsumerGroups to return error %s, not %s (%s)",
            rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
            rd_kafka_err2str(err), err ? errstr2 : "n/a");

        rd_kafka_event_destroy(rkev);

destroy:
        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
        SUB_TEST_PASS();
}

/**
 * @brief DescribeConsumerGroups tests
 *
 *
 *
 */
static void do_test_DescribeConsumerGroups(const char *what,
                                           rd_kafka_t *rk,
                                           rd_kafka_queue_t *useq,
                                           int with_options,
                                           rd_bool_t destroy) {
        rd_kafka_queue_t *q;
#define TEST_DESCRIBE_CONSUMER_GROUPS_CNT 4
        const char *group_names[TEST_DESCRIBE_CONSUMER_GROUPS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *error;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeConsumerGroups_result_t *res;
        const rd_kafka_ConsumerGroupDescription_t **resgroups;
        size_t resgroup_cnt;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DescribeConsumerGroups with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < TEST_DESCRIBE_CONSUMER_GROUPS_CNT; i++) {
                group_names[i] = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                if ((error =
                         rd_kafka_AdminOptions_set_include_authorized_operations(
                             options, 0))) {
                        fprintf(stderr,
                                "%% Failed to set require authorized "
                                "operations: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        TEST_FAIL(
                            "Failed to set include authorized operations\n");
                }

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DescribeConsumerGroups");
        TEST_SAY("Call DescribeConsumerGroups, timeout is %dms\n", exp_timeout);
        rd_kafka_DescribeConsumerGroups(
            rk, group_names, TEST_DESCRIBE_CONSUMER_GROUPS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (destroy)
                goto destroy;

        /* Poll result queue */
        TIMING_START(&timing, "DescribeConsumerGroups.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DescribeConsumerGroups: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DescribeConsumerGroups_result(rkev);
        TEST_ASSERT(res, "expected DescribeConsumerGroups_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting no error (errors will be per-group) */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(
            err == RD_KAFKA_RESP_ERR_NO_ERROR,
            "expected DescribeConsumerGroups to return error %s, not %s (%s)",
            rd_kafka_err2str(RD_KAFKA_RESP_ERR_NO_ERROR), rd_kafka_err2str(err),
            err ? errstr2 : "n/a");

        /* Extract groups, should return TEST_DESCRIBE_GROUPS_CNT groups. */
        resgroups =
            rd_kafka_DescribeConsumerGroups_result_groups(res, &resgroup_cnt);
        TEST_ASSERT(resgroups &&
                        resgroup_cnt == TEST_DESCRIBE_CONSUMER_GROUPS_CNT,
                    "expected %d result_groups, got %p cnt %" PRIusz,
                    TEST_DESCRIBE_CONSUMER_GROUPS_CNT, resgroups, resgroup_cnt);

        /* The returned groups should be in the original order, and
         * should all have timed out. */
        for (i = 0; i < TEST_DESCRIBE_CONSUMER_GROUPS_CNT; i++) {
                size_t authorized_operation_cnt;
                TEST_ASSERT(
                    !strcmp(group_names[i],
                            rd_kafka_ConsumerGroupDescription_group_id(
                                resgroups[i])),
                    "expected group '%s' at position %d, not '%s'",
                    group_names[i], i,
                    rd_kafka_ConsumerGroupDescription_group_id(resgroups[i]));
                TEST_ASSERT(
                    rd_kafka_error_code(rd_kafka_ConsumerGroupDescription_error(
                        resgroups[i])) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected group '%s' to have timed out, got %s",
                    group_names[i],
                    rd_kafka_error_string(
                        rd_kafka_ConsumerGroupDescription_error(resgroups[i])));

                rd_kafka_ConsumerGroupDescription_authorized_operations(
                    resgroups[i], &authorized_operation_cnt);
                TEST_ASSERT(authorized_operation_cnt == 0,
                            "Got authorized operations"
                            "when not requested");
        }

        rd_kafka_event_destroy(rkev);

destroy:
        for (i = 0; i < TEST_DESCRIBE_CONSUMER_GROUPS_CNT; i++) {
                rd_free((char *)group_names[i]);
        }

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
#undef TEST_DESCRIBE_CONSUMER_GROUPS_CNT

        SUB_TEST_PASS();
}

/**
 * @brief DescribeTopics tests
 *
 *
 *
 */
static void do_test_DescribeTopics(const char *what,
                                   rd_kafka_t *rk,
                                   rd_kafka_queue_t *useq,
                                   int with_options) {
        rd_kafka_queue_t *q;
#define TEST_DESCRIBE_TOPICS_CNT 4
        const char *topic_names[TEST_DESCRIBE_TOPICS_CNT];
        rd_kafka_TopicCollection_t *topics;
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *error;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeTopics_result_t *res;
        const rd_kafka_TopicDescription_t **restopics;
        size_t restopic_cnt;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DescribeTopics with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < TEST_DESCRIBE_TOPICS_CNT; i++) {
                topic_names[i] = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        }

        topics = rd_kafka_TopicCollection_of_topic_names(
            topic_names, TEST_DESCRIBE_TOPICS_CNT);

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                if ((error =
                         rd_kafka_AdminOptions_set_include_authorized_operations(
                             options, 0))) {
                        fprintf(stderr,
                                "%% Failed to set topic authorized operations: "
                                "%s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        TEST_FAIL(
                            "Failed to set topic authorized operations\n");
                }

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DescribeTopics");
        TEST_SAY("Call DescribeTopics, timeout is %dms\n", exp_timeout);
        rd_kafka_DescribeTopics(rk, topics, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue */
        TIMING_START(&timing, "DescribeTopics.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DescribeTopics: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DescribeTopics_result(rkev);
        TEST_ASSERT(res, "expected DescribeTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error (Fail while waiting for controller)*/
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DescribeTopics to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Extract topics, should return 0 topics. */
        restopics = rd_kafka_DescribeTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(!restopics && restopic_cnt == 0,
                    "expected no result topics, got %p cnt %" PRIusz, restopics,
                    restopic_cnt);

        rd_kafka_event_destroy(rkev);

        for (i = 0; i < TEST_DESCRIBE_TOPICS_CNT; i++) {
                rd_free((char *)topic_names[i]);
        }
        rd_kafka_TopicCollection_destroy(topics);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
#undef TEST_DESCRIBE_TOPICS_CNT

        SUB_TEST_PASS();
}

/**
 * @brief DescribeCluster tests
 *
 *
 *
 */
static void do_test_DescribeCluster(const char *what,
                                    rd_kafka_t *rk,
                                    rd_kafka_queue_t *useq,
                                    int with_options) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *error;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeCluster_result_t *res;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DescribeCluster with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                if ((error =
                         rd_kafka_AdminOptions_set_include_authorized_operations(
                             options, 0))) {
                        fprintf(stderr,
                                "%% Failed to set cluster authorized "
                                "operations: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        TEST_FAIL(
                            "Failed to set cluster authorized operations\n");
                }

                if (useq) {
                        my_opaque = (void *)456;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DescribeCluster");
        TEST_SAY("Call DescribeCluster, timeout is %dms\n", exp_timeout);
        rd_kafka_DescribeCluster(rk, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue */
        TIMING_START(&timing, "DescribeCluster.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DescribeCluster: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DescribeCluster_result(rkev);
        TEST_ASSERT(res, "expected DescribeCluster_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error (Fail while waiting for controller)*/
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DescribeCluster to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        SUB_TEST_PASS();
}

static void do_test_DeleteRecords(const char *what,
                                  rd_kafka_t *rk,
                                  rd_kafka_queue_t *useq,
                                  int with_options,
                                  rd_bool_t destroy) {
        rd_kafka_queue_t *q;
#define MY_DEL_RECORDS_CNT 4
        rd_kafka_AdminOptions_t *options         = NULL;
        rd_kafka_topic_partition_list_t *offsets = NULL;
        rd_kafka_DeleteRecords_t *del_records;
        const rd_kafka_DeleteRecords_result_t *res;
        char *topics[MY_DEL_RECORDS_CNT];
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DeleteRecords with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_DEL_RECORDS_CNT; i++) {
                topics[i] = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DELETERECORDS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;

                err = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)4567;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        offsets = rd_kafka_topic_partition_list_new(MY_DEL_RECORDS_CNT);

        for (i = 0; i < MY_DEL_RECORDS_CNT; i++)
                rd_kafka_topic_partition_list_add(offsets, topics[i], i)
                    ->offset = RD_KAFKA_OFFSET_END;

        del_records = rd_kafka_DeleteRecords_new(offsets);
        rd_kafka_topic_partition_list_destroy(offsets);

        TIMING_START(&timing, "DeleteRecords");
        TEST_SAY("Call DeleteRecords, timeout is %dms\n", exp_timeout);
        rd_kafka_DeleteRecords(rk, &del_records, 1, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);

        rd_kafka_DeleteRecords_destroy(del_records);

        if (destroy)
                goto destroy;

        /* Poll result queue */
        TIMING_START(&timing, "DeleteRecords.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteRecords: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteRecords_result(rkev);
        TEST_ASSERT(res, "expected DeleteRecords_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error (pre-fanout leader_req will fail) */
        err = rd_kafka_event_error(rkev);
        TEST_ASSERT(err, "expected DeleteRecords to fail");

        rd_kafka_event_destroy(rkev);

destroy:

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        for (i = 0; i < MY_DEL_RECORDS_CNT; i++)
                rd_free(topics[i]);

#undef MY_DEL_RECORDS_CNT

        SUB_TEST_PASS();
}


static void do_test_DeleteConsumerGroupOffsets(const char *what,
                                               rd_kafka_t *rk,
                                               rd_kafka_queue_t *useq,
                                               int with_options) {
        rd_kafka_queue_t *q;
#define MY_DEL_CGRPOFFS_CNT 1
        rd_kafka_AdminOptions_t *options = NULL;
        const rd_kafka_DeleteConsumerGroupOffsets_result_t *res;
        rd_kafka_DeleteConsumerGroupOffsets_t *cgoffsets[MY_DEL_CGRPOFFS_CNT];
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s DeleteConsumerGroupOffsets with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_DEL_CGRPOFFS_CNT; i++) {
                rd_kafka_topic_partition_list_t *partitions =
                    rd_kafka_topic_partition_list_new(3);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
                rd_kafka_topic_partition_list_add(partitions, "topic3", 15);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 1);
                cgoffsets[i] = rd_kafka_DeleteConsumerGroupOffsets_new(
                    "mygroup", partitions);
                rd_kafka_topic_partition_list_destroy(partitions);
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;

                err = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)99981;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TIMING_START(&timing, "DeleteConsumerGroupOffsets");
        TEST_SAY("Call DeleteConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_DeleteConsumerGroupOffsets(rk, cgoffsets, MY_DEL_CGRPOFFS_CNT,
                                            options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);

        /* Poll result queue */
        TIMING_START(&timing, "DeleteConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected DeleteConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err = rd_kafka_event_error(rkev);
        TEST_ASSERT(err, "expected DeleteConsumerGroupOffsets to fail");

        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        rd_kafka_DeleteConsumerGroupOffsets_destroy_array(cgoffsets,
                                                          MY_DEL_CGRPOFFS_CNT);

#undef MY_DEL_CGRPOFFS_CNT

        SUB_TEST_PASS();
}

/**
 * @brief AclBinding tests
 *
 *
 *
 */
static void do_test_AclBinding() {
        int i;
        char errstr[512];
        rd_kafka_AclBinding_t *new_acl;

        rd_bool_t valid_resource_types[] = {
            rd_false, rd_false, rd_true, rd_true, rd_true, rd_true, rd_false};
        rd_bool_t valid_resource_pattern_types[] = {
            rd_false, rd_false, rd_false, rd_true, rd_true, rd_false};
        rd_bool_t valid_acl_operation[] = {
            rd_false, rd_false, rd_true, rd_true, rd_true, rd_true, rd_true,
            rd_true,  rd_true,  rd_true, rd_true, rd_true, rd_true, rd_false};
        rd_bool_t valid_acl_permission_type[] = {rd_false, rd_false, rd_true,
                                                 rd_true, rd_false};
        const char *topic     = test_mk_topic_name(__FUNCTION__, 1);
        const char *principal = "User:test";
        const char *host      = "*";

        SUB_TEST_QUICK();

        // Valid acl binding
        *errstr = '\0';
        new_acl = rd_kafka_AclBinding_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(new_acl, "expected AclBinding");
        rd_kafka_AclBinding_destroy(new_acl);

        *errstr = '\0';
        new_acl = rd_kafka_AclBinding_new(
            RD_KAFKA_RESOURCE_TOPIC, NULL, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(!new_acl && !strcmp(errstr, "Invalid resource name"),
                    "expected error string \"Invalid resource name\", not %s",
                    errstr);

        *errstr = '\0';
        new_acl = rd_kafka_AclBinding_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            NULL, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(!new_acl && !strcmp(errstr, "Invalid principal"),
                    "expected error string \"Invalid principal\", not %s",
                    errstr);

        *errstr = '\0';
        new_acl = rd_kafka_AclBinding_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, NULL, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(!new_acl && !strcmp(errstr, "Invalid host"),
                    "expected error string \"Invalid host\", not %s", errstr);

        for (i = -1; i <= RD_KAFKA_RESOURCE__CNT; i++) {
                *errstr = '\0';
                new_acl = rd_kafka_AclBinding_new(
                    i, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal,
                    host, RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_resource_types[i]) {
                        TEST_ASSERT(new_acl, "expected AclBinding");
                        rd_kafka_AclBinding_destroy(new_acl);
                } else
                        TEST_ASSERT(
                            !new_acl &&
                                !strcmp(errstr, "Invalid resource type"),
                            "expected error string \"Invalid resource type\", "
                            "not %s",
                            errstr);
        }
        for (i = -1; i <= RD_KAFKA_RESOURCE_PATTERN_TYPE__CNT; i++) {
                *errstr = '\0';
                new_acl = rd_kafka_AclBinding_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic, i, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_resource_pattern_types[i]) {
                        TEST_ASSERT(new_acl, "expected AclBinding");
                        rd_kafka_AclBinding_destroy(new_acl);
                } else
                        TEST_ASSERT(
                            !new_acl &&
                                !strcmp(errstr,
                                        "Invalid resource pattern type"),
                            "expected error string \"Invalid resource pattern "
                            "type\", not %s",
                            errstr);
        }
        for (i = -1; i <= RD_KAFKA_ACL_OPERATION__CNT; i++) {
                *errstr = '\0';
                new_acl = rd_kafka_AclBinding_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal, host, i,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_acl_operation[i]) {
                        TEST_ASSERT(new_acl, "expected AclBinding");
                        rd_kafka_AclBinding_destroy(new_acl);
                } else
                        TEST_ASSERT(!new_acl &&
                                        !strcmp(errstr, "Invalid operation"),
                                    "expected error string \"Invalid "
                                    "operation\", not %s",
                                    errstr);
        }
        for (i = -1; i <= RD_KAFKA_ACL_PERMISSION_TYPE__CNT; i++) {
                *errstr = '\0';
                new_acl = rd_kafka_AclBinding_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL, i, errstr, sizeof(errstr));
                if (i >= 0 && valid_acl_permission_type[i]) {
                        TEST_ASSERT(new_acl, "expected AclBinding");
                        rd_kafka_AclBinding_destroy(new_acl);
                } else
                        TEST_ASSERT(
                            !new_acl &&
                                !strcmp(errstr, "Invalid permission type"),
                            "expected error string \"permission type\", not %s",
                            errstr);
        }

        SUB_TEST_PASS();
}

/**
 * @brief AclBindingFilter tests
 *
 *
 *
 */
static void do_test_AclBindingFilter() {
        int i;
        char errstr[512];
        rd_kafka_AclBindingFilter_t *new_acl_filter;

        rd_bool_t valid_resource_types[] = {rd_false, rd_true, rd_true, rd_true,
                                            rd_true,  rd_true, rd_false};
        rd_bool_t valid_resource_pattern_types[] = {
            rd_false, rd_true, rd_true, rd_true, rd_true, rd_false};
        rd_bool_t valid_acl_operation[] = {
            rd_false, rd_true, rd_true, rd_true, rd_true, rd_true, rd_true,
            rd_true,  rd_true, rd_true, rd_true, rd_true, rd_true, rd_false};
        rd_bool_t valid_acl_permission_type[] = {rd_false, rd_true, rd_true,
                                                 rd_true, rd_false};
        const char *topic     = test_mk_topic_name(__FUNCTION__, 1);
        const char *principal = "User:test";
        const char *host      = "*";

        SUB_TEST_QUICK();

        // Valid acl binding
        *errstr        = '\0';
        new_acl_filter = rd_kafka_AclBindingFilter_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(new_acl_filter, "expected AclBindingFilter");
        rd_kafka_AclBinding_destroy(new_acl_filter);

        *errstr        = '\0';
        new_acl_filter = rd_kafka_AclBindingFilter_new(
            RD_KAFKA_RESOURCE_TOPIC, NULL, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(new_acl_filter, "expected AclBindingFilter");
        rd_kafka_AclBinding_destroy(new_acl_filter);

        *errstr        = '\0';
        new_acl_filter = rd_kafka_AclBindingFilter_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            NULL, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(new_acl_filter, "expected AclBindingFilter");
        rd_kafka_AclBinding_destroy(new_acl_filter);

        *errstr        = '\0';
        new_acl_filter = rd_kafka_AclBindingFilter_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            principal, NULL, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        TEST_ASSERT(new_acl_filter, "expected AclBindingFilter");
        rd_kafka_AclBinding_destroy(new_acl_filter);

        for (i = -1; i <= RD_KAFKA_RESOURCE__CNT; i++) {
                *errstr        = '\0';
                new_acl_filter = rd_kafka_AclBindingFilter_new(
                    i, topic, RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal,
                    host, RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_resource_types[i]) {
                        TEST_ASSERT(new_acl_filter,
                                    "expected AclBindingFilter");
                        rd_kafka_AclBinding_destroy(new_acl_filter);
                } else
                        TEST_ASSERT(
                            !new_acl_filter &&
                                !strcmp(errstr, "Invalid resource type"),
                            "expected error string \"Invalid resource type\", "
                            "not %s",
                            errstr);
        }
        for (i = -1; i <= RD_KAFKA_RESOURCE_PATTERN_TYPE__CNT; i++) {
                *errstr        = '\0';
                new_acl_filter = rd_kafka_AclBindingFilter_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic, i, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_resource_pattern_types[i]) {
                        TEST_ASSERT(new_acl_filter,
                                    "expected AclBindingFilter");
                        rd_kafka_AclBinding_destroy(new_acl_filter);
                } else
                        TEST_ASSERT(
                            !new_acl_filter &&
                                !strcmp(errstr,
                                        "Invalid resource pattern type"),
                            "expected error string \"Invalid resource pattern "
                            "type\", not %s",
                            errstr);
        }
        for (i = -1; i <= RD_KAFKA_ACL_OPERATION__CNT; i++) {
                *errstr        = '\0';
                new_acl_filter = rd_kafka_AclBindingFilter_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal, host, i,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
                if (i >= 0 && valid_acl_operation[i]) {
                        TEST_ASSERT(new_acl_filter,
                                    "expected AclBindingFilter");
                        rd_kafka_AclBinding_destroy(new_acl_filter);
                } else
                        TEST_ASSERT(!new_acl_filter &&
                                        !strcmp(errstr, "Invalid operation"),
                                    "expected error string \"Invalid "
                                    "operation\", not %s",
                                    errstr);
        }
        for (i = -1; i <= RD_KAFKA_ACL_PERMISSION_TYPE__CNT; i++) {
                *errstr        = '\0';
                new_acl_filter = rd_kafka_AclBindingFilter_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL, i, errstr, sizeof(errstr));
                if (i >= 0 && valid_acl_permission_type[i]) {
                        TEST_ASSERT(new_acl_filter,
                                    "expected AclBindingFilter");
                        rd_kafka_AclBinding_destroy(new_acl_filter);
                } else
                        TEST_ASSERT(
                            !new_acl_filter &&
                                !strcmp(errstr, "Invalid permission type"),
                            "expected error string \"permission type\", not %s",
                            errstr);
        }

        SUB_TEST_PASS();
}


/**
 * @brief CreateAcls tests
 *
 *
 *
 */
static void do_test_CreateAcls(const char *what,
                               rd_kafka_t *rk,
                               rd_kafka_queue_t *useq,
                               rd_bool_t with_background_event_cb,
                               rd_bool_t with_options) {
        rd_kafka_queue_t *q;
#define MY_NEW_ACLS_CNT 2
        rd_kafka_AclBinding_t *new_acls[MY_NEW_ACLS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_CreateAcls_result_t *res;
        const rd_kafka_acl_result_t **resacls;
        size_t resacls_cnt;
        void *my_opaque       = NULL, *opaque;
        const char *principal = "User:test";
        const char *host      = "*";

        SUB_TEST_QUICK("%s CreaetAcls with %s, timeout %dms", rd_kafka_name(rk),
                       what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        /**
         * Construct AclBinding array
         */
        for (i = 0; i < MY_NEW_ACLS_CNT; i++) {
                const char *topic = test_mk_topic_name(__FUNCTION__, 1);
                new_acls[i]       = rd_kafka_AclBinding_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_LITERAL, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                my_opaque = (void *)123;
                rd_kafka_AdminOptions_set_opaque(options, my_opaque);
        }

        TIMING_START(&timing, "CreateAcls");
        TEST_SAY("Call CreateAcls, timeout is %dms\n", exp_timeout);
        rd_kafka_CreateAcls(rk, new_acls, MY_NEW_ACLS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (with_background_event_cb) {
                /* Result event will be triggered by callback from
                 * librdkafka background queue thread. */
                TIMING_START(&timing, "CreateAcls.wait_background_event_cb");
                rkev = wait_background_event_cb();
        } else {
                /* Poll result queue */
                TIMING_START(&timing, "CreateAcls.queue_poll");
                rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        }

        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("CreateAcls: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_CreateAcls_result(rkev);
        TEST_ASSERT(res, "expected CreateAcls_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected CreateAcls to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Attempt to extract acls results anyway, should return NULL. */
        resacls = rd_kafka_CreateAcls_result_acls(res, &resacls_cnt);
        TEST_ASSERT(!resacls && resacls_cnt == 0,
                    "expected no acl result, got %p cnt %" PRIusz, resacls,
                    resacls_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_AclBinding_destroy_array(new_acls, MY_NEW_ACLS_CNT);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_NEW_ACLS_CNT

        SUB_TEST_PASS();
}

/**
 * @brief DescribeAcls tests
 *
 *
 *
 */
static void do_test_DescribeAcls(const char *what,
                                 rd_kafka_t *rk,
                                 rd_kafka_queue_t *useq,
                                 rd_bool_t with_background_event_cb,
                                 rd_bool_t with_options) {
        rd_kafka_queue_t *q;
        rd_kafka_AclBindingFilter_t *describe_acls;
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeAcls_result_t *res;
        const rd_kafka_AclBinding_t **res_acls;
        size_t res_acls_cnt;
        void *my_opaque       = NULL, *opaque;
        const char *principal = "User:test";
        const char *host      = "*";

        SUB_TEST_QUICK("%s DescribeAcls with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        /**
         * Construct AclBindingFilter
         */
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        describe_acls     = rd_kafka_AclBindingFilter_new(
            RD_KAFKA_RESOURCE_TOPIC, topic, RD_KAFKA_RESOURCE_PATTERN_PREFIXED,
            principal, host, RD_KAFKA_ACL_OPERATION_ALL,
            RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                my_opaque = (void *)123;
                rd_kafka_AdminOptions_set_opaque(options, my_opaque);
        }

        TIMING_START(&timing, "DescribeAcls");
        TEST_SAY("Call DescribeAcls, timeout is %dms\n", exp_timeout);
        rd_kafka_DescribeAcls(rk, describe_acls, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (with_background_event_cb) {
                /* Result event will be triggered by callback from
                 * librdkafka background queue thread. */
                TIMING_START(&timing, "DescribeAcls.wait_background_event_cb");
                rkev = wait_background_event_cb();
        } else {
                /* Poll result queue */
                TIMING_START(&timing, "DescribeAcls.queue_poll");
                rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        }

        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DescribeAcls: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DescribeAcls_result(rkev);
        TEST_ASSERT(res, "expected DescribeAcls_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DescribeAcls to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Attempt to extract result acls anyway, should return NULL. */
        res_acls = rd_kafka_DescribeAcls_result_acls(res, &res_acls_cnt);
        TEST_ASSERT(!res_acls && res_acls_cnt == 0,
                    "expected no result acls, got %p cnt %" PRIusz, res_acls,
                    res_acls_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_AclBinding_destroy(describe_acls);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        SUB_TEST_PASS();
}


/**
 * @brief DeleteAcls tests
 *
 *
 *
 */
static void do_test_DeleteAcls(const char *what,
                               rd_kafka_t *rk,
                               rd_kafka_queue_t *useq,
                               rd_bool_t with_background_event_cb,
                               rd_bool_t with_options) {
#define DELETE_ACLS_FILTERS_CNT 2
        rd_kafka_queue_t *q;
        rd_kafka_AclBindingFilter_t *delete_acls[DELETE_ACLS_FILTERS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        int exp_timeout                  = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DeleteAcls_result_t *res;
        const rd_kafka_DeleteAcls_result_response_t **res_response;
        size_t res_response_cnt;
        void *my_opaque       = NULL, *opaque;
        const char *principal = "User:test";
        const char *host      = "*";

        SUB_TEST_QUICK("%s DeleteAcls with %s, timeout %dms", rd_kafka_name(rk),
                       what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        /**
         * Construct AclBindingFilter array
         */
        for (i = 0; i < DELETE_ACLS_FILTERS_CNT; i++) {
                const char *topic = test_mk_topic_name(__FUNCTION__, 1);
                delete_acls[i]    = rd_kafka_AclBindingFilter_new(
                    RD_KAFKA_RESOURCE_TOPIC, topic,
                    RD_KAFKA_RESOURCE_PATTERN_PREFIXED, principal, host,
                    RD_KAFKA_ACL_OPERATION_ALL,
                    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, errstr, sizeof(errstr));
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;
                err         = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                my_opaque = (void *)123;
                rd_kafka_AdminOptions_set_opaque(options, my_opaque);
        }

        TIMING_START(&timing, "DeleteAcls");
        TEST_SAY("Call DeleteAcls, timeout is %dms\n", exp_timeout);
        rd_kafka_DeleteAcls(rk, delete_acls, DELETE_ACLS_FILTERS_CNT, options,
                            q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        if (with_background_event_cb) {
                /* Result event will be triggered by callback from
                 * librdkafka background queue thread. */
                TIMING_START(&timing, "DeleteAcls.wait_background_event_cb");
                rkev = wait_background_event_cb();
        } else {
                /* Poll result queue */
                TIMING_START(&timing, "DeleteAcls.queue_poll");
                rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        }

        TIMING_ASSERT_LATER(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("DeleteAcls: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteAcls_result(rkev);
        TEST_ASSERT(res, "expected DeleteAcls_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err     = rd_kafka_event_error(rkev);
        errstr2 = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected DeleteAcls to return error %s, not %s (%s)",
                    rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                    rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Attempt to extract result responses anyway, should return NULL. */
        res_response =
            rd_kafka_DeleteAcls_result_responses(res, &res_response_cnt);
        TEST_ASSERT(!res_response && res_response_cnt == 0,
                    "expected no result response, got %p cnt %" PRIusz,
                    res_response, res_response_cnt);

        rd_kafka_event_destroy(rkev);

        rd_kafka_AclBinding_destroy_array(delete_acls, DELETE_ACLS_FILTERS_CNT);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef DELETE_ACLS_FILTERS_CNT

        SUB_TEST_PASS();
}


static void do_test_AlterConsumerGroupOffsets(const char *what,
                                              rd_kafka_t *rk,
                                              rd_kafka_queue_t *useq,
                                              int with_options) {
        rd_kafka_queue_t *q;
#define MY_ALTER_CGRPOFFS_CNT 1
        rd_kafka_AdminOptions_t *options = NULL;
        const rd_kafka_AlterConsumerGroupOffsets_result_t *res;
        rd_kafka_AlterConsumerGroupOffsets_t *cgoffsets[MY_ALTER_CGRPOFFS_CNT];
        rd_kafka_AlterConsumerGroupOffsets_t
            *cgoffsets_empty[MY_ALTER_CGRPOFFS_CNT];
        rd_kafka_AlterConsumerGroupOffsets_t
            *cgoffsets_negative[MY_ALTER_CGRPOFFS_CNT];
        rd_kafka_AlterConsumerGroupOffsets_t
            *cgoffsets_duplicate[MY_ALTER_CGRPOFFS_CNT];
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s AlterConsumerGroupOffsets with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_ALTER_CGRPOFFS_CNT; i++) {
                /* Call with three correct topic partitions. */
                rd_kafka_topic_partition_list_t *partitions =
                    rd_kafka_topic_partition_list_new(3);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 9)
                    ->offset = 9;
                rd_kafka_topic_partition_list_add(partitions, "topic3", 15)
                    ->offset = 15;
                rd_kafka_topic_partition_list_add(partitions, "topic1", 1)
                    ->offset = 1;
                cgoffsets[i] = rd_kafka_AlterConsumerGroupOffsets_new(
                    "mygroup", partitions);
                rd_kafka_topic_partition_list_destroy(partitions);

                /* Call with empty topic-partition list. */
                rd_kafka_topic_partition_list_t *partitions_empty =
                    rd_kafka_topic_partition_list_new(0);
                cgoffsets_empty[i] = rd_kafka_AlterConsumerGroupOffsets_new(
                    "mygroup", partitions_empty);
                rd_kafka_topic_partition_list_destroy(partitions_empty);

                /* Call with a topic-partition having negative offset. */
                rd_kafka_topic_partition_list_t *partitions_negative =
                    rd_kafka_topic_partition_list_new(4);
                rd_kafka_topic_partition_list_add(partitions_negative, "topic1",
                                                  9)
                    ->offset = 9;
                rd_kafka_topic_partition_list_add(partitions_negative, "topic3",
                                                  15)
                    ->offset = 15;
                rd_kafka_topic_partition_list_add(partitions_negative, "topic1",
                                                  1)
                    ->offset = 1;
                rd_kafka_topic_partition_list_add(partitions_negative, "topic1",
                                                  2)
                    ->offset          = -3;
                cgoffsets_negative[i] = rd_kafka_AlterConsumerGroupOffsets_new(
                    "mygroup", partitions_negative);
                rd_kafka_topic_partition_list_destroy(partitions_negative);

                /* Call with duplicate partitions. */
                rd_kafka_topic_partition_list_t *partitions_duplicate =
                    rd_kafka_topic_partition_list_new(3);
                rd_kafka_topic_partition_list_add(partitions_duplicate,
                                                  "topic1", 9)
                    ->offset = 9;
                rd_kafka_topic_partition_list_add(partitions_duplicate,
                                                  "topic3", 15)
                    ->offset = 15;
                rd_kafka_topic_partition_list_add(partitions_duplicate,
                                                  "topic1", 9)
                    ->offset = 1;

                cgoffsets_duplicate[i] = rd_kafka_AlterConsumerGroupOffsets_new(
                    "mygroup", partitions_duplicate);
                rd_kafka_topic_partition_list_destroy(partitions_duplicate);
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;

                err = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)99981;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        /* Empty topic-partition list */
        TIMING_START(&timing, "AlterConsumerGroupOffsets");
        TEST_SAY("Call AlterConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_AlterConsumerGroupOffsets(rk, cgoffsets_empty,
                                           MY_ALTER_CGRPOFFS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        rd_kafka_AlterConsumerGroupOffsets_destroy_array(cgoffsets_empty,
                                                         MY_ALTER_CGRPOFFS_CNT);

        /* Poll result queue */
        TIMING_START(&timing, "AlterConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, 0, 10);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("AlterConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);
        /* Convert event to proper result */
        res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected AlterConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));
        /* Expecting error */
        err                            = rd_kafka_event_error(rkev);
        const char *event_errstr_empty = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected AlterConsumerGroupOffsets to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected RD_KAFKA_RESP_ERR__INVALID_ARG, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(strcmp(event_errstr_empty,
                           "Non-empty topic partition list must be present") ==
                        0,
                    "expected \"Non-empty topic partition list must be "
                    "present\", not \"%s\"",
                    event_errstr_empty);
        rd_kafka_event_destroy(rkev);

        /* Negative topic-partition offset */
        TIMING_START(&timing, "AlterConsumerGroupOffsets");
        TEST_SAY("Call AlterConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_AlterConsumerGroupOffsets(rk, cgoffsets_negative,
                                           MY_ALTER_CGRPOFFS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        rd_kafka_AlterConsumerGroupOffsets_destroy_array(cgoffsets_negative,
                                                         MY_ALTER_CGRPOFFS_CNT);
        /* Poll result queue */
        TIMING_START(&timing, "AlterConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, 0, 10);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("AlterConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);
        /* Convert event to proper result */
        res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected AlterConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));
        /* Expecting error */
        err                               = rd_kafka_event_error(rkev);
        const char *event_errstr_negative = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected AlterConsumerGroupOffsets to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected RD_KAFKA_RESP_ERR__INVALID_ARG, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(
            strcmp(event_errstr_negative,
                   "All topic-partition offsets must be >= 0") == 0,
            "expected \"All topic-partition offsets must be >= 0\", not \"%s\"",
            event_errstr_negative);
        rd_kafka_event_destroy(rkev);

        /* Duplicate topic-partition offset */
        TIMING_START(&timing, "AlterConsumerGroupOffsets");
        TEST_SAY("Call AlterConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_AlterConsumerGroupOffsets(rk, cgoffsets_duplicate,
                                           MY_ALTER_CGRPOFFS_CNT, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        rd_kafka_AlterConsumerGroupOffsets_destroy_array(cgoffsets_duplicate,
                                                         MY_ALTER_CGRPOFFS_CNT);
        /* Poll result queue */
        TIMING_START(&timing, "AlterConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, 0, 10);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("AlterConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);
        /* Convert event to proper result */
        res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected AlterConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));
        /* Expecting error */
        err                                = rd_kafka_event_error(rkev);
        const char *event_errstr_duplicate = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected AlterConsumerGroupOffsets to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected RD_KAFKA_RESP_ERR__INVALID_ARG, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(strcmp(event_errstr_duplicate,
                           "Duplicate partitions not allowed") == 0,
                    "expected \"Duplicate partitions not allowed\", not \"%s\"",
                    event_errstr_duplicate);
        rd_kafka_event_destroy(rkev);

        /* Correct topic-partition list, local timeout */
        TIMING_START(&timing, "AlterConsumerGroupOffsets");
        TEST_SAY("Call AlterConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_AlterConsumerGroupOffsets(rk, cgoffsets, MY_ALTER_CGRPOFFS_CNT,
                                           options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        /* Poll result queue */
        TIMING_START(&timing, "AlterConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("AlterConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);
        /* Convert event to proper result */
        res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected AlterConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));
        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);
        /* Expecting error */
        err                      = rd_kafka_event_error(rkev);
        const char *event_errstr = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected AlterConsumerGroupOffsets to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected RD_KAFKA_RESP_ERR__TIMED_OUT, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(strcmp(event_errstr,
                           "Failed while waiting for response from broker: "
                           "Local: Timed out") == 0,
                    "expected \"Failed while waiting for response from broker: "
                    "Local: Timed out\", not \"%s\"",
                    event_errstr);
        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

        rd_kafka_AlterConsumerGroupOffsets_destroy_array(cgoffsets,
                                                         MY_ALTER_CGRPOFFS_CNT);

#undef MY_ALTER_CGRPOFFS_CNT

        SUB_TEST_PASS();
}


static void do_test_ListConsumerGroupOffsets(const char *what,
                                             rd_kafka_t *rk,
                                             rd_kafka_queue_t *useq,
                                             int with_options,
                                             rd_bool_t null_toppars) {
        rd_kafka_queue_t *q;
#define MY_LIST_CGRPOFFS_CNT 1
        rd_kafka_AdminOptions_t *options = NULL;
        const rd_kafka_ListConsumerGroupOffsets_result_t *res;
        rd_kafka_ListConsumerGroupOffsets_t *cgoffsets[MY_LIST_CGRPOFFS_CNT];
        rd_kafka_ListConsumerGroupOffsets_t
            *cgoffsets_empty[MY_LIST_CGRPOFFS_CNT];
        rd_kafka_ListConsumerGroupOffsets_t
            *cgoffsets_duplicate[MY_LIST_CGRPOFFS_CNT];
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        int i;
        char errstr[512];
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        void *my_opaque = NULL, *opaque;
        const char *errstr_ptr;

        SUB_TEST_QUICK("%s ListConsumerGroupOffsets with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        for (i = 0; i < MY_LIST_CGRPOFFS_CNT; i++) {
                rd_kafka_topic_partition_list_t *partitions =
                    rd_kafka_topic_partition_list_new(3);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
                rd_kafka_topic_partition_list_add(partitions, "topic3", 15);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 1);
                if (null_toppars) {
                        cgoffsets[i] = rd_kafka_ListConsumerGroupOffsets_new(
                            "mygroup", NULL);
                } else {
                        cgoffsets[i] = rd_kafka_ListConsumerGroupOffsets_new(
                            "mygroup", partitions);
                }
                rd_kafka_topic_partition_list_destroy(partitions);

                rd_kafka_topic_partition_list_t *partitions_empty =
                    rd_kafka_topic_partition_list_new(0);
                cgoffsets_empty[i] = rd_kafka_ListConsumerGroupOffsets_new(
                    "mygroup", partitions_empty);
                rd_kafka_topic_partition_list_destroy(partitions_empty);

                partitions = rd_kafka_topic_partition_list_new(3);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
                rd_kafka_topic_partition_list_add(partitions, "topic3", 15);
                rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
                cgoffsets_duplicate[i] = rd_kafka_ListConsumerGroupOffsets_new(
                    "mygroup", partitions);
                rd_kafka_topic_partition_list_destroy(partitions);
        }

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;

                err = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)99981;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        TEST_SAY(
            "Call ListConsumerGroupOffsets with empty topic-partition list.\n");
        rd_kafka_ListConsumerGroupOffsets(rk, cgoffsets_empty,
                                          MY_LIST_CGRPOFFS_CNT, options, q);
        rd_kafka_ListConsumerGroupOffsets_destroy_array(cgoffsets_empty,
                                                        MY_LIST_CGRPOFFS_CNT);
        /* Poll result queue */
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TEST_SAY("ListConsumerGroupOffsets: got %s\n",
                 rd_kafka_event_name(rkev));

        /* Expecting error */
        err = rd_kafka_event_error(rkev);
        TEST_ASSERT(err, "expected ListConsumerGroupOffsets to fail");

        errstr_ptr = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(
            !strcmp(errstr_ptr,
                    "NULL or non-empty topic partition list must be passed"),
            "expected error string \"NULL or non-empty topic partition list "
            "must be passed\", not %s",
            errstr_ptr);

        rd_kafka_event_destroy(rkev);


        TEST_SAY(
            "Call ListConsumerGroupOffsets with topic-partition list"
            "containing duplicates.\n");
        rd_kafka_ListConsumerGroupOffsets(rk, cgoffsets_duplicate, 1, options,
                                          q);
        rd_kafka_ListConsumerGroupOffsets_destroy_array(cgoffsets_duplicate,
                                                        MY_LIST_CGRPOFFS_CNT);
        /* Poll result queue */
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TEST_SAY("ListConsumerGroupOffsets: got %s\n",
                 rd_kafka_event_name(rkev));

        /* Expecting error */
        err = rd_kafka_event_error(rkev);
        TEST_ASSERT(err, "expected ListConsumerGroupOffsets to fail");

        errstr_ptr = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(!strcmp(errstr_ptr, "Duplicate partitions not allowed"),
                    "expected error string \"Duplicate partitions not allowed\""
                    ", not %s",
                    errstr_ptr);

        rd_kafka_event_destroy(rkev);


        TIMING_START(&timing, "ListConsumerGroupOffsets");
        TEST_SAY("Call ListConsumerGroupOffsets, timeout is %dms\n",
                 exp_timeout);
        rd_kafka_ListConsumerGroupOffsets(rk, cgoffsets, MY_LIST_CGRPOFFS_CNT,
                                          options, q);
        rd_kafka_ListConsumerGroupOffsets_destroy_array(cgoffsets,
                                                        MY_LIST_CGRPOFFS_CNT);
        TIMING_ASSERT_LATER(&timing, 0, 10);

        /* Poll result queue */
        TIMING_START(&timing, "ListConsumerGroupOffsets.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("ListConsumerGroupOffsets: got %s in %.3fs\n",
                 rd_kafka_event_name(rkev), TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_ListConsumerGroupOffsets_result(rkev);
        TEST_ASSERT(res, "expected ListConsumerGroupOffsets_result, not %s",
                    rd_kafka_event_name(rkev));

        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);

        /* Expecting error */
        err = rd_kafka_event_error(rkev);
        TEST_ASSERT(err, "expected ListConsumerGroupOffsets to fail");

        errstr_ptr = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(!strcmp(errstr_ptr,
                            "Failed while waiting for response from broker: "
                            "Local: Timed out"),
                    "expected error string \"Failed while waiting for response "
                    "from broker: Local: Timed out\", not %s",
                    errstr_ptr);

        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_LIST_CGRPOFFS_CNT

        SUB_TEST_PASS();
}

static void do_test_DescribeUserScramCredentials(const char *what,
                                                 rd_kafka_t *rk,
                                                 rd_kafka_queue_t *useq) {
        char errstr[512];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_queue_t *rkqu;

        SUB_TEST_QUICK("%s", what);

        rkqu = useq ? useq : rd_kafka_queue_new(rk);

        const char *users[2];
        users[0] = "Sam";
        users[1] = "Sam";

        /* Whenever a duplicate user is passed,
         * the request should fail with error code
         * RD_KAFKA_RESP_ERR__INVALID_ARG */
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 30 * 1000 /* 30s */, errstr, sizeof(errstr)));

        rd_kafka_DescribeUserScramCredentials(rk, users, RD_ARRAY_SIZE(users),
                                              options, rkqu);
        rd_kafka_AdminOptions_destroy(options);

        rkev = test_wait_admin_result(
            rkqu, RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT, 2000);

        TEST_ASSERT(
            rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__INVALID_ARG,
            "Expected \"Local: Invalid argument or configuration\", not %s",
            rd_kafka_err2str(rd_kafka_event_error(rkev)));

        rd_kafka_event_destroy(rkev);

        if (!useq)
                rd_kafka_queue_destroy(rkqu);

        SUB_TEST_PASS();
}

static void do_test_AlterUserScramCredentials(const char *what,
                                              rd_kafka_t *rk,
                                              rd_kafka_queue_t *useq) {
        char errstr[512];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_queue_t *rkqu;

        SUB_TEST_QUICK("%s", what);

        rkqu = useq ? useq : rd_kafka_queue_new(rk);

#if !WITH_SSL
        /* Whenever librdkafka wasn't built with OpenSSL,
         * the request should fail with error code
         * RD_KAFKA_RESP_ERR__INVALID_ARG */
        rd_kafka_UserScramCredentialAlteration_t *alterations_ssl[1];
        alterations_ssl[0] = rd_kafka_UserScramCredentialUpsertion_new(
            "user", RD_KAFKA_SCRAM_MECHANISM_SHA_256, 10000,
            (unsigned char *)"password", 8, (unsigned char *)"salt", 4);
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 30 * 1000 /* 30s */, errstr, sizeof(errstr)));

        rd_kafka_AlterUserScramCredentials(rk, alterations_ssl, 1, options,
                                           rkqu);
        rd_kafka_UserScramCredentialAlteration_destroy_array(
            alterations_ssl, RD_ARRAY_SIZE(alterations_ssl));
        rd_kafka_AdminOptions_destroy(options);

        rkev = test_wait_admin_result(
            rkqu, RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT, 2000);

        TEST_ASSERT(
            rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__INVALID_ARG,
            "Expected \"Local: Invalid argument or configuration\", not %s",
            rd_kafka_err2str(rd_kafka_event_error(rkev)));

        rd_kafka_event_destroy(rkev);
#endif

        rd_kafka_UserScramCredentialAlteration_t *alterations[1];
        alterations[0] = rd_kafka_UserScramCredentialDeletion_new(
            "", RD_KAFKA_SCRAM_MECHANISM_SHA_256);
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, 30 * 1000 /* 30s */, errstr, sizeof(errstr)));

        /* Whenever an empty array is passed,
         * the request should fail with error code
         * RD_KAFKA_RESP_ERR__INVALID_ARG */
        rd_kafka_AlterUserScramCredentials(rk, alterations, 0, options, rkqu);

        rkev = test_wait_admin_result(
            rkqu, RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT, 2000);

        TEST_ASSERT(
            rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__INVALID_ARG,
            "Expected \"Local: Invalid argument or configuration\", not %s",
            rd_kafka_err2str(rd_kafka_event_error(rkev)));

        rd_kafka_event_destroy(rkev);

        /* Whenever an empty user is passed,
         * the request should fail with error code
         * RD_KAFKA_RESP_ERR__INVALID_ARG */
        rd_kafka_AlterUserScramCredentials(
            rk, alterations, RD_ARRAY_SIZE(alterations), options, rkqu);
        rkev = test_wait_admin_result(
            rkqu, RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT, 2000);

        TEST_ASSERT(
            rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__INVALID_ARG,
            "Expected \"Local: Invalid argument or configuration\", not %s",
            rd_kafka_err2str(rd_kafka_event_error(rkev)));

        rd_kafka_event_destroy(rkev);


        rd_kafka_UserScramCredentialAlteration_destroy_array(
            alterations, RD_ARRAY_SIZE(alterations));
        rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(rkqu);

        SUB_TEST_PASS();
}

static void do_test_ElectLeaders(const char *what,
                                 rd_kafka_t *rk,
                                 rd_kafka_queue_t *useq,
                                 int with_options,
                                 rd_kafka_ElectionType_t election_type) {
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *rkev;
        rd_kafka_resp_err_t err;
        const rd_kafka_ElectLeaders_result_t *res;
        rd_kafka_ElectLeaders_t *duplicate_elect_leaders;
        rd_kafka_ElectLeaders_t *elect_leaders;
        int exp_timeout = MY_SOCKET_TIMEOUT_MS;
        test_timing_t timing;
        rd_kafka_topic_partition_list_t *partitions;
        char errstr[512];
        void *my_opaque = NULL, *opaque;

        SUB_TEST_QUICK("%s ElectLeaders with %s, timeout %dms",
                       rd_kafka_name(rk), what, exp_timeout);

        q = useq ? useq : rd_kafka_queue_new(rk);

        partitions = rd_kafka_topic_partition_list_new(3);
        rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
        rd_kafka_topic_partition_list_add(partitions, "topic3", 15);
        rd_kafka_topic_partition_list_add(partitions, "topic1", 1);
        elect_leaders = rd_kafka_ElectLeaders_new(election_type, partitions);
        rd_kafka_topic_partition_list_destroy(partitions);

        partitions = rd_kafka_topic_partition_list_new(3);
        rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
        rd_kafka_topic_partition_list_add(partitions, "topic3", 15);
        rd_kafka_topic_partition_list_add(partitions, "topic1", 9);
        duplicate_elect_leaders =
            rd_kafka_ElectLeaders_new(election_type, partitions);
        rd_kafka_topic_partition_list_destroy(partitions);

        if (with_options) {
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_ELECTLEADERS);

                exp_timeout = MY_SOCKET_TIMEOUT_MS * 2;

                err = rd_kafka_AdminOptions_set_request_timeout(
                    options, exp_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                if (useq) {
                        my_opaque = (void *)99981;
                        rd_kafka_AdminOptions_set_opaque(options, my_opaque);
                }
        }

        /*Duplicate topic-partition list*/
        TIMING_START(&timing, "ElectLeaders");
        TEST_SAY("Call ElectLeaders, timeout is %dms\n", exp_timeout);
        rd_kafka_ElectLeaders(rk, duplicate_elect_leaders, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        rd_kafka_ElectLeaders_destroy(duplicate_elect_leaders);

        /* Poll result queue */
        TIMING_START(&timing, "ElectLeaders.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, 0, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("ElectLeaders: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_ElectLeaders_result(rkev);
        TEST_ASSERT(res, "expected ElectLeaders_result, not %s",
                    rd_kafka_event_name(rkev));
        /*Expecting error*/
        err                                = rd_kafka_event_error(rkev);
        const char *event_errstr_duplicate = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected ElectLeaders to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "expected RD_KAFKA_RESP_ERR__INVALID_ARG, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(strcmp(event_errstr_duplicate,
                           "Duplicate partitions specified") == 0,
                    "expected \"Duplicate partitions specified\", not \"%s\"",
                    event_errstr_duplicate);
        rd_kafka_event_destroy(rkev);

        /*Correct topic-partition list*/
        TIMING_START(&timing, "ElectLeaders");
        TEST_SAY("Call ElectLeaders, timeout is %dms\n", exp_timeout);
        rd_kafka_ElectLeaders(rk, elect_leaders, options, q);
        TIMING_ASSERT_LATER(&timing, 0, 10);
        rd_kafka_ElectLeaders_destroy(elect_leaders);

        /* Poll result queue */
        TIMING_START(&timing, "ElectLeaders.queue_poll");
        rkev = rd_kafka_queue_poll(q, exp_timeout + 1000);
        TIMING_ASSERT(&timing, exp_timeout - 100, exp_timeout + 100);
        TEST_ASSERT(rkev != NULL, "expected result in %dms", exp_timeout);
        TEST_SAY("ElectLeaders: got %s in %.3fs\n", rd_kafka_event_name(rkev),
                 TIMING_DURATION(&timing) / 1000.0f);

        /* Convert event to proper result */
        res = rd_kafka_event_ElectLeaders_result(rkev);
        TEST_ASSERT(res, "expected ElectLeaders_result, not %s",
                    rd_kafka_event_name(rkev));
        opaque = rd_kafka_event_opaque(rkev);
        TEST_ASSERT(opaque == my_opaque, "expected opaque to be %p, not %p",
                    my_opaque, opaque);
        /*Expecting error*/
        err                   = rd_kafka_event_error(rkev);
        const char *event_err = rd_kafka_event_error_string(rkev);
        TEST_ASSERT(err, "expected ElectLeaders to fail");
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "expected RD_KAFKA_RESP_ERR__TIMED_OUT, not %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(strcmp(event_err,
                           "Failed while waiting for controller: "
                           "Local: Timed out") == 0,
                    "expected \"Failed while waiting for controller: "
                    "Local: Timed out\", not \"%s\"",
                    event_err);
        rd_kafka_event_destroy(rkev);

        if (options)
                rd_kafka_AdminOptions_destroy(options);
        if (!useq)
                rd_kafka_queue_destroy(q);

        SUB_TEST_PASS();
}

/**
 * @brief Test a mix of APIs using the same replyq.
 *
 *  - Create topics A,B
 *  - Delete topic B
 *  - Create topic C
 *  - Delete groups A,B,C
 *  - Delete records from A,B,C
 *  - Create extra partitions for topic D
 */
static void do_test_mix(rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
        char *topics[] = {"topicA", "topicB", "topicC"};
        int cnt        = 0;
        struct waiting {
                rd_kafka_event_type_t evtype;
                int seen;
        };
        struct waiting id1 = {RD_KAFKA_EVENT_CREATETOPICS_RESULT};
        struct waiting id2 = {RD_KAFKA_EVENT_DELETETOPICS_RESULT};
        struct waiting id3 = {RD_KAFKA_EVENT_CREATETOPICS_RESULT};
        struct waiting id4 = {RD_KAFKA_EVENT_DELETEGROUPS_RESULT};
        struct waiting id5 = {RD_KAFKA_EVENT_DELETERECORDS_RESULT};
        struct waiting id6 = {RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT};
        struct waiting id7 = {RD_KAFKA_EVENT_DELETECONSUMERGROUPOFFSETS_RESULT};
        struct waiting id8 = {RD_KAFKA_EVENT_DELETECONSUMERGROUPOFFSETS_RESULT};
        struct waiting id9 = {RD_KAFKA_EVENT_CREATETOPICS_RESULT};
        rd_kafka_topic_partition_list_t *offsets;


        SUB_TEST_QUICK();

        offsets = rd_kafka_topic_partition_list_new(3);
        rd_kafka_topic_partition_list_add(offsets, topics[0], 0)->offset =
            RD_KAFKA_OFFSET_END;
        rd_kafka_topic_partition_list_add(offsets, topics[1], 0)->offset =
            RD_KAFKA_OFFSET_END;
        rd_kafka_topic_partition_list_add(offsets, topics[2], 0)->offset =
            RD_KAFKA_OFFSET_END;

        test_CreateTopics_simple(rk, rkqu, topics, 2, 1, &id1);
        test_DeleteTopics_simple(rk, rkqu, &topics[1], 1, &id2);
        test_CreateTopics_simple(rk, rkqu, &topics[2], 1, 1, &id3);
        test_DeleteGroups_simple(rk, rkqu, topics, 3, &id4);
        test_DeleteRecords_simple(rk, rkqu, offsets, &id5);
        test_CreatePartitions_simple(rk, rkqu, "topicD", 15, &id6);
        test_DeleteConsumerGroupOffsets_simple(rk, rkqu, "mygroup", offsets,
                                               &id7);
        test_DeleteConsumerGroupOffsets_simple(rk, rkqu, NULL, NULL, &id8);
        /* Use broker-side defaults for partition count */
        test_CreateTopics_simple(rk, rkqu, topics, 2, -1, &id9);

        rd_kafka_topic_partition_list_destroy(offsets);

        while (cnt < 9) {
                rd_kafka_event_t *rkev;
                struct waiting *w;

                rkev = rd_kafka_queue_poll(rkqu, -1);
                TEST_ASSERT(rkev);

                TEST_SAY("Got event %s: %s\n", rd_kafka_event_name(rkev),
                         rd_kafka_event_error_string(rkev));

                w = rd_kafka_event_opaque(rkev);
                TEST_ASSERT(w);

                TEST_ASSERT(w->evtype == rd_kafka_event_type(rkev),
                            "Expected evtype %d, not %d (%s)", w->evtype,
                            rd_kafka_event_type(rkev),
                            rd_kafka_event_name(rkev));

                TEST_ASSERT(w->seen == 0, "Duplicate results");

                w->seen++;
                cnt++;

                rd_kafka_event_destroy(rkev);
        }

        SUB_TEST_PASS();
}


/**
 * @brief Test AlterConfigs and DescribeConfigs
 */
static void do_test_configs(rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
#define MY_CONFRES_CNT RD_KAFKA_RESOURCE__CNT + 2
        rd_kafka_ConfigResource_t *configs[MY_CONFRES_CNT];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_resp_err_t err;
        const rd_kafka_AlterConfigs_result_t *res;
        const rd_kafka_ConfigResource_t **rconfigs;
        size_t rconfig_cnt;
        char errstr[128];
        int i;

        SUB_TEST_QUICK();

        /* Check invalids */
        configs[0] = rd_kafka_ConfigResource_new((rd_kafka_ResourceType_t)-1,
                                                 "something");
        TEST_ASSERT(!configs[0]);

        configs[0] =
            rd_kafka_ConfigResource_new((rd_kafka_ResourceType_t)0, NULL);
        TEST_ASSERT(!configs[0]);


        for (i = 0; i < MY_CONFRES_CNT; i++) {
                int set_config = !(i % 2);

                /* librdkafka shall not limit the use of illogical
                 * or unknown settings, they are enforced by the broker. */
                configs[i] = rd_kafka_ConfigResource_new(
                    (rd_kafka_ResourceType_t)i, "3");
                TEST_ASSERT(configs[i] != NULL);

                if (set_config) {
                        rd_kafka_ConfigResource_set_config(configs[i],
                                                           "some.conf",
                                                           "which remains "
                                                           "unchecked");
                        rd_kafka_ConfigResource_set_config(
                            configs[i], "some.conf.null", NULL);
                }
        }


        options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);
        err = rd_kafka_AdminOptions_set_request_timeout(options, 1000, errstr,
                                                        sizeof(errstr));
        TEST_ASSERT(!err, "%s", errstr);

        /* AlterConfigs */
        rd_kafka_AlterConfigs(rk, configs, MY_CONFRES_CNT, options, rkqu);

        rkev = test_wait_admin_result(rkqu, RD_KAFKA_EVENT_ALTERCONFIGS_RESULT,
                                      2000);

        TEST_ASSERT(rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Expected timeout, not %s",
                    rd_kafka_event_error_string(rkev));

        res = rd_kafka_event_AlterConfigs_result(rkev);
        TEST_ASSERT(res);

        rconfigs = rd_kafka_AlterConfigs_result_resources(res, &rconfig_cnt);
        TEST_ASSERT(!rconfigs && !rconfig_cnt,
                    "Expected no result resources, got %" PRIusz, rconfig_cnt);

        rd_kafka_event_destroy(rkev);

        /* DescribeConfigs: reuse same configs and options */
        rd_kafka_DescribeConfigs(rk, configs, MY_CONFRES_CNT, options, rkqu);

        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_ConfigResource_destroy_array(configs, MY_CONFRES_CNT);

        rkev = test_wait_admin_result(
            rkqu, RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT, 2000);

        TEST_ASSERT(rd_kafka_event_error(rkev) == RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Expected timeout, not %s",
                    rd_kafka_event_error_string(rkev));

        res = rd_kafka_event_DescribeConfigs_result(rkev);
        TEST_ASSERT(res);

        rconfigs = rd_kafka_DescribeConfigs_result_resources(res, &rconfig_cnt);
        TEST_ASSERT(!rconfigs && !rconfig_cnt,
                    "Expected no result resources, got %" PRIusz, rconfig_cnt);

        rd_kafka_event_destroy(rkev);

        SUB_TEST_PASS();
}


/**
 * @brief Verify that an unclean rd_kafka_destroy() does not hang or crash.
 */
static void do_test_unclean_destroy(rd_kafka_type_t cltype, int with_mainq) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *q;
        rd_kafka_event_t *rkev;
        rd_kafka_DeleteTopic_t *topic;
        test_timing_t t_destroy;

        SUB_TEST_QUICK("Test unclean destroy using %s",
                       with_mainq ? "mainq" : "tempq");

        test_conf_init(&conf, NULL, 0);
        /* Remove brokers, if any, since this is a local test and we
         * rely on the controller not being found. */
        test_conf_set(conf, "bootstrap.servers", "");
        test_conf_set(conf, "socket.timeout.ms", "60000");
        if (test_consumer_group_protocol()) {
                test_conf_set(conf, "group.protocol",
                              test_consumer_group_protocol());
        }

        rk = rd_kafka_new(cltype, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "kafka_new(%d): %s", cltype, errstr);

        if (with_mainq)
                q = rd_kafka_queue_get_main(rk);
        else
                q = rd_kafka_queue_new(rk);

        topic = rd_kafka_DeleteTopic_new("test");
        rd_kafka_DeleteTopics(rk, &topic, 1, NULL, q);
        rd_kafka_DeleteTopic_destroy(topic);

        /* We're not expecting a result yet since DeleteTopics will attempt
         * to look up the controller for socket.timeout.ms (1 minute). */
        rkev = rd_kafka_queue_poll(q, 100);
        TEST_ASSERT(!rkev, "Did not expect result: %s",
                    rd_kafka_event_name(rkev));

        rd_kafka_queue_destroy(q);

        TEST_SAY(
            "Giving rd_kafka_destroy() 5s to finish, "
            "despite Admin API request being processed\n");
        test_timeout_set(5);
        TIMING_START(&t_destroy, "rd_kafka_destroy()");
        rd_kafka_destroy(rk);
        TIMING_STOP(&t_destroy);

        SUB_TEST_PASS();

        /* Restore timeout */
        test_timeout_set(60);
}


/**
 * @brief Test AdminOptions
 */
static void do_test_options(rd_kafka_t *rk) {
#define _all_apis                                                              \
        {                                                                      \
                RD_KAFKA_ADMIN_OP_CREATETOPICS,                                \
                    RD_KAFKA_ADMIN_OP_DELETETOPICS,                            \
                    RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,                        \
                    RD_KAFKA_ADMIN_OP_ALTERCONFIGS,                            \
                    RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS,                         \
                    RD_KAFKA_ADMIN_OP_DELETERECORDS,                           \
                    RD_KAFKA_ADMIN_OP_CREATEACLS,                              \
                    RD_KAFKA_ADMIN_OP_DESCRIBEACLS,                            \
                    RD_KAFKA_ADMIN_OP_DELETEACLS,                              \
                    RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,                      \
                    RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,                  \
                    RD_KAFKA_ADMIN_OP_DELETEGROUPS,                            \
                    RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,                \
                    RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,               \
                    RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS,              \
                    RD_KAFKA_ADMIN_OP_ELECTLEADERS,                            \
                    RD_KAFKA_ADMIN_OP_ANY /* Must be last */                   \
        }
        struct {
                const char *setter;
                const rd_kafka_admin_op_t valid_apis[17];
        } matrix[] = {
            {"request_timeout", _all_apis},
            {"operation_timeout",
             {RD_KAFKA_ADMIN_OP_CREATETOPICS, RD_KAFKA_ADMIN_OP_DELETETOPICS,
              RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,
              RD_KAFKA_ADMIN_OP_DELETERECORDS, RD_KAFKA_ADMIN_OP_ELECTLEADERS}},
            {"validate_only",
             {RD_KAFKA_ADMIN_OP_CREATETOPICS,
              RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,
              RD_KAFKA_ADMIN_OP_ALTERCONFIGS}},
            {"broker", _all_apis},
            {"require_stable_offsets",
             {RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS}},
            {"match_consumer_group_states",
             {RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS}},
            {"opaque", _all_apis},
            {NULL},
        };
        int i;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_consumer_group_state_t state[1] = {
            RD_KAFKA_CONSUMER_GROUP_STATE_STABLE};

        SUB_TEST_QUICK();

        for (i = 0; matrix[i].setter; i++) {
                static const rd_kafka_admin_op_t all_apis[] = _all_apis;
                const rd_kafka_admin_op_t *for_api;

                for (for_api = all_apis;; for_api++) {
                        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        rd_kafka_resp_err_t exp_err =
                            RD_KAFKA_RESP_ERR_NO_ERROR;
                        rd_kafka_error_t *error = NULL;
                        char errstr[512];
                        int fi;

                        options = rd_kafka_AdminOptions_new(rk, *for_api);
                        TEST_ASSERT(options, "AdminOptions_new(%d) failed",
                                    *for_api);

                        if (!strcmp(matrix[i].setter, "request_timeout"))
                                err = rd_kafka_AdminOptions_set_request_timeout(
                                    options, 1234, errstr, sizeof(errstr));
                        else if (!strcmp(matrix[i].setter, "operation_timeout"))
                                err =
                                    rd_kafka_AdminOptions_set_operation_timeout(
                                        options, 12345, errstr, sizeof(errstr));
                        else if (!strcmp(matrix[i].setter, "validate_only"))
                                err = rd_kafka_AdminOptions_set_validate_only(
                                    options, 1, errstr, sizeof(errstr));
                        else if (!strcmp(matrix[i].setter, "broker"))
                                err = rd_kafka_AdminOptions_set_broker(
                                    options, 5, errstr, sizeof(errstr));
                        else if (!strcmp(matrix[i].setter,
                                         "require_stable_offsets"))
                                error =
                                    rd_kafka_AdminOptions_set_require_stable_offsets(
                                        options, 0);
                        else if (!strcmp(matrix[i].setter,
                                         "match_consumer_group_states"))
                                error =
                                    rd_kafka_AdminOptions_set_match_consumer_group_states(
                                        options, state, 1);
                        else if (!strcmp(matrix[i].setter, "opaque")) {
                                rd_kafka_AdminOptions_set_opaque(
                                    options, (void *)options);
                                err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        } else
                                TEST_FAIL("Invalid setter: %s",
                                          matrix[i].setter);

                        if (error) {
                                err = rd_kafka_error_code(error);
                                snprintf(errstr, sizeof(errstr), "%s",
                                         rd_kafka_error_string(error));
                                rd_kafka_error_destroy(error);
                        }


                        TEST_SAYL(3,
                                  "AdminOptions_set_%s on "
                                  "RD_KAFKA_ADMIN_OP_%d options "
                                  "returned %s: %s\n",
                                  matrix[i].setter, *for_api,
                                  rd_kafka_err2name(err),
                                  err ? errstr : "success");

                        /* Scan matrix valid_apis to see if this
                         * setter should be accepted or not. */
                        if (exp_err) {
                                /* An expected error is already set */
                        } else if (*for_api != RD_KAFKA_ADMIN_OP_ANY) {
                                exp_err = RD_KAFKA_RESP_ERR__INVALID_ARG;

                                for (fi = 0; matrix[i].valid_apis[fi]; fi++) {
                                        if (matrix[i].valid_apis[fi] ==
                                            *for_api)
                                                exp_err =
                                                    RD_KAFKA_RESP_ERR_NO_ERROR;
                                }
                        } else {
                                exp_err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        }

                        if (err != exp_err)
                                TEST_FAIL_LATER(
                                    "Expected AdminOptions_set_%s "
                                    "for RD_KAFKA_ADMIN_OP_%d "
                                    "options to return %s, "
                                    "not %s",
                                    matrix[i].setter, *for_api,
                                    rd_kafka_err2name(exp_err),
                                    rd_kafka_err2name(err));

                        rd_kafka_AdminOptions_destroy(options);

                        if (*for_api == RD_KAFKA_ADMIN_OP_ANY)
                                break; /* This was the last one */
                }
        }

        /* Try an invalid for_api */
        options = rd_kafka_AdminOptions_new(rk, (rd_kafka_admin_op_t)1234);
        TEST_ASSERT(!options,
                    "Expected AdminOptions_new() to fail "
                    "with an invalid for_api, didn't.");

        TEST_LATER_CHECK();

        SUB_TEST_PASS();
}


static rd_kafka_t *create_admin_client(rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 0);
        /* Remove brokers, if any, since this is a local test and we
         * rely on the controller not being found. */
        test_conf_set(conf, "bootstrap.servers", "");
        test_conf_set(conf, "socket.timeout.ms", MY_SOCKET_TIMEOUT_MS_STR);
        if (test_consumer_group_protocol()) {
                test_conf_set(conf, "group.protocol",
                              test_consumer_group_protocol());
        }
        /* For use with the background queue */
        rd_kafka_conf_set_background_event_cb(conf, background_event_cb);

        rk = rd_kafka_new(cltype, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "kafka_new(%d): %s", cltype, errstr);

        return rk;
}


static void do_test_apis(rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        rd_kafka_queue_t *mainq, *backgroundq;

        mtx_init(&last_event_lock, mtx_plain);
        cnd_init(&last_event_cnd);

        do_test_unclean_destroy(cltype, 0 /*tempq*/);
        do_test_unclean_destroy(cltype, 1 /*mainq*/);

        rk = create_admin_client(cltype);

        mainq       = rd_kafka_queue_get_main(rk);
        backgroundq = rd_kafka_queue_get_background(rk);

        do_test_options(rk);

        do_test_CreateTopics("temp queue, no options", rk, NULL, 0, 0);
        do_test_CreateTopics("temp queue, no options, background_event_cb", rk,
                             backgroundq, 1, 0);
        do_test_CreateTopics("temp queue, options", rk, NULL, 0, 1);
        do_test_CreateTopics("main queue, options", rk, mainq, 0, 1);

        do_test_DeleteTopics("temp queue, no options", rk, NULL, 0);
        do_test_DeleteTopics("temp queue, options", rk, NULL, 1);
        do_test_DeleteTopics("main queue, options", rk, mainq, 1);

        do_test_ListConsumerGroups("temp queue, no options", rk, NULL, 0,
                                   rd_false);
        do_test_ListConsumerGroups("temp queue, options", rk, NULL, 1,
                                   rd_false);
        do_test_ListConsumerGroups("main queue", rk, mainq, 0, rd_false);

        do_test_DescribeConsumerGroups("temp queue, no options", rk, NULL, 0,
                                       rd_false);
        do_test_DescribeConsumerGroups("temp queue, options", rk, NULL, 1,
                                       rd_false);
        do_test_DescribeConsumerGroups("main queue, options", rk, mainq, 1,
                                       rd_false);

        do_test_DescribeTopics("temp queue, no options", rk, NULL, 0);
        do_test_DescribeTopics("temp queue, options", rk, NULL, 1);
        do_test_DescribeTopics("main queue, options", rk, mainq, 1);

        do_test_DescribeCluster("temp queue, no options", rk, NULL, 0);
        do_test_DescribeCluster("temp queue, options", rk, NULL, 1);
        do_test_DescribeCluster("main queue, options", rk, mainq, 1);

        do_test_DeleteGroups("temp queue, no options", rk, NULL, 0, rd_false);
        do_test_DeleteGroups("temp queue, options", rk, NULL, 1, rd_false);
        do_test_DeleteGroups("main queue, options", rk, mainq, 1, rd_false);

        do_test_DeleteRecords("temp queue, no options", rk, NULL, 0, rd_false);
        do_test_DeleteRecords("temp queue, options", rk, NULL, 1, rd_false);
        do_test_DeleteRecords("main queue, options", rk, mainq, 1, rd_false);

        do_test_DeleteConsumerGroupOffsets("temp queue, no options", rk, NULL,
                                           0);
        do_test_DeleteConsumerGroupOffsets("temp queue, options", rk, NULL, 1);
        do_test_DeleteConsumerGroupOffsets("main queue, options", rk, mainq, 1);

        do_test_AclBinding();
        do_test_AclBindingFilter();

        do_test_CreateAcls("temp queue, no options", rk, NULL, rd_false,
                           rd_false);
        do_test_CreateAcls("temp queue, options", rk, NULL, rd_false, rd_true);
        do_test_CreateAcls("main queue, options", rk, mainq, rd_false, rd_true);

        do_test_DescribeAcls("temp queue, no options", rk, NULL, rd_false,
                             rd_false);
        do_test_DescribeAcls("temp queue, options", rk, NULL, rd_false,
                             rd_true);
        do_test_DescribeAcls("main queue, options", rk, mainq, rd_false,
                             rd_true);

        do_test_DeleteAcls("temp queue, no options", rk, NULL, rd_false,
                           rd_false);
        do_test_DeleteAcls("temp queue, options", rk, NULL, rd_false, rd_true);
        do_test_DeleteAcls("main queue, options", rk, mainq, rd_false, rd_true);

        do_test_AlterConsumerGroupOffsets("temp queue, no options", rk, NULL,
                                          0);
        do_test_AlterConsumerGroupOffsets("temp queue, options", rk, NULL, 1);
        do_test_AlterConsumerGroupOffsets("main queue, options", rk, mainq, 1);

        do_test_ListConsumerGroupOffsets("temp queue, no options", rk, NULL, 0,
                                         rd_false);
        do_test_ListConsumerGroupOffsets("temp queue, options", rk, NULL, 1,
                                         rd_false);
        do_test_ListConsumerGroupOffsets("main queue, options", rk, mainq, 1,
                                         rd_false);
        do_test_ListConsumerGroupOffsets("temp queue, no options", rk, NULL, 0,
                                         rd_true);
        do_test_ListConsumerGroupOffsets("temp queue, options", rk, NULL, 1,
                                         rd_true);
        do_test_ListConsumerGroupOffsets("main queue, options", rk, mainq, 1,
                                         rd_true);

        do_test_DescribeUserScramCredentials("main queue", rk, mainq);
        do_test_DescribeUserScramCredentials("temp queue", rk, NULL);

        do_test_AlterUserScramCredentials("main queue", rk, mainq);
        do_test_AlterUserScramCredentials("temp queue", rk, NULL);

        do_test_ElectLeaders("main queue, options, Preffered Elections", rk,
                             mainq, 1, RD_KAFKA_ELECTION_TYPE_PREFERRED);
        do_test_ElectLeaders("main queue, options, Unclean Elections", rk,
                             mainq, 1, RD_KAFKA_ELECTION_TYPE_UNCLEAN);
        do_test_ElectLeaders("main queue, no options, Preffered Elections", rk,
                             mainq, 0, RD_KAFKA_ELECTION_TYPE_PREFERRED);
        do_test_ElectLeaders("main queue, no options, Unclean Elections", rk,
                             mainq, 0, RD_KAFKA_ELECTION_TYPE_UNCLEAN);
        do_test_ElectLeaders("temp queue, options, Preffered Elections", rk,
                             NULL, 1, RD_KAFKA_ELECTION_TYPE_PREFERRED);
        do_test_ElectLeaders("temp queue, options, Unclean Elections", rk, NULL,
                             1, RD_KAFKA_ELECTION_TYPE_UNCLEAN);
        do_test_ElectLeaders("temp queue, no options, Preffered Elections", rk,
                             NULL, 0, RD_KAFKA_ELECTION_TYPE_PREFERRED);
        do_test_ElectLeaders("temp queue, no options, Unclean Elections", rk,
                             NULL, 0, RD_KAFKA_ELECTION_TYPE_UNCLEAN);

        do_test_mix(rk, mainq);

        do_test_configs(rk, mainq);

        rd_kafka_queue_destroy(backgroundq);
        rd_kafka_queue_destroy(mainq);

        rd_kafka_destroy(rk);

        /*
         * Tests which require a unique unused client instance.
         */
        rk    = create_admin_client(cltype);
        mainq = rd_kafka_queue_get_main(rk);
        do_test_DeleteRecords("main queue, options, destroy", rk, mainq, 1,
                              rd_true /*destroy instance before finishing*/);
        rd_kafka_queue_destroy(mainq);
        rd_kafka_destroy(rk);

        rk    = create_admin_client(cltype);
        mainq = rd_kafka_queue_get_main(rk);
        do_test_DeleteGroups("main queue, options, destroy", rk, mainq, 1,
                             rd_true /*destroy instance before finishing*/);
        rd_kafka_queue_destroy(mainq);
        rd_kafka_destroy(rk);


        /* Done */
        mtx_destroy(&last_event_lock);
        cnd_destroy(&last_event_cnd);
}


int main_0080_admin_ut(int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}
