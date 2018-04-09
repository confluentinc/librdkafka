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
 * @brief Admin API integration tests.
 */


static int32_t *avail_brokers;
static size_t avail_broker_cnt;




static void do_test_CreateTopics (const char *what,
                                  rd_kafka_t *rk, rd_kafka_queue_t *useq,
                                  int op_timeout, rd_bool_t validate_only) {
        rd_kafka_queue_t *q = useq ? useq : rd_kafka_queue_new(rk);
#define MY_NEW_TOPICS_CNT 6
        char *topics[MY_NEW_TOPICS_CNT];
        rd_kafka_NewTopic_t *new_topics[MY_NEW_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_resp_err_t exp_topicerr[MY_NEW_TOPICS_CNT] = {0};
        rd_kafka_resp_err_t exp_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        /* Expected topics in metadata */
        rd_kafka_metadata_topic_t exp_mdtopics[MY_NEW_TOPICS_CNT] = {{0}};
        int exp_mdtopic_cnt = 0;
        /* Not expected topics in metadata */
        rd_kafka_metadata_topic_t exp_not_mdtopics[MY_NEW_TOPICS_CNT] = {{0}};
        int exp_not_mdtopic_cnt = 0;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_CreateTopics_result_t *res;
        const rd_kafka_topic_result_t **restopics;
        size_t restopic_cnt;
        int metadata_tmout ;
        int num_replicas = avail_broker_cnt;
        int32_t *replicas;

        /* Set up replicas */
        replicas = rd_alloca(sizeof(*replicas) * num_replicas);
        for (i = 0 ; i < num_replicas ; i++)
                replicas[i] = avail_brokers[i];

        TEST_SAY(_C_MAG "[ %s CreateTopics with %s, "
                 "op_timeout %d, validate_only %d ]\n",
                 rd_kafka_name(rk), what, op_timeout, validate_only);

        /**
         * Construct NewTopic array with different properties for
         * different partitions.
         */
        for (i = 0 ; i < MY_NEW_TOPICS_CNT ; i++) {
                char *topic = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
                int num_parts = i * 7 + 1;
                int add_config = (i & 1);
                int add_invalid_config = (i == 1);
                int set_replicas = !(i % 3);
                rd_kafka_resp_err_t this_exp_err = RD_KAFKA_RESP_ERR_NO_ERROR;

                topics[i] = topic;
                new_topics[i] = rd_kafka_NewTopic_new(topic,
                                                      num_parts,
                                                      set_replicas ? -1 :
                                                      num_replicas);

                if (add_config) {
                        /*
                         * Add various configuration properties
                         */
                        err = rd_kafka_NewTopic_add_config(
                                new_topics[i], "compression.type", "lz4");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

                        err = rd_kafka_NewTopic_add_config(
                                new_topics[i], "delete.retention.ms", "900");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }

                if (add_invalid_config) {
                        /* Add invalid config property */
                        err = rd_kafka_NewTopic_add_config(
                                new_topics[i],
                                "dummy.doesntexist",
                                "broker is verifying this");
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                        this_exp_err = RD_KAFKA_RESP_ERR_INVALID_CONFIG;
                }

                TEST_SAY("Expected result for topic #%d: %s "
                         "(add_config=%d, add_invalid_config=%d, "
                         "set_replicas=%d)\n",
                         i, rd_kafka_err2name(this_exp_err),
                         add_config, add_invalid_config, set_replicas);

                if (set_replicas) {
                        int32_t p;

                        /*
                         * Set valid replica assignments
                         */
                        for (p = 0 ; p < num_parts ; p++) {
                                err = rd_kafka_NewTopic_set_replica_assignment(
                                        new_topics[i], p,
                                        replicas, num_replicas);
                                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                        }
                }

                if (this_exp_err || validate_only) {
                        exp_topicerr[i] = this_exp_err;
                        exp_not_mdtopics[exp_not_mdtopic_cnt++].topic = topic;

                } else {
                        exp_mdtopics[exp_mdtopic_cnt].topic = topic;
                        exp_mdtopics[exp_mdtopic_cnt].partition_cnt =
                                num_parts;
                        exp_mdtopic_cnt++;
                }
        }

        if (op_timeout != -1 || validate_only) {
                options = rd_kafka_AdminOptions_new(rk);

                if (op_timeout != -1) {
                        err = rd_kafka_AdminOptions_set_operation_timeout(
                                options, op_timeout, errstr, sizeof(errstr));
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }

                if (validate_only) {
                        err = rd_kafka_AdminOptions_set_validate_only(
                                options, validate_only, errstr, sizeof(errstr));
                        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
                }
        }

        TIMING_START(&timing, "CreateTopics");
        TEST_SAY("Call CreateTopics\n");
        rd_kafka_admin_CreateTopics(rk, new_topics, MY_NEW_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue for CreateTopics result.
         * Print but otherwise ignore other event types
         * (typically generic Error events). */
        TIMING_START(&timing, "CreateTopics.queue_poll");
        do {
                rkev = rd_kafka_queue_poll(q, tmout_multip(20*1000));
                TEST_SAY("CreateTopics: got %s in %.3fms\n",
                         rd_kafka_event_name(rkev),
                         TIMING_DURATION(&timing) / 1000.0f);
                if (rd_kafka_event_error(rkev))
                        TEST_SAY("%s: %s\n",
                                 rd_kafka_event_name(rkev),
                                 rd_kafka_event_error_string(rkev));
        } while (rd_kafka_event_type(rkev) !=
                 RD_KAFKA_EVENT_CREATETOPICS_RESULT);

        /* Convert event to proper result */
        res = rd_kafka_event_CreateTopics_result(rkev);
        TEST_ASSERT(res, "expected CreateTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        /* Expecting error */
        errstr2 = (const char *)0x1;
        err = rd_kafka_CreateTopics_result_error(res, &errstr2);
        TEST_ASSERT(err == exp_err,
                    "expected CreateTopics to return %s, not %s (%s)",
                    rd_kafka_err2str(exp_err),
                    rd_kafka_err2str(err),
                    err ? errstr2 : "n/a");

        TEST_SAY("CreateTopics: returned %s (%s)\n",
                 rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Extract topics */
        restopics = rd_kafka_CreateTopics_result_topics(res, &restopic_cnt);


        /* Scan topics for proper fields and expected failures. */
        for (i = 0 ; i < (int)restopic_cnt ; i++) {
                const rd_kafka_topic_result_t *terr = restopics[i];

                /* Verify that topic order matches our request. */
                if (strcmp(rd_kafka_topic_result_name(terr), topics[i]))
                        TEST_FAIL_LATER("Topic result order mismatch at #%d: "
                                        "expected %s, got %s",
                                        i, topics[i],
                                        rd_kafka_topic_result_name(terr));

                TEST_SAY("CreateTopics result: #%d: %s: %s: %s\n",
                         i,
                         rd_kafka_topic_result_name(terr),
                         rd_kafka_err2name(rd_kafka_topic_result_error(terr)),
                         rd_kafka_topic_result_error_string(terr));
                if (rd_kafka_topic_result_error(terr) != exp_topicerr[i])
                        TEST_FAIL_LATER(
                                "Expected %s, not %d: %s",
                                rd_kafka_err2name(exp_topicerr[i]),
                                rd_kafka_topic_result_error(terr),
                                rd_kafka_err2name(rd_kafka_topic_result_error(
                                                          terr)));
        }

        /**
         * Verify that the expecteded topics are created and the non-expected
         * are not. Allow it some time to propagate.
         */
        if (validate_only) {
                /* No topics should have been created, give it some time
                 * before checking. */
                rd_sleep(2);
                metadata_tmout = 5 * 1000;
        } else {
                if (op_timeout > 0)
                        metadata_tmout = op_timeout + 1000;
                else
                        metadata_tmout = 10 * 1000;
        }

        test_wait_metadata_update(rk,
                                  exp_mdtopics,
                                  exp_mdtopic_cnt,
                                  exp_not_mdtopics,
                                  exp_not_mdtopic_cnt,
                                  metadata_tmout);

        rd_kafka_event_destroy(rkev);

        for (i = 0 ; i < MY_NEW_TOPICS_CNT ; i++) {
                rd_kafka_NewTopic_destroy(new_topics[i]);
                rd_free(topics[i]);
        }

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_NEW_TOPICS_CNT
}




/**
 * @brief Test deletion of topics
 *
 *
 */
static void do_test_DeleteTopics (const char *what,
                                  rd_kafka_t *rk, rd_kafka_queue_t *useq,
                                  int op_timeout) {
        rd_kafka_queue_t *q = useq ? useq : rd_kafka_queue_new(rk);
        const int skip_topic_cnt = 2;
#define MY_DEL_TOPICS_CNT 9
        char *topics[MY_DEL_TOPICS_CNT];
        rd_kafka_DeleteTopic_t *del_topics[MY_DEL_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_resp_err_t exp_topicerr[MY_DEL_TOPICS_CNT] = {0};
        rd_kafka_resp_err_t exp_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        /* Expected topics in metadata */
        rd_kafka_metadata_topic_t exp_mdtopics[MY_DEL_TOPICS_CNT] = {{0}};
        int exp_mdtopic_cnt = 0;
        /* Not expected topics in metadata */
        rd_kafka_metadata_topic_t exp_not_mdtopics[MY_DEL_TOPICS_CNT] = {{0}};
        int exp_not_mdtopic_cnt = 0;
        int i;
        char errstr[512];
        const char *errstr2;
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        rd_kafka_event_t *rkev;
        const rd_kafka_DeleteTopics_result_t *res;
        const rd_kafka_topic_result_t **restopics;
        size_t restopic_cnt;
        int metadata_tmout;

        TEST_SAY(_C_MAG "[ %s DeleteTopics with %s, op_timeout %d ]\n",
                 rd_kafka_name(rk), what, op_timeout);

        /**
         * Construct DeleteTopic array
         */
        for (i = 0 ; i < MY_DEL_TOPICS_CNT ; i++) {
                char *topic = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
                int notexist_topic = i >= MY_DEL_TOPICS_CNT - skip_topic_cnt;

                topics[i] = topic;

                del_topics[i] = rd_kafka_DeleteTopic_new(topic);

                if (notexist_topic)
                        exp_topicerr[i] =
                                RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
                else {
                        exp_topicerr[i] =
                                RD_KAFKA_RESP_ERR_NO_ERROR;

                        exp_mdtopics[exp_mdtopic_cnt++].topic = topic;
                }

                exp_not_mdtopics[exp_not_mdtopic_cnt++].topic = topic;
        }

        if (op_timeout != -1) {
                options = rd_kafka_AdminOptions_new(rk);

                err = rd_kafka_AdminOptions_set_operation_timeout(
                        options, op_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
        }


        /* Create the topics first, minus the skip count. */
        test_CreateTopics_simple(rk, NULL, topics,
                                 MY_DEL_TOPICS_CNT-skip_topic_cnt,
                                 2/*num_partitions*/,
                                 NULL);

        /* Verify that topics are reported by metadata */
        test_wait_metadata_update(rk,
                                  exp_mdtopics, exp_mdtopic_cnt,
                                  NULL, 0,
                                  15*1000);

        TIMING_START(&timing, "DeleteTopics");
        TEST_SAY("Call DeleteTopics\n");
        rd_kafka_admin_DeleteTopics(rk, del_topics, MY_DEL_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        /* Poll result queue for DeleteTopics result.
         * Print but otherwise ignore other event types
         * (typically generic Error events). */
        TIMING_START(&timing, "DeleteTopics.queue_poll");
        while (1) {
                rkev = rd_kafka_queue_poll(q, tmout_multip(20*1000));
                TEST_SAY("DeleteTopics: got %s in %.3fms\n",
                         rd_kafka_event_name(rkev),
                         TIMING_DURATION(&timing) / 1000.0f);
                if (rd_kafka_event_error(rkev))
                        TEST_SAY("%s: %s\n",
                                 rd_kafka_event_name(rkev),
                                 rd_kafka_event_error_string(rkev));

                if (rd_kafka_event_type(rkev) ==
                    RD_KAFKA_EVENT_DELETETOPICS_RESULT)
                        break;

                rd_kafka_event_destroy(rkev);
        }

        /* Convert event to proper result */
        res = rd_kafka_event_DeleteTopics_result(rkev);
        TEST_ASSERT(res, "expected DeleteTopics_result, not %s",
                    rd_kafka_event_name(rkev));

        /* Expecting error */
        errstr2 = (const char *)0x1;
        err = rd_kafka_DeleteTopics_result_error(res, &errstr2);
        TEST_ASSERT(err == exp_err,
                    "expected DeleteTopics to return %s, not %s (%s)",
                    rd_kafka_err2str(exp_err),
                    rd_kafka_err2str(err),
                    err ? errstr2 : "n/a");

        TEST_SAY("DeleteTopics: returned %s (%s)\n",
                 rd_kafka_err2str(err), err ? errstr2 : "n/a");

        /* Extract topics */
        restopics = rd_kafka_DeleteTopics_result_topics(res, &restopic_cnt);


        /* Scan topics for proper fields and expected failures. */
        for (i = 0 ; i < (int)restopic_cnt ; i++) {
                const rd_kafka_topic_result_t *terr = restopics[i];

                /* Verify that topic order matches our request. */
                if (strcmp(rd_kafka_topic_result_name(terr), topics[i]))
                        TEST_FAIL_LATER("Topic result order mismatch at #%d: "
                                        "expected %s, got %s",
                                        i, topics[i],
                                        rd_kafka_topic_result_name(terr));

                TEST_SAY("DeleteTopics result: #%d: %s: %s: %s\n",
                         i,
                         rd_kafka_topic_result_name(terr),
                         rd_kafka_err2name(rd_kafka_topic_result_error(terr)),
                         rd_kafka_topic_result_error_string(terr));
                if (rd_kafka_topic_result_error(terr) != exp_topicerr[i])
                        TEST_FAIL_LATER(
                                "Expected %s, not %d: %s",
                                rd_kafka_err2name(exp_topicerr[i]),
                                rd_kafka_topic_result_error(terr),
                                rd_kafka_err2name(rd_kafka_topic_result_error(
                                                          terr)));
        }

        /**
         * Verify that the expected topics are deleted and the non-expected
         * are not. Allow it some time to propagate.
         */
        if (op_timeout > 0)
                metadata_tmout = op_timeout + 1000;
        else
                metadata_tmout = 10 * 1000;

        test_wait_metadata_update(rk,
                                  NULL, 0,
                                  exp_not_mdtopics,
                                  exp_not_mdtopic_cnt,
                                  metadata_tmout);

        rd_kafka_event_destroy(rkev);

        for (i = 0 ; i < MY_DEL_TOPICS_CNT ; i++) {
                rd_kafka_DeleteTopic_destroy(del_topics[i]);
                rd_free(topics[i]);
        }

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_DEL_TOPICS_CNT
}



/**
 * @brief Test creation of partitions
 *
 *
 */
static void do_test_CreatePartitions (const char *what,
                                      rd_kafka_t *rk, rd_kafka_queue_t *useq,
                                      int op_timeout) {
        rd_kafka_queue_t *q = useq ? useq : rd_kafka_queue_new(rk);
#define MY_CRP_TOPICS_CNT 9
        char *topics[MY_CRP_TOPICS_CNT];
        rd_kafka_NewTopic_t *new_topics[MY_CRP_TOPICS_CNT];
        rd_kafka_NewPartitions_t *crp_topics[MY_CRP_TOPICS_CNT];
        rd_kafka_AdminOptions_t *options = NULL;
        /* Expected topics in metadata */
        rd_kafka_metadata_topic_t exp_mdtopics[MY_CRP_TOPICS_CNT] = {{0}};
        rd_kafka_metadata_partition_t exp_mdparts[2] = {{0}};
        int exp_mdtopic_cnt = 0;
        int i;
        char errstr[512];
        rd_kafka_resp_err_t err;
        test_timing_t timing;
        int metadata_tmout;
        int num_replicas = avail_broker_cnt;

        TEST_SAY(_C_MAG "[ %s CreatePartitions with %s, op_timeout %d ]\n",
                 rd_kafka_name(rk), what, op_timeout);

        /* Set up two expected partitions with different replication sets
         * so they can be matched by the metadata checker later.
         * Even partitions use exp_mdparts[0] while odd partitions
         * use exp_mdparts[1]. */

        /* Set valid replica assignments (even, and odd (reverse) ) */
        exp_mdparts[0].replicas = rd_alloca(sizeof(*exp_mdparts[0].replicas) *
                                            num_replicas);
        exp_mdparts[1].replicas = rd_alloca(sizeof(*exp_mdparts[1].replicas) *
                                            num_replicas);
        exp_mdparts[0].replica_cnt = num_replicas;
        exp_mdparts[1].replica_cnt = num_replicas;
        for (i = 0 ; i < num_replicas ; i++) {
                exp_mdparts[0].replicas[i] = avail_brokers[i];
                exp_mdparts[1].replicas[i] = avail_brokers[num_replicas-i-1];
        }

        /**
         * Construct CreatePartitions array
         */
        for (i = 0 ; i < MY_CRP_TOPICS_CNT ; i++) {
                char *topic = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
                int initial_part_cnt = 1 + (i * 2);
                int new_part_cnt = 1 + (i / 2);
                int final_part_cnt = initial_part_cnt + new_part_cnt;
                int set_replicas = !(i % 2);
                int pi;

                topics[i] = topic;

                /* Topic to create with initial partition count */
                new_topics[i] = rd_kafka_NewTopic_new(topic, initial_part_cnt,
                                                      set_replicas ?
                                                      -1 : num_replicas);

                /* .. and later add more partitions to */
                crp_topics[i] = rd_kafka_NewPartitions_new(topic,
                                                           final_part_cnt);

                if (set_replicas) {
                        exp_mdtopics[exp_mdtopic_cnt].partitions =
                                rd_alloca(final_part_cnt *
                                          sizeof(*exp_mdtopics[exp_mdtopic_cnt].
                                                 partitions));

                        for (pi = 0 ; pi < final_part_cnt ; pi++) {
                                const rd_kafka_metadata_partition_t *exp_mdp =
                                        &exp_mdparts[pi & 1];

                                exp_mdtopics[exp_mdtopic_cnt].
                                        partitions[pi] = *exp_mdp; /* copy */

                                exp_mdtopics[exp_mdtopic_cnt].
                                        partitions[pi].id = pi;

                                if (pi < initial_part_cnt) {
                                        /* Set replica assignment
                                         * for initial partitions */
                                        err = rd_kafka_NewTopic_set_replica_assignment(
                                                new_topics[i], pi,
                                                exp_mdp->replicas,
                                                (size_t)exp_mdp->replica_cnt);
                                        TEST_ASSERT(!err, "NewTopic_set_replica_assignment: %s", rd_kafka_err2str(err));
                                } else {
                                        /* Set replica assignment for new
                                         * partitions */
                                        err = rd_kafka_NewPartitions_set_replica_assignment(
                                                crp_topics[i],
                                                pi - initial_part_cnt,
                                                exp_mdp->replicas,
                                                (size_t)exp_mdp->replica_cnt);
                                        TEST_ASSERT(!err, "NewPartitions_set_replica_assignment: %s", rd_kafka_err2str(err));
                                }

                        }
                }

                TEST_SAY(_C_YEL "Topic %s with %d initial partitions will grow "
                         "by %d to %d total partitions with%s replicas set\n",
                         topics[i],
                         initial_part_cnt, new_part_cnt, final_part_cnt,
                         set_replicas ? "" : "out");

                exp_mdtopics[exp_mdtopic_cnt].topic = topic;
                exp_mdtopics[exp_mdtopic_cnt].partition_cnt = final_part_cnt;

                exp_mdtopic_cnt++;
        }

        if (op_timeout != -1) {
                options = rd_kafka_AdminOptions_new(rk);

                err = rd_kafka_AdminOptions_set_operation_timeout(
                        options, op_timeout, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));
        }

        /*
         * Create topics with initial partition count
         */
        TIMING_START(&timing, "CreateTopics");
        TEST_SAY("Creating topics with initial partition counts\n");
        rd_kafka_admin_CreateTopics(rk, new_topics, MY_CRP_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        err = test_wait_any_admin_result(q, RD_KAFKA_EVENT_CREATETOPICS_RESULT,
                                         15000);
        TEST_ASSERT(!err, "CreateTopics failed: %s", rd_kafka_err2str(err));

        rd_kafka_NewTopic_destroy_array(new_topics, MY_CRP_TOPICS_CNT);


        /*
         * Create new partitions
         */
        TIMING_START(&timing, "CreatePartitions");
        TEST_SAY("Creating partitions\n");
        rd_kafka_admin_CreatePartitions(rk, crp_topics, MY_CRP_TOPICS_CNT,
                                    options, q);
        TIMING_ASSERT_LATER(&timing, 0, 50);

        err = test_wait_any_admin_result(q,
                                         RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT,
                                         15000);
        TEST_ASSERT(!err, "CreatePartitions failed: %s", rd_kafka_err2str(err));

        rd_kafka_NewPartitions_destroy_array(crp_topics, MY_CRP_TOPICS_CNT);


        /**
         * Verify that the expected topics are deleted and the non-expected
         * are not. Allow it some time to propagate.
         */
        if (op_timeout > 0)
                metadata_tmout = op_timeout + 1000;
        else
                metadata_tmout = 10 * 1000;

        test_wait_metadata_update(rk,
                                  exp_mdtopics,
                                  exp_mdtopic_cnt,
                                  NULL, 0,
                                  metadata_tmout);

        for (i = 0 ; i < MY_CRP_TOPICS_CNT ; i++)
                rd_free(topics[i]);

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);

#undef MY_CRP_TOPICS_CNT
}



static void do_test_apis (rd_kafka_type_t cltype) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *mainq;

        /* Get the available brokers, but use a separate rd_kafka_t instance
         * so we don't jinx the tests by having up-to-date metadata. */
        avail_brokers = test_get_broker_ids(NULL, &avail_broker_cnt);
        TEST_SAY("%"PRIusz" brokers in cluster "
                 "which will be used for replica sets\n",
                 avail_broker_cnt);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "socket.timeout.ms", "10000");
        rk = test_create_handle(cltype, conf);

        mainq = rd_kafka_queue_get_main(rk);

        /* Create topics */
        do_test_CreateTopics("temp queue, op timeout 0",
                             rk, NULL, 0, 0);
        do_test_CreateTopics("temp queue, op timeout 15000",
                             rk, NULL, 15000, 0);
        do_test_CreateTopics("temp queue, op timeout 300, "
                             "validate only",
                             rk, NULL, 300, rd_true);
        do_test_CreateTopics("temp queue, op timeout 9000, validate_only",
                             rk, NULL, 9000, rd_true);
        do_test_CreateTopics("main queue, options", rk, mainq, -1, 0);

        /* Delete topics */
        do_test_DeleteTopics("temp queue, op timeout 0", rk, NULL, 0);
        do_test_DeleteTopics("main queue, op timeout 15000", rk, mainq, 1500);

        /* Create Partitions */
        do_test_CreatePartitions("temp queue, op timeout 6500", rk, NULL, 6500);
        do_test_CreatePartitions("main queue, op timeout 0", rk, mainq, 0);

        rd_kafka_queue_destroy(mainq);

        rd_kafka_destroy(rk);

        free(avail_brokers);
}


int main_0081_admin (int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}
