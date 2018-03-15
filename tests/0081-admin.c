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


/**
 * @brief Verify that all topics in \p topics are reported in metadata.
 *
 * @returns the number of failures (but does not FAIL).
 */
static int verify_topics_in_metadata (rd_kafka_t *rk,
                                      rd_kafka_metadata_topic_t *topics,
                                      int topic_cnt,
                                      rd_kafka_metadata_topic_t *not_topics,
                                      int not_topic_cnt) {
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t err;
        int ti, i;
        int fails = 0;

        /* Mark expected topics with dummy error which is overwritten
         * when topic is found in metadata, allowing us to check
         * for missed topics. */
        for (i = 0 ; i < topic_cnt ; i++)
                topics[i].err = -12345;

        err = rd_kafka_metadata(rk, 1/*all_topics*/, NULL, &md,
                                tmout_multip(5000));
        TEST_ASSERT(!err, "metadata failed: %s", rd_kafka_err2str(err));

        for (ti = 0 ; ti < md->topic_cnt ; ti++) {
                const rd_kafka_metadata_topic_t *mdt = &md->topics[ti];

                for (i = 0 ; i < topic_cnt ; i++) {
                        if (strcmp(topics[i].topic, mdt->topic))
                                continue;

                        topics[i].err = mdt->err; /* indicate found */
                        if (mdt->err) {
                                TEST_SAY("metadata: "
                                         "Topic %s has error %s\n",
                                         mdt->topic,
                                         rd_kafka_err2str(mdt->err));
                                fails++;
                        }

                        if (mdt->partition_cnt != topics[i].partition_cnt) {
                                TEST_SAY("metadata: "
                                         "Topic %s, expected %d topics"
                                         ", not %d\n",
                                         mdt->topic,
                                         topics[i].partition_cnt,
                                         mdt->partition_cnt);
                                fails++;
                        }
                }

                for (i = 0 ; i < not_topic_cnt ; i++) {
                        if (strcmp(not_topics[i].topic, mdt->topic))
                                continue;

                        TEST_SAY("metadata: "
                                 "Topic %s found in metadata, "
                                 "very unexpected\n",
                                 mdt->topic);
                        fails++;
                }

        }

        for (i  = 0 ; i < topic_cnt ; i++) {
                if (topics[i].err == -12345) {
                        TEST_SAY("metadata: "
                                 "Topic %s not seen in metadata\n",
                                 topics[i].topic);
                        fails++;
                }
        }

        if (fails > 0)
                TEST_SAY("Metadata verification for %d topics failed "
                         "with %d errors (see above)\n",
                         topic_cnt, fails);

        rd_kafka_metadata_destroy(md);

        return fails;
}



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
        rd_kafka_metadata_topic_t exp_mdtopics[MY_NEW_TOPICS_CNT] = {0};
        int exp_mdtopic_cnt = 0;
        /* Not expected topics in metadata */
        rd_kafka_metadata_topic_t exp_not_mdtopics[MY_NEW_TOPICS_CNT] = {0};
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
        int64_t abs_timeout;
        int num_replicas = jitter(1, avail_broker_cnt);

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
                                int32_t replicas[MY_NEW_TOPICS_CNT];
                                int j;

                                for (j = 0 ; j < num_replicas ; j++)
                                        replicas[j] = avail_brokers[j];

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
                TEST_SAY("CreateTopics: got %s in %.3fs\n",
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
         * Verify that the expect topics are created and the non-expected
         * are not. Allow it some time to propagate.
         */
        if (validate_only) {
                /* No topics should have been created, give it some time
                 * before checking. */
                rd_sleep(2);
                abs_timeout = test_clock() + 5000 + 1000;
        } else {
                int exp_creation_time;

                if (op_timeout > 0)
                        exp_creation_time = op_timeout + 1000;
                else
                        exp_creation_time = tmout_multip(5000);

                abs_timeout = test_clock() +
                        ((exp_creation_time + 1000) * 1000);

                test_timeout_set(10 + (exp_creation_time/1000));

                TEST_SAY("Waiting for up to %dms for CreateTopics "
                         "to propagate\n", exp_creation_time);
        }


        do {
                int md_fails;

                md_fails = verify_topics_in_metadata(
                        rk,
                        exp_mdtopics, exp_mdtopic_cnt,
                        exp_not_mdtopics, exp_not_mdtopic_cnt);

                if (!md_fails) {
                        TEST_SAY("All expected topics seen in metadata\n");
                        abs_timeout = 0;
                        break;
                } else if (validate_only) {
                        TEST_FAIL("validate_only=true but requested topics "
                                  "seen in metadata (%d failures)", md_fails);
                }

                rd_sleep(1);
        } while (test_clock() < abs_timeout);

        if (abs_timeout)
                TEST_FAIL("Expected topics not seen in given time.");

        rd_kafka_event_destroy(rkev);

        for (i = 0 ; i < MY_NEW_TOPICS_CNT ; i++) {
                rd_kafka_NewTopic_destroy(new_topics[i]);
                rd_free(topics[i]);
        }

        if (options)
                rd_kafka_AdminOptions_destroy(options);

        if (!useq)
                rd_kafka_queue_destroy(q);
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

        rd_kafka_queue_destroy(mainq);

        rd_kafka_destroy(rk);

        free(avail_brokers);
}


int main_0081_admin (int argc, char **argv) {
        do_test_apis(RD_KAFKA_PRODUCER);
        do_test_apis(RD_KAFKA_CONSUMER);
        return 0;
}
