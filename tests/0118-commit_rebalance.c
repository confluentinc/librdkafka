/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020-2022, Magnus Edenhill
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
#include "../src/rdstring.h"

/**
 * Issue #2933: Offset commit on revoke would cause hang.
 */

static rd_kafka_t *c1, *c2;

static rd_kafka_resp_err_t expected_commit_error = RD_KAFKA_RESP_ERR_NO_ERROR;

static void
rebalance_cb_commit_after_unassign(rd_kafka_t *rk,
                                   rd_kafka_resp_err_t err,
                                   rd_kafka_topic_partition_list_t *parts,
                                   void *opaque) {

        TEST_SAY("Rebalance for %s: %s: %d partition(s)\n", rd_kafka_name(rk),
                 rd_kafka_err2name(err), parts->cnt);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                TEST_CALL_ERR__(rd_kafka_assign(rk, parts));

        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                rd_kafka_resp_err_t commit_err;

                TEST_CALL_ERR__(rd_kafka_position(rk, parts));
                TEST_CALL_ERR__(rd_kafka_assign(rk, NULL));

                if (rk == c1)
                        return;

                /* Give the consumer some time to handle the
                 * unassignment and leave. */
                rd_sleep(3);

                TEST_SAY("%s: Committing\n", rd_kafka_name(rk));
                commit_err = rd_kafka_commit(rk, parts, 0 /*sync*/);
                TEST_SAY("%s: Commit result: %s\n", rd_kafka_name(rk),
                         rd_kafka_err2name(commit_err));

                if (expected_commit_error) {
                        TEST_ASSERT(commit_err == expected_commit_error,
                                    "Expected consumer %s's commit to "
                                    "fail with err %s, but got %s",
                                    rd_kafka_name(rk),
                                    rd_kafka_err2name(expected_commit_error),
                                    rd_kafka_err2name(commit_err));
                } else {
                        TEST_ASSERT(!commit_err,
                                    "Expected consumer %s's commit to "
                                    "succeed, but got %s",
                                    rd_kafka_name(rk),
                                    rd_kafka_err2name(commit_err));
                }
        } else {
                TEST_FAIL("Unhandled event: %s", rd_kafka_err2name(err));
        }
}

static void
rebalance_cb_commit_before_unassign(rd_kafka_t *rk,
                                    rd_kafka_resp_err_t err,
                                    rd_kafka_topic_partition_list_t *parts,
                                    void *opaque) {

        TEST_SAY("Rebalance for %s: %s: %d partition(s)\n", rd_kafka_name(rk),
                 rd_kafka_err2name(err), parts->cnt);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                TEST_CALL_ERR__(rd_kafka_assign(rk, parts));

        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                rd_kafka_resp_err_t commit_err;


                TEST_CALL_ERR__(rd_kafka_position(rk, parts));
                if (rk == c1) {
                        TEST_CALL_ERR__(rd_kafka_assign(rk, NULL));
                        return;
                }

                /* Revoke callback is slow to commit. */
                rd_sleep(3);

                TEST_SAY("%s: Committing\n", rd_kafka_name(rk));
                commit_err = rd_kafka_commit(rk, parts, 0 /*sync*/);
                TEST_SAY("%s: Commit result: %s\n", rd_kafka_name(rk),
                         rd_kafka_err2name(commit_err));

                if (expected_commit_error) {
                        TEST_ASSERT(commit_err == expected_commit_error,
                                    "Expected consumer %s's commit to "
                                    "fail with err %s, but got %s",
                                    rd_kafka_name(rk),
                                    rd_kafka_err2name(expected_commit_error),
                                    rd_kafka_err2name(commit_err));
                } else {
                        TEST_ASSERT(!commit_err,
                                    "Expected consumer %s's commit to "
                                    "succeed, but got %s",
                                    rd_kafka_name(rk),
                                    rd_kafka_err2name(commit_err));
                }

                TEST_CALL_ERR__(rd_kafka_assign(rk, NULL));

        } else {
                TEST_FAIL("Unhandled event: %s", rd_kafka_err2name(err));
        }
}

/**
 * @brief A consumer should be able to commit the assigned partitions
 *        when it's closing before revocation, and the commit should fail
 *        if it comes after revocation.
 *
 * @param commit_before_unassign if it should commit assigned partition before
 *                               or after revocation.
 */
static void test_rebalance_before_close(rd_bool_t commit_before_unassign) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        const int msgcnt    = 1000;
        int expected_msgcnt = 1000;
        /* FIXME: commit before close should succeed if it comes
         * before unassignment. */

        void (*current_rebalance_cb)(
            rd_kafka_t * rk, rd_kafka_resp_err_t err,
            rd_kafka_topic_partition_list_t * partitions, void *opaque) =
            rebalance_cb_commit_after_unassign;
        /* The closing consumer's commit is denied by the consumer
         * since it will have started to shut down after the assign
         * call. */
        expected_commit_error = RD_KAFKA_RESP_ERR__DESTROY;

        SUB_TEST_QUICK("commit_before_unassign: %s",
                       RD_STR_ToF(commit_before_unassign));

        if (commit_before_unassign) {
                current_rebalance_cb = rebalance_cb_commit_before_unassign;
                expected_msgcnt      = msgcnt - 10;
                /* The closing consumer's commit succeeds because
                 * it comes before the assign call. */
                expected_commit_error = RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.partition.eof", "true");
        rd_kafka_conf_set_rebalance_cb(conf, current_rebalance_cb);

        test_produce_msgs_easy_v(topic, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, 10,
                                 NULL);

        c1 = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(topic, NULL, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);


        test_consumer_poll("C1.PRE", c1, 0, 0, -1, 10, NULL);
        test_consumer_poll("C2.PRE", c2, 0, 0, -1, 10, NULL);

        /* Trigger rebalance */
        test_consumer_close(c2);
        rd_kafka_destroy(c2);

        /* If no offsets were successfully committed the remaining consumer
         * should be able to receive all messages,
         * otherwise all of them except 10. */
        test_consumer_poll("C1.POST", c1, 0, 1, -1, expected_msgcnt, NULL);

        rd_kafka_destroy(c1);

        SUB_TEST_PASS();
}

/**
 * @brief A consumer should be able to commit the assigned partitions
 *        before they are revoked, and the commit should fail
 *        if it comes after revocation.
 *        But currently it's not because broker is only checking the
 *        generation id.
 *
 * @param commit_before_unassign if it should commit assigned partition before
 *                               or after revocation.
 */
static void test_rebalance_while_processing(rd_bool_t commit_before_unassign) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        const int msgcnt    = 20;
        int expected_msgcnt = 20;
        void (*current_rebalance_cb)(
            rd_kafka_t * rk, rd_kafka_resp_err_t err,
            rd_kafka_topic_partition_list_t * partitions, void *opaque) =
            rebalance_cb_commit_after_unassign;

        /* Broker allows to commit arbitrary partitions
         * after rebalance because epoch has changed.
         * This seems incorrect and requires the rebalance callback to
         * avoid committing revoked partitions after the assign call. */
        expected_commit_error = RD_KAFKA_RESP_ERR_NO_ERROR;

        SUB_TEST_QUICK("commit_before_unassign: %s",
                       RD_STR_ToF(commit_before_unassign));

        if (commit_before_unassign) {
                current_rebalance_cb = rebalance_cb_commit_before_unassign;
                /* The processing consumer's commit succeeds because
                 * it comes before the assign call. */
                expected_commit_error = RD_KAFKA_RESP_ERR_NO_ERROR;
                expected_msgcnt       = 0;
        }

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "heartbeat.interval.ms", "100");
        test_conf_set(conf, "enable.partition.eof", "true");
        rd_kafka_conf_set_rebalance_cb(conf, current_rebalance_cb);



        c2 = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        /* Create topic with 2 partitions */
        test_create_topic(c2, topic, 2, -1);

        test_produce_msgs_easy_v(topic, 0, 0 /* partition */, 0, msgcnt / 2, 10,
                                 NULL);
        test_produce_msgs_easy_v(topic, 0, 1 /* partition */, 0, msgcnt / 2, 10,
                                 NULL);

        test_consumer_subscribe(c2, topic);
        test_consumer_poll("C2.PRE", c2, 0, 1, -1, msgcnt, NULL);

        c1 = test_create_consumer(topic, NULL, conf, NULL);

        /* Trigger rebalance */
        test_consumer_subscribe(c1, topic);

        /* Wait that C2 receives the join group request */
        rd_sleep(1);

        /* Releases partitions */
        test_consumer_poll("C2.REBALANCE", c2, 0, 1, -1, 0, NULL);


        /* If no offsets were successfully committed the remaining consumer
         * should be able to receive all the messages,
         * otherwise no message is received. */
        test_consumer_poll("C1.POST", c1, 0, 1, -1, expected_msgcnt, NULL);


        /* All offsets were committed previously, no need to commit
         * while closing. Error is _DESTROY if commit comes after the
         * assign call. */
        if (commit_before_unassign)
                expected_commit_error = RD_KAFKA_RESP_ERR__NO_OFFSET;
        else
                expected_commit_error = RD_KAFKA_RESP_ERR__DESTROY;

        test_consumer_close(c1);
        test_consumer_close(c2);

        rd_kafka_destroy(c2);
        rd_kafka_destroy(c1);

        SUB_TEST_PASS();
}


int main_0118_commit_rebalance(int argc, char **argv) {
        test_rebalance_before_close(rd_true /* commit before unassign */);
        test_rebalance_before_close(rd_false /* commit after unassign */);
        test_rebalance_while_processing(rd_true /* commit before unassign */);
        test_rebalance_while_processing(rd_false /* commit after unassign */);
        return 0;
}
