/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020-2022, Magnus Edenhill
 *               2025, Confluent Inc.
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

/**
 * Issue #2933: Offset commit on revoke would cause hang.
 */

static rd_kafka_t *c1, *c2;

static int rebalances = 0;

/* Set right before c2 is closed, so that the rebalance callback can tell the
 * close-triggered revocation apart from an ordinary one. */
static rd_bool_t c2_closing = rd_false;

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {

        TEST_SAY("Rebalance for %s: %s: %d partition(s)\n", rd_kafka_name(rk),
                 rd_kafka_err2name(err), parts->cnt);
        rebalances++;
        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                test_consumer_assign_by_rebalance_protocol("rebalance", rk,
                                                           parts);

        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                rd_kafka_resp_err_t commit_err;

                TEST_CALL_ERR__(rd_kafka_position(rk, parts));

                test_consumer_unassign_by_rebalance_protocol("rebalance", rk,
                                                             parts);

                if (rk == c1)
                        return;

                /* Only c2's close-triggered revocation must reach the commit
                 * below. In the `consumer` protocol all assignors are
                 * cooperative, so the coordinator may revoke a subset of c2's
                 * partitions while c2 is still a live member, for instance to
                 * hand them over to c1 when the initial assignment was
                 * uneven. A commit from a live member is legitimate and
                 * succeeds, which would fail the assertion below for the
                 * wrong reason. */
                if (!c2_closing)
                        return;

                /* Give the closing consumer time to complete the unassignment
                 * and leave the group, so that the coming commit fails. */
                rd_sleep(5);

                /* By now the cgrp has left the group and terminated, so this
                 * commit is denied locally with _DESTROY and never reaches
                 * the broker. What this test guards is that a failed commit
                 * on revoke doesn't leave the remaining consumer's cgrp
                 * unable to transition its next assignment to fetching
                 * (issue #2933). */
                TEST_SAY("%s: Committing\n", rd_kafka_name(rk));
                commit_err = rd_kafka_commit(rk, parts, 0 /*sync*/);
                TEST_SAY("%s: Commit result: %s\n", rd_kafka_name(rk),
                         rd_kafka_err2name(commit_err));

                TEST_ASSERT(commit_err,
                            "Expected closing consumer %s's commit to "
                            "fail, but got %s",
                            rd_kafka_name(rk), rd_kafka_err2name(commit_err));

        } else {
                TEST_FAIL("Unhandled event: %s", rd_kafka_err2name(err));
        }
}


int main_0118_commit_rebalance(int argc, char **argv) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        const int msgcnt          = 1000;
        const int exp_msg_cnt_pre = 10;
        int exp_msg_cnt_post      = msgcnt;
        int exp_msg_cnt_c1_pre    = exp_msg_cnt_pre;
        int exp_msg_cnt_c2_pre    = exp_msg_cnt_pre;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        test_produce_msgs_easy_v(topic, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, 10,
                                 NULL);

        c1 = test_create_consumer(topic, rebalance_cb, rd_kafka_conf_dup(conf),
                                  NULL);
        c2 = test_create_consumer(topic, rebalance_cb, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);

        while (exp_msg_cnt_c1_pre > 0 || exp_msg_cnt_c2_pre > 0) {
                if (exp_msg_cnt_c1_pre > 0 ||
                    exp_msg_cnt_c2_pre == exp_msg_cnt_pre) {
                        exp_msg_cnt_c1_pre -=
                            test_consumer_poll_once(c1, NULL, 100);
                        if (exp_msg_cnt_c2_pre == exp_msg_cnt_pre)
                                /* Slow down consumption until both have
                                 * partitions assigned. */
                                rd_usleep(100 * 1000, 0);
                }
                if (exp_msg_cnt_c2_pre > 0 ||
                    exp_msg_cnt_c1_pre == exp_msg_cnt_pre) {
                        exp_msg_cnt_c2_pre -=
                            test_consumer_poll_once(c2, NULL, 100);
                        if (exp_msg_cnt_c1_pre == exp_msg_cnt_pre)
                                /* Slow down consumption until both have
                                 * partitions assigned. */
                                rd_usleep(100 * 1000, 0);
                }
        }

        /* Trigger rebalance */
        c2_closing = rd_true;
        test_consumer_close(c2);
        rd_kafka_destroy(c2);

        /* Since all the assignors in the `consumer` protocol are COOPERATIVE
         * only the new partitions are assigned to the consumer. All the
         * previously assigned partitions will start consuming from the last
         * offset. */
        if (!test_consumer_group_protocol_classic())
                exp_msg_cnt_post =
                    msgcnt - exp_msg_cnt_pre + exp_msg_cnt_c1_pre;

        /* Since no offsets were successfully committed the remaining consumer
         * should be able to receive all messages. */
        test_consumer_poll("C1.POST", c1, 0, -1, -1, exp_msg_cnt_post, NULL);

        rd_kafka_destroy(c1);

        return 0;
}
