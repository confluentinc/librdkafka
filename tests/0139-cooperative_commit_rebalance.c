/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022, Magnus Edenhill
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
 * Issue #4059: With cooperative sticky and manual commit, consumers should not
 * be able to commit during rebalance
 */

static rd_kafka_t *c1, *c2;
static int count = 0;

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
        TEST_SAY("Rebalance for %s: %s: %d partition(s)\n", rd_kafka_name(rk),
                 rd_kafka_err2name(err), parts->cnt);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                test_consumer_incremental_assign("assign", rk, parts);
        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                rd_kafka_resp_err_t commit_err;
                TEST_CALL_ERR__(rd_kafka_position(rk, parts));

                /* Only attempt to commit offsets when the second consumer joins
                 * the group. Since the first consumer is only asked to revoke
                 * half of its partitions, it still owns partitions when it
                 * attempts to commit offset. As a result, a rebalance in
                 * progress error should be returned.
                 */
                if (count == 0) {
                        count++;
                        TEST_SAY("%s: Committing\n", rd_kafka_name(rk));
                        commit_err = rd_kafka_commit(rk, parts, 0 /*sync*/);
                        TEST_SAY("%s: Commit result: %d %s\n",
                                 rd_kafka_name(rk), commit_err,
                                 rd_kafka_err2name(commit_err));

                        TEST_ASSERT(commit_err ==
                                        RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
                                    "Expected closing consumer %s's commit to "
                                    "fail, but got %s",
                                    rd_kafka_name(rk),
                                    rd_kafka_err2name(commit_err));
                }

                test_consumer_incremental_unassign("unassign", rk, parts);

        } else {
                TEST_FAIL("Unhandled event: %s", rd_kafka_err2name(err));
        }
}


int main_0139_cooperative_commit_rebalance(int argc, char **argv) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        rd_kafka_t *p;
        const int partition_cnt        = 6;
        const int msgcnt_per_partition = 100;
        const int msgcnt               = partition_cnt * msgcnt_per_partition;
        uint64_t testid;
        int i;
        testid = test_id_generate();

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "partition.assignment.strategy",
                      "cooperative-sticky");
        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        p = test_create_producer();

        test_create_topic(p, topic, partition_cnt, 1);

        for (i = 0; i < partition_cnt; i++) {
                test_produce_msgs2(p, topic, testid, i,
                                   i * msgcnt_per_partition,
                                   msgcnt_per_partition, NULL, 0);
        }

        test_flush(p, -1);

        rd_kafka_destroy(p);

        /* Create two consumers to consume from the topic */
        c1 = test_create_consumer(topic, rebalance_cb, rd_kafka_conf_dup(conf),
                                  NULL);
        c2 = test_create_consumer(topic, rebalance_cb, rd_kafka_conf_dup(conf),
                                  NULL);

        /* Have the first consumer subscribe to the topic and consume messages
         */
        test_consumer_subscribe(c1, topic);

        /* Consume some messages so that we know we have an assignment
         * and something to commit. */
        test_consumer_poll("C1.PRECONSUME", c1, testid, -1, 0, msgcnt / 2,
                           NULL);

        /* Trigger a rebalance by having the second consumer joining the group
         */
        test_consumer_subscribe(c2, topic);

        /* Sleep to allow enough time for all partitions to move around */
        rd_sleep(5);

        /* Poll both consumers */
        test_consumer_poll("C1.PRE", c1, testid, -1, 0, 10, NULL);
        test_consumer_poll("C2.PRE", c2, testid, -1, 0, 10, NULL);

        TEST_SAY("Closing consumers\n");
        test_consumer_close(c1);
        test_consumer_close(c2);

        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        return 0;
}
