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
#include <pthread.h>
/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */

static rd_kafka_t *c1, *c2;
//static uint64_t testid;
//static char *topic;


typedef struct _consumer_s {
        const char *what;
        rd_kafka_queue_t *rkq;
        int timeout_ms;
        int consumemsgcnt;
        rd_kafka_t *rk;
        char *topic;
        uint64_t testid;
} _consumer_t;

//static int test_consumer_batch_queue(_consumer_t *args) {
static int test_consumer_batch_queue(const _consumer_t *arguments) {
//static int test_consumer_batch_queue (const char *what, rd_kafka_queue_t *rkq, int timeout_ms, rd_kafka_message_t **rkmessage, int consumemsgcnt, rd_kafka_t *rk) {
        //_consumer_t *args = (struct _consumer_t *)arguments;
        _consumer_t *args = (struct _consumer_t *)arguments;
        int cnt = 0;
        int eof_cnt = 0;
        int i = 0;
        test_timing_t t_cons;
        test_msgver_t mv;
        rd_kafka_queue_t *rkq = args->rkq;
        //rd_kafka_message_t **rkmessage = args->rkmessage;

        int timeout_ms = args->timeout_ms;
        int consumemsgcnt = args->consumemsgcnt;
        rd_kafka_message_t **rkmessage = malloc(consumemsgcnt * sizeof(rd_kafka_message_t *));

        int correct;
        rd_kafka_t *rk = args->rk;
        char *topic = args->topic;
        uint64_t testid = args->testid;
        test_msgver_init(&mv, testid);
        //mv.p_cnt = 4;
        TEST_SAY("Jing Liu Consumer1 testid %"PRId64"\n", testid);
        TEST_SAY("Jing Liu Consumer1 tmv estid %"PRId64"\n", mv.testid);

        const char *what = args->what;


        TEST_SAY(" %s Consumer\n", what);

        //TEST_SAY("%s: consume %d messages\n", what, exp_cnt);

        TIMING_START(&t_cons, "CONSUME");

        while (eof_cnt == 0) {
                //TEST_SAY("Jing Liu Consumer2 %s\n", what);
                //rd_kafka_message_t *rkmessage;

                eof_cnt = rd_kafka_consume_batch_queue(rkq, timeout_ms, rkmessage, consumemsgcnt);
                if (eof_cnt == 0) {
                        continue;
                }

                for(i = 0; i < eof_cnt; i++) {
                   correct = test_msgver_add_msg(rk, &mv, rkmessage[i]);
                   if (correct == 0) {
                	   TEST_FAIL("The message is not from testid %"PRId64" \n", testid);
                   }

                   //TEST_SAY("Jing Liu Consumer verify %s result %d\n", what, mv.p_cnt);
                }


                test_msgver_verify(what, &mv, TEST_MSGVER_ORDER|TEST_MSGVER_DUP, 0, eof_cnt);
                test_msgver_clear(&mv);

                TEST_SAY("Jing Liu Consumer %d %s \n", eof_cnt, what);

                //rd_kafka_message_destroy(rkmessage);
        //}
}
        free(rkmessage);
        TEST_SAY("Jing Liu Consumer2 %d %s\n", eof_cnt, what);
        TIMING_STOP(&t_cons);

        //TEST_SAY("%s: consumed %d/%d messages (%d/%d EOFs)\n",
        //         what, cnt, exp_cnt, eof_cnt, exp_eof_cnt);
       /*
        if (exp_cnt == 0)
                TEST_ASSERT(cnt == 0 && eof_cnt == exp_eof_cnt,
                            "%s: expected no messages and %d EOFs: "
                            "got %d messages and %d EOFs",
                            what, exp_eof_cnt, cnt, eof_cnt);*/
        return cnt;
}

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *parts,
                          void *opaque) {

        TEST_SAY("Rebalance for %s: %s: %d partition(s)\n",
                 rd_kafka_name(rk), rd_kafka_err2name(err), parts->cnt);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        TEST_SAY("Rebalance for 1");
                TEST_CALL_ERR__(rd_kafka_assign(rk, parts));

        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        TEST_SAY("Rebalance for 2");
                rd_kafka_resp_err_t commit_err;

                TEST_CALL_ERR__(rd_kafka_position(rk, parts));

                TEST_CALL_ERR__(rd_kafka_assign(rk, NULL));

                if (rk == c1)
                        return;

                /* Give the closing consumer some time to handle the
                 * unassignment and leave so that the coming commit fails. */
                rd_sleep(5);

                /* Committing after unassign will trigger an
                 * Illegal generation error from the broker, which would
                 * previously cause the cgrp to not properly transition
                 * the next assigned state to fetching.
                 * The closing consumer's commit is denied by the consumer
                 * since it will have started to shut down after the assign
                 * call. */
                TEST_SAY("%s: Committing\n", rd_kafka_name(rk));
                commit_err = rd_kafka_commit(rk, parts, 0/*sync*/);
                TEST_SAY("%s: Commit result: %s\n",
                         rd_kafka_name(rk), rd_kafka_err2name(commit_err));

                TEST_ASSERT(commit_err,
                            "Expected closing consumer %s's commit to "
                            "fail, but got %s",
                            rd_kafka_name(rk),
                            rd_kafka_err2name(commit_err));

        } else {
                TEST_SAY("Rebalance for 3");
                TEST_FAIL("Unhandled event: %s", rd_kafka_err2name(err));
        }

}

/**
 * Consume with batch + queue interface
 *
 */
static int do_test_consume_batch (void) {
        //const char *topic;
        const int partition_cnt = 4;
        const rd_kafka_queue_t *rkq1, *rkq2;
        const rd_kafka_topic_t *rkt;
	    const rd_kafka_resp_err_t err;
        const int producemsgcnt = 4000;
        const char *topic;
        test_msgver_t mv;
        int i, p;
        int batch_cnt = 0;
        int remains;
        int timeout_ms = 30;
        uint64_t testid;
        const int consumemsgcnt = 5000;
        rd_kafka_conf_t *conf;
        pthread_t thread_id;
        pthread_t thread_id2;
        _consumer_t *c1_args = (_consumer_t *) malloc(sizeof(_consumer_t));
        _consumer_t *c2_args = (_consumer_t *) malloc(sizeof(_consumer_t));

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        testid = test_id_generate();

        /* Produce messages */
        topic = test_mk_topic_name(__FUNCTION__, 1);

        for (p = 0 ; p < partition_cnt ; p++)
                test_produce_msgs_easy(topic,
                                       testid,
                                       p,
                                       producemsgcnt / partition_cnt);

        /* Create simple consumer */
        c1 = test_create_consumer(topic, rebalance_cb,
                                  rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(topic, NULL, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);
        rd_sleep(2);

        /* Create generic consume queue */
        rkq1 = rd_kafka_queue_get_consumer(c1);
        rkq2 = rd_kafka_queue_get_consumer(c2);

        c1_args->what = "C1.PRE";
        c1_args->rkq = rkq1;
        c1_args->timeout_ms = timeout_ms;
        c1_args->consumemsgcnt = consumemsgcnt;
        c1_args->rk = c1;
        c1_args->testid = testid;
        c1_args->topic = topic;

        pthread_create(&thread_id, NULL, test_consumer_batch_queue, c1_args);

        TEST_SAY("Jing Liu c2_args\n");
        c2_args->what = "C2.PRE";
        c2_args->rkq = rkq2;
        c2_args->timeout_ms = timeout_ms;
        c2_args->consumemsgcnt = consumemsgcnt;
        c2_args->rk = c2;
        c2_args->testid = testid;
        c2_args->topic = topic;
        TEST_SAY("Jing Liu c2_args\n");

        pthread_create(&thread_id2, NULL, test_consumer_batch_queue, c2_args);
        pthread_join(thread_id, NULL);
        pthread_join(thread_id2, NULL);


        rd_free(c1_args);
        rd_free(c2_args);
        rd_kafka_queue_destroy(rkq1);
        rd_kafka_queue_destroy(rkq2);

        test_consumer_close(c1);
        test_consumer_close(c2);
        //rd_kafka_destroy(c2);

        //rd_kafka_destroy(c1);
        //rd_kafka_destroy(c2);

        return 0;
}




int main_0122_buffer_cleaning_after_rebalance (int argc, char **argv) {
        int fails = 0;

        fails += do_test_consume_batch();

        if (fails > 0)
                TEST_FAIL("See %d previous error(s)\n", fails);

        return 0;
}
