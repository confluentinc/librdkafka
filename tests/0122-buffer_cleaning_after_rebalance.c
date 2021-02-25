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

static rd_kafka_t *c1;

typedef struct rd_kafka_consumer_s {
        char *what;
        rd_kafka_queue_t *rkq;
        int timeout_ms;
        int consumemsgcnt;
        rd_kafka_t *rk;
        uint64_t testid;
} rd_kafka_consumer_t;

static void test_consumer_batch_queue(const rd_kafka_consumer_t *arguments) {
	    rd_kafka_consumer_t *args = (struct rd_kafka_consumer_t *)arguments;
        int eof_cnt = 0;
        int i;
        int correct;
        test_timing_t t_cons;
        test_msgver_t mv;

        rd_kafka_queue_t *rkq = args->rkq;
        int timeout_ms = args->timeout_ms;
        int consumemsgcnt = args->consumemsgcnt;
        rd_kafka_t *rk = args->rk;
        uint64_t testid = args->testid;
        char *what = args->what;

        rd_kafka_message_t **rkmessage = malloc(consumemsgcnt * sizeof(rd_kafka_message_t *));
        test_msgver_init(&mv, testid);

        TIMING_START(&t_cons, "CONSUME");

        while (eof_cnt == 0) {
                eof_cnt = rd_kafka_consume_batch_queue(rkq, timeout_ms, rkmessage, consumemsgcnt);
                if (eof_cnt == 0)
                        continue;

                for (i = 0; i < eof_cnt; i++) {
                       correct = test_msgver_add_msg(rk, &mv, rkmessage[i]);
                       if (correct == 0)
                               TEST_FAIL("The message is not from testid %"PRId64" \n", testid);
                }

                test_msgver_verify(what, &mv, TEST_MSGVER_ORDER|TEST_MSGVER_DUP, 0, eof_cnt);
                test_msgver_clear(&mv);
        }
        for (i = 0; i < eof_cnt; i++)
                rd_kafka_message_destroy(rkmessage[i]);

        TIMING_STOP(&t_cons);
}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {
        rd_kafka_error_t *error = NULL;
        rd_kafka_resp_err_t ret_err = RD_KAFKA_RESP_ERR_NO_ERROR;

        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                TEST_SAY("Group rebalanced (%s): "
                         "%d new partition(s) assigned\n",
                         rd_kafka_rebalance_protocol(rk), partitions->cnt);

                if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
                        error = rd_kafka_incremental_assign(rk, partitions);
                else
                        ret_err = rd_kafka_assign(rk, partitions);

                break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                TEST_SAY("(%s): %d partition(s) revoked\n",
                         rd_kafka_rebalance_protocol(rk), partitions->cnt);

                if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
                        error = rd_kafka_incremental_unassign(rk, partitions);
                else
                        ret_err = rd_kafka_assign(rk, NULL);
                break;

        default:
                break;
        }

        if (error) {
                TEST_FAIL("%% incremental assign failure: %s\n",
                          rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        } else if (ret_err) {
                TEST_FAIL( "%% assign failure: %s\n",
                rd_kafka_err2str(ret_err));
        }
}

/**
 * Consume with batch + queue interface
 *
 */
static int do_test_consume_batch (char *strategy) {
        int partition_cnt = 4;
        rd_kafka_queue_t *rkq1, *rkq2;
        int producemsgcnt = 400;
        const char *topic;
        rd_kafka_t *c2;
        int p;
        int timeout_ms = 30000;
        uint64_t testid;
        int consumemsgcnt = 500;
        rd_kafka_conf_t *conf;
        pthread_t thread_id;
        pthread_t thread_id2;
        rd_kafka_consumer_t *c1_args = (rd_kafka_consumer_t *) malloc(sizeof(rd_kafka_consumer_t));
        rd_kafka_consumer_t *c2_args = (rd_kafka_consumer_t *) malloc(sizeof(rd_kafka_consumer_t));

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        testid = test_id_generate();

        /* Produce messages */
        topic = test_mk_topic_name(__FUNCTION__, 1);

        for (p = 0 ; p < partition_cnt ; p++)
                test_produce_msgs_easy(topic,
                                       testid,
                                       p,
                                       producemsgcnt / partition_cnt);
        /* Create simple consumer */
        if(strcmp(strategy, "cooperative-sticky") == 0)
                test_conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");

        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        c1 = test_create_consumer(topic, rebalance_cb,
                                  rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(topic, NULL, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);

        /* Create generic consume queue */
        rkq1 = rd_kafka_queue_get_consumer(c1);
        rkq2 = rd_kafka_queue_get_consumer(c2);

        c1_args->what = "C1.PRE";
        c1_args->rkq = rkq1;
        c1_args->timeout_ms = timeout_ms;
        c1_args->consumemsgcnt = consumemsgcnt;
        c1_args->rk = c1;
        c1_args->testid = testid;

        pthread_create(&thread_id, NULL, test_consumer_batch_queue, c1_args);

        c2_args->what = "C2.PRE";
        c2_args->rkq = rkq2;
        c2_args->timeout_ms = timeout_ms;
        c2_args->consumemsgcnt = consumemsgcnt;
        c2_args->rk = c2;
        c2_args->testid = testid;

        pthread_create(&thread_id2, NULL, test_consumer_batch_queue, c2_args);
        pthread_join(thread_id, NULL);
        pthread_join(thread_id2, NULL);

        rd_free(c1_args);
        rd_free(c2_args);

        rd_kafka_queue_destroy(rkq1);
        rd_kafka_queue_destroy(rkq2);

        test_consumer_close(c1);
        test_consumer_close(c2);

        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        return 0;
}


int main_0122_buffer_cleaning_after_rebalance (int argc, char **argv) {
        int fails = 0;

        fails += do_test_consume_batch("eager-rebalance");

        fails += do_test_consume_batch("cooperative-sticky");

        if (fails > 0)
                TEST_FAIL("See %d previous error(s)\n", fails);

        return 0;
}
