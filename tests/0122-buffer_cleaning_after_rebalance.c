/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2021, Magnus Edenhill
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
#include "rdkafka.h"  /* for Kafka driver */

typedef struct consumer_s {
        const char *what;
        rd_kafka_queue_t *rkq;
        int timeout_ms;
        int consume_msg_cnt;
        rd_kafka_t *rk;
        uint64_t testid;
        int produce_msg_cnt;
} consumer_t;

static int test_consumer_batch_queue (consumer_t *arguments) {
        int msg_cnt = 0;
        int i;
        test_timing_t t_cons;
        test_msgver_t mv;

        rd_kafka_queue_t *rkq = arguments->rkq;
        int timeout_ms = arguments->timeout_ms;
        const int consume_msg_cnt = arguments->consume_msg_cnt;
        rd_kafka_t *rk = arguments->rk;
        uint64_t testid = arguments->testid;
        const char *what = arguments->what;
        const int produce_msg_cnt = arguments->produce_msg_cnt;

        rd_kafka_message_t **rkmessage = malloc(consume_msg_cnt * sizeof(*rkmessage));

        test_msgver_init(&mv, testid);

        TIMING_START(&t_cons, "CONSUME");

        while ((msg_cnt = rd_kafka_consume_batch_queue(rkq,
                timeout_ms, rkmessage, consume_msg_cnt)) == 0)
                continue;
        for (i = 0; i < msg_cnt; i++) {
                if (test_msgver_add_msg(rk, &mv, rkmessage[i]) == 0)
                        TEST_FAIL("The message is not from testid "
                                  "%"PRId64" \n", testid);
        }
        test_msgver_verify(what, &mv, TEST_MSGVER_ORDER|TEST_MSGVER_DUP, 0, produce_msg_cnt/2);
        test_msgver_clear(&mv);

        for (i = 0; i < msg_cnt; i++)
                rd_kafka_message_destroy(rkmessage[i]);
        TIMING_STOP(&t_cons);
        return 0;
}


/**
 * Consume with batch + queue interface
 *
 */
static void do_test_consume_batch (const char *strategy) {
        const int partition_cnt = 4;
        rd_kafka_queue_t *rkq1, *rkq2;
        const char *topic;
        rd_kafka_t *c1;
        rd_kafka_t *c2;
        int p;
        const int timeout_ms = 30000;
        uint64_t testid;
        const int consume_msg_cnt = 500;
        const int produce_msg_cnt = 400;
        rd_kafka_conf_t *conf;
        struct consumer_s c1_args;
        struct consumer_s c2_args;

        SUB_TEST("partition.assignment.strategy = %s", strategy);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        testid = test_id_generate();

        /* Produce messages */
        topic = test_mk_topic_name("0122-buffer_cleaning", 1);

        for (p = 0 ; p < partition_cnt ; p++)
                test_produce_msgs_easy(topic,
                                       testid,
                                       p,
                                       produce_msg_cnt / partition_cnt);
        /* Create simple consumer */
        if (!strcmp(strategy, "cooperative-sticky"))
                test_conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");

        c1 = test_create_consumer(topic, NULL,
                                  rd_kafka_conf_dup(conf), NULL);
        c2 = test_create_consumer(topic, NULL, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_wait_assignment(c1, rd_false);

        test_consumer_subscribe(c2, topic);
        test_consumer_wait_assignment(c2, rd_false);

        /* Create generic consume queue */
        rkq1 = rd_kafka_queue_get_consumer(c1);
        rkq2 = rd_kafka_queue_get_consumer(c2);

        c1_args.what = "C1.PRE";
        c1_args.rkq = rkq1;
        c1_args.timeout_ms = timeout_ms;
        c1_args.consume_msg_cnt = consume_msg_cnt;
        c1_args.rk = c1;
        c1_args.testid = testid;
        c1_args.produce_msg_cnt = produce_msg_cnt;


        test_consumer_batch_queue(&c1_args);

        c2_args.what = "C2.PRE";
        c2_args.rkq = rkq2;
        c2_args.timeout_ms = timeout_ms;
        c2_args.consume_msg_cnt = consume_msg_cnt;
        c2_args.rk = c2;
        c2_args.testid = testid;
        c2_args.produce_msg_cnt = produce_msg_cnt;

        test_consumer_batch_queue(&c2_args);

        rd_kafka_queue_destroy(rkq1);
        rd_kafka_queue_destroy(rkq2);

        test_consumer_close(c1);
        test_consumer_close(c2);

        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        SUB_TEST_PASS();
}


int main_0122_buffer_cleaning_after_rebalance (int argc, char **argv) {
        do_test_consume_batch("range");
        do_test_consume_batch("cooperative-sticky");
        return 0;
}
