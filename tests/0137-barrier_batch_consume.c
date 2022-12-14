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
/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */
#include <unistd.h>

typedef struct consumer_s {
        const char *what;
        rd_kafka_queue_t *rkq;
        int timeout_ms;
        int consume_msg_cnt;
        int expected_msg_cnt;
        rd_kafka_t *rk;
        uint64_t testid;
        test_msgver_t *mv;
        struct test *test;
} consumer_t;

static int consumer_batch_queue(void *arg) {
        consumer_t *arguments = arg;
        int msg_cnt           = 0;
        int i;
        test_timing_t t_cons;

        rd_kafka_queue_t *rkq     = arguments->rkq;
        int timeout_ms            = arguments->timeout_ms;
        const int consume_msg_cnt = arguments->consume_msg_cnt;
        rd_kafka_t *rk            = arguments->rk;
        uint64_t testid           = arguments->testid;
        rd_kafka_message_t **rkmessage =
            malloc(consume_msg_cnt * sizeof(*rkmessage));

        if (arguments->test)
                test_curr = arguments->test;

        TEST_SAY(
            "%s calling consume_batch_queue(timeout=%d, msgs=%d) "
            "and expecting %d messages back\n",
            rd_kafka_name(rk), timeout_ms, consume_msg_cnt,
            arguments->expected_msg_cnt);

        TIMING_START(&t_cons, "CONSUME");
        msg_cnt = (int)rd_kafka_consume_batch_queue(rkq, timeout_ms, rkmessage,
                                                    consume_msg_cnt);
        TIMING_STOP(&t_cons);

        for (i = 0; i < msg_cnt; i++) {
            TEST_SAY("consumed message for partition %d with offset %lld\n", rkmessage[i]->partition, rkmessage[i]->offset);
        }

        TEST_SAY("%s consumed %d/%d/%d message(s)\n", rd_kafka_name(rk),
                 msg_cnt, arguments->consume_msg_cnt,
                 arguments->expected_msg_cnt);
        TEST_ASSERT(msg_cnt == arguments->expected_msg_cnt,
                    "consumed %d messages, expected %d", msg_cnt,
                    arguments->expected_msg_cnt);

        for (i = 0; i < msg_cnt; i++) {
                if (test_msgver_add_msg(rk, arguments->mv, rkmessage[i]) == 0)
                        TEST_FAIL(
                            "The message is not from testid "
                            "%" PRId64 " \n",
                            testid);
                rd_kafka_message_destroy(rkmessage[i]);
        }

        return 0;
}


static void do_test_consume_batch_with_seek() {
        const int partition_cnt = 2;
        rd_kafka_queue_t *rkq;
        const char *topic;
        rd_kafka_t *consumer;
        rd_kafka_topic_t *rkt;
        int p;
        const int timeout_ms = 10000;
        uint64_t testid;
        const int consume_msg_cnt = 10;
        const int produce_msg_cnt = 8;
        rd_kafka_conf_t *conf;
        consumer_t consumer_args = RD_ZERO_INIT;
        test_msgver_t mv;
        thrd_t thread_id;
        rd_kafka_resp_err_t err;
        const int32_t seek_partition = 0;
        const int64_t seek_offset = 1;
        const int expected_msg_cnt = produce_msg_cnt - seek_offset;

        SUB_TEST();

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        testid = test_id_generate();
        test_msgver_init(&mv, testid);

        /* Produce messages */
        topic = test_mk_topic_name("0137-barrier_batch_consume", 1);

        for (p = 0; p < partition_cnt; p++)
                test_produce_msgs_easy(topic, testid, p,
                                       produce_msg_cnt / partition_cnt);

        /* Create consumers */
        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);

        test_consumer_subscribe(consumer, topic);
        test_consumer_wait_assignment(consumer, rd_false);

        /* Create generic consume queue */
        rkq = rd_kafka_queue_get_consumer(consumer);

        consumer_args.what             = "CONSUMER";
        consumer_args.rkq              = rkq;
        consumer_args.timeout_ms       = timeout_ms;
        consumer_args.consume_msg_cnt  = consume_msg_cnt;
        consumer_args.expected_msg_cnt = expected_msg_cnt;
        consumer_args.rk               = consumer;
        consumer_args.testid           = testid;
        consumer_args.mv               = &mv;
        consumer_args.test             = test_curr;
        if (thrd_create(&thread_id, consumer_batch_queue, &consumer_args) !=
            thrd_success)
                TEST_FAIL("Failed to create thread for %s", "CONSUMER");

        rkt = test_create_producer_topic(consumer, topic);
        err = rd_kafka_seek(rkt, seek_partition, seek_offset, 3000);

        if(err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
            TEST_FAIL("Failed to seek partition %d for topic %s to offset %lld", seek_partition, topic, seek_offset);
        }

        thrd_join(thread_id, NULL);

        test_msgver_verify("CONSUME", &mv,
                           TEST_MSGVER_ORDER | TEST_MSGVER_DUP, 0,
                           expected_msg_cnt);
        test_msgver_clear(&mv);

        rd_kafka_topic_destroy(rkt);

        rd_kafka_queue_destroy(rkq);

        test_consumer_close(consumer);

        rd_kafka_destroy(consumer);

        SUB_TEST_PASS();
}


static void do_test_consume_batch_with_pause_and_resume() {
        const int partition_cnt = 2;
        rd_kafka_queue_t *rkq;
        const char *topic;
        rd_kafka_t *consumer;
        int p;
        const int timeout_ms = 10000;
        uint64_t testid;
        const int consume_msg_cnt = 10;
        const int produce_msg_cnt = 8;
        rd_kafka_conf_t *conf;
        consumer_t consumer_args = RD_ZERO_INIT;
        test_msgver_t mv;
        thrd_t thread_id;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *parition_list;
        int32_t pause_partition = 0;

        SUB_TEST();

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        testid = test_id_generate();
        test_msgver_init(&mv, testid);

        /* Produce messages */
        topic = test_mk_topic_name("0137-barrier_batch_consume", 1);

        for (p = 0; p < partition_cnt; p++)
                test_produce_msgs_easy(topic, testid, p,
                                       produce_msg_cnt / partition_cnt);

        /* Create consumers */
        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);

        test_consumer_subscribe(consumer, topic);
        test_consumer_wait_assignment(consumer, rd_false);

        /* Create generic consume queue */
        rkq = rd_kafka_queue_get_consumer(consumer);

        consumer_args.what             = "CONSUMER";
        consumer_args.rkq              = rkq;
        consumer_args.timeout_ms       = timeout_ms;
        consumer_args.consume_msg_cnt  = consume_msg_cnt;
        consumer_args.expected_msg_cnt = 4;
        consumer_args.rk               = consumer;
        consumer_args.testid           = testid;
        consumer_args.mv               = &mv;
        consumer_args.test             = test_curr;
        if (thrd_create(&thread_id, consumer_batch_queue, &consumer_args) !=
            thrd_success)
                TEST_FAIL("Failed to create thread for %s", "CONSUMER");

        parition_list = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parition_list, topic, pause_partition);

        sleep(1);
        err = rd_kafka_pause_partitions(consumer, parition_list);

        if(err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            TEST_FAIL("Failed to pause partition %d for topic %s", pause_partition, topic);
        }

        sleep(1);

        err = rd_kafka_resume_partitions(consumer, parition_list);

        if(err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            TEST_FAIL("Failed to resume partition %d for topic %s", pause_partition, topic);
        }

        thrd_join(thread_id, NULL);


        rd_kafka_message_t **abc =
            malloc(consume_msg_cnt * sizeof(*abc));

        int msg_cnt = (int)rd_kafka_consume_batch_queue(rkq, timeout_ms, abc,
                                                    consume_msg_cnt);

        TEST_ASSERT(msg_cnt == 4,
                    "consumed %d messages, expected %d", msg_cnt,
                    4);

        test_msgver_verify("CONSUME", &mv,
                           TEST_MSGVER_ORDER | TEST_MSGVER_DUP, 0,
                           produce_msg_cnt);
        test_msgver_clear(&mv);

        rd_kafka_queue_destroy(rkq);

        test_consumer_close(consumer);

        rd_kafka_destroy(consumer);

        SUB_TEST_PASS();
}


int main_0137_barrier_batch_consume(int argc, char **argv) {
        do_test_consume_batch_with_seek();
        // Run this test once consume batch is fully fixed.
        // do_test_consume_batch_with_pause_and_resume();
        return 0;
}
