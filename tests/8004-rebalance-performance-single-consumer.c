/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022, Magnus Edenhill
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

 #include <stdatomic.h>

 #include "test.h"
 /* Typical include path would be <librdkafka/rdkafka.h>, but this program
  * is built from within the librdkafka source tree and thus differs. */
 #include "rdkafka.h" /* for Kafka driver */
 #include "rdtime.h"

static int number_of_test_runs = 20;
static int partition_cnt = 6;
static int topic_cnt = 1;
// static int consumer_cnt = 6;
static atomic_int run = 0;

typedef struct consumer_s {
    int consumer_id;
} consumer_t;

typedef struct producer_s {
    int producer_id;
    char* topic;
} producer_t;

static int producer_thread(void *arg) {
    producer_t *producer_args = arg;
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    char buf[50];
    char payload[64];
    int i = 0;

    rk = test_create_handle(RD_KAFKA_PRODUCER, NULL);
    rkt = rd_kafka_topic_new(rk, producer_args->topic, NULL);

    TEST_SAY("Producer %d started for topic %s\n",
           producer_args->producer_id, producer_args->topic);

    while(run) {
            snprintf(buf, sizeof(buf), "Producer %d message %d", producer_args->producer_id, i);
            snprintf(payload, sizeof(payload), "Payload %s", buf);
            rd_kafka_produce(
                rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                payload, strlen(payload),
                NULL, 0, NULL);
            rd_usleep(5000, NULL); /* 5ms */
            i++;
            i %= 2147483643;
    }
    
    rd_kafka_flush(rk, 10000); // Wait for all messages to be sent before exiting
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
    rd_free(producer_args->topic); // Free the topic string allocated in main

    return 0;
}

int do_test() {
        const char *topics[topic_cnt];
        rd_kafka_t *consumer;
        uint64_t testid;
        rd_kafka_conf_t *conf;
        producer_t producer_args = RD_ZERO_INIT;
        test_msgver_t mv;
        thrd_t thread_id[topic_cnt];
        const int timeout_ms = 10000;
        int i;
        long long int start_time, end_time, elapsed_time_ms;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "heartbeat.interval.ms", "5000");

        testid = test_id_generate();
        test_msgver_init(&mv, testid);

        run = 1;

        for (i = 0; i < topic_cnt; i++) {
                topics[i] = test_mk_topic_name("8004_rebalance-performance-single-consumer", 1);
                test_create_topic_wait_exists(NULL, topics[i], partition_cnt, 1,
                                              5000);

                producer_args.producer_id = i;
                producer_args.topic       = strdup(topics[i]);
                if (thrd_create(&thread_id[i], producer_thread,
                                &producer_args) != thrd_success) {
                        fprintf(stderr, "Failed to create producer thread\n");
                        return 1;
                }
        }

        start_time = rd_uclock();

        /* Create consumers */
        consumer = test_create_consumer(topics[0], NULL, conf, NULL);
        test_consumer_subscribe_multi(consumer, topics, topic_cnt);
        test_consumer_wait_assignment(consumer, rd_false, 0);
        while (test_consumer_poll_once(consumer, NULL, timeout_ms) != 1)
                ;
        end_time     = rd_uclock();
        run          = 0;
        elapsed_time_ms = (end_time - start_time) / 1000;
        TEST_SAY("Rebalance took %llds and %lld ms\n", elapsed_time_ms / 1000,
                 (elapsed_time_ms % 1000));

        for (i = 0; i < topic_cnt; i++) {
                thrd_join(thread_id[i], NULL);
        }

        test_consumer_close(consumer);
        rd_kafka_destroy(consumer);

        test_delete_all_test_topics(timeout_ms);

        return elapsed_time_ms;
}

int main_8004_rebalance_performance_single_consumer(int argc, char **argv) {
    int avg_rebalance_time_ms = 0;
    int current_run = 1;
    for (current_run = 1; current_run <= number_of_test_runs; current_run++) {
        TEST_SAY("Starting run %d of %d\n", current_run, number_of_test_runs);
        avg_rebalance_time_ms += do_test();
    }
    avg_rebalance_time_ms /= number_of_test_runs;
    TEST_SAY("Average rebalance time: %d ms\n", avg_rebalance_time_ms);
    return 0;
}

