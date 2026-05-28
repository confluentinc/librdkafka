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

#if defined(__linux__)
#define SET_THREAD_NAME(name)                                                  \
        do {                                                                   \
                char pthread_thread_name[32];                                  \
                snprintf(pthread_thread_name, sizeof(pthread_thread_name),     \
                         "%s", name);                                          \
                pthread_setname_np(pthread_self(), pthread_thread_name);       \
        } while (0)
#else
#define SET_THREAD_NAME(name)                                                  \
        do {                                                                   \
        } while (0)
#endif

static int number_of_test_runs = 1;
static int partition_cnt       = 30;
static int topic_cnt           = 2;
static int consumer_cnt        = 60;
static int batch_size          = 60;
static atomic_int run          = 0;

typedef struct consumer_s {
        int consumer_id;
        int expected_assignment_cnt;
        char *group_id;
        const char **subscriptions;
        atomic_llong end_time;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *prev_assignment;
} consumer_t;

typedef struct producer_s {
        int producer_id;
        char *topic;
} producer_t;

static long long int max(long long int a, long long int b) {
        return (a > b) ? a : b;
}

static int producer_thread(void *arg) {
        producer_t *producer_args = arg;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        char buf[50];
        char payload[64];
        int i = 0;

        rk  = test_create_handle(RD_KAFKA_PRODUCER, NULL);
        rkt = rd_kafka_topic_new(rk, producer_args->topic, NULL);

        TEST_SAY("Producer %d started for topic %s\n",
                 producer_args->producer_id, producer_args->topic);

        while (run) {
                snprintf(buf, sizeof(buf), "Producer %d message %d",
                         producer_args->producer_id, i);
                snprintf(payload, sizeof(payload), "Payload %s", buf);
                rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA,
                                 RD_KAFKA_MSG_F_COPY, payload, strlen(payload),
                                 NULL, 0, NULL);
                rd_usleep(5000, NULL); /* 5ms */
                i++;
                i %= 2147483643;
        }

        // TEST_SAY_WHITE("Producer %d finished producing messages for topic
        // %s\n", producer_args->producer_id, producer_args->topic);
        rd_kafka_flush(
            rk, 10000);  // Wait for all messages to be sent before exiting
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        rd_free(
            producer_args->topic);  // Free the topic string allocated in main

        return 0;
}

static rd_kafka_topic_partition_list_t *
list_diff(rd_kafka_topic_partition_list_t *a,
          rd_kafka_topic_partition_list_t *b) {
        rd_kafka_topic_partition_list_t *result =
            rd_kafka_topic_partition_list_new(0);
        int i;

        for (i = 0; i < a->cnt; i++) {
                if (!rd_kafka_topic_partition_list_find(
                        b, a->elems[i].topic, a->elems[i].partition)) {
                        rd_kafka_topic_partition_list_add(
                            result, a->elems[i].topic, a->elems[i].partition);
                }
        }

        return result;
}

static int consumer_thread(void *arg) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *consumer;
        rd_kafka_topic_partition_list_t *assignment = NULL;
        consumer_t *consumer_args                   = arg;
        rd_kafka_topic_partition_list_t *diff =
            rd_kafka_topic_partition_list_new(0);
        int inside_list_diff = 0;

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        if (test_consumer_group_protocol_classic()) {
                test_conf_set(conf, "heartbeat.interval.ms", "500");
                test_conf_set(conf, "partition.assignment.strategy",
                              "cooperative-sticky");
        }
        // test_conf_set(conf, "debug", "conf");
        /* Create consumers */
        consumer =
            test_create_consumer(consumer_args->group_id, NULL, conf, NULL);
        consumer_args->consumer = consumer;
        test_consumer_subscribe_multi(consumer, consumer_args->subscriptions,
                                      topic_cnt);

        while (consumer_args->prev_assignment->cnt <
               consumer_args->expected_assignment_cnt) {
                // TEST_SAY_WHITE("Consumer %d waiting for assignment: %d <
                // %d\n", consumer_args->consumer_id,
                // consumer_args->prev_assignment->cnt,
                // consumer_args->expected_assignment_cnt);
                rd_kafka_assignment(consumer, &assignment);
                if (assignment->cnt != consumer_args->prev_assignment->cnt) {
                        // TEST_SAY("Consumer %d assignment changed: %d ->
                        // %d\n",
                        //          consumer_args->consumer_id,
                        //          consumer_args->prev_assignment->cnt,
                        //          assignment->cnt);
                        rd_kafka_topic_partition_list_destroy(diff);
                        inside_list_diff++;
                        // TEST_SAY_GREEN("Consumer %d inside list_diff: %d\n",
                        //          consumer_args->consumer_id,
                        //          inside_list_diff);
                        diff = list_diff(assignment,
                                         consumer_args->prev_assignment);
                        rd_kafka_topic_partition_list_destroy(
                            consumer_args->prev_assignment);
                        consumer_args->prev_assignment =
                            rd_kafka_topic_partition_list_copy(assignment);
                }
                rd_kafka_topic_partition_list_destroy(assignment);
                rd_usleep(100000, NULL); /* 10ms */
        }

        while (!(consumer_args->end_time)) {
                // TEST_SAY_WHITE("Consumer %d waiting for end_time to be
                // set\n", consumer_args->consumer_id);
                rd_kafka_message_t *rkmessage;
                rkmessage = rd_kafka_consumer_poll(consumer, 1);

                if (rkmessage) {
                        int64_t batch_end_time = rd_uclock();
                        // TEST_SAY_WHITE("Consumer %d received message from
                        // topic %s partition %d at %ld\n",
                        // consumer_args->consumer_id,
                        // rd_kafka_topic_name(rkmessage->rkt),
                        // rkmessage->partition, batch_end_time);
                        if (rd_kafka_topic_partition_list_find(
                                diff, rd_kafka_topic_name(rkmessage->rkt),
                                rkmessage->partition)) {
                                consumer_args->end_time = batch_end_time;
                                // TEST_SAY_WHITE("Consumer %d setting end_time
                                // to %lld\n", consumer_args->consumer_id,
                                // consumer_args->end_time);
                        }
                        rd_kafka_message_destroy(rkmessage);
                }
        }

        rd_kafka_topic_partition_list_destroy(diff);

        while (run) {
                rd_kafka_message_t *rkmessage;
                rkmessage = rd_kafka_consumer_poll(consumer, 1000);
                if (rkmessage) {
                        rd_kafka_message_destroy(rkmessage);
                }
                rd_usleep(50000, NULL); /* Sleep for 50 milli seconds to avoid
                                           busy waiting */
        }

        test_consumer_close(consumer);

        // TEST_SAY_WHITE("Consumer closed\n");

        rd_kafka_destroy(consumer);

        // TEST_SAY_WHITE("Consumer destroyed\n");
        return 0;
}

int do_test_performance_multiple_consumer() {
        char **topics;
        uint64_t testid;
        producer_t producer_args[topic_cnt];
        consumer_t consumer_args[consumer_cnt];
        test_msgver_t mv;
        thrd_t producer_thread_ids[topic_cnt],
            consumer_thread_ids[consumer_cnt];
        const int timeout_ms = 10000;
        int i;
        int number_of_batches = consumer_cnt / batch_size;
        int current_batch;
        int batch_sleep_wait_time_us = 10000;
        long long int start_time;
        long long int end_time;
        long long int batch_start_time;
        long long int batch_end_time;
        long long int elapsed_time_ms;
        long long int individual_batch_elapsed_time_ms[number_of_batches];
        long long int total_batch_elapsed_time_ms[number_of_batches];
        const char *topics_prefix =
            test_mk_topic_name("8002-rebalance_performance", 1);

        test_timeout_set(450);

        testid = test_id_generate();
        test_msgver_init(&mv, testid);

        run = 1;

        // Topic creation
        topics = rd_malloc(topic_cnt * sizeof(*topics));
        for (i = 0; i < topic_cnt; i++) {
                topics[i] = rd_malloc(64);
                rd_snprintf(topics[i], 64, "%s-%d", topics_prefix, i);

                /*
                 * TODO: Improve the topic creation logic to use multiple topics
                 * creation API instead of creating topics one by one.
                 */
                test_create_topic(NULL, topics[i], partition_cnt, 1);
        }
        // Wait for topics to be created and propogated to all the brokers
        test_wait_topic_exists(NULL, topics[topic_cnt - 1], timeout_ms);
        rd_sleep(5);

        // Producer thread creation
        for (i = 0; i < topic_cnt; i++) {
                producer_args[i].producer_id = i;
                producer_args[i].topic       = strdup(topics[i]);
                if (thrd_create(&producer_thread_ids[i], producer_thread,
                                &producer_args[i]) != thrd_success) {
                        fprintf(stderr, "Failed to create producer thread\n");
                        return 1;
                }
                char thread_name[32];
                snprintf(thread_name, sizeof(thread_name), "producer-%d", i);
                SET_THREAD_NAME(thread_name);
        }

        start_time = rd_uclock();

        for (current_batch = 0; current_batch < number_of_batches;
             current_batch++) {
                TEST_SAY("Starting batch %d of %d\n", current_batch + 1,
                         number_of_batches);
                int batch_start_index = current_batch * batch_size;
                int expected_assignment_cnt =
                    (partition_cnt * topic_cnt) /
                    ((current_batch + 1) * batch_size);
                batch_start_time = rd_uclock();
                for (i = 0; i < batch_size; i++) {
                        int consumer_index = batch_start_index + i;
                        consumer_args[consumer_index].consumer_id =
                            consumer_index;
                        consumer_args[consumer_index].group_id = topics[0];
                        consumer_args[consumer_index].subscriptions =
                            (const char **)topics;
                        consumer_args[consumer_index].prev_assignment =
                            rd_kafka_topic_partition_list_new(0);
                        consumer_args[consumer_index].expected_assignment_cnt =
                            expected_assignment_cnt;
                        consumer_args[consumer_index].end_time = 0;
                        TEST_SAY("Consumer %d started\n",
                                 consumer_args[consumer_index].consumer_id);
                        if (thrd_create(&consumer_thread_ids[consumer_index],
                                        consumer_thread,
                                        &consumer_args[consumer_index]) !=
                            thrd_success) {
                                fprintf(stderr,
                                        "Failed to create consumer thread\n");
                                return 1;
                        }
                        char thread_name[32];
                        snprintf(thread_name, sizeof(thread_name),
                                 "consumer-%d", consumer_index);
                        SET_THREAD_NAME(thread_name);
                }
                batch_end_time          = 0;
                int didnt_find_end_time = 1;
                while (didnt_find_end_time) {
                        TEST_SAY_WHITE("Waiting for batch %d to complete...\n",
                                       current_batch + 1);
                        rd_sleep(1);
                        // Since there is no revocation, we can just rely on the
                        // end_time of the new consumers in the batch as there
                        // are going to be assignments for the new consumers and
                        // old consumers will only have revocations.
                        didnt_find_end_time = 0;
                        for (i = 0; i < batch_size; i++) {
                                int consumer_index = batch_start_index + i;
                                rd_kafka_topic_partition_list_t *assignment;
                                rd_kafka_assignment(
                                    consumer_args[consumer_index].consumer,
                                    &assignment);
                                // TEST_SAY_WHITE("Checking consumer %d (%s)
                                // end_time: %lld. Number of assignments are
                                // %d\n",
                                //        consumer_index,
                                //        rd_kafka_memberid(consumer_args[consumer_index].consumer),
                                //        consumer_args[consumer_index].end_time,
                                //        assignment->cnt);
                                if (!(consumer_args[consumer_index].end_time)) {
                                        didnt_find_end_time = 1;
                                        for (int j = 0; j < assignment->cnt;
                                             j++) {
                                                TEST_SAY_YELLOW(
                                                    "Consumer %d (%s) has "
                                                    "assignment for topic %s "
                                                    "partition %d\n",
                                                    consumer_index,
                                                    rd_kafka_memberid(
                                                        consumer_args
                                                            [consumer_index]
                                                                .consumer),
                                                    assignment->elems[j].topic,
                                                    assignment->elems[j]
                                                        .partition);
                                        }
                                }
                                rd_kafka_topic_partition_list_destroy(
                                    assignment);
                                batch_end_time =
                                    max(batch_end_time,
                                        consumer_args[consumer_index].end_time);
                        }
                }

                TEST_SAY("Batch %d started at %lld\n", current_batch + 1,
                         batch_start_time);
                TEST_SAY("Batch %d completed with end time %lld\n",
                         current_batch + 1, batch_end_time);
                individual_batch_elapsed_time_ms[current_batch] =
                    (batch_end_time - batch_start_time) / 1000;
                TEST_SAY_RED(
                    "Batch %d took %llds and %lldms\n", current_batch + 1,
                    individual_batch_elapsed_time_ms[current_batch] / 1000,
                    (individual_batch_elapsed_time_ms[current_batch] % 1000));

                total_batch_elapsed_time_ms[current_batch] =
                    (batch_end_time - start_time) / 1000;
                TEST_SAY_RED(
                    "Total time after batch %d: %llds and %lldms\n",
                    current_batch + 1,
                    total_batch_elapsed_time_ms[current_batch] / 1000,
                    (total_batch_elapsed_time_ms[current_batch] % 1000));

                // if(individual_batch_elapsed_time_ms[current_batch] > 3000) {
                //         TEST_SAY_RED("Batch %d took too long: %llds and
                //         %lldms\n",
                //                   current_batch + 1,
                //                   individual_batch_elapsed_time_ms[current_batch]
                //                   / 1000,
                //                   (individual_batch_elapsed_time_ms[current_batch]
                //                   % 1000));
                //         break;
                // }

                rd_usleep(
                    batch_sleep_wait_time_us,
                    NULL);  // Sleep for 10ms before starting the next batch
        }

        end_time        = rd_uclock();
        run             = 0;
        elapsed_time_ms = (end_time - start_time) / 1000;
        TEST_SAY_RED("All rebalances took %llds and %lldms\n",
                     elapsed_time_ms / 1000, (elapsed_time_ms % 1000));

        // Clean ups

        // destroy all the previous assignments
        for (i = 0; i < consumer_cnt; i++) {
                rd_kafka_topic_partition_list_destroy(
                    consumer_args[i].prev_assignment);
        }

        // TEST_SAY_WHITE("Waiting for all producer threads to finish...\n");
        for (i = 0; i < topic_cnt; i++)
                thrd_join(producer_thread_ids[i], NULL);

        // TEST_SAY_WHITE("Waiting for all consumer threads to finish...\n");

        for (i = 0; i < consumer_cnt; i++)
                thrd_join(consumer_thread_ids[i], NULL);

        for (i = 0; i < topic_cnt; i++) {
                rd_free(topics[i]);
        }
        rd_free(topics);

        // TEST_SAY_WHITE("All consumer threads finished\n");
        test_delete_all_test_topics(timeout_ms);

        // TEST_SAY_WHITE("All topics deleted\n");

        return elapsed_time_ms;
}

int main_8002_rebalance_performance(int argc, char **argv) {
        int avg_rebalance_time_ms = 0;
        int current_run           = 1;
        SET_THREAD_NAME("main-8002");
        for (current_run = 1; current_run <= number_of_test_runs;
             current_run++) {
                TEST_SAY_RED("Starting run %d of %d\n", current_run,
                             number_of_test_runs);
                avg_rebalance_time_ms +=
                    do_test_performance_multiple_consumer();
        }
        avg_rebalance_time_ms /= number_of_test_runs;
        TEST_SAY_RED("Average rebalance time: %d ms\n", avg_rebalance_time_ms);
        return 0;
}
