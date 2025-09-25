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

 #define PARTITION_CNT 120
 #define CONSUMER_CNT 60
 #define TOPIC_CNT 1 // Currently working only for a single topic
 #define ITERATIONS 5
 #define CONSUMER_POOL_SIZE (CONSUMER_CNT * ITERATIONS)
 #define ITERATION_TIME_IN_US 15000000 /* 15 second */

static atomic_int run = 0;
static int64_t offsets[PARTITION_CNT]; // Currently working only for a single topic
static int assigned_partitions[PARTITION_CNT];
const int64_t assign_check_interval_us = ITERATION_TIME_IN_US / 30;
static atomic_int calculate_assignments = 1;

typedef struct consumer_s {
    int consumer_id;
    char *group_id;
    const char **subscriptions;
    atomic_int run;
    rd_kafka_t *consumer;
    rd_kafka_topic_partition_list_t *prev_assignment;
    rwlock_t prev_assignment_lock;
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

    TEST_SAY_MAGENTA("Producer %d started for topic %s\n",
           producer_args->producer_id, producer_args->topic);

    while(run) {
            snprintf(buf, sizeof(buf), "Producer %d message %d", producer_args->producer_id, i);
            snprintf(payload, sizeof(payload), "Payload %s", buf);
            rd_kafka_produce(
                rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                payload, strlen(payload),
                NULL, 0, NULL);
            rd_usleep(50000, NULL); /* 50ms */
            i++;
            i %= 2147483643;
    }
    
    // printf("Producer %d finished producing messages for topic %s\n", producer_args->producer_id, producer_args->topic);
    rd_kafka_flush(rk, 10000); // Wait for all messages to be sent before exiting
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
    rd_free(producer_args->topic); // Free the topic string allocated in main

    return 0;
}

static int consumer_thread(void *arg) {
    rd_kafka_conf_t *conf;
    rd_kafka_t *consumer;
    consumer_t *consumer_args = arg;
    rd_kafka_topic_partition_list_t *current_assignment = NULL;
    rd_kafka_message_t *rkmessage;

    test_conf_init(&conf, NULL, 60);
    test_conf_set(conf, "auto.offset.reset", "earliest");
    test_conf_set(conf, "auto.commit.interval.ms", "10000");


    TEST_SAY_GREEN("Consumer %d started with group id %s\n",
           consumer_args->consumer_id, consumer_args->group_id);

    /* Create consumers */
    consumer = test_create_consumer(consumer_args->group_id, NULL, conf, NULL);
    consumer_args->consumer = consumer;
    test_consumer_subscribe_multi(consumer, consumer_args->subscriptions, TOPIC_CNT);

    TEST_SAY_GREEN("Consumer %d subscribed to topics\n",
           consumer_args->consumer_id);
    while(consumer_args->run && run) {
        if (calculate_assignments) {
                if (rd_kafka_assignment(consumer, &current_assignment) == RD_KAFKA_RESP_ERR_NO_ERROR && current_assignment) {
                        int different = 0;

                        rwlock_rdlock(&consumer_args->prev_assignment_lock);
                        if (consumer_args->prev_assignment->cnt != current_assignment->cnt) {
                                different = 1;
                        } else {
                                for (int i = 0; i < current_assignment->cnt; i++) {
                                        if (!rd_kafka_topic_partition_list_find(consumer_args->prev_assignment,
                                                        current_assignment->elems[i].topic,
                                                        current_assignment->elems[i].partition)) {
                                                different = 1;
                                                break;
                                        }
                                }
                        }
                        rwlock_rdunlock(&consumer_args->prev_assignment_lock);

                        if (different) {
                                rwlock_wrlock(&consumer_args->prev_assignment_lock);
                                TEST_SAY_YELLOW("Consumer %d assignment changed: prev_cnt=%d, curr_cnt=%d\n",
                                        consumer_args->consumer_id, consumer_args->prev_assignment->cnt, current_assignment->cnt);
                                rd_kafka_topic_partition_list_destroy(consumer_args->prev_assignment);
                                consumer_args->prev_assignment = rd_kafka_topic_partition_list_copy(current_assignment);
                                rwlock_wrunlock(&consumer_args->prev_assignment_lock);
                        }

                        rd_kafka_topic_partition_list_destroy(current_assignment);
                }
        }

        rkmessage = rd_kafka_consumer_poll(consumer, 1000);
        if (rkmessage) {
                // TEST_SAY_GREEN("Consumer %d received message with offset %ld for partition %d\n",
                //        consumer_args->consumer_id, rkmessage->offset, rkmessage->partition);
                TEST_ASSERT(offsets[rkmessage->partition] + 1 == rkmessage->offset,
                        "Consumer %d received message with offset %ld for partition %d, expected %ld",
                        consumer_args->consumer_id, rkmessage->offset, rkmessage->partition, offsets[rkmessage->partition]+1);
                // rd_kafka_commit_message(consumer, rkmessage, 0);
                offsets[rkmessage->partition] = rkmessage->offset;
                rd_kafka_message_destroy(rkmessage);
        }
    }

    TEST_SAY_GREEN("Consumer %d finished consuming messages for group %s\n",
           consumer_args->consumer_id, consumer_args->group_id);

    test_consumer_close(consumer);

    TEST_SAY_GREEN("Consumer %d closed\n", consumer_args->consumer_id);

    rd_kafka_destroy(consumer);

    TEST_SAY_GREEN("Consumer %d destroyed\n", consumer_args->consumer_id);

    return 0;
}

static void wait_for_iteration_and_validate_assignments(consumer_t *consumer_pool, int first_running, int last_running) {
        int no_of_assigned_partitions = 0;
        rd_usleep(ITERATION_TIME_IN_US, NULL); /* Sleep for the iterations to complete */
        calculate_assignments = 0;
        TEST_SAY_MAGENTA("Validating consumers from %d to %d (including)\n", first_running, last_running);
        for (int s = first_running; s <= last_running; s++) {
                rwlock_rdlock(&consumer_pool[s].prev_assignment_lock);
                rd_kafka_topic_partition_list_t *current_assignment = consumer_pool[s].prev_assignment;
                if (current_assignment && current_assignment->cnt > 0) {
                        for (int p = 0; p < current_assignment->cnt; p++) {
                                int partition = current_assignment->elems[p].partition;
                                TEST_ASSERT(assigned_partitions[partition] == -1,
                                        "Partition %d assigned to multiple consumers: %d and %d",
                                        partition, assigned_partitions[partition], consumer_pool[s].consumer_id);
                                assigned_partitions[partition] = s;
                                no_of_assigned_partitions++;
                        }
                }
                rwlock_rdunlock(&consumer_pool[s].prev_assignment_lock);
        }
        TEST_ASSERT(no_of_assigned_partitions == PARTITION_CNT,
                "Expected %d assigned partitions, but got %d",
                PARTITION_CNT, no_of_assigned_partitions);
        for (int i = 0; i < PARTITION_CNT; i++) {
                assigned_partitions[i] = -1;
        }
        calculate_assignments = 1;
}

int do_test_chaos_testing_consumer_group() {
        char **topics;
        uint64_t testid;
        producer_t producer_args[TOPIC_CNT];
        consumer_t consumer_args_pool[CONSUMER_POOL_SIZE];
        thrd_t consumer_thread_ids_pool[CONSUMER_POOL_SIZE] = {0};
        test_msgver_t mv;
        thrd_t producer_thread_ids[TOPIC_CNT];
        const int timeout_ms = 10000;
        int i;
        long long int start_time;
        long long int end_time;
        long long int elapsed_time_ms;
        const char *topics_prefix = test_mk_topic_name("8003-chaos-testing-consumer-group", 1);
        int first_running = 0;
        int last_running = (CONSUMER_CNT / 2) - 1;
        int pool_next = 0;

        testid = test_id_generate();
        test_msgver_init(&mv, testid);

        for(i = 0; i < PARTITION_CNT; i++) {
                offsets[i] = -1; // Initialize offsets to -1
                assigned_partitions[i] = -1;
        }

        run = 1;

        // Topic creation
        topics = rd_malloc(TOPIC_CNT * sizeof(*topics));
        for (i = 0; i < TOPIC_CNT; i++) {
                topics[i] = rd_malloc(64);
                rd_snprintf(topics[i], 64, "%s-%d", topics_prefix, i);

                /*
                 * TODO: Improve the topic creation logic to use multiple topics creation
                 *       API instead of creating topics one by one.
                 */
                test_create_topic(NULL, topics[i], PARTITION_CNT, 1);
        }
        // Wait for topics to be created and propogated to all the brokers
        test_wait_topic_exists(NULL, topics[TOPIC_CNT - 1], timeout_ms);
        rd_sleep(5);

        // Producer thread creation
        for (i = 0; i < TOPIC_CNT; i++) {
                producer_args[i].producer_id = i;
                producer_args[i].topic       = strdup(topics[i]);
                if (thrd_create(&producer_thread_ids[i], producer_thread,
                                &producer_args[i]) != thrd_success) {
                        fprintf(stderr, "Failed to create producer thread\n");
                        return 1;
                }
        }

        start_time      = rd_uclock();

        srand((unsigned int)time(NULL));

        // Initialize the consumer_args_pool
        for (i = 0; i < CONSUMER_POOL_SIZE; i++) {
                consumer_args_pool[i].consumer_id = i;
                consumer_args_pool[i].group_id = topics[0];
                consumer_args_pool[i].subscriptions = (const char **) topics;
                consumer_args_pool[i].prev_assignment = rd_kafka_topic_partition_list_new(0);
                consumer_args_pool[i].run = 1;
                consumer_args_pool[i].consumer = NULL;
                rwlock_init(&consumer_args_pool[i].prev_assignment_lock);
        }

        // Start initial consumers
        for (i = 0; i < CONSUMER_CNT / 2; i++) {
                int idx = pool_next++;
                TEST_SAY_YELLOW("Consumer %d started\n", consumer_args_pool[idx].consumer_id);
                if (thrd_create(&consumer_thread_ids_pool[idx], consumer_thread, &consumer_args_pool[idx]) != thrd_success) {
                        fprintf(stderr, "Failed to create consumer thread\n");
                        return 1;
                }
        }

        for (int iter = 0; iter < ITERATIONS; iter++) {
                wait_for_iteration_and_validate_assignments(consumer_args_pool, first_running, last_running);

                TEST_SAY_RED("Iteration %d: Rebalancing consumers...\n", iter);
                // Calculate current running consumers
                int running_count = last_running - first_running + 1;
                if (running_count >= 1) {
                        // Randomly decide how many consumers to stop (at least 1, at most running_count)
                        int to_stop = (running_count > 1) ? (rand() % running_count) + 1 : 1;
                        TEST_SAY_MAGENTA("Stopping %d consumers out of %d running consumers\n", to_stop, running_count);
                        for (int s = 0; s < to_stop && first_running <= last_running; s++, first_running++) {
                                int idx = first_running;
                                TEST_SAY_YELLOW("Stopping consumer %d\n", consumer_args_pool[idx].consumer_id);
                                consumer_args_pool[idx].run = 0;
                        }
                }

                // Update running_count after stopping
                running_count = last_running - first_running + 1;
                if (running_count < 0)
                        running_count = 0;

                // Calculate how many can be started
                int can_start = CONSUMER_CNT - running_count;
                if (can_start < 1)
                        continue; // No more consumers can be started

                // Randomly decide how many consumers to start (at least 1, at most can_start)
                int to_start = (can_start > 1) ? (rand() % can_start) + 1 : 1;
                TEST_SAY_MAGENTA("Starting %d consumers out of %d available slots\n", to_start, can_start);
                for (int s = 0; s < to_start; s++) {
                        if (pool_next >= CONSUMER_POOL_SIZE)
                                break; // No more available in pool
                        int idx = pool_next++;
                        TEST_SAY_YELLOW("Starting consumer %d\n", consumer_args_pool[idx].consumer_id);
                        consumer_args_pool[idx].run = 1;
                        consumer_args_pool[idx].consumer = NULL;

                        if (thrd_create(&consumer_thread_ids_pool[idx], consumer_thread, &consumer_args_pool[idx]) != thrd_success) {
                                fprintf(stderr, "Failed to create consumer thread\n");
                                // Don't increment last_running, just skip
                        } else {
                                last_running = idx;
                        }
                }
        }

        TEST_SAY_MAGENTA("All iterations completed, waiting for consumers to finish...\n");

        wait_for_iteration_and_validate_assignments(consumer_args_pool, first_running, last_running);

        TEST_SAY_MAGENTA("Stopping all consumers...\n");

        run = 0; // Stop all producers and consumers
        end_time        = rd_uclock();
        elapsed_time_ms = (end_time - start_time) / 1000;
        TEST_SAY_YELLOW("All rebalances took %llds and %lldms\n", elapsed_time_ms / 1000,
                 (elapsed_time_ms % 1000));

        // Clean ups

        TEST_SAY_YELLOW("Destroying all the previous assignments...\n");
        // destroy all the previous assignments
        for (i = 0; i < CONSUMER_POOL_SIZE; i++) {
            rwlock_wrlock(&consumer_args_pool[i].prev_assignment_lock);
            rd_kafka_topic_partition_list_destroy(consumer_args_pool[i].prev_assignment);
            rwlock_wrunlock(&consumer_args_pool[i].prev_assignment_lock);
            rwlock_destroy(&consumer_args_pool[i].prev_assignment_lock);
        }

        TEST_SAY_YELLOW("Waiting for all producer threads to finish...\n");
        for (i = 0; i < TOPIC_CNT; i++)
                thrd_join(producer_thread_ids[i], NULL);

        TEST_SAY_YELLOW("Waiting for all consumer threads to finish...\n");
        for(i = 0; i < CONSUMER_POOL_SIZE; i++){
                if(consumer_thread_ids_pool[i]) {
                        thrd_join(consumer_thread_ids_pool[i], NULL);
                        TEST_SAY_YELLOW("Consumer %d finished\n", consumer_args_pool[i].consumer_id);
                }

        }

        for(i = 0; i < TOPIC_CNT; i++) {
                rd_free(topics[i]);
        }
        rd_free(topics);

        test_delete_all_test_topics(timeout_ms);

        return elapsed_time_ms;
}

int main_8003_chaos_testing_consumer_group(int argc, char **argv) {
        test_timeout_set(450);
        // if(test_consumer_group_protocol_classic()) {
        //         TEST_SKIP("Skipping test for classic consumer group protocol\n");
        //         return 0;
        // }
        do_test_chaos_testing_consumer_group();
        return 0;
}
