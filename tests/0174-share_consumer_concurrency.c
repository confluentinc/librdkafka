/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#include "rdkafka.h"

/**
 * @name Share Consumer Concurrency and Stress Tests
 *
 * Tests share consumer behavior under concurrent producer/consumer scenarios.
 * Each test uses config-based structure with producers and consumers running
 * in separate threads.
 */

#define MAX_TOPICS     16
#define MAX_PARTITIONS 16
#define MAX_CONSUMERS  10
#define MAX_PRODUCERS  10
#define BATCH_SIZE     500

/* Save test_curr for threads (test_curr is TLS) */
static struct test *this_test;

/***************************************************************************
 * Test Configuration and State
 ***************************************************************************/

/**
 * @brief Configuration for a concurrent share consumer test
 */
typedef struct {
        int consumer_cnt;              /**< Number of consumers to create */
        int producer_cnt;              /**< Number of producer threads */
        int topic_cnt;                 /**< Number of topics */
        int partitions[MAX_TOPICS];    /**< Partitions per topic */
        int msgs_per_partition;        /**< Messages to produce per partition */
        const char *group_name;        /**< Share group name */
        const char *test_name;         /**< Test description */
        int max_attempts;              /**< Max poll attempts (0 = default) */
        int consumer_delay_ms;         /**< Delay between consumer polls */
        int producer_delay_ms;         /**< Delay between produces */
        rd_bool_t explicit_ack;        /**< Use explicit acknowledgement */
        rd_bool_t staggered_start;     /**< Start producers/consumers at
                                            different times */
} concurrent_test_config_t;

/**
 * @brief Shared state for concurrent test threads
 */
typedef struct {
        mtx_t lock;                    /**< Mutex for state access */
        int total_produced;            /**< Total messages produced */
        int total_consumed;            /**< Total messages consumed */
        int expected_total;            /**< Expected total messages */
        rd_bool_t producers_done;      /**< All producers finished */
        rd_bool_t consumers_should_stop; /**< Signal consumers to stop */
        rd_bool_t test_failed;         /**< Test failure flag */
        char *topics[MAX_TOPICS];      /**< Topic names */
        int topic_cnt;                 /**< Number of topics */
        const char *group_name;        /**< Share group name */
        rd_bool_t explicit_ack;        /**< Use explicit acknowledgement */
} concurrent_test_state_t;

/**
 * @brief Arguments for producer thread
 */
typedef struct {
        concurrent_test_state_t *state;
        int producer_id;
        int msgs_to_produce;
        int delay_ms;
        int partitions[MAX_TOPICS];
} producer_thread_args_t;

/***************************************************************************
 * Thread Functions
 ***************************************************************************/

/**
 * @brief Producer thread function
 *
 * Produces messages to all topics/partitions in round-robin fashion.
 */
static int producer_thread_func(void *arg) {
        producer_thread_args_t *args = (producer_thread_args_t *)arg;
        concurrent_test_state_t *state = args->state;
        rd_kafka_t *producer;
        int produced = 0;
        int t, p;

        /* Restore test context for this thread (test_curr is TLS) */
        test_curr = this_test;
        test_curr->exp_dr_status = (rd_kafka_msg_status_t)-1;
        test_curr->ignore_dr_err = rd_true;

        producer = test_create_producer();

        TEST_SAY("Producer %d: starting, will produce %d messages\n",
                 args->producer_id, args->msgs_to_produce);

        while (produced < args->msgs_to_produce) {
                for (t = 0; t < state->topic_cnt && produced < args->msgs_to_produce; t++) {
                        for (p = 0; p < args->partitions[t] && produced < args->msgs_to_produce; p++) {
                                rd_kafka_resp_err_t err;
                                char value[64];

                                snprintf(value, sizeof(value), "prod%d-msg%d",
                                         args->producer_id, produced);

                                err = rd_kafka_producev(
                                    producer,
                                    RD_KAFKA_V_TOPIC(state->topics[t]),
                                    RD_KAFKA_V_PARTITION(p),
                                    RD_KAFKA_V_VALUE(value, strlen(value)),
                                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                    RD_KAFKA_V_END);

                                if (err) {
                                        TEST_SAY("Producer %d: produce failed: %s\n",
                                                 args->producer_id,
                                                 rd_kafka_err2str(err));
                                        mtx_lock(&state->lock);
                                        state->test_failed = rd_true;
                                        mtx_unlock(&state->lock);
                                        goto done;
                                }

                                produced++;

                                if (args->delay_ms > 0 && produced % 100 == 0)
                                        rd_usleep(args->delay_ms * 1000, NULL);
                        }
                }

                rd_kafka_flush(producer, 1000);
        }

        rd_kafka_flush(producer, 10000);

        TEST_SAY("Producer %d: finished, produced %d messages\n",
                 args->producer_id, produced);

done:
        mtx_lock(&state->lock);
        state->total_produced += produced;
        mtx_unlock(&state->lock);

        rd_kafka_destroy(producer);
        free(args);

        return thrd_success;
}

/***************************************************************************
 * Test Runner
 ***************************************************************************/

/**
 * @brief Run a concurrent share consumer test
 */
static void run_concurrent_test(const concurrent_test_config_t *config) {
        concurrent_test_state_t state = {0};
        thrd_t producer_threads[MAX_PRODUCERS];
        rd_kafka_share_t *consumers[MAX_CONSUMERS];
        rd_kafka_topic_partition_list_t *subs;
        int t, c, p;
        int total_partitions = 0;
        int msgs_per_producer;
        int max_attempts;

        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("=== %s ===\n", config->test_name);
        TEST_SAY("============================================================\n");
        TEST_SAY("Consumers: %d, Producers: %d, Topics: %d\n",
                 config->consumer_cnt, config->producer_cnt, config->topic_cnt);

        /* Save test context for threads (test_curr is TLS) */
        this_test = test_curr;

        /* Initialize state */
        mtx_init(&state.lock, mtx_plain);
        state.topic_cnt = config->topic_cnt;
        state.group_name = config->group_name;
        state.explicit_ack = config->explicit_ack;

        /* Calculate total expected messages */
        for (t = 0; t < config->topic_cnt; t++) {
                total_partitions += config->partitions[t];
        }
        state.expected_total = total_partitions * config->msgs_per_partition;

        TEST_SAY("Total partitions: %d, Messages per partition: %d, "
                 "Expected total: %d\n",
                 total_partitions, config->msgs_per_partition,
                 state.expected_total);

        /* Create topics */
        for (t = 0; t < config->topic_cnt; t++) {
                state.topics[t] = rd_strdup(
                    test_mk_topic_name("0174-concurrent", 1));
                test_create_topic_wait_exists(NULL, state.topics[t],
                                              config->partitions[t], -1,
                                              60 * 1000);
                TEST_SAY("Created topic %s with %d partitions\n",
                         state.topics[t], config->partitions[t]);
        }

        /* Create consumers */
        for (c = 0; c < config->consumer_cnt; c++) {
                consumers[c] = test_create_share_consumer(config->group_name, NULL);

                /* Configure group on first consumer */
                if (c == 0) {
                        test_share_set_auto_offset_reset(config->group_name, "earliest");
                }
        }

        /* Subscribe all consumers to all topics */
        subs = rd_kafka_topic_partition_list_new(config->topic_cnt);
        for (t = 0; t < config->topic_cnt; t++) {
                rd_kafka_topic_partition_list_add(subs, state.topics[t],
                                                  RD_KAFKA_PARTITION_UA);
        }
        for (c = 0; c < config->consumer_cnt; c++) {
                rd_kafka_share_subscribe(consumers[c], subs);
        }
        rd_kafka_topic_partition_list_destroy(subs);

        /* Allow consumers to join group */
        TEST_SAY("Waiting for consumers to join group...\n");
        rd_sleep(2);

        /* Staggered start: wait before starting producers */
        if (config->staggered_start) {
                TEST_SAY("Staggered start: waiting 2 seconds before producers\n");
                rd_sleep(2);
        }

        /* Start producer threads */
        msgs_per_producer = state.expected_total / config->producer_cnt;
        for (p = 0; p < config->producer_cnt; p++) {
                producer_thread_args_t *args =
                    rd_calloc(1, sizeof(*args));
                args->state = &state;
                args->producer_id = p;
                args->msgs_to_produce = msgs_per_producer;
                /* Last producer handles remainder */
                if (p == config->producer_cnt - 1) {
                        args->msgs_to_produce +=
                            state.expected_total % config->producer_cnt;
                }
                args->delay_ms = config->producer_delay_ms;
                for (t = 0; t < config->topic_cnt; t++) {
                        args->partitions[t] = config->partitions[t];
                }

                if (thrd_create(&producer_threads[p], producer_thread_func,
                                args) != thrd_success) {
                        TEST_FAIL("Failed to create producer thread %d", p);
                }
        }

        /* Poll consumers from main thread while producers run */
        max_attempts = config->max_attempts > 0 ? config->max_attempts : 100;
        {
                rd_kafka_message_t *batch[BATCH_SIZE];
                int attempts    = max_attempts;
                int idle_rounds = 0;

                TEST_SAY("Starting consumption (producers running in background)\n");

                while (state.total_consumed < state.expected_total &&
                       attempts-- > 0 && idle_rounds < 30) {
                        int round_consumed = 0;

                        /* Round-robin poll all consumers */
                        for (c = 0; c < config->consumer_cnt; c++) {
                                size_t rcvd = 0;
                                size_t m;
                                rd_kafka_error_t *err;

                                err = rd_kafka_share_consume_batch(
                                    consumers[c], 1000, batch, &rcvd);
                                if (err) {
                                        rd_kafka_error_destroy(err);
                                        continue;
                                }

                                for (m = 0; m < rcvd; m++) {
                                        if (!batch[m]->err) {
                                                if (config->explicit_ack) {
                                                        rd_kafka_share_acknowledge_type(
                                                            consumers[c], batch[m],
                                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                                                }
                                                state.total_consumed++;
                                        }
                                        rd_kafka_message_destroy(batch[m]);
                                }
                                round_consumed += (int)rcvd;
                        }

                        if (round_consumed > 0) {
                                idle_rounds = 0;
                        } else {
                                idle_rounds++;
                        }

                        /* Check if producers are done */
                        mtx_lock(&state.lock);
                        if (state.producers_done && idle_rounds >= 20) {
                                mtx_unlock(&state.lock);
                                break;
                        }
                        mtx_unlock(&state.lock);

                        if (attempts % 10 == 0) {
                                TEST_SAY("Progress: consumed %d/%d\n",
                                         state.total_consumed, state.expected_total);
                        }
                }
        }

        /* Wait for producer threads to finish */
        for (p = 0; p < config->producer_cnt; p++) {
                thrd_join(producer_threads[p], NULL);
        }

        mtx_lock(&state.lock);
        TEST_SAY("All producers done. Total produced: %d\n",
                 state.total_produced);
        mtx_unlock(&state.lock);

        /* Continue consuming remaining messages after producers are done */
        if (state.total_consumed < state.expected_total) {
                rd_kafka_message_t *batch[BATCH_SIZE];
                int idle_rounds = 0;

                TEST_SAY("Consuming remaining messages: %d/%d\n",
                         state.total_consumed, state.expected_total);

                while (state.total_consumed < state.expected_total &&
                       idle_rounds < 50) {
                        int round_consumed = 0;

                        for (c = 0; c < config->consumer_cnt; c++) {
                                size_t rcvd = 0;
                                size_t m;
                                rd_kafka_error_t *err;

                                err = rd_kafka_share_consume_batch(
                                    consumers[c], 1000, batch, &rcvd);
                                if (err) {
                                        rd_kafka_error_destroy(err);
                                        continue;
                                }

                                for (m = 0; m < rcvd; m++) {
                                        if (!batch[m]->err) {
                                                if (config->explicit_ack) {
                                                        rd_kafka_share_acknowledge_type(
                                                            consumers[c], batch[m],
                                                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                                                }
                                                state.total_consumed++;
                                        }
                                        rd_kafka_message_destroy(batch[m]);
                                }
                                round_consumed += (int)rcvd;
                        }

                        if (round_consumed > 0) {
                                idle_rounds = 0;
                        } else {
                                idle_rounds++;
                        }
                }
        }

        /* Verify results */
        TEST_SAY("Final: produced=%d, consumed=%d, expected=%d\n",
                 state.total_produced, state.total_consumed,
                 state.expected_total);

        TEST_ASSERT(state.total_produced == state.expected_total,
                    "Expected to produce %d, actually produced %d",
                    state.expected_total, state.total_produced);

        TEST_ASSERT(state.total_consumed == state.expected_total,
                    "Expected to consume %d, actually consumed %d",
                    state.expected_total, state.total_consumed);

        TEST_ASSERT(!state.test_failed, "Test marked as failed");

        TEST_SAY("SUCCESS: %s\n", config->test_name);

        /* Cleanup */
        for (c = 0; c < config->consumer_cnt; c++) {
                test_share_consumer_close(consumers[c]);
                test_share_destroy(consumers[c]);
        }
        for (t = 0; t < config->topic_cnt; t++) {
                rd_free(state.topics[t]);
        }
        mtx_destroy(&state.lock);
}

/***************************************************************************
 * Test Cases - Basic Concurrency
 ***************************************************************************/

/**
 * @brief Single producer, single consumer, single topic
 */
static void test_1p_1c_1t_1part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 1,
            .producer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-1p1c1t1p",
            .test_name          = "1 producer, 1 consumer, 1 topic, 1 partition"
        };
        run_concurrent_test(&config);
}

/**
 * @brief Single producer, multiple consumers, single topic
 */
static void test_1p_4c_1t_4part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-1p4c1t4p",
            .test_name          = "1 producer, 4 consumers, 1 topic, 4 partitions"
        };
        run_concurrent_test(&config);
}

/**
 * @brief Multiple producers, single consumer
 */
static void test_4p_1c_1t_4part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 1,
            .producer_cnt       = 4,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 1250,
            .group_name         = "share-conc-4p1c1t4p",
            .test_name          = "4 producers, 1 consumer, 1 topic, 4 partitions"
        };
        run_concurrent_test(&config);
}

/**
 * @brief Multiple producers, multiple consumers, single topic
 */
static void test_4p_4c_1t_4part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 4,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-4p4c1t4p",
            .test_name          = "4 producers, 4 consumers, 1 topic, 4 partitions"
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Multi-Topic Concurrency
 ***************************************************************************/

/**
 * @brief Multiple topics with concurrent access
 */
static void test_2p_2c_3t_2part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 2,
            .producer_cnt       = 2,
            .topic_cnt          = 3,
            .partitions         = {2, 2, 2},
            .msgs_per_partition = 1666,
            .group_name         = "share-conc-2p2c3t2p",
            .test_name          = "2 producers, 2 consumers, 3 topics, 2 partitions each"
        };
        run_concurrent_test(&config);
}

/**
 * @brief Many topics with single partition each
 */
static void test_2p_2c_8t_1part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 2,
            .producer_cnt       = 2,
            .topic_cnt          = 8,
            .partitions         = {1, 1, 1, 1, 1, 1, 1, 1},
            .msgs_per_partition = 1250,
            .group_name         = "share-conc-2p2c8t1p",
            .test_name          = "2 producers, 2 consumers, 8 topics, 1 partition each",
            .max_attempts       = 150
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - High Contention
 ***************************************************************************/

/**
 * @brief Many consumers competing for single partition
 */
static void test_1p_8c_1t_1part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 8,
            .producer_cnt       = 1,
            .topic_cnt          = 1,
            .partitions         = {1},
            .msgs_per_partition = 40000,
            .group_name         = "share-conc-1p8c1t1p",
            .test_name          = "1 producer, 8 consumers, 1 partition (high contention)"
        };
        run_concurrent_test(&config);
}

/**
 * @brief More consumers than partitions
 */
static void test_2p_6c_1t_2part(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 6,
            .producer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {2},
            .msgs_per_partition = 15000,
            .group_name         = "share-conc-2p6c1t2p",
            .test_name          = "2 producers, 6 consumers, 2 partitions (3:1 consumer ratio)"
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Explicit Acknowledgement
 ***************************************************************************/

/**
 * @brief Concurrent with explicit acknowledgement
 */
static void test_explicit_ack_4p_4c(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 4,
            .topic_cnt          = 2,
            .partitions         = {2, 2},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-explicit-4p4c",
            .test_name          = "4 producers, 4 consumers, explicit ack",
            .explicit_ack       = rd_true
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Staggered Start
 ***************************************************************************/

/**
 * @brief Consumers start before producers
 */
static void test_staggered_start(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 2,
            .producer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {2},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-staggered",
            .test_name          = "Staggered start: consumers before producers",
            .staggered_start    = rd_true
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - High Volume Stress
 ***************************************************************************/

/**
 * @brief High volume concurrent test
 */
static void test_high_volume_20k(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 4,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-highvol-20k",
            .test_name          = "High volume: 4p x 4c x 20k messages",
            .max_attempts       = 150
        };
        run_concurrent_test(&config);
}

/**
 * @brief High volume with many partitions
 */
static void test_high_volume_many_partitions(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 4,
            .topic_cnt          = 1,
            .partitions         = {8},
            .msgs_per_partition = 2500,
            .group_name         = "share-conc-highvol-8p",
            .test_name          = "High volume: 8 partitions x 2.5k messages each",
            .max_attempts       = 150
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Asymmetric Configurations
 ***************************************************************************/

/**
 * @brief Many producers, few consumers
 */
static void test_8p_2c(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 2,
            .producer_cnt       = 8,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 2500,
            .group_name         = "share-conc-8p2c",
            .test_name          = "8 producers, 2 consumers (producer heavy)"
        };
        run_concurrent_test(&config);
}

/**
 * @brief Few producers, many consumers
 */
static void test_2p_8c(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 8,
            .producer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {4},
            .msgs_per_partition = 10000,
            .group_name         = "share-conc-2p8c",
            .test_name          = "2 producers, 8 consumers (consumer heavy)"
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Complex Multi-Topic Multi-Partition
 ***************************************************************************/

/**
 * @brief Complex scenario with varied partition counts
 */
static void test_complex_varied_partitions(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 4,
            .topic_cnt          = 4,
            .partitions         = {1, 2, 3, 4},
            .msgs_per_partition = 2000,
            .group_name         = "share-conc-complex-varied",
            .test_name          = "4 topics with 1,2,3,4 partitions respectively",
            .max_attempts       = 150
        };
        run_concurrent_test(&config);
}

/**
 * @brief Maximum concurrent scenario
 */
static void test_max_concurrent(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = MAX_CONSUMERS,
            .producer_cnt       = MAX_PRODUCERS,
            .topic_cnt          = 4,
            .partitions         = {4, 4, 4, 4},
            .msgs_per_partition = 3125,
            .group_name         = "share-conc-max",
            .test_name          = "Maximum: 10 producers, 10 consumers, 4 topics",
            .max_attempts       = 200
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Test Cases - Slow Consumer/Producer Scenarios
 ***************************************************************************/

/**
 * @brief Slow consumers with fast producers
 */
static void test_slow_consumers(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 2,
            .producer_cnt       = 4,
            .topic_cnt          = 1,
            .partitions         = {2},
            .msgs_per_partition = 5000,
            .group_name         = "share-conc-slow-consumer",
            .test_name          = "Fast producers, slow consumers",
            .consumer_delay_ms  = 50,
            .max_attempts       = 200
        };
        run_concurrent_test(&config);
}

/**
 * @brief Fast consumers with slow producers
 */
static void test_slow_producers(void) {
        concurrent_test_config_t config = {
            .consumer_cnt       = 4,
            .producer_cnt       = 2,
            .topic_cnt          = 1,
            .partitions         = {2},
            .msgs_per_partition = 10000,
            .group_name         = "share-conc-slow-producer",
            .test_name          = "Slow producers, fast consumers",
            .producer_delay_ms  = 50,
            .max_attempts       = 200
        };
        run_concurrent_test(&config);
}

/***************************************************************************
 * Main Entry Point
 ***************************************************************************/

int main_0174_share_consumer_concurrency(int argc, char **argv) {

        test_timeout_set(600);

        /* Basic concurrency tests */
        test_1p_1c_1t_1part();   /* Baseline: 1 producer, 1 consumer */
        test_1p_4c_1t_4part();   /* Fan out: 1 producer, 4 consumers */
        test_4p_1c_1t_4part();   /* Fan in: 4 producers, 1 consumer */
        test_4p_4c_1t_4part();   /* Symmetric: 4 producers, 4 consumers */

        /* Multi-topic tests */
        test_2p_2c_3t_2part();   /* Multiple topics */
        test_2p_2c_8t_1part();   /* Many topics */

        /* High contention tests */
        test_1p_8c_1t_1part();   /* Many consumers, 1 partition */
        test_2p_6c_1t_2part();   /* More consumers than partitions */

        /* Explicit acknowledgement */
        test_explicit_ack_4p_4c();

        /* Staggered start */
        test_staggered_start();

        /* High volume stress tests */
        test_high_volume_20k();
        test_high_volume_many_partitions();

        /* Asymmetric configurations */
        test_8p_2c();            /* Producer heavy */
        test_2p_8c();            /* Consumer heavy */

        /* Complex scenarios */
        test_complex_varied_partitions();
        test_max_concurrent();

        /* Slow consumer/producer scenarios */
        test_slow_consumers();
        test_slow_producers();

        return 0;
}
