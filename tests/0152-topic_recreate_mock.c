/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
#include "../src/rdkafka_proto.h"

/**
 *  @name Verify that producer and consumer resumes operation after
 *       a topic has been deleted and recreated (with topic id change) using
 *       the mock broker.
 *
 * @note These tests should be revised after the implementation of KIP-516 is
 * added to the actual broker for Produce.
 */

/**
 * Test topic recreation for producer. There are two configurable flags:
 * 1. is_idempotent: decides whether producer is idempotent or not.
 * 2. leader_change: if set to true, changes leader of the initial topic to push
 *    the leader epoch beyond that of the recreated topic.
 */
static void do_test_topic_recreated_producer(rd_bool_t is_idempotent,
                                             rd_bool_t leader_change) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;

        SUB_TEST("Testing topic recreation with %s producer",
                 is_idempotent ? "idempotent" : "normal");

        /* Create mock cluster */
        mcluster = test_mock_cluster_new(3, &bootstrap_servers);

        /* Create topic and change leader to bump leader epoch */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);
        if (leader_change)
                rd_kafka_mock_partition_set_leader(mcluster, "test_topic", 0,
                                                   2);

        /* Create and init a producer */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        test_conf_set(conf, "enable.idempotence",
                      is_idempotent ? "true" : "false");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);


        /* Produce 10 messages */
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);

        /* Delete topic */
        rd_kafka_mock_topic_delete(mcluster, "test_topic");

        /* Re-create topic */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);

        /* Propagate topic change in metadata. */
        rd_sleep(2);

        /* Produce messages to recreated topic - it should be seamless. */
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * Test two topics' recreation (at the same time) with normal and idempotent
 * producer.
 */
static void do_test_two_topics_recreated_producer(rd_bool_t is_idempotent) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;

        SUB_TEST("Testing two topics' recreation with %s producer",
                 is_idempotent ? "idempotent" : "normal");

        /* Create mock cluster */
        mcluster = test_mock_cluster_new(3, &bootstrap_servers);

        /* Create topic and change leader to bump leader epoch */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);
        rd_kafka_mock_topic_create(mcluster, "test_topic2", 3, 1);
        rd_kafka_mock_partition_set_leader(mcluster, "test_topic", 0, 2);
        rd_kafka_mock_partition_set_leader(mcluster, "test_topic2", 0, 2);

        /* Create and init a producer */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        test_conf_set(conf, "enable.idempotence",
                      is_idempotent ? "true" : "false");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);


        /* Produce 10 messages */
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);
        test_produce_msgs2(rk, "test_topic2", 0, 0, 0, 10, "test", 10);

        /* Delete topic */
        rd_kafka_mock_topic_delete(mcluster, "test_topic");
        rd_kafka_mock_topic_delete(mcluster, "test_topic2");
        /* Re-create topic */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);
        rd_kafka_mock_topic_create(mcluster, "test_topic2", 3, 1);

        /* Propagate topic change in metadata. */
        rd_sleep(2);

        /* Produce messages to recreated topic - it should be seamless. */
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);
        test_produce_msgs2(rk, "test_topic2", 0, 0, 0, 10, "test", 10);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/* Test topic recreation with transactional producer */
static void do_test_topic_recreated_transactional_producer() {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;

        SUB_TEST("Testing topic recreation with transactional producer");

        /* Create mock cluster */
        mcluster = test_mock_cluster_new(3, &bootstrap_servers);

        /* Create topic and change leader to bump leader epoch */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);

        /* Note that a leader change is NECESSARY for testing a transactional
         * producer. A NOT_LEADER exception is why a metadata request is
         * triggered in the first place. Otherwise it just fails with a fatal
         * error (out of sequence) because the Produce RPC isn't aware of topic
         * IDs, and thus the client has no way to know. */
        rd_kafka_mock_partition_set_leader(mcluster, "test_topic", 0, 2);

        /* Create and init a transactional producer */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        test_conf_set(conf, "enable.idempotence", "true");
        test_conf_set(conf, "transactional.id", "test_tx");
        test_conf_set(conf, "max.in.flight", "1");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_init_transactions(rk, 5000);


        /* Produce 10 messages */
        rd_kafka_begin_transaction(rk);
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);
        rd_kafka_commit_transaction(rk, 5000);

        /* Delete topic */
        rd_kafka_mock_topic_delete(mcluster, "test_topic");

        /* Re-create topic */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);

        /* Propagate topic change in metadata*/
        rd_sleep(2);

        /* Produce messages to recreated topic. */
        rd_kafka_begin_transaction(rk);
        /* First message should queue without any problems. */
        err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("test_topic"),
                                RD_KAFKA_V_VALUE("test", 4), RD_KAFKA_V_END);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected NO_ERROR, not %s", rd_kafka_err2str(err));

        /* We might get Success or Purged from Queue, depending on when exactly
         * the metadata request is made. There's nothing we can do about it
         * until AK implements produce by topic id. So turn off error checking.
         */
        test_curr->ignore_dr_err = rd_true;

        /* Some Nth message should refuse to queue because we're in ERR__STATE
         * and we need an abort. We don't know exactly at what point it starts
         * to complain because we're not tracking the metadata request or the
         * time when epoch_drain_bump is called. So just rely on the test
         * timeout. */
        while (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("test_topic"),
                                        RD_KAFKA_V_VALUE("test", 4),
                                        RD_KAFKA_V_END);
                rd_sleep(1);
        }
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                    "Expected ERR__STATE error, not %s", rd_kafka_err2str(err));
        rd_kafka_abort_transaction(rk, 5000);

        /* Producer should work as normal after abort. */
        test_curr->ignore_dr_err = rd_false;
        rd_kafka_begin_transaction(rk);
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);
        rd_kafka_commit_transaction(rk, 5000);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}



/**
 * @brief Structure to hold information needed for test by the consumer thread.
 */
struct consumer_thread_arg {
        const char *topic_name;
        int expected_messages;
        int broadcast_msg_cnt;
        cnd_t broadcast_cnd;
        mtx_t broadcast_mtx;
        char *bootstrap_servers;
};

/**
 * @brief Consumer thread function
 *
 * Consumes messages from the specified topic and verifies that
 * the expected number of messages are received.
 */
static int run_consumer(void *arg) {
        struct consumer_thread_arg *tmc = (struct consumer_thread_arg *)arg;
        rd_kafka_t *consumer;
        rd_kafka_conf_t *conf;
        int msg_cnt = 0;
        int ret     = 0;

        TEST_SAY(
            "Consumer thread starting for topic %s, expecting %d messages\n",
            tmc->topic_name, tmc->expected_messages);

        /* Create consumer */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", tmc->bootstrap_servers);
        test_conf_set(conf, "group.id", "test_group");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        consumer = test_create_handle(RD_KAFKA_CONSUMER, conf);


        /* Subscribe to topic */
        test_consumer_subscribe(consumer, tmc->topic_name);

        /* Consume messages until we've received the expected count */
        while (msg_cnt < tmc->expected_messages) {
                rd_kafka_message_t *rkmessage;

                rkmessage = rd_kafka_consumer_poll(consumer, 1000);
                if (!rkmessage)
                        continue;

                if (rkmessage->err)
                        TEST_SAY("Consumer error: %s\n",
                                 rd_kafka_message_errstr(rkmessage));
                else {
                        msg_cnt++;
                        if (msg_cnt == tmc->broadcast_msg_cnt) {
                                mtx_lock(&tmc->broadcast_mtx);
                                cnd_broadcast(&tmc->broadcast_cnd);
                                mtx_unlock(&tmc->broadcast_mtx);
                        }
                        TEST_SAYL(3, "Received message %d/%d\n", msg_cnt,
                                  tmc->expected_messages);
                }

                rd_kafka_message_destroy(rkmessage);
        }

        TEST_SAY("Consumer thread for topic %s received all %d messages\n",
                 tmc->topic_name, msg_cnt);

        /* Clean up */
        rd_kafka_destroy(consumer);
        return ret;
}

/**
 * @brief Test topic recreation with consumer
 *
 * This test creates a consumer, produces 10 messages to a topic, and then
 * recreates the topic. It then produces 20 more messages to the new topic
 * and verifies that the consumer receives all 30 messages.
 */
static void do_test_topic_recreated_consumer() {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        struct consumer_thread_arg arg = {
            .topic_name        = "test_topic",
            .expected_messages = 30,
            .broadcast_msg_cnt = 10,
        };
        cnd_init(&arg.broadcast_cnd);
        mtx_init(&arg.broadcast_mtx, mtx_plain);

        SUB_TEST("Testing topic recreation with consumer\n");

        /* Create mock cluster */
        mcluster              = test_mock_cluster_new(3, &bootstrap_servers);
        arg.bootstrap_servers = bootstrap_servers;

        /* Create topic and bump leader epochs */
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);
        rd_kafka_mock_partition_set_leader(mcluster, "test_topic", 0, 2);
        rd_kafka_mock_partition_set_leader(mcluster, "test_topic", 0, 1);

        /* Start consumer thread and give it some time to start consuming */
        thrd_t consumer_thread;
        thrd_create(&consumer_thread, run_consumer, &arg);
        rd_sleep(5);

        /* Create producer and produce 10 messages */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 10, "test", 10);
        rd_kafka_destroy(rk);

        /* Wait for consumer to consume 10 messages */
        mtx_lock(&arg.broadcast_mtx);
        cnd_wait(&arg.broadcast_cnd, &arg.broadcast_mtx);
        mtx_unlock(&arg.broadcast_mtx);

        /* Re-create topic and wait for it to get into the metadata*/
        rd_kafka_mock_topic_delete(mcluster, "test_topic");
        rd_kafka_mock_topic_create(mcluster, "test_topic", 3, 1);
        rd_sleep(2);

        /* Create producer and produce 20 messages to new topic. */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_produce_msgs2(rk, "test_topic", 0, 0, 0, 20, "test", 10);
        rd_kafka_destroy(rk);

        /* Wait for consumer to consume remaining 20 messages. */
        thrd_join(consumer_thread, NULL);
        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

int main_0152_topic_recreate_mock(int argc, char **argv) {
        do_test_topic_recreated_producer(rd_false, rd_false);
        do_test_topic_recreated_producer(rd_false, rd_true);
        do_test_topic_recreated_producer(rd_true, rd_false);
        do_test_topic_recreated_producer(rd_true, rd_true);

        do_test_two_topics_recreated_producer(rd_false);
        do_test_two_topics_recreated_producer(rd_true);

        do_test_topic_recreated_transactional_producer();

        /* Consumer. */
        do_test_topic_recreated_consumer();
        return 0;
}
