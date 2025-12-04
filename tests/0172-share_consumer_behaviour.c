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

static volatile int share_ack_seen = 0;
static volatile long share_ack_count = -1;  // parse as long to match %ld

static void reset_share_ack_tracking(void) {
        share_ack_seen = 0;
        share_ack_count = -1;
}

static void test_share_ack_log_cb(const rd_kafka_t *rk,
                                  int level,
                                  const char *fac,
                                  const char *buf) {
        const char *prefix = "Share Acknowledgement Count: ";
        const char *p = strstr(buf, prefix);
        if (p) {
                long cnt = -1;
                if (sscanf(p + strlen(prefix), "%ld", &cnt) == 1) {
                        share_ack_count = cnt;
                        share_ack_seen = 1;
                }
        }
}

/**
 * @brief Test that the client is giving out all the messages in a single batch only for
 *        a single ShareFetch response for multiple partitions.
 *        This is done by setting fetch.wait.max.ms and fetch.min.bytes to encourage
 *        coalescing into a single ShareFetch response.
 */
static void test_batch_all_partitions_arrive_together(void) {
        char errstr[512];
        const char *group = "share-group-batch-all";
        const char *topic = test_mk_topic_name("0154-share-batch-all", 1);
        const int partition_cnt = 3;
        const int msgs_per_partition = 5;
        const int total_msgs = partition_cnt * msgs_per_partition;

        TEST_SAY("=== Expect all %d msgs across %d partitions to arrive together in one batch ===\n",
                 total_msgs, partition_cnt);

        /* Create topic and produce messages */
        test_create_topic_wait_exists(NULL, topic, partition_cnt, -1, 60 * 1000);
        for (int p = 0; p < partition_cnt; p++)
                test_produce_msgs_easy(topic, p, p, msgs_per_partition);

        /* Create share consumer */
        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

        /* Encourage coalescing into a single ShareFetch reply */
        rd_kafka_conf_set(conf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.min.bytes", "10000", errstr, sizeof(errstr));

        rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        /* Read from earliest */
        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};
        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP, group, grp_conf, 1);

        /* Subscribe */
        rd_kafka_topic_partition_list_t *subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(consumer, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        /* Keep consuming until we receive any messages, then assert all arrived in one batch */
        rd_kafka_message_t *msgs[64];
        size_t rcvd = 0;
        const int per_call_timeout_ms = 3000;
        const int max_attempts = 20; /* ~60s total */
        int attempts = 0;

        int counts[partition_cnt];
        memset(counts, 0, sizeof(counts));

        TEST_SAY("Polling batches until first non-empty batch...\n");

        while (attempts++ < max_attempts) {
                rd_kafka_error_t *err =
                    rd_kafka_share_consume_batch(consumer, per_call_timeout_ms, msgs, &rcvd);

                TEST_ASSERT(!err, "Consume error: %s",
                            err ? rd_kafka_error_string(err) : "");
                if (err) rd_kafka_error_destroy(err);

                if (rcvd == 0) {
                        TEST_SAY("No messages yet (attempt %d/%d). Retrying...\n",
                                 attempts, max_attempts);
                        continue;
                }

                TEST_SAY("Received %zu messages in first non-empty batch\n", rcvd);

                /* Assert we received all expected messages in this single batch */
                TEST_ASSERT(rcvd == (size_t)total_msgs,
                            "Expected all %d messages in a single batch, got %zu",
                            total_msgs, rcvd);

                /* Verify per-partition message counts and destroy messages */
                for (size_t i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = msgs[i];
                        TEST_ASSERT(!m->err, "Message error: %s",
                                    rd_kafka_message_errstr(m));
                        int p = m->partition;
                        TEST_ASSERT(p >= 0 && p < partition_cnt,
                                    "Unexpected partition %d", p);
                        counts[p]++;
                        rd_kafka_message_destroy(m);
                }
                for (int p = 0; p < partition_cnt; p++) {
                        TEST_ASSERT(counts[p] == msgs_per_partition,
                                    "Partition %d expected %d msgs, got %d",
                                    p, msgs_per_partition, counts[p]);
                }

                TEST_SAY("✓ First non-empty batch contained all %d msgs (5 per partition)\n",
                         total_msgs);
                break; /* success */
        }

        TEST_ASSERT(rcvd > 0,
                    "No messages received after %d attempts (~%ds).",
                    max_attempts, (per_call_timeout_ms * max_attempts) / 1000);

        /* Cleanup */
        test_delete_topic(consumer, topic);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Multiple topics, single partition each: all messages must arrive
 *        together in the first non-empty batch.
 */
static void test_batch_all_topics_single_partition_arrive_together(void) {
        char errstr[512];
        const int topics_cnt = 3;
        const int partitions_per_topic = 1;
        const int msgs_per_partition = 5;
        const int total_msgs = topics_cnt * partitions_per_topic * msgs_per_partition;

        const char *group = "share-group-batch-all-topics-sp";
        const char *topic_bases[] = {
                "0172-share-batch-all-topics-sp-0",
                "0172-share-batch-all-topics-sp-1",
                "0172-share-batch-all-topics-sp-2"
        };
        char *topics[topics_cnt];

        TEST_SAY("=== Expect all %d msgs across %d single-partition topics in one batch ===\n",
                 total_msgs, topics_cnt);

        for (int i = 0; i < topics_cnt; i++) {
                topics[i] = rd_strdup(test_mk_topic_name(topic_bases[i], 1));
                test_create_topic_wait_exists(NULL, topics[i], partitions_per_topic, -1, 60 * 1000);
                test_produce_msgs_easy(topics[i], 0, 0, msgs_per_partition);
        }

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.min.bytes", "10000", errstr, sizeof(errstr));

        rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};
        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP, group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs = rd_kafka_topic_partition_list_new(topics_cnt);
        for (int i = 0; i < topics_cnt; i++)
                rd_kafka_topic_partition_list_add(subs, topics[i], RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(consumer, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *msgs[128];
        size_t rcvd = 0;
        const int per_call_timeout_ms = 3000;
        const int max_attempts = 20;
        int attempts = 0;

        int counts[topics_cnt];
        memset(counts, 0, sizeof(counts));

        TEST_SAY("Polling until first non-empty batch (multi-topics single partition)...\n");

        while (attempts++ < max_attempts) {
                rd_kafka_error_t *err =
                        rd_kafka_share_consume_batch(consumer, per_call_timeout_ms, msgs, &rcvd);
                TEST_ASSERT(!err, "Consume error: %s",
                            err ? rd_kafka_error_string(err) : "");
                if (err) rd_kafka_error_destroy(err);

                if (rcvd == 0) {
                        TEST_SAY("No messages yet (attempt %d/%d).\n", attempts, max_attempts);
                        continue;
                }

                TEST_SAY("Received %zu messages in first non-empty batch (multi-topics SP)\n", rcvd);
                TEST_ASSERT(rcvd == (size_t)total_msgs,
                            "Expected %d messages total, got %zu", total_msgs, rcvd);

                for (size_t i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = msgs[i];
                        TEST_ASSERT(!m->err, "Message error: %s",
                                    rd_kafka_message_errstr(m));
                        const char *mtopic = rd_kafka_topic_name(m->rkt);
                        int tindex = -1;
                        for (int t = 0; t < topics_cnt; t++)
                                if (!strcmp(topics[t], mtopic)) {
                                        tindex = t;
                                        break;
                                }
                        TEST_ASSERT(tindex >= 0, "Unknown topic %s in message", mtopic);
                        TEST_ASSERT(m->partition == 0, "Unexpected partition %d", m->partition);
                        counts[tindex]++;
                        rd_kafka_message_destroy(m);
                }
                for (int t = 0; t < topics_cnt; t++)
                        TEST_ASSERT(counts[t] == msgs_per_partition,
                                    "Topic index %d expected %d msgs, got %d",
                                    t, msgs_per_partition, counts[t]);

                TEST_SAY("✓ First batch contained all %d msgs across %d topics (single partition)\n",
                         total_msgs, topics_cnt);
                break;
        }

        TEST_ASSERT(rcvd > 0, "No messages received after %d attempts", max_attempts);

        for (int i = 0; i < topics_cnt; i++) {
                test_delete_topic(consumer, topics[i]);
                rd_free(topics[i]);
        }
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

/**
 * @brief Multiple topics, multiple partitions each: all messages must arrive
 *        together in the first non-empty batch.
 */
static void test_batch_all_topics_multi_partitions_arrive_together(void) {
        char errstr[512];
        const int topics_cnt = 3;
        const int partitions_per_topic = 2;
        const int msgs_per_partition = 4;
        const int total_msgs = topics_cnt * partitions_per_topic * msgs_per_partition;

        const char *group = "share-group-batch-all-topics-mp";
        const char *topic_bases[] = {
                "0172-share-batch-all-topics-mp-0",
                "0172-share-batch-all-topics-mp-1",
                "0172-share-batch-all-topics-mp-2"
        };
        char *topics[topics_cnt];

        TEST_SAY("=== Expect all %d msgs across %d topics * %d partitions in one batch ===\n",
                 total_msgs, topics_cnt, partitions_per_topic);

        for (int i = 0; i < topics_cnt; i++) {
                topics[i] = rd_strdup(test_mk_topic_name(topic_bases[i], 1));
                test_create_topic_wait_exists(NULL, topics[i], partitions_per_topic, -1, 60 * 1000);
                for (int p = 0; p < partitions_per_topic; p++)
                        test_produce_msgs_easy(topics[i], p, p, msgs_per_partition);
        }

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        rd_kafka_conf_set(conf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "fetch.min.bytes", "20000", errstr, sizeof(errstr));

        rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        const char *grp_conf[] = {"share.auto.offset.reset","SET","earliest"};
        test_IncrementalAlterConfigs_simple(consumer, RD_KAFKA_RESOURCE_GROUP, group, grp_conf, 1);

        rd_kafka_topic_partition_list_t *subs = rd_kafka_topic_partition_list_new(topics_cnt);
        for (int i = 0; i < topics_cnt; i++)
                rd_kafka_topic_partition_list_add(subs, topics[i], RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(consumer, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *msgs[256];
        size_t rcvd = 0;
        const int per_call_timeout_ms = 3000;
        const int max_attempts = 20;
        int attempts = 0;

        int counts[topics_cnt][partitions_per_topic];
        memset(counts, 0, sizeof(counts));

        TEST_SAY("Polling until first non-empty batch (multi-topics multi-partitions)...\n");

        while (attempts++ < max_attempts) {
                rd_kafka_error_t *err =
                        rd_kafka_share_consume_batch(consumer, per_call_timeout_ms, msgs, &rcvd);
                TEST_ASSERT(!err, "Consume error: %s",
                            err ? rd_kafka_error_string(err) : "");
                if (err) rd_kafka_error_destroy(err);

                if (rcvd == 0) {
                        TEST_SAY("No messages yet (attempt %d/%d).\n", attempts, max_attempts);
                        continue;
                }

                TEST_SAY("Received %zu messages in first non-empty batch (multi-topics MP)\n", rcvd);
                TEST_ASSERT(rcvd == (size_t)total_msgs,
                            "Expected %d messages total, got %zu", total_msgs, rcvd);

                for (size_t i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = msgs[i];
                        TEST_ASSERT(!m->err, "Message error: %s",
                                    rd_kafka_message_errstr(m));
                        const char *mtopic = rd_kafka_topic_name(m->rkt);
                        int tindex = -1;
                        for (int t = 0; t < topics_cnt; t++)
                                if (!strcmp(topics[t], mtopic)) {
                                        tindex = t;
                                        break;
                                }
                        TEST_ASSERT(tindex >= 0, "Unknown topic %s", mtopic);
                        TEST_ASSERT(m->partition >= 0 && m->partition < partitions_per_topic,
                                    "Unexpected partition %d", m->partition);
                        counts[tindex][m->partition]++;
                        rd_kafka_message_destroy(m);
                }

                for (int t = 0; t < topics_cnt; t++)
                        for (int p = 0; p < partitions_per_topic; p++)
                                TEST_ASSERT(counts[t][p] == msgs_per_partition,
                                            "Topic idx %d partition %d expected %d msgs, got %d",
                                            t, p, msgs_per_partition, counts[t][p]);

                TEST_SAY("✓ First batch contained all %d msgs across %d topics * %d partitions\n",
                         total_msgs, topics_cnt, partitions_per_topic);
                break;
        }

        TEST_ASSERT(rcvd > 0, "No messages received after %d attempts", max_attempts);

        for (int i = 0; i < topics_cnt; i++) {
                test_delete_topic(consumer, topics[i]);
                rd_free(topics[i]);
        }
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

static void test_transactional_noop_normal_transaction_normal(void) {
        char errstr[512];
        const char *group = "share-group-txn-noop";
        const char *topic = test_mk_topic_name("0172-share-txn-noop", 1);
        const int partition = 0;
        const int batch_count = 5;

        TEST_SAY("=== Transactional noop → normal → transactional → normal sequence (linger 1000ms) ===\n");

        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        /* Plain producer */
        rd_kafka_conf_t *plain_conf;
        test_conf_init(&plain_conf, NULL, 60);
        rd_kafka_conf_set(plain_conf, "linger.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(plain_conf, "batch.size", "1048576", errstr, sizeof(errstr));
        rd_kafka_conf_set(plain_conf, "batch.num.messages", "1000", errstr, sizeof(errstr));
        rd_kafka_t *plain =
            rd_kafka_new(RD_KAFKA_PRODUCER, plain_conf, errstr, sizeof(errstr));
        TEST_ASSERT(plain, "plain producer create failed: %s", errstr);

        /* Transactional producer */
        rd_kafka_conf_t *tx_conf;
        test_conf_init(&tx_conf, NULL, 60);
        rd_kafka_conf_set(tx_conf, "transactional.id", topic, errstr, sizeof(errstr));
        rd_kafka_conf_set(tx_conf, "linger.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(tx_conf, "batch.size", "1048576", errstr, sizeof(errstr));
        rd_kafka_conf_set(tx_conf, "batch.num.messages", "1000", errstr, sizeof(errstr));
        rd_kafka_t *txp =
            rd_kafka_new(RD_KAFKA_PRODUCER, tx_conf, errstr, sizeof(errstr));
        TEST_ASSERT(txp, "txn producer create failed: %s", errstr);

        rd_kafka_error_t *ke = rd_kafka_init_transactions(txp, 30 * 1000);
        TEST_ASSERT(!ke, "init_transactions failed: %s",
                    ke ? rd_kafka_error_string(ke) : "");
        if (ke)
                rd_kafka_error_destroy(ke);

        /* No-op transaction (begin + commit without messages) */
        ke = rd_kafka_begin_transaction(txp);
        TEST_ASSERT(!ke, "begin_transaction (noop) failed: %s",
                    ke ? rd_kafka_error_string(ke) : "");
        if (ke)
                rd_kafka_error_destroy(ke);
        
        for (int i = 0; i < batch_count; i++) {
                char key[32], val[32];
                rd_snprintf(key, sizeof(key), "txn-%d", i);
                rd_snprintf(val, sizeof(val), "txn-payload-%d", i);
                TEST_ASSERT(!rd_kafka_producev(
                                    txp,
                                    RD_KAFKA_V_TOPIC(topic),
                                    RD_KAFKA_V_PARTITION(partition),
                                    RD_KAFKA_V_KEY(key, (int)strlen(key)),
                                    RD_KAFKA_V_VALUE(val, (int)strlen(val)),
                                    RD_KAFKA_V_END),
                            "txn produce failed");
        }

        ke = rd_kafka_commit_transaction(txp, -1);
        TEST_ASSERT(!ke, "commit_transaction (noop) failed: %s",
                    ke ? rd_kafka_error_string(ke) : "");
        if (ke)
                rd_kafka_error_destroy(ke);

        TEST_SAY("Committed initial empty transaction\n");

        /* First normal batch (5 messages) */
        for (int i = 0; i < batch_count; i++) {
                char key[32], val[32];
                rd_snprintf(key, sizeof(key), "normal-pre-%d", i+5);
                rd_snprintf(val, sizeof(val), "payload-%d", i+5);
                TEST_ASSERT(!rd_kafka_producev(
                                    plain,
                                    RD_KAFKA_V_TOPIC(topic),
                                    RD_KAFKA_V_PARTITION(partition),
                                    RD_KAFKA_V_KEY(key, (int)strlen(key)),
                                    RD_KAFKA_V_VALUE(val, (int)strlen(val)),
                                    RD_KAFKA_V_END),
                            "normal pre produce failed");
        }
        rd_kafka_flush(plain, 10 * 1000);

        TEST_SAY("Produced first %d normal messages\n", batch_count);

        /* Second transaction (5 messages) */
        ke = rd_kafka_begin_transaction(txp);
        TEST_ASSERT(!ke, "begin_transaction (payload) failed: %s",
                    ke ? rd_kafka_error_string(ke) : "");
        if (ke)
                rd_kafka_error_destroy(ke);

        for (int i = 0; i < batch_count; i++) {
                char key[32], val[32];
                rd_snprintf(key, sizeof(key), "txn-%d", i+10);
                rd_snprintf(val, sizeof(val), "txn-payload-%d", i+10);
                TEST_ASSERT(!rd_kafka_producev(
                                    txp,
                                    RD_KAFKA_V_TOPIC(topic),
                                    RD_KAFKA_V_PARTITION(partition),
                                    RD_KAFKA_V_KEY(key, (int)strlen(key)),
                                    RD_KAFKA_V_VALUE(val, (int)strlen(val)),
                                    RD_KAFKA_V_END),
                            "txn produce failed");
        }

        ke = rd_kafka_commit_transaction(txp, -1);
        TEST_ASSERT(!ke, "commit_transaction (payload) failed: %s",
                    ke ? rd_kafka_error_string(ke) : "");
        if (ke)
                rd_kafka_error_destroy(ke);

        rd_kafka_flush(txp, 10 * 1000);
        TEST_SAY("Committed transactional batch of %d messages\n", batch_count);

        /* Final normal batch (5 messages) */
        for (int i = 0; i < batch_count; i++) {
                char key[32], val[32];
                rd_snprintf(key, sizeof(key), "normal-post-%d", i+15);
                rd_snprintf(val, sizeof(val), "payload-post-%d", i+20);
                TEST_ASSERT(!rd_kafka_producev(
                                    plain,
                                    RD_KAFKA_V_TOPIC(topic),
                                    RD_KAFKA_V_PARTITION(partition),
                                    RD_KAFKA_V_KEY(key, (int)strlen(key)),
                                    RD_KAFKA_V_VALUE(val, (int)strlen(val)),
                                    RD_KAFKA_V_END),
                            "normal post produce failed");
        }
        rd_kafka_flush(plain, 10 * 1000);
        TEST_SAY("Produced final %d normal messages\n", batch_count);

        rd_kafka_destroy(txp);
        rd_kafka_destroy(plain);

        /* Share consumer */
        rd_kafka_conf_t *cconf;
        test_conf_init(&cconf, NULL, 60);
        rd_kafka_conf_set(cconf, "share.consumer", "true", errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "group.protocol", "consumer", errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "enable.auto.commit", "false", errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "fetch.min.bytes", "1", errstr, sizeof(errstr));
        rd_kafka_conf_set(cconf, "debug", "fetch", errstr, sizeof(errstr));

        rd_kafka_conf_set_log_cb(cconf, test_share_ack_log_cb);
        reset_share_ack_tracking();

        rd_kafka_t *consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, cconf, errstr, sizeof(errstr));
        TEST_ASSERT(consumer, "Failed to create consumer: %s", errstr);

        const char *grp_conf[] = {
                "share.auto.offset.reset", "SET", "earliest"
        };
        test_IncrementalAlterConfigs_simple(consumer,
                                            RD_KAFKA_RESOURCE_GROUP,
                                            group,
                                            grp_conf,
                                            1);

        rd_kafka_topic_partition_list_t *subs =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        TEST_ASSERT(!rd_kafka_subscribe(consumer, subs), "subscribe failed");
        rd_kafka_topic_partition_list_destroy(subs);

        rd_kafka_message_t *msgs[500];
        size_t rcvd = 0;
        const int per_call_timeout_ms = 3000;
        const int max_attempts = 10;
        int attempts = 0;

        TEST_SAY("Polling ShareFetch batches...\n");
        while (attempts++ < max_attempts) {
                rd_kafka_error_t *cerr =
                        rd_kafka_share_consume_batch(consumer,
                                                     per_call_timeout_ms,
                                                     msgs,
                                                     &rcvd);
                TEST_ASSERT(!cerr, "Consume error: %s",
                            cerr ? rd_kafka_error_string(cerr) : "");
                if (cerr)
                        rd_kafka_error_destroy(cerr);

                if (rcvd == 0) {
                        TEST_SAY("No messages yet (attempt %d/%d).\n",
                                 attempts, max_attempts);
                        continue;
                }

                TEST_SAY("Received %zu messages via ShareFetch:\n", rcvd);
                // Verify offsets set: allowed ranges [0-4], [6-15], [17-21]; deny 5 and 16.
                size_t ok_cnt = 0;
                for (size_t i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *m = msgs[i];
                        TEST_ASSERT(!m->err, "Message error: %s",
                                    rd_kafka_message_errstr(m));

                        int64_t off = m->offset;
                        rd_bool_t allowed =
                            (off >= 0  && off <= 4)  ||
                            (off >= 6  && off <= 15) ||
                            (off >= 17 && off <= 21);

                        TEST_ASSERT(allowed,
                                    "Unexpected offset %" PRId64
                                    " (disallowed: 5,16; allowed ranges: 0-4,6-15,17-21)",
                                    off);
                        TEST_ASSERT(off != 5,  "Offset 5 must not be delivered");
                        TEST_ASSERT(off != 16, "Offset 16 must not be delivered");

                        rd_kafka_message_destroy(m);
                        ok_cnt++;
                }

                // Expect exactly 20 messages (5 + 10 + 5)
                TEST_ASSERT(ok_cnt == 20,
                            "Expected 20 allowed offsets, got %zu", ok_cnt);
                break;
        }

        TEST_ASSERT(rcvd == 20, "Expected 20 messages, got %zu", rcvd);

        /* Assert Share Acknowledgement Count observed in logs equals 5 */
        TEST_ASSERT(share_ack_seen, "Share Acknowledgement Count log not seen");
        TEST_ASSERT(share_ack_count == 5,
                    "Expected Share Acknowledgement Count: 5, got %d",
                    share_ack_count);

        test_delete_topic(consumer, topic);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}



int main_0172_share_consumer_behaviour(int argc, char **argv) {
        test_batch_all_partitions_arrive_together();
        test_batch_all_topics_single_partition_arrive_together();
        test_batch_all_topics_multi_partitions_arrive_together();
        test_transactional_noop_normal_transaction_normal();
        return 0;
}