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


/**
 * @name Broker threads without a connection, partitions or requests wait for
 *       an op instead of waking up every second.
 *
 * A client starts one broker thread per broker in the cluster metadata, but
 * with sparse connections only connects to the brokers it needs. Each test
 * lets a client learn all brokers of a mock cluster, checks over an idle
 * period that the unconnected brokers' `wakeups` statistic doesn't grow (the
 * one-second loop gave about one per second), and then checks that work that
 * needs one of those brokers still wakes its thread and connects it.
 */

#define BROKER_CNT   10
#define IDLE_SECONDS 3

/** Latest statistics JSON, copied by the stats callback. */
static char *last_stats;
static int stats_cnt;

static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        free(last_stats);
        last_stats = rd_strndup(json, json_len);
        stats_cnt++;
        return 0;
}

/**
 * @brief Waits for a statistics callback newer than the current one.
 */
static void await_stats(rd_kafka_t *rk) {
        int cnt = stats_cnt;
        test_timing_t t;
        TIMING_START(&t, "await_stats");
        while (stats_cnt == cnt) {
                rd_kafka_poll(rk, 100);
                TEST_ASSERT(TIMING_DURATION(&t) < tmout_multip(10 * 1000000),
                            "no statistics in 10 s");
        }
        TIMING_STOP(&t);
}

/**
 * @brief The `state` and `wakeups` of the broker whose `name` contains
 *        \p name (such as "/3\"" for nodeid 3) in the latest statistics.
 *
 * @returns 0 on success, -1 if the broker isn't in the statistics.
 */
static int broker_stats(const char *name, char *state, int64_t *wakeups) {
        const char *p = last_stats;

        while ((p = strstr(p, "\"name\":\""))) {
                const char *end = strchr(p + 8, '"');
                const char *next, *s, *w;

                next = strstr(p + 8, "\"name\":\"");
                if (!end || !strstr(p + 8, name) || strstr(p + 8, name) > end) {
                        p = p + 8;
                        continue;
                }
                s = strstr(p, "\"state\":\"");
                w = strstr(p, "\"wakeups\":");
                if (!s || !w || (next && (s > next || w > next)))
                        return -1;
                sscanf(s + 9, "%31[^\"]", state);
                *wakeups = strtoll(w + 10, NULL, 10);
                return 0;
        }
        return -1;
}

/**
 * @brief Waits \p seconds and returns the wakeups of the brokers that stayed
 *        unconnected (INIT) for the whole period, counting them in \p idlep.
 */
static int64_t idle_wakeups(rd_kafka_t *rk, int seconds, int *idlep) {
        int64_t before[BROKER_CNT], sum = 0;
        char state[32], name[32];
        int64_t w;
        int i;

        *idlep = 0;

        await_stats(rk);
        for (i = 0; i < BROKER_CNT; i++) {
                rd_snprintf(name, sizeof(name), "/%d\"", i + 1);
                before[i] = -1;
                if (!broker_stats(name, state, &w) && !strcmp(state, "INIT"))
                        before[i] = w;
        }

        rd_sleep(seconds);

        await_stats(rk);
        for (i = 0; i < BROKER_CNT; i++) {
                rd_snprintf(name, sizeof(name), "/%d\"", i + 1);
                if (before[i] < 0 || broker_stats(name, state, &w) ||
                    strcmp(state, "INIT"))
                        continue;
                sum += w - before[i];
                (*idlep)++;
        }
        return sum;
}

static rd_kafka_conf_t *idle_conf(const char *bootstraps) {
        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "statistics.interval.ms", "500");
        rd_kafka_conf_set_stats_cb(conf, stats_cb);
        return conf;
}

/**
 * @brief Learns all brokers from metadata and checks that the unconnected
 *        ones stay quiet.
 */
static void learn_and_check_idle(rd_kafka_t *rk) {
        const struct rd_kafka_metadata *md;
        int64_t wakeups;
        int idle;

        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, NULL, &md, 5000));
        rd_kafka_metadata_destroy(md);

        wakeups = idle_wakeups(rk, IDLE_SECONDS, &idle);
        TEST_SAY("%" PRId64 " wakeups from %d unconnected brokers in %d s\n",
                 wakeups, idle, IDLE_SECONDS);
        TEST_ASSERT(idle >= BROKER_CNT / 2,
                    "only %d of %d brokers stayed unconnected", idle,
                    BROKER_CNT);
        /* The one-second loop gives about IDLE_SECONDS wakeups per broker. */
        TEST_ASSERT(wakeups <= idle,
                    "%" PRId64 " wakeups from %d unconnected brokers in %d s",
                    wakeups, idle, IDLE_SECONDS);
}

static void destroy_stats(void) {
        free(last_stats);
        last_stats = NULL;
}

/**
 * @brief A ListOffsets request to each partition leader wakes the leader's
 *        idle thread and connects it, and the client is destroyed promptly.
 */
static void do_test_leader_requests(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps, *topic = "idle_leaders";
        rd_kafka_t *rk;
        test_timing_t t_destroy;
        int i;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(BROKER_CNT, &bootstraps);
        TEST_CALL_ERR__(
            rd_kafka_mock_topic_create(mcluster, topic, BROKER_CNT, 1));
        rk = test_create_handle(RD_KAFKA_PRODUCER, idle_conf(bootstraps));

        learn_and_check_idle(rk);

        for (i = 0; i < BROKER_CNT; i++) {
                int64_t lo, hi;
                TEST_CALL_ERR__(rd_kafka_query_watermark_offsets(
                    rk, topic, i, &lo, &hi, tmout_multip(5000)));
        }

        TIMING_START(&t_destroy, "destroy");
        rd_kafka_destroy(rk);
        TIMING_STOP(&t_destroy);
        TIMING_ASSERT_LATER(&t_destroy, 0, 2000);

        test_mock_cluster_destroy(mcluster);
        destroy_stats();
        SUB_TEST_PASS();
}

/**
 * @brief A consumer that subscribes after the idle period joins its group
 *        and has every partition delegated to its idle leader.
 */
static void do_test_consumer_subscribe(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps, *topic = "idle_subscribe";
        rd_kafka_topic_partition_list_t *assignment;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(BROKER_CNT, &bootstraps);
        TEST_CALL_ERR__(
            rd_kafka_mock_topic_create(mcluster, topic, BROKER_CNT, 1));
        rd_kafka_mock_group_initial_rebalance_delay_ms(mcluster, 0);
        conf = idle_conf(bootstraps);
        test_conf_set(conf, "group.id", topic);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        learn_and_check_idle(rk);

        test_consumer_subscribe(rk, topic);
        test_consumer_wait_assignment(rk, rd_true);
        TEST_CALL_ERR__(rd_kafka_assignment(rk, &assignment));
        TEST_ASSERT(assignment->cnt == BROKER_CNT,
                    "%d of %d partitions assigned", assignment->cnt,
                    BROKER_CNT);
        rd_kafka_topic_partition_list_destroy(assignment);

        test_consumer_close(rk);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        destroy_stats();
        SUB_TEST_PASS();
}

static void dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkm, void *opaque) {
        int *delivered = opaque;
        if (!rkm->err)
                (*delivered)++;
}

/**
 * @brief Producing to a partition delegates it to its leader, whose idle
 *        thread must wake, connect and deliver.
 */
static void do_test_producer_delivery(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps, *topic = "idle_produce";
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int i, delivered = 0;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(BROKER_CNT, &bootstraps);
        TEST_CALL_ERR__(
            rd_kafka_mock_topic_create(mcluster, topic, BROKER_CNT, 1));
        conf = idle_conf(bootstraps);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);
        rd_kafka_conf_set_opaque(conf, &delivered);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        learn_and_check_idle(rk);

        for (i = 0; i < BROKER_CNT; i++)
                TEST_CALL_ERR__(rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(i),
                    RD_KAFKA_V_VALUE("x", 1), RD_KAFKA_V_END));
        TEST_CALL_ERR__(rd_kafka_flush(rk, tmout_multip(10000)));
        TEST_ASSERT(delivered == BROKER_CNT, "%d of %d messages delivered",
                    delivered, BROKER_CNT);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        destroy_stats();
        SUB_TEST_PASS();
}

int main_0193_idle_broker_wakeups_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_leader_requests();
        do_test_consumer_subscribe();
        do_test_producer_delivery();

        return 0;
}
