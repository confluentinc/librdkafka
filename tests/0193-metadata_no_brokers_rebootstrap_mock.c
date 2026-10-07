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
 * @name A Metadata response received while the connection to the last broker
 *       drops must not leave the client disconnected.
 *
 * A single-node KRaft broker excludes itself from Metadata responses once it
 * is fenced at the end of a controlled shutdown, and keeps answering requests
 * for a few more milliseconds until its request processors stop. A Metadata
 * request answered in that window returns no brokers but the requested topics
 * (with no leader), and the connection is closed right after.
 *
 * Seen in production with a consumer whose only learned broker was that
 * broker, both happen close enough that the broker thread sees the connection
 * close before the main thread handles the already received response:
 *  1. The broker goes UP -> DOWN. It was the last non-logical broker that was
 *     up, so rd_kafka_broker_set_state() calls rd_kafka_rebootstrap(), which
 *     sets rk_rebootstrap_in_progress and arms an immediate rebootstrap_tmr.
 *  2. The main thread then handles the response:
 *     rd_kafka_metadata_decommission_unavailable_brokers() decommissioned the
 *     broker (it is not in the response) and rd_kafka_handle_Metadata() called
 *     rd_kafka_rebootstrap_tmr_stop(), cancelling the re-bootstrap from 1.
 *     rk_rebootstrap_in_progress, only reset by the timer callback, stayed
 *     set, so every later rd_kafka_rebootstrap() was a no-op.
 * The client was left without any non-logical broker, so it couldn't refresh
 * metadata or reconnect, and it never re-bootstrapped: it stayed disconnected
 * forever, even when the cluster was back. A partition leader query with an
 * infinite timeout, such as rd_kafka_query_watermark_offsets(), then never
 * returned.
 *
 * The test makes the interleaving deterministic by holding the main thread in
 * the log callback at the "Received metadata" line, which
 * rd_kafka_handle_Metadata() logs before parsing the response, until the
 * broker thread has seen the connection close.
 *
 * Subtests:
 *  - fenced: the scenario above. The client must recover once the cluster is
 *    back: the hidden broker 1 keeps serving the bootstrap address and a new
 *    broker 2 becomes the partition leader.
 *  - not fenced: the held response lists broker 1. The re-bootstrap scheduled
 *    when the connection dropped must still run.
 */

static mtx_t state_lock;
static cnd_t state_cnd;

static rd_kafka_mock_cluster_t *mcluster;

/* Protected by state_lock */
static rd_bool_t bootstrap_decommissioned;
static rd_bool_t hold_next_metadata_response;
static rd_bool_t held_metadata_response;
static rd_bool_t broker_down;
static int rebootstrap_cnt;

/**
 * @brief Tracks broker state changes and, when armed, holds the main thread
 *        before it handles the next Metadata response until the connection to
 *        broker 1 has dropped.
 */
static void metadata_broker_down_log_cb(const rd_kafka_t *rk,
                                        int level,
                                        const char *fac,
                                        const char *buf) {
        rd_bool_t hold = rd_false;
        rd_bool_t down;

        mtx_lock(&state_lock);
        if (strstr(buf, "/bootstrap: Decommissioning this broker"))
                bootstrap_decommissioned = rd_true;
        if (strstr(buf, "/1: Broker changed state UP -> DOWN"))
                broker_down = rd_true;
        if (strstr(buf, "Starting re-bootstrap sequence"))
                rebootstrap_cnt++;
        if (hold_next_metadata_response &&
            strstr(buf, "===== Received metadata")) {
                hold_next_metadata_response = rd_false;
                held_metadata_response      = rd_true;
                hold                        = rd_true;
        }
        cnd_broadcast(&state_cnd);
        mtx_unlock(&state_lock);

        if (!hold)
                return;

        TEST_SAY("Holding the Metadata response, dropping broker 1\n");
        rd_kafka_mock_broker_set_down(mcluster, 1);

        mtx_lock(&state_lock);
        while (!broker_down) {
                if (cnd_timedwait_ms(&state_cnd, &state_lock,
                                     tmout_multip(10000)) == thrd_timedout)
                        break;
        }
        down = broker_down;
        mtx_unlock(&state_lock);

        /* "Broker changed state" is logged at the start of
         * rd_kafka_broker_set_state(), let it call rd_kafka_rebootstrap(). */
        rd_usleep(500 * 1000, 0);

        TEST_SAY("Releasing the Metadata response (broker 1 %s)\n",
                 down ? "is down" : "did NOT go down");
}

/**
 * @brief Waits up to \p timeout_ms for \p cond to be true.
 */
static rd_bool_t
wait_for(rd_bool_t (*cond)(void *), void *arg, int timeout_ms) {
        rd_bool_t value;
        rd_ts_t abs_timeout = test_clock() + (rd_ts_t)timeout_ms * 1000;

        mtx_lock(&state_lock);
        while (!(value = cond(arg)) && test_clock() < abs_timeout)
                cnd_timedwait_ms(&state_cnd, &state_lock, 100);
        mtx_unlock(&state_lock);

        return value;
}

/**
 * @brief Condition for wait_for(): \p flag is set.
 */
static rd_bool_t flag_is_set(void *flag) {
        return *(rd_bool_t *)flag;
}

/**
 * @brief Condition for wait_for(): a re-bootstrap started since there were
 *        \p rebootstraps_before of them.
 */
static rd_bool_t rebootstrapped_since(void *rebootstraps_before) {
        return rebootstrap_cnt > *(int *)rebootstraps_before;
}

/**
 * @brief Holds a Metadata response while the connection to broker 1, the
 *        only broker known to the consumer, drops.
 *
 * @param fenced Whether broker 1 is fenced: hidden from Metadata responses.
 */
static void do_test_metadata_response_during_broker_down(rd_bool_t fenced) {
        const char *topic            = test_mk_topic_name(__FUNCTION__, 1);
        const char *debug_contexts[] = {"broker", "metadata", NULL};
        test_conf_log_interceptor_t *log_interceptor;
        const rd_kafka_metadata_t *md;
        rd_kafka_topic_t *rkt;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;
        rd_bool_t held, down, rebootstrapped;
        int64_t low, high;
        rd_kafka_t *rk;
        int rebootstraps_before;
        rd_ts_t recover_until;
        int i;

        SUB_TEST("%s", fenced ? "fenced" : "not fenced");

        mtx_init(&state_lock, mtx_plain);
        cnd_init(&state_cnd);
        bootstrap_decommissioned    = rd_false;
        hold_next_metadata_response = rd_false;
        held_metadata_response      = rd_false;
        broker_down                 = rd_false;
        rebootstrap_cnt             = 0;

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", topic);
        /* Let the cached topic, without leader after a response without
         * brokers, expire during the test, after which leader queries wait
         * for a metadata refresh instead of failing with _UNKNOWN_PARTITION.
         * That's where a query without timeout hangs. */
        test_conf_set(conf, "metadata.max.age.ms", "3000");
        log_interceptor = test_conf_set_log_interceptor(
            conf, metadata_broker_down_log_cb, debug_contexts);
        /* Broker connection failures are expected. */
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        rk  = test_create_handle(RD_KAFKA_CONSUMER, conf);
        rkt = test_create_topic_object(rk, topic, NULL);

        TEST_SAY("Learning broker 1 from the bootstrap broker\n");
        err = rd_kafka_query_watermark_offsets(rk, topic, 0, &low, &high,
                                               tmout_multip(10000));
        TEST_ASSERT(!err, "Initial watermark query failed: %s",
                    rd_kafka_err2name(err));
        TEST_ASSERT(wait_for(flag_is_set, &bootstrap_decommissioned,
                             tmout_multip(10000)),
                    "Bootstrap broker was not decommissioned");

        if (fenced) {
                TEST_SAY(
                    "Fencing broker 1: no brokers in Metadata "
                    "responses\n");
                TEST_CALL_ERR__(
                    rd_kafka_mock_broker_remove_from_metadata(mcluster, 1));
        }

        mtx_lock(&state_lock);
        broker_down                 = rd_false;
        hold_next_metadata_response = rd_true;
        rebootstraps_before         = rebootstrap_cnt;
        mtx_unlock(&state_lock);

        err = rd_kafka_metadata(rk, 0, rkt, &md, tmout_multip(5000));
        TEST_SAY("Metadata request: %s\n", rd_kafka_err2name(err));
        if (!err) {
                TEST_SAY("Metadata response: %d broker(s), %d topic(s)\n",
                         md->broker_cnt, md->topic_cnt);
                rd_kafka_metadata_destroy(md);
        }

        mtx_lock(&state_lock);
        held = held_metadata_response;
        down = broker_down;
        mtx_unlock(&state_lock);
        TEST_ASSERT(held, "No Metadata response was held");
        TEST_ASSERT(down,
                    "Broker 1 did not go down while the Metadata response "
                    "was held");

        if (!fenced) {
                /* The re-bootstrap scheduled when broker 1 went down must not
                 * be cancelled by the Metadata response. */
                rebootstrapped =
                    wait_for(rebootstrapped_since, &rebootstraps_before,
                             tmout_multip(5000));
                TEST_ASSERT(rebootstrapped,
                            "No re-bootstrap after broker 1 went down");
                goto done;
        }

        TEST_SAY("Broker 1 is back (still fenced), broker 2 is leader\n");
        TEST_CALL_ERR__(rd_kafka_mock_broker_set_up(mcluster, 1));
        TEST_CALL_ERR__(rd_kafka_mock_broker_add(mcluster, 2));
        /* Partitions are reassigned with their current replica count, which
         * is 0 since broker 1 was hidden: set the leader explicitly. */
        TEST_CALL_ERR__(
            rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 2));

        /* The original bug happened when rd_kafka_query_watermark_offsets()
         * was called without a timeout: with the bug it never returned.
         * Bound each call here and allow plenty of time for the client to
         * recover. */
        recover_until = test_clock() + (rd_ts_t)tmout_multip(10000) * 1000;
        for (i = 0;; i++) {
                err = rd_kafka_query_watermark_offsets(
                    rk, topic, 0, &low, &high, tmout_multip(1000));
                if (!err || test_clock() >= recover_until)
                        break;
                TEST_SAY("Query %d: %s\n", i, rd_kafka_err2name(err));
                /* _UNKNOWN_PARTITION is returned right away */
                rd_usleep(500 * 1000, 0);
        }

        TEST_ASSERT(!err,
                    "Client did not recover after the cluster came back: "
                    "watermark query still fails with %s",
                    rd_kafka_err2name(err));

done:
        mtx_lock(&state_lock);
        TEST_SAY("%d re-bootstrap(s) since the Metadata response was held\n",
                 rebootstrap_cnt - rebootstraps_before);
        mtx_unlock(&state_lock);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        mcluster = NULL;
        rd_free(log_interceptor);
        cnd_destroy(&state_cnd);
        mtx_destroy(&state_lock);

        SUB_TEST_PASS();
}

int main_0193_metadata_no_brokers_rebootstrap_mock(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_metadata_response_during_broker_down(rd_true /*fenced*/);
        do_test_metadata_response_during_broker_down(rd_false /*!fenced*/);

        return 0;
}
