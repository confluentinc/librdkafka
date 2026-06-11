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
 * @name Share consumer rd_kafka_share_consume_batch() timeout matrix
 *
 * Exercises rd_kafka_share_consume_batch() across the full range of
 * timeout_ms values that an application can pass:
 *
 *   0, 1, 300, 500, 1000, 3000, 5000, 10000, 30000  (Phase A)
 *   -1 (infinite)                                    (Phase C)
 *
 * Phase A — empty topic
 *   For each timeout, the call must return NULL (no error) and rcvd == 0.
 *   The wall-clock duration must be approximately the requested timeout,
 *   with tolerance that scales so the upper-bound assertion is tight for
 *   short timeouts instead of being trivially satisfied.
 *
 * Phase B — consumer still alive
 *   Produce records and consume them with a short timeout. This is the
 *   assertion that the consumer was NOT fenced during the Phase-A polls
 *   that exceeded the heartbeat interval. While inside consume_batch the
 *   client sets rk_ts_last_poll = INT64_MAX so max.poll.interval.ms cannot
 *   fence the consumer regardless of timeout length, and the heartbeat
 *   thread runs independently of the app thread.
 *
 * Phase C — infinite timeout
 *   Produce records, then call consume_batch with timeout_ms = -1. Must
 *   return promptly with records, not hang. -1 is intentionally not in
 *   Phase A: an empty topic would block the test forever.
 *
 * Heartbeat interaction note: the default heartbeat.interval.ms is 3000
 * (server may override via group.consumer.heartbeat.interval.ms). The
 * 5000/10000/30000 timeouts intentionally span multiple heartbeat
 * intervals. Phase B's successful consume proves no fencing occurred.
 */

#define BATCH_SIZE 100

/** Common producer reused across tests. */
static rd_kafka_t *common_producer;

static void do_test_consume_batch_timeout_matrix(void) {
        rd_kafka_share_t *consumer;
        rd_kafka_message_t *batch[BATCH_SIZE];
        const char *topic;
        const char *group = "share-timeout-matrix";
        rd_kafka_topic_partition_list_t *subs;
        /* Phase A timeouts (empty topic). -1 (infinite) is tested in
         * Phase C only — it would hang here. */
        const int timeouts_ms[] = {0,    1,    300,   500,   1000,
                                   3000, 5000, 10000, 30000};
        size_t i;

        SUB_TEST();

        TEST_SAY("\n");
        TEST_SAY("=== consume_batch timeout matrix ===\n");

        consumer = test_create_share_consumer(group, NULL);
        topic    = test_mk_topic_name("0188-timeout-matrix", 1);
        test_create_topic_wait_exists(NULL, topic, 1, -1, 60 * 1000);

        test_share_set_auto_offset_reset(group, "earliest");
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(consumer, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Phase A — empty topic, walk every timeout. */
        for (i = 0; i < RD_ARRAY_SIZE(timeouts_ms); i++) {
                int timeout_ms = timeouts_ms[i];
                size_t rcvd    = 0;
                rd_ts_t t_start, t_end;
                int actual_ms;
                int tolerance_ms;
                rd_kafka_error_t *err;

                /* Tolerance scales with the requested timeout so the
                 * upper-bound assertion stays tight for short timeouts:
                 *   0 ms     : 50 ms   (non-blocking — must be instant)
                 *   1-1000 ms: 200 ms  (scheduling/jitter floor)
                 *   <10000   : 500 ms
                 *   >=10000  : 2000 ms (HB/network/IO overhead) */
                if (timeout_ms == 0)
                        tolerance_ms = 50;
                else if (timeout_ms <= 1000)
                        tolerance_ms = 200;
                else if (timeout_ms < 10000)
                        tolerance_ms = 500;
                else
                        tolerance_ms = 2000;

                TEST_SAY(
                    "Phase A: empty-topic poll, timeout_ms=%d (+/-%dms)\n",
                    timeout_ms, tolerance_ms);

                t_start = test_clock();
                err     = rd_kafka_share_consume_batch(consumer, timeout_ms,
                                                       batch, &rcvd);
                t_end   = test_clock();

                actual_ms = (int)((t_end - t_start) / 1000);

                TEST_ASSERT(
                    !err,
                    "timeout_ms=%d: unexpected error %s",
                    timeout_ms,
                    err ? rd_kafka_err2str(rd_kafka_error_code(err)) : "");
                TEST_ASSERT(
                    rcvd == 0,
                    "timeout_ms=%d: empty topic should return 0 records, "
                    "got %zu",
                    timeout_ms, rcvd);

                /* Upper bound: must not overshoot beyond the band. */
                TEST_ASSERT(
                    actual_ms <= timeout_ms + tolerance_ms,
                    "timeout_ms=%d: actual wait %dms exceeds upper bound "
                    "%dms (+%dms tolerance)",
                    timeout_ms, actual_ms, timeout_ms + tolerance_ms,
                    tolerance_ms);

                /* Lower bound: only meaningful once the requested timeout
                 * is large enough to dominate scheduling jitter. */
                if (timeout_ms >= 500)
                        TEST_ASSERT(
                            actual_ms >= timeout_ms - tolerance_ms,
                            "timeout_ms=%d: actual wait %dms returned too "
                            "early (lower bound %dms)",
                            timeout_ms, actual_ms,
                            timeout_ms - tolerance_ms);

                TEST_SAY("  timeout_ms=%d -> actual %dms (OK)\n", timeout_ms,
                         actual_ms);
        }

        /* Phase B — produce records and consume them, proving the consumer
         * survived the Phase-A blocking polls that exceeded the heartbeat
         * interval. */
        TEST_SAY(
            "Phase B: producing 10 records to verify consumer is still "
            "alive after long blocking polls\n");
        test_produce_msgs_simple(common_producer, topic, 0, 10);

        {
                int consumed = 0;
                int attempts = 0;
                while (consumed < 10 && attempts++ < 30) {
                        size_t rcvd = 0;
                        size_t j;
                        rd_kafka_error_t *err;

                        err = rd_kafka_share_consume_batch(
                            consumer, 1000, batch, &rcvd);
                        if (err) {
                                rd_kafka_error_destroy(err);
                                continue;
                        }
                        for (j = 0; j < rcvd; j++) {
                                if (!batch[j]->err)
                                        consumed++;
                                rd_kafka_message_destroy(batch[j]);
                        }
                }
                TEST_ASSERT(consumed == 10,
                            "Expected 10 records after the timeout matrix "
                            "(consumer should not have been fenced); got %d",
                            consumed);
                TEST_SAY("  consumer still alive: consumed=%d (OK)\n",
                         consumed);
        }

        /* Phase C — produce more, then poll with infinite timeout (-1).
         * Must return promptly with records, not hang. */
        TEST_SAY(
            "Phase C: producing 5 records, testing timeout_ms=-1 "
            "(infinite)\n");
        test_produce_msgs_simple(common_producer, topic, 0, 5);

        {
                size_t rcvd = 0;
                size_t k;
                rd_ts_t t_start, t_end;
                int actual_ms;
                rd_kafka_error_t *err;

                t_start = test_clock();
                err = rd_kafka_share_consume_batch(consumer, -1, batch, &rcvd);
                t_end   = test_clock();

                actual_ms = (int)((t_end - t_start) / 1000);

                TEST_ASSERT(
                    !err,
                    "timeout_ms=-1: unexpected error %s",
                    err ? rd_kafka_err2str(rd_kafka_error_code(err)) : "");
                TEST_ASSERT(
                    rcvd > 0,
                    "timeout_ms=-1 with available records returned 0");
                /* Generous upper bound; must not hang. */
                TEST_ASSERT(
                    actual_ms < 10000,
                    "timeout_ms=-1 took %dms with records available; "
                    "expected prompt return",
                    actual_ms);

                for (k = 0; k < rcvd; k++)
                        rd_kafka_message_destroy(batch[k]);

                TEST_SAY("  timeout_ms=-1 -> %dms, rcvd=%zu (OK)\n",
                         actual_ms, rcvd);
        }

        test_share_consumer_close(consumer);
        test_share_destroy(consumer);

        SUB_TEST_PASS();
}


int main_0188_share_consumer_consume_batch_timeout(int argc, char **argv) {
        common_producer = test_create_producer();

        /* Phase A sums to ~50 s of blocking on an empty topic, plus the
         * Phase B/C produce-and-consume overhead. Bump the test timeout
         * accordingly. */
        test_timeout_set(180);

        do_test_consume_batch_timeout_matrix();

        rd_kafka_destroy(common_producer);

        return 0;
}
