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
 * @name Request-level NOT_COORDINATOR on OffsetFetch retries (issue #5586).
 *
 * When a consumer assign()s a partition without an explicit offset, it must
 * first fetch the group's committed offset (an OffsetFetch request) before the
 * fetcher can start. If the coordinator it asks has just restarted, a real
 * broker replies with a REQUEST-LEVEL NOT_COORDINATOR error and an EMPTY
 * partition list.
 *
 * Before the fix, librdkafka's error action for NOT_COORDINATOR on OffsetFetch
 * was REFRESH only (no RETRY): the consumer re-discovered the coordinator but
 * never re-sent the OffsetFetch. Because the response carried no partitions,
 * the assignment code never moved the partition off the .queried list, and a
 * non-empty .queried list then blocked every future offset query. The
 * partition was wedged forever -- the fetcher never started, no records were
 * delivered, and only a consumer restart recovered it.
 *
 * The fix adds RETRY (alongside REFRESH) for the coordinator errors on
 * OffsetFetch, so the request is re-sent to the coordinator once it is back.
 *
 * This test injects ONE request-level NOT_COORDINATOR on the first OffsetFetch
 * (a single transient coordinator blip) and asserts the FIXED behaviour: the
 * consumer retries, gets its committed offset, starts the fetcher, and delivers
 * all produced records.
 */
static void do_test_offsetfetch_not_coordinator_retries(void) {
        const char *topic = "offsetfetch_not_coord_topic";
        const int msgcnt  = 5;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        uint64_t testid   = test_id_generate();
        int records_seen  = 0;
        int notcoord_seen = 0;
        int i;

        SUB_TEST();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* Produce msgcnt messages -> offsets 0..msgcnt-1. */
        test_produce_msgs_easy_v(topic, testid, 0, 0, msgcnt, 16,
                                 "bootstrap.servers", bootstraps, NULL);

        /* Fault injection: make the FIRST OffsetFetch (ApiKey 9) fail with a
         * request-level NOT_COORDINATOR. The mock then behaves like a real
         * non-coordinator broker: top-level error + EMPTY partition list.
         * Exactly one error is pushed, so the retried OffsetFetch succeeds. */
        rd_kafka_mock_push_request_errors(mcluster, 9 /*OffsetFetch*/, 1,
                                          RD_KAFKA_RESP_ERR_NOT_COORDINATOR);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", "offsetfetch-not-coord-grp");
        test_conf_set(conf, "enable.auto.commit", "true");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        c = test_create_consumer("offsetfetch-not-coord-grp", NULL, conf, NULL);

        /* Static assign with STORED offset forces the committed-offset
         * (OffsetFetch) query before the fetcher can start. */
        test_consumer_assign_partition("offsetfetch-not-coord", c, topic, 0,
                                       RD_KAFKA_OFFSET_STORED);

        /* The consumer should retry the OffsetFetch after the transient
         * NOT_COORDINATOR and then deliver all msgcnt records. */
        for (i = 0; i < 150 && records_seen < msgcnt; i++) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 200);
                if (!rkm)
                        continue;

                if (!rkm->err) {
                        records_seen++;
                        TEST_SAY("Delivered record at offset %" PRId64
                                 " (poll %d)\n",
                                 rkm->offset, i);
                } else if (rkm->err == RD_KAFKA_RESP_ERR_NOT_COORDINATOR) {
                        notcoord_seen++;
                        TEST_SAY("Transient consumer error (poll %d): %s\n", i,
                                 rd_kafka_message_errstr(rkm));
                } else {
                        TEST_SAY("Other event (poll %d): %s\n", i,
                                 rd_kafka_message_errstr(rkm));
                }
                rd_kafka_message_destroy(rkm);
        }

        TEST_SAY(
            "After %d poll(s): %d record(s) delivered, "
            "%d transient NOT_COORDINATOR error(s) surfaced\n",
            i, records_seen, notcoord_seen);

        /* The fix: despite the transient request-level NOT_COORDINATOR, the
         * OffsetFetch is retried, the partition leaves .queried, the fetcher
         * starts, and every record is delivered. */
        TEST_ASSERT(
            records_seen == msgcnt,
            "Issue #5586 fix: expected all %d records delivered after a "
            "transient NOT_COORDINATOR, but got %d (partition wedged in "
            ".queried => fix not effective)",
            msgcnt, records_seen);

        TEST_SAY(
            "Consumer recovered from the transient NOT_COORDINATOR and "
            "delivered all %d records\n",
            msgcnt);

        test_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0192_offsetfetch_not_coord_mock(int argc, char **argv) {
        do_test_offsetfetch_not_coordinator_retries();
        return 0;
}
