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
 * @name Consuming legacy (MsgVersion v0..v1) MessageSets with flexible
 *       Fetch versions (v12+), using the mock broker.
 *
 * Brokers with log.message.format.version < 0.11 (e.g. during a rolling
 * upgrade from an old cluster) store and return MessageSets in the legacy
 * v0/v1 format, regardless of the Fetch version used by the consumer.
 * The legacy MessageSet payload always uses non-compact (fixed-width)
 * encodings, even when the enclosing FetchResponse is a flexible version.
 *
 * This is a regression test: messages were returned to the application
 * with NULL key and value, because the Key and Value fields were parsed
 * as compact bytes (varint length) when the FetchResponse was a flexible
 * version.
 */


/**
 * @brief Produce and consume with the producer's ProduceRequest capped at
 *        \p max_produce_version, so that it writes legacy MessageSets:
 *        v2 => MsgVersion 1, v1 => MsgVersion 0.
 */
static void do_test_legacy_msgset_fetch(int16_t max_produce_version) {
        const char *topic = "legacy_msgset";
        const int msgcnt  = 100;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        uint64_t testid = test_id_generate();
        test_msgver_t mv;

        SUB_TEST_QUICK("max ProduceRequest v%" PRId16, max_produce_version);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* Cap the ProduceRequest version so the producer selects a legacy
         * MsgVersion (the MSGVER1 feature requires Produce >= v2 and
         * MSGVER2 requires Produce >= v3), while Fetch stays at a
         * flexible version (v12+). */
        TEST_CALL_ERR__(rd_kafka_mock_set_apiversion(mcluster, 0 /*Produce*/, 0,
                                                     max_produce_version));

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, testid, 0, 0, msgcnt, 100,
                                 "bootstrap.servers", bootstraps, NULL);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");

        c = test_create_consumer("legacy_msgset_group", NULL, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Verify that all messages are consumed with intact payloads:
         * with the bug the messages arrive with NULL key and value. */
        test_msgver_init(&mv, testid);
        test_consumer_poll("consume.legacy", c, testid, -1, 0, msgcnt, &mv);
        test_msgver_verify("verify.legacy", &mv, TEST_MSGVER_ALL, 0, msgcnt);
        test_msgver_clear(&mv);

        test_consumer_close(c);
        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0187_legacy_msgset_fetch_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        /* MsgVersion v1 */
        do_test_legacy_msgset_fetch(2);

        /* MsgVersion v0 */
        do_test_legacy_msgset_fetch(1);

        return 0;
}
