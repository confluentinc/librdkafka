/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2021, Magnus Edenhill
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
/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */


/**
 * @brief After config OIDC fields, make sure the producer gets created
 *        successfully.
 *
 */
static void do_test_create_producer() {
        const char *topic;
        uint64_t testid;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_conf_res_t res;
        char errstr[512];

        SUB_TEST("Test producer with oidc configuration");

        test_conf_init(&conf, NULL, 60);

        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.method", "oidc", errstr,
                                sizeof(errstr));

        if (res == RD_KAFKA_CONF_INVALID) {
                rd_kafka_conf_destroy(conf);
                TEST_SKIP("%s\n", errstr);
                return;
        }

        if (res != RD_KAFKA_CONF_OK)
                TEST_FAIL("%s", errstr);

        test_conf_set(conf, "sasl.oauthbearer.client.id", "randomuniqclientid");
        test_conf_set(conf, "sasl.oauthbearer.client.secret",
                      "randomuniqclientsecret");
        test_conf_set(conf, "sasl.oauthbearer.client.secret",
                      "randomuniqclientsecret");
        test_conf_set(conf, "sasl.oauthbearer.extensions",
                      "supportFeatureX=true");
        test_conf_set(conf, "sasl.oauthbearer.token.endpoint.url",
                      "https://localhost:1/token");

        testid = test_id_generate();
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        topic = test_mk_topic_name("0126-oauthbearer_oidc", 1);
        test_create_topic(rk, topic, 1, 1);

        /* Produce messages */
        test_produce_msgs2(rk, topic, testid, 1, 0, 0, NULL, 0);

        /* Verify messages were actually produced by consuming them back. */
        test_consume_msgs_easy(topic, topic, 0, 1, 1, NULL);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0126_oauthbearer_oidc(int argc, char **argv) {
        do_test_create_producer();
        return 0;
}
