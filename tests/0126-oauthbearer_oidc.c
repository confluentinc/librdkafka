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
 * @brief After config OIDC, make sure the producer and consumer
 *        can work successfully.
 *
 */
static void do_test_produce_consumer_with_OIDC(rd_kafka_conf_t *conf) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return;
        }

        const char *topic;
        uint64_t testid;
        rd_kafka_t *p1;
        rd_kafka_t *c1;
        int msgcounter;
        rd_kafka_topic_t *rkt;

        SUB_TEST("Test producer and consumer with oidc configuration");

        test_conf_set(conf, "sasl.oauthbearer.token.endpoint.url",
                      "https://dev-531534.okta.com/oauth2/default/v1/token");

        testid = test_id_generate();

        rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_dup(conf), test_dr_msg_cb);

        p1 = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));

        topic = test_mk_topic_name("0126-oauthbearer_oidc", 1);
        test_create_topic(p1, topic, 1, 3);
        TEST_SAY("Topic: %s is created\n", topic);

        rkt = test_create_producer_topic(p1, topic);
        TEST_SAY("Auto-creating topic %s\n", topic);
        test_auto_create_topic_rkt(p1, rkt, tmout_multip(5000));

        test_produce_msgs_nowait(p1, rkt, testid, 0, 0, 1, NULL, 0, 0,
                                 &msgcounter);
        TEST_SAY("Flushing..\n");
        rd_kafka_flush(p1, 1000);

        test_conf_set(conf, "auto.offset.reset", "earliest");
        c1 = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        test_consumer_subscribe(c1, topic);
        test_consumer_poll("OIDC.C1", c1, testid, 1, -1, 1, NULL);

        test_consumer_close(c1);

        rd_kafka_destroy(p1);
        rd_kafka_destroy(c1);
        SUB_TEST_PASS();
}


static void
auth_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
        if (err == RD_KAFKA_RESP_ERR__AUTHENTICATION ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN)
                TEST_SAY("Expected error: %s: %s\n", rd_kafka_err2str(err),
                         reason);
        else
                TEST_FAIL("Unexpected error: %s: %s", rd_kafka_err2str(err),
                          reason);
        rd_kafka_yield(rk);
}


/**
 * @brief After config OIDC, if the token is not valid, make sure the
 *        authentication fail as expected.
 *
 */
static void
do_test_produce_consumer_with_OIDC_should_fail(rd_kafka_conf_t *conf) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return;
        }

        rd_kafka_t *c1;
        uint64_t testid;

        SUB_TEST("Test OAUTHBEARER/OIDC failing with invalid JWT");

        test_conf_set(conf, "sasl.oauthbearer.token.endpoint.url",
                      "http://localhost:8080/retrieve/badformat");

        rd_kafka_conf_set_error_cb(conf, auth_error_cb);

        testid = test_id_generate();

        c1 = test_create_consumer("OIDC.fail.C1", NULL, rd_kafka_conf_dup(conf),
                                  NULL);

        rd_kafka_consumer_poll(c1, 10 * 1000);

        test_consumer_close(c1);
        rd_kafka_destroy(c1);
        SUB_TEST_PASS();
}


int main_0126_oauthbearer_oidc(int argc, char **argv) {
        rd_kafka_conf_res_t res;
        rd_kafka_conf_t *conf;
        char errstr[512];

        test_conf_init(&conf, NULL, 60);

        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.method", "OIDC", errstr,
                                sizeof(errstr));

        if (res == RD_KAFKA_CONF_INVALID) {
                rd_kafka_conf_destroy(conf);
                TEST_SKIP("%s\n", errstr);
                return 0;
        }

        if (res != RD_KAFKA_CONF_OK)
                TEST_FAIL("%s", errstr);

        test_conf_set(conf, "sasl.mechanisms", "OAUTHBEARER");
        test_conf_set(conf, "security.protocol", "SASL_PLAINTEXT");
        test_conf_set(conf, "sasl.oauthbearer.client.id", "clientid123");
        test_conf_set(conf, "sasl.oauthbearer.client.secret",
                      "clientsecret123");
        test_conf_set(
            conf, "sasl.oauthbearer.extensions",
            "ExtensionworkloadIdentity=develC348S,Extensioncluster=lkc123");
        test_conf_set(conf, "sasl.oauthbearer.scope", "test");

        do_test_produce_consumer_with_OIDC(rd_kafka_conf_dup(conf));
        do_test_produce_consumer_with_OIDC_should_fail(rd_kafka_conf_dup(conf));

        rd_kafka_conf_destroy(conf);
        return 0;
}
