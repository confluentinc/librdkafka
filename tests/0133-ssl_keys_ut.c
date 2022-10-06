/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022, Magnus Edenhill
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
#include "rdstring.h"

#define SSL_FIXTURES "/ssl/"
#define KEYSTORE_LOCATION                                                      \
        TEST_FIXTURES_FOLDER SSL_FIXTURES "client.keystore.p12"
#define CERTIFICATE_LOCATION                                                   \
        TEST_FIXTURES_FOLDER SSL_FIXTURES "client.certificate.pem"
#define KEY_LOCATION TEST_FIXTURES_FOLDER SSL_FIXTURES "client.key"

static void do_test_ssl_keys(const char *type, rd_bool_t correct_password) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[256];

        SUB_TEST_QUICK("keystore type = %s, correct password = %s", type,
                       RD_STR_ToF(correct_password));

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "security.protocol", "SSL");
        if (!strcmp(type, "PKCS12")) {
                test_conf_set(conf, "ssl.keystore.location", KEYSTORE_LOCATION);
                if (correct_password)
                        test_conf_set(conf, "ssl.keystore.password",
                                      TEST_FIXTURES_KEYSTORE_PASSWORD);
                else
                        test_conf_set(conf, "ssl.keystore.password",
                                      TEST_FIXTURES_KEYSTORE_PASSWORD
                                      " and more");
        } else if (!strcmp(type, "PEM")) {
                test_conf_set(conf, "ssl.certificate.location",
                              CERTIFICATE_LOCATION);
                test_conf_set(conf, "ssl.key.location", KEY_LOCATION);
                if (correct_password)
                        test_conf_set(conf, "ssl.key.password",
                                      TEST_FIXTURES_KEY_PASSWORD);
                else
                        test_conf_set(conf, "ssl.keystore.password",
                                      TEST_FIXTURES_KEYSTORE_PASSWORD
                                      " and more");
        } else {
                TEST_FAIL("Unexpected key type\n");
        }

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if ((rk != NULL) != correct_password) {
                TEST_FAIL("Expected rd_kafka creation to %s\n",
                          correct_password ? "succeed" : "fail");
        }

        if (rk)
                rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0133_ssl_keys_ut(int argc, char **argv) {
        do_test_ssl_keys("PKCS12", rd_true);
        do_test_ssl_keys("PKCS12", rd_false);
        do_test_ssl_keys("PEM", rd_true);
        do_test_ssl_keys("PEM", rd_false);
        return 0;
}
