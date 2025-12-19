/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Magnus Edenhill
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
 * @brief Tests that 3DES encrypted SSL keys/keystores are rejected when
 * FIPS mode is enabled.
 *
 * Uses keys from fixtures/ssl/fips_testing that are encrypted with 3DES,
 * which is not FIPS 140-3 compliant and should fail to load.
 */
static void do_test_3des_keys_fail(const char *type) {
#define TEST_FIPS_FIXTURES_FOLDER     "./fixtures/ssl/fips_testing/"
#define TEST_FIPS_KEYSTORE_PASSWORD   "use_strong_password_keystore_client"
#define TEST_FIPS_KEY_PASSWORD        "use_strong_password_keystore_client2"
#define TEST_FIPS_KEYSTORE_LOCATION   TEST_FIPS_FIXTURES_FOLDER "client.keystore.p12"
#define TEST_FIPS_CERTIFICATE_LOCATION                                         \
        TEST_FIPS_FIXTURES_FOLDER "client2.certificate.pem"
#define TEST_FIPS_KEY_LOCATION TEST_FIPS_FIXTURES_FOLDER "client2.key"

        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];

        SUB_TEST_QUICK("3DES keystore type = %s, expect failure in FIPS mode",
                       type);

        /* Don't use test_conf_init otherwise
         * key file configuration value conflicts
         * with PEM string configuration,
         * when running in --ssl mode. */
        conf = rd_kafka_conf_new();
        test_conf_set(conf, "security.protocol", "SSL");

        if (!strcmp(type, "PEM")) {
                test_conf_set(conf, "ssl.certificate.location",
                              TEST_FIPS_CERTIFICATE_LOCATION);
                test_conf_set(conf, "ssl.key.location",
                              TEST_FIPS_KEY_LOCATION);
                test_conf_set(conf, "ssl.key.password",
                              TEST_FIPS_KEY_PASSWORD);
        } else if (!strcmp(type, "PEM_STRING")) {
                char buf[1024 * 50];
                if (!test_read_file(TEST_FIPS_CERTIFICATE_LOCATION, buf,
                                    sizeof(buf)))
                        TEST_FAIL("Failed to read certificate file\n");
                test_conf_set(conf, "ssl.certificate.pem", buf);

                if (!test_read_file(TEST_FIPS_KEY_LOCATION, buf, sizeof(buf)))
                        TEST_FAIL("Failed to read key file\n");
                test_conf_set(conf, "ssl.key.pem", buf);

                test_conf_set(conf, "ssl.key.password",
                              TEST_FIPS_KEY_PASSWORD);
        } else {
                TEST_FAIL("Unexpected key type\n");
        }

        /* Attempt to create Kafka client - should FAIL due to 3DES in FIPS
         * mode */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (rk != NULL) {
                TEST_FAIL(
                    "Expected rd_kafka creation to fail with 3DES encrypted "
                    "keys in FIPS mode, but it succeeded\n");
                rd_kafka_destroy(rk);
        } else {
                TEST_SAY(
                    "rd_kafka_new() correctly failed with 3DES keys in FIPS "
                    "mode: %s\n",
                    errstr);
                /* Configuration is destroyed by rd_kafka_new on failure */
        }

        SUB_TEST_PASS();

#undef TEST_FIPS_KEYSTORE_PASSWORD
#undef TEST_FIPS_KEY_PASSWORD
#undef TEST_FIPS_KEYSTORE_LOCATION
#undef TEST_FIPS_CERTIFICATE_LOCATION
#undef TEST_FIPS_KEY_LOCATION
#undef TEST_FIPS_FIXTURES_FOLDER
}


int main_0154_ssl_keys_3des_fips(int argc, char **argv) {
        /* Test PEM and PEM string key format types with 3DES encryption
         * All should fail when FIPS 140-3 mode is enabled */
        do_test_3des_keys_fail("PEM");
        do_test_3des_keys_fail("PEM_STRING");

        return 0;
}