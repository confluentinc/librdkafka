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
#include "rdstring.h"

/**
 * @brief Tests that non-FIPS 140-3 compliant cipher suites are rejected when
 * FIPS mode is enabled.
 *
 * Attempts to configure cipher suites that are not FIPS 140-3 compliant
 * and expects the instantiation to fail
 */
static void do_test_nonfips_cipher_fail(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_topic_conf_t *topic_conf;
        rd_kafka_t *rk;
        char errstr[512];

        SUB_TEST_QUICK(
            "Non-FIPS cipher suite (CHACHA20-POLY1305), expect failure in FIPS mode");

        test_conf_init(&conf, &topic_conf, 20);
        test_conf_set(conf, "ssl.cipher.suites",
                      "ECDHE-RSA-CHACHA20-POLY1305");
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                TEST_SAY("Failed to create rdkafka instance [expected in FIPS mode]: %s\n", errstr);
        else {
                TEST_FAIL(
                    "Expected rd_kafka creation to fail with CHACHA20-POLY1305 cipher "
                    "but it succeeded\n");
                rd_kafka_destroy(rk);
        }
        SUB_TEST_PASS();
}


int main_0155_ssl_cipher_fips(int argc, char **argv) {
        /* Test that non-FIPS compliant cipher suites are rejected
         * when FIPS mode is enabled */
        do_test_nonfips_cipher_fail();
        return 0;
}