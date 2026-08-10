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
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
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
 * @name Verify that librdkafka can complete SASL authentication now that
 *       the SaslAuthenticate ceiling has been raised to v2 (the first
 *       KIP-482 flexible version), which brokers/proxies that enforce a
 *       KIP-482 floor require.
 *
 * The mock broker does not implement SASL, so this test requires a real,
 * SASL-configured broker (as already assumed by the sibling
 * 0135-sasl_credentials.cpp test) and is skipped when one isn't
 * configured, exactly like that test.
 */
static void do_test_sasl_authenticate(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        char username[128], password[128];
        size_t username_sz = sizeof(username);
        size_t password_sz = sizeof(password);
        const struct rd_kafka_metadata *md;
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK();

        test_conf_init(&conf, NULL, 30);

        if (rd_kafka_conf_get(conf, "sasl.username", username,
                              &username_sz) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_get(conf, "sasl.password", password,
                              &password_sz) != RD_KAFKA_CONF_OK ||
            username_sz <= 1 || password_sz <= 1) {
                rd_kafka_conf_destroy(conf);
                SUB_TEST_SKIP(
                    "sasl.username and/or sasl.password not configured\n");
                return;
        }

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(10000));
        TEST_ASSERT(!err, "metadata() failed: %s", rd_kafka_err2str(err));
        rd_kafka_metadata_destroy(md);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}

int main_0192_sasl_authenticate_flexver(int argc, char **argv) {
        do_test_sasl_authenticate();
        return 0;
}
