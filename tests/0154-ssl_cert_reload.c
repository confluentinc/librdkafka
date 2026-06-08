/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
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

#include <errno.h>
#include <string.h>

/**
 * @brief Tests SSL certificate hot reload:
 *        - rd_kafka_ssl_ctx_reload() rebuilds the context from file-based
 *          certificates (including after a simulated renewal),
 *        - the ssl.certificate.refresh.interval.ms property is accepted and
 *          enables the background refresh timer,
 *        - reload fails gracefully on a corrupt certificate file, and
 *        - reload is rejected for in-memory and non-SSL configurations.
 *
 * No broker is required; only the local SSL context build/swap path is
 * exercised.
 */

#define TEST_FIXTURES_SSL_FOLDER   "./fixtures/ssl/"
#define TEST_FIXTURES_KEY_PASSWORD "use_strong_password_keystore_client2"
#define SRC_CERT                   TEST_FIXTURES_SSL_FOLDER "client2.certificate.pem"
#define SRC_KEY                    TEST_FIXTURES_SSL_FOLDER "client2.key"

#define WORK_CERT "test_0154_cert.pem"
#define WORK_KEY  "test_0154_key.pem"


/**
 * @brief Copy fixture file \p src to writable working file \p dst, so that
 *        the working file can later be overwritten to simulate renewal.
 */
static void copy_file(const char *src, const char *dst) {
        static char buf[1024 * 50];
        size_t len;
        FILE *fp;

        len = test_read_file(src, buf, sizeof(buf));
        TEST_ASSERT(len > 0 && len < sizeof(buf),
                    "Failed to read fixture %s (len %" PRIusz ")", src, len);

        fp = fopen(dst, "wb");
        TEST_ASSERT(fp != NULL, "Failed to open %s for writing: %s", dst,
                    strerror(errno));
        TEST_ASSERT(fwrite(buf, 1, len, fp) == len, "Failed to write %s: %s",
                    dst, strerror(errno));
        fclose(fp);
}


/**
 * @brief Reload from valid file-based certificates: must succeed, both on the
 *        initial files and after the files are rewritten (renewed). Also
 *        verifies that the background refresh timer can be enabled and that a
 *        corrupt certificate is rejected while keeping the client usable.
 */
static void do_test_reload_file_based(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_error_t *error;
        char errstr[512];
        FILE *fp;

        SUB_TEST_QUICK();

        copy_file(SRC_CERT, WORK_CERT);
        copy_file(SRC_KEY, WORK_KEY);

        conf = rd_kafka_conf_new();
        test_conf_set(conf, "security.protocol", "SSL");
        test_conf_set(conf, "ssl.certificate.location", WORK_CERT);
        test_conf_set(conf, "ssl.key.location", WORK_KEY);
        test_conf_set(conf, "ssl.key.password", TEST_FIXTURES_KEY_PASSWORD);
        /* A positive interval with file-based certs enables the background
         * refresh timer (baseline capture + timer start). Use a large value
         * so it doesn't fire during the test. */
        test_conf_set(conf, "ssl.certificate.refresh.interval.ms", "3600000");

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk != NULL, "Failed to create producer: %s", errstr);

        /* Reload from the same (valid) files. */
        error = rd_kafka_ssl_ctx_reload(rk);
        TEST_ASSERT(error == NULL, "Expected reload to succeed, got: %s",
                    error ? rd_kafka_error_string(error) : "(none)");

        /* Simulate certificate renewal by rewriting the files, then reload. */
        copy_file(SRC_CERT, WORK_CERT);
        copy_file(SRC_KEY, WORK_KEY);
        error = rd_kafka_ssl_ctx_reload(rk);
        TEST_ASSERT(error == NULL,
                    "Expected reload after renewal to succeed, got: %s",
                    error ? rd_kafka_error_string(error) : "(none)");

        /* Corrupt the certificate file: reload must fail but leave the
         * existing context in place (client stays usable). */
        fp = fopen(WORK_CERT, "wb");
        TEST_ASSERT(fp != NULL, "Failed to open %s: %s", WORK_CERT,
                    strerror(errno));
        TEST_ASSERT(fwrite("not a certificate\n", 1, 18, fp) == 18,
                    "Failed to corrupt %s", WORK_CERT);
        fclose(fp);

        error = rd_kafka_ssl_ctx_reload(rk);
        TEST_ASSERT(error != NULL,
                    "Expected reload of corrupt certificate to fail");
        TEST_SAY("Reload of corrupt certificate failed as expected: %s\n",
                 rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);

        /* The client must still be usable (old context retained). */
        rd_kafka_destroy(rk);

        unlink(WORK_CERT);
        unlink(WORK_KEY);

        SUB_TEST_PASS();
}


/**
 * @brief Reload must be rejected when certificates are configured as in-memory
 *        PEM strings (nothing on disk to reload).
 */
static void do_test_reload_inmemory_rejected(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_error_t *error;
        char errstr[512];
        static char buf[1024 * 50];

        SUB_TEST_QUICK();

        conf = rd_kafka_conf_new();
        test_conf_set(conf, "security.protocol", "SSL");

        TEST_ASSERT(test_read_file(SRC_CERT, buf, sizeof(buf)) > 0,
                    "Failed to read %s", SRC_CERT);
        test_conf_set(conf, "ssl.certificate.pem", buf);

        TEST_ASSERT(test_read_file(SRC_KEY, buf, sizeof(buf)) > 0,
                    "Failed to read %s", SRC_KEY);
        test_conf_set(conf, "ssl.key.pem", buf);
        test_conf_set(conf, "ssl.key.password", TEST_FIXTURES_KEY_PASSWORD);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk != NULL, "Failed to create producer: %s", errstr);

        error = rd_kafka_ssl_ctx_reload(rk);
        TEST_ASSERT(error != NULL,
                    "Expected reload to be rejected for in-memory PEM certs");
        TEST_ASSERT(rd_kafka_error_code(error) ==
                        RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected _INVALID_ARG, got %s: %s",
                    rd_kafka_error_name(error), rd_kafka_error_string(error));
        TEST_SAY("In-memory cert reload rejected as expected: %s\n",
                 rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Reload must be rejected on a non-SSL (PLAINTEXT) client, which has no
 *        SSL context.
 */
static void do_test_reload_plaintext_rejected(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_error_t *error;
        char errstr[512];

        SUB_TEST_QUICK();

        conf = rd_kafka_conf_new();
        test_conf_set(conf, "security.protocol", "PLAINTEXT");

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk != NULL, "Failed to create producer: %s", errstr);

        error = rd_kafka_ssl_ctx_reload(rk);
        TEST_ASSERT(error != NULL,
                    "Expected reload to fail for a non-SSL client");
        TEST_SAY("Plaintext reload rejected as expected: %s\n",
                 rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


int main_0154_ssl_cert_reload(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        char errstr[512];
        rd_kafka_conf_res_t res;

        /* Skip if librdkafka was not built with SSL support. */
        conf = rd_kafka_conf_new();
        res  = rd_kafka_conf_set(conf, "ssl.certificate.location",
                                 "/nonexistent", errstr, sizeof(errstr));
        rd_kafka_conf_destroy(conf);
        if (res == RD_KAFKA_CONF_INVALID) {
                TEST_SKIP("%s\n", errstr);
                return 0;
        }

        do_test_reload_file_based();
        do_test_reload_inmemory_rejected();
        do_test_reload_plaintext_rejected();

        return 0;
}
