/* Standalone policy/certificate lifetime regression test: no broker required.
 * Link against the library under test. argv[1] is a PEM test CA; argv[2] is
 * "unavailable" (default) or "available" for an installed FIPS provider.
 * Optional argv[3] is a PEM private key for import-provenance regression.
 * This test does not assert CMVP qualification. */
#include "../src/rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(int condition, const char *message) {
        if (!condition) {
                fprintf(stderr, "FAIL: %s\n", message);
                exit(1);
        }
}

static void set(rd_kafka_conf_t *conf, const char *name, const char *value) {
        char error[512];
        check(rd_kafka_conf_set(conf, name, value, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK,
              name);
}

static rd_kafka_conf_t *strict(const char *ca) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        set(conf, "ssl.fips.mode", "required");
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", ca);
        if (getenv("RDKAFKA_TEST_FIPS_CONFIG"))
                set(conf, "ssl.fips.config", getenv("RDKAFKA_TEST_FIPS_CONFIG"));
        return conf;
}

static void refused(rd_kafka_conf_t *conf, const char *reason) {
        char error[1024];
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error,
                                      sizeof(error));
        check(!rk, "strict client unexpectedly initialized");
        check(strstr(error, reason) != NULL, error);
        rd_kafka_conf_destroy(conf); /* retained by caller on failure */
}

static void ordinary(void) {
        char error[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *rk;
        set(conf, "log_level", "0");
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        check(rk != NULL, error);
        rd_kafka_destroy(rk);
}

static void ordinary_tls(const char *ca) {
        char error[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *rk;
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", ca);
        set(conf, "log_level", "0");
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        check(rk != NULL, error);
        rd_kafka_destroy(rk);
}

static void admin_upsert_refused(rd_kafka_t *rk) {
        rd_kafka_queue_t *queue = rd_kafka_queue_new(rk);
        const unsigned char password[] = "test-password";
        const unsigned char salt[] = "explicit-test-salt-avoids-constructor-rng";
        rd_kafka_UserScramCredentialAlteration_t *alteration =
            rd_kafka_UserScramCredentialUpsertion_new(
                "policy-test-user", RD_KAFKA_SCRAM_MECHANISM_SHA_256, 4096,
                password, sizeof(password) - 1, salt, sizeof(salt) - 1);
        rd_kafka_event_t *event;
        rd_kafka_AlterUserScramCredentials(rk, &alteration, 1, NULL, queue);
        rd_kafka_UserScramCredentialAlteration_destroy(alteration);
        event = rd_kafka_queue_poll(queue, 3000);
        check(event != NULL, "strict Admin rejection must not wait for broker");
        check(rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR__INVALID_ARG,
              "strict Admin upsert error code");
        check(strstr(rd_kafka_event_error_string(event),
                     "ssl.fips.mode=required") != NULL,
              "strict Admin rejection diagnostic");
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
}

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char error[1024];
        char features[512];
        size_t feature_size = sizeof(features);
        FILE *file;
        char *pem;
        long length;
        int i;
        int available = argc > 2 && !strcmp(argv[2], "available");

        check(argc >= 2, "usage: tls_policy CA.pem [unavailable|available]");
        ordinary();
        conf = rd_kafka_conf_new();
        check(rd_kafka_conf_get(conf, "builtin.features", features,
                                &feature_size) == RD_KAFKA_CONF_OK,
              "builtin.features");
        set(conf, "ssl.fips.mode", "required");
        refused(conf, "ssl.fips.mode=required");

        if (!strstr(features, "ssl")) {
                puts("PASS: no-TLS required policy refuses initialization");
                return 0;
        }

        ordinary_tls(argv[1]);
        if (argc > 3) {
                FILE *key_file = fopen(argv[3], "rb");
                long key_length;
                unsigned char *key;
                volatile unsigned char *wipe;
                size_t j;
                check(key_file != NULL, "open test private key");
                check(fseek(key_file, 0, SEEK_END) == 0, "seek private key");
                key_length = ftell(key_file);
                check(key_length > 0 && key_length < 1024 * 1024,
                      "test private key size");
                rewind(key_file);
                key = malloc((size_t)key_length);
                check(key != NULL, "allocate private key buffer");
                check(fread(key, 1, (size_t)key_length, key_file) ==
                          (size_t)key_length, "read private key");
                fclose(key_file);
                conf = strict(argv[1]);
                {
                        rd_kafka_conf_t *probe = rd_kafka_conf_new();
                        char ignored[512];
                        int openssl = rd_kafka_conf_set(
                            probe, "ssl.providers", "default", ignored,
                            sizeof(ignored)) == RD_KAFKA_CONF_OK;
                        rd_kafka_conf_res_t result;
                        rd_kafka_conf_destroy(probe);
                        result = rd_kafka_conf_set_ssl_cert(
                            conf, RD_KAFKA_CERT_PRIVATE_KEY,
                            RD_KAFKA_CERT_ENC_PEM, key, (size_t)key_length,
                            error, sizeof(error));
                        if (openssl || !available)
                                check(result == RD_KAFKA_CONF_INVALID,
                                      "strict private-key setter must refuse "
                                      "unqualified parsing/provider");
                        else
                                check(result == RD_KAFKA_CONF_OK, error);
                }
                rd_kafka_conf_destroy(conf);
                conf = rd_kafka_conf_new();
                check(rd_kafka_conf_set_ssl_cert(
                          conf, RD_KAFKA_CERT_PRIVATE_KEY,
                          RD_KAFKA_CERT_ENC_PEM, key, (size_t)key_length,
                          error, sizeof(error)) == RD_KAFKA_CONF_OK, error);
                wipe = key;
                for (j = 0; j < (size_t)key_length; j++)
                        wipe[j] = 0;
                free(key);
                set(conf, "security.protocol", "ssl");
                set(conf, "ssl.ca.location", argv[1]);
                set(conf, "ssl.fips.mode", "required");
                refused(conf, "outside the selected strict provider context");
        }
        conf = strict(argv[1]);
        check(rd_kafka_conf_set_ssl_cert(
                  conf, RD_KAFKA_CERT_PRIVATE_KEY, RD_KAFKA_CERT_ENC_PEM,
                  "-----BEGIN ENCRYPTED PRIVATE KEY-----", 37,
                  error, sizeof(error)) == RD_KAFKA_CONF_INVALID,
              "strict setter must reject encrypted buffer before decryption");
        check(strstr(error, "rejects encrypted PEM") != NULL, error);
        set(conf, "enable.ssl.certificate.verification", "false");
        refused(conf, "certificate and hostname verification");
        conf = strict(argv[1]);
        set(conf, "ssl.endpoint.identification.algorithm", "none");
        refused(conf, "certificate and hostname verification");
        conf = strict(argv[1]);
        set(conf, "security.protocol", "sasl_ssl");
        refused(conf, "supports only security.protocol=ssl");
        conf = strict(argv[1]);
        set(conf, "ssl.cipher.suites", "AES128-SHA");
        refused(conf, "fixed TLS algorithm profile");
        conf = strict(argv[1]);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        if (available) {
                check(rk != NULL, error);
                admin_upsert_refused(rk);
                ordinary_tls(argv[1]); /* concurrently isolated policy */
                rd_kafka_destroy(rk);
        } else {
                check(rk == NULL, "FIPS provider unexpectedly available");
                check(strstr(error, "FIPS") || strstr(error, "fips"), error);
                rd_kafka_conf_destroy(conf);
        }
        ordinary_tls(argv[1]); /* strict init must not poison default crypto */

        file = fopen(argv[1], "rb");
        check(file != NULL, "open test CA");
        check(fseek(file, 0, SEEK_END) == 0, "seek test CA");
        length = ftell(file);
        check(length > 0 && length < 1024 * 1024, "test CA size");
        rewind(file);
        pem = malloc((size_t)length);
        check(pem != NULL, "allocate test CA buffer");
        check(fread(pem, 1, (size_t)length, file) == (size_t)length,
              "read test CA");
        fclose(file);
        conf = rd_kafka_conf_new();
        set(conf, "security.protocol", "ssl");
        set(conf, "log_level", "0");
        check(rd_kafka_conf_set_ssl_cert(
                  conf, RD_KAFKA_CERT_CA, RD_KAFKA_CERT_ENC_PEM,
                  pem, (size_t)length, error, sizeof(error)) == RD_KAFKA_CONF_OK,
              error);
        memset(pem, 0, (size_t)length);
        free(pem); /* setter must own/validate before returning */
        check(rd_kafka_conf_set_ssl_cert(
                  conf, RD_KAFKA_CERT_CA, RD_KAFKA_CERT_ENC_PEM,
                  "invalid", 7, error, sizeof(error)) == RD_KAFKA_CONF_INVALID,
              "malformed replacement must fail immediately");
        for (i = 0; i < 3; i++) {
                rd_kafka_conf_t *copy = rd_kafka_conf_dup(conf);
                rk = rd_kafka_new(RD_KAFKA_PRODUCER, copy, error,
                                  sizeof(error));
                check(rk != NULL, error);
                rd_kafka_destroy(rk);
        }
        check(rd_kafka_conf_set_ssl_cert(
                  conf, RD_KAFKA_CERT_CA, RD_KAFKA_CERT_ENC_PEM,
                  NULL, 0, error, sizeof(error)) == RD_KAFKA_CONF_OK,
              "certificate clear");
        rd_kafka_conf_destroy(conf);
        puts("PASS: strict policy, isolation and certificate ownership/duplication");
        return 0;
}
