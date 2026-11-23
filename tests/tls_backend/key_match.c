/* rustls client certificate and private-key match at client creation.
 * Usage: key_match CA.pem client.crt client.key other.key [opaque]
 * A matching pair must succeed, also when the provider key hides its public
 * key (opaque, as Windows CNG keys do). A detected mismatch must fail; an
 * opaque key cannot detect it, so that case is skipped. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(int ok, const char *what) {
        if (!ok) {
                fprintf(stderr, "FAIL: %s\n", what);
                exit(1);
        }
}

static void set(rd_kafka_conf_t *conf, const char *name, const char *value) {
        char error[512];
        check(rd_kafka_conf_set(conf, name, value, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK,
              error);
}

static rd_kafka_t *
client(char **argv, const char *key, char *error, size_t size) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *rk;
        set(conf, "log_level", "0");
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", argv[1]);
        set(conf, "ssl.certificate.location", argv[2]);
        set(conf, "ssl.key.location", key);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, size);
        if (!rk)
                rd_kafka_conf_destroy(conf);
        return rk;
}

int main(int argc, char **argv) {
        char error[512];
        rd_kafka_t *rk;
        int opaque = argc == 6 && !strcmp(argv[5], "opaque");

        check(argc == 5 || opaque,
              "Usage: key_match CA.pem client.crt client.key other.key "
              "[opaque]");
        rk = client(argv, argv[3], error, sizeof(error));
        check(rk != NULL, error);
        rd_kafka_destroy(rk);
        if (!opaque) {
                rk = client(argv, argv[4], error, sizeof(error));
                check(rk == NULL, "mismatched private key was accepted");
                check(strstr(error, "(7600)") != NULL, error);
        }
        printf("PASS: certificate/key match%s\n",
               opaque ? " with an opaque provider key" : " and mismatch");
        return 0;
}
