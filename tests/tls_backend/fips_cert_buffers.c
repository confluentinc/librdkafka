/* Positive rustls FIPS key-import regression; an actual FIPS artifact is required.
 * argv: CA.pem client.pem plaintext-client.key. This tests runtime checks only. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(int ok, const char *why) {
        if (!ok) { fprintf(stderr, "%s\n", why); exit(1); }
}
static void set(rd_kafka_conf_t *c, const char *key, const char *value) {
        char error[512];
        check(rd_kafka_conf_set(c, key, value, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK, error);
}
static unsigned char *read_file(const char *path, size_t *size) {
        FILE *f = fopen(path, "rb");
        long n;
        unsigned char *p;
        check(f && !fseek(f, 0, SEEK_END), "open fixture");
        n = ftell(f);
        check(n > 0 && n < 1024 * 1024 && !fseek(f, 0, SEEK_SET), "fixture length");
        p = malloc((size_t)n);
        check(p && fread(p, 1, (size_t)n, f) == (size_t)n, "read fixture");
        fclose(f); *size = (size_t)n; return p;
}
int main(int argc, char **argv) {
        rd_kafka_conf_t *conf, *copy;
        rd_kafka_t *rk;
        unsigned char *key;
        volatile unsigned char *wipe;
        size_t size, i;
        char error[1024];
        check(argc == 4, "Usage: fips_cert_buffers CA.pem client.pem client.key");
        key = read_file(argv[3], &size);
        conf = rd_kafka_conf_new();
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", argv[1]);
        set(conf, "ssl.certificate.location", argv[2]);
        set(conf, "ssl.fips.mode", "required");
        check(rd_kafka_conf_set_ssl_cert(conf, RD_KAFKA_CERT_PRIVATE_KEY,
                  RD_KAFKA_CERT_ENC_PEM, key, size, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK, error);
        copy = rd_kafka_conf_dup(conf);
        check(rd_kafka_conf_set_ssl_cert(conf, RD_KAFKA_CERT_PRIVATE_KEY,
                  RD_KAFKA_CERT_ENC_PEM, NULL, 0, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK, error);
        rd_kafka_conf_destroy(conf);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, copy, error, sizeof(error));
        check(rk != NULL, error);
        rd_kafka_destroy(rk);
        /* A buffer imported before the strict policy is selected is rejected
         * conservatively, even when this FFI's ordinary provider is also FIPS. */
        conf = rd_kafka_conf_new();
        check(rd_kafka_conf_set_ssl_cert(conf, RD_KAFKA_CERT_PRIVATE_KEY,
                  RD_KAFKA_CERT_ENC_PEM, key, size, error, sizeof(error)) ==
                  RD_KAFKA_CONF_OK, error);
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", argv[1]);
        set(conf, "ssl.certificate.location", argv[2]);
        set(conf, "ssl.fips.mode", "required");
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        check(rk == NULL && strstr(error, "outside the selected strict provider context"), error);
        rd_kafka_conf_destroy(conf);
        wipe = key;
        for (i = 0; i < size; i++) wipe[i] = 0;
        free(key);
        puts("PASS: strict rustls key import, duplicate lifetime and provenance rejection");
        return 0;
}
