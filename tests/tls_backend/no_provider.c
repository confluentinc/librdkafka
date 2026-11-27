/* rustls build without a usable provider (none installed, or an optional
 * REASON such as an empty provider): TLS client creation and private-key
 * import must fail clearly, with no fallback. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define REASON "no process-default rustls crypto provider is installed"

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

int main(int argc, char **argv) {
        char error[512], key[16384];
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        FILE *fp;
        size_t size;
        const char *reason;

        check(argc == 3 || argc == 4,
              "Usage: no_provider CA.pem client.key [REASON]");
        reason = argc == 4 ? argv[3] : REASON;
        conf   = rd_kafka_conf_new();
        set(conf, "log_level", "0");
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        check(rk != NULL, error);
        rd_kafka_destroy(rk);

        conf = rd_kafka_conf_new();
        set(conf, "log_level", "0");
        set(conf, "security.protocol", "ssl");
        set(conf, "ssl.ca.location", argv[1]);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        check(rk == NULL, "TLS client initialized without a provider");
        check(strstr(error, reason) != NULL, error);
        rd_kafka_conf_destroy(conf);

        fp = fopen(argv[2], "rb");
        check(fp != NULL, "open private key");
        size = fread(key, 1, sizeof(key), fp);
        fclose(fp);
        check(size > 0 && size < sizeof(key), "private key size");
        conf = rd_kafka_conf_new();
        check(rd_kafka_conf_set_ssl_cert(
                  conf, RD_KAFKA_CERT_PRIVATE_KEY, RD_KAFKA_CERT_ENC_PEM, key,
                  size, error, sizeof(error)) == RD_KAFKA_CONF_INVALID,
              "private key imported without a provider");
        check(strstr(error, reason) != NULL, error);
        rd_kafka_conf_destroy(conf);
        memset(key, 0, sizeof(key));
        printf("PASS: TLS client and key import refused: %s\n", reason);
        return 0;
}
