/* Broker-owned handshake deadline regression against a stalled TLS peer. */
#include "rdkafka.h"
#include <stdio.h>
#include <string.h>
static int timed_out;
static void logger(const rd_kafka_t *rk, int level, const char *facility,
                   const char *message) {
        (void)rk; (void)level; (void)facility;
        if (strstr(message, "Connection setup timed out in state SSL_HANDSHAKE"))
                timed_out = 1;
}
int main(int argc, char **argv) {
        rd_kafka_conf_t *c;
        rd_kafka_t *rk;
        char error[512];
        const struct rd_kafka_metadata *md = NULL;
        rd_kafka_resp_err_t result;
        const char *names[] = {"bootstrap.servers", "ssl.ca.location", "security.protocol",
                              "broker.address.family", "socket.connection.setup.timeout.ms"};
        const char *values[5];
        int i;
        if (argc != 3) return 2;
        values[0] = argv[1]; values[1] = argv[2]; values[2] = "ssl";
        values[3] = "v4"; values[4] = "1000";
        c = rd_kafka_conf_new();
        for (i = 0; i < 5; i++)
                if (rd_kafka_conf_set(c, names[i], values[i], error, sizeof(error)) != RD_KAFKA_CONF_OK)
                        return 2;
        rd_kafka_conf_set_log_cb(c, logger);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, c, error, sizeof(error));
        if (!rk) { fprintf(stderr, "%s\n", error); return 2; }
        result = rd_kafka_metadata(rk, 0, NULL, &md, 4000);
        if (!result) rd_kafka_metadata_destroy(md);
        rd_kafka_destroy(rk);
        if (!result || !timed_out) {
                fprintf(stderr, "Missing broker TLS handshake deadline failure\n");
                return 1;
        }
        puts("PASS: broker handshake deadline closes stalled TLS peer");
        return 0;
}
