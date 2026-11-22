/* Real Kafka acceptance client. No certificate contents are printed. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int delivered, delivery_errors;
static void dr(rd_kafka_t *rk, const rd_kafka_message_t *m, void *opaque) {
        (void)rk; (void)opaque;
        if (m->err) {
                fprintf(stderr, "delivery: %s\n", rd_kafka_err2str(m->err));
                delivery_errors++;
        } else delivered++;
}
static void set(rd_kafka_conf_t *c, const char *k, const char *v) {
        char e[512];
        if (rd_kafka_conf_set(c, k, v, e, sizeof(e)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "configuration %s: %s\n", k, e); exit(2);
        }
}
static rd_kafka_conf_t *config(const char *path, int consumer, int ack0) {
        rd_kafka_conf_t *c = rd_kafka_conf_new();
        FILE *f = fopen(path, "r"); char line[8192];
        if (!f) { perror("config"); exit(2); }
        while (fgets(line, sizeof(line), f)) {
                char *v;
                line[strcspn(line, "\r\n")] = 0;
                if (!line[0] || line[0] == '#') continue;
                v = strchr(line, '=');
                if (!v) { fprintf(stderr, "invalid config line\n"); exit(2); }
                *v++ = 0; set(c, line, v);
        }
        fclose(f);
        set(c, "socket.timeout.ms", "5000");
        set(c, "socket.connection.setup.timeout.ms", "5000");
        set(c, "socket.send.buffer.bytes", "1024");
        set(c, "socket.receive.buffer.bytes", "1024");
        set(c, "reconnect.backoff.ms", "100");
        set(c, "reconnect.backoff.max.ms", "500");
        if (consumer) {
                set(c, "group.id", "tls-backend-acceptance");
                set(c, "enable.auto.commit", "false");
                set(c, "auto.offset.reset", "earliest");
        } else {
                set(c, "message.timeout.ms", "30000");
                set(c, "acks", ack0 ? "0" : "all");
                if (!ack0) set(c, "enable.idempotence", "true");
                rd_kafka_conf_set_dr_msg_cb(c, dr);
        }
        return c;
}
static rd_kafka_t *client(const char *path, int consumer, int ack0) {
        char e[1024]; rd_kafka_conf_t *c = config(path, consumer, ack0);
        rd_kafka_t *rk = rd_kafka_new(consumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER,
                                     c, e, sizeof(e));
        if (!rk) { fprintf(stderr, "initialization: %s\n", e); rd_kafka_conf_destroy(c); exit(3); }
        return rk;
}
int main(int argc, char **argv) {
        rd_kafka_t *rk; const struct rd_kafka_metadata *md;
        rd_kafka_resp_err_t err; int i, count = 96, ack0;
        const size_t size = 65536; unsigned char *payload;
        const char *topic;
        if (argc < 3) return 2;
        ack0 = !strcmp(argv[1], "acks0");
        rk = client(argv[2], 0, ack0);
        err = rd_kafka_metadata(rk, 0, NULL, &md, 3500);
        if (err) {
                fprintf(stderr, "metadata: %s\n", rd_kafka_err2str(err));
                rd_kafka_destroy(rk); return 3;
        }
        if (md->broker_cnt < 1) return 4;
        rd_kafka_metadata_destroy(md);
        if (!strcmp(argv[1], "probe")) { rd_kafka_destroy(rk); puts("metadata OK"); return 0; }
        if (argc != 4) return 2;
        topic = argv[3]; payload = malloc(size);
        for (i = 0; i < count; i++) {
                char key[32];
                memset(payload, i, size); memcpy(payload, &i, sizeof(i));
                snprintf(key, sizeof(key), "key-%d", i);
                err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic),
                      RD_KAFKA_V_PARTITION(0), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                      RD_KAFKA_V_KEY(key, strlen(key)), RD_KAFKA_V_VALUE(payload, size),
                      RD_KAFKA_V_END);
                if (err) { fprintf(stderr, "produce: %s\n", rd_kafka_err2str(err)); return 4; }
        }
        free(payload);
        err = rd_kafka_flush(rk, 35000);
        if (err || delivered != count || delivery_errors) {
                fprintf(stderr, "flush: %s delivered=%d errors=%d\n", rd_kafka_err2str(err), delivered, delivery_errors);
                rd_kafka_destroy(rk); return 4;
        }
        rd_kafka_destroy(rk);
        rk = client(argv[2], 1, 0);
        rd_kafka_poll_set_consumer(rk);
        {
                rd_kafka_topic_partition_list_t *ps = rd_kafka_topic_partition_list_new(1);
                unsigned char seen[96] = {0}; int received = 0;
                time_t deadline = time(NULL) + 40;
                rd_kafka_topic_partition_list_add(ps, topic, 0)->offset = RD_KAFKA_OFFSET_BEGINNING;
                if (rd_kafka_assign(rk, ps)) return 5;
                rd_kafka_topic_partition_list_destroy(ps);
                while (received < count && time(NULL) < deadline) {
                        rd_kafka_message_t *m = rd_kafka_consumer_poll(rk, 500);
                        if (!m) continue;
                        if (!m->err) {
                                int n = -1; char key[32]; size_t j;
                                if (m->len == size) memcpy(&n, m->payload, sizeof(n));
                                if (n < 0 || n >= count || seen[n]) return 6;
                                snprintf(key, sizeof(key), "key-%d", n);
                                if (m->key_len != strlen(key) || memcmp(m->key, key, m->key_len)) return 6;
                                for (j = sizeof(n); j < size; j++)
                                        if (((unsigned char *)m->payload)[j] != (unsigned char)n) return 6;
                                seen[n] = 1; received++;
                        } else fprintf(stderr, "consume: %s\n", rd_kafka_message_errstr(m));
                        rd_kafka_message_destroy(m);
                }
                rd_kafka_consumer_close(rk); rd_kafka_destroy(rk);
                if (received != count) { fprintf(stderr, "count=%d expected=%d\n", received, count); return 6; }
        }
        printf("metadata/produce/consume OK: %d matching keys and 65536-byte values (%s)\n", count, ack0 ? "acks=0" : "acks=all");
        return 0;
}
