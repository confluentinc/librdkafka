/*
 * rdkafka_benchmark - librdkafka benchmark tool with structured JSON output
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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

/**
 * @brief Kafka producer/consumer benchmark using local librdkafka build.
 *
 * Designed to be orchestrated by tools/benchmark/ Python framework.
 *
 * STDOUT protocol:
 *   Consumer mode: prints "BENCHMARK_READY\n" once partition assignment fires,
 *                  then prints a JSON result object when done.
 *   Producer mode: prints a single JSON result object when done.
 *
 * All diagnostic/progress output goes to STDERR so it does not
 * interfere with JSON parsing from STDOUT.
 *
 * Build:
 *   cd <repo_root> && ./configure && make
 *   (rdkafka_benchmark is built as part of examples/)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <inttypes.h>

#ifndef _WIN32
#include <sys/time.h>
#include <unistd.h>
#else
#include "../win32/wingetopt.h"
#include <windows.h>
#endif

#include "rdkafka.h"

/* -----------------------------------------------------------------------
 * Globals */

static volatile sig_atomic_t g_run          = 1;
static volatile sig_atomic_t g_consumer_rdy = 0;

static void stop(int sig) {
        g_run = 0;
}

/* -----------------------------------------------------------------------
 * Wall-clock helpers (microseconds) */

static int64_t wall_clock_us(void) {
#ifdef _WIN32
        FILETIME ft;
        GetSystemTimeAsFileTime(&ft);
        return ((int64_t)ft.dwHighDateTime << 32 | ft.dwLowDateTime) / 10 -
               11644473600000000LL;
#else
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (int64_t)tv.tv_sec * 1000000LL + (int64_t)tv.tv_usec;
#endif
}

static double wall_clock_sec(void) {
        return (double)wall_clock_us() / 1e6;
}

/* -----------------------------------------------------------------------
 * Latency sample array
 *
 * Stores individual per-message latency values (in microseconds).
 * After collection, call lat_sort() then lat_percentile() for percentiles.
 */

#define LAT_CAP_MAX 10000000 /* max 10M samples (~80 MB) */

typedef struct {
        int64_t *data;
        size_t   cnt;
        size_t   cap;
} lat_t;

static void lat_init(lat_t *l, size_t initial_cap) {
        if (initial_cap < 64)
                initial_cap = 64;
        if (initial_cap > LAT_CAP_MAX)
                initial_cap = LAT_CAP_MAX;
        l->data = (int64_t *)malloc(sizeof(int64_t) * initial_cap);
        l->cnt  = 0;
        l->cap  = initial_cap;
}

static void lat_add(lat_t *l, int64_t us) {
        if (l->cnt >= LAT_CAP_MAX)
                return; /* silently cap */
        if (l->cnt >= l->cap) {
                size_t new_cap = l->cap * 2;
                if (new_cap > LAT_CAP_MAX)
                        new_cap = LAT_CAP_MAX;
                int64_t *p =
                    (int64_t *)realloc(l->data, sizeof(int64_t) * new_cap);
                if (!p)
                        return;
                l->data = p;
                l->cap  = new_cap;
        }
        l->data[l->cnt++] = us;
}

static int cmp_i64(const void *a, const void *b) {
        int64_t va = *(const int64_t *)a;
        int64_t vb = *(const int64_t *)b;
        return (va > vb) - (va < vb);
}

static void lat_sort(lat_t *l) {
        if (l->cnt > 1)
                qsort(l->data, l->cnt, sizeof(int64_t), cmp_i64);
}

/* p in [0, 100] */
static double lat_pct(const lat_t *l, double p) {
        if (l->cnt == 0)
                return 0.0;
        size_t idx = (size_t)(p / 100.0 * (double)(l->cnt - 1));
        if (idx >= l->cnt)
                idx = l->cnt - 1;
        return (double)l->data[idx];
}

static double lat_avg(const lat_t *l) {
        if (l->cnt == 0)
                return 0.0;
        int64_t sum = 0;
        for (size_t i = 0; i < l->cnt; i++)
                sum += l->data[i];
        return (double)sum / (double)l->cnt;
}

static double lat_min(const lat_t *l) {
        return l->cnt ? (double)l->data[0] : 0.0;
}

static double lat_max(const lat_t *l) {
        return l->cnt ? (double)l->data[l->cnt - 1] : 0.0;
}

static void lat_free(lat_t *l) {
        free(l->data);
        l->data = NULL;
        l->cnt = l->cap = 0;
}

/* -----------------------------------------------------------------------
 * Config helpers */

static void set_conf(rd_kafka_conf_t *conf,
                     const char *key,
                     const char *val,
                     const char *ctx) {
        char errstr[512];
        if (rd_kafka_conf_set(conf, key, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s: %s=%s: %s\n", ctx, key, val, errstr);
                exit(1);
        }
}

/* -----------------------------------------------------------------------
 * JSON output macros (all output to STDOUT) */

#define JSTR(k, v, c)  printf("\"" k "\":\"%s\"%s", (v), (c) ? "," : "")
#define JI64(k, v, c)  printf("\"" k "\":%" PRId64 "%s", (int64_t)(v), (c) ? "," : "")
#define JDBL(k, v, c)  printf("\"" k "\":%.2f%s", (double)(v), (c) ? "," : "")
#define JNULL(k, c)    printf("\"" k "\":null%s", (c) ? "," : "")

/* -----------------------------------------------------------------------
 * Producer delivery-report callback state */

typedef struct {
        int64_t msgs_delivered;
        int64_t msgs_failed;
        int64_t bytes_delivered;
        lat_t   dr_lat; /* DR latency samples in microseconds */
} prod_state_t;

static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *msg,
                      void *opaque) {
        prod_state_t *s = (prod_state_t *)opaque;

        if (msg->err) {
                s->msgs_failed++;
                fprintf(stderr, "%% Delivery failed: %s\n",
                        rd_kafka_err2str(msg->err));
                return;
        }

        s->msgs_delivered++;
        s->bytes_delivered += (int64_t)msg->len;

        /* Extract source timestamp from payload: "LATENCY:<us>\0..." */
        if (msg->payload && msg->len >= 10) {
                int64_t src_us;
                if (sscanf((const char *)msg->payload, "LATENCY:%" SCNd64,
                           &src_us) == 1) {
                        int64_t lat = wall_clock_us() - src_us;
                        /* Sanity check: 0 < lat < 5 min */
                        if (lat > 0 && lat < (int64_t)(5 * 60 * 1000000LL))
                                lat_add(&s->dr_lat, lat);
                }
        }
}

/* -----------------------------------------------------------------------
 * Consumer rebalance callback */

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                rd_kafka_assign(rk, parts);
                if (!g_consumer_rdy) {
                        g_consumer_rdy = 1;
                        /* Signal Python runner: consumer group joined */
                        printf("BENCHMARK_READY\n");
                        fflush(stdout);
                }
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                rd_kafka_assign(rk, NULL);
                break;

        default:
                fprintf(stderr, "%% Rebalance error: %s\n",
                        rd_kafka_err2str(err));
                break;
        }
}

/* -----------------------------------------------------------------------
 * Producer */

static int run_producer(const char *brokers,
                        const char *topic,
                        int msg_size,
                        int64_t msg_count, /* 0 = unlimited */
                        int duration_sec,  /* 0 = count-driven */
                        rd_kafka_conf_t *conf) {
        char errstr[512];
        prod_state_t state = {{0}};

        /* Pre-allocate latency sample buffer */
        size_t init_cap =
            (msg_count > 0 && msg_count < LAT_CAP_MAX) ? (size_t)msg_count
                                                        : 1000000;
        lat_init(&state.dr_lat, init_cap);

        /* Apply mandatory settings (override anything from -X) */
        set_conf(conf, "bootstrap.servers", brokers, "producer");
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_opaque(conf, &state);

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create producer: %s\n", errstr);
                lat_free(&state.dr_lat);
                return 1;
        }
        conf = NULL; /* owned by rk */

        /* Build payload buffer.
         * Layout: "LATENCY:<timestamp_us>\0" + padding to msg_size */
        char *payload = (char *)malloc(msg_size + 1);
        if (!payload) {
                fprintf(stderr, "%% malloc failed\n");
                rd_kafka_destroy(rk);
                lat_free(&state.dr_lat);
                return 1;
        }
        memset(payload, 'x', msg_size);
        payload[msg_size] = '\0';

        double t_start   = wall_clock_sec();
        double deadline  = duration_sec > 0 ? t_start + duration_sec : 0.0;
        int64_t max_msgs = msg_count > 0 ? msg_count : INT64_MAX;
        int64_t msgs_produced = 0;

        fprintf(stderr,
                "%% Producer: topic=%s size=%d count=%" PRId64
                " duration=%ds\n",
                topic, msg_size, msg_count, duration_sec);

        while (g_run && msgs_produced < max_msgs) {
                if (deadline > 0.0 && wall_clock_sec() >= deadline)
                        break;

                /* Embed wall-clock timestamp for latency measurement */
                if (msg_size >= 10) {
                        int ts_len = snprintf(payload, msg_size,
                                              "LATENCY:%" PRId64,
                                              wall_clock_us());
                        /* Fill remainder with 'x' */
                        if (ts_len > 0 && ts_len < msg_size) {
                                memset(payload + ts_len, 'x',
                                       msg_size - ts_len);
                                payload[msg_size] = '\0';
                        }
                }

                /* Produce with backpressure handling */
                while (g_run) {
                        rd_kafka_resp_err_t err = rd_kafka_producev(
                            rk, RD_KAFKA_V_TOPIC(topic),
                            RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_VALUE(payload, msg_size),
                            RD_KAFKA_V_END);

                        if (!err)
                                break;

                        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                                rd_kafka_poll(rk, 10);
                                continue;
                        }

                        fprintf(stderr, "%% produce error: %s\n",
                                rd_kafka_err2str(err));
                        g_run = 0;
                        break;
                }

                msgs_produced++;

                /* Poll periodically to fire DR callbacks */
                if (msgs_produced % 10000 == 0)
                        rd_kafka_poll(rk, 0);
        }

        double t_end_produce = wall_clock_sec();

        fprintf(stderr, "%% Flushing %d in-flight messages...\n",
                rd_kafka_outq_len(rk));
        rd_kafka_flush(rk, 30000 /* 30s */);

        double elapsed = t_end_produce - t_start;
        if (elapsed < 1e-9)
                elapsed = 1e-9;

        lat_sort(&state.dr_lat);

        /* --- JSON output to STDOUT --- */
        printf("{");
        JSTR("mode", "producer", 1);
        JI64("msgs_produced", msgs_produced, 1);
        JI64("msgs_delivered", state.msgs_delivered, 1);
        JI64("msgs_failed", state.msgs_failed, 1);
        JI64("bytes_delivered", state.bytes_delivered, 1);
        JDBL("elapsed_sec", elapsed, 1);
        JDBL("msgs_per_sec",
             state.msgs_delivered / elapsed, 1);
        JDBL("mb_per_sec",
             (double)state.bytes_delivered / elapsed / 1048576.0, 1);

        if (state.dr_lat.cnt > 0) {
                JDBL("dr_latency_avg_us", lat_avg(&state.dr_lat), 1);
                JDBL("dr_latency_p50_us", lat_pct(&state.dr_lat, 50.0), 1);
                JDBL("dr_latency_p95_us", lat_pct(&state.dr_lat, 95.0), 1);
                JDBL("dr_latency_p99_us", lat_pct(&state.dr_lat, 99.0), 1);
                JDBL("dr_latency_min_us", lat_min(&state.dr_lat), 1);
                JDBL("dr_latency_max_us", lat_max(&state.dr_lat), 1);
        } else {
                JNULL("dr_latency_avg_us", 1);
                JNULL("dr_latency_p50_us", 1);
                JNULL("dr_latency_p95_us", 1);
                JNULL("dr_latency_p99_us", 1);
                JNULL("dr_latency_min_us", 1);
                JNULL("dr_latency_max_us", 1);
        }

        JSTR("librdkafka_version", rd_kafka_version_str(), 0);
        printf("}\n");
        fflush(stdout);

        free(payload);
        lat_free(&state.dr_lat);
        rd_kafka_destroy(rk);

        return state.msgs_failed > 0 ? 1 : 0;
}

/* -----------------------------------------------------------------------
 * Consumer */

static int run_consumer(const char *brokers,
                        const char *topic,
                        const char *group_id,
                        int64_t msg_count, /* 0 = unlimited, stop via SIGTERM */
                        rd_kafka_conf_t *conf) {
        char errstr[512];
        lat_t e2e_lat = {0};

        size_t init_cap =
            (msg_count > 0 && msg_count < LAT_CAP_MAX) ? (size_t)msg_count
                                                        : 1000000;
        lat_init(&e2e_lat, init_cap);

        set_conf(conf, "bootstrap.servers", brokers, "consumer");
        set_conf(conf, "group.id", group_id, "consumer");
        /* Needed to detect end-of-partition */
        set_conf(conf, "enable.partition.eof", "true", "consumer");

        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create consumer: %s\n", errstr);
                lat_free(&e2e_lat);
                return 1;
        }
        conf = NULL;

        rd_kafka_poll_set_consumer(rk);

        rd_kafka_topic_partition_list_t *topics =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        if (err) {
                fprintf(stderr, "%% Subscribe failed: %s\n",
                        rd_kafka_err2str(err));
                rd_kafka_destroy(rk);
                lat_free(&e2e_lat);
                return 1;
        }

        fprintf(stderr, "%% Consumer: topic=%s group=%s count=%" PRId64 "\n",
                topic, group_id, msg_count);

        int64_t msgs_consumed = 0;
        int64_t bytes_consumed = 0;
        double t_start = 0.0, t_end = 0.0;
        int64_t max_msgs = msg_count > 0 ? msg_count : INT64_MAX;

        while (g_run && msgs_consumed < max_msgs) {
                rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, 1000);
                if (!msg)
                        continue;

                if (msg->err) {
                        if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                                rd_kafka_message_destroy(msg);
                                continue;
                        }
                        fprintf(stderr, "%% Consumer error: %s\n",
                                rd_kafka_message_errstr(msg));
                        rd_kafka_message_destroy(msg);
                        if (msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC ||
                            msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
                                g_run = 0;
                        }
                        continue;
                }

                double now = wall_clock_sec();
                if (t_start == 0.0)
                        t_start = now;
                t_end = now;

                msgs_consumed++;
                bytes_consumed += (int64_t)msg->len;

                /* E2E latency from "LATENCY:<us>" payload prefix */
                if (msg->payload && msg->len >= 10) {
                        int64_t src_us;
                        if (sscanf((const char *)msg->payload,
                                   "LATENCY:%" SCNd64, &src_us) == 1) {
                                int64_t lat = wall_clock_us() - src_us;
                                /* Sanity: 0 < lat < 5 min */
                                if (lat > 0 &&
                                    lat < (int64_t)(5 * 60 * 1000000LL))
                                        lat_add(&e2e_lat, lat);
                        }
                }

                rd_kafka_message_destroy(msg);
        }

        /* If we exit before BENCHMARK_READY was printed (e.g. SIGTERM before
         * group assignment), emit it now so the Python runner doesn't hang. */
        if (!g_consumer_rdy) {
                g_consumer_rdy = 1;
                printf("BENCHMARK_READY\n");
                fflush(stdout);
        }

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        double elapsed = (t_end > t_start) ? (t_end - t_start) : 1e-9;
        lat_sort(&e2e_lat);

        /* --- JSON output to STDOUT --- */
        printf("{");
        JSTR("mode", "consumer", 1);
        JI64("msgs_consumed", msgs_consumed, 1);
        JI64("bytes_consumed", bytes_consumed, 1);
        JDBL("elapsed_sec", elapsed, 1);
        JDBL("msgs_per_sec", (double)msgs_consumed / elapsed, 1);
        JDBL("mb_per_sec",
             (double)bytes_consumed / elapsed / 1048576.0, 1);

        if (e2e_lat.cnt > 0) {
                /* Convert µs → ms for E2E */
                JDBL("e2e_latency_avg_ms", lat_avg(&e2e_lat) / 1000.0, 1);
                JDBL("e2e_latency_p50_ms",
                     lat_pct(&e2e_lat, 50.0) / 1000.0, 1);
                JDBL("e2e_latency_p95_ms",
                     lat_pct(&e2e_lat, 95.0) / 1000.0, 1);
                JDBL("e2e_latency_p99_ms",
                     lat_pct(&e2e_lat, 99.0) / 1000.0, 1);
                JDBL("e2e_latency_min_ms", lat_min(&e2e_lat) / 1000.0, 1);
                JDBL("e2e_latency_max_ms", lat_max(&e2e_lat) / 1000.0, 1);
        } else {
                JNULL("e2e_latency_avg_ms", 1);
                JNULL("e2e_latency_p50_ms", 1);
                JNULL("e2e_latency_p95_ms", 1);
                JNULL("e2e_latency_p99_ms", 1);
                JNULL("e2e_latency_min_ms", 1);
                JNULL("e2e_latency_max_ms", 1);
        }

        JSTR("librdkafka_version", rd_kafka_version_str(), 0);
        printf("}\n");
        fflush(stdout);

        lat_free(&e2e_lat);
        return 0;
}

/* -----------------------------------------------------------------------
 * main */

int main(int argc, char **argv) {
        char         mode         = '\0';
        const char  *brokers      = "localhost:9092";
        const char  *topic        = NULL;
        const char  *group_id     = "rdkafka-benchmark-cg";
        int          msg_size     = 1024;
        int64_t      msg_count    = 0; /* 0 = unlimited */
        int          duration_sec = 0; /* 0 = count-driven */
        int          opt;
        char         errstr[512];

        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        signal(SIGINT, stop);
        signal(SIGTERM, stop);

        while ((opt = getopt(argc, argv, "PCb:t:G:s:c:d:X:")) != -1) {
                switch (opt) {
                case 'P':
                        mode = 'P';
                        break;
                case 'C':
                        mode = 'C';
                        break;
                case 'b':
                        brokers = optarg;
                        break;
                case 't':
                        topic = optarg;
                        break;
                case 'G':
                        group_id = optarg;
                        break;
                case 's':
                        msg_size = atoi(optarg);
                        if (msg_size < 1)
                                msg_size = 1;
                        break;
                case 'c':
                        msg_count = atoll(optarg);
                        break;
                case 'd':
                        duration_sec = atoi(optarg);
                        break;
                case 'X': {
                        char *val = strchr(optarg, '=');
                        if (!val) {
                                fprintf(stderr,
                                        "%% -X: expected key=value, got: %s\n",
                                        optarg);
                                exit(1);
                        }
                        *val++ = '\0';
                        set_conf(conf, optarg, val, "-X");
                        break;
                }
                default:
                        fprintf(
                            stderr,
                            "Usage: %s -P|-C -b <brokers> -t <topic> "
                            "[options]\n"
                            "\n"
                            "  -P             Producer mode\n"
                            "  -C             Consumer mode\n"
                            "  -b <brokers>   Bootstrap broker(s) "
                            "(default: localhost:9092)\n"
                            "  -t <topic>     Topic name (required)\n"
                            "  -G <group>     Consumer group ID\n"
                            "  -s <size>      Message size in bytes "
                            "(default: 1024)\n"
                            "  -c <count>     Message count; 0 = unlimited "
                            "(stop via SIGTERM or -d)\n"
                            "  -d <seconds>   Duration in seconds; 0 = "
                            "count-driven\n"
                            "  -X key=value   Set librdkafka config property "
                            "(can repeat)\n"
                            "\n"
                            "Output (stdout):\n"
                            "  Consumer: 'BENCHMARK_READY\\n' on partition "
                            "assignment, then JSON result.\n"
                            "  Producer: JSON result on completion.\n"
                            "  All diagnostic output goes to stderr.\n"
                            "\n"
                            "Build: cd <repo_root> && ./configure && make\n",
                            argv[0]);
                        exit(1);
                }
        }

        if (!topic) {
                fprintf(stderr, "%% -t <topic> is required\n");
                exit(1);
        }

        if (mode == '\0') {
                fprintf(stderr, "%% Either -P (producer) or -C (consumer) is "
                                "required\n");
                exit(1);
        }

        int rc;
        if (mode == 'P') {
                rc = run_producer(brokers, topic, msg_size, msg_count,
                                  duration_sec, conf);
        } else {
                rc = run_consumer(brokers, topic, group_id, msg_count, conf);
        }

        rd_kafka_wait_destroyed(2000);
        return rc;
}
