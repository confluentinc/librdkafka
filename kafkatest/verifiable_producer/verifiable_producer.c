/*
* librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#define _POSIX_C_SOURCE 200809L

#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "rdkafka.h"

struct producer_state {
        int64_t num_sent;
        int64_t num_acked;
        int64_t num_errors;
        int64_t target_throughput;
        int64_t start_ms;
};

static struct producer_state state;

static void emit_send_success(const char *topic,
                              int32_t partition,
                              int64_t offset,
                              const void *key,
                              size_t key_len,
                              const void *value,
                              size_t value_len) {
        stdout_lock();
        fputs("{\"name\":\"producer_send_success\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"topic\":", stdout);
        json_write_string(stdout, topic);
        fprintf(stdout, ",\"partition\":%" PRId32, partition);
        fprintf(stdout, ",\"offset\":%" PRId64, offset);
        fputs(",\"key\":", stdout);
        if (key) {
                char *k = malloc(key_len + 1);
                memcpy(k, key, key_len);
                k[key_len] = '\0';
                json_write_string(stdout, k);
                free(k);
        } else {
                fputs("null", stdout);
        }
        fputs(",\"value\":", stdout);
        char *v = malloc(value_len + 1);
        memcpy(v, value, value_len);
        v[value_len] = '\0';
        json_write_string(stdout, v);
        free(v);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_send_error(const char *topic,
                            const void *key,
                            size_t key_len,
                            const void *value,
                            size_t value_len,
                            const char *err_message,
                            const char *err_name) {
        stdout_lock();
        fputs("{\"name\":\"producer_send_error\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"topic\":", stdout);
        json_write_string(stdout, topic);
        fputs(",\"key\":", stdout);
        if (key) {
                char *k = malloc(key_len + 1);
                memcpy(k, key, key_len);
                k[key_len] = '\0';
                json_write_string(stdout, k);
                free(k);
        } else {
                fputs("null", stdout);
        }
        fputs(",\"value\":", stdout);
        char *v = malloc(value_len + 1);
        memcpy(v, value, value_len);
        v[value_len] = '\0';
        json_write_string(stdout, v);
        free(v);
        fputs(",\"message\":", stdout);
        json_write_string(stdout, err_message);
        fputs(",\"exception\":", stdout);
        json_write_string(stdout, err_name);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_tool_data(void) {
        int64_t elapsed_ms = now_ms() - state.start_ms;
        double avg =
            elapsed_ms > 0
                ? ((double)state.num_acked * 1000.0) / (double)elapsed_ms
                : 0.0;
        stdout_lock();
        fputs("{\"name\":\"tool_data\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fprintf(stdout, ",\"sent\":%" PRId64, state.num_sent);
        fprintf(stdout, ",\"acked\":%" PRId64, state.num_acked);
        fprintf(stdout, ",\"target_throughput\":%" PRId64,
                state.target_throughput);
        fprintf(stdout, ",\"avg_throughput\":%.2f", avg);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkm,
                      void *opaque) {
        (void)rk;
        (void)opaque;

        const char *topic =
            rkm->rkt ? rd_kafka_topic_name(rkm->rkt) : "";

        if (rkm->err) {
                state.num_errors++;
                emit_send_error(topic, rkm->key, rkm->key_len, rkm->payload,
                                rkm->len, rd_kafka_err2str(rkm->err),
                                rd_kafka_err2name(rkm->err));
        } else {
                state.num_acked++;
                emit_send_success(topic, rkm->partition, rkm->offset,
                                  rkm->key, rkm->key_len, rkm->payload,
                                  rkm->len);
        }
}

/* Inter-message rate limiter. Matches examples/rdkafka_performance.c:
 * sleep until the next-produce deadline, serving delivery reports
 * during the wait so throttled time isn't wasted. */
static int64_t now_us(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static void rate_limit(rd_kafka_t *rk, int64_t rate_sleep_us) {
        if (rate_sleep_us <= 0) {
                rd_kafka_poll(rk, 0);
                return;
        }
        int64_t next = now_us() + rate_sleep_us;
        do {
                int64_t remaining_us = next - now_us();
                int wait_ms = remaining_us > 0 ? (int)(remaining_us / 1000) : 0;
                rd_kafka_poll(rk, wait_ms);
        } while (next > now_us());
}

static void usage(const char *prog) {
        fprintf(stderr,
                "Usage: %s --topic <topic> --bootstrap-server <servers> "
                "[options]\n"
                "\nOptions:\n"
                "  --topic <t>               Topic to produce to (required)\n"
                "  --bootstrap-server <s>    Broker list (required; also "
                "--broker-list)\n"
                "  --max-messages <n>        Messages to send; -1=infinite "
                "(default -1)\n"
                "  --throughput <n>          Msgs/sec; -1=unlimited "
                "(default -1)\n"
                "  --acks <n>                Acks (0, 1, -1; default -1)\n"
                "  --value-prefix <n>        Prepend '<n>.' to values\n"
                "  --repeating-keys <n>      Cycle keys 0..n-1\n"
                "  --message-create-time <ms> Epoch ms baseline for "
                "timestamps\n"
                "  --producer.config <file>  Properties file (deprecated)\n"
                "  --command-config <file>   Properties file\n"
                "  --debug <flags>           librdkafka debug flags\n"
                "  -X, --property <k=v,..>   Raw librdkafka properties\n",
                prog);
}

int main(int argc, char **argv) {
        const char *topic              = NULL;
        const char *bootstrap          = NULL;
        int64_t max_messages           = -1;
        int64_t throughput             = -1;
        const char *acks               = "-1";
        int value_prefix               = 0;
        int value_prefix_set           = 0;
        int repeating_keys             = 0;
        int repeating_keys_set         = 0;
        int64_t message_create_time_ms = -1;
        const char *config_file        = NULL;
        const char *debug_flags        = NULL;
        const char *x_props            = NULL;

        static struct option long_opts[] = {
            {"topic", required_argument, 0, 't'},
            {"bootstrap-server", required_argument, 0, 'b'},
            {"broker-list", required_argument, 0, 'b'},
            {"max-messages", required_argument, 0, 'm'},
            {"throughput", required_argument, 0, 'T'},
            {"acks", required_argument, 0, 'a'},
            {"value-prefix", required_argument, 0, 'v'},
            {"repeating-keys", required_argument, 0, 'k'},
            {"message-create-time", required_argument, 0, 'c'},
            {"producer.config", required_argument, 0, 'p'},
            {"command-config", required_argument, 0, 'C'},
            {"debug", required_argument, 0, 'd'},
            {"property", required_argument, 0, 'X'},
            {"help", no_argument, 0, 'h'},
            {0, 0, 0, 0}};

        int opt;
        while ((opt = getopt_long(argc, argv, "X:h", long_opts, NULL)) != -1) {
                switch (opt) {
                case 't':
                        topic = optarg;
                        break;
                case 'b':
                        bootstrap = optarg;
                        break;
                case 'm':
                        max_messages = strtoll(optarg, NULL, 10);
                        break;
                case 'T':
                        throughput = strtoll(optarg, NULL, 10);
                        break;
                case 'a':
                        acks = optarg;
                        break;
                case 'v':
                        value_prefix     = atoi(optarg);
                        value_prefix_set = 1;
                        break;
                case 'k':
                        repeating_keys     = atoi(optarg);
                        repeating_keys_set = 1;
                        break;
                case 'c':
                        message_create_time_ms = strtoll(optarg, NULL, 10);
                        break;
                case 'p':
                case 'C':
                        config_file = optarg;
                        break;
                case 'd':
                        debug_flags = optarg;
                        break;
                case 'X':
                        x_props = optarg;
                        break;
                case 'h':
                        usage(argv[0]);
                        return 0;
                default:
                        usage(argv[0]);
                        return 1;
                }
        }

        if (!topic || !bootstrap) {
                fprintf(stderr, "--topic and --bootstrap-server are required\n");
                usage(argv[0]);
                return 1;
        }

        install_signals();

        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        if (conf_set(conf, "bootstrap.servers", bootstrap) == -1)
                return 1;
        if (conf_set(conf, "acks", acks) == -1)
                return 1;
        if (conf_set(conf, "retries", "0") == -1)
                return 1;

        if (config_file &&
            load_properties_file(config_file, conf, errstr, sizeof(errstr)) ==
                -1) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        if (x_props &&
            apply_x_properties(x_props, conf, errstr, sizeof(errstr)) == -1) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        if (debug_flags && conf_set(conf, "debug", debug_flags) == -1)
                return 1;

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "Failed to create producer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        state.target_throughput = throughput;
        state.start_ms          = now_ms();

        emit_event("startup_complete");

        /* Microseconds between messages, or 0 for unlimited. */
        int64_t rate_sleep_us = throughput > 0 ? 1000000 / throughput : 0;

        int64_t counter   = 0;
        int key_counter   = 0;
        char value_buf[64];
        char key_buf[32];

        while (run && (max_messages < 0 || counter < max_messages)) {
                int value_len;
                if (value_prefix_set)
                        value_len = snprintf(value_buf, sizeof(value_buf),
                                             "%d.%" PRId64, value_prefix,
                                             counter);
                else
                        value_len = snprintf(value_buf, sizeof(value_buf),
                                             "%" PRId64, counter);

                const void *key_ptr = NULL;
                size_t key_len      = 0;
                if (repeating_keys_set && repeating_keys > 0) {
                        int key_val = key_counter % repeating_keys;
                        key_len     = (size_t)snprintf(
                            key_buf, sizeof(key_buf), "%d", key_val);
                        key_ptr = key_buf;
                        key_counter++;
                }

                int64_t ts = 0;
                if (message_create_time_ms >= 0)
                        ts = message_create_time_ms +
                             (now_ms() - state.start_ms);

                rd_kafka_resp_err_t err;
                if (ts > 0) {
                        err = rd_kafka_producev(
                            rk, RD_KAFKA_V_TOPIC(topic),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_VALUE(value_buf, (size_t)value_len),
                            RD_KAFKA_V_KEY(key_ptr, key_len),
                            RD_KAFKA_V_TIMESTAMP(ts), RD_KAFKA_V_END);
                } else {
                        err = rd_kafka_producev(
                            rk, RD_KAFKA_V_TOPIC(topic),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_VALUE(value_buf, (size_t)value_len),
                            RD_KAFKA_V_KEY(key_ptr, key_len), RD_KAFKA_V_END);
                }

                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                        rd_kafka_poll(rk, 100);
                        continue;
                }
                if (err) {
                        state.num_errors++;
                        emit_send_error(topic, key_ptr, key_len, value_buf,
                                        (size_t)value_len,
                                        rd_kafka_err2str(err),
                                        rd_kafka_err2name(err));
                } else {
                        state.num_sent++;
                        counter++;
                }

                rate_limit(rk, rate_sleep_us);
        }

        rd_kafka_flush(rk, 30 * 1000);
        rd_kafka_destroy(rk);

        emit_event("shutdown_complete");
        emit_tool_data();
        return 0;
}