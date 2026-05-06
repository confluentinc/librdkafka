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

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "common.h"

volatile sig_atomic_t run = 1;

static pthread_mutex_t stdout_mtx = PTHREAD_MUTEX_INITIALIZER;

void stdout_lock(void) {
        pthread_mutex_lock(&stdout_mtx);
}

void stdout_unlock(void) {
        pthread_mutex_unlock(&stdout_mtx);
}

static void sig_handler(int sig) {
        (void)sig;
        run = 0;
}

void install_signals(void) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig_handler;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
}

int64_t now_ms(void) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void json_write_string(FILE *out, const char *s) {
        fputc('"', out);
        if (s) {
                for (const unsigned char *p = (const unsigned char *)s; *p;
                     p++) {
                        switch (*p) {
                        case '"':
                                fputs("\\\"", out);
                                break;
                        case '\\':
                                fputs("\\\\", out);
                                break;
                        case '\b':
                                fputs("\\b", out);
                                break;
                        case '\f':
                                fputs("\\f", out);
                                break;
                        case '\n':
                                fputs("\\n", out);
                                break;
                        case '\r':
                                fputs("\\r", out);
                                break;
                        case '\t':
                                fputs("\\t", out);
                                break;
                        default:
                                if (*p < 0x20)
                                        fprintf(out, "\\u%04x", *p);
                                else
                                        fputc(*p, out);
                        }
                }
        }
        fputc('"', out);
}

void emit_event(const char *name) {
        stdout_lock();
        fputs("{\"name\":", stdout);
        json_write_string(stdout, name);
        fprintf(stdout, ",\"timestamp\":%" PRId64 "}\n", now_ms());
        fflush(stdout);
        stdout_unlock();
}

int conf_set(rd_kafka_conf_t *conf, const char *key, const char *value) {
        char errstr[512];
        if (rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s: %s\n", key, errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }
        return 0;
}

static char *trim(char *s) {
        while (*s && isspace((unsigned char)*s))
                s++;
        char *end = s + strlen(s);
        while (end > s && isspace((unsigned char)end[-1]))
                end--;
        *end = '\0';
        return s;
}

static int apply_kv(rd_kafka_conf_t *conf,
                    const char *key,
                    const char *val,
                    char *errstr,
                    size_t errstr_size) {
        if (rd_kafka_conf_set(conf, key, val, errstr, errstr_size) !=
            RD_KAFKA_CONF_OK)
                return -1;
        return 0;
}

int load_properties_file(const char *path,
                         rd_kafka_conf_t *conf,
                         char *errstr,
                         size_t errstr_size) {
        FILE *fp = fopen(path, "r");
        if (!fp) {
                snprintf(errstr, errstr_size,
                         "Failed to open properties file %s: %s", path,
                         strerror(errno));
                return -1;
        }

        char buf[4096];
        int rc = 0;
        while (fgets(buf, sizeof(buf), fp)) {
                char *line = trim(buf);
                if (!*line || *line == '#' || *line == '!')
                        continue;

                char *sep = strpbrk(line, "=:");
                if (!sep || sep == line) {
                        /* Malformed line; skip silently (matches Java
                         * Properties behavior of ignoring lines without a
                         * separator). */
                        continue;
                }

                *sep = '\0';
                char *key = trim(line);
                char *val = trim(sep + 1);

                if (apply_kv(conf, key, val, errstr, errstr_size) == -1) {
                        rc = -1;
                        break;
                }
        }

        fclose(fp);
        return rc;
}

int apply_x_properties(const char *csv,
                       rd_kafka_conf_t *conf,
                       char *errstr,
                       size_t errstr_size) {
        char *copy = strdup(csv);
        if (!copy) {
                snprintf(errstr, errstr_size, "strdup failed");
                return -1;
        }

        int rc = 0;
        char *saveptr = NULL;
        for (char *tok = strtok_r(copy, ",", &saveptr); tok;
             tok = strtok_r(NULL, ",", &saveptr)) {
                tok     = trim(tok);
                char *eq = strchr(tok, '=');
                if (!eq || eq == tok) {
                        snprintf(errstr, errstr_size,
                                 "Malformed -X property: %s (expected key=value)",
                                 tok);
                        rc = -1;
                        break;
                }
                *eq       = '\0';
                char *key = trim(tok);
                char *val = trim(eq + 1);
                if (apply_kv(conf, key, val, errstr, errstr_size) == -1) {
                        rc = -1;
                        break;
                }
        }

        free(copy);
        return rc;
}