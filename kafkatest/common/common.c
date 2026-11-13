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
#include <execinfo.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

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

/* Dump a backtrace to stderr and re-raise with the default handler so
 * the process still dies and any core dump reflects the original signal.
 * Async-signal-safe: backtrace/backtrace_symbols_fd, write, _exit. */
static void crash_handler(int sig) {
        void *bt[64];
        int n;
        const char hdr[] = "\n*** fatal signal caught, backtrace: ***\n";
        write(STDERR_FILENO, hdr, sizeof(hdr) - 1);
        n = backtrace(bt, 64);
        backtrace_symbols_fd(bt, n, STDERR_FILENO);
        signal(sig, SIG_DFL);
        raise(sig);
}

void install_signals(void) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig_handler;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = crash_handler;
        sa.sa_flags   = SA_RESETHAND;
        sigaction(SIGSEGV, &sa, NULL);
        sigaction(SIGABRT, &sa, NULL);
        sigaction(SIGBUS, &sa, NULL);
        sigaction(SIGFPE, &sa, NULL);
        sigaction(SIGILL, &sa, NULL);
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

/* Extract the value of a quoted field from a JAAS config string.
 * Example: extract_jaas_field(s, "username", out, sizeof(out)) finds
 *   username="X" in s and copies X into out.
 * Returns 0 on success, -1 if not found. */
static int extract_jaas_field(const char *jaas,
                              const char *field,
                              char *out,
                              size_t out_size) {
        char needle[64];
        snprintf(needle, sizeof(needle), "%s=\"", field);
        const char *p = strstr(jaas, needle);
        if (!p)
                return -1;
        p += strlen(needle);
        const char *end = strchr(p, '"');
        if (!end)
                return -1;
        size_t len = (size_t)(end - p);
        if (len >= out_size)
                len = out_size - 1;
        memcpy(out, p, len);
        out[len] = '\0';
        return 0;
}

/* Slurp a small file into a heap-allocated string (caller frees).
 * Returns NULL on failure. */
static char *slurp_file(const char *path) {
        FILE *fp = fopen(path, "r");
        if (!fp)
                return NULL;
        if (fseek(fp, 0, SEEK_END) != 0) {
                fclose(fp);
                return NULL;
        }
        long sz = ftell(fp);
        if (sz < 0 || sz > 1024 * 1024) {
                fclose(fp);
                return NULL;
        }
        rewind(fp);
        char *buf = malloc((size_t)sz + 1);
        if (!buf) {
                fclose(fp);
                return NULL;
        }
        size_t n   = fread(buf, 1, (size_t)sz, fp);
        buf[n]     = '\0';
        fclose(fp);
        return buf;
}

int apply_jaas_from_kafka_opts(rd_kafka_conf_t *conf,
                               char *errstr,
                               size_t errstr_size) {
        const char *opts = getenv("KAFKA_OPTS");
        if (!opts)
                return 0;

        const char *p = strstr(opts, "-Djava.security.auth.login.config=");
        if (!p)
                return 0;
        p += strlen("-Djava.security.auth.login.config=");

        /* Path runs until whitespace or end-of-string. */
        char jaas_path[512];
        size_t i = 0;
        while (*p && !isspace((unsigned char)*p) && i + 1 < sizeof(jaas_path))
                jaas_path[i++] = *p++;
        jaas_path[i] = '\0';
        if (i == 0)
                return 0;

        char *content = slurp_file(jaas_path);
        if (!content) {
                snprintf(errstr, errstr_size,
                         "Failed to read JAAS file %s: %s", jaas_path,
                         strerror(errno));
                return -1;
        }

        /* Find KafkaClient block (case-sensitive). */
        const char *block = strstr(content, "KafkaClient");
        if (!block) {
                free(content);
                /* No KafkaClient block; not necessarily fatal. */
                return 0;
        }

        char username[256], password[256];
        int have_user = (extract_jaas_field(block, "username", username,
                                            sizeof(username)) == 0);
        int have_pwd  = (extract_jaas_field(block, "password", password,
                                            sizeof(password)) == 0);
        free(content);

        if (!have_user || !have_pwd)
                return 0;

        if (rd_kafka_conf_set(conf, "sasl.username", username, errstr,
                              errstr_size) != RD_KAFKA_CONF_OK)
                return -1;
        if (rd_kafka_conf_set(conf, "sasl.password", password, errstr,
                              errstr_size) != RD_KAFKA_CONF_OK)
                return -1;
        return 0;
}

/* Convert a PKCS#12 truststore to a PEM CA bundle for librdkafka.
 * Invokes `openssl pkcs12 -in <p12> -out <pem> -nokeys
 *                          -password pass:<pwd>`.
 * Returns 0 on success, -1 on failure. The output path is written to
 * `pem_path_out` (size pem_path_out_size).
 *
 * The PEM is placed in /tmp under a deterministic name derived from the
 * source path so re-runs reuse the same file. */
static int convert_pkcs12_truststore_to_pem(const char *p12_path,
                                            const char *password,
                                            char *pem_path_out,
                                            size_t pem_path_out_size) {
        const char *base = strrchr(p12_path, '/');
        base             = base ? base + 1 : p12_path;
        snprintf(pem_path_out, pem_path_out_size, "/tmp/%s.pem", base);

        char cmd[2048];
        snprintf(cmd, sizeof(cmd),
                 "openssl pkcs12 -in '%s' -out '%s' -nokeys "
                 "-password pass:'%s' >/dev/null 2>&1",
                 p12_path, pem_path_out, password ? password : "");
        int rc = system(cmd);
        if (rc != 0)
                return -1;
        return 0;
}

/* Apply a single key=value pair, translating Java-style keys to
 * librdkafka equivalents where needed. Stores deferred state (e.g.
 * truststore password to use during PKCS12 conversion) in static
 * variables keyed by caller; see translate_state below.
 *
 * Returns 0 on success (including silently-ignored keys), -1 on
 * unrecoverable error (errstr populated). */
static char saved_truststore_password[256];
static char saved_keystore_password[256];

static int apply_kv(rd_kafka_conf_t *conf,
                    const char *key,
                    const char *val,
                    char *errstr,
                    size_t errstr_size) {
        /* --- Java-only keys: silently ignore --- */
        if (strcmp(key, "ssl.truststore.type") == 0 ||
            strcmp(key, "ssl.keystore.type") == 0 ||
            strcmp(key, "sasl.mechanism.inter.broker.protocol") == 0 ||
            strcmp(key, "sasl.kerberos.service.name") == 0)
                return 0;

        /* --- Java truststore password: save for later PKCS12 conversion */
        if (strcmp(key, "ssl.truststore.password") == 0) {
                snprintf(saved_truststore_password,
                         sizeof(saved_truststore_password), "%s", val);
                return 0;
        }

        /* --- Java keystore password: also pass through for librdkafka */
        if (strcmp(key, "ssl.keystore.password") == 0) {
                snprintf(saved_keystore_password,
                         sizeof(saved_keystore_password), "%s", val);
                /* Pass through to librdkafka too (it accepts PKCS12). */
                return rd_kafka_conf_set(conf, key, val, errstr,
                                         errstr_size) == RD_KAFKA_CONF_OK
                    ? 0
                    : -1;
        }

        /* --- Java truststore.location (PKCS12): convert to PEM, set
         *      ssl.ca.location instead. */
        if (strcmp(key, "ssl.truststore.location") == 0) {
                char pem_path[512];
                if (convert_pkcs12_truststore_to_pem(
                        val, saved_truststore_password, pem_path,
                        sizeof(pem_path)) != 0) {
                        snprintf(errstr, errstr_size,
                                 "Failed to convert PKCS12 truststore %s "
                                 "to PEM (openssl pkcs12 -in ... -out ... "
                                 "-nokeys failed)",
                                 val);
                        return -1;
                }
                return rd_kafka_conf_set(conf, "ssl.ca.location", pem_path,
                                         errstr, errstr_size) ==
                               RD_KAFKA_CONF_OK
                           ? 0
                           : -1;
        }

        /* --- Java JAAS config: extract username/password for PLAIN/SCRAM. */
        if (strcmp(key, "sasl.jaas.config") == 0) {
                char username[256], password[256];
                if (extract_jaas_field(val, "username", username,
                                       sizeof(username)) == 0 &&
                    extract_jaas_field(val, "password", password,
                                       sizeof(password)) == 0) {
                        if (rd_kafka_conf_set(conf, "sasl.username", username,
                                              errstr, errstr_size) !=
                            RD_KAFKA_CONF_OK)
                                return -1;
                        if (rd_kafka_conf_set(conf, "sasl.password", password,
                                              errstr, errstr_size) !=
                            RD_KAFKA_CONF_OK)
                                return -1;
                        return 0;
                }
                /* Couldn't extract; fall through and let rd_kafka_conf_set
                 * complain. */
        }

        /* --- Default: pass through to librdkafka. */
        if (rd_kafka_conf_set(conf, key, val, errstr, errstr_size) !=
            RD_KAFKA_CONF_OK)
                return -1;
        return 0;
}

/* Pre-scan a properties file for ssl.truststore.password (and other
 * deferred-state values) so that apply_kv has them available when it
 * encounters ssl.truststore.location, regardless of file ordering. */
static void prescan_properties_file(const char *path) {
        FILE *fp = fopen(path, "r");
        if (!fp)
                return;
        char buf[4096];
        while (fgets(buf, sizeof(buf), fp)) {
                char *line = trim(buf);
                if (!*line || *line == '#' || *line == '!')
                        continue;
                char *sep = strpbrk(line, "=:");
                if (!sep || sep == line)
                        continue;
                *sep      = '\0';
                char *key = trim(line);
                char *val = trim(sep + 1);
                if (strcmp(key, "ssl.truststore.password") == 0)
                        snprintf(saved_truststore_password,
                                 sizeof(saved_truststore_password), "%s",
                                 val);
                else if (strcmp(key, "ssl.keystore.password") == 0)
                        snprintf(saved_keystore_password,
                                 sizeof(saved_keystore_password), "%s", val);
        }
        fclose(fp);
}

int load_properties_file(const char *path,
                         rd_kafka_conf_t *conf,
                         char *errstr,
                         size_t errstr_size) {
        /* First pass: capture passwords so order-dependent conversions
         * (e.g. PKCS12 truststore -> PEM) have the password ready. */
        prescan_properties_file(path);

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

                *sep      = '\0';
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

        int rc        = 0;
        char *saveptr = NULL;
        for (char *tok = strtok_r(copy, ",", &saveptr); tok;
             tok       = strtok_r(NULL, ",", &saveptr)) {
                tok      = trim(tok);
                char *eq = strchr(tok, '=');
                if (!eq || eq == tok) {
                        snprintf(
                            errstr, errstr_size,
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