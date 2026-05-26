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
#ifndef _KAFKATEST_COMMON_H_
#define _KAFKATEST_COMMON_H_

#include <stdio.h>
#include <stdint.h>
#include <signal.h>

#include "rdkafka.h"

/* Set to 0 by SIGINT/SIGTERM; main loops should poll this. */
extern volatile sig_atomic_t run;

/* Install SIGINT/SIGTERM handlers that clear `run`. */
void install_signals(void);

/* Wallclock milliseconds since Unix epoch (matches Java
 * System.currentTimeMillis). */
int64_t now_ms(void);

/* Write a JSON-escaped string to `out`, including surrounding quotes. */
void json_write_string(FILE *out, const char *s);

/* Emit a zero-field event: {"name":"<name>","timestamp":<ms>}\n on stdout. */
void emit_event(const char *name);

/* Set a single config key on `conf`. On error, prints "<key>: <errstr>\n"
 * to stderr, destroys `conf`, and returns -1. Returns 0 on success. */
int conf_set(rd_kafka_conf_t *conf, const char *key, const char *value);

/* If KAFKA_OPTS contains -Djava.security.auth.login.config=<path>, parse
 * that JAAS file's KafkaClient {} block and set sasl.username and
 * sasl.password on `conf`. Returns 0 if applied or if no JAAS path was
 * found; -1 if a JAAS path was found but parsing/setting failed. */
int apply_jaas_from_kafka_opts(rd_kafka_conf_t *conf,
                               char *errstr,
                               size_t errstr_size);

/* Parse a Java properties file and apply each key=value to `conf`.
 * Lines starting with '#' or '!' are comments; blank lines are skipped.
 * Splits on the first '=' or ':'.
 * Returns 0 on success, -1 on failure (errstr populated). */
int load_properties_file(const char *path,
                         rd_kafka_conf_t *conf,
                         char *errstr,
                         size_t errstr_size);

/* Parse a comma-separated key=value list (the -X / --property flag)
 * and apply each pair to `conf`.
 * Returns 0 on success, -1 on failure (errstr populated). */
int apply_x_properties(const char *csv,
                       rd_kafka_conf_t *conf,
                       char *errstr,
                       size_t errstr_size);

/* Serialize access to stdout across threads (librdkafka callbacks run on
 * internal threads; Ducktape requires intact newline-delimited JSON). */
void stdout_lock(void);
void stdout_unlock(void);

#endif /* _KAFKATEST_COMMON_H_ */