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

/* Wallclock milliseconds since Unix epoch (matches Java System.currentTimeMillis). */
int64_t now_ms(void);

/* Write a JSON-escaped string to `out`, including surrounding quotes. */
void json_write_string(FILE *out, const char *s);

/* Emit a zero-field event: {"name":"<name>","timestamp":<ms>}\n on stdout. */
void emit_event(const char *name);

/* Set a single config key on `conf`. On error, prints "<key>: <errstr>\n"
 * to stderr, destroys `conf`, and returns -1. Returns 0 on success. */
int conf_set(rd_kafka_conf_t *conf, const char *key, const char *value);

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