/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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
 * Example: AlterClientQuotas (KIP-546)
 *
 * Sets or removes per-client-id / per-user quotas via the Admin API.
 *
 * Usage:
 *   alter_client_quotas -b localhost:9092 \
 *       SET    user alice producer_byte_rate 1048576
 *       SET    user alice consumer_byte_rate 2097152
 *       REMOVE user alice request_percentage
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#ifdef _WIN32
#include "../win32/wingetopt.h"
#else
#include <getopt.h>
#endif

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree. */
#include "../src/rdkafka.h"

static const char *argv0;
static rd_kafka_queue_t *queue;
static volatile sig_atomic_t run = 1;

static void stop(int sig) {
        if (!run) {
                fprintf(stderr, "%% Forced termination\n");
                exit(2);
        }
        run = 0;
        rd_kafka_queue_yield(queue);
}

static void usage(const char *reason, ...) {
        fprintf(
            stderr,
            "Alter client quotas\n"
            "\n"
            "Usage: %s [options] <ops...>\n"
            "\n"
            "Each operation is one of:\n"
            "  SET    <entity-type> <entity-name> <quota-key> <value>\n"
            "  REMOVE <entity-type> <entity-name> <quota-key>\n"
            "\n"
            "entity-type : user | client-id\n"
            "entity-name : name string, or \"\" for the default entity\n"
            "quota-key   : producer_byte_rate | consumer_byte_rate |\n"
            "              request_percentage | controller_mutation_rate\n"
            "\n"
            "Options:\n"
            "  -b <brokers>    Bootstrap server list (default: "
            "localhost:9092)\n"
            "  -X <prop=val>   Set librdkafka configuration property.\n"
            "  -d <dbg,..>     Enable librdkafka debugging (%s).\n"
            "\n"
            "Example — throttle user 'alice' to 1 MiB/s produce:\n"
            "  %s -b localhost:9092 SET user alice producer_byte_rate 1048576\n"
            "\n",
            argv0, rd_kafka_get_debug_contexts(), argv0);

        if (reason) {
                va_list ap;
                char buf[512];
                va_start(ap, reason);
                vsnprintf(buf, sizeof(buf), reason, ap);
                va_end(ap);
                fprintf(stderr, "ERROR: %s\n", buf);
        }

        exit(reason ? 1 : 0);
}

#define fatal(...)                                                             \
        do {                                                                   \
                fprintf(stderr, "FATAL: " __VA_ARGS__);                        \
                fprintf(stderr, "\n");                                         \
                exit(2);                                                       \
        } while (0)

static void conf_set(rd_kafka_conf_t *conf, const char *name, const char *val) {
        char errstr[512];
        if (rd_kafka_conf_set(conf, name, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                fatal("Failed to set %s=%s: %s", name, val, errstr);
}

/**
 * @brief Print the result entries from an AlterClientQuotas response.
 */
static void print_result(const rd_kafka_AlterClientQuotas_result_t *result) {
        const rd_kafka_ClientQuotaEntry_t **entries;
        size_t entry_cnt, i;

        entries = rd_kafka_AlterClientQuotas_result_entries(result, &entry_cnt);

        printf("AlterClientQuotas result: %zu entr%s\n", entry_cnt,
               entry_cnt == 1 ? "y" : "ies");

        for (i = 0; i < entry_cnt; i++) {
                const rd_kafka_ClientQuotaEntry_t *entry = entries[i];
                const rd_kafka_error_t *error;
                const rd_kafka_ClientQuotaEntity_t **entities;
                size_t entity_cnt, j;

                /* Print entities this result entry covers */
                entities =
                    rd_kafka_ClientQuotaEntry_entities(entry, &entity_cnt);
                printf("  Entry %zu (", i);
                for (j = 0; j < entity_cnt; j++) {
                        const char *type =
                            rd_kafka_ClientQuotaEntity_type(entities[j]);
                        const char *name =
                            rd_kafka_ClientQuotaEntity_name(entities[j]);
                        printf("%s%s=%s", j ? ", " : "", type,
                               name ? name : "<default>");
                }
                printf("): ");

                error = rd_kafka_ClientQuotaEntry_error(entry);
                if (error &&
                    rd_kafka_error_code(error) != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        printf("ERROR %s: %s\n",
                               rd_kafka_err2name(rd_kafka_error_code(error)),
                               rd_kafka_error_string(error));
                } else {
                        printf("OK\n");
                }
        }
}


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *event;
        char errstr[512];
        int opt;
        /* Collected entries to alter */
        rd_kafka_ClientQuotaEntry_t **entries = NULL;
        size_t entry_cnt                      = 0;
        int i;

        argv0 = argv[0];

        conf = rd_kafka_conf_new();
        conf_set(conf, "bootstrap.servers", "localhost:9092");

        while ((opt = getopt(argc, argv, "b:X:d:h")) != -1) {
                switch (opt) {
                case 'b':
                        conf_set(conf, "bootstrap.servers", optarg);
                        break;
                case 'X': {
                        char *name  = optarg;
                        char *value = strchr(name, '=');
                        if (!value)
                                usage("-X requires prop=val");
                        *value++ = '\0';
                        conf_set(conf, name, value);
                        break;
                }
                case 'd':
                        conf_set(conf, "debug", optarg);
                        break;
                case 'h':
                        usage(NULL);
                        break;
                default:
                        usage("Unknown option -%c", opt);
                }
        }

        /* Parse operations from remaining arguments */
        i = optind;
        while (i < argc) {
                const char *verb        = argv[i++];
                int is_remove           = 0;
                const char *entity_type = NULL;
                const char *entity_name = NULL;
                const char *quota_key   = NULL;
                double quota_value      = 0.0;
                rd_kafka_ClientQuotaEntry_t *entry;
                rd_kafka_resp_err_t err;

                if (!strcasecmp(verb, "SET"))
                        is_remove = 0;
                else if (!strcasecmp(verb, "REMOVE"))
                        is_remove = 1;
                else
                        usage("Expected SET or REMOVE, got: %s", verb);

                if (i + 2 + (!is_remove ? 1 : 0) >= argc + 1) {
                        /* Need at least entity-type entity-name quota-key
                         * (and value for SET) */
                        if (is_remove)
                                usage(
                                    "REMOVE requires: <entity-type> "
                                    "<entity-name> <quota-key>");
                        else
                                usage(
                                    "SET requires: <entity-type> "
                                    "<entity-name> <quota-key> <value>");
                }

                entity_type = argv[i++];
                entity_name = argv[i++];
                quota_key   = argv[i++];

                if (!is_remove) {
                        char *end;
                        if (i >= argc)
                                usage("SET requires a <value> argument");
                        quota_value = strtod(argv[i++], &end);
                        if (*end != '\0')
                                usage("Invalid quota value: %s", argv[i - 1]);
                }

                /* Build a new entry for this entity+operation pair */
                entry = rd_kafka_ClientQuotaEntry_new();

                err = rd_kafka_ClientQuotaEntry_add_entity(
                    entry, entity_type,
                    /* empty string means default entity */
                    *entity_name ? entity_name : NULL, errstr, sizeof(errstr));
                if (err)
                        fatal("add_entity failed: %s", errstr);

                err = rd_kafka_ClientQuotaEntry_add_operation(
                    entry, quota_key, quota_value, is_remove, errstr,
                    sizeof(errstr));
                if (err)
                        fatal("add_operation failed: %s", errstr);

                entries = realloc(entries, (entry_cnt + 1) * sizeof(*entries));
                entries[entry_cnt++] = entry;
        }

        if (entry_cnt == 0)
                usage("No quota operations specified");

        /* Create producer handle (used purely for the admin client) */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create producer: %s", errstr);
        conf = NULL; /* now owned by rk */

        queue = rd_kafka_queue_new(rk);

        signal(SIGINT, stop);
        signal(SIGTERM, stop);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCLIENTQUOTAS);
        if (rd_kafka_AdminOptions_set_request_timeout(options, 30 * 1000,
                                                      errstr, sizeof(errstr)))
                fatal("Failed to set timeout: %s", errstr);

        rd_kafka_AlterClientQuotas(rk, entries, entry_cnt, options, queue);
        rd_kafka_AdminOptions_destroy(options);

        /* Destroy input entries — the API has taken copies */
        for (i = 0; i < (int)entry_cnt; i++)
                rd_kafka_ClientQuotaEntry_destroy(entries[i]);
        free(entries);

        /* Wait for result */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                fprintf(stderr, "%% Cancelled by user\n");
        } else if (rd_kafka_event_error(event)) {
                fprintf(stderr, "%% AlterClientQuotas failed: %s\n",
                        rd_kafka_event_error_string(event));
        } else {
                const rd_kafka_AlterClientQuotas_result_t *result =
                    rd_kafka_event_AlterClientQuotas_result(event);
                print_result(result);
        }

        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
        rd_kafka_destroy(rk);

        return 0;
}
