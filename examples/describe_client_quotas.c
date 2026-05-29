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
 * Example: DescribeClientQuotas (KIP-546)
 *
 * Queries per-client-id / per-user quotas via the Admin API.
 *
 * Usage:
 *   describe_client_quotas -b localhost:9092 \
 *       user    exact alice \
 *       client-id any
 *
 * With no filter components, all quotas are returned (non-strict mode).
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
            "Describe client quotas\n"
            "\n"
            "Usage: %s [options] [<filter-components...>]\n"
            "\n"
            "Each filter component is:\n"
            "  <entity-type> exact    <name>   Match this exact entity name\n"
            "  <entity-type> default           Match the default entity\n"
            "  <entity-type> any               Match any entity of this type\n"
            "\n"
            "entity-type : user | client-id\n"
            "\n"
            "With no filter components, all quotas are returned.\n"
            "\n"
            "Options:\n"
            "  -b <brokers>    Bootstrap server list (default: "
            "localhost:9092)\n"
            "  -s              Strict mode: only return entries that match\n"
            "                  all components exactly\n"
            "  -X <prop=val>   Set librdkafka configuration property.\n"
            "  -d <dbg,..>     Enable librdkafka debugging (%s).\n"
            "\n"
            "Examples:\n"
            "  # All quotas\n"
            "  %s -b localhost:9092\n"
            "\n"
            "  # Quotas for user 'alice'\n"
            "  %s -b localhost:9092 user exact alice\n"
            "\n"
            "  # Any user paired with client-id 'my-app' (strict)\n"
            "  %s -b localhost:9092 -s user any client-id exact my-app\n"
            "\n",
            argv0, rd_kafka_get_debug_contexts(), argv0, argv0, argv0);

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
 * @brief Print the result entries from a DescribeClientQuotas response.
 */
static void print_result(const rd_kafka_DescribeClientQuotas_result_t *result) {
        const rd_kafka_DescribeClientQuotas_result_entry_t **entries;
        size_t entry_cnt, i;

        entries =
            rd_kafka_DescribeClientQuotas_result_entries(result, &entry_cnt);

        printf("DescribeClientQuotas result: %zu entr%s\n", entry_cnt,
               entry_cnt == 1 ? "y" : "ies");

        for (i = 0; i < entry_cnt; i++) {
                const rd_kafka_DescribeClientQuotas_result_entry_t *entry =
                    entries[i];
                const rd_kafka_ClientQuotaEntity_t **entities;
                const rd_kafka_ClientQuotaValue_t **values;
                size_t entity_cnt, value_cnt, j;

                /* Print entities this result entry covers */
                entities = rd_kafka_DescribeClientQuotas_result_entry_entity(
                    entry, &entity_cnt);
                printf("  Entry %zu (", i);
                for (j = 0; j < entity_cnt; j++) {
                        const char *type =
                            rd_kafka_ClientQuotaEntity_type(entities[j]);
                        const char *name =
                            rd_kafka_ClientQuotaEntity_name(entities[j]);
                        printf("%s%s=%s", j ? ", " : "", type,
                               name ? name : "<default>");
                }
                printf("):\n");

                /* Print quota key/value pairs for this entry */
                values = rd_kafka_DescribeClientQuotas_result_entry_values(
                    entry, &value_cnt);
                if (value_cnt == 0) {
                        printf("    (no quota values)\n");
                } else {
                        for (j = 0; j < value_cnt; j++) {
                                printf(
                                    "    %-32s = %f\n",
                                    rd_kafka_ClientQuotaValue_key(values[j]),
                                    rd_kafka_ClientQuotaValue_value(values[j]));
                        }
                }
        }
}


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_ClientQuotaFilter_t *filter;
        rd_kafka_event_t *event;
        char errstr[512];
        int opt;
        int strict = 0;
        int i;

        argv0 = argv[0];

        conf = rd_kafka_conf_new();
        conf_set(conf, "bootstrap.servers", "localhost:9092");

        while ((opt = getopt(argc, argv, "b:sX:d:h")) != -1) {
                switch (opt) {
                case 'b':
                        conf_set(conf, "bootstrap.servers", optarg);
                        break;
                case 's':
                        strict = 1;
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

        /* Build filter from remaining positional arguments */
        filter = rd_kafka_ClientQuotaFilter_new(strict);

        i = optind;
        while (i < argc) {
                const char *entity_type = argv[i++];
                const char *match_str;
                rd_kafka_ClientQuotaMatchType_t match_type;
                const char *match_name = NULL;
                rd_kafka_resp_err_t err;

                if (i >= argc)
                        usage("Expected match-type after entity-type '%s'",
                              entity_type);
                match_str = argv[i++];

                if (!strcasecmp(match_str, "exact")) {
                        match_type = RD_KAFKA_CLIENT_QUOTA_MATCH_EXACT;
                        if (i >= argc)
                                usage(
                                    "match-type 'exact' requires a <name> "
                                    "argument");
                        match_name = argv[i++];
                } else if (!strcasecmp(match_str, "default")) {
                        match_type = RD_KAFKA_CLIENT_QUOTA_MATCH_DEFAULT;
                } else if (!strcasecmp(match_str, "any")) {
                        match_type = RD_KAFKA_CLIENT_QUOTA_MATCH_ANY;
                } else {
                        usage(
                            "Unknown match-type '%s': expected exact, "
                            "default, or any",
                            match_str);
                        /* unreachable */
                        match_type = RD_KAFKA_CLIENT_QUOTA_MATCH_ANY;
                }

                err = rd_kafka_ClientQuotaFilter_add_component(
                    filter, entity_type, match_type, match_name, errstr,
                    sizeof(errstr));
                if (err)
                        fatal("Failed to add filter component: %s", errstr);
        }

        /* Create producer handle (used purely for the admin client) */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create producer: %s", errstr);
        conf = NULL; /* now owned by rk */

        queue = rd_kafka_queue_new(rk);

        signal(SIGINT, stop);
        signal(SIGTERM, stop);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBECLIENTQUOTAS);
        if (rd_kafka_AdminOptions_set_request_timeout(options, 30 * 1000,
                                                      errstr, sizeof(errstr)))
                fatal("Failed to set timeout: %s", errstr);

        rd_kafka_DescribeClientQuotas(rk, filter, options, queue);
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_ClientQuotaFilter_destroy(filter);

        /* Wait for result */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                fprintf(stderr, "%% Cancelled by user\n");
        } else if (rd_kafka_event_error(event)) {
                fprintf(stderr, "%% DescribeClientQuotas failed: %s\n",
                        rd_kafka_event_error_string(event));
        } else {
                const rd_kafka_DescribeClientQuotas_result_t *result =
                    rd_kafka_event_DescribeClientQuotas_result(event);
                print_result(result);
        }

        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
        rd_kafka_destroy(rk);

        return 0;
}
