/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * ListTransactions Admin API example (KIP-664).
 *
 * Lists ongoing transactions on the Kafka cluster with optional filtering by:
 *   - Transaction state (e.g., "Ongoing", "PrepareCommit", "PrepareAbort")
 *   - Producer IDs
 *   - Duration (transactions running longer than specified milliseconds)
 *   - Transactional ID pattern (regex)
 *
 * Usage:
 *   list_transactions -b <broker> [options]
 *
 * Options:
 *   -s <state>      Filter by transaction state (can be specified multiple
 *                   times). Valid states: Ongoing, PrepareCommit, PrepareAbort,
 *                   CompleteCommit, CompleteAbort, Empty, Dead
 *   -p <id>         Filter by producer ID (can be specified multiple times)
 *   -D <ms>         Filter transactions running longer than <ms> milliseconds
 *                   (requires Kafka 3.6+, API v1)
 *   -P <pattern>    Filter by transactional ID pattern (regex)
 *                   (requires newer Kafka with API v2)
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
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


const char *argv0;

static rd_kafka_queue_t *queue; /** Admin result queue.
                                 *  This is a global so we can
                                 *  yield in stop() */
static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
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
            "ListTransactions Admin API example (KIP-664)\n"
            "\n"
            "Usage: %s -b <broker> [options]\n"
            "\n"
            "Options:\n"
            "   -b <brokers>    Bootstrap server list to connect to.\n"
            "   -s <state>      Filter by transaction state. Can be "
            "specified\n"
            "                   multiple times. Valid states:\n"
            "                   Ongoing, PrepareCommit, PrepareAbort,\n"
            "                   CompleteCommit, CompleteAbort, Empty, Dead\n"
            "   -p <id>         Filter by producer ID. Can be specified\n"
            "                   multiple times.\n"
            "   -D <ms>         Filter transactions running longer than\n"
            "                   <ms> milliseconds (requires API v1+).\n"
            "   -P <pattern>    Filter by transactional ID regex pattern\n"
            "                   (requires API v2+).\n"
            "   -X <prop=val>   Set librdkafka configuration property.\n"
            "                   See CONFIGURATION.md for full list.\n"
            "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
            "\n"
            "Examples:\n"
            "   # List all transactions\n"
            "   %s -b localhost:9092\n"
            "\n"
            "   # List only ongoing transactions\n"
            "   %s -b localhost:9092 -s Ongoing\n"
            "\n"
            "   # List transactions from specific producer IDs\n"
            "   %s -b localhost:9092 -p 12345 -p 67890\n"
            "\n"
            "   # List transactions running longer than 60 seconds\n"
            "   %s -b localhost:9092 -D 60000\n"
            "\n"
            "   # List transactions matching a pattern\n"
            "   %s -b localhost:9092 -P \"my-app-.*\"\n"
            "\n",
            argv0, rd_kafka_get_debug_contexts(), argv0, argv0, argv0, argv0,
            argv0);

        if (reason) {
                va_list ap;
                char reasonbuf[512];

                va_start(ap, reason);
                vsnprintf(reasonbuf, sizeof(reasonbuf), reason, ap);
                va_end(ap);

                fprintf(stderr, "ERROR: %s\n", reasonbuf);
        }

        exit(reason ? 1 : 0);
}


#define fatal(...)                                                             \
        do {                                                                   \
                fprintf(stderr, "ERROR: ");                                    \
                fprintf(stderr, __VA_ARGS__);                                  \
                fprintf(stderr, "\n");                                         \
                exit(2);                                                       \
        } while (0)


/**
 * @brief Set config property. Exit on failure.
 */
static void conf_set(rd_kafka_conf_t *conf, const char *name, const char *val) {
        char errstr[512];

        if (rd_kafka_conf_set(conf, name, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                fatal("Failed to set %s=%s: %s", name, val, errstr);
}

/**
 * @brief Print transaction listing information.
 */
static int
print_transactions_info(const rd_kafka_ListTransactions_result_t *result) {
        size_t i;
        const rd_kafka_TransactionListing_t **transactions;
        const rd_kafka_error_t **errors;
        size_t txn_cnt;
        size_t error_cnt;

        transactions =
            rd_kafka_ListTransactions_result_transactions(result, &txn_cnt);
        errors = rd_kafka_ListTransactions_result_errors(result, &error_cnt);

        if (txn_cnt == 0 && error_cnt == 0) {
                printf("No transactions found\n");
                return 0;
        }

        printf("Found %zu transaction(s):\n", txn_cnt);
        for (i = 0; i < txn_cnt; i++) {
                const rd_kafka_TransactionListing_t *txn = transactions[i];
                const char *txnid =
                    rd_kafka_TransactionListing_transactional_id(txn);
                int64_t producer_id =
                    rd_kafka_TransactionListing_producer_id(txn);
                const char *state =
                    rd_kafka_TransactionListing_transaction_state(txn);

                printf("  [%zu] transactional.id: %s\n", i + 1, txnid);
                printf("       producer.id:      %" PRId64 "\n", producer_id);
                printf("       state:            %s\n", state);
                printf("\n");
        }

        if (error_cnt > 0) {
                printf("\n%zu error(s) occurred:\n", error_cnt);
                for (i = 0; i < error_cnt; i++) {
                        const rd_kafka_error_t *error = errors[i];
                        printf("  Error[%zu]: %s (code %d)\n", i + 1,
                               rd_kafka_error_string(error),
                               rd_kafka_error_code(error));
                }
                return 1;
        }

        return 0;
}


/**
 * @brief Parse a producer ID or fail.
 */
static int64_t parse_producer_id(const char *str) {
        char *end;
        long long n = strtoll(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr,
                        "%% Invalid producer ID: %s: not a valid integer\n",
                        str);
                exit(1);
        }

        return (int64_t)n;
}


/**
 * @brief Call rd_kafka_ListTransactions() with optional filters.
 */
static void cmd_list_transactions(rd_kafka_conf_t *conf,
                                  const char **states,
                                  size_t states_cnt,
                                  int64_t *producer_ids,
                                  size_t producer_ids_cnt,
                                  int64_t duration_filter_ms,
                                  const char *txnid_pattern) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *event         = NULL;
        rd_kafka_error_t *error         = NULL;
        int retval                      = 0;

        /*
         * Create producer instance
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create new producer: %s", errstr);

        /*
         * List transactions
         */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTTRANSACTIONS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                goto exit;
        }

        /* Set state filter if specified */
        if (states_cnt > 0) {
                error = rd_kafka_AdminOptions_set_match_transaction_states(
                    options, states, states_cnt);
                if (error) {
                        fprintf(stderr,
                                "%% Failed to set transaction state filter: "
                                "%s\n",
                                rd_kafka_error_string(error));
                        goto exit;
                }
                printf("Filtering by %zu state(s)\n", states_cnt);
        }

        /* Set producer ID filter if specified */
        if (producer_ids_cnt > 0) {
                error = rd_kafka_AdminOptions_set_match_producer_ids(
                    options, producer_ids, producer_ids_cnt);
                if (error) {
                        fprintf(stderr,
                                "%% Failed to set producer ID filter: %s\n",
                                rd_kafka_error_string(error));
                        goto exit;
                }
                printf("Filtering by %zu producer ID(s)\n", producer_ids_cnt);
        }

        /* Set duration filter if specified (requires API v1+) */
        if (duration_filter_ms >= 0) {
                error = rd_kafka_AdminOptions_set_transaction_duration_filter(
                    options, duration_filter_ms);
                if (error) {
                        fprintf(stderr,
                                "%% Failed to set duration filter: %s\n",
                                rd_kafka_error_string(error));
                        goto exit;
                }
                printf("Filtering transactions running longer than %" PRId64
                       " ms\n",
                       duration_filter_ms);
        }

        /* Set transactional ID pattern if specified (requires API v2+) */
        if (txnid_pattern) {
                error = rd_kafka_AdminOptions_set_transactional_id_pattern(
                    options, txnid_pattern);
                if (error) {
                        fprintf(stderr,
                                "%% Failed to set transactional ID pattern: "
                                "%s\n",
                                rd_kafka_error_string(error));
                        goto exit;
                }
                printf("Filtering by transactional ID pattern: %s\n",
                       txnid_pattern);
        }

        printf("\nCalling ListTransactions...\n\n");

        rd_kafka_ListTransactions(rk, options, queue);
        rd_kafka_AdminOptions_destroy(options);
        options = NULL;

        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /* indefinitely but limited by
                                               * the request timeout set
                                               * above (30s) */);

        if (!event) {
                /* User hit Ctrl-C,
                 * see yield call in stop() signal handler */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                rd_kafka_resp_err_t err = rd_kafka_event_error(event);
                /* ListTransactions request failed */
                fprintf(stderr, "%% ListTransactions failed[%d]: %s\n", err,
                        rd_kafka_event_error_string(event));
                retval = 1;
                goto exit;

        } else {
                /* ListTransactions request succeeded, but individual
                 * brokers may have errors. */
                const rd_kafka_ListTransactions_result_t *result;

                result = rd_kafka_event_ListTransactions_result(event);
                retval = print_transactions_info(result);
        }


exit:
        if (options)
                rd_kafka_AdminOptions_destroy(options);
        if (error)
                rd_kafka_error_destroy(error);
        if (event)
                rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
        /* Destroy the client instance */
        rd_kafka_destroy(rk);

        exit(retval);
}

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf; /**< Client configuration object */
        int opt;
        int i;

        /* Filter arrays - dynamically sized */
        const char **states          = NULL;
        size_t states_cnt            = 0;
        size_t states_capacity       = 0;
        int64_t *producer_ids        = NULL;
        size_t producer_ids_cnt      = 0;
        size_t producer_ids_capacity = 0;
        int64_t duration_filter_ms   = -1; /* -1 means not set */
        const char *txnid_pattern    = NULL;

        argv0 = argv[0];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();


        /*
         * Parse options
         */
        while ((opt = getopt(argc, argv, "b:s:p:D:P:X:d:h")) != -1) {
                switch (opt) {
                case 'b':
                        conf_set(conf, "bootstrap.servers", optarg);
                        break;

                case 's':
                        /* Add state to filter list */
                        if (states_cnt >= states_capacity) {
                                states_capacity = states_capacity == 0
                                                      ? 4
                                                      : states_capacity * 2;
                                states = realloc(states, states_capacity *
                                                             sizeof(char *));
                        }
                        states[states_cnt++] = optarg;
                        break;

                case 'p':
                        /* Add producer ID to filter list */
                        if (producer_ids_cnt >= producer_ids_capacity) {
                                producer_ids_capacity =
                                    producer_ids_capacity == 0
                                        ? 4
                                        : producer_ids_capacity * 2;
                                producer_ids = realloc(producer_ids,
                                                       producer_ids_capacity *
                                                           sizeof(int64_t));
                        }
                        producer_ids[producer_ids_cnt++] =
                            parse_producer_id(optarg);
                        break;

                case 'D':
                        duration_filter_ms = parse_producer_id(optarg);
                        if (duration_filter_ms < 0)
                                fatal("Duration must be non-negative");
                        break;

                case 'P':
                        txnid_pattern = optarg;
                        break;

                case 'X': {
                        char *name = optarg, *val;

                        if (!(val = strchr(name, '=')))
                                fatal("-X expects a name=value argument");

                        *val = '\0';
                        val++;

                        conf_set(conf, name, val);
                        break;
                }

                case 'd':
                        conf_set(conf, "debug", optarg);
                        break;

                case 'h':
                        usage(NULL);
                        break;

                default:
                        usage("Unknown option %c", (char)opt);
                }
        }

        /* Print filter summary */
        if (states_cnt > 0) {
                printf("State filter(s): ");
                for (i = 0; i < (int)states_cnt; i++)
                        printf("%s%s", i > 0 ? ", " : "", states[i]);
                printf("\n");
        }
        if (producer_ids_cnt > 0) {
                printf("Producer ID filter(s): ");
                for (i = 0; i < (int)producer_ids_cnt; i++)
                        printf("%s%" PRId64, i > 0 ? ", " : "",
                               producer_ids[i]);
                printf("\n");
        }

        cmd_list_transactions(conf, states, states_cnt, producer_ids,
                              producer_ids_cnt, duration_filter_ms,
                              txnid_pattern);
        /* NOTREACHED: cmd_list_transactions() calls exit() */
        return 0;
}
