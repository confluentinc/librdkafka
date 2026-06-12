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
 * DescribeTransactions Admin API example (KIP-664).
 *
 * Describes transactions by their transactional IDs, returning detailed
 * information including:
 *   - Producer ID and epoch
 *   - Transaction state
 *   - Transaction timeout and start time
 *   - Topics and partitions involved in the transaction
 *
 * Usage:
 *   describe_transactions -b <broker> <transactional_id> [transactional_id ...]
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

        fprintf(stderr,
                "DescribeTransactions Admin API example (KIP-664)\n"
                "\n"
                "Usage: %s -b <broker> <transactional_id> [transactional_id "
                "...]\n"
                "\n"
                "Options:\n"
                "   -b <brokers>    Bootstrap server list to connect to.\n"
                "   -X <prop=val>   Set librdkafka configuration property.\n"
                "                   See CONFIGURATION.md for full list.\n"
                "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
                "\n"
                "Arguments:\n"
                "   transactional_id   One or more transactional IDs to "
                "describe.\n"
                "\n"
                "Examples:\n"
                "   # Describe a single transaction\n"
                "   %s -b localhost:9092 my-transactional-id\n"
                "\n"
                "   # Describe multiple transactions\n"
                "   %s -b localhost:9092 txn-1 txn-2 txn-3\n"
                "\n",
                argv0, rd_kafka_get_debug_contexts(), argv0, argv0);

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
 * @brief Print transaction description information.
 */
static int print_transaction_descriptions(
    const rd_kafka_DescribeTransactions_result_t *result) {
        size_t i, j;
        const rd_kafka_TransactionDescription_t **transactions;
        size_t txn_cnt;
        int errors_found = 0;

        transactions =
            rd_kafka_DescribeTransactions_result_transactions(result, &txn_cnt);

        if (txn_cnt == 0) {
                printf("No transactions found\n");
                return 0;
        }

        printf("Described %zu transaction(s):\n\n", txn_cnt);
        for (i = 0; i < txn_cnt; i++) {
                const rd_kafka_TransactionDescription_t *txn = transactions[i];
                const char *txnid =
                    rd_kafka_TransactionDescription_transactional_id(txn);
                const rd_kafka_error_t *error =
                    rd_kafka_TransactionDescription_error(txn);

                printf("[%zu] transactional.id: %s\n", i + 1, txnid);

                if (error) {
                        printf("     Error: %s (code %d)\n",
                               rd_kafka_error_string(error),
                               rd_kafka_error_code(error));
                        errors_found = 1;
                        printf("\n");
                        continue;
                }

                printf("     producer.id:             %" PRId64 "\n",
                       rd_kafka_TransactionDescription_producer_id(txn));
                printf("     producer.epoch:          %d\n",
                       rd_kafka_TransactionDescription_producer_epoch(txn));
                printf("     state:                   %s\n",
                       rd_kafka_TransactionDescription_transaction_state(txn));
                printf("     timeout.ms:              %" PRId32 "\n",
                       rd_kafka_TransactionDescription_transaction_timeout_ms(
                           txn));

                {
                        int64_t start_time_ms =
                            rd_kafka_TransactionDescription_transaction_start_time_ms(
                                txn);
                        if (start_time_ms >= 0) {
                                printf("     start.time.ms:           %" PRId64
                                       "\n",
                                       start_time_ms);
                        } else {
                                printf(
                                    "     start.time.ms:           (no "
                                    "transaction in progress)\n");
                        }
                }

                {
                        const rd_kafka_topic_partition_list_t *topics =
                            rd_kafka_TransactionDescription_topics(txn);

                        if (topics && topics->cnt > 0) {
                                printf("     topics/partitions:       ");
                                for (j = 0; j < (size_t)topics->cnt; j++) {
                                        const rd_kafka_topic_partition_t *tp =
                                            &topics->elems[j];
                                        if (j > 0)
                                                printf(", ");
                                        printf("%s[%" PRId32 "]", tp->topic,
                                               tp->partition);
                                }
                                printf("\n");
                        } else {
                                printf(
                                    "     topics/partitions:       (none)\n");
                        }
                }

                printf("\n");
        }

        return errors_found ? 1 : 0;
}


/**
 * @brief Call rd_kafka_DescribeTransactions() with the given transactional
 * IDs.
 */
static void cmd_describe_transactions(rd_kafka_conf_t *conf,
                                      const char **transactional_ids,
                                      size_t transactional_ids_cnt) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *event         = NULL;
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
         * Describe transactions
         */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBETRANSACTIONS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                goto exit;
        }

        printf("Describing %zu transaction(s)...\n\n", transactional_ids_cnt);

        rd_kafka_DescribeTransactions(rk, transactional_ids,
                                      transactional_ids_cnt, options, queue);
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
                /* DescribeTransactions request failed */
                fprintf(stderr, "%% DescribeTransactions failed[%d]: %s\n", err,
                        rd_kafka_event_error_string(event));
                retval = 1;
                goto exit;

        } else {
                /* DescribeTransactions request succeeded, but individual
                 * transactions may have errors. */
                const rd_kafka_DescribeTransactions_result_t *result;

                result = rd_kafka_event_DescribeTransactions_result(event);
                retval = print_transaction_descriptions(result);
        }


exit:
        if (options)
                rd_kafka_AdminOptions_destroy(options);
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

        argv0 = argv[0];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();


        /*
         * Parse options
         */
        while ((opt = getopt(argc, argv, "b:X:d:h")) != -1) {
                switch (opt) {
                case 'b':
                        conf_set(conf, "bootstrap.servers", optarg);
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

        /* Remaining arguments are transactional IDs */
        if (optind >= argc)
                usage("At least one transactional ID must be specified");

        cmd_describe_transactions(conf, (const char **)&argv[optind],
                                  argc - optind);

        return 0;
}
