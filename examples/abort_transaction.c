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
 * AbortTransaction Admin API example (KIP-664).
 *
 * Forcefully aborts a hanging transaction by writing transaction markers
 * to partition leaders. This is an admin operation that should only be
 * used when a transaction is hanging and blocking consumers.
 *
 * Typical workflow:
 *   1. Run describe_producers to identify a hanging transaction
 *   2. Extract producer_id, producer_epoch, coordinator_epoch, and
 *      txn_start_offset from the output
 *   3. Run this tool to abort the transaction
 *
 * Usage:
 *   abort_transaction -b <broker> -t <topic> -p <partition> \
 *                     -P <producer_id> -E <epoch> \
 *                     [-C <coord_epoch>] [-o <txn_start_offset>]
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
            "AbortTransaction Admin API example (KIP-664)\n"
            "\n"
            "Usage: %s -b <broker> -t <topic> -p <partition> "
            "-P <producer_id> -E <epoch> [-C <coord_epoch>] "
            "[-o <txn_start_offset>]\n"
            "\n"
            "Options:\n"
            "   -b <brokers>             Bootstrap server list to connect "
            "to.\n"
            "   -t <topic>               Topic name.\n"
            "   -p <partition>           Partition number.\n"
            "   -P <producer_id>         Producer ID from DescribeProducers.\n"
            "   -E <epoch>               Producer epoch from "
            "DescribeProducers.\n"
            "   -C <coord_epoch>         Coordinator epoch (optional, "
            "default: -1).\n"
            "   -o <txn_start_offset>    Transaction start offset "
            "(optional, default: -1).\n"
            "   -X <prop=val>            Set librdkafka configuration "
            "property.\n"
            "                            See CONFIGURATION.md for full "
            "list.\n"
            "   -d <dbg,..>              Enable librdkafka debugging (%s).\n"
            "\n"
            "Example:\n"
            "   %s -b localhost:9092 -t mytopic -p 0 -P 1000 -E 0 -C 1 -o "
            "42\n"
            "\n",
            argv0, rd_kafka_get_debug_contexts(), argv0);

        if (reason) {
                va_list ap;
                char reasonbuf[512];

                va_start(ap, reason);
                vsnprintf(reasonbuf, sizeof(reasonbuf), reason, ap);
                va_end(ap);

                fprintf(stderr, "Error: %s\n", reasonbuf);
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


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];
        int opt;
        const char *brokers       = NULL;
        const char *topic         = NULL;
        int partition             = -1;
        int64_t producer_id       = -1;
        int32_t producer_epoch    = -1;
        int32_t coordinator_epoch = -1; /* -1 means unknown */
        int64_t txn_start_offset  = -1; /* -1 means unknown */
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        rd_kafka_resp_err_t err;
        rd_kafka_AbortTransactionSpec_t **abort_specs;
        const rd_kafka_AbortTransaction_result_t *result;
        const rd_kafka_PartitionAbortResult_t **par_list;
        size_t par_cnt;
        int retval = 0;

        argv0 = argv[0];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();
        conf_set(conf, "bootstrap.servers", "");

        while ((opt = getopt(argc, argv, "hb:t:p:P:E:C:o:X:d:")) != -1) {
                switch (opt) {
                case 'b':
                        brokers = optarg;
                        conf_set(conf, "bootstrap.servers", brokers);
                        break;

                case 't':
                        topic = optarg;
                        break;

                case 'p':
                        partition = atoi(optarg);
                        break;

                case 'P':
                        producer_id = strtoll(optarg, NULL, 10);
                        break;

                case 'E':
                        producer_epoch = atoi(optarg);
                        break;

                case 'C':
                        coordinator_epoch = atoi(optarg);
                        break;

                case 'o':
                        txn_start_offset = strtoll(optarg, NULL, 10);
                        break;

                case 'X': {
                        char *name = optarg, *val;

                        if (!(val = strchr(name, '=')))
                                usage("-X requires a value");

                        *val = '\0';
                        val++;

                        conf_set(conf, name, val);
                        break;
                }

                case 'd':
                        conf_set(conf, "debug", optarg);
                        break;

                case 'h':
                default:
                        usage(opt == 'h' ? NULL : "Unknown option %c", opt);
                }
        }

        if (!brokers)
                usage("Missing -b <broker>");

        if (!topic)
                usage("Missing -t <topic>");

        if (partition < 0)
                usage("Missing -p <partition>");

        if (producer_id < 0)
                usage("Missing -P <producer_id>");

        if (producer_epoch < 0)
                usage("Missing -E <epoch>");


        signal(SIGINT, stop);
#ifdef SIGTERM
        signal(SIGTERM, stop);
#endif

        /*
         * Create producer instance.
         * NOTE: rd_kafka_new() takes ownership of the conf object.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create Kafka producer: %s", errstr);

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that gets passed to AbortTransaction(). */
        queue = rd_kafka_queue_new(rk);

        /* Build the abort spec */
        abort_specs    = malloc(sizeof(*abort_specs));
        abort_specs[0] = rd_kafka_AbortTransactionSpec_new(
            topic, partition, producer_id, producer_epoch, coordinator_epoch,
            txn_start_offset);

        /* Create AdminOptions */
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ABORTTRANSACTION);
        if (rd_kafka_AdminOptions_set_request_timeout(options, 30 * 1000,
                                                      errstr, sizeof(errstr)))
                fatal("Failed to set timeout: %s", errstr);

        /* Call AbortTransaction */
        printf("Aborting transaction for %s [%d]:\n", topic, partition);
        printf("  Producer ID: %" PRId64 "\n", producer_id);
        printf("  Producer epoch: %" PRId32 "\n", producer_epoch);
        printf("  Coordinator epoch: %" PRId32 "%s\n", coordinator_epoch,
               coordinator_epoch == -1 ? " (unknown)" : "");
        printf("  Transaction start offset: %" PRId64 "%s\n", txn_start_offset,
               txn_start_offset == -1 ? " (unknown)" : "");

        rd_kafka_AbortTransaction(rk, abort_specs, 1, options, queue);

        rd_kafka_AdminOptions_destroy(options);

        /* Wait for result */
        rkev = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!rkev) {
                fprintf(stderr, "%% Interrupted\n");
                retval = 1;
                goto cleanup;
        }

        if (rd_kafka_event_error(rkev)) {
                fprintf(stderr, "%% AbortTransaction failed: %s: %s\n",
                        rd_kafka_err2name(rd_kafka_event_error(rkev)),
                        rd_kafka_event_error_string(rkev));
                retval = 1;
                rd_kafka_event_destroy(rkev);
                goto cleanup;
        }

        result = rd_kafka_event_AbortTransaction_result(rkev);
        if (!result) {
                fprintf(stderr, "%% Expected AbortTransaction result, got %s\n",
                        rd_kafka_event_name(rkev));
                retval = 1;
                rd_kafka_event_destroy(rkev);
                goto cleanup;
        }

        par_list =
            rd_kafka_AbortTransaction_result_partitions(result, &par_cnt);

        if (par_cnt == 0) {
                fprintf(stderr, "%% No result returned\n");
                retval = 1;
                rd_kafka_event_destroy(rkev);
                goto cleanup;
        }

        {
                const rd_kafka_PartitionAbortResult_t *par = par_list[0];

                err = rd_kafka_PartitionAbortResult_error(par);

                printf("\nResult for %s [%d]:\n",
                       rd_kafka_PartitionAbortResult_topic(par),
                       rd_kafka_PartitionAbortResult_partition(par));

                if (err) {
                        printf("  Error: %s\n", rd_kafka_err2str(err));
                        retval = 1;
                } else {
                        printf("  Success: Transaction aborted\n");
                }
        }

        rd_kafka_event_destroy(rkev);

cleanup:
        rd_kafka_AbortTransactionSpec_destroy(abort_specs[0]);
        free(abort_specs);
        rd_kafka_queue_destroy(queue);
        rd_kafka_destroy(rk);

        return retval;
}
