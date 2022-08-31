/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022, Magnus Edenhill
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
 * Example utility that shows how to use ListConsumerGroupOffsets (AdminAPI)
 * to list the committed offsets of a specific consumer group.
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


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

/**
 * @brief Parse an integer or fail.
 */
int64_t parse_int(const char *what, const char *str) {
        char *end;
        unsigned long n = strtoull(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr, "%% Invalid input for %s: %s: not an integer\n",
                        what, str);
                exit(1);
        }

        return (int64_t)n;
}

static void
print_partition_list(FILE *fp,
                     const rd_kafka_topic_partition_list_t *partitions) {
        int i;
        for (i = 0; i < partitions->cnt; i++) {
                fprintf(fp, "%s %s [%" PRId32 "] offset %" PRId64,
                        i > 0 ? "\n" : "", partitions->elems[i].topic,
                        partitions->elems[i].partition,
                        partitions->elems[i].offset);
        }
        fprintf(fp, "\n");
}


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf; /* Temporary configuration object */
        char errstr[512];      /* librdkafka API error reporting buffer */
        const char *bootstrap_servers;   /* Argument: bootstrap servers */
        rd_kafka_t *rk;        /* Admin client instance */
        rd_kafka_AdminOptions_t *options;      /* (Optional) Options for
                                                * ListConsumerGroupOffsets() */
        rd_kafka_event_t *event;               /* ListConsumerGroupOffsets result event */
        const int min_argc = 4;
        char *topic;
        int partition;
        int exitcode = 0, print_usage = 0, require_stable = 0, num_partitions = 0;

        /*
         * Argument validation
         */
        print_usage = argc < min_argc;
        print_usage |= (argc - min_argc) % 2 != 0;
        if (!print_usage) {
                require_stable = parse_int("require_stable", argv[3]);
                print_usage = require_stable < 0 || require_stable > 1;
        }
        if (print_usage) {
                fprintf(stderr,
                        "%% Usage: %s <bootstrap_servers> "
                        "<group_id> "
                        "<require_stable> "
                        "<topic1> "
                        "<partition1> "
                        "<topic2> "
                        "<partition2> "
                        "..."
                        "\n",
                        argv[0]);
                return 1;
        }

        num_partitions = (argc - min_argc) / 2;
        bootstrap_servers = argv[1];
        const char *group = argv[2];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /* Set bootstrap broker(s) as a comma-separated list of
         * host or host:port (default port 9092).
         * librdkafka will use the bootstrap brokers to acquire the full
         * set of brokers from the cluster. */
        if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }
        rd_kafka_conf_set(conf, "debug", "admin,topic,metadata", NULL, 0);

        /*
         * Create an admin client, it can be created using any client type,
         * so we choose producer since it requires no extra configuration
         * and is more light-weight than the consumer.
         *
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new producer: %s\n",
                        errstr);
                return 1;
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to ListConsumerGroupOffsets() */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        /* Set timeout (optional) */
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                return 1;
        }
        /* Set requested require stable */
        if (rd_kafka_AdminOptions_set_require_stable(
                options, require_stable, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set require stable: %s\n", errstr);
                return 1;
        }

        /* Read passed partition-offsets */
        rd_kafka_topic_partition_list_t *partitions = NULL;
        if(num_partitions > 0) {
                partitions = rd_kafka_topic_partition_list_new(num_partitions);
                for (int i = 0; i < num_partitions; i++) {
                        topic = argv[min_argc + i * 2];
                        partition = parse_int("partition", argv[min_argc + i * 2 + 1]);
                        rd_kafka_topic_partition_list_add(
                                partitions,
                                topic,
                                partition);
                }
        }

        /* Create argument */
        rd_kafka_ListConsumerGroupOffsets_t * list_cgrp_offsets = rd_kafka_ListConsumerGroupOffsets_new(group, partitions);
        /* Call ListConsumerGroupOffsets */
        rd_kafka_ListConsumerGroupOffsets(rk, &list_cgrp_offsets, 1, options, queue);

        /* Clean up input arguments */
        rd_kafka_ListConsumerGroupOffsets_destroy(list_cgrp_offsets);
        rd_kafka_AdminOptions_destroy(options);


        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* ListConsumerGroupOffsets request failed */
                fprintf(stderr, "%% ListConsumerGroupOffsets failed: %s\n",
                        rd_kafka_event_error_string(event));
                exitcode = 2;

        } else {
                /* ListConsumerGroupOffsets request succeeded, but individual
                 * partitions may have errors. */
                const rd_kafka_ListConsumerGroupOffsets_result_t *result;
                const rd_kafka_group_result_t **groups;
                size_t n_groups;

                result  = rd_kafka_event_ListConsumerGroupOffsets_result(event);
                groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &n_groups);

                printf("ListConsumerGroupOffsets results:\n");
                for (size_t i = 0; i < n_groups; i++) {
                        const rd_kafka_group_result_t *group = groups[i];
                        const rd_kafka_topic_partition_list_t *partitions = rd_kafka_group_result_partitions(group);
                        print_partition_list(stderr, partitions);
                }
        }

        /* Destroy event object when we're done with it.
         * Note: rd_kafka_event_destroy() allows a NULL event. */
        rd_kafka_event_destroy(event);

        signal(SIGINT, SIG_DFL);

        /* Destroy queue */
        rd_kafka_queue_destroy(queue);

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);

        return exitcode;
}
