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
 * DescribeTopics usage example.
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
                "Describe topics usage examples\n"
                "\n"
                "Usage: %s <options> <include_topic_authorized_operations>\n"
                "<topic1> <topic2> ...\n"
                "\n"
                "Options:\n"
                "   -b <brokers>    Bootstrap server list to connect to.\n"
                "   -X <prop=val>   Set librdkafka configuration property.\n"
                "                   See CONFIGURATION.md for full list.\n"
                "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
                "\n",
                argv0, rd_kafka_get_debug_contexts());

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
/**
 * @brief Print topics information.
 */
static int print_topics_info(const rd_kafka_DescribeTopics_result_t *topicdesc,
                             int topic_cnt) {
        size_t i, j;
        const rd_kafka_TopicDescription_t **result_topics;
        size_t result_topics_cnt;
        result_topics = rd_kafka_DescribeTopics_result_topics(
            topicdesc, &result_topics_cnt);

        if (result_topics_cnt == 0) {
                if (topic_cnt > 0) {
                        fprintf(stderr, "No matching topics found\n");
                        return 1;
                } else {
                        fprintf(stderr, "No topics in cluster\n");
                }
        }

        for (i = 0; i < result_topics_cnt; i++) {
                int j, acl_operation;
                const rd_kafka_error_t *error;
                const rd_kafka_TopicDescription_t *topic = result_topics[i];
                const char *topic_name =
                    rd_kafka_TopicDescription_topic_name(topic);
                int topic_authorized_operations_cnt =
                    rd_kafka_TopicDescription_topic_authorized_operation_count(
                        topic);
                int partition_cnt =
                    rd_kafka_TopicDescription_topic_partition_count(topic);
                error = rd_kafka_TopicDescription_error(topic);

                if (rd_kafka_error_code(error)) {
                        printf("Topic: %s has error[%" PRId32 "]: %s\n",
                               topic_name, rd_kafka_error_code(error),
                               rd_kafka_error_string(error));
                        continue;
                }

                printf(
                    "Topic: %s succeeded, has %d topic authorized operations "
                    "allowed, they are:\n",
                    topic_name, topic_authorized_operations_cnt);
                for (j = 0; j < topic_authorized_operations_cnt; j++) {
                        acl_operation =
                            rd_kafka_TopicDescription_authorized_operation(
                                topic, j);
                        printf("\t%s operation is allowed\n",
                               rd_kafka_AclOperation_name(acl_operation));
                }

                printf("partition count is: %d\n", partition_cnt);
                for (j = 0; j < partition_cnt; j++) {
                        const rd_kafka_error_t *partition_error;
                        int leader, id, isr_cnt, replica_cnt, k;
                        partition_error =
                            rd_kafka_TopicDescription_partition_error(topic, j);

                        if (rd_kafka_error_code(partition_error)) {
                                printf(
                                    "\tPartition at index %d has error[%" PRId32
                                    "]: %s\n",
                                    j, rd_kafka_error_code(partition_error),
                                    rd_kafka_error_string(partition_error));
                                continue;
                        }
                        printf("\tPartition at index %d succeeded\n", j);
                        id = rd_kafka_TopicDescription_partition_id(topic, j);
                        leader =
                            rd_kafka_TopicDescription_partition_leader(topic, j);
                        isr_cnt = rd_kafka_TopicDescription_partition_isr_count(
                            topic, j);
                        replica_cnt =
                            rd_kafka_TopicDescription_partition_replica_count(
                                topic, j);
                        printf("\tPartition has id: %d with leader: %d\n", id,
                               leader);
                        if (isr_cnt) {
                                printf(
                                    "\tThe in-sync replica count is: %d, they "
                                    "are: ",
                                    isr_cnt);
                                for (k = 0; k < isr_cnt; k++)
                                        printf(
                                            "%d ",
                                            rd_kafka_TopicDescription_partition_isr(
                                                topic, j, k));
                                printf("\n");
                        } else
                                printf("\tThe in-sync replica count is 0\n");

                        if (replica_cnt) {
                                printf("\tThe replica count is: %d, they are: ",
                                       replica_cnt);
                                for (k = 0; k < replica_cnt; k++)
                                        printf(
                                            "%d ",
                                            rd_kafka_TopicDescription_partition_replica(
                                                topic, j, k));
                                printf("\n");
                        } else
                                printf("\tThe replica count is 0\n");
                        printf("\n");
                }
                printf("\n");
        }
        return 0;
}
/**
 * @brief Call rd_kafka_DescribeTopics() with a list of
 * topics.
 */
static void cmd_describe_topics(rd_kafka_conf_t *conf, int argc, char **argv) {
        rd_kafka_t *rk;
        int i;
        const char **topics = NULL;
        char errstr[512];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *event = NULL;
        rd_kafka_error_t *error;
        int retval     = 0;
        int topics_cnt = 0;

        int include_topic_authorized_operations =
            parse_int("include_topic_authorized_operations", argv[0]);
        if (include_topic_authorized_operations < 0 ||
            include_topic_authorized_operations > 1)
                usage("include_topic_authorized_operations not a 0-1 int");

        if (argc >= 1) {
                topics     = (const char **)&argv[1];
                topics_cnt = argc - 1;
        }
        /*
         * Create consumer instance
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create new consumer: %s", errstr);

        /*
         * Describe topics
         */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 10 * 1000 /* 10s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                goto exit;
        }
        if ((error =
                 rd_kafka_AdminOptions_set_include_topic_authorized_operations(
                     options, include_topic_authorized_operations))) {
                fprintf(stderr,
                        "%% Failed to set require topic authorized operations: "
                        "%s\n",
                        rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                exit(1);
        }

        rd_kafka_DescribeTopics(rk, topics, topics_cnt, options, queue);

        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /* indefinitely but limited by
                                               * the request timeout set
                                               * above (10s) */);

        if (!event) {
                /* User hit Ctrl-C,
                 * see yield call in stop() signal handler */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                rd_kafka_resp_err_t err = rd_kafka_event_error(event);
                /* DescribeTopics request failed */
                fprintf(stderr, "%% DescribeTopics failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                goto exit;

        } else {
                /* DescribeTopics request succeeded, but individual
                 * groups may have errors. */
                const rd_kafka_DescribeTopics_result_t *result;

                result = rd_kafka_event_DescribeTopics_result(event);
                printf("DescribeTopics results:\n");
                retval = print_topics_info(result, topics_cnt);
        }


exit:
        if (event)
                rd_kafka_event_destroy(event);
        rd_kafka_AdminOptions_destroy(options);
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
         * Parse common options
         */
        while ((opt = getopt(argc, argv, "b:X:d:")) != -1) {
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

                default:
                        usage("Unknown option %c", (char)opt);
                }
        }

        cmd_describe_topics(conf, argc - optind, &argv[optind]);
        return 0;
}
