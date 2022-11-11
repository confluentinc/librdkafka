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
 * Admin API usage examples.
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
                "Admin API usage examples\n"
                "\n"
                "Usage: %s <options> <command> [<command arguments>]\n"
                "\n"
                "Commands:\n"
                " Describe groups:\n"
                "   %s -b <brokers> describe_groups <group1> <group2> ...\n\n"
                " List group offsets:\n"
                "   %s -b <brokers> list_group_offsets <group_id> "
                "<require_stable>\n"
                "                   <topic1> <partition1>\n"
                "                   <topic2> <partition2>\n"
                "                   ...\n\n"
                " Alter group offsets:\n"
                "   %s -b <brokers> alter_group_offsets <group_id> <topic>\n"
                "                   <partition1> <offset1>\n"
                "                   <partition2> <offset2>\n"
                "                   ...\n\n"
                " Delete records:\n"
                "   %s -b <brokers> delete_records\n"
                "                   <topic1> <partition1> <offset_before1>\n"
                "                   <topic2> <partition2> <offset_before2>\n"
                "                   ...\n"
                "\n"
                "                   Delete all messages up to but not\n"
                "                   including the specified offset(s).\n"
                "\n"
                "Common options for all commands:\n"
                "   -b <brokers>    Bootstrap server list to connect to.\n"
                "   -X <prop=val>   Set librdkafka configuration property.\n"
                "                   See CONFIGURATION.md for full list.\n"
                "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
                "\n",
                argv0, argv0, argv0, argv0, argv0,
                rd_kafka_get_debug_contexts());

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


static void
print_partition_list(FILE *fp,
                     const rd_kafka_topic_partition_list_t *partitions,
                     int print_offset,
                     const char *prefix) {
        int i;
        char offset_string[521] = {};
        for (i = 0; i < partitions->cnt; i++) {
                memset(&offset_string, 0, sizeof(offset_string));
                if (print_offset) {
                        snprintf(offset_string, sizeof(offset_string),
                                 " offset %" PRId64,
                                 partitions->elems[i].offset);
                }
                fprintf(fp, "%s%s %s [%" PRId32 "]%s error %s",
                        i > 0 ? "\n" : "", prefix, partitions->elems[i].topic,
                        partitions->elems[i].partition, offset_string,
                        rd_kafka_err2str(partitions->elems[i].err));
        }
        fprintf(fp, "\n");
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
 * @brief Print group information.
 */
static int print_groups_info(const rd_kafka_DescribeGroups_result_t *desc,
                             int groups_cnt) {
        size_t i;
        const rd_kafka_ConsumerGroupDescription_t **result_groups;
        size_t result_groups_cnt;
        result_groups =
            rd_kafka_DescribeGroups_result_groups(desc, &result_groups_cnt);

        if (result_groups_cnt == 0) {
                if (groups_cnt > 0) {
                        fprintf(stderr, "No matching groups found\n");
                        return 1;
                } else {
                        fprintf(stderr, "No groups in cluster\n");
                }
        }

        for (i = 0; i < result_groups_cnt; i++) {
                int j, member_cnt;
                const rd_kafka_error_t *error;
                const rd_kafka_ConsumerGroupDescription_t *group =
                    result_groups[i];
                char coordinator_desc[512]   = {};
                rd_kafka_Node_t *coordinator = NULL;
                const char *group_id =
                    rd_kafka_ConsumerGroupDescription_group_id(group);
                char *partition_assignor =
                    rd_kafka_ConsumerGroupDescription_partition_assignor(group);
                rd_kafka_consumer_group_state_t state =
                    rd_kafka_ConsumerGroupDescription_state(group);
                member_cnt =
                    rd_kafka_ConsumerGroupDescription_member_cnt(group);
                error = rd_kafka_ConsumerGroupDescription_error(group);
                coordinator =
                    rd_kafka_ConsumerGroupDescription_coordinator(group);

                if (coordinator != NULL) {
                        snprintf(coordinator_desc, sizeof(coordinator_desc),
                                 ", coordinator [id: %" PRId32
                                 ", host: %s"
                                 ", port: %" PRId32 "]",
                                 rd_kafka_Node_id(coordinator),
                                 rd_kafka_Node_host(coordinator),
                                 rd_kafka_Node_port(coordinator));
                }
                printf(
                    "Group \"%s\", partition assignor \"%s\", "
                    "state %" PRId32 "%s, with %" PRId32 " member(s)",
                    group_id, partition_assignor, state, coordinator_desc,
                    member_cnt);
                if (error)
                        printf(" error[%" PRId32 "]: %s",
                               rd_kafka_error_code(error),
                               rd_kafka_error_string(error));
                printf("\n");
                for (j = 0; j < member_cnt; j++) {
                        rd_kafka_MemberDescription_t *member =
                            rd_kafka_ConsumerGroupDescription_member(group, j);
                        printf("  Member \"%s\" with client-id %s, host %s\n",
                               rd_kafka_MemberDescription_consumer_id(member),
                               rd_kafka_MemberDescription_client_id(member),
                               rd_kafka_MemberDescription_host(member));
                        const rd_kafka_MemberAssignment_t *assignment =
                            rd_kafka_MemberDescription_assignment(member);
                        rd_kafka_topic_partition_list_t *topic_partitions =
                            rd_kafka_MemberAssignment_topic_partitions(
                                assignment);
                        if (!topic_partitions) {
                                printf("    No assignment\n");
                        } else if (topic_partitions->cnt == 0) {
                                printf("    Empty assignment\n");
                        } else {
                                printf("    Assignment:\n");
                                print_partition_list(stdout, topic_partitions,
                                                     0, "      ");
                        }
                }
        }
        return 0;
}

/**
 * @brief Call rd_kafka_DescribeGroups() with a list of
 * groups.
 */
static void cmd_describe_groups(rd_kafka_conf_t *conf, int argc, char **argv) {
        rd_kafka_t *rk;
        const char **groups = NULL;
        char errstr[512];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_queue_t *queue;
        rd_kafka_event_t *event = NULL;
        int retval              = 0;
        int groups_cnt          = 0;

        if (argc >= 1) {
                groups     = (const char **)&argv[0];
                groups_cnt = argc;
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
         * Describe consumer groups
         */
        queue = rd_kafka_queue_new(rk);
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEGROUPS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 10 * 1000 /* 10s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                goto exit;
        }

        rd_kafka_DescribeGroups(rk, groups, groups_cnt, options, queue);

        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                rd_kafka_resp_err_t err = rd_kafka_event_error(event);
                /* DescribeGroups request failed */
                fprintf(stderr, "%% DescribeGroups failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                goto exit;

        } else {
                /* DescribeGroups request succeeded, but individual
                 * groups may have errors. */
                const rd_kafka_DescribeGroups_result_t *result;

                result = rd_kafka_event_DescribeGroups_result(event);
                retval = print_groups_info(result, groups_cnt);
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


static void
cmd_alter_group_offsets(rd_kafka_conf_t *conf, int argc, char **argv) {
        char errstr[512]; /* librdkafka API error reporting buffer */
        rd_kafka_t *rk;   /* Admin client instance */
        rd_kafka_AdminOptions_t *options; /* (Optional) Options for
                                           * AlterConsumerGroupOffsets() */
        rd_kafka_event_t *event; /* AlterConsumerGroupOffsets result event */
        const int min_argc = 2;
        int num_partitions = 0;

        /*
         * Argument validation
         */
        int print_usage = argc < min_argc;
        print_usage |= (argc - min_argc) % 2 != 0;
        if (print_usage) {
                usage("Wrong number of arguments");
        }

        num_partitions    = (argc - min_argc) / 2;
        const char *group = argv[0];
        const char *topic = argv[1];

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
                exit(1);
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to AlterConsumerGroupOffsets() */
        queue = rd_kafka_queue_new(rk);

        /* Set timeout (optional) */
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                exit(1);
        }

        /* Read passed partition-offsets */
        rd_kafka_topic_partition_list_t *partitions =
            rd_kafka_topic_partition_list_new(num_partitions);
        for (int i = 0; i < num_partitions; i++) {
                rd_kafka_topic_partition_list_add(
                    partitions, topic,
                    parse_int("partition", argv[min_argc + i * 2]))
                    ->offset = parse_int("offset", argv[min_argc + 1 + i * 2]);
        }

        /* Create argument */
        rd_kafka_AlterConsumerGroupOffsets_t *alter_consumer_group_offsets =
            rd_kafka_AlterConsumerGroupOffsets_new(group, partitions);
        /* Call AlterConsumerGroupOffsets */
        rd_kafka_AlterConsumerGroupOffsets(rk, &alter_consumer_group_offsets, 1,
                                           options, queue);

        /* Clean up input arguments */
        rd_kafka_AlterConsumerGroupOffsets_destroy(
            alter_consumer_group_offsets);
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_topic_partition_list_destroy(partitions);


        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* AlterConsumerGroupOffsets request failed */
                fprintf(stderr, "%% AlterConsumerGroupOffsets failed: %s\n",
                        rd_kafka_event_error_string(event));
                exit(1);

        } else {
                /* AlterConsumerGroupOffsets request succeeded, but individual
                 * partitions may have errors. */
                const rd_kafka_AlterConsumerGroupOffsets_result_t *result;
                const rd_kafka_group_result_t **groups;
                size_t n_groups;

                result = rd_kafka_event_AlterConsumerGroupOffsets_result(event);
                groups = rd_kafka_AlterConsumerGroupOffsets_result_groups(
                    result, &n_groups);

                printf("AlterConsumerGroupOffsets results:\n");
                for (size_t i = 0; i < n_groups; i++) {
                        const rd_kafka_group_result_t *group = groups[i];
                        const rd_kafka_topic_partition_list_t *partitions =
                            rd_kafka_group_result_partitions(group);
                        print_partition_list(stderr, partitions, 1, "      ");
                }
        }

        /* Destroy event object when we're done with it.
         * Note: rd_kafka_event_destroy() allows a NULL event. */
        rd_kafka_event_destroy(event);

        /* Destroy queue */
        rd_kafka_queue_destroy(queue);

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);
}

static void cmd_delete_records(rd_kafka_conf_t *conf, int argc, char **argv) {
        char errstr[512]; /* librdkafka API error reporting buffer */
        rd_kafka_t *rk;   /* Admin client instance */
        rd_kafka_topic_partition_list_t *offsets_before; /* Delete messages up
                                                          * to but not
                                                          * including these
                                                          * offsets */
        rd_kafka_DeleteRecords_t *del_records; /* Container for offsets_before*/
        rd_kafka_AdminOptions_t *options;      /* (Optional) Options for
                                                * DeleteRecords() */
        rd_kafka_event_t *event;               /* DeleteRecords result event */
        int i;

        /* Parse topic partition offset tuples and add to offsets list */
        offsets_before = rd_kafka_topic_partition_list_new(argc / 3);
        for (i = 0; i < argc; i += 3) {
                const char *topic = argv[i];
                int partition     = parse_int("partition", argv[i + 1]);
                int64_t offset    = parse_int("offset_before", argv[i + 2]);

                rd_kafka_topic_partition_list_add(offsets_before, topic,
                                                  partition)
                    ->offset = offset;
        }

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
                exit(1);
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to DeleteRecords() */
        queue = rd_kafka_queue_new(rk);

        /* Set timeout (optional) */
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETERECORDS);
        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                exit(1);
        }

        /* Create argument */
        del_records = rd_kafka_DeleteRecords_new(offsets_before);
        /* We're now done with offsets_before */
        rd_kafka_topic_partition_list_destroy(offsets_before);

        /* Call DeleteRecords */
        rd_kafka_DeleteRecords(rk, &del_records, 1, options, queue);

        /* Clean up input arguments */
        rd_kafka_DeleteRecords_destroy(del_records);
        rd_kafka_AdminOptions_destroy(options);


        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);

        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* DeleteRecords request failed */
                fprintf(stderr, "%% DeleteRecords failed: %s\n",
                        rd_kafka_event_error_string(event));
                exit(1);

        } else {
                /* DeleteRecords request succeeded, but individual
                 * partitions may have errors. */
                const rd_kafka_DeleteRecords_result_t *result;
                const rd_kafka_topic_partition_list_t *offsets;
                int i;

                result  = rd_kafka_event_DeleteRecords_result(event);
                offsets = rd_kafka_DeleteRecords_result_offsets(result);

                printf("DeleteRecords results:\n");
                for (i = 0; i < offsets->cnt; i++)
                        printf(" %s [%" PRId32 "] offset %" PRId64 ": %s\n",
                               offsets->elems[i].topic,
                               offsets->elems[i].partition,
                               offsets->elems[i].offset,
                               rd_kafka_err2str(offsets->elems[i].err));
        }

        /* Destroy event object when we're done with it.
         * Note: rd_kafka_event_destroy() allows a NULL event. */
        rd_kafka_event_destroy(event);

        /* Destroy queue */
        rd_kafka_queue_destroy(queue);

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);
}

static void
cmd_list_group_offsets(rd_kafka_conf_t *conf, int argc, char **argv) {
        char errstr[512]; /* librdkafka API error reporting buffer */
        rd_kafka_t *rk;   /* Admin client instance */
        rd_kafka_AdminOptions_t *options; /* (Optional) Options for
                                           * ListConsumerGroupOffsets() */
        rd_kafka_event_t *event; /* ListConsumerGroupOffsets result event */
        const int min_argc = 2;
        char *topic;
        int partition;
        int print_usage = 0, require_stable = 0, num_partitions = 0;

        /*
         * Argument validation
         */
        print_usage = argc < min_argc;
        print_usage |= (argc - min_argc) % 2 != 0;
        if (print_usage)
                usage("Wrong number of arguments");
        else {
                require_stable = parse_int("require_stable", argv[1]);
                print_usage    = require_stable < 0 || require_stable > 1;
                if (print_usage)
                        usage("Require stable not a 0-1 int");
        }

        num_partitions    = (argc - min_argc) / 2;
        const char *group = argv[0];

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
                exit(1);
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to ListConsumerGroupOffsets() */
        queue = rd_kafka_queue_new(rk);

        /* Set timeout (optional) */
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                exit(1);
        }
        /* Set requested require stable */
        if (rd_kafka_AdminOptions_set_require_stable(options, require_stable,
                                                     errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set require stable: %s\n",
                        errstr);
                exit(1);
        }

        /* Read passed partition-offsets */
        rd_kafka_topic_partition_list_t *partitions = NULL;
        if (num_partitions > 0) {
                partitions = rd_kafka_topic_partition_list_new(num_partitions);
                for (int i = 0; i < num_partitions; i++) {
                        topic = argv[min_argc + i * 2];
                        partition =
                            parse_int("partition", argv[min_argc + i * 2 + 1]);
                        rd_kafka_topic_partition_list_add(partitions, topic,
                                                          partition);
                }
        }

        /* Create argument */
        rd_kafka_ListConsumerGroupOffsets_t *list_cgrp_offsets =
            rd_kafka_ListConsumerGroupOffsets_new(group, partitions);
        /* Call ListConsumerGroupOffsets */
        rd_kafka_ListConsumerGroupOffsets(rk, &list_cgrp_offsets, 1, options,
                                          queue);

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
                exit(1);

        } else {
                /* ListConsumerGroupOffsets request succeeded, but individual
                 * partitions may have errors. */
                const rd_kafka_ListConsumerGroupOffsets_result_t *result;
                const rd_kafka_group_result_t **groups;
                size_t n_groups;

                result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
                groups = rd_kafka_ListConsumerGroupOffsets_result_groups(
                    result, &n_groups);

                printf("ListConsumerGroupOffsets results:\n");
                for (size_t i = 0; i < n_groups; i++) {
                        const rd_kafka_group_result_t *group = groups[i];
                        const rd_kafka_topic_partition_list_t *partitions =
                            rd_kafka_group_result_partitions(group);
                        print_partition_list(stderr, partitions, 1, "      ");
                }
        }

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        /* Destroy event object when we're done with it.
         * Note: rd_kafka_event_destroy() allows a NULL event. */
        rd_kafka_event_destroy(event);

        /* Destroy queue */
        rd_kafka_queue_destroy(queue);

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);
}

int main(int argc, char **argv) {
        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        rd_kafka_conf_t *conf; /**< Client configuration object */
        int opt, i;
        const char *cmd;
        static const struct {
                const char *cmd;
                void (*func)(rd_kafka_conf_t *conf, int argc, char **argv);
        } cmds[] = {
            {"describe_groups", cmd_describe_groups},
            {"list_group_offsets", cmd_list_group_offsets},
            {"alter_group_offsets", cmd_alter_group_offsets},
            {"delete_records", cmd_delete_records},
            {NULL},
        };

        argv0 = argv[0];

        if (argc == 1)
                usage(NULL);

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


        if (optind == argc)
                usage("No command specified");


        cmd = argv[optind++];

        /*
         * Find matching command and run it
         */
        for (i = 0; cmds[i].cmd; i++) {
                if (!strcmp(cmds[i].cmd, cmd)) {
                        cmds[i].func(conf, argc - optind, &argv[optind]);
                        exit(0);
                }
        }

        usage("Unknown command: %s", cmd);

        /* NOTREACHED */
        return 0;
}
