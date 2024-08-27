/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
 *               
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
 * DescribeConsumerGroups usage example.
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

static rd_kafka_queue_t *queue = NULL; /** Admin result queue.
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

        if (queue)
                rd_kafka_queue_yield(queue);
}


static void usage(const char *reason, ...) {

        fprintf(stderr,
                "Describe groups usage examples\n"
                "\n"
                "Usage: %s <options> <include_authorized_operations> <group1> "
                "<group2> ...\n"
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

static void
print_partition_list(FILE *fp,
                     const rd_kafka_topic_partition_list_t *partitions,
                     int print_offset,
                     const char *prefix) {
        int i;

        if (partitions->cnt == 0) {
                fprintf(fp, "%sNo partition found", prefix);
        }
        for (i = 0; i < partitions->cnt; i++) {
                char offset_string[512] = {};
                *offset_string          = '\0';
                if (print_offset) {
                        snprintf(offset_string, sizeof(offset_string),
                                 " offset %" PRId64,
                                 partitions->elems[i].offset);
                }
                fprintf(fp, "%s%s Topic:%s [%" PRId32 "]%s error %s",
                        i > 0 ? "\n" : "", prefix,
                        partitions->elems[i].topic,
                        partitions->elems[i].partition, offset_string,
                        rd_kafka_err2str(partitions->elems[i].err));
        }
        fprintf(fp, "\n");
}

static void print_group_member_info(const rd_kafka_Member_t *member) {
        const char *member_id = rd_kafka_Member_member_id(member);
        const char *instance_id = rd_kafka_Member_instance_id(member);
        const char *rack_id = rd_kafka_Member_rack_id(member);
        int32_t member_epoch = rd_kafka_Member_member_epoch(member);
        const char *client_id = rd_kafka_Member_client_id(member);
        const char *client_host = rd_kafka_Member_client_host(member);
        const char *subscribed_topic_names = rd_kafka_Member_subscribed_topic_names(member);
        const char *subscribed_topic_regex = rd_kafka_Member_subscribed_topic_regex(member);

        printf("  Member \"%s\" (instance \"%s\", rack \"%s\", member epoch %" PRId32
               ", client id \"%s\", client host \"%s\", subscribed topic names \"%s\"" 
               "subscribed topic regex \"%s\")\n",
               member_id, instance_id, rack_id, member_epoch, client_id, client_host, 
               subscribed_topic_names, subscribed_topic_regex);

        const rd_kafka_MemberAssignment_t *assignment = 
            rd_kafka_Member_assignment(member);
        const rd_kafka_topic_partition_list_t *partitions = 
            rd_kafka_Member_assignment_partitions(assignment);

        if(!partitions) {
                printf("    No assignment\n");
                return;
        } else if (partitions->cnt == 0) {
                printf("    Empty assignment\n");
                return;
        } else {
                printf("    Assignment:\n");
                print_partition_list(stdout, partitions, 0, "      ");
        }
        printf("\n");
        const rd_kafka_MemberAssignment_t *target_assignment =
            rd_kafka_Member_target_assignment(member);
        const rd_kafka_topic_partition_list_t *target_partitions =
            rd_kafka_Member_target_assignment_partitions(target_assignment);
        if (!target_partitions) {
                printf("    No target assignment\n");
                return;
        } else if (target_partitions->cnt == 0) {
                printf("    Empty target assignment\n");
                return;
        } else {
                printf("    Target Assignment:\n");
                print_partition_list(stdout, target_partitions, 0, "      ");
        }
        printf("\n");     
}

static void print_group_info(const rd_kafka_ConsumerGroupDescribeResponseData_t *grpdesc) {
        size_t member_cnt;
        size_t j;
        size_t authorized_operations_cnt;
        const rd_kafka_AclOperation_t *authorized_operations;
        const rd_kafka_error_t *error;
        const char *group_id = rd_kafka_ConsumerGroupDescribeResponseData_group_id(
            grpdesc);
        rd_kafka_consumer_group_state_t state = 
            rd_kafka_ConsumerGroupDescribeResponseData_state(grpdesc);
        int32_t group_epoch = rd_kafka_ConsumerGroupDescribeResponseData_group_epoch(
            grpdesc);
        int32_t assignment_epoch = 
            rd_kafka_ConsumerGroupDescribeResponseData_assignment_epoch(grpdesc);
        const char *partition_assignor =
            rd_kafka_ConsumerGroupDescribeResponseData_partition_assignor(
                grpdesc);
        member_cnt = rd_kafka_ConsumerGroupDescribeResponseData_member_count(
            grpdesc);
        authorized_operations = 
            rd_kafka_ConsumerGroupDescribeResponseData_authorized_operations(grpdesc);
        authorized_operations_cnt = 
            rd_kafka_ConsumerGroupDescribeResponseData_authorized_operations_count(grpdesc);
        error = rd_kafka_ConsumerGroupDescribeResponseData_error(grpdesc);

        printf("Group \"%s\" (state %s, group epoch %" PRId32
               ", assignment epoch %" PRId32 ", partition assignor \"%s\") with %" PRId32 
               " member(s)\n " ,
               group_id, rd_kafka_consumer_group_state_name(state), group_epoch,
               assignment_epoch, partition_assignor, member_cnt);
        for (j = 0; j < authorized_operations_cnt; j++) {
                printf("%s operation is allowed\n",
                       rd_kafka_AclOperation_name(authorized_operations[j]));
        }
        if (error)
                printf(" error[%" PRId32 "]: %s", rd_kafka_error_code(error),
                       rd_kafka_error_string(error));
        printf("\n");
        for(j = 0; j < member_cnt; j++) {
                const rd_kafka_Member_t *member =
                    rd_kafka_ConsumerGroupDescribeResponseData_member(grpdesc, j);
                print_group_member_info(member);
        }
       
}

static void 
print_groups_info(const rd_kafka_ConsumerGroupDescribe_result_t *grpdesc,
                              int groups_cnt ) {
        size_t i;
        const rd_kafka_ConsumerGroupDescribeResponseData_t **result_grps;
        size_t result_groups_cnt;
        result_grps = rd_kafka_ConsumerGroupDescribe_result_groups(
            grpdesc, &result_groups_cnt);
        if (result_groups_cnt == 0) {
                if (groups_cnt > 0) {
                        fprintf(stderr, "No matching groups found\n");
                } else {
                        fprintf(stderr, "No groups in cluster\n");
                }
        }

        for (i = 0; i < result_groups_cnt; i++) {
                print_group_info(result_grps[i]);
                printf("\n");
        }
}

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
        long n = strtol(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr, "%% Invalid input for %s: %s: not an integer\n",
                        what, str);
                exit(1);
        }

        return (int64_t)n;
}

/**
 * @brief Call rd_kafka_ConsumerGroupDescribe() with a list of
 * groups.
 */
static void
cmd_consumer_groups_describe(rd_kafka_conf_t *conf, int argc, char **argv) {
        rd_kafka_t *rk      = NULL;
        const char **groups = NULL;
        char errstr[512];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *event          = NULL;
        rd_kafka_error_t *error;
        int retval         = 0;
        int groups_cnt     = 0;
        const int min_argc = 2;
        int include_authorized_operations;

        if (argc < min_argc)
                usage("Wrong number of arguments");

        include_authorized_operations =
            parse_int("include_authorized_operations", argv[0]);
        if (include_authorized_operations < 0 ||
            include_authorized_operations > 1)
                usage("include_authorized_operations not a 0-1 int");

        groups     = (const char **)&argv[1];
        groups_cnt = argc - 1;

        for(int i=0; i < groups_cnt; i++) {
            printf("Group %d: %s\n", i, groups[i]);
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
         * Consumer Group Describe
         */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_CONSUMERGROUPDESCRIBE);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 10 * 1000 /* 10s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                retval = 1;
                goto exit;
        }
        if ((error = rd_kafka_AdminOptions_set_include_authorized_operations(
                 options, include_authorized_operations))) {
                fprintf(stderr,
                        "%% Failed to set require authorized operations: %s\n",
                        rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                retval = 1;
                goto exit;
        }

        rd_kafka_ConsumerGroupDescribe(rk, groups, groups_cnt, options, queue);

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
                /* DescribeConsumerGroups request failed */
                fprintf(stderr,
                        "%% ConsumerGroupDescribe failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                retval = 1;

        } else {
                /* DescribeConsumerGroups request succeeded, but individual
                 * groups may have errors. */
                const rd_kafka_ConsumerGroupDescribe_result_t *result;

                result = rd_kafka_event_ConsumerGroupDescribe_result(event);
                printf("ConsumerGroupDescribe result:\n");
                print_groups_info(result, groups_cnt);
        }


exit:
        /* Cleanup. */
        if (event)
                rd_kafka_event_destroy(event);
        if (options)
                rd_kafka_AdminOptions_destroy(options);
        if (queue)
                rd_kafka_queue_destroy(queue);
        if (rk)
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

        cmd_consumer_groups_describe(conf, argc - optind, &argv[optind]);

        return 0;
}
