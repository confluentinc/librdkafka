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
 * DescribeCluster usage example.
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
                "Describe cluster usage examples\n"
                "\n"
                "Usage: %s <options> <include_cluster_authorized_operations> ...\n"
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
 * @brief Print cluster information.
 */
static int
print_cluster_info(const rd_kafka_DescribeCluster_result_t *clusterdesc) {
        size_t i;
        int j, acl_operation;
        const rd_kafka_ClusterDescription_t *desc;
        int controller_id, node_cnt, cluster_authorized_operations_cnt;
        const char* cluster_id;

        desc = rd_kafka_DescribeCluster_result_description(clusterdesc);

        controller_id = rd_kafka_ClusterDescription_controller_id(desc);
        node_cnt = rd_kafka_ClusterDescription_node_cnt(desc);
        cluster_authorized_operations_cnt = rd_kafka_ClusterDescription_cluster_acl_operations_cnt(desc);
        cluster_id = rd_kafka_ClusterDescription_cluster_id(desc);

        printf("Cluster id: %s\t Controller id: %d\t ACL operations count allowed: %d\n",
                cluster_id, controller_id, cluster_authorized_operations_cnt);
        for(j=0;j<cluster_authorized_operations_cnt;j++){
                acl_operation = 
                        rd_kafka_ClusterDescription_authorized_operation_idx(desc,j);
                printf("\t%s operation is allowed\n",
                rd_kafka_AclOperation_name(acl_operation));
        }
        
        for(j=0;j<node_cnt;j++){
                const rd_kafka_Node_t *node = NULL;
                node = rd_kafka_ClusterDescription_node_idx(desc, j);
                printf("Node [id: %" PRId32
                                 ", host: %s"
                                 ", port: %" PRIu16 "]\n",
                                 rd_kafka_Node_id(node),
                                 rd_kafka_Node_host(node),
                                 rd_kafka_Node_port(node));
        }
        return 0;
}

/**
 * @brief Call rd_kafka_DescribeCluster()
 */
static void
cmd_describe_cluster(rd_kafka_conf_t *conf, int argc, char **argv) {
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *event = NULL;
        rd_kafka_error_t *error;
        int retval     = 0;

        int include_cluster_authorized_operations =
            parse_int("include_cluster_authorized_operations", argv[0]);
        if (include_cluster_authorized_operations < 0 ||
            include_cluster_authorized_operations > 1)
                usage("include_cluster_authorized_operations not a 0-1 int");

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
         * Describe cluster
         */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 10 * 1000 /* 10s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                goto exit;
        }
        if ((error = rd_kafka_AdminOptions_set_include_cluster_authorized_operations(
                 options, include_cluster_authorized_operations))) {
                fprintf(stderr,
                        "%% Failed to set require cluster authorized operations: %s\n",
                        rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                exit(1);
        }

        rd_kafka_DescribeCluster(rk, options, queue);

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
                /* DescribeCluster request failed */
                fprintf(stderr,
                        "%% DescribeCluster failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                goto exit;

        } else {
                /* DescribeCluster request succeeded */
                const rd_kafka_DescribeCluster_result_t *result;

                result = rd_kafka_event_DescribeCluster_result(event);
                printf("DescribeCluster results:\n");
                retval = print_cluster_info(result);
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
        conf_set(conf, "sasl.username", "broker");
        conf_set(conf, "sasl.password", "broker");
        conf_set(conf, "sasl.mechanism", "SCRAM-SHA-256");
        conf_set(conf, "security.protocol", "SASL_PLAINTEXT");

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

        cmd_describe_cluster(conf, argc - optind, &argv[optind]);
        return 0;
}