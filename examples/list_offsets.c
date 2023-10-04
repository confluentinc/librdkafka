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
 * ARE DISCLAIMED. IN NO EVENT SH THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/**
 * Example utility that shows how to use ListOffsets (AdminAPI)
 * to list the offset[EARLIEST,LATEST,FORTIMESTAMP] for
 * one or more topic partitions.
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


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf; /* Temporary configuration object */
        char errstr[512];      /* librdkafka API error reporting buffer */
        const char *brokers = "localhost:9092"; /* Argument: broker list */
        rd_kafka_t *rk;                         /* Admin client instance */
        rd_kafka_topic_partition_list_t *topic_partitions; /* Delete messages up
                                                            * to but not
                                                            * including these
                                                            * offsets */
        rd_kafka_AdminOptions_t *options; /* (Optional) Options for */
        rd_kafka_event_t *event;          /* Result event */
        int exitcode = 0;

        // /* Set OffsetSpec to LATEST */
        // topic_partition->offset = RD_KAFKA_OFFSET_SPEC_LATEST;
        // /* Set OffsetSpec to MAXTIMESTAMP */
        // topic_partition->offset = RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP;
        // /* Set OffsetSpec to a timestamp */
        // topic_partition->offset = 99999999999;

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /* Set bootstrap broker(s) as a comma-separated list of
         * host or host:port (default port 9092).
         * librdkafka will use the bootstrap brokers to acquire the full
         * set of brokers from the cluster. */
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
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
                return 1;
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to ListOffsets() */
        queue                 = rd_kafka_queue_new(rk);
        char *topicname       = "newalphagammaone";
        char *message_one     = "Message-1";
        char *message_two     = "Message-2";
        char *message_three   = "Message-3";
        int64_t basetimestamp = 10000000;
        int64_t t1            = basetimestamp + 100;
        int64_t t2            = basetimestamp + 400;
        int64_t t3            = basetimestamp + 250;
        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);
        rd_kafka_NewTopic_t *topic[1];
        topic[0] =
            rd_kafka_NewTopic_new(topicname, 1, 1, errstr, sizeof(errstr));
        rd_kafka_CreateTopics(rk, topic, 1, NULL, queue);
        rd_kafka_NewTopic_destroy_array(topic, 1);
        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);
        rd_kafka_event_destroy(event);
        rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(topicname),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE(message_one, strlen(message_one)),

            RD_KAFKA_V_TIMESTAMP(t1),
            /* Per-Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            /* End sentinel */
            RD_KAFKA_V_END);
        rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(topicname),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE(message_two, strlen(message_two)),

            RD_KAFKA_V_TIMESTAMP(t2),
            /* Per-Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            /* End sentinel */
            RD_KAFKA_V_END);
        rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(topicname),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE(message_three, strlen(message_three)),

            RD_KAFKA_V_TIMESTAMP(t3),
            /* Per-Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            /* End sentinel */
            RD_KAFKA_V_END);
        rd_kafka_flush(rk, 10 * 1000);
        /* Set timeout (optional) */
        options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTOFFSETS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                return 1;
        }

        topic_partitions = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_t *topic_partition =
            rd_kafka_topic_partition_list_add(topic_partitions, topicname, 0);
        topic_partition->offset = RD_KAFKA_OFFSET_SPEC_LATEST;
        /* Call ListOffsets */
        rd_kafka_ListOffsets(rk, topic_partitions, options, queue);
        rd_kafka_AdminOptions_destroy(options);
        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);
        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* ListOffsets request failed */
                fprintf(stderr, "%% ListOffsets failed: %s\n",
                        rd_kafka_event_error_string(event));
                exitcode = 2;

        } else {
                /* ListOffsets request succeeded, but individual
                 * partitions may have errors. */
                const rd_kafka_ListOffsets_result_t *result;
                rd_kafka_ListOffsetsResultInfo_t **result_infos;
                size_t cnt;
                size_t i;
                result       = rd_kafka_event_ListOffsets_result(event);
                result_infos = rd_kafka_ListOffsets_result_infos(result, &cnt);
                printf("ListOffsets results:\n");
                for (i = 0; i < cnt; i++) {
                        const rd_kafka_topic_partition_t *topic_partition =
                            rd_kafka_ListOffsetsResultInfo_topic_partition(
                                result_infos[i]);
                        int64_t timestamp =
                            rd_kafka_ListOffsetsResultInfo_timestamp(
                                result_infos[i]);
                        printf(
                            "Topic : %s PartitionIndex : %d ErrorCode : %d "
                            "Offset : %" PRId64 " Timestamp : %" PRId64 "\n",
                            topic_partition->topic, topic_partition->partition,
                            topic_partition->err, topic_partition->offset,
                            timestamp);
                }
        }

        /* Destroy event object when we're done with it.
         * Note: rd_kafka_event_destroy() allows a NULL event. */
        rd_kafka_event_destroy(event);
        rd_kafka_DeleteTopic_t *del_topics[1];
        del_topics[0] = rd_kafka_DeleteTopic_new(topicname);
        rd_kafka_DeleteTopics(rk, del_topics, 1, NULL, queue);

        rd_kafka_DeleteTopic_destroy_array(del_topics, 1);
        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);
        rd_kafka_event_destroy(event);

        /* Destroy queue */
        rd_kafka_queue_destroy(queue);

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);

        return exitcode;
}
