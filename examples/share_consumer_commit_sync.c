/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
 * Share consumer example using rd_kafka_share_commit_sync() to
 * explicitly acknowledge and synchronously commit records.
 *
 * Usage:
 *   share_consumer_commit_sync <broker> <group.id> <topic1> [topic2 ...]
 *
 * This example demonstrates:
 *  - Consuming records with rd_kafka_share_consume_batch()
 *  - Explicitly acknowledging individual records with
 *    rd_kafka_share_acknowledge_type() using ACCEPT, RELEASE, or REJECT
 *  - Committing acknowledgements synchronously with
 *    rd_kafka_share_commit_sync() at ~10% rate mid-batch,
 *    printing per-partition results
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 199309L
#endif

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <time.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) {
        run = 0;
}


/**
 * @returns 1 if all bytes are printable, else 0.
 */
static int is_printable(const char *buf, size_t size) {
        size_t i;

        for (i = 0; i < size; i++)
                if (!isprint((int)buf[i]))
                        return 0;

        return 1;
}

static const char *
ack_type_to_str(rd_kafka_share_AcknowledgeType_t type) {
        switch (type) {
        case RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT:
                return "ACCEPT";
        case RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE:
                return "RELEASE";
        case RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT:
                return "REJECT";
        default:
                return "UNKNOWN";
        }
}


int main(int argc, char **argv) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;
        char errstr[512];
        const char *brokers;
        const char *groupid;
        char **topics;
        int topic_cnt;
        rd_kafka_topic_partition_list_t *subscription;
        int i;

        if (argc < 4) {
                fprintf(stderr,
                        "%% Usage: "
                        "%s <broker> <group.id> <topic1> [topic2 ..]\n",
                        argv[0]);
                return 1;
        }

        brokers   = argv[1];
        groupid   = argv[2];
        topics    = &argv[3];
        topic_cnt = argc - 3;

        conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        if (rd_kafka_conf_set(conf, "group.id", groupid, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        if (rd_kafka_conf_set(conf, "share.acknowledgement.mode", "explicit",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!rkshare) {
                fprintf(stderr, "%% Failed to create new share consumer: %s\n",
                        errstr);
                return 1;
        }

        conf = NULL;

        subscription = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0; i < topic_cnt; i++)
                rd_kafka_topic_partition_list_add(subscription, topics[i],
                                                  RD_KAFKA_PARTITION_UA);

        err = rd_kafka_share_subscribe(rkshare, subscription);
        if (err) {
                fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                        subscription->cnt, rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(subscription);
                rd_kafka_share_destroy(rkshare);
                return 1;
        }

        fprintf(stderr,
                "%% Subscribed to %d topic(s), "
                "waiting for rebalance and messages...\n",
                subscription->cnt);

        rd_kafka_topic_partition_list_destroy(subscription);

        signal(SIGINT, stop);

        srand((unsigned int)time(NULL));

        rd_kafka_message_t *rkmessages[10001];
        while (run) {
                size_t rcvd_msgs = 0;
                rd_kafka_error_t *error;

                printf("Calling rd_kafka_share_consume_batch()\n");
                error = rd_kafka_share_consume_batch(rkshare, 3000, rkmessages,
                                                     &rcvd_msgs);

                if (error) {
                        fprintf(stderr, "%% Consume error: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd_msgs == 0)
                        continue;

                printf("Received %zu messages\n", rcvd_msgs);

                for (i = 0; i < (int)rcvd_msgs; i++) {
                        rd_kafka_message_t *rkm = rkmessages[i];
                        rd_kafka_share_AcknowledgeType_t ack_type;
                        int r;

                        if (rkm->err) {
                                fprintf(stderr, "%% Consumer error: %d: %s\n",
                                        rkm->err, rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Randomly choose ack type:
                         *   50% ACCEPT, 30% RELEASE, 20% REJECT */
                        r = rand() % 100;
                        if (r < 50)
                                ack_type =
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                        else if (r < 80)
                                ack_type =
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
                        else
                                ack_type =
                                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;

                        printf("Message on %s [%" PRId32 "] at offset %" PRId64
                               " (delivery count: %" PRId16 ") -> %s",
                               rd_kafka_topic_name(rkm->rkt), rkm->partition,
                               rkm->offset,
                               rd_kafka_message_delivery_count(rkm),
                               ack_type_to_str(ack_type));

                        if (rkm->key && is_printable(rkm->key, rkm->key_len))
                                printf(" Key: %.*s", (int)rkm->key_len,
                                       (const char *)rkm->key);

                        if (rkm->payload &&
                            is_printable(rkm->payload, rkm->len))
                                printf(" Value: %.*s", (int)rkm->len,
                                       (const char *)rkm->payload);

                        printf("\n");

                        err = rd_kafka_share_acknowledge_type(rkshare, rkm,
                                                              ack_type);
                        if (err)
                                fprintf(stderr,
                                        "%% Acknowledge error for "
                                        "%s [%" PRId32 "] @ %" PRId64 ": %s\n",
                                        rd_kafka_topic_name(rkm->rkt),
                                        rkm->partition, rkm->offset,
                                        rd_kafka_err2str(err));

                        rd_kafka_message_destroy(rkm);

                        /* Randomly commit ~10% of the time to
                         * exercise sync commit mid-batch. */
                        if (run && (rand() % 100) < 10) {
                                rd_kafka_topic_partition_list_t *partitions =
                                    NULL;

                                printf("Calling "
                                       "rd_kafka_share_commit_sync()\n");
                                error = rd_kafka_share_commit_sync(
                                    rkshare, 30000, &partitions);
                                if (error) {
                                        fprintf(stderr,
                                                "%% Commit sync error: %s\n",
                                                rd_kafka_error_string(error));
                                        rd_kafka_error_destroy(error);
                                } else if (partitions) {
                                        int j;
                                        printf("Commit sync results "
                                               "(%d partitions):\n",
                                               partitions->cnt);
                                        for (j = 0; j < partitions->cnt; j++) {
                                                rd_kafka_topic_partition_t
                                                    *rktpar =
                                                        &partitions->elems[j];
                                                printf("  %s [%" PRId32
                                                       "]: %s\n",
                                                       rktpar->topic,
                                                       rktpar->partition,
                                                       rd_kafka_err2str(
                                                           rktpar->err));
                                        }
                                        rd_kafka_topic_partition_list_destroy(
                                            partitions);
                                } else {
                                        printf("No pending acks to commit\n");
                                }
                        }
                }
        }

        fprintf(stderr, "%% Closing share consumer\n");
        rd_kafka_share_consumer_close(rkshare);

        rd_kafka_share_destroy(rkshare);

        return 0;
}
