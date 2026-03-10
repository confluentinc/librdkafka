/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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
 * Share consumer example using rd_kafka_share_commit_async() to
 * explicitly acknowledge and commit records between polls.
 *
 * Usage:
 *   share_consumer_commit_async <broker> <group.id> <topic1> [topic2 ...]
 *
 * This example demonstrates:
 *  - Consuming records with rd_kafka_share_consume_batch()
 *  - Explicitly acknowledging individual records with
 *    rd_kafka_share_acknowledge_type() using ACCEPT or RELEASE
 *    (RELEASE at ~50% rate to simulate redelivery)
 *  - Committing acknowledgements asynchronously with
 *    rd_kafka_share_commit_async() at ~10% rate mid-batch
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

                        if (rkm->err) {
                                fprintf(stderr, "%% Consumer error: %d: %s\n",
                                        rkm->err, rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Randomly RELEASE ~50% of messages,
                         * ACCEPT the rest. */
                        rd_kafka_share_ack_type_t ack_type =
                            ((rand() % 100) < 50)
                                ? RD_KAFKA_SHARE_ACK_TYPE_RELEASE
                                : RD_KAFKA_SHARE_ACK_TYPE_ACCEPT;

                        printf("Message on %s [%" PRId32 "] at offset %" PRId64
                               " -> %s",
                               rd_kafka_topic_name(rkm->rkt), rkm->partition,
                               rkm->offset,
                               ack_type == RD_KAFKA_SHARE_ACK_TYPE_ACCEPT
                                   ? "ACCEPT"
                                   : "RELEASE");

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
                         * exercise async commit mid-batch. */
                        if (run && (rand() % 100) < 10) {
                                printf(
                                    "Calling "
                                    "rd_kafka_share_commit_async()\n");
                                error = rd_kafka_share_commit_async(rkshare);
                                if (error) {
                                        fprintf(stderr,
                                                "%% Commit async "
                                                "error: %s\n",
                                                rd_kafka_error_string(error));
                                        rd_kafka_error_destroy(error);
                                }
                        }
                }
        }

        fprintf(stderr, "%% Closing share consumer\n");
        rd_kafka_share_consumer_close(rkshare);

        rd_kafka_share_destroy(rkshare);

        return 0;
}
