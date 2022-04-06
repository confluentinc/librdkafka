/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019, Magnus Edenhill
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
 * Simple high-level balanced Apache Kafka consumer
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
//#include <librdkafka/rdkafka.h>
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

static int rd_kafka_group_member_cmp(const void *_a, const void *_b) {
        const rd_kafka_group_member_t *a = (const rd_kafka_group_member_t *)_a;
        const rd_kafka_group_member_t *b = (const rd_kafka_group_member_t *)_b;

        int res = strcmp(a->rkgm_userdata->data, b->rkgm_userdata->data);

        if (res != 0) {
                return res;
        }

        return strcmp(a->rkgm_member_id, b->rkgm_member_id);
}

static rd_kafka_resp_err_t
rd_kafka_custom_assignor_assign_cb(rd_kafka_t *rk,
                                   void *opaque,
                                   const char *member_id,
                                   const rd_kafka_metadata_t *metadata,
                                   rd_kafka_group_member_t *members,
                                   size_t member_cnt,
                                   rd_kafka_assignor_topic_t *eligible_topics,
                                   size_t eligible_topic_cnt,
                                   char *errstr,
                                   size_t errstr_size) {
        fprintf(stderr, "Rebalancing Leader \"%s\"\n", member_id);

        if (eligible_topic_cnt != 1) {
                fprintf(stderr, "Invalid number of topics %zu\n",
                        eligible_topic_cnt);
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }

        rd_kafka_assignor_topic_t *eligible_topic = &eligible_topics[0];

        if (member_cnt == 0) {
                fprintf(stderr, "Invalid number of members %zu\n", member_cnt);
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }

        qsort(members, member_cnt, sizeof(*members), rd_kafka_group_member_cmp);

        /* Take member with the highest priority. */
        rd_kafka_group_member_t *member = &members[0];

        /* Assign all eligible partitions of a given topic.
         */
        int p;
        for (p = 0; p < eligible_topic->metadata->partition_cnt; p++) {
                int32_t partition = eligible_topic->metadata->partitions[p].id;

                fprintf(stderr,
                        "Member \"%s\"%s: "
                        "assigned topic %s partition %d (metadata: %.*s)\n",
                        member->rkgm_member_id,
                        strcmp(member_id, member->rkgm_member_id) == 0 ? " (me)"
                                                                       : "",
                        eligible_topic->metadata->topic, partition,
                        (int)member->rkgm_userdata->len - 1,
                        (char *)member->rkgm_userdata->data);

                rd_kafka_topic_partition_list_add(
                    member->rkgm_assignment, eligible_topic->metadata->topic,
                    partition);
        }


        return 0;
}

static rd_kafka_member_userdata_serialized_t *
rd_kafka_custom_assignor_get_empty_userdata(
    void *opaque,
    const char *member_id,
    const rd_kafka_topic_partition_list_t *owned_partitions,
    int32_t rkcg_generation_id) {
        const char *prio = opaque;
        return rd_kafka_member_userdata_serialized_new(prio, strlen(prio) + 1);
}


int main(int argc, char **argv) {
        rd_kafka_t *rk;          /* Consumer instance handle */
        rd_kafka_conf_t *conf;   /* Temporary configuration object */
        rd_kafka_resp_err_t err; /* librdkafka API error code */
        char errstr[512];        /* librdkafka API error reporting buffer */
        const char *brokers;     /* Argument: broker list */
        const char *groupid;     /* Argument: Consumer group id */
        char *topic;             /* Argument: topic to subscribe to */
        char *prio;              /* Argument: subscriber priority */

        /*
         * Argument validation
         */
        if (argc < 5) {
                fprintf(stderr,
                        "%% Usage: "
                        "%s <broker> <group.id> <topic> <prio>\n",
                        argv[0]);
                return 1;
        }

        brokers = argv[1];
        groupid = argv[2];
        topic   = argv[3];
        prio    = argv[4];

        /*
         * Register custom assigned
         */
        err = rd_kafka_assignor_register(
            "custom", RD_KAFKA_REBALANCE_PROTOCOL_EAGER,
            rd_kafka_custom_assignor_assign_cb,
            rd_kafka_custom_assignor_get_empty_userdata, prio);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "failed to register assignor: %s\n",
                        rd_kafka_err2str(err));
                return 1;
        }

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
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        /* Set the consumer group id.
         * All consumers sharing the same group id will join the same
         * group, and the subscribed topic' partitions will be assigned
         * according to the partition.assignment.strategy
         * (consumer config property) to the consumers in the group. */
        if (rd_kafka_conf_set(conf, "group.id", groupid, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        /* If there is no previously committed offset for a partition
         * the auto.offset.reset strategy will be used to decide where
         * in the partition to start fetching messages.
         * By setting this to earliest the consumer will read all messages
         * in the partition if there was no previously committed offset. */
        if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        /* Enable client debug. Most useful for us is "assignor,cgrp"
         *
         */
        if (getenv("DEBUG") != NULL) {
                if (rd_kafka_conf_set(conf, "debug", getenv("DEBUG"), errstr,
                                      sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                        fprintf(stderr, "%s\n", errstr);
                        rd_kafka_conf_destroy(conf);
                        return 1;
                }
        }

        /* Set assignor registered above.
         */
        if (rd_kafka_conf_set(conf, "partition.assignment.strategy", "custom",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        /*
         * Create consumer instance.
         *
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new consumer: %s\n",
                        errstr);
                return 1;
        }

        conf = NULL; /* Configuration object is now owned, and freed,
                      * by the rd_kafka_t instance. */


        /* Redirect all messages from per-partition queues to
         * the main queue so that messages can be consumed with one
         * call from all assigned partitions.
         *
         * The alternative is to poll the main queue (for events)
         * and each partition queue separately, which requires setting
         * up a rebalance callback and keeping track of the assignment:
         * but that is more complex and typically not recommended. */
        rd_kafka_poll_set_consumer(rk);


        rd_kafka_topic_partition_list_t *subscription;
        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          /* the partition is ignored
                                           * by subscribe() */
                                          RD_KAFKA_PARTITION_UA);
        err = rd_kafka_subscribe(rk, subscription);
        if (err) {
                fprintf(stderr, "%% Failed to subscribe to topic: %s\n",
                        rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(subscription);
                rd_kafka_destroy(rk);
                return 1;
        }
        fprintf(stderr,
                "%% Subscribed to %s topic, "
                "waiting for rebalance and messages...\n",
                topic);
        rd_kafka_topic_partition_list_destroy(subscription);


        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        /* Subscribing to topic will trigger a group rebalance
         * which may take some time to finish, but there is no need
         * for the application to handle this idle period in a special way
         * since a rebalance may happen at any time.
         * Start polling for messages. */

        while (run) {
                rd_kafka_message_t *rkm;

                rkm = rd_kafka_consumer_poll(rk, 100);
                if (!rkm)
                        continue; /* Timeout: no message within 100ms,
                                   *  try again. This short timeout allows
                                   *  checking for `run` at frequent intervals.
                                   */

                /* consumer_poll() will return either a proper message
                 * or a consumer error (rkm->err is set). */
                if (rkm->err) {
                        /* Consumer errors are generally to be considered
                         * informational as the consumer will automatically
                         * try to recover from all types of errors. */
                        fprintf(stderr, "%% Consumer error: %s\n",
                                rd_kafka_message_errstr(rkm));
                        rd_kafka_message_destroy(rkm);
                        continue;
                }

                /* Proper message. */
                printf("Message on %s [%" PRId32 "] at offset %" PRId64 ":\n",
                       rd_kafka_topic_name(rkm->rkt), rkm->partition,
                       rkm->offset);

                /* Print the message key. */
                if (rkm->key && is_printable(rkm->key, rkm->key_len))
                        printf(" Key: %.*s\n", (int)rkm->key_len,
                               (const char *)rkm->key);
                else if (rkm->key)
                        printf(" Key: (%d bytes)\n", (int)rkm->key_len);

                /* Print the message value/payload. */
                if (rkm->payload && is_printable(rkm->payload, rkm->len))
                        printf(" Value: %.*s\n", (int)rkm->len,
                               (const char *)rkm->payload);
                else if (rkm->payload)
                        printf(" Value: (%d bytes)\n", (int)rkm->len);

                rd_kafka_message_destroy(rkm);
        }


        /* Close the consumer: commit final offsets and leave the group. */
        fprintf(stderr, "%% Closing consumer\n");
        rd_kafka_consumer_close(rk);


        /* Destroy the consumer */
        rd_kafka_destroy(rk);

        return 0;
}
