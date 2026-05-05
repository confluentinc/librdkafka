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
 * Share consumer example demonstrating wakeup usage.
 *
 * This example shows how to use rd_kafka_share_wakeup() to gracefully
 * shutdown a share consumer from a signal handler.
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"

static rd_kafka_share_t *g_rkshare = NULL;
static volatile sig_atomic_t g_running = 1;


/**
 * @brief Signal handler for graceful shutdown.
 */
static void sigint_handler(int sig) {
        fprintf(stderr, "\nReceived signal %d, shutting down...\n", sig);
        g_running = 0;

        /* Wake up the blocking consume_batch call.
         * This is thread-safe and can be called from the signal handler. */
        if (g_rkshare)
                rd_kafka_share_wakeup(g_rkshare);
}


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_error_t *err;
        char errstr[512];
        const char *brokers     = "localhost:9092";
        const char *group_id    = "example-share-group";
        const char *topics      = "test-topic";
        int msg_cnt             = 0;

        if (argc > 1)
                brokers = argv[1];
        if (argc > 2)
                group_id = argv[2];
        if (argc > 3)
                topics = argv[3];

        /*
         * Create Kafka client configuration
         */
        conf = rd_kafka_conf_new();

        /* Set bootstrap broker(s) */
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }

        /* Set share group ID */
        if (rd_kafka_conf_set(conf, "group.id", group_id, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }

        /*
         * Create share consumer instance
         */
        g_rkshare = rd_kafka_share_consumer_new(conf, errstr,
                                                 sizeof(errstr));
        if (!g_rkshare) {
                fprintf(stderr, "Failed to create share consumer: %s\n",
                        errstr);
                return 1;
        }

        conf = NULL; /* Configuration object is now owned by g_rkshare */

        /* Subscribe to topic(s) */
        err = rd_kafka_share_subscribe(g_rkshare, topics);
        if (err) {
                fprintf(stderr, "Failed to subscribe to %s: %s\n", topics,
                        rd_kafka_error_string(err));
                rd_kafka_error_destroy(err);
                goto cleanup;
        }

        fprintf(stderr,
                "Share consumer subscribed to %s\n"
                "Share group: %s\n"
                "Press Ctrl-C to exit\n\n",
                topics, group_id);

        /* Set up signal handler for graceful shutdown */
        signal(SIGINT, sigint_handler);
        signal(SIGTERM, sigint_handler);

        /*
         * Main consumption loop
         */
        while (g_running) {
                rd_kafka_message_t *rkmessages[100];
                size_t msg_batch_size;

                /* Poll for messages with 1 second timeout.
                 * If wakeup is called from the signal handler, this will
                 * return with RD_KAFKA_RESP_ERR__WAKEUP. */
                err = rd_kafka_share_consume_batch(g_rkshare, 1000,
                                                   rkmessages, &msg_batch_size);

                if (err) {
                        rd_kafka_resp_err_t code = rd_kafka_error_code(err);

                        if (code == RD_KAFKA_RESP_ERR__WAKEUP) {
                                /* Woken up by signal handler, check
                                 * g_running */
                                fprintf(stderr,
                                        "Woken up, checking shutdown flag...\n");
                                rd_kafka_error_destroy(err);
                                continue;
                        }

                        /* Other error */
                        fprintf(stderr, "Error consuming messages: %s\n",
                                rd_kafka_error_string(err));
                        rd_kafka_error_destroy(err);
                        break;
                }

                /* Process messages */
                if (msg_batch_size > 0) {
                        fprintf(stderr, "Consumed %zu messages:\n",
                                msg_batch_size);

                        for (size_t i = 0; i < msg_batch_size; i++) {
                                rd_kafka_message_t *rkmessage = rkmessages[i];

                                if (rkmessage->err) {
                                        fprintf(stderr,
                                                "  Message error: %s\n",
                                                rd_kafka_message_errstr(
                                                    rkmessage));
                                } else {
                                        printf("  [%s] offset %ld: %.*s\n",
                                               rd_kafka_topic_name(
                                                   rkmessage->rkt),
                                               rkmessage->offset,
                                               (int)rkmessage->len,
                                               (char *)rkmessage->payload);
                                        msg_cnt++;
                                }

                                rd_kafka_message_destroy(rkmessage);
                        }
                }
        }

        fprintf(stderr, "\nTotal messages consumed: %d\n", msg_cnt);

cleanup:
        fprintf(stderr, "Closing share consumer...\n");

        /* Close the consumer. This will leave the share group and
         * commit any pending acknowledgements. */
        err = rd_kafka_share_consumer_close(g_rkshare);
        if (err) {
                fprintf(stderr, "Failed to close share consumer: %s\n",
                        rd_kafka_error_string(err));
                rd_kafka_error_destroy(err);
        }

        /* Destroy consumer instance */
        rd_kafka_share_destroy(g_rkshare);
        g_rkshare = NULL;

        fprintf(stderr, "Done\n");

        return 0;
}
