/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2017, Magnus Edenhill
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
 * Tests messages are produced in order.
 */


#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


static int stats_count = 0;

/**
 * Handle stats
 */
static void handle_stats (rd_kafka_event_t *rkev) {
    const char *stats_json = NULL;
    stats_json = rd_kafka_event_stats(rkev);
    if (stats_json != NULL) {
        TEST_SAY("Stats: %s\n", stats_json);
        stats_count++;
    } else {
        TEST_FAIL("Stats: failed to get stats\n");
    }
}

int main_0062_stats_event (int argc, char **argv) {
    int partition = 0;
    int r;
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    rd_kafka_conf_t *conf;
    char msg[128];
    int msgcnt = 1000;
    int i;
    test_timing_t t_produce, t_delivery;
    rd_kafka_queue_t *eventq;

    test_conf_init(NULL, NULL, 10);

    /* Set up a global config object */
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf,"statistics.interval.ms", "100", NULL, 0);

    rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_STATS);

    /* Create kafka instance */
    rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

    eventq = rd_kafka_queue_get_main(rk);

    rkt = rd_kafka_topic_new(rk, test_mk_topic_name("0062", 0), NULL);
    if (!rkt)
        TEST_FAIL("Failed to create topic: %s\n", rd_strerror(errno));

    /* Produce messages */
    TIMING_START(&t_produce, "PRODUCE");
    for (i = 0 ; i < msgcnt ; i++) {
        int *msgidp = malloc(sizeof(*msgidp));
        *msgidp = i;
        rd_snprintf(msg, sizeof(msg), "%s test message #%i", argv[0], i);
        r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                             msg, strlen(msg), NULL, 0, msgidp);
        if (r == -1)
            TEST_FAIL("Failed to produce message #%i: %s\n",
                      i, rd_strerror(errno));
    }
    TIMING_STOP(&t_produce);
    TEST_SAY("Produced %i messages, waiting for stats event\n", msgcnt);

    /* Wait for stats event */
    TIMING_START(&t_delivery, "STATS_EVENT");
    while (rd_kafka_outq_len(rk) > 0) {
        rd_kafka_event_t *rkev;
        rkev = rd_kafka_queue_poll(eventq, 100);
        switch (rd_kafka_event_type(rkev))
        {
        case RD_KAFKA_EVENT_STATS:
            TEST_SAY("%s event\n", rd_kafka_event_name(rkev));
            handle_stats(rkev);
            break;
        default:
            TEST_SAY("Ignore event: %s\n", rd_kafka_event_name(rkev));
            break;
        }
        rd_kafka_event_destroy(rkev);
        if (stats_count > 0)
            break;
    }
    TIMING_STOP(&t_delivery);

    if (stats_count == 0)
        TEST_FAIL("Fail to receive any stats events\n");

    rd_kafka_queue_destroy(eventq);

    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);

    /* Destroy rdkafka instance */
    TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
    rd_kafka_destroy(rk);

    return 0;
}
