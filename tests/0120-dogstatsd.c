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

#include <string.h>

#include "test.h"

#include "rdkafka.h"  /* for Kafka driver */


struct sent_metrics {
    char *name;
    char *tags;
};

static int iterations;

static rd_kafka_resp_err_t on_sendto (rd_kafka_t *rk,
                                     int socket,
                                     struct sockaddr *addr,
                                     const char *message,
                                     void *ic_opaque) {
        char *token;
        char *saveptr;
        int i = 0;

        struct sent_metrics expected[] = {
                {"kafka.consumer.topic.batchsize.avg",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.min",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.max",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.sum",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.count",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.hdrsize",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p50",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p75",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p90",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p95",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p99",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchsize.p99_99",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.avg",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.min",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.max",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.sum",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.count",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.hdrsize",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p50",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p75",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p90",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p95",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p99",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.batchcount.p99_99",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.age",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.topic.metadata_age",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1,topic:0120"},
                {"kafka.consumer.messages",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.messages.size",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.messages.max",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.messages.size_max",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.metadata_cache",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.tx",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.tx_bytes",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.rx",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.rx_bytes",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.txmsgs",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.txmsg_bytes",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.rxmsgs",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
                {"kafka.consumer.rxmsg_bytes",
                 "consumer_group:42,name:0120_dogstatsd#consumer-1"},
        };
        int expected_metrics_size = 39;

        /* The topic may not be created yet */
        if (strncmp(message, expected[0].name, strlen(expected[0].name)) != 0)
            return RD_KAFKA_RESP_ERR_NO_ERROR;

        char *cpy_msg = rd_malloc(strlen(message));
        strcpy(cpy_msg, message);

        for (token = strtok_r(cpy_msg, ":", &saveptr);
                token != NULL; 
                token = strtok_r(NULL, ":", &saveptr)) {

                TEST_ASSERT(i < expected_metrics_size);

                TEST_ASSERT(strcmp(token, expected[i].name) == 0);
                token = strtok_r(NULL, "#", &saveptr);
                token = strtok_r(NULL, "\n", &saveptr);
                TEST_ASSERT(strcmp(token, expected[i].tags) == 0);

                i++;
        }

        rd_free(cpy_msg);
        
    iterations++;
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_kafka_resp_err_t on_new (rd_kafka_t *rk, const rd_kafka_conf_t *conf,
                                   void *ic_opaque,
                                   char *errstr, size_t errstr_size) {
        rd_kafka_interceptor_add_on_sendto(rk, __FILE__, on_sendto, NULL);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

int main_0120_dogstatsd (int argc, char **argv) {
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t *topic_conf;

    test_timeout_set(5);
    test_conf_init(&conf, &topic_conf, 10);

    /* Set up a global config object */
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "statistics.interval.ms", "100", NULL, 0);
    rd_kafka_conf_set(conf, "dogstatsd.endpoint", "localhost:8125", NULL, 0);

    rd_kafka_conf_interceptor_add_on_new(conf, __FILE__, on_new, NULL);

    /* Create kafka instance */
    rk = test_create_consumer("42", NULL, conf, NULL);
    rkt = rd_kafka_topic_new(rk, "0120", topic_conf);

    iterations = 0;
    while (iterations < 10)
        usleep(200 * 1000);

    test_consumer_close(rk);
    rd_kafka_destroy(rk);

    return 0;
}
