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
        ssize_t token;
        ssize_t offset;
        char *saveptr;
        char common_tags[128] = "";
        char topic_tags[128] = "";
        int i = 0;

        /* The name is 0120_dogstatsd#consumer-??.
           The id is not consistent between 2 tests run. */
        rd_snprintf(common_tags, 128, "consumer_group:42,name:%s",
                    rd_kafka_name(rk));
        rd_snprintf(topic_tags, 128, "%s,topic:0120", common_tags);

        struct sent_metrics expected[] = {
                {"kafka.consumer.topic.batchsize.avg", topic_tags},
                {"kafka.consumer.topic.batchsize.min", topic_tags},
                {"kafka.consumer.topic.batchsize.max", topic_tags},
                {"kafka.consumer.topic.batchsize.sum", topic_tags},
                {"kafka.consumer.topic.batchsize.count", topic_tags},
                {"kafka.consumer.topic.batchsize.hdrsize", topic_tags},
                {"kafka.consumer.topic.batchsize.p50", topic_tags},
                {"kafka.consumer.topic.batchsize.p75", topic_tags},
                {"kafka.consumer.topic.batchsize.p90", topic_tags},
                {"kafka.consumer.topic.batchsize.p95", topic_tags},
                {"kafka.consumer.topic.batchsize.p99", topic_tags},
                {"kafka.consumer.topic.batchsize.p99_99", topic_tags},
                {"kafka.consumer.topic.batchcount.avg", topic_tags},
                {"kafka.consumer.topic.batchcount.min", topic_tags},
                {"kafka.consumer.topic.batchcount.max", topic_tags},
                {"kafka.consumer.topic.batchcount.sum", topic_tags},
                {"kafka.consumer.topic.batchcount.count", topic_tags},
                {"kafka.consumer.topic.batchcount.hdrsize", topic_tags},
                {"kafka.consumer.topic.batchcount.p50", topic_tags},
                {"kafka.consumer.topic.batchcount.p75", topic_tags},
                {"kafka.consumer.topic.batchcount.p90", topic_tags},
                {"kafka.consumer.topic.batchcount.p95", topic_tags},
                {"kafka.consumer.topic.batchcount.p99", topic_tags},
                {"kafka.consumer.topic.batchcount.p99_99", topic_tags},
                {"kafka.consumer.topic.age", topic_tags},
                {"kafka.consumer.topic.metadata_age", topic_tags},
                {"kafka.consumer.messages", common_tags},
                {"kafka.consumer.messages.size", common_tags},
                {"kafka.consumer.messages.max", common_tags},
                {"kafka.consumer.messages.size_max", common_tags},
                {"kafka.consumer.metadata_cache", common_tags},
                {"kafka.consumer.tx", common_tags},
                {"kafka.consumer.tx_bytes", common_tags},
                {"kafka.consumer.rx", common_tags},
                {"kafka.consumer.rx_bytes", common_tags},
                {"kafka.consumer.txmsgs", common_tags},
                {"kafka.consumer.txmsg_bytes", common_tags},
                {"kafka.consumer.rxmsgs", common_tags},
                {"kafka.consumer.rxmsg_bytes", common_tags},
        };
        int expected_metrics_size = 39;

        /* The topic may not be created yet */
        if (strncmp(message, expected[0].name, strlen(expected[0].name)) != 0)
            return RD_KAFKA_RESP_ERR_NO_ERROR;

        token = 0;
        while (message[token] != '\0') {
                /* Read each line and check the name and the tags are correct */
                TEST_ASSERT(i < expected_metrics_size);
                offset = token;

                while (message[token] != ':') {
                        TEST_ASSERT(message[token] != '\0');
                        token++;
                }
                TEST_ASSERT(strncmp(message + offset, expected[i].name,
                            token - offset - 1) == 0);
                while (message[token] != '#') {
                        TEST_ASSERT(message[token] != '\0');
                        token++;
                }
                offset = token + 1;
                while (message[token] != '\n') {
                        TEST_ASSERT(message[token] != '\0');
                        token++;
                }
                TEST_ASSERT(strncmp(message + offset, expected[i].tags,
                            token - offset - 1) == 0);

                i++;
                token++; /* Next line */
        }
 
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

        test_conf_init(&conf, &topic_conf, 10);

        rd_kafka_conf_set(conf, "statistics.interval.ms", "100", NULL, 0);
        rd_kafka_conf_set(conf, "dogstatsd.endpoint", "localhost:8125",
                          NULL, 0);

        rd_kafka_conf_interceptor_add_on_new(conf, __FILE__, on_new, NULL);

        /* Create kafka instance */
        rk = test_create_consumer("42", NULL, conf, NULL);
        rkt = rd_kafka_topic_new(rk, "0120", topic_conf);

        iterations = 0;
        while (iterations < 10)
                rd_usleep(200 * 1000, NULL);

        test_consumer_close(rk);
        rd_kafka_destroy(rk);

        return 0;
}
