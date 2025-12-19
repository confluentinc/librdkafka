/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Magnus Edenhill
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

#include "test.h"

static int prod_msg_remains = 0;

/**
 * Delivery reported callback.
 * Called for each message once to signal its delivery status.
 */
static void dr_cb(rd_kafka_t *rk,
                  void *payload,
                  size_t len,
                  rd_kafka_resp_err_t err,
                  void *opaque,
                  void *msg_opaque) {
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                TEST_FAIL("Message delivery failed: %s\n",
                          rd_kafka_err2str(err));

        if (prod_msg_remains == 0)
                TEST_FAIL("Too many messages delivered (prod_msg_remains %i)",
                          prod_msg_remains);

        prod_msg_remains--;
}

static void produce_messages(rd_kafka_t *rk,
                             rd_kafka_topic_t *rkt,
                             uint64_t testid,
                             int msgcnt) {
        int r;
        char msg[128];
        int failcnt = 0;
        int i;
        rd_kafka_message_t *rkmessages;
        int32_t partition = 0;
        int msgid = 0;

        /* Create messages. */
        prod_msg_remains = msgcnt;
        rkmessages = calloc(sizeof(*rkmessages), msgcnt);

        for (i = 0; i < msgcnt; i++) {
                rd_snprintf(msg, sizeof(msg),
                            "testid=%" PRIu64 ", partition=%i, msg=%i",
                            testid, (int)partition, msgid);
                rkmessages[i].payload = rd_strdup(msg);
                rkmessages[i].len = strlen(msg);
                msgid++;
        }

        TEST_SAY("Start produce to partition %i: msgs #%d..%d\n",
                 (int)partition, 0, msgid);

        /* Produce batch for this partition */
        r = rd_kafka_produce_batch(rkt, partition, RD_KAFKA_MSG_F_FREE,
                                   rkmessages, msgcnt);

        if (r == -1)
                TEST_FAIL("Failed to produce batch for partition %i: %s",
                          (int)partition,
                          rd_kafka_err2str(rd_kafka_last_error()));

        /* Scan through messages to check for errors. */
        for (i = 0; i < msgcnt; i++) {
                if (rkmessages[i].err) {
                        failcnt++;
                        if (failcnt < 100)
                                TEST_SAY("Message #%i failed: %s\n", i,
                                         rd_kafka_err2str(rkmessages[i].err));
                }
        }

        /* All messages should've been produced. */
        if (r < msgcnt) {
                TEST_SAY("Not all messages were accepted by produce_batch(): "
                         "%i < %i\n", r, msgcnt);

                if (msgcnt - r != failcnt)
                        TEST_SAY("Discrepancy between failed messages (%i) "
                                 "and return value %i (%i - %i)\n",
                                 failcnt, msgcnt - r, msgcnt, r);
                TEST_FAIL("%i/%i messages failed\n", msgcnt - r, msgcnt);
        }

        TEST_SAY("Produced %i messages to partition %i, waiting for deliveries\n",
                 r, partition);

        free(rkmessages);

        /* Wait for messages to be delivered */
        while (rd_kafka_outq_len(rk) > 0)
                rd_kafka_poll(rk, 100);

        if (prod_msg_remains != 0)
                TEST_FAIL("Still waiting for %i messages to be produced",
                          prod_msg_remains);

        TEST_SAY("All messages delivered successfully\n");
}

static void do_test_produce_with_3des_cipher()
{
        uint64_t testid;
        const char *topic;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_conf_t *topic_conf;
        rd_kafka_topic_t *rkt;
        char errstr[512];
        rd_kafka_t *rk;

        testid = test_id_generate();
        test_conf_init(NULL, NULL, 20);
        topic = test_mk_topic_name("0155", 1);
        TEST_SAY("Topic %s, testid %" PRIu64 "\n", topic, testid);
        test_conf_init(&conf, &topic_conf, 20);

        // This cipher is valid in FIPS 140-2 and invalid in FIPS 140-3
        test_conf_set(conf, "ssl.cipher.suites",
                          "ECDHE-RSA-DES-CBC3-SHA");

        rd_kafka_conf_set_dr_cb(conf, dr_cb);

        /* Make sure all replicas are in-sync after producing */
        rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "-1",
                                    errstr, sizeof(errstr));

        /* Set default topic config */
        rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
        {
                if (strstr(errstr, "SSL_CTX_set_cipher_list error") &&
                    strstr(errstr, "SSL routines::no cipher match")) {
                        // In FIPS 140-3 mode, we are expected to reach here
                        TEST_SAY("Failed to create rdkafka instance with expected cipher not found error: %s\n", errstr);
                        return;
                }
                TEST_FAIL("Failed to create rdkafka instance with unknown error: %s\n", errstr);
        }

        TEST_SAY("Created kafka instance %s\n", rd_kafka_name(rk));
        rkt = rd_kafka_topic_new(rk, topic, NULL);
        if (!rkt)
                TEST_FAIL("Failed to create topic: %s\n", rd_strerror(errno));

        /* Produce 100 messages to partition 0 */
        produce_messages(rk, rkt, testid, 100);

        /* Destroy topic */
        rd_kafka_topic_destroy(rkt);

        /* Destroy rdkafka instance */
        TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
        rd_kafka_destroy(rk);

        // In FIPS 140-2, we are expected to reach here
        TEST_FAIL("All messages were produced successfully\n");
}



int main_0155_ssl_cipher_fips(int argc, char **argv) {
        /* Test that non-FIPS compliant cipher suites are rejected
         * when FIPS mode is enabled */
        do_test_produce_with_3des_cipher();
        return 0;
}