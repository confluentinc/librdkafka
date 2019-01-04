/*
* librdkafka - Apache Kafka C library
*
* Copyright (c) 2012-2015, Magnus Edenhill
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

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
* is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


/**
* Basic compression tests, with rather lacking verification.
*/


static void precreate_topic(rd_kafka_t *rk_p, const char *topic_name, const char* compression_type) {
        rd_kafka_queue_t *q = rd_kafka_queue_new(rk_p);
        rd_kafka_event_t *rkev;
        const rd_kafka_CreateTopics_result_t *res;
        const rd_kafka_topic_result_t **restopics;
        size_t restopic_cnt;
        rd_kafka_NewTopic_t *new_topic =
                rd_kafka_NewTopic_new(topic_name,
                        1,
                        1,
                        NULL, 0);
        /* Set compression type */
        rd_kafka_resp_err_t err = rd_kafka_NewTopic_set_config(
                new_topic, "compression.type", compression_type);
        TEST_ASSERT(!err, "%s", rd_kafka_err2str(err));

        /* Create topic */
        TEST_SAY(_C_MAG "Call CreateTopics for %s\n" _C_CLR, topic_name);
        rd_kafka_CreateTopics(rk_p, &new_topic, 1, NULL, q);

        /* Wait for CreateTopics result */
        do {
                rkev = rd_kafka_queue_poll(q, tmout_multip(20 * 1000));
                TEST_SAY("CreateTopics: got %s\n",
                        rd_kafka_event_name(rkev));
                if (rd_kafka_event_error(rkev))
                        TEST_SAY("%s: %s\n",
                                rd_kafka_event_name(rkev),
                                rd_kafka_event_error_string(rkev));
        } while (rd_kafka_event_type(rkev) !=
                RD_KAFKA_EVENT_CREATETOPICS_RESULT);
        /* Convert event to proper result */
        res = rd_kafka_event_CreateTopics_result(rkev);
        TEST_ASSERT(res, "expected CreateTopics_result, not %s",
                rd_kafka_event_name(rkev));

        /* Extract topics */
        restopics = rd_kafka_CreateTopics_result_topics(res, &restopic_cnt);
        TEST_ASSERT(restopic_cnt==1, "expected single topic from CreateTopics");
        {
                const rd_kafka_topic_result_t *terr = restopics[0];
                TEST_SAY("CreateTopics result: %s: %s: %s\n",
                        rd_kafka_topic_result_name(terr),
                        rd_kafka_err2name(rd_kafka_topic_result_error(terr)),
                        rd_kafka_topic_result_error_string(terr));
                if (rd_kafka_topic_result_error(terr) != RD_KAFKA_RESP_ERR_NO_ERROR)
                        TEST_FAIL(
                                "Expected %s, not %d: %s",
                                rd_kafka_err2name(RD_KAFKA_RESP_ERR_NO_ERROR),
                                rd_kafka_topic_result_error(terr),
                                rd_kafka_err2name(rd_kafka_topic_result_error(
                                        terr)));
        }
        rd_kafka_event_destroy(rkev);
        rd_kafka_NewTopic_destroy(new_topic);
        rd_kafka_queue_destroy(q);
}


int main_0017_compression(int argc, char **argv) {
        rd_kafka_t *rk_p, *rk_c;
        const int msg_cnt = 1000;
        int msg_base = 0;
        uint64_t testid;
#define GET_ARRAY_SIZE(x) (sizeof(x)/sizeof(x[0]))
        typedef struct {
                const char *codec;
                rd_bool_t pre_create_topic;
        } rd_codec_spec_t;
        const rd_codec_spec_t codecs[] = {
                { "none" },
#if WITH_ZLIB
                { "gzip" },
#endif
#if WITH_SNAPPY
                { "snappy"},
#endif
#if WITH_ZSTD
                { "zstd" },
                { "zstd", 
                  test_broker_version >= 
                  TEST_BRKVER(2, 1, 0, 0) ? rd_true : rd_false },
#endif
                { "lz4" }
        };

        char *topics[GET_ARRAY_SIZE(codecs)];
        const int32_t partition = 0;
        unsigned int i;
        int crc;

        testid = test_id_generate();

        /* Produce messages */
        rk_p = test_create_producer();
        for (i = 0; i < GET_ARRAY_SIZE(codecs); i++) {
                rd_kafka_topic_t *rkt_p;
                topics[i] = rd_strdup(test_mk_topic_name(codecs[i].codec, 1));

                /* Check if topic should be pre-created with specific 
                 * compression.type */
                if (codecs[i].pre_create_topic == rd_true) {
                        precreate_topic(rk_p, topics[i], codecs[i].codec);
                }

                TEST_SAY("Produce %d messages with %s compression to "
                        "topic %s\n",
                        msg_cnt, codecs[i].codec, topics[i]);
                rkt_p = test_create_producer_topic(rk_p, topics[i],
                        "compression.codec", codecs[i].codec, NULL);

                /* Produce small message that will not decrease with
                 * compression (issue #781) */
                test_produce_msgs(rk_p, rkt_p, testid, partition,
                                  msg_base + (partition*msg_cnt), 1,
                                  NULL, 5);

                /* Produce standard sized messages */
                test_produce_msgs(rk_p, rkt_p, testid, partition,
                                  msg_base + (partition*msg_cnt) + 1, msg_cnt-1,
                                  NULL, 512);
                rd_kafka_topic_destroy(rkt_p);
        }

        rd_kafka_destroy(rk_p);


        /* restart timeout (mainly for helgrind use since it is very slow) */
        test_timeout_set(30);

        /* Consume messages: Without and with CRC checking */
        for (crc = 0 ; crc < 2 ; crc++) {
                const char *crc_tof = crc ? "true":"false";
                rd_kafka_conf_t *conf;

                test_conf_init(&conf, NULL, 0);
                test_conf_set(conf, "check.crcs", crc_tof);

                rk_c = test_create_consumer(NULL, NULL, conf, NULL);

                for (i = 0; i < GET_ARRAY_SIZE(codecs); i++) {
                        rd_kafka_topic_t *rkt_c = rd_kafka_topic_new(rk_c,
                                                                     topics[i],
                                                                     NULL);

                        TEST_SAY("Consume %d messages from topic %s (crc=%s)\n",
                                 msg_cnt, topics[i], crc_tof);
                        /* Start consuming */
                        test_consumer_start(codecs[i].codec, rkt_c, partition,
                                            RD_KAFKA_OFFSET_BEGINNING);

                        /* Consume messages */
                        test_consume_msgs(
                                codecs[i].codec, rkt_c, testid, partition,
                                /* Use offset 0 here, which is wrong, should
                                 * be TEST_NO_SEEK, but it exposed a bug
                                 * where the Offset query was postponed
                                 * till after the seek, causing messages
                                 * to be replayed. */
                                0,
                                msg_base, msg_cnt, 1 /* parse format */);

                        test_consumer_stop(codecs[i].codec, rkt_c, partition);

                        rd_kafka_topic_destroy(rkt_c);
                }

                rd_kafka_destroy(rk_c);
        }

        for (i = 0 ; i < GET_ARRAY_SIZE(codecs); i++)
                rd_free(topics[i]);


        return 0;
}
