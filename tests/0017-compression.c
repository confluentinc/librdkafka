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


int main_0017_compression(int argc, char **argv) {
	rd_kafka_t *rk_p, *rk_c;
	const int msg_cnt = 1000;
	int msg_base = 0;
	uint64_t testid;
#define CODEC_CNT 3
	const char *codecs[CODEC_CNT+1] = {
		"none",
#if WITH_ZLIB
		"gzip",
#endif
#if WITH_SNAPPY
		"snappy",
#endif
		NULL
	};
	const char *topics[CODEC_CNT];
	const int32_t partition = 0;
	int i;

	testid = test_id_generate();

	/* Produce messages */
	rk_p = test_create_producer();
	for (i = 0; codecs[i] != NULL ; i++) {
		rd_kafka_topic_t *rkt_p;

		topics[i] = test_mk_topic_name(codecs[i], 1);
		TEST_SAY("Produce %d messages with %s compression to "
			 "topic %s\n",
			msg_cnt, codecs[i], topics[i]);
		rkt_p = test_create_producer_topic(rk_p, topics[i],
			"compression.codec", codecs[i], NULL);

		test_produce_msgs(rk_p, rkt_p, testid, partition,
				  msg_base + (partition*msg_cnt), msg_cnt,
				  NULL, 0);
		rd_kafka_topic_destroy(rkt_p);
	}

	rd_kafka_destroy(rk_p);


	/* Consume messages */
	rk_c = test_create_consumer(NULL, NULL, NULL, NULL);

	for (i = 0; codecs[i] != NULL ; i++) {
		rd_kafka_topic_t *rkt_c = rd_kafka_topic_new(rk_c,
							     topics[i], NULL);
		TEST_SAY("Consume %d messages from topic %s\n",
			msg_cnt, topics[i]);
		/* Start consuming */
		test_consumer_start(codecs[i], rkt_c, partition,
				    RD_KAFKA_OFFSET_BEGINNING);

		/* Consume messages */
		test_consume_msgs(codecs[i], rkt_c, testid, partition, 0,
				  msg_base, msg_cnt, 1 /* parse format */);
		rd_kafka_topic_destroy(rkt_c);
	}

	rd_kafka_destroy(rk_c);

	return 0;
}
