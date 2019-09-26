/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013, Magnus Edenhill
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
 * Tests the extended broker metadata API.
 */

#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */



int main_0105_broker_metadata (int argc, char **argv) {
	char topic[64];
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	const rd_kafka_metadata_t *md = NULL;
	rd_kafka_resp_err_t err;
	int i;

	/* Generate unique topic name */
	test_conf_init(&conf, &topic_conf, 10);

	rd_snprintf(topic, sizeof(topic), "rdkafkatest1_unk_%x%x",
		 rand(), rand());

	/* Create kafka instance */
	rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  	  strerror(errno));

	
	err = rd_kafka_metadata(rk, 0, rkt, &md, 5000);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		TEST_FAIL("Failed to get metadata: %s\n",
			  	  strerror(errno));

	if (!md) {
		TEST_FAIL("Metadata was not assigned\n");
	}

	TEST_SAY("Broker count: %d\n", md->broker_cnt);
	if (md->broker_cnt <= 0) {
		// TODO verify broker count
		TEST_FAIL("Invalid broker count: %"PRId32"\n",
				  md->broker_cnt);
	}

	for (i = 0; i < md->broker_cnt; i++) {
		const rd_kafka_metadata_broker_extended_t *b;
		const char *host, *rack;
		int port;
		int32_t bid;

		b = rd_kafka_metadata_broker_get(md, i);
		if (!b) {
			TEST_FAIL("broker (%d/%d) return null\n",
					  i, md->broker_cnt);
		}

		bid = rd_kafka_metadata_broker_id(b);
		host = rd_kafka_metadata_broker_host(b);
		port = rd_kafka_metadata_broker_port(b);
		rack = rd_kafka_metadata_broker_rack(b);

		TEST_SAY("Broker (%d/%d): %s:%d (Rack: %s) Node %d\n",
				 i, md->broker_cnt, host, port, rack, bid);

		if (strcmp(host, "localhost") != 0) {
			TEST_FAIL("unexpected host: %s\n", host);
		}
		if (bid <= 0) {
			TEST_FAIL("unexpected broker id: %d\n", bid);
		}
		if (port <= 0) {
			TEST_FAIL("unexpected port: %d\n", port);
		}
		if (rack) {
			TEST_FAIL("expected rack to be NULL");
		}

		// ABI backwards-compatibility checks
		if (md->brokers[i].id != bid) {
			TEST_FAIL("ABI compatibility check failed: "
					  "id %d != %d\n",
					  md->brokers[i].id, bid);
		}
		if (strcmp(md->brokers[i].host, host) != 0) {
			TEST_FAIL("ABI compatibility check failed: "
					  "host \"%s\" != \"%s\"\n",
					  md->brokers[i].host, host);
		}
		if (md->brokers[i].port != port) {
			TEST_FAIL("ABI compatibility check failed: "
					  "port %d != %d\n",
					  md->brokers[i].port, port);
		}
	}

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);

	return 0;
}
