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
 * Tests multiple rd_kafka_t object creations and destructions.
 * Issue #20
 */

#define _GNU_SOURCE
#include <sys/time.h>
#include <sys/resource.h>

#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */

int main (int argc, char **argv) {
	char *topic = "rdkafkatest1";
	int partition = RD_KAFKA_PARTITION_UA; /* random */
	int i;
	const int NUM_ITER = 100;
	struct rlimit rlim = {};

	/*
	 * Put some limits to catch bad cleanups by librdkafka (issue #20)
	 */
	
	/* File descriptors. One or two per broker. */
	rlim.rlim_cur = rlim.rlim_max = NUM_ITER * 5;
	setrlimit(RLIMIT_NOFILE, &rlim); /* best effort, fails under valgrind */

	TEST_SAY("Creating and destroying %i kafka instances\n", NUM_ITER);

	/* Create, use and destroy NUM_ITER kafka instances. */
	for (i = 0 ; i < NUM_ITER ; i++) {
		rd_kafka_t *rk;
		rd_kafka_topic_t *rkt;
		rd_kafka_conf_t *conf;
		rd_kafka_topic_conf_t *topic_conf;
		char errstr[512];
		char msg[128];

		test_conf_init(&conf, &topic_conf, 30);

		rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
				  errstr, sizeof(errstr));
		if (!rk)
			TEST_FAIL("Failed to create rdkafka instance #%i: %s\n",
				  i, errstr);

		rkt = rd_kafka_topic_new(rk, topic, topic_conf);
		if (!rkt)
			TEST_FAIL("Failed to create topic for "
				  "rdkafka instance #%i: %s\n",
				  i, strerror(errno));

		snprintf(msg, sizeof(msg), "%s test message for iteration #%i",
			 argv[0], i);

		/* Produce a message */
		rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
				 msg, strlen(msg), NULL, 0, NULL);
		
		/* Wait for it to be sent (and possibly acked) */
		while (rd_kafka_outq_len(rk) > 0)
			rd_kafka_poll(rk, 50);

		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);
		
		/* Destroy rdkafka instance */
		rd_kafka_destroy(rk);
	}

	/* Wait for everything to be cleaned up since broker destroys are
	 * handled in its own thread. */
	test_wait_exit(10);

	/* If we havent failed at this point then
	 * there were no threads leaked */
	return 0;
}
