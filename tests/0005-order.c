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
 * Tests messages are produced in order.
 */

#define _GNU_SOURCE
#include <sys/time.h>
#include <time.h>

#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


static int msgid_next = 0;
static int fails = 0;

/**
 * Delivery reported callback.
 * Called for each message once to signal its delivery status.
 */
static void dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
	int msgid = *(int *)msg_opaque;

	free(msg_opaque);

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		TEST_FAIL("Message delivery failed: %s\n",
			  rd_kafka_err2str(err));

	if (msgid != msgid_next) {
		fails++;
		TEST_FAIL("Delivered msg %i, expected %i\n",
			 msgid, msgid_next);
		return;
	}

	msgid_next = msgid+1;
}


int main (int argc, char **argv) {
	int partition = 0;
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	char msg[128];
	int msgcnt = 100000;
	int i;

	test_conf_init(&conf, &topic_conf, 10);

	/* Set delivery report callback */
	rd_kafka_conf_set_dr_cb(conf, dr_cb);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
			  errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	rkt = rd_kafka_topic_new(rk, test_mk_topic_name("generic", 0),
                                 topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  strerror(errno));

	/* Produce a message */
	for (i = 0 ; i < msgcnt ; i++) {
		int *msgidp = malloc(sizeof(*msgidp));
		*msgidp = i;
		snprintf(msg, sizeof(msg), "%s test message #%i", argv[0], i);
		r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
				     msg, strlen(msg), NULL, 0, msgidp);
		if (r == -1)
			TEST_FAIL("Failed to produce message #%i: %s\n",
				  i, strerror(errno));
	}

	TEST_SAY("Produced %i messages, waiting for deliveries\n", msgcnt);

	/* Wait for messages to time out */
	while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 50);

	if (fails)
		TEST_FAIL("%i failures, see previous errors", fails);

	if (msgid_next != msgcnt)
		TEST_FAIL("Still waiting for messages: next %i != end %i\n",
			  msgid_next, msgcnt);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);

	/* Wait for everything to be cleaned up since broker destroys are
	 * handled in its own thread. */
	test_wait_exit(10);

	/* If we havent failed at this point then
	 * there were no threads leaked */
	return 0;
}
