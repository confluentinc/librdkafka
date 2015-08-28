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
 * Delivery reported callback.
 * Called for each message once to signal its delivery status.
 */
static void dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
	int *remainsp = msg_opaque;

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		TEST_FAIL("Message delivery failed: %s\n",
			  rd_kafka_err2str(err));

	if (*remainsp == 0)
		TEST_FAIL("Too many messages delivered (remains %i)",
			  *remainsp);

	(*remainsp)--;
}


rd_kafka_t *create_producer (void) {
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	char errstr[512];
	
	test_conf_init(&conf, NULL, 20);

	rd_kafka_conf_set_dr_cb(conf, dr_cb);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	return rk;
}

rd_kafka_topic_t *create_producer_topic (rd_kafka_t *rk, const char *topic) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	
	test_conf_init(NULL, &topic_conf, 20);

	/* Make sure all replicas are in-sync after producing
	 * so that consume test wont fail. */
        rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "-1",
                                errstr, sizeof(errstr));


	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
                          rd_kafka_err2str(rd_kafka_errno2err(errno)));

	return rkt;

}

void produce_msgs (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
		   uint64_t testid, int32_t partition,
		   int msg_base, int cnt) {
	int msg_id;
	test_timing_t t_all;
	int remains = 0;

	TEST_SAY("Produce to %s [%"PRId32"]: messages #%d..%d\n",
		 rd_kafka_topic_name(rkt), partition, msg_base, msg_base+cnt);

	TIMING_START(&t_all, "PRODUCE");

	for (msg_id = msg_base ; msg_id < msg_base + cnt ; msg_id++) {
		char key[128];
		char buf[128];

		test_msg_fmt(key, sizeof(key), testid, partition, msg_id);
		snprintf(buf, sizeof(buf), "data: %s", key);

		remains++;

		if (rd_kafka_produce(rkt, partition,
				     RD_KAFKA_MSG_F_COPY,
				     buf, strlen(buf),
				     key, strlen(key),
				     &remains) == -1)
			TEST_FAIL("Failed to produce message %i "
				  "to partition %i: %s",
				  msg_id, (int)partition,
				  rd_kafka_err2str(rd_kafka_errno2err(errno)));

        }


	/* Wait for messages to be delivered */
	while (remains > 0 && rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 10);

	TIMING_STOP(&t_all);
}



rd_kafka_t *create_consumer (void) {
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	char errstr[512];
	
	test_conf_init(&conf, NULL, 20);

	rd_kafka_conf_set_dr_cb(conf, dr_cb);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	return rk;
}

rd_kafka_topic_t *create_consumer_topic (rd_kafka_t *rk, const char *topic) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;
	
	test_conf_init(NULL, &topic_conf, 20);

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
                          rd_kafka_err2str(rd_kafka_errno2err(errno)));

	return rkt;
}


void consumer_start (const char *what,
		     rd_kafka_topic_t *rkt, int32_t partition,
		     int64_t start_offset) {

	TEST_SAY("%s: consumer_start: %s [%"PRId32"] at offset %"PRId64"\n",
		 what, rd_kafka_topic_name(rkt), partition, start_offset);
	
	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1)
		TEST_FAIL("%s: consume_start failed: %s\n",
			  what, rd_kafka_err2str(rd_kafka_errno2err(errno)));
	
}

void consumer_stop (const char *what,
		    rd_kafka_topic_t *rkt, int32_t partition) {

	TEST_SAY("%s: consumer_stop: %s [%"PRId32"]\n",
		 what, rd_kafka_topic_name(rkt), partition);

	if (rd_kafka_consume_stop(rkt, partition) == -1)
		TEST_FAIL("%s: consume_stop failed: %s\n",
			  what, rd_kafka_err2str(rd_kafka_errno2err(errno)));
			  
}

void consumer_seek (const char *what, rd_kafka_topic_t *rkt, int32_t partition,
		    int64_t offset) {
	int err;

	TEST_SAY("%s: consumer_seek: %s [%"PRId32"] to offset %"PRId64"\n",
		 what, rd_kafka_topic_name(rkt), partition, offset);

	if ((err = rd_kafka_seek(rkt, partition, offset, 2000)))
		TEST_FAIL("%s: consume_seek(%s, %"PRId32", %"PRId64") "
			  "failed: %s\n",
			  what,
			  rd_kafka_topic_name(rkt), partition, offset,
			  rd_kafka_err2str(err));
}


#define _NO_SEEK  -1
/**
 * Returns offset of the last message consumed
 */
int64_t consume_msgs (const char *what, rd_kafka_topic_t *rkt,
		      uint64_t testid, int32_t partition, int64_t offset,
		      int exp_msg_base, int exp_cnt) {
	int cnt = 0;
	int msg_next = exp_msg_base;
	int fails = 0;
	int64_t offset_last = -1;
	test_timing_t t_first, t_all;

	TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: expect msg #%d..%d "
		 "at offset %"PRId64"\n",
		 what, rd_kafka_topic_name(rkt), partition,
		 exp_msg_base, exp_cnt, offset);

	if (offset != _NO_SEEK) {
		rd_kafka_resp_err_t err;
		test_timing_t t_seek;

		TIMING_START(&t_seek, "SEEK");
		if ((err = rd_kafka_seek(rkt, partition, offset, 5000)))
			TEST_FAIL("%s: consume_msgs: %s [%"PRId32"]: "
				  "seek to %"PRId64" failed: %s\n",
				  what, rd_kafka_topic_name(rkt), partition,
				  offset, rd_kafka_err2str(err));
		TIMING_STOP(&t_seek);
		TEST_SAY("%s: seeked to offset %"PRId64"\n", what, offset);
	}

	TIMING_START(&t_first, "FIRST MSG");
	TIMING_START(&t_all, "ALL MSGS");

	while (cnt < exp_cnt) {
		rd_kafka_message_t *rkmessage;
		int msg_id;

		rkmessage = rd_kafka_consume(rkt, partition, 5000);
		if (!rkmessage)
			TEST_FAIL("%s: consume_msgs: %s [%"PRId32"]: "
				  "expected msg #%d (%d/%d): timed out\n",
				  what, rd_kafka_topic_name(rkt), partition,
				  msg_next, cnt, exp_cnt);

		if (rkmessage->err)
			TEST_FAIL("%s: consume_msgs: %s [%"PRId32"]: "
				  "expected msg #%d (%d/%d): got error: %s\n",
				  what, rd_kafka_topic_name(rkt), partition,
				  msg_next, cnt, exp_cnt,
				  rd_kafka_err2str(rkmessage->err));

		if (cnt == 0)
			TIMING_STOP(&t_first);
		
		test_msg_parse(testid, rkmessage->key, rkmessage->key_len,
			       partition, &msg_id);

		if (test_level >= 3)
			TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: "
				 "got msg #%d at offset %"PRId64
				 " (expect #%d at offset %"PRId64")\n",
				 what, rd_kafka_topic_name(rkt), partition,
				 msg_id, rkmessage->offset,
				 msg_next,
				 offset >= 0 ? offset + cnt : -1);

		if (msg_id != msg_next) {
			TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: "
				 "expected msg #%d (%d/%d): got msg #%d\n",
				 what, rd_kafka_topic_name(rkt), partition,
				 msg_next, cnt, exp_cnt, msg_id);
			fails++;
		}

		cnt++;
		msg_next++;
		offset_last = rkmessage->offset;

		rd_kafka_message_destroy(rkmessage);
	}

	TIMING_STOP(&t_all);

	if (fails)
		TEST_FAIL("%s: consume_msgs: %s [%"PRId32"]: %d failures\n",
			  what, rd_kafka_topic_name(rkt), partition, fails);

	TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: "
		 "%d/%d messages consumed succesfully\n",
		 what, rd_kafka_topic_name(rkt), partition,
		 cnt, exp_cnt);
	return offset_last;
}


int main_0015_offsets_seek (int argc, char **argv) {
	const char *topic = test_mk_topic_name("0015", 1);
	rd_kafka_t *rk_p, *rk_c;
	rd_kafka_topic_t *rkt_p, *rkt_c;
	int msg_cnt = 1000;
	int msg_base = 0;
	int32_t partition = 0;
	int i;
	int64_t offset_last, offset_base;
	uint64_t testid;
	int dance_iterations = 10;
	int msgs_per_dance = 10;

	testid = test_id_generate();
	
	/* Produce messages */
	rk_p = create_producer();
	rkt_p = create_producer_topic(rk_p, topic);

	produce_msgs(rk_p, rkt_p, testid, partition, msg_base, msg_cnt);

	rd_kafka_topic_destroy(rkt_p);
	rd_kafka_destroy(rk_p);


	rk_c = create_consumer();
	rkt_c = create_consumer_topic(rk_c, topic);

	
	/* Start consumer tests */
	consumer_start("verify.all", rkt_c, partition,
		       RD_KAFKA_OFFSET_BEGINNING);
	/* Make sure all messages are available */
	offset_last = consume_msgs("verify.all", rkt_c,
				   testid, partition, _NO_SEEK,
				   msg_base, msg_cnt);

	/* Rewind offset back to its base. */
	offset_base = offset_last - msg_cnt + 1;

	TEST_SAY("%s [%"PRId32"]: Do random seek&consume for msgs #%d+%d with "
		 "offsets %"PRId64"..%"PRId64"\n",
		 rd_kafka_topic_name(rkt_c), partition,
		 msg_base, msg_cnt, offset_base, offset_last);
		 
	/* Now go dancing over the entire range with offset seeks. */
	for (i = 0 ; i < dance_iterations ; i++) {
		int64_t offset = jitter(offset_base, offset_base+msg_cnt);

		consume_msgs("dance", rkt_c,
			     testid, partition, offset,
			     msg_base + (offset - offset_base),
			     RD_MIN(msgs_per_dance, offset_last - offset));
	}
		
	
	consumer_stop("1", rkt_c, partition);
	
	rd_kafka_topic_destroy(rkt_c);
	rd_kafka_destroy(rk_c);

	return 0;
}
