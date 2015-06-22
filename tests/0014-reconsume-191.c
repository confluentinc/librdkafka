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

#define _GNU_SOURCE
#include <sys/time.h>
#include <time.h>

#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */

static int prod_msg_remains = 0;
static int fails = 0;

/**
 * Delivery reported callback.
 * Called for each message once to signal its delivery status.
 */
static void dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		TEST_FAIL("Message delivery failed: %s\n",
			  rd_kafka_err2str(err));

	if (prod_msg_remains == 0)
		TEST_FAIL("Too many messages delivered (prod_msg_remains %i)",
			  prod_msg_remains);

	prod_msg_remains--;
}


/**
 * Produces 'msgcnt' messages split over 'partition_cnt' partitions.
 */
static void produce_messages (uint64_t testid, const char *topic,
                              int partition_cnt, int msgcnt) {
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
        int failcnt;
	int i;
	int32_t partition;
	int msgid = 0;

	test_conf_init(&conf, &topic_conf, 20);

	rd_kafka_conf_set_dr_cb(conf, dr_cb);

        /* Make sure all replicas are in-sync after producing
         * so that consume test wont fail. */
        rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "-1",
                                errstr, sizeof(errstr));

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
			       errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  strerror(errno));

        /* Produce messages */
	prod_msg_remains = msgcnt;
	for (partition = 0 ; partition < partition_cnt ; partition++) {
		int batch_cnt = msgcnt / partition_cnt;

		for (i = 0 ; i < batch_cnt ; i++) {
                        char key[128];
                        char buf[128];
			snprintf(key, sizeof(key),
				 "testid=%"PRIu64", partition=%i, msg=%i",
				 testid, (int)partition, msgid);
                        snprintf(buf, sizeof(buf),
                                 "data: testid=%"PRIu64", partition=%i, msg=%i",
				 testid, (int)partition, msgid);

                        r = rd_kafka_produce(rkt, partition, 0,
                                             buf, strlen(buf),
                                             key, strlen(key),
                                             NULL);
                        if (r == -1)
                                TEST_FAIL("Failed to produce message %i "
                                          "to partition %i: %s",
                                          msgid, (int)partition,
                                          rd_kafka_err2str(
                                                  rd_kafka_errno2err(errno)));
			msgid++;
		}
        }


	/* Wait for messages to be delivered */
	while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 100);

	if (fails)
		TEST_FAIL("%i failures, see previous errors", fails);

	if (prod_msg_remains != 0)
		TEST_FAIL("Still waiting for %i messages to be produced",
			  prod_msg_remains);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);
}



static int *cons_msgs;
static int  cons_msgs_size;
static int  cons_msgs_cnt;

static void verify_consumed_msg_reset (int msgcnt) {
	if (cons_msgs) {
		free(cons_msgs);
		cons_msgs = NULL;
	}

	if (msgcnt) {
		int i;

		cons_msgs = malloc(sizeof(*cons_msgs) * msgcnt);
		for (i = 0 ; i < msgcnt ; i++)
			cons_msgs[i] = -1;
	}

	cons_msgs_size = msgcnt;
	cons_msgs_cnt = 0;
}


static int int_cmp (const void *_a, const void *_b) {
	int a = *(int *)_a;
	int b = *(int *)_b;
	return a - b;
}

static void verify_consumed_msg_check0 (const char *func, int line) {
	int i;
	int fails = 0;

	if (cons_msgs_cnt < cons_msgs_size) {
		TEST_SAY("Missing %i messages in consumer\n",
			 cons_msgs_size - cons_msgs_cnt);
		fails++;
	}

	qsort(cons_msgs, cons_msgs_size, sizeof(*cons_msgs), int_cmp);

	for (i = 0 ; i < cons_msgs_size ; i++) {
		if (cons_msgs[i] != i) {
			TEST_SAY("Consumed message #%i is wrong, "
				 "expected #%i\n",
				 cons_msgs[i], i);
			fails++;
		}
	}

	if (fails)
		TEST_FAIL("See above error(s)");

	verify_consumed_msg_reset(0);
}


#define verify_consumed_msg_check() \
	verify_consumed_msg_check0(__FUNCTION__,__LINE__)



static void verify_consumed_msg0 (const char *func, int line,
				  uint64_t testid, int32_t partition,
				  int msgnum,
				  rd_kafka_message_t *rkmessage) {
	uint64_t in_testid;
	int in_part;
	int in_msgnum;
	char buf[128];

	if (rkmessage->key_len +1 >= sizeof(buf))
		TEST_FAIL("Incoming message key too large (%i): "
			  "not sourced by this test",
			  (int)rkmessage->key_len);

	snprintf(buf, sizeof(buf), "%.*s",
		 (int)rkmessage->key_len, (char *)rkmessage->key);

	if (sscanf(buf, "testid=%"SCNd64", partition=%i, msg=%i",
		   &in_testid, &in_part, &in_msgnum) != 3)
		TEST_FAIL("Incorrect key format: %s", buf);

        if (test_level > 2) {
		TEST_SAY("%s:%i: Our testid %"PRIu64", part %i (%i), "
			 "msg %i/%i did "
			 ", key's: \"%s\"\n",
			 func, line,
			 testid, (int)partition, (int)rkmessage->partition,
			 msgnum, cons_msgs_size, buf);
	}

	if (testid != in_testid ||
	    (partition != -1 && partition != in_part) ||
	    (msgnum != -1 && msgnum != in_msgnum) ||
	    (in_msgnum < 0 || in_msgnum > cons_msgs_size))
		goto fail_match;

	if (cons_msgs_cnt == cons_msgs_size) {
		TEST_SAY("Too many messages in cons_msgs (%i) while reading "
			 "message key \"%s\"\n",
			 cons_msgs_cnt, buf);
		verify_consumed_msg_check();
		TEST_FAIL("See above error(s)");
	}

	cons_msgs[cons_msgs_cnt++] = in_msgnum;

	return;

 fail_match:
	TEST_FAIL("%s:%i: Our testid %"PRIu64", part %i, msg %i/%i did "
		  "not match message's key: \"%s\"\n",
		  func, line,
		  testid, (int)partition, msgnum, cons_msgs_size, buf);
}

#define verify_consumed_msg(testid,part,msgnum,rkmessage) \
	verify_consumed_msg0(__FUNCTION__,__LINE__,testid,part,msgnum,rkmessage)


static int msg_next;
static void consume_cb (rd_kafka_message_t *rkmessage, void *opaque) {
        int64_t testid = *(int64_t *)opaque;


        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                TEST_SAY("EOF at offset %"PRId64"\n", rkmessage->offset);
                return;
        }

        if (rkmessage->err)
                TEST_FAIL("Consume message from partition %i "
                          "has error: %s",
                          (int)rkmessage->partition,
                          rd_kafka_err2str(rkmessage->err));

        verify_consumed_msg(testid, rkmessage->partition,
                            msg_next, rkmessage);

        msg_next++;
}

static void consume_messages_callback_multi (uint64_t testid, const char *topic,
                                             int32_t partition, int msg_base,
                                             int batch_cnt, int msgcnt,
                                             int iterations) {
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	char msg[128];
        int failcnt;
	int i;
        rd_kafka_message_t *rkmessages;
	int cnt = 0;

	test_conf_init(&conf, &topic_conf, 20);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

        rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "smallest",
                                NULL, 0);

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  strerror(errno));

        msg_next = msg_base;

        for (i = 0 ; i < iterations ; i++) {
                int cnta;

                TEST_SAY("Iteration #%i: Consuming from partition %i\n",
                         i, partition);

                /* Consume messages */
                if (rd_kafka_consume_start(rkt, partition,
                                           RD_KAFKA_OFFSET_STORED) == -1)
                        TEST_FAIL("consume_start(%i) failed: %s",
                                  (int)partition,
                                  rd_kafka_err2str(rd_kafka_errno2err(errno)));


                cnta = msg_next;
                do {
                        rd_kafka_consume_callback(rkt, partition, 5000,
                                                  consume_cb, &testid);
                } while (msg_next < msg_base + batch_cnt);
                TEST_SAY("Iteration #%i: consumed %i messages\n",
                         i, msg_next - cnta);

                rd_kafka_consume_stop(rkt, partition);
                //sleep(5);
        }

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);
}



static void test_produce_consume (void) {
	int msgcnt = 10000;
        int partition_cnt = 1;
	int i;
	uint64_t testid;
	int msg_base = 0;
        const char *topic;

	/* Generate a testid so we can differentiate messages
	 * from other tests */
	testid = test_id_generate();

        /* Read test.conf to configure topic name */
        test_conf_init(NULL, NULL, 20);
        topic = test_mk_topic_name("0013", 0);

	TEST_SAY("Topic %s, testid %"PRIu64"\n", topic, testid);

	/* Produce messages */
	produce_messages(testid, topic, partition_cnt, msgcnt);


	/* Consume messages with callbacks. */
	verify_consumed_msg_reset(msgcnt);
	for (i = 0 ; i < partition_cnt ; i++)
		consume_messages_callback_multi(testid, topic, i,
                                                msg_base,
                                                msgcnt / partition_cnt, msgcnt,
                                                2);
	verify_consumed_msg_check();

	/* Wait for everything to be cleaned up since broker destroys are
	 * handled in its own thread. */
	test_wait_exit(10);

	/* If we havent failed at this point then
	 * there were no threads leaked */
	return;
}




int main (int argc, char **argv) {
	test_produce_consume();
	return 0;
}
