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
 * Various regression tests for hangs on destroy.
 */




/**
 * Request offset for nonexisting partition.
 * Will cause rd_kafka_destroy() to hang.
 */

static int nonexist_part (void) {
	const char *topic = test_mk_topic_name(__FUNCTION__, 1);
	rd_kafka_t *rk;
	rd_kafka_topic_partition_list_t *parts;
	rd_kafka_resp_err_t err;
        test_timing_t t_pos;
        const int msgcnt = 1000;
        uint64_t testid;
        int i;
	int it, iterations = 5;

        /* Produce messages */
        testid = test_produce_msgs_easy(topic, 0,
                                        RD_KAFKA_PARTITION_UA, msgcnt);

	for (it = 0 ; it < iterations ; it++) {
		char group_id[32];

		test_str_id_generate(group_id, sizeof(group_id));

		TEST_SAY("Iteration %d/%d, using group.id %s\n", it, iterations,
			 group_id);

		/* Consume messages */
		test_consume_msgs_easy(group_id, topic, testid, -1,
				       msgcnt, NULL);

		/*
		 * Now start a new consumer and query stored offsets (positions)
		 */

		rk = test_create_consumer(group_id, NULL, NULL, NULL, NULL);

		/* Fill in partition set */
		parts = rd_kafka_topic_partition_list_new(2);
		/* existing */
		rd_kafka_topic_partition_list_add(parts, topic, 0);
		/* non-existing */
		rd_kafka_topic_partition_list_add(parts, topic, 123);


		TIMING_START(&t_pos, "POSITION");
		err = rd_kafka_position(rk, parts, tmout_multip(5000));
		TIMING_STOP(&t_pos);
		if (err)
			TEST_FAIL("Failed to acquire positions: %s\n",
				  rd_kafka_err2str(err));

		for (i = 0 ; i < parts->cnt ; i++) {
			TEST_SAY("%s [%"PRId32"] returned offset %"PRId64
				 ": %s\n",
				 parts->elems[i].topic,
				 parts->elems[i].partition,
				 parts->elems[i].offset,
				 rd_kafka_err2str(parts->elems[i].err));
			if (parts->elems[i].partition == 0 &&
			    parts->elems[i].offset <= 0)
				TEST_FAIL("Partition %"PRId32" should have a "
					  "proper offset, not %"PRId64"\n",
					  parts->elems[i].partition,
					  parts->elems[i].offset);
			else if (parts->elems[i].partition == 123 &&
				 parts->elems[i].offset !=
				 RD_KAFKA_OFFSET_INVALID)
				TEST_FAIL("Partition %"PRId32
					  " should have failed\n",
					  parts->elems[i].partition);
		}

		rd_kafka_topic_partition_list_destroy(parts);

		test_consumer_close(rk);

		/* Hangs if bug isn't fixed */
		rd_kafka_destroy(rk);
	}

        return 0;
}


/**
 * Issue #530:
 * "Legacy Consumer. Delete hangs if done right after RdKafka::Consumer::create.
 *  But If I put a start and stop in between, there is no issue."
 */
static int legacy_consumer_early_destroy (void) {
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	char errstr[512];
	int pass;
	const char *topic = test_mk_topic_name(__FUNCTION__, 0);

	for (pass = 0 ; pass < 2 ; pass++) {
		TEST_SAY("%s: pass #%d\n", __FUNCTION__, pass);

		rk = rd_kafka_new(RD_KAFKA_CONSUMER, NULL,
				  errstr, sizeof(errstr));
		TEST_ASSERT(rk, "failed to create consumer: %s", errstr);

		if (pass == 1) {
			/* Second pass, create a topic too. */
			rkt = rd_kafka_topic_new(rk, topic, NULL);
			TEST_ASSERT(rkt, "failed to create topic: %s",
				    rd_kafka_err2str(
					    rd_kafka_errno2err(errno)));
			rd_sleep(1);
			rd_kafka_topic_destroy(rkt);
		}

		rd_kafka_destroy(rk);
	}

	return 0;
}


int main_0020_destroy_hang (int argc, char **argv) {
        int fails = 0;

	test_conf_init(NULL, NULL, 30);

        fails += nonexist_part();

	fails += legacy_consumer_early_destroy();

        if (fails > 0)
                TEST_FAIL("See %d previous error(s)\n", fails);

        return 0;
}
