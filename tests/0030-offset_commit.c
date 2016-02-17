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
 * Consumer: various offset commit constellations, matrix:
 *   enable.auto.commit, enable.auto.offset.store, async
 */

static const char *topic;
static const int msgcnt = 100;
static const int partition = 0;
static uint64_t testid;

static int64_t expected_offset = 0;
static int64_t committed_offset = -1;


static void offset_commit_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
			      rd_kafka_topic_partition_list_t *offsets,
			      void *opaque) {
	rd_kafka_topic_partition_t *rktpar;

	TEST_SAYL(3, "Offset committed: %s:\n", rd_kafka_err2str(err));
	if (err == RD_KAFKA_RESP_ERR__NO_OFFSET)
		return;

	test_print_partition_list(offsets);
	if (err)
		TEST_FAIL("Offset commit failed: %s", rd_kafka_err2str(err));
	if (offsets->cnt == 0)
		TEST_FAIL("Expected at least one partition in offset_commit_cb");

	/* Find correct partition */
	if (!(rktpar = rd_kafka_topic_partition_list_find(offsets,
							  topic, partition)))
		return;

	if (rktpar->err)
		TEST_FAIL("Offset commit failed for partitioń : %s",
			  rd_kafka_err2str(rktpar->err));

	if (rktpar->offset > expected_offset)
		TEST_FAIL("Offset committed %"PRId64
			  " > expected offset %"PRId64,
			  rktpar->offset, expected_offset);

	if (rktpar->offset <= committed_offset)
		TEST_FAIL("Old offset %"PRId64" (re)committed: "
			  "should be above committed_offset %"PRId64,
			  rktpar->offset, committed_offset);

	committed_offset = rktpar->offset;

	if (rktpar->offset < expected_offset) {
		TEST_SAYL(3, "Offset committed %"PRId64
			  " < expected offset %"PRId64"\n",
			  rktpar->offset, expected_offset);
		return;
	}

	TEST_SAYL(3, "Expected offset committed: %"PRId64"\n", rktpar->offset);
}


static void do_offset_test (const char *what, int auto_commit, int auto_store,
			    int async) {
	test_timing_t t_all;
	char groupid[64];
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *tconf;
	int cnt = 0;
	const int extra_cnt = 5;
	rd_kafka_resp_err_t err;
	char errstr[512];
	rd_kafka_topic_partition_list_t *parts;
	rd_kafka_topic_partition_t *rktpar;

	test_conf_init(&conf, &tconf, 20);
	test_conf_set(conf, "enable.auto.commit", auto_commit ? "true":"false");
	test_conf_set(conf, "enable.auto.offset.store", auto_store ?"true":"false");
	test_conf_set(conf, "auto.commit.interval.ms", "500");
	rd_kafka_conf_set_offset_commit_cb(conf, offset_commit_cb);
	test_topic_conf_set(tconf, "auto.offset.reset", "smallest");
	test_str_id_generate(groupid, sizeof(groupid));
	test_conf_set(conf, "group.id", groupid);
	rd_kafka_conf_set_default_topic_conf(conf, tconf);

	TEST_SAY(_C_MAG "[ do_offset_test: %s with group.id %s ]\n",
		 what, groupid);

	TIMING_START(&t_all, what);

	expected_offset  = 0;
	committed_offset = -1;

	/* MO:
	 *  - Create consumer.
	 *  - Start consuming from beginning
	 *  - Perform store & commits according to settings
	 *  - Stop storing&committing when half of the messages are consumed,
	 *  - but consume 5 more to check against.
	 *  - Query position.
	 *  - Destroy consumer.
	 *  - Create new consumer with same group.id using stored offsets
	 *  - Should consume the expected message.
	 */

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, rd_kafka_conf_dup(conf),
			  errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("%s: Failed to create rdkafka instance: %s\n",
			  what, errstr);
	rd_kafka_poll_set_consumer(rk);

	test_consumer_subscribe(rk, topic);

	while (cnt - extra_cnt < msgcnt / 2) {
		rd_kafka_message_t *rkm;

		rkm = rd_kafka_consumer_poll(rk, 10*1000);
		if (!rkm)
			continue;

		if (rkm->err == RD_KAFKA_RESP_ERR__TIMED_OUT)
			TEST_FAIL("%s: Timed out waiting for message %d", what,cnt);
		else if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			rd_kafka_message_destroy(rkm);
			continue;
		} else if (rkm->err)
			TEST_FAIL("%s: Consumer error: %s",
				  what, rd_kafka_message_errstr(rkm));

		if (cnt < msgcnt / 2) {
			if (!auto_store) {
				err = rd_kafka_offset_store(rkm->rkt,rkm->partition,
							    rkm->offset);
				if (err)
					TEST_FAIL("%s: offset_store failed: %s\n",
						  what, rd_kafka_err2str(err));
			}
			if (!auto_commit) {
				test_timing_t t_commit;
				TIMING_START(&t_commit,
					     async?"commit.async":"commit.sync");
				err = rd_kafka_commit_message(rk, rkm, async);
				TIMING_STOP(&t_commit);
				if (err)
					TEST_FAIL("%s: commit failed: %s\n",
						  what, rd_kafka_err2str(err));
			}
			expected_offset = rkm->offset+1;
		} else if (auto_store && auto_commit)
			expected_offset = rkm->offset+1;

		rd_kafka_message_destroy(rkm);
		cnt++;
	}

	TEST_SAY("%s: done consuming after %d messages, at offset %"PRId64"\n",
		 what, cnt, expected_offset);

	/* Pause messages while waiting so we can serve callbacks
	 * without having more messages received. */
	if ((err = rd_kafka_assignment(rk, &parts)))
		TEST_FAIL("%s: failed to get assignment(): %s\n",
			  what, rd_kafka_err2str(err));

	if ((err = rd_kafka_pause_partitions(rk, parts)))
		TEST_FAIL("%s: failed to pause partitions: %s\n",
			  what, rd_kafka_err2str(err));
	rd_kafka_topic_partition_list_destroy(parts);

	/* Fire off any enqueued offset_commit_cb */
	test_consumer_poll_no_msgs(what, rk, testid, 0);

	TEST_SAY("%s: committed_offset %"PRId64", expected_offset %"PRId64"\n",
		 what, committed_offset, expected_offset);

	if (!auto_commit && !async) {
		/* Sync commits should be up to date at this point. */
		if (committed_offset != expected_offset)
			TEST_FAIL("%s: Sync commit: committed offset %"PRId64
				  " should be same as expected offset "
				  "%"PRId64,
				  what, committed_offset, expected_offset);
	} else {

		/* Wait for offset commits to catch up */
		while (committed_offset < expected_offset) {
			TEST_SAYL(3, "%s: Wait for committed offset %"PRId64
				  " to reach expected offset %"PRId64"\n",
				  what, committed_offset, expected_offset);
			test_consumer_poll_no_msgs(what, rk, testid, 1000);
		}

	}

	TEST_SAY("%s: phase 1 complete, %d messages consumed, "
		 "next expected offset is %"PRId64"\n",
		 what, cnt, expected_offset);
	
	/* Query position */
	parts = rd_kafka_topic_partition_list_new(1);
	rd_kafka_topic_partition_list_add(parts, topic, partition);

	err = rd_kafka_position(rk, parts, tmout_multip(5*1000));
	if (err)
		TEST_FAIL("%s: position() failed: %s", what, rd_kafka_err2str(err));
	if (!(rktpar = rd_kafka_topic_partition_list_find(parts,
							  topic, partition)))
		TEST_FAIL("%s: position(): topic lost\n", what);
	if (rktpar->offset != expected_offset)
		TEST_FAIL("%s: Expected position() offset %"PRId64", got %"PRId64,
			  what, expected_offset, rktpar->offset);
	TEST_SAY("%s: Position is at %"PRId64", good!\n", what, rktpar->offset);

	rd_kafka_topic_partition_list_destroy(parts);
	test_consumer_close(rk);
	rd_kafka_destroy(rk);



	/* Fire up a new consumer and continue from where we left off. */
	TEST_SAY("%s: phase 2: starting new consumer to resume consumption\n",what);
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("%s: Failed to create rdkafka instance: %s\n",
			  what, errstr);
	rd_kafka_poll_set_consumer(rk);

	test_consumer_subscribe(rk, topic);

	while (cnt < msgcnt) {
		rd_kafka_message_t *rkm;

		rkm = rd_kafka_consumer_poll(rk, 10*1000);
		if (!rkm)
			continue;

		if (rkm->err == RD_KAFKA_RESP_ERR__TIMED_OUT)
			TEST_FAIL("%s: Timed out waiting for message %d", what,cnt);
		else if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			rd_kafka_message_destroy(rkm);
			continue;
		} else if (rkm->err)
			TEST_FAIL("%s: Consumer error: %s",
				  what, rd_kafka_message_errstr(rkm));

		if (rkm->offset != expected_offset)
			TEST_FAIL("%s: Received message offset %"PRId64
				  ", expected %"PRId64" at msgcnt %d/%d\n",
				  what, rkm->offset, expected_offset,
				  cnt, msgcnt);

		rd_kafka_message_destroy(rkm);
		expected_offset++;
		cnt++;
	}


	TEST_SAY("%s: phase 2: complete\n", what);
	test_consumer_close(rk);
	rd_kafka_destroy(rk);
	

	TIMING_STOP(&t_all);
}

int main_0030_offset_commit (int argc, char **argv) {

	topic = test_mk_topic_name(__FUNCTION__, 1);
	testid = test_produce_msgs_easy(topic, 0, partition, msgcnt);

	do_offset_test("AUTO.COMMIT & AUTO.STORE",
		       1 /* enable.auto.commit */,
		       1 /* enable.auto.offset.store */,
		       0 /* not used. */);

	do_offset_test("AUTO.COMMIT & MANUAL.STORE",
		       1 /* enable.auto.commit */,
		       0 /* enable.auto.offset.store */,
		       0 /* not used */);

	do_offset_test("MANUAL.COMMIT.ASYNC & AUTO.STORE",
		       0 /* enable.auto.commit */,
		       1 /* enable.auto.offset.store */,
		       1 /* async */);

	do_offset_test("MANUAL.COMMIT.SYNC & AUTO.STORE",
		       0 /* enable.auto.commit */,
		       1 /* enable.auto.offset.store */,
		       0 /* async */);

	do_offset_test("MANUAL.COMMIT.ASYNC & MANUAL.STORE",
		       0 /* enable.auto.commit */,
		       0 /* enable.auto.offset.store */,
		       1 /* sync */);

	do_offset_test("MANUAL.COMMIT.SYNC & MANUAL.STORE",
		       0 /* enable.auto.commit */,
		       0 /* enable.auto.offset.store */,
		       0 /* sync */);

        return 0;
}
