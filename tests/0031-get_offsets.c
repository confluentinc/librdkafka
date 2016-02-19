
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
 * Verify that rd_kafka_get_offsets() works.
 */


int main_0031_get_offsets (int argc, char **argv) {
	const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        const int msgcnt = 100;
	rd_kafka_t *rk;
	int64_t low = -1234, high = -1235;
	rd_kafka_resp_err_t err;
	test_timing_t t_get;

        /* Produce messages */
        test_produce_msgs_easy(topic, 0, 0, msgcnt);

	/* Get offsets */
	rk = test_create_consumer(NULL, NULL, NULL, NULL, NULL);

	TIMING_START(&t_get, "get_offsets");
	err = rd_kafka_get_offsets(rk, topic, 0, &low, &high, 10*1000);
	TIMING_STOP(&t_get);
	if (err)
		TEST_FAIL("get_offsets failed: %s\n", rd_kafka_err2str(err));

	if (low != 0 && high != msgcnt)
		TEST_FAIL("Expected low,high %d,%d, but got "
			  "%"PRId64",%"PRId64,
			  0, msgcnt, low, high);

	TEST_SAY("Got offsets %"PRId64", %"PRId64"\n", low, high);

	rd_kafka_destroy(rk);

        return 0;
}
