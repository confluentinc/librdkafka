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

#include "test.h"
#include <signal.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


int test_level = 2;
int test_seed = 0;

static void sig_alarm (int sig) {
	TEST_FAIL("Test timed out");
}

static void test_error_cb (rd_kafka_t *rk, int err,
			   const char *reason, void *opaque) {
	TEST_FAIL("rdkafka error: %s: %s", rd_kafka_err2str(err), reason);
}

static void test_init (void) {
	int seed;
	char *tmp;

	if (test_seed)
		return;

	if ((tmp = getenv("TEST_LEVEL")))
		test_level = atoi(tmp);
	if ((tmp = getenv("TEST_SEED")))
		seed = atoi(tmp);
	else
		seed = test_clock() & 0xffffffff;

	srand(seed);
	test_seed = seed;
}


/**
 * Creates and sets up kafka configuration objects.
 * Will read "test.conf" file if it exists.
 */
void test_conf_init (rd_kafka_conf_t **conf, rd_kafka_topic_conf_t **topic_conf,
		     int timeout) {
	FILE *fp;
	char buf[512];
	int line = 0;
	const char *test_conf = getenv("RDKAFKA_TEST_CONF") ? : "test.conf";
	char errstr[512];

	test_init();

	/* Limit the test run time. */
	alarm(timeout);
	signal(SIGALRM, sig_alarm);

	*conf = rd_kafka_conf_new();
	*topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_error_cb(*conf, test_error_cb);

	/* Open and read optional local test configuration file, if any. */
	if (!(fp = fopen(test_conf, "r"))) {
		if (errno == ENOENT)
			TEST_FAIL("%s not found\n", test_conf);
		else
			TEST_FAIL("Failed to read %s: %s",
				  test_conf, strerror(errno));
	}

	while (fgets(buf, sizeof(buf)-1, fp)) {
		char *t;
		char *b = buf;
		rd_kafka_conf_res_t res;
		char *name, *val;

		line++;
		if ((t = strchr(b, '\n')))
			*t = '\0';

		if (*b == '#' || !*b)
			continue;

		if (!(t = strchr(b, '=')))
			TEST_FAIL("%s:%i: expected name=value format\n",
				  test_conf, line);

		name = b;
		*t = '\0';
		val = t+1;

		if (!strncmp(name, "topic.", strlen("topic."))) {
			name += strlen("topic.");
			res = rd_kafka_topic_conf_set(*topic_conf,
						      name, val,
						      errstr, sizeof(errstr));
		} else
			res = rd_kafka_conf_set(*conf,
						name, val,
						errstr, sizeof(errstr));

		if (res != RD_KAFKA_CONF_OK)
			TEST_FAIL("%s:%i: %s\n",
				  test_conf, line, errstr);
	}

	fclose(fp);
}


/**
 * Wait 'timeout' seconds for rdkafka to kill all its threads and clean up.
 */
void test_wait_exit (int timeout) {
	int r;

	while ((r = rd_kafka_thread_cnt()) && timeout-- >= 0) {
		TEST_SAY("%i thread(s) in use by librdkafka, waiting...\n", r);
		sleep(1);
	}

	TEST_SAY("%i thread(s) in use by librdkafka\n", r);

	if (r > 0) {
		assert(0);
		TEST_FAIL("%i thread(s) still active in librdkafka", r);
	}
}


/**
 * Generate a "unique" test id.
 */
uint64_t test_id_generate (void) {
	test_init();
	return (((uint64_t)rand()) << 32) | (uint64_t)rand();
}
