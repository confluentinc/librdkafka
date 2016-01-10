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

#include <stdarg.h>
#include "test.h"
#include <signal.h>
#include <stdlib.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


int test_level = 2;
int test_seed = 0;

static char test_topic_prefix[128] = "rdkafkatest";
static int  test_topic_random = 0;
static int  tests_run_in_parallel = 0;
static int  tests_running_cnt = 0;
const RD_TLS char *test_curr = NULL;
RD_TLS int64_t test_start = 0;
double test_timeout_multiplier  = 1.0;

int  test_session_timeout_ms = 6000;

static mtx_t test_lock;


static void sig_alarm (int sig) {
	TEST_FAIL("Test timed out");
}

static void test_error_cb (rd_kafka_t *rk, int err,
			   const char *reason, void *opaque) {
	TEST_FAIL("rdkafka error: %s: %s", rd_kafka_err2str(err), reason);
}

static void test_init (void) {
	int seed;
#ifndef _MSC_VER
	char *tmp;
#endif

	if (test_seed)
		return;

#ifndef _MSC_VER
	if ((tmp = getenv("TEST_LEVEL")))
		test_level = atoi(tmp);
	if ((tmp = getenv("TEST_SEED")))
		seed = atoi(tmp);
	else
#endif
		seed = test_clock() & 0xffffffff;

	srand(seed);
	test_seed = seed;
}


const char *test_mk_topic_name (const char *suffix, int randomized) {
        static RD_TLS char ret[128];

        if (test_topic_random || randomized)
                rd_snprintf(ret, sizeof(ret), "%s_rnd%"PRIx64"_%s",
                         test_topic_prefix, test_id_generate(), suffix);
        else
                rd_snprintf(ret, sizeof(ret), "%s_%s", test_topic_prefix, suffix);

        TEST_SAY("Using topic \"%s\"\n", ret);

        return ret;
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
	const char *test_conf =
#ifndef _MSC_VER
		getenv("RDKAFKA_TEST_CONF") ? getenv("RDKAFKA_TEST_CONF") : 
#endif
		"test.conf";
	char errstr[512];

	test_init();

        if (conf) {
#ifndef _MSC_VER
                char *tmp;
#endif

                *conf = rd_kafka_conf_new();
                rd_kafka_conf_set_error_cb(*conf, test_error_cb);

#ifndef _MSC_VER
                if ((tmp = getenv("TEST_DEBUG")) && *tmp)
                        test_conf_set(*conf, "debug", tmp);
#endif

#ifdef SIGIO
                /* Quick termination */
                rd_snprintf(buf, sizeof(buf), "%i", SIGIO);
                rd_kafka_conf_set(*conf, "internal.termination.signal",
                                  buf, NULL, 0);
                signal(SIGIO, SIG_IGN);
#endif
        }

	if (topic_conf)
		*topic_conf = rd_kafka_topic_conf_new();

	/* Open and read optional local test configuration file, if any. */
#ifndef _MSC_VER
	fp = fopen(test_conf, "r");
#else
	fp = NULL;
	errno = fopen_s(&fp, test_conf, "r");
#endif
	if (!fp) {
		if (errno == ENOENT)
			TEST_FAIL("%s not found\n", test_conf);
		else
			TEST_FAIL("Failed to read %s: errno %i",
				  test_conf, errno);
	}

	while (fgets(buf, sizeof(buf)-1, fp)) {
		char *t;
		char *b = buf;
		rd_kafka_conf_res_t res = RD_KAFKA_CONF_UNKNOWN;
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

                if (!strcmp(name, "test.timeout.multiplier")) {
                        test_timeout_multiplier = strtod(val, NULL);
                        timeout = tmout_multip(timeout*1000);
                        res = RD_KAFKA_CONF_OK;
                } else if (!strcmp(name, "test.topic.prefix")) {
					rd_snprintf(test_topic_prefix, sizeof(test_topic_prefix),
						"%s", val);
				    res = RD_KAFKA_CONF_OK;
                } else if (!strcmp(name, "test.topic.random")) {
                        if (!strcmp(val, "true") ||
                            !strcmp(val, "1"))
                                test_topic_random = 1;
                        else
                                test_topic_random = 0;
                        res = RD_KAFKA_CONF_OK;
                } else if (!strncmp(name, "topic.", strlen("topic."))) {
			name += strlen("topic.");
                        if (topic_conf)
                                res = rd_kafka_topic_conf_set(*topic_conf,
                                                              name, val,
                                                              errstr,
                                                              sizeof(errstr));
                        else
                                res = RD_KAFKA_CONF_OK;
                        name -= strlen("topic.");
                }

                if (res == RD_KAFKA_CONF_UNKNOWN) {
                        if (conf)
                                res = rd_kafka_conf_set(*conf,
                                                        name, val,
                                                        errstr, sizeof(errstr));
                        else
                                res = RD_KAFKA_CONF_OK;
                }

		if (res != RD_KAFKA_CONF_OK)
			TEST_FAIL("%s:%i: %s\n",
				  test_conf, line, errstr);
	}

	fclose(fp);

        if (timeout) {
                /* Limit the test run time. */
#ifndef _MSC_VER
                alarm(timeout);
                signal(SIGALRM, sig_alarm);
#endif
        }
}


/**
 * Wait 'timeout' seconds for rdkafka to kill all its threads and clean up.
 */
void test_wait_exit (int timeout) {
	int r;
        time_t start = time(NULL);

	while ((r = rd_kafka_thread_cnt()) && timeout-- >= 0) {
		TEST_SAY("%i thread(s) in use by librdkafka, waiting...\n", r);
		rd_sleep(1);
	}

	TEST_SAY("%i thread(s) in use by librdkafka\n", r);

	if (r > 0) {
		TEST_FAIL("%i thread(s) still active in librdkafka", r);
	}

        timeout -= (int)(time(NULL) - start);
        if (timeout > 0) {
		TEST_SAY("Waiting %d seconds for all librdkafka memory "
			 "to be released\n", timeout);
                if (rd_kafka_wait_destroyed(timeout * 1000) == -1)
			TEST_FAIL("Not all internal librdkafka "
				  "objects destroyed\n");
	}

}



/**
 * Generate a "unique" test id.
 */
uint64_t test_id_generate (void) {
	test_init();
	return (((uint64_t)rand()) << 32) | (uint64_t)rand();
}


/**
 * Generate a "unique" string id
 */
char *test_str_id_generate (char *dest, size_t dest_size) {
        rd_snprintf(dest, dest_size, "%"PRId64, test_id_generate());
	return dest;
}


/**
 * Format a message token
 */
void test_msg_fmt (char *dest, size_t dest_size,
		   uint64_t testid, int32_t partition, int msgid) {

	rd_snprintf(dest, dest_size,
		    "testid=%"PRIu64", partition=%"PRId32", msg=%i",
		    testid, partition, msgid);
}



/**
 * Parse a message token
 */
void test_msg_parse0 (const char *func, int line,
		      uint64_t testid, const void *ptr, size_t size,
		      int32_t exp_partition, int *msgidp) {
	char buf[128];
	uint64_t in_testid;
	int in_part;

	if (!ptr)
		TEST_FAIL("%s:%i: Message has empty key\n",
			  func, line);

	rd_snprintf(buf, sizeof(buf), "%.*s", (int)size, (char *)ptr);

	if (sscanf(buf, "testid=%"SCNd64", partition=%i, msg=%i",
		   &in_testid, &in_part, msgidp) != 3)
		TEST_FAIL("%s:%i: Incorrect key format: %s", func, line, buf);


	if (testid != in_testid ||
	    (exp_partition != -1 && exp_partition != in_part))
		TEST_FAIL("%s:%i: Our testid %"PRIu64", part %i did "
			  "not match message: \"%s\"\n",
		  func, line, testid, (int)exp_partition, buf);
}


struct run_args {
        const char *testname;
        int (*test_main) (int, char **);
        int argc;
        char **argv;
};

static int run_test0 (struct run_args *run_args) {
	test_timing_t t_run;
	int r;

	test_curr = run_args->testname;
	TEST_SAY("================= Running test %s =================\n",
		 run_args->testname);
	TIMING_START(&t_run, run_args->testname);
	test_start = t_run.ts_start;
	r = run_args->test_main(run_args->argc, run_args->argv);
	TIMING_STOP(&t_run);

	if (r)
		TEST_SAY("\033[31m"
			 "================= Test %s FAILED ================="
			 "\033[0m\n",
			 run_args->testname);
	else
		TEST_SAY("\033[32m"
			 "================= Test %s PASSED ================="
			 "\033[0m\n",
			 run_args->testname);

	return r;
}




static int run_test_from_thread (void *arg) {
        struct run_args *run_args = arg;

	thrd_detach(thrd_current());

	run_test0(run_args);

        mtx_lock(&test_lock);
        tests_running_cnt--;
        mtx_unlock(&test_lock);

        free(run_args);

        return 0;
}



static int run_test (const char *testname,
                     int (*test_main) (int, char **),
                     int argc, char **argv) {
        int r = 0;

        if (tests_run_in_parallel) {
		thrd_t thr;
                struct run_args *run_args = calloc(1, sizeof(*run_args));
                run_args->testname = testname;
                run_args->test_main = test_main;
                run_args->argc = argc;
                run_args->argv = argv;

                mtx_lock(&test_lock);
                tests_running_cnt++;
                mtx_unlock(&test_lock);

		if (thrd_create(&thr, run_test_from_thread, run_args) !=
		    thrd_success) {
                        mtx_lock(&test_lock);
                        tests_running_cnt--;
                        mtx_unlock(&test_lock);

                        TEST_FAIL("Failed to start thread for test %s\n",
                                  testname);
                }
        } else {
		struct run_args run_args = { .testname = testname,
					     .test_main = test_main,
					     .argc = argc,
					     .argv = argv };

		tests_running_cnt++;
		r = run_test0(&run_args);
		tests_running_cnt--;

                /* Wait for everything to be cleaned up since broker
                 * destroys are handled in its own thread. */
                test_wait_exit(5);

		test_curr = NULL;
        }
        return r;
}

int main(int argc, char **argv) {
	int r = 0;
        const char *tests_to_run = NULL; /* all */
        int i;
	test_timing_t t_all;

	mtx_init(&test_lock, mtx_plain);

#ifndef _MSC_VER
        tests_to_run = getenv("TESTS");
#endif

        for (i = 1 ; i < argc ; i++) {
			if (!strcmp(argv[i], "-p"))
				tests_run_in_parallel = 1;
			else if (i == 1)
				tests_to_run = argv[i];
                else {
                        printf("Unknown option: %s\n"
                               "\n"
							   "Usage: %s [options] [<test-match-substr>]\n"
                               "Options:\n"
                               "  -p     Run tests in parallel\n"
                               "\n",
                               argv[0], argv[i]);
                        exit(1);
                }
        }

	test_curr = "<MAIN>";
	test_start = test_clock();

	TEST_SAY("Tests to run: %s\n", tests_to_run ? tests_to_run : "all");

#define RUN_TEST(NAME) do { \
	extern int main_ ## NAME (int, char **); \
        if (!tests_to_run || strstr(# NAME, tests_to_run)) {     \
                r |= run_test(# NAME, main_ ## NAME, argc, argv);	\
        } else { \
                TEST_SAY("================= Skipping test %s "	\
			 "================\n", # NAME );	\
        } \
	} while (0)

	TIMING_START(&t_all, "ALL-TESTS");
	RUN_TEST(0001_multiobj);
	RUN_TEST(0002_unkpart);
	RUN_TEST(0003_msgmaxsize);
	RUN_TEST(0004_conf);
	RUN_TEST(0005_order);
	RUN_TEST(0006_symbols);
	RUN_TEST(0007_autotopic);
	RUN_TEST(0008_reqacks);
	RUN_TEST(0011_produce_batch);
	RUN_TEST(0012_produce_consume);
        RUN_TEST(0013_null_msgs);
        RUN_TEST(0014_reconsume_191);
	RUN_TEST(0015_offsets_seek);
	RUN_TEST(0017_compression);
	RUN_TEST(0018_cgrp_term);
        RUN_TEST(0019_list_groups);
        RUN_TEST(0020_destroy_hang);
        RUN_TEST(0021_rkt_destroy);

        if (tests_run_in_parallel) {
                mtx_lock(&test_lock);
                while (tests_running_cnt > 0) {
                        TEST_SAY("%d test(s) still running\n",
                                 tests_running_cnt);
                        mtx_unlock(&test_lock);
                        rd_sleep(1);
                        mtx_lock(&test_lock);
                }
                mtx_unlock(&test_lock);
        }

	TIMING_STOP(&t_all);

        /* Wait for everything to be cleaned up since broker destroys are
	 * handled in its own thread. */
	test_wait_exit(tests_run_in_parallel ? 10 : 5);

	/* If we havent failed at this point then
	 * there were no threads leaked */

	TEST_SAY("\n============== ALL TESTS PASSED ==============\n");
	return r;
}





/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

void test_dr_cb (rd_kafka_t *rk, void *payload, size_t len,
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


rd_kafka_t *test_create_producer (void) {
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	char errstr[512];

	test_conf_init(&conf, NULL, 20);

	rd_kafka_conf_set_dr_cb(conf, test_dr_cb);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	return rk;
}

rd_kafka_topic_t *test_create_producer_topic (rd_kafka_t *rk,
	const char *topic, ...) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	va_list ap;
	const char *name, *val;

	test_conf_init(NULL, &topic_conf, 20);

	va_start(ap, topic);
	while ((name = va_arg(ap, const char *)) &&
	       (val = va_arg(ap, const char *))) {
		if (rd_kafka_topic_conf_set(topic_conf, name, val,
			errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
			TEST_FAIL("Conf failed: %s\n", errstr);
	}
	va_end(ap);

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

/**
 * Produces \p cnt messages and returns immediately.
 * Does not wait for delivery.
 * \p msgcounterp is incremented for each produced messages and passed
 * as \p msg_opaque which is later used in test_dr_cb to decrement
 * the counter on delivery.
 */
void test_produce_msgs_nowait (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
                               uint64_t testid, int32_t partition,
                               int msg_base, int cnt,
                               const char *payload, size_t size,
                               int *msgcounterp) {
	int msg_id;
	test_timing_t t_all;

	TEST_SAY("Produce to %s [%"PRId32"]: messages #%d..%d\n",
		 rd_kafka_topic_name(rkt), partition, msg_base, msg_base+cnt);

	TIMING_START(&t_all, "PRODUCE");

	for (msg_id = msg_base ; msg_id < msg_base + cnt ; msg_id++) {
		char key[128];
		char buf[128];
		const char *use_payload;
		size_t use_size;

		if (payload) {
			use_payload = payload;
			use_size = size;
		} else {
			test_msg_fmt(key, sizeof(key), testid, partition,
				     msg_id);
			rd_snprintf(buf, sizeof(buf), "%s: data", key);
			use_payload = buf;
			use_size = strlen(buf);
		}

		if (rd_kafka_produce(rkt, partition,
				     RD_KAFKA_MSG_F_COPY,
				     (void *)use_payload, use_size,
				     key, strlen(key),
				     msgcounterp) == -1)
			TEST_FAIL("Failed to produce message %i "
				  "to partition %i: %s",
				  msg_id, (int)partition,
				  rd_kafka_err2str(rd_kafka_errno2err(errno)));

                (*msgcounterp)++;

        }

	TIMING_STOP(&t_all);
}

/**
 * Waits for the messages tracked by counter \p msgcounterp to be delivered.
 */
void test_wait_delivery (rd_kafka_t *rk, int *msgcounterp) {
	test_timing_t t_all;

        TIMING_START(&t_all, "PRODUCE.DELIVERY.WAIT");

	/* Wait for messages to be delivered */
	while (*msgcounterp > 0 && rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 10);

	TIMING_STOP(&t_all);

}

/**
 * Produces \p cnt messages and waits for succesful delivery
 */
void test_produce_msgs (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
                        uint64_t testid, int32_t partition,
                        int msg_base, int cnt,
			const char *payload, size_t size) {
	int remains = 0;

        test_produce_msgs_nowait(rk, rkt, testid, partition, msg_base, cnt,
                                 payload, size, &remains);

        test_wait_delivery(rk, &remains);
}



/**
 * Create producer, produce \p msgcnt messages to \p topic \p partition,
 * destroy consumer, and returns the used testid.
 */
uint64_t
test_produce_msgs_easy (const char *topic, int32_t partition, int msgcnt) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        uint64_t testid;
        test_timing_t t_produce;

        testid = test_id_generate();
        rk = test_create_producer();
        rkt = test_create_producer_topic(rk, topic, NULL);

        TIMING_START(&t_produce, "PRODUCE");
        test_produce_msgs(rk, rkt, testid, partition, 0, msgcnt, NULL, 0);
        TIMING_STOP(&t_produce);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        return testid;
}


rd_kafka_t *test_create_consumer (const char *group_id,
				  void (*rebalance_cb) (
					  rd_kafka_t *rk,
					  rd_kafka_resp_err_t err,
					  rd_kafka_topic_partition_list_t
					  *partitions,
					  void *opaque),
                                  rd_kafka_topic_conf_t *default_topic_conf,
				  void *opaque) {
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	char errstr[512];
	char tmp[64];

	test_conf_init(&conf, NULL, 20);

        if (group_id) {
                if (rd_kafka_conf_set(conf, "group.id", group_id,
                                      errstr, sizeof(errstr)) !=
                    RD_KAFKA_CONF_OK)
                        TEST_FAIL("Conf failed: %s\n", errstr);
        }

	rd_snprintf(tmp, sizeof(tmp), "%d", test_session_timeout_ms);
	if (rd_kafka_conf_set(conf, "session.timeout.ms", tmp,
			      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		TEST_FAIL("Conf failed: %s\n", errstr);

	rd_kafka_conf_set_opaque(conf, opaque);

	if (rebalance_cb)
		rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        if (default_topic_conf)
                rd_kafka_conf_set_default_topic_conf(conf, default_topic_conf);

	/* Create kafka instance */
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
		TEST_FAIL("Failed to create rdkafka instance: %s\n", errstr);

	TEST_SAY("Created    kafka instance %s\n", rd_kafka_name(rk));

	return rk;
}

rd_kafka_topic_t *test_create_consumer_topic (rd_kafka_t *rk,
                                              const char *topic) {
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_conf_t *topic_conf;

	test_conf_init(NULL, &topic_conf, 20);

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
                          rd_kafka_err2str(rd_kafka_errno2err(errno)));

	return rkt;
}


void test_consumer_start (const char *what,
                          rd_kafka_topic_t *rkt, int32_t partition,
                          int64_t start_offset) {

	TEST_SAY("%s: consumer_start: %s [%"PRId32"] at offset %"PRId64"\n",
		 what, rd_kafka_topic_name(rkt), partition, start_offset);

	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1)
		TEST_FAIL("%s: consume_start failed: %s\n",
			  what, rd_kafka_err2str(rd_kafka_errno2err(errno)));
}

void test_consumer_stop (const char *what,
                         rd_kafka_topic_t *rkt, int32_t partition) {

	TEST_SAY("%s: consumer_stop: %s [%"PRId32"]\n",
		 what, rd_kafka_topic_name(rkt), partition);

	if (rd_kafka_consume_stop(rkt, partition) == -1)
		TEST_FAIL("%s: consume_stop failed: %s\n",
			  what, rd_kafka_err2str(rd_kafka_errno2err(errno)));
}

void test_consumer_seek (const char *what, rd_kafka_topic_t *rkt,
                         int32_t partition, int64_t offset) {
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



/**
 * Returns offset of the last message consumed
 */
int64_t test_consume_msgs (const char *what, rd_kafka_topic_t *rkt,
                           uint64_t testid, int32_t partition, int64_t offset,
                           int exp_msg_base, int exp_cnt, int parse_fmt) {
	int cnt = 0;
	int msg_next = exp_msg_base;
	int fails = 0;
	int64_t offset_last = -1;
	test_timing_t t_first, t_all;

	TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: expect msg #%d..%d "
		 "at offset %"PRId64"\n",
		 what, rd_kafka_topic_name(rkt), partition,
		 exp_msg_base, exp_msg_base+exp_cnt, offset);

	if (offset != TEST_NO_SEEK) {
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

		if (parse_fmt)
			test_msg_parse(testid, rkmessage->key,
				       rkmessage->key_len, partition, &msg_id);
		else
			msg_id = 0;

		if (test_level >= 3)
			TEST_SAY("%s: consume_msgs: %s [%"PRId32"]: "
				 "got msg #%d at offset %"PRId64
				 " (expect #%d at offset %"PRId64")\n",
				 what, rd_kafka_topic_name(rkt), partition,
				 msg_id, rkmessage->offset,
				 msg_next,
				 offset >= 0 ? offset + cnt : -1);

		if (parse_fmt && msg_id != msg_next) {
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


/**
 * Create high-level consumer subscribing to \p topic from BEGINNING
 * and expects \d exp_msgcnt with matching \p testid
 * Destroys consumer when done.
 *
 * If \p group_id is NULL a new unique group is generated
 */
void
test_consume_msgs_easy (const char *group_id, const char *topic,
                        uint64_t testid, int exp_msgcnt) {
        rd_kafka_t *rk;
        rd_kafka_topic_conf_t *tconf;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topics;
	char grpid0[64];

        test_conf_init(NULL, &tconf, 0);

	if (!group_id)
		group_id = test_str_id_generate(grpid0, sizeof(grpid0));

        test_topic_conf_set(tconf, "auto.offset.reset", "smallest");
        rk = test_create_consumer(group_id, NULL, tconf, NULL);

        rd_kafka_poll_set_consumer(rk);

        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);

        TEST_SAY("Subscribing to topic %s in group %s "
                 "(expecting %d msgs with testid %"PRIu64")\n",
                 topic, group_id, exp_msgcnt, testid);

        err = rd_kafka_subscribe(rk, topics);
        if (err)
                TEST_FAIL("Failed to subscribe to %s: %s\n",
                          topic, rd_kafka_err2str(err));

        rd_kafka_topic_partition_list_destroy(topics);

        /* Consume messages */
        test_consumer_poll("consume.easy", rk, testid, -1, -1, exp_msgcnt);

        test_consumer_close(rk);

        rd_kafka_destroy(rk);
}



void test_consumer_assign (const char *what, rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *partitions) {
        rd_kafka_resp_err_t err;
        test_timing_t timing;

        TIMING_START(&timing, "ASSIGN.PARTITIONS");
        err = rd_kafka_assign(rk, partitions);
        TIMING_STOP(&timing);
        if (err)
                TEST_FAIL("%s: failed to assign %d partition(s): %s\n",
			  what, partitions->cnt, rd_kafka_err2str(err));
        else
                TEST_SAY("%s: assigned %d partition(s)\n",
			 what, partitions->cnt);
}


void test_consumer_unassign (const char *what, rd_kafka_t *rk) {
        rd_kafka_resp_err_t err;
        test_timing_t timing;

        TIMING_START(&timing, "UNASSIGN.PARTITIONS");
        err = rd_kafka_assign(rk, NULL);
        TIMING_STOP(&timing);
        if (err)
                TEST_FAIL("%s: failed to unassign current partitions: %s\n",
                          what, rd_kafka_err2str(err));
        else
                TEST_SAY("%s: unassigned current partitions\n", what);
}


void test_verify_rkmessage0 (const char *func, int line,
                             rd_kafka_message_t *rkmessage, uint64_t testid,
                             int32_t partition, int msgnum) {
	uint64_t in_testid;
	int in_part;
	int in_msgnum;
	char buf[128];

	rd_snprintf(buf, sizeof(buf), "%.*s",
		 (int)rkmessage->len, (char *)rkmessage->payload);

	if (sscanf(buf, "testid=%"SCNd64", partition=%i, msg=%i",
		   &in_testid, &in_part, &in_msgnum) != 3)
		TEST_FAIL("Incorrect format: %s", buf);

	if (testid != in_testid ||
	    (partition != -1 && partition != in_part) ||
	    (msgnum != -1 && msgnum != in_msgnum) ||
	    in_msgnum < 0)
		goto fail_match;

	if (test_level > 2) {
		TEST_SAY("%s:%i: Our testid %"PRIu64", part %i (%i), msg %i\n",
			 func, line,
			 testid, (int)partition, (int)rkmessage->partition,
			 msgnum);
	}


        return;

fail_match:
	TEST_FAIL("%s:%i: Our testid %"PRIu64", part %i, msg %i did "
		  "not match message: \"%s\"\n",
		  func, line,
		  testid, (int)partition, msgnum, buf);
}




int test_consumer_poll (const char *what, rd_kafka_t *rk, uint64_t testid,
                        int exp_eof_cnt, int exp_msg_base, int exp_cnt) {
        int eof_cnt = 0;
        int cnt = 0;
        test_timing_t t_cons;

        TEST_SAY("%s: consume %d messages\n", what, exp_cnt);

        TIMING_START(&t_cons, "CONSUME");

        while ((exp_eof_cnt == -1 || eof_cnt < exp_eof_cnt) &&
               (cnt < exp_cnt)) {
                rd_kafka_message_t *rkmessage;

                rkmessage = rd_kafka_consumer_poll(rk, 10*1000);
                if (!rkmessage) /* Shouldn't take this long to get a msg */
                        TEST_FAIL("%s: consumer_poll() timeout\n", what);


                if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        TEST_SAY("%s [%"PRId32"] reached EOF at "
                                 "offset %"PRId64"\n",
                                 rd_kafka_topic_name(rkmessage->rkt),
                                 rkmessage->partition,
                                 rkmessage->offset);
                        eof_cnt++;

                } else if (rkmessage->err) {
                        TEST_SAY("%s [%"PRId32"] error (offset %"PRId64"): %s",
                                 rkmessage->rkt ?
                                 rd_kafka_topic_name(rkmessage->rkt) :
                                 "(no-topic)",
                                 rkmessage->partition,
                                 rkmessage->offset,
                                 rd_kafka_message_errstr(rkmessage));

                } else {
			if (test_level > 2)
				TEST_SAY("%s [%"PRId32"] "
					 "message at offset %"PRId64"\n",
					 rd_kafka_topic_name(rkmessage->rkt),
					 rkmessage->partition,
					 rkmessage->offset);

                        test_verify_rkmessage(rkmessage, testid, -1, -1);
                        cnt++;
                }

                rd_kafka_message_destroy(rkmessage);
        }

        TIMING_STOP(&t_cons);

        TEST_SAY("%s: consumed %d/%d messages (%d/%d EOFs)\n",
                 what, cnt, exp_cnt, eof_cnt, exp_eof_cnt);
        return cnt;
}

void test_consumer_close (rd_kafka_t *rk) {
        rd_kafka_resp_err_t err;
        test_timing_t timing;

        TEST_SAY("Closing consumer\n");

        TIMING_START(&timing, "CONSUMER.CLOSE");
        err = rd_kafka_consumer_close(rk);
        TIMING_STOP(&timing);
        if (err)
                TEST_FAIL("Failed to close consumer: %s\n",
                          rd_kafka_err2str(err));
}


void test_conf_set (rd_kafka_conf_t *conf, const char *name, const char *val) {
        char errstr[512];
        if (rd_kafka_conf_set(conf, name, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                TEST_FAIL("Failed to set config \"%s\"=\"%s\": %s\n",
                          name, val, errstr);
}


void test_topic_conf_set (rd_kafka_topic_conf_t *tconf,
                          const char *name, const char *val) {
        char errstr[512];
        if (rd_kafka_topic_conf_set(tconf, name, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                TEST_FAIL("Failed to set topic config \"%s\"=\"%s\": %s\n",
                          name, val, errstr);
}


void test_print_partition_list (const rd_kafka_topic_partition_list_t
				*partitions) {
        int i;
        for (i = 0 ; i < partitions->cnt ; i++) {
		TEST_SAY(" %s [%"PRId32"]\n",
                        partitions->elems[i].topic,
                        partitions->elems[i].partition);
        }
}
