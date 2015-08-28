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
#include <stdlib.h>

#ifndef _MSC_VER
#include <pthread.h>
#endif


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


int test_level = 2;
int test_seed = 0;

static char test_topic_prefix[128] = "rdkafkatest";
static int  test_topic_random = 0;
static int  tests_run_in_parallel = 0;
static int  tests_running_cnt = 0;

#ifndef _MSC_VER
static pthread_mutex_t test_lock;
#endif

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
                rd_snprintf(ret, sizeof(ret), "%s_%"PRIx64"_%s",
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
                *conf = rd_kafka_conf_new();
                rd_kafka_conf_set_error_cb(*conf, test_error_cb);
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
                        timeout = (int)((float)timeout * strtod(val, NULL));
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

	/* Limit the test run time. */
#ifndef _MSC_VER
	alarm(timeout);
	signal(SIGALRM, sig_alarm);
#endif
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


#ifndef _MSC_VER
struct run_args {
        const char *testname;
        int (*test_main) (int, char **);
        int argc;
        char **argv;
};
static void *run_test_from_thread (void *arg) {
        struct run_args *run_args = arg;
        int r;
	test_timing_t t_run;
	
        pthread_detach(pthread_self());

	TIMING_START(&t_run, run_args->testname);

        r = run_args->test_main(run_args->argc, run_args->argv);

	TIMING_STOP(&t_run);

        TEST_SAY("================= Test %s %s =================\n",
                 run_args->testname, r ? "FAILED" : "PASSED");

        pthread_mutex_lock(&test_lock);
        tests_running_cnt--;
        pthread_mutex_unlock(&test_lock);

        free(run_args);

        return NULL;
}
#endif



static int run_test (const char *testname,
                     int (*test_main) (int, char **),
                     int argc, char **argv) {
        int r;

        if (tests_run_in_parallel) {
#ifdef _MSC_VER
                TEST_FAIL("Parallel runs not supported on this platform, yet\n");
#else
                pthread_t thr;
                struct run_args *run_args = calloc(1, sizeof(*run_args));
                run_args->testname = testname;
                run_args->test_main = test_main;
                run_args->argc = argc;
                run_args->argv = argv;

                pthread_mutex_lock(&test_lock);
                tests_running_cnt++;
                pthread_mutex_unlock(&test_lock);

                r = pthread_create(&thr, NULL, run_test_from_thread, run_args);
                if (r != 0) {
                        pthread_mutex_lock(&test_lock);
                        tests_running_cnt--;
                        pthread_mutex_unlock(&test_lock);

                        TEST_FAIL("Failed to start thread for test %s: %s\n",
                                  testname, strerror(r));
                }
#endif
        } else {
		test_timing_t t_run;

		TIMING_START(&t_run, testname);
                tests_running_cnt++;
                r = test_main(argc, argv);
                tests_running_cnt--;
		TIMING_STOP(&t_run);
		
                /* Wait for everything to be cleaned up since broker
                 * destroys are handled in its own thread. */
                test_wait_exit(10);

        }
        return r;
}

int main(int argc, char **argv) {
	int r = 0;
        const char *tests_to_run = NULL; /* all */
        int i;

#ifndef _MSC_VER
        tests_to_run = getenv("TESTS");
#endif

        for (i = 1 ; i < argc ; i++) {
                if (!strcmp(argv[i], "-p") )
                        tests_run_in_parallel = 1;
                else {
                        printf("Unknown option: %s\n"
                               "\n"
                               "Usage: %s [options]\n"
                               "Options:\n"
                               "  -p     Run tests in parallel\n"
                               "\n",
                               argv[0], argv[i]);
                        exit(1);
                }
        }

        printf("Tests to run: %s\n", tests_to_run ? tests_to_run : "all");

#define RUN_TEST(NAME) do { \
	extern int main_ ## NAME (int, char **); \
        if (!tests_to_run || strstr(# NAME, tests_to_run)) {     \
                int _r;                                                 \
		TEST_SAY("================= Run test %s %s=================\n", # NAME, tests_run_in_parallel ? "in parallel " : ""); \
                _r = run_test(# NAME, main_ ## NAME, argc, argv);        \
		TEST_SAY("================= Test %s %s =================\n", \
                         # NAME, \
                         _r ? (tests_run_in_parallel ? "FAILED TO START":"FAILED") : \
                         (tests_run_in_parallel ? "STARTED" : "PASSED")); \
		r |= _r; \
        } else { \
                TEST_SAY("================= Skipping test %s ================\n", # NAME ); \
        } \
	} while (0)
	RUN_TEST(0001_multiobj);
	RUN_TEST(0002_unkpart);
	RUN_TEST(0003_msgmaxsize);
	RUN_TEST(0004_conf);
	RUN_TEST(0005_order);
	RUN_TEST(0006_symbols);
	RUN_TEST(0007_autotopic);
	RUN_TEST(0008_reqacks);
	RUN_TEST(0010_enforcereqacks);
	RUN_TEST(0011_produce_batch);
	RUN_TEST(0012_produce_consume);
        RUN_TEST(0013_null_msgs);
        RUN_TEST(0014_reconsume_191);
	RUN_TEST(0015_offsets_seek);

        if (tests_run_in_parallel) {
                while (tests_running_cnt > 0)
                        rd_sleep(1);
        }

        /* Wait for everything to be cleaned up since broker destroys are
	 * handled in its own thread. */
	test_wait_exit(10);

	/* If we havent failed at this point then
	 * there were no threads leaked */

	TEST_SAY("\n============== ALL TESTS PASSED ==============\n");
	return r;
}
