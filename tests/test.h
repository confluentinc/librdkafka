#pragma once

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>

#include "rdkafka.h"

/**
 * Test output is controlled through "TEST_LEVEL=N" environemnt variable.
 * N < 2: TEST_SAY() is quiet.
 */
extern int test_level;

extern int test_seed;

#define TEST_FAIL(reason...) do {					\
		fprintf(stderr, "### Test failed at %s:%i:%s(): ###\n", \
			__FILE__,__LINE__,__FUNCTION__);		\
		fprintf(stderr, reason);				\
		fprintf(stderr, "\n");					\
                fprintf(stderr, "### Test random seed was %i ###\n",    \
                        test_seed);                                     \
		exit(1);						\
	} while (0)


#define TEST_PERROR(call) do {						\
		if (!(call))						\
			TEST_FAIL(#call " failed: %s", strerror(errno)); \
	} while (0)

#define TEST_SAY(what...) do {			\
	if (test_level >= 2)			\
		fprintf(stderr, what);		\
	} while (0)

#define TEST_REPORT(what...) do {		\
	fprintf(stdout, what);			\
	} while(0)


void test_conf_init (rd_kafka_conf_t **conf, rd_kafka_topic_conf_t **topic_conf,
		     int timeout);


void test_wait_exit (int timeout);

uint64_t test_id_generate (void);


/**
 * A microsecond monotonic clock
 */
static inline int64_t test_clock (void) __attribute__((unused));
static inline int64_t test_clock (void) {
#ifdef __APPLE__
	/* No monotonic clock on Darwin */
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return ((int64_t)tv.tv_sec * 1000000LLU) + (int64_t)tv.tv_usec;
#else
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ((int64_t)ts.tv_sec * 1000000LLU) +
		((int64_t)ts.tv_nsec / 1000LLU);
#endif
}
