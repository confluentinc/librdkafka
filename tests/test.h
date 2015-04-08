#pragma once

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <sys/time.h>
#endif
#include <errno.h>
#include <assert.h>
#include <time.h>

#include "rdkafka.h"

#include "../src/rd.h"

#ifdef _MSC_VER
#define sscanf(...) sscanf_s(__VA_ARGS__)
#endif

/**
 * Test output is controlled through "TEST_LEVEL=N" environemnt variable.
 * N < 2: TEST_SAY() is quiet.
 */
extern int test_level;

extern int test_seed;

#define TEST_FAIL(...) do {					\
		fprintf(stderr, "### Test failed at %s:%i:%s(): ###\n", \
			__FILE__,__LINE__,__FUNCTION__);		\
		fprintf(stderr, __VA_ARGS__);				\
		fprintf(stderr, "\n");					\
                fprintf(stderr, "### Test random seed was %i ###\n",    \
                        test_seed);                                     \
						assert(0); \
		exit(1);						\
	} while (0)


#define TEST_PERROR(call) do {						\
		if (!(call))						\
			TEST_FAIL(#call " failed: %s", rd_strerror(errno)); \
	} while (0)

#define TEST_SAY(...) do {			\
	if (test_level >= 2)			\
		fprintf(stderr, __VA_ARGS__);		\
	} while (0)

#define TEST_REPORT(...) do {		\
	fprintf(stdout, __VA_ARGS__);			\
	} while(0)


const char *test_mk_topic_name (const char *suffix, int randomized);

void test_conf_init (rd_kafka_conf_t **conf, rd_kafka_topic_conf_t **topic_conf,
		     int timeout);


void test_wait_exit (int timeout);

uint64_t test_id_generate (void);


/**
 * A microsecond monotonic clock
 */
static __inline int64_t test_clock (void)
#ifndef _MSC_VER
__attribute__((unused))
#endif
;
static __inline int64_t test_clock (void) {
#ifdef __APPLE__
	/* No monotonic clock on Darwin */
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return ((int64_t)tv.tv_sec * 1000000LLU) + (int64_t)tv.tv_usec;
#elif _MSC_VER
	return (int64_t)GetTickCount64() * 1000LLU;
#else
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ((int64_t)ts.tv_sec * 1000000LLU) +
		((int64_t)ts.tv_nsec / 1000LLU);
#endif
}


#ifndef _MSC_VER
#define rd_sleep(S) sleep(S)
#else
#define rd_sleep(S) Sleep((S)*1000)
#endif
