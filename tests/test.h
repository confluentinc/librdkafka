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
extern const RD_TLS char *test_curr;
extern RD_TLS int64_t test_start;

extern double test_timeout_multiplier;
extern int  test_session_timeout_ms; /* Group session timeout */

#define tmout_multip(msecs)  ((int)(((double)(msecs)) * test_timeout_multiplier))


#define TEST_FAIL(...) do {					\
		fprintf(stderr, "\033[31m### Test \"%s\" failed at %s:%i:%s(): ###\n", \
			test_curr ? test_curr:"(n/a)",                  \
                        __FILE__,__LINE__,__FUNCTION__);                \
		fprintf(stderr, __VA_ARGS__);				\
		fprintf(stderr, "\n");					\
                fprintf(stderr, "### Test random seed was %i ###\033[0m\n",    \
                        test_seed);                                     \
						assert(0); \
		exit(1);						\
	} while (0)


#define TEST_PERROR(call) do {						\
		if (!(call))						\
			TEST_FAIL(#call " failed: %s", rd_strerror(errno)); \
	} while (0)

#define TEST_SAY(...) do {                                              \
	if (test_level >= 2) {                                          \
                fprintf(stderr, "\033[36m[%-28s/%7.3fs] ",		\
			test_curr,					\
			test_start ?					\
			((float)(test_clock() - test_start)/1000000.0f) : 0); \
		fprintf(stderr, __VA_ARGS__);				\
                fprintf(stderr, "\033[0m");                             \
        }                                                               \
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

typedef struct test_timing_s {
	char name[64];
	int64_t ts_start;
	int64_t duration;
} test_timing_t;

#define TIMING_START(TIMING,NAME) do {					\
	rd_snprintf((TIMING)->name, sizeof((TIMING)->name), "%s", (NAME)); \
	(TIMING)->ts_start = test_clock();				\
	(TIMING)->duration = 0;						\
	} while (0)

#define TIMING_STOP(TIMING) do {				\
	(TIMING)->duration = test_clock() - (TIMING)->ts_start; \
	TEST_SAY("%s: duration %.3fms\n",				\
		 (TIMING)->name, (float)(TIMING)->duration / 1000.0f);	\
	} while (0)

#ifndef _MSC_VER
#define rd_sleep(S) sleep(S)
#else
#define rd_sleep(S) Sleep((S)*1000)
#endif




void test_msg_fmt (char *dest, size_t dest_size,
		   uint64_t testid, int32_t partition, int msgid);
void test_msg_parse0 (const char *func, int line,
		      uint64_t testid, const void *ptr, size_t size,
		      int32_t exp_partition, int *msgidp);
#define test_msg_parse(testid,ptr,size,exp_partition,msgidp)	\
	test_msg_parse0(__FUNCTION__,__LINE__,\
			testid,ptr,size,exp_partition,msgidp)


static __inline int jitter (int low, int high) RD_UNUSED;
static __inline int jitter (int low, int high) {
	return (low + (rand() % (high+1)));
}



/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/**
 * Delivery reported callback.
 * Called for each message once to signal its delivery status.
 */
void test_dr_cb (rd_kafka_t *rk, void *payload, size_t len,
                 rd_kafka_resp_err_t err, void *opaque, void *msg_opaque);

rd_kafka_t *test_create_producer (void);
rd_kafka_topic_t *test_create_producer_topic(rd_kafka_t *rk,
	const char *topic, ...);
void test_produce_msgs (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
                        uint64_t testid, int32_t partition,
                        int msg_base, int cnt,
			const char *payload, size_t size);
rd_kafka_t *test_create_consumer (const char *group_id,
				  void (*rebalance_cb) (
					  rd_kafka_t *rk,
					  rd_kafka_resp_err_t err,
					  rd_kafka_topic_partition_list_t
					  *partitions,
					  void *opaque),
                                  rd_kafka_topic_conf_t *default_topic_conf,
				  void *opaque);
rd_kafka_topic_t *test_create_consumer_topic (rd_kafka_t *rk,
                                              const char *topic);
void test_consumer_start (const char *what,
                          rd_kafka_topic_t *rkt, int32_t partition,
                          int64_t start_offset);
void test_consumer_stop (const char *what,
                         rd_kafka_topic_t *rkt, int32_t partition);
void test_consumer_seek (const char *what, rd_kafka_topic_t *rkt,
                         int32_t partition, int64_t offset);

#define TEST_NO_SEEK  -1
int64_t test_consume_msgs (const char *what, rd_kafka_topic_t *rkt,
                           uint64_t testid, int32_t partition, int64_t offset,
                           int exp_msg_base, int exp_cnt, int parse_fmt);


int test_consumer_poll (const char *what, rd_kafka_t *rk, uint64_t testid,
                        int exp_eof_cnt, int exp_msg_base, int exp_cnt);

void test_consumer_assign (const char *what, rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *parts);
void test_consumer_unassign (const char *what, rd_kafka_t *rk);

void test_consumer_close (rd_kafka_t *rk);

void test_print_partition_list (const rd_kafka_topic_partition_list_t
				*partitions);
