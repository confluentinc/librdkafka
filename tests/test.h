#pragma once

#include "../src/rd.h"

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
#include "../src/tinycthread.h"

#ifdef _MSC_VER
#define sscanf(...) sscanf_s(__VA_ARGS__)
#endif

/**
 * Test output is controlled through "TEST_LEVEL=N" environemnt variable.
 * N < 2: TEST_SAY() is quiet.
 */
extern int test_level;

extern int test_seed;
extern RD_TLS struct test *test_curr;
extern int test_assert_on_fail;
extern int tests_running_cnt;
extern double test_timeout_multiplier;
extern int  test_session_timeout_ms; /* Group session timeout */

extern mtx_t test_mtx;

#define TEST_LOCK()   mtx_lock(&test_mtx)
#define TEST_UNLOCK() mtx_unlock(&test_mtx)


static __inline RD_UNUSED
int tmout_multip (int msecs) {
        int r;
        TEST_LOCK();
        r = (int)(((double)(msecs)) * test_timeout_multiplier);
        TEST_UNLOCK();
        return r;
}



#define _C_CLR "\033[0m"
#define _C_RED "\033[31m"
#define _C_GRN "\033[32m"
#define _C_YEL "\033[33m"
#define _C_BLU "\033[34m"
#define _C_MAG "\033[35m"
#define _C_CYA "\033[36m"

struct test {
        /**
         * Setup
         */
        const char *name;    /**< e.g. Same as filename minus extension */
        int (*mainfunc) (int argc, char **argv); /**< test's main func */
        const int flags;     /**< Test flags */
#define TEST_F_LOCAL   0x1   /**< Test is local, no broker requirement */

        /**
         * Runtime
         */
        int64_t start;
        int64_t duration;
        FILE   *stats_fp;
        enum {
                TEST_NOT_STARTED,
                TEST_SKIPPED,
                TEST_RUNNING,
                TEST_PASSED,
                TEST_FAILED,
        } state;
};


#define TEST_FAIL(...) do {                                             \
                int is_thrd = 0;                                        \
		fprintf(stderr, "\033[31m### Test \"%s\" failed at %s:%i:%s(): ###\n", \
			test_curr->name,                                \
                        __FILE__,__LINE__,__FUNCTION__);                \
		fprintf(stderr, __VA_ARGS__);				\
		fprintf(stderr, "\n");					\
                fprintf(stderr, "### Test random seed was %i ###\033[0m\n",    \
                        test_seed);                                     \
                TEST_LOCK();                                            \
                test_curr->state = TEST_FAILED;                         \
                if (test_curr->mainfunc) {                              \
                        tests_running_cnt--;                            \
                        is_thrd = 1;                                    \
                }                                                       \
                TEST_UNLOCK();                                          \
                if (test_assert_on_fail || !is_thrd)                    \
                        assert(0);                                      \
                else                                                    \
                        thrd_exit(0);                                   \
	} while (0)


#define TEST_PERROR(call) do {						\
		if (!(call))						\
			TEST_FAIL(#call " failed: %s", rd_strerror(errno)); \
	} while (0)

#define TEST_WARN(...) do {                                              \
                fprintf(stderr, "\033[33m[%-28s/%7.3fs] WARN: ",	\
			test_curr->name,                                \
			test_curr->start ?                              \
			((float)(test_clock() -                         \
                                 test_curr->start)/1000000.0f) : 0);    \
		fprintf(stderr, __VA_ARGS__);				\
                fprintf(stderr, "\033[0m");                             \
	} while (0)

#define TEST_SAY0(...)  fprintf(stderr, __VA_ARGS__)
#define TEST_SAY(...) do {                                              \
	if (test_level >= 2) {                                          \
                fprintf(stderr, "\033[36m[%-28s/%7.3fs] ",		\
			test_curr->name,                                \
			test_curr->start ?                              \
			((float)(test_clock() -                         \
                                 test_curr->start)/1000000.0f) : 0);    \
		fprintf(stderr, __VA_ARGS__);				\
                fprintf(stderr, "\033[0m");                             \
        }                                                               \
	} while (0)

#define TEST_REPORT(...) do {		\
	fprintf(stdout, __VA_ARGS__);			\
	} while(0)

/* "..." is a failure reason in printf format, include as much info as needed */
#define TEST_ASSERT(expr,...) do {            \
        if (!(expr)) {                        \
                      TEST_FAIL("Test assertion failed: \"" # expr  "\": " \
                                __VA_ARGS__);                           \
                      }                                                 \
        } while (0)


const char *test_mk_topic_name (const char *suffix, int randomized);

void test_conf_init (rd_kafka_conf_t **conf, rd_kafka_topic_conf_t **topic_conf,
		     int timeout);


void test_wait_exit (int timeout);

uint64_t test_id_generate (void);
char *test_str_id_generate (char *dest, size_t dest_size);


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
	return (int64_t)GetTickCount64() * 1000LL;
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

#define TIMING_DURATION(TIMING) ((TIMING)->duration)

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
void test_wait_delivery (rd_kafka_t *rk, int *msgcounterp);
void test_produce_msgs_nowait (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
                               uint64_t testid, int32_t partition,
                               int msg_base, int cnt,
                               const char *payload, size_t size,
                               int *msgcounterp);
void test_produce_msgs (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
                        uint64_t testid, int32_t partition,
                        int msg_base, int cnt,
			const char *payload, size_t size);
uint64_t
test_produce_msgs_easy (const char *topic, uint64_t testid,
                        int32_t partition, int msgcnt);
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
rd_kafka_topic_t *test_create_topic (rd_kafka_t *rk,
                                     const char *topic, ...);
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


void test_verify_rkmessage0 (const char *func, int line,
                             rd_kafka_message_t *rkmessage, uint64_t testid,
                             int32_t partition, int msgnum);
#define test_verify_rkmessage(rkmessage,testid,partition,msgnum) \
        test_verify_rkmessage0(__FUNCTION__,__LINE__,\
                               rkmessage,testid,partition,msgnum)

void
test_consume_msgs_easy (const char *group_id, const char *topic,
                        uint64_t testid, int exp_msgcnt);

int test_consumer_poll (const char *what, rd_kafka_t *rk, uint64_t testid,
                        int exp_eof_cnt, int exp_msg_base, int exp_cnt);

void test_consumer_assign (const char *what, rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *parts);
void test_consumer_unassign (const char *what, rd_kafka_t *rk);

void test_consumer_close (rd_kafka_t *rk);

void test_conf_set (rd_kafka_conf_t *conf, const char *name, const char *val);
void test_topic_conf_set (rd_kafka_topic_conf_t *tconf,
                          const char *name, const char *val);

void test_print_partition_list (const rd_kafka_topic_partition_list_t
				*partitions);
