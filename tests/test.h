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
extern char test_mode[64];
extern RD_TLS struct test *test_curr;
extern int test_assert_on_fail;
extern int tests_running_cnt;
extern double test_timeout_multiplier;
extern int  test_session_timeout_ms; /* Group session timeout */
extern int  test_flags;
extern int  test_neg_flags;

extern mtx_t test_mtx;

#define TEST_LOCK()   mtx_lock(&test_mtx)
#define TEST_UNLOCK() mtx_unlock(&test_mtx)


static RD_INLINE RD_UNUSED
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
#define TEST_F_KNOWN_ISSUE 0x2 /**< Known issue, can fail without affecting
				*   total test run status. */
	int minver;          /**< Limit tests to broker version range. */
	int maxver;

	const char *extra;   /**< Extra information to print in test_summary. */

	char **report_arr;   /**< Test-specific reporting, JSON array of objects. */
	int report_cnt;
	int report_size;

        /**
         * Runtime
         */
        int64_t start;
        int64_t duration;
        FILE   *stats_fp;
	int64_t timeout;
        enum {
                TEST_NOT_STARTED,
                TEST_SKIPPED,
                TEST_RUNNING,
                TEST_PASSED,
                TEST_FAILED,
        } state;
};


/** @brief Broker version to int */
#define TEST_BRKVER(A,B,C,D) \
	(((A) << 24) | ((B) << 16) | ((C) << 8) | (D))
/** @brief return single version component from int */
#define TEST_BRKVER_X(V,I) \
	(((V) >> (24-((I)*8))) & 0xff)

extern int test_broker_version;


#define TEST_FAIL0(fail_now,...) do {					\
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
		if (!fail_now) break;					\
                if (test_assert_on_fail || !is_thrd)                    \
                        assert(0);                                      \
                else                                                    \
                        thrd_exit(0);                                   \
	} while (0)

/* Whine and abort test */
#define TEST_FAIL(...) TEST_FAIL0(1, __VA_ARGS__)

/* Whine right away, mark the test as failed, but continue the test. */
#define TEST_FAIL_LATER(...) TEST_FAIL0(0, __VA_ARGS__)


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
#define TEST_SAYL(LVL,...) do {						\
	if (test_level >= LVL) {                                        \
                fprintf(stderr, "\033[36m[%-28s/%7.3fs] ",		\
			test_curr->name,                                \
			test_curr->start ?                              \
			((float)(test_clock() -                         \
                                 test_curr->start)/1000000.0f) : 0);    \
		fprintf(stderr, __VA_ARGS__);				\
                fprintf(stderr, "\033[0m");                             \
        }                                                               \
	} while (0)
#define TEST_SAY(...) TEST_SAYL(2, __VA_ARGS__)

/**
 * Append JSON object (as string) to this tests' report array.
 */
#define TEST_REPORT(...) test_report_add(test_curr, __VA_ARGS__)


/* "..." is a failure reason in printf format, include as much info as needed */
#define TEST_ASSERT(expr,...) do {            \
        if (!(expr)) {                        \
                      TEST_FAIL("Test assertion failed: \"" # expr  "\": " \
                                __VA_ARGS__);                           \
                      }                                                 \
        } while (0)

/* Skip the current test. Argument is textual reason (printf format) */
#define TEST_SKIP(...) do {		     \
		TEST_WARN("SKIPPING TEST: " __VA_ARGS__); \
		TEST_LOCK();			     \
		test_curr->state = TEST_SKIPPED;     \
		TEST_UNLOCK();			     \
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
static RD_INLINE int64_t test_clock (void)
#ifndef _MSC_VER
__attribute__((unused))
#endif
;
static RD_INLINE int64_t test_clock (void) {
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
	int64_t ts_every; /* Last every */
} test_timing_t;

#define TIMING_START(TIMING,NAME) do {					\
	rd_snprintf((TIMING)->name, sizeof((TIMING)->name), "%s", (NAME)); \
	(TIMING)->ts_start = test_clock();				\
	(TIMING)->duration = 0;						\
	(TIMING)->ts_every = (TIMING)->ts_start;			\
	} while (0)

#define TIMING_STOP(TIMING) do {				\
	(TIMING)->duration = test_clock() - (TIMING)->ts_start; \
	TEST_SAY("%s: duration %.3fms\n",				\
		 (TIMING)->name, (float)(TIMING)->duration / 1000.0f);	\
	} while (0)

#define TIMING_DURATION(TIMING) ((TIMING)->duration ? (TIMING)->duration : \
				 (test_clock() - (TIMING)->ts_start))

/* Trigger something every US microseconds. */
static RD_UNUSED int TIMING_EVERY (test_timing_t *timing, int us) {
	int64_t now = test_clock();
	if (timing->ts_every + us <= now) {
		timing->ts_every = now;
		return 1;
	}
	return 0;
}

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


static RD_INLINE int jitter (int low, int high) RD_UNUSED;
static RD_INLINE int jitter (int low, int high) {
	return (low + (rand() % (high+1)));
}



/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/



/****************************************************************
 * Message verification services				*
 *								*
 *								*
 *								*
 ****************************************************************/


/**
 * A test_msgver_t is first fed with messages from any number of
 * topics and partitions, it is then checked for expected messages, such as:
 *   - all messages received, based on message payload information.
 *   - messages received in order
 *   - EOF
 */
typedef struct test_msgver_s {
	struct test_mv_p **p;  /* Partitions array */
	int p_cnt;             /* Partition count */
	int p_size;            /* p size */
	int msgcnt;            /* Total message count */
	uint64_t testid;       /* Only accept messages for this testid */

	struct test_msgver_s *fwd;  /* Also forward add_msg() to this mv */

	int log_cnt;           /* Current number of warning logs */
	int log_max;           /* Max warning logs before suppressing. */
	int log_suppr_cnt;     /* Number of suppressed log messages. */
} test_msgver_t;

/* Message */
struct test_mv_m {
	int64_t offset;   /* Message offset */
	int     msgid;    /* Message id */
};


/* Message vector */
struct test_mv_mvec {
	struct test_mv_m *m;
	int cnt;
	int size;  /* m[] size */
};

/* Partition */
struct test_mv_p {
	char *topic;
	int32_t partition;
	struct test_mv_mvec mvec;
	int64_t eof_offset;
};

/* Verification state */
struct test_mv_vs {
	int msg_base;
	int exp_cnt;

	/* used by verify_range */
	int msgid_min;
	int msgid_max;

	struct test_mv_mvec mvec;
} vs;


void test_msgver_init (test_msgver_t *mv, uint64_t testid);
void test_msgver_clear (test_msgver_t *mv);
int test_msgver_add_msg0 (const char *func, int line,
			  test_msgver_t *mv, rd_kafka_message_t *rkm);
#define test_msgver_add_msg(mv,rkm) \
	test_msgver_add_msg0(__FUNCTION__,__LINE__,mv,rkm)

/**
 * Flags to indicate what to verify.
 */
#define TEST_MSGVER_ORDER    0x1  /* Order */
#define TEST_MSGVER_DUP      0x2  /* Duplicates */
#define TEST_MSGVER_RANGE    0x4  /* Range of messages */

#define TEST_MSGVER_ALL      0xf  /* All verifiers */

#define TEST_MSGVER_BY_MSGID  0x10000 /* Verify by msgid (unique in testid) */
#define TEST_MSGVER_BY_OFFSET 0x20000 /* Verify by offset (unique in partition)*/

/* Only test per partition, not across all messages received on all partitions.
 * This is useful when doing incremental verifications with multiple partitions
 * and the total number of messages has not been received yet.
 * Can't do range check here since messages may be spread out on multiple
 * partitions and we might just have read a few partitions. */
#define TEST_MSGVER_PER_PART ((TEST_MSGVER_ALL & ~TEST_MSGVER_RANGE) | \
			      TEST_MSGVER_BY_MSGID | TEST_MSGVER_BY_OFFSET)

/* Test on all messages across all partitions.
 * This can only be used to check with msgid, not offset since that
 * is partition local. */
#define TEST_MSGVER_ALL_PART (TEST_MSGVER_ALL | TEST_MSGVER_BY_MSGID)


int test_msgver_verify_part0 (const char *func, int line, const char *what,
			      test_msgver_t *mv, int flags,
			      const char *topic, int partition,
			      int msg_base, int exp_cnt);
#define test_msgver_verify_part(what,mv,flags,topic,partition,msg_base,exp_cnt) \
	test_msgver_verify_part0(__FUNCTION__,__LINE__,			\
				 what,mv,flags,topic,partition,msg_base,exp_cnt)

int test_msgver_verify0 (const char *func, int line, const char *what,
			 test_msgver_t *mv, int flags,
			 int msg_base, int exp_cnt);
#define test_msgver_verify(what,mv,flags,msg_base,exp_cnt)		\
	test_msgver_verify0(__FUNCTION__,__LINE__,			\
			    what,mv,flags,msg_base,exp_cnt)


rd_kafka_t *test_create_handle (int mode, rd_kafka_conf_t *conf);

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
				  rd_kafka_conf_t *conf,
                                  rd_kafka_topic_conf_t *default_topic_conf,
				  void *opaque);
rd_kafka_topic_t *test_create_consumer_topic (rd_kafka_t *rk,
                                              const char *topic);
rd_kafka_topic_t *test_create_topic_object (rd_kafka_t *rk,
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

void test_consumer_subscribe (rd_kafka_t *rk, const char *topic);

void
test_consume_msgs_easy (const char *group_id, const char *topic,
                        uint64_t testid, int exp_eofcnt, int exp_msgcnt,
			rd_kafka_topic_conf_t *tconf);

void test_consumer_poll_no_msgs (const char *what, rd_kafka_t *rk,
				 uint64_t testid, int timeout_ms);
int test_consumer_poll_once (rd_kafka_t *rk, test_msgver_t *mv, int timeout_ms);
int test_consumer_poll (const char *what, rd_kafka_t *rk, uint64_t testid,
                        int exp_eof_cnt, int exp_msg_base, int exp_cnt,
			test_msgver_t *mv);

void test_consumer_assign (const char *what, rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *parts);
void test_consumer_unassign (const char *what, rd_kafka_t *rk);

void test_consumer_close (rd_kafka_t *rk);

void test_conf_set (rd_kafka_conf_t *conf, const char *name, const char *val);
char *test_conf_get (rd_kafka_conf_t *conf, const char *name);
void test_topic_conf_set (rd_kafka_topic_conf_t *tconf,
                          const char *name, const char *val);

void test_print_partition_list (const rd_kafka_topic_partition_list_t
				*partitions);

void test_create_topic (const char *topicname, int partition_cnt,
			int replication_factor);
int test_check_builtin (const char *feature);
void test_timeout_set (int timeout);

char *tsprintf (const char *fmt, ...) RD_FORMAT(printf, 1, 2);

void test_report_add (struct test *test, const char *fmt, ...);
