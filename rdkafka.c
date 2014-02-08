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


#define _GNU_SOURCE
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <syslog.h>
#include <pthread.h>

#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_broker.h"
#include "rdkafka_topic.h"
#include "rdkafka_offset.h"

#include "rdtime.h"

static pthread_once_t rd_kafka_global_init_once = PTHREAD_ONCE_INIT;


/**
 * Current number of threads created by rdkafka.
 * This is used in regression tests.
 */
int rd_kafka_thread_cnt_curr = 0;
int rd_kafka_thread_cnt (void) {
	return rd_kafka_thread_cnt_curr;
}

/**
 * Current number of live rd_kafka_t handles.
 * This is used by rd_kafka_wait_destroyed() to know when the library
 * has fully cleaned up after itself.
 */
static int rd_kafka_handle_cnt_curr = 0; /* atomic */

/**
 * Wait for all rd_kafka_t objects to be destroyed.
 * Returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 */
int rd_kafka_wait_destroyed (int timeout_ms) {
	rd_ts_t timeout = rd_clock() + (timeout_ms * 1000);

	while (rd_kafka_thread_cnt() > 0 ||
               rd_kafka_handle_cnt_curr > 0) {
		if (rd_clock() >= timeout) {
			errno = ETIMEDOUT;
			return -1;
		}
		usleep(25000); /* 25ms */
	}

	return 0;
}


/**
 * Wrapper for pthread_cond_timedwait() that makes it simpler to use
 * for delta timeouts.
 * `timeout_ms' is the delta timeout in milliseconds.
 */
int pthread_cond_timedwait_ms (pthread_cond_t *cond,
			       pthread_mutex_t *mutex,
			       int timeout_ms) {
	struct timeval tv;
	struct timespec ts;

	gettimeofday(&tv, NULL);
	TIMEVAL_TO_TIMESPEC(&tv, &ts);

	ts.tv_sec  += timeout_ms / 1000;
	ts.tv_nsec += (timeout_ms % 1000) * 1000000;

	if (ts.tv_nsec > 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	return pthread_cond_timedwait(cond, mutex, &ts);
}


void rd_kafka_log_buf (const rd_kafka_t *rk, int level,
		       const char *fac, const char *buf) {

	if (!rk->rk_log_cb || level > rk->rk_log_level)
		return;

	rk->rk_log_cb(rk, level, fac, buf);
}

void rd_kafka_log0 (const rd_kafka_t *rk, const char *extra, int level,
		   const char *fac, const char *fmt, ...) {
	char buf[2048];
	va_list ap;
	int elen = 0;

	if (!rk->rk_log_cb || level > rk->rk_log_level)
		return;

	if (extra) {
		elen = snprintf(buf, sizeof(buf), "%s: ", extra);
		if (unlikely(elen >= sizeof(buf)))
			elen = sizeof(buf);
	}

	va_start(ap, fmt);
	vsnprintf(buf+elen, sizeof(buf)-elen, fmt, ap);
	va_end(ap);

	rk->rk_log_cb(rk, level, fac, buf);
}



void rd_kafka_log_print (const rd_kafka_t *rk, int level,
			 const char *fac, const char *buf) {
	struct timeval tv;

	gettimeofday(&tv, NULL);

	fprintf(stderr, "%%%i|%u.%03u|%s|%s| %s\n",
		level, (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		fac, rk ? rk->rk_name : "", buf);
}

void rd_kafka_log_syslog (const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf) {
	static int initialized = 0;

	if (!initialized)
		openlog("rdkafka", LOG_PID|LOG_CONS, LOG_USER);

	syslog(level, "%s: %s: %s", fac, rk ? rk->rk_name : "", buf);
}


void rd_kafka_set_logger (rd_kafka_t *rk,
			  void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf)) {
	rk->rk_log_cb = func;
}

void rd_kafka_set_log_level (rd_kafka_t *rk, int level) {
	rk->rk_log_level = level;
}







rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type) {
	rd_kafka_op_t *rko;

	rko = calloc(1, sizeof(*rko));
	rko->rko_type = type;

	return rko;
}


void rd_kafka_op_destroy (rd_kafka_op_t *rko) {
	
	/* Decrease refcount on rkbuf to eventually free the shared buffer */
	if (rko->rko_rkbuf)
		rd_kafka_buf_destroy(rko->rko_rkbuf);
	else if (rko->rko_payload && rko->rko_flags & RD_KAFKA_OP_F_FREE)
		free(rko->rko_payload);
	
	free(rko);
}

/**
 * Destroy a queue. The queue must be empty.
 */
void rd_kafka_q_destroy (rd_kafka_q_t *rkq) {
	assert(TAILQ_EMPTY(&rkq->rkq_q));
	pthread_mutex_destroy(&rkq->rkq_lock);
	pthread_cond_destroy(&rkq->rkq_cond);
}

/**
 * Initialize a queue.
 */
void rd_kafka_q_init (rd_kafka_q_t *rkq) {
	TAILQ_INIT(&rkq->rkq_q);
	rkq->rkq_qlen = 0;
	
	pthread_mutex_init(&rkq->rkq_lock, NULL);
	pthread_cond_init(&rkq->rkq_cond, NULL);
}


/**
 * Purge all entries from a queue.
 */
void rd_kafka_q_purge (rd_kafka_q_t *rkq) {
	rd_kafka_op_t *rko, *next;

	pthread_mutex_lock(&rkq->rkq_lock);
	next = TAILQ_FIRST(&rkq->rkq_q);
	while ((rko = next)) {
		next = TAILQ_NEXT(next, rko_link);
		rd_kafka_op_destroy(rko);
	}

	TAILQ_INIT(&rkq->rkq_q);
	(void)rd_atomic_set(&rkq->rkq_qlen, 0);

	pthread_mutex_unlock(&rkq->rkq_lock);
}


/**
 * Move 'cnt' entries from 'srcq' to 'dstq'.
 * Returns the number of entries moved.
 */
size_t rd_kafka_q_move_cnt (rd_kafka_q_t *dstq, rd_kafka_q_t *srcq,
			    size_t cnt) {
	rd_kafka_op_t *rko;
	size_t mcnt = 0;

	pthread_mutex_lock(&srcq->rkq_lock);
	pthread_mutex_lock(&dstq->rkq_lock);

	/* Optimization, if 'cnt' is equal/larger than all items of 'srcq'
	 * we can move the entire queue. */
	if (cnt >= srcq->rkq_qlen) {
		mcnt = srcq->rkq_qlen;
		TAILQ_CONCAT(&dstq->rkq_q, &srcq->rkq_q, rko_link);
		TAILQ_INIT(&srcq->rkq_q);
		(void)rd_atomic_set(&srcq->rkq_qlen, 0);
		(void)rd_atomic_add(&dstq->rkq_qlen, mcnt);
	} else {
		while (mcnt < cnt && (rko = TAILQ_FIRST(&srcq->rkq_q))) {
			TAILQ_REMOVE(&srcq->rkq_q, rko, rko_link);
			TAILQ_INSERT_TAIL(&dstq->rkq_q, rko, rko_link);
			(void)rd_atomic_sub(&dstq->rkq_qlen, 1);
			(void)rd_atomic_add(&dstq->rkq_qlen, 1);
			mcnt++;
		}
	}

	pthread_mutex_unlock(&dstq->rkq_lock);
	pthread_mutex_unlock(&srcq->rkq_lock);

	return mcnt;
}


/**
 * Pop an op from a queue.
 *
 * Locality: any thread.
 */
rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms) {
	rd_kafka_op_t *rko;

	pthread_mutex_lock(&rkq->rkq_lock);

	while (!(rko = TAILQ_FIRST(&rkq->rkq_q)) &&
	       timeout_ms != RD_POLL_NOWAIT) {

		if (timeout_ms != RD_POLL_INFINITE) {
			if (pthread_cond_timedwait_ms(&rkq->rkq_cond,
						      &rkq->rkq_lock,
						      timeout_ms) ==
			    ETIMEDOUT) {
				pthread_mutex_unlock(&rkq->rkq_lock);
				return NULL;
			}
			timeout_ms = 0;
		} else
			pthread_cond_wait(&rkq->rkq_cond, &rkq->rkq_lock);
	}

	if (rko) {
		TAILQ_REMOVE(&rkq->rkq_q, rko, rko_link);
		(void)rd_atomic_sub(&rkq->rkq_qlen, 1);
	}

	pthread_mutex_unlock(&rkq->rkq_lock);

	return rko;
}




/**
 * Pop all available ops from a queue and call the provided 
 * callback for each op.
 *
 * Returns the number of ops served.
 *
 * Locality: any thread.
 */
int rd_kafka_q_serve (rd_kafka_t *rk,
		      rd_kafka_q_t *rkq, int timeout_ms,
		      void (*callback) (rd_kafka_op_t *rko,
					void *opaque),
		      void *opaque) {
	rd_kafka_op_t *rko, *tmp;
	rd_kafka_q_t localq;

	TAILQ_INIT(&localq.rkq_q);

	pthread_mutex_lock(&rkq->rkq_lock);
	
	/* Wait for op */
	while (!(rko = TAILQ_FIRST(&rkq->rkq_q)) && timeout_ms != 0) {
		
		if (timeout_ms != RD_POLL_INFINITE) {
			if (pthread_cond_timedwait_ms(&rkq->rkq_cond,
						      &rkq->rkq_lock,
						      timeout_ms) ==
			    ETIMEDOUT)
				break;

			timeout_ms = 0;

		} else
			pthread_cond_wait(&rkq->rkq_cond,
					  &rkq->rkq_lock);
	}

	if (!rko) {
		pthread_mutex_unlock(&rkq->rkq_lock);
		return 0;
	}

	/* Move all ops to local queue */
	TAILQ_CONCAT(&localq.rkq_q, &rkq->rkq_q, rko_link);
	localq.rkq_qlen = rkq->rkq_qlen;

	/* Reset real queue */
	TAILQ_INIT(&rkq->rkq_q);
	(void)rd_atomic_set(&rkq->rkq_qlen, 0);
	pthread_mutex_unlock(&rkq->rkq_lock);

	rd_kafka_dbg(rk, QUEUE, "QSERVE", "Serving %i ops", localq.rkq_qlen);

	/* Call callback for each op */
	TAILQ_FOREACH_SAFE(rko, &localq.rkq_q, rko_link, tmp) {
		callback(rko, opaque);
		rd_kafka_op_destroy(rko);
	}

	return localq.rkq_qlen;
}


/**
 * Send an op back to the application.
 *
 * Locality: Kafka threads
 */
void rd_kafka_op_reply0 (rd_kafka_t *rk, rd_kafka_op_t *rko,
			 rd_kafka_op_type_t type,
			 rd_kafka_resp_err_t err,
			 void *payload, int len) {

	rko->rko_type        = type;
	rko->rko_flags      |= RD_KAFKA_OP_F_FREE;
	rko->rko_payload     = payload;
	rko->rko_len         = len;
	rko->rko_err         = err;
}


/**
 * Send an op back to the application.
 *
 * Locality: Kafka thread
 */
void rd_kafka_op_reply (rd_kafka_t *rk,
			rd_kafka_op_type_t type,
			rd_kafka_resp_err_t err,
			void *payload, int len) {
	rd_kafka_op_t *rko;

	rko = calloc(1, sizeof(*rko));

	if (err && !payload) {
		/* Provide human readable error string if not provided. */

		/* Provide more info for some errors. */
		if (err == RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE) {
			char tmp[512];
			snprintf(tmp, sizeof(tmp), "%s (%"PRIu64")",
				 rd_kafka_err2str(err),
				 rk->rk_consumer.offset);
			payload = strdup(tmp);
		} else
			payload = strdup(rd_kafka_err2str(err));

		len = strlen(payload);
	}

	rd_kafka_op_reply0(rk, rko, type, err, payload, len);
	rd_kafka_q_enq(&rk->rk_rep, rko);
}


void rd_kafka_op_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	rd_kafka_q_enq(&rk->rk_rep, rko);
}


/**
 * Propogate an error event to the application.
 * If no error_cb has been set by the application the error will
 * be logged instead.
 */
void rd_kafka_op_err (rd_kafka_t *rk, rd_kafka_resp_err_t err,
		      const char *fmt, ...) {
	va_list ap;
	char buf[2048];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (rk->rk_conf.error_cb)
		rd_kafka_op_reply(rk, RD_KAFKA_OP_ERR, err,
				  strdup(buf), strlen(buf));
	else
		rd_kafka_log_buf(rk, LOG_ERR, "ERROR", buf);
}


static const char *rd_kafka_type2str (rd_kafka_type_t type) {
	static const char *types[] = {
		[RD_KAFKA_PRODUCER] = "producer",
		[RD_KAFKA_CONSUMER] = "consumer",
	};
	return types[type];
}

const char *rd_kafka_err2str (rd_kafka_resp_err_t err) {
	static __thread char ret[32];
	switch (err)
	{
	case RD_KAFKA_RESP_ERR__BAD_MSG:
		return "Local: Bad message format";
	case RD_KAFKA_RESP_ERR__BAD_COMPRESSION:
		return "Local: Invalid compressed data";
	case RD_KAFKA_RESP_ERR__DESTROY:
		return "Local: Broker handle destroyed";
	case RD_KAFKA_RESP_ERR__FAIL:
		return "Local: Communication failure with broker";
	case RD_KAFKA_RESP_ERR__TRANSPORT:
		return "Local: Broker transport failure";
	case RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE:
		return "Local: Critical system resource failure";
	case RD_KAFKA_RESP_ERR__RESOLVE:
		return "Local: Host resolution failure";
	case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
		return "Local: Message timed out";
	case RD_KAFKA_RESP_ERR__PARTITION_EOF:
		return "Broker: No more messages";
	case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
		return "Local: Unknown partition";
	case RD_KAFKA_RESP_ERR__FS:
		return "Local: File or filesystem error";
	case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
		return "Local: Unknown topic";
	case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
		return "Local: All broker connections are down";
	case RD_KAFKA_RESP_ERR__INVALID_ARG:
		return "Local: Invalid argument or configuration";
	case RD_KAFKA_RESP_ERR__TIMED_OUT:
		return "Local: Timed out";
	case RD_KAFKA_RESP_ERR__QUEUE_FULL:
		return "Local: Queue full";

	case RD_KAFKA_RESP_ERR_UNKNOWN:
		return "Unknown error";
	case RD_KAFKA_RESP_ERR_NO_ERROR:
		return "Success";
	case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
		return "Broker: Offset out of range";
	case RD_KAFKA_RESP_ERR_INVALID_MSG:
		return "Broker: Invalid message";
	case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
		return "Broker: Unknown topic or partition";
	case RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE:
		return "Broker: Invalid message size";
	case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
		return "Broker: Leader not available";
	case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
		return "Broker: Not leader for partition";
	case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
		return "Broker: Request timed out";
	case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
		return "Broker: Broker not available";
	case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
		return "Broker: Replica not available";
	case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
		return "Broker: Message size too large";
	case RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH:
		return "Broker: StaleControllerEpochCode";
	case RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE:
		return "Broker: Offset metadata string too large";
	default:
		snprintf(ret, sizeof(ret), "Err-%i?", err);
		return ret;
	}
}


rd_kafka_resp_err_t rd_kafka_errno2err (int errnox) {
	switch (errnox)
	{
	case EINVAL:
		return RD_KAFKA_RESP_ERR__INVALID_ARG;

	case ENOENT:
		return RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;

	case ESRCH:
		return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

	case ETIMEDOUT:
		return RD_KAFKA_RESP_ERR__TIMED_OUT;

	case EMSGSIZE:
		return RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE;

	case ENOBUFS:
		return RD_KAFKA_RESP_ERR__QUEUE_FULL;

	default:
		return RD_KAFKA_RESP_ERR__FAIL;
	}
}


void rd_kafka_destroy0 (rd_kafka_t *rk) {
	if (rd_atomic_sub(&rk->rk_refcnt, 1) > 0)
		return;

	/* Purge op-queue */
	rd_kafka_q_purge(&rk->rk_rep);

	rd_kafkap_str_destroy(rk->rk_clientid);
	rd_kafka_anyconf_destroy(_RK_GLOBAL, &rk->rk_conf);

	pthread_mutex_destroy(&rk->rk_lock);

	free(rk);

        rd_atomic_sub(&rd_kafka_handle_cnt_curr, 1);
}


/* NOTE: Must only be called by application.
 *       librdkafka itself must use rd_kafka_destroy0(). */
void rd_kafka_destroy (rd_kafka_t *rk) {
	rd_kafka_topic_t *rkt, *rkt_tmp;

	rd_kafka_dbg(rk, GENERIC, "DESTROY", "Terminating instance");
	(void)rd_atomic_add(&rk->rk_terminate, 1);

	/* Decommission all topics */
	rd_kafka_lock(rk);
	TAILQ_FOREACH_SAFE(rkt, &rk->rk_topics, rkt_link, rkt_tmp) {
		rd_kafka_unlock(rk);
		rd_kafka_topic_partitions_remove(rkt);
		rd_kafka_lock(rk);
	}
	rd_kafka_unlock(rk);

	/* Brokers pick up on rk_terminate automatically. */

	rd_kafka_destroy0(rk);
}



/* Stats buffer printf */
#define _st_printf(fmt...) do {					\
		ssize_t r;					\
		ssize_t rem = size-of;				\
		r = snprintf(buf+of, rem, fmt);			\
		if (r > rem) {					\
			size *= 2;				\
			buf = realloc(buf, size);		\
			r = snprintf(buf+of, size-of, fmt);	\
		}						\
		of += r;					\
	} while (0)

/**
 * Emit stats for toppar
 */
static inline void rd_kafka_stats_emit_toppar (char **bufp, size_t *sizep,
					       int *ofp,
					       rd_kafka_toppar_t *rktp,
					       int first) {
	char *buf = *bufp;
	size_t size = *sizep;
	int of = *ofp;

	_st_printf("%s\"%"PRId32"\": { "
		   "\"partition\":%"PRId32", "
		   "\"leader\":%"PRId32", "
		   "\"desired\":%s, "
		   "\"unknown\":%s, "
		   "\"msgq_cnt\":%i, "
		   "\"msgq_bytes\":%"PRIu64", "
		   "\"xmit_msgq_cnt\":%i, "
		   "\"xmit_msgq_bytes\":%"PRIu64", "
		   "\"fetchq_cnt\":%i, "
		   "\"fetch_state\":\"%s\", "
		   "\"query_offset\":%"PRId64", "
		   "\"next_offset\":%"PRId64", "
		   "\"app_offset\":%"PRId64", "
		   "\"commited_offset\":%"PRId64", "
		   "\"eof_offset\":%"PRId64", "
		   "\"txmsgs\":%"PRIu64", "
		   "\"txbytes\":%"PRIu64" "
		   "} ",
		   first ? "" : ", ",
		   rktp->rktp_partition,
		   rktp->rktp_partition,
		   rktp->rktp_leader ? rktp->rktp_leader->rkb_nodeid : -1,
		   (rktp->rktp_flags&RD_KAFKA_TOPPAR_F_DESIRED)?"true":"false",
		   (rktp->rktp_flags&RD_KAFKA_TOPPAR_F_UNKNOWN)?"true":"false",
		   rktp->rktp_msgq.rkmq_msg_cnt,
		   rktp->rktp_msgq.rkmq_msg_bytes,
		   rktp->rktp_xmit_msgq.rkmq_msg_cnt,
		   rktp->rktp_xmit_msgq.rkmq_msg_bytes,
		   rktp->rktp_fetchq.rkq_qlen,
		   rd_kafka_fetch_states[rktp->rktp_fetch_state],
		   rktp->rktp_query_offset,
		   rktp->rktp_next_offset,
		   rktp->rktp_app_offset,
		   rktp->rktp_commited_offset,
		   rktp->rktp_eof_offset,
		   rktp->rktp_c.tx_msgs,
		   rktp->rktp_c.tx_bytes);

	*bufp = buf;
	*sizep = size;
	*ofp = of;
}

/**
 * Emit all statistics
 */
static void rd_kafka_stats_emit_all (rd_kafka_t *rk) {
	char  *buf;
	size_t size = 1024*rk->rk_refcnt;
	int    of = 0;
	rd_kafka_broker_t *rkb;
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t *rktp;
	rd_ts_t now;

	buf = malloc(size);


	rd_kafka_lock(rk);

	now = rd_clock();
	_st_printf("{ "
		   "\"ts\":%"PRIu64", "
		   "\"time\":%lli, "
		   "\"replyq\":%i, "
		   "\"brokers\":{ "/*open brokers*/,
		   now,
		   (signed long long)time(NULL),
		   rk->rk_rep.rkq_qlen);


	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		rd_kafka_avg_rollover(&rkb->rkb_rtt_last, &rkb->rkb_rtt_curr);
		_st_printf("%s\"%s\": { "/*open broker*/
			   "\"name\":\"%s\", "
			   "\"nodeid\":%"PRId32", "
			   "\"state\":\"%s\", "
			   "\"outbuf_cnt\":%i, "
			   "\"waitresp_cnt\":%i, "
			   "\"tx\":%"PRIu64", "
			   "\"txbytes\":%"PRIu64", "
			   "\"txerrs\":%"PRIu64", "
			   "\"txretries\":%"PRIu64", "
			   "\"rx\":%"PRIu64", "
			   "\"rxbytes\":%"PRIu64", "
			   "\"rxerrs\":%"PRIu64", "
                           "\"rxcorriderrs\":%"PRIu64", "
			   "\"rtt\": {"
			   " \"min\":%"PRIu64","
			   " \"max\":%"PRIu64","
			   " \"avg\":%"PRIu64","
			   " \"cnt\":%i "
			   "}, "
			   "\"toppars\":{ "/*open toppars*/,
			   rkb == TAILQ_FIRST(&rk->rk_brokers) ? "" : ", ",
			   rkb->rkb_name,
			   rkb->rkb_name,
			   rkb->rkb_nodeid,
			   rd_kafka_broker_state_names[rkb->rkb_state],
			   rkb->rkb_outbufs.rkbq_cnt,
			   rkb->rkb_waitresps.rkbq_cnt,
			   rkb->rkb_c.tx,
			   rkb->rkb_c.tx_bytes,
			   rkb->rkb_c.tx_err,
			   rkb->rkb_c.tx_retries,
			   rkb->rkb_c.rx,
			   rkb->rkb_c.rx_bytes,
			   rkb->rkb_c.rx_err,
                           rkb->rkb_c.rx_corrid_err,
			   rkb->rkb_rtt_last.ra_min,
			   rkb->rkb_rtt_last.ra_max,
			   rkb->rkb_rtt_last.ra_avg,
			   rkb->rkb_rtt_last.ra_cnt);

		rd_kafka_broker_toppars_rdlock(rkb);
		TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
			_st_printf("%s\"%.*s\": { "
				   "\"topic\":\"%.*s\", "
				   "\"partition\":%"PRId32"} ",
				   rktp==TAILQ_FIRST(&rkb->rkb_toppars)?"":", ",
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				   rktp->rktp_partition);
		}
		rd_kafka_broker_toppars_unlock(rkb);

		rd_kafka_broker_unlock(rkb);

		_st_printf("} "/*close toppars*/
			   "} "/*close broker*/);
	}


	_st_printf("}, " /* close "brokers" array */
		   "\"topics\":{ ");

	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		int i;

		rd_kafka_topic_rdlock(rkt);
		_st_printf("%s\"%.*s\": { "
			   "\"topic\":\"%.*s\", "
			   "\"partitions\":{ " /*open partitions*/,
			   rkt==TAILQ_FIRST(&rk->rk_topics)?"":", ",
			   RD_KAFKAP_STR_PR(rkt->rkt_topic),
			   RD_KAFKAP_STR_PR(rkt->rkt_topic));

		for (i = 0 ; i < rkt->rkt_partition_cnt ; i++)
			rd_kafka_stats_emit_toppar(&buf, &size, &of,
						   rkt->rkt_p[i],
						   i == 0);

		TAILQ_FOREACH(rktp, &rkt->rkt_desp, rktp_rktlink)
			rd_kafka_stats_emit_toppar(&buf, &size, &of, rktp,
						   i++ == 0);

		if (rkt->rkt_ua)
			rd_kafka_stats_emit_toppar(&buf, &size, &of,
						   rkt->rkt_ua, i++ == 0);
		rd_kafka_topic_unlock(rkt);

		_st_printf("} "/*close partitions*/
			   "} "/*close topic*/);

	}

	rd_kafka_unlock(rk);

	_st_printf("} "/*close topics*/
		   "}"/*close object*/);


	/* Enqueue op for application */
	rd_kafka_op_reply(rk, RD_KAFKA_OP_STATS, 0, buf, of);
}



static void rd_kafka_topic_scan_tmr_cb (rd_kafka_t *rk, void *arg) {
	rd_kafka_topic_scan_all(rk, rd_clock());
}

static void rd_kafka_stats_emit_tmr_cb (rd_kafka_t *rk, void *arg) {
	rd_kafka_stats_emit_all(rk);
}

/**
 * Main loop for Kafka handler thread.
 */
static void *rd_kafka_thread_main (void *arg) {
	rd_kafka_t *rk = arg;
	rd_kafka_timer_t tmr_topic_scan = {};
	rd_kafka_timer_t tmr_stats_emit = {};

	(void)rd_atomic_add(&rd_kafka_thread_cnt_curr, 1);

	rd_kafka_timer_start(rk, &tmr_topic_scan, 1000000,
			     rd_kafka_topic_scan_tmr_cb, NULL);
	rd_kafka_timer_start(rk, &tmr_stats_emit,
			     rk->rk_conf.stats_interval_ms * 1000,
			     rd_kafka_stats_emit_tmr_cb, NULL);

	while (likely(rk->rk_terminate == 0)) {
		rd_kafka_timers_run(rk, 1000000);
	}

	rd_kafka_destroy0(rk); /* destroy handler thread's refcnt */

	(void)rd_atomic_sub(&rd_kafka_thread_cnt_curr, 1);

	return NULL;
}


static void rd_kafka_global_init (void) {
}

rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size) {
	rd_kafka_t *rk;
	static int rkid = 0;
	pthread_attr_t attr;
	int err;

	pthread_once(&rd_kafka_global_init_once, rd_kafka_global_init);

	/*
	 * Set up the handle.
	 */
	rk = calloc(1, sizeof(*rk));

	rk->rk_type = type;

	if (!conf)
		conf = rd_kafka_conf_new();
	rk->rk_conf = *conf;
	free(conf);

	rd_kafka_keep(rk); /* application refcnt */

	pthread_mutex_init(&rk->rk_lock, NULL);

	rd_kafka_q_init(&rk->rk_rep);

	TAILQ_INIT(&rk->rk_brokers);
	TAILQ_INIT(&rk->rk_topics);
	TAILQ_INIT(&rk->rk_timers);
	pthread_mutex_init(&rk->rk_timers_lock, NULL);
	pthread_cond_init(&rk->rk_timers_cond, NULL);

	rk->rk_log_cb = rd_kafka_log_print;

	if (rk->rk_conf.debug)
		rd_kafka_set_log_level(rk, LOG_DEBUG);
	else
		rk->rk_log_level = LOG_INFO;

	/* Construct a client id if none is given. */
	if (!rk->rk_conf.clientid)
		rk->rk_conf.clientid = strdup("rdkafka");

	snprintf(rk->rk_name, sizeof(rk->rk_name), "%s#%s-%i",
		 rk->rk_conf.clientid, rd_kafka_type2str(rk->rk_type), rkid++);

	/* Construct clientid kafka string */
	rk->rk_clientid = rd_kafkap_str_new(rk->rk_conf.clientid);

	if (rk->rk_type == RD_KAFKA_CONSUMER) {
		/* Pre-build RequestHeader */
		rk->rk_conf.FetchRequest.ReplicaId = htonl(-1);
		rk->rk_conf.FetchRequest.MaxWaitTime =
			htonl(rk->rk_conf.fetch_wait_max_ms);
		rk->rk_conf.FetchRequest.MinBytes =
			htonl(rk->rk_conf.fetch_min_bytes);
	}


	/* Create handler thread */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	rd_kafka_keep(rk); /* one refcnt for handler thread */
	if ((err = pthread_create(&rk->rk_thread, &attr,
				  rd_kafka_thread_main, rk))) {
		if (errstr)
			snprintf(errstr, errstr_size,
				 "Failed to create thread: %s", strerror(err));
		rd_kafka_destroy0(rk); /* handler thread */
		rd_kafka_destroy0(rk); /* application refcnt */
		errno = err;
		return NULL;
	}


	/* Add initial list of brokers from configuration */
	if (rk->rk_conf.brokerlist)
		rd_kafka_brokers_add(rk, rk->rk_conf.brokerlist);

        rd_atomic_add(&rd_kafka_handle_cnt_curr, 1);

	return rk;
}





/**
 * Produce a single message.
 * Locality: any application thread
 */
int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partition,
		      int msgflags,
		      void *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque) {

	return rd_kafka_msg_new(rkt, partition,
				msgflags, payload, len,
				key, keylen, msg_opaque);
}



int rd_kafka_consume_start (rd_kafka_topic_t *rkt, int32_t partition,
			    int64_t offset) {
	rd_kafka_toppar_t *rktp;

	if (partition < 0) {
		errno = ESRCH;
		return -1;
	}

	rd_kafka_topic_wrlock(rkt);
	rktp = rd_kafka_toppar_desired_add(rkt, partition);
	rd_kafka_topic_unlock(rkt);

	rd_kafka_toppar_lock(rktp);
	switch (offset)
	{
	case RD_KAFKA_OFFSET_BEGINNING:
	case RD_KAFKA_OFFSET_END:
		rktp->rktp_query_offset = offset;
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY;
		break;
	case RD_KAFKA_OFFSET_STORED:
		if (!rkt->rkt_conf.auto_commit) {
			rd_kafka_toppar_unlock(rktp);
			rd_kafka_toppar_destroy(rktp);
			errno = EINVAL;
			return -1;
		}
		rd_kafka_offset_store_init(rktp);
		break;
	default:
		rktp->rktp_next_offset = offset;
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_ACTIVE;
	}

	rd_kafka_toppar_unlock(rktp);

	rd_kafka_dbg(rkt->rkt_rk, TOPIC, "CONSUMER",
		     "Start consuming %.*s [%"PRId32"] at "
		     "offset %"PRId64,
		     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		     rktp->rktp_partition, offset);

	return 0;
}


int rd_kafka_consume_stop (rd_kafka_topic_t *rkt, int32_t partition) {
	rd_kafka_toppar_t *rktp;

	if (partition == RD_KAFKA_PARTITION_UA) {
		errno = EINVAL;
		return -1;
	}

	rd_kafka_topic_wrlock(rkt);
	if (!(rktp = rd_kafka_toppar_get(rkt, partition, 0)) &&
	    !(rktp = rd_kafka_toppar_desired_get(rkt, partition))) {
		rd_kafka_topic_unlock(rkt);
		errno = ESRCH;
		return -1;
	}

	rd_kafka_toppar_desired_del(rktp);
	rd_kafka_topic_unlock(rkt);

	rd_kafka_toppar_lock(rktp);
	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_NONE;

	if (rktp->rktp_offset_path)
		rd_kafka_offset_store_term(rktp);

	/* Purge receive queue. */
	rd_kafka_q_purge(&rktp->rktp_fetchq);

	rd_kafka_dbg(rkt->rkt_rk, TOPIC, "CONSUMER",
		     "Stop consuming %.*s [%"PRId32"] currently at offset "
		     "%"PRId64,
		     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		     rktp->rktp_partition,
		     rktp->rktp_next_offset);
	rd_kafka_toppar_unlock(rktp);

	rd_kafka_toppar_destroy(rktp); /* .._get() */

	return 0;
}


void rd_kafka_message_destroy (rd_kafka_message_t *rkmessage) {
	rd_kafka_op_t *rko;

	if (likely((rko = (rd_kafka_op_t *)rkmessage->_private) != NULL))
		rd_kafka_op_destroy(rko);
	else
		free(rkmessage);
}


static rd_kafka_message_t *rd_kafka_message_get (rd_kafka_op_t *rko) {
	rd_kafka_message_t *rkmessage;

	if (rko) {
		rkmessage = &rko->rko_rkmessage;
		rkmessage->_private = rko;
	} else
		rkmessage = calloc(1, sizeof(*rkmessage));

	return rkmessage;
}





ssize_t rd_kafka_consume_batch (rd_kafka_topic_t *rkt, int32_t partition,
				int timeout_ms,
				rd_kafka_message_t **rkmessages,
				size_t rkmessages_size) {
	rd_kafka_toppar_t *rktp;
	struct timeval tv;
	struct timespec ts;
	ssize_t cnt = 0;

	gettimeofday(&tv, NULL);
	TIMEVAL_TO_TIMESPEC(&tv, &ts);
	ts.tv_sec  += timeout_ms / 1000;
	ts.tv_nsec += (timeout_ms % 1000) * 1000000;
	if (ts.tv_nsec > 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	/* Get toppar */
	rd_kafka_topic_rdlock(rkt);
	rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!rktp))
		rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_unlock(rkt);

	if (unlikely(!rktp)) {
		/* No such toppar known */
		errno = ESRCH;
		return -1;
	}

	/* Populate application's rkmessages array. */
	while (cnt < rkmessages_size) {
		rd_kafka_op_t *rko;

		pthread_mutex_lock(&rktp->rktp_fetchq.rkq_lock);

		while (!(rko = TAILQ_FIRST(&rktp->rktp_fetchq.rkq_q))) {
			if (pthread_cond_timedwait(&rktp->rktp_fetchq.rkq_cond,
						   &rktp->rktp_fetchq.rkq_lock,
						   &ts) == ETIMEDOUT)
				break;
		}

		if (!rko) {
			/* Timed out */
			pthread_mutex_unlock(&rktp->rktp_fetchq.rkq_lock);
			break;
		}

		TAILQ_REMOVE(&rktp->rktp_fetchq.rkq_q, rko, rko_link);
		(void)rd_atomic_sub(&rktp->rktp_fetchq.rkq_qlen, 1);

		pthread_mutex_unlock(&rktp->rktp_fetchq.rkq_lock);

		/* Get rkmessage from rko and append to array. */
		rkmessages[cnt++] = rd_kafka_message_get(rko);
	}

	/* Auto store offset of last message in batch, if enabled */
	if (cnt > 0 && rkt->rkt_conf.auto_commit)
		rd_kafka_offset_store0(rktp, rkmessages[cnt-1]->offset,
				       1/*lock*/);

	rd_kafka_toppar_destroy(rktp); /* refcnt from .._get() */

	return cnt;
}


struct consume_ctx {
	void (*consume_cb) (rd_kafka_message_t *rkmessage, void *opaque);
	void *opaque;
	rd_kafka_toppar_t *rktp;
};


/**
 * Trampoline for application's consume_cb()
 */
static void rd_kafka_consume_cb (rd_kafka_op_t *rko, void *opaque) {
	struct consume_ctx *ctx = opaque;
	rd_kafka_message_t *rkmessage;

	rkmessage = rd_kafka_message_get(rko);
	if (ctx->rktp->rktp_rkt->rkt_conf.auto_commit)
		rd_kafka_offset_store0(ctx->rktp, rkmessage->offset, 1/*lock*/);
	ctx->consume_cb(rkmessage, ctx->opaque);
}



int rd_kafka_consume_callback (rd_kafka_topic_t *rkt, int32_t partition,
			       int timeout_ms,
			       void (*consume_cb) (rd_kafka_message_t
						   *rkmessage,
						   void *opaque),
			       void *opaque) {
	rd_kafka_toppar_t *rktp;
	struct consume_ctx ctx = { .consume_cb = consume_cb, .opaque = opaque };
	int r;

	/* Get toppar */
	rd_kafka_topic_rdlock(rkt);
	rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!rktp))
		rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_unlock(rkt);

	if (unlikely(!rktp)) {
		/* No such toppar known */
		errno = ESRCH;
		return -1;
	}

	ctx.rktp = rktp;

	r = rd_kafka_q_serve(rkt->rkt_rk, &rktp->rktp_fetchq, timeout_ms,
			     rd_kafka_consume_cb, &ctx);

	rd_kafka_toppar_destroy(rktp);

	return r;
}



rd_kafka_message_t *rd_kafka_consume (rd_kafka_topic_t *rkt, int32_t partition,
				      int timeout_ms) {
	rd_kafka_op_t *rko;
	rd_kafka_toppar_t *rktp;
	rd_kafka_message_t *rkmessage;

	rd_kafka_topic_rdlock(rkt);
	rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!rktp))
		rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_unlock(rkt);

	if (unlikely(!rktp)) {
		/* No such toppar known */
		errno = ESRCH;
		return NULL;
	}

	/* Pop op from queue. May either be an error or a message. */
	rko = rd_kafka_q_pop(&rktp->rktp_fetchq, timeout_ms);
	if (!rko) {
		/* Timeout reached with no op returned. */
		rd_kafka_toppar_destroy(rktp); /* refcnt from .._get() */
		errno = ETIMEDOUT;
		return NULL;
	}

	/* Get rkmessage from rko */
	rkmessage = rd_kafka_message_get(rko);

	/* Store offset */
	if (rktp->rktp_rkt->rkt_conf.auto_commit)
		rd_kafka_offset_store0(rktp, rkmessage->offset, 1/*lock*/);

	rd_kafka_toppar_destroy(rktp); /* refcnt from .._get() */

	return rkmessage;
}







static void rd_kafka_poll_cb (rd_kafka_op_t *rko, void *opaque) {
	rd_kafka_t *rk = opaque;
	rd_kafka_msg_t *rkm;
	static int dcnt = 0;

	switch (rko->rko_type)
	{
	case RD_KAFKA_OP_FETCH:
		/* FIXME */
		break;

	case RD_KAFKA_OP_ERR:
		if (rk->rk_conf.error_cb)
			rk->rk_conf.error_cb(rk, rko->rko_err,
					     strndupa(rko->rko_payload,
						      rko->rko_len),
					     rk->rk_conf.opaque);
		else
			rd_kafka_log(rk, LOG_ERR, "ERROR",
				     "%s: %s: %.*s",
				     rk->rk_name,
				     rd_kafka_err2str(rko->rko_err),
				     (int)rko->rko_len,
				     (char *)rko->rko_payload);
		break;

	case RD_KAFKA_OP_DR:
		/* Delivery report:
		 * call application DR callback for each message. */
		while ((rkm = TAILQ_FIRST(&rko->rko_msgq.rkmq_msgs))) {
			TAILQ_REMOVE(&rko->rko_msgq.rkmq_msgs, rkm, rkm_link);

			dcnt++;

			rk->rk_conf.dr_cb(rk,
					  rkm->rkm_payload,
					  rkm->rkm_len,
					  rko->rko_err,
					  rk->rk_conf.opaque,
					  rkm->rkm_opaque);

			rd_kafka_msg_destroy(rk, rkm);
		}

		rd_kafka_msgq_init(&rko->rko_msgq);

		if (!(dcnt % 1000))
			rd_kafka_dbg(rk, MSG, "POLL",
				     "Now %i messages delivered to app", dcnt);
		break;

	case RD_KAFKA_OP_STATS:
		/* Statistics */
		if (rk->rk_conf.stats_cb &&
		    rk->rk_conf.stats_cb(rk, rko->rko_json, rko->rko_json_len,
					 rk->rk_conf.opaque) == 1)
			rko->rko_json = NULL; /* Application wanted json ptr */
		break;

	default:
		rd_kafka_dbg(rk, ALL, "POLLCB",
			     "cant handle op %i here", rko->rko_type);
		assert(!*"cant handle op type");
		break;
	}
}

int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms) {
	return rd_kafka_q_serve(rk, &rk->rk_rep, timeout_ms,
				rd_kafka_poll_cb, rk);
}



static void rd_kafka_toppar_dump (FILE *fp, const char *indent,
				  rd_kafka_toppar_t *rktp) {
	
	fprintf(fp, "%s%.*s [%"PRId32"] leader %s\n",
		indent,
		RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		rktp->rktp_partition,
		rktp->rktp_leader ?
		rktp->rktp_leader->rkb_name : "none");
	fprintf(fp,
		"%s refcnt %i\n"
		"%s msgq:      %i messages\n"
		"%s xmit_msgq: %i messages\n"
		"%s total:     %"PRIu64" messages, %"PRIu64" bytes\n",
		indent, rktp->rktp_refcnt,
		indent, rktp->rktp_msgq.rkmq_msg_cnt,
		indent, rktp->rktp_xmit_msgq.rkmq_msg_cnt,
		indent, rktp->rktp_c.tx_msgs, rktp->rktp_c.tx_bytes);
}

void rd_kafka_dump (FILE *fp, rd_kafka_t *rk) {
	rd_kafka_broker_t *rkb;
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t *rktp;

	rd_kafka_lock(rk);
	fprintf(fp, "rd_kafka_t %p: %s\n", rk, rk->rk_name);

	fprintf(fp, " refcnt %i\n", rk->rk_refcnt);
	fprintf(fp, " rk_rep reply queue: %i ops\n", rk->rk_rep.rkq_qlen);

	fprintf(fp, " brokers:\n");
	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		fprintf(fp, " rd_kafka_broker_t %p: %s NodeId %"PRId32
			" in state %s\n",
			rkb, rkb->rkb_name, rkb->rkb_nodeid,
			rd_kafka_broker_state_names[rkb->rkb_state]);
		fprintf(fp, "  refcnt %i\n", rkb->rkb_refcnt);
		fprintf(fp, "  outbuf_cnt: %i waitresp_cnt: %i\n",
			rkb->rkb_outbufs.rkbq_cnt, rkb->rkb_waitresps.rkbq_cnt);
		fprintf(fp, 
			"  %"PRIu64 " messages sent, %"PRIu64" bytes, "
			"%"PRIu64" errors\n"
			"  %"PRIu64 " messages received, %"PRIu64" bytes, "
			"%"PRIu64" errors\n"
			"  %"PRIu64 " messageset transmissions were retried\n",
			rkb->rkb_c.tx, rkb->rkb_c.tx_bytes,
			rkb->rkb_c.tx_err,
			rkb->rkb_c.rx, rkb->rkb_c.rx_bytes,
			rkb->rkb_c.rx_err,
			rkb->rkb_c.tx_retries);

		fprintf(fp, "  %i toppars:\n", rkb->rkb_toppar_cnt);
		rd_kafka_broker_toppars_rdlock(rkb);
		TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink)
			rd_kafka_toppar_dump(fp, "   ", rktp);
		rd_kafka_broker_toppars_unlock(rkb);
		rd_kafka_broker_unlock(rkb);
	}

	fprintf(fp, " topics:\n");
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		fprintf(fp, "  %.*s with %"PRId32" partitions, refcnt %i\n",
			RD_KAFKAP_STR_PR(rkt->rkt_topic),
			rkt->rkt_partition_cnt, rkt->rkt_refcnt);
		if (rkt->rkt_ua)
			rd_kafka_toppar_dump(fp, "   ", rkt->rkt_ua);
                if (!TAILQ_EMPTY(&rkt->rkt_desp)) {
                        fprintf(fp, "   desired partitions:");
                        TAILQ_FOREACH(rktp, &rkt->rkt_desp, rktp_rktlink)
                                fprintf(fp, " %"PRId32, rktp->rktp_partition);
                        fprintf(fp, "\n");
                }
	}
	rd_kafka_unlock(rk);
}



const char *rd_kafka_name (const rd_kafka_t *rk) {
	return rk->rk_name;
}

int rd_kafka_outq_len (rd_kafka_t *rk) {
	return rk->rk_producer.msg_cnt;
}


int rd_kafka_version (void) {
	return RD_KAFKA_VERSION;
}

const char *rd_kafka_version_str (void) {
	static char ret[64];
	int ver = rd_kafka_version();

	if (!*ret)
		snprintf(ret, sizeof(ret), "%i.%i.%i",
			 (ver >> 24) & 0xff,
			 (ver >> 16) & 0xff,
			 (ver >> 8) & 0xff);

	return ret;
}
