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
 * Wrapper for pthread_cond_timedwait() that makes it simpler to use
 * for delta timeouts.
 * `timeout_ms' is the delta timeout in milliseconds.
 */
static int pthread_cond_timedwait_ms (pthread_cond_t *cond,
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


void rd_kafka_op_destroy (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	
	if (rko->rko_payload && rko->rko_flags & RD_KAFKA_OP_F_FREE)
		free(rko->rko_payload);
	
	free(rko);
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
 * Pop an op from a queue.
 *
 * Locality: any thread.
 */
rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms) {
	rd_kafka_op_t *rko;

	pthread_mutex_lock(&rkq->rkq_lock);

	/* FIXME: concat queue to local queue without lock. */
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
	rkq->rkq_qlen = 0;

	pthread_mutex_unlock(&rkq->rkq_lock);

	rd_kafka_dbg(rk, QUEUE, "QSERVE", "Serving %i ops", localq.rkq_qlen);

	/* Call callback for each op */
	TAILQ_FOREACH_SAFE(rko, tmp, &localq.rkq_q, rko_link) {
		callback(rko, opaque);
		rd_kafka_op_destroy(rk, rko);
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
			 rd_kafka_resp_err_t err, uint8_t compression,
			 void *payload, int len,
			 uint64_t offset_len) {

	rko->rko_type        = type;
	rko->rko_flags      |= RD_KAFKA_OP_F_FREE;
	rko->rko_payload     = payload;
	rko->rko_len         = len;
	rko->rko_err         = err;
	rko->rko_compression = compression;
	rko->rko_offset      = offset_len;
}


/**
 * Send an op back to the application.
 *
 * Locality: Kafka thread
 */
void rd_kafka_op_reply (rd_kafka_t *rk,
			rd_kafka_op_type_t type,
			rd_kafka_resp_err_t err, uint8_t compression,
			void *payload, int len,
			uint64_t offset_len) {
	rd_kafka_op_t *rko;

	rko = calloc(1, sizeof(*rko));

	if (err && !payload) {
		/* Provide human readable error string if not provided. */

		/* Provide more info for some errors. */
		if (err == RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE) {
			char tmp[512];
			snprintf(tmp, sizeof(tmp), "%s (%"PRIu64")",
				 rd_kafka_err2str(rk, err),
				 rk->rk_consumer.offset);
			payload = strdup(tmp);
		} else
			payload = strdup(rd_kafka_err2str(rk, err));

		len = strlen(payload);
	}

	rd_kafka_op_reply0(rk, rko, type, err, compression,
			   payload, len, offset_len);
	rd_kafka_q_enq(&rk->rk_rep, rko);
}


void rd_kafka_op_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	rd_kafka_q_enq(&rk->rk_rep, rko);
}



static const char *rd_kafka_type2str (rd_kafka_type_t type) {
	static const char *types[] = {
		[RD_KAFKA_PRODUCER] = "producer",
		[RD_KAFKA_CONSUMER] = "consumer",
	};
	return types[type];
}

const char *rd_kafka_err2str (rd_kafka_t *rk, rd_kafka_resp_err_t err) {
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






void rd_kafka_destroy0 (rd_kafka_t *rk) {
	if (rd_atomic_sub(&rk->rk_refcnt, 1) > 0)
		return;

	rd_kafkap_str_destroy(rk->rk_clientid);
	rd_kafka_conf_destroy(&rk->rk_conf);

	pthread_mutex_destroy(&rk->rk_lock);

	free(rk);
}


/* NOTE: Must only be called by application.
 *       librdkafka itself must use rd_kafka_destroy0(). */
void rd_kafka_destroy (rd_kafka_t *rk) {
	rd_kafka_topic_t *rkt, *rkt_tmp;

	rd_kafka_dbg(rk, GENERIC, "DESTROY", "Terminating instance");
	rd_atomic_add(&rk->rk_terminate, 1);

	/* Decommission all topics */
	rd_kafka_lock(rk);
	TAILQ_FOREACH_SAFE(rkt, rkt_tmp, &rk->rk_topics, rkt_link) {
		rd_kafka_unlock(rk);
		rd_kafka_topic_partitions_remove(rkt);
		rd_kafka_lock(rk);
	}
	rd_kafka_unlock(rk);

	/* Brokers pick up on rk_terminate automatically. */

	rd_kafka_destroy0(rk);
}


static void rd_kafka_global_init (void) {
}

rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size) {
	rd_kafka_t *rk;
	static int rkid = 0;

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

	rd_kafka_keep(rk);

	pthread_mutex_init(&rk->rk_lock, NULL);

	rd_kafka_q_init(&rk->rk_op);
	rd_kafka_q_init(&rk->rk_rep);

	TAILQ_INIT(&rk->rk_brokers);
	TAILQ_INIT(&rk->rk_topics);

	rk->rk_log_cb = rd_kafka_log_print;

	if (rk->rk_conf.debug)
		rd_kafka_set_log_level(rk, LOG_DEBUG);
	else
		rk->rk_log_level = LOG_INFO;

	/* Construct a client id if none is given. */
	if (!rk->rk_conf.clientid)
		rk->rk_conf.clientid = strdup("default");
	
	snprintf(rk->rk_name, sizeof(rk->rk_name), "%s#%s-%i",
		 rk->rk_conf.clientid, rd_kafka_type2str(rk->rk_type), rkid++);

	/* Construct clientid kafka string */
	rk->rk_clientid = rd_kafkap_str_new(rk->rk_conf.clientid);

	/* Add initial list of brokers from configuration */
	if (rk->rk_conf.brokerlist)
		rd_kafka_brokers_add(rk, rk->rk_conf.brokerlist);

	return rk;
}





/**
 * Produce a single message.
 *
 * If 'partition' is unassigned (RD_KAFKA_PARTITION_UA) the configured or
 * default partitioner will be used to designate the target partition.
 *
 * See rdkafka.h for 'msgflags'.
 *
 * Returns: 0 on success or -1 on error (see errno for details)
 *
 * errnos:
 *    ENOBUFS - conf.producer.max_msg_cnt would be exceeded.
 *
 * Locality: application thread
 */
int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partition,
		      int msgflags,
		      char *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque) {

	return rd_kafka_msg_new(rkt, partition,
				msgflags, payload, len,
				key, keylen, msg_opaque);
}



static void rd_kafka_poll_cb (rd_kafka_op_t *rko, void *opaque) {
	rd_kafka_t *rk = opaque;
	rd_kafka_msg_t *rkm;
	static int dcnt = 0;

	switch (rko->rko_type)
	{
	case RD_KAFKA_OP_FETCH:
		rd_kafka_dbg(rk, QUEUE, "POLL", "fixme: fetch cb");
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
				     rd_kafka_err2str(rk, rko->rko_err),
				     rko->rko_len, rko->rko_payload);
		break;

	case RD_KAFKA_OP_DR:
		/* Delivery report:
		 * call application DR callback for each message. */
		while ((rkm = TAILQ_FIRST(&rko->rko_msgq.rkmq_msgs))) {
			TAILQ_REMOVE(&rko->rko_msgq.rkmq_msgs, rkm, rkm_link);

			dcnt++;

			rk->rk_conf.producer.dr_cb(rk,
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
	fprintf(fp, " rk_op request queue: %i ops\n", rk->rk_op.rkq_qlen);
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
	}
	rd_kafka_unlock(rk);
	
}



const char *rd_kafka_name (const rd_kafka_t *rk) {
	return rk->rk_name;
}

int rd_kafka_outq_len (rd_kafka_t *rk) {
	return rk->rk_producer.msg_cnt;
}
