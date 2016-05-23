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
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_broker.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_offset.h"
#include "rdkafka_transport.h"
#include "rdkafka_cgrp.h"
#include "rdkafka_assignor.h"
#include "rdkafka_request.h"

#if WITH_SASL
#include "rdkafka_sasl.h"
#endif

#include "rdtime.h"
#ifdef _MSC_VER
#include <sys/types.h>
#include <sys/timeb.h>
#endif

static once_flag rd_kafka_global_init_once = ONCE_FLAG_INIT;


/**
 * Last API error code, per thread.
 * Shared among all rd_kafka_t instances.
 */
rd_kafka_resp_err_t RD_TLS rd_kafka_last_error_code;


/**
 * Current number of threads created by rdkafka.
 * This is used in regression tests.
 */
rd_atomic32_t rd_kafka_thread_cnt_curr;
int rd_kafka_thread_cnt (void) {
#if ENABLE_SHAREDPTR_DEBUG
        rd_shared_ptrs_dump();
#endif

	return rd_atomic32_get(&rd_kafka_thread_cnt_curr);
}

/**
 * Current thread's name (TLS)
 */
char RD_TLS rd_kafka_thread_name[64] = "app";

/**
 * Current number of live rd_kafka_t handles.
 * This is used by rd_kafka_wait_destroyed() to know when the library
 * has fully cleaned up after itself.
 */
static rd_atomic32_t rd_kafka_handle_cnt_curr; /* atomic */

/**
 * Wait for all rd_kafka_t objects to be destroyed.
 * Returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 */
int rd_kafka_wait_destroyed (int timeout_ms) {
	rd_ts_t timeout = rd_clock() + (timeout_ms * 1000);

	while (rd_kafka_thread_cnt() > 0 ||
               rd_atomic32_get(&rd_kafka_handle_cnt_curr) > 0) {
		if (rd_clock() >= timeout) {
			rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__TIMED_OUT,
						ETIMEDOUT);
#if ENABLE_SHAREDPTR_DEBUG
                        rd_shared_ptrs_dump();
#endif
			return -1;
		}
		rd_usleep(25000, NULL); /* 25ms */
	}

	return 0;
}



void rd_kafka_log_buf (const rd_kafka_t *rk, int level,
		       const char *fac, const char *buf) {

	if (!rk->rk_conf.log_cb || level > rk->rk_conf.log_level)
		return;

	rk->rk_conf.log_cb(rk, level, fac, buf);
}

void rd_kafka_log0 (const rd_kafka_t *rk, const char *extra, int level,
		   const char *fac, const char *fmt, ...) {
	char buf[2048];
	va_list ap;
	unsigned int elen = 0;
        unsigned int of = 0;

	if (!rk->rk_conf.log_cb || level > rk->rk_conf.log_level)
		return;

	if (rk->rk_conf.log_thread_name) {
		elen = rd_snprintf(buf, sizeof(buf), "[thrd:%s]: ",
				   rd_kafka_thread_name);
		if (unlikely(elen >= sizeof(buf)))
			elen = sizeof(buf);
		of = elen;
	}

	if (extra) {
		elen = rd_snprintf(buf+of, sizeof(buf)-of, "%s: ", extra);
		if (unlikely(elen >= sizeof(buf)-of))
			elen = sizeof(buf)-of;
                of += elen;
	}

	va_start(ap, fmt);
	rd_vsnprintf(buf+of, sizeof(buf)-of, fmt, ap);
	va_end(ap);

	rk->rk_conf.log_cb(rk, level, fac, buf);
}



void rd_kafka_log_print(const rd_kafka_t *rk, int level,
	const char *fac, const char *buf) {
	int secs, msecs;
	struct timeval tv;
	rd_gettimeofday(&tv, NULL);
	secs = (int)tv.tv_sec;
	msecs = (int)(tv.tv_usec / 1000);
	fprintf(stderr, "%%%i|%u.%03u|%s|%s| %s\n",
		level, secs, msecs,
		fac, rk ? rk->rk_name : "", buf);
}

#ifndef _MSC_VER
void rd_kafka_log_syslog (const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf) {
	static int initialized = 0;

	if (!initialized)
		openlog("rdkafka", LOG_PID|LOG_CONS, LOG_USER);

	syslog(level, "%s: %s: %s", fac, rk ? rk->rk_name : "", buf);
}
#endif

void rd_kafka_set_logger (rd_kafka_t *rk,
			  void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf)) {
	rk->rk_conf.log_cb = func;
}

void rd_kafka_set_log_level (rd_kafka_t *rk, int level) {
	rk->rk_conf.log_level = level;
}






static const char *rd_kafka_type2str (rd_kafka_type_t type) {
	static const char *types[] = {
		[RD_KAFKA_PRODUCER] = "producer",
		[RD_KAFKA_CONSUMER] = "consumer",
	};
	return types[type];
}

#define _ERR_DESC(ENUM,DESC) \
	[ENUM - RD_KAFKA_RESP_ERR__BEGIN] = { ENUM, # ENUM + 18/*pfx*/, DESC }

static const struct rd_kafka_err_desc rd_kafka_err_descs[] = {
	_ERR_DESC(RD_KAFKA_RESP_ERR__BEGIN, NULL),
	_ERR_DESC(RD_KAFKA_RESP_ERR__BAD_MSG,
		  "Local: Bad message format"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
		  "Local: Invalid compressed data"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__DESTROY,
		  "Local: Broker handle destroyed"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__FAIL,
		  "Local: Communication failure with broker"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__TRANSPORT,
		  "Local: Broker transport failure"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
		  "Local: Critical system resource failure"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__RESOLVE,
		  "Local: Host resolution failure"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
		  "Local: Message timed out"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__PARTITION_EOF,
		  "Broker: No more messages"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
		  "Local: Unknown partition"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__FS,
		  "Local: File or filesystem error"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC,
		  "Local: Unknown topic"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
		  "Local: All broker connections are down"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__INVALID_ARG,
		  "Local: Invalid argument or configuration"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__TIMED_OUT,
		  "Local: Timed out"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__QUEUE_FULL,
		  "Local: Queue full"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__ISR_INSUFF,
		  "Local: ISR count insufficient"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__NODE_UPDATE,
		  "Local: Broker node update"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__SSL,
		  "Local: SSL error"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__WAIT_COORD,
		  "Local: Waiting for coordinator"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__UNKNOWN_GROUP,
		  "Local: Unknown group"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__IN_PROGRESS,
		  "Local: Operation in progress"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS,
		  "Local: Previous operation in progress"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION,
		  "Local: Existing subscription"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
		  "Local: Assign partitions"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
		  "Local: Revoke partitions"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__CONFLICT,
		  "Local: Conflicting use"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__STATE,
		  "Local: Erroneous state"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL,
		  "Local: Unknown protocol"),
        _ERR_DESC(RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
		  "Local: Not implemented"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__AUTHENTICATION,
		  "Local: Authentication failure"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__NO_OFFSET,
		  "Local: No offset stored"),
	_ERR_DESC(RD_KAFKA_RESP_ERR__OUTDATED,
		  "Local: Outdated"),

	_ERR_DESC(RD_KAFKA_RESP_ERR_UNKNOWN,
		  "Unknown broker error"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_NO_ERROR,
		  "Success"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE,
		  "Broker: Offset out of range"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_MSG,
		  "Broker: Invalid message"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
		  "Broker: Unknown topic or partition"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE,
		  "Broker: Invalid message size"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE,
		  "Broker: Leader not available"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION,
		  "Broker: Not leader for partition"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
		  "Broker: Request timed out"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE,
		  "Broker: Broker not available"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE,
		  "Broker: Replica not available"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
		  "Broker: Message size too large"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH,
		  "Broker: StaleControllerEpochCode"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE,
		  "Broker: Offset metadata string too large"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION,
		  "Broker: Broker disconnected before response received"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS,
		  "Broker: Group coordinator load in progress"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
		  "Broker: Group coordinator not available"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP,
		  "Broker: Not coordinator for group"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION,
		  "Broker: Invalid topic"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE,
		  "Broker: Message batch larger than configured server "
		  "segment size"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS,
		  "Broker: Not enough in-sync replicas"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
		  "Broker: Message(s) written to insufficient number of "
		  "in-sync replicas"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS,
		  "Broker: Invalid required acks value"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
		  "Broker: Specified group generation id is not valid"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL,
		  "Broker: Inconsistent group protocol"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_GROUP_ID,
		  "Broker: Invalid group.id"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
		  "Broker: Unknown member"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT,
		  "Broker: Invalid session timeout"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
		  "Broker: Group rebalance in progress"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE,
		  "Broker: Commit offset data size is not valid"),
        _ERR_DESC(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
		  "Broker: Topic authorization failed"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
		  "Broker: Group authorization failed"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
		  "Broker: Cluster authorization failed"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP,
		  "Broker: Invalid timestamp"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM,
		  "Broker: Unsupported SASL mechanism"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE,
		  "Broker: Request not valid in current SASL state"),
	_ERR_DESC(RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION,
		  "Broker: API version not supported"),

	_ERR_DESC(RD_KAFKA_RESP_ERR__END, NULL)
};


void rd_kafka_get_err_descs (const struct rd_kafka_err_desc **errdescs,
			     size_t *cntp) {
	*errdescs = rd_kafka_err_descs;
	*cntp = RD_ARRAYSIZE(rd_kafka_err_descs);
}


const char *rd_kafka_err2str (rd_kafka_resp_err_t err) {
	static RD_TLS char ret[32];
	int idx = err - RD_KAFKA_RESP_ERR__BEGIN;

	if (unlikely(err <= RD_KAFKA_RESP_ERR__BEGIN ||
		     err >= RD_KAFKA_RESP_ERR_END_ALL ||
		     !rd_kafka_err_descs[idx].desc)) {
		rd_snprintf(ret, sizeof(ret), "Err-%i?", err);
		return ret;
	}

	return rd_kafka_err_descs[idx].desc;
}


const char *rd_kafka_err2name (rd_kafka_resp_err_t err) {
	static RD_TLS char ret[32];
	int idx = err - RD_KAFKA_RESP_ERR__BEGIN;

	if (unlikely(err <= RD_KAFKA_RESP_ERR__BEGIN ||
		     err >= RD_KAFKA_RESP_ERR_END_ALL ||
		     !rd_kafka_err_descs[idx].desc)) {
		rd_snprintf(ret, sizeof(ret), "ERR_%i?", err);
		return ret;
	}

	return rd_kafka_err_descs[idx].name;
}


rd_kafka_resp_err_t rd_kafka_last_error (void) {
	return rd_kafka_last_error_code;
}


rd_kafka_resp_err_t rd_kafka_errno2err (int errnox) {
	switch (errnox)
	{
	case EINVAL:
		return RD_KAFKA_RESP_ERR__INVALID_ARG;

        case EBUSY:
                return RD_KAFKA_RESP_ERR__CONFLICT;

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



static void rd_kafka_simple_consumer_cleanup (rd_kafka_t *rk) {
        if (rk->rk_cgrp)
		rd_kafka_cgrp_terminate0(rk->rk_cgrp, NULL);
}


/**
 * Final destructor for rd_kafka_t, must only be called with refcnt 0.
 */
void rd_kafka_destroy_final (rd_kafka_t *rk) {

        rd_kafka_assert(rk, rd_atomic32_get(&rk->rk_terminate) != 0);

        /* Synchronize state */
        rd_kafka_wrlock(rk);
        rd_kafka_wrunlock(rk);

        rd_kafka_assignors_term(rk);

        rd_kafka_timers_destroy(&rk->rk_timers);

        /* Destroy cgrp */
        if (rk->rk_cgrp) {
                /* Reset queue forwarding (rep -> cgrp) */
                rd_kafka_q_fwd_set(&rk->rk_rep, NULL);
                rd_kafka_cgrp_destroy_final(rk->rk_cgrp);
        }

	/* Purge op-queues */
	rd_kafka_q_destroy(&rk->rk_rep);
	rd_kafka_q_destroy(&rk->rk_ops);

#if WITH_SSL
	if (rk->rk_conf.ssl.ctx)
		rd_kafka_transport_ssl_ctx_term(rk);
#endif

	rd_kafkap_str_destroy(rk->rk_conf.client_id);
        rd_kafkap_str_destroy(rk->rk_conf.group_id);
	rd_kafka_anyconf_destroy(_RK_GLOBAL, &rk->rk_conf);

	rd_kafkap_bytes_destroy((rd_kafkap_bytes_t *)rk->rk_null_bytes);
	rwlock_destroy(&rk->rk_lock);

	rd_free(rk);
        rd_atomic32_sub(&rd_kafka_handle_cnt_curr, 1);
}


static void rd_kafka_destroy_app (rd_kafka_t *rk, int blocking) {
        thrd_t thrd;

        rd_kafka_dbg(rk, ALL, "DESTROY", "Terminating instance");
        rd_kafka_wrlock(rk);
        thrd = rk->rk_thread;
	rd_atomic32_add(&rk->rk_terminate, 1);
        rd_kafka_timers_interrupt(&rk->rk_timers);
        rd_kafka_wrunlock(rk);

#ifndef _MSC_VER
        /* Interrupt main kafka thread to speed up termination. */
        if (rk->rk_conf.term_sig)
                pthread_kill(thrd, rk->rk_conf.term_sig);
#endif

        if (!blocking)
                return; /* FIXME: thread resource leak */

        if (thrd_join(thrd, NULL) != thrd_success)
                rd_kafka_assert(NULL, !*"failed to join main thread");
}


/* NOTE: Must only be called by application.
 *       librdkafka itself must use rd_kafka_destroy0(). */
void rd_kafka_destroy (rd_kafka_t *rk) {
        rd_kafka_destroy_app(rk, 1);
}


/**
 * Main destructor for rd_kafka_t
 *
 * Locality: rdkafka main thread
 */
static void rd_kafka_destroy_internal (rd_kafka_t *rk) {
	rd_kafka_itopic_t *rkt, *rkt_tmp;
	rd_kafka_broker_t *rkb, *rkb_tmp;
        rd_list_t wait_thrds;
        thrd_t *thrd;
        int i;

        rd_kafka_dbg(rk, ALL, "DESTROY", "Destroy internal");

	/* Brokers pick up on rk_terminate automatically. */

        /* The legacy/simple consumer lacks an API to close down the consumer*/
        if (rd_kafka_is_simple_consumer(rk))
                rd_kafka_simple_consumer_cleanup(rk);

        /* List of (broker) threads to join to synchronize termination */
        rd_list_init(&wait_thrds, rd_atomic32_get(&rk->rk_broker_cnt));

	rd_kafka_wrlock(rk);

        rd_kafka_dbg(rk, ALL, "DESTROY", "Remove all topics");
	/* Decommission all topics */
	TAILQ_FOREACH_SAFE(rkt, &rk->rk_topics, rkt_link, rkt_tmp) {
		rd_kafka_wrunlock(rk);
		rd_kafka_topic_partitions_remove(rkt);
		rd_kafka_wrlock(rk);
	}

        /* Decommission brokers.
         * Broker thread holds a refcount and detects when broker refcounts
         * reaches 1 and then decommissions itself. */
        TAILQ_FOREACH_SAFE(rkb, &rk->rk_brokers, rkb_link, rkb_tmp) {
                /* Add broker's thread to wait_thrds list for later joining */
                thrd = malloc(sizeof(*thrd));
                *thrd = rkb->rkb_thread;
                rd_list_add(&wait_thrds, thrd);
                rd_kafka_wrunlock(rk);

#ifndef _MSC_VER
                /* Interrupt IO threads to speed up termination. */
                if (rk->rk_conf.term_sig)
			pthread_kill(rkb->rkb_thread, rk->rk_conf.term_sig);
#endif

                rd_kafka_broker_destroy(rkb);

                rd_kafka_wrlock(rk);
        }


        rd_kafka_wrunlock(rk);

	/* Purge op-queue */
        rd_kafka_q_disable(&rk->rk_rep);
	rd_kafka_q_purge(&rk->rk_rep);

	/* Loose our special reference to the internal broker. */
        mtx_lock(&rk->rk_internal_rkb_lock);
	if ((rkb = rk->rk_internal_rkb)) {
                rk->rk_internal_rkb = NULL;
                thrd = malloc(sizeof(*thrd));
                *thrd = rkb->rkb_thread;
                rd_list_add(&wait_thrds, thrd);
        }
        mtx_unlock(&rk->rk_internal_rkb_lock);
	if (rkb)
		rd_kafka_broker_destroy(rkb);


        /* Join broker threads */
        RD_LIST_FOREACH(thrd, &wait_thrds, i) {
                if (thrd_join(*thrd, NULL) != thrd_success)
                        ;
                free(thrd);
        }

        rd_list_destroy(&wait_thrds, NULL);

}


/* Stats buffer printf */
#define _st_printf(...) do {					\
		ssize_t r;					\
		ssize_t rem = size-of;				\
		r = rd_snprintf(buf+of, rem, __VA_ARGS__);	\
		if (r >= rem) {					\
			size *= 2;				\
			rem = size-of;				\
			buf = rd_realloc(buf, size);		\
			r = rd_snprintf(buf+of, rem, __VA_ARGS__);	\
		}						\
		of += r;					\
	} while (0)

/**
 * Emit stats for toppar
 */
static RD_INLINE void rd_kafka_stats_emit_toppar (char **bufp, size_t *sizep,
					       size_t *ofp,
					       rd_kafka_toppar_t *rktp,
					       int first) {
	char *buf = *bufp;
	size_t size = *sizep;
	size_t of = *ofp;
        int64_t consumer_lag = -1;
        struct offset_stats offs;
        int32_t leader_nodeid = -1;

        rd_kafka_toppar_lock(rktp);

        if (rktp->rktp_leader) {
                rd_kafka_broker_lock(rktp->rktp_leader);
                leader_nodeid = rktp->rktp_leader->rkb_nodeid;
                rd_kafka_broker_unlock(rktp->rktp_leader);
        }

        /* Grab a copy of the latest finalized offset stats */
        offs = rktp->rktp_offsets_fin;

        if (offs.hi_offset != RD_KAFKA_OFFSET_INVALID && offs.fetch_offset > 0){
                if (offs.fetch_offset > offs.hi_offset)
                        consumer_lag = 0;
                else
                        consumer_lag = offs.hi_offset - offs.fetch_offset;
        }

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
		   "\"fetchq_size\":%"PRIu64", "
		   "\"fetch_state\":\"%s\", "
		   "\"query_offset\":%"PRId64", "
		   "\"next_offset\":%"PRId64", "
		   "\"app_offset\":%"PRId64", "
		   "\"stored_offset\":%"PRId64", "
		   "\"commited_offset\":%"PRId64", " /*FIXME: issue #80 */
		   "\"committed_offset\":%"PRId64", "
		   "\"eof_offset\":%"PRId64", "
		   "\"lo_offset\":%"PRId64", "
		   "\"hi_offset\":%"PRId64", "
                   "\"consumer_lag\":%"PRId64", "
		   "\"txmsgs\":%"PRIu64", "
		   "\"txbytes\":%"PRIu64", "
                   "\"msgs\": %"PRIu64", "
                   "\"rx_ver_drops\": %"PRIu64" "
		   "} ",
		   first ? "" : ", ",
		   rktp->rktp_partition,
		   rktp->rktp_partition,
                   leader_nodeid,
		   (rktp->rktp_flags&RD_KAFKA_TOPPAR_F_DESIRED)?"true":"false",
		   (rktp->rktp_flags&RD_KAFKA_TOPPAR_F_UNKNOWN)?"true":"false",
		   rd_atomic32_get(&rktp->rktp_msgq.rkmq_msg_cnt),
		   rd_atomic64_get(&rktp->rktp_msgq.rkmq_msg_bytes),
		   rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt),
		   rd_atomic64_get(&rktp->rktp_xmit_msgq.rkmq_msg_bytes),
		   rd_kafka_q_len(&rktp->rktp_fetchq),
		   rd_kafka_q_size(&rktp->rktp_fetchq),
		   rd_kafka_fetch_states[rktp->rktp_fetch_state],
		   rktp->rktp_query_offset,
                   offs.fetch_offset,
		   rktp->rktp_app_offset,
		   rktp->rktp_stored_offset,
		   rktp->rktp_committed_offset, /* FIXME: issue #80 */
		   rktp->rktp_committed_offset,
                   offs.eof_offset,
		   rktp->rktp_lo_offset,
		   rktp->rktp_hi_offset,
                   consumer_lag,
                   rd_atomic64_get(&rktp->rktp_c.tx_msgs),
		   rd_atomic64_get(&rktp->rktp_c.tx_bytes),
		   rd_atomic64_get(&rktp->rktp_c.msgs),
                   rd_atomic64_get(&rktp->rktp_c.rx_ver_drops));

        rd_kafka_toppar_unlock(rktp);

	*bufp = buf;
	*sizep = size;
	*ofp = of;
}

/**
 * Emit all statistics
 */
static void rd_kafka_stats_emit_all (rd_kafka_t *rk) {
	char  *buf;
	size_t size = 1024*10;
	size_t of = 0;
	rd_kafka_broker_t *rkb;
	rd_kafka_itopic_t *rkt;
	shptr_rd_kafka_toppar_t *s_rktp;
	rd_ts_t now;

	buf = rd_malloc(size);


	rd_kafka_rdlock(rk);

	now = rd_clock();
	_st_printf("{ "
                   "\"name\": \"%s\", "
                   "\"type\": \"%s\", "
		   "\"ts\":%"PRId64", "
		   "\"time\":%lli, "
		   "\"replyq\":%i, "
                   "\"msg_cnt\":%i, "
                   "\"msg_max\":%i, "
                   "\"simple_cnt\":%i, "
		   "\"brokers\":{ "/*open brokers*/,
                   rk->rk_name,
                   rd_kafka_type2str(rk->rk_type),
		   now,
		   (signed long long)time(NULL),
		   rd_kafka_q_len(&rk->rk_rep),
                   rd_atomic32_get(&rk->rk_producer.msg_cnt),
                   rk->rk_conf.queue_buffering_max_msgs,
                   rd_atomic32_get(&rk->rk_simple_cnt));


	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_avg_t rtt, throttle;
		rd_kafka_toppar_t *rktp;

		rd_kafka_broker_lock(rkb);
		rd_avg_rollover(&rtt, &rkb->rkb_avg_rtt);
		rd_avg_rollover(&throttle, &rkb->rkb_avg_throttle);
		_st_printf("%s\"%s\": { "/*open broker*/
			   "\"name\":\"%s\", "
			   "\"nodeid\":%"PRId32", "
			   "\"state\":\"%s\", "
                           "\"stateage\":%"PRId64", "
			   "\"outbuf_cnt\":%i, "
			   "\"outbuf_msg_cnt\":%i, "
			   "\"waitresp_cnt\":%i, "
			   "\"waitresp_msg_cnt\":%i, "
			   "\"tx\":%"PRIu64", "
			   "\"txbytes\":%"PRIu64", "
			   "\"txerrs\":%"PRIu64", "
			   "\"txretries\":%"PRIu64", "
			   "\"req_timeouts\":%"PRIu64", "
			   "\"rx\":%"PRIu64", "
			   "\"rxbytes\":%"PRIu64", "
			   "\"rxerrs\":%"PRIu64", "
                           "\"rxcorriderrs\":%"PRIu64", "
                           "\"rxpartial\":%"PRIu64", "
			   "\"rtt\": {"
			   " \"min\":%"PRId64","
			   " \"max\":%"PRId64","
			   " \"avg\":%"PRId64","
			   " \"sum\":%"PRId64","
			   " \"cnt\":%i "
			   "}, "
			   "\"throttle\": {"
			   " \"min\":%"PRId64","
			   " \"max\":%"PRId64","
			   " \"avg\":%"PRId64","
			   " \"sum\":%"PRId64","
			   " \"cnt\":%i "
			   "}, "
			   "\"toppars\":{ "/*open toppars*/,
			   rkb == TAILQ_FIRST(&rk->rk_brokers) ? "" : ", ",
			   rkb->rkb_name,
			   rkb->rkb_name,
			   rkb->rkb_nodeid,
			   rd_kafka_broker_state_names[rkb->rkb_state],
                           rkb->rkb_ts_state ? now - rkb->rkb_ts_state : 0,
			   rd_atomic32_get(&rkb->rkb_outbufs.rkbq_cnt),
			   rd_atomic32_get(&rkb->rkb_outbufs.rkbq_msg_cnt),
			   rd_atomic32_get(&rkb->rkb_waitresps.rkbq_cnt),
			   rd_atomic32_get(&rkb->rkb_waitresps.rkbq_msg_cnt),
			   rd_atomic64_get(&rkb->rkb_c.tx),
			   rd_atomic64_get(&rkb->rkb_c.tx_bytes),
			   rd_atomic64_get(&rkb->rkb_c.tx_err),
			   rd_atomic64_get(&rkb->rkb_c.tx_retries),
			   rd_atomic64_get(&rkb->rkb_c.req_timeouts),
			   rd_atomic64_get(&rkb->rkb_c.rx),
			   rd_atomic64_get(&rkb->rkb_c.rx_bytes),
			   rd_atomic64_get(&rkb->rkb_c.rx_err),
			   rd_atomic64_get(&rkb->rkb_c.rx_corrid_err),
			   rd_atomic64_get(&rkb->rkb_c.rx_partial),
			   rtt.ra_v.minv,
			   rtt.ra_v.maxv,
			   rtt.ra_v.avg,
			   rtt.ra_v.sum,
			   rtt.ra_v.cnt,
			   throttle.ra_v.minv,
			   throttle.ra_v.maxv,
			   throttle.ra_v.avg,
			   throttle.ra_v.sum,
			   throttle.ra_v.cnt);

		TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
			_st_printf("%s\"%.*s\": { "
				   "\"topic\":\"%.*s\", "
				   "\"partition\":%"PRId32"} ",
				   rktp==TAILQ_FIRST(&rkb->rkb_toppars)?"":", ",
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				   rktp->rktp_partition);
		}

		rd_kafka_broker_unlock(rkb);

		_st_printf("} "/*close toppars*/
			   "} "/*close broker*/);
	}


	_st_printf("}, " /* close "brokers" array */
		   "\"topics\":{ ");

	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		int i, j;

		rd_kafka_topic_rdlock(rkt);
		_st_printf("%s\"%.*s\": { "
			   "\"topic\":\"%.*s\", "
			   "\"metadata_age\":%"PRId64", "
			   "\"partitions\":{ " /*open partitions*/,
			   rkt==TAILQ_FIRST(&rk->rk_topics)?"":", ",
			   RD_KAFKAP_STR_PR(rkt->rkt_topic),
			   RD_KAFKAP_STR_PR(rkt->rkt_topic),
			   rkt->rkt_ts_metadata ?
			   (rd_clock() - rkt->rkt_ts_metadata)/1000 : 0);

		for (i = 0 ; i < rkt->rkt_partition_cnt ; i++)
			rd_kafka_stats_emit_toppar(&buf, &size, &of,
						   rd_kafka_toppar_s2i(rkt->rkt_p[i]),
						   i == 0);

                RD_LIST_FOREACH(s_rktp, &rkt->rkt_desp, j)
			rd_kafka_stats_emit_toppar(&buf, &size, &of,
						   rd_kafka_toppar_s2i(s_rktp),
						   i+j == 0);

                i += j;

		if (rkt->rkt_ua)
			rd_kafka_stats_emit_toppar(&buf, &size, &of,
						   rd_kafka_toppar_s2i(rkt->rkt_ua),
                                                   i++ == 0);
		rd_kafka_topic_rdunlock(rkt);

		_st_printf("} "/*close partitions*/
			   "} "/*close topic*/);

	}

	rd_kafka_rdunlock(rk);

	_st_printf("} "/*close topics*/
		   "}"/*close object*/);


	/* Enqueue op for application */
	rd_kafka_op_app_reply(&rk->rk_rep, RD_KAFKA_OP_STATS, 0, 0, buf, of);
}



static void rd_kafka_topic_scan_tmr_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_t *rk = rkts->rkts_rk;
	rd_kafka_topic_scan_all(rk, rd_clock());
}

static void rd_kafka_stats_emit_tmr_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_t *rk = rkts->rkts_rk;
	rd_kafka_stats_emit_all(rk);
}

static void rd_kafka_metadata_refresh_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_t *rk = rkts->rkts_rk;
        rd_kafka_broker_t *rkb;

        rd_kafka_rdlock(rk);
        rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP,
                                  rd_kafka_broker_filter_non_blocking, NULL);
        rd_kafka_rdunlock(rk);

        if (!rkb)
                return;

        if (rk->rk_conf.metadata_refresh_sparse)
                rd_kafka_broker_metadata_req(rkb, 0 /* known topics */, NULL,
                                             NULL, "sparse periodic refresh");
        else
                rd_kafka_broker_metadata_req(rkb, 1 /* all topics */, NULL,
                                             NULL, "periodic refresh");

        rd_kafka_broker_destroy(rkb);
}



static int rd_kafka_toppar_q_cb (rd_kafka_t *rk, rd_kafka_op_t *rko,
				 int cb_type, void *opaque) {
	rd_kafka_toppar_op_serve(rk, rko);
	return 1; /* op handled */
}


static void rd_kafka_toppars_q_serve (rd_kafka_q_t *rkq, int timeout_ms) {
	rd_kafka_q_serve(rkq, timeout_ms, 0,
			 _Q_CB_GLOBAL, rd_kafka_toppar_q_cb, NULL);
}
/**
 * Main loop for Kafka handler thread.
 */
static int rd_kafka_thread_main (void *arg) {
        rd_kafka_t *rk = arg;
	rd_kafka_timer_t tmr_topic_scan = RD_ZERO_INIT;
	rd_kafka_timer_t tmr_stats_emit = RD_ZERO_INIT;
	rd_kafka_timer_t tmr_metadata_refresh = RD_ZERO_INIT;

        rd_snprintf(rd_kafka_thread_name, sizeof(rd_kafka_thread_name), "main");

	(void)rd_atomic32_add(&rd_kafka_thread_cnt_curr, 1);

	/* Acquire lock (which was held by thread creator during creation)
	 * to synchronise state. */
	rd_kafka_wrlock(rk);
	rd_kafka_wrunlock(rk);

	rd_kafka_timer_start(&rk->rk_timers, &tmr_topic_scan, 1000000,
			     rd_kafka_topic_scan_tmr_cb, NULL);
	rd_kafka_timer_start(&rk->rk_timers, &tmr_stats_emit,
			     rk->rk_conf.stats_interval_ms * 1000,
			     rd_kafka_stats_emit_tmr_cb, NULL);
        if (rk->rk_conf.metadata_refresh_interval_ms >= 0)
                rd_kafka_timer_start(&rk->rk_timers, &tmr_metadata_refresh,
                                     rk->rk_conf.metadata_refresh_interval_ms *
                                     1000,
                                     rd_kafka_metadata_refresh_cb, NULL);

	if (rk->rk_cgrp)
		rd_kafka_cgrp_reassign_broker(rk->rk_cgrp);

	while (likely(!rd_kafka_terminating(rk) ||
		      rd_kafka_q_len(&rk->rk_ops))) {
		rd_ts_t sleeptime = rd_kafka_timers_next(
			&rk->rk_timers,
			rk->rk_conf.socket_blocking_max_ms * 1000, 1/*lock*/);
		rd_kafka_toppars_q_serve(&rk->rk_ops,
					 (int)(sleeptime / 1000));
		if (rk->rk_cgrp)
			rd_kafka_cgrp_serve(rk->rk_cgrp);
		rd_kafka_timers_run(&rk->rk_timers, RD_POLL_NOWAIT);
	}

	rd_kafka_q_disable(&rk->rk_ops);
	rd_kafka_q_purge(&rk->rk_ops);

        rd_kafka_timer_stop(&rk->rk_timers, &tmr_topic_scan, 1);
        rd_kafka_timer_stop(&rk->rk_timers, &tmr_stats_emit, 1);
        if (rk->rk_conf.metadata_refresh_interval_ms >= 0)
                rd_kafka_timer_stop(&rk->rk_timers, &tmr_metadata_refresh, 1);

        /* Synchronise state */
        rd_kafka_wrlock(rk);
        rd_kafka_wrunlock(rk);

        rd_kafka_destroy_internal(rk);
        rd_kafka_destroy_final(rk);

	rd_atomic32_sub(&rd_kafka_thread_cnt_curr, 1);

	return 0;
}


static void rd_kafka_term_sig_handler (int sig) {
	/* nop */
}

static void rd_kafka_global_init (void) {
#if ENABLE_SHAREDPTR_DEBUG
        LIST_INIT(&rd_shared_ptr_debug_list);
        mtx_init(&rd_shared_ptr_debug_mtx, mtx_plain);
        atexit(rd_shared_ptrs_dump);
#endif
	rd_kafka_transport_init();
#if WITH_SASL
	rd_kafka_sasl_global_init();
#endif
}

rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size) {
	rd_kafka_t *rk;
	static rd_atomic32_t rkid;
#ifndef _MSC_VER
        sigset_t newset, oldset;
#endif
	int err;

	call_once(&rd_kafka_global_init_once, rd_kafka_global_init);


	if (!conf)
		conf = rd_kafka_conf_new();

        /* Verify mandatory configuration */
        if (!conf->socket_cb) {
                rd_snprintf(errstr, errstr_size,
                         "Mandatory config property 'socket_cb' not set");
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

        if (!conf->open_cb) {
                rd_snprintf(errstr, errstr_size,
                         "Mandatory config property 'open_cb' not set");
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

	/*
	 * Set up the handle.
	 */
	rk = rd_calloc(1, sizeof(*rk));

	rk->rk_type = type;

	rk->rk_conf = *conf;
	rd_free(conf);

	rwlock_init(&rk->rk_lock);
        mtx_init(&rk->rk_internal_rkb_lock, mtx_plain);

	rd_kafka_q_init(&rk->rk_rep, rk);
	rd_kafka_q_init(&rk->rk_ops, rk);

	TAILQ_INIT(&rk->rk_brokers);
	TAILQ_INIT(&rk->rk_topics);
        rd_kafka_timers_init(&rk->rk_timers, rk);


	/* Convenience Kafka protocol null bytes */
	rk->rk_null_bytes = rd_kafkap_bytes_new(NULL, 0);

	if (rk->rk_conf.debug)
                rk->rk_conf.log_level = LOG_DEBUG;

	rd_snprintf(rk->rk_name, sizeof(rk->rk_name), "%s#%s-%i",
                    rk->rk_conf.client_id_str, rd_kafka_type2str(rk->rk_type),
                    rd_atomic32_add(&rkid, 1));

	/* Construct clientid kafka string */
	rk->rk_conf.client_id = rd_kafkap_str_new(rk->rk_conf.client_id_str,-1);

        /* Convert group.id to kafka string (may be NULL) */
        rk->rk_conf.group_id = rd_kafkap_str_new(rk->rk_conf.group_id_str,-1);

        /* Config fixups */
        rk->rk_conf.queued_max_msg_bytes =
                (int64_t)rk->rk_conf.queued_max_msg_kbytes * 1000ll;


        if (rd_kafka_assignors_init(rk, errstr, errstr_size) == -1) {
		rd_kafka_destroy_internal(rk);
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG, EINVAL);
		return NULL;
	}

#if WITH_SASL
	if (rk->rk_conf.security_protocol == RD_KAFKA_PROTO_SASL_SSL ||
	    rk->rk_conf.security_protocol == RD_KAFKA_PROTO_SASL_PLAINTEXT) {
		/* Validate SASL config */
		if (rd_kafka_sasl_conf_validate(rk, errstr, errstr_size) == -1) {
                        rd_kafka_destroy_internal(rk);
			rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG,
						EINVAL);
			return NULL;
		}
	}
#endif

#if WITH_SSL
	if (rk->rk_conf.security_protocol == RD_KAFKA_PROTO_SSL ||
	    rk->rk_conf.security_protocol == RD_KAFKA_PROTO_SASL_SSL) {
		/* Create SSL context */
		if (rd_kafka_transport_ssl_ctx_init(rk, errstr,
						    errstr_size) == -1) {

                        rd_kafka_destroy_internal(rk);
			rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG,
						EINVAL);
			return NULL;
		}
	}
#endif

	/* Client group, eligible both in consumer and producer mode. */
        if (RD_KAFKAP_STR_LEN(rk->rk_conf.group_id) > 0)
                rk->rk_cgrp = rd_kafka_cgrp_new(rk,
                                                rk->rk_conf.group_id,
                                                rk->rk_conf.client_id);



#ifndef _MSC_VER
        /* Block all signals in newly created thread.
         * To avoid race condition we block all signals in the calling
         * thread, which the new thread will inherit its sigmask from,
         * and then restore the original sigmask of the calling thread when
         * we're done creating the thread. */
        sigemptyset(&oldset);
        sigfillset(&newset);
	if (rk->rk_conf.term_sig) {
		struct sigaction sa_term = {
			.sa_handler = rd_kafka_term_sig_handler
		};
		sigaction(rk->rk_conf.term_sig, &sa_term, NULL);
	}
        pthread_sigmask(SIG_SETMASK, &newset, &oldset);
#endif

	/* Lock handle here to synchronise state, i.e., hold off
	 * the thread until we've finalized the handle. */
	rd_kafka_wrlock(rk);

	/* Create handler thread */
	if ((err = thrd_create(&rk->rk_thread,
			       rd_kafka_thread_main, rk)) != thrd_success) {
		if (errstr)
			rd_snprintf(errstr, errstr_size,
				 "Failed to create thread: %s (%i)",
				    rd_strerror(err), err);
		rd_kafka_wrunlock(rk);
                rd_kafka_destroy_internal(rk);
#ifndef _MSC_VER
		/* Restore sigmask of caller */
		pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
					err);
		return NULL;
	}

        rd_kafka_wrunlock(rk);

        mtx_lock(&rk->rk_internal_rkb_lock);
	rk->rk_internal_rkb = rd_kafka_broker_add(rk, RD_KAFKA_INTERNAL,
						  RD_KAFKA_PROTO_PLAINTEXT,
						  "", 0, RD_KAFKA_NODEID_UA);
        mtx_unlock(&rk->rk_internal_rkb_lock);


        rd_atomic32_add(&rd_kafka_handle_cnt_curr, 1);


	/* Add initial list of brokers from configuration */
	if (rk->rk_conf.brokerlist) {
		if (rd_kafka_brokers_add0(rk, rk->rk_conf.brokerlist) == 0)
			rd_kafka_op_err(rk, RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
					"No brokers configured");
	}

#ifndef _MSC_VER
	/* Restore sigmask of caller */
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif

	rd_kafka_set_last_error(0, 0);

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
	return rd_kafka_msg_new(rd_kafka_topic_a2i(rkt), partition,
				msgflags, payload, len,
				key, keylen, msg_opaque);
}


/**
 * Counts usage of the legacy/simple consumer (rd_kafka_consume_start() with
 * friends) since it does not have an API for stopping the cgrp we will need to
 * sort that out automatically in the background when all consumption
 * has stopped.
 *
 * Returns 0 if a  High level consumer is already instantiated
 * which means a Simple consumer cannot co-operate with it, else 1.
 *
 * A rd_kafka_t handle can never migrate from simple to high-level, or
 * vice versa, so we dont need a ..consumer_del().
 */
int rd_kafka_simple_consumer_add (rd_kafka_t *rk) {
        if (rd_atomic32_get(&rk->rk_simple_cnt) < 0)
                return 0;

        return (int)rd_atomic32_add(&rk->rk_simple_cnt, 1);
}




/**
 * rktp fetch is split up in these parts:
 *   * application side:
 *   * broker side (handled by current leader broker thread for rktp):
 *          - the fetch state, initial offset, etc.
 *          - fetching messages, updating fetched offset, etc.
 *          - offset commits
 *
 * Communication between the two are:
 *    app side -> broker side: rktp_ops
 *    broker side -> app side: rktp_fetchq
 *
 * There is no shared state between these two sides (threads), instead
 * state is communicated through the two op queues, and state synchronization
 * is performed by version barriers.
 *
 */

static RD_UNUSED
int rd_kafka_consume_start0 (rd_kafka_itopic_t *rkt, int32_t partition,
				    int64_t offset, rd_kafka_q_t *rkq) {
	shptr_rd_kafka_toppar_t *s_rktp;

	if (partition < 0) {
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
					ESRCH);
		return -1;
	}

        if (!rd_kafka_simple_consumer_add(rkt->rkt_rk)) {
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG, EINVAL);
                return -1;
        }

	rd_kafka_topic_wrlock(rkt);
	s_rktp = rd_kafka_toppar_desired_add(rkt, partition);
	rd_kafka_topic_wrunlock(rkt);

        /* Verify offset */
	if (offset == RD_KAFKA_OFFSET_BEGINNING ||
	    offset == RD_KAFKA_OFFSET_END ||
            offset <= RD_KAFKA_OFFSET_TAIL_BASE) {
                /* logical offsets */

	} else if (offset == RD_KAFKA_OFFSET_STORED) {
		/* offset manager */

                if (rkt->rkt_conf.offset_store_method ==
                    RD_KAFKA_OFFSET_METHOD_BROKER &&
                    RD_KAFKAP_STR_IS_NULL(rkt->rkt_rk->rk_conf.group_id)) {
                        /* Broker based offsets require a group id. */
                        rd_kafka_toppar_destroy(s_rktp);
			rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG,
						EINVAL);
                        return -1;
                }

	} else if (offset < 0) {
		rd_kafka_toppar_destroy(s_rktp);
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG,
					EINVAL);
		return -1;

        }

        rd_kafka_toppar_op_fetch_start(rd_kafka_toppar_s2i(s_rktp), offset,
                                      rkq, NULL);

        rd_kafka_toppar_destroy(s_rktp);

	rd_kafka_set_last_error(0, 0);
	return 0;
}




int rd_kafka_consume_start (rd_kafka_topic_t *app_rkt, int32_t partition,
			    int64_t offset) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
        rd_kafka_dbg(rkt->rkt_rk, TOPIC, "START",
                     "Start consuming partition %"PRId32,partition);
 	return rd_kafka_consume_start0(rkt, partition, offset, NULL);
}

int rd_kafka_consume_start_queue (rd_kafka_topic_t *app_rkt, int32_t partition,
				  int64_t offset, rd_kafka_queue_t *rkqu) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);

 	return rd_kafka_consume_start0(rkt, partition, offset, &rkqu->rkqu_q);
}




static RD_UNUSED int rd_kafka_consume_stop0 (rd_kafka_toppar_t *rktp) {
        rd_kafka_q_t *tmpq = NULL;
        rd_kafka_resp_err_t err;

        rd_kafka_topic_wrlock(rktp->rktp_rkt);
        rd_kafka_toppar_lock(rktp);
	rd_kafka_toppar_desired_del(rktp);
        rd_kafka_toppar_unlock(rktp);
	rd_kafka_topic_wrunlock(rktp->rktp_rkt);

        tmpq = rd_kafka_q_new(rktp->rktp_rkt->rkt_rk);

        rd_kafka_toppar_op_fetch_stop(rktp, tmpq);

        /* Synchronisation: Wait for stop reply from broker thread */
        err = rd_kafka_q_wait_result(tmpq, RD_POLL_INFINITE);
        rd_kafka_q_destroy(tmpq);

	rd_kafka_set_last_error(err, err ? EINVAL : 0);

	return err ? -1 : 0;
}


int rd_kafka_consume_stop (rd_kafka_topic_t *app_rkt, int32_t partition) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
	shptr_rd_kafka_toppar_t *s_rktp;
        int r;

	if (partition == RD_KAFKA_PARTITION_UA) {
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__INVALID_ARG, EINVAL);
		return -1;
	}

	rd_kafka_topic_wrlock(rkt);
	if (!(s_rktp = rd_kafka_toppar_get(rkt, partition, 0)) &&
	    !(s_rktp = rd_kafka_toppar_desired_get(rkt, partition))) {
		rd_kafka_topic_wrunlock(rkt);
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
					ESRCH);
		return -1;
	}
        rd_kafka_topic_wrunlock(rkt);

        r = rd_kafka_consume_stop0(rd_kafka_toppar_s2i(s_rktp));
	/* set_last_error() called by stop0() */

        rd_kafka_toppar_destroy(s_rktp);

        return r;
}



rd_kafka_resp_err_t rd_kafka_seek (rd_kafka_topic_t *app_rkt,
                                   int32_t partition,
                                   int64_t offset,
                                   int timeout_ms) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
        shptr_rd_kafka_toppar_t *s_rktp;
	rd_kafka_toppar_t *rktp;
        rd_kafka_q_t *tmpq = NULL;
        rd_kafka_resp_err_t err;

        /* FIXME: simple consumer check */

	if (partition == RD_KAFKA_PARTITION_UA)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

	rd_kafka_topic_rdlock(rkt);
	if (!(s_rktp = rd_kafka_toppar_get(rkt, partition, 0)) &&
	    !(s_rktp = rd_kafka_toppar_desired_get(rkt, partition))) {
		rd_kafka_topic_rdunlock(rkt);
                return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
	}
	rd_kafka_topic_rdunlock(rkt);

        if (timeout_ms)
                tmpq = rd_kafka_q_new(rkt->rkt_rk);

        rktp = rd_kafka_toppar_s2i(s_rktp);
        if ((err = rd_kafka_toppar_op_seek(rktp, offset, tmpq))) {
                if (tmpq)
                        rd_kafka_q_destroy(tmpq);
                rd_kafka_toppar_destroy(s_rktp);
                return err;
        }

	rd_kafka_toppar_destroy(s_rktp);

        if (tmpq) {
                err = rd_kafka_q_wait_result(tmpq, timeout_ms);
                rd_kafka_q_destroy(tmpq);
                return err;
        }

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



static ssize_t rd_kafka_consume_batch0 (rd_kafka_q_t *rkq,
					int timeout_ms,
					rd_kafka_message_t **rkmessages,
					size_t rkmessages_size) {
	/* Populate application's rkmessages array. */
	return rd_kafka_q_serve_rkmessages(rkq, timeout_ms,
					   rkmessages, rkmessages_size);
}


ssize_t rd_kafka_consume_batch (rd_kafka_topic_t *app_rkt, int32_t partition,
				int timeout_ms,
				rd_kafka_message_t **rkmessages,
				size_t rkmessages_size) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
	shptr_rd_kafka_toppar_t *s_rktp;
        rd_kafka_toppar_t *rktp;
	ssize_t cnt;

	/* Get toppar */
	rd_kafka_topic_rdlock(rkt);
	s_rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!s_rktp))
		s_rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_rdunlock(rkt);

	if (unlikely(!s_rktp)) {
		/* No such toppar known */
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
					ESRCH);
		return -1;
	}

        rktp = rd_kafka_toppar_s2i(s_rktp);

	/* Populate application's rkmessages array. */
	cnt = rd_kafka_q_serve_rkmessages(&rktp->rktp_fetchq, timeout_ms,
					  rkmessages, rkmessages_size);

	rd_kafka_toppar_destroy(s_rktp); /* refcnt from .._get() */

	rd_kafka_set_last_error(0, 0);

	return cnt;
}

ssize_t rd_kafka_consume_batch_queue (rd_kafka_queue_t *rkqu,
				      int timeout_ms,
				      rd_kafka_message_t **rkmessages,
				      size_t rkmessages_size) {
	/* Populate application's rkmessages array. */
	return rd_kafka_consume_batch0(&rkqu->rkqu_q, timeout_ms,
				       rkmessages, rkmessages_size);
}


struct consume_ctx {
	void (*consume_cb) (rd_kafka_message_t *rkmessage, void *opaque);
	void *opaque;
};


/**
 * Trampoline for application's consume_cb()
 */
static int rd_kafka_consume_cb (rd_kafka_t *rk, rd_kafka_op_t *rko,
                                int cb_type, void *opaque) {
	struct consume_ctx *ctx = opaque;
	rd_kafka_message_t *rkmessage;
        rd_kafka_toppar_t *rktp;

        rktp = rko->rko_rktp ? rd_kafka_toppar_s2i(rko->rko_rktp) : NULL;

        if (unlikely(rko->rko_version && rktp &&
                     rko->rko_version < rd_atomic32_get(&rktp->rktp_version)))
                return 1;

	rkmessage = rd_kafka_message_get(rko);
	if (!rko->rko_err) {
		rd_kafka_toppar_lock(rktp);
		rktp->rktp_app_offset = rkmessage->offset+1;
		if (rk->rk_conf.enable_auto_offset_store)
			rd_kafka_offset_store0(rktp, rkmessage->offset+1,
					       0/*no lock*/);
		rd_kafka_toppar_unlock(rktp);
	}

	ctx->consume_cb(rkmessage, ctx->opaque);

        return 1;
}



static int rd_kafka_consume_callback0 (rd_kafka_q_t *rkq,
				       int timeout_ms,
                                       int max_cnt,
				       void (*consume_cb) (rd_kafka_message_t
							   *rkmessage,
							   void *opaque),
				       void *opaque) {
	struct consume_ctx ctx = { .consume_cb = consume_cb, .opaque = opaque };
	return rd_kafka_q_serve(rkq, timeout_ms, max_cnt,
                                _Q_CB_CONSUMER, rd_kafka_consume_cb, &ctx);

}


int rd_kafka_consume_callback (rd_kafka_topic_t *app_rkt, int32_t partition,
			       int timeout_ms,
			       void (*consume_cb) (rd_kafka_message_t
						   *rkmessage,
						   void *opaque),
			       void *opaque) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
        shptr_rd_kafka_toppar_t *s_rktp;
	rd_kafka_toppar_t *rktp;
	int r;

	/* Get toppar */
	rd_kafka_topic_rdlock(rkt);
	s_rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!s_rktp))
		s_rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_rdunlock(rkt);

	if (unlikely(!s_rktp)) {
		/* No such toppar known */
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
					ESRCH);
		return -1;
	}

        rktp = rd_kafka_toppar_s2i(s_rktp);
	r = rd_kafka_consume_callback0(&rktp->rktp_fetchq, timeout_ms,
                                       rkt->rkt_conf.consume_callback_max_msgs,
				       consume_cb, opaque);

	rd_kafka_toppar_destroy(s_rktp);

	rd_kafka_set_last_error(0, 0);

	return r;
}



int rd_kafka_consume_callback_queue (rd_kafka_queue_t *rkqu,
				     int timeout_ms,
				     void (*consume_cb) (rd_kafka_message_t
							 *rkmessage,
							 void *opaque),
				     void *opaque) {
	return rd_kafka_consume_callback0(&rkqu->rkqu_q, timeout_ms, 0,
					  consume_cb, opaque);
}


/**
 * Serve queue 'rkq' and return one message.
 * By serving the queue it will also call any registered callbacks
 * registered for matching events, this includes consumer_cb()
 * in which case no message will be returned.
 */
static rd_kafka_message_t *rd_kafka_consume0 (rd_kafka_t *rk,
                                              rd_kafka_q_t *rkq,
					      int timeout_ms) {
	rd_kafka_op_t *rko;
	rd_kafka_message_t *rkmessage = NULL;
	rd_ts_t ts_end;

	if (timeout_ms == RD_POLL_NOWAIT)
		ts_end = 0;
	else if (timeout_ms == RD_POLL_INFINITE)
		ts_end = INT64_MAX;
	else
		ts_end = rd_clock() + timeout_ms * 1000;

	rd_kafka_yield_thread = 0;
        while ((rko = rd_kafka_q_pop(rkq, timeout_ms, 0))) {
                if (rd_kafka_poll_cb(rk, rko, _Q_CB_CONSUMER, NULL)) {
                        /* Message was handled by callback. */
                        rd_kafka_op_destroy(rko);

			if (unlikely(rd_kafka_yield_thread)) {
				/* Callback called rd_kafka_yield(), we must
				 * stop dispatching the queue and return. */
				rko = NULL;
				break;
			}

			if (timeout_ms > 0 && rd_clock() > ts_end) {
				rko = NULL;
				break;
			}
                        continue;
                }
                break;
        }

	if (!rko) {
		/* Timeout reached with no op returned. */
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__TIMED_OUT,
					ETIMEDOUT);
		return NULL;
	}

        rd_kafka_assert(rk,
                        rko->rko_type == RD_KAFKA_OP_FETCH ||
                        rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR);

	/* Get rkmessage from rko */
	rkmessage = rd_kafka_message_get(rko);

	/* Store offset */
	if (!rko->rko_err) {
                rd_kafka_toppar_t *rktp;
                rktp = rd_kafka_toppar_s2i(rko->rko_rktp);
		rd_kafka_toppar_lock(rktp);
		rktp->rktp_app_offset = rkmessage->offset+1;
                if (rk->rk_conf.enable_auto_offset_store)
                        rd_kafka_offset_store0(rktp, rkmessage->offset+1,
                                               0/*no lock*/);
		rd_kafka_toppar_unlock(rktp);
        }

	rd_kafka_set_last_error(0, 0);

	return rkmessage;
}

rd_kafka_message_t *rd_kafka_consume (rd_kafka_topic_t *app_rkt,
                                      int32_t partition,
				      int timeout_ms) {
        rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(app_rkt);
        shptr_rd_kafka_toppar_t *s_rktp;
	rd_kafka_toppar_t *rktp;
	rd_kafka_message_t *rkmessage;

	rd_kafka_topic_rdlock(rkt);
	s_rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua on miss*/);
	if (unlikely(!s_rktp))
		s_rktp = rd_kafka_toppar_desired_get(rkt, partition);
	rd_kafka_topic_rdunlock(rkt);

	if (unlikely(!s_rktp)) {
		/* No such toppar known */
		rd_kafka_set_last_error(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
					ESRCH);
		return NULL;
	}

        rktp = rd_kafka_toppar_s2i(s_rktp);
	rkmessage = rd_kafka_consume0(rkt->rkt_rk,
                                      &rktp->rktp_fetchq, timeout_ms);

	rd_kafka_toppar_destroy(s_rktp); /* refcnt from .._get() */

	rd_kafka_set_last_error(0, 0);

	return rkmessage;
}


rd_kafka_message_t *rd_kafka_consume_queue (rd_kafka_queue_t *rkqu,
					    int timeout_ms) {
	return rd_kafka_consume0(rkqu->rkqu_rk, &rkqu->rkqu_q, timeout_ms);
}




rd_kafka_resp_err_t rd_kafka_poll_set_consumer (rd_kafka_t *rk) {
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rd_kafka_q_fwd_set(&rk->rk_rep, &rkcg->rkcg_q);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}




rd_kafka_message_t *rd_kafka_consumer_poll (rd_kafka_t *rk,
                                            int timeout_ms) {
        rd_kafka_cgrp_t *rkcg;

        if (unlikely(!(rkcg = rd_kafka_cgrp_get(rk)))) {
                rd_kafka_message_t *rkmessage = rd_kafka_message_new();
                rkmessage->err = RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;
                return rkmessage;
        }

        return rd_kafka_consume0(rk, &rkcg->rkcg_q, timeout_ms);
}


rd_kafka_resp_err_t rd_kafka_consumer_close (rd_kafka_t *rk) {
        rd_kafka_cgrp_t *rkcg;
        rd_kafka_op_t *rko;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rd_kafka_q_keep(&rkcg->rkcg_q);
        rd_kafka_cgrp_terminate(rkcg, &rkcg->rkcg_q); /* async */

        while ((rko = rd_kafka_q_pop(&rkcg->rkcg_q, RD_POLL_INFINITE, 0))) {
                if (rko->rko_type == RD_KAFKA_OP_TERMINATE) {
                        err = rko->rko_err;
                        rd_kafka_op_destroy(rko);
                        break;
                }
                rd_kafka_poll_cb(rk, rko, _Q_CB_CONSUMER, NULL);
                rd_kafka_op_destroy(rko);
        }

        rd_kafka_q_destroy(&rkcg->rkcg_q);
        return err;
}



rd_kafka_resp_err_t
rd_kafka_committed (rd_kafka_t *rk,
		    rd_kafka_topic_partition_list_t *partitions,
		    int timeout_ms) {
        rd_kafka_q_t *replyq;
        rd_kafka_resp_err_t err;
        rd_kafka_cgrp_t *rkcg;
	rd_ts_t abs_timeout = rd_timeout_init(timeout_ms);

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

	/* Set default offsets. */
	rd_kafka_topic_partition_list_reset_offsets(partitions,
						    RD_KAFKA_OFFSET_INVALID);

        replyq = rd_kafka_q_new(rk);
        do {
                rd_kafka_op_t *rko;

                rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_FETCH);
                rko->rko_replyq = replyq;
                rd_kafka_q_keep(rko->rko_replyq);

                rd_kafka_op_payload_set(rko, partitions, NULL);

                rd_kafka_q_enq(&rkcg->rkcg_ops, rko);

                rko = rd_kafka_q_pop(replyq, timeout_ms, 0);
                if (rko) {
                        rd_kafka_topic_partition_list_t *offsets =
                                rko->rko_payload;

                        if (!(err = rko->rko_err)) {
                                rd_kafka_assert(NULL, offsets == partitions);
                                rko->rko_payload = NULL;
                        } else if (err == RD_KAFKA_RESP_ERR__WAIT_COORD ||
				   err == RD_KAFKA_RESP_ERR__TRANSPORT) {
                                rd_usleep(10*1000, &rk->rk_terminate);
				rd_timeout_adjust(abs_timeout, &timeout_ms);
			}

                        rd_kafka_op_destroy(rko);
                } else
                        err = RD_KAFKA_RESP_ERR__TIMED_OUT;
        } while (err == RD_KAFKA_RESP_ERR__TRANSPORT ||
		 err == RD_KAFKA_RESP_ERR__WAIT_COORD);

        rd_kafka_q_destroy(replyq);

        return err;
}



rd_kafka_resp_err_t
rd_kafka_position (rd_kafka_t *rk,
		   rd_kafka_topic_partition_list_t *partitions) {
 	int i;

	/* Set default offsets. */
	rd_kafka_topic_partition_list_reset_offsets(partitions,
						    RD_KAFKA_OFFSET_INVALID);

	for (i = 0 ; i < partitions->cnt ; i++) {
		rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
		shptr_rd_kafka_toppar_t *s_rktp;
		rd_kafka_toppar_t *rktp;

		if (!(s_rktp = rd_kafka_toppar_get2(rk, rktpar->topic,
						    rktpar->partition, 0, 0))) {
			rktpar->err = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
			rktpar->offset = RD_KAFKA_OFFSET_INVALID;
			continue;
		}

		rktp = rd_kafka_toppar_s2i(s_rktp);
		rd_kafka_toppar_lock(rktp);
		rktpar->offset = rktp->rktp_app_offset;
		rktpar->err = RD_KAFKA_RESP_ERR_NO_ERROR;
		rd_kafka_toppar_unlock(rktp);
		rd_kafka_toppar_destroy(s_rktp);
	}

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



struct _query_wmark_offsets_state {
	rd_kafka_resp_err_t err;
	const char *topic;
	int32_t partition;
	int64_t offsets[2];
	size_t  cnt;
	rd_ts_t ts_end;
};

static void rd_kafka_query_wmark_offsets_resp_cb (rd_kafka_t *rk,
						  rd_kafka_broker_t *rkb,
						  rd_kafka_resp_err_t err,
						  rd_kafka_buf_t *rkbuf,
						  rd_kafka_buf_t *request,
						  void *opaque) {
	struct _query_wmark_offsets_state *state = opaque;

	err = rd_kafka_handle_Offset(rk, rkb, err, rkbuf, request,
				     state->topic, state->partition,
				     state->offsets, &state->cnt);
	if (err == RD_KAFKA_RESP_ERR__IN_PROGRESS)
		return; /* Retrying */

	/* Retry if no broker connection is available yet. */
	if ((err == RD_KAFKA_RESP_ERR__WAIT_COORD ||
	     err == RD_KAFKA_RESP_ERR__TRANSPORT) &&
	    rkb &&
	    rd_clock() + (50 * 1000) < state->ts_end) {
		/* Sleep and retry */
		rd_usleep(50 * 1000, &rkb->rkb_rk->rk_terminate);

		request->rkbuf_retries = 0;
		if (rd_kafka_buf_retry(rkb, request))
			return; /* Retry in progress */
		/* FALLTHRU */
	}

	state->err = err;
}


rd_kafka_resp_err_t
rd_kafka_query_watermark_offsets (rd_kafka_t *rk, const char *topic,
				  int32_t partition,
				  int64_t *low, int64_t *high, int timeout_ms) {
	rd_kafka_broker_t *rkb;
	rd_kafka_q_t *replyq;
	struct _query_wmark_offsets_state state;
	shptr_rd_kafka_toppar_t *s_rktp;
	rd_kafka_toppar_t *rktp;
	rd_ts_t ts_end = rd_clock() +
		(timeout_ms == RD_POLL_INFINITE ? INT_MAX : timeout_ms) * 1000;

	/* Look up toppar so we know which broker to query. */
	s_rktp = rd_kafka_toppar_get2(rk, topic, partition, 0, 1);
	if (!s_rktp)
		return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
	rktp = rd_kafka_toppar_s2i(s_rktp);

	/* Get toppar's leader broker. */
	do {
		if ((rkb = rd_kafka_toppar_leader(rktp, 1)))
			break;

		if (timeout_ms > 0)
			rd_usleep(RD_MIN(10, timeout_ms)*1000,
				  &rk->rk_terminate);
	} while (rd_clock() < ts_end);

	if (!rkb) {
		rd_kafka_toppar_destroy(s_rktp);
		return RD_KAFKA_RESP_ERR__WAIT_COORD;
	}

        replyq = rd_kafka_q_new(rk);

	state.topic = topic;
	state.partition = partition;
	state.offsets[0] = RD_KAFKA_OFFSET_BEGINNING;
	state.offsets[1] = RD_KAFKA_OFFSET_END;
	state.cnt = 2;
	state.err = RD_KAFKA_RESP_ERR__IN_PROGRESS;
	state.ts_end = ts_end;

	rd_kafka_OffsetRequest(rkb, topic, partition, state.offsets, state.cnt,
			       0, replyq, rd_kafka_query_wmark_offsets_resp_cb,
			       &state);
        rd_kafka_broker_destroy(rkb);

        /* Wait for reply (or timeout) */
	while (state.err == RD_KAFKA_RESP_ERR__IN_PROGRESS)
		rd_kafka_q_serve(replyq, 100, 0, _Q_CB_GLOBAL,
				 rd_kafka_poll_cb, NULL);

        rd_kafka_q_destroy(replyq);
	rd_kafka_toppar_destroy(s_rktp);

	if (state.err)
		return state.err;
	else if (state.cnt == 0 || state.cnt > 2)
		return RD_KAFKA_RESP_ERR__FAIL;

	/* Broker may return offsets in no parcitular order. */
	if (state.offsets[0] < state.offsets[1]) {
		*low = state.offsets[0];
		*high  = state.offsets[1];
	} else {
		*low = state.offsets[1];
		*high = state.offsets[0];
	}

	/* If partition is empty only one offset (the last) will be returned. */
	if (*low < 0 && *high >= 0)
		*low = *high;

	return RD_KAFKA_RESP_ERR_NO_ERROR;
}


rd_kafka_resp_err_t
rd_kafka_get_watermark_offsets (rd_kafka_t *rk, const char *topic,
				int32_t partition,
				int64_t *low, int64_t *high) {
	shptr_rd_kafka_toppar_t *s_rktp;
	rd_kafka_toppar_t *rktp;

	s_rktp = rd_kafka_toppar_get2(rk, topic, partition, 0, 1);
	if (!s_rktp)
		return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
	rktp = rd_kafka_toppar_s2i(s_rktp);

	rd_kafka_toppar_lock(rktp);
	*low = rktp->rktp_lo_offset;
	*high = rktp->rktp_hi_offset;
	rd_kafka_toppar_unlock(rktp);

	rd_kafka_toppar_destroy(s_rktp);

	return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * rd_kafka_poll() (and similar) op callback handler.
 * Will either call registered callback depending on cb_type and op type
 * or return op to application, if applicable (e.g., fetch message).
 *
 * Returns 1 if op was handled, else 0.
 *
 * Locality: application thread
 */
int rd_kafka_poll_cb (rd_kafka_t *rk, rd_kafka_op_t *rko,
                      int cb_type, void *opaque) {
	rd_kafka_msg_t *rkm;
	static int dcnt = 0;

	switch ((int)rko->rko_type)
	{
        case RD_KAFKA_OP_CALLBACK:
                rd_kafka_op_call(rk, rko);
                break;

	case RD_KAFKA_OP_FETCH:
		if (!rk->rk_conf.consume_cb)
			return 0; /* Dont handle here */
		{
			struct consume_ctx ctx = {
				.consume_cb = rk->rk_conf.consume_cb,
				.opaque = rk->rk_conf.opaque };

			rd_kafka_consume_cb(rk, rko, _Q_CB_CONSUMER, &ctx);
		}
		break;

        case RD_KAFKA_OP_REBALANCE:
                rk->rk_conf.rebalance_cb(rk, rko->rko_err,
					 (rd_kafka_topic_partition_list_t *)
					 rko->rko_payload,
                                         rk->rk_conf.opaque);
		break;

        case RD_KAFKA_OP_OFFSET_COMMIT | RD_KAFKA_OP_REPLY:
                rk->rk_conf.offset_commit_cb(
                        rk, rko->rko_err,
                        (rd_kafka_topic_partition_list_t *)rko->rko_payload,
                        rk->rk_conf.opaque);
                break;

        case RD_KAFKA_OP_CONSUMER_ERR:
                /* rd_kafka_consumer_poll() (_Q_CB_CONSUMER):
                 *   Consumer errors are returned to the application
                 *   as rkmessages, not error callbacks.
                 *
                 * rd_kafka_poll() (_Q_CB_GLOBAL):
                 *   convert to ERR op (fallthru)
                 */
                if (cb_type == _Q_CB_CONSUMER)
                        return 0; /* return as message_t to application */
                /* FALLTHRU */

	case RD_KAFKA_OP_ERR:
		if (rk->rk_conf.error_cb) {
			char *errstr = rd_strndup(rko->rko_payload,
                                                  rko->rko_len);
			rk->rk_conf.error_cb(rk, rko->rko_err,
                                             errstr, rk->rk_conf.opaque);
			rd_free(errstr);
		} else
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

                        if (rk->rk_conf.dr_msg_cb) {
                                rd_kafka_message_t rkmessage = {
                                        .payload    = rkm->rkm_payload,
                                        .len        = rkm->rkm_len,
                                        .err        = rko->rko_err,
                                        .offset     = rkm->rkm_offset,
                                        .rkt        = rko->rko_rkt,
                                        .partition  = rkm->rkm_partition,
                                        ._private   = rkm->rkm_opaque,
                                };

                                if (rkm->rkm_key &&
                                    !RD_KAFKAP_BYTES_IS_NULL(rkm->rkm_key)) {
                                        rkmessage.key =
						(void *)rkm->rkm_key->data;
                                        rkmessage.key_len =
                                                RD_KAFKAP_BYTES_LEN(
                                                        rkm->rkm_key);
                                }

                                rk->rk_conf.dr_msg_cb(rk, &rkmessage,
                                                      rk->rk_conf.opaque);

                        } else {

                                rk->rk_conf.dr_cb(rk,
                                                  rkm->rkm_payload,
                                                  rkm->rkm_len,
                                                  rko->rko_err,
                                                  rk->rk_conf.opaque,
                                                  rkm->rkm_opaque);
                        }

			rd_kafka_msg_destroy(rk, rkm);
		}

		rd_kafka_msgq_init(&rko->rko_msgq);

		if (!(dcnt % 1000))
			rd_kafka_dbg(rk, MSG, "POLL",
				     "Now %i messages delivered to app", dcnt);
		break;

	case RD_KAFKA_OP_THROTTLE:
		if (rk->rk_conf.throttle_cb)
			rk->rk_conf.throttle_cb(rk, rko->rko_nodename,
						rko->rko_nodeid,
						(int) rko->rko_throttle_time,
						rk->rk_conf.opaque);
		break;

	case RD_KAFKA_OP_STATS:
		/* Statistics */
		if (rk->rk_conf.stats_cb &&
		    rk->rk_conf.stats_cb(rk, rko->rko_json,
                                         rko->rko_json_len,
					 rk->rk_conf.opaque) == 1)
			rko->rko_json = NULL; /* Application wanted json ptr */
		break;

        case RD_KAFKA_OP_RECV_BUF:
                /* Handle response */
                rd_kafka_buf_handle_op(rko, rko->rko_err);
                break;

	default:
		rd_kafka_dbg(rk, ALL, "POLLCB",
			     "cant handle op %i here", rko->rko_type);
		rd_kafka_assert(rk, !*"cant handle op type");
		break;
	}

        return 1; /* op was handled */
}

int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms) {
	return rd_kafka_q_serve(&rk->rk_rep, timeout_ms, 0,
				_Q_CB_GLOBAL, rd_kafka_poll_cb, NULL);
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
		indent, rd_refcnt_get(&rktp->rktp_refcnt),
		indent, rd_atomic32_get(&rktp->rktp_msgq.rkmq_msg_cnt),
		indent, rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt),
		indent, rd_atomic64_get(&rktp->rktp_c.tx_msgs), rd_atomic64_get(&rktp->rktp_c.tx_bytes));
}

static void rd_kafka_broker_dump (FILE *fp, rd_kafka_broker_t *rkb, int locks) {
	rd_kafka_toppar_t *rktp;

        if (locks)
                rd_kafka_broker_lock(rkb);
        fprintf(fp, " rd_kafka_broker_t %p: %s NodeId %"PRId32
                " in state %s (for %.3fs)\n",
                rkb, rkb->rkb_name, rkb->rkb_nodeid,
                rd_kafka_broker_state_names[rkb->rkb_state],
                rkb->rkb_ts_state ?
                (float)(rd_clock() - rkb->rkb_ts_state) / 1000000.0f :
                0.0f);
        fprintf(fp, "  refcnt %i\n", rd_refcnt_get(&rkb->rkb_refcnt));
        fprintf(fp, "  outbuf_cnt: %i waitresp_cnt: %i\n",
                rd_atomic32_get(&rkb->rkb_outbufs.rkbq_cnt),
                rd_atomic32_get(&rkb->rkb_waitresps.rkbq_cnt));
        fprintf(fp,
                "  %"PRIu64 " messages sent, %"PRIu64" bytes, "
                "%"PRIu64" errors, %"PRIu64" timeouts\n"
                "  %"PRIu64 " messages received, %"PRIu64" bytes, "
                "%"PRIu64" errors\n"
                "  %"PRIu64 " messageset transmissions were retried\n",
                rd_atomic64_get(&rkb->rkb_c.tx), rd_atomic64_get(&rkb->rkb_c.tx_bytes),
                rd_atomic64_get(&rkb->rkb_c.tx_err), rd_atomic64_get(&rkb->rkb_c.req_timeouts),
                rd_atomic64_get(&rkb->rkb_c.rx), rd_atomic64_get(&rkb->rkb_c.rx_bytes),
                rd_atomic64_get(&rkb->rkb_c.rx_err),
                rd_atomic64_get(&rkb->rkb_c.tx_retries));

        fprintf(fp, "  %i toppars:\n", rkb->rkb_toppar_cnt);
        TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink)
                rd_kafka_toppar_dump(fp, "   ", rktp);
        if (locks) {
                rd_kafka_broker_unlock(rkb);
        }
}


static void rd_kafka_dump0 (FILE *fp, rd_kafka_t *rk, int locks) {
	rd_kafka_broker_t *rkb;
	rd_kafka_itopic_t *rkt;
	rd_kafka_toppar_t *rktp;
        shptr_rd_kafka_toppar_t *s_rktp;
        int i;

	if (locks)
                rd_kafka_rdlock(rk);

        fprintf(fp, "rd_kafka_op_cnt: %d\n", rd_atomic32_get(&rd_kafka_op_cnt));
	fprintf(fp, "rd_kafka_t %p: %s\n", rk, rk->rk_name);

	fprintf(fp, " producer.msg_cnt %i\n",
		rd_atomic32_get(&rk->rk_producer.msg_cnt));
	fprintf(fp, " rk_rep reply queue: %i ops\n",
		rd_kafka_q_len(&rk->rk_rep));

	fprintf(fp, " brokers:\n");
        if (locks)
                mtx_lock(&rk->rk_internal_rkb_lock);
        if (rk->rk_internal_rkb)
                rd_kafka_broker_dump(fp, rk->rk_internal_rkb, locks);
        if (locks)
                mtx_unlock(&rk->rk_internal_rkb_lock);

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_kafka_broker_dump(fp, rkb, locks);
	}

        fprintf(fp, " cgrp:\n");
        if (rk->rk_cgrp) {
                rd_kafka_cgrp_t *rkcg = rk->rk_cgrp;
                fprintf(fp, "  %.*s in state %s, flags 0x%x\n",
                        RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                        rd_kafka_cgrp_state_names[rkcg->rkcg_state],
                        rkcg->rkcg_flags);
                fprintf(fp, "   coord_id %"PRId32", managing broker %s\n",
                        rkcg->rkcg_coord_id,
                        rkcg->rkcg_rkb ?
                        rd_kafka_broker_name(rkcg->rkcg_rkb) : "(none)");

                fprintf(fp, "  toppars:\n");
                RD_LIST_FOREACH(s_rktp, &rkcg->rkcg_toppars, i) {
                        rktp = rd_kafka_toppar_s2i(s_rktp);
                        fprintf(fp, "   %.*s [%"PRId32"] in state %s\n",
                                RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                rktp->rktp_partition,
                                rd_kafka_fetch_states[rktp->rktp_fetch_state]);
                }
        }

	fprintf(fp, " topics:\n");
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		fprintf(fp, "  %.*s with %"PRId32" partitions, state %s, "
                        "refcnt %i\n",
			RD_KAFKAP_STR_PR(rkt->rkt_topic),
			rkt->rkt_partition_cnt,
                        rd_kafka_topic_state_names[rkt->rkt_state],
                        rd_refcnt_get(&rkt->rkt_refcnt));
		if (rkt->rkt_ua)
			rd_kafka_toppar_dump(fp, "   ",
                                             rd_kafka_toppar_s2i(rkt->rkt_ua));
                if (rd_list_empty(&rkt->rkt_desp)) {
                        fprintf(fp, "   desired partitions:");
                        RD_LIST_FOREACH(s_rktp, &rkt->rkt_desp,  i)
                                fprintf(fp, " %"PRId32,
                                        rd_kafka_toppar_s2i(s_rktp)->
                                        rktp_partition);
                        fprintf(fp, "\n");
                }
	}
        if (locks)
                rd_kafka_rdunlock(rk);
}

void rd_kafka_dump (FILE *fp, rd_kafka_t *rk) {

        if (rk)
                rd_kafka_dump0(fp, rk, 1/*locks*/);

#if ENABLE_SHAREDPTR_DEBUG
        rd_shared_ptrs_dump();
#endif
}



const char *rd_kafka_name (const rd_kafka_t *rk) {
	return rk->rk_name;
}


char *rd_kafka_memberid (const rd_kafka_t *rk) {
	rd_kafka_op_t *rko;
	rd_kafka_cgrp_t *rkcg;
	char *memberid;

	if (!(rkcg = rd_kafka_cgrp_get(rk)))
		return NULL;

	rko = rd_kafka_op_req2(&rkcg->rkcg_ops, RD_KAFKA_OP_NAME);
	if (!rko)
		return NULL;
	memberid = rko->rko_payload;
	rko->rko_payload = NULL;
	rd_kafka_op_destroy(rko);

	return memberid;
}


void *rd_kafka_opaque (const rd_kafka_t *rk) {
        return rk->rk_conf.opaque;
}


int rd_kafka_outq_len (rd_kafka_t *rk) {
	return rd_atomic32_get(&rk->rk_producer.msg_cnt) +
                rd_kafka_q_len(&rk->rk_rep);
}


int rd_kafka_version (void) {
	return RD_KAFKA_VERSION;
}

const char *rd_kafka_version_str (void) {
	static char ret[64];
	int ver = rd_kafka_version();

	if (!*ret) {
		int prel = (ver & 0xff);
		rd_snprintf(ret, sizeof(ret), "%i.%i.%i",
			    (ver >> 24) & 0xff,
			    (ver >> 16) & 0xff,
			    (ver >> 8) & 0xff);
		if (prel != 0xff) {
			/* pre-builds below 200 are just running numbers,
			 * abouve 200 are RC numbers. */
			if (prel <= 200)
				rd_snprintf(ret+strlen(ret),
					    sizeof(ret)-strlen(ret),
					    "-pre%d", prel);
			else
				rd_snprintf(ret+strlen(ret),
					    sizeof(ret)-strlen(ret),
					    "-RC%d", prel - 200);
		}
	}


	return ret;
}


/**
 * Assert trampoline to print some debugging information on crash.
 */
void
RD_NORETURN
rd_kafka_crash (const char *file, int line, const char *function,
                rd_kafka_t *rk, const char *reason) {
        fprintf(stderr, "*** %s:%i:%s: %s ***\n",
                file, line, function, reason);
        if (rk)
                rd_kafka_dump0(stderr, rk, 0/*no locks*/);
        abort();
}


rd_kafka_resp_err_t
rd_kafka_metadata (rd_kafka_t *rk, int all_topics,
                   rd_kafka_topic_t *only_rkt,
                   const struct rd_kafka_metadata **metadatap,
                   int timeout_ms) {
        rd_kafka_q_t *replyq;
        rd_kafka_broker_t *rkb;
        rd_kafka_op_t *rko;

        /* Query any broker that is up, and if none are up pick the first one,
         * if we're lucky it will be up before the timeout */
        rd_kafka_rdlock(rk);
        if (!(rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP,
                                        rd_kafka_broker_filter_non_blocking,
                                        NULL))) {
                rkb = TAILQ_FIRST(&rk->rk_brokers);
                if (rkb)
                        rd_kafka_broker_keep(rkb);
        }
        rd_kafka_rdunlock(rk);

        if (!rkb)
                return RD_KAFKA_RESP_ERR__TRANSPORT;

        replyq = rd_kafka_q_new(rk);

        /* Async: request metadata */
        rd_kafka_broker_metadata_req(rkb, all_topics,
                                     only_rkt ?
                                     rd_kafka_topic_a2i(only_rkt) : NULL,
                                     replyq,
                                     "application requested");

        rd_kafka_broker_destroy(rkb);

        /* Wait for reply (or timeout) */
        rko = rd_kafka_q_pop(replyq, timeout_ms, 0);

        rd_kafka_q_destroy(replyq);

        /* Timeout */
        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        /* Error */
        if (rko->rko_err) {
                rd_kafka_resp_err_t err = rko->rko_err;
                rd_kafka_op_destroy(rko);
                return err;
        }

        /* Reply: pass metadata pointer to application who now owns it*/
        rd_kafka_assert(rk, rko->rko_metadata);
        *metadatap = rko->rko_metadata;
        rko->rko_metadata = NULL;
        rd_kafka_op_destroy(rko);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

void rd_kafka_metadata_destroy (const struct rd_kafka_metadata *metadata) {
        rd_free((void *)metadata);
}




struct list_groups_state {
        rd_kafka_q_t *q;
        rd_kafka_resp_err_t err;
        int wait_cnt;
        const char *desired_group;
        struct rd_kafka_group_list *grplist;
        int grplist_size;
};

static void rd_kafka_DescribeGroups_resp_cb (rd_kafka_t *rk,
					     rd_kafka_broker_t *rkb,
                                             rd_kafka_resp_err_t err,
                                             rd_kafka_buf_t *reply,
                                             rd_kafka_buf_t *request,
                                             void *opaque) {
        struct list_groups_state *state = opaque;
        const int log_decode_errors = 1;
        int cnt;

        state->wait_cnt--;

        if (err)
                goto err;

        rd_kafka_buf_read_i32(reply, &cnt);

        while (cnt-- > 0) {
                int16_t ErrorCode;
                rd_kafkap_str_t Group, GroupState, ProtoType, Proto;
                int MemberCnt;
                struct rd_kafka_group_info *gi;

                if (state->grplist->group_cnt == state->grplist_size) {
                        /* Grow group array */
                        state->grplist_size *= 2;
                        state->grplist->groups =
                                rd_realloc(state->grplist->groups,
                                           state->grplist_size *
                                           sizeof(*state->grplist->groups));
                }

                gi = &state->grplist->groups[state->grplist->group_cnt++];
                memset(gi, 0, sizeof(*gi));

                rd_kafka_buf_read_i16(reply, &ErrorCode);
                rd_kafka_buf_read_str(reply, &Group);
                rd_kafka_buf_read_str(reply, &GroupState);
                rd_kafka_buf_read_str(reply, &ProtoType);
                rd_kafka_buf_read_str(reply, &Proto);
                rd_kafka_buf_read_i32(reply, &MemberCnt);

                if (MemberCnt > 100000) {
                        err = RD_KAFKA_RESP_ERR__BAD_MSG;
                        goto err;
                }

                rd_kafka_broker_lock(rkb);
                gi->broker.id = rkb->rkb_nodeid;
                gi->broker.host = rd_strdup(rkb->rkb_origname);
                gi->broker.port = rkb->rkb_port;
                rd_kafka_broker_unlock(rkb);

                gi->err = ErrorCode;
                gi->group = RD_KAFKAP_STR_DUP(&Group);
                gi->state = RD_KAFKAP_STR_DUP(&GroupState);
                gi->protocol_type = RD_KAFKAP_STR_DUP(&ProtoType);
                gi->protocol = RD_KAFKAP_STR_DUP(&Proto);

                gi->members = rd_malloc(MemberCnt * sizeof(*gi->members));

                while (MemberCnt-- > 0) {
                        rd_kafkap_str_t MemberId, ClientId, ClientHost;
                        rd_kafkap_bytes_t Meta, Assignment;
                        struct rd_kafka_group_member_info *mi;

                        mi = &gi->members[gi->member_cnt++];
                        memset(mi, 0, sizeof(*mi));

                        rd_kafka_buf_read_str(reply, &MemberId);
                        rd_kafka_buf_read_str(reply, &ClientId);
                        rd_kafka_buf_read_str(reply, &ClientHost);
                        rd_kafka_buf_read_bytes(reply, &Meta);
                        rd_kafka_buf_read_bytes(reply, &Assignment);

                        mi->member_id = RD_KAFKAP_STR_DUP(&MemberId);
                        mi->client_id = RD_KAFKAP_STR_DUP(&ClientId);
                        mi->client_host = RD_KAFKAP_STR_DUP(&ClientHost);

                        if (RD_KAFKAP_BYTES_IS_NULL(&Meta)) {
                                mi->member_metadata_size = 0;
                                mi->member_metadata = NULL;
                        } else {
                                mi->member_metadata_size =
                                        RD_KAFKAP_BYTES_LEN(&Meta);
                                mi->member_metadata =
                                        rd_memdup(Meta.data,
                                                  mi->member_metadata_size);
                        }

                        if (RD_KAFKAP_BYTES_IS_NULL(&Assignment)) {
                                mi->member_assignment_size = 0;
                                mi->member_assignment = NULL;
                        } else {
                                mi->member_assignment_size =
                                        RD_KAFKAP_BYTES_LEN(&Assignment);
                                mi->member_assignment =
                                        rd_memdup(Assignment.data,
                                                  mi->member_assignment_size);
                        }
                }
        }

err:
        state->err = err;
}

static void rd_kafka_ListGroups_resp_cb (rd_kafka_t *rk,
					 rd_kafka_broker_t *rkb,
                                         rd_kafka_resp_err_t err,
                                         rd_kafka_buf_t *reply,
                                         rd_kafka_buf_t *request,
                                         void *opaque) {
        struct list_groups_state *state = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode;
        char **grps;
        int cnt, grpcnt, i = 0;

        state->wait_cnt--;

        if (err)
                goto err;

        rd_kafka_buf_read_i16(reply, &ErrorCode);
        if (ErrorCode) {
                err = ErrorCode;
                goto err;
        }

        rd_kafka_buf_read_i32(reply, &cnt);

        if (state->desired_group)
                grpcnt = 1;
        else
                grpcnt = cnt;

        if (cnt == 0 || grpcnt == 0)
                return;

        grps = rd_malloc(sizeof(*grps) * grpcnt);

        while (cnt-- > 0) {
                rd_kafkap_str_t grp, proto;

                rd_kafka_buf_read_str(reply, &grp);
                rd_kafka_buf_read_str(reply, &proto);

                if (state->desired_group &&
                    rd_kafkap_str_cmp_str(&grp, state->desired_group))
                        continue;

                grps[i++] = RD_KAFKAP_STR_DUP(&grp);

                if (i == grpcnt)
                        break;
        }

        if (i > 0) {
                state->wait_cnt++;
                rd_kafka_DescribeGroupsRequest(rkb,
                                               (const char **)grps, i,
                                               state->q,
                                               rd_kafka_DescribeGroups_resp_cb,
                                               state);

                while (i-- > 0)
                        rd_free(grps[i]);
        }


        rd_free(grps);

err:
        state->err = err;
}

rd_kafka_resp_err_t
rd_kafka_list_groups (rd_kafka_t *rk, const char *group,
                      const struct rd_kafka_group_list **grplistp,
                      int timeout_ms) {
        rd_kafka_broker_t *rkb;
        int rkb_cnt = 0;
        struct list_groups_state state = RD_ZERO_INIT;
        rd_ts_t tmout;

        tmout = rd_clock() + (timeout_ms * 1000);

        /* Wait until metadata has been fetched from cluster so
         * that we have a full broker list. */
        rd_kafka_rdlock(rk);
        while (!rk->rk_ts_metadata) {
                rd_kafka_rdunlock(rk);

                if (rd_clock() > tmout)
                        return RD_KAFKA_RESP_ERR__TIMED_OUT;

                rd_usleep(5*1000, &rk->rk_terminate);
                rd_kafka_rdlock(rk);
        }

        state.q = rd_kafka_q_new(rk);
        state.desired_group = group;
        state.grplist = rd_calloc(1, sizeof(*state.grplist));
        state.grplist_size = group ? 1 : 32;

        state.grplist->groups = rd_malloc(state.grplist_size *
                                          sizeof(*state.grplist->groups));

        /* Query each broker for its list of groups */
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_kafka_broker_lock(rkb);
                if (rkb->rkb_nodeid == -1) {
                        rd_kafka_broker_unlock(rkb);
                        continue;
                }

                state.wait_cnt++;
                rd_kafka_ListGroupsRequest(rkb,
                                           state.q, rd_kafka_ListGroups_resp_cb,
                                           &state);

                rkb_cnt++;

                rd_kafka_broker_unlock(rkb);

        }
        rd_kafka_rdunlock(rk);

        if (rkb_cnt == 0) {
                state.err = RD_KAFKA_RESP_ERR__TRANSPORT;

        } else {
                while (state.wait_cnt > 0)
                        rd_kafka_q_serve(state.q, 100, 0, _Q_CB_GLOBAL,
                                         rd_kafka_poll_cb, NULL);
        }

	rd_dassert(state.q->rkq_refcnt == 1);
        rd_kafka_q_destroy(state.q);

        if (state.err)
                rd_kafka_group_list_destroy(state.grplist);
        else
                *grplistp = state.grplist;

        return state.err;
}


void rd_kafka_group_list_destroy (const struct rd_kafka_group_list *grplist0) {
        struct rd_kafka_group_list *grplist =
                (struct rd_kafka_group_list *)grplist0;

        while (grplist->group_cnt-- > 0) {
                struct rd_kafka_group_info *gi;
                gi = &grplist->groups[grplist->group_cnt];

                if (gi->broker.host)
                        rd_free(gi->broker.host);
                if (gi->group)
                        rd_free(gi->group);
                if (gi->state)
                        rd_free(gi->state);
                if (gi->protocol_type)
                        rd_free(gi->protocol_type);
                if (gi->protocol)
                        rd_free(gi->protocol);

                while (gi->member_cnt-- > 0) {
                        struct rd_kafka_group_member_info *mi;
                        mi = &gi->members[gi->member_cnt];

                        if (mi->member_id)
                                rd_free(mi->member_id);
                        if (mi->client_id)
                                rd_free(mi->client_id);
                        if (mi->client_host)
                                rd_free(mi->client_host);
                        if (mi->member_metadata)
                                rd_free(mi->member_metadata);
                        if (mi->member_assignment)
                                rd_free(mi->member_assignment);
                }

                if (gi->members)
                        rd_free(gi->members);
        }

        if (grplist->groups)
                rd_free(grplist->groups);

        rd_free(grplist);
}



const char *rd_kafka_get_debug_contexts(void) {
	return RD_KAFKA_DEBUG_CONTEXTS;
}


int rd_kafka_path_is_dir (const char *path) {
#ifdef _MSC_VER
	struct _stat st;
	return (_stat(path, &st) == 0 && st.st_mode & S_IFDIR);
#else
	struct stat st;
	return (stat(path, &st) == 0 && S_ISDIR(st.st_mode));
#endif
}


void rd_kafka_mem_free (rd_kafka_t *rk, void *ptr) {
        free(ptr);
}


int rd_kafka_errno (void) {
        return errno;
}


#if ENABLE_SHAREDPTR_DEBUG
struct rd_shptr0_head rd_shared_ptr_debug_list;
mtx_t rd_shared_ptr_debug_mtx;

void rd_shared_ptrs_dump (void) {
        rd_shptr0_t *sptr;

        printf("################ Current shared pointers ################\n");
        printf("### op_cnt: %d\n", rd_atomic32_get(&rd_kafka_op_cnt));
        mtx_lock(&rd_shared_ptr_debug_mtx);
        LIST_FOREACH(sptr, &rd_shared_ptr_debug_list, link)
                printf("# shptr ((%s*)%p): object %p refcnt %d: at %s:%d\n",
                       sptr->typename, sptr, sptr->obj,
                       rd_refcnt_get(sptr->ref), sptr->func, sptr->line);
        mtx_unlock(&rd_shared_ptr_debug_mtx);
        printf("#########################################################\n");
}
#endif
