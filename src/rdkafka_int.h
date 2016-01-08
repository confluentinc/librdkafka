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

#pragma once


#ifndef _MSC_VER
#define _GNU_SOURCE  /* for strndup() */
#include <syslog.h>
#else
typedef int mode_t;
#endif
#include <fcntl.h>


#include "rdsysqueue.h"

#include "rdkafka.h"
#include "rd.h"
#include "rdlog.h"
#include "rdtime.h"
#include "rdaddr.h"
#include "rdinterval.h"
#include "rdavg.h"

#define RD_POLL_INFINITE  -1
#define RD_POLL_NOWAIT     0

#if WITH_SSL
#include <openssl/ssl.h>
#endif




typedef struct rd_kafka_itopic_s rd_kafka_itopic_t;
typedef struct rd_ikafka_s rd_ikafka_t;


#define rd_kafka_assert(rk, cond) do {                                  \
                if (unlikely(!(cond)))                                  \
                        rd_kafka_crash(__FILE__,__LINE__, __FUNCTION__, \
                                       (rk), "assert: " # cond);        \
        } while (0)

void
RD_NORETURN
rd_kafka_crash (const char *file, int line, const char *function,
                rd_kafka_t *rk, const char *reason);


/* Forward declarations */
struct rd_kafka_s;
struct rd_kafka_itopic_s;
struct rd_kafka_msg_s;
struct rd_kafka_broker_s;

typedef RD_SHARED_PTR_TYPE(, struct rd_kafka_toppar_s) shptr_rd_kafka_toppar_t;
typedef RD_SHARED_PTR_TYPE(, struct rd_kafka_itopic_s) shptr_rd_kafka_itopic_t;



#include "rdkafka_op.h"
#include "rdkafka_queue.h"
#include "rdkafka_msg.h"
#include "rdkafka_proto.h"
#include "rdkafka_buf.h"
#include "rdkafka_pattern.h"
#include "rdkafka_conf.h"
#include "rdkafka_transport.h"
#include "rdkafka_timer.h"
#include "rdkafka_assignor.h"

/**
 * Protocol level sanity
 */
#define RD_KAFKAP_BROKERS_MAX     1000
#define RD_KAFKAP_TOPICS_MAX      10000
#define RD_KAFKAP_PARTITIONS_MAX  10000


#define RD_KAFKA_OFFSET_IS_LOGICAL(OFF)  ((OFF) < 0)







/**
 * Kafka handle, internal representation of the application's rd_kafka_t.
 */

typedef RD_SHARED_PTR_TYPE(shptr_rd_ikafka_s, rd_ikafka_t) shptr_rd_ikafka_t;

struct rd_kafka_s {
	rd_kafka_q_t rk_rep;   /* kafka -> application reply queue */
	rd_kafka_q_t rk_ops;   /* any -> rdkafka main thread ops */

	TAILQ_HEAD(, rd_kafka_broker_s) rk_brokers;
	rd_atomic32_t              rk_broker_cnt;
	rd_atomic32_t              rk_broker_down_cnt;
        mtx_t                      rk_internal_rkb_lock;
	rd_kafka_broker_t         *rk_internal_rkb;


	TAILQ_HEAD(, rd_kafka_itopic_s)  rk_topics;
	int              rk_topic_cnt;

        struct rd_kafka_cgrp_s *rk_cgrp;

	char             rk_name[128];
	rd_kafkap_str_t *rk_clientid;
	rd_kafka_conf_t  rk_conf;
	int              rk_flags;
	rd_atomic32_t    rk_terminate;
	rwlock_t         rk_lock;
	rd_kafka_type_t  rk_type;
	struct timeval   rk_tv_state_change;

	rd_atomic32_t    rk_last_throttle;  /* Last throttle_time_ms value
					     * from broker. */

        /* Locks: rd_kafka_*lock() */
        rd_ts_t          rk_ts_metadata;    /* Timestamp of most recent
                                             * metadata. */

        /* Simple consumer count:
         *  >0: Running in legacy / Simple Consumer mode,
         *   0: No consumers running
         *  <0: Running in High level consumer mode */
        rd_atomic32_t    rk_simple_cnt;

	const rd_kafkap_bytes_t *rk_null_bytes;

	union {
		struct {
			rd_atomic32_t msg_cnt;  /* current message count */
		} producer;
	} rk_u;
#define rk_consumer rk_u.consumer
#define rk_producer rk_u.producer

        rd_kafka_timers_t rk_timers;
	thrd_t rk_thread;
};

#define rd_kafka_wrlock(rk)    rwlock_wrlock(&(rk)->rk_lock)
#define rd_kafka_rdlock(rk)    rwlock_rdlock(&(rk)->rk_lock)
#define rd_kafka_rdunlock(rk)    rwlock_rdunlock(&(rk)->rk_lock)
#define rd_kafka_wrunlock(rk)    rwlock_wrunlock(&(rk)->rk_lock)


void rd_kafka_destroy_final (rd_kafka_t *rk);


/**
 * Returns true if 'rk' handle is terminating.
 */
#define rd_kafka_terminating(rk) (rd_atomic32_get(&(rk)->rk_terminate))

#define rd_kafka_is_simple_consumer(rk) \
        (rd_atomic32_get(&(rk)->rk_simple_cnt) > 0)
int rd_kafka_simple_consumer_add (rd_kafka_t *rk);


#include "rdkafka_topic.h"
#include "rdkafka_partition.h"














/**
 * Debug contexts
 */
#define RD_KAFKA_DBG_GENERIC    0x1
#define RD_KAFKA_DBG_BROKER     0x2
#define RD_KAFKA_DBG_TOPIC      0x4
#define RD_KAFKA_DBG_METADATA   0x8
#define RD_KAFKA_DBG_PRODUCER   0x10
#define RD_KAFKA_DBG_QUEUE      0x20
#define RD_KAFKA_DBG_MSG        0x40
#define RD_KAFKA_DBG_PROTOCOL   0x80
#define RD_KAFKA_DBG_CGRP       0x100
#define RD_KAFKA_DBG_SECURITY   0x200
#define RD_KAFKA_DBG_FETCH      0x400
#define RD_KAFKA_DBG_ALL        0xfff


void rd_kafka_log_buf (const rd_kafka_t *rk, int level,
		       const char *fac, const char *buf);
void rd_kafka_log0(const rd_kafka_t *rk, const char *extra, int level,
	const char *fac, const char *fmt, ...)	RD_FORMAT(printf, 5, 6);

#define rd_kafka_log(rk,level,fac,...) rd_kafka_log0(rk,NULL,level,fac,__VA_ARGS__)
#define rd_kafka_dbg(rk,ctx,fac,...) do {				  \
		if (unlikely((rk)->rk_conf.debug & (RD_KAFKA_DBG_ ## ctx))) \
			rd_kafka_log0(rk,NULL,LOG_DEBUG,fac,__VA_ARGS__); \
	} while (0)

#define rd_rkb_log(rkb,level,fac,...)					\
	rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name, level, fac, __VA_ARGS__)

#define rd_rkb_dbg(rkb,ctx,fac,...) do {				\
		if (unlikely((rkb)->rkb_rk->rk_conf.debug &		\
			     (RD_KAFKA_DBG_ ## ctx)))			\
			rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name,	\
				      LOG_DEBUG, fac, __VA_ARGS__);		\
	} while (0)






extern rd_atomic32_t rd_kafka_thread_cnt_curr;

extern char RD_TLS rd_kafka_thread_name[64];





int rd_kafka_path_is_dir (const char *path);

int rd_kafka_poll_cb (rd_kafka_t *rk, rd_kafka_op_t *rko,
                      int cb_type, void *opaque);

rd_kafka_resp_err_t rd_kafka_subscribe_rkt (rd_kafka_itopic_t *rkt);

