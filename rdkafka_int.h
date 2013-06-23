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

#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/queue.h>
#include <syslog.h>
#include <poll.h>

#include "rdkafka.h"
#include "rd.h"
#include "rdaddr.h"
#include "rdlog.h"


#define RD_POLL_INFINITE  -1
#define RD_POLL_NOWAIT     0


#include "rdkafka_proto.h"



/**
 * Protocol level sanity
 */
#define RD_KAFKAP_BROKERS_MAX     1000
#define RD_KAFKAP_TOPICS_MAX      10000
#define RD_KAFKAP_PARTITIONS_MAX  1000





struct rd_kafka_s;
struct rd_kafka_toppar_s;
struct rd_kafka_topic_s;
struct rd_kafka_msg_s;
struct rd_kafka_broker_s;


typedef struct rd_kafka_msg_s {
	TAILQ_ENTRY(rd_kafka_msg_s)  rkm_link;
	int        rkm_flags;
	size_t     rkm_len;
	void      *rkm_payload;
	void      *rkm_opaque;
	int32_t    rkm_partition;  /* partition specified */
	rd_kafkap_bytes_t *rkm_key;
	rd_ts_t    rkm_ts_timeout;
} rd_kafka_msg_t;

typedef struct rd_kafka_msgq_s {
	TAILQ_HEAD(, rd_kafka_msg_s) rkmq_msgs;
	int        rkmq_msg_cnt;
	uint64_t   rkmq_msg_bytes;
} rd_kafka_msgq_t;

#define RD_KAFKA_MSGQ_INITIALIZER(rkmq) \
	{ rkmq_msgs: TAILQ_HEAD_INITIALIZER((rkmq).rkmq_msgs) }

#define RD_KAFKA_MSGQ_FOREACH(elm,head) \
	TAILQ_FOREACH(elm, &(head)->rkmq_msgs, rkm_link)


typedef enum {
	RD_KAFKA_OP_PRODUCE,  /* Application  -> Kafka thread */
	RD_KAFKA_OP_FETCH,    /* Kafka thread -> Application */
	RD_KAFKA_OP_ERR,      /* Kafka thread -> Application */
	RD_KAFKA_OP_DR,       /* Kafka thread -> Application
			       * Produce message delivery report */

	/* Internal librdkafka ops */
	RD_KAFKA_OP_REPLY,    /* Kafka thread: reply received from broker */
} rd_kafka_op_type_t;

typedef struct rd_kafka_op_s {
	TAILQ_ENTRY(rd_kafka_op_s) rko_link;
	rd_kafka_op_type_t rko_type;
	struct rd_kafka_topic_s *rko_rkt;
	int       rko_flags;
#define RD_KAFKA_OP_F_FREE        0x1  /* Free the payload when done with it. */
#define RD_KAFKA_OP_F_FLASH       0x2  /* Internal: insert at head of queue */
#define RD_KAFKA_OP_F_NOCOPY      0x4  /* Do not copy the payload, point to it*/
#define RD_KAFKA_OP_F_NO_RESPONSE 0x8  /* rkbuf: Not expecting a response */
	rd_kafka_msgq_t rko_msgq;

	/* For PRODUCE */
	rd_kafka_msg_t *rko_rkm;
	/* For ERR */
	char *rko_payload;
	int   rko_len;
	/* For FETCH */
	uint64_t  rko_offset;
#define           rko_max_size rko_len
	/* For replies */
	rd_kafka_resp_err_t rko_err;
	int8_t    rko_compression;
	int64_t   rko_offset_len;  /* Length to use to advance the offset. */
} rd_kafka_op_t;


typedef struct rd_kafka_q_s {
	pthread_mutex_t rkq_lock;
	pthread_cond_t  rkq_cond;
	TAILQ_HEAD(, rd_kafka_op_s) rkq_q;
	int             rkq_qlen;
} rd_kafka_q_t;



typedef struct rd_kafka_buf_s {
	TAILQ_ENTRY(rd_kafka_buf_s) rkbuf_link;

	int32_t rkbuf_corrid;
	
	rd_ts_t rkbuf_ts_retry;    /* Absolute send retry time */

	int     rkbuf_flags; /* RD_KAFKA_OP_F */
	struct msghdr rkbuf_msg;
	struct iovec *rkbuf_iov;
	int           rkbuf_iovcnt;
	size_t        rkbuf_iovof; /* byte offset for beginning of current iov*/
	size_t  rkbuf_of;          /* recv/send: byte offset */
	size_t  rkbuf_len;         /* send: total length */
	size_t  rkbuf_size;        /* allocated size */

	char   *rkbuf_buf;
	char   *rkbuf_buf2;
	struct rd_kafkap_reqhdr rkbuf_reqhdr;
	struct rd_kafkap_reshdr rkbuf_reshdr;

	int32_t rkbuf_expected_size;  /* expected size of message */

	/* Response callback */
	void  (*rkbuf_cb) (struct rd_kafka_broker_s *,
			   int err,
			   struct rd_kafka_buf_s *reprkbuf,
			   struct rd_kafka_buf_s *reqrkbuf,
			   void *opaque);
	
	void   *rkbuf_opaque;

	int     rkbuf_retries;

	rd_ts_t rkbuf_ts_enq;
	rd_ts_t rkbuf_ts_sent;
	rd_ts_t rkbuf_ts_timeout;

	rd_kafka_msgq_t rkbuf_msgq;
} rd_kafka_buf_t;


typedef enum {
	RD_KAFKA_CONFIGURED,
	RD_KAFKA_LEARNED,
} rd_kafka_confsource_t;
	

typedef struct rd_kafka_broker_s {
	TAILQ_ENTRY(rd_kafka_broker_s) rkb_link;

	int32_t             rkb_nodeid;
#define RD_KAFKA_NODEID_UA -1

	rd_sockaddr_list_t *rkb_rsal;
	time_t              rkb_t_rsal_last;
	int                 rkb_s; /* TCP socket */

	struct pollfd       rkb_pfd;

	uint32_t            rkb_corrid;

	rd_kafka_q_t        rkb_ops;

	TAILQ_HEAD(, rd_kafka_toppar_s) rkb_toppars;
	pthread_rwlock_t    rkb_toppar_lock;
	int                 rkb_toppar_cnt;

#define rd_kafka_broker_toppars_rdlock(rkb) \
	pthread_rwlock_rdlock(&(rkb)->rkb_toppar_lock)
#define rd_kafka_broker_toppars_wrlock(rkb) \
	pthread_rwlock_wrlock(&(rkb)->rkb_toppar_lock)
#define rd_kafka_broker_toppars_unlock(rkb) \
	pthread_rwlock_unlock(&(rkb)->rkb_toppar_lock)

	enum {
		RD_KAFKA_BROKER_STATE_DOWN,
		RD_KAFKA_BROKER_STATE_CONNECTING,
		RD_KAFKA_BROKER_STATE_UP,
	} rkb_state;

	rd_kafka_confsource_t  rkb_source;
	struct {
		uint64_t tx_bytes;
		uint64_t tx;    /* Kafka-messages (not payload msgs) */
		uint64_t tx_err;
		uint64_t tx_retries;

		uint64_t rx_bytes;
		uint64_t rx;    /* Kafka messages (not payload msgs) */
		uint64_t rx_err;
	} rkb_c;

	int                 rkb_terminate;
	pthread_mutex_t     rkb_lock;
	pthread_t           rkb_thread;

	int                 rkb_refcnt;

	struct rd_kafka_s  *rkb_rk;

	struct {
		char msg[512];
		int  err;  /* errno */
	} rkb_err;

	rd_kafka_buf_t     *rkb_recv_buf;

	TAILQ_HEAD(, rd_kafka_buf_s) rkb_outbufs;
	int                 rkb_outbuf_cnt;

	TAILQ_HEAD(, rd_kafka_buf_s) rkb_waitresps;
	int                 rkb_waitresp_cnt;

	TAILQ_HEAD(, rd_kafka_buf_s) rkb_retrybufs;
	int                 rkb_retrybuf_cnt;

	char                rkb_name[128];      /* Display name */
	char                rkb_nodename[128];  /* host:port */


} rd_kafka_broker_t;

#define rd_kafka_broker_keep(rkb) rd_atomic_add(&(rkb)->rkb_refcnt, 1)

#define rd_kafka_broker_lock(rkb)   pthread_mutex_lock(&(rkb)->rkb_lock)
#define rd_kafka_broker_unlock(rkb) pthread_mutex_unlock(&(rkb)->rkb_lock)


typedef struct rd_kafka_topic_s {
	TAILQ_ENTRY(rd_kafka_topic_s) rkt_link;

	pthread_rwlock_t   rkt_lock;
	rd_kafkap_str_t   *rkt_topic;
	struct rd_kafka_toppar_s  *rkt_ua;  /* unassigned partition */
	struct rd_kafka_toppar_s **rkt_p;
	int32_t            rkt_partition_cnt;

	int                rkt_refcnt;

	struct rd_kafka_s *rkt_rk;

	rd_kafka_topic_conf_t rkt_conf;
} rd_kafka_topic_t;

#define rd_kafka_topic_rdlock(rkt)     pthread_rwlock_rdlock(&(rkt)->rkt_lock)
#define rd_kafka_topic_wrlock(rkt)     pthread_rwlock_wrlock(&(rkt)->rkt_lock)
#define rd_kafka_topic_unlock(rkt)     pthread_rwlock_unlock(&(rkt)->rkt_lock)

/**
 * Topic + Partition combination
 */
typedef struct rd_kafka_toppar_s {
	TAILQ_ENTRY(rd_kafka_toppar_s) rktp_rklink;  /* rd_kafka_t link */
	TAILQ_ENTRY(rd_kafka_toppar_s) rktp_rkblink; /* rd_kafka_broker_t link*/
	rd_kafka_topic_t  *rktp_rkt;
	int32_t            rktp_partition;
	/* Partition is unassigned */
	rd_kafka_broker_t *rktp_leader;  /* Leader broker */
	int                rktp_refcnt;
	pthread_mutex_t    rktp_lock;

	rd_kafka_msgq_t    rktp_msgq;      /* application->rdkafka queue.
					    * protected by rktp_lock */
	rd_kafka_msgq_t    rktp_xmit_msgq; /* internal broker xmit queue */
	
	struct {
		uint64_t tx_msgs;
		uint64_t tx_bytes;
	} rktp_c;
} rd_kafka_toppar_t;

#define rd_kafka_toppar_keep(rktp) rd_atomic_add(&(rktp)->rktp_refcnt, 1)

#define rd_kafka_toppar_destroy(rktp) do {				\
		if (rd_atomic_sub(&(rktp)->rktp_refcnt, 1) == 0)	\
			rd_kafka_toppar_destroy0(rktp);			\
	} while (0)
#define rd_kafka_toppar_lock(rktp)     pthread_mutex_lock(&(rktp)->rktp_lock)
#define rd_kafka_toppar_unlock(rktp)   pthread_mutex_unlock(&(rktp)->rktp_lock)

static const char *rd_kafka_toppar_name (const rd_kafka_toppar_t *rktp)
	RD_UNUSED;
static const char *rd_kafka_toppar_name (const rd_kafka_toppar_t *rktp) {
	static __thread char ret[256];

	snprintf(ret, sizeof(ret), "%.*s [%"PRId32"]",
		 RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		 rktp->rktp_partition);

	return ret;
}


/**
 * Kafka handle.
 */
typedef struct rd_kafka_s {
	rd_kafka_q_t rk_op;    /* application -> kafka operation queue */
	rd_kafka_q_t rk_rep;   /* kafka -> application reply queue */

	TAILQ_HEAD(, rd_kafka_broker_s) rk_brokers;
	TAILQ_HEAD(, rd_kafka_topic_s)  rk_topics;
	int              rk_topic_cnt;

	char             rk_name[128];
	rd_kafkap_str_t *rk_clientid;
	rd_kafka_conf_t  rk_conf;
	int              rk_flags;
	int              rk_terminate;
	pthread_mutex_t  rk_lock;
	int              rk_refcnt;
	rd_kafka_type_t  rk_type;
	struct timeval   rk_tv_state_change;
	union {
		struct {
			char    *topic;
			int32_t  partition;
			uint64_t offset;
			uint64_t app_offset;
			int      offset_file_fd;
		} consumer;
		struct {
			int msg_cnt;  /* current message count */
		} producer;
	} rk_u;
#define rk_consumer rk_u.consumer
#define rk_producer rk_u.producer

	void (*rk_log_cb) (const rd_kafka_t *rk, int level,
			   const char *fac,
			   const char *buf);
	int    rk_log_level;

	struct {
		char msg[512];
		int  err;  /* errno */
	} rk_err;
} rd_kafka_t;

#define rd_kafka_lock(rk)    pthread_mutex_lock(&(rk)->rk_lock)
#define rd_kafka_unlock(rk)  pthread_mutex_unlock(&(rk)->rk_lock)








/**
 * Destroys an op as returned by rd_kafka_consume().
 *
 * Locality: any thread
 */
void        rd_kafka_op_destroy (rd_kafka_t *rk, rd_kafka_op_t *rko);














/* FIXME: NEW */

void rd_kafka_log0 (const rd_kafka_t *rk, const char *extra, int level,
		   const char *fac, const char *fmt, ...)
	__attribute__((format (printf, 5, 6)));
#define rd_kafka_log(rk,level,fac,fmt...) rd_kafka_log0(rk,NULL,level,fac,fmt)
#define rd_kafka_dbg(rk,fac,fmt...)  rd_kafka_log0(rk,NULL,LOG_DEBUG,fac,fmt)

#define rd_rkb_log(rkb,level,fac,fmt...)				\
	rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name, level, fac, fmt)

#define rd_rkb_dbg(rkb,fac,fmt...)					\
	rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name, LOG_DEBUG, fac, fmt)

rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms);


rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type);
void rd_kafka_op_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko);
void rd_kafka_op_reply0 (rd_kafka_t *rk, rd_kafka_op_t *rko,
			 rd_kafka_op_type_t type,
			 rd_kafka_resp_err_t err, uint8_t compression,
			 void *payload, int len,
			 uint64_t offset_len);
void rd_kafka_op_reply (rd_kafka_t *rk,
			rd_kafka_op_type_t type,
			rd_kafka_resp_err_t err, uint8_t compression,
			void *payload, int len,
			uint64_t offset_len);




#define RD_KAFKA_SEND_END -1
