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


/*
 * Portability
 */

/* MacOSX does not have strndupa() */
#ifndef strndupa
#define strndupa(PTR,LEN) ({ int _L = (LEN); char *_x = alloca(_L+1); \
      memcpy(_x, (PTR), _L); *(_x+_L) = 0; _x;})
#endif

#ifndef strdupa
#define strdupa(PTR) ({ const char *_P = (PTR); int _L = strlen(_P); \
      char *_x = alloca(_L+1); memcpy(_x, _P, _L); *(_x+_L) = 0; _x;})
#endif

/* Some versions of MacOSX dont have IOV_MAX */
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif





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
struct rd_kafka_conf_s;
struct rd_kafka_topic_conf_s;



/**
 * MessageSet compression codecs
 */
typedef enum {
	RD_KAFKA_COMPRESSION_NONE,
	RD_KAFKA_COMPRESSION_GZIP,   /* FIXME: not supported */
	RD_KAFKA_COMPRESSION_SNAPPY, /* FIXME: not supported */
} rd_kafka_compression_t;


/**
 * Optional configuration struct passed to rd_kafka_new*().
 *
 * The struct can also be populated through string properties
 * by calling rd_kafka_conf_set().
 *
 * The properties are identical to the Apache Kafka configuration properties
 * whenever possible.
 *
 * See rdkafka_defaultconf.c for defaults.
 * See rd_kafka_defaultconf_set() for usage.
 * See rd_kafka_conf_set().
 */
struct rd_kafka_conf_s {
	/* Property: client.id
	 *
	 * Client identifier */
	char   *clientid;

	/* Property: metadata.broker.list
	 *
	 * Initial list of brokers.
	 * The application may also use rd_kafka_brokers_add() */
	char   *brokerlist;

         /* Property: message.max.bytes
	  *
	  * Maximum receive message size.
	  * This is a safety precaution to avoid memory exhaustion in case of
	  * protocol hickups. */
	int     max_msg_size;  

	/* Property: metadata.request.timeout.ms
	 *
	 * Non-topic request timeout in milliseconds.
	 * This is for metadata requests, etc. */
	int     metadata_request_timeout_ms;


	/* Property: topic.metadata.refresh.interval.ms
	 *
	 * Topic metadata refresh interval in milliseconds.
	 * The metadata is automatically refreshed on
	 * error and connect.
	 * A negative value disables the intervalled refresh. */
	int metadata_refresh_interval_ms;

	/* Property: topic.metadata.refresh.fast.cnt
	 *
	 * Following a topic without leader error this many
	 * metadata requests are sent with
	 * 'topic.metadata.refresh.fash.interval.ms' interval disregarding
	 * the topic.metadata.refresh.interval.ms value.
	 * This is used to recover fast from transitioning leader brokers.
	 * Default value: 10 */
	int metadata_refresh_fast_cnt;

	/* Property: topic.metadata.refresh.fast.interval.ms
	 *
	 * See 'topic.metadata.refresh.fast.cnt' description.
	 * Default value: 250ms */
	int metadata_refresh_fast_interval_ms;


	/* Boolean flags: RD_KAFKA_CONF_F_... */
	int     flags;

	/* No automatic offset storage will be performed.
	 * The application needs to call rd_kafka_offset_store() explicitly.
	 * This may be used to make sure a message is properly handled
	 * before storing the offset.
	 * If not set, and an offset storage is available, the
	 * offset will be stored just prior to passing the
	 * message to the application.*/
#define RD_KAFKA_CONF_F_APP_OFFSET_STORE  0x1  


	/* How long to cache the broker address resolving results. */
	int     broker_addr_lifetime; 

	/* Error callback */
	void  (*error_cb) (rd_kafka_t *rk, int err,
			   const char *reason, void *opaque);

	/* Opaque passed to all registered callbacks. */
	void   *opaque;


	/* Property: debug
	 *
	 * A comma-separated list of debug contexts to enable.
	 * See RD_KAFKA_DEBUG_CONTEXTS for supported values.
	 */
	int     debug;


	/* Property: socket.timeout.ms
	 *
	 * Timeout for network requests. */
	int     socket_timeout_ms;

	/* Consumer configuration */
	struct {
		/* Property: queued.min.messages
		 *
		 * Minimum number of messages that should to be available
		 * for consumption by application. */
		int queued_min_msgs;

		/* Property: fetch.wait.max.ms
		 *
		 * Maximum time the broker may wait to fill the response
		 * with fetch.min.bytes. */
		int fetch_wait_max_ms;

		/* Property: fetch.min.bytes
		 *
		 * Minimum number of bytes the broker responds with.
		 * If fetch.wait.max.ms expires the accumulated data will
		 * be sent to the client regardless of this setting. */
		int fetch_min_bytes;

		/* Property: fetch.error.backoff.ms
		 *
		 * How long to postpone the next fetch request for a
		 * topic+partition in case of a fetch error. */
		int fetch_error_backoff_ms;

		/* FIXME */
		char *offset_file;    /* File to read/store current
				       * offset from/in.
				       * If the path is a directory then a
				       * filename is generated (including
				       * the topic and partition) and
				       * appended. */
		int offset_file_flags; /* open(2) flags. */
#define RD_KAFKA_OFFSET_FILE_FLAGMASK (O_SYNC|O_ASYNC)
		
		/* Internal.
		 * Pre-built Fetch request header. */
		struct rd_kafkap_FetchRequest FetchRequest;
	} consumer;


	/* Producer configuration */
	struct {
		/* Property: queue.buffering.max.messages
		 *
		 * Maximum number of messages allowed on the producer queue. */
		int max_messages;

		/* Property: queue.buffering.max.ms
		 *
		 * Maximum time, in milliseconds, for buffering data
		 * on the producer queue. */
		int buffering_max_ms;

		/* Property: message.send.max.retries
		 *
		 * How many times to retry sending a failing MessageSet.
		 * Note: retrying can cause reordering. */
		int max_retries;

		/* Property: retry.backoff.ms
		 *
		 * The backoff time in milliseconds before retrying. */
		int retry_backoff_ms;

		/* Property: compression.codec
		 *
		 * Specify compression codec for all data generated by
		 * this produced. */
		rd_kafka_compression_t compression_codec;

		/* Property: queue.enqueue.timeout.ms
		 *
		 * Message enqueue timeout:
		 *  0   Messages will be enqueued immediately or dropped 
		 *      if the queue is full.
		 * <0   Enqueue will block indefinately if the queue is full.
		 * >0   Enqueue will block up to this many milliseconds if
		 *      the queue is full. */
		/* FIXME: Not implemented */
		int enqueue_timeout_ms;

		/* Property: batch.num.messages
		 *
		 * Maximum number of messages batched in one MessageSet. */
		int batch_num_messages;

		/* Message delivery report callback.
		 * Called once for each produced message, either on
		 * succesful and acknowledged delivery to the broker in which
		 * case 'err' is 0, or if the message could not be delivered
		 * in which case 'err' is non-zero (use rd_kafka_err2str()
		 * to obtain a human-readable error reason).
		 *
		 * If the message was produced with neither RD_KAFKA_MSG_F_FREE
		 * or RD_KAFKA_MSG_F_COPY set then 'payload' is the original
		 * pointer provided to rd_kafka_produce().
		 * rdkafka will not perform any further actions on 'payload'
		 * at this point and the application may free the payload data
		 * at this point.
		 *
		 * 'opaque' is 'conf.opaque', while 'msg_opaque' is
		 * the opaque pointer provided in the rd_kafka_produce() call.
		 */
		void (*dr_cb) (rd_kafka_t *rk,
			       void *payload, size_t len,
			       rd_kafka_resp_err_t err,
			       void *opaque, void *msg_opaque);

	} producer;

};



struct rd_kafka_topic_conf_s {
	/* Property: request.required.acks
	 *
	 * This field indicates how many acknowledgements the brokers
	 * should receive before responding to the request:
	 *  0   The broker does not send any response.
	 *  1   The broker will wait until the data is written to the 
	 *      local log before sending a response.
	 * -1   The broker will block until the message is committed by all
	 *      in sync replicas before sending a response.
	 * >1   For any number > 1 the broker will block waiting for this
	 *      number of acknowledgements to occur
	 *      (but the server will never wait for more acknowledgements
	 *       than there are in-sync replicas). */
	int16_t required_acks;

	/* Property: request.timeout.ms
	 *
	 * The ack timeout of the producer request in milliseconds.
	 * This value is only enforced by the broker and relies
	 * on required_acks being > 0. */
	int32_t request_timeout_ms;

	/* Property: message.timeout.ms
	 *
	 * Local message timeout.
	 * This value is only enforced locally and limits the time a
	 * produced message waits for succesful delivery. */
	int     message_timeout_ms;

	/* Application provided message partitioner.
	 * The partitioner may be called in any thread at any time,
	 * it may be called multiple times for the same key.
	 * Partitioner function constraints:
	 *    - MUST NOT call any rd_kafka_*() functions
	 *    - MUST NOT block or execute for prolonged periods of time.
	 *    - MUST return a value between 0 and partition_cnt-1, or the
	 *      special RD_KAFKA_PARTITION_UA value if partitioning
	 *      could not be performed.
	 */

	int32_t (*partitioner) (const void *keydata,
				size_t keylen,
				int32_t partition_cnt,
				void *rkt_opaque,
				void *msg_opaque);

	/* Application provided opaque pointer (this is rkt_opaque) */
	void   *opaque;
};



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


typedef struct rd_kafka_buf_s {
	TAILQ_ENTRY(rd_kafka_buf_s) rkbuf_link;

	int32_t rkbuf_corrid;
	
	rd_ts_t rkbuf_ts_retry;    /* Absolute send retry time */

	int     rkbuf_flags; /* RD_KAFKA_OP_F */
	struct msghdr rkbuf_msg;
	struct iovec *rkbuf_iov;
	int           rkbuf_iovcnt;
	size_t  rkbuf_of;          /* recv/send: byte offset */
	size_t  rkbuf_len;         /* send: total length */
	size_t  rkbuf_size;        /* allocated size */

	char   *rkbuf_buf;         /* Main buffer */
	char   *rkbuf_buf2;        /* Aux buffer */
	struct rd_kafkap_reqhdr rkbuf_reqhdr;
	struct rd_kafkap_reshdr rkbuf_reshdr;

	int32_t rkbuf_expected_size;  /* expected size of message */

	/* Response callback */
	void  (*rkbuf_cb) (struct rd_kafka_broker_s *,
			   int err,
			   struct rd_kafka_buf_s *reprkbuf,
			   struct rd_kafka_buf_s *reqrkbuf,
			   void *opaque);

	int     rkbuf_refcnt;
	void   *rkbuf_opaque;

	int     rkbuf_retries;

	rd_ts_t rkbuf_ts_enq;
	rd_ts_t rkbuf_ts_sent;
	rd_ts_t rkbuf_ts_timeout;

	rd_kafka_msgq_t rkbuf_msgq;
} rd_kafka_buf_t;


typedef struct rd_kafka_bufq_s {
	TAILQ_HEAD(, rd_kafka_buf_s) rkbq_bufs;
	int                          rkbq_cnt;
} rd_kafka_bufq_t;


typedef enum {
	RD_KAFKA_OP_FETCH,    /* Kafka thread -> Application */
	RD_KAFKA_OP_ERR,      /* Kafka thread -> Application */
	RD_KAFKA_OP_DR,       /* Kafka thread -> Application
			       * Produce message delivery report */

	RD_KAFKA_OP_METADATA_REQ, /* any -> Broker thread: request metadata */
} rd_kafka_op_type_t;

typedef struct rd_kafka_op_s {
	TAILQ_ENTRY(rd_kafka_op_s) rko_link;
	rd_kafka_op_type_t rko_type;
	int       rko_flags;
#define RD_KAFKA_OP_F_FREE        0x1  /* Free the payload when done with it. */
#define RD_KAFKA_OP_F_FLASH       0x2  /* Internal: insert at head of queue */
#define RD_KAFKA_OP_F_NOCOPY      0x4  /* Do not copy the payload, point to it*/
#define RD_KAFKA_OP_F_NO_RESPONSE 0x8  /* rkbuf: Not expecting a response */
	rd_kafka_msgq_t rko_msgq;

	/* For PRODUCE */
	rd_kafka_msg_t *rko_rkm;

	/* For FETCH and ERR */
	char *rko_payload;
	int   rko_len;
	struct rd_kafka_toppar_s *rko_rktp;

	/* For FETCH */
	rd_kafka_buf_t    *rko_rkbuf;
	rd_kafkap_bytes_t *rko_key;
	uint64_t           rko_next_offset;

	/* For METADATA */
	struct rd_kafka_topic_s *rko_rkt;

	/* For replies */
	rd_kafka_resp_err_t rko_err;
} rd_kafka_op_t;


typedef struct rd_kafka_q_s {
	pthread_mutex_t rkq_lock;
	pthread_cond_t  rkq_cond;
	TAILQ_HEAD(, rd_kafka_op_s) rkq_q;
	int             rkq_qlen;
} rd_kafka_q_t;





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

	rd_ts_t             rkb_ts_fetch_backoff;
	int                 rkb_fetching;

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

	rd_ts_t             rkb_ts_metadata_poll; /* Next metadata poll time */
	int                 rkb_metadata_fast_poll_cnt; /* Perform fast
							 * metadata polls. */
	pthread_mutex_t     rkb_lock;
	pthread_t           rkb_thread;

	int                 rkb_refcnt;

	struct rd_kafka_s  *rkb_rk;

	struct {
		char msg[512];
		int  err;  /* errno */
	} rkb_err;

	rd_kafka_buf_t     *rkb_recv_buf;

	rd_kafka_bufq_t     rkb_outbufs;
	rd_kafka_bufq_t     rkb_waitresps;
	rd_kafka_bufq_t     rkb_retrybufs;

	char                rkb_name[128];      /* Display name */
	char                rkb_nodename[128];  /* host:port */


} rd_kafka_broker_t;

#define rd_kafka_broker_keep(rkb) rd_atomic_add(&(rkb)->rkb_refcnt, 1)
#define rd_kafka_broker_lock(rkb)   pthread_mutex_lock(&(rkb)->rkb_lock)
#define rd_kafka_broker_unlock(rkb) pthread_mutex_unlock(&(rkb)->rkb_lock)

/* rd_kafka_topic_t */
struct rd_kafka_topic_s {
	TAILQ_ENTRY(rd_kafka_topic_s) rkt_link;

	pthread_rwlock_t   rkt_lock;
	rd_kafkap_str_t   *rkt_topic;
	struct rd_kafka_toppar_s  *rkt_ua;  /* unassigned partition */
	struct rd_kafka_toppar_s **rkt_p;
	int32_t            rkt_partition_cnt;

	int                rkt_refcnt;

	struct rd_kafka_s *rkt_rk;

	rd_kafka_topic_conf_t rkt_conf;
};

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
	rd_kafka_broker_t *rktp_leader;  /* Leader broker */
	int                rktp_refcnt;
	pthread_mutex_t    rktp_lock;

	rd_kafka_msgq_t    rktp_msgq;      /* application->rdkafka queue.
					    * protected by rktp_lock */
	rd_kafka_msgq_t    rktp_xmit_msgq; /* internal broker xmit queue */

	rd_ts_t            rktp_ts_last_xmit;

	int                rktp_flags;
	#define RD_KAFKA_TOPPAR_F_CONSUMING  0x1

	/* Consumer */
	rd_kafka_q_t       rktp_fetchq;          /* Queue of fetched messages
						  * from broker. */
	int64_t            rktp_next_offset;     /* Next offset to fetch */
	int64_t            rktp_app_offset;      /* Last offset delivered to
						  * application */
	int64_t            rktp_commited_offset; /* Last commited offset */
	struct {
		uint64_t tx_msgs;
		uint64_t tx_bytes;
	} rktp_c;
} rd_kafka_toppar_t;

#define rd_kafka_toppar_keep(rktp) rd_atomic_add(&(rktp)->rktp_refcnt, 1)
#define rd_kafka_toppar_destroy(rktp) do {				\
	if (rd_atomic_sub(&(rktp)->rktp_refcnt, 1) == 0)		\
		rd_kafka_toppar_destroy0(rktp);				\
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
 * Kafka handle. (rd_kafka_t)
 */
struct rd_kafka_s {
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
};

#define rd_kafka_lock(rk)    pthread_mutex_lock(&(rk)->rk_lock)
#define rd_kafka_unlock(rk)  pthread_mutex_unlock(&(rk)->rk_lock)








/**
 * Destroys an op as returned by rd_kafka_consume().
 *
 * Locality: any thread
 */
void        rd_kafka_op_destroy (rd_kafka_t *rk, rd_kafka_op_t *rko);














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
#define RD_KAFKA_DBG_ALL        0xff


void rd_kafka_log0 (const rd_kafka_t *rk, const char *extra, int level,
		   const char *fac, const char *fmt, ...)
	__attribute__((format (printf, 5, 6)));
#define rd_kafka_log(rk,level,fac,fmt...) rd_kafka_log0(rk,NULL,level,fac,fmt)
#define rd_kafka_dbg(rk,ctx,fac,fmt...) do {				  \
		if (unlikely((rk)->rk_conf.debug & RD_KAFKA_DBG_ ## ctx)) \
			rd_kafka_log0(rk,NULL,LOG_DEBUG,fac,fmt);	  \
	} while (0)

#define rd_rkb_log(rkb,level,fac,fmt...)				\
	rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name, level, fac, fmt)

#define rd_rkb_dbg(rkb,ctx,fac,fmt...) do {				\
		if (unlikely((rkb)->rkb_rk->rk_conf.debug &		\
			     RD_KAFKA_DBG_ ## ctx))			\
			rd_kafka_log0((rkb)->rkb_rk, (rkb)->rkb_name,	\
				      LOG_DEBUG, fac, fmt);		\
	} while (0)


void rd_kafka_q_init (rd_kafka_q_t *rkq);

/**
 * Enqueue the 'rko' op at the tail of the queue 'rkq'.
 *
 * Locality: any thread.
 */
static inline RD_UNUSED
void rd_kafka_q_enq (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
	pthread_mutex_lock(&rkq->rkq_lock);
	TAILQ_INSERT_TAIL(&rkq->rkq_q, rko, rko_link);
	(void)rd_atomic_add(&rkq->rkq_qlen, 1);
	pthread_cond_signal(&rkq->rkq_cond);
	pthread_mutex_unlock(&rkq->rkq_lock);
}

/**
 * Concat all elements of 'srcq' onto tail of 'rkq'.
 * 'dstq' will be be locked, but 'srcq' will not.
 *
 * Locality: any thread.
 */
static inline RD_UNUSED
void rd_kafka_q_concat (rd_kafka_q_t *rkq, rd_kafka_q_t *srcq) {
	pthread_mutex_lock(&rkq->rkq_lock);
	TAILQ_CONCAT(&rkq->rkq_q, &srcq->rkq_q, rko_link);
	(void)rd_atomic_add(&rkq->rkq_qlen, srcq->rkq_qlen);
	pthread_cond_signal(&rkq->rkq_cond);
	pthread_mutex_unlock(&rkq->rkq_lock);
}

#define rd_kafka_q_len(rkq) ((rkq)->rkq_qlen)

rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms);


rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type);
void rd_kafka_op_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko);
void rd_kafka_op_reply0 (rd_kafka_t *rk, rd_kafka_op_t *rko,
			 rd_kafka_op_type_t type,
			 rd_kafka_resp_err_t err, uint8_t compression,
			 void *payload, int len);
void rd_kafka_op_reply (rd_kafka_t *rk,
			rd_kafka_op_type_t type,
			rd_kafka_resp_err_t err, uint8_t compression,
			void *payload, int len);

#define rd_kafka_keep(rk) rd_atomic_add(&(rk)->rk_refcnt, 1)
void rd_kafka_destroy0 (rd_kafka_t *rk);

void rd_kafka_conf_destroy (rd_kafka_conf_t *conf);
void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf);

extern int rd_kafka_thread_cnt_curr;

#define RD_KAFKA_SEND_END -1
