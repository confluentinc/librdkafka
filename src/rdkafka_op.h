/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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


#include "rdkafka_msg.h"

/* Forward declarations */
typedef struct rd_kafka_q_s rd_kafka_q_t;
typedef struct rd_kafka_toppar_s rd_kafka_toppar_t;
typedef struct rd_kafka_op_s rd_kafka_op_t;

/* One-off reply queue + reply version.
 * All APIs that take a rd_kafka_replyq_t makes a copy of the
 * struct as-is and grabs hold of the existing .q refcount.
 * Think of replyq as a (Q,VERSION) tuple. */
typedef struct rd_kafka_replyq_s {
	rd_kafka_q_t *q;
	int32_t       version;
#if ENABLE_DEVEL
	char *_id; /* Devel id used for debugging reference leaks.
		    * Is a strdup() of the caller's function name,
		    * which makes for easy debugging with valgrind. */
#endif
} rd_kafka_replyq_t;




/**
 * Flags used by:
 *   - rd_kafka_op_t.rko_flags
 *   - rd_kafka_buf_t.rkbuf_flags
 */
#define RD_KAFKA_OP_F_FREE        0x1  /* rd_free payload when done with it */
#define RD_KAFKA_OP_F_FLASH       0x2  /* Internal: insert at head of queue */
#define RD_KAFKA_OP_F_NO_RESPONSE 0x4  /* rkbuf: Not expecting a response */
#define RD_KAFKA_OP_F_CRC         0x8  /* rkbuf: Perform CRC calculation */
#define RD_KAFKA_OP_F_BLOCKING    0x10 /* rkbuf: blocking protocol request */
#define RD_KAFKA_OP_F_REPROCESS   0x20 /* cgrp: Reprocess at a later time. */


typedef enum {
        RD_KAFKA_OP_NONE,     /* No specific type, use OP_CB */
	RD_KAFKA_OP_FETCH,    /* Kafka thread -> Application */
	RD_KAFKA_OP_ERR,      /* Kafka thread -> Application */
        RD_KAFKA_OP_CONSUMER_ERR, /* Kafka thread -> Application */
	RD_KAFKA_OP_DR,       /* Kafka thread -> Application
			       * Produce message delivery report */
	RD_KAFKA_OP_STATS,    /* Kafka thread -> Application */

        RD_KAFKA_OP_OFFSET_COMMIT, /* any -> toppar's Broker thread */
        RD_KAFKA_OP_NODE_UPDATE,   /* any -> Broker thread: node update */

        RD_KAFKA_OP_XMIT_BUF, /* transmit buffer: any -> broker thread */
        RD_KAFKA_OP_RECV_BUF, /* received response buffer: broker thr -> any */
        RD_KAFKA_OP_XMIT_RETRY, /* retry buffer xmit: any -> broker thread */
        RD_KAFKA_OP_FETCH_START, /* Application -> toppar's handler thread */
        RD_KAFKA_OP_FETCH_STOP,  /* Application -> toppar's handler thread */
        RD_KAFKA_OP_SEEK,        /* Application -> toppar's handler thread */
	RD_KAFKA_OP_PAUSE,       /* Application -> toppar's handler thread */
        RD_KAFKA_OP_OFFSET_FETCH, /* Broker -> broker thread: fetch offsets
                                   * for topic. */

        RD_KAFKA_OP_PARTITION_JOIN,  /* * -> cgrp op:   add toppar to cgrp
                                      * * -> broker op: add toppar to broker */
        RD_KAFKA_OP_PARTITION_LEAVE, /* * -> cgrp op:   remove toppar from cgrp
                                      * * -> broker op: remove toppar from rkb*/
        RD_KAFKA_OP_REBALANCE,       /* broker thread -> app:
                                      * group rebalance */
        RD_KAFKA_OP_TERMINATE,       /* For generic use */
        RD_KAFKA_OP_COORD_QUERY,     /* Query for coordinator */
        RD_KAFKA_OP_SUBSCRIBE,       /* New subscription */
        RD_KAFKA_OP_ASSIGN,          /* New assignment */
        RD_KAFKA_OP_GET_SUBSCRIPTION,/* Get current subscription.
				      * Reuses u.subscribe */
        RD_KAFKA_OP_GET_ASSIGNMENT,  /* Get current assignment.
				      * Reuses u.assign */
	RD_KAFKA_OP_THROTTLE,        /* Throttle info */
	RD_KAFKA_OP_NAME,            /* Request name */
	RD_KAFKA_OP_OFFSET_RESET,    /* Offset reset */
        RD_KAFKA_OP_METADATA,        /* Metadata response */
        RD_KAFKA_OP_LOG,             /* Log */
        RD_KAFKA_OP__END
} rd_kafka_op_type_t;

/* Flags used with op_type_t */
#define RD_KAFKA_OP_CB        (1 << 30)  /* Callback op. */
#define RD_KAFKA_OP_REPLY     (1 << 31)  /* Reply op. */
#define RD_KAFKA_OP_FLAGMASK  (RD_KAFKA_OP_CB | RD_KAFKA_OP_REPLY)


#define RD_KAFKA_OP_TYPE_ASSERT(rko,type) \
	rd_kafka_assert(NULL, (rko)->rko_type == (type) && # type)

struct rd_kafka_op_s {
	TAILQ_ENTRY(rd_kafka_op_s) rko_link;

	rd_kafka_op_type_t    rko_type;   /* Internal op type */
	rd_kafka_event_type_t rko_evtype;
	int                   rko_flags;  /* See RD_KAFKA_OP_F_... above */
	int32_t               rko_version;
	rd_kafka_resp_err_t   rko_err;
	int32_t               rko_len;    /* Depends on type, typically the
					   * message length. */

	shptr_rd_kafka_toppar_t *rko_rktp;

        /*
	 * Generic fields
	 */

	/* Indicates request: enqueue reply on rko_replyq.q with .version.
	 * .q is refcounted. */
	rd_kafka_replyq_t rko_replyq;

        /* Original queue's op serve callback and opaque, if any.
         * Mainly used for forwarded queues to use the original queue's
         * serve function from the forwarded position.
         * Shall return 1 if op was handled and destroyed, else 0. */
        int (*rko_serve) (rd_kafka_t *rk, rd_kafka_op_t *rko,
                          int cb_type, void *opaque);
        void *rko_serve_opaque;

	rd_kafka_t     *rko_rk;

#if ENABLE_DEVEL
        const char *rko_source;  /**< Where op was created */
#endif

        /* RD_KAFKA_OP_CB */
        void          (*rko_op_cb) (rd_kafka_t *rk, struct rd_kafka_op_s *rko);

	union {
		struct {
			rd_kafka_buf_t *rkbuf;
			rd_kafka_msg_t  rkm;
			int evidx;
		} fetch;

		struct {
			rd_kafka_topic_partition_list_t *partitions;
			int do_free; /* free .partitions on destroy() */
		} offset_fetch;

		struct {
			rd_kafka_topic_partition_list_t *partitions;
			void (*cb) (rd_kafka_t *rk,
				    rd_kafka_resp_err_t err,
				    rd_kafka_topic_partition_list_t *offsets,
				    void *opaque);
			void *opaque;
			int silent_empty; /**< Fail silently if there are no
					   *   offsets to commit. */
                        rd_ts_t ts_timeout;
                        char *reason;
		} offset_commit;

		struct {
			rd_kafka_topic_partition_list_t *topics;
		} subscribe; /* also used for GET_SUBSCRIPTION */

		struct {
			rd_kafka_topic_partition_list_t *partitions;
		} assign; /* also used for GET_ASSIGNMENT */

		struct {
			rd_kafka_topic_partition_list_t *partitions;
		} rebalance;

		struct {
			char *str;
		} name;

		struct {
			int64_t offset;
			char *errstr;
			rd_kafka_msg_t rkm;
		} err;  /* used for ERR and CONSUMER_ERR */

		struct {
			int throttle_time;
			int32_t nodeid;
			char *nodename;
		} throttle;

		struct {
			char *json;
			size_t json_len;
		} stats;

		struct {
			rd_kafka_buf_t *rkbuf;
		} xbuf; /* XMIT_BUF and RECV_BUF */

                /* RD_KAFKA_OP_METADATA */
                rd_kafka_metadata_t *metadata;

		struct {
			shptr_rd_kafka_itopic_t *s_rkt;
			rd_kafka_msgq_t msgq;
			rd_kafka_msgq_t msgq2;
			int do_purge2;
		} dr;

		struct {
			int32_t nodeid;
			char    nodename[RD_KAFKA_NODENAME_SIZE];
		} node;

		struct {
			int64_t offset;
			char *reason;
		} offset_reset;

		struct {
			int64_t offset;
			struct rd_kafka_cgrp_s *rkcg;
		} fetch_start; /* reused for SEEK */

		struct {
			int pause;
			int flag;
		} pause;

                struct {
                        char fac[64];
                        int  level;
                        char *str;
                } log;
	} rko_u;
};

TAILQ_HEAD(rd_kafka_op_head_s, rd_kafka_op_s);




const char *rd_kafka_op2str (rd_kafka_op_type_t type);
void rd_kafka_op_destroy (rd_kafka_op_t *rko);
rd_kafka_op_t *rd_kafka_op_new0 (const char *source, rd_kafka_op_type_t type);
#if ENABLE_DEVEL
#define _STRINGIFYX(A) #A
#define _STRINGIFY(A) _STRINGIFYX(A)
#define rd_kafka_op_new(type)                                   \
        rd_kafka_op_new0(__FILE__ ":" _STRINGIFY(__LINE__), type)
#else
#define rd_kafka_op_new(type) rd_kafka_op_new0(NULL, type)
#endif
rd_kafka_op_t *rd_kafka_op_new_reply (rd_kafka_op_t *rko_orig,
                                      rd_kafka_resp_err_t err);
rd_kafka_op_t *rd_kafka_op_new_cb (rd_kafka_t *rk,
                                   rd_kafka_op_type_t type,
                                   void (*cb) (rd_kafka_t *rk,
                                               rd_kafka_op_t *rko));
int rd_kafka_op_reply (rd_kafka_op_t *rko, rd_kafka_resp_err_t err);




#define rd_kafka_op_err(rk,err,...) do {				\
		if (!(rk)->rk_conf.error_cb) {				\
			rd_kafka_log(rk, LOG_ERR, "ERROR", __VA_ARGS__); \
			break;						\
		}							\
		rd_kafka_q_op_err((rk)->rk_rep, RD_KAFKA_OP_ERR, err, 0, \
				  NULL, 0, __VA_ARGS__);		\
	} while (0)

void rd_kafka_q_op_err (rd_kafka_q_t *rkq, rd_kafka_op_type_t optype,
                        rd_kafka_resp_err_t err, int32_t version,
                        rd_kafka_toppar_t *rktp, int64_t offset,
			const char *fmt, ...);
rd_kafka_op_t *rd_kafka_op_req (rd_kafka_q_t *destq,
                                rd_kafka_op_t *rko,
                                int timeout_ms);
rd_kafka_op_t *rd_kafka_op_req2 (rd_kafka_q_t *destq, rd_kafka_op_type_t type);
rd_kafka_resp_err_t rd_kafka_op_err_destroy (rd_kafka_op_t *rko);

void rd_kafka_op_call (rd_kafka_t *rk, rd_kafka_op_t *rko);

void rd_kafka_op_throttle_time (struct rd_kafka_broker_s *rkb,
				rd_kafka_q_t *rkq,
				int throttle_time);

int rd_kafka_op_handle_std (rd_kafka_t *rk, rd_kafka_op_t *rko, int cb_type);
int rd_kafka_op_handle (rd_kafka_t *rk, rd_kafka_op_t *rko,
                        int cb_type, void *opaque,
                        int (*callback) (rd_kafka_t *rk, rd_kafka_op_t *rko,
                                         int cb_type, void *opaque));

extern rd_atomic32_t rd_kafka_op_cnt;

void rd_kafka_op_print (FILE *fp, const char *prefix, rd_kafka_op_t *rko);

void rd_kafka_op_offset_store (rd_kafka_t *rk, rd_kafka_op_t *rko,
			       const rd_kafka_message_t *rkmessage);
