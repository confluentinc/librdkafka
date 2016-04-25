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
        RD_KAFKA_OP_NONE,
	RD_KAFKA_OP_FETCH,    /* Kafka thread -> Application */
	RD_KAFKA_OP_ERR,      /* Kafka thread -> Application */
        RD_KAFKA_OP_CONSUMER_ERR, /* Kafka thread -> Application */
	RD_KAFKA_OP_DR,       /* Kafka thread -> Application
			       * Produce message delivery report */
	RD_KAFKA_OP_STATS,    /* Kafka thread -> Application */

	RD_KAFKA_OP_METADATA_REQ,  /* any -> Broker thread: request metadata */
        RD_KAFKA_OP_OFFSET_COMMIT, /* any -> toppar's Broker thread */
        RD_KAFKA_OP_NODE_UPDATE,   /* any -> Broker thread: node update */

        RD_KAFKA_OP_XMIT_BUF, /* transmit buffer: any -> broker thread */
        RD_KAFKA_OP_RECV_BUF, /* received response buffer: broker thr -> any */
        RD_KAFKA_OP_XMIT_RETRY, /* retry buffer xmit: any -> broker thread */
        RD_KAFKA_OP_FETCH_START, /* Application -> toppar's Broker thread */
        RD_KAFKA_OP_FETCH_STOP,  /* Application -> toppar's Broker thread */
        RD_KAFKA_OP_SEEK,        /* Application -> toppar's Broker thread */
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
        RD_KAFKA_OP_GET_SUBSCRIPTION,/* Get current subscription */
        RD_KAFKA_OP_GET_ASSIGNMENT,  /* Get current assignment */
	RD_KAFKA_OP_THROTTLE,        /* Throttle info */
        RD_KAFKA_OP_CALLBACK,        /* Calls rko_op_cb */
	RD_KAFKA_OP_NAME,            /* Request name */
        RD_KAFKA_OP__END
} rd_kafka_op_type_t;


/* Flags used with op_type_t */
#define RD_KAFKA_OP_REPLY  (1 << 31)  /* Reply op. */


typedef struct rd_kafka_op_s {
	TAILQ_ENTRY(rd_kafka_op_s) rko_link;

	rd_kafka_op_type_t rko_type;
	int                rko_flags;  /* See RD_KAFKA_OP_F_... above */

        /* Generic fields */
	rd_kafka_msgq_t rko_msgq;
        rd_kafka_q_t   *rko_replyq;    /* Indicates request: enq reply
                                        * on this queue. Refcounted. */
        int             rko_intarg;    /* Generic integer argument */

        /* RD_KAFKA_OP_CALLBACK */
        void          (*rko_op_cb) (rd_kafka_t *rk, struct rd_kafka_op_s *rko);

        void          (*rko_free_cb) (void *);/* Callback to free rko_payload if
                                               * RD_KAFKA_OP_F_FREE flag is set.
                                               * Default is rd_free() */

	/* For PRODUCE */
	rd_kafka_msg_t *rko_rkm;

	/* For ERR */
#define rko_err     rko_rkmessage.err
#define rko_payload rko_rkmessage.payload
#define rko_len     rko_rkmessage.len

	/* For FETCH */
	rd_kafka_message_t rko_rkmessage;
	rd_kafka_buf_t    *rko_rkbuf;
	rd_kafka_timestamp_type_t rko_tstype;
	int64_t            rko_timestamp;

	/* For METADATA */
#define rko_rkt         rko_rkmessage.rkt
#define rko_all_topics  rko_intarg
#define rko_reason      rko_rkmessage.payload
        struct rd_kafka_metadata *rko_metadata;

	/* For STATS */
#define rko_json      rko_rkmessage.payload
#define rko_json_len  rko_rkmessage.len

        /* For OFFSET_COMMIT, FETCH_START */
        shptr_rd_kafka_toppar_t *rko_rktp;
#define rko_offset    rko_rkmessage.offset

        /* For CGRP_DELEGATE */
        struct rd_kafka_cgrp_s *rko_cgrp;

        /* For FETCH_START, FETCH */
#define rko_version   rko_intarg

        /* For BROKER_UPDATE and THROTTLE */
#define rko_nodename  rko_rkmessage.payload
#define rko_nodeid    rko_rkmessage.partition

	/* For THROTTLE */
#define rko_throttle_time rko_rkmessage.offset

} rd_kafka_op_t;

TAILQ_HEAD(rd_kafka_op_head_s, rd_kafka_op_s);




const char *rd_kafka_op2str (rd_kafka_op_type_t type);
void rd_kafka_op_destroy (rd_kafka_op_t *rko);
rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type);
rd_kafka_op_t *rd_kafka_op_new_reply (rd_kafka_op_t *rko_orig);
void rd_kafka_op_payload_move (rd_kafka_op_t *dst, rd_kafka_op_t *src);

/**
 * Sets the op payload to \p payload and if \p free_cb is set also
 * assigns it as the free callback as well as sets the RD_KAFKA_OP_F_FREE flag.
 */
static RD_UNUSED void rd_kafka_op_payload_set (rd_kafka_op_t *rko,
                                               void *payload,
                                               void (*free_cb) (void *)) {
        rko->rko_payload = payload;
        if (free_cb) {
                rko->rko_flags |= RD_KAFKA_OP_F_FREE;
                rko->rko_free_cb = free_cb;
        }
}


void rd_kafka_op_app_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko);
void rd_kafka_op_app_reply (rd_kafka_q_t *rkq,
                            rd_kafka_op_type_t type,
                            rd_kafka_resp_err_t err,
                            int32_t version,
                            void *payload, size_t len);

int rd_kafka_op_reply (rd_kafka_op_t *orig_rko,
                       rd_kafka_resp_err_t err,
                       void *payload, size_t len, void (*free_cb) (void *));
void rd_kafka_op_sprintf (rd_kafka_op_t *rko, const char *fmt, ...);

void rd_kafka_op_err (rd_kafka_t *rk, rd_kafka_resp_err_t err,
		      const char *fmt, ...);
void rd_kafka_q_op_err (rd_kafka_q_t *rkq, rd_kafka_op_type_t optype,
                        rd_kafka_resp_err_t err, int32_t version,
                        const char *fmt, ...);
void rd_kafka_op_app (rd_kafka_q_t *rkq, rd_kafka_op_type_t type,
                      int op_flags, rd_kafka_toppar_t *rktp,
                      rd_kafka_resp_err_t err,
                      void *payload, size_t len,
                      void (*free_cb) (void *));
void rd_kafka_op_app_fmt (rd_kafka_q_t *rkq, rd_kafka_op_type_t type,
                          rd_kafka_toppar_t *rktp,
                          rd_kafka_resp_err_t err,
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

extern rd_atomic32_t rd_kafka_op_cnt;
