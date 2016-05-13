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

#include <stdarg.h>

#include "rdkafka_int.h"
#include "rdkafka_op.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"

/* Current number of rd_kafka_op_t */
rd_atomic32_t rd_kafka_op_cnt;


const char *rd_kafka_op2str (rd_kafka_op_type_t type) {
        int skiplen = 6;
        static const char *names[] = {
                "REPLY:NONE",
                "REPLY:FETCH",
                "REPLY:ERR",
                "REPLY:CONSUMER_ERR",
                "REPLY:DR",
                "REPLY:STATS",
                "REPLY:METADATA_REQ",
                "REPLY:OFFSET_COMMIT",
		"REPLY:NODE_UPDATE",
                "REPLY:XMIT_BUF",
                "REPLY:RECV_BUF",
                "REPLY:XMIT_RETRY",
                "REPLY:FETCH_START",
                "REPLY:FETCH_STOP",
                "REPLY:SEEK",
                "REPLY:OFFSET_FETCH",
                "REPLY:PARTITION_JOIN",
                "REPLY:PARTITION_LEAVE",
                "REPLY:REBALANCE",
                "REPLY:TERMINATE",
                "REPLY:COORD_QUERY",
                "REPLY:SUBSCRIBE",
                "REPLY:ASSIGN",
                "REPLY:GET_SUBSCRIPTION",
                "REPLY:GET_ASSIGNMENT",
		"REPLY:THROTTLE",
                "REPLY:CALLBACK",
		"REPLY:NAME"
        };

        if (type & RD_KAFKA_OP_REPLY) {
                type &= ~RD_KAFKA_OP_REPLY;
                skiplen = 0;
        }

        return names[type]+skiplen;
}

rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type) {
	rd_kafka_op_t *rko;

	rko = rd_calloc(1, sizeof(*rko));
	rko->rko_type = type;

        rd_atomic32_add(&rd_kafka_op_cnt, 1);
	return rko;
}


void rd_kafka_op_destroy (rd_kafka_op_t *rko) {

	/* Decrease refcount on rkbuf to eventually rd_free the shared buffer*/
	if (rko->rko_rkbuf)
		rd_kafka_buf_handle_op(rko, RD_KAFKA_RESP_ERR__DESTROY);
	else if (rko->rko_payload && rko->rko_flags & RD_KAFKA_OP_F_FREE) {
                if (rko->rko_free_cb)
                        rko->rko_free_cb(rko->rko_payload);
                else
                        rd_free(rko->rko_payload);
        }
        if (rko->rko_rkt)
                rd_kafka_topic_destroy0(rd_kafka_topic_a2s(rko->rko_rkt));
        if (rko->rko_rktp)
                rd_kafka_toppar_destroy(rko->rko_rktp);
        if (rko->rko_metadata)
                rd_kafka_metadata_destroy(rko->rko_metadata);
        if (rko->rko_replyq)
                rd_kafka_q_destroy(rko->rko_replyq);

        if (rd_atomic32_sub(&rd_kafka_op_cnt, 1) < 0)
                rd_kafka_assert(NULL, !*"rd_kafka_op_cnt < 0");

	rd_free(rko);
}









/**
 * Send an op back to the application.
 *
 * Locality: Kafka thread
 */
void rd_kafka_op_app_reply (rd_kafka_q_t *rkq,
                            rd_kafka_op_type_t type,
                            rd_kafka_resp_err_t err,
                            int32_t version,
                            void *payload, size_t len) {
	rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(type);

	if (err && !payload) {
		/* Provide human readable error string if not provided. */

                payload = rd_strdup(rd_kafka_err2str(err));
		len = strlen(payload);
	}

        rko->rko_flags      |= RD_KAFKA_OP_F_FREE;
        rko->rko_version     = version;
	rko->rko_payload     = payload;
	rko->rko_len         = len;
	rko->rko_err         = err;

	rd_kafka_q_enq(rkq, rko);
}


void rd_kafka_op_app_reply2 (rd_kafka_t *rk, rd_kafka_op_t *rko) {
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
	rd_vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (rk->rk_conf.error_cb)
		rd_kafka_op_app_reply(&rk->rk_rep, RD_KAFKA_OP_ERR, err, 0,
				  rd_strdup(buf), strlen(buf));
	else
		rd_kafka_log_buf(rk, LOG_ERR, "ERROR", buf);
}


/**
 * sprintf a message in rko->rko_payload (typically error string)
 */
void rd_kafka_op_sprintf (rd_kafka_op_t *rko, const char *fmt, ...) {
	va_list ap;
	char buf[2048];

	va_start(ap, fmt);
	rd_vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	rd_kafka_assert(NULL, !rko->rko_payload);
	rko->rko_payload = rd_strdup(buf);
	rko->rko_len = strlen(buf);
	rko->rko_flags |= RD_KAFKA_OP_F_FREE;
	rko->rko_free_cb = rd_free;
}

/**
 * Propagate an error event to the application on a specific queue.
 * \p optype should be RD_KAFKA_OP_ERR for generic errors and
 * RD_KAFKA_OP_CONSUMER_ERR for consumer errors.
 */
void rd_kafka_q_op_err (rd_kafka_q_t *rkq, rd_kafka_op_type_t optype,
                        rd_kafka_resp_err_t err, int32_t version,
                        const char *fmt, ...) {
	va_list ap;
	char buf[2048];

	va_start(ap, fmt);
	rd_vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

        rd_kafka_op_app_reply(rkq, optype, err, version,
                              rd_strdup(buf), strlen(buf));
}


/**
 * Enqueue op for app. Convenience function
 */
void rd_kafka_op_app (rd_kafka_q_t *rkq, rd_kafka_op_type_t type,
                      int op_flags, rd_kafka_toppar_t *rktp,
                      rd_kafka_resp_err_t err,
                      void *payload, size_t len,
                      void (*free_cb) (void *)) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(type);
        if (rktp) {
                rko->rko_rktp = rd_kafka_toppar_keep(rktp);
                rko->rko_version = rktp->rktp_fetch_version;
                rko->rko_rkmessage.partition = rktp->rktp_partition;
        }

        rko->rko_err     = err;
        rko->rko_payload = payload;
        rko->rko_len     = len;
        rko->rko_flags  |= op_flags;
        rko->rko_free_cb = free_cb;

        rd_kafka_q_enq(rkq, rko);
}


/**
 * Enqueue op for app. Convenience function
 */
void rd_kafka_op_app_fmt (rd_kafka_q_t *rkq, rd_kafka_op_type_t type,
                          rd_kafka_toppar_t *rktp,
                          rd_kafka_resp_err_t err,
                          const char *fmt, ...) {
        char buf[1024];
        va_list ap;
        va_start(ap, fmt);
        rd_vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);

        rd_kafka_op_app(rkq, type, RD_KAFKA_OP_F_FREE,
                        rktp, err, rd_strdup(buf), strlen(buf), NULL);
}


/**
 * Moves a payload (pointer and free_cb, not actual memory content)
 * from src to dst.
 */
void rd_kafka_op_payload_move (rd_kafka_op_t *dst, rd_kafka_op_t *src) {
        dst->rko_payload = src->rko_payload;
        dst->rko_len = src->rko_len;
        dst->rko_free_cb = src->rko_free_cb;
        dst->rko_flags = (src->rko_flags & RD_KAFKA_OP_F_FREE);

        src->rko_payload = NULL;
        src->rko_len = 0;
        src->rko_free_cb = NULL;
        src->rko_flags &= ~RD_KAFKA_OP_F_FREE;
}


/**
 * Creates a reply opp based on 'rko_orig'.
 * If 'rko_orig' has rko_op_cb set the reply op will be of type
 * RD_KAFKA_OP_CALLBACK, else the reply type will be the original rko_type OR:ed
 * with RD_KAFKA_OP_REPLY.
 */
rd_kafka_op_t *rd_kafka_op_new_reply (rd_kafka_op_t *rko_orig) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(rko_orig->rko_op_cb ?
                              RD_KAFKA_OP_CALLBACK :
                              (rko_orig->rko_type | RD_KAFKA_OP_REPLY));
        rko->rko_op_cb = rko_orig->rko_op_cb;
        rko->rko_version = rko_orig->rko_version;

        return rko;
}



/**
 * Reply to 'rko_orig' using err,payload,len if a replyq is set up,
 * else do nothing.
 *
 * Returns 0 if 'rko_orig' did not have a replyq and nothing was enqueued,
 * else 1.
 */
int rd_kafka_op_reply (rd_kafka_op_t *rko_orig,
                       rd_kafka_resp_err_t err,
                       void *payload, size_t len, void (*free_cb) (void *)) {
        rd_kafka_op_t *rko;

        if (!rko_orig->rko_replyq)
                return 0;

        rko = rd_kafka_op_new(rko_orig->rko_type);
        rko->rko_err     = err;
        rko->rko_payload = payload;
        rko->rko_len     = len;
        rko->rko_free_cb = free_cb;
	if (free_cb)
		rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        rko->rko_version = rko_orig->rko_version;

        return rd_kafka_q_enq(rko_orig->rko_replyq, rko);
}


/**
 * Send request to queue, wait for response.
 */
rd_kafka_op_t *rd_kafka_op_req0 (rd_kafka_q_t *destq,
                                 rd_kafka_q_t *recvq,
                                 rd_kafka_op_t *rko,
                                 int timeout_ms) {
        rd_kafka_op_t *reply;

        /* Indicate to destination where to send reply. */
        rko->rko_replyq = recvq;
        if (recvq)
                rd_kafka_q_keep(rko->rko_replyq);

        /* Enqueue op */
        rd_kafka_q_enq(destq, rko);

        /* Wait for reply */
        reply = rd_kafka_q_pop(recvq, timeout_ms, 0);

        /* May be NULL for timeout */
        return reply;
}

/**
 * Send request to queue, wait for response.
 * Creates a temporary reply queue.
 */
rd_kafka_op_t *rd_kafka_op_req (rd_kafka_q_t *destq,
                                rd_kafka_op_t *rko,
                                int timeout_ms) {
        rd_kafka_q_t *recvq;
        rd_kafka_op_t *reply;

        recvq = rd_kafka_q_new(destq->rkq_rk);

        reply = rd_kafka_op_req0(destq, recvq, rko, timeout_ms);

        rd_kafka_q_destroy(recvq);

        return reply;
}


/**
 * Send simple type-only request to queue, wait for response.
 */
rd_kafka_op_t *rd_kafka_op_req2 (rd_kafka_q_t *destq, rd_kafka_op_type_t type) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(type);
        return rd_kafka_op_req(destq, rko, RD_POLL_INFINITE);
}

/**
 * Destroys the rko and returns its error.
 */
rd_kafka_resp_err_t rd_kafka_op_err_destroy (rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__TIMED_OUT;

	if (rko) {
		err = rko->rko_err;
		rd_kafka_op_destroy(rko);
	}
        return err;
}


/**
 * Call op callback
 */
void rd_kafka_op_call (rd_kafka_t *rk, rd_kafka_op_t *rko) {
        rko->rko_op_cb(rk, rko);
}


/**
 * Enqueue ERR__THROTTLE op, if desired.
 */
void rd_kafka_op_throttle_time (rd_kafka_broker_t *rkb,
				rd_kafka_q_t *rkq,
				int throttle_time) {
	rd_kafka_op_t *rko;

	rd_avg_add(&rkb->rkb_avg_throttle, throttle_time);

	/* We send throttle events when:
	 *  - throttle_time > 0
	 *  - throttle_time == 0 and last throttle_time > 0
	 */
	if (!rkb->rkb_rk->rk_conf.throttle_cb ||
	    (!throttle_time && !rd_atomic32_get(&rkb->rkb_rk->rk_last_throttle)))
		return;

	rd_atomic32_set(&rkb->rkb_rk->rk_last_throttle, throttle_time);

	rko = rd_kafka_op_new(RD_KAFKA_OP_THROTTLE);
	rko->rko_nodename      = rd_strdup(rkb->rkb_nodename);
	rko->rko_flags        |= RD_KAFKA_OP_F_FREE; /* free nodename */
	rko->rko_nodeid        = rkb->rkb_nodeid;
	rko->rko_throttle_time = throttle_time;
	rd_kafka_q_enq(rkq, rko);
}
