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

        if (type & RD_KAFKA_OP_REPLY)
                skiplen = 0;

        return names[type & ~RD_KAFKA_OP_FLAGMASK]+skiplen;
}

rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type) {
	rd_kafka_op_t *rko;
	static const size_t op2size[RD_KAFKA_OP__END] = {
		[RD_KAFKA_OP_FETCH] = sizeof(rko->rko_u.fetch),
		[RD_KAFKA_OP_ERR] = sizeof(rko->rko_u.err),
		[RD_KAFKA_OP_CONSUMER_ERR] = sizeof(rko->rko_u.err),
		[RD_KAFKA_OP_DR] = sizeof(rko->rko_u.dr),
		[RD_KAFKA_OP_STATS] = sizeof(rko->rko_u.stats),
		[RD_KAFKA_OP_METADATA_REQ] = sizeof(rko->rko_u.metadata),
		[RD_KAFKA_OP_OFFSET_COMMIT] = sizeof(rko->rko_u.offset_commit),
		[RD_KAFKA_OP_NODE_UPDATE] = sizeof(rko->rko_u.node),
		[RD_KAFKA_OP_XMIT_BUF] = sizeof(rko->rko_u.xbuf),
		[RD_KAFKA_OP_RECV_BUF] = sizeof(rko->rko_u.xbuf),
		[RD_KAFKA_OP_XMIT_RETRY] = sizeof(rko->rko_u.xbuf),
		[RD_KAFKA_OP_FETCH_START] = sizeof(rko->rko_u.fetch_start),
		[RD_KAFKA_OP_FETCH_STOP] = 0,
		[RD_KAFKA_OP_SEEK] = sizeof(rko->rko_u.fetch_start),
		[RD_KAFKA_OP_OFFSET_FETCH] = sizeof(rko->rko_u.offset_fetch),
		[RD_KAFKA_OP_PARTITION_JOIN] = 0,
		[RD_KAFKA_OP_PARTITION_LEAVE] = 0,
		[RD_KAFKA_OP_REBALANCE] = sizeof(rko->rko_u.rebalance),
		[RD_KAFKA_OP_TERMINATE] = 0,
		[RD_KAFKA_OP_COORD_QUERY] = 0,
		[RD_KAFKA_OP_SUBSCRIBE] = sizeof(rko->rko_u.subscribe),
		[RD_KAFKA_OP_ASSIGN] = sizeof(rko->rko_u.assign),
		[RD_KAFKA_OP_GET_SUBSCRIPTION] = sizeof(rko->rko_u.subscribe),
		[RD_KAFKA_OP_GET_ASSIGNMENT] = sizeof(rko->rko_u.assign),
		[RD_KAFKA_OP_THROTTLE] = sizeof(rko->rko_u.throttle),
		[RD_KAFKA_OP_OFFSET_RESET] = sizeof(rko->rko_u.offset_reset),
		[RD_KAFKA_OP_NAME] = sizeof(rko->rko_u.name),
	};
	size_t tsize = op2size[type & ~RD_KAFKA_OP_FLAGMASK];

	rko = rd_calloc(1, sizeof(*rko)-sizeof(rko->rko_u)+tsize);
	rko->rko_type = type;

        rd_atomic32_add(&rd_kafka_op_cnt, 1);
	return rko;
}


void rd_kafka_op_destroy (rd_kafka_op_t *rko) {

	switch (rko->rko_type & ~RD_KAFKA_OP_REPLY)
	{
	case RD_KAFKA_OP_FETCH:
		/* Decrease refcount on rkbuf to eventually rd_free shared buf*/
		if (rko->rko_u.fetch.rkbuf)
			rd_kafka_buf_handle_op(rko, RD_KAFKA_RESP_ERR__DESTROY);

		break;

	case RD_KAFKA_OP_OFFSET_FETCH:
		if (rko->rko_u.offset_fetch.partitions &&
		    rko->rko_u.offset_fetch.do_free)
			rd_kafka_topic_partition_list_destroy(
				rko->rko_u.offset_fetch.partitions);
		break;

	case RD_KAFKA_OP_OFFSET_COMMIT:
		RD_IF_FREE(rko->rko_u.offset_commit.partitions,
			   rd_kafka_topic_partition_list_destroy);
		break;

	case RD_KAFKA_OP_SUBSCRIBE:
	case RD_KAFKA_OP_GET_SUBSCRIPTION:
		RD_IF_FREE(rko->rko_u.subscribe.topics,
			   rd_kafka_topic_partition_list_destroy);
		break;

	case RD_KAFKA_OP_ASSIGN:
	case RD_KAFKA_OP_GET_ASSIGNMENT:
		RD_IF_FREE(rko->rko_u.assign.partitions,
			   rd_kafka_topic_partition_list_destroy);
		break;

	case RD_KAFKA_OP_REBALANCE:
		RD_IF_FREE(rko->rko_u.rebalance.partitions,
			   rd_kafka_topic_partition_list_destroy);
		break;

	case RD_KAFKA_OP_NAME:
		RD_IF_FREE(rko->rko_u.name.str, rd_free);
		break;

	case RD_KAFKA_OP_ERR:
	case RD_KAFKA_OP_CONSUMER_ERR:
		RD_IF_FREE(rko->rko_u.err.errstr, rd_free);
		break;

		break;

	case RD_KAFKA_OP_THROTTLE:
		RD_IF_FREE(rko->rko_u.throttle.nodename, rd_free);
		break;

	case RD_KAFKA_OP_STATS:
		RD_IF_FREE(rko->rko_u.stats.json, rd_free);
		break;

	case RD_KAFKA_OP_XMIT_RETRY:
	case RD_KAFKA_OP_XMIT_BUF:
	case RD_KAFKA_OP_RECV_BUF:
		if (rko->rko_u.xbuf.rkbuf)
			rd_kafka_buf_handle_op(rko, RD_KAFKA_RESP_ERR__DESTROY);

		RD_IF_FREE(rko->rko_u.xbuf.rkbuf, rd_kafka_buf_destroy);
		break;

	case RD_KAFKA_OP_METADATA_REQ:
		if (rko->rko_u.metadata.rkt)
			rd_kafka_topic_destroy0(
				rd_kafka_topic_a2s(rko->rko_u.metadata.rkt));
		RD_IF_FREE(rko->rko_u.metadata.metadata,
			   rd_kafka_metadata_destroy);
		break;

	case RD_KAFKA_OP_DR:
		rd_kafka_msgq_purge(rko->rko_rk, &rko->rko_u.dr.msgq);
		if (rko->rko_u.dr.do_purge2)
			rd_kafka_msgq_purge(rko->rko_rk, &rko->rko_u.dr.msgq2);

		if (rko->rko_u.dr.rkt)
			rd_kafka_topic_destroy0(
				rd_kafka_topic_a2s(rko->rko_u.dr.rkt));
		break;

	case RD_KAFKA_OP_OFFSET_RESET:
		RD_IF_FREE(rko->rko_u.offset_reset.reason, rd_free);
		break;

	default:
		break;
	}

	RD_IF_FREE(rko->rko_rktp, rd_kafka_toppar_destroy);

        if (rko->rko_replyq)
                rd_kafka_q_destroy(rko->rko_replyq);

        if (rd_atomic32_sub(&rd_kafka_op_cnt, 1) < 0)
                rd_kafka_assert(NULL, !*"rd_kafka_op_cnt < 0");

	rd_free(rko);
}











/**
 * Propagate an error event to the application on a specific queue.
 * \p optype should be RD_KAFKA_OP_ERR for generic errors and
 * RD_KAFKA_OP_CONSUMER_ERR for consumer errors.
 */
void rd_kafka_q_op_err (rd_kafka_q_t *rkq, rd_kafka_op_type_t optype,
                        rd_kafka_resp_err_t err, int32_t version,
			rd_kafka_toppar_t *rktp, int64_t offset,
                        const char *fmt, ...) {
	va_list ap;
	char buf[2048];
	rd_kafka_op_t *rko;

	va_start(ap, fmt);
	rd_vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	rko = rd_kafka_op_new(optype);
	rko->rko_version = version;
	rko->rko_err = err;
	rko->rko_u.err.offset = offset;
	rko->rko_u.err.errstr = rd_strdup(buf);
	if (rktp)
		rko->rko_rktp = rd_kafka_toppar_keep(rktp);

	rd_kafka_q_enq(rkq, rko);
}



/**
 * Creates a reply opp based on 'rko_orig'.
 * If 'rko_orig' has rko_op_cb set the reply op will be OR:ed with
 * RD_KAFKA_OP_CB, else the reply type will be the original rko_type OR:ed
 * with RD_KAFKA_OP_REPLY.
 */
rd_kafka_op_t *rd_kafka_op_new_reply (rd_kafka_op_t *rko_orig,
				      rd_kafka_resp_err_t err) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(rko_orig->rko_type |
			      (rko_orig->rko_op_cb ?
			       RD_KAFKA_OP_CB : RD_KAFKA_OP_REPLY));
	rko->rko_op_cb   = rko_orig->rko_op_cb;
        rko->rko_version = rko_orig->rko_version;
	rko->rko_err     = err;

        return rko;
}



/**
 * @brief Reply to 'rko' re-using the same rko.
 * If there is no replyq the rko is destroyed.
 *
 * @returns 1 if op was enqueued, else 0 and rko is destroyed.
 */
int rd_kafka_op_reply (rd_kafka_op_t *rko, rd_kafka_resp_err_t err) {
	rd_kafka_q_t *replyq = rko->rko_replyq;

        if (!replyq) {
		rd_kafka_op_destroy(rko);
                return 0;
	}

	rko->rko_replyq = NULL;
	rko->rko_type  |= rko->rko_type |
		(rko->rko_op_cb ? RD_KAFKA_OP_CB : RD_KAFKA_OP_REPLY);
        rko->rko_err    = err;

        return rd_kafka_q_enq(replyq, rko);
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
	rko->rko_u.throttle.nodename = rd_strdup(rkb->rkb_nodename);
	rko->rko_u.throttle.nodeid   = rkb->rkb_nodeid;
	rko->rko_u.throttle.throttle_time = throttle_time;
	rd_kafka_q_enq(rkq, rko);
}


/**
 * @brief Handle standard op types.
 * @returns 1 if handled, else 0.
 */
int rd_kafka_op_handle_std (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	if (rko->rko_type & RD_KAFKA_OP_CB) {
		rko->rko_op_cb(rk, rko);
		return 1;
	}

	return 0;
}
