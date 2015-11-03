#include "rdkafka_int.h"
#include "rdkafka_op.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"

/* Current number of rd_kafka_op_t */
rd_atomic32_t rd_kafka_op_cnt;


const char *rd_kafka_op2str (rd_kafka_op_type_t type) {
        static const char *names[] = {
                "NONE",
                "FETCH",
                "ERR",
                "CONSUMER_ERR",
                "DR",
                "STATS",
                "METADATA_REQ",
                "OFFSET_COMMIT",
		"NODE_UPDATE",
                "REPLY",
                "XMIT_BUF",
                "RECV_BUF",
                "XMIT_RETRY",
                "FETCH_START",
                "FETCH_STOP",
                "SEEK",
                "CGRP_DELEGATE",
                "OFFSET_FETCH",
                "OFFSET",
                "PARTITION_JOIN",
                "PARTITION_LEAVE",
                "REBALANCE",
                "STOP",
                "TERMINATE",
                "RESTART",
                "COORD_QUERY",
                "SUBSCRIBE",
                "ASSIGN",
                "GET_SUBSCRIPTION",
                "GET_ASSIGNMENT",
                "SYNCGROUP"
        };

        return names[type];
}

rd_kafka_op_t *rd_kafka_op_new (rd_kafka_op_type_t type) {
	rd_kafka_op_t *rko;

	rko = rd_calloc(1, sizeof(*rko));
	rko->rko_type = type;

        rd_atomic32_add(&rd_kafka_op_cnt, 1);
	return rko;
}


void rd_kafka_op_destroy (rd_kafka_op_t *rko) {

	/* Decrease refcount on rkbuf to eventually rd_free the shared buffer */
	if (rko->rko_rkbuf)
		rd_kafka_buf_destroy(rko->rko_rkbuf);
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
                            void *payload, int len) {
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
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (rk->rk_conf.error_cb)
		rd_kafka_op_app_reply(&rk->rk_rep, RD_KAFKA_OP_ERR, err, 0,
				  rd_strdup(buf), strlen(buf));
	else
		rd_kafka_log_buf(rk, LOG_ERR, "ERROR", buf);
}

/**
 * Propagate an error event to the application on a specific queue.
 */
void rd_kafka_q_op_err (rd_kafka_q_t *rkq, rd_kafka_resp_err_t err,
                        int32_t version,
                        const char *fmt, ...) {
	va_list ap;
	char buf[2048];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

        rd_kafka_op_app_reply(rkq, RD_KAFKA_OP_ERR, err, version,
                              strdup(buf), strlen(buf));
}


/**
 * Enqueue op for app. Convenience function
 */
void rd_kafka_op_app (rd_kafka_q_t *rkq, rd_kafka_op_type_t type,
                      int op_flags, rd_kafka_toppar_t *rktp,
                      rd_kafka_resp_err_t err,
                      void *payload, int len,
                      void (*free_cb) (void *)) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(type);
        if (rktp) {
                rko->rko_rktp = rd_kafka_toppar_keep(rktp);
                rko->rko_version = rktp->rktp_fetch_version;
                rko->rko_rkmessage.rkt = rd_kafka_topic_keep_a(rktp->rktp_rkt);
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
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);

        rd_kafka_op_app(rkq, type, RD_KAFKA_OP_F_FREE,
                        rktp, err, rd_strdup(buf), strlen(buf), NULL);
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
                       void *payload, int len, void (*free_cb) (void *)) {
        rd_kafka_op_t *rko;

        if (!rko_orig->rko_replyq)
                return 0;

        rko = rd_kafka_op_new(rko_orig->rko_type);
        rko->rko_err     = err;
        rko->rko_payload = payload;
        rko->rko_len     = len;
        rko->rko_free_cb = free_cb;
        rko->rko_version = rko_orig->rko_version;

        rd_kafka_q_enq(rko_orig->rko_replyq, rko);

        return 1;
}

/**
 * Send request to queue, wait for response.
 */
rd_kafka_op_t *rd_kafka_op_req0 (rd_kafka_q_t *destq,
                                 rd_kafka_q_t *recvq,
                                 rd_kafka_op_t *rko,
                                 int timeout_ms) {
        rd_kafka_op_t *reply;

        /* Bump refcount for destination, destination will decrease refcount
         * after posting reply. */
        rd_kafka_q_keep(recvq);

        /* Indicate to destination where to send reply. */
        rko->rko_replyq = recvq;

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
        rd_kafka_resp_err_t err = rko->rko_err;
        rd_kafka_op_destroy(rko);
        return err;
}
