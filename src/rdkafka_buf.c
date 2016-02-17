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

#include "rdkafka_int.h"
#include "rdkafka_buf.h"
#include "rdkafka_broker.h"

void rd_kafka_buf_destroy_final (rd_kafka_buf_t *rkbuf) {

        if (rkbuf->rkbuf_response)
                rd_kafka_buf_destroy(rkbuf->rkbuf_response);

        if (rkbuf->rkbuf_buf2)
                rd_free(rkbuf->rkbuf_buf2);

        if (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FREE && rkbuf->rkbuf_buf)
                rd_free(rkbuf->rkbuf_buf);

        if (rkbuf->rkbuf_rkb)
                rd_kafka_broker_destroy(rkbuf->rkbuf_rkb);

        rd_refcnt_destroy(&rkbuf->rkbuf_refcnt);

	rd_free(rkbuf);
}

void rd_kafka_buf_auxbuf_add (rd_kafka_buf_t *rkbuf, void *auxbuf) {
	rd_kafka_assert(NULL, rkbuf->rkbuf_buf2 == NULL);
	rkbuf->rkbuf_buf2 = auxbuf;
}


/**
 * Allocate another receive buffer that fits 'size' bytes.
 * This to ensure entire payload resides in contigous memory.
 */
void rd_kafka_buf_alloc_recvbuf (rd_kafka_buf_t *rkbuf, size_t size) {
	rkbuf->rkbuf_len  = size;
	rkbuf->rkbuf_buf2 = rd_malloc(size);
	/* Point read buffer to payload buffer. */
	rkbuf->rkbuf_rbuf = rkbuf->rkbuf_buf2;
	/* Reset offsets for new buffer */
	rkbuf->rkbuf_of   = 0;
	rkbuf->rkbuf_wof  = 0;
	/* Write to first iovec */
	rkbuf->rkbuf_iov[0].iov_base = rkbuf->rkbuf_buf2;
	rkbuf->rkbuf_iov[0].iov_len = rkbuf->rkbuf_len;
	rkbuf->rkbuf_msg.msg_iovlen = 1;
}


/**
 * Rewind write offset pointer and iovec poiner to a previous stored value.
 */
void rd_kafka_buf_rewind (rd_kafka_buf_t *rkbuf, int iovindex, size_t new_of,
	size_t new_of_init) {
	rkbuf->rkbuf_msg.msg_iovlen = iovindex;
	rkbuf->rkbuf_wof = new_of;
	rkbuf->rkbuf_wof_init = new_of_init;

}

struct iovec *rd_kafka_buf_iov_next (rd_kafka_buf_t *rkbuf) {
	rd_kafka_assert(NULL,
                        (int)rkbuf->rkbuf_msg.msg_iovlen + 1 <=
			rkbuf->rkbuf_iovcnt);
	rkbuf->rkbuf_wof_init = rkbuf->rkbuf_wof;
	return &rkbuf->rkbuf_iov[rkbuf->rkbuf_msg.msg_iovlen++];
}

/**
 * Pushes 'buf' & 'len' onto the previously allocated iov stack for 'rkbuf'.
 */
void rd_kafka_buf_push0 (rd_kafka_buf_t *rkbuf, const void *buf, size_t len,
			 int allow_crc_calc) {
	struct iovec *iov;

	iov = rd_kafka_buf_iov_next(rkbuf);

	iov->iov_base = (void *)buf;
	iov->iov_len = len;

	if (allow_crc_calc && (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_CRC))
		rkbuf->rkbuf_crc = rd_crc32_update(rkbuf->rkbuf_crc, buf, len);
}

/**
 * Simply pushes the write-buffer onto the iovec stack.
 * This is to be used when the rd_kafka_buf_write*() set of functions
 * are used to construct a buffer rather than individual rd_kafka_buf_push()es.
 * WARNING:
 *   If used with growable buffers this call must only be performed
 *   once and after all buf_write()s have been performed.
 */
void rd_kafka_buf_autopush (rd_kafka_buf_t *rkbuf) {
	rd_kafka_assert(NULL, rkbuf->rkbuf_wof > rkbuf->rkbuf_wof_init);
        rd_kafka_buf_push0(rkbuf, rkbuf->rkbuf_wbuf + rkbuf->rkbuf_wof_init,
			   rkbuf->rkbuf_wof - rkbuf->rkbuf_wof_init,
			   0/* No CRC calc */);
}


/**
 * Grow buffer to `needed_len`.
 *
 * NOTE: This is a costly operation since it uses realloc()
 */
void rd_kafka_buf_grow (rd_kafka_buf_t *rkbuf, size_t needed_len) {
        size_t alen = rkbuf->rkbuf_size;
        void *new;

        rd_kafka_assert(NULL, rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FREE);

        if (alen < 256) /* avoid slow upramp */
                alen = 256;
        while (needed_len > alen)
                alen *= 2;

        new = rd_realloc(rkbuf->rkbuf_buf, alen);
        rd_kafka_assert(NULL, new != NULL);

        /* If memory was moved we need to repoint the iovecs */
        if (unlikely(rkbuf->rkbuf_buf != new))
                rkbuf->rkbuf_iov[0].iov_base = new;

        rkbuf->rkbuf_buf  = new;
        rkbuf->rkbuf_wbuf = rkbuf->rkbuf_buf;
        rkbuf->rkbuf_size = alen;
}

rd_kafka_buf_t *rd_kafka_buf_new_growable (const rd_kafka_t *rk,
                                           int iovcnt, size_t init_size) {
        rd_kafka_assert(NULL, iovcnt == 1);/* growables only support one iovec */
        return rd_kafka_buf_new0(rk, iovcnt, init_size, RD_KAFKA_OP_F_FREE);
}


/**
 * Create a new buffer with 'iovcnt' iovecs and 'size' bytes buffer memory.
 * If 'rk' is non-NULL (typical case):
 * Additional space for the Kafka protocol headers is inserted automatically.
 *
 */
rd_kafka_buf_t *rd_kafka_buf_new0 (const rd_kafka_t *rk,
                                   int iovcnt, size_t size, int flags) {
	rd_kafka_buf_t *rkbuf;
	size_t iovsize;
	size_t fullsize;
        size_t extsize = 0;
        size_t headersize = 0;
        int growable = (flags & RD_KAFKA_OP_F_FREE);

        /* Make room for common protocol request headers */
        if (rk) {
                headersize = RD_KAFKAP_REQHDR_SIZE +
                        RD_KAFKAP_STR_SIZE(rk->rk_conf.client_id);
                size += headersize;
                iovcnt += 1; /* headers */
        }

        iovsize = sizeof(struct iovec) * iovcnt;
        fullsize = iovsize + sizeof(*rkbuf);

        /* If the buffer is growable the rkbuf_buf is allocated separetely
         * to cater for realloc() calls.
         * If not growable the rkbuf_buf memory is allocated with the rkbuf
         * following the rkbuf struct and iovecs. */
        if (growable)
                extsize = size;
        else
                fullsize += size;

	rkbuf = rd_malloc(fullsize);
	memset(rkbuf, 0, sizeof(*rkbuf));

        rkbuf->rkbuf_flags = flags;
	rkbuf->rkbuf_iov = (struct iovec *)(rkbuf+1);
	rkbuf->rkbuf_iovcnt = iovcnt;
	rd_kafka_assert(NULL, rkbuf->rkbuf_iovcnt <= IOV_MAX);
	rkbuf->rkbuf_msg.msg_iov = rkbuf->rkbuf_iov;

        if (growable)
                rkbuf->rkbuf_buf = rd_malloc(extsize);
        else
                rkbuf->rkbuf_buf  = ((char *)(rkbuf+1))+iovsize;
        rkbuf->rkbuf_wbuf = rkbuf->rkbuf_buf;
	rkbuf->rkbuf_msg.msg_iovlen = 0;

        if (rk) {
                /* save the first iovecs for the header + clientid */
                rkbuf->rkbuf_iov[0].iov_base = rkbuf->rkbuf_buf;
                rkbuf->rkbuf_iov[0].iov_len  = headersize;
                rkbuf->rkbuf_msg.msg_iovlen = 1;
        }

	rkbuf->rkbuf_size     = size;
        rkbuf->rkbuf_wof      = headersize;
	rkbuf->rkbuf_wof_init = rkbuf->rkbuf_wof;

	rd_kafka_msgq_init(&rkbuf->rkbuf_msgq);

        rd_refcnt_init(&rkbuf->rkbuf_refcnt, 1);

	return rkbuf;
}

/**
 * Create new rkbuf shadowing a memory region in rkbuf_buf2.
 */
rd_kafka_buf_t *rd_kafka_buf_new_shadow (const void *ptr, size_t size) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_calloc(1, sizeof(*rkbuf));

	rkbuf->rkbuf_buf2 = (void *)ptr;
	rkbuf->rkbuf_rbuf = rkbuf->rkbuf_buf2;
	rkbuf->rkbuf_len  = size;
	rkbuf->rkbuf_wof  = size;

	rd_kafka_msgq_init(&rkbuf->rkbuf_msgq);

        rd_refcnt_init(&rkbuf->rkbuf_refcnt, 1);

	return rkbuf;
}

void rd_kafka_bufq_enq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf) {
	TAILQ_INSERT_TAIL(&rkbufq->rkbq_bufs, rkbuf, rkbuf_link);
	(void)rd_atomic32_add(&rkbufq->rkbq_cnt, 1);
	(void)rd_atomic32_add(&rkbufq->rkbq_msg_cnt,
                            rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt));
}

void rd_kafka_bufq_deq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf) {
	TAILQ_REMOVE(&rkbufq->rkbq_bufs, rkbuf, rkbuf_link);
	rd_kafka_assert(NULL, rd_atomic32_get(&rkbufq->rkbq_cnt) > 0);
	(void)rd_atomic32_sub(&rkbufq->rkbq_cnt, 1);
	(void)rd_atomic32_sub(&rkbufq->rkbq_msg_cnt,
                          rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt));
}

void rd_kafka_bufq_init(rd_kafka_bufq_t *rkbufq) {
	TAILQ_INIT(&rkbufq->rkbq_bufs);
	rd_atomic32_set(&rkbufq->rkbq_cnt, 0);
	rd_atomic32_set(&rkbufq->rkbq_msg_cnt, 0);
}

/**
 * Concat all buffers from 'src' to tail of 'dst'
 */
void rd_kafka_bufq_concat (rd_kafka_bufq_t *dst, rd_kafka_bufq_t *src) {
	TAILQ_CONCAT(&dst->rkbq_bufs, &src->rkbq_bufs, rkbuf_link);
	(void)rd_atomic32_add(&dst->rkbq_cnt, rd_atomic32_get(&src->rkbq_cnt));
	(void)rd_atomic32_add(&dst->rkbq_msg_cnt, rd_atomic32_get(&src->rkbq_msg_cnt));
	rd_kafka_bufq_init(src);
}

/**
 * Purge the wait-response queue.
 * NOTE: 'rkbufq' must be a temporary queue and not one of rkb_waitresps
 *       or rkb_outbufs since buffers may be re-enqueued on those queues.
 *       'rkbufq' needs to be bufq_init():ed before reuse after this call.
 */
void rd_kafka_bufq_purge (rd_kafka_broker_t *rkb,
                          rd_kafka_bufq_t *rkbufq,
                          rd_kafka_resp_err_t err) {
	rd_kafka_buf_t *rkbuf, *tmp;

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	rd_rkb_dbg(rkb, QUEUE, "BUFQ", "Purging bufq with %i buffers",
		   rd_atomic32_get(&rkbufq->rkbq_cnt));

	TAILQ_FOREACH_SAFE(rkbuf, &rkbufq->rkbq_bufs, rkbuf_link, tmp) {
                rd_kafka_buf_callback(rkb->rkb_rk, rkb, err, NULL, rkbuf);
        }
}



size_t rd_kafka_buf_write_Message (rd_kafka_buf_t *rkbuf,
				   int64_t Offset, int8_t MagicByte,
				   int8_t Attributes,
				   const rd_kafkap_bytes_t *key,
				   const void *payload, int32_t len,
				   int *outlenp) {
	int32_t MessageSize;
	size_t begin_of;
	size_t of_Crc;

	/*
	 * MessageSet's per-Message header.
	 */
	/* Offset */
	begin_of = rd_kafka_buf_write_i64(rkbuf, Offset);

	/* MessageSize */
	MessageSize = (4 + 1 + 1 + /* Crc+MagicByte+Attributes */
		       RD_KAFKAP_BYTES_SIZE(key) +
		       4 /* Message.ValueLength */ +
		       len /* Value length */);
	rd_kafka_buf_write_i32(rkbuf, MessageSize);

	/*
	 * Message
	 */
	/* Crc: will be updated later */
	of_Crc = rd_kafka_buf_write_i32(rkbuf, 0);

	/* Start Crc calculation of all buf writes. */
	rd_kafka_buf_crc_init(rkbuf);

	/* MagicByte */
	rd_kafka_buf_write_i8(rkbuf, MagicByte);

	/* Attributes */
	rd_kafka_buf_write_i8(rkbuf, Attributes);

	/* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

	/* Message Key */
	rd_kafka_buf_push_kbytes(rkbuf, key);

	/* Value(payload) length */
	rd_kafka_buf_write_i32(rkbuf, payload ? len : RD_KAFKAP_BYTES_LEN_NULL);

	/* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

	/* Value */
	if (payload)
		rd_kafka_buf_push(rkbuf, payload, len);

	/* Finalize Crc */
	rd_kafka_buf_update_u32(rkbuf, of_Crc,
				rd_kafka_buf_crc_finalize(rkbuf));


	if (outlenp)
		*outlenp = 8/*Offset*/ + 4/*MessageSize*/ + MessageSize;

	return begin_of;
}


/**
 * Retry failed request, depending on the error.
 * Returns 1 if the request was scheduled for retry, else 0.
 */
int rd_kafka_buf_retry (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf) {

	/* FIXME: remove err ^ */

        if (unlikely(rkb->rkb_source == RD_KAFKA_INTERNAL ||
		     rd_kafka_terminating(rkb->rkb_rk) ||
		     rkbuf->rkbuf_retries + 1 >
		     rkb->rkb_rk->rk_conf.max_retries))
                return 0;

	/* Try again */
	rkbuf->rkbuf_ts_sent = 0;
	rkbuf->rkbuf_retries++;
	rd_kafka_buf_keep(rkbuf);
	rd_kafka_broker_buf_retry(rkb, rkbuf);
	return 1;
}


/**
 * Handle RD_KAFKA_OP_RECV_BUF
 */
void rd_kafka_buf_handle_op (rd_kafka_op_t *rko) {
        rd_kafka_buf_t *request, *response;

        request = rko->rko_rkbuf;

        rd_kafka_q_destroy(request->rkbuf_replyq);
        request->rkbuf_replyq = NULL;

        /* Let buf_callback() do destroy()s */
        response = request->rkbuf_response; /* May be NULL */
        request->rkbuf_response = NULL;
        rko->rko_rkbuf = NULL;

        rd_kafka_buf_callback(request->rkbuf_rkb->rkb_rk,
			      request->rkbuf_rkb, rko->rko_err,
                              response, request);
}



/**
 * Call request.rkbuf_cb(), but:
 *  - if the rkbuf has a rkbuf_replyq the buffer is enqueued on that queue
 *    with op type RD_KAFKA_OP_RECV_BUF.
 *  - else call rkbuf_cb().
 *
 * \p response may be NULL.
 *
 * Will decrease refcount for both response and request, eventually.
 */
void rd_kafka_buf_callback (rd_kafka_t *rk,
			    rd_kafka_broker_t *rkb, rd_kafka_resp_err_t err,
                            rd_kafka_buf_t *response, rd_kafka_buf_t *request) {

        /* Decide if the request should be retried.
         * This is always done in the originating broker thread. */
        if (unlikely(err && rd_kafka_buf_retry(rkb, request)))
                return;

        if (request->rkbuf_replyq) {
                rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_RECV_BUF);

		rd_kafka_assert(NULL, !request->rkbuf_response);
		request->rkbuf_response = response;

                /* Increment refcnt since rko_rkbuf will be decref:ed
                 * if q_enq() fails and we dont want the rkbuf gone in that
                 * case. */
                rd_kafka_buf_keep(request);
                rko->rko_rkbuf = request;

                rko->rko_err = err;
                if (likely(rd_kafka_q_enq(request->rkbuf_replyq, rko))) {
                        /* Enqueued */
                        rd_kafka_buf_destroy(request); /* from keep above */
                        return;
                }

                /* Enqueue failed because replyq is disabled,
                 * fall through to let callback clean up. */
                err = RD_KAFKA_RESP_ERR__DESTROY;
		request->rkbuf_response = NULL;
                rd_kafka_q_destroy(request->rkbuf_replyq);
                request->rkbuf_replyq = NULL;
        }

        if (request->rkbuf_cb)
                request->rkbuf_cb(rk, rkb, err, response, request,
                                  request->rkbuf_opaque);

        rd_kafka_buf_destroy(request);
	if (response)
		rd_kafka_buf_destroy(response);
}


/**
 * Create new Kafka bytes object from a buffer.
 * The buffer must only have one iovec.
 */
rd_kafkap_bytes_t *rd_kafkap_bytes_from_buf (const rd_kafka_buf_t *rkbuf) {
        rd_kafka_assert(NULL, rkbuf->rkbuf_msg.msg_iovlen == 1);
        rd_kafka_assert(NULL, rkbuf->rkbuf_wof < INT32_MAX);
        return rd_kafkap_bytes_new(rkbuf->rkbuf_wbuf, (int32_t) rkbuf->rkbuf_wof);
}


void rd_kafka_buf_hexdump (const char *what, const rd_kafka_buf_t *rkbuf,
			   int read_buffer) {

	rd_hexdump(stdout, what,
		   read_buffer ? rkbuf->rkbuf_rbuf : rkbuf->rkbuf_buf,
		   rkbuf->rkbuf_wof);
}
