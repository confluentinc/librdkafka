/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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


#define __need_IOV_MAX
#define _GNU_SOURCE
#include <stdio.h>
#include <sys/socket.h>
#include <stdarg.h>
#include <syslog.h>
#include <string.h>

#include <zlib.h>

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdkafka_offset.h"
#include "rdtime.h"
#include "rdthread.h"
#include "rdcrc32.h"
#include "rdrand.h"
#include "rdgz.h"
#include "snappy.h"
#include "endian_compat.h"

const char *rd_kafka_broker_state_names[] = {
	"INIT",
	"DOWN",
	"UP"
};


static void rd_kafka_broker_update (rd_kafka_t *rk,
				    const char *name,
				    uint16_t port,
				    uint32_t nodeid);

static int rd_kafka_send (rd_kafka_broker_t *rkb);

static void msghdr_print (rd_kafka_t *rk,
			  const char *what, const struct msghdr *msg,
			  int hexdump) RD_UNUSED;
static void msghdr_print (rd_kafka_t *rk,
			  const char *what, const struct msghdr *msg,
			  int hexdump) {
	int i;

	rd_kafka_dbg(rk, MSG, "MSG", "%s: iovlen %zd",
		     what, (size_t)msg->msg_iovlen);

	for (i = 0 ; i < msg->msg_iovlen ; i++) {
		rd_kafka_dbg(rk, MSG, what,
			     " iov #%i: %zd",
			     i, msg->msg_iov[i].iov_len);
		rd_hexdump(stdout, what, msg->msg_iov[i].iov_base,
			   msg->msg_iov[i].iov_len);
	}
}


static size_t rd_kafka_msghdr_size (const struct msghdr *msg) {
	int i;
	size_t tot = 0;

	for (i = 0 ; i < msg->msg_iovlen ; i++)
		tot += msg->msg_iov[i].iov_len;

	return tot;
}

/**
 * Locks: rd_kafka_broker_lock() MUST be held.
 * Locality: broker thread
 */
static void rd_kafka_broker_set_state (rd_kafka_broker_t *rkb,
				       int state) {
	if (rkb->rkb_state == state)
		return;

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "STATE",
		     "%s: Broker changed state %s -> %s",
		     rkb->rkb_name,
		     rd_kafka_broker_state_names[rkb->rkb_state],
		     rd_kafka_broker_state_names[state]);

	if (state == RD_KAFKA_BROKER_STATE_DOWN) {
		/* Propagate ALL_BROKERS_DOWN event if all brokers are
		 * now down, unless we're terminating. */
		if (rd_atomic_add(&rkb->rkb_rk->rk_broker_down_cnt, 1) ==
		    rkb->rkb_rk->rk_broker_cnt &&
		    !rkb->rkb_rk->rk_terminate)
			rd_kafka_op_err(rkb->rkb_rk,
					RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
					"%i/%i brokers are down",
					rkb->rkb_rk->rk_broker_down_cnt,
					rkb->rkb_rk->rk_broker_cnt);
	} else if (rkb->rkb_state == RD_KAFKA_BROKER_STATE_DOWN)
		rd_atomic_sub(&rkb->rkb_rk->rk_broker_down_cnt, 1);

	rkb->rkb_state = state;
}



void rd_kafka_buf_destroy (rd_kafka_buf_t *rkbuf) {

	if (rd_atomic_sub(&rkbuf->rkbuf_refcnt, 1) > 0)
		return;

	if (rkbuf->rkbuf_buf2)
		free(rkbuf->rkbuf_buf2);

	if (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FREE && rkbuf->rkbuf_buf)
		free(rkbuf->rkbuf_buf);

	free(rkbuf);
}

static void rd_kafka_buf_auxbuf_add (rd_kafka_buf_t *rkbuf, void *auxbuf) {
	assert(rkbuf->rkbuf_buf2 == NULL);
	rkbuf->rkbuf_buf2 = auxbuf;
}

static void rd_kafka_buf_rewind (rd_kafka_buf_t *rkbuf, int iovindex) {
	rkbuf->rkbuf_msg.msg_iovlen = iovindex;
}


static struct iovec *rd_kafka_buf_iov_next (rd_kafka_buf_t *rkbuf) {
	
	assert(rkbuf->rkbuf_msg.msg_iovlen + 1 <= rkbuf->rkbuf_iovcnt);
	return &rkbuf->rkbuf_iov[rkbuf->rkbuf_msg.msg_iovlen++];
}

/**
 * Pushes 'buf' & 'len' onto the previously allocated iov stack for 'rkbuf'.
 */
static void rd_kafka_buf_push (rd_kafka_buf_t *rkbuf, void *buf, size_t len) {
	struct iovec *iov;

	iov = rd_kafka_buf_iov_next(rkbuf);

	iov->iov_base = buf;
	iov->iov_len = len;
}


#define RD_KAFKA_HEADERS_IOV_CNT   2
#define RD_KAFKA_PAYLOAD_IOV_MAX  (IOV_MAX-RD_KAFKA_HEADERS_IOV_CNT)

static rd_kafka_buf_t *rd_kafka_buf_new (int iovcnt, size_t size) {
	rd_kafka_buf_t *rkbuf;
	const int iovcnt_fixed = RD_KAFKA_HEADERS_IOV_CNT;
	size_t iovsize = sizeof(struct iovec) * (iovcnt+iovcnt_fixed);
	size_t fullsize = iovsize + sizeof(*rkbuf) + size;

	rkbuf = malloc(fullsize);
	memset(rkbuf, 0, sizeof(*rkbuf));

	rkbuf->rkbuf_iov = (struct iovec *)(rkbuf+1);
	rkbuf->rkbuf_iovcnt = (iovcnt+iovcnt_fixed);
	assert(rkbuf->rkbuf_iovcnt <= IOV_MAX);
	rkbuf->rkbuf_msg.msg_iov = rkbuf->rkbuf_iov;

	/* save the first two iovecs for the header + clientid */
	rkbuf->rkbuf_msg.msg_iovlen = iovcnt_fixed;
	memset(rkbuf->rkbuf_iov, 0, sizeof(*rkbuf->rkbuf_iov) * iovcnt_fixed);

	rkbuf->rkbuf_size = size;
	rkbuf->rkbuf_buf = ((char *)(rkbuf+1))+iovsize;

	rd_kafka_msgq_init(&rkbuf->rkbuf_msgq);

	rd_kafka_buf_keep(rkbuf);

	return rkbuf;
}

/**
 * Create new rkbuf shadowing a memory region in rkbuf_buf2.
 */
static rd_kafka_buf_t *rd_kafka_buf_new_shadow (void *ptr, size_t size) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = calloc(1, sizeof(*rkbuf));

	rkbuf->rkbuf_buf2 = ptr;
	rkbuf->rkbuf_len  = size;

	rd_kafka_msgq_init(&rkbuf->rkbuf_msgq);

	rd_kafka_buf_keep(rkbuf);

	return rkbuf;
}

static void rd_kafka_bufq_enq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf) {
	TAILQ_INSERT_TAIL(&rkbufq->rkbq_bufs, rkbuf, rkbuf_link);
	(void)rd_atomic_add(&rkbufq->rkbq_cnt, 1);
}

static void rd_kafka_bufq_deq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf) {
	TAILQ_REMOVE(&rkbufq->rkbq_bufs, rkbuf, rkbuf_link);
	assert(rkbufq->rkbq_cnt > 0);
	(void)rd_atomic_sub(&rkbufq->rkbq_cnt, 1);
}

static void rd_kafka_bufq_init (rd_kafka_bufq_t *rkbufq) {
	TAILQ_INIT(&rkbufq->rkbq_bufs);
	rkbufq->rkbq_cnt = 0;
}

/**
 * Concat all buffers from 'src' to tail of 'dst'
 */
static void rd_kafka_bufq_concat (rd_kafka_bufq_t *dst, rd_kafka_bufq_t *src) {
	TAILQ_CONCAT(&dst->rkbq_bufs, &src->rkbq_bufs, rkbuf_link);
	(void)rd_atomic_add(&dst->rkbq_cnt, src->rkbq_cnt);
	rd_kafka_bufq_init(src);
}

/**
 * Purge the wait-response queue.
 * NOTE: 'rkbufq' must be a temporary queue and not one of rkb_waitresps
 *       or rkb_outbufs since buffers may be re-enqueued on those queues.
 */
static void rd_kafka_bufq_purge (rd_kafka_broker_t *rkb,
				 rd_kafka_bufq_t *rkbufq,
				 rd_kafka_resp_err_t err) {
	rd_kafka_buf_t *rkbuf, *tmp;

	assert(pthread_self() == rkb->rkb_thread);

	rd_rkb_dbg(rkb, QUEUE, "BUFQ", "Purging bufq with %i buffers",
		   rkbufq->rkbq_cnt);

	TAILQ_FOREACH_SAFE(rkbuf, &rkbufq->rkbq_bufs, rkbuf_link, tmp)
		rkbuf->rkbuf_cb(rkb, err, NULL, rkbuf, rkbuf->rkbuf_opaque);
}


/**
 * Scan the wait-response queue for message timeouts.
 */
static void rd_kafka_broker_waitresp_timeout_scan (rd_kafka_broker_t *rkb,
						   rd_ts_t now) {
	rd_kafka_buf_t *rkbuf, *tmp;
	int cnt = 0;

	assert(pthread_self() == rkb->rkb_thread);

	TAILQ_FOREACH_SAFE(rkbuf,
			   &rkb->rkb_waitresps.rkbq_bufs, rkbuf_link, tmp) {
		if (likely(rkbuf->rkbuf_ts_timeout > now))
			continue;

		rd_kafka_bufq_deq(&rkb->rkb_waitresps, rkbuf);

		rkbuf->rkbuf_cb(rkb, RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
				NULL, rkbuf, rkbuf->rkbuf_opaque);
		cnt++;
	}

	if (cnt > 0)
		rd_rkb_dbg(rkb, MSG, "REQTMOUT", "Timed out %i requests", cnt);
}


/**
 * Failure propagation to application.
 * Will tear down connection to broker and trigger a reconnect.
 *
 * If 'fmt' is NULL nothing will be logged or propagated to the application.
 * 
 * Locality: Broker thread
 */
static void rd_kafka_broker_fail (rd_kafka_broker_t *rkb,
				  rd_kafka_resp_err_t err,
				  const char *fmt, ...) {
	va_list ap;
	int errno_save = errno;
	rd_kafka_toppar_t *rktp;
	rd_kafka_bufq_t tmpq;

	assert(pthread_self() == rkb->rkb_thread);
	rd_kafka_broker_lock(rkb);

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "BROKERFAIL",
		     "%s: failed: err: %s: (errno: %s)",
		     rkb->rkb_name, rd_kafka_err2str(err),
		     strerror(errno_save));

	rkb->rkb_err.err = errno_save;

	if (rkb->rkb_s != -1) {
		close(rkb->rkb_s);
		rkb->rkb_s = -1;
		rkb->rkb_pfd.fd = rkb->rkb_s;
	}

	if (rkb->rkb_recv_buf) {
		rd_kafka_buf_destroy(rkb->rkb_recv_buf);
		rkb->rkb_recv_buf = NULL;
	}

	/* The caller may omit the format if it thinks this is a recurring
	 * failure, in which case the following things are omitted:
	 *  - log message
	 *  - application OP_ERR
	 *  - metadata request
	 */
	if (fmt) {
		int of;

		/* Insert broker name in log message if it fits. */
		of = snprintf(rkb->rkb_err.msg, sizeof(rkb->rkb_err.msg),
			      "%s: ", rkb->rkb_name);
		if (of >= sizeof(rkb->rkb_err.msg))
			of = 0;
		va_start(ap, fmt);
		vsnprintf(rkb->rkb_err.msg+of,
			  sizeof(rkb->rkb_err.msg)-of, fmt, ap);
		va_end(ap);

		rd_kafka_log(rkb->rkb_rk, LOG_ERR, "FAIL",
			     "%s", rkb->rkb_err.msg);

		/* Send ERR op back to application for processing. */
		rd_kafka_op_err(rkb->rkb_rk, err,
				"%s", rkb->rkb_err.msg);
	}

	/* Set broker state */
	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_DOWN);

	/*
	 * Purge all buffers
	 * (put on a temporary queue since bufs may be requeued)
	 */
	rd_kafka_bufq_init(&tmpq);
	rd_kafka_bufq_concat(&tmpq, &rkb->rkb_waitresps);
	rd_kafka_bufq_concat(&tmpq, &rkb->rkb_outbufs);

	/* Unlock broker since a requeue will try to lock it. */
	rd_kafka_broker_unlock(rkb);

	/* Purge the buffers */
	rd_kafka_bufq_purge(rkb, &tmpq, err);


	/* Undelegate all toppars from this broker. */
	rd_kafka_broker_toppars_wrlock(rkb);
	while ((rktp = TAILQ_FIRST(&rkb->rkb_toppars))) {
		rd_kafka_topic_t *rkt = rktp->rktp_rkt;

		rd_kafka_topic_keep(rkt); /* Hold on to rkt */
		rd_kafka_toppar_keep(rktp);
		rd_kafka_broker_toppars_unlock(rkb);
		rd_rkb_dbg(rkb, TOPIC, "BRKTP",
			   "Undelegating %.*s [%"PRId32"]",
			   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			   rktp->rktp_partition);

		rd_kafka_topic_wrlock(rktp->rktp_rkt);
		/* Undelegate */
		rd_kafka_toppar_broker_delegate(rktp, NULL);
		rd_kafka_topic_unlock(rktp->rktp_rkt);

		rd_kafka_toppar_destroy(rktp);
		rd_kafka_topic_destroy0(rkt); /* Let go of rkt */

		rd_kafka_broker_toppars_wrlock(rkb);

	}
	rd_kafka_broker_toppars_unlock(rkb);

	/* Query for the topic leaders (async) */
	if (fmt && err != RD_KAFKA_RESP_ERR__DESTROY)
		rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);
}

static ssize_t rd_kafka_broker_send (rd_kafka_broker_t *rkb,
				     const struct msghdr *msg) {
	ssize_t r;

	assert(rkb->rkb_state >= RD_KAFKA_BROKER_STATE_UP);
	assert(rkb->rkb_s != -1);

	r = sendmsg(rkb->rkb_s, msg, MSG_DONTWAIT
#ifdef MSG_NOSIGNAL
		    |MSG_NOSIGNAL
#endif
		);
	if (r == -1) {
		if (errno == EAGAIN)
			return 0;

		rd_kafka_dbg(rkb->rkb_rk, BROKER, "BRKSEND",
			     "sendmsg FAILED for iovlen %zd (%i)",
			     (size_t)msg->msg_iovlen,
			     IOV_MAX);
		rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
				     "Send failed: %s", strerror(errno));
		rkb->rkb_c.tx_err++;
		return -1;
	}

	(void)rd_atomic_add(&rkb->rkb_c.tx_bytes, r);
	(void)rd_atomic_add(&rkb->rkb_c.tx, 1);
	return r;
}




static int rd_kafka_broker_resolve (rd_kafka_broker_t *rkb) {
	const char *errstr;

	if (rkb->rkb_rsal &&
	    rkb->rkb_t_rsal_last + rkb->rkb_rk->rk_conf.broker_addr_ttl <
	    time(NULL)) {
		/* Address list has expired. */
		rd_sockaddr_list_destroy(rkb->rkb_rsal);
		rkb->rkb_rsal = NULL;
	}

	if (!rkb->rkb_rsal) {
		/* Resolve */

		rkb->rkb_rsal = rd_getaddrinfo(rkb->rkb_nodename, 
					       RD_KAFKA_PORT_STR,
					       AI_ADDRCONFIG,
					       AF_UNSPEC, SOCK_STREAM,
					       IPPROTO_TCP, &errstr);

		if (!rkb->rkb_rsal) {
			char tmp[512];

			snprintf(tmp, sizeof(tmp),
				 "Failed to resolve '%s': %s",
				 rkb->rkb_nodename, errstr);

			/* Send ERR op back to application for processing. */
			rd_kafka_op_err(rkb->rkb_rk,RD_KAFKA_RESP_ERR__RESOLVE,
					"%s", tmp);

			rd_rkb_log(rkb, LOG_ERR, "GETADDR", "%s", tmp);
			return -1;
		}
	}

	return 0;
}


static void rd_kafka_broker_buf_enq0 (rd_kafka_broker_t *rkb,
				      rd_kafka_buf_t *rkbuf, int at_head) {
	assert(pthread_self() == rkb->rkb_thread);

	if (unlikely(at_head)) {
		/* Insert message at head of queue */
		rd_kafka_buf_t *prev;

		/* Put us behind any flash messages. */
		TAILQ_FOREACH(prev, &rkb->rkb_outbufs.rkbq_bufs, rkbuf_link)
			if (!(prev->rkbuf_flags & RD_KAFKA_OP_F_FLASH))
				break;

		if (prev)
			TAILQ_INSERT_AFTER(&rkb->rkb_outbufs.rkbq_bufs,
					   prev, rkbuf, rkbuf_link);
		else
			TAILQ_INSERT_HEAD(&rkb->rkb_outbufs.rkbq_bufs,
					  rkbuf, rkbuf_link);
	} else {
		/* Insert message at tail of queue */
		TAILQ_INSERT_TAIL(&rkb->rkb_outbufs.rkbq_bufs,
				  rkbuf, rkbuf_link);
	}

	(void)rd_atomic_add(&rkb->rkb_outbufs.rkbq_cnt, 1);
}


static void rd_kafka_broker_buf_enq1 (rd_kafka_broker_t *rkb,
				      int16_t ApiKey,
				      rd_kafka_buf_t *rkbuf,
				      void (*reply_cb) (
					      rd_kafka_broker_t *,
					      rd_kafka_resp_err_t err,
					      rd_kafka_buf_t *reply,
					      rd_kafka_buf_t *request,
					      void *opaque),
				      void *opaque) {

	assert(pthread_self() == rkb->rkb_thread);

	rkbuf->rkbuf_corrid = ++rkb->rkb_corrid;

	/* Header */
	rkbuf->rkbuf_reqhdr.ApiKey = htons(ApiKey);
	rkbuf->rkbuf_reqhdr.ApiVersion = 0;
	rkbuf->rkbuf_reqhdr.CorrId = htonl(rkbuf->rkbuf_corrid);

	rkbuf->rkbuf_iov[0].iov_base = &rkbuf->rkbuf_reqhdr;
	rkbuf->rkbuf_iov[0].iov_len  = sizeof(rkbuf->rkbuf_reqhdr);

	/* Header ClientId */
	rkbuf->rkbuf_iov[1].iov_base = rkb->rkb_rk->rk_clientid;
	rkbuf->rkbuf_iov[1].iov_len =
		RD_KAFKAP_STR_SIZE(rkb->rkb_rk->rk_clientid);

	rkbuf->rkbuf_cb = reply_cb;
	rkbuf->rkbuf_opaque = opaque;

	rkbuf->rkbuf_ts_enq = rd_clock();

	/* Calculate total message buffer length. */
	rkbuf->rkbuf_of          = 0;
	rkbuf->rkbuf_len         = rd_kafka_msghdr_size(&rkbuf->rkbuf_msg);
	rkbuf->rkbuf_reqhdr.Size = ntohl(rkbuf->rkbuf_len-4);

	rd_kafka_broker_buf_enq0(rkb, rkbuf,
				 (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FLASH)?
				 1/*head*/: 0/*tail*/);
}


static void rd_kafka_broker_buf_enq (rd_kafka_broker_t *rkb,
				     int16_t ApiKey,
				     char *buf, int32_t size, int flags,
				     void (*reply_cb) (
					     rd_kafka_broker_t *,
					     int err,
					     rd_kafka_buf_t *reply,
					     rd_kafka_buf_t *request,
					     void *opaque),
				     void *opaque) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_kafka_buf_new(1, flags & RD_KAFKA_OP_F_FREE ? 0 : size);
	rkbuf->rkbuf_ts_timeout = rd_clock() + 
		rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;
	rkbuf->rkbuf_flags |= flags;

	if (size > 0) {
		if (!(flags & RD_KAFKA_OP_F_FREE)) {
			/* Duplicate data */
			memcpy(rkbuf->rkbuf_buf, buf, size);
			buf = rkbuf->rkbuf_buf;
		} else {
			rkbuf->rkbuf_buf = buf;
			rkbuf->rkbuf_size = size;
		}

		rd_kafka_buf_push(rkbuf, buf, size);
	}


	rd_kafka_broker_buf_enq1(rkb, ApiKey, rkbuf, reply_cb, opaque);
}



/**
 * Memory reading helper macros to be used when parsing network responses.
 *
 * Assumptions:
 *   - data base pointer is in 'char *buf'
 *   - data total size is in 'size_t size'
 *   - current read offset is in 'size_t of' which must be initialized to 0.
 *   - the broker the message was received from must be 'rkb'
 *   - an 'err:' label must be available for error bailouts.
 */

#define _FAIL(fmt...) do {						\
		rd_rkb_dbg(rkb, BROKER, "PROTOERR",			\
			   "Protocol parse failure at %s:%i", \
			   __FUNCTION__, __LINE__);			\
		rd_rkb_dbg(rkb, BROKER, "PROTOERR", fmt);		\
		goto err;						\
	} while (0)

#define _REMAIN() (size - of)

#define _CHECK_LEN(len) do {						\
		int _LEN = (int)(len);					\
	if (unlikely(_LEN > _REMAIN())) {				\
		_FAIL("expected %i bytes > %i remaining bytes",		\
		      _LEN, (int)_REMAIN());				\
		goto err;						\
	}								\
	} while (0)

#define _SKIP(len) do {				\
		_CHECK_LEN(len);		\
		of += (len);			\
	} while (0)

#define _READ(dstptr,len) do {			\
		_CHECK_LEN(len);		\
		memcpy((dstptr), buf+(of), (len));	\
		of += (len);				\
	} while (0)

#define _READ_I64(dstptr) do {						\
		_READ(dstptr, 8);					\
		*(int64_t *)(dstptr) = be64toh(*(int64_t *)(dstptr));	\
	} while (0)

#define _READ_I32(dstptr) do {						\
		_READ(dstptr, 4);					\
		*(int32_t *)(dstptr) = ntohl(*(int32_t *)(dstptr));	\
	} while (0)

#define _READ_I16(dstptr) do {						\
		_READ(dstptr, 2);					\
		*(int16_t *)(dstptr) = ntohs(*(int16_t *)(dstptr));	\
	} while (0)

/* Read Kafka String representation (2+N) */
#define _READ_STR(kstr) do {					\
		int _klen;					\
		_CHECK_LEN(2);					\
		kstr = (rd_kafkap_str_t *)((char *)buf+of);	\
		_klen = RD_KAFKAP_STR_SIZE(kstr);		\
		of += _klen;					\
	} while (0)

/* Read Kafka Bytes representation (4+N) */
#define _READ_BYTES(kbytes) do {				\
		int32_t _klen;					\
		_CHECK_LEN(4);					\
		kbytes = (rd_kafkap_bytes_t *)((char *)buf+of);	\
		_klen = RD_KAFKAP_BYTES_SIZE(kbytes);		\
		of += (_klen);					\
	} while (0)

/* Reference memory, dont copy it */
#define _READ_REF(dstptr,len) do {			\
		_CHECK_LEN(len);			\
		(dstptr) = (void *)((char *)buf+of);	\
		of += (len);				\
	} while(0)



/**
 * Handle a Metadata response message.
 * If 'rkt' is non-NULL the metadata originated from a topic-specific request.
 *
 * Locality: broker thread
 */
static void rd_kafka_metadata_handle (rd_kafka_broker_t *rkb,
				      rd_kafka_topic_t *req_rkt,
				      const char *buf, size_t size) {
	struct {
		int32_t          NodeId;
		rd_kafkap_str_t *Host;
		int32_t          Port;
	}      *Brokers = NULL;
	int32_t Broker_cnt;
	struct rd_kafka_TopicMetadata *TopicMetadata = NULL;
	int32_t TopicMetadata_cnt;
	int i, j, k;
	int of = 0;
	int req_rkt_seen = 0;


	/* Read Brokers */
	_READ_I32(&Broker_cnt);
	if (Broker_cnt > RD_KAFKAP_BROKERS_MAX)
		_FAIL("Broker_cnt %"PRId32" > BROKERS_MAX %i",
		      Broker_cnt, RD_KAFKAP_BROKERS_MAX);

	Brokers = malloc(sizeof(*Brokers) * Broker_cnt);

	for (i = 0 ; i < Broker_cnt ; i++) {
		_READ_I32(&Brokers[i].NodeId);
		_READ_STR(Brokers[i].Host);
		_READ_I32(&Brokers[i].Port);
	}



	/* Read TopicMetadata */
	_READ_I32(&TopicMetadata_cnt);
	rd_rkb_dbg(rkb, METADATA, "METADATA", "%"PRId32" brokers, "
		   "%"PRId32" topics", Broker_cnt, TopicMetadata_cnt);

	if (TopicMetadata_cnt > RD_KAFKAP_TOPICS_MAX)
		_FAIL("TopicMetadata_cnt %"PRId32" > TOPICS_MAX %i",
		      TopicMetadata_cnt, RD_KAFKAP_TOPICS_MAX);

	TopicMetadata = malloc(sizeof(*TopicMetadata) * TopicMetadata_cnt);

	for (i = 0 ; i < TopicMetadata_cnt ; i++) {

		_READ_I16(&TopicMetadata[i].ErrorCode);
		_READ_STR(TopicMetadata[i].Name);

		/* PartitionMetadata */
		_READ_I32(&TopicMetadata[i].PartitionMetadata_cnt);
		if (TopicMetadata[i].PartitionMetadata_cnt >
		    RD_KAFKAP_PARTITIONS_MAX)
			_FAIL("TopicMetadata[%i].PartitionMetadata_cnt "
			      "%"PRId32" > PARTITIONS_MAX %i",
			      i, TopicMetadata[i].PartitionMetadata_cnt,
			      RD_KAFKAP_PARTITIONS_MAX);

		TopicMetadata[i].PartitionMetadata =
			alloca(sizeof(*TopicMetadata[i].PartitionMetadata) *
			       TopicMetadata[i].PartitionMetadata_cnt);

		for (j = 0 ; j < TopicMetadata[i].PartitionMetadata_cnt ; j++) {
			_READ_I16(&TopicMetadata[i].PartitionMetadata[j].
				  ErrorCode);
			_READ_I32(&TopicMetadata[i].PartitionMetadata[j].
				  PartitionId);
			_READ_I32(&TopicMetadata[i].PartitionMetadata[j].
				  Leader);

			/* Replicas */
			_READ_I32(&TopicMetadata[i].PartitionMetadata[j].
				  Replicas_cnt);
			if (TopicMetadata[i].PartitionMetadata[j].Replicas_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				_FAIL("TopicMetadata[%i]."
				      "PartitionMetadata[%i].Replicas_cnt "
				      "%"PRId32" > BROKERS_MAX %i",
				      i, j,
				      TopicMetadata[i].PartitionMetadata[j].
				      Replicas_cnt,
				      RD_KAFKAP_BROKERS_MAX);

			TopicMetadata[i].PartitionMetadata[j].Replicas =
				alloca(sizeof(*TopicMetadata[i].
					      PartitionMetadata[j].Replicas)
				       * TopicMetadata[i].PartitionMetadata[j].
				       Replicas_cnt);

			for (k = 0 ; k < TopicMetadata[i].PartitionMetadata[j].
				     Replicas_cnt ; k++)
				_READ_I32(&TopicMetadata[i].
					  PartitionMetadata[j].Replicas[k]);


			/* Isr */
			_READ_I32(&TopicMetadata[i].PartitionMetadata[j].
				  Isr_cnt);
			if (TopicMetadata[i].PartitionMetadata[j].Isr_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				_FAIL("TopicMetadata[%i]."
				      "PartitionMetadata[%i].Isr_cnt %"PRId32
				      " > BROKERS_MAX %i",
				      i, j,
				      TopicMetadata[i].
				      PartitionMetadata[j].Isr_cnt,
				      RD_KAFKAP_BROKERS_MAX);

			TopicMetadata[i].PartitionMetadata[j].Isr =
				alloca(sizeof(*TopicMetadata[i].
					      PartitionMetadata[j].Isr) *
				       TopicMetadata[i].
				       PartitionMetadata[j].Isr_cnt);

			for (k = 0 ; k < TopicMetadata[i].PartitionMetadata[j].
				     Isr_cnt ; k++)
				_READ_I32(&TopicMetadata[i].
					  PartitionMetadata[j].Isr[k]);
		}
	}

	/* Update our list of brokers. */
	for (i = 0 ; i < Broker_cnt ; i++) {
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "  Broker #%i/%i: %.*s:%"PRId32" NodeId %"PRId32,
			   i, Broker_cnt, RD_KAFKAP_STR_PR(Brokers[i].Host),
			   Brokers[i].Port, Brokers[i].NodeId);
		rd_kafka_broker_update(rkb->rkb_rk,
				       rd_kafkap_strdupa(Brokers[i].Host),
				       Brokers[i].Port,
				       Brokers[i].NodeId);
	}

	/* Update partition count and leader for each topic we know about */
	for (i = 0 ; i < TopicMetadata_cnt ; i++) {
		if (req_rkt &&
		    !rd_kafkap_str_cmp(TopicMetadata[i].Name,
				       req_rkt->rkt_topic))
			req_rkt_seen++;

		rd_kafka_topic_metadata_update(rkb, &TopicMetadata[i]);
	}


	/* Requested topics not seen in metadata? Propogate to topic code. */
	if (req_rkt) {
		rd_rkb_dbg(rkb, TOPIC, "METADATA",
			   "Requested topic %s %sseen in metadata",
			   req_rkt->rkt_topic->str, req_rkt_seen ? "" : "not ");
		if (!req_rkt_seen)
			rd_kafka_topic_metadata_none(req_rkt);
	}


err:
	if (Brokers)
		free(Brokers);
	if (TopicMetadata)
		free(TopicMetadata);
}


static void rd_kafka_broker_metadata_reply (rd_kafka_broker_t *rkb,
					    int err,
					    rd_kafka_buf_t *reply,
					    rd_kafka_buf_t *request,
					    void *opaque) {
	rd_kafka_topic_t *rkt = opaque;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "===== Received metadata from %s =====",
		   rkb->rkb_name);

	/* Avoid metadata updates when we're terminating. */
	if (rkb->rkb_rk->rk_terminate)
		goto done;

	if (unlikely(err)) {
		/* FIXME: handle error */
		rd_rkb_log(rkb, LOG_WARNING, "METADATA",
			   "Metadata request failed: %s",
			   rd_kafka_err2str(err));
	} else {
		rd_kafka_metadata_handle(rkb, rkt,
					 reply->rkbuf_buf2,
					 reply->rkbuf_len);
	}

done:
	if (rkt) {
                rd_kafka_topic_wrlock(rkt);
                rkt->rkt_flags &= ~RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_unlock(rkt);
		rd_kafka_topic_destroy0(rkt);
        }

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}



/**
 * all_topics := if 1: retreive all topics&partitions from the broker
 *               if 0: just retrieve the topics we know about.
 * rkt        := all_topics=0 && only_rkt is set: only ask for specified topic.
 */
static void rd_kafka_broker_metadata_req (rd_kafka_broker_t *rkb,
					  int all_topics,
					  rd_kafka_topic_t *only_rkt,
					  const char *reason) {
	char *buf;
	size_t of = 0;
	int32_t arrsize = 0;
	size_t tnamelen = 0;
	rd_kafka_topic_t *rkt;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Request metadata for %s: %s",
		   only_rkt ? only_rkt->rkt_topic->str :
		   (all_topics ? "all topics" : "locally known topics"),
		   reason ? : "");

	/* If called from other thread than the broker's own then post an
	 * op for the broker's thread instead since all transmissions must
	 * be performed by the broker thread. */
	if (pthread_self() != rkb->rkb_thread) {
		rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_METADATA_REQ);
                if (only_rkt) {
                        rko->rko_rkt = only_rkt;
                        rd_kafka_topic_keep(only_rkt);
                }
		rd_kafka_q_enq(&rkb->rkb_ops, rko);
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "Request metadata: scheduled: not in broker thread");
		return;
	}

	/* FIXME: Use iovs and ..next here */

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Requesting metadata for %stopics",
		   all_topics ? "all ": "known ");


	if (!only_rkt) {
		/* Push the next intervalled metadata refresh forward since
		 * we are performing one now (which might be intervalled). */
		if (rkb->rkb_rk->rk_conf.metadata_refresh_interval_ms >= 0) {
			if (rkb->rkb_metadata_fast_poll_cnt > 0) {
				/* Fast poll after topic loosings its leader */
				rkb->rkb_metadata_fast_poll_cnt--;
				rkb->rkb_ts_metadata_poll = rd_clock() +
					(rkb->rkb_rk->rk_conf.
					 metadata_refresh_fast_interval_ms *
					 1000);
			} else {
				/* According to configured poll interval */
				rkb->rkb_ts_metadata_poll = rd_clock() +
					(rkb->rkb_rk->rk_conf.
					 metadata_refresh_interval_ms * 1000);
			}
		}
	}


	if (only_rkt || !all_topics) {
		rd_kafka_lock(rkb->rkb_rk);

		/* Calculate size to hold all known topics */
		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			if (only_rkt && only_rkt != rkt)
				continue;

			arrsize++;
			tnamelen += RD_KAFKAP_STR_SIZE(rkt->rkt_topic);
		}
	}
	
	buf = malloc(sizeof(arrsize) + tnamelen);
	arrsize = htonl(arrsize);
	memcpy(buf+of, &arrsize, 4);
	of += 4;


	if (only_rkt || !all_topics) {
		/* Just our locally known topics */
			
		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			int tlen;
			if (only_rkt && only_rkt != rkt)
				continue;
			tlen = RD_KAFKAP_STR_SIZE(rkt->rkt_topic);
			memcpy(buf+of, rkt->rkt_topic, tlen);
			of += tlen;
		}
		rd_kafka_unlock(rkb->rkb_rk);
	}


	if (only_rkt)
		rd_kafka_topic_keep(only_rkt);

	rd_kafka_broker_buf_enq(rkb, RD_KAFKAP_Metadata,
				buf, of,
				RD_KAFKA_OP_F_FREE|RD_KAFKA_OP_F_FLASH,
				rd_kafka_broker_metadata_reply, only_rkt);
}



/**
 * Locks: rd_kafka_lock(rk) MUST be held.
 * Locality: any thread
 */
static rd_kafka_broker_t *rd_kafka_broker_any (rd_kafka_t *rk, int state) {
	rd_kafka_broker_t *rkb;

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if (rkb->rkb_state == state) {
			rd_kafka_broker_keep(rkb);
			rd_kafka_broker_unlock(rkb);
			return rkb;
		}
		rd_kafka_broker_unlock(rkb);
	}

	return NULL;
}


/**
 * Trigger broker metadata query for topic leader.
 * 'rkt' may be NULL to query for all topics.
 */
void rd_kafka_topic_leader_query0 (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
				   int do_rk_lock) {
	rd_kafka_broker_t *rkb;

	if (do_rk_lock)
		rd_kafka_lock(rk);
	if (!(rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP))) {
		if (do_rk_lock)
			rd_kafka_unlock(rk);
		return; /* No brokers are up */
	}
	if (do_rk_lock)
		rd_kafka_unlock(rk);

        if (rkt) {
                rd_kafka_topic_wrlock(rkt);
                /* Avoid multiple leader queries if there is already
                 * an outstanding one waiting for reply. */
                if (rkt->rkt_flags & RD_KAFKA_TOPIC_F_LEADER_QUERY) {
                        rd_kafka_topic_unlock(rkt);
                        rd_kafka_broker_destroy(rkb);
                        return;
                }
                rkt->rkt_flags |= RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_unlock(rkt);
        }

	rd_kafka_broker_metadata_req(rkb, 0, rkt, "leader query");

	/* Release refcnt from rd_kafka_broker_any() */
	rd_kafka_broker_destroy(rkb);
}



/**
 * Find a waitresp (rkbuf awaiting response) by the correlation id.
 */
static rd_kafka_buf_t *rd_kafka_waitresp_find (rd_kafka_broker_t *rkb,
					       int32_t corrid) {
	rd_kafka_buf_t *rkbuf;
	rd_ts_t now = rd_clock();

	assert(pthread_self() == rkb->rkb_thread);

	TAILQ_FOREACH(rkbuf, &rkb->rkb_waitresps.rkbq_bufs, rkbuf_link)
		if (rkbuf->rkbuf_corrid == corrid) {
			rd_kafka_avg_add(&rkb->rkb_rtt_curr,
					 now - rkbuf->rkbuf_ts_sent);

			rd_kafka_bufq_deq(&rkb->rkb_waitresps, rkbuf);
			return rkbuf;
		}
	return NULL;
}




/**
 * Map a response message to a request.
 */
static int rd_kafka_req_response (rd_kafka_broker_t *rkb,
				  rd_kafka_buf_t *rkbuf) {
	rd_kafka_buf_t *req;

	assert(pthread_self() == rkb->rkb_thread);


	/* Find corresponding request message by correlation id */
	if (unlikely(!(req =
		       rd_kafka_waitresp_find(rkb,
					      rkbuf->rkbuf_reshdr.CorrId)))) {
		/* unknown response. probably due to request timeout */
                rkb->rkb_c.rx_corrid_err++;
		rd_rkb_dbg(rkb, BROKER, "RESPONSE",
			   "Response for unknown CorrId %"PRId32" (timed out?)",
			   rkbuf->rkbuf_reshdr.CorrId);
                rd_kafka_buf_destroy(rkbuf);
                return -1;
	}

	/* Call callback. Ownership of 'rkbuf' is delegated to callback. */
	req->rkbuf_cb(rkb, 0, rkbuf, req, req->rkbuf_opaque);

	return 0;
}


/**
 * Rebuilds 'src' into 'dst' starting at byte offset 'of'.
 */
static void rd_kafka_msghdr_rebuild (struct msghdr *dst, size_t dst_len,
				     struct msghdr *src,
				     off_t of) {
	int i;
	size_t len = 0;
	void *iov = dst->msg_iov;
	*dst = *src;
	dst->msg_iov = iov;
	dst->msg_iovlen = 0;

	for (i = 0 ; i < src->msg_iovlen ; i++) {
		off_t vof = of - len;

		if (0)
			printf(" #%i/%zd and %zd: of %jd, len %zd, "
			       "vof %jd: iov %zd\n",
			       i,
			       (size_t)src->msg_iovlen,
			       (size_t)dst->msg_iovlen,
			       (intmax_t)of, len, (intmax_t)vof,
			       src->msg_iov[i].iov_len);
		if (vof < 0)
			vof = 0;

		if (vof < src->msg_iov[i].iov_len) {
			assert(dst->msg_iovlen < dst_len);
			dst->msg_iov[dst->msg_iovlen].iov_base =
				(char *)src->msg_iov[i].iov_base + vof;
			dst->msg_iov[dst->msg_iovlen].iov_len =
				src->msg_iov[i].iov_len - vof;
			dst->msg_iovlen++;
		}

		len += src->msg_iov[i].iov_len;
	}
}



static int rd_kafka_recv (rd_kafka_broker_t *rkb) {
	rd_kafka_buf_t *rkbuf;
	ssize_t r;
	struct msghdr msg;
	char errstr[512];
	rd_kafka_resp_err_t err_code = 0;

	if (0)
	rd_rkb_dbg(rkb, BROKER, "RECV",
		   "%s: Receive on socket %i (buf %p)",
		   rkb->rkb_name, rkb->rkb_s, rkb->rkb_recv_buf);


	/**
	 * The receive buffers are split up in two parts:
	 *   - the first part is mainly for reading the first 4 bytes
	 *     where the remaining length is coded.
	 *     But for short packets we want to avoid a second recv() call
	 *     so the first buffer should be large enough for common short
	 *     packets.
	 *     This is iov[0] and iov[1].
	 *
	 *   - the second part is mainly for data response, this buffer
	 *     must be contigious and will be provided to the application
	 *     as is (Fetch response).
	 *     This is iov[2].
	 *
	 *   It is impossible to estimate the correct size of the first
	 *   buffer, so we make it big enough to probably fit all kinds of
	 *   non-data responses so we dont have to allocate a second buffer
	 *   for such responses. And we make it small enough that a copy
	 *   to the second buffer isn't too costly in case we receive a
	 *   real data packet.
	 *
	 * Minimum packet sizes per response type:
	 *   Metadata: 4+4+2+host+4+4+2+2+topic+2+4+4+4+4+4+4.. =~ 48
	 *   Produce:  4+2+topic+4+4+2+8.. =~ 24
	 *   Fetch:    4+2+topic+4+4+2+8+8+4.. =~ 36
	 *   Offset:   4+2+topic+4+4+2+4+8.. =~ 28
	 *   ...
	 *
	 * Plus 4 + 4 for Size and CorrId.
	 *
	 * First buffer size should thus be: 96 bytes
	 */
	/* FIXME: skip the above, just go for the header. */
	if (!(rkbuf = rkb->rkb_recv_buf)) {
		/* No receive in progress: new message. */

		rkbuf = rd_kafka_buf_new(0, 0);

		rkbuf->rkbuf_iov[0].iov_base = &rkbuf->rkbuf_reshdr;
		rkbuf->rkbuf_iov[0].iov_len = sizeof(rkbuf->rkbuf_reshdr);
		
		rkbuf->rkbuf_msg.msg_iov = rkbuf->rkbuf_iov;
		rkbuf->rkbuf_msg.msg_iovlen = 1;

		msg = rkbuf->rkbuf_msg;

		rkb->rkb_recv_buf = rkbuf;

	} else {
		/* Receive in progress: adjust the msg to allow more data. */
		msg.msg_iov = alloca(sizeof(struct iovec) *
				     rkbuf->rkbuf_iovcnt);
		
		rd_kafka_msghdr_rebuild(&msg, rkbuf->rkbuf_msg.msg_iovlen,
					&rkbuf->rkbuf_msg,
					rkbuf->rkbuf_of);
	}

	assert(rd_kafka_msghdr_size(&msg) > 0);

	if ((r = recvmsg(rkb->rkb_s, &msg, MSG_DONTWAIT)) == -1) {
		if (errno == EAGAIN)
			return 0;
		snprintf(errstr, sizeof(errstr),
			 "Receive error: %s", strerror(errno));
		err_code = RD_KAFKA_RESP_ERR__TRANSPORT;
		rkb->rkb_c.rx_err++;
		goto err;
	}

	if (r == 0) {
		/* Receive 0 after POLLIN event means connection closed. */
		snprintf(errstr, sizeof(errstr), "Disconnected");
		err_code = RD_KAFKA_RESP_ERR__TRANSPORT;
		goto err;
	}

	rkbuf->rkbuf_of += r;

	if (rkbuf->rkbuf_len == 0) {
		/* Packet length not known yet. */

		if (unlikely(rkbuf->rkbuf_of < sizeof(rkbuf->rkbuf_reshdr))) {
			/* Need response header for packet length and corrid.
			 * Wait for more data. */ 
			return 0;
		}

		rkbuf->rkbuf_len = ntohl(rkbuf->rkbuf_reshdr.Size);
		rkbuf->rkbuf_reshdr.CorrId = ntohl(rkbuf->rkbuf_reshdr.CorrId);

		/* Make sure message size is within tolerable limits. */
		if (rkbuf->rkbuf_len < 4/*CorrId*/ ||
		    rkbuf->rkbuf_len > rkb->rkb_rk->rk_conf.recv_max_msg_size) {
			snprintf(errstr, sizeof(errstr),
				 "Invalid message size %zd (0..%i): "
				 "increase receive.message.max.bytes",
				 rkbuf->rkbuf_len-4,
				 rkb->rkb_rk->rk_conf.max_msg_size);
			rkb->rkb_c.rx_err++;
			err_code = RD_KAFKA_RESP_ERR__BAD_MSG;

			goto err;
		}

		rkbuf->rkbuf_len -= 4; /*CorrId*/

		if (rkbuf->rkbuf_len > 0) {
			/* Allocate another buffer that fits all data (short of
			 * the common response header). We want all
			 * data to be in contigious memory. */

			rkbuf->rkbuf_buf2 = malloc(rkbuf->rkbuf_len);
			assert(rkbuf->rkbuf_msg.msg_iovlen == 1);
			rkbuf->rkbuf_iov[1].iov_base = rkbuf->rkbuf_buf2;
			rkbuf->rkbuf_iov[1].iov_len = rkbuf->rkbuf_len;
			rkbuf->rkbuf_msg.msg_iovlen = 2;
		}
	}

	if (rkbuf->rkbuf_of == rkbuf->rkbuf_len + sizeof(rkbuf->rkbuf_reshdr)) {
		/* Message is complete, pass it on to the original requester. */
		rkb->rkb_recv_buf = NULL;
		(void)rd_atomic_add(&rkb->rkb_c.rx, 1);
		(void)rd_atomic_add(&rkb->rkb_c.rx_bytes, rkbuf->rkbuf_of);
		rd_kafka_req_response(rkb, rkbuf);
	}

	return 1;

err:
	rd_kafka_broker_fail(rkb, err_code, "Receive failed: %s", errstr);
	return -1;
}


static int rd_kafka_broker_connect (rd_kafka_broker_t *rkb) {
	rd_sockaddr_inx_t *sinx;
	int one __attribute__((unused)) = 1;

	rd_rkb_dbg(rkb, BROKER, "CONNECT",
		   "broker in state %s connecting",
		   rd_kafka_broker_state_names[rkb->rkb_state]);

	if (rd_kafka_broker_resolve(rkb) == -1)
		return -1;

	sinx = rd_sockaddr_list_next(rkb->rkb_rsal);

	assert(rkb->rkb_s == -1);

	if ((rkb->rkb_s = socket(sinx->sinx_family,
				 SOCK_STREAM, IPPROTO_TCP)) == -1) {
		rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__FAIL,
				     "Failed to create %s socket: %s",
				     rd_family2str(sinx->sinx_family),
				     strerror(errno));
		return -1;
	}

#ifdef SO_NOSIGPIPE
	/* Disable SIGPIPE signalling for this socket on OSX */
	if (setsockopt(rkb->rkb_s, SOL_SOCKET, SO_NOSIGPIPE,
		       &one, sizeof(one)) == -1)
	    rd_rkb_dbg(rkb, BROKER, "SOCKET", "Failed to set SO_NOSIGPIPE: %s",
		       strerror(errno));
#endif

	if (connect(rkb->rkb_s, (struct sockaddr *)sinx,
		    RD_SOCKADDR_INX_LEN(sinx)) == -1) {
		rd_rkb_dbg(rkb, BROKER, "CONNECT",
			   "couldn't connect to %s: %s",
			   rd_sockaddr2str(sinx,
					   RD_SOCKADDR2STR_F_PORT |
					   RD_SOCKADDR2STR_F_FAMILY),
			   strerror(errno));
		/* Avoid duplicate log messages */
		if (rkb->rkb_err.err == errno)
			rd_kafka_broker_fail(rkb,
					     RD_KAFKA_RESP_ERR__FAIL, NULL);
		else
			rd_kafka_broker_fail(rkb,
					     RD_KAFKA_RESP_ERR__TRANSPORT,
					     "Failed to connect to "
					     "broker at %s: %s",
					     rd_sockaddr2str(sinx,
							     RD_SOCKADDR2STR_F_NICE),
					     strerror(errno));
		return -1;
	}

	rd_rkb_dbg(rkb, BROKER, "CONNECTED", "connected to %s",
		   rd_sockaddr2str(sinx, RD_SOCKADDR2STR_F_NICE));

	/* Set socket send & receive buffer sizes if configuerd */
	if (rkb->rkb_rk->rk_conf.socket_sndbuf_size != 0) {
		if (setsockopt(rkb->rkb_s, SOL_SOCKET, SO_SNDBUF,
			       &rkb->rkb_rk->rk_conf.socket_sndbuf_size,
			       sizeof(rkb->rkb_rk->rk_conf.
				      socket_sndbuf_size)) == -1)
			rd_rkb_log(rkb, LOG_WARNING, "SNDBUF",
				   "Failed to set socket send "
				   "buffer size to %i: %s",
				   rkb->rkb_rk->rk_conf.socket_sndbuf_size,
				   strerror(errno));
	}

	if (rkb->rkb_rk->rk_conf.socket_rcvbuf_size != 0) {
		if (setsockopt(rkb->rkb_s, SOL_SOCKET, SO_RCVBUF,
			       &rkb->rkb_rk->rk_conf.socket_rcvbuf_size,
			       sizeof(rkb->rkb_rk->rk_conf.
				      socket_rcvbuf_size)) == -1)
			rd_rkb_log(rkb, LOG_WARNING, "RCVBUF",
				   "Failed to set socket receive "
				   "buffer size to %i: %s",
				   rkb->rkb_rk->rk_conf.socket_rcvbuf_size,
				   strerror(errno));
	}


	rd_kafka_broker_lock(rkb);
	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_UP);
	rkb->rkb_err.err = 0;
	rd_kafka_broker_unlock(rkb);

	rkb->rkb_pfd.fd = rkb->rkb_s;
	rkb->rkb_pfd.events = POLLIN;

	/* Request metadata (async) */
	rd_kafka_broker_metadata_req(rkb, 1 /* all topics */, NULL,
				     "connected");
	return 0;
}



/**
 * Send queued messages to broker
 *
 * Locality: io thread
 */
static int rd_kafka_send (rd_kafka_broker_t *rkb) {
	rd_kafka_buf_t *rkbuf;
	unsigned int cnt = 0;

	assert(pthread_self() == rkb->rkb_thread);

	while (rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP &&
	       (rkbuf = TAILQ_FIRST(&rkb->rkb_outbufs.rkbq_bufs))) {
		ssize_t r;
		struct msghdr *msg = &rkbuf->rkbuf_msg;
		struct msghdr msg2;
		struct iovec iov[IOV_MAX];

		if (rkbuf->rkbuf_of != 0) {
			/* If message has been partially sent we need
			 * to construct a new msg+iovec skipping the
			 * sent bytes. */
			msg2.msg_iov = iov;
			rd_kafka_msghdr_rebuild(&msg2, IOV_MAX,
						&rkbuf->rkbuf_msg,
						rkbuf->rkbuf_of);
			msg = &msg2;
		}

		if (0)
			rd_rkb_dbg(rkb, BROKER, "SEND",
				   "Send buf corrid %"PRId32" at "
				   "offset %zd/%zd",
				   rkbuf->rkbuf_corrid,
				   rkbuf->rkbuf_of, rkbuf->rkbuf_len);

		if ((r = rd_kafka_broker_send(rkb, msg)) == -1) {
			/* FIXME: */
			return -1;
		}

		rkbuf->rkbuf_of += r;

		/* Partial send? Continue next time. */
		if (rkbuf->rkbuf_of < rkbuf->rkbuf_len) {
			return 0;
		}

		/* Entire buffer sent, unlink from outbuf */
		rd_kafka_bufq_deq(&rkb->rkb_outbufs, rkbuf);

		/* Store time for RTT calculation */
		rkbuf->rkbuf_ts_sent = rd_clock();

		/* Put buffer on response wait list unless we are not
		 * expecting a response (required_acks=0). */
		if (!(rkbuf->rkbuf_flags & RD_KAFKA_OP_F_NO_RESPONSE))
			rd_kafka_bufq_enq(&rkb->rkb_waitresps, rkbuf);
		else /* Call buffer callback for delivery report. */
			rkbuf->rkbuf_cb(rkb, 0, NULL, rkbuf,
					rkbuf->rkbuf_opaque);

		cnt++;
	}

	rd_rkb_dbg(rkb, BROKER, "SEND", "Sent %i bufs", cnt);

	return cnt;
}


/**
 * Add 'rkbuf' to broker 'rkb's retry queue.
 */
static void rd_kafka_broker_buf_retry (rd_kafka_broker_t *rkb,
				       rd_kafka_buf_t *rkbuf) {
	
	assert(pthread_self() == rkb->rkb_thread);

	rkb->rkb_c.tx_retries++;

	rkbuf->rkbuf_ts_retry = rd_clock() +
		(rkb->rkb_rk->rk_conf.retry_backoff_ms * 1000);
	/* Reset send offset */
	rkbuf->rkbuf_of = 0;

	rd_kafka_bufq_enq(&rkb->rkb_retrybufs, rkbuf);
}


/**
 * Move buffers that have expired their retry backoff time from the 
 * retry queue to the outbuf.
 */
static void rd_kafka_broker_retry_bufs_move (rd_kafka_broker_t *rkb) {
	rd_ts_t now = rd_clock();
	rd_kafka_buf_t *rkbuf;

	while ((rkbuf = TAILQ_FIRST(&rkb->rkb_retrybufs.rkbq_bufs))) {
		if (rkbuf->rkbuf_ts_retry > now)
			break;

		rd_kafka_bufq_deq(&rkb->rkb_retrybufs, rkbuf);

		rd_kafka_broker_buf_enq0(rkb, rkbuf, 0/*tail*/);
	}
}
	

/**
 * Propagate delivery report for entire message queue.
 */
void rd_kafka_dr_msgq (rd_kafka_t *rk,
		       rd_kafka_msgq_t *rkmq, rd_kafka_resp_err_t err) {

	if (rk->rk_conf.dr_cb) {
		/* Pass all messages to application thread in one op. */
		rd_kafka_op_t *rko;

		rko = rd_kafka_op_new(RD_KAFKA_OP_DR);
		rko->rko_err = err;

		/* Move all messages to op's msgq */
		rd_kafka_msgq_move(&rko->rko_msgq, rkmq);

		rd_kafka_op_reply2(rk, rko);

	} else {
		/* No delivery report callback, destroy the messages
		 * right away. */
		rd_kafka_msgq_purge(rk, rkmq);
	}
}



/**
 * Parses a Produce reply.
 * Returns 0 on success or an error code on failure.
 */
static rd_kafka_resp_err_t
rd_kafka_produce_reply_handle (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf) {
	char *buf = rkbuf->rkbuf_buf2;
	size_t size = rkbuf->rkbuf_len;
	size_t of = 0;
	int32_t TopicArrayCnt;
	rd_kafkap_str_t *topic;
	int32_t PartitionArrayCnt;
	struct {
		int32_t Partition;
		int16_t ErrorCode;
		int64_t Offset;
	} RD_PACKED *hdr;

	_READ_I32(&TopicArrayCnt);
	if (TopicArrayCnt != 1)
		goto err;

	/* Since we only produce to one single topic+partition in each
	 * request we assume that the reply only contains one topic+partition
	 * and that it is the same that we requested.
	 * If not the broker is buggy. */
	_READ_STR(topic);
	_READ_I32(&PartitionArrayCnt);

	if (PartitionArrayCnt != 1)
		goto err;

	_READ_REF(hdr, sizeof(*hdr));

	return ntohs(hdr->ErrorCode);

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}


/**
 * Locality: io thread
 */
static void rd_kafka_produce_msgset_reply (rd_kafka_broker_t *rkb,
					   rd_kafka_resp_err_t err,
					   rd_kafka_buf_t *reply,
					   rd_kafka_buf_t *request,
					   void *opaque) {
	rd_kafka_toppar_t *rktp = opaque;

	rd_rkb_dbg(rkb, MSG, "MSGSET",
		   "MessageSet with %i message(s) %sdelivered",
		   request->rkbuf_msgq.rkmq_msg_cnt, err ? "not ": "");

	/* Parse Produce reply (unless the request errored) */
	if (!err && reply)
		err = rd_kafka_produce_reply_handle(rkb, reply);


	if (err) {
		rd_rkb_dbg(rkb, MSG, "MSGSET", "MessageSet with %i message(s) "
			   "encountered error: %s",
			   request->rkbuf_msgq.rkmq_msg_cnt,
			   rd_kafka_err2str(err));

		switch (err)
		{
		case RD_KAFKA_RESP_ERR__DESTROY:
		case RD_KAFKA_RESP_ERR_INVALID_MSG:
		case RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE:
		case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
			/* Fatal errors: no message transmission retries */
			break;

		case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
			/* Try again */
			if (++request->rkbuf_retries <
			    rkb->rkb_rk->rk_conf.max_retries) {

				if (reply)
					rd_kafka_buf_destroy(reply);

				rd_kafka_broker_buf_retry(rkb, request);
				return;
			}
			break;

		case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
		case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
		case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR__TRANSPORT:
		default:
			/* Request metadata information update */
			rkb->rkb_metadata_fast_poll_cnt =
				rkb->rkb_rk->rk_conf.metadata_refresh_fast_cnt;
			rd_kafka_topic_leader_query(rkb->rkb_rk,
						    rktp->rktp_rkt);

			/* FIXME: Should message retries be incremented? */

			/* Move messages (in the rkbuf) back to the partition's
			 * queue head. They will be resent when a new leader
			 * is delegated. */
			rd_kafka_toppar_insert_msgq(rktp, &request->rkbuf_msgq);
			goto done;
		}

		/* FALLTHRU */
	}


	/* Enqueue messages for delivery report */
	rd_kafka_dr_msgq(rkb->rkb_rk, &request->rkbuf_msgq, err);

done:
	rd_kafka_toppar_destroy(rktp); /* from produce_toppar() */

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}


/**
 * Produce messages from 'rktp's queue.
 */
static int rd_kafka_broker_produce_toppar (rd_kafka_broker_t *rkb,
					   rd_kafka_toppar_t *rktp) {
	int cnt;
	rd_kafka_msg_t *rkm;
	int msgcnt;
	rd_kafka_buf_t *rkbuf;
	rd_kafka_topic_t *rkt = rktp->rktp_rkt;
	struct {
		struct {
			int16_t RequiredAcks;
			int32_t Timeout;
			int32_t TopicArrayCnt;
		} __attribute__((packed)) part1;
		/* TopicName is inserted here */
		struct {
			int32_t PartitionArrayCnt;
			int32_t Partition;
			int32_t MessageSetSize;
		} __attribute__((packed)) part2;
		/* MessageSet headers follows */
	} *prodhdr;
	/* Both MessageSet and Message headers combined: */
	struct {
		struct {
			int64_t Offset;
			int32_t MessageSize;
			int32_t Crc;
			int8_t  MagicByte;
			int8_t  Attributes;
		} __attribute__((packed)) part3;
		struct {
			int32_t Value_len;
		} __attribute__((packed)) part4;
	} *msghdr;
	int iovcnt;
	int iov_firstmsg;

	/* iovs:
	 *  1 * RequiredAcks + Timeout (part1)
	 *  1 * Topic
	 *  1 * Partition + MessageSetSize (part2)
	 *  msgcnt * messagehdr (part3)
	 *  msgcnt * key
	 *  msgcnt * Value_len (part4)
	 *  msgcnt * messagepayload
	 * = 3 + (4 * msgcnt)
	 *
	 * We are bound both by configuration and IOV_MAX
	 */

	if (rktp->rktp_xmit_msgq.rkmq_msg_cnt > 0)
		assert(TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs));
	msgcnt = RD_MIN(rktp->rktp_xmit_msgq.rkmq_msg_cnt,
			rkb->rkb_rk->rk_conf.batch_num_messages);
	assert(msgcnt > 0);
	iovcnt = 3 + (4 * msgcnt);

	if (iovcnt > RD_KAFKA_PAYLOAD_IOV_MAX) {
		iovcnt = RD_KAFKA_PAYLOAD_IOV_MAX;
		msgcnt = ((iovcnt / 4) - 3);
	}

	if (0)
		rd_rkb_dbg(rkb, MSG, "PRODUCE",
			   "Serve %i/%i messages (%i iovecs) "
			   "for %.*s [%"PRId32"] (%"PRIu64" bytes)",
			   msgcnt, rktp->rktp_msgq.rkmq_msg_cnt,
			   iovcnt,
			   RD_KAFKAP_STR_PR(rkt->rkt_topic),
			   rktp->rktp_partition,
			   rktp->rktp_msgq.rkmq_msg_bytes);


	/* Allocate iovecs to hold all headers and messages,
	 * and allocate auxilliery space for the headers. */
	rkbuf = rd_kafka_buf_new(iovcnt,
				 sizeof(*prodhdr) +
				 (sizeof(*msghdr) * msgcnt));
	prodhdr = (void *)rkbuf->rkbuf_buf;
	msghdr = (void *)(prodhdr+1);

	/* Insert first part of Produce header */
	prodhdr->part1.RequiredAcks  = htons(rkt->rkt_conf.required_acks);
	prodhdr->part1.Timeout       = htonl(rkt->rkt_conf.request_timeout_ms);
	prodhdr->part1.TopicArrayCnt = htonl(1);
	rd_kafka_buf_push(rkbuf, &prodhdr->part1, sizeof(prodhdr->part1));

	/* Insert topic */
	rd_kafka_buf_push(rkbuf, rkt->rkt_topic,
			  RD_KAFKAP_STR_SIZE(rkt->rkt_topic));

	/* Insert second part of Produce header */
	prodhdr->part2.PartitionArrayCnt = htonl(1);
	prodhdr->part2.Partition = htonl(rktp->rktp_partition);
	/* Will be finalized later*/
	prodhdr->part2.MessageSetSize = 0;

	rd_kafka_buf_push(rkbuf, &prodhdr->part2, sizeof(prodhdr->part2));

	iov_firstmsg = rkbuf->rkbuf_msg.msg_iovlen;

	while (msgcnt > 0 &&
	       (rkm = TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs))) {

		if (prodhdr->part2.MessageSetSize + rkm->rkm_len >
		    rkb->rkb_rk->rk_conf.max_msg_size) {
			rd_rkb_dbg(rkb, MSG, "PRODUCE",
				   "No more space in current message "
				   "(%i messages)",
				   rkbuf->rkbuf_msgq.rkmq_msg_cnt);
			/* Not enough remaining size. */
			break;
		}
		
		rd_kafka_msgq_deq(&rktp->rktp_xmit_msgq, rkm, 1);

		rd_kafka_msgq_enq(&rkbuf->rkbuf_msgq, rkm);

		msgcnt--;

		/* Message header */
		msghdr->part3.Offset = 0;
		msghdr->part3.MessageSize = 
			(sizeof(msghdr->part3) -
			 sizeof(msghdr->part3.Offset) -
			 sizeof(msghdr->part3.MessageSize)) +
			RD_KAFKAP_BYTES_SIZE(rkm->rkm_key) +
			sizeof(msghdr->part4) +
			rkm->rkm_len;
		prodhdr->part2.MessageSetSize +=
			sizeof(msghdr->part3.Offset) +
			sizeof(msghdr->part3.MessageSize) +
			msghdr->part3.MessageSize;
		msghdr->part3.MessageSize =
			htonl(msghdr->part3.MessageSize);


		msghdr->part3.Crc = rd_crc32_init();
		msghdr->part3.MagicByte = 0;  /* FIXME: what? */
		msghdr->part3.Attributes = 0; /* No compression */

		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					(void *)
					&msghdr->part3.MagicByte,
					sizeof(msghdr->part3.
					       MagicByte) +
					sizeof(msghdr->part3.
					       Attributes));

		/* Message header */
		rd_kafka_buf_push(rkbuf, &msghdr->part3, sizeof(msghdr->part3));


		/* Key */
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					(void *)rkm->rkm_key,
					RD_KAFKAP_BYTES_SIZE(rkm->
							     rkm_key));

		rd_kafka_buf_push(rkbuf, rkm->rkm_key,
				  RD_KAFKAP_BYTES_SIZE(rkm->rkm_key));


		/* Value(payload) length */
		msghdr->part4.Value_len = htonl(rkm->rkm_len);
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					(void *)
					&msghdr->part4.Value_len,
					sizeof(msghdr->part4.
					       Value_len));

		rd_kafka_buf_push(rkbuf, &msghdr->part4, sizeof(msghdr->part4));
			

		/* Payload */
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					rkm->rkm_payload,
					rkm->rkm_len);
		rd_kafka_buf_push(rkbuf, rkm->rkm_payload, rkm->rkm_len);


		/* Finalize Crc */
		msghdr->part3.Crc =
			htonl(rd_crc32_finalize(msghdr->part3.Crc));
		msghdr++;
	}

	/* No messages added, bail out early. */
	if (unlikely(rkbuf->rkbuf_msgq.rkmq_msg_cnt == 0)) {
		rd_kafka_buf_destroy(rkbuf);
		return -1;
	}

	/* Compress the messages */
	if (rkb->rkb_rk->rk_conf.compression_codec) {
		int    siovlen = 1;
		size_t coutlen;
		int r;
		struct {
			int64_t Offset;
			int32_t MessageSize;
			int32_t Crc;
			int8_t  MagicByte;
			int8_t  Attributes;
			int32_t Key_len; /* -1 */
			int32_t Value_len;
		} RD_PACKED *msghdr2 = NULL;
		int32_t ctotlen;
		struct snappy_env senv;
		struct iovec siov;
		z_stream strm;
		int i;

		switch (rkb->rkb_rk->rk_conf.compression_codec) {
		case RD_KAFKA_COMPRESSION_NONE:
			abort(); /* unreachable */
			break;

		case RD_KAFKA_COMPRESSION_GZIP:
			/* Initialize gzip compression */
			memset(&strm, 0, sizeof(strm));
			r = deflateInit2(&strm, Z_DEFAULT_COMPRESSION,
					 Z_DEFLATED, 15+16,
					 8, Z_DEFAULT_STRATEGY);
			if (r != Z_OK) {
				rd_rkb_log(rkb, LOG_ERR, "GZIP",
					   "Failed to initialize gzip for "
					   "compressing %"PRId32" bytes in "
					   "topic %.*s [%"PRId32"]: %s (%i): "
					   "sending uncompressed",
					   prodhdr->part2.MessageSetSize,
					   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
							    rkt_topic),
					   rktp->rktp_partition,
					   strm.msg ? : "", r);
				goto do_send;
			}

			/* Calculate maximum compressed size and
			 * allocate an output buffer accordingly, being
			 * prefixed with the Message header. */
			siov.iov_len = deflateBound(&strm,
						    prodhdr->part2.
						    MessageSetSize);
			msghdr2 = malloc(sizeof(*msghdr2) + siov.iov_len);
			siov.iov_base = (void *)(msghdr2+1);

			strm.next_out = siov.iov_base;
			strm.avail_out = siov.iov_len;

			/* Iterate through each message and compress it. */
			for (i = iov_firstmsg ;
			     i < rkbuf->rkbuf_msg.msg_iovlen ; i++) {

				if (rkbuf->rkbuf_msg.msg_iov[i].iov_len == 0)
					continue;

				strm.next_in = rkbuf->rkbuf_msg.
					msg_iov[i].iov_base;
				strm.avail_in = rkbuf->rkbuf_msg.
					msg_iov[i].iov_len;

				/* Compress message */
				if ((r = deflate(&strm, Z_NO_FLUSH) != Z_OK)) {
					rd_rkb_log(rkb, LOG_ERR, "GZIP",
						   "Failed to gzip-compress "
						   "%zd bytes for "
						   "topic %.*s [%"PRId32"]: "
						   "%s (%i): "
						   "sending uncompressed",
						   rkbuf->rkbuf_msg.msg_iov[i].
						   iov_len,
						   RD_KAFKAP_STR_PR(rktp->
								    rktp_rkt->
								    rkt_topic),
						   rktp->rktp_partition,
						   strm.msg ? : "", r);
					deflateEnd(&strm);
					free(msghdr2);
					goto do_send;
				}

				assert(strm.avail_in == 0);
			}

			/* Finish the compression */
			if ((r = deflate(&strm, Z_FINISH)) != Z_STREAM_END) {
				rd_rkb_log(rkb, LOG_ERR, "GZIP",
					   "Failed to finish gzip compression "
					   " of %"PRId32" bytes for "
					   "topic %.*s [%"PRId32"]: "
					   "%s (%i): "
					   "sending uncompressed",
					   prodhdr->part2.MessageSetSize,
					   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
							    rkt_topic),
					   rktp->rktp_partition,
					   strm.msg ? : "", r);
				deflateEnd(&strm);
				free(msghdr2);
				goto do_send;
			}

			coutlen = strm.total_out;

			/* Deinitialize compression */
			deflateEnd(&strm);
			break;


		case RD_KAFKA_COMPRESSION_SNAPPY:
			/* Initialize snappy compression environment */
			snappy_init_env_sg(&senv, 1/*iov enable*/);

			/* Calculate maximum compressed size and
			 * allocate an output buffer accordingly, being
			 * prefixed with the Message header. */
			siov.iov_len =
				snappy_max_compressed_length(prodhdr->part2.
							     MessageSetSize);
			msghdr2 = malloc(sizeof(*msghdr2) + siov.iov_len);
			siov.iov_base = (void *)(msghdr2+1);

			/* Compress each message */
			if ((r = snappy_compress_iov(&senv,
						     &rkbuf->
						     rkbuf_iov[iov_firstmsg],
						     rkbuf->rkbuf_msg.
						     msg_iovlen -
						     iov_firstmsg,
						     prodhdr->part2.
						     MessageSetSize,
						     &siov, &siovlen,
						     &coutlen)) != 0) {
				rd_rkb_log(rkb, LOG_ERR, "SNAPPY",
					   "Failed to snappy-compress "
					   "%"PRId32" bytes for "
					   "topic %.*s [%"PRId32"]: %s: "
					   "sending uncompressed",
					   prodhdr->part2.MessageSetSize,
					   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
							    rkt_topic),
					   rktp->rktp_partition,
					   strerror(-r));
				free(msghdr2);
				goto do_send;
			}

			/* Free snappy environment */
			snappy_free_env(&senv);

		}

		/* Create a new Message who's Value is the compressed data */
		ctotlen = sizeof(*msghdr2) + coutlen;
		msghdr2->Offset      = 0;
		msghdr2->MessageSize = htonl(4+1+1+4+4 + coutlen);
		msghdr2->MagicByte   = 0;
		msghdr2->Attributes  = rkb->rkb_rk->rk_conf.
			compression_codec & 0x7;
		msghdr2->Key_len = htonl(-1);
		msghdr2->Value_len = htonl(coutlen);
		msghdr2->Crc = rd_crc32_init();
		msghdr2->Crc = rd_crc32_update(msghdr2->Crc,
					       (void *)&msghdr2->MagicByte,
					       1+1+4+4);
		msghdr2->Crc = rd_crc32_update(msghdr2->Crc,
					       siov.iov_base, coutlen);
		msghdr2->Crc = htonl(rd_crc32_finalize(msghdr2->Crc));

		/* Update enveloping MessageSet's length. */
		prodhdr->part2.MessageSetSize = ctotlen;

		/* Rewind rkbuf to the pre-message checkpoint.
		 * This replaces all the original Messages with just the
		 * Message containing the compressed payload. */
		rd_kafka_buf_rewind(rkbuf, iov_firstmsg);

		/* Add allocated buffer as auxbuf to rkbuf so that
		 * it will get freed with the rkbuf */
		rd_kafka_buf_auxbuf_add(rkbuf, msghdr2);

		/* Push the new Message onto the buffer stack. */
		rd_kafka_buf_push(rkbuf, msghdr2, ctotlen);
	}

do_send:

	(void)rd_atomic_add(&rktp->rktp_c.tx_msgs,
			    rkbuf->rkbuf_msgq.rkmq_msg_cnt);
	(void)rd_atomic_add(&rktp->rktp_c.tx_bytes,
			    prodhdr->part2.MessageSetSize);

	prodhdr->part2.MessageSetSize =
		htonl(prodhdr->part2.MessageSetSize);

	rd_rkb_dbg(rkb, MSG, "PRODUCE",
		   "produce messageset with %i messages "
		   "(%"PRId32" bytes)",
		   rkbuf->rkbuf_msgq.rkmq_msg_cnt,
		   ntohl(prodhdr->part2.MessageSetSize));
	
	cnt = rkbuf->rkbuf_msgq.rkmq_msg_cnt;

	if (!rkt->rkt_conf.required_acks)
		rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_NO_RESPONSE;

	/* Use timeout from first message. */
	rkbuf->rkbuf_ts_timeout =
		TAILQ_FIRST(&rkbuf->rkbuf_msgq.rkmq_msgs)->rkm_ts_timeout;

	rd_kafka_toppar_keep(rktp); /* refcount for msgset_reply() */
	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Produce, rkbuf,
				 rd_kafka_produce_msgset_reply, rktp);


	return cnt;
}

/**
 * Serve a broker op (an op posted by another thread to be handled by
 * this broker's thread).
 */
static void rd_kafka_broker_op_serve (rd_kafka_broker_t *rkb,
				      rd_kafka_op_t *rko) {

	assert(pthread_self() == rkb->rkb_thread);

	switch (rko->rko_type)
	{
	case RD_KAFKA_OP_METADATA_REQ:
		if (rko->rko_rkt) {
			rd_kafka_broker_metadata_req(rkb, 0, rko->rko_rkt,
						     NULL);
                        /* Loose refcnt from op enq */
                        rd_kafka_topic_destroy0(rko->rko_rkt);
		} else
			rd_kafka_broker_metadata_req(rkb, 1 /*all topics*/,
						     NULL, NULL);
		break;

	default:
		assert(!*"unhandled op type");
	}

	rd_kafka_op_destroy(rko);
}


static void rd_kafka_broker_io_serve (rd_kafka_broker_t *rkb) {
	rd_kafka_op_t *rko;
	rd_ts_t now = rd_clock();

	/* Serve broker ops */
	if (unlikely(rd_kafka_q_len(&rkb->rkb_ops) > 0))
		while ((rko = rd_kafka_q_pop(&rkb->rkb_ops, RD_POLL_NOWAIT)))
			rd_kafka_broker_op_serve(rkb, rko);

	/* Periodic metadata poll */
	if (unlikely(now >= rkb->rkb_ts_metadata_poll))
		rd_kafka_broker_metadata_req(rkb, 1 /* all topics */, NULL,
					     "periodic refresh");

	if (rkb->rkb_outbufs.rkbq_cnt > 0)
		rkb->rkb_pfd.events |= POLLOUT;
	else
		rkb->rkb_pfd.events &= ~POLLOUT;

	if (poll(&rkb->rkb_pfd, 1,
		 rkb->rkb_rk->rk_conf.buffering_max_ms) <= 0)
		return;

	if (rkb->rkb_pfd.revents & POLLIN)
		while (rd_kafka_recv(rkb) > 0)
			;

	if (rkb->rkb_pfd.revents & POLLHUP)
		return rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
					    "Connection closed");

	if (rkb->rkb_pfd.revents & POLLOUT)
		while (rd_kafka_send(rkb) > 0)
			;
}


/**
 * Idle function for unassigned brokers
 */
static void rd_kafka_broker_ua_idle (rd_kafka_broker_t *rkb) {
	while (!rkb->rkb_rk->rk_terminate &&
	       rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP)
		rd_kafka_broker_io_serve(rkb);
}


/**
 * Producer serving
 */
static void rd_kafka_broker_producer_serve (rd_kafka_broker_t *rkb) {
	rd_ts_t last_timeout_scan = rd_clock();
	rd_kafka_msgq_t timedout = RD_KAFKA_MSGQ_INITIALIZER(timedout);

	assert(pthread_self() == rkb->rkb_thread);

	rd_kafka_broker_lock(rkb);

	while (!rkb->rkb_rk->rk_terminate &&
	       rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP) {
		rd_kafka_toppar_t *rktp;
		int cnt;
		rd_ts_t now;
		int do_timeout_scan = 0;

		now = rd_clock();
		if (unlikely(last_timeout_scan + 1000000 < now)) {
			do_timeout_scan = 1;
			last_timeout_scan = now;
		}

		rd_kafka_broker_toppars_rdlock(rkb);

		do {
			cnt = 0;

			TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {

				rd_rkb_dbg(rkb, TOPIC, "TOPPAR",
					   "%.*s [%"PRId32"] %i+%i msgs",
					   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
							    rkt_topic),
					   rktp->rktp_partition,
					   rktp->rktp_msgq.rkmq_msg_cnt,
					   rktp->rktp_xmit_msgq.rkmq_msg_cnt);

				rd_kafka_toppar_lock(rktp);
				if (rktp->rktp_msgq.rkmq_msg_cnt > 0)
					rd_kafka_msgq_concat(&rktp->
							     rktp_xmit_msgq,
							     &rktp->rktp_msgq);
				rd_kafka_toppar_unlock(rktp);

				/* Timeout scan */
				if (unlikely(do_timeout_scan))
					rd_kafka_msgq_age_scan(&rktp->
							       rktp_xmit_msgq,
							       &timedout,
							       now);

				if (rktp->rktp_xmit_msgq.rkmq_msg_cnt == 0)
					continue;
				/* Attempt to fill the batch size, but limit
				 * our waiting to queue.buffering.max.ms
				 * and batch.num.messages. */
				if (rktp->rktp_ts_last_xmit +
				    (rkb->rkb_rk->rk_conf.
				     buffering_max_ms * 1000) > now &&
				    rktp->rktp_xmit_msgq.rkmq_msg_cnt <
				    rkb->rkb_rk->rk_conf.
				    batch_num_messages) {
					/* Wait for more messages */
					continue;
				}

				rktp->rktp_ts_last_xmit = now;

				/* Send Produce requests for this toppar */
				while (rktp->rktp_xmit_msgq.rkmq_msg_cnt > 0) {
					int r = rd_kafka_broker_produce_toppar(
						rkb, rktp);
					if (likely(r > 0))
						cnt += r;
					else
						break;
				}
			}

		} while (cnt);

		/* Trigger delivery report for timed out messages */
		if (unlikely(timedout.rkmq_msg_cnt > 0))
			rd_kafka_dr_msgq(rkb->rkb_rk, &timedout,
					 RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);

		rd_kafka_broker_toppars_unlock(rkb);

		/* Check and move retry buffers */
		if (unlikely(rkb->rkb_retrybufs.rkbq_cnt) > 0)
			rd_kafka_broker_retry_bufs_move(rkb);

		rd_kafka_broker_unlock(rkb);

		/* Serve IO */
		rd_kafka_broker_io_serve(rkb);

		/* Scan wait-response queue
		 * Note: 'now' may be a bit outdated by now. */
		if (do_timeout_scan)
			rd_kafka_broker_waitresp_timeout_scan(rkb, now);

		rd_kafka_broker_lock(rkb);
	}

	rd_kafka_broker_unlock(rkb);
}


/**
 * Decompress Snappy message with Snappy-java framing.
 * Returns a malloced buffer with the uncompressed data, or NULL on failure.
 */
static char *rd_kafka_snappy_java_decompress (rd_kafka_broker_t *rkb,
					      int64_t Offset,
					      const char *inbuf,
					      size_t inlen,
					      size_t *outlenp) {
	int pass;
	char *outbuf = NULL;

	/**
	 * Traverse all chunks in two passes:
	 *  pass 1: calculate total uncompressed length
	 *  pass 2: uncompress
	 *
	 * Each chunk is prefixed with 4: length */

	for (pass = 1 ; pass <= 2 ; pass++) {
		ssize_t of = 0;  /* inbuf offset */
		ssize_t uof = 0; /* outbuf offset */

		while (of + 4 <= inlen) {
			/* compressed length */
			uint32_t clen = ntohl(*(uint32_t *)(inbuf+of));
			/* uncompressed length */
			size_t ulen;
			int r;

			of += 4;

			if (unlikely(clen > inlen - of)) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Invalid snappy-java chunk length for "
					   "message at offset %"PRId64" "
					   "(%"PRIu32">%zd: ignoring message",
					   Offset, clen, inlen - of);
				return NULL;
			}

			/* Acquire uncompressed length */
			if (unlikely(!snappy_uncompressed_length(inbuf+of,
								 clen, &ulen))) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Failed to get length of "
					   "(snappy-java framed) Snappy "
					   "compressed payload for message at "
					   "offset %"PRId64" (%"PRId32" bytes): "
					   "ignoring message",
					   Offset, clen);
				return NULL;
			}

			if (pass == 1) {
				/* pass 1: calculate total length */
				of  += clen;
				uof += ulen;
				continue;
			}

			/* pass 2: Uncompress to outbuf */
			if (unlikely((r = snappy_uncompress(inbuf+of, clen,
							    outbuf+uof)))) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Failed to decompress Snappy-java framed "
					   "payload for message at offset %"PRId64
					   " (%"PRId32" bytes): %s: ignoring message",
					   Offset, clen,
					   strerror(-r/*negative errno*/));
				free(outbuf);
				return NULL;
			}

			of  += clen;
			uof += ulen;
		}

		if (unlikely(of != inlen)) {
			rd_rkb_dbg(rkb, MSG, "SNAPPY",
				   "%zd trailing bytes in Snappy-java framed compressed "
				   "data at offset %"PRId64": ignoring message",
				   inlen - of, Offset);
			return NULL;
		}

		if (pass == 1) {
			if (uof <= 0) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Empty Snappy-java framed data "
					   "at offset %"PRId64" (%zd bytes): "
					   "ignoring message",
					   Offset, uof);
				return NULL;
			}

			/* Allocate memory for uncompressed data */
			outbuf = malloc(uof);
			if (unlikely(!outbuf)) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Failed to allocate memory for uncompressed "
					   "Snappy data at offset %"PRId64
					   " (%zd bytes): %s",
					   Offset, uof, strerror(errno));
				return NULL;
			}

		} else {
			/* pass 2 */
			*outlenp = uof;
		}
	}

	return outbuf;
}


/**
 * Parses a MessageSet and enqueues internal ops on the local
 * application queue for each Message.
 */
static rd_kafka_resp_err_t rd_kafka_messageset_handle (rd_kafka_broker_t *rkb,
						       rd_kafka_toppar_t *rktp,
						       rd_kafka_q_t *rkq,
						       rd_kafka_buf_t *rkbuf,
						       void *buf, size_t size) {
	size_t of = 0;
	rd_kafka_buf_t *rkbufz;

	if (_REMAIN() == 0)
		_FAIL("empty messageset");

	while (_REMAIN() > 0) {
		struct {
			int64_t Offset;
			int32_t MessageSize;
			int32_t Crc;
			int8_t  MagicByte;
			int8_t  Attributes;
		} RD_PACKED *hdr;
		rd_kafkap_bytes_t *Key;
		rd_kafkap_bytes_t *Value;
		int32_t Value_len;
		rd_kafka_op_t *rko;
		size_t outlen;
		void *outbuf = NULL;

		_READ_REF(hdr, sizeof(*hdr));
		hdr->Offset      = be64toh(hdr->Offset);
		hdr->MessageSize = ntohl(hdr->MessageSize);
		_CHECK_LEN(hdr->MessageSize - 6/*Crc+Magic+Attr*/);
		/* Ignore CRC (for now) */

		/* Extract key */
		_READ_BYTES(Key);
		
		/* Extract Value */
		_READ_BYTES(Value);

		Value_len = RD_KAFKAP_BYTES_LEN(Value);

		/* Check for message compression.
		 * The Key is ignored for compressed messages. */
		switch (hdr->Attributes & 0x3)
		{
		case RD_KAFKA_COMPRESSION_NONE:
			/* Pure uncompressed message, this is the innermost
			 * handler after all compression and cascaded
			 * messagesets have been peeled off. */

			/* Create op and push on temporary queue. */
			rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH);

			if (!RD_KAFKAP_BYTES_IS_NULL(Key)) {
				rko->rko_rkmessage.key = Key->data;
				rko->rko_rkmessage.key_len =
					RD_KAFKAP_BYTES_LEN(Key);
			}

			rko->rko_rkmessage.payload   = Value->data;
			rko->rko_rkmessage.len       = Value_len;

			rko->rko_rkmessage.offset    = hdr->Offset;
			rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
			rko->rko_rkmessage.partition = rktp->rktp_partition;

			rktp->rktp_next_offset = hdr->Offset + 1;

			/* Since all the ops share the same payload buffer
			 * (rkbuf->rkbuf_buf2) a refcnt is used on the rkbuf
			 * that makes sure all consume_cb() will have been
			 * called for each of these ops before the rkbuf
			 * and its rkbuf_buf2 are freed. */
			rko->rko_rkbuf = rkbuf;
			rd_kafka_buf_keep(rkbuf);

			if (0)
			rd_rkb_dbg(rkb, MSG, "MSG",
				   "Pushed message at offset %"PRId64
				   " onto queue", hdr->Offset);
				   
			rd_kafka_q_enq(rkq, rko);
			break;


		case RD_KAFKA_COMPRESSION_GZIP:
		{
			uint64_t outlenx = 0;

			/* Decompress Message payload */
			outbuf = rd_gz_decompress(Value->data, Value_len,
						  &outlenx);
			if (unlikely(!outbuf)) {
				rd_rkb_dbg(rkb, MSG, "GZIP",
					   "Failed to decompress Gzip "
					   "message at offset %"PRId64
					   " of %"PRId32" bytes: "
					   "ignoring message",
					   hdr->Offset, Value_len);
				continue;
			}

			outlen = outlenx;
		}
		break;

		case RD_KAFKA_COMPRESSION_SNAPPY:
		{
			char *inbuf = Value->data;
			int r;
			static const char snappy_java_magic[] =
				{ 0x82, 'S','N','A','P','P','Y', 0 };
			static const int snappy_java_hdrlen = 8+4+4;

			/* snappy-java adds its own header (SnappyCodec)
			 * which is not compatible with the official Snappy
			 * implementation.
			 *   8: magic, 4: version, 4: compatible
			 * followed by any number of chunks:
			 *   4: length
			 * ...: snappy-compressed data. */
			if (likely(Value_len > snappy_java_hdrlen + 4 &&
				   !memcmp(inbuf, snappy_java_magic, 8))) {
				/* snappy-java framing */

				inbuf = inbuf + snappy_java_hdrlen;
				Value_len -= snappy_java_hdrlen;
				outbuf = rd_kafka_snappy_java_decompress(rkb,
									 hdr->Offset,
									 inbuf,
									 Value_len,
									 &outlen);
				if (unlikely(!outbuf))
					continue;

			} else {
				/* no framing */

				/* Acquire uncompressed length */
				if (unlikely(!snappy_uncompressed_length(inbuf,
									 Value_len,
									 &outlen))) {
					rd_rkb_dbg(rkb, MSG, "SNAPPY",
						   "Failed to get length of Snappy "
						   "compressed payload "
						   "for message at offset %"PRId64
						   " (%"PRId32" bytes): "
						   "ignoring message",
						   hdr->Offset, Value_len);
					continue;
				}

				/* Allocate output buffer for uncompressed data */
				outbuf = malloc(outlen);

				/* Uncompress to outbuf */
				if (unlikely((r = snappy_uncompress(inbuf,
								    Value_len,
								    outbuf)))) {
					rd_rkb_dbg(rkb, MSG, "SNAPPY",
						   "Failed to decompress Snappy "
						   "payload for message at offset "
						   "%"PRId64
						   " (%"PRId32" bytes): %s: "
						   "ignoring message",
						   hdr->Offset, Value_len,
						   strerror(-r/*negative errno*/));
					free(outbuf);
					continue;
				}
			}

		}
		break;
		}


		if (outbuf) {
			/* With a new allocated buffer (outbuf) we need
			 * a separate rkbuf for it to allow multiple fetch ops
			 * to share the same payload buffer. */
			rkbufz = rd_kafka_buf_new_shadow(outbuf, outlen);

			/* Now parse the contained Messages */
			rd_kafka_messageset_handle(rkb, rktp, rkq, rkbufz,
						   outbuf, outlen);

			/* Loose our refcnt of the rkbuf.
			 * Individual rko's will have their own. */
			rd_kafka_buf_destroy(rkbufz);
		}

	}

	return 0;

err:
	/* "A Guide To The Kafka Protocol" states:
	 *   "As an optimization the server is allowed to return a partial
	 *    message at the end of the message set.
	 *    Clients should handle this case."
	 * We're handling it by not passing the error upstream. */
	return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Backoff the next Fetch request (due to error).
 */
static void rd_kafka_broker_fetch_backoff (rd_kafka_broker_t *rkb) {
	rkb->rkb_ts_fetch_backoff = rd_clock() +
		(rkb->rkb_rk->rk_conf.fetch_error_backoff_ms*1000);
}


/**
 * Parses and handles a Fetch reply.
 * Returns 0 on success or an error code on failure.
 */
static rd_kafka_resp_err_t rd_kafka_fetch_reply_handle (rd_kafka_broker_t *rkb,
							rd_kafka_buf_t *rkbuf) {
	char *buf = rkbuf->rkbuf_buf2;
	size_t size = rkbuf->rkbuf_len;
	size_t of = 0;
	int32_t TopicArrayCnt;
	int i;

	_READ_I32(&TopicArrayCnt);
	/* Verify that TopicArrayCnt seems to be in line with remaining size */
	_CHECK_LEN(TopicArrayCnt * (3/*topic min size*/ +
				    4/*PartitionArrayCnt*/ +
				    4+2+8+4/*inner header*/));

	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t *topic;
		rd_kafka_toppar_t *rktp;
		int32_t PartitionArrayCnt;
		struct {
			int32_t Partition;
			int16_t ErrorCode;
			int64_t HighwaterMarkOffset;
			int32_t MessageSetSize;
		} RD_PACKED *hdr;
		rd_kafka_resp_err_t err2;
		int j;

		_READ_STR(topic);
		_READ_I32(&PartitionArrayCnt);

		_CHECK_LEN(PartitionArrayCnt * (4+2+8+4/*inner header*/));

		for (j = 0 ; j < PartitionArrayCnt ; j++) {
			rd_kafka_q_t tmp_opq; /* Temporary queue for ops */

			_READ_REF(hdr, sizeof(*hdr));
			hdr->Partition = ntohl(hdr->Partition);
			hdr->ErrorCode = ntohs(hdr->ErrorCode);
			hdr->HighwaterMarkOffset = be64toh(hdr->
							   HighwaterMarkOffset);
			hdr->MessageSetSize = ntohl(hdr->MessageSetSize);

			/* Look up topic+partition */
			rktp = rd_kafka_toppar_get2(rkb->rkb_rk, topic,
						    hdr->Partition, 0);
			if (unlikely(!rktp)) {
				rd_rkb_dbg(rkb, TOPIC, "UNKTOPIC",
					   "Received Fetch response "
					   "(error %hu) for unknown topic "
					   "%.*s [%"PRId32"]: ignoring",
					   hdr->ErrorCode,
					   RD_KAFKAP_STR_PR(topic),
					   hdr->Partition);
				_SKIP(hdr->MessageSetSize);
				continue;
			}

			rd_rkb_dbg(rkb, MSG, "FETCH",
				   "Topic %.*s [%"PRId32"] MessageSet "
				   "size %"PRId32", error \"%s\", "
				   "MaxOffset %"PRId64,
				   RD_KAFKAP_STR_PR(topic), hdr->Partition,
				   hdr->MessageSetSize,
				   rd_kafka_err2str(hdr->ErrorCode),
				   hdr->HighwaterMarkOffset);


			/* If this is the last message of the queue,
			 * signal EOF back to the application. */
			if (hdr->HighwaterMarkOffset == rktp->rktp_next_offset
			    &&
			    rktp->rktp_eof_offset != rktp->rktp_next_offset) {
				hdr->ErrorCode =
					RD_KAFKA_RESP_ERR__PARTITION_EOF;
				rktp->rktp_eof_offset = rktp->rktp_next_offset;
			}


			/* Handle partition-level errors. */
			if (unlikely(hdr->ErrorCode !=
				     RD_KAFKA_RESP_ERR_NO_ERROR)) {
				rd_kafka_op_t *rko;

				/* Some errors should be passed to the
				 * application while some handled by rdkafka */
				switch (hdr->ErrorCode)
				{
					/* Errors handled by rdkafka */
				case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
				case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
				case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
				case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
					/* Request metadata information update*/
					rd_kafka_topic_leader_query(rkb->rkb_rk,
								    rktp->
								    rktp_rkt);
					break;

					/* Application errors */
				case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
					rd_kafka_offset_reset(rktp,
							      rktp->
							      rktp_next_offset,
							      hdr->ErrorCode,
							      "Fetch response");
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
				case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
				default: /* and all other errors */
					rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);

					rko->rko_err = hdr->ErrorCode;
					rko->rko_rkmessage.offset =
						rktp->rktp_next_offset;

					rko->rko_rkmessage.rkt = rktp->rktp_rkt;
					rko->rko_rkmessage.partition =
						rktp->rktp_partition;

					rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
					break;
				}

				rd_kafka_broker_fetch_backoff(rkb);

				rd_kafka_toppar_destroy(rktp); /* from get2() */
				continue;
			}

			if (hdr->MessageSetSize <= 0) {
				rd_kafka_toppar_destroy(rktp); /* from get2() */
				continue;
			}

			/* All parsed messages are put on this temporary op
			 * queue first and then moved in one go to the
			 * real op queue. */
			rd_kafka_q_init(&tmp_opq);

			/* Parse and handle the message set */
			err2 = rd_kafka_messageset_handle(rkb, rktp, &tmp_opq,
							  rkbuf, buf+of,
							  hdr->MessageSetSize);
			if (err2) {
				rd_kafka_toppar_destroy(rktp); /* from get2() */
				_FAIL("messageset handle failed");
			}

			/* Concat all messages onto the real op queue */
			rd_rkb_dbg(rkb, MSG, "CONSUME",
				   "Enqueue %i messages on %s [%"PRId32"] "
				   "fetch queue (%i)",
				   tmp_opq.rkq_qlen,
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   rd_kafka_q_len(&rktp->rktp_fetchq));

			if (tmp_opq.rkq_qlen > 0)
				rd_kafka_q_concat(&rktp->rktp_fetchq,
						  &tmp_opq);

			rd_kafka_toppar_destroy(rktp); /* from get2() */

			rd_kafka_q_destroy(&tmp_opq);

			_SKIP(hdr->MessageSetSize);
		}
	}

	if (_REMAIN() != 0)
		_FAIL("Remaining data after message set parse: %zd bytes",
		      _REMAIN());

	return 0;

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}



static void rd_kafka_broker_fetch_reply (rd_kafka_broker_t *rkb,
					 rd_kafka_resp_err_t err,
					 rd_kafka_buf_t *reply,
					 rd_kafka_buf_t *request,
					 void *opaque) {
	assert(rkb->rkb_fetching > 0);
	rkb->rkb_fetching = 0;

	/* Parse and handle the messages (unless the request errored) */
	if (!err && reply)
		err = rd_kafka_fetch_reply_handle(rkb, reply);

	rd_rkb_dbg(rkb, MSG, "FETCH", "Fetch reply: %s",
		   rd_kafka_err2str(err));

	if (unlikely(err)) {
		switch (err)
		{
		case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
		case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
		case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
			/* Request metadata information update */
			rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);
			/* FALLTHRU */

		case RD_KAFKA_RESP_ERR__TRANSPORT:
		case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
			/* Try again */
                        /* FIXME: Not sure we should retry here, the fetch
                         *        is already intervalled. */
			if (++request->rkbuf_retries <
			    /* FIXME: producer? */
			    rkb->rkb_rk->rk_conf.max_retries) {

				if (reply)
					rd_kafka_buf_destroy(reply);
							
				rd_kafka_broker_buf_retry(rkb, request);
				return;
			}
			break;

		default:
			break;
		}

		rd_kafka_broker_fetch_backoff(rkb);
		/* FALLTHRU */
	}



	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}



/**
 * Parse and handle an OffsetResponse message.
 * Returns an error code.
 */
static rd_kafka_resp_err_t
rd_kafka_toppar_offset_reply_handle (rd_kafka_broker_t *rkb,
				     rd_kafka_buf_t *rkbuf,
				     rd_kafka_toppar_t *rktp) {
	char *buf = rkbuf->rkbuf_buf2;
	size_t size = rkbuf->rkbuf_len;
	size_t of = 0;
	int32_t TopicArrayCnt;
	int i;

	_READ_I32(&TopicArrayCnt);
	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t *topic;
		int32_t PartitionOffsetsArrayCnt;
		int j;

		_READ_STR(topic);
		_READ_I32(&PartitionOffsetsArrayCnt);

		for (j = 0 ; j < PartitionOffsetsArrayCnt ; j++) {
			int32_t Partition;
			int16_t ErrorCode;
			int32_t OffsetArrayCnt;
			int64_t Offset;

			_READ_I32(&Partition);
			_READ_I16(&ErrorCode);
			_READ_I32(&OffsetArrayCnt);


			/* Skip toppars we didnt ask for. */
			if (unlikely(rktp->rktp_partition != Partition
				     || rd_kafkap_str_cmp(rktp->
							  rktp_rkt->
							  rkt_topic,
							  topic)))
				continue;

			if (unlikely(ErrorCode != RD_KAFKA_RESP_ERR_NO_ERROR))
				return ErrorCode;

			/* No offsets returned, convert to error code. */
			if (OffsetArrayCnt == 0)
				return RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE;

			/* We only asked for one offset, so just read the
			 * first one returned. */
			_READ_I64(&Offset);

			rd_rkb_dbg(rkb, TOPIC, "OFFSET",
				   "OffsetReply for topic %s [%"PRId32"]: "
				   "offset %"PRId64": activating fetch",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   Offset);

			rktp->rktp_next_offset = Offset;
			rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_ACTIVE;

			/* We just asked for one toppar and one offset, so
			 * we're probably done now. */
			return RD_KAFKA_RESP_ERR_NO_ERROR;
		}
	}


	return RD_KAFKA_RESP_ERR_NO_ERROR;

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}

/**
 * Parses and handles an OffsetRequest reply.
 */
static void rd_kafka_toppar_offset_reply (rd_kafka_broker_t *rkb,
					  rd_kafka_resp_err_t err,
					  rd_kafka_buf_t *reply,
					  rd_kafka_buf_t *request,
					  void *opaque) {
	rd_kafka_toppar_t *rktp = opaque;
	rd_kafka_op_t *rko;

	if (likely(!err && reply))
		err = rd_kafka_toppar_offset_reply_handle(rkb, reply, rktp);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetReply for topic %s [%"PRId32"]: %s",
		   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
		   rd_kafka_err2str(err));

	if (unlikely(err)) {
		switch (err)
		{
		case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
		case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
		case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
			/* Request metadata information update */
			rd_kafka_topic_leader_query(rkb->rkb_rk,
						    rktp->rktp_rkt);
			/* FALLTHRU */

		case RD_KAFKA_RESP_ERR__TRANSPORT:
		case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
			/* Try again */
			if (++request->rkbuf_retries <
			    /* FIXME: producer? */
			    rkb->rkb_rk->rk_conf.max_retries) {

				if (reply)
					rd_kafka_buf_destroy(reply);

				rd_kafka_broker_buf_retry(rkb, request);
				return;
			}
			break;

		default:
			break;
		}

		/* Backoff until next retry */
		rktp->rktp_ts_offset_req_next = rd_clock() + 500000; /* 500ms */
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY;

		/* Signal error back to application */
		rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);
		rko->rko_err = err;
		rko->rko_rkmessage.offset    = rktp->rktp_query_offset;
		rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
		rko->rko_rkmessage.partition = rktp->rktp_partition;
		rd_kafka_q_enq(&rktp->rktp_fetchq, rko);

		/* FALLTHRU */
	}

	rd_kafka_toppar_destroy(rktp); /* refcnt from request */

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}



/**
 * Send OffsetRequest for toppar.
 */
static void rd_kafka_toppar_offset_request (rd_kafka_broker_t *rkb,
					    rd_kafka_toppar_t *rktp) {
	struct {
		int32_t ReplicaId;
		int32_t TopicArrayCnt;
	} RD_PACKED *part1;
	/* Topic goes here */
	struct {
		int32_t PartitionArrayCnt;
		int32_t Partition;
		int64_t Time;
		int32_t MaxNumberOfOffsets;
	} RD_PACKED *part2;
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_kafka_buf_new(3/*part1,topic,part2*/,
				 sizeof(*part1) + sizeof(*part2));

	part1 = (void *)rkbuf->rkbuf_buf;
	part1->ReplicaId          = htonl(-1);
	part1->TopicArrayCnt      = htonl(1);
	rd_kafka_buf_push(rkbuf, part1, sizeof(*part1));

	rd_kafka_buf_push(rkbuf, rktp->rktp_rkt->rkt_topic,
			  RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->rkt_topic));

	rd_kafka_toppar_lock(rktp);
	part2 = (void *)(part1+1);
	part2->PartitionArrayCnt  = htonl(1);
	part2->Partition          = htonl(rktp->rktp_partition);
	part2->Time               = htobe64(rktp->rktp_query_offset);
	part2->MaxNumberOfOffsets = htonl(1);
	rd_kafka_buf_push(rkbuf, part2, sizeof(*part2));

	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_WAIT;
	rd_kafka_toppar_unlock(rktp);

	rd_kafka_toppar_keep(rktp); /* refcnt for request */

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetRequest (%"PRId64") for topic %s [%"PRId32"]",
		   rktp->rktp_query_offset,
		   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

	rkbuf->rkbuf_ts_timeout = rd_clock() +
		rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Offset, rkbuf,
				 rd_kafka_toppar_offset_reply, rktp);

}



/**
 * Build and send a Fetch request message for all underflowed toppars
 * for a specific broker.
 */
static int rd_kafka_broker_fetch_toppars (rd_kafka_broker_t *rkb) {
	struct rd_kafkap_FetchRequest *fr;
	struct { /* Per toppar header */
		int32_t PartitionArrayCnt;
		int32_t Partition;
		int64_t FetchOffset;
		int32_t MaxBytes;
	} RD_PACKED *tp;
	rd_kafka_toppar_t *rktp;
	char *next;
	int cnt = 0;
	rd_kafka_buf_t *rkbuf;
	rd_ts_t now = rd_clock();

	/* Create buffer and iovecs:
	 *   1 x part1 header
	 *   N x topic name
	 *   N x tp header
	 * where N = number of toppars */

	rd_kafka_broker_toppars_rdlock(rkb);

	rkbuf = rd_kafka_buf_new(1 + (rkb->rkb_toppar_cnt * (1 + 1)),
				 sizeof(*fr) +
				 (sizeof(*tp) * rkb->rkb_toppar_cnt));

	/* Set up header from pre-built header */
	fr = (void *)rkbuf->rkbuf_buf;
	*fr = rkb->rkb_rk->rk_conf.FetchRequest;
	rd_kafka_buf_push(rkbuf, fr, sizeof(*fr));
	next = (void *)(fr+1);

	TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {

		/* Check Toppar Fetch state */
		if (unlikely(rktp->rktp_fetch_state !=
			     RD_KAFKA_TOPPAR_FETCH_ACTIVE)) {

			switch (rktp->rktp_fetch_state)
			{
			case RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY:
				if (rktp->rktp_ts_offset_req_next <= now)
					rd_kafka_toppar_offset_request(rkb,
								       rktp);
				break;
			default:
				break;
			}

			/* Skip this toppar until its state changes */
			rd_rkb_dbg(rkb, TOPIC, "FETCH",
				   "Skipping topic %s [%"PRId32"] in state %s",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   rd_kafka_fetch_states[rktp->
							 rktp_fetch_state]);
			continue;
		}

		/* Skip toppars who's local message queue is already above
		 * the lower threshold. */
		if (rd_kafka_q_len(&rktp->rktp_fetchq) >=
		    rkb->rkb_rk->rk_conf.queued_min_msgs)
			continue;

		/* Push topic name onto buffer stack. */
		rd_kafka_buf_push(rkbuf, rktp->rktp_rkt->rkt_topic,
				  RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->
						     rkt_topic));
		
		/* Set up toppar header and push it */
		tp = (void *)next;
		tp->PartitionArrayCnt = htonl(1);
		tp->Partition = htonl(rktp->rktp_partition);
		tp->FetchOffset = htobe64(rktp->rktp_next_offset);
		tp->MaxBytes = htonl(rkb->rkb_rk->rk_conf.fetch_msg_max_bytes);
		rd_kafka_buf_push(rkbuf, tp, sizeof(*tp));
		next = (void *)(tp+1);

		rd_rkb_dbg(rkb, TOPIC, "FETCH",
			   "Fetch topic %.*s [%"PRId32"] at offset %"PRId64,
			   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			   rktp->rktp_partition,
			   rktp->rktp_next_offset);

		cnt++;
	}

	rd_kafka_broker_toppars_unlock(rkb);


	rd_rkb_dbg(rkb, MSG, "CONSUME",
		   "consume from %i toppar(s)", cnt);

	if (!cnt) {
		rd_kafka_buf_destroy(rkbuf);
		return cnt;
	}

	fr->TopicArrayCnt = htonl(cnt);


	/* Use configured timeout */
	rkbuf->rkbuf_ts_timeout = now +
		((rkb->rkb_rk->rk_conf.socket_timeout_ms +
		  rkb->rkb_rk->rk_conf.fetch_wait_max_ms) * 1000);

	rkb->rkb_fetching = 1;
	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Fetch, rkbuf,
				 rd_kafka_broker_fetch_reply, NULL);

	return cnt;
}




/**
 * Consumer serving
 */
static void rd_kafka_broker_consumer_serve (rd_kafka_broker_t *rkb) {

	assert(pthread_self() == rkb->rkb_thread);

	rd_kafka_broker_lock(rkb);

	while (!rkb->rkb_rk->rk_terminate &&
	       rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP) {
		int cnt = 0;
		rd_ts_t now;
		int do_timeout_scan = 1; /* FIXME */

		now = rd_clock();

		/* Send Fetch request message for all underflowed toppars */
		if (!rkb->rkb_fetching && rkb->rkb_ts_fetch_backoff < now)
			cnt = rd_kafka_broker_fetch_toppars(rkb);

		rd_rkb_dbg(rkb, BROKER, "FETCH",
			   "Fetch for %i toppars, fetching=%i",
			   cnt, rkb->rkb_fetching);

		/* Check and move retry buffers */
		if (unlikely(rkb->rkb_retrybufs.rkbq_cnt) > 0)
			rd_kafka_broker_retry_bufs_move(rkb);

		rd_kafka_broker_unlock(rkb);

		/* Serve IO */
		rd_kafka_broker_io_serve(rkb);

		/* Scan wait-response queue
		 * Note: 'now' may be a bit outdated by now. */
		if (do_timeout_scan)
			rd_kafka_broker_waitresp_timeout_scan(rkb, now);

		rd_kafka_broker_lock(rkb);
	}

	rd_kafka_broker_unlock(rkb);
}


static void *rd_kafka_broker_thread_main (void *arg) {
	rd_kafka_broker_t *rkb = arg;
	rd_kafka_t *rk = rkb->rkb_rk;

	(void)rd_atomic_add(&rd_kafka_thread_cnt_curr, 1);

	rd_thread_sigmask(SIG_BLOCK,
			  SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2,
			  RD_SIG_END);

	rd_rkb_dbg(rkb, BROKER, "BRKMAIN", "Enter main broker thread");

	while (!rkb->rkb_rk->rk_terminate) {
		switch (rkb->rkb_state)
		{
		case RD_KAFKA_BROKER_STATE_INIT:
			/* The INIT state exists so that an initial connection
			 * failure triggers a state transition which might
			 * trigger a ALL_BROKERS_DOWN error. */
		case RD_KAFKA_BROKER_STATE_DOWN:
			/* ..connect() will block until done (or failure) */
			if (rd_kafka_broker_connect(rkb) == -1) {
				/* Try the next one until we've tried them all,
				 * in which case we sleep a short while to
				 * avoid the busy looping. */
				if (!rkb->rkb_rsal ||
				    rkb->rkb_rsal->rsal_cnt == 0 ||
				    rkb->rkb_rsal->rsal_curr + 1 ==
				    rkb->rkb_rsal->rsal_cnt)
					sleep(1);
			}
			break;

		case RD_KAFKA_BROKER_STATE_UP:
			if (rkb->rkb_nodeid == RD_KAFKA_NODEID_UA)
				rd_kafka_broker_ua_idle(rkb);
			else if (rk->rk_type == RD_KAFKA_PRODUCER)
				rd_kafka_broker_producer_serve(rkb);
			else if (rk->rk_type == RD_KAFKA_CONSUMER)
				rd_kafka_broker_consumer_serve(rkb);
			break;
		}

	}

	rd_kafka_lock(rkb->rkb_rk);
	TAILQ_REMOVE(&rkb->rkb_rk->rk_brokers, rkb, rkb_link);
	rd_atomic_sub(&rkb->rkb_rk->rk_broker_cnt, 1);
	rd_kafka_unlock(rkb->rkb_rk);
	rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__DESTROY, NULL);
	rd_kafka_broker_destroy(rkb);

	(void)rd_atomic_sub(&rd_kafka_thread_cnt_curr, 1);

	return NULL;
}


void rd_kafka_broker_destroy (rd_kafka_broker_t *rkb) {
        rd_kafka_op_t *rko;

	if (rd_atomic_sub(&rkb->rkb_refcnt, 1) > 0)
		return;

	assert(TAILQ_EMPTY(&rkb->rkb_outbufs.rkbq_bufs));
	assert(TAILQ_EMPTY(&rkb->rkb_toppars));

	if (rkb->rkb_recv_buf)
		rd_kafka_buf_destroy(rkb->rkb_recv_buf);

	if (rkb->rkb_rsal)
		rd_sockaddr_list_destroy(rkb->rkb_rsal);

        /* Clean up any references in the op queue before purging it */
        TAILQ_FOREACH(rko, &rkb->rkb_ops.rkq_q, rko_link) {
                switch (rko->rko_type)
                {
                case RD_KAFKA_OP_METADATA_REQ:
                        if (rko->rko_rkt)
                                rd_kafka_topic_destroy0(rko->rko_rkt);
                        break;
                default:
                        break;
                }
        }

	rd_kafka_q_purge(&rkb->rkb_ops);
	rd_kafka_q_destroy(&rkb->rkb_ops);

	rd_kafka_destroy0(rkb->rkb_rk);

	pthread_rwlock_destroy(&rkb->rkb_toppar_lock);
	pthread_mutex_destroy(&rkb->rkb_lock);

	free(rkb);
}


/**
 *
 * Locks: rd_kafka_lock(rk) must be held
 */
static rd_kafka_broker_t *rd_kafka_broker_add (rd_kafka_t *rk,
					       rd_kafka_confsource_t source,
					       const char *name, uint16_t port,
					       int32_t nodeid) {
	rd_kafka_broker_t *rkb;
	int err;
	pthread_attr_t attr;

	rd_kafka_keep(rk);

	rkb = calloc(1, sizeof(*rkb));

	snprintf(rkb->rkb_nodename, sizeof(rkb->rkb_nodename),
		 "%s:%hu", name, port);
	if (nodeid == RD_KAFKA_NODEID_UA)
		snprintf(rkb->rkb_name, sizeof(rkb->rkb_name),
			 "%s/bootstrap", rkb->rkb_nodename);
	else
		snprintf(rkb->rkb_name, sizeof(rkb->rkb_name),
			 "%s/%"PRId32, rkb->rkb_nodename, nodeid);
	rkb->rkb_source = source;
	rkb->rkb_rk = rk;
	rkb->rkb_nodeid = nodeid;

	rkb->rkb_s = -1;
	pthread_rwlock_init(&rkb->rkb_toppar_lock, NULL);
	TAILQ_INIT(&rkb->rkb_toppars);
	rd_kafka_bufq_init(&rkb->rkb_outbufs);
	rd_kafka_bufq_init(&rkb->rkb_waitresps);
	rd_kafka_bufq_init(&rkb->rkb_retrybufs);
	rd_kafka_q_init(&rkb->rkb_ops);
	rd_kafka_broker_keep(rkb);

	/* Set next intervalled metadata refresh, offset by a random
	 * value to avoid all brokers to be queried simultaneously. */
	if (rkb->rkb_rk->rk_conf.metadata_refresh_interval_ms >= 0)
		rkb->rkb_ts_metadata_poll = rd_clock() +
			(rkb->rkb_rk->rk_conf.
			 metadata_refresh_interval_ms * 1000) +
			(rd_jitter(500,1500) * 1000);
	else /* disabled */
		rkb->rkb_ts_metadata_poll = UINT64_MAX;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	if ((err = pthread_create(&rkb->rkb_thread, &attr,
				  rd_kafka_broker_thread_main, rkb))) {
		char tmp[512];
		snprintf(tmp, sizeof(tmp),
			 "Unable to create broker thread: %s",
			 strerror(err));
		rd_kafka_log(rk, LOG_CRIT, "THREAD", "%s", tmp);

		/* Send ERR op back to application for processing. */
		rd_kafka_op_err(rk, RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
				"%s", tmp);

		free(rkb);
		rd_kafka_destroy(rk);
		return NULL;
	}

	TAILQ_INSERT_TAIL(&rkb->rkb_rk->rk_brokers, rkb, rkb_link);
	rd_atomic_add(&rkb->rkb_rk->rk_broker_cnt, 1);

	rd_rkb_dbg(rkb, BROKER, "BROKER",
		   "Added new broker with NodeId %"PRId32,
		   rkb->rkb_nodeid);

	return rkb;
}

/**
 * Locks: rd_kafka_lock()
 * NOTE: caller must release rkb reference by rd_kafka_broker_destroy()
 */
rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid (rd_kafka_t *rk,
						   int32_t nodeid) {
	rd_kafka_broker_t *rkb;

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if (!rk->rk_terminate &&
		    rkb->rkb_nodeid == nodeid) {
			rd_kafka_broker_keep(rkb);
			rd_kafka_broker_unlock(rkb);
			return rkb;
		}
		rd_kafka_broker_unlock(rkb);
	}

	return NULL;

}

/**
 * Locks: rd_kafka_lock(rk) must be held
 */
static rd_kafka_broker_t *rd_kafka_broker_find (rd_kafka_t *rk,
						const char *name,
						uint16_t port) {
	rd_kafka_broker_t *rkb;
	char fullname[sizeof(rkb->rkb_name)];

	snprintf(fullname, sizeof(fullname), "%s:%hu", name, port);

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if (!rk->rk_terminate &&
		    !strcmp(rkb->rkb_name, fullname)) {
			rd_kafka_broker_keep(rkb);
			rd_kafka_broker_unlock(rkb);
			return rkb;
		}
		rd_kafka_broker_unlock(rkb);
	}

	return NULL;
}


int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist) {
	char *s = strdupa(brokerlist);
	char *t, *t2, *n;
	int cnt = 0;
	rd_kafka_broker_t *rkb;

	/* Parse comma-separated list of brokers. */
	while (*s) {
		uint16_t port = 0;

		if (*s == ',' || *s == ' ') {
			s++;
			continue;
		}

		if ((n = strchr(s, ',')))
			*n = '\0';
		else
			n = s + strlen(s)-1;

		/* Check if port has been specified, but try to identify IPv6
		 * addresses first:
		 *  t = last ':' in string
		 *  t2 = first ':' in string
		 *  If t and t2 are equal then only one ":" exists in name
		 *  and thus an IPv4 address with port specified.
		 *  Else if not equal and t is prefixed with "]" then it's an
		 *  IPv6 address with port specified.
		 *  Else no port specified. */
		if ((t = strrchr(s, ':')) &&
		    ((t2 = strchr(s, ':')) == t || *(t-1) == ']')) {
			*t = '\0';
			port = atoi(t+1);
		}

		if (!port)
			port = RD_KAFKA_PORT;

		rd_kafka_lock(rk);

		if ((rkb = rd_kafka_broker_find(rk, s, port)) &&
		    rkb->rkb_source == RD_KAFKA_CONFIGURED) {
			cnt++;
		} else if (rd_kafka_broker_add(rk, RD_KAFKA_CONFIGURED, s, port,
					       RD_KAFKA_NODEID_UA) != NULL)
			cnt++;

		rd_kafka_unlock(rk);

		s = n+1;
	}

	return cnt;
}


/**
 * Adds a new broker or updates an existing one.
 */
static void rd_kafka_broker_update (rd_kafka_t *rk,
				    const char *name,
				    uint16_t port,
				    uint32_t nodeid) {
	rd_kafka_broker_t *rkb;

	rd_kafka_lock(rk);
	if ((rkb = rd_kafka_broker_find_by_nodeid(rk, nodeid)))
		rd_kafka_broker_destroy(rkb);
	else
		rd_kafka_broker_add(rk, RD_KAFKA_LEARNED,
				    name, port, nodeid);
	rd_kafka_unlock(rk);
		
	/* FIXME: invalidate Leader if required. */
}



void rd_kafka_brokers_init (void) {
}








