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

/**
 * Apache Kafka consumer & producer implementation.
 * This implementation used to
 * reside in librd (https://github.com/edenhill/librd)
 * but was moved here to provide a purely stand-alone library.
 * Thus the rd-prefix.
 */

#include <sys/socket.h>
#include <sys/types.h>
#include <limits.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <syslog.h>
#include <pthread.h>
#include <poll.h>
#define __need_IOV_MAX
#include <stdio.h>
#include <sys/socket.h>

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif
#include <endian.h>

#include <zlib.h>

#define NEED_RD_KAFKAPROTO_DEF
#include "rdkafka.h"

#ifndef WITH_LIBRD
#include "rdcrc32.h"
#include "rdgz.h"
#include "rdfile.h"
#include "rdtime.h"
#else
#include <librd/rdcrc32.h>
#include <librd/rdgz.h>
#include <librd/rdfile.h>
#include <librd/rdtime.h>
#endif



/**
 * librd usage:
 *   trivial:
 *    -- RD_SIZE_*
 *    * rd_atomic
 *    -- rd_mutex
 *    -- rd_cond
 *    * rd_clock
 *    * RD_POLL
 *    * rd_ts
 *    -- rd_cond_timedwait
 *    -- rd_memdup
 *    * rd_file_mode
 *    - -rd_thread_create
 *
 *   moderate:
 *    * rd_sockaddr_list*
 *    -- rd_tsprintf
 *    -- rd_io_poll_single
 *    * rd_gz_decompress

 *
 *
 *   critical:
 *   
 */


/**
 * Default configuration.
 * Use a copy of this struct as the base of your own configuration.
 * See rdkafka.h for more information.
 */
const rd_kafka_conf_t rd_kafka_defaultconf = {
	consumer: {
		poll_interval: 1000 /* 1s */,
		replyq_low_thres: 1,
		max_size: 500000,
	},
	max_msg_size: 4000000,
};


static void (*rd_kafka_log_cb) (const rd_kafka_t *rk, int level,
				const char *fac,
				const char *buf) = rd_kafka_log_print;

static int rd_kafka_recv (rd_kafka_t *rk);
static void rd_kafka_op_reply (rd_kafka_t *rk,
			       rd_kafka_op_type_t type,
			       rd_kafka_resp_err_t err, uint8_t compression,
			       void *payload, int len,
			       uint64_t offset_len);


/**
 * Minimalistic replacement for rd_tsprintf() in case librd is not available.
 */
static char *rd_tsprintf (const char *fmt, ...) {
	static __thread char ret[2048];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(ret, sizeof(ret), fmt, ap);
	va_end(ap);
	
	return ret;
}


/**
 * Wrapper for pthread_cond_timedwait() that makes it simpler to use
 * for delta timeouts.
 * `timeout_ms' is the delta timeout in milliseconds.
 */
static int pthread_cond_timedwait_ms (pthread_cond_t *cond,
				      pthread_mutex_t *mutex,
				      int timeout_ms) {
	struct timeval tv;
	struct timespec ts;

	gettimeofday(&tv, NULL);
	TIMEVAL_TO_TIMESPEC(&tv, &ts);

	ts.tv_sec  += timeout_ms / 1000;
	ts.tv_nsec += (timeout_ms % 1000) * 1000000;

	if (ts.tv_nsec > 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	return pthread_cond_timedwait(cond, mutex, &ts);
}


static void rd_kafka_log (const rd_kafka_t *rk, int level,
			  const char *fac, const char *fmt, ...) {
	char buf[2048];
	va_list ap;

	if (!rd_kafka_log_cb)
		return;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	rd_kafka_log_cb(rk, level, fac, buf);
}

#define rd_kafka_dbg(rk,fac,fmt...) rd_kafka_log(rk,LOG_DEBUG,fac,fmt)


void rd_kafka_log_print (const rd_kafka_t *rk, int level,
			 const char *fac, const char *buf) {
	struct timeval tv;

	gettimeofday(&tv, NULL);

	fprintf(stderr, "%%%i|%u.%03u|%s|%s| %s\n",
		level, (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		fac, rk ? rk->rk_broker.name : "", buf);
}

void rd_kafka_log_syslog (const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf) {
	static int initialized = 0;

	if (!initialized)
		openlog("rdkafka", LOG_PID|LOG_CONS, LOG_USER);

	syslog(level, "%s: %s: %s", fac, rk ? rk->rk_broker.name : "", buf);
}


void rd_kafka_set_logger (void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf)) {
	rd_kafka_log_cb = func;
}


static void rd_kafka_destroy0 (rd_kafka_t *rk) {
	if (rk->rk_broker.s != -1)
		close(rk->rk_broker.s);

	if (rk->rk_broker.rsal)
		rd_sockaddr_list_destroy(rk->rk_broker.rsal);

	switch (rk->rk_type)
	{
	case RD_KAFKA_CONSUMER:
		if (rk->rk_consumer.topic)
			free(rk->rk_consumer.topic);
		if (rk->rk_conf.consumer.offset_file)
			free(rk->rk_conf.consumer.offset_file);
		if (rk->rk_consumer.offset_file_fd != -1)
			close(rk->rk_consumer.offset_file_fd);
		break;
	case RD_KAFKA_PRODUCER:
		break;
	}

	free(rk);
}


void rd_kafka_destroy (rd_kafka_t *rk) {
	rk->rk_terminate = 1;

	if (rd_atomic_sub(&rk->rk_refcnt, 1) == 0)
		rd_kafka_destroy0(rk);
}


/**
 *
 * Locality: Kafka thread
 */
static inline void rd_kafka_set_state (rd_kafka_t *rk,
				       rd_kafka_state_t new_state) {
	if (rk->rk_state == new_state)
		return;

	rk->rk_state = new_state;
	gettimeofday(&rk->rk_tv_state_change, NULL);
}



/**
 * Failure propagation to application.
 * Will tear down connection to broker and trigger a reconnect.
 *
 * If 'fmt' is NULL nothing will be logged or propagated to the application.
 * 
 * Locality: Kafka thread
 */
static void rd_kafka_fail (rd_kafka_t *rk, const char *fmt, ...) {
	va_list ap;

	pthread_mutex_lock(&rk->rk_lock);

	rk->rk_err.err = errno;

	rd_kafka_set_state(rk, RD_KAFKA_STATE_DOWN);

	if (rk->rk_broker.s != -1) {
		close(rk->rk_broker.s);
		rk->rk_broker.s = -1;
	}

	if (fmt) {
		va_start(ap, fmt);
		vsnprintf(rk->rk_err.msg, sizeof(rk->rk_err.msg), fmt, ap);
		va_end(ap);

		rd_kafka_log(rk, LOG_ERR, "FAIL", "%s", rk->rk_err.msg);

		/* Send ERR op back to application for processing. */
		rd_kafka_op_reply(rk, RD_KAFKA_OP_ERR,
				  RD_KAFKA_RESP_ERR__FAIL, 0,
				  strdup(rk->rk_err.msg),
				  strlen(rk->rk_err.msg), 0);
	}

	pthread_mutex_unlock(&rk->rk_lock);
}



int rd_kafka_offset_store (rd_kafka_t *rk, uint64_t offset) {

	/* File-based */
	if (rk->rk_conf.consumer.offset_file) {
		char tmp[32];
		int len;
		int r;

		snprintf(tmp, sizeof(tmp), "%"PRIu64"\n", offset);
		len = strlen(tmp);

		if (ftruncate(rk->rk_consumer.offset_file_fd, 0) == -1 ||
		    lseek(rk->rk_consumer.offset_file_fd, SEEK_SET, 0) == -1) {
			rd_kafka_log(rk, LOG_ERR, "OFFTRUNC",
				     "truncate/lseek of offset file %s (fd %i) "
				     "failed: %s",
				     rk->rk_conf.consumer.offset_file,
				     rk->rk_consumer.offset_file_fd,
				     strerror(errno));
			return -1;
		}

		if ((r = write(rk->rk_consumer.offset_file_fd,
			       tmp, len)) != len) {
			rd_kafka_log(rk, LOG_ERR, "OFFWRITE",
				     "offset %"PRIu64 " write to "
				     "file %s failed: %s",
				     offset,
				     rk->rk_conf.consumer.offset_file,
				     r == -1 ? strerror(errno) :
				     "partial write");
			return -1;
		}
		return 0;
		
	} else {
		/* No storage defined for permanent offsets */
		return 0;
	}
}



/**
 * Blocking connect attempt.
 *
 * Locality: Kafka thread
 */	
static int rd_kafka_connect (rd_kafka_t *rk) {
	rd_sockaddr_inx_t *sinx = rd_sockaddr_list_next(rk->rk_broker.rsal);

	if ((rk->rk_broker.s = socket(sinx->sinx_family,
				      SOCK_STREAM, IPPROTO_TCP)) == -1) {
		rd_kafka_fail(rk,
			      "Failed to create %s socket: %s",
			      rd_family2str(sinx->sinx_family),
			      strerror(errno));
		return -1;
	}


	rd_kafka_set_state(rk, RD_KAFKA_STATE_CONNECTING);

	if (connect(rk->rk_broker.s, (struct sockaddr *)sinx,
		    RD_SOCKADDR_INX_LEN(sinx)) == -1) {
		/* Avoid duplicate log messages */
		if (rk->rk_err.err == errno)
			rd_kafka_fail(rk, NULL);
		else
			rd_kafka_fail(rk,
				      "Failed to connect to broker at %s: %s",
				      rd_sockaddr2str(sinx,
						      RD_SOCKADDR2STR_F_NICE),
				      strerror(errno));
		return -1;
	}

	rd_kafka_dbg(rk, "CONNECTED", "connected to %s",
		     rd_sockaddr2str(sinx, RD_SOCKADDR2STR_F_NICE));

	rd_kafka_set_state(rk, RD_KAFKA_STATE_UP);
	rk->rk_err.err = 0;

	return 0;
}


void rd_kafka_op_destroy (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	
	if (rko->rko_topic && rko->rko_flags & RD_KAFKA_OP_F_FREE_TOPIC)
		free(rko->rko_topic);

	if (rko->rko_payload && rko->rko_flags & RD_KAFKA_OP_F_FREE)
		free(rko->rko_payload);
	
	free(rko);
}


static int rd_kafka_send (rd_kafka_t *rk, const struct msghdr *msg) {
	int r;

	r = sendmsg(rk->rk_broker.s, msg, 0);
	if (r == -1) {
		rd_kafka_fail(rk, "Send failed: %s", strerror(errno));
		return -1;
	}

	rk->rk_broker.stats.tx_bytes += r;
	rk->rk_broker.stats.tx++;
	return r;
}


#define RD_KAFKA_SEND_END -1

static void rd_kafka_send_request (rd_kafka_t *rk,
				   uint16_t msgtype,
				   struct rd_kafkap_topicpart *topicpart,
				   ...) {
	va_list ap;
	int sz;
	void *ptr;
	const int iov_begin = 2; /* Request header + topic&partition */
	struct iovec iov[IOV_MAX];
	struct msghdr msg = {
	msg_iov: iov,
	msg_iovlen: iov_begin,
	};
	struct rd_kafkap_req req = {
	rkpr_type: htons(msgtype),
	};
	int len = 0;
	int hdr_len;


	/* Request header */
	iov[0].iov_base = &req;
	iov[0].iov_len  = sizeof(req);
	iov[1].iov_base = topicpart->rkptp_buf;
	iov[1].iov_len  = topicpart->rkptp_len;
	hdr_len = iov[0].iov_len - sizeof(req.rkpr_len) + iov[1].iov_len;

	va_start(ap, topicpart);
	while ((sz = va_arg(ap, int)) != RD_KAFKA_SEND_END) {
		ptr = va_arg(ap, void *);

		iov[msg.msg_iovlen].iov_len = sz;
		iov[msg.msg_iovlen++].iov_base = ptr;
		len += sz;

		if (msg.msg_iovlen == IOV_MAX) {
			req.rkpr_len = htonl(hdr_len + len);
			req.rkpr_topic_len = htons(topicpart->rkptp_len - 4);
			if (rd_kafka_send(rk, &msg) == -1)
				break; /* Send failed, socket failure. */

			msg.msg_iovlen = iov_begin;
		}
	}
	va_end(ap);

	if (msg.msg_iovlen > iov_begin) {
		req.rkpr_len = htonl(hdr_len + len);
		req.rkpr_topic_len = htons(topicpart->rkptp_len - 4);

		rd_kafka_send(rk, &msg);
	}
}


/**
 * Serialize a topic+partition combo into a temporary buffer.
 */
static struct rd_kafkap_topicpart *
rd_kafka_topicpart_serialize (const char *topic, uint32_t partition) {
	static __thread union {
		struct rd_kafkap_topicpart topicpart;
		char buf[sizeof(struct rd_kafkap_topicpart) +
			 RD_KAFKA_TOPIC_MAXLEN + sizeof(partition)];
	} u;
	int len;

	len = strlen(topic);
	if (len > RD_KAFKA_TOPIC_MAXLEN) {
		/* This may look silly, but it saves us from mallocs,
		 * and it should suffice for most uses, if not, increase it! */
		fprintf(stderr,
			"RD-KAFKA-FIXME: Topic name (%s) is too long (max %i),"
			" will truncate it (increase in rdkafka.h)\n",
		       topic, RD_KAFKA_TOPIC_MAXLEN);
		len = RD_KAFKA_TOPIC_MAXLEN;
	}

	partition = htonl(partition);
	
	memcpy(u.topicpart.rkptp_buf, topic, len);
	memcpy(u.topicpart.rkptp_buf+len, &partition, sizeof(partition));
	u.topicpart.rkptp_len = len + sizeof(partition);

	return &u.topicpart;
}


/**
 * Initialize a queue.
 */
static void rd_kafka_q_init (rd_kafka_q_t *rkq) {
	TAILQ_INIT(&rkq->rkq_q);
	rkq->rkq_qlen = 0;
	
	pthread_mutex_init(&rkq->rkq_lock, NULL);
	pthread_cond_init(&rkq->rkq_cond, NULL);
}


/**
 * Enqueue the 'rko' op at the tail of the queue 'rkq'.
 *
 * Locality: any thread.
 */
static inline void rd_kafka_q_enq (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
	pthread_mutex_lock(&rkq->rkq_lock);
	TAILQ_INSERT_TAIL(&rkq->rkq_q, rko, rko_link);
	(void)rd_atomic_add(&rkq->rkq_qlen, 1);
	pthread_cond_signal(&rkq->rkq_cond);
	pthread_mutex_unlock(&rkq->rkq_lock);
}


/**
 * Pop an op from a queue.
 *
 * Locality: any thread.
 */
static rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms) {
	rd_kafka_op_t *rko;
	rd_ts_t last;

	pthread_mutex_lock(&rkq->rkq_lock);
	
	while (!(rko = TAILQ_FIRST(&rkq->rkq_q)) &&
	       (timeout_ms == RD_POLL_INFINITE || timeout_ms > 0)) {

		if (timeout_ms != RD_POLL_INFINITE) {
			last = rd_clock();
			if (pthread_cond_timedwait_ms(&rkq->rkq_cond,
						      &rkq->rkq_lock,
						      timeout_ms) ==
			    ETIMEDOUT) {
				pthread_mutex_unlock(&rkq->rkq_lock);
				return NULL;
			}
			timeout_ms -= (rd_clock() - last) / 1000;
		} else
			pthread_cond_wait(&rkq->rkq_cond, &rkq->rkq_lock);
	}

	if (rko) {
		TAILQ_REMOVE(&rkq->rkq_q, rko, rko_link);
		(void)rd_atomic_sub(&rkq->rkq_qlen, 1);
	}

	pthread_mutex_unlock(&rkq->rkq_lock);

	return rko;
}



/**
 * Send an op back to the application.
 *
 * Locality: Kafka threads
 */
static void rd_kafka_op_reply0 (rd_kafka_t *rk, rd_kafka_op_t *rko,
				rd_kafka_op_type_t type,
				rd_kafka_resp_err_t err, uint8_t compression,
				void *payload, int len,
				uint64_t offset_len) {

	rko->rko_type        = type;
	rko->rko_flags      |= RD_KAFKA_OP_F_FREE;
	rko->rko_payload     = payload;
	rko->rko_len         = len;
	rko->rko_err         = err;
	rko->rko_compression = compression;
	rko->rko_offset      = offset_len;
}


/**
 * Send an op back to the application.
 *
 * Locality: Kafka thread
 */
static void rd_kafka_op_reply (rd_kafka_t *rk,
			       rd_kafka_op_type_t type,
			       rd_kafka_resp_err_t err, uint8_t compression,
			       void *payload, int len,
			       uint64_t offset_len) {
	rd_kafka_op_t *rko;

	rko = calloc(1, sizeof(*rko));

	if (err && !payload) {
		/* Provide human readable error string if not provided. */

		/* Provide more info for some errors. */
		if (err == RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE)
			payload =
				strdup(rd_tsprintf("%s (%"PRIu64")",
						   rd_kafka_err2str(err),
						   rk->rk_consumer.offset));
		else
			payload = strdup(rd_kafka_err2str(err));

		len = strlen(payload);
	}

	rd_kafka_op_reply0(rk, rko, type, err, compression,
			   payload, len, offset_len);
	rd_kafka_q_enq(&rk->rk_rep, rko);
}




/**
 * Receive and purge data.
 */
static void rd_kafka_recv_null (rd_kafka_t *rk, int total_len) {
	char buf[256];

	while (total_len > 0) {
		int len = total_len < sizeof(buf) ? total_len : sizeof(buf);
		int r;

		if ((r = recv(rk->rk_broker.s, buf, len, MSG_DONTWAIT)) < 1)
			return;

		total_len -= r;
	}
}


/**
 * Read data from socket.
 */
static int rd_kafka_recv0 (rd_kafka_t *rk, const char *what,
			   void *data, int len, int partial_ok) {
	int r;

	if ((r = recv(rk->rk_broker.s, data, len,
		      partial_ok ? 0 : MSG_WAITALL)) != len) {
		if (r > 0) {
			if (partial_ok)
				return r;
			else
				rd_kafka_fail(rk, "Partial %s response "
					      "(%i<%i bytes)",
					      what, r, len);
		} else if (r == 0)
			rd_kafka_fail(rk, "No more data while expecting %s "
				      "(%i bytes)", what, len);
		else
			rd_kafka_fail(rk, "Failed to recv %i bytes for %s: %s",
				      len, what, strerror(errno));
		return -1;
	}

	return r;
}


/**
 * Parse a single message from buf and passes it to the application.
 * 'rko' is optional and will be used, without enqueuing, instead of
 * creating a new op, if non-NULL.
 *
 * If 'offset_len' is non-zero it depicts the outer (compressed) message's
 * offset length.
 *
 * Returns -1 on failure or the consumed data length on success.
 */
static int rd_kafka_msg_parse (rd_kafka_t *rk, char *buf, int len,
			       rd_kafka_op_t *rko, uint64_t offset_len) {
	struct rd_kafkap_msg *msg;
	char *payload;

	/* Transmissions may be terminated mid-message, this is
	 * a soft-error that should be ignored and the client
	 * should try again. */
	msg = (struct rd_kafkap_msg *)buf;

	msg->rkpm_len = ntohl(msg->rkpm_len);
		
	if (msg->rkpm_len < sizeof(*msg) - sizeof(msg->rkpm_len)) {
		/* Formatting error. */
		rd_kafka_op_reply0(rk, rko, RD_KAFKA_OP_FETCH,
				   RD_KAFKA_RESP_ERR__BAD_MSG,
				   0, NULL, 0, 0);
		return -1;
	}


	if (!offset_len)
		offset_len = msg->rkpm_len + sizeof(msg->rkpm_len);

	if (msg->rkpm_len > 4000000) {
		rd_kafka_op_reply0(rk, rko, RD_KAFKA_OP_FETCH,
				   RD_KAFKA_RESP_ERR__BAD_MSG,
				   0, NULL, 0, offset_len);
		return msg->rkpm_len + sizeof(msg->rkpm_len);
	}

	msg->rkpm_len -= sizeof(*msg) - sizeof(msg->rkpm_len);

	payload = malloc(msg->rkpm_len);
	memcpy(payload, msg+1, msg->rkpm_len);

	rd_kafka_op_reply0(rk, rko, RD_KAFKA_OP_FETCH, 0,
			   msg->rkpm_compression,
			   payload, msg->rkpm_len, offset_len);

	return sizeof(*msg) + msg->rkpm_len;
}



/**
 * Receive an entire message from the broker.
 *
 * Returns the number of data reply ops created.
 *
 * Locality: Kafka thread
 */
static int rd_kafka_recv (rd_kafka_t *rk) {
	struct rd_kafkap_resp resp;
	int replycnt = 0;

	if (rd_kafka_recv0(rk, "response header",
			   &resp, sizeof(resp), 0) == -1)
		return 0;

	resp.rkprp_len = ntohl(resp.rkprp_len);
	resp.rkprp_error = ntohs(resp.rkprp_error);

	/* Error from broker? */
	if (resp.rkprp_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
		rd_kafka_op_reply(rk, RD_KAFKA_OP_FETCH, resp.rkprp_error, 0,
				  NULL, 0, 0);
		/* Consume remaining buffer */
		if (resp.rkprp_len)
			rd_kafka_recv_null(rk, resp.rkprp_len - 4);
		return 0;
	}

	if (resp.rkprp_len < sizeof(resp.rkprp_error) ||
	    resp.rkprp_len > rk->rk_conf.max_msg_size) {
		rd_kafka_fail(rk, "Invalid (or too long) response length %lu",
			      resp.rkprp_len);
		return 0;
	}

	resp.rkprp_len -= sizeof(resp.rkprp_error);

	/* Start chewing off messages, send one op back to application
	 * for each message parsed. */
	while (resp.rkprp_len >= sizeof(struct rd_kafkap_msg)) {
		struct rd_kafkap_msg msg;
		char *buf;
		
		/* Transmissions may be terminated mid-message, this is
		 * a soft-error that should be ignored and the client
		 * should try again at a later offset. */
		if (rd_kafka_recv0(rk, "message",
				   &msg, sizeof(msg), 0) == -1)
			return replycnt;

		msg.rkpm_len = ntohl(msg.rkpm_len);
		resp.rkprp_len -= sizeof(msg);

		if (msg.rkpm_len < sizeof(msg) - sizeof(msg.rkpm_len))
			return replycnt;

		if (msg.rkpm_len > rk->rk_conf.max_msg_size) {
			rd_kafka_fail(rk, "Invalid (or too long) response "
				      "message length %lu",
				      msg.rkpm_len);
			return replycnt;
		}

		msg.rkpm_len -= sizeof(msg) - 4;

		if (msg.rkpm_len > resp.rkprp_len) {
			/* Partial message, drop it. */
			if (resp.rkprp_len)
				rd_kafka_recv_null(rk, resp.rkprp_len);
			return replycnt;
		}

		/*
		 * Message payload
		 */
		resp.rkprp_len -= msg.rkpm_len;

		buf = malloc(msg.rkpm_len);
		
		if (rd_kafka_recv0(rk, "message payload",
				   buf, msg.rkpm_len, 0) == -1) {
			free(buf);
			return replycnt;
		}

		rk->rk_consumer.offset += msg.rkpm_len + sizeof(msg);

		rd_kafka_op_reply(rk, RD_KAFKA_OP_FETCH, 0,
				  msg.rkpm_compression,
				  buf, msg.rkpm_len, rk->rk_consumer.offset);

		replycnt++;
	}

	/* Drop padding */
	if (resp.rkprp_len > 0)
		rd_kafka_recv_null(rk, resp.rkprp_len);

	return replycnt;
}




/**
 * Send PRODUCE message.
 *
 * Locality: Kafka thread
 */
static void rd_kafka_produce_send (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	struct rd_kafkap_msg msg = {
	rkpm_len: htonl(sizeof(msg) - sizeof(msg.rkpm_len) + rko->rko_len),
	rkpm_magic: RD_KAFKAP_MSG_MAGIC_COMPRESSION_ATTR,
	rkpm_compression: RD_KAFKAP_MSG_COMPRESSION_NONE,
	rkpm_cksum: htonl(rd_crc32(rko->rko_payload, rko->rko_len)),
	};
	struct rd_kafkap_produce prod = {
	rkpp_msgs_len: htonl(sizeof(msg) + rko->rko_len),
	
	};

	rd_kafka_send_request(rk,
			      RD_KAFKAP_PRODUCE,
			      rd_kafka_topicpart_serialize(rko->rko_topic,
							   rko->rko_partition),
			      sizeof(prod), &prod,
			      sizeof(msg), &msg,
			      rko->rko_len, rko->rko_payload,
			      RD_KAFKA_SEND_END);
}


/**
 * Send FETCH message
 *
 * Locality: Kafka thread
 */
static void rd_kafka_fetch_send (rd_kafka_t *rk, uint64_t offset,
				 uint32_t max_size) {
	struct rd_kafkap_fetch_req freq = {
	rkpfr_offset: htobe64(offset),
	rkpfr_max_size: htonl(max_size),
	};

	rd_kafka_send_request(rk,
			      RD_KAFKAP_FETCH,
			      rd_kafka_topicpart_serialize(rk->
							   rk_consumer.topic,
							   rk->
							   rk_consumer.
							   partition),
			      sizeof(freq), &freq,
			      RD_KAFKA_SEND_END);
}



/**
 * Producer: Wait for PRODUCE events from application.
 *
 * Locality: Kafka thread
 */
static void rd_kafka_wait_op (rd_kafka_t *rk) {
	
	while (!rk->rk_terminate && rk->rk_state == RD_KAFKA_STATE_UP) {
		rd_kafka_op_t *rko =
			rd_kafka_q_pop(&rk->rk_op, RD_POLL_INFINITE);
		
		rd_kafka_produce_send(rk, rko);

                rd_kafka_op_destroy(rk, rko);
	}
}		



/**
 * Consumer: Wait for IO from broker.
 *
 * Locality: Kafka thread
 */
static void rd_kafka_consumer_wait_io (rd_kafka_t *rk) {
	while (!rk->rk_terminate && rk->rk_state == RD_KAFKA_STATE_UP) {
		int r;

		if (rd_kafka_replyq_len(rk) <
		    rk->rk_conf.consumer.replyq_low_thres) {
			/* Below low watermark, fetch more messages. */
			rd_kafka_fetch_send(rk,
					    rk->rk_consumer.offset,
					    rk->rk_conf.consumer.max_size);
		} else {
			struct pollfd pfd = { fd: rk->rk_broker.s,
					      events: POLLIN };
			/* We have messages, wait for more data from broker,
			 * if any. */
			r = poll(&pfd, 1, rk->rk_conf.consumer.poll_interval);

			if (r == -1) { /* Error */
				if (errno == EINTR)
					continue;

				rd_kafka_fail(rk,
					      "Failed to poll socket %i: %s",
					      rk->rk_broker.s,
					      strerror(errno));
				break;
			} else if (r == 0) /* Timeout */
				continue;
		}

		/* Blocking receive of message. */
		r = rd_kafka_recv(rk);

		/* No messages received? Wait a while before retrying. */
		if (!r)
			usleep(rk->rk_conf.consumer.poll_interval * 1000);
	}
}		


/**
 * Kafka thread's main loop.
 *
 * Locality: Kafka thread.
 */
static void *rd_kafka_thread_main (void *arg) {
	rd_kafka_t *rk = arg;

	while (!rk->rk_terminate) {
		switch (rk->rk_state)
		{
		case RD_KAFKA_STATE_DOWN:
			/* ..connect() will block until done (or failure) */
			if (rd_kafka_connect(rk) == -1)
				sleep(1); /*Sleep between connection attempts*/
			break;
		case RD_KAFKA_STATE_CONNECTING:
			break;
		case RD_KAFKA_STATE_UP:
			/* .._wait_*() blocks for as long as the
			 * state remains UP. */
			if (rk->rk_type == RD_KAFKA_PRODUCER)
				rd_kafka_wait_op(rk);
			else
				rd_kafka_consumer_wait_io(rk);
			break;
		}
	}

	rd_kafka_destroy(rk);

	return NULL;
}


static const char *rd_kafka_type2str (rd_kafka_type_t type) {
	static const char *types[] = {
		[RD_KAFKA_PRODUCER] = "producer",
		[RD_KAFKA_CONSUMER] = "consumer",
	};
	return RD_ARRAY_ELEM(types, type) ? : "???";
}


const char *rd_kafka_err2str (rd_kafka_resp_err_t err) {
	switch (err)
	{
	case RD_KAFKA_RESP_ERR__BAD_MSG:
		return "Local: Bad message format";
	case RD_KAFKA_RESP_ERR__BAD_COMPRESSION:
		return "Local: Invalid compressed data";
	case RD_KAFKA_RESP_ERR__FAIL:
		return "Local: Communication failure with broker";
	case RD_KAFKA_RESP_ERR_UNKNOWN:
		return "Unknown error";
	case RD_KAFKA_RESP_ERR_NO_ERROR:
		return "Success";
	case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
		return "Broker: Offset out of range";
	case RD_KAFKA_RESP_ERR_INVALID_MSG:
		return "Broker: Invalid message";
	case RD_KAFKA_RESP_ERR_WRONG_PARTITION:
		return "Broker: Wrong partition";
	case RD_KAFKA_RESP_ERR_INVALID_FETCH_SIZE:
		return "Broker: Invalid fetch size";
	default:
		return rd_tsprintf("Err-%i?", err);
	}
}


rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, const char *broker,
			  const rd_kafka_conf_t *conf) {
	rd_kafka_t *rk;
	rd_sockaddr_list_t *rsal;
	const char *errstr;
	static int rkid = 0;
	int err;

	/* If broker is NULL, default it to localhost. */
	if (!broker)
		broker = "localhost";

	if (!(rsal = rd_getaddrinfo(broker, RD_KAFKA_PORT_STR, AI_ADDRCONFIG,
				    AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP,
				    &errstr))) {
		rd_kafka_log(NULL, LOG_ERR, "GETADDR",
			     "getaddrinfo failed for '%s': %s",
			     broker, errstr);
		return NULL;
	}
			
	/*
	 * Set up the handle.
	 */
	rk = calloc(1, sizeof(*rk));

	rk->rk_type = type;
	rk->rk_broker.rsal = rsal;
	rk->rk_broker.s = -1;

	if (conf)
		rk->rk_conf = *conf;
	else
		rk->rk_conf = rd_kafka_defaultconf;

	rk->rk_refcnt = 2; /* One for caller, one for us. */

	if (rk->rk_type == RD_KAFKA_CONSUMER)
		rk->rk_refcnt++; /* Add another refcount for recv thread */

	rd_kafka_set_state(rk, RD_KAFKA_STATE_DOWN);

	pthread_mutex_init(&rk->rk_lock, NULL);

	rd_kafka_q_init(&rk->rk_op);
	rd_kafka_q_init(&rk->rk_rep);

	switch (rk->rk_type)
	{
	case RD_KAFKA_CONSUMER:
		/* Set up consumer specifics. */

		rk->rk_consumer.offset_file_fd = -1;
		assert(rk->rk_conf.consumer.topic);
		rk->rk_consumer.topic =	strdup(rk->rk_conf.consumer.topic);
		rk->rk_consumer.partition = rk->rk_conf.consumer.partition;
		rk->rk_consumer.offset = rk->rk_conf.consumer.offset;
		rk->rk_consumer.app_offset = rk->rk_conf.consumer.offset;

		/* File-based load&store of offset. */
		if (rk->rk_conf.consumer.offset_file) {
			char buf[32];
			int r;
			char *tmp;
			mode_t mode;

			/* If path is a directory we need to generate the
			 * filename (which is a good idea). */
			mode = rd_file_mode(rk->rk_conf.consumer.offset_file);
			if (mode == 0) {
				/* Error: bail out. */
				int errno_save = errno;
				/* Avoid free of non-strdup:ed pointer */
				rk->rk_conf.consumer.offset_file = NULL;
				rd_kafka_destroy0(rk);
				errno = errno_save;
				return NULL;
			}

			if (S_ISDIR(mode))
				rk->rk_conf.consumer.offset_file =
					rd_tsprintf("%s/%s-%"PRIu32,
						    rk->rk_conf.consumer.
						    offset_file,
						    rk->rk_consumer.topic,
						    rk->rk_consumer.partition);

			rk->rk_conf.consumer.offset_file =
				strdup(rk->rk_conf.consumer.offset_file);

			/* Open file, or create it. */
			if ((rk->rk_consumer.offset_file_fd =
			     open(rk->rk_conf.consumer.offset_file,
				  O_CREAT|O_RDWR |
				  (rk->rk_conf.consumer.offset_file_flags &
				   RD_KAFKA_OFFSET_FILE_FLAGMASK),
				  0640)) == -1) {
				rd_kafka_destroy0(rk);
				return NULL;
			}

			/* Read current offset from file, or default to 0. */
			r = read(rk->rk_consumer.offset_file_fd,
				 buf, sizeof(buf)-1);
			if (r == -1) {
				rd_kafka_destroy0(rk);
				return NULL;
			}

			buf[r] = '\0';

			rk->rk_consumer.offset = strtoull(buf, &tmp, 10);
			if (tmp == buf) /* empty or not an integer */
				rk->rk_consumer.offset = 0;
			else
				rd_kafka_dbg(rk, "OFFREAD",
					     "Read offset %"PRIu64" from "
					     "file %s",
					     rk->rk_consumer.offset,
					     rk->rk_conf.consumer.offset_file);
		}

		break;
	case RD_KAFKA_PRODUCER:
		break;
	}


	/* Construct a conveniant name for this handle. */
	snprintf(rk->rk_broker.name, sizeof(rk->rk_broker.name), "%s#%s-%i",
		 broker, rd_kafka_type2str(rk->rk_type), rkid++);

	/* Start the Kafka thread. */
	if ((err = pthread_create(&rk->rk_thread, NULL,
				  rd_kafka_thread_main, rk))) {
		rd_sockaddr_list_destroy(rk->rk_broker.rsal);
		free(rk);
		return NULL;
	}


	return rk;
}




rd_kafka_t *rd_kafka_new_consumer (const char *broker,
				   const char *topic,
				   uint32_t partition,
				   uint64_t offset,
				   const rd_kafka_conf_t *conf) {
	rd_kafka_t *rk;
	rd_kafka_conf_t conf0;

	if (!conf)
		conf0 = rd_kafka_defaultconf;
	else
		conf0 = *conf;

	conf0.consumer.topic = (char *)topic;
	conf0.consumer.partition = partition;
	conf0.consumer.offset = offset;

	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, broker, &conf0)))
		return NULL;


	return rk;
}



rd_kafka_op_t *rd_kafka_consume (rd_kafka_t *rk, int timeout_ms) {
	rd_kafka_op_t *reply;

	if (!(reply = rd_kafka_q_pop(&rk->rk_rep, timeout_ms))) {
		errno = ETIMEDOUT;
		return NULL;
	}

	/* Update application offset for returned messages.
	 * The offset points to the next unread message. */
	if (reply->rko_offset) {
		rk->rk_consumer.app_offset = reply->rko_offset;
		if (!(rk->rk_conf.flags & RD_KAFKA_CONF_F_APP_OFFSET_STORE))
			rd_kafka_offset_store(rk, rk->rk_consumer.app_offset);
	}

	return reply;
}



/**
 * Produce one single message and send it off to the broker.
 *
 * See rdkafka.h for 'msgflags'.
 *
 * Locality: application thread
 */
int rd_kafka_produce (rd_kafka_t *rk, char *topic, uint32_t partition,
		      int msgflags,
		      char *payload, size_t len) {
	rd_kafka_op_t *rko;

	if (rk->rk_conf.producer.max_outq_msg_cnt &&
	    rk->rk_op.rkq_qlen >= rk->rk_conf.producer.max_outq_msg_cnt) {
		errno = ENOBUFS;
		return -1;
	}

	rko = calloc(1, sizeof(*rko));

	rko->rko_type      = RD_KAFKA_OP_PRODUCE;
	rko->rko_topic     = topic;
	rko->rko_partition = partition;
	rko->rko_flags    |= msgflags;
	rko->rko_payload   = payload;
	rko->rko_len       = len;

	rd_kafka_q_enq(&rk->rk_op, rko);

	return 0;
}





/**
 * Decompress message payload.
 */
void rd_kafka_op_inflate (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	uint64_t offset_len = rko->rko_len + sizeof(struct rd_kafkap_msg);
	uint64_t declen = 0;
	char *buf, *origbuf = NULL;
	int len = 0;
	rd_kafka_op_t *rko2;

	switch (rko->rko_compression)
	{
	case RD_KAFKAP_MSG_COMPRESSION_GZIP:

		if (!(buf = rd_gz_decompress(rko->rko_payload,
					     rko->rko_len, &declen))) {
			/* FIXME: How to interact with application on this? */
			rd_kafka_log(rk, LOG_WARNING, "GUNZIP",
				     "GZ-decompression of %i bytes failed for "
				     "message payload", rko->rko_len);
			goto fail;
		}
		break;

	case RD_KAFKAP_MSG_COMPRESSION_SNAPPY:
		rd_kafka_log(rk, LOG_WARNING, "INFLATE",
			     "FIXME: use application callback to "
			     "perform decompression");
		goto fail;

	default:
		rd_kafka_log(rk, LOG_WARNING, "INFLATE",
			     "FIXME: unknown compression type %i: "
			     "dropping message",
			     rko->rko_compression);
		goto fail;
	}

	len = declen;
	origbuf = buf;


	/* Reuse the current op for the first message so we have something
	 * to return to the current parsing application thread. */
	rko2 = rko;

	free(rko->rko_payload);
	rko->rko_payload = NULL;
	rko->rko_len = 0;

	/* Decompressed data is now in 'buf' with 'len' bytes,
	 * it will consist of one or more messages that we need to parse
	 * and enqueue for the application. */
	while (len > 0) {
		int r;

		if ((r = rd_kafka_msg_parse(rk, buf, len,
					    rko2, offset_len)) == -1) {
			/* Message formatting errors are serious, we cant
			 * decode the rest of the buffer now. */
			goto fail;
		}

		rko2 = NULL;

		len -= r;
		buf += r;
	}

	free(origbuf);

	return;

fail:
	if (origbuf)
		free(origbuf);

	rko->rko_err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
	free(rko->rko_payload);
	rko->rko_payload = NULL;
	rko->rko_len = 0;
}
