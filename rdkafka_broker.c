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
#include <sys/epoll.h>

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdtime.h"
#include "rdthread.h"
#include "rdcrc32.h"



const char *rd_kafka_broker_state_names[] = {
	"DOWN",
	"CONNECTING",
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
		     what, msg->msg_iovlen);

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

static void rd_kafka_broker_set_state (rd_kafka_broker_t *rkb,
				       int state) {
	if (rkb->rkb_state == state)
		return;

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "STATE",
		     "%s: Broker changed state %s -> %s", 
		     rkb->rkb_name,
		     rd_kafka_broker_state_names[rkb->rkb_state],
		     rd_kafka_broker_state_names[state]);

	rkb->rkb_state = state;
}



static void rd_kafka_buf_destroy (rd_kafka_buf_t *rkbuf) {

	if (rkbuf->rkbuf_buf2)
		free(rkbuf->rkbuf_buf2);

	free(rkbuf);
}

static struct iovec *rd_kafka_buf_iov_next (rd_kafka_buf_t *rkbuf) {
	
	assert(rkbuf->rkbuf_msg.msg_iovlen + 1 <= rkbuf->rkbuf_iovcnt);
	if (rkbuf->rkbuf_msg.msg_iovlen > 0)
		rkbuf->rkbuf_iovof +=
			rkbuf->rkbuf_iov[rkbuf->rkbuf_msg.msg_iovlen].iov_len;
	return &rkbuf->rkbuf_iov[rkbuf->rkbuf_msg.msg_iovlen++];
}


/**
 * Pushes 'buf' & 'len' onto the previously allocated iov stack for 'rkbuf'.
 * Returns the offset for the beginning of the pushed iovec.
 */
static int rd_kafka_buf_push (rd_kafka_buf_t *rkbuf, int flags,
			      void *buf, size_t len) {
	struct iovec *iov;

	iov = rd_kafka_buf_iov_next(rkbuf);

	/* FIXME: store and adhere to flags */
	iov->iov_base = buf;
	iov->iov_len = len;

	return rkbuf->rkbuf_iovof;
}


#define RD_KAFKA_HEADERS_IOV_CNT   2
#define RD_KAFKA_PAYLOAD_IOV_MAX  (IOV_MAX-RD_KAFKA_HEADERS_IOV_CNT)

static rd_kafka_buf_t *rd_kafka_buf_new (int iovcnt, size_t size) {
	rd_kafka_buf_t *rkbuf;
	const int iovcnt_fixed = RD_KAFKA_HEADERS_IOV_CNT;
	size_t iovsize = sizeof(struct iovec) * (iovcnt+iovcnt_fixed);
	size_t fullsize = iovsize + (sizeof(*rkbuf) + size);

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

	return rkbuf;
}


/**
 * Purge the wait-response queue.
 */
static void rd_kafka_broker_waitresp_purge (rd_kafka_broker_t *rkb,
					    rd_kafka_resp_err_t err) {
	rd_kafka_buf_t *rkbuf, *tmp;

	assert(pthread_self() == rkb->rkb_thread);

	TAILQ_FOREACH_SAFE(rkbuf, tmp, &rkb->rkb_waitresps,  rkbuf_link)
		rkbuf->rkbuf_cb(rkb, err, NULL, rkbuf, rkbuf->rkbuf_opaque);

	TAILQ_INIT(&rkb->rkb_waitresps);
	rkb->rkb_waitresp_cnt = 0;
}


/**
 * Scan the wait-response queue for message timeouts.
 */
static void rd_kafka_broker_waitresp_timeout_scan (rd_kafka_broker_t *rkb,
						   rd_ts_t now) {
	rd_kafka_buf_t *rkbuf, *tmp;
	int cnt = 0;

	assert(pthread_self() == rkb->rkb_thread);

	TAILQ_FOREACH_SAFE(rkbuf, tmp, &rkb->rkb_waitresps,  rkbuf_link) {
		if (likely(rkbuf->rkbuf_ts_timeout > now))
			continue;

		TAILQ_REMOVE(&rkb->rkb_waitresps, rkbuf, rkbuf_link);
		rkb->rkb_waitresp_cnt--;

		rkbuf->rkbuf_cb(rkb, RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
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
 * Locality: Kafka thread
 */
static void rd_kafka_broker_fail (rd_kafka_broker_t *rkb,
				  rd_kafka_resp_err_t err,
				  const char *fmt, ...) {
	va_list ap;
	int errno_save = errno;
	rd_kafka_toppar_t *rktp;

	pthread_mutex_lock(&rkb->rkb_lock);

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "BROKERFAIL",
		     "%s: failed: err: %s: (errno: %s)",
		     rkb->rkb_name, rd_kafka_err2str(rkb->rkb_rk, err),
		     strerror(errno_save));

	rkb->rkb_err.err = errno_save;

	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_DOWN);

	if (rkb->rkb_s != -1) {
		close(rkb->rkb_s);
		rkb->rkb_s = -1;
		rkb->rkb_pfd.fd = rkb->rkb_s;
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

		rd_rkb_log(rkb, LOG_ERR, "FAIL", "%s", rkb->rkb_err.msg);

		/* Send ERR op back to application for processing. */
		rd_kafka_op_reply(rkb->rkb_rk, RD_KAFKA_OP_ERR,
				  RD_KAFKA_RESP_ERR__FAIL, 0,
				  strdup(rkb->rkb_err.msg),
				  strlen(rkb->rkb_err.msg), 0);
	}

	rd_kafka_broker_waitresp_purge(rkb, err);

	pthread_mutex_unlock(&rkb->rkb_lock);

	/* Undelegate all toppars from this broker. */
	rd_kafka_broker_toppars_wrlock(rkb);
	while ((rktp = TAILQ_FIRST(&rkb->rkb_toppars))) {
		rd_kafka_broker_toppars_unlock(rkb);
		rd_rkb_dbg(rkb, TOPIC, "BRKTP",
			   "Undelegating %.*s [%"PRId32"]",
			   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			   rktp->rktp_partition);

		/* Undelegate */
		rd_kafka_topic_wrlock(rktp->rktp_rkt);
		rd_kafka_toppar_broker_delegate(rktp, NULL);
		rd_kafka_topic_unlock(rktp->rktp_rkt);

		rd_kafka_broker_toppars_wrlock(rkb);
	}
	rd_kafka_broker_toppars_unlock(rkb);

	/* Query for the topic leaders (async) */
	if (fmt)
		rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);
}

static ssize_t rd_kafka_broker_send (rd_kafka_broker_t *rkb,
				     const struct msghdr *msg) {
	ssize_t r;

	assert(rkb->rkb_state >= RD_KAFKA_BROKER_STATE_UP);
	assert(rkb->rkb_s != -1);

	r = sendmsg(rkb->rkb_s, msg, MSG_DONTWAIT);
	if (r == -1) {
		if (errno == EAGAIN)
			return 0;

		rd_kafka_dbg(rkb->rkb_rk, BROKER, "BRKSEND",
			     "sendmsg FAILED for iovlen %zd (%i)",
			     msg->msg_iovlen,
			     IOV_MAX);
		rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
				     "Send failed: %s", strerror(errno));
		rkb->rkb_c.tx_err++;
		return -1;
	}

	rkb->rkb_c.tx_bytes += r;
	rkb->rkb_c.tx++;
	return r;
}




static int rd_kafka_broker_resolve (rd_kafka_broker_t *rkb) {
	const char *errstr;

	if (rkb->rkb_rsal &&
	    rkb->rkb_t_rsal_last + rkb->rkb_rk->rk_conf.broker_addr_lifetime <
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
			rd_rkb_log(rkb, LOG_ERR, "GETADDR",
				   "failed to resolve '%s': %s",
				   rkb->rkb_nodename, errstr);
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
		TAILQ_FOREACH(prev, &rkb->rkb_outbufs, rkbuf_link)
			if (!(prev->rkbuf_flags & RD_KAFKA_OP_F_FLASH))
				break;

		if (prev)
			TAILQ_INSERT_AFTER(&rkb->rkb_outbufs,
					   prev, rkbuf, rkbuf_link);
		else
			TAILQ_INSERT_HEAD(&rkb->rkb_outbufs, rkbuf, rkbuf_link);
	} else {
		/* Insert message at tail of queue */
		TAILQ_INSERT_TAIL(&rkb->rkb_outbufs, rkbuf, rkbuf_link);
	}

	rd_atomic_add(&rkb->rkb_outbuf_cnt, 1);
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
		rkb->rkb_rk->rk_conf.metadata_request_timeout_ms * 1000;
	rkbuf->rkbuf_flags |= flags;

	if (size > 0) {
		if (!(flags & RD_KAFKA_OP_F_FREE)) {
			/* Duplicate data */
			memcpy(rkbuf->rkbuf_buf, buf, size);
			buf = rkbuf->rkbuf_buf;
		}

		rd_kafka_buf_push(rkbuf, flags, buf, size);
	}


	rd_kafka_broker_buf_enq1(rkb, ApiKey, rkbuf, reply_cb, opaque);
}



/* FIXME: error handling */
#define _CHECK_LEN(len) do {			\
	if (of + (len) > size)			\
		goto err;			\
	} while (0)
	

#define _READ(dstptr,len) do {			\
		_CHECK_LEN(len);		\
		memcpy((dstptr), buf+(of), (len));	\
		of += (len);				\
	} while (0)

#define _READ_I32(dstptr) do {						\
		_READ(dstptr, 4);					\
		*(int32_t *)(dstptr) = ntohl(*(int32_t *)(dstptr));	\
	} while (0)

#define _READ_I16(dstptr) do {						\
		_READ(dstptr, 2);					\
		*(int16_t *)(dstptr) = ntohs(*(int16_t *)(dstptr));	\
	} while (0)

#define _READ_STR(kstr) do {			\
	_CHECK_LEN(2);				\
	kstr = (rd_kafkap_str_t *)(buf+of);	\
	if (ntohs((kstr)->len) == -1)		\
		(kstr)->len = 0;		\
	_CHECK_LEN(ntohs((kstr)->len));		\
	of += 2 + ntohs((kstr)->len);		\
	} while (0)

#define _FAIL(reason...) do {					\
		rd_rkb_dbg(rkb, METADATA, "METADATA", reason);	\
		goto err;					\
	} while (0)


/**
 * Handle a Metadata response message.
 *
 * Locality: broker thread
 */
static void rd_kafka_metadata_handle (rd_kafka_broker_t *rkb,
				      const char *buf, size_t size) {
	struct {
		int32_t          NodeId;
		rd_kafkap_str_t *Host;
		int32_t          Port;
	}      *Brokers = NULL;
	int32_t Broker_cnt;

	struct {
		int16_t          ErrorCode;
		rd_kafkap_str_t *Name;
		struct {
			int16_t  ErrorCode;
			int32_t  PartitionId;
			int32_t  Leader;
			int32_t *Replicas;
			int32_t  Replicas_cnt;
			int32_t *Isr;
			int32_t  Isr_cnt;
		}  *PartitionMetadata;
		int32_t PartitionMetadata_cnt;
	}      *TopicMetadata = NULL;
	int32_t TopicMetadata_cnt;
	int i, j, k;
	int of = 0;



	/* Read Brokers */
	_READ_I32(&Broker_cnt);
	rd_rkb_dbg(rkb, METADATA, "METADATA", "%"PRId32" brokers", Broker_cnt);
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

	/* Update Leader for each topic we know about */
	for (i = 0 ; i < TopicMetadata_cnt ; i++) {
		const char *topic = rd_kafkap_strdupa(TopicMetadata[i].Name);
		
		rd_kafka_topic_partition_cnt_update(rkb->rkb_rk, topic,
						    TopicMetadata[i].
						    PartitionMetadata_cnt);

		for (j = 0 ;
		     j < TopicMetadata[i].PartitionMetadata_cnt ; j++) {
			rd_rkb_dbg(rkb, METADATA, "METADATA",
				   "  Topic #%i/%i: %s partition %"PRId32
				   " Leader %"PRId32,
				   i, TopicMetadata_cnt,
				   topic,
				   TopicMetadata[i].
				   PartitionMetadata[j].PartitionId,
				   TopicMetadata[i].
				   PartitionMetadata[j].Leader);
				     
			rd_kafka_topic_update(rkb->rkb_rk,
					      topic,
					      TopicMetadata[i].
					      PartitionMetadata[j].PartitionId,
					      TopicMetadata[i].
					      PartitionMetadata[j].Leader);
		}

		/* Try to assign unassigned messages to new partitions */
		rd_kafka_topic_assign_uas(rkb->rkb_rk, topic);
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

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "===== Received metadata from %s =====",
		   rkb->rkb_name);

	if (unlikely(err)) {
		/* FIXME: handle error */
		rd_rkb_log(rkb, LOG_WARNING, "METADATA",
			   "Metadata request failed: %i", err);
	} else {
		rd_kafka_metadata_handle(rkb,
					 reply->rkbuf_buf2,
					 reply->rkbuf_len);
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
	int32_t arrsize;
	size_t tnamelen = 0;
	rd_kafka_topic_t *rkt;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Request metadata: %s", reason ? : "");

	/* If called from other thread than the broker's own post an
	 * op for the broker's thread instead since all transmissions must
	 * be performed by the broker thread. */
	if (pthread_self() != rkb->rkb_thread) {
		rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_METADATA_REQ);
		rko->rko_rkt = only_rkt;
		rd_kafka_q_enq(&rkb->rkb_ops, rko);
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "Request metadata: scheduled: not in broker thread");
		return;
	}

	/* FIXME: Use iovs and ..next here */

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Requesting metadata for %stopics",
		   all_topics ? "all ": "known ");

	if (all_topics)
		arrsize = 0;
	else {
		arrsize = rkb->rkb_rk->rk_topic_cnt;

		rd_kafka_lock(rkb->rkb_rk);
		/* Calculate size to hold all known topics */
		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			if (only_rkt && only_rkt != rkt)
				continue;

			tnamelen += RD_KAFKAP_STR_SIZE(rkt->rkt_topic);
		}

	}
	
	buf = malloc(sizeof(arrsize) + tnamelen);
	arrsize = htonl(arrsize);
	memcpy(buf+of, &arrsize, 4);
	of += 4;


	if (!all_topics) {
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


	rd_kafka_broker_buf_enq(rkb, RD_KAFKAP_Metadata,
				buf, of,
				RD_KAFKA_OP_F_FREE|RD_KAFKA_OP_F_FLASH,
				rd_kafka_broker_metadata_reply, NULL);
}



static rd_kafka_broker_t *rd_kafka_broker_any (rd_kafka_t *rk, int state) {
	rd_kafka_broker_t *rkb;

	/* FIXME: lock */
	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link)
		if (rkb->rkb_state == state) {
			rd_kafka_broker_keep(rkb);
			return rkb;
		}

	return NULL;
}


/**
 * Trigger broker metadata query for topic leader.
 * 'rkt' may be NULL to query for all topics.
 */
void rd_kafka_topic_leader_query (rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
	rd_kafka_broker_t *rkb;

	rd_kafka_lock(rk);
	if (!(rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP))) {
		rd_kafka_unlock(rk);
		return; /* No brokers are up */
	}
	rd_kafka_unlock(rk);

	rd_kafka_broker_metadata_req(rkb, 0, rkt);

	/* Release refcnt from rd_kafka_broker_any() */
	rd_kafka_broker_destroy(rkb);
}



static rd_kafka_buf_t *rd_kafka_waitresp_find (rd_kafka_broker_t *rkb,
					       int32_t corrid) {
	rd_kafka_buf_t *rkbuf;

	assert(pthread_self() == rkb->rkb_thread);

	/* FIXME: just making sure its the first message.
	*         we could probably skip the queue (scan) */
	assert(!TAILQ_EMPTY(&rkb->rkb_waitresps));
	assert(TAILQ_FIRST(&rkb->rkb_waitresps)->rkbuf_corrid == corrid);

	TAILQ_FOREACH(rkbuf, &rkb->rkb_waitresps, rkbuf_link)
		if (rkbuf->rkbuf_corrid == corrid) {
			TAILQ_REMOVE(&rkb->rkb_waitresps, rkbuf, rkbuf_link);
			rkb->rkb_waitresp_cnt--;
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
		/* FIXME: unknown response */
		rd_rkb_dbg(rkb, BROKER, "RESPONSE",
			   "Unknown response for CorrId %"PRId32,
			   rkbuf->rkbuf_reshdr.CorrId);
		goto err;
	}


	/* Call callback. Ownership of 'rkbuf' is delegated to callback. */
	req->rkbuf_cb(rkb, 0, rkbuf, req, req->rkbuf_opaque);

	return 0;

err:
	/* FIXME */
	rd_rkb_dbg(rkb, BROKER, "RESP",
		   "Response error for corrid %"PRId32,
		   rkbuf->rkbuf_reshdr.CorrId);

	rd_kafka_buf_destroy(rkbuf);
	return -1;
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
			printf(" #%i/%zd and %zd: of %zd, len %zd, "
			       "vof %zd: iov %zd\n",
			       i, src->msg_iovlen, dst->msg_iovlen,
			       of, len, vof, src->msg_iov[i].iov_len);
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

		rkbuf->rkbuf_len = ntohl(rkbuf->rkbuf_reshdr.Size) -4/*CorrId*/;
		rkbuf->rkbuf_reshdr.CorrId = ntohl(rkbuf->rkbuf_reshdr.CorrId);

		/* Make sure message size is within tolerable limits */
		if (rkbuf->rkbuf_len < 0 ||
		    rkbuf->rkbuf_len > rkb->rkb_rk->rk_conf.max_msg_size) {
			snprintf(errstr, sizeof(errstr),
				 "Invalid message size %zd (0..%i)",
				 rkbuf->rkbuf_len,
				 rkb->rkb_rk->rk_conf.max_msg_size);
			rkb->rkb_c.rx_err++;
			err_code = RD_KAFKA_RESP_ERR__BAD_MSG;
			goto err;
		}

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
		rkb->rkb_c.rx++;
		rkb->rkb_c.rx_bytes += rkbuf->rkbuf_of;
		rd_kafka_req_response(rkb, rkbuf);
	}

	return 1;

err:
	rd_kafka_broker_fail(rkb, err_code, "Receive failed: %s", errstr);
	return -1;
}


static int rd_kafka_broker_connect (rd_kafka_broker_t *rkb) {
	rd_sockaddr_inx_t *sinx;

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

	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_CONNECTING);

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

	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_UP);
	rkb->rkb_err.err = 0;

	rkb->rkb_pfd.fd = rkb->rkb_s;
	rkb->rkb_pfd.events = EPOLLIN;

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
	       (rkbuf = TAILQ_FIRST(&rkb->rkb_outbufs))) {
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
		TAILQ_REMOVE(&rkb->rkb_outbufs, rkbuf, rkbuf_link);
		assert(rkb->rkb_outbuf_cnt > 0);
		rd_atomic_sub(&rkb->rkb_outbuf_cnt, 1);

		/* Put buffer on response wait list unless we are not
		 * expecting a response (required_acks=0). */
		if (!(rkbuf->rkbuf_flags & RD_KAFKA_OP_F_NO_RESPONSE)) {
			TAILQ_INSERT_TAIL(&rkb->rkb_waitresps, rkbuf,
					  rkbuf_link);
			rkb->rkb_waitresp_cnt++;
		} else {
			/* Call buffer callback for delivery report. */
			rkbuf->rkbuf_cb(rkb, 0, NULL, rkbuf,
					rkbuf->rkbuf_opaque);
		}

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

	rkb->rkb_retrybuf_cnt++;
	rkbuf->rkbuf_ts_retry = rd_clock() +
		(rkb->rkb_rk->rk_conf.producer.retry_backoff_ms * 1000);
	/* Reset send offset */
	rkbuf->rkbuf_of = 0;

	TAILQ_INSERT_TAIL(&rkb->rkb_retrybufs, rkbuf, rkbuf_link);
}


/**
 * Move buffers that have expired their retry backoff time from the 
 * retry queue to the outbuf.
 */
static void rd_kafka_broker_retry_bufs_move (rd_kafka_broker_t *rkb) {
	rd_ts_t now = rd_clock();
	rd_kafka_buf_t *rkbuf;

	while ((rkbuf = TAILQ_FIRST(&rkb->rkb_retrybufs))) {
		if (rkbuf->rkbuf_ts_retry > now)
			break;

		rkb->rkb_retrybuf_cnt--;
		TAILQ_REMOVE(&rkb->rkb_retrybufs, rkbuf, rkbuf_link);

		rd_kafka_broker_buf_enq0(rkb, rkbuf, 0/*tail*/);
	}
}
	

/**
 * Propagate delivery report for message queue.
 */
static void rd_kafka_dr_msgq (rd_kafka_t *rk,
			      rd_kafka_msgq_t *rkmq, rd_kafka_resp_err_t err) {

	if (rk->rk_conf.producer.dr_cb) {
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
 * Locality: io thread
 */
static void rd_kafka_produce_msgset_reply (rd_kafka_broker_t *rkb,
					   rd_kafka_resp_err_t err,
					   rd_kafka_buf_t *reply,
					   rd_kafka_buf_t *request,
					   void *opaque) {

	rd_rkb_dbg(rkb, MSG, "MSGSET",
		   "MessageSet with %i message(s) %sdelivered",
		   request->rkbuf_msgq.rkmq_msg_cnt, err ? "not ": "");

	if (err) {
		rd_rkb_dbg(rkb, MSG, "MSGSET", "MessageSet with %i message(s) "
			   "encountered error: %s",
			   request->rkbuf_msgq.rkmq_msg_cnt,
			   rd_kafka_err2str(rkb->rkb_rk, err));

		switch (err)
		{
		case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
		case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
		case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
		case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
			/* Request metadata information update */
			rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);

		case RD_KAFKA_RESP_ERR__TRANSPORT:
		case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
			/* Try again */
			if (++request->rkbuf_retries <
			    rkb->rkb_rk->rk_conf.producer.max_retries) {

				if (reply)
					rd_kafka_buf_destroy(reply);
							
				rd_kafka_broker_buf_retry(rkb, request);
				return;
			}
			break;

		default:
			break;
		}

		/* FALLTHRU */
	}

	rd_kafka_dr_msgq(rkb->rkb_rk, &request->rkbuf_msgq, err);


	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}


static int rd_kafka_broker_send_toppar (rd_kafka_broker_t *rkb,
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
			rkb->rkb_rk->rk_conf.producer.batch_num_messages);
	assert(msgcnt > 0);
	iovcnt = 3 + (4 * msgcnt);

	if (iovcnt > RD_KAFKA_PAYLOAD_IOV_MAX) {
		iovcnt = RD_KAFKA_PAYLOAD_IOV_MAX;
		msgcnt = ((iovcnt / 4) - 3);
	}

	if (0)
		rd_rkb_dbg(rkb, MSG, "PRODUCE",
			   "Serve %i/%i messages (%i iovecs) "
			   "for %.*s [%"PRId32"] (%zd bytes)",
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
	rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
			  &prodhdr->part1, sizeof(prodhdr->part1));

	/* Insert topic */
	rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
			  rkt->rkt_topic,
			  RD_KAFKAP_STR_SIZE(rkt->rkt_topic));

	/* Insert second part of Produce header */
	prodhdr->part2.PartitionArrayCnt = htonl(1);
	prodhdr->part2.Partition = htonl(rktp->rktp_partition);
	/* Will be finalized later*/
	prodhdr->part2.MessageSetSize = 0;

	rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
			  &prodhdr->part2, sizeof(prodhdr->part2));

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
		rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
				  &msghdr->part3,
				  sizeof(msghdr->part3));


		/* Key */
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					(void *)rkm->rkm_key,
					RD_KAFKAP_BYTES_SIZE(rkm->
							     rkm_key));

		rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
				  rkm->rkm_key,
				  RD_KAFKAP_BYTES_SIZE(rkm->rkm_key));


		/* Value(payload) length */
		msghdr->part4.Value_len = htonl(rkm->rkm_len);
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					(void *)
					&msghdr->part4.Value_len,
					sizeof(msghdr->part4.
					       Value_len));

		rd_kafka_buf_push(rkbuf, RD_KAFKA_OP_F_NOCOPY,
				  &msghdr->part4,
				  sizeof(msghdr->part4));
			

		/* Payload */
		msghdr->part3.Crc =
			rd_crc32_update(msghdr->part3.Crc,
					rkm->rkm_payload,
					rkm->rkm_len);
		rd_kafka_buf_push(rkbuf, rkm->rkm_flags,
				  rkm->rkm_payload, rkm->rkm_len);


		/* Finalize Crc */
		msghdr->part3.Crc =
			htonl(rd_crc32_finalize(msghdr->part3.Crc));
		msghdr++;
	}

	rktp->rktp_c.tx_msgs  += rkbuf->rkbuf_msgq.rkmq_msg_cnt;
	rktp->rktp_c.tx_bytes += prodhdr->part2.MessageSetSize;

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

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Produce, rkbuf,
				 rd_kafka_produce_msgset_reply, NULL);


	return cnt;
}

/**
 * Serve a broker op (an op posted by another thread to be handled by
 * this broker's thread).
 */
static void rd_kafka_broker_op_serve (rd_kafka_broker_t *rkb,
				      rd_kafka_op_t *rko) {

	assert(pthread_self() == rkb->rkb_thread);

	rd_rkb_dbg(rkb, BROKER, "BRKOP",
		   "Serve broker op type %i", rko->rko_type);

	switch (rko->rko_type)
	{
	case RD_KAFKA_OP_METADATA_REQ:
		if (rko->rko_rkt)
			rd_kafka_broker_metadata_req(rkb, 0, rko->rko_rkt,
						     NULL);
		else
			rd_kafka_broker_metadata_req(rkb, 1 /*all topics*/,
						     NULL, NULL);
		break;

	default:
		assert(!*"unhandled op type");
	}

	rd_kafka_op_destroy(rkb->rkb_rk, rko);
}


static void rd_kafka_broker_io_serve (rd_kafka_broker_t *rkb) {
	rd_kafka_op_t *rko;

	/* Serve broker ops */
	if (unlikely(rd_kafka_q_len(&rkb->rkb_ops) > 0))
		while ((rko = rd_kafka_q_pop(&rkb->rkb_ops, RD_POLL_NOWAIT)))
			rd_kafka_broker_op_serve(rkb, rko);
	
	if (rkb->rkb_outbuf_cnt > 0)
		rkb->rkb_pfd.events |= EPOLLOUT;
	else
		rkb->rkb_pfd.events &= ~EPOLLOUT;

	if (poll(&rkb->rkb_pfd, 1,
		 rkb->rkb_rk->rk_conf.producer.buffering_max_ms) <= 0)
		return;
	
	if (rkb->rkb_pfd.revents & EPOLLIN)
		while (rd_kafka_recv(rkb) > 0)
			;

	if (rkb->rkb_pfd.revents & POLLHUP)
		return rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
					    "Connection closed");
	
	if (rkb->rkb_pfd.revents & EPOLLOUT)
		while (rd_kafka_send(rkb) > 0)
			;
}



/**
 * Idle function for unassigned brokers
 */
static void rd_kafka_broker_ua_idle (rd_kafka_broker_t *rkb) {
	while (rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP)
		rd_kafka_broker_io_serve(rkb);
}

static void rd_kafka_broker_serve (rd_kafka_broker_t *rkb) {
	rd_ts_t last_timeout_scan = rd_clock();
	rd_kafka_msgq_t timedout = RD_KAFKA_MSGQ_INITIALIZER(timedout);

	assert(pthread_self() == rkb->rkb_thread);

	rd_kafka_broker_lock(rkb);

	while (rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP) {
		rd_kafka_toppar_t *rktp;
		int cnt;
		rd_ts_t now;
		int do_timeout_scan = 0;

		now = rd_clock();
		if (unlikely(last_timeout_scan * 1000000 < now)) {
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

				while (rktp->rktp_xmit_msgq.rkmq_msg_cnt > 0) {
					int r;
					r = rd_kafka_broker_send_toppar(rkb,
									rktp);
					if (r > 0)
						cnt += r;
				}
			}

		} while (cnt);

		/* Trigger delivery report for timed out messages */
		if (unlikely(timedout.rkmq_msg_cnt > 0))
			rd_kafka_dr_msgq(rkb->rkb_rk, &timedout,
					 RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT);

		rd_kafka_broker_toppars_unlock(rkb);

		/* Check and move retry buffers */
		if (unlikely(rkb->rkb_retrybuf_cnt) > 0)
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

	rd_thread_sigmask(SIG_BLOCK,
			  SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2,
			  RD_SIG_END);
	
	rd_rkb_dbg(rkb, BROKER, "BRKMAIN", "Enter main broker thread");

	while (!rkb->rkb_terminate) {
		switch (rkb->rkb_state)
		{
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

		case RD_KAFKA_BROKER_STATE_CONNECTING:
			break;

		case RD_KAFKA_BROKER_STATE_UP:
			if (rkb->rkb_nodeid == RD_KAFKA_NODEID_UA)
				rd_kafka_broker_ua_idle(rkb);
			else
				rd_kafka_broker_serve(rkb);
			
			break;
		}

	}

	/* FIXME: destroy rkb? */

	return NULL;
}


void rd_kafka_broker_destroy (rd_kafka_broker_t *rkb) {
	if (rd_atomic_sub(&rkb->rkb_refcnt, 1) > 0)
		return;

	rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__DESTROY, NULL);

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

	rkb = calloc(1, sizeof(*rkb));

	snprintf(rkb->rkb_nodename, sizeof(rkb->rkb_nodename),
		 "%s:%hu", name, port);
	if (nodeid == RD_KAFKA_NODEID_UA)
		snprintf(rkb->rkb_name, sizeof(rkb->rkb_name),
			 "%s/bootstrap", rkb->rkb_nodename);
	else
		snprintf(rkb->rkb_name, sizeof(rkb->rkb_name),
			 "%s/%"PRId32, rkb->rkb_nodename, nodeid);

	rkb->rkb_s = -1;
	rkb->rkb_source = source;

	rkb->rkb_rk = rk;
	rkb->rkb_nodeid = nodeid;

	TAILQ_INIT(&rkb->rkb_toppars);
	pthread_rwlock_init(&rkb->rkb_toppar_lock, NULL);
	TAILQ_INIT(&rkb->rkb_outbufs);
	TAILQ_INIT(&rkb->rkb_waitresps);
	TAILQ_INIT(&rkb->rkb_retrybufs);
	rd_kafka_q_init(&rkb->rkb_ops);
	rd_kafka_broker_keep(rkb);
	
	if ((err = pthread_create(&rkb->rkb_thread, NULL,
				  rd_kafka_broker_thread_main, rkb))) {
		rd_kafka_log(rk, LOG_CRIT, "THREAD",
			     "Unable to create broker thread: %s",
			     strerror(err));
		free(rkb);
		return NULL;
	}

	TAILQ_INSERT_TAIL(&rk->rk_brokers, rkb, rkb_link);

	rd_rkb_dbg(rkb, BROKER, "BROKER",
		   "Added new broker with NodeId %"PRId32,
		   rkb->rkb_nodeid);

	return rkb;
}

rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid (rd_kafka_t *rk,
						   int32_t nodeid) {
	rd_kafka_broker_t *rkb;

	/* FIXME: locking */

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		if (rkb->rkb_nodeid == nodeid) {
			rd_kafka_broker_keep(rkb);
			return rkb;
		}
	}

	return NULL;

}

static rd_kafka_broker_t *rd_kafka_broker_find (rd_kafka_t *rk,
						const char *name,
						uint16_t port) {
	rd_kafka_broker_t *rkb;
	char fullname[sizeof(rkb->rkb_name)];

	snprintf(fullname, sizeof(fullname), "%s:%hu", name, port);

	/* FIXME: locking */

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		if (!strcmp(rkb->rkb_name, fullname)) {
			rd_kafka_broker_keep(rkb);
			return rkb;
		}
	}

	return NULL;
}


int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist) {
	char *s = strdupa(brokerlist);
	char *t, *n;
	int cnt = 0;
	rd_kafka_broker_t *rkb;

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

		if ((t = strchr(s, ':'))) {
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
	if (!(rkb = rd_kafka_broker_find_by_nodeid(rk, nodeid)) &&
	    !(rkb = rd_kafka_broker_add(rk, RD_KAFKA_LEARNED,
					name, port, nodeid))) {
		rd_kafka_unlock(rk);
		return;
	}
	rd_kafka_unlock(rk);
		
	/* FIXME: invalidate Leader if required. */
}



void rd_kafka_brokers_init (void) {
}








