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

#ifndef _MSC_VER
#define _GNU_SOURCE
#ifndef _AIX    /* AIX defines this and the value needs to be set correctly */
#define _XOPEN_SOURCE
#endif
#include <signal.h>
#endif

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdkafka_offset.h"
#include "rdkafka_transport.h"
#include "rdkafka_proto.h"
#include "rdkafka_buf.h"
#include "rdtime.h"
#include "rdcrc32.h"
#include "rdrand.h"
#include "rdgz.h"
#include "snappy.h"
#include "rdendian.h"


const char *rd_kafka_broker_state_names[] = {
	"INIT",
	"DOWN",
	"CONNECT",
	"UP",
        "UPDATE"
};

const char *rd_kafka_secproto_names[] = {
	[RD_KAFKA_PROTO_PLAINTEXT] = "plaintext",
#if WITH_SSL
	[RD_KAFKA_PROTO_SSL] = "ssl",
#endif
	NULL
};



static void rd_kafka_toppar_offsetcommit_request (rd_kafka_broker_t *rkb,
                                                  rd_kafka_toppar_t *rktp,
                                                  int64_t offset);

static void rd_kafka_toppar_next_offset_handle (rd_kafka_toppar_t *rktp,
                                                int64_t Offset, void *opaque);
static void rd_kafka_toppar_op_serve (rd_kafka_toppar_t *rktp);

static void msghdr_print (rd_kafka_t *rk,
			  const char *what, const struct msghdr *msg,
			  int hexdump) RD_UNUSED;
static void msghdr_print (rd_kafka_t *rk,
			  const char *what, const struct msghdr *msg,
			  int hexdump) {
	int i;
	int len = 0;

	

	rd_kafka_dbg(rk, MSG, "MSG", "%s: iovlen %"PRIdsz"",
		     what, (size_t)msg->msg_iovlen);

	for (i = 0 ; i < (int)msg->msg_iovlen ; i++) {
		rd_kafka_dbg(rk, MSG, what,
			     " iov #%i: %"PRIdsz"",
			     i, msg->msg_iov[i].iov_len);
		if (hexdump)
			rd_hexdump(stdout, what, msg->msg_iov[i].iov_base,
				   msg->msg_iov[i].iov_len);

		len += msg->msg_iov[i].iov_len;
	}
	rd_kafka_dbg(rk, MSG, "MSG", "^ message was %d bytes in total", len);
}



static size_t rd_kafka_msghdr_size (const struct msghdr *msg) {
	int i;
	size_t tot = 0;

	for (i = 0 ; i < (int)msg->msg_iovlen ; i++)
		tot += msg->msg_iov[i].iov_len;

	return tot;
}


/**
 * Construct broker nodename.
 */
static void rd_kafka_mk_nodename (char *dest, size_t dsize,
                                  const char *name, uint16_t port) {
        rd_snprintf(dest, dsize, "%s:%hu", name, port);
}

/**
 * Construct descriptive broker name
 */
static void rd_kafka_mk_brokername (char *dest, size_t dsize,
				    rd_kafka_secproto_t proto,
				    const char *nodename, int32_t nodeid,
				    rd_kafka_confsource_t source) {

	/* Prepend protocol name to brokername, unless it is a
	 * standard plaintext broker in which case we omit the protocol part. */
	if (proto != RD_KAFKA_PROTO_PLAINTEXT) {
		int r = rd_snprintf(dest, dsize, "%s://",
				    rd_kafka_secproto_names[proto]);
		if (r >= (int)dsize) /* Skip proto name if it wont fit.. */
			r = 0;

		dest += r;
		dsize -= r;
	}

	if (nodeid == RD_KAFKA_NODEID_UA)
		rd_snprintf(dest, dsize, "%s/%s",
			    nodename,
			    source == RD_KAFKA_INTERNAL ?
			    "internal":"bootstrap");
	else
		rd_snprintf(dest, dsize, "%s/%"PRId32, nodename, nodeid);
}

/**
 * Locks: rd_kafka_broker_lock() MUST be held.
 * Locality: broker thread
 */
static void rd_kafka_broker_set_state (rd_kafka_broker_t *rkb,
				       int state) {
	if ((int)rkb->rkb_state == state)
		return;

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "STATE",
		     "%s: Broker changed state %s -> %s",
		     rkb->rkb_name,
		     rd_kafka_broker_state_names[rkb->rkb_state],
		     rd_kafka_broker_state_names[state]);

	if (state == RD_KAFKA_BROKER_STATE_DOWN) {
		/* Propagate ALL_BROKERS_DOWN event if all brokers are
		 * now down, unless we're terminating. */
		if (rd_atomic32_add(&rkb->rkb_rk->rk_broker_down_cnt, 1) ==
		    rd_atomic32_get(&rkb->rkb_rk->rk_broker_cnt) &&
		    !rd_atomic32_get(&rkb->rkb_rk->rk_terminate))
			rd_kafka_op_err(rkb->rkb_rk,
					RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
					"%i/%i brokers are down",
					rd_atomic32_get(&rkb->rkb_rk->rk_broker_down_cnt),
					rd_atomic32_get(&rkb->rkb_rk->rk_broker_cnt));
	} else if (rkb->rkb_state == RD_KAFKA_BROKER_STATE_DOWN)
		(void)rd_atomic32_sub(&rkb->rkb_rk->rk_broker_down_cnt, 1);

	rkb->rkb_state = state;
        rkb->rkb_ts_state = rd_clock();
}






/**
 * Failure propagation to application.
 * Will tear down connection to broker and trigger a reconnect.
 *
 * If 'fmt' is NULL nothing will be logged or propagated to the application.
 * 
 * Locality: Broker thread
 */
void rd_kafka_broker_fail (rd_kafka_broker_t *rkb,
			   rd_kafka_resp_err_t err,
			   const char *fmt, ...) {
	va_list ap;
	int errno_save = errno;
	rd_kafka_toppar_t *rktp;
	rd_kafka_bufq_t tmpq;
        int statechange;
	rd_kafka_broker_t *internal_rkb;

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	rd_kafka_dbg(rkb->rkb_rk, BROKER, "BROKERFAIL",
		     "%s: failed: err: %s: (errno: %s)",
		     rkb->rkb_name, rd_kafka_err2str(err),
		     rd_strerror(errno_save));

	rkb->rkb_err.err = errno_save;

	if (rkb->rkb_transport) {
		rd_kafka_transport_close(rkb->rkb_transport);
		rkb->rkb_transport = NULL;
	}

	rkb->rkb_req_timeouts = 0;

	if (rkb->rkb_recv_buf) {
		rd_kafka_buf_destroy(rkb->rkb_recv_buf);
		rkb->rkb_recv_buf = NULL;
	}

	/* The caller may omit the format if it thinks this is a recurring
	 * failure, in which case the following things are omitted:
	 *  - log message
	 *  - application OP_ERR
	 *  - metadata request
	 *
	 * Dont log anything if this was the termination signal.
	 */
	if (fmt && !(errno_save == EINTR && rd_atomic32_get(&rkb->rkb_rk->rk_terminate))) {
		int of;

		/* Insert broker name in log message if it fits. */
		of = rd_snprintf(rkb->rkb_err.msg, sizeof(rkb->rkb_err.msg),
			      "%s: ", rkb->rkb_name);
		if (of >= (int)sizeof(rkb->rkb_err.msg))
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

	rd_kafka_broker_lock(rkb);

	/* Set broker state */
        statechange = rkb->rkb_state != RD_KAFKA_BROKER_STATE_DOWN;
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

	internal_rkb = rd_kafka_broker_internal(rkb->rkb_rk);

	/* Undelegate all toppars from this broker. */
	rd_kafka_broker_toppars_wrlock(rkb);
	while ((rktp = TAILQ_FIRST(&rkb->rkb_toppars))) {
		rd_kafka_topic_t *rkt = rktp->rktp_rkt;

		rd_kafka_topic_keep(rkt); /* Hold on to rkt */
		rd_kafka_toppar_keep(rktp);
		rd_kafka_broker_toppars_wrunlock(rkb);
		rd_rkb_dbg(rkb, TOPIC, "BRKTP",
			   "Undelegating %.*s [%"PRId32"]",
			   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			   rktp->rktp_partition);

		rd_kafka_topic_wrlock(rktp->rktp_rkt);
		/* Undelegate */
		rd_kafka_toppar_broker_delegate(rktp,
						rkb != internal_rkb ?
						internal_rkb : NULL);

		rd_kafka_topic_wrunlock(rktp->rktp_rkt);

		rd_kafka_toppar_destroy(rktp);
		rd_kafka_topic_destroy0(rkt); /* Let go of rkt */

		rd_kafka_broker_toppars_wrlock(rkb);

	}
	rd_kafka_broker_toppars_wrunlock(rkb);

	if (internal_rkb)
		rd_kafka_broker_destroy(internal_rkb);

	/* Query for the topic leaders (async) */
	if (fmt && err != RD_KAFKA_RESP_ERR__DESTROY && statechange)
		rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);
}


/**
 * Scan the wait-response queue for message timeouts.
 *
 * Locality: Broker thread
 */
static void rd_kafka_broker_waitresp_timeout_scan (rd_kafka_broker_t *rkb,
						   rd_ts_t now) {
	rd_kafka_buf_t *rkbuf, *tmp;
	int cnt = 0;

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	TAILQ_FOREACH_SAFE(rkbuf,
			   &rkb->rkb_waitresps.rkbq_bufs, rkbuf_link, tmp) {
		if (likely(rkbuf->rkbuf_ts_timeout > now))
			continue;

		rd_kafka_bufq_deq(&rkb->rkb_waitresps, rkbuf);

		rkbuf->rkbuf_cb(rkb, RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
				NULL, rkbuf, rkbuf->rkbuf_opaque);
		cnt++;
	}

	if (cnt > 0) {
		rd_rkb_dbg(rkb, MSG, "REQTMOUT", "Timed out %i requests", cnt);

                /* Fail the broker if socket.max.fails is configured and
                 * now exceeded. */
                rkb->rkb_req_timeouts   += cnt;
                rd_atomic64_add(&rkb->rkb_c.req_timeouts, cnt);

                if (rkb->rkb_rk->rk_conf.socket_max_fails &&
                    rkb->rkb_req_timeouts >=
                    rkb->rkb_rk->rk_conf.socket_max_fails &&
                    rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP) {
                        errno = ETIMEDOUT;
                        rd_kafka_broker_fail(rkb,
                                             RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
                                             "%i request(s) timed out: "
                                             "disconnect",
                                             rkb->rkb_req_timeouts);
                }
        }
}



static ssize_t rd_kafka_broker_send (rd_kafka_broker_t *rkb,
				     const struct msghdr *msg) {
	ssize_t r;
	char errstr[128];

	rd_kafka_assert(rkb->rkb_rk, rkb->rkb_state>=RD_KAFKA_BROKER_STATE_UP);
	rd_kafka_assert(rkb->rkb_rk, rkb->rkb_transport);

	r = rd_kafka_transport_sendmsg(rkb->rkb_transport, msg, errstr, sizeof(errstr));

	if (r == -1) {
		rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
			"Send failed: %s", errstr);
		rd_atomic64_add(&rkb->rkb_c.tx_err, 1);
		return -1;
	}

	(void)rd_atomic64_add(&rkb->rkb_c.tx_bytes, r);
	(void)rd_atomic64_add(&rkb->rkb_c.tx, 1);
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
					       rkb->rkb_rk->rk_conf.
                                               broker_addr_family,
                                               SOCK_STREAM,
					       IPPROTO_TCP, &errstr);

		if (!rkb->rkb_rsal) {
                        rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__RESOLVE,
                                             /* Avoid duplicate log messages */
                                             rkb->rkb_err.err == errno ?
                                             NULL :
                                             "Failed to resolve '%s': %s",
                                             rkb->rkb_nodename, errstr);
			return -1;
		}
	}

	return 0;
}


static void rd_kafka_broker_buf_enq0 (rd_kafka_broker_t *rkb,
				      rd_kafka_buf_t *rkbuf, int at_head) {
	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	/* Update CorrId header field. */
        rkbuf->rkbuf_corrid = ++rkb->rkb_corrid;
	rd_kafka_buf_update_i32(rkbuf, 4+2+2, rkbuf->rkbuf_corrid);

        rkbuf->rkbuf_ts_enq = rd_clock();

        /* Set timeout if not already set */
        if (!rkbuf->rkbuf_ts_timeout)
        	rkbuf->rkbuf_ts_timeout = rkbuf->rkbuf_ts_enq +
                        rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;


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

	(void)rd_atomic32_add(&rkb->rkb_outbufs.rkbq_cnt, 1);
	(void)rd_atomic32_add(&rkb->rkb_outbufs.rkbq_msg_cnt,
                            rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt));
}


/**
 * Finalize a stuffed rkbuf for sending to broker.
 */
static void rd_kafka_buf_finalize (rd_kafka_t *rk, rd_kafka_buf_t *rkbuf,
				   int16_t ApiKey) {
	int of_Size;

	rkbuf->rkbuf_reqhdr.ApiKey = ApiKey;

	/* Write header */
	rd_kafka_buf_write_seek(rkbuf, 0);
	of_Size = rd_kafka_buf_write_i32(rkbuf, 0); /* Size: Updated below */
	rd_kafka_buf_write_i16(rkbuf, rkbuf->rkbuf_reqhdr.ApiKey);
	rd_kafka_buf_write_i16(rkbuf, rkbuf->rkbuf_reqhdr.ApiVersion);
	rd_kafka_buf_write_i32(rkbuf, 0); /* CorrId: Updated in enq0() */
	
	/* Write clientId */
	rkbuf->rkbuf_iov[1].iov_base = RD_KAFKAP_STR_SER(rk->rk_conf.client_id);
	rkbuf->rkbuf_iov[1].iov_len = RD_KAFKAP_STR_SIZE(rk->rk_conf.client_id);

	/* Calculate total message buffer length. */
	rkbuf->rkbuf_of          = 0;  /* Indicates send position */
	rkbuf->rkbuf_len         = rd_kafka_msghdr_size(&rkbuf->rkbuf_msg);

	rd_kafka_buf_update_i32(rkbuf, of_Size, rkbuf->rkbuf_len-4);
}


void rd_kafka_broker_buf_enq1 (rd_kafka_broker_t *rkb,
                               int16_t ApiKey,
                               rd_kafka_buf_t *rkbuf,
                               void (*reply_cb) (
                                       rd_kafka_broker_t *,
                                       rd_kafka_resp_err_t err,
                                       rd_kafka_buf_t *reply,
                                       rd_kafka_buf_t *request,
                                       void *opaque),
                               void *opaque) {

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

        rkbuf->rkbuf_cb     = reply_cb;
	rkbuf->rkbuf_opaque = opaque;

        rd_kafka_buf_finalize(rkb->rkb_rk, rkbuf, ApiKey);

	rd_kafka_broker_buf_enq0(rkb, rkbuf,
				 (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FLASH)?
				 1/*head*/: 0/*tail*/);
}



/**
 * Serve a buffer op.
 */
void rd_kafka_buf_op_serve (rd_kafka_op_t *rko) {
        switch (rko->rko_type)
        {
        case RD_KAFKA_OP_RECV_BUF:
                rko->rko_rkbuf->rkbuf_parse_cb(rko->rko_rkbuf, rko->rko_opaque);
                break;
        default:
                rd_kafka_assert(NULL, !*"unknown buf op");
        }
}


static void rd_kafka_buf_route_to_op (rd_kafka_broker_t *rkb,
                                      rd_kafka_resp_err_t err,
                                      rd_kafka_buf_t *reply,
                                      rd_kafka_buf_t *request,
                                      void *opaque) {
        rd_kafka_op_t *rko = opaque;
        rko->rko_type      = RD_KAFKA_OP_RECV_BUF;
        rko->rko_rkbuf     = reply;
        reply->rkbuf_err   = err;
        reply->rkbuf_rkb   = rkb;
        reply->rkbuf_parse_cb = request->rkbuf_parse_cb;
        rd_kafka_broker_keep(rkb);
        rd_kafka_buf_version_set(reply, request->rkbuf_reqhdr.ApiVersion);
        rd_kafka_q_enq(rko->rko_replyq, rko);
}

/**
 * Enqueue a buffer for tranmission to broker.
 * Regarding _safe:
 *   * This function must only be used from a non-broker thread,
 *     the other enq functions must only be used from the broker thread.
 *   * The response buffer is forwarded to 'replyq'.
 *   * The 'replyq' server should dispatch rkbuf to parse_cb.
 *
 * NOTE: 'rkbuf' is no longer usable after this call (ownership shifted to rko).
 .*/

void rd_kafka_broker_buf_enq_safe (rd_kafka_broker_t *rkb, int16_t ApiKey,
                                   rd_kafka_buf_t *rkbuf,
                                   rd_kafka_q_t *replyq,
                                   void (*parse_cb) (rd_kafka_buf_t *reply,
                                                     void *opaque),
                                   void *opaque) {

        rd_kafka_op_t *rko;

        rd_kafka_assert(rkb->rkb_rk, !thrd_is_current(rkb->rkb_thread));

        rkbuf->rkbuf_cb       = rd_kafka_buf_route_to_op;
        rkbuf->rkbuf_parse_cb = parse_cb;

        rd_kafka_buf_finalize(rkb->rkb_rk, rkbuf, ApiKey);

        /* Create an op and enqueue it for broker thread. */
        rko = rd_kafka_op_new(RD_KAFKA_OP_XMIT_BUF);
        rko->rko_rkbuf        = rkbuf;
        rko->rko_replyq       = replyq;
        rko->rko_opaque       = opaque;
	rkbuf->rkbuf_opaque   = rko;
        rd_kafka_q_enq(&rkb->rkb_ops, rko);
}







/**
 * Handle a Metadata response message.
 * If 'rkt' is non-NULL the metadata originated from a topic-specific request.
 *
 * The metadata will be marshalled into 'struct rd_kafka_metadata*' structs.
 *
 * Returns the marshalled metadata, or NULL on parse error.
 *
 * Locality: broker thread
 */
static struct rd_kafka_metadata *
rd_kafka_metadata_handle (rd_kafka_broker_t *rkb,
                          rd_kafka_op_t *rko, rd_kafka_buf_t *rkbuf) {
	int i, j, k;
	int req_rkt_seen = 0;
        char *msh_buf = NULL;
        int   msh_of  = 0;
        int   msh_size;
        struct rd_kafka_metadata *md = NULL;
        int rkb_namelen = strlen(rkb->rkb_name)+1;
        const int log_decode_errors = 1;

        /* We assume that the marshalled representation is
         * no more than 4 times larger than the wire representation. */
        msh_size = sizeof(*md) + rkb_namelen + (rkbuf->rkbuf_len * 4);
        msh_buf = rd_malloc(msh_size);

        _MSH_ALLOC(rkbuf, md, sizeof(*md));
        md->orig_broker_id = rkb->rkb_nodeid;
        _MSH_ALLOC(rkbuf, md->orig_broker_name, rkb_namelen);
        memcpy(md->orig_broker_name, rkb->rkb_name, rkb_namelen);

	/* Read Brokers */
	rd_kafka_buf_read_i32a(rkbuf, md->broker_cnt);
	if (md->broker_cnt > RD_KAFKAP_BROKERS_MAX)
		rd_kafka_buf_parse_fail(rkbuf, "Broker_cnt %i > BROKERS_MAX %i",
					md->broker_cnt, RD_KAFKAP_BROKERS_MAX);

        _MSH_ALLOC(rkbuf, md->brokers, md->broker_cnt * sizeof(*md->brokers));

	for (i = 0 ; i < md->broker_cnt ; i++) {
                rd_kafka_buf_read_i32a(rkbuf, md->brokers[i].id);
                rd_kafka_buf_read_str_msh(rkbuf, md->brokers[i].host);
		rd_kafka_buf_read_i32a(rkbuf, md->brokers[i].port);
	}


	/* Read TopicMetadata */
	rd_kafka_buf_read_i32a(rkbuf, md->topic_cnt);
	rd_rkb_dbg(rkb, METADATA, "METADATA", "%i brokers, %i topics",
                   md->broker_cnt, md->topic_cnt);

	if (md->topic_cnt > RD_KAFKAP_TOPICS_MAX)
		rd_kafka_buf_parse_fail(rkbuf, "TopicMetadata_cnt %"PRId32
					" > TOPICS_MAX %i",
					md->topic_cnt, RD_KAFKAP_TOPICS_MAX);

        _MSH_ALLOC(rkbuf, md->topics, md->topic_cnt * sizeof(*md->topics));

	for (i = 0 ; i < md->topic_cnt ; i++) {

		rd_kafka_buf_read_i16a(rkbuf, md->topics[i].err);
		rd_kafka_buf_read_str_msh(rkbuf, md->topics[i].topic);

		/* PartitionMetadata */
                rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partition_cnt);
		if (md->topics[i].partition_cnt > RD_KAFKAP_PARTITIONS_MAX)
			rd_kafka_buf_parse_fail(rkbuf,
						"TopicMetadata[%i]."
						"PartitionMetadata_cnt %i "
						"> PARTITIONS_MAX %i",
						i, md->topics[i].partition_cnt,
						RD_KAFKAP_PARTITIONS_MAX);

                _MSH_ALLOC(rkbuf, md->topics[i].partitions,
                           md->topics[i].partition_cnt *
                           sizeof(*md->topics[i].partitions));

		for (j = 0 ; j < md->topics[i].partition_cnt ; j++) {
			rd_kafka_buf_read_i16a(rkbuf, md->topics[i].partitions[j].err);
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].id);
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].leader);

			/* Replicas */
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].replica_cnt);
			if (md->topics[i].partitions[j].replica_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				rd_kafka_buf_parse_fail(rkbuf,
							"TopicMetadata[%i]."
							"PartitionMetadata[%i]."
							"Replica_cnt "
							"%i > BROKERS_MAX %i",
							i, j,
							md->topics[i].
							partitions[j].
							replica_cnt,
							RD_KAFKAP_BROKERS_MAX);

                        _MSH_ALLOC(rkbuf, md->topics[i].partitions[j].replicas,
                                   md->topics[i].partitions[j].replica_cnt *
                                   sizeof(*md->topics[i].partitions[j].
                                          replicas));

                        for (k = 0 ;
                             k < md->topics[i].partitions[j].replica_cnt; k++)
				rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].
                                           replicas[k]);

			/* Isrs */
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].isr_cnt);
			if (md->topics[i].partitions[j].isr_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				rd_kafka_buf_parse_fail(rkbuf,
							"TopicMetadata[%i]."
							"PartitionMetadata[%i]."
							"Isr_cnt "
							"%i > BROKERS_MAX %i",
							i, j,
							md->topics[i].
							partitions[j].isr_cnt,
							RD_KAFKAP_BROKERS_MAX);

                        _MSH_ALLOC(rkbuf, md->topics[i].partitions[j].isrs,
                                   md->topics[i].partitions[j].isr_cnt *
                                   sizeof(*md->topics[i].partitions[j].isrs));
                        for (k = 0 ;
                             k < md->topics[i].partitions[j].isr_cnt; k++)
				rd_kafka_buf_read_i32a(rkbuf, md->topics[i].
						       partitions[j].isrs[k]);

		}
	}

        /* Entire Metadata response now parsed without errors:
         * now update our internal state according to the response. */

	/* Update our list of brokers. */
	for (i = 0 ; i < md->broker_cnt ; i++) {
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "  Broker #%i/%i: %s:%i NodeId %"PRId32,
			   i, md->broker_cnt,
                           md->brokers[i].host,
                           md->brokers[i].port,
                           md->brokers[i].id);
		rd_kafka_broker_update(rkb->rkb_rk, rkb->rkb_proto,
				       &md->brokers[i]);
	}

	/* Update partition count and leader for each topic we know about */
	for (i = 0 ; i < md->topic_cnt ; i++) {
                rd_rkb_dbg(rkb, METADATA, "METADATA",
                           "  Topic #%i/%i: %s with %i partitions%s%s",
                           i, md->topic_cnt, md->topics[i].topic,
                           md->topics[i].partition_cnt,
                           md->topics[i].err ? ": " : "",
                           md->topics[i].err ?
                           rd_kafka_err2str(md->topics[i].err) : "");


		if (rko->rko_rkt &&
		    !rd_kafkap_str_cmp_str(rko->rko_rkt->rkt_topic,
                                           md->topics[i].topic))
			req_rkt_seen++;

		rd_kafka_topic_metadata_update(rkb, &md->topics[i]);
	}


	/* Requested topics not seen in metadata? Propogate to topic code. */
	if (rko->rko_rkt) {
		rd_rkb_dbg(rkb, TOPIC, "METADATA",
			   "Requested topic %s %sseen in metadata",
			   rko->rko_rkt->rkt_topic->str,
                           req_rkt_seen ? "" : "not ");
		if (!req_rkt_seen)
			rd_kafka_topic_metadata_none(rko->rko_rkt);
	}


        /* This metadata request was triggered by someone wanting
         * the metadata information back as a reply, so send that reply now.
         * In this case we must not rd_free the metadata memory here,
         * the requestee will do. */
        return md;

err:
        rd_free(msh_buf);
        return NULL;
}


static void rd_kafka_broker_metadata_reply (rd_kafka_broker_t *rkb,
					    int err,
					    rd_kafka_buf_t *reply,
					    rd_kafka_buf_t *request,
					    void *opaque) {
        rd_kafka_op_t *rko = opaque;
        struct rd_kafka_metadata *md = NULL;
        rd_kafka_q_t *replyq;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "===== Received metadata from %s =====",
		   rkb->rkb_name);

	/* Avoid metadata updates when we're terminating. */
	if (rd_atomic32_get(&rkb->rkb_rk->rk_terminate))
                err = RD_KAFKA_RESP_ERR__DESTROY;

	if (unlikely(err)) {
		/* FIXME: handle error */
                if (err != RD_KAFKA_RESP_ERR__DESTROY)
                        rd_rkb_log(rkb, LOG_WARNING, "METADATA",
                                   "Metadata request failed: %s",
                                   rd_kafka_err2str(err));
	} else {
		md = rd_kafka_metadata_handle(rkb, rko, reply);
        }

        if (rko->rko_rkt) {
                rd_kafka_topic_wrlock(rko->rko_rkt);
                rko->rko_rkt->rkt_flags &=
                        ~RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_wrunlock(rko->rko_rkt);
        }

        if ((replyq = rko->rko_replyq)) {
                /* Reply to metadata requester, passing on the metadata.
                 * Reuse requesting rko for the reply. */
                rko->rko_err = err;
                rko->rko_replyq = NULL;
                rko->rko_metadata = md;
                rd_kafka_q_enq(replyq, rko);
                /* Drop refcount to queue */
                rd_kafka_q_destroy(replyq);
        } else {
                if (md)
                        rd_free(md);
                rd_kafka_op_destroy(rko);
        }

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}



static void rd_kafka_broker_metadata_req_op (rd_kafka_broker_t *rkb,
                                             rd_kafka_op_t *rko) {
	rd_kafka_buf_t *rkbuf;
	int32_t arrsize = 0;
	size_t tnamelen = 0;
	rd_kafka_topic_t *rkt;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Request metadata for %s: %s",
		   rko->rko_rkt ? rko->rko_rkt->rkt_topic->str :
		   (rko->rko_all_topics ? "all topics":"locally known topics"),
                   (char *)rko->rko_reason ? (char *)rko->rko_reason : "");

	/* If called from other thread than the broker's own then post an
	 * op for the broker's thread instead since all transmissions must
	 * be performed by the broker thread. */
	if (!thrd_is_current(rkb->rkb_thread)) { // FIXME
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			"Request metadata: scheduled: not in broker thread");
                rd_kafka_q_enq(&rkb->rkb_ops, rko);
		return;
	}

	/* FIXME: Use iovs and ..next here */

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Requesting metadata for %stopics",
		   rko->rko_all_topics ? "all ": "known ");


	if (!rko->rko_rkt) {
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


	if (rko->rko_rkt || !rko->rko_all_topics) {
		rd_kafka_rdlock(rkb->rkb_rk);

		/* Calculate size to hold all known topics */
		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			if (rko->rko_rkt && rko->rko_rkt != rkt)
				continue;

			arrsize++;
			tnamelen += RD_KAFKAP_STR_SIZE(rkt->rkt_topic);
		}
	}

	rkbuf = rd_kafka_buf_new(1, sizeof(arrsize) + tnamelen);
	rd_kafka_buf_write_i32(rkbuf, arrsize);


	if (rko->rko_rkt || !rko->rko_all_topics) {
		/* Just our locally known topics */

		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			if (rko->rko_rkt && rko->rko_rkt != rkt)
				continue;
			rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);
		}
		rd_kafka_rdunlock(rkb->rkb_rk);
	}


	rd_kafka_buf_autopush(rkbuf);

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Metadata, rkbuf,
				rd_kafka_broker_metadata_reply, rko);
}


/**
 * Initiate metadata request
 *
 * all_topics - if true, all topics in cluster will be requested, else only
 *              the ones known locally.
 * only_rkt   - only request this specific topic (optional)
 * replyq     - enqueue reply op on this queue (optional)
 * reason     - metadata request reason
 *
 */
void rd_kafka_broker_metadata_req (rd_kafka_broker_t *rkb,
                                   int all_topics,
                                   rd_kafka_topic_t *only_rkt,
                                   rd_kafka_q_t *replyq,
                                   const char *reason) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(RD_KAFKA_OP_METADATA_REQ);
        rko->rko_all_topics = all_topics;
        rko->rko_rkt        = only_rkt;
        if (rko->rko_rkt)
                rd_kafka_topic_keep(rko->rko_rkt);

        rko->rko_replyq     = replyq;

        if (RD_IS_CONSTANT(reason))
                rko->rko_reason = (void *)reason;
        else {
                rko->rko_reason = rd_strdup(reason);
                rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        }

        rd_kafka_broker_metadata_req_op(rkb, rko);
}

/**
 * all_topics := if 1: retreive all topics&partitions from the broker
 *               if 0: just retrieve the topics we know about.
 * rkt        := all_topics=0 && only_rkt is set: only ask for specified topic.
 */


/**
 * Returns a random broker (with refcnt increased) in state 'state'.
 * Uses Reservoir sampling.
 *
 * Locks: rd_kafka_rdlock(rk) MUST be held.
 * Locality: any thread
 */
rd_kafka_broker_t *rd_kafka_broker_any (rd_kafka_t *rk, int state) {
	rd_kafka_broker_t *rkb, *good = NULL;
        int cnt = 0;

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if ((int)rkb->rkb_state == state) {
                        if (cnt < 1 || rd_jitter(0, cnt) < 1) {
                                if (good)
                                        rd_kafka_broker_destroy(good);
                                rd_kafka_broker_keep(rkb);
                                good = rkb;
                        }
                        cnt += 1;
                }
		rd_kafka_broker_unlock(rkb);
	}

        return good;
}


/**
 * Returns a broker in state `state`, preferring the one with matching `broker_id`.
 * Uses Reservoir sampling.
 *
 * Locks: rd_kafka_rdlock(rk) MUST be held.
 * Locality: any thread
 */
rd_kafka_broker_t *rd_kafka_broker_prefer (rd_kafka_t *rk, int32_t broker_id,
					   int state) {
	rd_kafka_broker_t *rkb, *good = NULL;
        int cnt = 0;

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if ((int)rkb->rkb_state == state) {
                        if (broker_id != -1 && rkb->rkb_nodeid == broker_id) {
                                if (good)
                                        rd_kafka_broker_destroy(good);
                                rd_kafka_broker_keep(rkb);
                                good = rkb;
                                rd_kafka_broker_unlock(rkb);
                                break;
                        }
                        if (cnt < 1 || rd_jitter(0, cnt) < 1) {
                                if (good)
                                        rd_kafka_broker_destroy(good);
                                rd_kafka_broker_keep(rkb);
                                good = rkb;
                        }
                        cnt += 1;
                }
		rd_kafka_broker_unlock(rkb);
	}

        return good;
}


/**
 * Trigger broker metadata query for topic leader.
 * 'rkt' may be NULL to query for all topics.
 */
void rd_kafka_topic_leader_query0 (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
				   int do_rk_lock) {
	rd_kafka_broker_t *rkb;

	if (do_rk_lock)
		rd_kafka_rdlock(rk);
	if (!(rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP))) {
		if (do_rk_lock)
			rd_kafka_rdunlock(rk);
		return; /* No brokers are up */
	}
	if (do_rk_lock)
		rd_kafka_rdunlock(rk);

        if (rkt) {
                rd_kafka_topic_wrlock(rkt);
                /* Avoid multiple leader queries if there is already
                 * an outstanding one waiting for reply. */
                if (rkt->rkt_flags & RD_KAFKA_TOPIC_F_LEADER_QUERY) {
                        rd_kafka_topic_wrunlock(rkt);
                        rd_kafka_broker_destroy(rkb);
                        return;
                }
                rkt->rkt_flags |= RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_wrunlock(rkt);
        }

	rd_kafka_broker_metadata_req(rkb, 0, rkt, NULL, "leader query");

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

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	TAILQ_FOREACH(rkbuf, &rkb->rkb_waitresps.rkbq_bufs, rkbuf_link)
		if (rkbuf->rkbuf_corrid == corrid) {
			/* Convert ts_sent to RTT */
			rkbuf->rkbuf_ts_sent = now - rkbuf->rkbuf_ts_sent;
			rd_kafka_avg_add(&rkb->rkb_avg_rtt,
					 rkbuf->rkbuf_ts_sent);

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

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));


	/* Find corresponding request message by correlation id */
	if (unlikely(!(req =
		       rd_kafka_waitresp_find(rkb,
					      rkbuf->rkbuf_reshdr.CorrId)))) {
		/* unknown response. probably due to request timeout */
                rd_atomic64_add(&rkb->rkb_c.rx_corrid_err, 1);
		rd_rkb_dbg(rkb, BROKER, "RESPONSE",
			   "Response for unknown CorrId %"PRId32" (timed out?)",
			   rkbuf->rkbuf_reshdr.CorrId);
                rd_kafka_buf_destroy(rkbuf);
                return -1;
	}

	rd_rkb_dbg(rkb, PROTOCOL, "RECV",
		   "Received %sResponse (v%hd, %"PRIdsz" bytes, CorrId %"PRId32
		   ", rtt %.2fms)",
		   rd_kafka_ApiKey2str(req->rkbuf_reqhdr.ApiKey),
                   req->rkbuf_reqhdr.ApiVersion,
		   rkbuf->rkbuf_len, rkbuf->rkbuf_reshdr.CorrId,
		   (float)req->rkbuf_ts_sent / 1000.0f);

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

	for (i = 0 ; i < (int)src->msg_iovlen ; i++) {
		off_t vof = of - len;

		if (0)
			printf(" #%i/%"PRIdsz" and %"PRIdsz": of %jd, len %"PRIdsz", "
			       "vof %jd: iov %"PRIdsz"\n",
			       i,
			       (size_t)src->msg_iovlen,
			       (size_t)dst->msg_iovlen,
			       (intmax_t)of, len, (intmax_t)vof,
			       src->msg_iov[i].iov_len);
		if (vof < 0)
			vof = 0;

		if ((size_t)vof < src->msg_iov[i].iov_len) {
			rd_kafka_assert(NULL, (size_t)dst->msg_iovlen < dst_len);
			dst->msg_iov[dst->msg_iovlen].iov_base =
				(char *)src->msg_iov[i].iov_base + vof;
			dst->msg_iov[dst->msg_iovlen].iov_len =
				src->msg_iov[i].iov_len - vof;
			dst->msg_iovlen++;
		}

		len += src->msg_iov[i].iov_len;
	}
}



int rd_kafka_recv (rd_kafka_broker_t *rkb) {
	rd_kafka_buf_t *rkbuf;
	ssize_t r;
	struct msghdr msg;
	struct iovec iov;
	char errstr[512];
	rd_kafka_resp_err_t err_code = RD_KAFKA_RESP_ERR__BAD_MSG;
	const int log_decode_errors = 1;

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

		rkbuf->rkbuf_rkb = rkb;
		rd_kafka_broker_keep(rkb);

		/* The iov[0] buffer is already allocated by buf_new(),
		 * shrink it to only allow for the response header. */
		rkbuf->rkbuf_iov[0].iov_len = RD_KAFKAP_RESHDR_SIZE;
		rkbuf->rkbuf_wof = 0;

		rkbuf->rkbuf_msg.msg_iov = rkbuf->rkbuf_iov;
		rkbuf->rkbuf_msg.msg_iovlen = 1;

		msg = rkbuf->rkbuf_msg;

		/* Point read buffer to main buffer. */
		rkbuf->rkbuf_rbuf = rkbuf->rkbuf_buf;

		rkb->rkb_recv_buf = rkbuf;

	} else {
		/* Receive in progress: adjust the msg to allow more data. */
		msg.msg_iov = &iov;
		rd_kafka_msghdr_rebuild(&msg, rkbuf->rkbuf_msg.msg_iovlen,
					&rkbuf->rkbuf_msg,
					rkbuf->rkbuf_wof);
	}

	rd_kafka_assert(rkb->rkb_rk, rd_kafka_msghdr_size(&msg) > 0);

	r = rd_kafka_transport_recvmsg(rkb->rkb_transport, &msg,
				       errstr, sizeof(errstr));
	if (r == 0)
		return 0; /* EAGAIN */
	else if (r == -1) {
		err_code = RD_KAFKA_RESP_ERR__TRANSPORT;
		rd_atomic64_add(&rkb->rkb_c.rx_err, 1);
		goto err;
	}

	rkbuf->rkbuf_wof += r;

	if (rkbuf->rkbuf_len == 0) {
		/* Packet length not known yet. */

		if (unlikely(rkbuf->rkbuf_wof < RD_KAFKAP_RESHDR_SIZE)) {
			/* Need response header for packet length and corrid.
			 * Wait for more data. */ 
			return 0;
		}

		/* Read protocol header */
		rd_kafka_buf_read_i32(rkbuf, &rkbuf->rkbuf_reshdr.Size);
		rd_kafka_buf_read_i32(rkbuf, &rkbuf->rkbuf_reshdr.CorrId);
		rkbuf->rkbuf_len = rkbuf->rkbuf_reshdr.Size;

		/* Make sure message size is within tolerable limits. */
		if (rkbuf->rkbuf_len < 4/*CorrId*/ ||
		    rkbuf->rkbuf_len >
		    (size_t)rkb->rkb_rk->rk_conf.recv_max_msg_size) {
			rd_snprintf(errstr, sizeof(errstr),
				 "Invalid message size %"PRIdsz" (0..%i): "
				 "increase receive.message.max.bytes",
				 rkbuf->rkbuf_len-4,
				 rkb->rkb_rk->rk_conf.recv_max_msg_size);
			rd_atomic64_add(&rkb->rkb_c.rx_err, 1);
			err_code = RD_KAFKA_RESP_ERR__BAD_MSG;

			goto err;
		}

		rkbuf->rkbuf_len -= 4; /*CorrId*/

		if (rkbuf->rkbuf_len > 0) {
			/* Allocate another buffer that fits all data (short of
			 * the common response header). We want all
			 * data to be in contigious memory. */

			rkbuf->rkbuf_buf2 = rd_malloc(rkbuf->rkbuf_len);
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
	}

	if (rkbuf->rkbuf_wof == rkbuf->rkbuf_len) {
		/* Message is complete, pass it on to the original requester. */
		rkb->rkb_recv_buf = NULL;
		(void)rd_atomic64_add(&rkb->rkb_c.rx, 1);
		(void)rd_atomic64_add(&rkb->rkb_c.rx_bytes, rkbuf->rkbuf_wof);
		rd_kafka_req_response(rkb, rkbuf);
	}

	return 1;

err:
	rd_kafka_broker_fail(rkb, err_code, "Receive failed: %s", errstr);
	return -1;
}


/**
 * Linux version of socket_cb providing racefree CLOEXEC.
 */
int rd_kafka_socket_cb_linux (int domain, int type, int protocol,
                              void *opaque) {
#ifdef SOCK_CLOEXEC
        return socket(domain, type | SOCK_CLOEXEC, protocol);
#else
        return rd_kafka_socket_cb_generic(domain, type, protocol, opaque);
#endif
}

/**
 * Fallback version of socket_cb NOT providing racefree CLOEXEC,
 * but setting CLOEXEC after socket creation (if FD_CLOEXEC is defined).
 */
int rd_kafka_socket_cb_generic (int domain, int type, int protocol,
                                void *opaque) {
        int s;
        int on = 1;
        s = socket(domain, type, protocol);
        if (s == -1)
                return -1;
#ifdef FD_CLOEXEC
        fcntl(s, F_SETFD, FD_CLOEXEC, &on);
#endif
        return s;
}


/**
 * Initiate asynchronous connection attempt to the next address
 * in the broker's address list.
 * While the connect is asynchronous and its IO served in the CONNECT state,
 * the initial name resolve is blocking.
 *
 * Returns -1 on error, else 0.
 */
static int rd_kafka_broker_connect (rd_kafka_broker_t *rkb) {
	rd_sockaddr_inx_t *sinx;
	char errstr[512];

	rd_rkb_dbg(rkb, BROKER, "CONNECT",
		"broker in state %s connecting",
		rd_kafka_broker_state_names[rkb->rkb_state]);

	if (rd_kafka_broker_resolve(rkb) == -1)
		return -1;

	sinx = rd_sockaddr_list_next(rkb->rkb_rsal);

	rd_kafka_assert(rkb->rkb_rk, !rkb->rkb_transport);

	if (!(rkb->rkb_transport = rd_kafka_transport_connect(rkb, sinx,
		errstr, sizeof(errstr)))) {
		/* Avoid duplicate log messages */
		if (rkb->rkb_err.err == errno)
			rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__FAIL,
					     NULL);
		else
			rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__TRANSPORT,
					     "%s", errstr);
		return -1;
	}
	
	rd_kafka_broker_lock(rkb);
	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_CONNECT);
	rkb->rkb_err.err = 0;
	rd_kafka_broker_unlock(rkb);

	return 0;
}


/**
 * Call when asynchronous connection attempt completes, either succesfully
 * (if errstr is NULL) or fails.
 *
 * Locality: broker thread
 */
void rd_kafka_broker_connect_done (rd_kafka_broker_t *rkb, const char *errstr) {

	if (errstr) {
		/* Connect failed */
		if (rkb->rkb_err.err == errno)
			rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__FAIL,
					     NULL);
		else
			rd_kafka_broker_fail(rkb,
					     RD_KAFKA_RESP_ERR__TRANSPORT,
					     "%s", errstr);
		return;
	}

	/* Connect succeeded */

	rd_rkb_dbg(rkb, BROKER, "CONNECTED", "Connected");
	
	rd_kafka_broker_lock(rkb);
	rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_UP);
	rkb->rkb_err.err = 0;
	rd_kafka_broker_unlock(rkb);

	rd_kafka_transport_poll_set(rkb->rkb_transport, POLLIN);

	/* Request metadata (async) */
        rd_kafka_broker_metadata_req(rkb,
                                     rkb->rkb_rk->rk_conf.
                                     metadata_refresh_sparse ?
                                     0 /* known topics */ : 1 /* all topics */,
                                     NULL, NULL, "connected");
}


/**
 * Send queued messages to broker
 *
 * Locality: io thread
 */
int rd_kafka_send (rd_kafka_broker_t *rkb) {
	rd_kafka_buf_t *rkbuf;
	unsigned int cnt = 0;

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

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

		if (0) {
			rd_rkb_dbg(rkb, BROKER, "SEND",
				   "Send buf corrid %"PRId32" at "
				   "offset %"PRIdsz"/%"PRIdsz"",
				   rkbuf->rkbuf_corrid,
				   rkbuf->rkbuf_of, rkbuf->rkbuf_len);
			msghdr_print(rkb->rkb_rk, "SEND", msg, 1);
		}
		if ((r = rd_kafka_broker_send(rkb, msg)) == -1) {
			/* FIXME: */
			return -1;
		}

		rkbuf->rkbuf_of += r;

		/* Partial send? Continue next time. */
		if (rkbuf->rkbuf_of < rkbuf->rkbuf_len) {
			return 0;
		}

		rd_rkb_dbg(rkb, PROTOCOL, "SEND",
			   "Sent %sRequest (v%hd, %"PRIdsz" bytes, "
			   "CorrId %"PRId32")",
			   rd_kafka_ApiKey2str(rkbuf->rkbuf_reqhdr.ApiKey),
                           rkbuf->rkbuf_reqhdr.ApiVersion,
			   rkbuf->rkbuf_len, rkbuf->rkbuf_corrid);

		/* Entire buffer sent, unlink from outbuf */
		rd_kafka_bufq_deq(&rkb->rkb_outbufs, rkbuf);
                (void)rd_atomic32_sub(&rkb->rkb_outbuf_msgcnt,
                                    rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt));

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

	return cnt;
}


/**
 * Add 'rkbuf' to broker 'rkb's retry queue.
 */
static void rd_kafka_broker_buf_retry (rd_kafka_broker_t *rkb,
				       rd_kafka_buf_t *rkbuf) {
	
	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	rd_atomic64_add(&rkb->rkb_c.tx_retries, 1);

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
void rd_kafka_dr_msgq (rd_kafka_topic_t *rkt,
		       rd_kafka_msgq_t *rkmq, rd_kafka_resp_err_t err) {
        rd_kafka_t *rk = rkt->rkt_rk;

	if (unlikely(rd_kafka_msgq_len(rkmq) == 0))
	    return;

        if ((rk->rk_conf.dr_cb || rk->rk_conf.dr_msg_cb) &&
	    (!rk->rk_conf.dr_err_only || err)) {
		/* Pass all messages to application thread in one op. */
		rd_kafka_op_t *rko;

		rko = rd_kafka_op_new(RD_KAFKA_OP_DR);
		rko->rko_err = err;
                rd_kafka_topic_keep(rkt);
                rko->rko_rkt = rkt;

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
rd_kafka_produce_reply_handle (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf,
                               int64_t *offsetp) {
	int32_t TopicArrayCnt;
	int32_t PartitionArrayCnt;
	struct {
		int32_t Partition;
		int16_t ErrorCode;
		int64_t Offset;
	} hdr;
        const int log_decode_errors = 1;

	rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
	if (TopicArrayCnt != 1)
		goto err;

	/* Since we only produce to one single topic+partition in each
	 * request we assume that the reply only contains one topic+partition
	 * and that it is the same that we requested.
	 * If not the broker is buggy. */
	rd_kafka_buf_skip_str(rkbuf);
	rd_kafka_buf_read_i32(rkbuf, &PartitionArrayCnt);

	if (PartitionArrayCnt != 1)
		goto err;

	rd_kafka_buf_read_i32(rkbuf, &hdr.Partition);
	rd_kafka_buf_read_i16(rkbuf, &hdr.ErrorCode);
	rd_kafka_buf_read_i64(rkbuf, &hdr.Offset);

        *offsetp = hdr.Offset;

	return hdr.ErrorCode;

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
        int64_t offset = -1;

	rd_rkb_dbg(rkb, MSG, "MSGSET",
		   "MessageSet with %i message(s) %sdelivered",
		   rd_atomic32_get(&request->rkbuf_msgq.rkmq_msg_cnt),
		   err ? "not ": "");

	/* Parse Produce reply (unless the request errored) */
	if (!err && reply)
		err = rd_kafka_produce_reply_handle(rkb, reply, &offset);


	if (err) {
		rd_rkb_dbg(rkb, MSG, "MSGSET", "MessageSet with %i message(s) "
			   "encountered error: %s",
			   rd_atomic32_get(&request->rkbuf_msgq.rkmq_msg_cnt),
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

        /* Propagate assigned offset back to app. */
        if (likely(offset != -1)) {
                rd_kafka_msg_t *rkm;
                if (rktp->rktp_rkt->rkt_conf.produce_offset_report) {
                        /* produce.offset.report: each message */
                        TAILQ_FOREACH(rkm, &request->rkbuf_msgq.rkmq_msgs,
                                      rkm_link)
                                rkm->rkm_offset = offset++;
                } else {
                        /* Last message in each batch */
                        rkm = TAILQ_LAST(&request->rkbuf_msgq.rkmq_msgs,
                                         rd_kafka_msg_head_s);
                        rkm->rkm_offset = offset +
                                rd_atomic32_get(&request->rkbuf_msgq.
                                                rkmq_msg_cnt);
                }
        }

	/* Enqueue messages for delivery report */
        rd_kafka_dr_msgq(rktp->rktp_rkt, &request->rkbuf_msgq, err);

done:
	rd_kafka_toppar_destroy(rktp); /* from produce_toppar() */

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}


/**
 * Compresses a MessageSet
 */
static int rd_kafka_compress_MessageSet_buf (rd_kafka_broker_t *rkb,
					     rd_kafka_toppar_t *rktp,
					     rd_kafka_buf_t *rkbuf,
					     int iov_firstmsg, int of_firstmsg,
					     int32_t *MessageSetSizep) {
	int32_t MessageSetSize = *MessageSetSizep;
	int    siovlen = 1;
	size_t coutlen = 0;
	int    outlen;
	int r;
	struct snappy_env senv;
	struct iovec siov;
#if WITH_ZLIB
	z_stream strm;
	int i;
#endif

	switch (rkb->rkb_rk->rk_conf.compression_codec) {
	case RD_KAFKA_COMPRESSION_NONE:
		abort(); /* unreachable */
		break;

#if WITH_ZLIB
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
				   MessageSetSize,
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				   rktp->rktp_partition,
				   strm.msg ? : "", r);
			return -1;
		}

		/* Calculate maximum compressed size and
		 * allocate an output buffer accordingly, being
		 * prefixed with the Message header. */
		siov.iov_len = deflateBound(&strm, MessageSetSize);
		siov.iov_base = rd_malloc(siov.iov_len);

		strm.next_out = (void *)siov.iov_base;
		strm.avail_out = siov.iov_len;

		/* Iterate through each message and compress it. */
		for (i = iov_firstmsg ;
		     i < (int)rkbuf->rkbuf_msg.msg_iovlen ; i++) {

			if (rkbuf->rkbuf_msg.msg_iov[i].iov_len == 0)
				continue;

			strm.next_in = (void *)rkbuf->rkbuf_msg.
				msg_iov[i].iov_base;
			strm.avail_in = rkbuf->rkbuf_msg.msg_iov[i].iov_len;

			/* Compress message */
			if ((r = deflate(&strm, Z_NO_FLUSH) != Z_OK)) {
				rd_rkb_log(rkb, LOG_ERR, "GZIP",
					   "Failed to gzip-compress "
					   "%"PRIdsz" bytes for "
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
				rd_free(siov.iov_base);
				return -1;
			}

			rd_kafka_assert(rkb->rkb_rk, strm.avail_in == 0);
		}

		/* Finish the compression */
		if ((r = deflate(&strm, Z_FINISH)) != Z_STREAM_END) {
			rd_rkb_log(rkb, LOG_ERR, "GZIP",
				   "Failed to finish gzip compression "
				   " of %"PRId32" bytes for "
				   "topic %.*s [%"PRId32"]: "
				   "%s (%i): "
				   "sending uncompressed",
				   MessageSetSize,
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
						    rkt_topic),
				   rktp->rktp_partition,
				   strm.msg ? : "", r);
			deflateEnd(&strm);
			rd_free(siov.iov_base);
			return -1;
		}

		coutlen = strm.total_out;

		/* Deinitialize compression */
		deflateEnd(&strm);
		break;
#endif

	case RD_KAFKA_COMPRESSION_SNAPPY:
		/* Initialize snappy compression environment */
		snappy_init_env_sg(&senv, 1/*iov enable*/);

		/* Calculate maximum compressed size and
		 * allocate an output buffer accordingly. */
		siov.iov_len = snappy_max_compressed_length(MessageSetSize);
		siov.iov_base = rd_malloc(siov.iov_len);

		/* Compress each message */
		if ((r = snappy_compress_iov(&senv,
					     &rkbuf->
					     rkbuf_iov[iov_firstmsg],
					     rkbuf->rkbuf_msg.
					     msg_iovlen -
					     iov_firstmsg,
					     MessageSetSize,
					     &siov, &siovlen,
					     &coutlen)) != 0) {
			rd_rkb_log(rkb, LOG_ERR, "SNAPPY",
				   "Failed to snappy-compress "
				   "%"PRId32" bytes for "
				   "topic %.*s [%"PRId32"]: %s: "
				   "sending uncompressed",
				   MessageSetSize,
				   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
						    rkt_topic),
				   rktp->rktp_partition,
				   rd_strerror(-r));
			rd_free(siov.iov_base);
			return -1;
		}

		/* rd_free snappy environment */
		snappy_free_env(&senv);
		break;

	default:
		rd_kafka_assert(rkb->rkb_rk,
				!*"notreached: compression.codec");
		break;

	}

	/* Rewind rkbuf to the pre-message checkpoint.
	 * This is to replace all the original Messages with just the
	 * Message containing the compressed payload. */
	rd_kafka_buf_rewind(rkbuf, iov_firstmsg, of_firstmsg);

	rd_kafka_buf_write_Message(rkbuf, 0, 0,
				   rkb->rkb_rk->rk_conf.compression_codec,
				   &rd_kafkap_bytes_null,
				   (void *)siov.iov_base, coutlen,
				   &outlen);

	/* Update enveloping MessageSet's length. */
	*MessageSetSizep = outlen;

	/* Add allocated buffer as auxbuf to rkbuf so that
	 * it will get freed with the rkbuf */
	rd_kafka_buf_auxbuf_add(rkbuf, siov.iov_base);

	return 0;
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
	int iovcnt;
	int iov_firstmsg;
	int of_firstmsg;
	int of_MessageSetSize;
	int32_t MessageSetSize = 0;
	int outlen;

	/* iovs:
	 *  1 * (RequiredAcks + Timeout + Topic + Partition + MessageSetSize)
	 *  msgcnt * messagehdr
	 *  msgcnt * key (ext memory)
	 *  msgcnt * Value_len
	 *  msgcnt * messagepayload (ext memory)
	 * = 1 + (4 * msgcnt)
	 *
	 * We are bound both by configuration and IOV_MAX
	 */

	if (rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt) > 0)
		rd_kafka_assert(rkb->rkb_rk,
                                TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs));
	msgcnt = RD_MIN(rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt),
			rkb->rkb_rk->rk_conf.batch_num_messages);
	rd_kafka_assert(rkb->rkb_rk, msgcnt > 0);
	iovcnt = 1 + (4 * msgcnt);

	if (iovcnt > RD_KAFKA_PAYLOAD_IOV_MAX) {
		iovcnt = RD_KAFKA_PAYLOAD_IOV_MAX;
		msgcnt = ((iovcnt / 4) - 1);
	}

	/* Allocate iovecs to hold all headers and messages,
	 * and allocate auxilliery space for the headers. */
	rkbuf = rd_kafka_buf_new(iovcnt,
				 /* RequiredAcks + Timeout + TopicCnt */
				 2 + 4 + 4 +
				 /* Topic */
				 RD_KAFKAP_STR_SIZE(rkt->rkt_topic) +
				 /* PartitionCnt + Partition + MessageSetSize */
				 4 + 4 + 4 +
				 /* MessageSet+Message * msgcnt */
				 ((8 + 4 + /* Offset+MessageSize*/
				   4 + 1 + 1 + 4 /* Crc+Magic+Attr+ValueLen */)
				  * msgcnt));

	/*
	 * Insert first part of Produce header
	 */
	/* RequiredAcks */
	rd_kafka_buf_write_i16(rkbuf, rkt->rkt_conf.required_acks);
	/* Timeout */
	rd_kafka_buf_write_i32(rkbuf, rkt->rkt_conf.request_timeout_ms);
	/* TopicArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);

	/* Insert topic */
	rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);

	/*
	 * Insert second part of Produce header
	 */
	/* PartitionArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	/* Partition */
	rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);
	/* MessageSetSize: Will be finalized later*/
	of_MessageSetSize = rd_kafka_buf_write_i32(rkbuf, 0);

	/* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

	iov_firstmsg = rkbuf->rkbuf_msg.msg_iovlen;
	of_firstmsg = rkbuf->rkbuf_wof;

	while (msgcnt > 0 &&
	       (rkm = TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs))) {

		if (MessageSetSize + rkm->rkm_len >
		    (size_t)rkb->rkb_rk->rk_conf.max_msg_size) {
			rd_rkb_dbg(rkb, MSG, "PRODUCE",
				   "No more space in current message "
				   "(%i messages)",
				   rd_atomic32_get(&rkbuf->rkbuf_msgq.
						   rkmq_msg_cnt));
			/* Not enough remaining size. */
			break;
		}

		rd_kafka_msgq_deq(&rktp->rktp_xmit_msgq, rkm, 1);
		rd_kafka_msgq_enq(&rkbuf->rkbuf_msgq, rkm);
		msgcnt--;

		/* Write message to buffer */
		rd_kafka_buf_write_Message(rkbuf, 0, 0,
					   RD_KAFKA_COMPRESSION_NONE,
					   rkm->rkm_key,
					   rkm->rkm_payload, rkm->rkm_len,
					   &outlen);

		MessageSetSize += outlen;
	}

	/* No messages added, bail out early. */
	if (unlikely(rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt) == 0)) {
		rd_kafka_buf_destroy(rkbuf);
		return -1;
	}


	/* Compress the message(s) */
	if (rkb->rkb_rk->rk_conf.compression_codec) {
		/* Update MessageSetSize prior to compression */
		rd_kafka_buf_update_i32(rkbuf,
					of_MessageSetSize, MessageSetSize);

		rd_kafka_compress_MessageSet_buf(rkb, rktp, rkbuf,
						 iov_firstmsg, of_firstmsg,
						 &MessageSetSize);
	}

	/* Update MessageSetSize */
	rd_kafka_buf_update_i32(rkbuf, of_MessageSetSize, MessageSetSize);
	
	rd_atomic64_add(&rktp->rktp_c.tx_msgs,
			rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt));
	rd_atomic64_add(&rktp->rktp_c.tx_bytes, MessageSetSize);

	rd_rkb_dbg(rkb, MSG, "PRODUCE",
		   "produce messageset with %i messages "
		   "(%"PRId32" bytes)",
		   rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt),
		   MessageSetSize);

	cnt = rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt);

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

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	switch (rko->rko_type)
	{
	case RD_KAFKA_OP_METADATA_REQ:
                rd_kafka_broker_metadata_req_op(rkb, rko);
                rko = NULL; /* metadata_req assumes rko ownership */
		break;

        case RD_KAFKA_OP_OFFSET_COMMIT:
                rd_kafka_toppar_offsetcommit_request(rkb,
                                                     rko->rko_rktp,
                                                     rko->rko_offset);
                break;

        case RD_KAFKA_OP_NODE_UPDATE:
        {
                enum {
                        _UPD_NAME = 0x1,
                        _UPD_ID = 0x2
                } updated = 0;
                char brokername[RD_KAFKA_NODENAME_SIZE];

                if (rko->rko_nodename) {
                        if (strcmp(rkb->rkb_nodename, rko->rko_nodename)) {
                                rd_rkb_dbg(rkb, BROKER, "UPDATE",
                                           "Nodename changed from %s to %s",
                                           rkb->rkb_nodename,
                                           (char *)rko->rko_nodename);
                                strncpy(rkb->rkb_nodename, rko->rko_nodename,
                                        sizeof(rkb->rkb_nodename)-1);
                                updated |= _UPD_NAME;
                        }
                        free(rko->rko_nodename);
                        rko->rko_nodename = NULL;
                }

                if (rko->rko_nodeid != -1 &&
                    rko->rko_nodeid != rkb->rkb_nodeid) {
                        rd_rkb_dbg(rkb, BROKER, "UPDATE",
                                   "NodeId changed from %"PRId32" to %"PRId32,
                                   rkb->rkb_nodeid, (int32_t)rko->rko_nodeid);
                        rkb->rkb_nodeid = rko->rko_nodeid;
                        updated |= _UPD_ID;
                }

                rd_kafka_mk_brokername(brokername, sizeof(brokername),
                                       rkb->rkb_proto,
				       rkb->rkb_nodename, rkb->rkb_nodeid,
				       RD_KAFKA_LEARNED);
                if (strcmp(rkb->rkb_name, brokername)) {
                        rd_rkb_dbg(rkb, BROKER, "UPDATE",
                                   "Name changed from %s to %s",
                                   rkb->rkb_name, brokername);
                        strncpy(rkb->rkb_name, brokername,
                                sizeof(rkb->rkb_name)-1);
                }

                if (updated & _UPD_NAME)
                        rd_kafka_broker_fail(rkb,
                                             RD_KAFKA_RESP_ERR__NODE_UPDATE,
                                             "Broker hostname updated");
                else if (updated & _UPD_ID) {
                        /* Query for topic leaders.
                         * This is done automatically from broker_fail()
                         * so we dont need this if the nodename changed too. */
                        rd_kafka_topic_leader_query(rkb->rkb_rk, NULL);
                        rd_kafka_broker_set_state(rkb,
                                                  RD_KAFKA_BROKER_STATE_UPDATE);
                }
                break;
        }

        case RD_KAFKA_OP_XMIT_BUF:
                rd_kafka_broker_buf_enq0(rkb, rko->rko_rkbuf,
                                         (rko->rko_rkbuf->rkbuf_flags &
                                          RD_KAFKA_OP_F_FLASH) ?
                                         1/*head*/: 0/*tail*/);
                rko->rko_rkbuf = NULL; /* buffer now owned by broker */
                if (rko->rko_replyq) {
                        /* Op will be reused for forwarding response. */
                        rko = NULL;
                }
                break;


	default:
		rd_kafka_assert(rkb->rkb_rk, !*"unhandled op type");
	}

        if (rko)
                rd_kafka_op_destroy(rko);
}


static void rd_kafka_broker_io_serve(rd_kafka_broker_t *rkb) {
	rd_kafka_op_t *rko;
	rd_ts_t now = rd_clock();

	/* Serve broker ops */
	if (unlikely(rd_kafka_q_len(&rkb->rkb_ops) > 0))
                while ((rko = rd_kafka_q_pop(&rkb->rkb_ops, RD_POLL_NOWAIT, 0)))
                        rd_kafka_broker_op_serve(rkb, rko);

	/* Periodic metadata poll */
	if (unlikely(!rkb->rkb_rk->rk_conf.metadata_refresh_sparse &&
		now >= rkb->rkb_ts_metadata_poll))
		rd_kafka_broker_metadata_req(rkb, 1 /* all topics */, NULL,
		NULL, "periodic refresh");

	/* Serve IO events */
	rd_kafka_transport_io_serve(rkb->rkb_transport);
}


/**
 * Idle function for unassigned brokers
 */
static void rd_kafka_broker_ua_idle (rd_kafka_broker_t *rkb) {
	rd_ts_t last_timeout_scan = rd_clock();
	int initial_state = rkb->rkb_state;

	/* Since ua_idle is used during connection setup 
	 * in state ..BROKER_STATE_CONNECT we only run this loop
	 * as long as the state remains the same as the initial, on a state
	 * change - most likely to UP, a correct serve() function
	 * should be used instead. */
	while (!rd_atomic32_get(&rkb->rkb_rk->rk_terminate) &&
	       (int)rkb->rkb_state == initial_state) {
		rd_ts_t now;


		if (rkb->rkb_source == RD_KAFKA_INTERNAL) {
			rd_kafka_toppar_t *rktp;

			rd_kafka_broker_toppars_rdlock(rkb);
			TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
				/* Serve toppar op queue to
				   update desired rktp state */
				rd_kafka_toppar_op_serve(rktp);
			}
			rd_kafka_broker_toppars_rdunlock(rkb);
			rd_usleep(100000);
			break;
		}

		rd_kafka_broker_io_serve(rkb);
                now = rd_clock();

		if (unlikely(rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP &&
			     last_timeout_scan + 1000000 < now)) {
                        /* Scan wait-response queue */
                        rd_kafka_broker_waitresp_timeout_scan(rkb, now);
			last_timeout_scan = now;
		}
        }
}


/**
 * Serve a toppar for producing.
 * NOTE: toppar_lock(rktp) MUST NOT be held.
 *
 * Returns the number of messages produced.
 */
static int rd_kafka_toppar_producer_serve (rd_kafka_broker_t *rkb,
                                           rd_kafka_toppar_t *rktp,
                                           int do_timeout_scan, rd_ts_t now) {
        int cnt = 0;

        rd_rkb_dbg(rkb, QUEUE, "TOPPAR",
                   "%.*s [%"PRId32"] %i+%i msgs",
                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->
                                    rkt_topic),
                   rktp->rktp_partition,
                   rd_atomic32_get(&rktp->rktp_msgq.rkmq_msg_cnt),
                   rd_atomic32_get(&rktp->rktp_xmit_msgq.
                                   rkmq_msg_cnt));

        rd_kafka_toppar_lock(rktp);

        /* Enforce ISR cnt (if set) */
        if (unlikely(rktp->rktp_rkt->rkt_conf.enforce_isr_cnt >
                     rktp->rktp_metadata.isr_cnt)) {

                /* Trigger delivery report for
                 * ISR failed msgs */
                rd_kafka_dr_msgq(rktp->rktp_rkt, &rktp->rktp_msgq,
                                 RD_KAFKA_RESP_ERR__ISR_INSUFF);
        }


        if (rd_atomic32_get(&rktp->rktp_msgq.rkmq_msg_cnt) > 0)
                rd_kafka_msgq_concat(&rktp->rktp_xmit_msgq, &rktp->rktp_msgq);
        rd_kafka_toppar_unlock(rktp);

        /* Timeout scan */
        if (unlikely(do_timeout_scan)) {
                rd_kafka_msgq_t timedout = RD_KAFKA_MSGQ_INITIALIZER(timedout);

                if (rd_kafka_msgq_age_scan(&rktp->rktp_xmit_msgq,
                                           &timedout, now)) {
                        /* Trigger delivery report for timed out messages */
                        rd_kafka_dr_msgq(rktp->rktp_rkt, &timedout,
                                         RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);
                }
        }

        if (rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt) == 0)
                return 0;

        /* Attempt to fill the batch size, but limit
         * our waiting to queue.buffering.max.ms
         * and batch.num.messages. */
        if (rktp->rktp_ts_last_xmit +
            (rkb->rkb_rk->rk_conf.buffering_max_ms * 1000) > now &&
            rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt) <
            rkb->rkb_rk->rk_conf.batch_num_messages) {
                /* Wait for more messages */
                return 0;
        }

        rktp->rktp_ts_last_xmit = now;

        /* Send Produce requests for this toppar */
        while (rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt) > 0) {
                int r = rd_kafka_broker_produce_toppar(rkb, rktp);
                if (likely(r > 0))
                        cnt += r;
                else
                        break;
        }

        return cnt;
}


/**
 * Producer serving
 */
static void rd_kafka_broker_producer_serve (rd_kafka_broker_t *rkb) {
	rd_ts_t last_timeout_scan = rd_clock();

        rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	rd_kafka_broker_lock(rkb);

	while (!rd_atomic32_get(&rkb->rkb_rk->rk_terminate) &&
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

                        /* Try producing each toppar */
			TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
                                cnt += rd_kafka_toppar_producer_serve(rkb, rktp,
                                                                      do_timeout_scan, 
                                                                      now);
			}

		} while (cnt);


		rd_kafka_broker_toppars_rdunlock(rkb);

		/* Check and move retry buffers */
		if (unlikely(rd_atomic32_get(&rkb->rkb_retrybufs.rkbq_cnt) > 0))
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

		while (of + 4 <= (ssize_t)inlen) {
			/* compressed length */
			uint32_t clen = be32toh(*(uint32_t *)(inbuf+of));
			/* uncompressed length */
			size_t ulen;
			int r;

			of += 4;

			if (unlikely(clen > inlen - of)) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Invalid snappy-java chunk length for "
					   "message at offset %"PRId64" "
					   "(%"PRIu32">%"PRIdsz": ignoring message",
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
					   rd_strerror(-r/*negative errno*/));
				rd_free(outbuf);
				return NULL;
			}

			of  += clen;
			uof += ulen;
		}

		if (unlikely(of != (ssize_t)inlen)) {
			rd_rkb_dbg(rkb, MSG, "SNAPPY",
				   "%"PRIdsz" trailing bytes in Snappy-java framed compressed "
				   "data at offset %"PRId64": ignoring message",
				   inlen - of, Offset);
			return NULL;
		}

		if (pass == 1) {
			if (uof <= 0) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Empty Snappy-java framed data "
					   "at offset %"PRId64" (%"PRIdsz" bytes): "
					   "ignoring message",
					   Offset, uof);
				return NULL;
			}

			/* Allocate memory for uncompressed data */
			outbuf = rd_malloc(uof);
			if (unlikely(!outbuf)) {
				rd_rkb_dbg(rkb, MSG, "SNAPPY",
					   "Failed to allocate memory for uncompressed "
					   "Snappy data at offset %"PRId64
					   " (%"PRIdsz" bytes): %s",
					   Offset, uof, rd_strerror(errno));
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
						       rd_kafka_buf_t *rkbuf_orig,
						       void *buf, size_t size) {
        rd_kafka_buf_t *rkbuf; /* Slice of rkbuf_orig */
	rd_kafka_buf_t *rkbufz;
        /* Dont log decode errors since Fetch replies may be partial. */
        const int log_decode_errors = 0;

        /* Set up a shadow rkbuf for parsing the slice of rkbuf_orig
         * pointed out by buf,size. */
        rkbuf = rd_kafka_buf_new_shadow(buf, size);

	if (rd_kafka_buf_remain(rkbuf) == 0)
		rd_kafka_buf_parse_fail(rkbuf,
					"%s [%"PRId32"] empty messageset",
					rktp->rktp_rkt->rkt_topic->str,
					rktp->rktp_partition);

	while (rd_kafka_buf_remain(rkbuf) > 0) {
		struct {
			int64_t Offset;
			int32_t MessageSize;
			uint32_t Crc;
			int8_t  MagicByte;
			int8_t  Attributes;
		} hdr;
		rd_kafkap_bytes_t Key;
		rd_kafkap_bytes_t Value;
		int32_t Value_len;
		rd_kafka_op_t *rko;
		size_t outlen;
		void *outbuf = NULL;

		rd_kafka_buf_read_i64(rkbuf, &hdr.Offset);
		rd_kafka_buf_read_i32(rkbuf, &hdr.MessageSize);
		rd_kafka_buf_read_i32(rkbuf, &hdr.Crc);
		rd_kafka_buf_read_i8(rkbuf, &hdr.MagicByte);
		rd_kafka_buf_read_i8(rkbuf, &hdr.Attributes);

                if (hdr.MessageSize - 6 > rd_kafka_buf_remain(rkbuf)) {
                        /* Broker may send partial messages.
                         * Bail out silently.
			 * "A Guide To The Kafka Protocol" states:
			 *   "As an optimization the server is allowed to
			 *    return a partial message at the end of the
			 *    message set.
			 *    Clients should handle this case."
			 * We're handling it by not passing the error upstream.
			 */
                        goto err;
                }
		/* Ignore CRC (for now) */

		/* Extract key */
		rd_kafka_buf_read_bytes(rkbuf, &Key);

		/* Extract Value */
		rd_kafka_buf_read_bytes(rkbuf, &Value);

		Value_len = RD_KAFKAP_BYTES_LEN(&Value);

		/* Check for message compression.
		 * The Key is ignored for compressed messages. */
		switch (hdr.Attributes & 0x3)
		{
		case RD_KAFKA_COMPRESSION_NONE:
			/* Pure uncompressed message, this is the innermost
			 * handler after all compression and cascaded
			 * messagesets have been peeled off. */

                        /* MessageSets may contain offsets earlier than we
                         * requested (compressed messagesets in particular),
                         * drop the earlier messages. */
                        if (hdr.Offset < rktp->rktp_next_offset)
                                continue;

			/* Create op and push on temporary queue. */
			rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH);

                        rko->rko_version = rktp->rktp_op_version;

			if (!RD_KAFKAP_BYTES_IS_NULL(&Key)) {
				rko->rko_rkmessage.key = (void *)Key.data;
				rko->rko_rkmessage.key_len =
					RD_KAFKAP_BYTES_LEN(&Key);
			}

                        /* Forward NULL message notation to application. */
			rko->rko_rkmessage.payload   =
                                RD_KAFKAP_BYTES_IS_NULL(&Value) ?
                                NULL : (void *)Value.data;
			rko->rko_rkmessage.len       = Value_len;

			rko->rko_rkmessage.offset    = hdr.Offset;
			rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
			rko->rko_rkmessage.partition = rktp->rktp_partition;
                        rd_kafka_topic_keep(rko->rko_rkmessage.rkt);

			rko->rko_rktp = rktp;
			rd_kafka_toppar_keep(rktp);

			rktp->rktp_next_offset = hdr.Offset + 1;

			/* Since all the ops share the same payload buffer
			 * (rkbuf->rkbuf_buf2) a refcnt is used on the rkbuf
			 * that makes sure all consume_cb() will have been
			 * called for each of these ops before the rkbuf
			 * and its rkbuf_buf2 are freed. */
			rko->rko_rkbuf = rkbuf_orig; /* original rkbuf */
			rd_kafka_buf_keep(rkbuf_orig);

			if (0)
			rd_rkb_dbg(rkb, MSG, "MSG",
				   "Pushed message at offset %"PRId64
				   " onto queue", hdr.Offset);

			rd_kafka_q_enq(rkq, rko);
			break;

#if ENABLE_GZIP
		case RD_KAFKA_COMPRESSION_GZIP:
		{
			uint64_t outlenx = 0;

			/* Decompress Message payload */
			outbuf = rd_gz_decompress(Value.data, Value_len,
						  &outlenx);
			if (unlikely(!outbuf)) {
				rd_rkb_dbg(rkb, MSG, "GZIP",
					   "Failed to decompress Gzip "
					   "message at offset %"PRId64
					   " of %"PRId32" bytes: "
					   "ignoring message",
					   hdr.Offset, Value_len);
				continue;
			}

			outlen = outlenx;
		}
		break;
#endif

		case RD_KAFKA_COMPRESSION_SNAPPY:
		{
			const char *inbuf = Value.data;
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
									 hdr.Offset,
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
						   hdr.Offset, Value_len);
					continue;
				}

				/* Allocate output buffer for uncompressed data */
				outbuf = rd_malloc(outlen);

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
						   hdr.Offset, Value_len,
						   rd_strerror(-r/*negative errno*/));
					rd_free(outbuf);
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

        /* rkbuf is a temporary shadow of rkbuf_orig, reset buf2 pointer
         * to avoid it being freed now. */
        rkbuf->rkbuf_buf2 = NULL;
        rd_kafka_buf_destroy(rkbuf);
	return 0;

err:
        /* Count all errors as partial message errors. */
        rd_atomic64_add(&rkb->rkb_c.rx_partial, 1);

        /* rkbuf is a temporary shadow of rkbuf_orig, reset buf2 pointer
         * to avoid it being freed now. */
        rkbuf->rkbuf_buf2 = NULL;
        rd_kafka_buf_destroy(rkbuf);

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
	int32_t TopicArrayCnt;
	int i;
        const int log_decode_errors = 1;

	rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
	/* Verify that TopicArrayCnt seems to be in line with remaining size */
	rd_kafka_buf_check_len(rkbuf,
			       TopicArrayCnt * (3/*topic min size*/ +
						4/*PartitionArrayCnt*/ +
						4+2+8+4/*inner header*/));

	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t topic;
		rd_kafka_toppar_t *rktp;
		int32_t PartitionArrayCnt;
		struct {
			int32_t Partition;
			int16_t ErrorCode;
			int64_t HighwaterMarkOffset;
			int32_t MessageSetSize;
		} hdr;
		rd_kafka_resp_err_t err2;
		int j;

		rd_kafka_buf_read_str(rkbuf, &topic);
		rd_kafka_buf_read_i32(rkbuf, &PartitionArrayCnt);

		rd_kafka_buf_check_len(rkbuf,
				       PartitionArrayCnt *
				       (4+2+8+4/*inner header*/));

		for (j = 0 ; j < PartitionArrayCnt ; j++) {
			rd_kafka_q_t tmp_opq; /* Temporary queue for ops */

			rd_kafka_buf_read_i32(rkbuf, &hdr.Partition);
			rd_kafka_buf_read_i16(rkbuf, &hdr.ErrorCode);
			rd_kafka_buf_read_i64(rkbuf, &hdr.HighwaterMarkOffset);
			rd_kafka_buf_read_i32(rkbuf, &hdr.MessageSetSize);

			rd_rkb_dbg(rkb, TOPIC, "MSGS",
				   "MessageSet: part %d, err %d, mss %d",
				   hdr.Partition, hdr.ErrorCode,
				   hdr.MessageSetSize);
                        if (hdr.MessageSetSize < 0)
                                rd_kafka_buf_parse_fail(rkbuf, "%.*s [%"PRId32"]: "
                                      "invalid MessageSetSize %"PRId32,
                                      RD_KAFKAP_STR_PR(&topic),
                                      hdr.Partition,
                                      hdr.MessageSetSize);

			/* Look up topic+partition */
			rktp = rd_kafka_toppar_get2(rkb->rkb_rk, &topic,
						    hdr.Partition, 0);
			if (unlikely(!rktp)) {
				rd_rkb_dbg(rkb, TOPIC, "UNKTOPIC",
					   "Received Fetch response "
					   "(error %hu) for unknown topic "
					   "%.*s [%"PRId32"]: ignoring",
					   hdr.ErrorCode,
					   RD_KAFKAP_STR_PR(&topic),
					   hdr.Partition);
				rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
				continue;
			}

			rd_rkb_dbg(rkb, MSG, "FETCH",
				   "Topic %.*s [%"PRId32"] MessageSet "
				   "size %"PRId32", error \"%s\", "
				   "MaxOffset %"PRId64", "
                                   "Ver %"PRId32"/%"PRId32,
				   RD_KAFKAP_STR_PR(&topic), hdr.Partition,
				   hdr.MessageSetSize,
				   rd_kafka_err2str(hdr.ErrorCode),
				   hdr.HighwaterMarkOffset,
                                   rktp->rktp_fetch_version,
                                   rktp->rktp_op_version);

                        /* This Fetch is for an outdated fetch version,
                         * ignore it. */
                        if (rktp->rktp_fetch_version <
                            rktp->rktp_op_version) {
                                rd_atomic64_add(&rktp->rktp_c. rx_ver_drops, 1);
                                rd_kafka_toppar_destroy(rktp); /* from get2 */
                                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                                continue;
                        }


			/* If this is the last message of the queue,
			 * signal EOF back to the application. */
			if (hdr.HighwaterMarkOffset == rktp->rktp_next_offset
			    &&
			    rktp->rktp_eof_offset != rktp->rktp_next_offset) {
				hdr.ErrorCode =
					RD_KAFKA_RESP_ERR__PARTITION_EOF;
				rktp->rktp_eof_offset = rktp->rktp_next_offset;
			}


			/* Handle partition-level errors. */
			if (unlikely(hdr.ErrorCode !=
				     RD_KAFKA_RESP_ERR_NO_ERROR)) {
				rd_kafka_op_t *rko;

				/* Some errors should be passed to the
				 * application while some handled by rdkafka */
				switch (hdr.ErrorCode)
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
							      hdr.ErrorCode,
							      "Fetch response");
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
				case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
				default: /* and all other errors */
					rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);
                                        rko->rko_rktp = rktp;
                                        rd_kafka_toppar_keep(rktp);
                                        rko->rko_version =
                                                rktp->rktp_fetch_version;
					rko->rko_err = hdr.ErrorCode;
					rko->rko_rkmessage.offset =
						rktp->rktp_next_offset;

					rko->rko_rkmessage.rkt = rktp->rktp_rkt;
					rko->rko_rkmessage.partition =
						rktp->rktp_partition;
                                        rd_kafka_topic_keep(rko->rko_rkmessage.
                                                            rkt);

					rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
					break;
				}

                                /* FIXME: only back off this rktp */
				rd_kafka_broker_fetch_backoff(rkb);

				rd_kafka_toppar_destroy(rktp); /* from get2() */

                                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
				continue;
			}

			if (hdr.MessageSetSize <= 0) {
				rd_kafka_toppar_destroy(rktp); /* from get2() */
				continue;
			}

			/* All parsed messages are put on this temporary op
			 * queue first and then moved in one go to the
			 * real op queue. */
			rd_kafka_q_init(&tmp_opq);

			/* Parse and handle the message set */
			err2 = rd_kafka_messageset_handle(rkb, rktp, &tmp_opq,
							  rkbuf,
							  rkbuf->rkbuf_rbuf+
							  rkbuf->rkbuf_of,
							  hdr.MessageSetSize);
			if (err2) {
				rd_kafka_q_destroy(&tmp_opq);
				rd_kafka_toppar_destroy(rktp); /* from get2() */
				rd_kafka_buf_parse_fail(rkbuf, "messageset handle failed");
			}

			/* Concat all messages onto the real op queue */
			rd_rkb_dbg(rkb, MSG, "CONSUME",
				   "Enqueue %i messages on %s [%"PRId32"] "
				   "fetch queue (%i)",
				   rd_atomic32_get(&tmp_opq.rkq_qlen),
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   rd_kafka_q_len(&rktp->rktp_fetchq));

			if (rd_atomic32_get(&tmp_opq.rkq_qlen) > 0) {
                                rd_atomic64_add(&rktp->rktp_c.msgs,
						rd_atomic32_get(&tmp_opq.rkq_qlen));
				rd_kafka_q_concat(&rktp->rktp_fetchq, &tmp_opq);
 				rd_kafka_q_reset(&tmp_opq);
                        }

			rd_kafka_toppar_destroy(rktp); /* from get2() */

			rd_kafka_q_destroy(&tmp_opq);

			rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
		}
	}

	if (rd_kafka_buf_remain(rkbuf) != 0) {
		rd_kafka_assert(NULL, 0);
		rd_kafka_buf_parse_fail(rkbuf,
					"Remaining data after message set "
					"parse: %i bytes",
					rd_kafka_buf_remain(rkbuf));
	}

	return 0;

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}



static void rd_kafka_broker_fetch_reply (rd_kafka_broker_t *rkb,
					 rd_kafka_resp_err_t err,
					 rd_kafka_buf_t *reply,
					 rd_kafka_buf_t *request,
					 void *opaque) {
	rd_kafka_assert(rkb->rkb_rk, rkb->rkb_fetching > 0);
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
			/* The fetch is already intervalled from
                         * consumer_serve() so dont retry. */
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
 * Parse and handle an OffsetCommitResponse message.
 * Returns an error code.
 */
static rd_kafka_resp_err_t
rd_kafka_toppar_offsetcommit_reply_handle (rd_kafka_broker_t *rkb,
                                           rd_kafka_buf_t *rkbuf,
                                           rd_kafka_toppar_t *rktp,
                                           int64_t offset) {
	int32_t TopicArrayCnt;
	int i;
        const int log_decode_errors = 1;

	rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t topic;
		int32_t PartitionArrayCnt;
		int j;

		rd_kafka_buf_read_str(rkbuf, &topic);
		rd_kafka_buf_read_i32(rkbuf, &PartitionArrayCnt);

		for (j = 0 ; j < PartitionArrayCnt ; j++) {
			int32_t Partition;
			int16_t ErrorCode;

			rd_kafka_buf_read_i32(rkbuf, &Partition);
			rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

			/* Skip toppars we didnt ask for. */
			if (unlikely(rktp->rktp_partition != Partition
				     || rd_kafkap_str_cmp(rktp->
							  rktp_rkt->
							  rkt_topic,
							  &topic)))
				continue;

			if (unlikely(ErrorCode != RD_KAFKA_RESP_ERR_NO_ERROR))
				return ErrorCode;

                        rktp->rktp_commited_offset = offset;

			rd_rkb_dbg(rkb, TOPIC, "OFFSET",
				   "OffsetCommitResponse "
                                   "for topic %s [%"PRId32"]: "
				   "offset %"PRId64" commited",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   offset);

			/* We just commited one toppar, so
			 * we're probably done now. */
			return RD_KAFKA_RESP_ERR_NO_ERROR;
		}
	}

	return RD_KAFKA_RESP_ERR_NO_ERROR;

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}



/**
 * Parses and handles an OffsetCommitResponse.
 */
static void rd_kafka_toppar_offsetcommit_reply (rd_kafka_broker_t *rkb,
                                                rd_kafka_resp_err_t err,
                                                rd_kafka_buf_t *reply,
                                                rd_kafka_buf_t *request,
                                                void *opaque) {
	rd_kafka_toppar_t *rktp = opaque;
	rd_kafka_op_t *rko;

	if (likely(!err && reply))
		err = rd_kafka_toppar_offsetcommit_reply_handle(rkb,
                                                                reply, rktp,
                                                                request->
                                                                rkbuf_offset);

	rd_rkb_dbg(rkb, TOPIC, "OFFSETCI",
		   "OffsetCommitResponse (%"PRId64") "
                   "for topic %s [%"PRId32"]: %s",
                   rktp->rktp_committing_offset,
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

		/* Signal error back to application */
		rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);
                rko->rko_version = rktp->rktp_op_version;
		rko->rko_err = err;
                /* FIXME: signal type of error */
		rko->rko_rkmessage.offset    = request->rkbuf_offset;
		rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
		rko->rko_rkmessage.partition = rktp->rktp_partition;
                rd_kafka_topic_keep(rko->rko_rkmessage.rkt);
		rd_kafka_q_enq(&rktp->rktp_fetchq, rko);

		/* FALLTHRU */
	}

	rd_kafka_toppar_destroy(rktp); /* refcnt from request */

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}



/**
 * Send OffsetCommitRequest for toppar.
 */
static void rd_kafka_toppar_offsetcommit_request (rd_kafka_broker_t *rkb,
                                                  rd_kafka_toppar_t *rktp,
                                                  int64_t offset) {
	rd_kafka_buf_t *rkbuf;
        static const rd_kafkap_str_t metadata = { .len = (uint16_t)-1 };

	rkbuf = rd_kafka_buf_new(1,
                                 /* How much memory to allocate for buffer: */
                                 /* static fields */
                                 4 + 4 + 4 + 8 +
                                 /* dynamic fields */
                                 RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->
                                                    rkt_conf.group_id) +
                                 RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->rkt_topic) +
                                 RD_KAFKAP_STR_SIZE(&metadata));

        rd_kafka_toppar_lock(rktp);

        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rktp->rktp_rkt->rkt_conf.group_id);
        /* TopicArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);
        /* TopicName */
        rd_kafka_buf_write_kstr(rkbuf, rktp->rktp_rkt->rkt_topic);
        /* PartitionArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);
        /* Partition */
        rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);
        /* Offset */
        rd_kafka_buf_write_i64(rkbuf, offset);
        /* Metadata (always empty for now) */
        rd_kafka_buf_write_kstr(rkbuf, &metadata);

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        rkbuf->rkbuf_offset = offset;
        rktp->rktp_committing_offset = offset;

        rd_kafka_toppar_keep(rktp);
        rd_kafka_toppar_unlock(rktp);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetCommitRequest (%"PRId64") for topic %s [%"PRId32"]",
                   offset,
		   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_OffsetCommit, rkbuf,
				 rd_kafka_toppar_offsetcommit_reply, rktp);

}



/**
 * Commit toppar's offset on broker.
 * This is an asynch operation, this function simply enqueues an op
 * on the broker's operations queue, if available.
 * NOTE: rd_kafka_toppar_lock(rktp) MUST be held.
 */
rd_kafka_resp_err_t rd_kafka_toppar_offset_commit (rd_kafka_toppar_t *rktp,
                                                   int64_t offset) {
        rd_kafka_op_t *rko;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

        rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_COMMIT);
        rko->rko_offset = offset;
        rko->rko_rktp = rktp;
        rd_kafka_toppar_keep(rko->rko_rktp);

        if (rktp->rktp_leader)
                rd_kafka_q_enq(&rktp->rktp_leader->rkb_ops, rko);
        else {
                rd_kafka_op_destroy(rko);
                err = RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
        }

        return err;
}







/**
 * Parse and handle an OffsetFetchResponse message.
 * Returns an error code.
 */
static rd_kafka_resp_err_t
rd_kafka_toppar_offsetfetch_reply_handle (rd_kafka_broker_t *rkb,
                                          rd_kafka_buf_t *rkbuf,
                                          rd_kafka_toppar_t *rktp) {
	int32_t TopicArrayCnt;
	int i;
        const int log_decode_errors = 1;

	rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t topic;
		int32_t PartitionArrayCnt;
		int j;

		rd_kafka_buf_read_str(rkbuf, &topic);
		rd_kafka_buf_read_i32(rkbuf, &PartitionArrayCnt);

		for (j = 0 ; j < PartitionArrayCnt ; j++) {
			int32_t Partition;
                        int64_t Offset;
                        rd_kafkap_str_t Metadata;
			int16_t ErrorCode;

			rd_kafka_buf_read_i32(rkbuf, &Partition);
                        rd_kafka_buf_read_i64(rkbuf, &Offset);
                        rd_kafka_buf_read_str(rkbuf, &Metadata);
			rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

			/* Skip toppars we didnt ask for. */
			if (unlikely(rktp->rktp_partition != Partition
				     || rd_kafkap_str_cmp(rktp->
							  rktp_rkt->rkt_topic,
							  &topic)))
				continue;

			if (unlikely(ErrorCode != RD_KAFKA_RESP_ERR_NO_ERROR))
				return ErrorCode;

			rd_rkb_dbg(rkb, TOPIC, "OFFSET",
				   "OffsetFetchResponse "
                                   "for topic %s [%"PRId32"]: "
				   "offset %"PRId64": activating fetch",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   Offset);

			rktp->rktp_next_offset = Offset;
			rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_ACTIVE;

			/* We just commited one toppar, so
			 * we're probably done now. */
			return RD_KAFKA_RESP_ERR_NO_ERROR;
		}
	}

	return RD_KAFKA_RESP_ERR_NO_ERROR;

err:
	return RD_KAFKA_RESP_ERR__BAD_MSG;
}



/**
 * Parse and handle an OffsetResponse message.
 * Returns an error code.
 */
static rd_kafka_resp_err_t
rd_kafka_toppar_offset_reply_handle (rd_kafka_broker_t *rkb,
                                     rd_kafka_buf_t *request,
				     rd_kafka_buf_t *rkbuf,
				     rd_kafka_toppar_t *rktp) {
	int32_t TopicArrayCnt;
	int i;
        const int log_decode_errors = 1;

	rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
	for (i = 0 ; i < TopicArrayCnt ; i++) {
		rd_kafkap_str_t topic;
		int32_t PartitionOffsetsArrayCnt;
		int j;

		rd_kafka_buf_read_str(rkbuf, &topic);
		rd_kafka_buf_read_i32(rkbuf, &PartitionOffsetsArrayCnt);

		for (j = 0 ; j < PartitionOffsetsArrayCnt ; j++) {
			int32_t Partition;
			int16_t ErrorCode;
			int32_t OffsetArrayCnt;
			int64_t Offset;

			rd_kafka_buf_read_i32(rkbuf, &Partition);
			rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
			rd_kafka_buf_read_i32(rkbuf, &OffsetArrayCnt);


			/* Skip toppars we didnt ask for. */
			if (unlikely(rktp->rktp_partition != Partition
				     || rd_kafkap_str_cmp(rktp->
							  rktp_rkt->rkt_topic,
							  &topic)))
				continue;

			if (unlikely(ErrorCode != RD_KAFKA_RESP_ERR_NO_ERROR))
				return ErrorCode;

			/* No offsets returned, convert to error code. */
			if (OffsetArrayCnt == 0)
				return RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE;

                        /* We only asked for one offset, so just read the
                         * first one returned. */
                        rd_kafka_buf_read_i64(rkbuf, &Offset);

			rd_rkb_dbg(rkb, TOPIC, "OFFSET",
				   "OffsetReply for topic %s [%"PRId32"]: "
				   "offset %"PRId64": activating fetch",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
				   Offset);


                        /* Call handler */
                        if (request->rkbuf_hndcb) {
                                void (*hndcb) (rd_kafka_toppar_t *, int64_t,
                                               void *);

                                hndcb = (void *)request->rkbuf_hndcb;
                                hndcb(rktp, Offset, request->rkbuf_hndopaque);
                        }

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
 * Parses and handles Offset and OffsetFetch replies.
 */
static void rd_kafka_toppar_offset_reply (rd_kafka_broker_t *rkb,
					  rd_kafka_resp_err_t err,
					  rd_kafka_buf_t *reply,
					  rd_kafka_buf_t *request,
					  void *opaque) {
	rd_kafka_toppar_t *rktp = opaque;
	rd_kafka_op_t *rko;

	if (likely(!err && reply)) {
                if (request->rkbuf_reqhdr.ApiKey == RD_KAFKAP_OffsetFetch)
                        err = rd_kafka_toppar_offsetfetch_reply_handle(rkb,
                                                                       reply,
                                                                       rktp);
                else
                        err = rd_kafka_toppar_offset_reply_handle(rkb,
                                                                  request,
                                                                  reply,
                                                                  rktp);
        }

	if (unlikely(err)) {
		int data_path_request = 0;
		if (request->rkbuf_hndcb == (void *)rd_kafka_toppar_next_offset_handle) {
			data_path_request = 1;
		}

                rd_rkb_dbg(rkb, TOPIC, "OFFSET",
                           "Offset (type %hd) reply error for %s "
                           "topic %s [%"PRId32"]: %s",
                           request->rkbuf_reqhdr.ApiKey,
                           data_path_request ? "data fetch" : "consumer lag",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           rd_kafka_err2str(err));


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

                if (request->rkbuf_hndcb ==
                    (void *)rd_kafka_toppar_next_offset_handle){
			/* Backoff until next retry */
			rktp->rktp_ts_offset_req_next = rd_clock() + 500000; /* 500ms */
			rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY;

                        /* Signal error back to application */
                        rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);
                        rko->rko_err = err;
                        if (rktp->rktp_query_offset <=
                            RD_KAFKA_OFFSET_TAIL_BASE)
                                rko->rko_rkmessage.offset =
                                        rktp->rktp_query_offset -
                                        RD_KAFKA_OFFSET_TAIL_BASE;
                        else
                                rko->rko_rkmessage.offset =
                                        rktp->rktp_query_offset;
                        rko->rko_rkmessage.rkt       = rktp->rktp_rkt;
                        rko->rko_rkmessage.partition = rktp->rktp_partition;
                        rd_kafka_topic_keep(rko->rko_rkmessage.rkt);
                        rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
                }

		/* FALLTHRU */
	}

	rd_kafka_toppar_destroy(rktp); /* refcnt from request */

	rd_kafka_buf_destroy(request);
	if (reply)
		rd_kafka_buf_destroy(reply);
}


/**
 * Send OffsetFetchRequest for toppar.
 */
static void rd_kafka_toppar_offsetfetch_request (rd_kafka_broker_t *rkb,
                                                 rd_kafka_toppar_t *rktp) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_kafka_buf_new(1,
                                 /* How much memory to allocate for buffer: */
                                 RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->rkt_conf.
                                                    group_id) +
                                 4 +
                                 RD_KAFKAP_STR_SIZE(rktp->rktp_rkt->rkt_topic) +
                                 4 + 4);


        rd_kafka_toppar_lock(rktp);

        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rktp->rktp_rkt->rkt_conf.group_id);
        /* TopicArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);
        /* TopicName */
        rd_kafka_buf_write_kstr(rkbuf, rktp->rktp_rkt->rkt_topic);
        /* PartitionArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);
        /* Partition */
        rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_WAIT;

        rd_kafka_toppar_keep(rktp);
        rd_kafka_toppar_unlock(rktp);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetFetchRequest for topic %s [%"PRId32"]",
		   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_OffsetFetch, rkbuf,
				 rd_kafka_toppar_offset_reply, rktp);

}




/**
 * Send OffsetRequest for toppar 'rktp'.
 * FIXME: Send for all interested rktps.
 */
static void rd_kafka_toppar_offset_request0 (rd_kafka_broker_t *rkb,
                                             rd_kafka_toppar_t *rktp,
                                             int64_t query_offset,
                                             void *hndcb, void *hndopaque) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_kafka_buf_new(3,/*Header,topic,subpart*/
				 /* ReplicaId+TopicArrayCnt */
				 4+4 +
				 /* Topic (pushed) */
				 /* PartArrayCnt+Partition+Time+MaxNumOffs */
				 4+4+8+4);

	/* ReplicaId */
	rd_kafka_buf_write_i32(rkbuf, -1);
	/* TopicArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	rd_kafka_buf_autopush(rkbuf);

	/* Topic */
	rd_kafka_buf_push_kstr(rkbuf, rktp->rktp_rkt->rkt_topic);

	/* PartitionArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	/* Partition */
	rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);
	/* Time/Offset */
	rd_kafka_buf_write_i64(rkbuf, query_offset);
	/* MaxNumberOfOffsets */
	rd_kafka_buf_write_i32(rkbuf, 1);
	rd_kafka_buf_autopush(rkbuf);
	
	rd_kafka_toppar_keep(rktp); /* refcnt for request */

        rkbuf->rkbuf_hndcb     = hndcb;
        rkbuf->rkbuf_hndopaque = hndopaque;

	rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_Offset, rkbuf,
				 rd_kafka_toppar_offset_reply, rktp);


	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetRequest (%"PRId64") for topic %s [%"PRId32"]",
                   query_offset,
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

}


static void rd_kafka_toppar_lo_offset_handle (rd_kafka_toppar_t *rktp,
                                              int64_t Offset, void *opaque) {
        rktp->rktp_lo_offset = Offset;
}

static void rd_kafka_toppar_hi_offset_handle (rd_kafka_toppar_t *rktp,
                                              int64_t Offset, void *opaque) {
        rktp->rktp_hi_offset = Offset;
}


static void rd_kafka_toppar_next_offset_handle (rd_kafka_toppar_t *rktp,
                                                int64_t Offset, void *opaque) {
        /* Adjust by TAIL count if, if wanted */
        if (rktp->rktp_query_offset <=
            RD_KAFKA_OFFSET_TAIL_BASE) {
                int64_t orig_Offset = Offset;
                int64_t tail_cnt =
                        llabs(rktp->rktp_query_offset -
                              RD_KAFKA_OFFSET_TAIL_BASE);

                if (tail_cnt > Offset)
                        Offset = 0;
                else
                        Offset -= tail_cnt;

                rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
                             "OffsetReply for topic %s [%"PRId32"]: "
                             "offset %"PRId64": adjusting for "
                             "OFFSET_TAIL(%"PRId64"): "
                             "effective offset %"PRId64,
                             rktp->rktp_rkt->rkt_topic->str,
                             rktp->rktp_partition,
                             orig_Offset, tail_cnt,
                             Offset);
        }

        rktp->rktp_next_offset = Offset;
        rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_ACTIVE;
}


/**
 * Send OffsetRequest for toppar.
 */
static void rd_kafka_toppar_offset_request (rd_kafka_broker_t *rkb,
					    rd_kafka_toppar_t *rktp,
                                            int64_t query_offset) {

	if (query_offset == RD_KAFKA_OFFSET_STORED &&
            rktp->rktp_rkt->rkt_conf.offset_store_method ==
		RD_KAFKA_OFFSET_METHOD_BROKER) {
		rd_kafka_toppar_offsetfetch_request(rkb, rktp);
		return;
	}

	rd_kafka_toppar_lock(rktp);
	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_WAIT;
	rd_kafka_toppar_unlock(rktp);

        rd_kafka_toppar_offset_request0(rkb, rktp,
                                        query_offset <=
                                        RD_KAFKA_OFFSET_TAIL_BASE ?
                                        RD_KAFKA_OFFSET_END :
                                        query_offset,
                                        rd_kafka_toppar_next_offset_handle,
                                        NULL);
}



static void rd_kafka_toppar_fetch_start (rd_kafka_toppar_t *rktp,
                                         int64_t offset,
                                         int32_t version,
                                         rd_kafka_op_t *rko_orig) {

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "FETCH",
                     "Start fetch for %.*s [%"PRId32"] in "
                     "state %s at offset %"PRId64" (v%"PRId32")",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
                     rd_kafka_fetch_states[rktp->rktp_fetch_state],
                     offset, version);

        rd_kafka_toppar_lock(rktp);
        if (offset == RD_KAFKA_OFFSET_BEGINNING ||
	    offset == RD_KAFKA_OFFSET_END ||
            offset <= RD_KAFKA_OFFSET_TAIL_BASE) {
                rktp->rktp_query_offset = offset;
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY;

	} else if (offset == RD_KAFKA_OFFSET_STORED) {
                rd_kafka_offset_store_init(rktp);
	} else {
		rktp->rktp_next_offset = offset;
		rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_ACTIVE;
	}

        rktp->rktp_op_version = version;
        rktp->rktp_eof_offset = -1;

        rd_kafka_toppar_unlock(rktp);

        /* Signal back to application thread that start has commenced */
        if (rko_orig->rko_replyq) {
                rd_kafka_op_t *rko;
                rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH_START);
                rko->rko_version = rko_orig->rko_version;
                rd_kafka_toppar_keep(rktp);
                rko->rko_rktp = rktp;
                rd_kafka_q_enq(rko_orig->rko_replyq, rko);
        }

}

static void rd_kafka_toppar_fetch_stop (rd_kafka_toppar_t *rktp,
                                        rd_kafka_op_t *rko_orig) {
        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "FETCH",
                     "Stopping fetch for %.*s [%"PRId32"] in state %s",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
                     rd_kafka_fetch_states[rktp->rktp_fetch_state]);
        rd_kafka_toppar_lock(rktp);
        rktp->rktp_op_version = rko_orig->rko_version;
        rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_NONE;
        rd_kafka_offset_store_term(rktp);
        rd_kafka_toppar_unlock(rktp);

        /* Signal back to application thread that stop is done. */
	if (rko_orig->rko_replyq) {
		rd_kafka_op_t *rko;
		rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH_STOP);
		rko->rko_version = rko_orig->rko_version;
		rd_kafka_toppar_keep(rktp);
		rko->rko_rktp = rktp;
		rd_kafka_q_enq(rko_orig->rko_replyq, rko);
	}
}

/**
 * Serve toppar's op queue to update thread-local state.
 */
static void rd_kafka_toppar_op_serve (rd_kafka_toppar_t *rktp) {
        rd_kafka_op_t *rko;

        while ((rko = rd_kafka_q_pop(&rktp->rktp_ops, RD_POLL_NOWAIT, 0))) {
                rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OP",
                             "%.*s [%"PRId32"] received op %s in state %s",
                             RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                             rktp->rktp_partition,
                             rd_kafka_op2str(rko->rko_type),
                             rd_kafka_fetch_states[rktp->rktp_fetch_state]);

                rd_kafka_assert(rktp->rktp_rkt->rkt_rk, rko->rko_rktp == rktp);
                switch (rko->rko_type)
                {
                case RD_KAFKA_OP_FETCH_START:
                        rd_kafka_toppar_fetch_start(rktp, rko->rko_offset,
                                                    rko->rko_version,
                                                    rko);
                        break;

                case RD_KAFKA_OP_FETCH_STOP:
                        rd_kafka_toppar_fetch_stop(rktp, rko);
                        break;

                default:
                        rd_kafka_assert(rktp->rktp_rkt->rkt_rk,
                                        !*"unknown type");
                        break;
                }

                rd_kafka_op_destroy(rko);
        }
}


/**
 * Request information from broker to keep track of consumer lag.
 *
 * Locality: broker thread
 */
static void rd_kafka_toppar_consumer_lag_req (rd_kafka_broker_t *rkb,
					      rd_kafka_toppar_t *rktp) {
	/* Ask for oldest and newest offset */
	rd_kafka_toppar_offset_request0(rkb, rktp,
					RD_KAFKA_OFFSET_BEGINNING,
					rd_kafka_toppar_lo_offset_handle, NULL);
	rd_kafka_toppar_offset_request0(rkb, rktp,
					RD_KAFKA_OFFSET_END,
					rd_kafka_toppar_hi_offset_handle, NULL);
}


/**
 * Build and send a Fetch request message for all underflowed toppars
 * for a specific broker.
 */
static int rd_kafka_broker_fetch_toppars (rd_kafka_broker_t *rkb) {
	rd_kafka_toppar_t *rktp;
	rd_kafka_buf_t *rkbuf;
	rd_ts_t now = rd_clock();
        int consumer_lag_intvl = rkb->rkb_rk->rk_conf.stats_interval_ms * 1000;
	int cnt = 0;
        int max_cnt;
	int iov_cnt;
	int of_TopicArrayCnt = 0;
	int TopicArrayCnt = 0;
	int of_PartitionArrayCnt = 0;
	int PartitionArrayCnt = 0;
	rd_kafka_topic_t *rkt_last = NULL;
	int warned_once_topparcnt = 0;

	/* Create buffer and iovecs:
	 *   1 x ReplicaId MaxWaitTime MinBytes TopicArrayCnt
	 *   N x topic name
	 *   N x PartitionArrayCnt Partition FetchOffset MaxBytes
	 * where N = number of toppars.
	 * Since we dont keep track of the number of topics served by
	 * this broker, only the partition count, we do a worst-case calc
	 * when allocation iovecs and assume each partition is on its own topic
	 */

	rd_kafka_broker_toppars_rdlock(rkb);
	iov_cnt = 1 + (rkb->rkb_toppar_cnt * 2); 

	/* Limit to maximum iovecs. This has the downside that if the
	 * application consumes from more than ~ IOV_MAX/2 partitions the
	 * over shooting partitions will not be consumed.
	 * Doing a WFQ-like round-robin solves this. */
	if (iov_cnt > RD_KAFKA_PAYLOAD_IOV_MAX)
		iov_cnt = RD_KAFKA_PAYLOAD_IOV_MAX;
	max_cnt = (iov_cnt - 1) / 2;

	rkbuf = rd_kafka_buf_new(iov_cnt,
				 /* ReplicaId+MaxWaitTime+MinBytes+TopicCnt */
				 4+4+4+4+
				 /* N x PartCnt+Partition+FetchOffset+MaxBytes*/
				 (max_cnt * (4+4+8+4)));

	/* FetchRequest header */
	/* ReplicaId */
	rd_kafka_buf_write_i32(rkbuf, -1);
	/* MaxWaitTime */
	rd_kafka_buf_write_i32(rkbuf, rkb->rkb_rk->rk_conf.fetch_wait_max_ms);
	/* MinBytes */
	rd_kafka_buf_write_i32(rkbuf, rkb->rkb_rk->rk_conf.fetch_min_bytes);

	/* Write zero TopicArrayCnt but store pointer for later update */
	of_TopicArrayCnt = rd_kafka_buf_write_i32(rkbuf, 0);

	rd_kafka_buf_autopush(rkbuf);

	TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
                /* Serve toppar op queue to update desired rktp state */
                rd_kafka_toppar_op_serve(rktp);

                /* Request offsets to measure consumer lag */
                if (consumer_lag_intvl &&
                    rktp->rktp_ts_offset_lag + consumer_lag_intvl < now) {
			rd_kafka_toppar_consumer_lag_req(rkb, rktp);
                        rktp->rktp_ts_offset_lag = now;
                }

		/* Check Toppar Fetch state */
		if (unlikely(rktp->rktp_fetch_state !=
			     RD_KAFKA_TOPPAR_FETCH_ACTIVE)) {

			switch (rktp->rktp_fetch_state)
			{
                        case RD_KAFKA_TOPPAR_FETCH_COORD_QUERY:
                                break;

			case RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY:
				if (rktp->rktp_ts_offset_req_next <= now)
					rd_kafka_toppar_offset_request(rkb,
								       rktp,
                                                                       rktp->rktp_query_offset);

				break;
			default:
				break;
			}

			/* Skip this toppar until its state changes */
                        if (rktp->rktp_fetch_state != RD_KAFKA_TOPPAR_FETCH_NONE)
                                rd_rkb_dbg(rkb, TOPIC, "FETCH",
                                           "Skipping topic %s [%"PRId32"] "
                                           "in state %s",
                                           rktp->rktp_rkt->rkt_topic->str,
                                           rktp->rktp_partition,
                                           rd_kafka_fetch_states[rktp->
                                                                 rktp_fetch_state]);
			continue;
		}

                rd_rkb_dbg(rkb, QUEUE, "FETCH",
                           "Topic %s [%"PRId32"] "
                           "fetch queue %i (%"PRIu64"kb) >= "
                           "queued.min.messages %i "
                           "(queued.max.messages.kbytes %i)?",
                           rktp->rktp_rkt->rkt_topic->str,
                           rktp->rktp_partition,
                           rd_kafka_q_len(&rktp->rktp_fetchq),
                           rd_kafka_q_size(&rktp->rktp_fetchq) / 1024,
                           rkb->rkb_rk->rk_conf.queued_min_msgs,
                           rkb->rkb_rk->rk_conf.queued_max_msg_kbytes);


		/* Skip toppars who's local message queue is already above
		 * the lower threshold. */
		if (rd_kafka_q_len(&rktp->rktp_fetchq) >=
		    rkb->rkb_rk->rk_conf.queued_min_msgs) {
			rd_rkb_dbg(rkb, TOPIC, "FETCH",
				   "Skipping topic %s [%"PRId32"]: "
                                   "threshold queued.min.messages=%i "
                                   "exceeded: %i messages in queue",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
                                   rkb->rkb_rk->rk_conf.queued_min_msgs,
                                   rd_kafka_q_len(&rktp->rktp_fetchq));
			continue;
                }

                if ((int64_t)rd_kafka_q_size(&rktp->rktp_fetchq) >=
                    rkb->rkb_rk->rk_conf.queued_max_msg_bytes) {
			rd_rkb_dbg(rkb, TOPIC, "FETCH",
				   "Skipping topic %s [%"PRId32"]: "
                                   "threshold queued.max.messages.kbytes=%i "
                                   "exceeded: %"PRId64" bytes in queue",
				   rktp->rktp_rkt->rkt_topic->str,
				   rktp->rktp_partition,
                                   rkb->rkb_rk->rk_conf.queued_max_msg_kbytes,
                                   rd_kafka_q_size(&rktp->rktp_fetchq));
			continue;
                }

		if (rd_kafka_buf_iov_remain(rkbuf) < 2) {
			if (!warned_once_topparcnt++)
				rd_rkb_dbg(rkb, TOPIC, "FETCH",
					   "FIXME: Fetching too many "
					   "partitions (>%i), see issue #110",
					   cnt);
			continue;
                }

		if (rkt_last != rktp->rktp_rkt) {
			if (rkt_last != NULL) {
				/* Update PartitionArrayCnt */
				rd_kafka_buf_update_i32(rkbuf,
							of_PartitionArrayCnt,
							PartitionArrayCnt);
				rd_kafka_buf_autopush(rkbuf);
			}

			/* Push topic name onto buffer stack. */
			rd_kafka_buf_push_kstr(rkbuf,
					       rktp->rktp_rkt->rkt_topic);
			TopicArrayCnt++;
			rkt_last = rktp->rktp_rkt;
			of_PartitionArrayCnt = rd_kafka_buf_write_i32(rkbuf, 0);
			PartitionArrayCnt = 0;
		}

		PartitionArrayCnt++;
		/* Partition */
		rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);
		/* FetchOffset */
		rd_kafka_buf_write_i64(rkbuf, rktp->rktp_next_offset);
		/* MaxBytes */
		rd_kafka_buf_write_i32(rkbuf, rkb->rkb_rk->rk_conf.
				       fetch_msg_max_bytes);

		rd_rkb_dbg(rkb, TOPIC, "FETCH",
			   "Fetch topic %.*s [%"PRId32"] at offset %"PRId64,
			   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			   rktp->rktp_partition,
			   rktp->rktp_next_offset);

                rktp->rktp_fetch_version = rktp->rktp_op_version;

		cnt++;
	}

	rd_kafka_broker_toppars_rdunlock(rkb);

	rd_rkb_dbg(rkb, MSG, "CONSUME", "consume from %i toppar(s)", cnt);
	if (!cnt) {
		rd_kafka_buf_destroy(rkbuf);
		return cnt;
	}

	if (rkt_last != NULL) {
		/* Update last topic's PartitionArrayCnt */
		rd_kafka_buf_update_i32(rkbuf,
					of_PartitionArrayCnt,
					PartitionArrayCnt);
		rd_kafka_buf_autopush(rkbuf);
	}

	/* Update TopicArrayCnt */
	rd_kafka_buf_update_i32(rkbuf, of_TopicArrayCnt, TopicArrayCnt);


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

	rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

	rd_kafka_broker_lock(rkb);

	while (!rd_atomic32_get(&rkb->rkb_rk->rk_terminate) &&
	       rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP) {
		int cnt = 0;
		rd_ts_t now;
		int do_timeout_scan = 1; /* FIXME */

		now = rd_clock();

		/* Send Fetch request message for all underflowed toppars */
		if (!rkb->rkb_fetching && rkb->rkb_ts_fetch_backoff < now)
			cnt = rd_kafka_broker_fetch_toppars(rkb);

		rd_rkb_dbg(rkb, QUEUE, "FETCH",
			   "Fetch for %i toppars, fetching=%i, "
                           "backoff=%"PRId64"ms",
			   cnt, rkb->rkb_fetching,
                           rkb->rkb_ts_fetch_backoff ?
                           (rkb->rkb_ts_fetch_backoff - now)/1000 : 0);

		/* Check and move retry buffers */
		if (unlikely(rd_atomic32_get(&rkb->rkb_retrybufs.rkbq_cnt) > 0))
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


static int rd_kafka_broker_thread_main (void *arg) {
	rd_kafka_broker_t *rkb = arg;
	rd_kafka_t *rk = rkb->rkb_rk;

	thrd_detach(thrd_current());
        rd_snprintf(rd_kafka_thread_name, sizeof(rd_kafka_thread_name),
		    "%s", rkb->rkb_name);

	(void)rd_atomic32_add(&rd_kafka_thread_cnt_curr, 1);

	/* Acquire lock (which was held by thread creator during creation)
	 * to synchronise state. */
	rd_kafka_broker_lock(rkb);
	rd_kafka_broker_unlock(rkb);
	rd_rkb_dbg(rkb, BROKER, "BRKMAIN", "Enter main broker thread");

	while (!rd_atomic32_get(&rkb->rkb_rk->rk_terminate)) {
		switch (rkb->rkb_state)
		{
		case RD_KAFKA_BROKER_STATE_INIT:
			/* The INIT state exists so that an initial connection
			 * failure triggers a state transition which might
			 * trigger a ALL_BROKERS_DOWN error. */
		case RD_KAFKA_BROKER_STATE_DOWN:
			if (rkb->rkb_source == RD_KAFKA_INTERNAL) {
				rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_UP);
				break;
			}

			/* Initiate asynchronous connection attempt.
			 * Only the host lookup is blocking here. */
			if (rd_kafka_broker_connect(rkb) == -1) {
				/* Immediate failure, most likely host
				 * resolving failed.
				 * Try the next resolve result until we've
				 * tried them all, in which case we sleep a
				 * short while to avoid the busy looping. */
				if (!rkb->rkb_rsal ||
					rkb->rkb_rsal->rsal_cnt == 0 ||
					rkb->rkb_rsal->rsal_curr + 1 ==
					rkb->rkb_rsal->rsal_cnt)
					rd_usleep(1000000); /* 1s */
			}
			break;

		case RD_KAFKA_BROKER_STATE_CONNECT:
			/* Asynchronous connect in progress. */
			rd_kafka_broker_ua_idle(rkb);

			if (rkb->rkb_state == RD_KAFKA_BROKER_STATE_DOWN) {
				/* Connect failure.
				 * Try the next resolve result until we've
				 * tried them all, in which case we sleep a
				 * short while to avoid the busy looping. */
				if (!rkb->rkb_rsal ||
					rkb->rkb_rsal->rsal_cnt == 0 ||
					rkb->rkb_rsal->rsal_curr + 1 ==
					rkb->rkb_rsal->rsal_cnt)
					rd_usleep(1000000); /* 1s */
			}
			break;

                case RD_KAFKA_BROKER_STATE_UPDATE:
                        /* FALLTHRU */
		case RD_KAFKA_BROKER_STATE_UP:
			if (rkb->rkb_nodeid == RD_KAFKA_NODEID_UA)
				rd_kafka_broker_ua_idle(rkb);
			else if (rk->rk_type == RD_KAFKA_PRODUCER)
				rd_kafka_broker_producer_serve(rkb);
			else if (rk->rk_type == RD_KAFKA_CONSUMER)
				rd_kafka_broker_consumer_serve(rkb);

			if (rkb->rkb_state == RD_KAFKA_BROKER_STATE_UPDATE)
				rd_kafka_broker_set_state(rkb, RD_KAFKA_BROKER_STATE_UP);
			else if (!rd_atomic32_get(&rkb->rkb_rk->rk_terminate)) {
				/* Connection torn down, sleep a short while to
				 * avoid busy-looping on protocol errors */
				rd_usleep(100*1000);
			}
			break;
		}

	}

	if (rkb->rkb_source != RD_KAFKA_INTERNAL) {
		rd_kafka_wrlock(rkb->rkb_rk);
		TAILQ_REMOVE(&rkb->rkb_rk->rk_brokers, rkb, rkb_link);
		(void)rd_atomic32_sub(&rkb->rkb_rk->rk_broker_cnt, 1);
		rd_kafka_wrunlock(rkb->rkb_rk);
	}
	rd_kafka_broker_fail(rkb, RD_KAFKA_RESP_ERR__DESTROY, NULL);
	rd_kafka_broker_destroy(rkb);

	(void)rd_atomic32_sub(&rd_kafka_thread_cnt_curr, 1);

	return 0;
}


void rd_kafka_broker_destroy (rd_kafka_broker_t *rkb) {

	if (rd_atomic32_sub(&rkb->rkb_refcnt, 1) > 0)
		return;

	rd_kafka_assert(rkb->rkb_rk, TAILQ_EMPTY(&rkb->rkb_outbufs.rkbq_bufs));
	rd_kafka_assert(rkb->rkb_rk, TAILQ_EMPTY(&rkb->rkb_toppars));

	if (rkb->rkb_recv_buf)
		rd_kafka_buf_destroy(rkb->rkb_recv_buf);

	if (rkb->rkb_rsal)
		rd_sockaddr_list_destroy(rkb->rkb_rsal);

	rd_kafka_q_purge(&rkb->rkb_ops);
	rd_kafka_q_destroy(&rkb->rkb_ops);

        rd_kafka_avg_destroy(&rkb->rkb_avg_rtt);

	rd_kafka_destroy0(rkb->rkb_rk);

	rwlock_destroy(&rkb->rkb_toppar_lock);
	mtx_destroy(&rkb->rkb_lock);

	rd_free(rkb);
}

/**
 * Returns the internal broker with refcnt increased.
 * NOTE: rd_kafka_*lock() MUST NOT be held.
 */
rd_kafka_broker_t *rd_kafka_broker_internal (rd_kafka_t *rk) {
	rd_kafka_broker_t *rkb;

	rd_kafka_rdlock(rk);
	rkb = rk->rk_internal_rkb;
	rd_kafka_rdunlock(rk);

	if (rkb)
		rd_kafka_broker_keep(rkb);

	return rkb;
}


/**
 *
 * Locks: rd_kafka_wrlock(rk) must be held
 */
rd_kafka_broker_t *rd_kafka_broker_add (rd_kafka_t *rk,
					rd_kafka_confsource_t source,
					rd_kafka_secproto_t proto,
					const char *name, uint16_t port,
					int32_t nodeid) {
	rd_kafka_broker_t *rkb;
	int err;
#ifndef _MSC_VER
	sigset_t newset, oldset;
#endif

	rd_kafka_keep(rk);

	rkb = rd_calloc(1, sizeof(*rkb));

        rd_kafka_mk_nodename(rkb->rkb_nodename, sizeof(rkb->rkb_nodename),
                             name, port);
        rd_kafka_mk_brokername(rkb->rkb_name, sizeof(rkb->rkb_name),
                               proto, rkb->rkb_nodename, nodeid, source);

	rkb->rkb_source = source;
	rkb->rkb_rk = rk;
	rkb->rkb_nodeid = nodeid;
	rkb->rkb_proto = proto;

	mtx_init(&rkb->rkb_lock, mtx_plain);
	rwlock_init(&rkb->rkb_toppar_lock);
	TAILQ_INIT(&rkb->rkb_toppars);
	rd_kafka_bufq_init(&rkb->rkb_outbufs);
	rd_kafka_bufq_init(&rkb->rkb_waitresps);
	rd_kafka_bufq_init(&rkb->rkb_retrybufs);
	rd_kafka_q_init(&rkb->rkb_ops);
	rd_kafka_avg_init(&rkb->rkb_avg_rtt, RD_KAFKA_AVG_GAUGE);
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

#ifndef _MSC_VER
        /* Block all signals in newly created thread.
         * To avoid race condition we block all signals in the calling
         * thread, which the new thread will inherit its sigmask from,
         * and then restore the original sigmask of the calling thread when
         * we're done creating the thread.
	 * NOTE: term_sig remains unblocked since we use it on termination
	 *       to quickly interrupt system calls. */
        sigemptyset(&oldset);
        sigfillset(&newset);
	if (rkb->rkb_rk->rk_conf.term_sig)
		sigdelset(&newset, rkb->rkb_rk->rk_conf.term_sig);
        pthread_sigmask(SIG_SETMASK, &newset, &oldset);
#endif

	/* Lock broker's lock here to synchronise state, i.e., hold off
	 * the broker thread until we've finalized the rkb. */
	rd_kafka_broker_lock(rkb);
	if ((err = thrd_create(&rkb->rkb_thread,
		rd_kafka_broker_thread_main, rkb)) != thrd_success) {
		char tmp[512];
		rd_snprintf(tmp, sizeof(tmp),
			 "Unable to create broker thread: %s (%i)",
			 rd_strerror(err), err);
		rd_kafka_log(rk, LOG_CRIT, "THREAD", "%s", tmp);

		rd_kafka_broker_unlock(rkb);

		/* Send ERR op back to application for processing. */
		rd_kafka_op_err(rk, RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
				"%s", tmp);

		rd_free(rkb);
		rd_kafka_destroy(rk);

#ifndef _MSC_VER
		/* Restore sigmask of caller */
		pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif

		return NULL;
	}

	if (rkb->rkb_source != RD_KAFKA_INTERNAL) {
		TAILQ_INSERT_TAIL(&rkb->rkb_rk->rk_brokers, rkb, rkb_link);
		(void)rd_atomic32_add(&rkb->rkb_rk->rk_broker_cnt, 1);
		rd_rkb_dbg(rkb, BROKER, "BROKER",
			   "Added new broker with NodeId %"PRId32,
			   rkb->rkb_nodeid);
	}

	rd_kafka_broker_unlock(rkb);

#ifndef _MSC_VER
	/* Restore sigmask of caller */
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif

	return rkb;
}

/**
 * Locks: rd_kafka_rdlock()
 * NOTE: caller must release rkb reference by rd_kafka_broker_destroy()
 */
rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid0 (rd_kafka_t *rk,
                                                    int32_t nodeid,
                                                    int state) {
	rd_kafka_broker_t *rkb;

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if (!rd_atomic32_get(&rk->rk_terminate) &&
		    rkb->rkb_nodeid == nodeid) {
                        if (state != -1 && (int)rkb->rkb_state != state) {
                                rd_kafka_broker_unlock(rkb);
                                break;
                        }
			rd_kafka_broker_keep(rkb);
			rd_kafka_broker_unlock(rkb);
			return rkb;
		}
		rd_kafka_broker_unlock(rkb);
	}

	return NULL;

}

/**
 * Locks: rd_kafka_rdlock(rk) must be held
 * NOTE: caller must release rkb reference by rd_kafka_broker_destroy()
 */
static rd_kafka_broker_t *rd_kafka_broker_find (rd_kafka_t *rk,
						rd_kafka_secproto_t proto,
						const char *name,
						uint16_t port) {
	rd_kafka_broker_t *rkb;
	char nodename[RD_KAFKA_NODENAME_SIZE];

        rd_kafka_mk_nodename(nodename, sizeof(nodename), name, port);

	TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
		rd_kafka_broker_lock(rkb);
		if (!rd_atomic32_get(&rk->rk_terminate) &&
		    rkb->rkb_proto == proto &&
		    !strcmp(rkb->rkb_nodename, nodename)) {
			rd_kafka_broker_keep(rkb);
			rd_kafka_broker_unlock(rkb);
			return rkb;
		}
		rd_kafka_broker_unlock(rkb);
	}

	return NULL;
}


/**
 * Parse a broker host name.
 * The string 'name' is modified and null-terminated portions of it
 * are returned in 'proto', 'host', and 'port'.
 *
 * Returns 0 on success or -1 on parse error.
 */
static int rd_kafka_broker_name_parse (rd_kafka_t *rk,
				       char **name,
				       rd_kafka_secproto_t *proto,
				       const char **host,
				       uint16_t *port) {
	char *s = *name;
	char *n, *t, *t2;

	/* Find end of this name (either by delimiter or end of string */
	if ((n = strchr(s, ',')))
		*n = '\0';
	else
		n = s + strlen(s)-1;


	/* Check if this looks like an url. */
	if ((t = strstr(s, "://"))) {
		int i;
		/* "proto://host[:port]" */

		if (t == s)
			return -1; /* empty proto */

		/* Make protocol uppercase */
		for (t2 = s ; t2 < t ; t2++)
			*t2 = toupper(*t2);

		*t = '\0';

		/* Find matching protocol by name. */
		for (i = 0 ; i < RD_KAFKA_PROTO_NUM ; i++)
			if (!rd_strcasecmp(s, rd_kafka_secproto_names[i]))
				break;

		/* Unsupported protocol */
		if (i == RD_KAFKA_PROTO_NUM)
			return -1;

		*proto = i;

                /* Enforce protocol */
		if (rk->rk_conf.security_protocol != *proto)
			return -1;

		/* Hostname starts here */
		s = t+3;

		/* Ignore anything that looks like the path part of an URL */
		if ((t = strchr(s, '/')))
			*t = '\0';

	} else
		*proto = rk->rk_conf.security_protocol; /* Default protocol */


	*port = RD_KAFKA_PORT;
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
		*port = atoi(t+1);
	}

	/* Empty host name -> localhost */
	if (!*s) 
		s = "localhost";

	*host = s;
	*name = n+1;  /* past this name. e.g., next name/delimiter to parse */

	return 0;
}


int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist) {
	char *s_copy = rd_strdup(brokerlist);
	char *s = s_copy;
	int cnt = 0;
	rd_kafka_broker_t *rkb;

	/* Parse comma-separated list of brokers. */
	while (*s) {
		uint16_t port;
		const char *host;
		rd_kafka_secproto_t proto;
		

		if (*s == ',' || *s == ' ') {
			s++;
			continue;
		}

		if (rd_kafka_broker_name_parse(rk, &s, &proto,
					       &host, &port) == -1)
			break;

		rd_kafka_wrlock(rk);

		if ((rkb = rd_kafka_broker_find(rk, proto, host, port)) &&
		    rkb->rkb_source == RD_KAFKA_CONFIGURED) {
			cnt++;
		} else if (rd_kafka_broker_add(rk, RD_KAFKA_CONFIGURED,
					       proto, host, port,
					       RD_KAFKA_NODEID_UA) != NULL)
			cnt++;
		
		/* If rd_kafka_broker_find returned a broker its
		 * reference needs to be released 
		 * See issue #193 */
		if (rkb)
			rd_kafka_broker_destroy(rkb);

		rd_kafka_wrunlock(rk);
	}

	rd_free(s_copy);

	return cnt;
}


/**
 * Adds a new broker or updates an existing one.
 *
 */
void rd_kafka_broker_update (rd_kafka_t *rk, rd_kafka_secproto_t proto,
                             const struct rd_kafka_metadata_broker *mdb) {
	rd_kafka_broker_t *rkb;
        char nodename[RD_KAFKA_NODENAME_SIZE];
        int needs_update = 0;

        rd_kafka_mk_nodename(nodename, sizeof(nodename), mdb->host, mdb->port);

	rd_kafka_wrlock(rk);
	if (unlikely(rd_atomic32_get(&rk->rk_terminate))) {
		/* Dont update metadata while terminating, do this
		 * after acquiring lock for proper synchronisation */
		rd_kafka_wrunlock(rk);
		return;
	}

	if ((rkb = rd_kafka_broker_find_by_nodeid(rk, mdb->id))) {
                /* Broker matched by nodeid, see if we need to update
                 * the hostname. */
                if (strcmp(rkb->rkb_nodename, nodename))
                        needs_update = 1;
        } else if ((rkb = rd_kafka_broker_find(rk, proto,
					       mdb->host, mdb->port))) {
                /* Broker matched by hostname (but not by nodeid),
                 * update the nodeid. */
                needs_update = 1;

        } else {
		rd_kafka_broker_add(rk, RD_KAFKA_LEARNED,
				    proto, mdb->host, mdb->port, mdb->id);
	}

	rd_kafka_wrunlock(rk);

        if (rkb) {
                /* Existing broker */
                if (needs_update) {
                        rd_kafka_op_t *rko;

                        rko = rd_kafka_op_new(RD_KAFKA_OP_NODE_UPDATE);
                        rko->rko_nodename = strdup(nodename);
                        rko->rko_nodeid   = mdb->id;
                        rd_kafka_q_enq(&rkb->rkb_ops, rko);
                }
                rd_kafka_broker_destroy(rkb);
        }
}



void rd_kafka_brokers_init (void) {
}








