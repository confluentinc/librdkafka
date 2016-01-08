/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012,2013 Magnus Edenhill
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

extern const char *rd_kafka_broker_state_names[];
extern const char *rd_kafka_secproto_names[];

#define RD_KAFKA_NODENAME_SIZE  128

struct rd_kafka_broker_s { /* rd_kafka_broker_t */
	TAILQ_ENTRY(rd_kafka_broker_s) rkb_link;

	int32_t             rkb_nodeid;
#define RD_KAFKA_NODEID_UA -1

	rd_sockaddr_list_t *rkb_rsal;
	time_t              rkb_t_rsal_last;
        const rd_sockaddr_inx_t  *rkb_addr_last; /* Last used connect address */

	rd_kafka_transport_t *rkb_transport;

	uint32_t            rkb_corrid;

	rd_kafka_q_t        rkb_ops;

        /* Toppars handled by this broker */
	TAILQ_HEAD(, rd_kafka_toppar_s) rkb_toppars;
	int                 rkb_toppar_cnt;

        /* Underflowed toppars that are eligible for fetching. */
        CIRCLEQ_HEAD(, rd_kafka_toppar_s) rkb_fetch_toppars;
        int                 rkb_fetch_toppar_cnt;
        rd_kafka_toppar_t  *rkb_fetch_toppar_next;  /* Next 'first' toppar
                                                     * in fetch list.
                                                     * This is used for
                                                     * round-robin. */


        rd_kafka_cgrp_t    *rkb_cgrp;

	rd_ts_t             rkb_ts_fetch_backoff;
	int                 rkb_fetching;

	enum {
		RD_KAFKA_BROKER_STATE_INIT,
		RD_KAFKA_BROKER_STATE_DOWN,
		RD_KAFKA_BROKER_STATE_CONNECT,
		RD_KAFKA_BROKER_STATE_AUTH,
		RD_KAFKA_BROKER_STATE_UP,
                RD_KAFKA_BROKER_STATE_UPDATE,
	} rkb_state;

        rd_ts_t             rkb_ts_state;        /* Timestamp of last
                                                  * state change */
        rd_interval_t       rkb_timeout_scan_intvl;  /* Waitresp timeout scan
                                                      * interval. */

        rd_atomic32_t       rkb_blocking_request_cnt; /* The number of
                                                       * in-flight blocking
                                                       * requests.
                                                       * A blocking request is
                                                       * one that is known to
                                                       * possibly block on the
                                                       * broker for longer than
                                                       * the typical processing
                                                       * time, e.g.:
                                                       * JoinGroup, SyncGroup */

	rd_kafka_confsource_t  rkb_source;
	struct {
		rd_atomic64_t tx_bytes;
		rd_atomic64_t tx;    /* Kafka-messages (not payload msgs) */
		rd_atomic64_t tx_err;
		rd_atomic64_t tx_retries;
		rd_atomic64_t req_timeouts;  /* Accumulated value */

		rd_atomic64_t rx_bytes;
		rd_atomic64_t rx;    /* Kafka messages (not payload msgs) */
		rd_atomic64_t rx_err;
		rd_atomic64_t rx_corrid_err; /* CorrId misses */
		rd_atomic64_t rx_partial;    /* Partial messages received
					 * and dropped. */
	} rkb_c;

        int                 rkb_req_timeouts;  /* Current value */

	rd_ts_t             rkb_ts_metadata_poll; /* Next metadata poll time */
	int                 rkb_metadata_fast_poll_cnt; /* Perform fast
							 * metadata polls. */
	mtx_t               rkb_lock;
	thrd_t              rkb_thread;

	rd_refcnt_t         rkb_refcnt;

        rd_kafka_t         *rkb_rk;

	rd_kafka_buf_t     *rkb_recv_buf;

	rd_kafka_bufq_t     rkb_outbufs;
	rd_atomic32_t       rkb_outbuf_msgcnt;
	rd_kafka_bufq_t     rkb_waitresps;
	rd_kafka_bufq_t     rkb_retrybufs;

	rd_avg_t            rkb_avg_rtt;        /* Current RTT period */
	rd_avg_t            rkb_avg_throttle;   /* Current throttle period */

	char                rkb_name[RD_KAFKA_NODENAME_SIZE];  /* Displ name */
	char                rkb_nodename[RD_KAFKA_NODENAME_SIZE]; /* host:port*/
        uint16_t            rkb_port;                          /* TCP port */
        char               *rkb_origname;                      /* Original
                                                                * host name */


	rd_kafka_secproto_t rkb_proto;

#if WITH_SASL
	rd_kafka_timer_t    rkb_sasl_kinit_refresh_tmr;
#endif


	struct {
		char msg[512];
		int  err;  /* errno */
	} rkb_err;
};

#define rd_kafka_broker_keep(rkb)   rd_refcnt_add(&(rkb)->rkb_refcnt)
#define rd_kafka_broker_lock(rkb)   mtx_lock(&(rkb)->rkb_lock)
#define rd_kafka_broker_unlock(rkb) mtx_unlock(&(rkb)->rkb_lock)



rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid (rd_kafka_t *rk,
						   int32_t nodeid);
rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid0 (rd_kafka_t *rk,
                                                    int32_t nodeid,
                                                    int state);
#define rd_kafka_broker_find_by_nodeid(rk,nodeid) \
        rd_kafka_broker_find_by_nodeid0(rk,nodeid,-1)

/**
 * Filter out brokers that are currently in a blocking request.
 */
static __inline RD_UNUSED int
rd_kafka_broker_filter_non_blocking (rd_kafka_broker_t *rkb, void *opaque) {
        return rd_atomic32_get(&rkb->rkb_blocking_request_cnt) > 0;
}

rd_kafka_broker_t *rd_kafka_broker_any (rd_kafka_t *rk, int state,
                                        int (*filter) (rd_kafka_broker_t *rkb,
                                                       void *opaque),
                                        void *opaque);
rd_kafka_broker_t *rd_kafka_broker_prefer (rd_kafka_t *rk, int32_t broker_id, int state);

int rd_kafka_brokers_add0 (rd_kafka_t *rk, const char *brokerlist);
void rd_kafka_broker_set_state (rd_kafka_broker_t *rkb, int state);

void rd_kafka_broker_fail (rd_kafka_broker_t *rkb,
			   rd_kafka_resp_err_t err,
			   const char *fmt, ...);

void rd_kafka_topic_leader_query0 (rd_kafka_t *rk, rd_kafka_itopic_t *rkt,
				   int do_rk_lock);
#define rd_kafka_topic_leader_query(rk,rkt) \
	rd_kafka_topic_leader_query0(rk,rkt,1)
void rd_kafka_broker_destroy_final (rd_kafka_broker_t *rkb);
#define rd_kafka_broker_destroy(rkb)                                    \
        rd_refcnt_destroywrapper(&(rkb)->rkb_refcnt,                    \
                                 rd_kafka_broker_destroy_final(rkb))

void rd_kafka_broker_update (rd_kafka_t *rk, rd_kafka_secproto_t proto,
                             const struct rd_kafka_metadata_broker *mdb);
rd_kafka_broker_t *rd_kafka_broker_add (rd_kafka_t *rk,
					rd_kafka_confsource_t source,
					rd_kafka_secproto_t proto,
					const char *name, uint16_t port,
					int32_t nodeid);

void rd_kafka_broker_connect_done (rd_kafka_broker_t *rkb, const char *errstr);

int rd_kafka_send (rd_kafka_broker_t *rkb);
int rd_kafka_recv (rd_kafka_broker_t *rkb);

void rd_kafka_dr_msgq (rd_kafka_itopic_t *rkt,
		       rd_kafka_msgq_t *rkmq, rd_kafka_resp_err_t err);

void rd_kafka_broker_buf_enq1 (rd_kafka_broker_t *rkb,
                               int16_t ApiKey,
                               rd_kafka_buf_t *rkbuf,
                               rd_kafka_resp_cb_t *resp_cb,
                               void *opaque);

void rd_kafka_broker_buf_enq_replyq (rd_kafka_broker_t *rkb,
                                     int16_t ApiKey,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_q_t *replyq,
                                     rd_kafka_resp_cb_t *resp_cb,
                                     void *opaque);

void rd_kafka_broker_buf_retry (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf);

void rd_kafka_broker_metadata_req (rd_kafka_broker_t *rkb,
                                   int all_topics,
                                   rd_kafka_itopic_t *only_rkt,
                                   rd_kafka_q_t *replyq,
                                   const char *reason);


rd_kafka_broker_t *rd_kafka_broker_internal (rd_kafka_t *rk);

void msghdr_print (rd_kafka_t *rk,
		   const char *what, const struct msghdr *msg,
		   int hexdump);
