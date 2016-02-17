/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2015 Magnus Edenhill
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

#include "rdkafka_topic.h"
#include "rdkafka_cgrp.h"
#include "rdkafka_broker.h"

extern const char *rd_kafka_fetch_states[];


/**
 * @brief Offset statistics
 */
struct offset_stats {
        int64_t fetch_offset; /**< Next offset to fetch */
        int64_t eof_offset;   /**< Last offset we reported EOF for */
        int64_t hi_offset;    /**< Current broker hi offset */
};

/**
 * @brief Reset offset_stats struct to default values
 */
static RD_UNUSED void rd_kafka_offset_stats_reset (struct offset_stats *offs) {
        offs->fetch_offset = 0;
        offs->eof_offset = RD_KAFKA_OFFSET_INVALID;
        offs->hi_offset = RD_KAFKA_OFFSET_INVALID;
}



/**
 * Topic + Partition combination
 */
struct rd_kafka_toppar_s { /* rd_kafka_toppar_t */
        int rktp_removed;
	TAILQ_ENTRY(rd_kafka_toppar_s) rktp_rklink;  /* rd_kafka_t link */
	TAILQ_ENTRY(rd_kafka_toppar_s) rktp_rkblink; /* rd_kafka_broker_t link*/
        CIRCLEQ_ENTRY(rd_kafka_toppar_s) rktp_fetchlink; /* rkb_fetch_toppars */
	TAILQ_ENTRY(rd_kafka_toppar_s) rktp_rktlink; /* rd_kafka_itopic_t link*/
        TAILQ_ENTRY(rd_kafka_toppar_s) rktp_cgrplink;/* rd_kafka_cgrp_t link */
        rd_kafka_itopic_t       *rktp_rkt;
        shptr_rd_kafka_itopic_t *rktp_s_rkt;  /* shared pointer for rktp_rkt */
	int32_t            rktp_partition;
        //LOCK: toppar_lock() + topic_wrlock()
        //LOCK: .. in partition_available()
	rd_kafka_broker_t *rktp_leader;      /**< Current leader broker */
        rd_kafka_broker_t *rktp_next_leader; /**< Next leader broker after
                                              *   async migration op. */
	rd_refcnt_t        rktp_refcnt;
	mtx_t              rktp_lock;

        //LOCK: toppar_lock. Should move the lock inside the msgq instead
        //LOCK: toppar_lock. toppar_insert_msg(), concat_msgq()
        //LOCK: toppar_lock. toppar_enq_msg(), deq_msg(), insert_msgq()
	rd_kafka_msgq_t    rktp_msgq;      /* application->rdkafka queue.
					    * protected by rktp_lock */
	rd_kafka_msgq_t    rktp_xmit_msgq; /* internal broker xmit queue */

	rd_ts_t            rktp_ts_last_xmit;

        int                rktp_fetch;     /* On rkb_fetch_toppars list */

	/* Consumer */
	rd_kafka_q_t       rktp_fetchq;          /* Queue of fetched messages
						  * from broker.
                                                  * Broker thread -> App */
        rd_kafka_q_t       rktp_ops;             /* * -> Broker thread */

        rd_atomic32_t      rktp_version;         /* Latest op version.
                                                  * Authoritative (app thread)*/
        int32_t            rktp_fetch_version;   /* Op version of curr fetch.
                                                    (broker thread) */

	enum {
		RD_KAFKA_TOPPAR_FETCH_NONE = 0,
                RD_KAFKA_TOPPAR_FETCH_STOPPING,
                RD_KAFKA_TOPPAR_FETCH_STOPPED,
		RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY,
		RD_KAFKA_TOPPAR_FETCH_OFFSET_WAIT,
		RD_KAFKA_TOPPAR_FETCH_ACTIVE,
	} rktp_fetch_state;           /* Broker thread's state */

#define RD_KAFKA_TOPPAR_FETCH_IS_STARTED(fetch_state) \
        ((fetch_state) >= RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY)


	int64_t            rktp_query_offset;    /* Offset to query broker for*/
	int64_t            rktp_next_offset;     /* Next offset to start
                                                  * fetching from.
                                                  * Locality: toppar thread */
	int64_t            rktp_app_offset;      /* Last offset delivered to
						  * application + 1 */
	int64_t            rktp_stored_offset;   /* Last stored offset, but
						  * maybe not committed yet. */
        int64_t            rktp_committing_offset; /* Offset currently being
                                                    * committed */
	int64_t            rktp_committed_offset; /* Last committed offset */
	rd_ts_t            rktp_ts_committed_offset; /* Timestamp of last
                                                      * commit */

        struct offset_stats rktp_offsets; /* Current offsets.
                                           * Locality: broker thread*/
        struct offset_stats rktp_offsets_fin; /* Finalized offset for stats.
                                               * Updated periodically
                                               * by broker thread.
                                               * Locks: toppar_lock */
        int64_t rktp_lo_offset;              /* Current broker low offset.
                                              * This is outside of the stats
                                              * struct due to this field
                                              * being populated by the
                                              * toppar thread rather than
                                              * the broker thread.
                                              * Locality: toppar thread
                                              * Locks: toppar_lock */

        rd_ts_t            rktp_ts_offset_lag;

	char              *rktp_offset_path;     /* Path to offset file */
	FILE              *rktp_offset_fp;       /* Offset file pointer */
        rd_kafka_cgrp_t   *rktp_cgrp;            /* Belongs to this cgrp */

        int                rktp_assigned;   /* Partition in cgrp assignment */

        rd_kafka_q_t      *rktp_replyq;     /* Current replyq for propagating
                                             * major operations, e.g.,
                                             * FETCH_STOP. */
        //LOCK: toppar_lock().  RD_KAFKA_TOPPAR_F_DESIRED
        //LOCK: toppar_lock().  RD_KAFKA_TOPPAR_F_UNKNOWN
	int                rktp_flags;
#define RD_KAFKA_TOPPAR_F_DESIRED  0x1      /* This partition is desired
					     * by a consumer. */
#define RD_KAFKA_TOPPAR_F_UNKNOWN  0x2      /* Topic is (not yet) seen on
					     * a broker. */
#define RD_KAFKA_TOPPAR_F_OFFSET_STORE 0x4  /* Offset store is active */
#define RD_KAFKA_TOPPAR_F_OFFSET_STORE_STOPPING 0x8 /* Offset store stopping */
#define RD_KAFKA_TOPPAR_F_APP_PAUSE  0x10   /* App pause()d consumption */
#define RD_KAFKA_TOPPAR_F_LIB_PAUSE  0x20   /* librdkafka paused consumption */

        shptr_rd_kafka_toppar_t *rktp_s_for_desp; /* Shared pointer for
                                                   * rkt_desp list */
        shptr_rd_kafka_toppar_t *rktp_s_for_cgrp; /* Shared pointer for
                                                   * rkcg_toppars list */
        shptr_rd_kafka_toppar_t *rktp_s_for_rkb;  /* Shared pointer for
                                                   * rkb_toppars list */

	/*
	 * Timers
	 */
	rd_kafka_timer_t rktp_offset_query_tmr;  /* Offset query timer */
	rd_kafka_timer_t rktp_offset_commit_tmr; /* Offset commit timer */
	rd_kafka_timer_t rktp_offset_sync_tmr;   /* Offset file sync timer */
        rd_kafka_timer_t rktp_consumer_lag_tmr;  /* Consumer lag monitoring
						  * timer */

        int rktp_wait_consumer_lag_resp;         /* Waiting for consumer lag
                                                  * response. */

	struct {
		rd_atomic64_t tx_msgs;
		rd_atomic64_t tx_bytes;
                rd_atomic64_t msgs;
                rd_atomic64_t rx_ver_drops;
	} rktp_c;

};


/**
 * Check if toppar is paused (consumer).
 * Locks: toppar_lock() MUST be held.
 */
#define RD_KAFKA_TOPPAR_IS_PAUSED(rktp)				\
	((rktp)->rktp_flags & (RD_KAFKA_TOPPAR_F_APP_PAUSE |	\
			       RD_KAFKA_TOPPAR_F_LIB_PAUSE))




/* Converts a shptr..toppar_t to a toppar_t */
#define rd_kafka_toppar_s2i(s_rktp) rd_shared_ptr_obj(s_rktp)


/**
 * Returns a shared pointer for the topic.
 */
#define rd_kafka_toppar_keep(rktp)                                      \
        rd_shared_ptr_get(rktp, &(rktp)->rktp_refcnt, shptr_rd_kafka_toppar_t)


/**
 * Frees a shared pointer previously returned by ..toppar_keep()
 */
#define rd_kafka_toppar_destroy(s_rktp)                                 \
        rd_shared_ptr_put(s_rktp,                                       \
                          &rd_kafka_toppar_s2i(s_rktp)->rktp_refcnt,    \
                          rd_kafka_toppar_destroy_final(                \
                                  rd_kafka_toppar_s2i(s_rktp)))




#define rd_kafka_toppar_lock(rktp)     mtx_lock(&(rktp)->rktp_lock)
#define rd_kafka_toppar_unlock(rktp)   mtx_unlock(&(rktp)->rktp_lock)

static const char *rd_kafka_toppar_name (const rd_kafka_toppar_t *rktp)
	RD_UNUSED;
static const char *rd_kafka_toppar_name (const rd_kafka_toppar_t *rktp) {
	static RD_TLS char ret[256];

	rd_snprintf(ret, sizeof(ret), "%.*s [%"PRId32"]",
		    RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		    rktp->rktp_partition);

	return ret;
}
shptr_rd_kafka_toppar_t *rd_kafka_toppar_new (rd_kafka_itopic_t *rkt,
                                              int32_t partition);
void rd_kafka_toppar_destroy_final (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_remove (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_purge_queues (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_set_fetch_state (rd_kafka_toppar_t *rktp,
                                      int fetch_state);
void rd_kafka_toppar_insert_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_enq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_deq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_insert_msgq (rd_kafka_toppar_t *rktp,
				  rd_kafka_msgq_t *rkmq);
void rd_kafka_toppar_concat_msgq (rd_kafka_toppar_t *rktp,
				  rd_kafka_msgq_t *rkmq);
void rd_kafka_toppar_move_msgs (rd_kafka_toppar_t *dst, rd_kafka_toppar_t *src);
void rd_kafka_toppar_enq_error (rd_kafka_toppar_t *rktp,
                                rd_kafka_resp_err_t err);
shptr_rd_kafka_toppar_t *rd_kafka_toppar_get (const rd_kafka_itopic_t *rkt,
                                              int32_t partition,
                                              int ua_on_miss);
shptr_rd_kafka_toppar_t *rd_kafka_toppar_get2 (rd_kafka_t *rk,
                                               const char *topic,
                                               int32_t partition,
                                               int ua_on_miss,
                                               int create_on_miss);
shptr_rd_kafka_toppar_t *
rd_kafka_toppar_get_avail (const rd_kafka_itopic_t *rkt,
                           int32_t partition,
                           int ua_on_miss,
                           rd_kafka_resp_err_t *errp);

shptr_rd_kafka_toppar_t *rd_kafka_toppar_desired_get (rd_kafka_itopic_t *rkt,
                                                      int32_t partition);
shptr_rd_kafka_toppar_t *rd_kafka_toppar_desired_add (rd_kafka_itopic_t *rkt,
                                                      int32_t partition);
void rd_kafka_toppar_desired_link (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_desired_unlink (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_desired_del (rd_kafka_toppar_t *rktp);

int rd_kafka_toppar_ua_move (rd_kafka_itopic_t *rkt, rd_kafka_msgq_t *rkmq);

void rd_kafka_toppar_next_offset_handle (rd_kafka_toppar_t *rktp,
                                         int64_t Offset);

void rd_kafka_toppar_offset_commit (rd_kafka_toppar_t *rktp, int64_t offset,
				    const char *metadata);

void rd_kafka_toppar_broker_delegate (rd_kafka_toppar_t *rktp,
				      rd_kafka_broker_t *rkb);
void rd_kafka_toppar_op (rd_kafka_toppar_t *rktp, rd_kafka_op_type_t type,
                         int64_t offset, rd_kafka_cgrp_t *rkcg,
                         rd_kafka_q_t *replyq);

rd_kafka_resp_err_t rd_kafka_toppar_op_fetch_start (rd_kafka_toppar_t *rktp,
                                                    int64_t offset,
                                                    rd_kafka_q_t *fwdq,
                                                    rd_kafka_q_t *replyq);

rd_kafka_resp_err_t rd_kafka_toppar_op_fetch_stop (rd_kafka_toppar_t *rktp,
                                                   rd_kafka_q_t *replyq);

rd_kafka_resp_err_t rd_kafka_toppar_op_seek (rd_kafka_toppar_t *rktp,
                                             int64_t offset,
                                             rd_kafka_q_t *replyq);

void rd_kafka_toppar_fetch_stopped (rd_kafka_toppar_t *rktp,
                                    rd_kafka_resp_err_t err);

/**
 * Updates the current toppar fetch round-robin next pointer.
 */
static __inline RD_UNUSED
void rd_kafka_broker_fetch_toppar_next (rd_kafka_broker_t *rkb,
                                        rd_kafka_toppar_t *sugg_next) {
        if (CIRCLEQ_EMPTY(&rkb->rkb_fetch_toppars) ||
            (void *)sugg_next == CIRCLEQ_ENDC(&rkb->rkb_fetch_toppars))
                rkb->rkb_fetch_toppar_next = NULL;
        else if (sugg_next)
                rkb->rkb_fetch_toppar_next = sugg_next;
        else
                rkb->rkb_fetch_toppar_next =
                        CIRCLEQ_FIRST(&rkb->rkb_fetch_toppars);
}


void rd_kafka_toppar_fetch_decide (rd_kafka_toppar_t *rktp,
				   rd_kafka_broker_t *rkb,
				   int force_remove);



void rd_kafka_toppar_op_serve (rd_kafka_t *rk, rd_kafka_op_t *rko);
void rd_kafka_broker_consumer_toppar_serve (rd_kafka_broker_t *rkb,
					    rd_kafka_toppar_t *rktp);


void rd_kafka_toppar_offset_fetch (rd_kafka_toppar_t *rktp,
                                   rd_kafka_q_t *replyq);

void rd_kafka_toppar_offset_request (rd_kafka_toppar_t *rktp,
				     int64_t query_offset, int backoff_ms);


rd_kafka_assignor_t *
rd_kafka_assignor_find (rd_kafka_t *rk, const char *protocol);


rd_kafka_broker_t *rd_kafka_toppar_leader (rd_kafka_toppar_t *rktp,
                                           int proper_broker);


rd_kafka_resp_err_t
rd_kafka_toppars_pause_resume (rd_kafka_t *rk, int pause, int flag,
			       rd_kafka_topic_partition_list_t *partitions);


rd_kafka_topic_partition_t *
rd_kafka_topic_partition_list_add0 (rd_kafka_topic_partition_list_t *rktparlist,
                                    const char *topic, int32_t partition,
                                    void *_private);

int rd_kafka_topic_partition_match (rd_kafka_t *rk,
				    const rd_kafka_group_member_t *rkgm,
				    const rd_kafka_topic_partition_t *rktpar,
				    const char *topic, int *matched_by_regex);


void rd_kafka_topic_partition_list_sort_by_topic (
        rd_kafka_topic_partition_list_t *rktparlist);

int rd_kafka_topic_partition_list_set_offsets (
	rd_kafka_t *rk,
        rd_kafka_topic_partition_list_t *rktparlist,
        int from_rktp, int64_t def_value, int is_commit);

shptr_rd_kafka_toppar_t *
rd_kafka_topic_partition_list_get_toppar (
        rd_kafka_t *rk, rd_kafka_topic_partition_list_t *rktparlist, int idx);
