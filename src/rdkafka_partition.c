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
#include "rdkafka_int.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdkafka_request.h"
#include "rdkafka_offset.h"
#include "rdkafka_partition.h"
#include "trex.h"



const char *rd_kafka_fetch_states[] = {
	"none",
        "stopping",
        "stopped",
	"offset-query",
	"offset-wait",
	"active"
};


static __inline void rd_kafka_broker_fetch_toppar_del (rd_kafka_broker_t *rkb,
                                                       rd_kafka_toppar_t *rktp);




/**
 * Request information from broker to keep track of consumer lag.
 *
 * Locality: toppar handle thread
 */
static void rd_kafka_toppar_consumer_lag_req (rd_kafka_toppar_t *rktp) {
	rd_kafka_broker_t *rkb;

	if (!(rkb = rktp->rktp_leader))
		return;

	/* Ask for oldest offset. The newest offset is automatically
         * propagated in FetchResponse.HighwaterMark. */
        rd_kafka_OffsetRequest(rkb, rktp, RD_KAFKA_OFFSET_BEGINNING,
                               &rktp->rktp_ops,
                               rd_kafka_toppar_lag_handle_Offset,
                               rd_kafka_toppar_keep(rktp));
}



/**
 * Request earliest offset to measure consumer lag
 *
 * Locality: toppar handler thread
 */
static void rd_kafka_toppar_consumer_lag_tmr_cb (rd_kafka_timers_t *rkts,
						 void *arg) {
	rd_kafka_toppar_t *rktp = arg;
	rd_kafka_toppar_consumer_lag_req(rktp);
}


/**
 * Add new partition to topic.
 *
 * Locks: rd_kafka_topic_wrlock() must be held.
 * Locks: rd_kafka_wrlock() must be held.
 */
shptr_rd_kafka_toppar_t *rd_kafka_toppar_new (rd_kafka_itopic_t *rkt,
                                              int32_t partition) {
	rd_kafka_toppar_t *rktp;

	rktp = rd_calloc(1, sizeof(*rktp));

	rktp->rktp_partition = partition;
	rktp->rktp_rkt = rkt;
	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_NONE;
	rktp->rktp_offset_fp = NULL;
        rktp->rktp_lo_offset = -1;
        rktp->rktp_hi_offset = -1;
        rktp->rktp_stored_offset = -1;
        rktp->rktp_committed_offset = -1;
        rktp->rktp_eof_offset = -1;
	rd_kafka_msgq_init(&rktp->rktp_msgq);
	rd_kafka_msgq_init(&rktp->rktp_xmit_msgq);
	mtx_init(&rktp->rktp_lock, mtx_plain);

	rd_kafka_q_init(&rktp->rktp_fetchq, rkt->rkt_rk);
        rd_kafka_q_init(&rktp->rktp_ops, rkt->rkt_rk);
        rd_atomic32_set(&rktp->rktp_version, 1);


	/* FIXME: Use a global timer to collect offsets for all partitions */
        if (rktp->rktp_rkt->rkt_rk->rk_conf.stats_interval_ms > 0 &&
            rkt->rkt_rk->rk_type == RD_KAFKA_CONSUMER)
		rd_kafka_timer_start(&rkt->rkt_rk->rk_timers,
				     &rktp->rktp_consumer_lag_tmr,
				     rktp->rktp_rkt->rkt_rk->
				     rk_conf.stats_interval_ms * 1000,
				     rd_kafka_toppar_consumer_lag_tmr_cb,
				     rktp);
				     


        rktp->rktp_s_rkt = rd_kafka_topic_keep(rkt);

	rd_kafka_q_fwd_set(&rktp->rktp_ops, &rkt->rkt_rk->rk_ops);

	TAILQ_INSERT_TAIL(&rkt->rkt_rk->rk_toppars, rktp, rktp_rklink);

	return rd_kafka_toppar_keep(rktp);
}

/**
 * Final destructor for partition.
 */
void rd_kafka_toppar_destroy_final (rd_kafka_toppar_t *rktp) {
	rd_kafka_timer_stop(&rktp->rktp_rkt->rkt_rk->rk_timers,
			    &rktp->rktp_offset_query_tmr, 1/*lock*/);
	rd_kafka_timer_stop(&rktp->rktp_rkt->rkt_rk->rk_timers,
			    &rktp->rktp_consumer_lag_tmr, 1/*lock*/);

	rd_kafka_q_fwd_set(&rktp->rktp_ops, NULL);
	rd_kafka_wrlock(rktp->rktp_rkt->rkt_rk);
	TAILQ_REMOVE(&rktp->rktp_rkt->rkt_rk->rk_toppars, rktp, rktp_rklink);
	rd_kafka_wrunlock(rktp->rktp_rkt->rkt_rk);

	/* Clear queues */
	rd_kafka_dr_msgq(rktp->rktp_rkt, &rktp->rktp_xmit_msgq,
			 RD_KAFKA_RESP_ERR__DESTROY);
	rd_kafka_dr_msgq(rktp->rktp_rkt, &rktp->rktp_msgq,
			 RD_KAFKA_RESP_ERR__DESTROY);
	rd_kafka_q_destroy(&rktp->rktp_fetchq);
        rd_kafka_q_destroy(&rktp->rktp_ops);

        rd_kafka_assert(rktp->rktp_rkt->rkt_rk, !rktp->rktp_cgrp);

        if (rktp->rktp_replyq)
                rd_kafka_q_destroy(rktp->rktp_replyq);

	rd_kafka_topic_destroy0(rktp->rktp_s_rkt);

	mtx_destroy(&rktp->rktp_lock);
	rd_free(rktp);
}


/**
 * Set toppar fetching state.
 *
 * Locality: broker thread
 */
void rd_kafka_toppar_set_fetch_state (rd_kafka_toppar_t *rktp,
                                      int fetch_state) {
	rd_kafka_assert(NULL,
			thrd_is_current(rktp->rktp_rkt->rkt_rk->rk_thread));

        if ((int)rktp->rktp_fetch_state == fetch_state)
                return;

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "PARTSTATE",
                     "Partition %.*s [%"PRId32"] changed fetch state %s -> %s",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
                     rd_kafka_fetch_states[rktp->rktp_fetch_state],
                     rd_kafka_fetch_states[fetch_state]);

        rktp->rktp_fetch_state = fetch_state;
}


/**
 * Returns the appropriate toppar for a given rkt and partition.
 * The returned toppar has increased refcnt and must be unreffed by calling
 *  rd_kafka_toppar_destroy().
 * May return NULL.
 *
 * If 'ua_on_miss' is true the UA (unassigned) toppar is returned if
 * 'partition' was not known locally, else NULL is returned.
 *
 * Locks: Caller must hold rd_kafka_topic_*lock()
 */
shptr_rd_kafka_toppar_t *rd_kafka_toppar_get (const rd_kafka_itopic_t *rkt,
                                              int32_t partition,
                                              int ua_on_miss) {
        shptr_rd_kafka_toppar_t *s_rktp;

	if (partition >= 0 && partition < rkt->rkt_partition_cnt)
		s_rktp = rkt->rkt_p[partition];
	else if (partition == RD_KAFKA_PARTITION_UA || ua_on_miss)
		s_rktp = rkt->rkt_ua;
	else
		return NULL;

	if (s_rktp)
		return rd_kafka_toppar_keep(rd_kafka_toppar_s2i(s_rktp));

	return NULL;
}


/**
 * Same as rd_kafka_toppar_get() but no need for locking and
 * looks up the topic first.
 *
 * Locality: any
 * Locks: none
 */
shptr_rd_kafka_toppar_t *rd_kafka_toppar_get2 (rd_kafka_t *rk,
                                               const char *topic,
                                               int32_t partition,
                                               int ua_on_miss,
                                               int create_on_miss) {
	shptr_rd_kafka_itopic_t *s_rkt;
        rd_kafka_itopic_t *rkt;
        shptr_rd_kafka_toppar_t *s_rktp;

        rd_kafka_wrlock(rk);

        /* Find or create topic */
	if (unlikely(!(s_rkt = rd_kafka_topic_find(rk, topic, 0/*no-lock*/)))) {
                if (!create_on_miss) {
                        rd_kafka_wrunlock(rk);
                        return NULL;
                }
                s_rkt = rd_kafka_topic_new0(rk, topic,
                                            rk->rk_conf.topic_conf ?
                                            rd_kafka_topic_conf_dup(rk->rk_conf.
                                                                    topic_conf)
                                            : NULL, NULL, 0/*no-lock*/);
                if (!s_rkt) {
                        rd_kafka_wrunlock(rk);
                        rd_kafka_log(rk, LOG_ERR, "TOPIC",
                                     "Failed to create local topic \"%s\": %s",
                                     topic, rd_strerror(errno));
                        return NULL;
                }
        }

        rd_kafka_wrunlock(rk);

        rkt = rd_kafka_topic_s2i(s_rkt);

	rd_kafka_topic_wrlock(rkt);
	s_rktp = rd_kafka_toppar_desired_add(rkt, partition);
	rd_kafka_topic_wrunlock(rkt);

        rd_kafka_topic_destroy0(s_rkt);

	return s_rktp;
}


/**
 * Returns a toppar if it is available in the cluster.
 * '*errp' is set to the error-code if lookup fails.
 *
 * Locks: topic_*lock() MUST be held
 */
shptr_rd_kafka_toppar_t *
rd_kafka_toppar_get_avail (const rd_kafka_itopic_t *rkt,
                           int32_t partition, int ua_on_miss,
                           rd_kafka_resp_err_t *errp) {
	shptr_rd_kafka_toppar_t *s_rktp;

        switch (rkt->rkt_state)
        {
        case RD_KAFKA_TOPIC_S_UNKNOWN:
                /* No metadata received from cluster yet.
                 * Put message in UA partition and re-run partitioner when
                 * cluster comes up. */
		partition = RD_KAFKA_PARTITION_UA;
                break;

        case RD_KAFKA_TOPIC_S_NOTEXISTS:
                /* Topic not found in cluster.
                 * Fail message immediately. */
                *errp = RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
                return NULL;

        case RD_KAFKA_TOPIC_S_EXISTS:
                /* Topic exists in cluster. */

                /* Topic exists but has no partitions.
                 * This is usually an transient state following the
                 * auto-creation of a topic. */
                if (unlikely(rkt->rkt_partition_cnt == 0)) {
                        partition = RD_KAFKA_PARTITION_UA;
                        break;
                }

                /* Check that partition exists. */
                if (partition >= rkt->rkt_partition_cnt) {
                        *errp = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
                        return NULL;
                }
                break;

        default:
                rd_kafka_assert(rkt->rkt_rk, !*"NOTREACHED");
                break;
        }

	/* Get new partition */
	s_rktp = rd_kafka_toppar_get(rkt, partition, 0);

	if (unlikely(!s_rktp)) {
		/* Unknown topic or partition */
		if (rkt->rkt_state == RD_KAFKA_TOPIC_S_NOTEXISTS)
			*errp = RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
		else
			*errp = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

		return NULL;
	}

	return s_rktp;
}


/**
 * Looks for partition 'i' in topic 'rkt's desired list.
 *
 * The desired partition list is the list of partitions that are desired
 * (e.g., by the consumer) but not yet seen on a broker.
 * As soon as the partition is seen on a broker the toppar is moved from
 * the desired list and onto the normal rkt_p array.
 * When the partition on the broker goes away a desired partition is put
 * back on the desired list.
 *
 * Locks: rd_kafka_topic_*lock() must be held.
 * Note: 'rktp' refcount is increased.
 */

shptr_rd_kafka_toppar_t *rd_kafka_toppar_desired_get (rd_kafka_itopic_t *rkt,
                                                      int32_t partition) {
	shptr_rd_kafka_toppar_t *s_rktp;
        int i;

	RD_LIST_FOREACH(s_rktp, &rkt->rkt_desp, i) {
                rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);
		if (rktp->rktp_partition == partition)
			return rd_kafka_toppar_keep(rktp);
        }

	return NULL;
}


/**
 * Link toppar on desired list.
 *
 * Locks: rd_kafka_topic_wrlock() and toppar_lock() must be held.
 */
void rd_kafka_toppar_desired_link (rd_kafka_toppar_t *rktp) {
        shptr_rd_kafka_toppar_t *s_rktp = rd_kafka_toppar_keep(rktp);
        rd_list_add(&rktp->rktp_rkt->rkt_desp, s_rktp);
        rktp->rktp_s_for_desp = s_rktp; /* Desired list refcount */
}

/**
 * Unlink toppar from desired list.
 *
 * Locks: rd_kafka_topic_wrlock() and toppar_lock() must be held.
 */
void rd_kafka_toppar_desired_unlink (rd_kafka_toppar_t *rktp) {
        rd_kafka_assert(rktp->rktp_rkt->rkt_rk, rktp->rktp_s_for_desp != NULL);
        rd_list_remove(&rktp->rktp_rkt->rkt_desp, rktp->rktp_s_for_desp);
        rd_kafka_toppar_destroy(rktp->rktp_s_for_desp);
        rktp->rktp_s_for_desp = NULL;
 }


/**
 * Adds 'partition' as a desired partition to topic 'rkt', or updates
 * an existing partition to be desired.
 *
 * Locks: rd_kafka_topic_wrlock() must be held.
 */
shptr_rd_kafka_toppar_t *rd_kafka_toppar_desired_add (rd_kafka_itopic_t *rkt,
                                                      int32_t partition) {
	shptr_rd_kafka_toppar_t *s_rktp;
        rd_kafka_toppar_t *rktp;

	if ((s_rktp = rd_kafka_toppar_get(rkt,
                                          partition, 0/*no_ua_on_miss*/))) {
                rktp = rd_kafka_toppar_s2i(s_rktp);
		rd_kafka_toppar_lock(rktp);
		rd_kafka_dbg(rkt->rkt_rk, TOPIC, "DESP",
			     "Setting topic %s [%"PRId32"] partition "
			     "as desired",
			     rkt->rkt_topic->str, rktp->rktp_partition);
		rktp->rktp_flags |= RD_KAFKA_TOPPAR_F_DESIRED;
		rd_kafka_toppar_unlock(rktp);
		return s_rktp;
	}

	if ((s_rktp = rd_kafka_toppar_desired_get(rkt, partition)))
		return s_rktp;

	s_rktp = rd_kafka_toppar_new(rkt, partition);
        rktp = rd_kafka_toppar_s2i(s_rktp);

	rktp->rktp_flags |= RD_KAFKA_TOPPAR_F_DESIRED |
                RD_KAFKA_TOPPAR_F_UNKNOWN;

	rd_kafka_dbg(rkt->rkt_rk, TOPIC, "DESP",
		     "Adding desired topic %s [%"PRId32"]",
		     rkt->rkt_topic->str, rktp->rktp_partition);

        rd_kafka_toppar_desired_link(rktp);

	return s_rktp; /* Callers refcount */
}




/**
 * Unmarks an 'rktp' as desired.
 *
 * Locks: rd_kafka_topic_wrlock() must be held.
 */
void rd_kafka_toppar_desired_del (rd_kafka_toppar_t *rktp) {
        shptr_rd_kafka_toppar_t *s_rktp;

	rd_kafka_toppar_lock(rktp);
	if (!(rktp->rktp_flags & RD_KAFKA_TOPPAR_F_DESIRED)) {
		rd_kafka_toppar_unlock(rktp);
		return;
	}

        s_rktp = rd_kafka_toppar_keep(rktp);

	rktp->rktp_flags &= ~RD_KAFKA_TOPPAR_F_DESIRED;


	if (rktp->rktp_flags & RD_KAFKA_TOPPAR_F_UNKNOWN) {
		rktp->rktp_flags &= ~RD_KAFKA_TOPPAR_F_UNKNOWN;
                rd_kafka_toppar_desired_unlink(rktp);
        }


        rd_kafka_toppar_unlock(rktp);

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "DESP",
		     "Removing (un)desired topic %s [%"PRId32"]",
		     rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

	rd_kafka_toppar_destroy(s_rktp);
}



/**
 * Move all messages from toppar 'src' to 'dst'.
 * This is used when messages migrate between partitions.
 *
 * NOTE: Both dst and src must be locked.
 */
void rd_kafka_toppar_move_msgs (rd_kafka_toppar_t *dst, rd_kafka_toppar_t *src){
        shptr_rd_kafka_toppar_t *s_rktp;
	s_rktp = rd_kafka_toppar_keep(src);
	rd_kafka_msgq_concat(&dst->rktp_msgq, &src->rktp_msgq);
	rd_kafka_toppar_destroy(s_rktp);
}


/**
 * Insert message at head of 'rktp' message queue.
 * This is typically for non-data flash messages.
 */
void rd_kafka_toppar_insert_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm) {
	rd_kafka_toppar_lock(rktp);
	rd_kafka_msgq_insert(&rktp->rktp_msgq, rkm);
	rd_kafka_toppar_unlock(rktp);
}

/**
 * Append message at tail of 'rktp' message queue.
 */
void rd_kafka_toppar_enq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm) {

	rd_kafka_toppar_lock(rktp);
	rd_kafka_msgq_enq(&rktp->rktp_msgq, rkm);
	rd_kafka_toppar_unlock(rktp);
}


/**
 * Dequeue message from 'rktp' message queue.
 */
void rd_kafka_toppar_deq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm) {
	rd_kafka_toppar_lock(rktp);
	rd_kafka_msgq_deq(&rktp->rktp_msgq, rkm, 1);
	rd_kafka_toppar_unlock(rktp);
}

/**
 * Inserts all messages from 'rkmq' at head of toppar 'rktp's queue.
 * 'rkmq' will be cleared.
 */
void rd_kafka_toppar_insert_msgq (rd_kafka_toppar_t *rktp,
				  rd_kafka_msgq_t *rkmq) {
	rd_kafka_toppar_lock(rktp);
	rd_kafka_msgq_concat(rkmq, &rktp->rktp_msgq);
	rd_kafka_msgq_move(&rktp->rktp_msgq, rkmq);
	rd_kafka_toppar_unlock(rktp);
}


/**
 * Concats all messages from 'rkmq' at tail of toppar 'rktp's queue.
 * 'rkmq' will be cleared.
 */
void rd_kafka_toppar_concat_msgq (rd_kafka_toppar_t *rktp,
				  rd_kafka_msgq_t *rkmq) {
	rd_kafka_toppar_lock(rktp);
	rd_kafka_msgq_concat(&rktp->rktp_msgq, rkmq);
	rd_kafka_toppar_unlock(rktp);
}

/**
 * Move all messages in 'rkmq' to the unassigned partition, if any.
 * Returns 0 on success or -1 if there was no UA partition.
 */
int rd_kafka_toppar_ua_move (rd_kafka_itopic_t *rkt, rd_kafka_msgq_t *rkmq) {
	shptr_rd_kafka_toppar_t *s_rktp_ua;

	rd_kafka_topic_rdlock(rkt);
	s_rktp_ua = rd_kafka_toppar_get(rkt, RD_KAFKA_PARTITION_UA, 0);
	rd_kafka_topic_rdunlock(rkt);

	if (unlikely(s_rktp_ua == NULL))
		return -1;

	rd_kafka_msgq_concat(&rd_kafka_toppar_s2i(s_rktp_ua)->rktp_msgq, rkmq);

	rd_kafka_toppar_destroy(s_rktp_ua);

	return 0;
}


/**
 * Helper method for purging queues when removing a toppar.
 * Locks: none
 */
void rd_kafka_toppar_purge_queues (rd_kafka_toppar_t *rktp) {
        rd_kafka_toppar_lock(rktp);
        rd_kafka_msgq_purge(rktp->rktp_rkt->rkt_rk, &rktp->rktp_msgq);
        rd_kafka_toppar_unlock(rktp);
        rd_kafka_q_disable(&rktp->rktp_fetchq);
        rd_kafka_q_purge(&rktp->rktp_fetchq);
        rd_kafka_q_disable(&rktp->rktp_ops);
        rd_kafka_q_purge(&rktp->rktp_ops);
}



/**
 * Delegates broker 'rkb' as leader for toppar 'rktp'.
 * 'rkb' may be NULL to undelegate leader.
 *
 * Locks: Caller must have rd_kafka_topic_wrlock(rktp->rktp_rkt) 
 *        AND rd_kafka_toppar_lock(rktp) held.
 */
void rd_kafka_toppar_broker_delegate (rd_kafka_toppar_t *rktp,
				      rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rktp->rktp_rkt->rkt_rk;
        shptr_rd_kafka_toppar_t *s_rktp;
        int internal_fallback = 0;

        /* Delegate toppars with no leader to the
         * internal broker for bookkeeping. */
        if (!rkb && !rd_kafka_terminating(rk)) {
                rkb = rd_kafka_broker_internal(rk);
                internal_fallback = 1;
        }

	if (rktp->rktp_leader == rkb) {
                if (internal_fallback)
                        rd_kafka_broker_destroy(rkb);
		return;
        }

	s_rktp = rd_kafka_toppar_keep(rktp);

	if (rktp->rktp_leader) {
		rd_kafka_broker_t *old_rkb = rktp->rktp_leader;

		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "BRKDELGT",
			     "Broker %s no longer leader "
			     "for topic %.*s [%"PRId32"]",
			     rktp->rktp_leader->rkb_name,
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition);

		rd_kafka_broker_toppars_wrlock(old_rkb);
		TAILQ_REMOVE(&old_rkb->rkb_toppars, rktp, rktp_rkblink);
		old_rkb->rkb_toppar_cnt--;
		rktp->rktp_leader = NULL;
		rd_kafka_broker_toppars_wrunlock(old_rkb);
		rd_kafka_toppar_destroy(rktp->rktp_s_for_rkb);
		rd_kafka_broker_destroy(old_rkb);

	}

	if (rkb) {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "BRKDELGT",
			     "Broker %s is now leader for topic %.*s "
			     "[%"PRId32"] with %i messages "
			     "(%"PRIu64" bytes) queued",
			     rkb->rkb_name,
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition,
			     rd_atomic32_get(&rktp->rktp_msgq.rkmq_msg_cnt),
			     rd_atomic64_get(&rktp->rktp_msgq.rkmq_msg_bytes));
		rd_kafka_broker_toppars_wrlock(rkb);
		rktp->rktp_s_for_rkb = rd_kafka_toppar_keep(rktp);
		TAILQ_INSERT_TAIL(&rkb->rkb_toppars, rktp, rktp_rkblink);
		rkb->rkb_toppar_cnt++;
		rktp->rktp_leader = rkb;
                if (!internal_fallback) /* already have refcnt from internal()*/
                        rd_kafka_broker_keep(rkb);
		rd_kafka_broker_toppars_wrunlock(rkb);

	} else {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "BRKDELGT",
			     "No broker is leader for topic %.*s [%"PRId32"]",
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition);
	}

	rd_kafka_toppar_destroy(s_rktp); /* from keep() above */
}





/**
 * Commit toppar's offset on broker.
 * This is an asynch operation, this function simply enqueues an op
 * on the cgrp's queue.
 *
 * Locality: rktp's broker thread
 */
void rd_kafka_toppar_offset_commit (rd_kafka_toppar_t *rktp, int64_t offset,
				    const char *metadata) {
        rd_kafka_topic_partition_list_t *offsets;
        rd_kafka_topic_partition_t *rktpar;

        rd_kafka_assert(rktp->rktp_rkt->rkt_rk, rktp->rktp_cgrp != NULL);
        rd_kafka_assert(rktp->rktp_rkt->rkt_rk,
                        rktp->rktp_flags & RD_KAFKA_TOPPAR_F_OFFSET_STORE);

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, CGRP, "OFFSETCMT",
                     "%.*s [%"PRId32"]: committing offset %"PRId64,
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition, offset);

        offsets = rd_kafka_topic_partition_list_new(1);
        rktpar = rd_kafka_topic_partition_list_add(
                offsets, rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);
        rktpar->offset = offset;
        if (metadata) {
                rktpar->metadata = rd_strdup(metadata);
                rktpar->metadata_size = strlen(metadata);
        }

        rktp->rktp_committing_offset = offset;

        rd_kafka_commit(rktp->rktp_rkt->rkt_rk, offsets, 1/*async*/);

        rd_kafka_topic_partition_list_destroy(offsets);
}














/**
 * Handle the next offset to consume for a toppar.
 * This is used during initial setup when trying to figure out what
 * offset to start consuming from.
 *
 * Locality: toppar handler thread.
 * Locks: toppar_lock(rktp) must be held
 */
void rd_kafka_toppar_next_offset_handle (rd_kafka_toppar_t *rktp,
                                         int64_t Offset) {

        if (RD_KAFKA_OFFSET_IS_LOGICAL(Offset)) {
                /* Offset storage returned logical offset (e.g. "end"),
                 * look it up. */
                rd_kafka_offset_reset(rktp, Offset, RD_KAFKA_RESP_ERR_NO_ERROR,
                                      "update");
                return;
        }

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
        rd_kafka_toppar_set_fetch_state(rktp, RD_KAFKA_TOPPAR_FETCH_ACTIVE);
}



/**
 * Fetch stored offset for a single partition. (simple consumer)
 */
void rd_kafka_toppar_offset_fetch (rd_kafka_toppar_t *rktp,
                                   rd_kafka_q_t *replyq) {
        rd_kafka_t *rk = rktp->rktp_rkt->rkt_rk;
        rd_kafka_topic_partition_list_t *part;
        rd_kafka_op_t *rko;

        rd_kafka_dbg(rk, TOPIC, "OFFSETREQ",
                     "Partition %.*s [%"PRId32"]: querying cgrp for "
                     "stored offset (opv %d)",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition, rktp->rktp_op_version);

        part = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add0(part,
                                           rktp->rktp_rkt->rkt_topic->str,
                                           rktp->rktp_partition,
                                           rd_kafka_toppar_keep(rktp));

        rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_FETCH);
        rko->rko_payload = part;
        rko->rko_replyq = replyq;
        rd_kafka_q_keep(replyq);
        rko->rko_version = rktp->rktp_op_version;
        rd_kafka_q_enq(&rktp->rktp_cgrp->rkcg_ops, rko);
}


/**
 * Send OffsetRequest for toppar.
 *
 * Locality: toppar handler thread
 * Locks: toppar_lock() must be held
 */
void rd_kafka_toppar_offset_request (rd_kafka_toppar_t *rktp,
				     int64_t query_offset) {
	rd_kafka_broker_t *rkb;

	rd_kafka_assert(NULL,
			thrd_is_current(rktp->rktp_rkt->rkt_rk->rk_thread));

	if (!(rkb = rktp->rktp_leader)) {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
			     "%s [%"PRId32"]: no current leader for partition, "
			     "starting offset query timer for offset %s",
			     rktp->rktp_rkt->rkt_topic->str,
			     rktp->rktp_partition,
			     rd_kafka_offset2str(query_offset));
		rd_kafka_timer_start(&rktp->rktp_rkt->rkt_rk->rk_timers,
				     &rktp->rktp_offset_query_tmr,
				     500*1000,
				     rd_kafka_offset_query_tmr_cb, rktp);
		return;
	} else {
		rd_kafka_timer_stop(&rktp->rktp_rkt->rkt_rk->rk_timers,
				    &rktp->rktp_offset_query_tmr, 1/*lock*/);
	}


	if (query_offset == RD_KAFKA_OFFSET_STORED &&
            rktp->rktp_rkt->rkt_conf.offset_store_method ==
            RD_KAFKA_OFFSET_METHOD_BROKER) {
                /*
                 * Get stored offset from broker based storage:
                 * ask cgrp manager for offsets
                 */
                rd_kafka_toppar_offset_fetch(rktp, &rktp->rktp_ops);

	} else {
                shptr_rd_kafka_toppar_t *s_rktp;
                /*
                 * Look up logical offset (end,beginning,tail,..)
                 */

                rd_rkb_dbg(rkb, TOPIC, "OFFREQ",
                           "Partition %.*s [%"PRId32"]: querying for logical "
                           "offset %s (opv %d)",
                           RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                           rktp->rktp_partition,
                           rd_kafka_offset2str(query_offset),
                           rktp->rktp_op_version);

                s_rktp = rd_kafka_toppar_keep(rktp);
                rd_kafka_OffsetRequest(rkb, rktp,
                                       query_offset <=
                                       RD_KAFKA_OFFSET_TAIL_BASE ?
                                       RD_KAFKA_OFFSET_END :
                                       query_offset,
                                       &rktp->rktp_ops,
                                       rd_kafka_toppar_handle_Offset,
                                       s_rktp);
        }

        rd_kafka_toppar_set_fetch_state(rktp,
					RD_KAFKA_TOPPAR_FETCH_OFFSET_WAIT);
}


/**
 * Start fetching toppar.
 *
 * Locality: any
 * Locks: none
 */
static void rd_kafka_toppar_fetch_start (rd_kafka_toppar_t *rktp,
                                         int64_t offset,
                                         int32_t version,
                                         rd_kafka_op_t *rko_orig) {
        rd_kafka_cgrp_t *rkcg;
        rd_kafka_resp_err_t err = 0;

        rkcg = rko_orig->rko_cgrp;

	rd_kafka_toppar_lock(rktp);

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "FETCH",
                     "Start fetch for %.*s [%"PRId32"] in "
                     "state %s at offset %s (v%"PRId32")",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
                     rd_kafka_fetch_states[rktp->rktp_fetch_state],
                     rd_kafka_offset2str(offset), version);

        if (rktp->rktp_fetch_state == RD_KAFKA_TOPPAR_FETCH_STOPPING) {
                err = RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS;
		rd_kafka_toppar_unlock(rktp);
                goto err_reply;
        }

        if (rkcg) {
                rd_kafka_assert(rktp->rktp_rkt->rkt_rk, !rktp->rktp_cgrp);
                /* Attach toppar to cgrp */
                rktp->rktp_cgrp = rkcg;
                rd_kafka_cgrp_op(rkcg, rktp, NULL,
                                 RD_KAFKA_OP_PARTITION_JOIN, 0);
        }


        if (offset == RD_KAFKA_OFFSET_BEGINNING ||
	    offset == RD_KAFKA_OFFSET_END ||
            offset <= RD_KAFKA_OFFSET_TAIL_BASE) {
		rd_kafka_toppar_next_offset_handle(rktp, offset);

	} else if (offset == RD_KAFKA_OFFSET_STORED) {
                rd_kafka_offset_store_init(rktp);

	} else if (offset == RD_KAFKA_OFFSET_ERROR) {
		rd_kafka_offset_reset(rktp, offset,
				      RD_KAFKA_RESP_ERR__NO_OFFSET,
				      "no previously committed offset "
				      "available");

	} else {
		rktp->rktp_next_offset = offset;
                rd_kafka_toppar_set_fetch_state(rktp,
						RD_KAFKA_TOPPAR_FETCH_ACTIVE);
	}

        rktp->rktp_op_version = version;
        rktp->rktp_eof_offset = -1;

	rd_kafka_toppar_unlock(rktp);

        /* Signal back to caller thread that start has commenced, or err */
err_reply:
        if (rko_orig->rko_replyq) {
                rd_kafka_op_t *rko;
                rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH_START);
                rko->rko_err = err;
                rko->rko_version = rko_orig->rko_version;
                rko->rko_rktp = rd_kafka_toppar_keep(rktp);
                rd_kafka_q_enq(rko_orig->rko_replyq, rko);
        }
}




/**
 * Mark toppar's fetch state as stopped (all decommissioning is done,
 * offsets are stored, etc).
 *
 * Locality: toppar handler thread
 * Locks: toppar_lock(rktp)
 */
void rd_kafka_toppar_fetch_stopped (rd_kafka_toppar_t *rktp,
                                    rd_kafka_resp_err_t err) {


        rd_kafka_toppar_set_fetch_state(rktp, RD_KAFKA_TOPPAR_FETCH_STOPPED);

        if (rktp->rktp_cgrp) {
                /* Detach toppar from cgrp */
                rd_kafka_cgrp_op(rktp->rktp_cgrp, rktp, NULL,
                                 RD_KAFKA_OP_PARTITION_LEAVE, 0);
                rktp->rktp_cgrp = NULL;
        }

        /* Signal back to application thread that stop is done. */
	if (rktp->rktp_replyq) {
		rd_kafka_op_t *rko;
		rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH_STOP);
                rko->rko_err = err;
		rko->rko_rktp = rd_kafka_toppar_keep(rktp);
		rd_kafka_q_enq(rktp->rktp_replyq, rko);
                rd_kafka_q_destroy(rktp->rktp_replyq);
                rktp->rktp_replyq = NULL;
	}
}


/**
 * Stop toppar fetcher.
 * This is usually an async operation.
 *
 * Locality: broker thread
 */
static void rd_kafka_toppar_fetch_stop (rd_kafka_toppar_t *rktp,
                                        rd_kafka_op_t *rko_orig) {

	rd_kafka_toppar_lock(rktp);

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "FETCH",
                     "Stopping fetch for %.*s [%"PRId32"] in state %s",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
                     rd_kafka_fetch_states[rktp->rktp_fetch_state]);

        rktp->rktp_op_version = rko_orig->rko_version;

        /* Clear out the forwarding queue. */
        // FIXME: purge fwdq by version first? */
        rd_kafka_q_fwd_set(&rktp->rktp_fetchq, NULL);

        /* Assign the future replyq to propagate stop results. */
        rd_kafka_assert(rktp->rktp_rkt->rkt_rk, rktp->rktp_replyq == NULL);
        rktp->rktp_replyq = rko_orig->rko_replyq;
        rko_orig->rko_replyq = NULL;
        rd_kafka_toppar_set_fetch_state(rktp, RD_KAFKA_TOPPAR_FETCH_STOPPING);

        /* Stop offset store (possibly async).
         * NOTE: will call .._stopped() if store finishes immediately,
         *       so no more operations after this call! */
        rd_kafka_offset_store_stop(rktp);

	rd_kafka_toppar_unlock(rktp);
}


/**
 * Update a toppars offset.
 * The toppar must have been previously FETCH_START:ed
 */
static void rd_kafka_toppar_seek (rd_kafka_toppar_t *rktp,
                                  int64_t offset, int32_t version,
                                  rd_kafka_op_t *rko_orig) {
        rd_kafka_resp_err_t err = 0;

	rd_kafka_toppar_lock(rktp);

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "FETCH",
                     "Seek %.*s [%"PRId32"] to offset %s "
                     "in state %s (v%"PRId32")",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition,
		     rd_kafka_offset2str(offset),
                     rd_kafka_fetch_states[rktp->rktp_fetch_state], version);


        if (rktp->rktp_fetch_state == RD_KAFKA_TOPPAR_FETCH_STOPPING) {
                err = RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS;
                goto err_reply;
        } else if (!RD_KAFKA_TOPPAR_FETCH_IS_STARTED(rktp->rktp_fetch_state)) {
                err = RD_KAFKA_RESP_ERR__STATE;
                goto err_reply;
        }

        if (offset == RD_KAFKA_OFFSET_BEGINNING ||
	    offset == RD_KAFKA_OFFSET_END ||
            offset <= RD_KAFKA_OFFSET_TAIL_BASE) {
		rd_kafka_toppar_next_offset_handle(rktp, offset);

	} else if (offset == RD_KAFKA_OFFSET_STORED) {
                err = RD_KAFKA_RESP_ERR__INVALID_ARG;

	} else {
		rktp->rktp_next_offset = offset;
                rd_kafka_toppar_set_fetch_state(rktp,
						RD_KAFKA_TOPPAR_FETCH_ACTIVE);
	}

        rktp->rktp_op_version = version;
        rktp->rktp_eof_offset = -1;

        /* Signal back to caller thread that seek has commenced, or err */
err_reply:
	rd_kafka_toppar_unlock(rktp);

        if (rko_orig->rko_replyq) {
                rd_kafka_op_t *rko;
                rko = rd_kafka_op_new(RD_KAFKA_OP_SEEK);
                rko->rko_err = err;
                rko->rko_version = rko_orig->rko_version;
                rko->rko_rktp = rd_kafka_toppar_keep(rktp);
                rd_kafka_q_enq(rko_orig->rko_replyq, rko);
        }
}




/**
 * Add toppar to fetch list.
 *
 * Locality: broker thread
 * Locks: broker_toppars_wrlock()
 */
static __inline void rd_kafka_broker_fetch_toppar_add (rd_kafka_broker_t *rkb,
                                                       rd_kafka_toppar_t *rktp){
        if (rktp->rktp_fetch)
                return; /* Already added */

        CIRCLEQ_INSERT_TAIL(&rkb->rkb_fetch_toppars, rktp, rktp_fetchlink);
        rkb->rkb_fetch_toppar_cnt++;
        rktp->rktp_fetch = 1;

        if (unlikely(rkb->rkb_fetch_toppar_cnt == 1))
                rd_kafka_broker_fetch_toppar_next(rkb, rktp);

        rd_rkb_dbg(rkb, TOPIC, "FETCHADD",
                   "Added %.*s [%"PRId32"] to fetch list (%d entries)",
                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                   rktp->rktp_partition,
                   rkb->rkb_fetch_toppar_cnt);
}


/**
 * Remove toppar from fetch list.
 *
 * Locality: broker thread
 * Locks: broker_toppars_wrlock()
 */
static __inline void rd_kafka_broker_fetch_toppar_del (rd_kafka_broker_t *rkb,
                                                       rd_kafka_toppar_t *rktp){
        if (!rktp->rktp_fetch)
                return; /* Not added */

        CIRCLEQ_REMOVE(&rkb->rkb_fetch_toppars, rktp, rktp_fetchlink);
        rkb->rkb_fetch_toppar_cnt--;
        rktp->rktp_fetch = 0;

        if (rkb->rkb_fetch_toppar_next == rktp) {
                /* Update next pointer */
                rd_kafka_broker_fetch_toppar_next(
			rkb, CIRCLEQ_LOOP_NEXT(&rkb->rkb_fetch_toppars,
					       rktp, rktp_fetchlink));
        }

        rd_rkb_dbg(rkb, TOPIC, "FETCHADD",
                   "Removed %.*s [%"PRId32"] from fetch list (%d entries)",
                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                   rktp->rktp_partition,
                   rkb->rkb_fetch_toppar_cnt);

}



/**
 * Decide whether this toppar should be on the fetch list or not.
 *
 * Locality: broker thread
 */
void rd_kafka_toppar_fetch_decide (rd_kafka_toppar_t *rktp,
				   rd_kafka_broker_t *rkb) {
        int should_fetch = 1;
        const char *reason = "";

	rd_kafka_toppar_lock(rktp);

	/* Skip toppars not in active fetch state */
	if (rktp->rktp_fetch_state != RD_KAFKA_TOPPAR_FETCH_ACTIVE) {
                reason = "not in active fetch state";
		should_fetch = 0;
		rd_kafka_toppar_unlock(rktp);
		goto done;
	}

	rd_kafka_toppar_unlock(rktp);

        if (RD_KAFKA_OFFSET_IS_LOGICAL(rktp->rktp_next_offset)) {
                should_fetch = 0;
                reason = "no concrete offset";

        } else if (rd_kafka_q_len(&rktp->rktp_fetchq) >=
		   rkb->rkb_rk->rk_conf.queued_min_msgs) {
		/* Skip toppars who's local message queue is already above
		 * the lower threshold. */
                reason = "queued.min.messages exceeded";
                should_fetch = 0;

        } else if ((int64_t)rd_kafka_q_size(&rktp->rktp_fetchq) >=
            rkb->rkb_rk->rk_conf.queued_max_msg_bytes) {
                reason = "queued.max.messages.kbytes exceeded";
                should_fetch = 0;
        }

 done:
        if (rktp->rktp_fetch != should_fetch) {
                rd_rkb_dbg(rkb, FETCH, "FETCH",
                           "Topic %s [%"PRId32"] at offset %s "
                           "(%d/%d msgs, %"PRId64"/%d kb queued) "
                           "is %sfetchable: %s",
                           rktp->rktp_rkt->rkt_topic->str,
                           rktp->rktp_partition,
                           rd_kafka_offset2str(rktp->rktp_next_offset),
                           rd_kafka_q_len(&rktp->rktp_fetchq),
                           rkb->rkb_rk->rk_conf.queued_min_msgs,
                           rd_kafka_q_size(&rktp->rktp_fetchq) / 1024,
                           rkb->rkb_rk->rk_conf.queued_max_msg_kbytes,
                           should_fetch ? "" : "not ", reason);

                if (should_fetch)
                        rd_kafka_broker_fetch_toppar_add(rkb, rktp);
                else
                        rd_kafka_broker_fetch_toppar_del(rkb, rktp);
        }
}


/**
 * Serve a toppar in a consumer broker thread.
 * This is considered the fast path and should be minimal, mostly focusing
 * on fetch related mechanisms.
 *
 * Locality: broker thread
 * Locks: toppars_wrlock(rkb)
 */
void rd_kafka_broker_consumer_toppar_serve (rd_kafka_broker_t *rkb,
					    rd_kafka_toppar_t *rktp) {
	rd_kafka_toppar_fetch_decide(rktp, rkb);
}



/**
 * Serve a toppar op
 * 'rktp' may be NULL for certain ops (OP_RECV_BUF)
 *
 * Locality: toppar handler thread
 */
void rd_kafka_toppar_op_serve (rd_kafka_t *rk, rd_kafka_op_t *rko) {
	rd_kafka_toppar_t *rktp = NULL;
	int outdated = 0;

	if (rko->rko_rktp)
		rktp = rd_kafka_toppar_s2i(rko->rko_rktp);

	if (rktp) {
		outdated = rko->rko_version != 0 &&
			rko->rko_version < rktp->rktp_op_version;

		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OP",
			     "%.*s [%"PRId32"] received %sop %s "
			     "(v%"PRId32") in fetch-state %s",
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition,
			     outdated ? "outdated ": "",
			     rd_kafka_op2str(rko->rko_type),
			     rko->rko_version,
			     // FIXME: lock
			     rd_kafka_fetch_states[rktp->rktp_fetch_state]);
	}

	switch ((int)rko->rko_type)
	{
        case RD_KAFKA_OP_CALLBACK:
                rd_kafka_op_call(rk, rko);
                break;

	case RD_KAFKA_OP_FETCH_START:
		rd_kafka_toppar_fetch_start(rktp, rko->rko_offset,
					    rko->rko_version,
					    rko);
		break;

	case RD_KAFKA_OP_FETCH_STOP:
		rd_kafka_toppar_fetch_stop(rktp, rko);
		break;

	case RD_KAFKA_OP_SEEK:
		rd_kafka_toppar_seek(rktp, rko->rko_offset,
				     rko->rko_version, rko);
		break;

	case RD_KAFKA_OP_OFFSET_FETCH | RD_KAFKA_OP_REPLY:
        {
                /* OffsetFetch reply */
                rd_kafka_topic_partition_list_t *offsets = rko->rko_payload;
                shptr_rd_kafka_toppar_t *s_rktp;

                s_rktp = offsets->elems[0]._private;
                rko->rko_offset = offsets->elems[0].offset;
                rko->rko_err = offsets->elems[0].err;
                offsets->elems[0]._private = NULL;
                rd_kafka_topic_partition_list_destroy(offsets);
		rko->rko_payload = NULL;
                rktp = rd_kafka_toppar_s2i(s_rktp);

                if (!outdated)
                        rd_kafka_timer_stop(&rktp->rktp_rkt->rkt_rk->rk_timers,
                                            &rktp->rktp_offset_query_tmr,
                                            1/*lock*/);


		rd_kafka_toppar_lock(rktp);

		if (rko->rko_err) {
			rd_kafka_dbg(rktp->rktp_rkt->rkt_rk,
				     TOPIC, "OFFSET",
				     "Failed to fetch offset for "
				     "%.*s [%"PRId32"]: %s",
				     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
				     rktp->rktp_partition,
				     rd_kafka_err2str(rko->rko_err));

			/* Keep on querying until we succeed. */
			rd_kafka_assert(NULL, !*"FIXME");
			rd_kafka_toppar_set_fetch_state(rktp, RD_KAFKA_TOPPAR_FETCH_OFFSET_QUERY);

			rd_kafka_toppar_unlock(rktp);

			rd_kafka_timer_start(&rktp->rktp_rkt->rkt_rk->rk_timers,
					     &rktp->rktp_offset_query_tmr,
					     500*1000,
					     rd_kafka_offset_query_tmr_cb,
					     rktp);

			/* Propagate error to application */
			if (rko->rko_err != RD_KAFKA_RESP_ERR__WAIT_COORD) {
				rd_kafka_op_app_fmt(&rktp->rktp_fetchq,
						    RD_KAFKA_OP_ERR,
						    rktp,
						    rko->rko_err,
						    "Failed to fetch "
						    "offsets from brokers: "
						    "%s",
						    rd_kafka_err2str(rko->rko_err));
			}

			break;
		}

		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk,
			     TOPIC, "OFFSET",
			     "%.*s [%"PRId32"]: OffsetFetch returned "
			     "offset %s (%"PRId64")",
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition,
			     rd_kafka_offset2str(rko->rko_offset),
			     rko->rko_offset);

		if (rko->rko_offset > 0)
			rktp->rktp_committed_offset = rko->rko_offset;

		if (rko->rko_offset >= 0)
			rd_kafka_toppar_next_offset_handle(rktp,
							   rko->rko_offset);
		else
			rd_kafka_offset_reset(rktp, rko->rko_offset,
					      RD_KAFKA_RESP_ERR__NO_OFFSET,
					      "no previously committed offset "
					      "available");
		rd_kafka_toppar_unlock(rktp);

                rd_kafka_toppar_destroy(s_rktp);
        }
        break;

	case RD_KAFKA_OP_OFFSET_COMMIT:
		/* This is the result of an offset commit.
		 * The rko is the original rko, so forward the reply
		 * to a replyq if it is set. */
		printf("op: %p Recved offset commit with err %d and replyq %p\n",
		       rko, rko->rko_err, rko->rko_replyq);
		printf("my q is %p and %p and %p\n", &rktp->rktp_ops,
		       &rktp->rktp_rkt->rkt_rk->rk_ops, &rktp->rktp_fetchq);
		if (rko->rko_err) {
			rd_kafka_q_op_err(rko->rko_replyq ?
					  rko->rko_replyq : &rktp->rktp_fetchq,
					  rko->rko_err, rko->rko_version,
					  "Commit of offset %"PRId64
					  " failed: %s",
					  rko->rko_err,
					  rd_kafka_err2str(rko->
							   rko_err));
			break;
		}


		rd_kafka_toppar_lock(rktp);
		rktp->rktp_committed_offset = rko->rko_offset;

		/* When stopping toppars:
		 * Final commit is now done (or failed), propagate. */
		if (rktp->rktp_fetch_state == RD_KAFKA_TOPPAR_FETCH_STOPPING)
			rd_kafka_toppar_fetch_stopped(rktp, rko->rko_err);

		rd_kafka_toppar_unlock(rktp);

		/* Send reply to replyq. We must make a copy here. */
		if (rko->rko_replyq) {
			rd_kafka_op_t *rko2 = rd_kafka_op_new(rko->rko_type);
			rko2->rko_err = rko->rko_err;
			rko2->rko_offset = rko->rko_offset;
			rko2->rko_rktp = rko->rko_rktp;
			rko->rko_rktp = NULL;
			rko2->rko_rkmessage.partition = rko->rko_rkmessage.partition;
			rko2->rko_rkt = rko->rko_rkt;
			rko->rko_rkt = NULL;
			rd_kafka_q_enq(rko->rko_replyq, rko2);
		}
		break;

	case RD_KAFKA_OP_RECV_BUF:
		/* Handle response */
		rd_kafka_buf_handle_op(rko);
		break;

	default:
		rd_kafka_assert(NULL, !*"unknown type");
		break;
	}


}





/**
 * Send command op to toppar (handled by toppar's current broker thread).
 * If 'new_barrier' is true a new barrier will be inserted.
 *
 * Ownership of the shared pointer 's_rktp' will be delegated to the op.
 *
 * Returns the barrier version used.
 *
 * Locality: any thread
 */
int32_t rd_kafka_toppar_op (rd_kafka_toppar_t *rktp,
                            rd_kafka_op_type_t type,
                            int new_barrier,
                            int64_t offset, rd_kafka_cgrp_t *rkcg,
                            rd_kafka_q_t *replyq) {
        rd_kafka_op_t *rko;
        int32_t version;

        rko = rd_kafka_op_new(type);
        if (rkcg)
                rko->rko_cgrp = rkcg;

        rko->rko_offset = offset;
        if (new_barrier)
                rko->rko_version = rd_atomic32_add(&rktp->rktp_version, 1);
        else
                rko->rko_version = rd_atomic32_get(&rktp->rktp_version);
        version = rko->rko_version;

        rko->rko_rktp = rd_kafka_toppar_keep(rktp);
        if (replyq) {
                rko->rko_replyq = replyq;
                rd_kafka_q_keep(replyq);
        }

        rd_kafka_q_enq(&rktp->rktp_ops, rko);

        return version;
}



/**
 * Start consuming partition (async operation).
 *  'offset' is the initial offset
 *  'fwdq' is an optional queue to forward messages to, if this is NULL
 *  then messages will be enqueued on rktp_fetchq.
 *  'replyq' is an optional queue for handling the consume_start ack.
 *  'versionp' is an optional pointer to hold the updated barrier version
 *  for the rktp.
 *
 * This is the thread-safe interface that can be called from any thread.
 *
 * Returns the bumped barrier version in 'versionp' if non-NULL.
 */
rd_kafka_resp_err_t rd_kafka_toppar_op_fetch_start (rd_kafka_toppar_t *rktp,
                                                    int64_t offset,
                                                    rd_kafka_q_t *fwdq,
                                                    rd_kafka_q_t *replyq,
                                                    int32_t *versionp) {
        int32_t version;

        if (fwdq)
                rd_kafka_q_fwd_set(&rktp->rktp_fetchq, fwdq);

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "CONSUMER",
		     "Start consuming %.*s [%"PRId32"] at "
		     "offset %s",
		     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		     rktp->rktp_partition, rd_kafka_offset2str(offset));

        version = rd_kafka_toppar_op(rktp, RD_KAFKA_OP_FETCH_START,
                                     1/*new barrier*/,  offset,
                                     rktp->rktp_rkt->rkt_rk->rk_cgrp,
                                     replyq);

        if (versionp)
                *versionp = version;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Stop consuming partition (async operatoin)
 * This is thread-safe interface that can be called from any thread.
 * Returns the bumped barrier version in 'versionp' if non-NULL.
 *
 * Locality: any thread
 */
rd_kafka_resp_err_t rd_kafka_toppar_op_fetch_stop (rd_kafka_toppar_t *rktp,
                                                   rd_kafka_q_t *replyq,
                                                   int32_t *versionp) {
        int32_t version;

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "CONSUMER",
		     "Stop consuming %.*s [%"PRId32"]",
		     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		     rktp->rktp_partition);

        version = rd_kafka_toppar_op(rktp, RD_KAFKA_OP_FETCH_STOP,
                                     1/*new barrier*/, 0,
                                     rktp->rktp_rkt->rkt_rk->rk_cgrp,
                                     replyq);

	/* Flush the fetchq */
	rd_kafka_q_purge_toppar_version(&rktp->rktp_fetchq, rktp, version);

        if (versionp)
                *versionp = version;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Set/Seek offset of a consumed partition (async operation).
 *  'offset' is the target offset
 *  'replyq' is an optional queue for handling the ack.
 *  'versionp' is an optional pointer to hold the updated barrier version
 *  for the rktp.
 *
 * This is the thread-safe interface that can be called from any thread.
 *
 * Returns the bumped barrier version in 'versionp' if non-NULL.
 */
rd_kafka_resp_err_t rd_kafka_toppar_op_seek (rd_kafka_toppar_t *rktp,
                                             int64_t offset,
                                             rd_kafka_q_t *replyq,
                                             int32_t *versionp) {
        int32_t version;

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "CONSUMER",
		     "Seek %.*s [%"PRId32"] to "
		     "offset %s",
		     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
		     rktp->rktp_partition, rd_kafka_offset2str(offset));

        version = rd_kafka_toppar_op(rktp, RD_KAFKA_OP_SEEK,
                                     1/*new barrier*/,  offset,
                                     rktp->rktp_rkt->rkt_rk->rk_cgrp,
                                     replyq);

        if (versionp)
                *versionp = version;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}




/**
 * Propagate error for toppar
 */
void rd_kafka_toppar_enq_error (rd_kafka_toppar_t *rktp,
                                rd_kafka_resp_err_t err) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(RD_KAFKA_OP_ERR);
        rko->rko_err                 = err;
        rko->rko_rkmessage.rkt = rd_kafka_topic_keep_a(rktp->rktp_rkt);
        rko->rko_rkmessage.partition = rktp->rktp_partition;
        rko->rko_payload             = rd_strdup(rd_kafka_err2str(rko->rko_err));
        rko->rko_len                 = strlen(rko->rko_payload);
        rko->rko_flags              |= RD_KAFKA_OP_F_FREE;

        rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
}


rd_kafka_topic_partition_t *rd_kafka_topic_partition_new (const char *topic,
                                                          int32_t partition) {
        rd_kafka_topic_partition_t *rktpar = rd_calloc(1, sizeof(*rktpar));
        return rktpar;
}

void rd_kafka_topic_partition_destroy (rd_kafka_topic_partition_t *rktpar) {
        rd_free(rktpar);
}


const char *
rd_kafka_topic_partition_topic (const rd_kafka_topic_partition_t *rktpar) {
        const rd_kafka_toppar_t *rktp = (const rd_kafka_toppar_t *)rktpar;
        return rktp->rktp_rkt->rkt_topic->str;
}

int32_t
rd_kafka_topic_partition_partition (const rd_kafka_topic_partition_t *rktpar) {
        const rd_kafka_toppar_t *rktp = (const rd_kafka_toppar_t *)rktpar;
        return rktp->rktp_partition;
}

void rd_kafka_topic_partition_get (const rd_kafka_topic_partition_t *rktpar,
                                   const char **name, int32_t *partition) {
        const rd_kafka_toppar_t *rktp = (const rd_kafka_toppar_t *)rktpar;
        *name = rktp->rktp_rkt->rkt_topic->str;
        *partition = rktp->rktp_partition;
}




/**
 *
 * rd_kafka_topic_partition_t lists
 * Fixed-size non-growable list of partitions for propagation to application.
 *
 */


static void
rd_kafka_topic_partition_list_grow (rd_kafka_topic_partition_list_t *rktparlist,
                                    int add_size) {
        if (add_size < rktparlist->size)
                add_size = RD_MAX(rktparlist->size, 32);

        rktparlist->size += add_size;
        rktparlist->elems = rd_realloc(rktparlist->elems,
                                       sizeof(*rktparlist->elems) *
                                       rktparlist->size);

}
/**
 * Create a list for fitting 'size' topic_partitions (rktp).
 */
rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list_new (int size) {
        rd_kafka_topic_partition_list_t *rktparlist;

        rktparlist = rd_calloc(1, sizeof(*rktparlist));

        rktparlist->size = size;
        rktparlist->cnt = 0;

        if (size > 0)
                rd_kafka_topic_partition_list_grow(rktparlist, size);

        return rktparlist;
}


/**
 * Destroys a list previously created with .._list_new() and drops
 * any references to contained toppars.
 */
void
rd_kafka_topic_partition_list_destroy (rd_kafka_topic_partition_list_t *rktparlist) {
        int i;

        for (i = 0 ; i < rktparlist->cnt ; i++) {
                if (rktparlist->elems[i].topic)
                        rd_free(rktparlist->elems[i].topic);
                if (rktparlist->elems[i].metadata)
                        rd_free(rktparlist->elems[i].metadata);
                if (rktparlist->elems[i]._private)
                        rd_kafka_toppar_destroy((shptr_rd_kafka_toppar_t *)
                                                rktparlist->elems[i]._private);
        }

        if (rktparlist->elems)
                rd_free(rktparlist->elems);

        rd_free(rktparlist);
}


/**
 * Add a partition to an rktpar list.
 * The list must have enough room to fit it.
 *
 * '_private' must be NULL or a valid 'shptr_rd_kafka_toppar_t *'.
 *
 * Returns a pointer to the added element.
 */
rd_kafka_topic_partition_t *
rd_kafka_topic_partition_list_add0 (rd_kafka_topic_partition_list_t *rktparlist,
                                    const char *topic, int32_t partition,
                                    void *_private) {
        rd_kafka_topic_partition_t *rktpar;
        if (rktparlist->cnt == rktparlist->size)
                rd_kafka_topic_partition_list_grow(rktparlist, 1);
        rd_kafka_assert(NULL, rktparlist->cnt < rktparlist->size);

        rktpar = &rktparlist->elems[rktparlist->cnt++];
        memset(rktpar, 0, sizeof(*rktpar));
        rktpar->topic = rd_strdup(topic);
        rktpar->partition = partition;
        rktpar->_private = _private;

        return rktpar;
}


rd_kafka_topic_partition_t *
rd_kafka_topic_partition_list_add (rd_kafka_topic_partition_list_t *rktparlist,
                                   const char *topic, int32_t partition) {
        return rd_kafka_topic_partition_list_add0(rktparlist,
                                                  topic, partition, NULL);
}


/**
 * Adds a consecutive list of partitions to a list
 */
void
rd_kafka_topic_partition_list_add_range (rd_kafka_topic_partition_list_t
                                         *rktparlist,
                                         const char *topic,
                                         int32_t start, int32_t stop) {

        for (; start <= stop ; start++)
                rd_kafka_topic_partition_list_add(rktparlist, topic, start);
}


/**
 * Create and return a copy of list 'src'
 */
rd_kafka_topic_partition_list_t *
rd_kafka_topic_partition_list_copy (const rd_kafka_topic_partition_list_t *src){
        rd_kafka_topic_partition_list_t *dst;
        int i;

        dst = rd_kafka_topic_partition_list_new(src->size);

        for (i = 0 ; i < src->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar;
                rktpar = rd_kafka_topic_partition_list_add0(
                        dst,
                        src->elems[i].topic,
                        src->elems[i].partition,
                        src->elems[i]._private ?
                        rd_kafka_toppar_keep(
                                rd_kafka_toppar_s2i((shptr_rd_kafka_toppar_t *)
                                                    src->elems[i]._private)) :
                        NULL);
                rktpar->offset = src->elems[i].offset;
                rktpar->opaque = src->elems[i].opaque;
                rktpar->err = src->elems[i].err;
                if (src->elems[i].metadata_size > 0) {
                        rktpar->metadata =
                                rd_malloc(src->elems[i].metadata_size);
                        rktpar->metadata_size = src->elems[i].metadata_size;
                        memcpy((void *)rktpar->metadata, src->elems[i].metadata,
                               src->elems[i].metadata_size);
                }
        }
        return dst;
}


static int rd_kafka_topic_partition_cmp (const void *_a, const void *_b) {
        const rd_kafka_topic_partition_t *a = _a;
        const rd_kafka_topic_partition_t *b = _b;
        int r = strcmp(a->topic, b->topic);
        if (r)
                return r;
        else
                return a->partition - b->partition;
}


/**
 * Search 'rktparlist' for 'topic' and 'partition'.
 * '*start_idx' is currently ignored but will be used for divide and conquer.
 */
rd_kafka_topic_partition_t *
rd_kafka_topic_partition_list_find (rd_kafka_topic_partition_list_t *rktparlist,
                                    const char *topic, int32_t partition,
                                    int *start_idx) {
        rd_kafka_topic_partition_t skel;
        int i;

        skel.topic = (char *)topic;
        skel.partition = partition;

        for (i = 0 ; i < rktparlist->cnt ; i++) {
                if (!rd_kafka_topic_partition_cmp(&skel,
                                                  &rktparlist->elems[i]))
                        return &rktparlist->elems[i];
        }

        return NULL;
}



/**
 * Returns true if 'topic' matches the 'rktpar', else false.
 * On match, if rktpar is a regex pattern then 'matched_by_regex' is set to 1.
 */
int rd_kafka_topic_partition_match (rd_kafka_t *rk,
				    const rd_kafka_group_member_t *rkgm,
				    const rd_kafka_topic_partition_t *rktpar,
				    const char *topic, int *matched_by_regex) {
	int ret = 0;

	if (*rktpar->topic == '^') {
		TRex *re;
		const char *error;

		/* FIXME: cache compiled regex */
		if (!(re = trex_compile(rktpar->topic, &error))) {
			rd_kafka_dbg(rk, CGRP,
				     "SUBMATCH",
				     "Invalid regex for member "
				     "\"%.*s\" subscription \"%s\": %s",
				     RD_KAFKAP_STR_PR(rkgm->rkgm_member_id),
				     rktpar->topic, error);
			return 0;
		}

		if (trex_match(re, topic)) {
			if (matched_by_regex)
				*matched_by_regex = 1;

			ret = 1;
		}

		trex_free(re);

	} else if (!strcmp(rktpar->topic, topic)) {

		if (matched_by_regex)
			*matched_by_regex = 0;

		ret = 1;
	}

	return ret;
}



void rd_kafka_topic_partition_list_sort_by_topic (
        rd_kafka_topic_partition_list_t *rktparlist) {

        qsort(rktparlist->elems, rktparlist->cnt, sizeof(*rktparlist->elems),
              rd_kafka_topic_partition_cmp);
}



/**
 * Set offset values in partition list based on toppar's last stored offset.
 *
 *  from_rktp - true: set rktp's last stored offset, false: set def_value
 *  is_commit: indicates that set offset is to be committed (for debug log)
 *
 * Returns the number of valid non-logical offsets (>=0).
 */
int rd_kafka_topic_partition_list_set_offsets (
	rd_kafka_t *rk,
        rd_kafka_topic_partition_list_t *rktparlist,
        int from_rktp, int64_t def_value, int is_commit) {
        int i;
	int valid_cnt = 0;

        for (i = 0 ; i < rktparlist->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &rktparlist->elems[i];

                if (from_rktp) {
                        shptr_rd_kafka_toppar_t *s_rktp = rktpar->_private;
                        rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);
                        rktpar->offset = rktp->rktp_stored_offset;
                } else {
                        rktpar->offset = def_value;
                }

		rd_kafka_dbg(rk, CGRP | RD_KAFKA_DBG_TOPIC, "OFFSET",
			     "Topic %s [%"PRId32"]: "
			     "%s %s offset %"PRId64,
			     rktpar->topic, rktpar->partition,
			     is_commit ? "committing" : "setting",
			     from_rktp ? "stored" : "default", rktpar->offset);

		if (rktpar->offset >= 0)
			valid_cnt++;
        }

	return valid_cnt;
}


/**
 * Returns a new shared toppar pointer for partition at index 'idx',
 * or NULL if not set, not found, or out of range.
 */
rd_kafka_toppar_t *
rd_kafka_topic_partition_list_get_toppar (
        rd_kafka_t *rk, rd_kafka_topic_partition_list_t *rktparlist, int idx) {
        rd_kafka_topic_partition_t *rktpar;
        shptr_rd_kafka_toppar_t *s_rktp;

        if (idx >= rktparlist->cnt)
                return NULL;

        rktpar = &rktparlist->elems[idx];

        if ((s_rktp = rktpar->_private))
                return rd_kafka_toppar_keep(rd_kafka_toppar_s2i(s_rktp));

        return rd_kafka_toppar_get2(rk, rktpar->topic, rktpar->partition, 0, 0);
}

