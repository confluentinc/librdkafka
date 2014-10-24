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

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdlog.h"
#include "rdsysqueue.h"
#include "rdtime.h"


const char *rd_kafka_fetch_states[] = {
	"none",
	"offset-query",
	"offset-wait",
	"active"
};

const char *rd_kafka_topic_state_names[] = {
        "unknown",
        "exists",
        "notexists"
};

static rd_kafka_toppar_t *rd_kafka_toppar_new (rd_kafka_topic_t *rkt,
					       int32_t partition) {
	rd_kafka_toppar_t *rktp;

	rktp = calloc(1, sizeof(*rktp));

	rktp->rktp_partition = partition;
	rktp->rktp_rkt = rkt;
	rktp->rktp_fetch_state = RD_KAFKA_TOPPAR_FETCH_NONE;
	rktp->rktp_offset_fd = -1;

	rd_kafka_msgq_init(&rktp->rktp_msgq);
	rd_kafka_msgq_init(&rktp->rktp_xmit_msgq);
	pthread_mutex_init(&rktp->rktp_lock, NULL);

	rd_kafka_q_init(&rktp->rktp_fetchq);

	rd_kafka_toppar_keep(rktp);
	rd_kafka_topic_keep(rkt);

	return rktp;
}


void rd_kafka_toppar_destroy0 (rd_kafka_toppar_t *rktp) {

	/* Clear queues */
	rd_kafka_dr_msgq(rktp->rktp_rkt->rkt_rk, &rktp->rktp_xmit_msgq,
			 RD_KAFKA_RESP_ERR__DESTROY);
	rd_kafka_dr_msgq(rktp->rktp_rkt->rkt_rk, &rktp->rktp_msgq,
			 RD_KAFKA_RESP_ERR__DESTROY);
	rd_kafka_q_purge(&rktp->rktp_fetchq);

	rd_kafka_topic_destroy0(rktp->rktp_rkt);

	pthread_mutex_destroy(&rktp->rktp_lock);
	free(rktp);
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
 * NOTE: Caller must hold rd_kafka_topic_*lock()
 */
rd_kafka_toppar_t *rd_kafka_toppar_get (const rd_kafka_topic_t *rkt,
					int32_t partition,
					int ua_on_miss) {
	rd_kafka_toppar_t *rktp;

	if (partition >= 0 && partition < rkt->rkt_partition_cnt)
		rktp = rkt->rkt_p[partition];
	else if (partition == RD_KAFKA_PARTITION_UA || ua_on_miss)
		rktp = rkt->rkt_ua;
	else
		return NULL;

	if (rktp)
		rd_kafka_toppar_keep(rktp);

	return rktp;
}


/**
 * Same as rd_kafka_toppar_get() but no need for locking and
 * looks up the topic first.
 */
rd_kafka_toppar_t *rd_kafka_toppar_get2 (rd_kafka_t *rk,
					 const rd_kafkap_str_t *topic,
					 int32_t partition,
					 int ua_on_miss) {
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t *rktp;

	if (unlikely(!(rkt = rd_kafka_topic_find0(rk, topic))))
		return NULL;

	rd_kafka_topic_rdlock(rkt);
	rktp = rd_kafka_toppar_get(rkt, partition, ua_on_miss);
	rd_kafka_topic_unlock(rkt);

        rd_kafka_topic_destroy0(rkt);

	return rktp;
}


/**
 * Returns a toppar if it is available in the cluster.
 * '*errp' is set to the error-code if lookup fails.
 */
rd_kafka_toppar_t *rd_kafka_toppar_get_avail (const rd_kafka_topic_t *rkt,
                                              int32_t partition,
                                              int ua_on_miss,
                                              rd_kafka_resp_err_t *errp) {
	rd_kafka_toppar_t *rktp;

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
	rktp = rd_kafka_toppar_get(rkt, partition, 0);

	if (unlikely(!rktp)) {
		/* Unknown topic or partition */
		if (rkt->rkt_state == RD_KAFKA_TOPIC_S_NOTEXISTS)
			*errp = RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
		else
			*errp = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

		return NULL;
	}

	return rktp;
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

rd_kafka_toppar_t *rd_kafka_toppar_desired_get (rd_kafka_topic_t *rkt,
						int32_t partition) {
	rd_kafka_toppar_t *rktp;

	TAILQ_FOREACH(rktp, &rkt->rkt_desp, rktp_rktlink)
		if (rktp->rktp_partition == partition) {
			rd_kafka_toppar_keep(rktp);
			return rktp;
		}

	return NULL;
}


/**
 * Adds 'partition' as a desired partition to topic 'rkt', or updates
 * an existing partition to be desired.
 *
 * Locks: rd_kafka_topic_wrlock() must be held.
 * NOTE: 'rktp' refcount is increased'
 */
rd_kafka_toppar_t *rd_kafka_toppar_desired_add (rd_kafka_topic_t *rkt,
						int32_t partition) {
	rd_kafka_toppar_t *rktp;

	if ((rktp = rd_kafka_toppar_get(rkt, partition, 0/*no_ua_on_miss*/))) {
		rd_kafka_toppar_lock(rktp);
		rd_kafka_dbg(rkt->rkt_rk, TOPIC, "DESP",
			     "Setting topic %s [%"PRId32"] partition "
			     "as desired",
			     rkt->rkt_topic->str, rktp->rktp_partition);
		rktp->rktp_flags |= RD_KAFKA_TOPPAR_F_DESIRED;
		rd_kafka_toppar_unlock(rktp);
		return rktp;
	}

	if ((rktp = rd_kafka_toppar_desired_get(rkt, partition)))
		return rktp;

	rktp = rd_kafka_toppar_new(rkt, partition);
	rktp->rktp_flags |= RD_KAFKA_TOPPAR_F_DESIRED |
		RD_KAFKA_TOPPAR_F_UNKNOWN;

	rd_kafka_dbg(rkt->rkt_rk, TOPIC, "DESP",
		     "Adding desired topic %s [%"PRId32"]",
		     rkt->rkt_topic->str, rktp->rktp_partition);

	TAILQ_INSERT_TAIL(&rkt->rkt_desp, rktp, rktp_rktlink);

	return rktp;
}


/**
 * Unmarks an 'rktp' as desired.
 *
 * Locks: rd_kafka_topic_wrlock() must be held.
 */
void rd_kafka_toppar_desired_del (rd_kafka_toppar_t *rktp) {


	rd_kafka_toppar_lock(rktp);
	if (!(rktp->rktp_flags & RD_KAFKA_TOPPAR_F_DESIRED)) {
		rd_kafka_toppar_unlock(rktp);
		return;
	}

	rktp->rktp_flags &= ~RD_KAFKA_TOPPAR_F_DESIRED;

	if (rktp->rktp_flags & RD_KAFKA_TOPPAR_F_UNKNOWN) {
		rktp->rktp_flags &= ~RD_KAFKA_TOPPAR_F_UNKNOWN;
		TAILQ_REMOVE(&rktp->rktp_rkt->rkt_desp, rktp, rktp_rktlink);
	}

	rd_kafka_toppar_unlock(rktp);

	rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "DESP",
		     "Removing (un)desired topic %s [%"PRId32"]",
		     rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);


	rd_kafka_toppar_destroy(rktp);
}



/**
 * Move all messages from toppar 'src' to 'dst'.
 * This is used when messages migrate between partitions.
 *
 * NOTE: Both dst and src must be locked.
 */
static void rd_kafka_toppar_move_msgs (rd_kafka_toppar_t *dst,
				       rd_kafka_toppar_t *src) {
	rd_kafka_toppar_keep(src);
	rd_kafka_msgq_concat(&dst->rktp_msgq, &src->rktp_msgq);
	rd_kafka_toppar_destroy(src);
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
int rd_kafka_toppar_ua_move (rd_kafka_topic_t *rkt, rd_kafka_msgq_t *rkmq) {
	rd_kafka_toppar_t *rktp_ua;

	rd_kafka_topic_rdlock(rkt);
	rktp_ua = rd_kafka_toppar_get(rkt, RD_KAFKA_PARTITION_UA, 0);
	rd_kafka_topic_unlock(rkt);

	if (unlikely(rktp_ua == NULL))
		return -1;

	rd_kafka_msgq_concat(&rktp_ua->rktp_msgq, rkmq);

	rd_kafka_toppar_destroy(rktp_ua);

	return 0;
}


void rd_kafka_topic_destroy0 (rd_kafka_topic_t *rkt) {

	if (likely(rd_atomic_sub(&rkt->rkt_refcnt, 1) > 0))
		return;

	rd_kafka_assert(rkt->rkt_rk, rkt->rkt_refcnt == 0);

	if (rkt->rkt_topic)
		rd_kafkap_str_destroy(rkt->rkt_topic);

	rd_kafka_wrlock(rkt->rkt_rk);
	TAILQ_REMOVE(&rkt->rkt_rk->rk_topics, rkt, rkt_link);
	rkt->rkt_rk->rk_topic_cnt--;
	rd_kafka_unlock(rkt->rkt_rk);

	rd_kafka_destroy0(rkt->rkt_rk);

	rd_kafka_anyconf_destroy(_RK_TOPIC, &rkt->rkt_conf);

	pthread_rwlock_destroy(&rkt->rkt_lock);

	free(rkt);
}

void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt) {
	return rd_kafka_topic_destroy0(rkt);
}


/**
 * Finds and returns a topic based on its name, or NULL if not found.
 * The 'rkt' refcount is increased by one and the caller must call
 * rd_kafka_topic_destroy() when it is done with the topic to decrease
 * the refcount.
 *
 * Locality: any thread
 */
rd_kafka_topic_t *rd_kafka_topic_find (rd_kafka_t *rk,
				       const char *topic, int do_lock) {
	rd_kafka_topic_t *rkt;

        if (do_lock)
                rd_kafka_rdlock(rk);
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		if (!rd_kafkap_str_cmp_str(rkt->rkt_topic, topic)) {
			rd_kafka_topic_keep(rkt);
			break;
		}
	}
        if (do_lock)
                rd_kafka_unlock(rk);

	return rkt;
}

/**
 * Same semantics as ..find() but takes a Kafka protocol string instead.
 */
rd_kafka_topic_t *rd_kafka_topic_find0 (rd_kafka_t *rk,
					const rd_kafkap_str_t *topic) {
	rd_kafka_topic_t *rkt;

	rd_kafka_rdlock(rk);
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		if (!rd_kafkap_str_cmp(rkt->rkt_topic, topic)) {
			rd_kafka_topic_keep(rkt);
			break;
		}
	}
	rd_kafka_unlock(rk);

	return rkt;
}


/**
 * Create new topic handle. 
 *
 * Locality: application thread
 */
rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic,
				      rd_kafka_topic_conf_t *conf) {
	rd_kafka_topic_t *rkt;

	/* Verify configuration */
	if (!topic ||
	    (conf &&
	     (conf->message_timeout_ms < 0 ||
	      conf->request_timeout_ms <= 0))) {
		errno = EINVAL;
		return NULL;
	}

        rd_kafka_wrlock(rk);
	if ((rkt = rd_kafka_topic_find(rk, topic, 0/*no lock*/))) {
                rd_kafka_unlock(rk);
		return rkt;
        }

	rkt = calloc(1, sizeof(*rkt));

	rkt->rkt_topic     = rd_kafkap_str_new(topic);
	rkt->rkt_rk        = rk;

	if (!conf)
		conf = rd_kafka_topic_conf_new();
	rkt->rkt_conf = *conf;
	free(conf);

	/* Default partitioner: random */
	if (!rkt->rkt_conf.partitioner)
		rkt->rkt_conf.partitioner = rd_kafka_msg_partitioner_random;

	rd_kafka_dbg(rk, TOPIC, "TOPIC", "New local topic: %.*s",
		     RD_KAFKAP_STR_PR(rkt->rkt_topic));

	TAILQ_INIT(&rkt->rkt_desp);
	rd_kafka_topic_keep(rkt);
	rd_kafka_keep(rk);

	pthread_rwlock_init(&rkt->rkt_lock, NULL);

	/* Create unassigned partition */
	rkt->rkt_ua = rd_kafka_toppar_new(rkt, RD_KAFKA_PARTITION_UA);

	TAILQ_INSERT_TAIL(&rk->rk_topics, rkt, rkt_link);
	rk->rk_topic_cnt++;
	rd_kafka_unlock(rk);

	/* Query for the topic leader (async) */
	rd_kafka_topic_leader_query(rk, rkt);

	return rkt;
}


/**
 * Sets the state for topic.
 * NOTE: rd_kafka_topic_wrlock(rkt) MUST be held
 */
static void rd_kafka_topic_set_state (rd_kafka_topic_t *rkt, int state) {

        if (rkt->rkt_state == state)
                return;

        rd_kafka_dbg(rkt->rkt_rk, TOPIC, "STATE",
                     "Topic %s changed state %s -> %s",
                     rkt->rkt_topic->str,
                     rd_kafka_topic_state_names[rkt->rkt_state],
                     rd_kafka_topic_state_names[state]);
        rkt->rkt_state = state;
}

/**
 * Returns the name of a topic.
 * NOTE:
 *   The topic Kafka String representation is crafted with an extra byte
 *   at the end for the Nul that is not included in the length, this way
 *   we can use the topic's String directly.
 *   This is not true for Kafka Strings read from the network.
 */
const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt) {
	return rkt->rkt_topic->str;
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

	if (rktp->rktp_leader == rkb)
		return;


	rd_kafka_toppar_keep(rktp);

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
		rd_kafka_broker_toppars_unlock(old_rkb);
		rd_kafka_toppar_destroy(rktp);
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
			     rktp->rktp_msgq.rkmq_msg_cnt,
			     rktp->rktp_msgq.rkmq_msg_bytes);
		rd_kafka_broker_toppars_wrlock(rkb);
		rd_kafka_toppar_keep(rktp);
		TAILQ_INSERT_TAIL(&rkb->rkb_toppars, rktp, rktp_rkblink);
		rkb->rkb_toppar_cnt++;
		rktp->rktp_leader = rkb;
		rd_kafka_broker_keep(rkb);
		rd_kafka_broker_toppars_unlock(rkb);

	} else {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "BRKDELGT",
			     "No broker is leader for topic %.*s [%"PRId32"]",
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition);
	}

	rd_kafka_toppar_destroy(rktp); /* from keep() above */
}



/**
 * Update the leader for a topic+partition.
 * Returns 1 if the leader was changed, else 0, or -1 if leader is unknown.
 * NOTE: rd_kafka_topic_wrlock(rkt) MUST be held.
 */
static int rd_kafka_topic_leader_update (rd_kafka_topic_t *rkt,
                                         const struct
                                         rd_kafka_metadata_partition *mdp,
					 rd_kafka_broker_t *rkb) {
	rd_kafka_t *rk = rkt->rkt_rk;
	rd_kafka_toppar_t *rktp;

	rktp = rd_kafka_toppar_get(rkt, mdp->id, 0);
        if (unlikely(!rktp)) {
                /* Have only seen this in issue #132.
                 * Probably caused by corrupt broker state. */
                rd_kafka_log(rk, LOG_WARNING, "LEADER",
                             "Topic %s: partition [%"PRId32"] is unknown "
                             "(partition_cnt %i)",
                             rkt->rkt_topic->str, mdp->id,
                             rkt->rkt_partition_cnt);
                return -1;
        }

        rd_kafka_toppar_lock(rktp);
        /* Store a copy of the metadata for this partition.
         * The sub-arrays are NULLed to avoid extra alloc. */
        rktp->rktp_metadata          = *mdp;
        rktp->rktp_metadata.replicas = NULL;
        rktp->rktp_metadata.isrs     = NULL;
        rd_kafka_toppar_unlock(rktp);

	if (!rkb) {
		int had_leader = rktp->rktp_leader ? 1 : 0;

		if (mdp->leader == -1)
			/* Topic lost its leader */;
		else
			rd_kafka_log(rk, LOG_NOTICE, "TOPICBRK",
				     "Topic %s [%"PRId32"] migrated to unknown "
				     "broker %"PRId32": "
				     "requesting metadata update",
				     rkt->rkt_topic->str, mdp->id,
                                     mdp->leader);

		rd_kafka_toppar_broker_delegate(rktp, NULL);

		rd_kafka_toppar_destroy(rktp); /* from get() */

		return had_leader ? -1 : 0;
	}


	if (rktp->rktp_leader) {
		if (rktp->rktp_leader == rkb) {
			/* No change in broker */
			rd_kafka_dbg(rk, TOPIC, "TOPICUPD",
				     "No leader change for topic %s "
				     "[%"PRId32"] with leader %"PRId32,
				     rkt->rkt_topic->str,
                                     mdp->id, mdp->leader);
			rd_kafka_toppar_destroy(rktp); /* from get() */
			return 0;
		}

		rd_kafka_dbg(rk, TOPIC, "TOPICUPD",
			     "Topic %s [%"PRId32"] migrated from "
			     "broker %"PRId32" to %"PRId32,
			     rkt->rkt_topic->str, mdp->id,
			     rktp->rktp_leader->rkb_nodeid,
			     rkb->rkb_nodeid);
	}

	rd_kafka_toppar_broker_delegate(rktp, rkb);

	rd_kafka_toppar_destroy(rktp); /* from get() */

	return 1;
}



/**
 * Remove all partitions from a topic, including the ua.
 */
void rd_kafka_topic_partitions_remove (rd_kafka_topic_t *rkt) {
	rd_kafka_toppar_t *rktp;
	int i;

	rd_kafka_topic_keep(rkt);
	rd_kafka_topic_wrlock(rkt);

	/* Remove all partitions */
	for (i = 0 ; i < rkt->rkt_partition_cnt ; i++) {

		if (!(rktp = rd_kafka_toppar_get(rkt, i, 0)))
			continue;

		rd_kafka_toppar_lock(rktp);
		rd_kafka_msgq_purge(rkt->rkt_rk, &rktp->rktp_msgq);
		rd_kafka_toppar_unlock(rktp);
		rd_kafka_toppar_destroy(rktp); /* _get() */
		rd_kafka_toppar_destroy(rktp); /* remove partition */
	}

	if (rkt->rkt_p)
		free(rkt->rkt_p);

	rkt->rkt_p = NULL;
	rkt->rkt_partition_cnt = 0;

	if ((rktp = rd_kafka_toppar_get(rkt, RD_KAFKA_PARTITION_UA, 0))) {
		rd_kafka_toppar_lock(rktp);
		rd_kafka_msgq_purge(rkt->rkt_rk, &rktp->rktp_msgq);
		rkt->rkt_ua = NULL;
		rd_kafka_toppar_unlock(rktp);
		rd_kafka_toppar_destroy(rktp); /* for get() */
		rd_kafka_toppar_destroy(rktp); /* for final destruction */
	}

	rd_kafka_topic_unlock(rkt);
	rd_kafka_topic_destroy0(rkt);
}


/**
 * Update the number of partitions for a topic and takes according actions.
 * Returns 1 if the partition count changed, else 0.
 * NOTE: rd_kafka_topic_wrlock(rkt) MUST be held.
 */
static int rd_kafka_topic_partition_cnt_update (rd_kafka_topic_t *rkt,
						int32_t partition_cnt) {
	rd_kafka_t *rk = rkt->rkt_rk;
	rd_kafka_toppar_t **rktps;
	rd_kafka_toppar_t *rktp_ua;
	rd_kafka_toppar_t *rktp;
	int32_t i;

	if (rkt->rkt_partition_cnt == partition_cnt) {
		rd_kafka_dbg(rk, TOPIC, "PARTCNT",
			     "No change in partition count for topic %s",
			     rkt->rkt_topic->str);
		return 0; /* No change in partition count */
	}

	if (unlikely(rkt->rkt_partition_cnt != 0))
		rd_kafka_log(rk, LOG_NOTICE, "PARTCNT",
			     "Topic %s partition count changed "
			     "from %"PRId32" to %"PRId32,
			     rkt->rkt_topic->str,
			     rkt->rkt_partition_cnt, partition_cnt);
	else
		rd_kafka_dbg(rk, TOPIC, "PARTCNT",
			     "Topic %s partition count changed "
			     "from %"PRId32" to %"PRId32,
			     rkt->rkt_topic->str,
			     rkt->rkt_partition_cnt, partition_cnt);


	/* Create and assign new partition list */
	if (partition_cnt > 0)
		rktps = calloc(partition_cnt, sizeof(*rktps));
	else
		rktps = NULL;

	for (i = 0 ; i < partition_cnt ; i++) {
		if (i >= rkt->rkt_partition_cnt) {
			/* New partition. Check if its in the list of
			 * desired partitions first. */
			if ((rktp = rd_kafka_toppar_desired_get(rkt, i))) {
				/* Remove from desp list since the partition
				 * is now known. */
				rd_kafka_toppar_lock(rktp);
				rktp->rktp_flags &= ~RD_KAFKA_TOPPAR_F_UNKNOWN;
				rd_kafka_toppar_unlock(rktp);
				TAILQ_REMOVE(&rkt->rkt_desp, rktp,
					     rktp_rktlink);
			} else
				rktp = rd_kafka_toppar_new(rkt, i);
			rktps[i] = rktp;
		} else {
			/* Move existing partition */
			rktps[i] = rkt->rkt_p[i];
		}
	}

	rktp_ua = rd_kafka_toppar_get(rkt, RD_KAFKA_PARTITION_UA, 0);

	/* Remove excessive partitions if partition count decreased. */
	for (; i < rkt->rkt_partition_cnt ; i++) {
		rktp = rkt->rkt_p[i];

		/* Partition has gone away, move messages to UA or drop */
		if (likely(rktp_ua != NULL))
			rd_kafka_toppar_move_msgs(rktp_ua, rktp);
		else
			rd_kafka_msgq_purge(rkt->rkt_rk, &rktp->rktp_msgq);

		/* If this is a desired partition move it back on to
		 * the desired list. */
		rd_kafka_toppar_lock(rktp);
		if (rktp->rktp_flags & RD_KAFKA_TOPPAR_F_DESIRED) {
			/* Reinsert on desp list since the partition
			 * is no longer known. */
			rd_kafka_assert(rkt->rkt_rk,
                                        !(rktp->rktp_flags &
                                          RD_KAFKA_TOPPAR_F_UNKNOWN));
			rktp->rktp_flags |= RD_KAFKA_TOPPAR_F_UNKNOWN;
			TAILQ_INSERT_TAIL(&rkt->rkt_desp, rktp, rktp_rktlink);
		}
		rd_kafka_toppar_unlock(rktp);

		rd_kafka_toppar_destroy(rktp);
	}

	if (likely(rktp_ua != NULL))
		rd_kafka_toppar_destroy(rktp_ua); /* .._get() above */

	if (rkt->rkt_p)
		free(rkt->rkt_p);

	rkt->rkt_p = rktps;

	rkt->rkt_partition_cnt = partition_cnt;

	return 1;
}


/**
 * Assign messages on the UA partition to available partitions.
 * Locks: rd_kafka_topic_*lock() must be held.
 */
static void rd_kafka_topic_assign_uas (rd_kafka_topic_t *rkt) {
	rd_kafka_t *rk = rkt->rkt_rk;
	rd_kafka_toppar_t *rktp_ua;
	rd_kafka_msg_t *rkm, *tmp;
	rd_kafka_msgq_t uas = RD_KAFKA_MSGQ_INITIALIZER(uas);
	rd_kafka_msgq_t failed = RD_KAFKA_MSGQ_INITIALIZER(failed);
	int cnt;

	rktp_ua = rd_kafka_toppar_get(rkt, RD_KAFKA_PARTITION_UA, 0);
	if (unlikely(!rktp_ua)) {
		rd_kafka_dbg(rk, TOPIC, "ASSIGNUA",
			     "No UnAssigned partition available for %s",
			     rkt->rkt_topic->str);
		return;
	}

	/* Assign all unassigned messages to new topics. */
	rd_kafka_dbg(rk, TOPIC, "PARTCNT",
		     "Partitioning %i unassigned messages in topic %.*s to "
		     "%"PRId32" partitions",
		     rktp_ua->rktp_msgq.rkmq_msg_cnt, 
		     RD_KAFKAP_STR_PR(rkt->rkt_topic),
		     rkt->rkt_partition_cnt);

	rd_kafka_toppar_lock(rktp_ua);
	rd_kafka_msgq_move(&uas, &rktp_ua->rktp_msgq);
	cnt = uas.rkmq_msg_cnt;
	rd_kafka_toppar_unlock(rktp_ua);

	TAILQ_FOREACH_SAFE(rkm, &uas.rkmq_msgs, rkm_link, tmp) {
		/* Fast-path for failing messages with forced partition */
		if (rkm->rkm_partition != RD_KAFKA_PARTITION_UA &&
		    rkm->rkm_partition >= rkt->rkt_partition_cnt &&
		    rkt->rkt_state != RD_KAFKA_TOPIC_S_UNKNOWN) {
			rd_kafka_msgq_enq(&failed, rkm);
			continue;
		}

		if (unlikely(rd_kafka_msg_partitioner(rkt, rkm, 0) != 0)) {
			/* Desired partition not available */
			rd_kafka_msgq_enq(&failed, rkm);
		}
	}

	rd_kafka_dbg(rk, TOPIC, "UAS",
		     "%i/%i messages were partitioned in topic %s",
		     cnt - failed.rkmq_msg_cnt, cnt, rkt->rkt_topic->str);

	if (failed.rkmq_msg_cnt > 0) {
		/* Fail the messages */
		rd_kafka_dbg(rk, TOPIC, "UAS",
			     "%i/%i messages failed partitioning in topic %s",
			     uas.rkmq_msg_cnt, cnt, rkt->rkt_topic->str);
		rd_kafka_dr_msgq(rk, &failed,
				 rkt->rkt_state == RD_KAFKA_TOPIC_S_NOTEXISTS ?
				 RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC :
				 RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION);
	}

	rd_kafka_toppar_destroy(rktp_ua); /* from get() */
}


/**
 * Received metadata request contained no information about topic 'rkt'
 * and thus indicates the topic is not available in the cluster.
 */
void rd_kafka_topic_metadata_none (rd_kafka_topic_t *rkt) {
	rd_kafka_topic_wrlock(rkt);

	rkt->rkt_ts_metadata = rd_clock();

        rd_kafka_topic_set_state(rkt, RD_KAFKA_TOPIC_S_NOTEXISTS);

	/* Update number of partitions */
	rd_kafka_topic_partition_cnt_update(rkt, 0);

	/* Purge messages with forced partition */
	rd_kafka_topic_assign_uas(rkt);

	rd_kafka_topic_unlock(rkt);
}


/**
 * Update a topic from metadata.
 * Returns 1 if the number of partitions changed, 0 if not, and -1 if the
 * topic is unknown.
 */
int rd_kafka_topic_metadata_update (rd_kafka_broker_t *rkb,
                                    const struct rd_kafka_metadata_topic *mdt) {
	rd_kafka_topic_t *rkt;
	int upd = 0;
	int j;
        rd_kafka_broker_t **partbrokers;
        int query_leader = 0;

	if (!(rkt = rd_kafka_topic_find(rkb->rkb_rk, mdt->topic, 1/*lock*/)))
		return -1; /* Ignore topics that we dont have locally. */

	if (mdt->err != RD_KAFKA_RESP_ERR_NO_ERROR)
		rd_rkb_dbg(rkb, TOPIC, "METADATA",
			   "Error in metadata reply for "
			   "topic %s (PartCnt %i): %s",
			   rkt->rkt_topic->str, mdt->partition_cnt,
			   rd_kafka_err2str(mdt->err));

        partbrokers = alloca(mdt->partition_cnt * sizeof(*partbrokers));

	/* Look up brokers before acquiring rkt lock to preserve lock order */
	rd_kafka_rdlock(rkb->rkb_rk);
	for (j = 0 ; j < mdt->partition_cnt ; j++) {
		if (mdt->partitions[j].leader == -1) {
                        partbrokers[j] = NULL;
			continue;
		}

                partbrokers[j] =
                        rd_kafka_broker_find_by_nodeid(rkb->rkb_rk,
                                                       mdt->partitions[j].
                                                       leader);
	}
	rd_kafka_unlock(rkb->rkb_rk);


	rd_kafka_topic_wrlock(rkt);

	rkt->rkt_ts_metadata = rd_clock();

	/* Set topic state */
	if (mdt->err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
                rd_kafka_topic_set_state(rkt, RD_KAFKA_TOPIC_S_NOTEXISTS);
        else
                rd_kafka_topic_set_state(rkt, RD_KAFKA_TOPIC_S_EXISTS);

	/* Update number of partitions */
	upd += rd_kafka_topic_partition_cnt_update(rkt, mdt->partition_cnt);

	/* Update leader for each partition */
	for (j = 0 ; j < mdt->partition_cnt ; j++) {
                int r;

		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "  Topic %s partition %i Leader %"PRId32,
			   rkt->rkt_topic->str,
			   mdt->partitions[j].id,
			   mdt->partitions[j].leader);

		/* Update leader for partition */
		r = rd_kafka_topic_leader_update(rkt,
                                                 &mdt->partitions[j],
                                                 partbrokers[j]);
                if (r == -1)
                        query_leader = 1;

                upd += (r != 0 ? 1 : 0);

                /* Drop reference to broker (from find()) */
                if (partbrokers[j])
			rd_kafka_broker_destroy(partbrokers[j]);

	}

	/* Try to assign unassigned messages to new partitions, or fail them */
	if (upd > 0 || rkt->rkt_state == RD_KAFKA_TOPIC_S_NOTEXISTS)
		rd_kafka_topic_assign_uas(rkt);

	rd_kafka_topic_unlock(rkt);

        /* Query for the topic leader (async) */
        if (query_leader)
                rd_kafka_topic_leader_query(rkt->rkt_rk, rkt);

	rd_kafka_topic_destroy0(rkt); /* from find() */

	return upd;
}



/**
 * Scan all topics and partitions for:
 *  - timed out messages.
 *  - topics that needs to be created on the broker.
 *  - topics who's metadata is too old.
 */
int rd_kafka_topic_scan_all (rd_kafka_t *rk, rd_ts_t now) {
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t *rktp;
	rd_kafka_msgq_t timedout;
	int tpcnt = 0;
	int totcnt;

	rd_kafka_msgq_init(&timedout);

	rd_kafka_rdlock(rk);
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		int p;

		rd_kafka_topic_wrlock(rkt);

                /* Check if metadata information has timed out:
                 * older than 3 times the metadata.refresh.interval.ms */
                if (rkt->rkt_state != RD_KAFKA_TOPIC_S_UNKNOWN &&
		    rkt->rkt_rk->rk_conf.metadata_refresh_interval_ms >= 0 &&
                    rd_clock() > rkt->rkt_ts_metadata +
                    (rkt->rkt_rk->rk_conf.metadata_refresh_interval_ms *
                     1000 * 3)) {
                        rd_kafka_dbg(rk, TOPIC, "NOINFO",
                                     "Topic %s metadata information timed out "
                                     "(%"PRIu64"ms old)",
                                     rkt->rkt_topic->str,
                                     (rd_clock() - rkt->rkt_ts_metadata)/1000);
                        rd_kafka_topic_set_state(rkt, RD_KAFKA_TOPIC_S_UNKNOWN);
                }

                /* Just need a read-lock from here on. */
                rd_kafka_topic_unlock(rkt);
                rd_kafka_topic_rdlock(rkt);

		if (rkt->rkt_partition_cnt == 0) {
			/* If this partition is unknown by brokers try
			 * to create it by sending a topic-specific
			 * metadata request.
			 * This requires "auto.create.topics.enable=true"
			 * on the brokers. */
			rd_kafka_topic_unlock(rkt);
			rd_kafka_topic_leader_query0(rk, rkt, 0/*no_rk_lock*/);
			rd_kafka_topic_rdlock(rkt);
		}

		for (p = RD_KAFKA_PARTITION_UA ;
		     p < rkt->rkt_partition_cnt ; p++) {
			if (!(rktp = rd_kafka_toppar_get(rkt, p, 0)))
				continue;

			rd_kafka_toppar_lock(rktp);

			/* Scan toppar's message queue for timeouts */
			if (rd_kafka_msgq_age_scan(&rktp->rktp_msgq,
						   &timedout, now) > 0)
				tpcnt++;

			rd_kafka_toppar_unlock(rktp);
			rd_kafka_toppar_destroy(rktp);
		}
		rd_kafka_topic_unlock(rkt);
	}
	rd_kafka_unlock(rk);

	if ((totcnt = timedout.rkmq_msg_cnt) > 0) {
		rd_kafka_dbg(rk, MSG, "TIMEOUT",
			     "%i message(s) from %i toppar(s) timed out",
			     timedout.rkmq_msg_cnt, tpcnt);
		rd_kafka_dr_msgq(rk, &timedout,
				 RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);
	}

	return totcnt;
}


/**
 * Locks: rd_kafka_topic_*lock() must be held.
 */
int rd_kafka_topic_partition_available (const rd_kafka_topic_t *rkt,
					int32_t partition) {
	int avail;
	rd_kafka_toppar_t *rktp;

	rktp = rd_kafka_toppar_get(rkt, partition, 0/*no ua-on-miss*/);
	if (unlikely(!rktp))
		return 0;

	rd_kafka_toppar_lock(rktp);
	avail = rktp->rktp_leader ? 1 : 0;
	rd_kafka_toppar_unlock(rktp);

	rd_kafka_toppar_destroy(rktp);
	return avail;
}

