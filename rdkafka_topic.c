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

static rd_kafka_toppar_t *rd_kafka_toppar_new (rd_kafka_topic_t *rkt,
					       int32_t partition) {
	rd_kafka_toppar_t *rktp;

	rktp = calloc(1, sizeof(*rktp));
							
	rktp->rktp_partition = partition;
	rktp->rktp_rkt = rkt;

	rd_kafka_msgq_init(&rktp->rktp_msgq);
	rd_kafka_msgq_init(&rktp->rktp_xmit_msgq);

	rd_kafka_toppar_keep(rktp);

	rd_kafka_toppar_keep(rktp); /* FIXME: cheating */

	return rktp;
}


void rd_kafka_toppar_destroy0 (rd_kafka_toppar_t *rktp) {
	/* FIXME: toppar destroy */
	assert(!*"toppar_destroy: not implemented");
	free(rktp);
}


/**
 * Returns the appropriate toppar for a given rkt and partition.
 * The returned toppar has increased refcnt and must be unreffed by calling
 *  rd_kafka_toppar_destroy().
 *
 * NOTE: Caller must provide proper locking.
 */
static rd_kafka_toppar_t *rd_kafka_toppar_get (rd_kafka_topic_t *rkt,
					       int32_t partition) {
	rd_kafka_toppar_t *rktp;

	if (partition < 0 || partition >= rkt->rkt_partition_cnt)
		rktp = rkt->rkt_ua;
	else
		rktp = rkt->rkt_p[partition];

	rd_kafka_toppar_keep(rktp);

	return rktp;
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



void rd_kafka_topic_destroy0 (rd_kafka_topic_t *rkt) {

	if (likely(rd_atomic_sub(&rkt->rkt_refcnt, 1) > 0))
		return;

	assert(rkt->rkt_refcnt == 0);

	if (rkt->rkt_topic)
		rd_kafkap_str_destroy(rkt->rkt_topic);

	free(rkt);
}

void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt) {
	return rd_kafka_topic_destroy0(rkt);
}

static rd_kafka_topic_t *rd_kafka_topic_find (rd_kafka_t *rk,
					      const char *topic) {
	rd_kafka_topic_t *rkt;

	rd_kafka_lock(rk);
	TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
		if (!rd_kafkap_str_cmp_str(rkt->rkt_topic, topic))
			break;
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
				      const rd_kafka_topic_conf_t *conf) {
	rd_kafka_topic_t *rkt;

	/* Verify configuration */
	if (!topic ||
	    conf->message_timeout_ms <= 0 ||
	    conf->request_timeout_ms <= 0) {
		errno = EINVAL;
		return NULL;
	}

	if ((rkt = rd_kafka_topic_find(rk, topic))) {
		rd_kafka_topic_keep(rkt); /* one refcnt for caller */
		return rkt;
	}
	
	rkt = calloc(1, sizeof(*rkt));

	rkt->rkt_topic     = rd_kafkap_str_new(topic);
	rkt->rkt_rk        = rk;
	if (conf)
		rkt->rkt_conf = *conf;
	else
		rd_kafka_topic_defaultconf_set(&rkt->rkt_conf);

	/* Default partitioner: random */
	if (!rkt->rkt_conf.partitioner)
		rkt->rkt_conf.partitioner = rd_kafka_msg_partitioner_random;

	rd_kafka_dbg(rk, "TOPIC", "new topic: %.*s",
		     RD_KAFKAP_STR_PR(rkt->rkt_topic));

	rd_kafka_topic_keep(rkt); /* one refcnt for rk */

	pthread_rwlock_init(&rkt->rkt_lock, NULL);

	/* Create unassigned partition */
	rkt->rkt_ua = rd_kafka_toppar_new(rkt, RD_KAFKA_PARTITION_UA);

	rd_kafka_lock(rk);
	TAILQ_INSERT_TAIL(&rk->rk_topics, rkt, rkt_link);
	rk->rk_topic_cnt++;
	rd_kafka_unlock(rk);

	rd_kafka_topic_keep(rkt); /* one refcnt for caller */

	return rkt;
}


/**
 * Returns the name of a topic
 */
const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt) {
	return rkt->rkt_topic->str;
}


/**
 * Delegates broker 'rkb' as leader for toppar 'rktp'.
 * 'rkb' may be NULL to undelegate leader.
 *
 * Locks: Caller must have topic_lock held.
 */
void rd_kafka_toppar_broker_delegate (rd_kafka_toppar_t *rktp,
				      rd_kafka_broker_t *rkb) {

	if (rktp->rktp_leader == rkb)
		return;


	if (rktp->rktp_leader) {
		rd_kafka_broker_t *old_rkb = rktp->rktp_leader;

		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, "BRKDELGT",
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
		rd_kafka_broker_destroy(old_rkb);

	}

	if (rkb) {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, "BRKDELGT",
			     "Broker %s is now leader for topic %.*s "
			     "[%"PRId32"] with %i messages "
			     "(%"PRIu64" bytes) queued",
			     rkb->rkb_name,
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition,
			     rktp->rktp_msgq.rkmq_msg_cnt,
			     rktp->rktp_msgq.rkmq_msg_bytes);
		rd_kafka_broker_toppars_wrlock(rkb);
		TAILQ_INSERT_TAIL(&rkb->rkb_toppars, rktp, rktp_rkblink);
		rkb->rkb_toppar_cnt++;
		rktp->rktp_leader = rkb;
		rd_kafka_broker_keep(rkb);
		rd_kafka_broker_toppars_unlock(rkb);

		
	} else {
		rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, "BRKDELGT",
			     "No broker is leader for topic %.*s [%"PRId32"]",
			     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
			     rktp->rktp_partition);
	}
}



void rd_kafka_topic_update (rd_kafka_t *rk,
			    const char *topic, int32_t partition,
			    int32_t leader) {
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t *rktp;
	rd_kafka_broker_t *rkb;

	if (!(rkt = rd_kafka_topic_find(rk, topic))) {
		rd_kafka_dbg(rk, "TOPICUPD",
			     "Ignoring topic %s: not found locally", topic);
		return;
	}

	/* Find broker */
	rd_kafka_lock(rk);
	rkb = rd_kafka_broker_find_by_nodeid(rk, leader);
	rd_kafka_unlock(rk);


	rd_kafka_topic_wrlock(rkt);

	rktp = rd_kafka_toppar_get(rkt, partition);

	assert(rktp->rktp_partition != RD_KAFKA_PARTITION_UA);

	if (leader == -1) {
		/* Topic lost its leader */
		rd_kafka_toppar_broker_delegate(rktp, NULL);
		rd_kafka_topic_unlock(rkt);

		/* Query for the topic leader (async) */
		rd_kafka_topic_leader_query(rk, rkt);
		return;
	}


	if (!rkb) {
		rd_kafka_log(rk, LOG_NOTICE, "TOPICBRK",
			     "Topic %s [%"PRId32"] migrated to unknown "
			     "broker %"PRId32": requesting metadata update",
			     topic, partition, leader);
		rd_kafka_toppar_broker_delegate(rktp, NULL);
		rd_kafka_topic_unlock(rkt);

		/* Query for the topic leader (async) */
		rd_kafka_topic_leader_query(rk, rkt);
		return;
	}


	if (rktp->rktp_leader) {
		if (rktp->rktp_leader == rkb) {
			/* No change in broker */
			rd_kafka_dbg(rk, "TOPICUPD",
				     "No leader change for topic %s "
				     "[%"PRId32"] with leader %"PRId32,
				     topic, partition, leader);

			rd_kafka_topic_unlock(rkt);
			rd_kafka_broker_destroy(rkb); /* refcnt from find */
			return;
		}

		rd_kafka_dbg(rk, "TOPICUPD",
			     "Topic %s [%"PRId32"] migrated from "
			     "broker %"PRId32" to %"PRId32,
			     topic, partition, rktp->rktp_leader->rkb_nodeid,
			     rkb->rkb_nodeid);
	}
	
	rd_kafka_toppar_broker_delegate(rktp, rkb);
	rd_kafka_topic_unlock(rkt);
	rd_kafka_broker_destroy(rkb); /* refcnt from find */
}


/**
 * Update the number of partitions for a topic and takes according actions.
 */
void rd_kafka_topic_partition_cnt_update (rd_kafka_t *rk,
					  const char *topic,
					  int32_t partition_cnt) {
	rd_kafka_topic_t *rkt;
	rd_kafka_toppar_t **rktps;
	int32_t i;

	if (!(rkt = rd_kafka_topic_find(rk, topic))) {
		rd_kafka_dbg(rk, "PARTCNT", "Ignore unknown topic %s", topic);
		return; /* Ignore topics that we dont have locally. */
	}

	if (rkt->rkt_partition_cnt == partition_cnt) {
		rd_kafka_dbg(rk, "PARTCNT",
			     "No change in partition count for topic %s",
			     topic);
		return; /* No change in partition count */
	}

	rd_kafka_log(rk,
		     rkt->rkt_partition_cnt == 0 ? LOG_DEBUG : LOG_NOTICE,
		     "PARTCNT",
		     "Topic %.*s partition count changed "
		     "from %"PRId32" to %"PRId32,
		     RD_KAFKAP_STR_PR(rkt->rkt_topic),
		     rkt->rkt_partition_cnt, partition_cnt);

	/* Create and assign new partition list */
	if (partition_cnt > 0)
		rktps = calloc(partition_cnt, sizeof(*rktps));
	else
		rktps = NULL;

	rd_kafka_topic_wrlock(rkt);

	for (i = 0 ; i < partition_cnt ; i++) {
		if (i >= rkt->rkt_partition_cnt) {
			/* New partition */
			rktps[i] = rd_kafka_toppar_new(rkt, i);
		} else {
			/* Move existing partition */
			rktps[i] = rkt->rkt_p[i];
		}
	}

	/* Remove excessive partitions if partition count decreased. */
	for (; i < rkt->rkt_partition_cnt ; i++) {
		/* Partition has gone away, move messages to UA. */
		rd_kafka_toppar_move_msgs(rkt->rkt_ua, rkt->rkt_p[i]);
		rd_kafka_toppar_destroy(rkt->rkt_p[i]);
	}

	if (rkt->rkt_p)
		free(rkt->rkt_p);

	rkt->rkt_p = rktps;

	rkt->rkt_partition_cnt = partition_cnt;

	rd_kafka_topic_unlock(rkt);
}


void rd_kafka_topic_assign_uas (rd_kafka_t *rk, const char *topic) {
	rd_kafka_topic_t *rkt;
	rd_kafka_msg_t *rkm, *tmp;
	rd_kafka_msgq_t uas;
	rd_kafka_msgq_t failed = RD_KAFKA_MSGQ_INITIALIZER(failed);
	int cnt;

	if (!(rkt = rd_kafka_topic_find(rk, topic))) {
		rd_kafka_dbg(rk, "PARTCNT", "Ignore unknown topic %s", topic);
		return; /* Ignore topics that we dont have locally. */
	}

	/* Assign all unassigned messages to new topics. */
	rd_kafka_dbg(rk, "PARTCNT",
		     "Partitioning %i unassigned messages in topic %.*s to "
		     "%"PRId32" partitions",
		     rkt->rkt_ua->rktp_msgq.rkmq_msg_cnt, 
		     RD_KAFKAP_STR_PR(rkt->rkt_topic),
		     rkt->rkt_partition_cnt);

	rd_kafka_toppar_lock(rkt->rkt_ua);
	cnt = rkt->rkt_ua->rktp_msgq.rkmq_msg_cnt;
	rd_kafka_msgq_move(&uas, &rkt->rkt_ua->rktp_msgq);
	rd_kafka_toppar_unlock(rkt->rkt_ua);

	TAILQ_FOREACH_SAFE(rkm, tmp, &uas.rkmq_msgs, rkm_link) {
		if (unlikely(rd_kafka_msg_partitioner(rkt, NULL, rkm) == -1)) {
			/* Desired partition not available */
			rd_kafka_msgq_enq(&failed, rkm);
		}
	}

	rd_kafka_dbg(rk, "UAS",
		     "%i/%i messages were partitioned",
		     cnt - failed.rkmq_msg_cnt, cnt);

	if (failed.rkmq_msg_cnt > 0) {
		/* Add the messages to the UA partition's head to
		 * preserve some message order. */
		rd_kafka_dbg(rk, "UAS",
			     "%i/%i messages failed partitioning",
			     uas.rkmq_msg_cnt, cnt);
		rd_kafka_toppar_lock(rkt->rkt_ua);
		rd_kafka_msgq_concat(&failed, &rkt->rkt_ua->rktp_msgq);
		rd_kafka_msgq_move(&rkt->rkt_ua->rktp_msgq, &failed);
		rd_kafka_toppar_unlock(rkt->rkt_ua);
	}
}
