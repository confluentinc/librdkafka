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
#include "rdrand.h"
#include "rdtime.h"

#include "rdsysqueue.h"

void rd_kafka_msg_destroy (rd_kafka_t *rk, rd_kafka_msg_t *rkm) {

	rd_kafka_assert(rk, rk->rk_producer.msg_cnt > 0);
	(void)rd_atomic_sub(&rk->rk_producer.msg_cnt, 1);

	if (rkm->rkm_flags & RD_KAFKA_MSG_F_FREE)
		free(rkm->rkm_payload);

	if (rkm->rkm_key)
		rd_kafkap_bytes_destroy(rkm->rkm_key);

	free(rkm);
}

/**
 * Create a new message.
 *
 * Returns 0 on success or -1 on error.
 * Both errno and 'errp' are set appropriately.
 */
static rd_kafka_msg_t *rd_kafka_msg_new0 (rd_kafka_topic_t *rkt,
                                          int32_t force_partition,
                                          int msgflags,
                                          char *payload, size_t len,
                                          const void *key, size_t keylen,
                                          void *msg_opaque,
                                          rd_kafka_resp_err_t *errp,
                                          rd_ts_t now) {
	rd_kafka_msg_t *rkm;
	size_t mlen = sizeof(*rkm);

	if (unlikely(len + keylen > rkt->rkt_rk->rk_conf.max_msg_size)) {
                *errp = RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE;
                errno = EMSGSIZE;
		return NULL;
	}

	/* If we are to make a copy of the payload, allocate space for it too */
	if (msgflags & RD_KAFKA_MSG_F_COPY) {
		msgflags &= ~RD_KAFKA_MSG_F_FREE;
		mlen += len;
	}

	/* Note: using malloc here, not calloc, so make sure all fields
	 *       are properly set up. */
	rkm = malloc(mlen);
	rkm->rkm_len        = len;
	rkm->rkm_flags      = msgflags;
	rkm->rkm_opaque     = msg_opaque;
	rkm->rkm_key        = rd_kafkap_bytes_new(key, keylen);
	rkm->rkm_partition  = force_partition;
	if (rkt->rkt_conf.message_timeout_ms == 0) {
		rkm->rkm_ts_timeout = INT64_MAX;
	} else {
		rkm->rkm_ts_timeout = now +
			rkt->rkt_conf.message_timeout_ms * 1000;
	}

	if (msgflags & RD_KAFKA_MSG_F_COPY) {
		/* Copy payload to space following the ..msg_t */
		rkm->rkm_payload = (void *)(rkm+1);
		memcpy(rkm->rkm_payload, payload, len);

	} else {
		/* Just point to the provided payload. */
		rkm->rkm_payload = payload;
	}

        return rkm;
}


/**
 * Produce: creates a new message, runs the partitioner and enqueues
 *          into on the selected partition.
 *
 * Returns 0 on success or -1 on error.
 */
int rd_kafka_msg_new (rd_kafka_topic_t *rkt, int32_t force_partition,
		      int msgflags,
		      char *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque) {
	rd_kafka_msg_t *rkm;
	rd_kafka_resp_err_t err;

	if (unlikely(rd_atomic_add(&rkt->rkt_rk->rk_producer.msg_cnt, 1) >
		     rkt->rkt_rk->rk_conf.queue_buffering_max_msgs)) {
		(void)rd_atomic_sub(&rkt->rkt_rk->rk_producer.msg_cnt, 1);
		errno = ENOBUFS;
		return -1;
	}

        /* Create message */
        rkm = rd_kafka_msg_new0(rkt, force_partition, msgflags, 
                                payload, len, key, keylen, msg_opaque, &err,
                                rd_clock());
        if (unlikely(!rkm)) {
                /* errno is already set by msg_new() */
                (void)rd_atomic_sub(&rkt->rkt_rk->rk_producer.msg_cnt, 1);
                return -1;
        }


        /* Partition the message */
	err = rd_kafka_msg_partitioner(rkt, rkm, 1);
	if (likely(!err))
		return 0;

	/* Handle partitioner failures: it only fails when the application
	 * attempts to force a destination partition that does not exist
	 * in the cluster. */

	rd_kafka_msg_destroy(rkt->rkt_rk, rkm);

	/* Translate error codes to errnos. */
	if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
		errno = ESRCH;
	else if (err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
		errno = ENOENT;
	else
		errno = EINVAL; /* NOTREACHED */

	return -1;
}




/**
 * Produce a batch of messages.
 * Returns the number of messages succesfully queued for producing.
 * Each message's .err will be set accordingly.
 */
int rd_kafka_produce_batch (rd_kafka_topic_t *rkt, int32_t partition,
                            int msgflags,
                            rd_kafka_message_t *rkmessages, int message_cnt) {
        rd_kafka_msgq_t tmpq = RD_KAFKA_MSGQ_INITIALIZER(tmpq);
        int i;
        rd_ts_t now = rd_clock();
        int good = 0;
        rd_kafka_toppar_t *rktp = NULL;
        rd_kafka_resp_err_t all_err = 0;

        /* For partitioner; hold lock for entire run,
         * for one partition: only acquire when needed at the end. */
	if (partition == RD_KAFKA_PARTITION_UA)
		rd_kafka_topic_rdlock(rkt);

        for (i = 0 ; i < message_cnt ; i++) {
                rd_kafka_msg_t *rkm;

                /* Propagate error for all messages. */
                if (unlikely(all_err)) {
                        rkmessages[i].err = all_err;
                        continue;
                }

                /* buffering.max.messages reached */
                if (unlikely(rkt->rkt_rk->rk_producer.msg_cnt +
                             /* For partitioner: msg_cnt is increased per
                              *                  message,
                              * For single partition: msg_cnt is increased
                              *                       just once at the end */
                             (partition == RD_KAFKA_PARTITION_UA ? 0 : good)
                             + 1 >
                             rkt->rkt_rk->rk_conf.queue_buffering_max_msgs)) {
                        all_err = RD_KAFKA_RESP_ERR__QUEUE_FULL;
                        rkmessages[i].err = all_err;
                        continue;
                }

                /* Create message */
                rkm = rd_kafka_msg_new0(rkt,
                                        partition , msgflags,
                                        rkmessages[i].payload,
                                        rkmessages[i].len,
                                        rkmessages[i].key,
                                        rkmessages[i].key_len,
                                        rkmessages[i]._private,
                                        &rkmessages[i].err,
                                        now);
                if (!rkm)
                        continue;

                /* Two cases here:
                 *  partition==UA:     run the partitioner (slow)
                 *  fixed partition:   simply concatenate the queue to partit */
                if (partition == RD_KAFKA_PARTITION_UA) {
                        (void)rd_atomic_add(&rkt->rkt_rk->rk_producer.msg_cnt,
                                            1);

                        /* Partition the message */
                        rkmessages[i].err =
                                rd_kafka_msg_partitioner(rkt, rkm,
                                                         0/*already locked*/);

                        if (unlikely(rkmessages[i].err)) {
                                rd_kafka_msg_destroy(rkt->rkt_rk, rkm);
                                continue;
                        }


                } else {
                        /* Single destination partition, enqueue message
                         * on temporary queue for later queue concat. */
                        rd_kafka_msgq_enq(&tmpq, rkm);
                }

                rkmessages[i].err = RD_KAFKA_RESP_ERR_NO_ERROR;
                good++;
        }



	/* Specific partition */
        if (partition != RD_KAFKA_PARTITION_UA) {
		rd_kafka_topic_rdlock(rkt);

                rktp = rd_kafka_toppar_get_avail(rkt, partition,
                                                 1/*ua on miss*/, &all_err);
                /* Concatenate tmpq onto partition queue. */
                if (likely(rktp != NULL)) {
                        if (good > 0)
                                (void)rd_atomic_add(&rkt->rkt_rk->
                                                    rk_producer.msg_cnt, good);

                        (void)rd_atomic_add(&rktp->rktp_c.msgs, good);
                        rd_kafka_toppar_concat_msgq(rktp, &tmpq);
                        rd_kafka_toppar_destroy(rktp);
                }
        }

	rd_kafka_topic_unlock(rkt);

        return good;
}

/**
 * Scan 'rkmq' for messages that have timed out and remove them from
 * 'rkmq' and add to 'timedout'.
 */
int rd_kafka_msgq_age_scan (rd_kafka_msgq_t *rkmq,
			    rd_kafka_msgq_t *timedout,
			    rd_ts_t now) {
	rd_kafka_msg_t *rkm, *tmp;
	int cnt = timedout->rkmq_msg_cnt;

	/* Assume messages are added in time sequencial order */
	TAILQ_FOREACH_SAFE(rkm, &rkmq->rkmq_msgs, rkm_link, tmp) {
		if (likely(rkm->rkm_ts_timeout > now))
			break;

		rd_kafka_msgq_deq(rkmq, rkm, 1);
		rd_kafka_msgq_enq(timedout, rkm);
	}

	return timedout->rkmq_msg_cnt - cnt;
}





int32_t rd_kafka_msg_partitioner_random (const rd_kafka_topic_t *rkt,
					 const void *key, size_t keylen,
					 int32_t partition_cnt,
					 void *rkt_opaque,
					 void *msg_opaque) {
	int32_t p = rd_jitter(0, partition_cnt-1);
	if (unlikely(!rd_kafka_topic_partition_available(rkt, p)))
		return rd_jitter(0, partition_cnt-1);
	else
		return p;
}

/**
 * Assigns a message to a topic partition using a partitioner.
 * Returns RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION or .._UNKNOWN_TOPIC if
 * partitioning failed, or 0 on success.
 */
int rd_kafka_msg_partitioner (rd_kafka_topic_t *rkt, rd_kafka_msg_t *rkm,
			      int do_lock) {
	int32_t partition;
	rd_kafka_toppar_t *rktp_new;
	rd_kafka_resp_err_t err;

	if (do_lock)
		rd_kafka_topic_rdlock(rkt);

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
                err = RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
		if (do_lock)
			rd_kafka_topic_unlock(rkt);
                return err;

        case RD_KAFKA_TOPIC_S_EXISTS:
                /* Topic exists in cluster. */

                /* Topic exists but has no partitions.
                 * This is usually an transient state following the
                 * auto-creation of a topic. */
                if (unlikely(rkt->rkt_partition_cnt == 0)) {
                        partition = RD_KAFKA_PARTITION_UA;
                        break;
                }

                /* Partition not assigned, run partitioner. */
                if (rkm->rkm_partition == RD_KAFKA_PARTITION_UA)
                        partition = rkt->rkt_conf.
                                partitioner(rkt,
                                            rkm->rkm_key->data,
                                            RD_KAFKAP_BYTES_LEN(rkm->
                                                                rkm_key),
                                            rkt->rkt_partition_cnt,
                                            rkt->rkt_conf.opaque,
                                            rkm->rkm_opaque);
                else
                        partition = rkm->rkm_partition;

                /* Check that partition exists. */
                if (partition >= rkt->rkt_partition_cnt) {
                        err = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
                        if (do_lock)
                                rd_kafka_topic_unlock(rkt);
                        return err;
                }
                break;

        default:
                rd_kafka_assert(rkt->rkt_rk, !*"NOTREACHED");
                break;
        }

	/* Get new partition */
	rktp_new = rd_kafka_toppar_get(rkt, partition, 0);

	if (unlikely(!rktp_new)) {
		/* Unknown topic or partition */
		if (rkt->rkt_state == RD_KAFKA_TOPIC_S_NOTEXISTS)
			err = RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
		else
			err = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

		if (do_lock)
			rd_kafka_topic_unlock(rkt);

		return  err;
	}

        (void)rd_atomic_add(&rktp_new->rktp_c.msgs, 1);

	/* Partition is available: enqueue msg on partition's queue */
	rd_kafka_toppar_enq_msg(rktp_new, rkm);
	if (do_lock)
		rd_kafka_topic_unlock(rkt);
	rd_kafka_toppar_destroy(rktp_new); /* from _get() */
	return 0;
}
