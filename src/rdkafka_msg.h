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

#include "rdsysqueue.h"

#include "rdkafka_proto.h"


/**
 * Message.Attributes
 */
#define RD_KAFKA_MSG_ATTR_GZIP             (1 << 0)
#define RD_KAFKA_MSG_ATTR_SNAPPY           (1 << 1)
#define RD_KAFKA_MSG_ATTR_LZ4              (1 << 2)
#define RD_KAFKA_MSG_ATTR_COMPRESSION_MASK 0x7
#define RD_KAFKA_MSG_ATTR_CREATE_TIME      (0 << 3)
#define RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME  (1 << 3)


typedef struct rd_kafka_msg_s {
	TAILQ_ENTRY(rd_kafka_msg_s)  rkm_link;
	int        rkm_flags;
	size_t     rkm_len;
	void      *rkm_payload;
	void      *rkm_opaque;
	int32_t    rkm_partition;  /* partition specified */
	rd_kafkap_bytes_t *rkm_key;
        int64_t    rkm_offset;
	int64_t    rkm_timestamp;  /* Message format V1.
				    * Meaning of timestamp depends on
				    * message Attribute LogAppendtime (broker)
				    * or CreateTime (producer).
				    * Unit is milliseconds since epoch (UTC).*/
	rd_ts_t    rkm_ts_timeout;
} rd_kafka_msg_t;

TAILQ_HEAD(rd_kafka_msg_head_s, rd_kafka_msg_s);

typedef struct rd_kafka_msgq_s {
	TAILQ_HEAD(, rd_kafka_msg_s) rkmq_msgs;
	rd_atomic32_t rkmq_msg_cnt;
	rd_atomic64_t rkmq_msg_bytes;
} rd_kafka_msgq_t;

#define RD_KAFKA_MSGQ_INITIALIZER(rkmq) \
	{ .rkmq_msgs = TAILQ_HEAD_INITIALIZER((rkmq).rkmq_msgs) }

#define RD_KAFKA_MSGQ_FOREACH(elm,head) \
	TAILQ_FOREACH(elm, &(head)->rkmq_msgs, rkm_link)

/**
 * Returns the number of messages in the specified queue.
 */
static RD_INLINE RD_UNUSED int rd_kafka_msgq_len (rd_kafka_msgq_t *rkmq) {
	return (int)rd_atomic32_get(&rkmq->rkmq_msg_cnt);
}


void rd_kafka_msg_destroy (rd_kafka_t *rk, rd_kafka_msg_t *rkm);

int rd_kafka_msg_new (rd_kafka_itopic_t *rkt, int32_t force_partition,
		      int msgflags,
		      char *payload, size_t len,
		      const void *keydata, size_t keylen,
		      void *msg_opaque);


static RD_INLINE RD_UNUSED void rd_kafka_msgq_init (rd_kafka_msgq_t *rkmq) {
	TAILQ_INIT(&rkmq->rkmq_msgs);
	rd_atomic32_set(&rkmq->rkmq_msg_cnt, 0);
	rd_atomic64_set(&rkmq->rkmq_msg_bytes, 0);
}

/**
 * Concat all elements of 'src' onto tail of 'dst'.
 * 'src' will be cleared.
 * Proper locks for 'src' and 'dst' must be held.
 */
static RD_INLINE RD_UNUSED void rd_kafka_msgq_concat (rd_kafka_msgq_t *dst,
						   rd_kafka_msgq_t *src) {
	TAILQ_CONCAT(&dst->rkmq_msgs, &src->rkmq_msgs, rkm_link);
	(void)rd_atomic32_add(&dst->rkmq_msg_cnt, rd_atomic32_get(&src->rkmq_msg_cnt));
	(void)rd_atomic64_add(&dst->rkmq_msg_bytes, rd_atomic64_get(&src->rkmq_msg_bytes));
	rd_kafka_msgq_init(src);
}

/**
 * Move queue 'src' to 'dst' (overwrites dst)
 * Source will be cleared.
 */
static RD_INLINE RD_UNUSED void rd_kafka_msgq_move (rd_kafka_msgq_t *dst,
						 rd_kafka_msgq_t *src) {
	TAILQ_MOVE(&dst->rkmq_msgs, &src->rkmq_msgs, rkm_link);
	rd_atomic32_set(&dst->rkmq_msg_cnt, rd_atomic32_get(&src->rkmq_msg_cnt));
	rd_atomic64_set(&dst->rkmq_msg_bytes, rd_atomic64_get(&src->rkmq_msg_bytes));
	rd_kafka_msgq_init(src);
}


/**
 * rd_free all msgs in msgq and reinitialize the msgq.
 */
static RD_INLINE RD_UNUSED void rd_kafka_msgq_purge (rd_kafka_t *rk,
                                                    rd_kafka_msgq_t *rkmq) {
	rd_kafka_msg_t *rkm, *next;

	next = TAILQ_FIRST(&rkmq->rkmq_msgs);
	while (next) {
		rkm = next;
		next = TAILQ_NEXT(next, rkm_link);

		rd_kafka_msg_destroy(rk, rkm);
	}

	rd_kafka_msgq_init(rkmq);
}


/**
 * Remove message from message queue
 */
static RD_INLINE RD_UNUSED 
rd_kafka_msg_t *rd_kafka_msgq_deq (rd_kafka_msgq_t *rkmq,
				   rd_kafka_msg_t *rkm,
				   int do_count) {
	if (likely(do_count)) {
		rd_kafka_assert(NULL, rd_atomic32_get(&rkmq->rkmq_msg_cnt) > 0);
		rd_kafka_assert(NULL, rd_atomic64_get(&rkmq->rkmq_msg_bytes) >= (int64_t)rkm->rkm_len);
		(void)rd_atomic32_sub(&rkmq->rkmq_msg_cnt, 1);
		(void)rd_atomic64_sub(&rkmq->rkmq_msg_bytes, rkm->rkm_len);
	}

	TAILQ_REMOVE(&rkmq->rkmq_msgs, rkm, rkm_link);

	return rkm;
}

static RD_INLINE RD_UNUSED
rd_kafka_msg_t *rd_kafka_msgq_pop (rd_kafka_msgq_t *rkmq) {
	rd_kafka_msg_t *rkm;

	if (((rkm = TAILQ_FIRST(&rkmq->rkmq_msgs))))
		rd_kafka_msgq_deq(rkmq, rkm, 1);

	return rkm;
}

/**
 * Insert message at head of message queue.
 */
static RD_INLINE RD_UNUSED void rd_kafka_msgq_insert (rd_kafka_msgq_t *rkmq,
						   rd_kafka_msg_t *rkm) {
	TAILQ_INSERT_HEAD(&rkmq->rkmq_msgs, rkm, rkm_link);
	(void)rd_atomic32_add(&rkmq->rkmq_msg_cnt, 1);
	(void)rd_atomic64_add(&rkmq->rkmq_msg_bytes, rkm->rkm_len);
}

/**
 * Append message to tail of message queue.
 */
static RD_INLINE RD_UNUSED void rd_kafka_msgq_enq (rd_kafka_msgq_t *rkmq,
						rd_kafka_msg_t *rkm) {
	TAILQ_INSERT_TAIL(&rkmq->rkmq_msgs, rkm, rkm_link);
	(void)rd_atomic32_add(&rkmq->rkmq_msg_cnt, 1);
	(void)rd_atomic64_add(&rkmq->rkmq_msg_bytes, rkm->rkm_len);
}


/**
 * Scans a message queue for timed out messages and removes them from
 * 'rkmq' and adds them to 'timedout', returning the number of timed out
 * messages.
 * 'timedout' must be initialized.
 */
int rd_kafka_msgq_age_scan (rd_kafka_msgq_t *rkmq,
			    rd_kafka_msgq_t *timedout,
			    rd_ts_t now);


int rd_kafka_msg_partitioner (rd_kafka_itopic_t *rkt, rd_kafka_msg_t *rkm,
			      int do_lock);
