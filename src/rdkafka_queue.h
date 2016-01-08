#pragma once

#include "rdkafka_op.h"

struct rd_kafka_q_s {
	mtx_t  rkq_lock;
	cnd_t  rkq_cond;
	struct rd_kafka_q_s *rkq_fwdq; /* Forwarded/Routed queue.
					* Used in place of this queue
					* for all operations. */
	TAILQ_HEAD(, rd_kafka_op_s) rkq_q;
	rd_atomic32_t rkq_qlen;      /* Number of entries in queue */
	rd_atomic64_t rkq_qsize;     /* Size of all entries in queue */
	rd_refcnt_t   rkq_refcnt;
        int           rkq_flags;
#define RD_KAFKA_Q_F_ALLOCATED  0x1  /* Allocated: rd_free on destroy */
#define RD_KAFKA_Q_F_READY      0x2  /* Queue is ready to be used.
                                      * Flag is cleared on destory */

        rd_kafka_t   *rkq_rk;
};



enum {
        _Q_CB_GLOBAL,   /* rd_kafka_poll() */
        _Q_CB_CONSUMER  /* rd_kafka_consumer_poll() */
};


void rd_kafka_q_init (rd_kafka_q_t *rkq, rd_kafka_t *rk);
rd_kafka_q_t *rd_kafka_q_new (rd_kafka_t *rk);
void rd_kafka_q_destroy_final (rd_kafka_q_t *rkq);
#define rd_kafka_q_keep(rkq) rd_refcnt_add(&(rkq)->rkq_refcnt)
#define rd_kafka_q_destroy(rkq) \
        rd_refcnt_destroywrapper(&(rkq)->rkq_refcnt, \
                                 rd_kafka_q_destroy_final(rkq))

/**
 * Reset a queue.
 * WARNING: All messages will be lost and leaked.
 */
static __inline RD_UNUSED
void rd_kafka_q_reset (rd_kafka_q_t *rkq) {
	TAILQ_INIT(&rkq->rkq_q);
	rd_atomic32_set(&rkq->rkq_qlen, 0);
	rd_atomic64_set(&rkq->rkq_qsize, 0);
}


/**
 * Disable a queue.
 * Attempting to enqueue messages to the queue will cause an assert.
 */
static __inline RD_UNUSED
void rd_kafka_q_disable (rd_kafka_q_t *rkq) {
        mtx_lock(&rkq->rkq_lock);
        rkq->rkq_flags &= ~RD_KAFKA_Q_F_READY;
        mtx_unlock(&rkq->rkq_lock);
}

/**
 * Forward 'srcq' to 'destq'
 */
void rd_kafka_q_fwd_set (rd_kafka_q_t *srcq, rd_kafka_q_t *destq);

/**
 * Enqueue the 'rko' op at the tail of the queue 'rkq'.
 * Returns 1 if op was enqueued or 0 if queue is disabled and
 * there was no replyq to enqueue on.
 *
 * Locality: any thread.
 */
static __inline RD_UNUSED
int rd_kafka_q_enq (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
        rd_kafka_assert(NULL, rd_refcnt_get(&rkq->rkq_refcnt) > 0);


	mtx_lock(&rkq->rkq_lock);
        if (unlikely(!(rkq->rkq_flags & RD_KAFKA_Q_F_READY))) {
                int r;

                /* Queue has been disabled, reply to and fail the rko. */
                mtx_unlock(&rkq->rkq_lock);

                r = rd_kafka_op_reply(rko, RD_KAFKA_RESP_ERR__DESTROY,
                                      rko->rko_payload, rko->rko_len,
                                      rko->rko_free_cb);
                if (r)
                        rko->rko_payload = NULL; /* Now owned by reply op */

                rd_kafka_op_destroy(rko);
                return r;
        }
	if (!rkq->rkq_fwdq) {
		TAILQ_INSERT_TAIL(&rkq->rkq_q, rko, rko_link);
		(void)rd_atomic32_add(&rkq->rkq_qlen, 1);
		(void)rd_atomic64_add(&rkq->rkq_qsize, rko->rko_len);
		cnd_signal(&rkq->rkq_cond);
	} else
		rd_kafka_q_enq(rkq->rkq_fwdq, rko);

	mtx_unlock(&rkq->rkq_lock);

        return 1;
}


/**
 * Dequeue 'rko' from queue 'rkq'.
 *
 * NOTE: rkq_lock MUST be held
 * Locality: any thread
 */
static __inline RD_UNUSED
void rd_kafka_q_deq0 (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
        rd_kafka_assert(NULL, rkq->rkq_flags & RD_KAFKA_Q_F_READY);
	TAILQ_REMOVE(&rkq->rkq_q, rko, rko_link);
	rd_atomic32_sub(&rkq->rkq_qlen, 1);
	rd_atomic64_sub(&rkq->rkq_qsize, rko->rko_len);
}

/**
 * Concat all elements of 'srcq' onto tail of 'rkq'.
 * 'rkq' will be be locked (if 'do_lock'==1), but 'srcq' will not.
 * NOTE: 'srcq' will be reset.
is not in a usable state after this call.
 *
 * Locality: any thread.
 */
static __inline RD_UNUSED
void rd_kafka_q_concat0 (rd_kafka_q_t *rkq, rd_kafka_q_t *srcq,
			 int do_lock) {
	if (do_lock)
		mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq && !srcq->rkq_fwdq) {
		TAILQ_CONCAT(&rkq->rkq_q, &srcq->rkq_q, rko_link);
		rd_atomic32_add(&rkq->rkq_qlen,
                                rd_atomic32_get(&srcq->rkq_qlen));
		rd_atomic64_add(&rkq->rkq_qsize,
                                rd_atomic64_get(&srcq->rkq_qsize));
		cnd_signal(&rkq->rkq_cond);

                rd_kafka_q_reset(srcq);
	} else
		rd_kafka_q_concat0(rkq->rkq_fwdq ? rkq->rkq_fwdq : rkq,
				   srcq->rkq_fwdq ? srcq->rkq_fwdq : srcq,
				   rkq->rkq_fwdq ? do_lock : 0);
	if (do_lock)
		mtx_unlock(&rkq->rkq_lock);
}

#define rd_kafka_q_concat(dstq,srcq) rd_kafka_q_concat0(dstq,srcq,1/*lock*/)


/**
 * Prepend all elements of 'srcq' onto head of 'rkq'.
 * 'rkq' will be be locked (if 'do_lock'==1), but 'srcq' will not.
 *
 * NOTE: 'srcq' is not in a usable state after this call.
 *
 * Locality: any thread.
 */
static __inline RD_UNUSED
void rd_kafka_q_prepend0 (rd_kafka_q_t *rkq, rd_kafka_q_t *srcq,
                          int do_lock) {
	if (do_lock)
		mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq && !srcq->rkq_fwdq) {
                /* Concat rkq on srcq */
                TAILQ_CONCAT(&srcq->rkq_q, &rkq->rkq_q, rko_link);
                /* Move srcq to rkq */
                TAILQ_MOVE(&rkq->rkq_q, &srcq->rkq_q, rko_link);
		rd_atomic32_add(&srcq->rkq_qlen,
                                rd_atomic32_get(&rkq->rkq_qlen));
		rd_atomic64_add(&srcq->rkq_qsize,
                                rd_atomic64_get(&rkq->rkq_qsize));
	} else
		rd_kafka_q_prepend0(rkq->rkq_fwdq ? rkq->rkq_fwdq : rkq,
                                    srcq->rkq_fwdq ? srcq->rkq_fwdq : srcq,
                                    rkq->rkq_fwdq ? do_lock : 0);
	if (do_lock)
		mtx_unlock(&rkq->rkq_lock);
}

#define rd_kafka_q_prepend(dstq,srcq) rd_kafka_q_prepend0(dstq,srcq,1/*lock*/)


/* Returns the number of elements in the queue */
static __inline RD_UNUSED
int rd_kafka_q_len (rd_kafka_q_t *rkq) {
	int qlen;
	mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq)
		qlen = rd_atomic32_get(&rkq->rkq_qlen);
	else
		qlen = rd_kafka_q_len(rkq->rkq_fwdq);
	mtx_unlock(&rkq->rkq_lock);
	return qlen;
}

/* Returns the total size of elements in the queue */
static __inline RD_UNUSED
uint64_t rd_kafka_q_size (rd_kafka_q_t *rkq) {
	uint64_t sz;
	mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq)
		sz = rd_atomic64_get(&rkq->rkq_qsize);
	else
		sz = rd_kafka_q_size(rkq->rkq_fwdq);
	mtx_unlock(&rkq->rkq_lock);
	return sz;
}


rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms,
                               int32_t version);
int rd_kafka_q_serve (rd_kafka_q_t *rkq, int timeout_ms,
                      int max_cnt, int cb_type,
                      int (*callback) (rd_kafka_t *rk, rd_kafka_op_t *rko,
                                       int cb_type,
                                              void *opaque),
                      void *opaque);

int  rd_kafka_q_purge (rd_kafka_q_t *rkq);
void rd_kafka_q_purge_toppar_version (rd_kafka_q_t *rkq,
                                      rd_kafka_toppar_t *rktp, int version);

int rd_kafka_q_move_cnt (rd_kafka_q_t *dstq, rd_kafka_q_t *srcq,
			 int cnt, int do_locks);

int rd_kafka_q_serve_rkmessages (rd_kafka_q_t *rkq, int timeout_ms,
                                 rd_kafka_message_t **rkmessages,
                                 size_t rkmessages_size);
rd_kafka_message_t *rd_kafka_message_get (rd_kafka_op_t *rko);
rd_kafka_message_t *rd_kafka_message_new (void);

rd_kafka_resp_err_t rd_kafka_q_wait_result (rd_kafka_q_t *rkq, int timeout_ms);


/* Public interface */
struct rd_kafka_queue_s {
	rd_kafka_q_t rkqu_q;
        rd_kafka_t  *rkqu_rk;
};




#if 0

struct rd_kafka_qpollq {
        rd_kafka_q_t *rkq;
        void         *opaque;
};
typedef struct rd_kafka_qpoll_s {
        mtx_t         *rkqp_lock;
        cnd_t         *rkq_cond;
        uint64_t       rkqp_events;
        struct rd_kafka_qpollq *rkqp_qs;
        int            rkqp_qsize;
        int            rkqp_qcnt;
} rd_kafka_qpoll_t;


void rd_kafka_qpoll_add_q (rd_kafka_qpoll_t *rkqp,
                           rd_kafka_q_t *rkq, void *opaque) {
        int i;

        if (rkqp->rkqp_qcnt == rkqp->rkqp_qsize) {
                rkqp->rkqp_qsize = (rkqp->rkqp_qsize + 16) * 2;
                rkqp->rkqp_qs = rd_realloc(rkqp->rkqp_qs,
                                           sizeof(*rkqp->rkqp_qs) *
                                           rkqp->rkqp_qsize);
        }

        i = rkqp->rkqp_qcnt++;
        rkq->rkq_pollid = i;
        rkqp->rkqp_qs[i].rkq = rkq;
        rkqp->rkqp_qs[i].opaque = opaque;
}

void rd_kafka_qpoll_del_q (rd_kafka_qpoll_t *rkqp, rd_kafka_q_t *rkq) {
        rkqp->rkqp_qs[i].rkq = rkq;
        rkqp->rkqp_qs[i].opaque = opaque;
}


int rd_kafka_qpoll (rd_kafka_qpoll_t *rkqp, int timeout_ms,
                    struct rd_kafka_qpollq *pollqs, int pollq_size) {
}
#endif

