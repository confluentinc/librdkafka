#pragma once

#include "rdkafka_op.h"
#include "rdkafka_int.h"

#ifdef _MSC_VER
#include <io.h> /* for _write() */
#endif


TAILQ_HEAD(rd_kafka_op_tailq, rd_kafka_op_s);

struct rd_kafka_q_s {
	mtx_t  rkq_lock;
	cnd_t  rkq_cond;
	struct rd_kafka_q_s *rkq_fwdq; /* Forwarded/Routed queue.
					* Used in place of this queue
					* for all operations. */

	struct rd_kafka_op_tailq rkq_q;  /* TAILQ_HEAD(, rd_kafka_op_s) */
	int           rkq_qlen;      /* Number of entries in queue */
        int64_t       rkq_qsize;     /* Size of all entries in queue */
        int           rkq_refcnt;
        int           rkq_flags;
#define RD_KAFKA_Q_F_ALLOCATED  0x1  /* Allocated: rd_free on destroy */
#define RD_KAFKA_Q_F_READY      0x2  /* Queue is ready to be used.
                                      * Flag is cleared on destroy */

        rd_kafka_t   *rkq_rk;
	struct rd_kafka_q_io *rkq_qio;   /* FD-based application signalling */

#if ENABLE_DEVEL
	char rkq_name[64];       /* Debugging: queue name (FUNC:LINE) */
#else
	const char *rkq_name;    /* Debugging: queue name (FUNC) */
#endif
};


/* FD-based application signalling state holder. */
struct rd_kafka_q_io {
	int    fd;
	void  *payload;
	size_t size;
};



/**
 * @return true if queue is ready/enabled, else false.
 * @remark queue luck must be held by caller (if applicable)
 */
static RD_INLINE RD_UNUSED
int rd_kafka_q_ready (rd_kafka_q_t *rkq) {
	return rkq->rkq_flags & RD_KAFKA_Q_F_READY;
}


enum {
        _Q_CB_GLOBAL,   /* rd_kafka_poll() */
        _Q_CB_CONSUMER, /* rd_kafka_consumer_poll() */
	_Q_CB_EVENT     /* return event */
};


void rd_kafka_q_init (rd_kafka_q_t *rkq, rd_kafka_t *rk);
rd_kafka_q_t *rd_kafka_q_new0 (rd_kafka_t *rk, const char *func, int line);
#define rd_kafka_q_new(rk) rd_kafka_q_new0(rk,__FUNCTION__,__LINE__)
void rd_kafka_q_destroy_final (rd_kafka_q_t *rkq);


static RD_INLINE RD_UNUSED
rd_kafka_q_t *rd_kafka_q_keep (rd_kafka_q_t *rkq) {
        mtx_lock(&rkq->rkq_lock);
        rkq->rkq_refcnt++;
        mtx_unlock(&rkq->rkq_lock);
	return rkq;
}

static RD_INLINE RD_UNUSED
rd_kafka_q_t *rd_kafka_q_keep_nolock (rd_kafka_q_t *rkq) {
        rkq->rkq_refcnt++;
	return rkq;
}


/**
 * @returns the queue's name (used for debugging)
 */
static RD_INLINE RD_UNUSED
const char *rd_kafka_q_name (rd_kafka_q_t *rkq) {
	return rkq->rkq_name;
}

/**
 * @returns the final destination queue name (after forwarding)
 * @remark rkq MUST NOT be locked
 */
static RD_INLINE RD_UNUSED
const char *rd_kafka_q_dest_name (rd_kafka_q_t *rkq) {
	const char *ret;
	mtx_lock(&rkq->rkq_lock);
	if (rkq->rkq_fwdq)
		ret = rd_kafka_q_dest_name(rkq->rkq_fwdq);
	else
		ret = rd_kafka_q_name(rkq);
	mtx_unlock(&rkq->rkq_lock);
	return ret;
}


static RD_INLINE RD_UNUSED
void rd_kafka_q_destroy (rd_kafka_q_t *rkq) {
        int do_delete = 0;

        mtx_lock(&rkq->rkq_lock);
        rd_kafka_assert(NULL, rkq->rkq_refcnt > 0);
        do_delete = !--rkq->rkq_refcnt;
        mtx_unlock(&rkq->rkq_lock);

        if (unlikely(do_delete))
                rd_kafka_q_destroy_final(rkq);
}


/**
 * Reset a queue.
 * WARNING: All messages will be lost and leaked.
 * NOTE: No locking is performed.
 */
static RD_INLINE RD_UNUSED
void rd_kafka_q_reset (rd_kafka_q_t *rkq) {
	TAILQ_INIT(&rkq->rkq_q);
        rd_dassert(TAILQ_EMPTY(&rkq->rkq_q));
        rkq->rkq_qlen = 0;
        rkq->rkq_qsize = 0;
}


/**
 * Disable a queue.
 * Attempting to enqueue messages to the queue will destroy them.
 */
static RD_INLINE RD_UNUSED
void rd_kafka_q_disable0 (rd_kafka_q_t *rkq, int do_lock) {
        if (do_lock)
                mtx_lock(&rkq->rkq_lock);
        rkq->rkq_flags &= ~RD_KAFKA_Q_F_READY;
        if (do_lock)
                mtx_unlock(&rkq->rkq_lock);
}
#define rd_kafka_q_disable(rkq) rd_kafka_q_disable0(rkq, 1/*lock*/)

/**
 * Forward 'srcq' to 'destq'
 */
void rd_kafka_q_fwd_set0 (rd_kafka_q_t *srcq, rd_kafka_q_t *destq, int do_lock);
#define rd_kafka_q_fwd_set(S,D) rd_kafka_q_fwd_set0(S,D,1/*lock*/)


/**
 * @returns true if queue is forwarded, else false.
 *
 * @remark Thread-safe.
 */
static RD_INLINE RD_UNUSED int rd_kafka_q_is_fwded (rd_kafka_q_t *rkq) {
	int r;
	mtx_lock(&rkq->rkq_lock);
	r = rkq->rkq_fwdq ? 1 : 0;
	mtx_unlock(&rkq->rkq_lock);
	return r;
}



/**
 * @brief Trigger an IO event for this queue.
 *
 * @remark Queue MUST be locked
 */
static RD_INLINE RD_UNUSED
void rd_kafka_q_io_event (rd_kafka_q_t *rkq) {
	ssize_t r;

	if (likely(!rkq->rkq_qio))
		return;

#ifdef _MSC_VER
	r = _write(rkq->rkq_qio->fd, rkq->rkq_qio->payload, (int)rkq->rkq_qio->size);
#else
        r = write(rkq->rkq_qio->fd, rkq->rkq_qio->payload, rkq->rkq_qio->size);
#endif
	if (r == -1) {
		fprintf(stderr,
			"[ERROR:librdkafka:rd_kafka_q_io_event: "
			"write(%d,..,%d) failed on queue %p \"%s\": %s: "
			"disabling further IO events]\n",
			rkq->rkq_qio->fd, (int)rkq->rkq_qio->size,
			rkq, rd_kafka_q_name(rkq), rd_strerror(errno));
		/* FIXME: Log this, somehow */
		rd_free(rkq->rkq_qio);
		rkq->rkq_qio = NULL;
	}
}


/**
 * @brief Enqueue the 'rko' op at the tail of the queue 'rkq'.
 *
 * The provided 'rko' is either enqueued or destroyed.
 *
 * @returns 1 if op was enqueued or 0 if queue is disabled and
 * there was no replyq to enqueue on in which case the rko is destroyed.
 *
 * Locality: any thread.
 */
static RD_INLINE RD_UNUSED
int rd_kafka_q_enq (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
	rd_kafka_q_t *fwdq;

	mtx_lock(&rkq->rkq_lock);

        rd_dassert(rkq->rkq_refcnt > 0);

        if (unlikely(!(rkq->rkq_flags & RD_KAFKA_Q_F_READY))) {

                /* Queue has been disabled, reply to and fail the rko. */
                mtx_unlock(&rkq->rkq_lock);

                return rd_kafka_op_reply(rko, RD_KAFKA_RESP_ERR__DESTROY);
        }
	if (!(fwdq = rkq->rkq_fwdq)) {
		TAILQ_INSERT_TAIL(&rkq->rkq_q, rko, rko_link);
                rkq->rkq_qlen++;
                rkq->rkq_qsize += rko->rko_len;
		cnd_signal(&rkq->rkq_cond);
		if (rkq->rkq_qlen == 1)
			rd_kafka_q_io_event(rkq);
		mtx_unlock(&rkq->rkq_lock);
	} else {
		rd_kafka_q_keep(fwdq);
		mtx_unlock(&rkq->rkq_lock);
		rd_kafka_q_enq(fwdq, rko);
		rd_kafka_q_destroy(fwdq);
	}

        return 1;
}


/**
 * Dequeue 'rko' from queue 'rkq'.
 *
 * NOTE: rkq_lock MUST be held
 * Locality: any thread
 */
static RD_INLINE RD_UNUSED
void rd_kafka_q_deq0 (rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
        rd_dassert(rkq->rkq_flags & RD_KAFKA_Q_F_READY);
	rd_dassert(rkq->rkq_qlen > 0 &&
                   rkq->rkq_qsize >= (int64_t)rko->rko_len);

        TAILQ_REMOVE(&rkq->rkq_q, rko, rko_link);
        rkq->rkq_qlen--;
        rkq->rkq_qsize -= rko->rko_len;
}

/**
 * Concat all elements of 'srcq' onto tail of 'rkq'.
 * 'rkq' will be be locked (if 'do_lock'==1), but 'srcq' will not.
 * NOTE: 'srcq' will be reset.
 *
 * Locality: any thread.
 *
 * @returns 0 if operation was performed or -1 if rkq is disabled.
 */
static RD_INLINE RD_UNUSED
int rd_kafka_q_concat0 (rd_kafka_q_t *rkq, rd_kafka_q_t *srcq, int do_lock) {
	int r = 0;
	if (do_lock)
		mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq && !srcq->rkq_fwdq) {
                rd_dassert(TAILQ_EMPTY(&srcq->rkq_q) ||
                           srcq->rkq_qlen > 0);
		if (unlikely(!(rkq->rkq_flags & RD_KAFKA_Q_F_READY))) {
                        if (do_lock)
                                mtx_unlock(&rkq->rkq_lock);
			return -1;
		}
		TAILQ_CONCAT(&rkq->rkq_q, &srcq->rkq_q, rko_link);
		if (rkq->rkq_qlen == 0 && srcq->rkq_qlen > 0)
			rd_kafka_q_io_event(rkq);
                rkq->rkq_qlen += srcq->rkq_qlen;
                rkq->rkq_qsize += srcq->rkq_qsize;
		cnd_signal(&rkq->rkq_cond);

                rd_kafka_q_reset(srcq);
	} else
		r = rd_kafka_q_concat0(rkq->rkq_fwdq ? rkq->rkq_fwdq : rkq,
				       srcq->rkq_fwdq ? srcq->rkq_fwdq : srcq,
				       rkq->rkq_fwdq ? do_lock : 0);
	if (do_lock)
		mtx_unlock(&rkq->rkq_lock);

	return r;
}

#define rd_kafka_q_concat(dstq,srcq) rd_kafka_q_concat0(dstq,srcq,1/*lock*/)


/**
 * Prepend all elements of 'srcq' onto head of 'rkq'.
 * 'rkq' will be be locked (if 'do_lock'==1), but 'srcq' will not.
 * 'srcq' will be reset.
 *
 * Locality: any thread.
 */
static RD_INLINE RD_UNUSED
void rd_kafka_q_prepend0 (rd_kafka_q_t *rkq, rd_kafka_q_t *srcq,
                          int do_lock) {
	if (do_lock)
		mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq && !srcq->rkq_fwdq) {
                /* Concat rkq on srcq */
                TAILQ_CONCAT(&srcq->rkq_q, &rkq->rkq_q, rko_link);
                /* Move srcq to rkq */
                TAILQ_MOVE(&rkq->rkq_q, &srcq->rkq_q, rko_link);
		if (rkq->rkq_qlen == 0 && srcq->rkq_qlen > 0)
			rd_kafka_q_io_event(rkq);
                rkq->rkq_qlen += srcq->rkq_qlen;
                rkq->rkq_qsize += srcq->rkq_qsize;

                rd_kafka_q_reset(srcq);
	} else
		rd_kafka_q_prepend0(rkq->rkq_fwdq ? rkq->rkq_fwdq : rkq,
                                    srcq->rkq_fwdq ? srcq->rkq_fwdq : srcq,
                                    rkq->rkq_fwdq ? do_lock : 0);
	if (do_lock)
		mtx_unlock(&rkq->rkq_lock);
}

#define rd_kafka_q_prepend(dstq,srcq) rd_kafka_q_prepend0(dstq,srcq,1/*lock*/)


/* Returns the number of elements in the queue */
static RD_INLINE RD_UNUSED
int rd_kafka_q_len (rd_kafka_q_t *rkq) {
	int qlen;
	mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq)
		qlen = rkq->rkq_qlen;
	else
		qlen = rd_kafka_q_len(rkq->rkq_fwdq);
	mtx_unlock(&rkq->rkq_lock);
	return qlen;
}

/* Returns the total size of elements in the queue */
static RD_INLINE RD_UNUSED
uint64_t rd_kafka_q_size (rd_kafka_q_t *rkq) {
	uint64_t sz;
	mtx_lock(&rkq->rkq_lock);
	if (!rkq->rkq_fwdq)
		sz = rkq->rkq_qsize;
	else
		sz = rd_kafka_q_size(rkq->rkq_fwdq);
	mtx_unlock(&rkq->rkq_lock);
	return sz;
}


/* Construct temporary on-stack replyq with increased Q refcount and
 * optional VERSION. */
#if ENABLE_DEVEL
#define RD_KAFKA_REPLYQ(Q,VERSION) \
	(rd_kafka_replyq_t){rd_kafka_q_keep(Q), VERSION, \
			rd_strdup(__FUNCTION__) }
#else
#define RD_KAFKA_REPLYQ(Q,VERSION) \
	(rd_kafka_replyq_t){rd_kafka_q_keep(Q), VERSION}
#endif

/* Construct temporary on-stack replyq for indicating no replyq. */
#if ENABLE_DEVEL
#define RD_KAFKA_NO_REPLYQ (rd_kafka_replyq_t){NULL, 0, NULL}
#else
#define RD_KAFKA_NO_REPLYQ (rd_kafka_replyq_t){NULL, 0}
#endif

/**
 * Set up replyq.
 * Q refcnt is increased.
 */
static RD_INLINE RD_UNUSED void
rd_kafka_set_replyq (rd_kafka_replyq_t *replyq,
		     rd_kafka_q_t *rkq, int32_t version) {
	replyq->q = rkq ? rd_kafka_q_keep(rkq) : NULL;
	replyq->version = version;
#if ENABLE_DEVEL
	replyq->_id = strdup(__FUNCTION__);
#endif
}

/**
 * Set rko's replyq with an optional version (versionptr != NULL).
 * Q refcnt is increased.
 */
static RD_INLINE RD_UNUSED void
rd_kafka_op_set_replyq (rd_kafka_op_t *rko, rd_kafka_q_t *rkq,
			rd_atomic32_t *versionptr) {
	rd_kafka_set_replyq(&rko->rko_replyq, rkq,
			    versionptr ? rd_atomic32_get(versionptr) : 0);
}

/* Set reply rko's version from replyq's version */
#define rd_kafka_op_get_reply_version(REPLY_RKO, ORIG_RKO) do {		\
		(REPLY_RKO)->rko_version = (ORIG_RKO)->rko_replyq.version; \
	} while (0)


/* Clear replyq holder without decreasing any .q references. */
static RD_INLINE RD_UNUSED void
rd_kafka_replyq_clear (rd_kafka_replyq_t *replyq) {
	memset(replyq, 0, sizeof(*replyq));
}


/**
 * Clear replyq holder and destroy any .q references.
 */
static RD_INLINE RD_UNUSED void
rd_kafka_replyq_destroy (rd_kafka_replyq_t *replyq) {
	if (replyq->q)
		rd_kafka_q_destroy(replyq->q);
#if ENABLE_DEVEL
	if (replyq->_id) {
		rd_free(replyq->_id);
		replyq->_id = NULL;
	}
#endif
	rd_kafka_replyq_clear(replyq);
}


/**
 * @brief Wrapper for rd_kafka_q_enq() that takes a replyq,
 *        steals its queue reference, enqueues the op with the replyq version,
 *        and then destroys the queue reference.
 *
 *        If \p version is non-zero it will be updated, else replyq->version.
 *
 * @returns Same as rd_kafka_q_enq()
 */
static RD_INLINE RD_UNUSED int
rd_kafka_replyq_enq (rd_kafka_replyq_t *replyq, rd_kafka_op_t *rko,
		     int version) {
	rd_kafka_q_t *rkq = replyq->q;
	int r;

	if (version)
		rko->rko_version = version;
	else
		rko->rko_version = replyq->version;

	/* The replyq queue reference is done after we've enqueued the rko
	 * so clear it here. */
	replyq->q = NULL;

#if ENABLE_DEVEL
	if (replyq->_id) {
		rd_free(replyq->_id);
		replyq->_id = NULL;
	}
#endif

	/* Retain replyq->version since it is used by buf_callback
	 * when dispatching the callback. */

	r = rd_kafka_q_enq(rkq, rko);

	rd_kafka_q_destroy(rkq);

	return r;
}



rd_kafka_op_t *rd_kafka_q_pop_serve (rd_kafka_q_t *rkq, int timeout_ms,
				     int32_t version, int cb_type,
				     int (*callback) (rd_kafka_t *rk,
						      rd_kafka_op_t *rko,
						      int cb_type,
						      void *opaque),
				     void *opaque);
rd_kafka_op_t *rd_kafka_q_pop (rd_kafka_q_t *rkq, int timeout_ms,
                               int32_t version);
int rd_kafka_q_serve (rd_kafka_q_t *rkq, int timeout_ms,
                      int max_cnt, int cb_type,
                      int (*callback) (rd_kafka_t *rk, rd_kafka_op_t *rko,
                                       int cb_type,
                                              void *opaque),
                      void *opaque);

int  rd_kafka_q_purge0 (rd_kafka_q_t *rkq, int do_lock);
#define rd_kafka_q_purge(rkq) rd_kafka_q_purge0(rkq, 1/*lock*/)
void rd_kafka_q_purge_toppar_version (rd_kafka_q_t *rkq,
                                      rd_kafka_toppar_t *rktp, int version);

int rd_kafka_q_move_cnt (rd_kafka_q_t *dstq, rd_kafka_q_t *srcq,
			 int cnt, int do_locks);

int rd_kafka_q_serve_rkmessages (rd_kafka_q_t *rkq, int timeout_ms,
                                 rd_kafka_message_t **rkmessages,
                                 size_t rkmessages_size);
rd_kafka_message_t *rd_kafka_message_get (rd_kafka_op_t *rko);
rd_kafka_message_t *rd_kafka_message_get_from_rkm (rd_kafka_op_t *rko,
						   rd_kafka_msg_t *rkm);
rd_kafka_message_t *rd_kafka_message_new (void);

rd_kafka_resp_err_t rd_kafka_q_wait_result (rd_kafka_q_t *rkq, int timeout_ms);

int rd_kafka_q_apply (rd_kafka_q_t *rkq,
                      int (*callback) (rd_kafka_q_t *rkq, rd_kafka_op_t *rko,
                                       void *opaque),
                      void *opaque);

void rd_kafka_q_fix_offsets (rd_kafka_q_t *rkq, int64_t min_offset,
			     int64_t base_offset);

/**
 * @returns the last op in the queue matching \p op_type and \p allow_err (bool)
 * @remark The \p rkq must be properly locked before this call, the returned rko
 *         is not removed from the queue and may thus not be held for longer
 *         than the lock is held.
 */
static RD_INLINE RD_UNUSED
rd_kafka_op_t *rd_kafka_q_last (rd_kafka_q_t *rkq, rd_kafka_op_type_t op_type,
				int allow_err) {
	rd_kafka_op_t *rko;
	TAILQ_FOREACH_REVERSE(rko, &rkq->rkq_q, rd_kafka_op_tailq, rko_link) {
		if (rko->rko_type == op_type &&
		    (allow_err || !rko->rko_err))
			return rko;
	}

	return NULL;
}

/* Public interface */
struct rd_kafka_queue_s {
	rd_kafka_q_t *rkqu_q;
        rd_kafka_t   *rkqu_rk;
};



extern int RD_TLS rd_kafka_yield_thread;
