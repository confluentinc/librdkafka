/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012-2013, Magnus Edenhill
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
#include "rdthread.h"
#include "rdqueue.h"



void rd_fifoq_destroy (rd_fifoq_t *rfq) {
	rd_fifoq_elm_t *rfqe;

	rd_mutex_lock(&rfq->rfq_lock);
	while ((rfqe = TAILQ_FIRST(&rfq->rfq_q))) {
		TAILQ_REMOVE(&rfq->rfq_q, rfqe, rfqe_link);
		free(rfqe);
	}

	rd_mutex_unlock(&rfq->rfq_lock);
}

rd_fifoq_t *rd_fifoq_init (rd_fifoq_t *rfq) {
	if (!rfq)
		rfq = calloc(1, sizeof(*rfq));

	TAILQ_INIT(&rfq->rfq_q);
	rd_mutex_init(&rfq->rfq_lock);
	rd_cond_init(&rfq->rfq_cond, NULL);

	rfq->rfq_inited = 1;

	return rfq;
}

void rd_fifoq_set_max_size (rd_fifoq_t *rfq, int max_size, int taildrop) {
	rd_mutex_lock(&rfq->rfq_lock);
	rfq->rfq_max_size = max_size;
	rfq->rfq_taildrop = !!taildrop;
	rd_mutex_unlock(&rfq->rfq_lock);
}


/**
 * Adds 'ptr' to FIFO queue.
 * The optional '*ptr_purged' will be set to the purged element's ptr
 * if the max_size settings of the fifo has been exceeded; i.e., it will
 * contain the pushed-out element's ptr so that the application can
 * update that object's state.
 */

void rd_fifoq_add0 (rd_fifoq_t *rfq, void *ptr, void **ptr_purged) {
	rd_fifoq_elm_t *rfqe;

	if (ptr_purged)
		*ptr_purged = NULL;

	assert(rfq->rfq_inited);

	rfqe = malloc(sizeof(*rfqe));

	rfqe->rfqe_refcnt = 2; /* one for rfq, one for caller */
	rfqe->rfqe_ptr = ptr;

	rd_mutex_lock(&rfq->rfq_lock);

	if (rfq->rfq_max_size != 0 &&
	    rfq->rfq_cnt >= rfq->rfq_max_size) {
		rd_fifoq_elm_t *purge;
		/* Queue has reached max size, drop an entry. */

		if (rfq->rfq_taildrop)
			purge = TAILQ_LAST(&rfq->rfq_q, rd_fifoq_elm_head_s);
		else
			purge = TAILQ_FIRST(&rfq->rfq_q);

		if (ptr_purged)
			*ptr_purged = purge->rfqe_ptr;

		rfq->rfq_cnt--;
		TAILQ_REMOVE(&rfq->rfq_q, purge, rfqe_link);

		if (purge->rfqe_refcnt == 1) {
			/* Only fifoq's refcount remained,
			 * this entry is no longer desired on the fifo,
			 * ignore and remove it. */
			rd_fifoq_elm_release0(rfq, purge);
		}
		
	}

	TAILQ_INSERT_TAIL(&rfq->rfq_q, rfqe, rfqe_link);
	rfq->rfq_cnt++;
	rd_cond_signal(&rfq->rfq_cond);
	rd_mutex_unlock(&rfq->rfq_lock);
}




rd_fifoq_elm_t *rd_fifoq_pop0 (rd_fifoq_t *rfq, int nowait, int timeout_ms) {
	rd_fifoq_elm_t *rfqe;

	/* Pop the next valid element from the FIFO. */
	do {
		rd_mutex_lock(&rfq->rfq_lock);

		while (!(rfqe = TAILQ_FIRST(&rfq->rfq_q))) {
			if (nowait) {
				rd_mutex_unlock(&rfq->rfq_lock);
				return NULL;
			}

			if (timeout_ms) {
				if (rd_cond_timedwait_ms(&rfq->rfq_cond,
							 &rfq->rfq_lock,
							 timeout_ms)
				    == ETIMEDOUT) {
					rd_mutex_unlock(&rfq->rfq_lock);
					return NULL;
				}
			} else
				rd_cond_wait(&rfq->rfq_cond, &rfq->rfq_lock);
		}

		assert(rfq->rfq_cnt > 0);
		rfq->rfq_cnt--;
		TAILQ_REMOVE(&rfq->rfq_q, rfqe, rfqe_link);

		if (rfqe->rfqe_refcnt == 1) {
			/* Only fifoq's refcount remains,
			 * this entry is no longer desired on the fifo,
			 * ignore and remove it. */
			rd_fifoq_elm_release0(rfq, rfqe);
			continue;
		}

		break;

	} while (rfqe == NULL);
	
	rd_fifoq_elm_release0(rfq, rfqe);

	rd_mutex_unlock(&rfq->rfq_lock);

	return rfqe;
}


