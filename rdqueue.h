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

#pragma once

#include "rd.h"
#include "rdtypes.h"
#include "rdsysqueue.h"




/**
 * Thread-safe FIFO queues.
 * Typical usage is for work-queues (see tests/0001-fifoq.c for an example)
 *
 *
 * It is up the object code to properly lock the object itself.
 *
 *
 * Usage:
 *
 *   Caller thread:
 *       -- Add to fifoq:
 *       rd_fifoq_add(&my_fifoq, myobj);
 *
 *   In worker thread:
 *       -- Dequeue and process:
 *       while (1) {
 *         rfqe = rd_fifoq_pop_wait(&my_fifoq);
 *         myobj = rfqe->ptr;
 *         perform_work(myobj);
 *         rd_fifoq_elm_release(rfqe);
 *       }
 */

typedef struct rd_fifoq_elm_s {
	TAILQ_ENTRY(rd_fifoq_elm_s) rfqe_link;
	int         rfqe_refcnt;
	void       *rfqe_ptr;
} rd_fifoq_elm_t;


TAILQ_HEAD(rd_fifoq_elm_head_s, rd_fifoq_elm_s);

typedef struct rd_fifoq_s {
	TAILQ_HEAD(, rd_fifoq_elm_s) rfq_q;
	rd_mutex_t  rfq_lock;
	rd_cond_t   rfq_cond;
	int         rfq_cnt;
	int         rfq_max_size;
	int         rfq_taildrop;
	int         rfq_inited;
} rd_fifoq_t;

void        rd_fifoq_destroy (rd_fifoq_t *rfg);
rd_fifoq_t *rd_fifoq_init (rd_fifoq_t *rfq);

#define RD_FIFOQ_INITIALIZER(rfq)			\
	{						\
	.rfq_q = TAILQ_HEAD_INITIALIZER((rfq).rfq_q),	\
		.rfq_lock = RD_MUTEX_INITIALIZER,	\
		.rfq_cond = RD_COND_INITIALIZER,	\
		.rfq_inited = 1				\
			}
		

void rd_fifoq_set_max_size (rd_fifoq_t *rfq, int max_size, int taildrop);

void rd_fifoq_add0 (rd_fifoq_t *rfq, void *ptr, void **ptr_purged);
#define rd_fifoq_add(rfq,ptr) rd_fifoq_add0(rfq,ptr,NULL)
#define rd_fifoq_add_purge(rfq,ptr,ptr_purged) \
	rd_fifoq_add0(rfq,ptr,(void **)ptr_purged)

rd_fifoq_elm_t *rd_fifoq_pop0 (rd_fifoq_t *rfq, int no_wait, int timeout_ms);
#define rd_fifoq_pop_wait(rfq) rd_fifoq_pop0(rfq, 0, 0)
#define rd_fifoq_pop_timedwait(rfq,tmo) rd_fifoq_pop0(rfq, 0, tmo)
#define rd_fifoq_pop(rfq) rd_fifoq_pop0(rfq, 1, 0)

static inline void rd_fifoq_elm_release0 (rd_fifoq_t *rfq,
					  rd_fifoq_elm_t *rfqe) {
	if (rd_atomic_sub(&rfqe->rfqe_refcnt, 1) > 0)
		return;

	free(rfqe);
}

#define rd_fifoq_elm_release(RFQ,RFQE) do {   \
	rd_mutex_lock(&(RFQ)->rfq_lock);      \
	rd_fifoq_elm_release0(RFQ, RFQE);     \
	rd_mutex_unlock(&(RFQ)->rfq_lock);    \
	} while (0)
	
    

