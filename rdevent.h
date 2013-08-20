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

#include "rdthread.h"



#define rd_thread_event_f(F)  void (F) (void *ptr)

typedef struct rd_thread_event_s {
	rd_thread_event_f(*rte_callback);
	void        *rte_ptr;
} rd_thread_event_t;



/**
 * Enqueue event (callback call) on thread 'rdt'.
 * Requires 'rdt' to call rd_thread_dispatch().
 */
static void rd_thread_event_add (rd_thread_t *rdt,
				 rd_thread_event_f(*callback),
				 void *ptr) RD_UNUSED;

static void rd_thread_event_add (rd_thread_t *rdt,
				 rd_thread_event_f(*callback),
				 void *ptr) {
	rd_thread_event_t *rte = malloc(sizeof(*rte));

	rte->rte_callback = callback;
	rte->rte_ptr = ptr;

	rd_fifoq_add(&rdt->rdt_eventq, rte);
}


/**
 * Convenience function to enqueue a call to function 'cb' on thread 'rdt'.
 * Depending on the value of 'argcnt' (0..4) 'cb' may be one of:
 *
 *  argcnt | prototype
 *  -------+--------------------------------------------------------------
 *     0   | void (*cb) (void)
 *     1   | void (*cb) (void *arg1)
 *     2   | void (*cb) (void *arg1, void *arg2)
 *     3   | void (*cb) (void *arg1, void *arg2, void *arg3)
 *     4   | void (*cb) (void *arg1, void *arg2, void *arg3, void *arg4)
 */
void rd_thread_func_call (rd_thread_t *rdt, void *cb, int argcnt, void **args);
#define rd_thread_func_call0(rdt,cb)                                    \
	rd_thread_func_call(rdt,cb,0,NULL)
#define rd_thread_func_call1(rdt,cb,arg1)			        \
	rd_thread_func_call(rdt, cb, 1, ((void *[]){ arg1 }))
#define rd_thread_func_call2(rdt,cb,arg1,arg2)				\
	rd_thread_func_call(rdt, cb, 2, ((void *[]){ arg1, arg2 }))
#define rd_thread_func_call3(rdt,cb,arg1,arg2,arg3)			\
	rd_thread_func_call(rdt, cb, 3, ((void *[]){ arg1, arg2, arg3 }))
#define rd_thread_func_call4(rdt,cb,arg1,arg2,arg3,arg4)		\
	rd_thread_func_call(rdt, cb, 4, ((void *[]){ arg1, arg2, arg3, arg4 }))

/**
 * Calls the callback and destroys the event.
 */
static void rd_thread_event_call (rd_thread_event_t *rte) RD_UNUSED;
static void rd_thread_event_call (rd_thread_event_t *rte) {

	rte->rte_callback(rte->rte_ptr);

	free(rte);
}
