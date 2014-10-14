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


#include <pthread.h>

#include "rdqueue.h"
#include "rdtime.h"
#include "rdsignal.h"


typedef struct rd_thread_s {
	pthread_t rdt_thread;
	char     *rdt_name;
	void   *(*rdt_start)(void *);
	void     *rdt_start_arg;
	enum {
		RD_THREAD_S_NONE,
		RD_THREAD_S_RUNNING,
		RD_THREAD_S_EXITING,
		RD_THREAD_S_DEAD,
	} rdt_state;

	rd_fifoq_t rdt_eventq;
} rd_thread_t;


extern rd_thread_t *rd_mainthread;
extern __thread rd_thread_t *rd_currthread;

/**
 * Slow controlled thread exit through rd_thread_dispatch().
 * Called by exiting thread.
 */
#define rd_thread_exit() do {					  \
	rd_currthread_get();				          \
	assert(rd_currthread->rdt_state == RD_THREAD_S_RUNNING || \
	       rd_currthread->rdt_state == RD_THREAD_S_EXITING);  \
	(rd_currthread->rdt_state = RD_THREAD_S_EXITING);	  \
	} while (0)


/**
 * Slow controlled thread exit through rd_thread_dispatch().
 * Called by other thread.
 */
#define rd_thread_kill(rdt) do {					\
		(rdt)->rdt_state = RD_THREAD_S_EXITING;			\
	} while (0)


static inline int rd_thread_kill_join (rd_thread_t *rdt,
				       void **retval) RD_UNUSED;
static inline int rd_thread_kill_join (rd_thread_t *rdt,
				       void **retval) {
	pthread_t pthread;

	pthread = rdt->rdt_thread;
	rd_thread_kill(rdt);

	return pthread_join(pthread, retval);
}


/**
 * Clean up / free resources allocated to the current thread.
 * Use prior to thread destruction.
 *
 * Locality: the thread itself
 */
void         rd_thread_cleanup (void);


rd_thread_t *rd_thread_create0 (const char *name, pthread_t *pthread);

/**
 * Creates and starts a new thread and returns its rd_thread_t handle.
 * The new thread handle is assigned to '*rdt' (if 'rdt' is non-null).
 *
 * Same semantics as pthread_create() with the following exceptions:
 *  - User-defined signals are by default blocked by the new thread:
 *    SIGUSR1, SIGUSR2.
 *    Use rd_thread_sigmask() to unblock.
 */
int rd_thread_create (rd_thread_t **rdt, const char *name,
		      const pthread_attr_t *attr,
		      void *(*start_routine)(void*),
		      void *arg);



/**
 * Wrapper around rd_thread_create() for creating multiple ('threadcount')
 * threads.
 *
 * Useful for creating a set of worker threads or similar.
 */
int rd_threads_create (const char *nameprefix, int threadcount,
		       const pthread_attr_t *attr,
		       void *(*start_routine)(void*),
		       void *arg);


/**
 * This creates the rd_currthread handle for threads that where not
 * created through rd_thread_create().
 */
static inline rd_thread_t *rd_currthread_get (void) RD_UNUSED;
static inline rd_thread_t *rd_currthread_get (void) {

	if (unlikely(!rd_currthread)) {
		pthread_t thr = pthread_self();
		char thrname[32];
                char *p = (char *)(void *)&thr;
                int of = 0, i;
                /* Construct thread name from the id in a portable fashion */
                for (i = 0 ; i < sizeof(thr) && of < sizeof(thrname) ; i++)
                        of += snprintf(thrname+of, sizeof(thrname)-of,
                                       "%02x", (unsigned int)p[i]);
		rd_currthread = rd_thread_create0(thrname, &thr);
	}

	return rd_currthread;
}


/**
 * va-arg wrapper for pthread_sigmask().
 * The va-arg-list must be terminated with RD_SIG_END.
 * RD_SIG_ALL means all signals.
 *
 * Example:
 *   rd_thread_sigmask(SIG_BLOCK, RD_SIG_ALL, RD_SIG_END);
 *  or
 *   rd_thread_sigmask(SIG_SETMASK, SIGUSR1, SIGUSR2, SIGIO, RD_SIG_END);
 *
 * Returns the return value from pthread_sigmask().
 */
int rd_thread_sigmask (int how, ...);


#define rd_assert_inthread(rdt)      assert(rd_currthread == (rdt))
#define rd_assert_inpthread(pthread) assert(pthread_self() == (pthread))


#define RD_MUTEX_INITIALIZER  PTHREAD_MUTEX_INITIALIZER

#define rd_mutex_init(MTX)    pthread_mutex_init(MTX, NULL)
#define rd_mutex_destroy(MTX) pthread_mutex_destroy(MTX)
#define rd_mutex_lock(MTX)    pthread_mutex_lock(MTX)
#define rd_mutex_unlock(MTX)  pthread_mutex_unlock(MTX)

#define RD_MUTEX_LOCKED(MTX,CODE...) do {	\
	rd_mutex_lock(MTX);			\
	CODE;					\
	rd_mutex_unlock(MTX);			\
	} while (0)

#define rd_rwlock_init(RWL)    pthread_rwlock_init(RWL,NULL)
#define rd_rwlock_destroy(RWL) pthread_rwlock_destroy(RWL)
#define rd_rwlock_rdlock(RWL)  pthread_rwlock_rdlock(RWL)
#define rd_rwlock_wrlock(RWL)  pthread_rwlock_wrlock(RWL)
#define rd_rwlock_unlock(RWL)  pthread_rwlock_unlock(RWL)

#define RD_RWLOCK_RDLOCKED(RWL,CODE...) do {	\
	rd_rwlock_rdlock(RWL);			\
	CODE;					\
	rd_rwlock_unlock(RWL);			\
	} while (0)

#define RD_RWLOCK_WRLOCKED(RWL,CODE...) do {	\
	rd_rwlock_wrlock(RWL);			\
	CODE;					\
	rd_rwlock_unlock(RWL);			\
	} while (0)

#define rd_cond_init(COND,ATTR) pthread_cond_init(COND,ATTR)
#define RD_COND_INITIALIZER     PTHREAD_COND_INITIALIZER
#define rd_cond_signal(COND)    pthread_cond_signal(COND)
#define rd_cond_timedwait(COND,MTX,TS) pthread_cond_timedwait(COND,MTX,TS)
#define rd_cond_wait(COND,MTX) pthread_cond_wait(COND,MTX)

/**
 * Wrapper for pthread_cond_timedwait() that makes it simpler to use
 * for delta timeouts.
 * `timeout_ms' is the delta timeout in milliseconds.
 */
static int rd_cond_timedwait_ms (rd_cond_t *cond,
				 rd_mutex_t *mutex,
				 int timeout_ms) RD_UNUSED;
static int rd_cond_timedwait_ms (rd_cond_t *cond,
				 rd_mutex_t *mutex,
				 int timeout_ms) {
	struct timeval tv;
	struct timespec ts;

	gettimeofday(&tv, NULL);
	TIMEVAL_TO_TIMESPEC(&tv, &ts);

	ts.tv_sec  += timeout_ms / 1000;
	ts.tv_nsec += (timeout_ms % 1000) * 1000000;

	if (ts.tv_nsec >= 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	return rd_cond_timedwait(cond, mutex, &ts);
}






int rd_thread_poll (int timeout_ms);
void rd_thread_dispatch (void);

void rd_thread_init (void);
