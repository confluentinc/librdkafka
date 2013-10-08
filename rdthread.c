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
#include "rdevent.h"
#include "rdlog.h"

#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <stdarg.h>

rd_thread_t *rd_mainthread;
__thread rd_thread_t *rd_currthread;


void rd_thread_init (void) {
	pthread_t thr = pthread_self();
	rd_mainthread = rd_thread_create0("main", &thr);
	rd_currthread = rd_mainthread;
}

int rd_thread_poll (int timeout_ms) {
	rd_fifoq_elm_t *rfqe;
	int cnt = 0;
	int nowait = timeout_ms == 0;

	while ((rfqe = rd_fifoq_pop0(&rd_currthread->rdt_eventq,
				     nowait, timeout_ms))) {
		rd_thread_event_t *rte = rfqe->rfqe_ptr;
		
		rd_thread_event_call(rte);
		
		rd_fifoq_elm_release(&rd_currthread->rdt_eventq, rfqe);

		cnt++;
	}

	return cnt;
}


static void rd_thread_destroy (rd_thread_t *rdt) {
	assert(rdt->rdt_state != RD_THREAD_S_RUNNING);
	if (rdt->rdt_name)
		free(rdt->rdt_name);
	rd_fifoq_destroy(&rdt->rdt_eventq);
	free(rdt);
}

void rd_thread_cleanup (void) {
}


void rd_thread_dispatch (void) {

	while (rd_currthread->rdt_state == RD_THREAD_S_RUNNING) {
		/* FIXME: Proper conding for all thread inputs. */
		rd_thread_poll(100);
	}

	rd_thread_cleanup();
}


static void *rd_thread_start_routine (void *arg) {
	rd_thread_t *rdt = arg;
	void *ret;
	
	/* By default with block the user-defined signals. */
	rd_thread_sigmask(SIG_BLOCK, SIGUSR1, SIGUSR2, RD_SIG_END);

	rd_currthread = rdt;

	ret = rdt->rdt_start(rdt->rdt_start_arg);

	rd_thread_cleanup();
	rd_thread_destroy(rdt);

	return ret;
}


rd_thread_t *rd_thread_create0 (const char *name, pthread_t *pthread) {
	rd_thread_t *rdt;

	rdt = calloc(1, sizeof(*rdt));
  
	if (name)
		rdt->rdt_name = strdup(name);

	rdt->rdt_state = RD_THREAD_S_RUNNING;

	rd_fifoq_init(&rdt->rdt_eventq);

	if (pthread)
		rdt->rdt_thread = *pthread;

	return rdt;
}


int rd_thread_create (rd_thread_t **rdt,
		      const char *name,
		      const pthread_attr_t *attr,
		      void *(*start_routine)(void*),
		      void *arg) {
	rd_thread_t *rdt0;

	rdt0 = rd_thread_create0(name, NULL);

	rdt0->rdt_start = start_routine;
	rdt0->rdt_start_arg = arg;

	if (rdt)
		*rdt = rdt0;
	
	/* FIXME: We should block all signals until pthread_create returns. */
	if (pthread_create(&rdt0->rdt_thread, attr,
			   rd_thread_start_routine, rdt0)) {
		int errno_save = errno;
		rd_thread_destroy(rdt0);
		if (rdt)
			*rdt = NULL;
		errno = errno_save;
		return -1;
	}

#ifdef PR_SET_NAME
	prctl(PR_SET_NAME, (char *)rdt0->rdt_name, 0, 0, 0);
#endif
	
	return 0;
}


int rd_threads_create (const char *nameprefix, int threadcount,
		       const pthread_attr_t *attr,
		       void *(*start_routine)(void*),
		       void *arg) {
	int i;
	char *name = alloca(strlen(nameprefix) + 4);
	int failed = 0;

	if (threadcount >= 1000) {
		errno = E2BIG;
		return -1;
	}
		
	for (i = 0 ; i < threadcount ; i++) {
		sprintf(name, "%s%i", nameprefix, i);
		if (!rd_thread_create(NULL, name, attr, start_routine, arg))
			failed++;
	}

	if (failed == threadcount)
		return -1;

	return threadcount - failed;
}


int rd_thread_sigmask (int how, ...) {
	va_list ap;
	sigset_t set;
	int sig;

	sigemptyset(&set);

	va_start(ap, how);
	while ((sig = va_arg(ap, int)) != RD_SIG_END) {
		if (sig == RD_SIG_ALL)
			sigfillset(&set);
		else
			sigaddset(&set, sig);
	}
	va_end(ap);

	return pthread_sigmask(how, &set, NULL);
}

