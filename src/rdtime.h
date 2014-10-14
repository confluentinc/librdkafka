/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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


#ifndef TIMEVAL_TO_TIMESPEC
#define TIMEVAL_TO_TIMESPEC(tv,ts) do {		\
    (ts)->tv_sec = (tv)->tv_sec;		\
    (ts)->tv_nsec = (tv)->tv_usec * 1000;	\
  } while (0)

#define TIMESPEC_TO_TIMEVAL(tv, ts) do {  \
    (tv)->tv_sec = (ts)->tv_sec;	  \
    (tv)->tv_usec = (ts)->tv_nsec / 1000; \
  } while (0)
#endif

#define TIMESPEC_TO_TS(ts) \
	(((rd_ts_t)(ts)->tv_sec * 1000000LLU) + ((ts)->tv_nsec / 1000))

#define TS_TO_TIMESPEC(ts,tsx) do {			\
	(ts)->tv_sec  = (tsx) / 1000000;		\
        (ts)->tv_nsec = ((tsx) % 1000000) * 1000;	\
	if ((ts)->tv_nsec >= 1000000000LLU) {		\
	   (ts)->tv_sec++;				\
	   (ts)->tv_nsec -= 1000000000LLU;		\
	}						\
       } while (0)

#define TIMESPEC_CLEAR(ts) ((ts)->tv_sec = (ts)->tv_nsec = 0LLU)


static inline rd_ts_t rd_clock (void) RD_UNUSED;
static inline rd_ts_t rd_clock (void) {
#ifdef __APPLE__
	/* No monotonic clock on Darwin */
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return ((rd_ts_t)tv.tv_sec * 1000000LLU) + (rd_ts_t)tv.tv_usec;
#else
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ((rd_ts_t)ts.tv_sec * 1000000LLU) + 
		((rd_ts_t)ts.tv_nsec / 1000LLU);
#endif
}



/**
 * Thread-safe version of ctime() that strips the trailing newline.
 */
static inline const char *rd_ctime (const time_t *t) RD_UNUSED;
static inline const char *rd_ctime (const time_t *t) {
	static __thread char ret[27];

	ctime_r(t, ret);

	ret[25] = '\0';

	return ret;
}
