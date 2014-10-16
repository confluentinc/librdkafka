/*
 * librdkafka - Apache Kafka C library
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

#include "rdkafka_int.h"
#include "rd.h"
#include "rdtime.h"
#include "rdsysqueue.h"


#define rd_kafka_timers_lock(rk)   pthread_mutex_lock(&(rk)->rk_timers_lock)
#define rd_kafka_timers_unlock(rk) pthread_mutex_unlock(&(rk)->rk_timers_lock)


static inline int rd_kafka_timer_started (const rd_kafka_timer_t *rtmr) {
	return rtmr->rtmr_next ? 1 : 0;
}


static int rd_kafka_timer_cmp (const void *_a, const void *_b) {
	const rd_kafka_timer_t *a = _a, *b = _b;
	return a->rtmr_next - b->rtmr_next;
}

static void rd_kafka_timer_unschedule (rd_kafka_t *rk, rd_kafka_timer_t *rtmr) {
	TAILQ_REMOVE(&rk->rk_timers, rtmr, rtmr_link);
	rtmr->rtmr_next = 0;
}

static void rd_kafka_timer_schedule (rd_kafka_t *rk, rd_kafka_timer_t *rtmr) {
	rd_kafka_timer_t *first;

	/* Timer has been stopped */
	if (!rtmr->rtmr_interval)
		return;

	rtmr->rtmr_next = rd_clock() + rtmr->rtmr_interval;

	if (!(first = TAILQ_FIRST(&rk->rk_timers)) ||
	    first->rtmr_next > rtmr->rtmr_next) {
		TAILQ_INSERT_HEAD(&rk->rk_timers, rtmr, rtmr_link);
		pthread_cond_signal(&rk->rk_timers_cond);
	} else
		TAILQ_INSERT_SORTED(&rk->rk_timers, rtmr, rtmr_link,
				    rd_kafka_timer_cmp);
}

/**
 * Stop a timer that may be started.
 * If called from inside a timer callback 'lock' must be 0, else 1.
 */
void rd_kafka_timer_stop (rd_kafka_t *rk, rd_kafka_timer_t *rtmr, int lock) {
	if (lock)
		rd_kafka_timers_lock(rk);

	if (!rd_kafka_timer_started(rtmr)) {
		if (lock)
			rd_kafka_timers_unlock(rk);
		return;
	}

	rd_kafka_timer_unschedule(rk, rtmr);
	rtmr->rtmr_interval = 0;

	if (lock)
		rd_kafka_timers_unlock(rk);
}


/**
 * Start the provided timer with the given interval.
 * Upon expiration of the interval the callback will be called in the
 * main rdkafka thread, after callback return the timer will be restarted.
 *
 * Use rd_kafka_timer_stop() to stop a timer.
 */
void rd_kafka_timer_start (rd_kafka_t *rk,
			   rd_kafka_timer_t *rtmr, int interval,
			   void (*callback) (rd_kafka_t *rk, void *arg),
			   void *arg) {
	rd_kafka_timers_lock(rk);

	if (rd_kafka_timer_started(rtmr))
		rd_kafka_timer_stop(rk, rtmr, 0/*!lock*/);

	rtmr->rtmr_interval = interval;
	rtmr->rtmr_callback = callback;
	rtmr->rtmr_arg      = arg;

	rd_kafka_timer_schedule(rk, rtmr);

	rd_kafka_timers_unlock(rk);
}



/**
 * Dispatch timers.
 * Will block up to 'timeout' microseconds before returning.
 */
void rd_kafka_timers_run (rd_kafka_t *rk, int timeout) {
	rd_ts_t now = rd_clock();
	rd_ts_t end = now + timeout;

        rd_kafka_timers_lock(rk);

	while (!rk->rk_terminate && now <= end) {
		int64_t sleeptime;
		rd_kafka_timer_t *rtmr;

		if (likely((rtmr = TAILQ_FIRST(&rk->rk_timers)) != NULL))
			sleeptime = rtmr->rtmr_next - now;
		else
			sleeptime = 100000000000000llu;

		if (sleeptime > 0) {
			if (sleeptime > (end - now))
				sleeptime = end - now;

			pthread_cond_timedwait_ms(&rk->rk_timers_cond,
						  &rk->rk_timers_lock,
						  sleeptime / 1000);
			now = rd_clock();
		}

		while ((rtmr = TAILQ_FIRST(&rk->rk_timers)) &&
		       rtmr->rtmr_next <= now) {

			rd_kafka_timer_unschedule(rk, rtmr);
                        rd_kafka_timers_unlock(rk);

			rtmr->rtmr_callback(rk, rtmr->rtmr_arg);

                        rd_kafka_timers_lock(rk);
			rd_kafka_timer_schedule(rk, rtmr);
		}
	}

	rd_kafka_timers_unlock(rk);
}
