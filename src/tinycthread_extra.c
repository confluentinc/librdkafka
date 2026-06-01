/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2018-2022, Magnus Edenhill
 *               2025, Confluent Inc.
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


/**
 * @brief Extra methods added to tinycthread/c11threads
 */

#include "rd.h"
#include "rdtime.h"
#include "tinycthread.h"


int thrd_setname(const char *name) {
#if HAVE_PTHREAD_SETNAME_GNU
        if (!pthread_setname_np(pthread_self(), name))
                return thrd_success;
#elif HAVE_PTHREAD_SETNAME_DARWIN
        pthread_setname_np(name);
        return thrd_success;
#elif HAVE_PTHREAD_SETNAME_FREEBSD
        pthread_set_name_np(pthread_self(), name);
        return thrd_success;
#endif
        return thrd_error;
}

int thrd_is_current(thrd_t thr) {
#if defined(_TTHREAD_WIN32_)
        return GetThreadId(thr) == GetCurrentThreadId();
#else
        return (pthread_self() == thr);
#endif
}

unsigned long thrd_current_id(void) {
#ifdef _WIN32
        /* Windows makes a distinction between thread handle
         * and thread id, which means we can't use the
         * thrd_current() API that returns the handle. */
        return (unsigned long)GetCurrentThreadId();
#else
        return (unsigned long)(intptr_t)thrd_current();
#endif
}


#ifdef _WIN32
void cnd_wait_enter(cnd_t *cond) {
        /* Increment number of waiters */
        EnterCriticalSection(&cond->mWaitersCountLock);
        ++cond->mWaitersCount;
        LeaveCriticalSection(&cond->mWaitersCountLock);
}

void cnd_wait_exit(cnd_t *cond) {
        /* Increment number of waiters */
        EnterCriticalSection(&cond->mWaitersCountLock);
        --cond->mWaitersCount;
        LeaveCriticalSection(&cond->mWaitersCountLock);
}
#endif



int cnd_timedwait_ms(cnd_t *cnd, mtx_t *mtx, int timeout_ms) {
        int ret;
        rd_ts_t abs_timeout;
        rd_bool_t continue_timedwait = rd_true;

        if (timeout_ms == RD_POLL_INFINITE)
                return cnd_wait(cnd, mtx);
#if defined(_TTHREAD_WIN32_)
        return _cnd_timedwait_win32(cnd, mtx, (DWORD)timeout_ms);
#else
        abs_timeout = rd_timeout_init(timeout_ms);
        do {
                struct timeval tv;
                struct timespec ts;

                gettimeofday(&tv, NULL);
                ts.tv_sec  = tv.tv_sec;
                ts.tv_nsec = tv.tv_usec * 1000;

                ts.tv_sec += timeout_ms / 1000;
                ts.tv_nsec += (timeout_ms % 1000) * 1000000;

                if (ts.tv_nsec >= 1000000000) {
                        ts.tv_sec++;
                        ts.tv_nsec -= 1000000000;
                }

                ret                = cnd_timedwait(cnd, mtx, &ts);
                continue_timedwait = ret == thrd_timedout;
                if (continue_timedwait) {
                        timeout_ms = rd_timeout_remains(abs_timeout);
                        if (rd_timeout_expired(timeout_ms))
                                continue_timedwait = rd_false;
                }
        } while (continue_timedwait);
        return ret;
#endif
}

int cnd_timedwait_msp(cnd_t *cnd, mtx_t *mtx, int *timeout_msp) {
        rd_ts_t pre = rd_clock();
        int r;
        r = cnd_timedwait_ms(cnd, mtx, *timeout_msp);
        if (r != thrd_timedout) {
                /* Subtract spent time */
                (*timeout_msp) -= (int)(rd_clock() - pre) / 1000;
        }
        return r;
}

int cnd_timedwait_abs(cnd_t *cnd, mtx_t *mtx, rd_ts_t abs_timeout) {
        int r = thrd_timedout;
        int timeout_ms;
        if (abs_timeout == RD_POLL_INFINITE)
                return cnd_wait(cnd, mtx);
        else if (abs_timeout == RD_POLL_NOWAIT)
                return thrd_timedout;

        do {
                timeout_ms = rd_timeout_remains(abs_timeout);
                if (timeout_ms == RD_POLL_NOWAIT)
                        break;
                r = cnd_timedwait_ms(cnd, mtx, timeout_ms);
        } while (r == thrd_timedout);

        return r;
}


/**
 * @name Read-write locks
 * @{
 */
#ifndef _WIN32

/* Diagnostic watchdog for wrlock waits.
 *
 * Goal: prove or disprove the macOS pthread_rwlock wedged-state hypothesis.
 *
 * Mechanism: every thread that calls rwlock_wrlock registers a waiter entry
 * before calling pthread_rwlock_wrlock, and unregisters after the call
 * returns. A single background watchdog thread walks the waiter list every
 * few seconds; for any waiter blocked >30s, it calls pthread_rwlock_trywrlock
 * on the same lock from outside, and logs the result:
 *   - try_r == 0   : lock was free, but original wrlock was blocked.
 *                    pthread state machine is wedged on a free lock.
 *   - try_r == EBUSY: lock is genuinely held by someone we did not log.
 *
 * Diagnostic only. Remove after the destroy hang is root-caused. */

struct rwlock_wait_entry {
        rwlock_t                  *rwl;
        unsigned long              waiter_tid;
        rd_ts_t                    start_us;
        int                        diag_count;
        struct rwlock_wait_entry  *next;
};

static mtx_t                      g_rwl_diag_mtx;
static struct rwlock_wait_entry  *g_rwl_diag_head;
static thrd_t                     g_rwl_diag_thread;
static int                        g_rwl_diag_started; /* protected by g_rwl_diag_mtx */
static int                        g_rwl_diag_initialized; /* set once at process start */

static int rwlock_diag_watchdog(void *arg) {
        (void)arg;
        for (;;) {
                struct timespec sleep_ts = { 5, 0 };
                nanosleep(&sleep_ts, NULL);

                rd_ts_t now_us = rd_clock();
                mtx_lock(&g_rwl_diag_mtx);
                struct rwlock_wait_entry *e;
                for (e = g_rwl_diag_head; e; e = e->next) {
                        rd_ts_t blocked_us = now_us - e->start_us;
                        if (blocked_us < 30 * 1000000LL)
                                continue;

                        /* Re-throttle subsequent prints to every 30s per
                         * waiter, not every 5s. */
                        if (e->diag_count > 0 &&
                            blocked_us - (rd_ts_t)e->diag_count * 30 * 1000000LL <
                                30 * 1000000LL)
                                continue;

                        int try_r = pthread_rwlock_trywrlock(e->rwl);
                        int try_errno = errno;
                        fprintf(stderr,
                                "[RWLOCK_DIAG] rwl=%p waiter_tid=%lu "
                                "blocked_ms=%lld trywrlock=%d errno=%d "
                                "(EBUSY=%d) -> %s\n",
                                (void *)e->rwl, e->waiter_tid,
                                (long long)(blocked_us / 1000),
                                try_r, try_errno, EBUSY,
                                try_r == 0
                                    ? "WEDGED-FREE-LOCK (pthread_rwlock_wrlock "
                                      "blocked on a lock that trywrlock could "
                                      "acquire)"
                                    : (try_r == EBUSY
                                           ? "GENUINELY-HELD (someone else has "
                                             "the lock)"
                                           : "OTHER-ERROR"));
                        fflush(stderr);
                        if (try_r == 0) {
                                /* We accidentally acquired the lock. Release
                                 * it immediately so the legitimate waiter has
                                 * a chance to take it. */
                                pthread_rwlock_unlock(e->rwl);
                        }
                        e->diag_count++;
                }
                mtx_unlock(&g_rwl_diag_mtx);
        }
        return 0;
}

void rwlock_diag_init(void) {
        if (g_rwl_diag_initialized)
                return;
        g_rwl_diag_initialized = 1;
        mtx_init(&g_rwl_diag_mtx, mtx_plain);
        g_rwl_diag_head = NULL;
        if (thrd_create(&g_rwl_diag_thread, rwlock_diag_watchdog, NULL) ==
            thrd_success)
                g_rwl_diag_started = 1;
}

int rwlock_init(rwlock_t *rwl) {
        int r = pthread_rwlock_init(rwl, NULL);
        if (r) {
                errno = r;
                return thrd_error;
        }
        return thrd_success;
}

int rwlock_destroy(rwlock_t *rwl) {
        int r = pthread_rwlock_destroy(rwl);
        if (r) {
                errno = r;
                return thrd_error;
        }
        return thrd_success;
}

int rwlock_rdlock(rwlock_t *rwl) {
        int r = pthread_rwlock_rdlock(rwl);
        assert(r == 0);
        return thrd_success;
}

int rwlock_wrlock(rwlock_t *rwl) {
        /* Register as a pending writer so the watchdog can probe this lock
         * if we end up blocked >30s. */
        struct rwlock_wait_entry entry;
        entry.rwl        = rwl;
        entry.waiter_tid = (unsigned long)(uintptr_t)pthread_self();
        entry.start_us   = rd_clock();
        entry.diag_count = 0;
        entry.next       = NULL;

        if (g_rwl_diag_started) {
                mtx_lock(&g_rwl_diag_mtx);
                entry.next      = g_rwl_diag_head;
                g_rwl_diag_head = &entry;
                mtx_unlock(&g_rwl_diag_mtx);
        }

        int r = pthread_rwlock_wrlock(rwl);

        if (g_rwl_diag_started) {
                mtx_lock(&g_rwl_diag_mtx);
                struct rwlock_wait_entry **pp = &g_rwl_diag_head;
                while (*pp && *pp != &entry)
                        pp = &(*pp)->next;
                if (*pp == &entry)
                        *pp = entry.next;
                mtx_unlock(&g_rwl_diag_mtx);
        }

        assert(r == 0);
        return thrd_success;
}

int rwlock_rdunlock(rwlock_t *rwl) {
        int r = pthread_rwlock_unlock(rwl);
        assert(r == 0);
        return thrd_success;
}

int rwlock_wrunlock(rwlock_t *rwl) {
        int r = pthread_rwlock_unlock(rwl);
        assert(r == 0);
        return thrd_success;
}
/**@}*/


#endif /* !_MSC_VER */
