/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
 * @name Minimal pthread_rwlock reproducer for the macOS arm64 wedge bug
 *
 * On macOS 15 arm64 (Apple Silicon), pthread_rwlock_t can permanently wedge
 * in a state where all subsequent operations on the lock — both reader and
 * writer acquires — block forever, even though no thread actually holds the
 * lock. Observed wedged state (decoded from libpthread internals):
 *
 *   lcntval & PTH_RWL_WBIT  set    (writers queued)
 *   lcntval & PTH_RWL_KBIT  set    (kernel has waiters)
 *   lcntval & PTH_RWL_EBIT  CLEAR  (no writer holds the lock)
 *   lcntval & PTH_RWL_UBIT  CLEAR  (lock not freely acquirable)
 *   (lcntval count - ucntval count) matches number of queued waiters
 *
 * In other words: the writer-handoff completed at the previous-holder side
 * (EBIT cleared, ucntval incremented) but the next-in-line writer was never
 * actually granted the lock (EBIT not re-set). The flags WBIT and KBIT stay
 * set forever, so all future acquires block.
 *
 * This reproducer drives the same pattern librdkafka's destroy path exercises:
 * multiple writer threads doing rapid wrlock/unlock cycles on a small set of
 * rwlocks. A watchdog thread probes each lock with pthread_rwlock_trywrlock
 * and pthread_rwlock_tryrdlock. If both return EBUSY for >5s on a lock with
 * no holder per our own bookkeeping, the wedge has fired — fail the test
 * with a byte dump of the rwlock for offline decoding.
 *
 * Expected outcomes:
 *   macOS 15 arm64: FAIL (bug fires within seconds-to-minutes)
 *   macOS 15 Intel: PASS
 *   Linux glibc:    PASS
 */

#include "test.h"
#include <pthread.h>
#include <string.h>

#ifdef _WIN32
/* pthread_rwlock is POSIX-only; this test is a no-op on Windows. */
int main_0185_pthread_rwlock_repro(int argc, char **argv) {
        TEST_SAY("Test skipped: pthread_rwlock not applicable on Windows\n");
        return 0;
}
#else
#include <stdint.h>
#include <errno.h>

#define RW_NUM_LOCKS        16
#define RW_DRIVERS_PER_LOCK 3
#define RW_ITERATIONS       50000
#define RW_TEST_TIMEOUT_MS  60000 /* abort after 60s if not wedged */

typedef struct rw_slot_s {
        /* Heap-allocate the rwlock so its address has the same alignment
         * characteristics as one embedded in a malloc()'d rd_kafka_t. */
        pthread_rwlock_t *rwl;
        rd_atomic32_t     wedged; /* set by watchdog when wedge detected */
} rw_slot_t;

typedef struct rw_state_s {
        rw_slot_t      slots[RW_NUM_LOCKS];
        rd_atomic64_t  acquires_done;
        rd_atomic32_t  next_idx; /* monotonically increasing thread index */
} rw_state_t;

static void rw_dump_wedged(rw_state_t *st, int idx) {
        pthread_rwlock_t        *rwl = st->slots[idx].rwl;
        const unsigned char     *bytes = (const unsigned char *)rwl;
        size_t                   sz    = sizeof(*rwl);
        size_t                   i;

        TEST_SAY("*** WEDGED rwlock detected at index %d (rwl=%p) ***\n",
                 idx, (void *)rwl);
        TEST_SAY("trywrlock=EBUSY, tryrdlock=EBUSY on a lock with no holder.\n");

        fprintf(stderr, "[RWLOCK_BYTES] rwl=%p sizeof=%zu words=",
                (void *)rwl, sz);
        for (i = 0; i + 4 <= sz; i += 4) {
                uint32_t w;
                memcpy(&w, bytes + i, 4);
                fprintf(stderr, "%s%08x", i == 0 ? "" : ",", w);
        }
        fprintf(stderr, "\n");
        fflush(stderr);

        /* The seqaddr is the 16-byte-aligned uint32_t triplet starting at
         * the first 16-byte boundary after the rwlock signature and the
         * embedded unfair lock. On LP64 macOS that lands at byte offset 32.
         * Decode the candidate triplet for convenience. */
        if (sz >= 44) {
                uint32_t lcnt, seq, ucnt;
                memcpy(&lcnt, bytes + 32, 4);
                memcpy(&seq, bytes + 36, 4);
                memcpy(&ucnt, bytes + 40, 4);
                TEST_SAY("Decoded seqaddr (offset 32):\n");
                TEST_SAY("  lcntval=0x%08x  count=%u  flags=0x%02x  "
                         "WBIT=%d KBIT=%d EBIT=%d UBIT=%d IBIT=%d\n",
                         lcnt, lcnt >> 8, lcnt & 0xff,
                         !!(lcnt & 0x04), !!(lcnt & 0x01),
                         !!(lcnt & 0x02), !!(lcnt & 0x40),
                         !!(lcnt & 0x80));
                TEST_SAY("  rw_seq =0x%08x  count=%u  saved=0x%02x\n",
                         seq, seq >> 8, seq & 0xff);
                TEST_SAY("  ucntval=0x%08x  count=%u\n",
                         ucnt, ucnt >> 8);
                TEST_SAY("  diff (lcnt-ucnt)/256 = %u  "
                         "(should equal number of queued waiters)\n",
                         (lcnt >> 8) - (ucnt >> 8));
        }
}

static int rw_driver_thread(void *arg) {
        rw_state_t *st = arg;
        int         i;
        int         lock_idx;
        int         my_n;
        pthread_rwlock_t *rwl;

        /* Each driver thread picks a lock based on the order in which it
         * started: ascending atomic counter modulo number of locks. */
        my_n     = (int)rd_atomic32_add(&st->next_idx, 1) - 1;
        lock_idx = (my_n / RW_DRIVERS_PER_LOCK) % RW_NUM_LOCKS;
        rwl      = st->slots[lock_idx].rwl;

        for (i = 0; i < RW_ITERATIONS; i++) {
                /* Mix in ~10% rdlock acquires to match librdkafka, which
                 * has continuous broker-thread rdlock traffic interleaved
                 * with the destroy-path wrlock acquires. */
                if (i % 10 == 0) {
                        if (pthread_rwlock_rdlock(rwl) != 0)
                                return -1;
                        pthread_rwlock_unlock(rwl);
                        if (pthread_rwlock_rdlock(rwl) != 0)
                                return -1;
                        pthread_rwlock_unlock(rwl);
                } else {
                        /* Pattern matching librdkafka's broker_decommission():
                         * release at 6574, immediately reacquire at 6615. */
                        if (pthread_rwlock_wrlock(rwl) != 0)
                                return -1;
                        pthread_rwlock_unlock(rwl);

                        if (pthread_rwlock_wrlock(rwl) != 0)
                                return -1;
                        pthread_rwlock_unlock(rwl);
                }

                rd_atomic64_add(&st->acquires_done, 2);

                if (rd_atomic32_get(&st->slots[lock_idx].wedged))
                        return 0;
        }
        return 0;
}

static int rw_watchdog_thread(void *arg) {
        rw_state_t *st       = arg;
        int64_t     last_seen = rd_atomic64_get(&st->acquires_done);
        rd_ts_t     deadline  = test_clock() + (rd_ts_t)RW_TEST_TIMEOUT_MS * 1000;
        int         stuck_iters = 0;

        while (test_clock() < deadline) {
                int i;
                int64_t now_seen;

                rd_sleep(1);
                now_seen = rd_atomic64_get(&st->acquires_done);

                if (now_seen != last_seen) {
                        last_seen   = now_seen;
                        stuck_iters = 0;
                        continue;
                }

                stuck_iters++;
                if (stuck_iters < 3)
                        continue; /* wait 3s of no progress before probing */

                /* No progress for >=3s — probe every lock. */
                for (i = 0; i < RW_NUM_LOCKS; i++) {
                        pthread_rwlock_t *rwl = st->slots[i].rwl;
                        int w, r;

                        w = pthread_rwlock_trywrlock(rwl);
                        if (w == 0) {
                                pthread_rwlock_unlock(rwl);
                                continue;
                        }
                        r = pthread_rwlock_tryrdlock(rwl);
                        if (r == 0) {
                                pthread_rwlock_unlock(rwl);
                                continue;
                        }
                        /* Both EBUSY: wedged. */
                        if (w == EBUSY && r == EBUSY) {
                                rd_atomic32_set(&st->slots[i].wedged, 1);
                                rw_dump_wedged(st, i);
                                return 1;
                        }
                }
                stuck_iters = 0;
        }
        return 0;
}

int main_0185_pthread_rwlock_repro(int argc, char **argv) {
        rw_state_t        st;
        thrd_t            drivers[RW_NUM_LOCKS * RW_DRIVERS_PER_LOCK];
        thrd_t            watchdog;
        int               i;
        int               wedge_found    = 0;
        int               watchdog_res   = 0;

        memset(&st, 0, sizeof(st));
        rd_atomic64_init(&st.acquires_done, 0);
        rd_atomic32_init(&st.next_idx, 0);
        for (i = 0; i < RW_NUM_LOCKS; i++) {
                st.slots[i].rwl = malloc(sizeof(pthread_rwlock_t));
                if (!st.slots[i].rwl)
                        TEST_FAIL("malloc(pthread_rwlock_t) failed");
                if (pthread_rwlock_init(st.slots[i].rwl, NULL) != 0)
                        TEST_FAIL("pthread_rwlock_init failed");
                rd_atomic32_init(&st.slots[i].wedged, 0);
        }

        TEST_SAY("Spawning %d driver threads across %d rwlocks, %d iters each\n",
                 RW_NUM_LOCKS * RW_DRIVERS_PER_LOCK, RW_NUM_LOCKS,
                 RW_ITERATIONS);

        if (thrd_create(&watchdog, rw_watchdog_thread, &st) != thrd_success)
                TEST_FAIL("thrd_create(watchdog) failed");

        for (i = 0; i < RW_NUM_LOCKS * RW_DRIVERS_PER_LOCK; i++) {
                if (thrd_create(&drivers[i], rw_driver_thread, &st) !=
                    thrd_success)
                        TEST_FAIL("thrd_create(driver %d) failed", i);
        }

        for (i = 0; i < RW_NUM_LOCKS * RW_DRIVERS_PER_LOCK; i++) {
                int r;
                thrd_join(drivers[i], &r);
        }
        thrd_join(watchdog, &watchdog_res);

        for (i = 0; i < RW_NUM_LOCKS; i++) {
                if (rd_atomic32_get(&st.slots[i].wedged)) {
                        wedge_found++;
                        TEST_SAY("Lock %d (rwl=%p) reported wedged\n",
                                 i, (void *)st.slots[i].rwl);
                }
        }

        TEST_SAY("Total acquires completed: %" PRId64 "\n",
                 rd_atomic64_get(&st.acquires_done));

        for (i = 0; i < RW_NUM_LOCKS; i++) {
                /* Don't pthread_rwlock_destroy a wedged lock — it may
                 * itself hang. Leak it (and its malloc) intentionally. */
                if (!rd_atomic32_get(&st.slots[i].wedged)) {
                        pthread_rwlock_destroy(st.slots[i].rwl);
                        free(st.slots[i].rwl);
                }
        }

        if (wedge_found > 0)
                TEST_FAIL("%d rwlock(s) wedged: macOS pthread_rwlock bug "
                          "reproduced",
                          wedge_found);

        return 0;
}
#endif /* !_WIN32 */