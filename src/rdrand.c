/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
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
#include "rdrand.h"
#include "rdtime.h"
#include "tinycthread.h"
#include "rdmurmur2.h"
#ifndef _WIN32
/* getentropy() can be present in one of these two */
#include <unistd.h>
#include <sys/random.h>
#endif

/* Initial seed with time+thread id */
unsigned int rd_seed() {
        unsigned int seed = 0;
        struct timeval tv;
        rd_gettimeofday(&tv, NULL);
        seed = (unsigned int)(tv.tv_usec);
        seed ^= thrd_current_id();

        /* Apply the murmur2 hash to distribute entropy to
         * the whole seed. */
        seed = (unsigned int)rd_murmur2(&seed, sizeof(seed));
        return seed;
}

static int rd_rand() {
        int rand_num;
#if HAVE_RAND_R
        static RD_TLS unsigned int seed = 0;
        if (unlikely(seed == 0)) {
                seed = rd_seed();
        }
        rand_num = rand_r(&seed);
#else
        rand_num = rand();
#endif
        return rand_num;
}

#if HAVE_OSSL_SECURE_RAND_BYTES
static int rd_rand_bytes_by_ossl(unsigned char *buf, int num) {
        int res = -1;
        while ((res = RAND_priv_bytes(buf, num)) == 0) {
                rd_usleep(1000, 0); /* wait for more entropy */
        }
        return res;
}
#endif

#ifdef _WIN32
static void rd_rand_bytes_by_rand_s(unsigned char *buf, int num) {
        unsigned int rand;
        while (num > 0) {
                errno_t err = 0;
                rand_s(&rand);
                int i = sizeof(int);
                while (i-- > 0 && num > 0) {
                        *buf++ = (unsigned char)(rand & 0xff);
                        rand >>= 8;
                        num--;
                }
        }
}
#endif

int rd_rand_bytes(unsigned char *buf, unsigned int num) {
#if HAVE_OSSL_SECURE_RAND_BYTES
        if (rd_rand_bytes_by_ossl(buf, num) == 1)
                return 1;
#endif
#if HAVE_GETENTROPY
        if (getentropy(buf, (size_t)num) == 0)
                return 1;
#endif
#ifdef _WIN32
        rd_rand_bytes_by_rand_s(buf, num);
        return 1;
#else
        return 0;
#endif
}

int rd_jitter(int low, int high) {
        int rand_num = rd_rand();
        return (low + (rand_num % ((high - low) + 1)));
}

void rd_array_shuffle(void *base, size_t nmemb, size_t entry_size) {
        int i;
        void *tmp = rd_alloca(entry_size);

        /* FIXME: Optimized version for word-sized entries. */

        for (i = (int)nmemb - 1; i > 0; i--) {
                int j = rd_jitter(0, i);
                if (unlikely(i == j))
                        continue;

                memcpy(tmp, (char *)base + (i * entry_size), entry_size);
                memcpy((char *)base + (i * entry_size),
                       (char *)base + (j * entry_size), entry_size);
                memcpy((char *)base + (j * entry_size), tmp, entry_size);
        }
}
