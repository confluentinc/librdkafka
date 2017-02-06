/*
* librdkafka - Apache Kafka C library
*
* Copyright (c) 2012-2015, Magnus Edenhill
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

/**
 * C variables and functions shared with C++ tests
 */

/** @brief Broker version to int */
#define TEST_BRKVER(A,B,C,D) \
        (((A) << 24) | ((B) << 16) | ((C) << 8) | (D))
/** @brief return single version component from int */
#define TEST_BRKVER_X(V,I) \
        (((V) >> (24-((I)*8))) & 0xff)

extern int test_broker_version;


const char *test_mk_topic_name (const char *suffix, int randomized);

void test_FAIL (const char *file, int line, int fail_now, const char *str);
void test_SAY (const char *file, int line, int level, const char *str);

void test_timeout_set (int timeout);
int test_set_special_conf (const char *name, const char *val, int *timeoutp);
const char *test_conf_get_path (void);
const char *test_getenv (const char *env, const char *def);

#ifndef _MSC_VER
#include <sys/time.h>
#ifndef RD_UNUSED
#define RD_UNUSED __attribute__((unused))
#endif

#else

#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#endif

#ifndef RD_UNUSED
#define RD_UNUSED
#endif


/**
* A microsecond monotonic clock
*/
static RD_INLINE int64_t test_clock (void)
#ifndef _MSC_VER
__attribute__((unused))
#endif
;
static RD_INLINE int64_t test_clock (void) {
#ifdef __APPLE__
        /* No monotonic clock on Darwin */
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return ((int64_t)tv.tv_sec * 1000000LLU) + (int64_t)tv.tv_usec;
#elif _MSC_VER
        return (int64_t)GetTickCount64() * 1000LLU;
#else
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ((int64_t)ts.tv_sec * 1000000LLU) +
                ((int64_t)ts.tv_nsec / 1000LLU);
#endif
}
