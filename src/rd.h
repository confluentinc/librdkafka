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

#ifndef _MSC_VER
#define _GNU_SOURCE  /* for strndup() */
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <assert.h>

#include "tinycthread.h"

#ifdef _MSC_VER
/* Visual Studio */
#include "win32_config.h"
#else
/* POSIX / UNIX based systems */
#include "../config.h" /* mklove output */
#endif


#ifdef _MSC_VER
/* Win32/Visual Studio */
#include "rdwin32.h"

#else
/* POSIX / UNIX based systems */
#include "rdposix.h"
#endif

#include "rdtypes.h"


/**
* Allocator wrappers.
* We serve under the premise that if a (small) memory
* allocation fails all hope is lost and the application
* will fail anyway, so no need to handle it handsomely.
*/
static __inline RD_UNUSED void *rd_calloc(size_t num, size_t sz) {
	void *p = calloc(num, sz);
	assert(p);
	return p;
}

static __inline RD_UNUSED void *rd_malloc(size_t sz) {
	void *p = malloc(sz);
	assert(p);
	return p;
}

static __inline RD_UNUSED void *rd_realloc(void *ptr, size_t sz) {
	void *p = realloc(ptr, sz);
	assert(p);
	return p;
}

static __inline RD_UNUSED void rd_free(void *ptr) {
	free(ptr);
}

static __inline RD_UNUSED char *rd_strdup(const char *s) {
#ifndef _MSC_VER
	char *n = strdup(s);
#else
	char *n = _strdup(s);
#endif
	assert(n);
	return n;
}

static __inline RD_UNUSED char *rd_strndup(const char *s, size_t len) {
#ifndef _MSC_VER
	char *n = strndup(s, len);
	assert(n);
#else
	char *n = malloc(len + 1);
	assert(n);
	memcpy(n, s, len);
	n[len] = '\0';
#endif
	return n;
}




#define RD_ARRAY_SIZE(A)          (sizeof((A)) / sizeof(*(A)))
#define RD_ARRAYSIZE(A)           RD_ARRAY_SIZE(A)
#define RD_SIZEOF(TYPE,MEMBER)    sizeof(((TYPE *)NULL)->MEMBER)
#define RD_OFFSETOF(TYPE,MEMBER)  ((size_t) &(((TYPE *)NULL)->MEMBER))

/**
 * Returns the 'I'th array element from static sized array 'A'
 * or NULL if 'I' is out of range.
 * var-args is an optional prefix to provide the correct return type.
 */
#define RD_ARRAY_ELEM(A,I,...)				\
	((unsigned int)(I) < RD_ARRAY_SIZE(A) ? __VA_ARGS__ (A)[(I)] : NULL)

								  
#define RD_STRINGIFY(X)  # X



#define RD_MIN(a,b) ((a) < (b) ? (a) : (b))
#define RD_MAX(a,b) ((a) > (b) ? (a) : (b))


/**
 * Cap an integer (of any type) to reside within the defined limit.
 */
#define RD_INT_CAP(val,low,hi) \
	((val) < (low) ? low : ((val) > (hi) ? (hi) : (val)))


typedef struct {
	int32_t val;
} rd_atomic32_t;

typedef struct {
	int64_t val;
} rd_atomic64_t;

static __inline int32_t RD_UNUSED rd_atomic32_add (rd_atomic32_t *ra, int32_t v) {
#ifndef _MSC_VER
	return ATOMIC_OP(add, fetch, &ra->val, v);
#else
	return InterlockedAdd(&ra->val, v);
#endif
}

static __inline int32_t RD_UNUSED rd_atomic32_sub(rd_atomic32_t *ra, int32_t v) {
#ifndef _MSC_VER
	return ATOMIC_OP(sub, fetch, &ra->val, v);
#else
	return InterlockedAdd(&ra->val, -v);
#endif
}

static __inline int32_t RD_UNUSED rd_atomic32_get(rd_atomic32_t *ra) {
#ifndef _MSC_VER
	return ATOMIC_OP(fetch, add, &ra->val, 0);
#else
	return ra->val;
#endif
}

static __inline int32_t RD_UNUSED rd_atomic32_set(rd_atomic32_t *ra, int32_t v) {
#ifndef _MSC_VER
	return ra->val = v; // FIXME
#else
	return InterlockedExchange(&ra->val, v);
#endif
}


static __inline int64_t RD_UNUSED rd_atomic64_add (rd_atomic64_t *ra, int64_t v) {
#ifndef _MSC_VER
	return ATOMIC_OP(add, fetch, &ra->val, v);
#else
	return InterlockedAdd64(&ra->val, v);
#endif
}

static __inline int64_t RD_UNUSED rd_atomic64_sub(rd_atomic64_t *ra, int64_t v) {
#ifndef _MSC_VER
	return ATOMIC_OP(sub, fetch, &ra->val, v);
#else
	return InterlockedAdd64(&ra->val, -v);
#endif
}

static __inline int64_t RD_UNUSED rd_atomic64_get(rd_atomic64_t *ra) {
#ifndef _MSC_VER
	return ATOMIC_OP(fetch, add, &ra->val, 0);
#else
	return ra->val;
#endif
}


static __inline int64_t RD_UNUSED rd_atomic64_set(rd_atomic64_t *ra, int64_t v) {
#ifndef _MSC_VER
	return ra->val = v; // FIXME
#else
	return InterlockedExchange64(&ra->val, v);
#endif
}



