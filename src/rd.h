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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <alloca.h>
#include <assert.h>
#include <pthread.h>

#include "../config.h"

#include "rdtypes.h"


#ifndef likely
#define likely(x)   __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

#define RD_UNUSED   __attribute__((unused))
#define RD_PACKED   __attribute__((packed))
#define RD_IS_CONSTANT(p)  __builtin_constant_p((p))

#define RD_ARRAY_SIZE(A)          (sizeof((A)) / sizeof(*(A)))
#define RD_ARRAYSIZE(A)           RD_ARRAY_SIZE(A)
#define RD_SIZEOF(TYPE,MEMBER)    sizeof(((TYPE *)NULL)->MEMBER)
#define RD_OFFSETOF(TYPE,MEMBER)  ((size_t) &(((TYPE *)NULL)->MEMBER))

/**
 * Returns the 'I'th array element from static sized array 'A'
 * or NULL if 'I' is out of range.
 * 'PFX' is an optional prefix to provide the correct return type.
 */
#define RD_ARRAY_ELEM(A,I,PFX...)				\
	((unsigned int)(I) < RD_ARRAY_SIZE(A) ? PFX (A)[(I)] : NULL)

								  
#define RD_STRINGIFY(X)  # X



#define RD_MIN(a,b) ((a) < (b) ? (a) : (b))
#define RD_MAX(a,b) ((a) > (b) ? (a) : (b))


/**
 * Cap an integer (of any type) to reside within the defined limit.
 */
#define RD_INT_CAP(val,low,hi) \
	((val) < (low) ? low : ((val) > (hi) ? (hi) : (val)))


#define rd_atomic_add(PTR,VAL)  ATOMIC_OP(add,fetch,PTR,VAL)
#define rd_atomic_sub(PTR,VAL)  ATOMIC_OP(sub,fetch,PTR,VAL)

#define rd_atomic_add_prev(PTR,VAL)  ATOMIC_OP(fetch,add,PTR,VAL)
#define rd_atomic_sub_prev(PTR,VAL)  ATOMIC_OP(fetch,sub,PTR,VAL)



#ifdef sun
#include <sys/isa_defs.h>
#include <sys/byteorder.h>
# if _BIG_ENDIAN
#define be64toh(x) (x)
#define htobe64(x) (x)
# else
#  ifndef ntohll
#define ntohll(x) BSWAP_64(x)
#define htonll(x) ntohll(x)
#  endif
#define be64toh(x)  ntohll(x)
#define htobe64(x)  htonll(x)
# endif
#endif /* sun */

#ifndef be64toh
#ifndef __APPLE__
#ifndef sun
#include <byteswap.h>
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
#define be64toh(x) (x)
#else
# if __BYTE_ORDER == __LITTLE_ENDIAN
#define be64toh(x)  bswap_64(x)
# endif
#endif

#define htobe64(x) be64toh(x)
#endif
#endif


void rd_init (void);
