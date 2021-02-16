/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014-2018 Magnus Edenhill
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

#ifndef __OS400_ASSERT_H_
#define __OS400_ASSERT_H_

#ifndef NDEBUG
// Of course, ILE C library has an assert function. But unfortunately, it has no Ascii equivalence for it. 
// We have to implement handmade assert function to be able to use it with QAdrt ascii runtime
#ifdef assert
#undef assert
#endif
#define assert(__expr) ((__expr) ? ((void)0) : \
        (fprintf(stderr,"Assertion failed: %s file %s, line %d, function %s\n", \
                 #__expr,__FILE__,__LINE__, __func__), \
        abort()))
#else
#define assert(expr) 
#endif

#define gai_strerror(n) gai_strerror_a(n)
#include <netdb.h>
/* Ascii version of gai_strerror.  gai_strerror is not included in QADRT */
static char *gai_strerror_a(int n) {
        static char msg[10][60];
        static int  msgi=0;
        switch(n) {
        case 0: return "";
        case EAI_AGAIN: return "The name could not be resolved at this time. Future attempts may succeed.";
        case EAI_BADFLAGS: "The flags parameter had an invalid value.";
        case EAI_FAIL: return "A non-recoverable error occurred when attempting to resolve the name.";
        case EAI_FAMILY: return "The address family was not recognized or the address length was invalid for the specified family";
        case EAI_MEMORY: return "There was a memory allocation failure when trying to allocate storage for the return value.";
        case EAI_NONAME: return "The name does not resolve for the supplied parameters. Neither nodename nor servname were passed. At least one of these must be passed.";
        case EAI_SERVICE: return "The service passed was not recognized for the specified socket type.";
        case EAI_SOCKTYPE: return "The intended socket type was not recognized.";
        case EAI_SYSTEM: return "A system error occurred; the error code can be found in errno";
        case EAI_BADEXTFLAGS: return "The extended flags parameter had an invalid value.";
        case EAI_OVERFLOW: return "There is not enough buffer space for the requested operation. The buffer pointed to by nodename or servname is not large enough.";
        default: {
                char *m=msg[(msgi++)%10];
                sprintf(m, "Unknown error code at getaddrinfo/getnameinfo %d", n);
                return m;
                 }
        }
        return "";
}

#endif