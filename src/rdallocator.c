/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2021, Magnus Edenhill
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

static void *rd_default_malloc(size_t sz, void *opaque) {
        void *p = malloc(sz);
        rd_assert(p);
        return p;
}

static void *rd_default_calloc(size_t n, size_t sz, void *opaque) {
        void *p = calloc(n, sz);
        rd_assert(p);
        return p;
}

static void *rd_default_realloc(void *ptr, size_t sz, void *opaque) {
        void *p = realloc(ptr, sz);
        rd_assert(p);
        return p;
}

static void rd_default_free(void *ptr, void *opaque) {
        free(ptr);
}

static char *rd_default_strdup(const char *s, void *opaque) {
#ifndef _WIN32
        char *n = strdup(s);
#else
        char *n = _strdup(s);
#endif
        rd_assert(n);
        return n;
}

static char *rd_default_strndup(const char *s, size_t len, void *opaque) {
#if HAVE_STRNDUP
        char *n = strndup(s, len);
        rd_assert(n);
#else
        char *n = (char *)rd_malloc(len + 1);
        rd_assert(n);
        memcpy(n, s, len);
        n[len] = '\0';
#endif
        return n;
}

struct rd_allocator _rd_allocator = {rd_false,          rd_default_malloc,
                                     rd_default_calloc, rd_default_realloc,
                                     rd_default_strdup, rd_default_strndup,
                                     rd_default_free,   NULL};

void rd_kafka_set_allocator(void *f_malloc,
                            void *f_calloc,
                            void *f_realloc,
                            void *f_strdup,
                            void *f_strndup,
                            void *f_free,
                            void *opaque) {
        rd_assert(!_rd_allocator.is_set);
        _rd_allocator._malloc  = f_malloc;
        _rd_allocator._calloc  = f_calloc;
        _rd_allocator._realloc = f_realloc;
        _rd_allocator._strdup  = f_strdup;
        _rd_allocator._strndup = f_strndup;
        _rd_allocator._free    = f_free;
        _rd_allocator.opaque   = opaque;
        _rd_allocator.is_set   = rd_true;
}
