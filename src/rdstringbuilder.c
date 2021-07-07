/*
 * librdkafka - The Apache Kafka C/C++ library
 * 
 * Copyright (c) 2017 Magnus Edenhill
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

#include <ctype.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "rdstringbuilder.h"
#include "rd.h"
#include "rdunittest.h"


static const size_t str_builder_min_size = 32;


struct str_builder {
    char   *str;
    size_t  alloced;
    size_t  len;
};


str_builder_t *str_builder_create (void)
{
    str_builder_t *sb;

    sb          = calloc(1, sizeof(*sb));
    sb->str     = malloc(str_builder_min_size);
    *sb->str    = '\0';
    sb->alloced = str_builder_min_size;
    sb->len     = 0;

    return sb;
}


void str_builder_destroy (str_builder_t *sb)
{
    if (sb == NULL)
        return;
    rd_free(sb->str);
    rd_free(sb);
}


/** Ensure there is enough space for data being added plus a NULL terminator.
 *
 * param[in,out] sb      Builder.
 * param[in]     add_len The length that needs to be added *not* including a NULL terminator.
 */
static void str_builder_ensure_space (str_builder_t *sb, size_t add_len)
{
    if (sb == NULL || add_len == 0)
        return;

    if (sb->alloced >= sb->len+add_len+1)
        return;

    while (sb->alloced < sb->len+add_len+1) {
        /* Doubling growth strategy. */
        sb->alloced <<= 1;
        if (sb->alloced == 0) {
            /* Left shift of max bits will go to 0. An unsigned type set to
             * -1 will return the maximum possible size. However, we should
             *  have run out of memory well before we need to do this. Since
             *  this is the theoretical maximum total system memory we don't
             *  have a flag saying we can't grow any more because it should
             *  be impossible to get to this point. */
            sb->alloced--;
        }
    }
    char *tmp = realloc(sb->str, sb->alloced);
    if (tmp)   
        sb->str = tmp;
    else
        exit(1);
}


void str_builder_add_str (str_builder_t *sb, const char *str)
{
    if (sb == NULL || str == NULL || *str == '\0')
        return;

    size_t len = strlen(str);

    str_builder_ensure_space(sb, len);
    memmove(sb->str+sb->len, str, len);
    sb->len += len;
    sb->str[sb->len] = '\0';
}


void str_builder_clear (str_builder_t *sb)
{
    if (sb == NULL)
        return;
    str_builder_truncate(sb, 0);
}


void str_builder_truncate (str_builder_t *sb, size_t len)
{
    if (sb == NULL || len >= sb->len)
        return;

    sb->len = len;
    sb->str[sb->len] = '\0';
}


void str_builder_drop (str_builder_t *sb, size_t len)
{
    if (sb == NULL || len == 0)
        return;

    if (len >= sb->len) {
        str_builder_clear(sb);
        return;
    }

    sb->len -= len;
    /* +1 to move the NULL. */
    memmove(sb->str, sb->str+len, sb->len+1);
}


size_t str_builder_len (const str_builder_t *sb)
{
    if (sb == NULL)
        return 0;
    return sb->len;
}


const char *str_builder_peek (const str_builder_t *sb)
{
    if (sb == NULL)
        return NULL;
    return sb->str;
}


char *str_builder_dump (const str_builder_t *sb)
{
    char *out;

    if (sb == NULL)
        return NULL;

    out = malloc(sb->len+1);
    memcpy(out, sb->str, sb->len+1);
    return out;
}


static int unittest_build_string (void) {
    RD_UT_BEGIN();
    str_builder_t *sb;
    sb = str_builder_create();
    str_builder_add_str(sb, "AWS4");
    RD_UT_ASSERT(4 == str_builder_len(sb), "expected: %d\nactual: %d", 4, (int)str_builder_len(sb));
    RD_UT_ASSERT(strcmp("AWS4", str_builder_peek(sb)) == 0, "expected: %s\nactual: %s", "AWS4", str_builder_peek(sb));
    
    str_builder_clear(sb);
    str_builder_add_str(sb, "TEST1");
    RD_UT_ASSERT(5 == str_builder_len(sb), "expected: %d\nactual: %d", 5, (int)str_builder_len(sb));
    RD_UT_ASSERT(strcmp("TEST1", str_builder_peek(sb)) == 0, "expected: %s\nactual: %s", "TEST1", str_builder_peek(sb));
    
    str_builder_drop(sb, 2);
    RD_UT_ASSERT(3 == str_builder_len(sb), "expected: %d\nactual: %d", 3, (int)str_builder_len(sb));
    RD_UT_ASSERT(strcmp("ST1", str_builder_peek(sb)) == 0, "expected: %s\nactual: %s", "ST1", str_builder_peek(sb));
    
    str_builder_truncate(sb, 1);
    RD_UT_ASSERT(1 == str_builder_len(sb), "expected: %d\nactual: %d", 1, (int)str_builder_len(sb));
    RD_UT_ASSERT(strcmp("S", str_builder_peek(sb)) == 0, "expected: %s\nactual: %s", "S", str_builder_peek(sb));
    
    char *out = str_builder_dump(sb);
    RD_UT_ASSERT(1 == strlen(out), "expected: %d\nactual: %d", 1, (int)strlen(out));
    RD_UT_ASSERT(strcmp("S", out) == 0, "expected: %s\nactual: %s", "S", out);
    
    RD_IF_FREE(out, rd_free);
    str_builder_destroy(sb);
    
    RD_UT_PASS();
}


int unittest_stringbuilder (void) {
        int fails = 0;

        fails += unittest_build_string();

        return fails;
}
