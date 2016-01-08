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

#include "rd.h"
#include "rdlist.h"


void rd_list_dump (const char *what, const rd_list_t *rl) {
        int i;
        printf("%s: (rd_list_t*)%p cnt %d, size %d, elems %p:\n",
               what, rl, rl->rl_cnt, rl->rl_size, rl->rl_elems);
        for (i = 0 ; i < rl->rl_cnt ; i++)
                printf("  #%d: %p at &%p\n", i,
                       rl->rl_elems[i], &rl->rl_elems[i]);
}

static void rd_list_grow (rd_list_t *rl, int add_size) {
        rl->rl_size += add_size;
        rl->rl_elems = rd_realloc(rl->rl_elems,
                                  sizeof(*rl->rl_elems) * rl->rl_size);
}

void rd_list_init (rd_list_t *rl, int initial_size) {
        memset(rl, 0, sizeof(*rl));

        rd_list_grow(rl, initial_size);
}

rd_list_t *rd_list_new (int initial_size) {
	rd_list_t *rl = malloc(sizeof(*rl));
	rd_list_init(rl, initial_size);
	rl->rl_allocated = 1;
	return rl;
}


void rd_list_set_free_cb (rd_list_t *rl, void (*free_cb) (void *)) {
	rl->rl_free_cb = free_cb;
}


void rd_list_add (rd_list_t *rl, void *elem) {
        if (rl->rl_cnt == rl->rl_size)
                rd_list_grow(rl, rl->rl_size ? rl->rl_size * 2 : 16);
        rl->rl_elems[rl->rl_cnt++] = elem;
}

static void rd_list_remove0 (rd_list_t *rl, int idx) {
        rd_assert(idx < rl->rl_cnt);

        if (idx + 1 < rl->rl_cnt)
                memmove(&rl->rl_elems[idx],
                        &rl->rl_elems[idx+1],
                        sizeof(*rl->rl_elems) * (rl->rl_cnt - (idx+1)));
        rl->rl_cnt--;
}

void *rd_list_remove (rd_list_t *rl, void *match_elem) {
        void *elem;
        int i;

        RD_LIST_FOREACH(elem, rl, i) {
                if (elem == match_elem) {
                        rd_list_remove0(rl, i);
                        return elem;
                }
        }

        return NULL;
}


void *rd_list_remove_cmp (rd_list_t *rl, void *match_elem,
                          int (*cmp) (void *_a, void *_b)) {
        void *elem;
        int i;

        RD_LIST_FOREACH(elem, rl, i) {
                if (match_elem == cmp ||
                    !cmp(elem, match_elem)) {
                        rd_list_remove0(rl, i);
                        return elem;
                }
        }

        return NULL;
}


void rd_list_sort (rd_list_t *rl, int (*cmp) (const void *, const void *)) {
        qsort(rl->rl_elems, rl->rl_cnt, sizeof(*rl->rl_elems), cmp);
}

void rd_list_clear (rd_list_t *rl) {
        rl->rl_cnt = 0;
}


void rd_list_destroy (rd_list_t *rl, void (*free_cb) (void *)) {

	if (!free_cb)
		free_cb = rl->rl_free_cb;

        if (rl->rl_elems) {
                int i;
                if (free_cb) {
                        for (i = 0 ; i < rl->rl_cnt ; i++)
                                if (rl->rl_elems[i])
                                        free_cb(rl->rl_elems[i]);
                }
                rd_free(rl->rl_elems);
        }

	if (rl->rl_allocated)
		rd_free(rl);
}


void *rd_list_elem (const rd_list_t *rl, int idx) {
        if (likely(idx < rl->rl_cnt))
                return (void *)rl->rl_elems[idx];
        return NULL;
}

void *rd_list_find (const rd_list_t *rl, const void *match,
                    int (*cmp) (const void *, const void *)) {
        int i;
        const void *elem;

        RD_LIST_FOREACH(elem, rl, i) {
                if (!cmp(match, elem))
                        return (void *)elem;
        }

        return NULL;
}
