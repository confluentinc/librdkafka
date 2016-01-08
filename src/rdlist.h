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
 *
 * Simple light-weight append-only list to be used as a collection convenience.
 *
 */

typedef struct rd_list_s {
        int    rl_size;
        int    rl_cnt;
        void **rl_elems;
	void (*rl_free_cb) (void *);
	int    rl_allocated;
} rd_list_t;


/**
 * Initialize a list, preallocate space for 'initial_size' elements (optional)
 */
void rd_list_init (rd_list_t *rl, int initial_size);


/**
 * Allocate a new list pointer and initialize it according to rd_list_init().
 *
 * Use rd_list_destroy() to free.
 */
rd_list_t *rd_list_new (int initial_size);


/**
 * Set element free callback
 */
void rd_list_set_free_cb (rd_list_t *rl, void (*free_cb) (void *));


/**
 * Append element to list
 */
void rd_list_add (rd_list_t *rl, void *elem);


/**
 * Remove element from list.
 * This is a slow O(n) + memmove operation.
 * Returns the removed element.
 */
void *rd_list_remove (rd_list_t *rl, void *match_elem);

/**
 * Remove element from list using comparator.
 * See rd_list_remove()
 */
void *rd_list_remove_cmp (rd_list_t *rl, void *match_elem,
                         int (*cmp) (void *_a, void *_b));

/**
 * Sort list using comparator
 */
void rd_list_sort (rd_list_t *rl, int (*cmp) (const void *, const void *));


/**
 * Empties the list (but does not free any memory)
 */
void rd_list_clear (rd_list_t *rl);

/**
 * Empties the list, frees the element array, and optionally frees
 * each element using 'free_cb' or 'rl->rl_free_cb'.
 *
 * If the list was previously allocated with rd_list_new() it will be freed.
 */
void rd_list_destroy (rd_list_t *rl, void (*free_cb) (void *));


/**
 * Returns the element at index 'idx', or NULL if out of range.
 *
 * Typical iteration is:
 *    int i = 0;
 *    my_type_t *obj;
 *    while ((obj = rd_list_elem(rl, i++)))
 *        do_something(obj);
 */
void *rd_list_elem (const rd_list_t *rl, int idx);

#define RD_LIST_FOREACH(elem,listp,idx) \
        for (idx = 0 ; (elem = rd_list_elem(listp, idx)) ; idx++)

#define RD_LIST_FOREACH_REVERSE(elem,listp,idx)                         \
        for (idx = (listp)->rl_cnt-1 ;                                  \
             idx >= 0 && (elem = rd_list_elem(listp, idx)) ; idx--)

/**
 * Returns the number of elements in list.
 */
static __inline RD_UNUSED int rd_list_cnt (const rd_list_t *rl) {
        return rl->rl_cnt;
}


/**
 * Returns true if list is empty
 */
#define rd_list_empty(rl) (rd_list_cnt(rl) == 0)



/**
 * Find element using comparator
 * 'match' will be the first argument to 'cmp', and each element (up to a match)
 * will be the second argument to 'cmp'.
 */
void *rd_list_find (const rd_list_t *rl, const void *match,
                    int (*cmp) (const void *, const void *));



/**
 * Debugging: Print list to stdout.
 */
void rd_list_dump (const char *what, const rd_list_t *rl);
