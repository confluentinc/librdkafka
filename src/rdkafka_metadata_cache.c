/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013, Magnus Edenhill
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
#include "rdkafka_int.h"
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdkafka_request.h"
#include "rdkafka_metadata.h"

#include <string.h>
/**
 * @{
 *
 * @brief Metadata cache
 *
 */


/**
 * @brief Remove and free cache entry.
 *
 * @remark The expiry timer is not updated, for simplicity.
 * @locks rd_kafka_wrlock()
 */
static RD_INLINE void
rd_kafka_metadata_cache_delete (rd_kafka_t *rk,
                                struct rd_kafka_metadata_cache_entry *rkmce,
                                int unlink_avl) {
        if (unlink_avl)
                RD_AVL_REMOVE_ELM(&rk->rk_metadata_cache.rkmc_avl, rkmce);
        TAILQ_REMOVE(&rk->rk_metadata_cache.rkmc_expiry, rkmce, rkmce_link);
        rd_kafka_assert(NULL, rk->rk_metadata_cache.rkmc_cnt > 0);
        rk->rk_metadata_cache.rkmc_cnt--;
        rd_free(rkmce);
}

static int rd_kafka_metadata_cache_evict (rd_kafka_t *rk);

/**
 * @brief Cache eviction timer callback.
 * @locality rdkafka main thread
 * @locks NOT rd_kafka_*lock()
 */
static void rd_kafka_metadata_cache_evict_tmr_cb (rd_kafka_timers_t *rkts,
                                                  void *arg) {
        rd_kafka_t *rk = arg;

        rd_kafka_wrlock(rk);
        rd_kafka_metadata_cache_evict(rk);
        rd_kafka_wrunlock(rk);
}


/**
 * @brief Evict timed out entries from cache and rearm timer for
 *        next expiry.
 *
 * @returns the number of entries evicted.
 *
 * @locks rd_kafka_wrlock()
 */
static int rd_kafka_metadata_cache_evict (rd_kafka_t *rk) {
        int cnt = 0;
        rd_ts_t now = rd_clock();
        struct rd_kafka_metadata_cache_entry *rkmce;

        while ((rkmce = TAILQ_FIRST(&rk->rk_metadata_cache.rkmc_expiry)) &&
               rkmce->rkmce_ts_expires <= now) {
                rd_kafka_metadata_cache_delete(rk, rkmce, 1);
                cnt++;
        }

        if (rkmce)
                rd_kafka_timer_start(&rk->rk_timers,
                                     &rk->rk_metadata_cache.rkmc_expiry_tmr,
                                     rkmce->rkmce_ts_expires - now,
                                     rd_kafka_metadata_cache_evict_tmr_cb,
                                     rk);
        else
                rd_kafka_timer_stop(&rk->rk_timers,
                                    &rk->rk_metadata_cache.rkmc_expiry_tmr, 1);

        rd_kafka_dbg(rk, METADATA, "METADATA",
                     "Expired %d entries from metadata cache "
                     "(%d entries remain)",
                     cnt, rk->rk_metadata_cache.rkmc_cnt);

        return cnt;
}


/**
 * @brief Find cache entry by topic name
 *
 * @locks rd_kafka_*lock()
 */
static RD_INLINE struct rd_kafka_metadata_cache_entry *
rd_kafka_metadata_cache_find (rd_kafka_t *rk, const char *topic) {
        struct rd_kafka_metadata_cache_entry skel;
        skel.rkmce_mtopic.topic = (char *)topic;
        return RD_AVL_FIND(&rk->rk_metadata_cache.rkmc_avl, &skel);
}


/**
 * @brief Partition (id) comparator
 */
static int rd_kafka_metadata_partition_id_cmp (const void *_a,
                                               const void *_b) {
        const rd_kafka_metadata_partition_t *a = _a, *b = _b;
        return a->id - b->id;
}


/**
 * @brief Add (and replace) cache entry for topic.
 *
 * This makes a copy of \p topic
 *
 * @locks rd_kafka_wrlock()
 */
static struct rd_kafka_metadata_cache_entry *
rd_kafka_metadata_cache_insert (rd_kafka_t *rk,
                                const rd_kafka_metadata_topic_t *mtopic,
                                rd_ts_t ts_expires) {
        struct rd_kafka_metadata_cache_entry *rkmce, *old;
        size_t topic_len, needed_size;
        char *ptr;
        int i;

        topic_len = strlen(mtopic->topic) + 1;
        needed_size = sizeof(*rkmce) + sizeof(*mtopic) + topic_len +
                (mtopic->partition_cnt * sizeof(*mtopic->partitions));

        rkmce = rd_malloc(needed_size);


        rkmce->rkmce_mtopic = *mtopic;
        /* Memory for topic name and partitions is allocated directly
         * following the entry struct */
        ptr = (char *)(rkmce+1);

        /* Topic name */
        rkmce->rkmce_mtopic.topic = ptr;
        memcpy(rkmce->rkmce_mtopic.topic, mtopic->topic, topic_len);
        ptr += topic_len;

        /* Partition array */
        rkmce->rkmce_mtopic.partitions = (rd_kafka_metadata_partition_t *)ptr;
        for (i = 0 ; i < mtopic->partition_cnt ; i++) {
                rkmce->rkmce_mtopic.partitions[i] = mtopic->partitions[i];
                /* Clear uncached fields. */
                rkmce->rkmce_mtopic.partitions[i].replicas = NULL;
                rkmce->rkmce_mtopic.partitions[i].replica_cnt = 0;
                rkmce->rkmce_mtopic.partitions[i].isrs = NULL;
                rkmce->rkmce_mtopic.partitions[i].isr_cnt = 0;
        }

        /* Sort partitions for future bsearch() lookups. */
        qsort(rkmce->rkmce_mtopic.partitions,
              rkmce->rkmce_mtopic.partition_cnt,
              sizeof(*rkmce->rkmce_mtopic.partitions),
              rd_kafka_metadata_partition_id_cmp);

        TAILQ_INSERT_TAIL(&rk->rk_metadata_cache.rkmc_expiry,
                          rkmce, rkmce_link);
        rk->rk_metadata_cache.rkmc_cnt++;
        rkmce->rkmce_ts_expires = ts_expires;

        /* Insert (and replace existing) entry. */
        old = RD_AVL_INSERT(&rk->rk_metadata_cache.rkmc_avl, rkmce,
                            rkmce_avlnode);
        if (old)
                rd_kafka_metadata_cache_delete(rk, old, 0);



        return rkmce;
}


/**
 * @brief Purge the metadata cache
 *
 * @locks rd_kafka_wrlock()
 */
static void rd_kafka_metadata_cache_purge (rd_kafka_t *rk) {
        struct rd_kafka_metadata_cache_entry *rkmce;

        while ((rkmce = TAILQ_FIRST(&rk->rk_metadata_cache.rkmc_expiry)))
                rd_kafka_metadata_cache_delete(rk, rkmce, 1);

        rd_kafka_timer_stop(&rk->rk_timers,
                            &rk->rk_metadata_cache.rkmc_expiry_tmr, 1);
}



/**
 * @brief Update the metadata cache with the provided metadata.
 *
 * @param abs_update int: absolute update: purge cache before updating.
 *
 * @locks rd_kafka_wrlock()
 */
void rd_kafka_metadata_cache_update (rd_kafka_t *rk,
                                     const rd_kafka_metadata_t *md,
                                     int abs_update) {
        struct rd_kafka_metadata_cache_entry *rkmce;
        rd_ts_t now = rd_clock();
        rd_ts_t ts_expires = now + (rk->rk_conf.metadata_refresh_interval_ms *
                                    1000 * 2);
        int i;

        rd_kafka_dbg(rk, METADATA, "METADATA",
                     "Updating metadata cache with %d topic(s)",
                     md->topic_cnt);

        if (abs_update)
                rd_kafka_metadata_cache_purge(rk);


        for (i = 0 ; i < md->topic_cnt ; i++)
                rd_kafka_metadata_cache_insert(rk, &md->topics[i], ts_expires);

        /* Update expiry timer */
        if ((rkmce = TAILQ_FIRST(&rk->rk_metadata_cache.rkmc_expiry)))
                rd_kafka_timer_start(&rk->rk_timers,
                                     &rk->rk_metadata_cache.rkmc_expiry_tmr,
                                     rkmce->rkmce_ts_expires - now,
                                     rd_kafka_metadata_cache_evict_tmr_cb,
                                     rk);
}



/**
 * @brief Cache entry comparator (on topic name)
 */
static int rd_kafka_metadata_cache_entry_cmp (const void *_a, const void *_b) {
        const struct rd_kafka_metadata_cache_entry *a = _a, *b = _b;
        return strcmp(a->rkmce_mtopic.topic, b->rkmce_mtopic.topic);
}


/**
 * @brief Initialize the metadata cache
 *
 * @locks rd_kafka_wrlock()
 */
void rd_kafka_metadata_cache_init (rd_kafka_t *rk) {
        rd_avl_init(&rk->rk_metadata_cache.rkmc_avl,
                    rd_kafka_metadata_cache_entry_cmp, 0);
        TAILQ_INIT(&rk->rk_metadata_cache.rkmc_expiry);
}

/**
 * @brief Purge and destroy metadata cache
 *
 * @locks rd_kafka_wrlock()
 */
void rd_kafka_metadata_cache_destroy (rd_kafka_t *rk) {
        rd_kafka_metadata_cache_purge(rk);
        rd_avl_destroy(&rk->rk_metadata_cache.rkmc_avl);
}


/**
 * @returns the shared metadata for a topic, or NULL if not found in
 *          cache.
 *
 * @locks rd_kafka_*lock()
 */
const rd_kafka_metadata_topic_t *
rd_kafka_metadata_cache_topic_get (rd_kafka_t *rk, const char *topic) {
        struct rd_kafka_metadata_cache_entry *rkmce;

        if (!(rkmce = rd_kafka_metadata_cache_find(rk, topic)))
                return NULL;

        return &rkmce->rkmce_mtopic;
}



/**
 * @returns the shared metadata for a partition, or NULL if not found in
 *          cache.
 *
 * @locks rd_kafka_*lock()
 */
const rd_kafka_metadata_partition_t *
rd_kafka_metadata_cache_partition_get (rd_kafka_t *rk,
                                       const char *topic, int32_t partition) {
        const rd_kafka_metadata_topic_t *mtopic;
        rd_kafka_metadata_partition_t skel = { .id = partition };

        if (!(mtopic = rd_kafka_metadata_cache_topic_get(rk, topic)))
                return NULL;

        /* Partitions array may be sparse so use bsearch lookup. */
        return bsearch(&skel, mtopic->partitions,
                       mtopic->partition_cnt,
                       sizeof(*mtopic->partitions),
                       rd_kafka_metadata_partition_id_cmp);
}


/**@}*/
