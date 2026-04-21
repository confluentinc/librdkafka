/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#include "rdkafka_int.h"
#include "rdkafka_share_acknowledgement.h"

rd_kafka_share_ack_batch_entry_t *
rd_kafka_share_ack_batch_entry_new(int64_t start_offset,
                                   int64_t end_offset,
                                   int32_t types_cnt,
                                   int16_t delivery_count) {
        rd_kafka_share_ack_batch_entry_t *entry;

        entry                 = rd_calloc(1, sizeof(*entry));
        entry->start_offset   = start_offset;
        entry->end_offset     = end_offset;
        entry->size           = end_offset - start_offset + 1;
        entry->types_cnt      = types_cnt;
        entry->delivery_count = delivery_count;
        entry->types =
            rd_calloc((size_t)types_cnt,
                      sizeof(rd_kafka_share_internal_acknowledgement_type));
        return entry;
}

void rd_kafka_share_ack_batch_entry_destroy(
    rd_kafka_share_ack_batch_entry_t *entry) {
        if (!entry)
                return;
        rd_free(entry->types);
        rd_free(entry);
}

static void rd_kafka_share_ack_batch_entry_destroy_free(void *ptr) {
        rd_kafka_share_ack_batch_entry_destroy(
            (rd_kafka_share_ack_batch_entry_t *)ptr);
}

rd_bool_t
rd_kafka_share_acknowledgement_mode_is_implicit(rd_kafka_share_t *rkshare) {
        return rkshare->rkshare_rk->rk_conf.share.share_acknowledgement_mode &&
               !strcmp(rkshare->rkshare_rk->rk_conf.share
                           .share_acknowledgement_mode,
                       "implicit");
}

rd_bool_t
rd_kafka_share_acknowledgement_mode_is_explicit(rd_kafka_share_t *rkshare) {
        return rkshare->rkshare_rk->rk_conf.share.share_acknowledgement_mode &&
               !strcmp(rkshare->rkshare_rk->rk_conf.share
                           .share_acknowledgement_mode,
                       "explicit");
}

void rd_kafka_share_acknowledge_all_if_implicit(rd_kafka_share_t *rkshare) {
        if (rd_kafka_share_acknowledgement_mode_is_implicit(rkshare))
                rd_kafka_share_ack_all(rkshare);
}

rd_kafka_error_t *
rd_kafka_share_ensure_all_acknowledged_if_explicit(rd_kafka_share_t *rkshare) {
        if (rd_kafka_share_acknowledgement_mode_is_explicit(rkshare) &&
            rkshare->rkshare_unacked_cnt > 0)
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__STATE,
                    "%" PRId64
                    " records from previous poll have not "
                    "been acknowledged",
                    rkshare->rkshare_unacked_cnt);
        return NULL;
}

rd_kafka_share_ack_batch_entry_t *rd_kafka_share_ack_batch_entry_copy(
    const rd_kafka_share_ack_batch_entry_t *src) {
        rd_kafka_share_ack_batch_entry_t *dst;

        dst = rd_kafka_share_ack_batch_entry_new(
            src->start_offset, src->end_offset, src->types_cnt,
            src->delivery_count);
        memcpy(dst->types, src->types,
               (size_t)src->types_cnt *
                   sizeof(rd_kafka_share_internal_acknowledgement_type));
        return dst;
}

static void *rd_kafka_share_ack_batch_entry_copy_void(const void *elem,
                                                      void *opaque) {
        return rd_kafka_share_ack_batch_entry_copy(
            (const rd_kafka_share_ack_batch_entry_t *)elem);
}

rd_kafka_share_ack_batches_t *rd_kafka_share_ack_batches_new_empty(void) {
        return rd_kafka_share_ack_batches_new(NULL, 0, 0, 0);
}

rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batches_new(rd_kafka_topic_partition_t *rktpar,
                               int32_t response_leader_id,
                               int32_t response_leader_epoch,
                               int64_t response_acquired_offsets_count) {
        rd_kafka_share_ack_batches_t *batches;

        batches                        = rd_calloc(1, sizeof(*batches));
        batches->rktpar                = rktpar;
        batches->response_leader_id    = response_leader_id;
        batches->response_leader_epoch = response_leader_epoch;
        batches->response_acquired_offsets_count =
            response_acquired_offsets_count;
        rd_list_init(&batches->entries, 0,
                     rd_kafka_share_ack_batch_entry_destroy_free);
        return batches;
}

void rd_kafka_share_ack_batches_destroy(rd_kafka_share_ack_batches_t *batches) {
        rd_list_destroy(&batches->entries);
        if (batches->rktpar)
                rd_kafka_topic_partition_destroy(batches->rktpar);
        rd_free(batches);
}

void rd_kafka_share_ack_batches_destroy_free(void *ptr) {
        rd_kafka_share_ack_batches_destroy((rd_kafka_share_ack_batches_t *)ptr);
}

rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batches_copy(const rd_kafka_share_ack_batches_t *src) {
        rd_kafka_share_ack_batches_t *dst;

        dst = rd_kafka_share_ack_batches_new(
            src->rktpar ? rd_kafka_topic_partition_copy(src->rktpar) : NULL,
            src->response_leader_id, src->response_leader_epoch,
            src->response_acquired_offsets_count);

        /* Deep copy all entries and preserve flags (e.g., RD_LIST_F_SORTED) */
        rd_list_copy_to(&dst->entries, &src->entries,
                        rd_kafka_share_ack_batch_entry_copy_void, NULL);
        dst->entries.rl_flags = src->entries.rl_flags;
        return dst;
}

void *rd_kafka_share_ack_batches_copy_void(const void *elem, void *opaque) {
        return rd_kafka_share_ack_batches_copy(
            (const rd_kafka_share_ack_batches_t *)elem);
}

/**
 * @brief Transfer inflight acks from response RKO into rkshare's inflight map.
 *
 * Takes each batch from the response's inflight_acks list and stores it in
 * the map (key = topic-partition). Ownership is transferred; the response
 * RKO must not free these when destroyed.
 * In the future, the map will be cleared per partition after acks are sent.
 *
 * @param rkshare Share consumer handle
 * @param response_rko The share fetch response RKO containing inflight_acks
 */
void rd_kafka_share_build_inflight_acks_map(rd_kafka_share_t *rkshare,
                                            rd_kafka_op_t *response_rko) {
        rd_list_t *list =
            response_rko->rko_u.share_fetch_response.inflight_acks;

        while (rd_list_cnt(list) > 0) {
                rd_kafka_share_ack_batches_t *batches = rd_list_pop(list);
                rd_kafka_topic_partition_t *key;
                rd_kafka_share_ack_batch_entry_t *entry;
                int i, k;

                rd_dassert(batches->rktpar != NULL);

                key = rd_kafka_topic_partition_new_with_id_and_name(
                    rd_kafka_topic_partition_get_topic_id(batches->rktpar),
                    batches->rktpar->topic, batches->rktpar->partition);

                /* Each topic-partition is always a new entry (no overwrites).
                 */
                RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);

                /* Count ACQUIRED types for unacked tracking */
                RD_LIST_FOREACH(entry, &batches->entries, i) {
                        for (k = 0; k < entry->types_cnt; k++) {
                                if (entry->types[k] ==
                                    RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED)
                                        rkshare->rkshare_unacked_cnt++;
                        }
                }
        }
}

/**
 * @brief Create a new collated batch entry with a single acknowledgement type.
 *
 * Creates an entry where size=1 and types[0] holds the single ack type
 * for the entire offset range [start_offset, end_offset].
 *
 * @param start_offset First offset in the range
 * @param end_offset Last offset in the range (inclusive)
 * @param type The acknowledgement type for the entire range
 *
 * @returns Newly allocated collated entry (caller must free)
 */
static rd_kafka_share_ack_batch_entry_t *
rd_kafka_share_ack_batch_entry_collated_new(
    int64_t start_offset,
    int64_t end_offset,
    rd_kafka_share_internal_acknowledgement_type type,
    int16_t delivery_count) {
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_kafka_share_ack_batch_entry_new(start_offset, end_offset, 1,
                                               delivery_count);
        entry->types[0] = type;
        return entry;
}

/**
 * @brief Implicit acknowledgement: convert all ACQUIRED types to ACCEPT.
 *
 * In implicit ack mode, all records delivered to the application are
 * automatically acknowledged as ACCEPT on the next poll(). This function
 * walks through all entries in the inflight map and changes ACQUIRED
 * types to ACCEPT so that rd_kafka_share_build_ack_details() will
 * extract them for sending to the broker.
 *
 * @param rkshare Share consumer handle
 */
void rd_kafka_share_ack_all(rd_kafka_share_t *rkshare) {
        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *inflight_batches;

        RD_MAP_FOREACH(tp_key, inflight_batches,
                       &rkshare->rkshare_inflight_acks) {
                rd_kafka_share_ack_batch_entry_t *entry;
                int i;
                rd_kafka_dbg(rkshare->rkshare_rk, CGRP, "SHAREACK",
                             "Implicit ack: converting ACQUIRED to ACCEPT "
                             "for %s [%" PRId32 "]",
                             tp_key->topic, tp_key->partition);
                RD_LIST_FOREACH(entry, &inflight_batches->entries, i) {
                        int k;
                        for (k = 0; k < entry->types_cnt; k++) {
                                if (entry->types[k] ==
                                    RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED)
                                        entry->types[k] =
                                            RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
                        }
                }
        }
}

/**
 * @brief Free an ack details batch, its entries, and its owned rktpar.
 *
 * Used as rd_list_t free callback for ack_details output lists where
 * each batch owns a copy of its rktpar.
 */
static void rd_kafka_share_ack_details_batch_destroy(void *ptr) {
        rd_kafka_share_ack_batches_t *batch = ptr;
        // TODO KIP-932: Check if this null check is needed
        if (!batch)
                return;
        rd_kafka_share_ack_batches_destroy(batch);
}

/**
 * @brief Find an existing ack batch for the same topic-partition in a list.
 *
 * @param ack_list List of rd_kafka_share_ack_batches_t*
 * @param rktpar Topic-partition to match (by topic id and partition)
 *
 * @returns Matching batch, or NULL if not found.
 * @locality main thread
 */
rd_kafka_share_ack_batches_t *
rd_kafka_share_find_ack_batch(rd_list_t *ack_list,
                              const rd_kafka_topic_partition_t *rktpar) {
        rd_kafka_share_ack_batches_t *existing;
        int i;

        RD_LIST_FOREACH(existing, ack_list, i) {
                /* TODO KIP-932: We might need to use leader id and epoch
                 * as well to understand about the stale topic partition
                 * information */
                if (rd_kafka_topic_partition_by_id_cmp(existing->rktpar,
                                                       rktpar) == 0)
                        return existing;
        }
        return NULL;
}

/**
 * @brief Segregate ack batches from a FANOUT op by partition leader.
 *
 * For each ack batch, looks up the current leader broker via the
 * toppar reference in batch->rktpar, and merges the batch into that
 * broker's rkb_share_async_ack_details list. If the broker already
 * has cached acks for the same topic-partition, the entries are
 * appended to the existing batch.
 *
 * @param rk Client instance
 * @param ack_batches List of rd_kafka_share_ack_batches_t* from FANOUT op.
 *                    Elements whose leader is found are moved to broker
 *                    ack_details; remaining elements are not freed.
 *
 * @locality main thread
 */
void rd_kafka_share_segregate_acks_by_leader(rd_kafka_t *rk,
                                             rd_list_t *ack_batches) {
        rd_kafka_share_ack_batches_t *batch;
        int batch_cnt = rd_list_cnt(ack_batches);

        while ((batch = rd_list_pop(ack_batches))) {
                rd_kafka_toppar_t *rktp;
                rd_kafka_broker_t *leader_rkb;
                rd_kafka_share_ack_batches_t *existing;

                rktp = rd_kafka_topic_partition_toppar(rk, batch->rktpar);
                if (!rktp || !rktp->rktp_leader) {
                        rd_kafka_dbg(rk, CGRP, "SHARE",
                                     "Ack batch for leader %" PRId32
                                     " dropped: toppar or leader not available",
                                     batch->response_leader_id);
                        rd_kafka_share_ack_batches_destroy(batch);
                        continue;
                }
                leader_rkb = rktp->rktp_leader;

                /* Allocate list on first use with incoming batch count
                 * as initial capacity hint */
                if (!leader_rkb->rkb_share_async_ack_details)
                        leader_rkb->rkb_share_async_ack_details = rd_list_new(
                            batch_cnt, rd_kafka_share_ack_batches_destroy_free);

                /* Check if there's already a batch for this topic-partition.
                 * If so, merge entries; otherwise add as new. */
                existing = rd_kafka_share_find_ack_batch(
                    leader_rkb->rkb_share_async_ack_details, batch->rktpar);

                if (existing) {
                        /* Merge: deep-copy entries from new batch into
                         * existing, preserving order. The source batch
                         * is then fully destroyed (freeing its entries). */
                        rd_kafka_share_ack_batch_entry_t *entry;
                        int j;
                        RD_LIST_FOREACH(entry, &batch->entries, j) {
                                rd_list_add(
                                    &existing->entries,
                                    rd_kafka_share_ack_batch_entry_copy(entry));
                        }
                        rd_kafka_share_ack_batches_destroy(batch);
                } else {
                        rd_list_add(leader_rkb->rkb_share_async_ack_details,
                                    batch);
                }
        }
}

/**
 * @brief Extract acknowledged (non-ACQUIRED) records from inflight map.
 *
 * Iterates through the inflight acknowledgement map and separates each
 * entry's per-offset types into:
 *   - Non-ACQUIRED offsets: collated into ack_details for sending to broker
 *     (consecutive same-type offsets merged into single entry with 1 type)
 *   - ACQUIRED offsets: kept in the map as new entries (per-offset types)
 *
 * Map entries with no remaining ACQUIRED offsets are removed.
 * If the map becomes empty after processing, it is cleared.
 *
 * @param rkshare Share consumer handle
 * @returns Allocated list of rd_kafka_share_ack_batches_t*, or NULL if
 *          there are no ack details to send. Caller must destroy.
 * TODO KIP-932: Change name.
 */
rd_list_t *rd_kafka_share_build_ack_details(rd_kafka_share_t *rkshare) {
        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *inflight_batches;
        rd_list_t keys_to_delete;
        rd_list_t *ack_details = NULL;
        int i;
        rd_kafka_topic_partition_t *del_key;

        rd_list_init(&keys_to_delete, 0, NULL);

        rkshare->rkshare_unacked_cnt = 0;

        RD_MAP_FOREACH(tp_key, inflight_batches,
                       &rkshare->rkshare_inflight_acks) {
                rd_kafka_share_ack_batches_t *ack_batch = NULL;
                rd_kafka_share_ack_batch_entry_t *entry;
                rd_list_t new_entries;
                int ei;

                rd_kafka_dbg(rkshare->rkshare_rk, CGRP, "SHAREACK",
                             "Building ack details for %s [%" PRId32 "]",
                             tp_key->topic, tp_key->partition);

                rd_list_init(&new_entries, 0,
                             rd_kafka_share_ack_batch_entry_destroy_free);

                RD_LIST_FOREACH(entry, &inflight_batches->entries, ei) {
                        int64_t j = 0;

                        while (j < entry->types_cnt) {
                                rd_kafka_share_internal_acknowledgement_type
                                    run_type      = entry->types[j];
                                int64_t run_start = entry->start_offset + j;
                                int64_t k         = j + 1;

                                /* Find end of consecutive same-type run */
                                while (k < entry->types_cnt &&
                                       entry->types[k] == run_type)
                                        k++;

                                int64_t run_end = entry->start_offset + k - 1;

                                if (run_type ==
                                    RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED) {
                                        /* ACQUIRED: keep in map */
                                        int32_t cnt = (int32_t)(k - j);
                                        rd_kafka_share_ack_batch_entry_t
                                            *new_entry =
                                                rd_kafka_share_ack_batch_entry_new(
                                                    run_start, run_end, cnt,
                                                    entry->delivery_count);
                                        int32_t m;
                                        for (m = 0; m < cnt; m++)
                                                new_entry->types[m] =
                                                    RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
                                        rd_list_add(&new_entries, new_entry);
                                        rkshare->rkshare_unacked_cnt += cnt;
                                } else {
                                        /* Non-ACQUIRED: add to ack_details
                                         */
                                        if (!ack_batch) {
                                                ack_batch = rd_kafka_share_ack_batches_new(
                                                    rd_kafka_topic_partition_copy(
                                                        inflight_batches
                                                            ->rktpar),
                                                    inflight_batches
                                                        ->response_leader_id,
                                                    inflight_batches
                                                        ->response_leader_epoch,
                                                    0);
                                        }
                                        rd_kafka_dbg(
                                            rkshare->rkshare_rk, CGRP,
                                            "SHAREACK",
                                            "    Adding ack detail for offsets "
                                            "[%" PRId64 " - %" PRId64
                                            "] with type %d",
                                            run_start, run_end, run_type);
                                        /* Collated: 1 type for entire
                                         * range */
                                        rd_list_add(
                                            &ack_batch->entries,
                                            rd_kafka_share_ack_batch_entry_collated_new(
                                                run_start, run_end, run_type,
                                                entry->delivery_count));
                                }

                                j = k;
                        }
                }

                /**
                 * TODO KIP-932: Check if it can be optimized as these are
                 *               internal thread operations and they should
                 *               be as fast as possible as this will be called
                 *               again and again.
                 */
                if (rd_list_is_sorted(&new_entries,
                                      rd_kafka_share_ack_entries_sort_cmp_ptr))
                        new_entries.rl_flags |= RD_LIST_F_SORTED;

                /* Replace inflight entries with ACQUIRED-only entries */
                rd_list_destroy(&inflight_batches->entries);
                inflight_batches->entries = new_entries;

                /* If no ACQUIRED offsets remain, mark for removal */
                if (rd_list_cnt(&inflight_batches->entries) == 0) {
                        rd_kafka_topic_partition_t *del_key =
                            rd_kafka_topic_partition_new_with_id_and_name(
                                rd_kafka_topic_partition_get_topic_id(
                                    inflight_batches->rktpar),
                                inflight_batches->rktpar->topic,
                                inflight_batches->rktpar->partition);
                        rd_list_add(&keys_to_delete, del_key);
                }

                if (ack_batch) {
                        if (!ack_details)
                                ack_details = rd_list_new(
                                    0,
                                    rd_kafka_share_ack_details_batch_destroy);
                        rd_list_add(ack_details, ack_batch);
                }
        }

        /* Remove map entries with no remaining ACQUIRED offsets */
        RD_LIST_FOREACH(del_key, &keys_to_delete, i) {
                RD_MAP_DELETE(&rkshare->rkshare_inflight_acks, del_key);
                rd_kafka_topic_partition_destroy(del_key);
        }
        rd_list_destroy(&keys_to_delete);

        /* Clear map if empty */
        if (RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 0)
                RD_MAP_CLEAR(&rkshare->rkshare_inflight_acks);

        return ack_details;
}

/**
 * @brief Update acknowledgement type for a specific offset within an entry.
 *
 * Handles the transition from ACQUIRED state by decrementing the unacked count.
 *
 * @param rkshare Share consumer handle.
 * @param entry The batch entry containing the offset.
 * @param idx Index within the entry's types array.
 * @param type New acknowledgement type.
 */
static void rd_kafka_share_update_acknowledgement_type(
    rd_kafka_share_t *rkshare,
    rd_kafka_share_ack_batch_entry_t *entry,
    int64_t idx,
    rd_kafka_share_AcknowledgeType_t type) {
        /* Decrement unacked count when transitioning from ACQUIRED */
        if (entry->types[idx] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED)
                rkshare->rkshare_unacked_cnt--;

        /* Update the type */
        entry->types[idx] = (rd_kafka_share_internal_acknowledgement_type)type;
}

/**
 * @brief Comparator for sorting/checking entries by start_offset.
 *
 * Used with rd_list_sort() and rd_list_is_sorted().
 */
int rd_kafka_share_ack_entries_sort_cmp_ptr(const void *_a, const void *_b) {
        const rd_kafka_share_ack_batch_entry_t *a =
            (const rd_kafka_share_ack_batch_entry_t *)_a;
        const rd_kafka_share_ack_batch_entry_t *b =
            (const rd_kafka_share_ack_batch_entry_t *)_b;

        if (a->start_offset < b->start_offset)
                return -1;
        if (a->start_offset > b->start_offset)
                return 1;
        return 0;
}

/**
 * @brief Comparator for finding an entry containing a given offset.
 *
 * Used with rd_list_find() for binary search when RD_LIST_F_SORTED is set.
 *
 * @param _offset Pointer to the offset being searched (int64_t *)
 * @param _entry Pointer to the batch entry (rd_kafka_share_ack_batch_entry_t *)
 *
 * @returns negative if offset < entry->start_offset
 * @returns positive if offset > entry->end_offset
 * @returns 0 if offset is within [start_offset, end_offset]
 */
static int rd_kafka_share_ack_entries_offset_find_cmp_ptr(const void *_offset,
                                                          const void *_entry) {
        const int64_t *offset = (const int64_t *)_offset;
        const rd_kafka_share_ack_batch_entry_t *entry =
            (const rd_kafka_share_ack_batch_entry_t *)_entry;

        if (*offset < entry->start_offset)
                return -1;
        if (*offset > entry->end_offset)
                return 1;
        return 0;
}

/**
 * @brief Look up a batch entry containing the given offset.
 *
 * Finds the partition in the inflight_acks map and locates the entry
 * containing the specified offset using binary search.
 *
 * @param rkshare Share consumer handle.
 * @param topic Topic name.
 * @param partition Partition id.
 * @param offset Offset to find.
 * @param entry_out Output parameter for the found entry.
 * @param idx_out Output parameter for the index within the entry.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success,
 *          RD_KAFKA_RESP_ERR__STATE if partition or offset not found.
 */
static rd_kafka_resp_err_t
rd_kafka_share_find_ack_entry(rd_kafka_share_t *rkshare,
                              const char *topic,
                              int32_t partition,
                              int64_t offset,
                              rd_kafka_share_ack_batch_entry_t **entry_out,
                              int64_t *idx_out) {
        rd_kafka_topic_partition_t *lookup_key;
        rd_kafka_share_ack_batches_t *batches;
        rd_kafka_share_ack_batch_entry_t *entry;

        /* Find partition in inflight_acks map */
        lookup_key = rd_kafka_topic_partition_new(topic, partition);
        batches    = RD_MAP_GET(&rkshare->rkshare_inflight_acks, lookup_key);
        rd_kafka_topic_partition_destroy(lookup_key);
        if (!batches)
                return RD_KAFKA_RESP_ERR__STATE;

        /* Find entry containing offset using binary search.
         * Entries are sorted by start_offset and don't overlap. */
        entry = rd_list_find(&batches->entries, &offset,
                             rd_kafka_share_ack_entries_offset_find_cmp_ptr);
        if (!entry)
                return RD_KAFKA_RESP_ERR__STATE;

        *entry_out = entry;
        *idx_out   = offset - entry->start_offset;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t
rd_kafka_share_acknowledge(rd_kafka_share_t *rkshare,
                           const rd_kafka_message_t *rkmessage) {
        return rd_kafka_share_acknowledge_type(
            rkshare, rkmessage, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
}

rd_kafka_resp_err_t
rd_kafka_share_acknowledge_type(rd_kafka_share_t *rkshare,
                                const rd_kafka_message_t *rkmessage,
                                rd_kafka_share_AcknowledgeType_t type) {
        if (!rkmessage)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        if (!rkmessage->rkt)
                return RD_KAFKA_RESP_ERR__STATE;

        return rd_kafka_share_acknowledge_offset(
            rkshare, rd_kafka_topic_name(rkmessage->rkt), rkmessage->partition,
            rkmessage->offset, type);
}

rd_kafka_resp_err_t
rd_kafka_share_acknowledge_offset(rd_kafka_share_t *rkshare,
                                  const char *topic,
                                  int32_t partition,
                                  int64_t offset,
                                  rd_kafka_share_AcknowledgeType_t type) {
        rd_kafka_share_ack_batch_entry_t *entry;
        int64_t idx;
        rd_kafka_resp_err_t err;

        if (!rkshare || !topic || partition < 0 || offset < 0)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        /* Explicit acknowledge APIs require explicit acknowledgement mode */
        if (rd_kafka_share_acknowledgement_mode_is_implicit(rkshare))
                return RD_KAFKA_RESP_ERR__STATE;

        /* Validate type - ACCEPT, RELEASE, REJECT allowed */
        if (type < RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT ||
            type > RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        if (unlikely(
                (err = rd_kafka_share_consumer_closed_or_closing_err(rkshare))))
                return err;

        /* Find partition and entry containing the offset */
        err = rd_kafka_share_find_ack_entry(rkshare, topic, partition, offset,
                                            &entry, &idx);
        if (err)
                return err;

        /* GAP records cannot be acknowledged */
        if (entry->types[idx] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP)
                return RD_KAFKA_RESP_ERR__STATE;

        rd_kafka_share_update_acknowledgement_type(rkshare, entry, idx, type);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Initialize a partition offsets element with topic, partition, and
 * offsets.
 *
 * @param elem Element to initialize (must be pre-allocated, e.g., in an array).
 * @param topic Topic name (will be duplicated).
 * @param partition Partition id.
 * @param offsets_cnt Number of offsets to allocate space for.
 */
void rd_kafka_share_partition_offsets_init(
    rd_kafka_share_partition_offsets_t *elem,
    const char *topic,
    int32_t partition,
    int offsets_cnt) {
        elem->partition.topic     = rd_strdup(topic);
        elem->partition.partition = partition;
        elem->offsets = rd_calloc((size_t)offsets_cnt, sizeof(int64_t));
        elem->cnt     = offsets_cnt;
}

/**
 * @brief Clear a partition offsets element (free owned memory).
 *
 * Does not free the element itself since it is stored inline in an array.
 *
 * @param elem Element to clear.
 */
void rd_kafka_share_partition_offsets_clear(
    rd_kafka_share_partition_offsets_t *elem) {
        if (!elem)
                return;
        RD_IF_FREE(elem->partition.topic, rd_free);
        RD_IF_FREE(elem->offsets, rd_free);
        elem->partition.topic = NULL;
        elem->offsets         = NULL;
        elem->cnt             = 0;
}

/**
 * @brief Allocate and initialize a partition offsets list with given capacity.
 *
 * @param capacity Initial capacity for elements.
 * @returns Newly allocated list, or NULL if capacity is 0.
 *          Caller must destroy with
 * rd_kafka_share_partition_offsets_list_destroy().
 */
rd_kafka_share_partition_offsets_list_t *
rd_kafka_share_partition_offsets_list_new(int capacity) {
        rd_kafka_share_partition_offsets_list_t *list;

        if (capacity <= 0)
                return NULL;

        list        = rd_calloc(1, sizeof(*list));
        list->cnt   = 0;
        list->size  = capacity;
        list->elems = rd_calloc((size_t)capacity, sizeof(*list->elems));

        return list;
}

rd_kafka_share_partition_offsets_list_t *
rd_kafka_share_build_partition_offsets_list(
    rd_kafka_share_ack_batches_t *batches) {
        rd_kafka_share_partition_offsets_list_t *list;
        rd_kafka_share_partition_offsets_t *elem;
        rd_kafka_share_ack_batch_entry_t *entry;
        int total_offsets = 0;
        int offset_idx    = 0;
        int j;

        if (!batches || !batches->rktpar || rd_list_cnt(&batches->entries) == 0)
                return NULL;

        /* Count total offsets */
        RD_LIST_FOREACH(entry, &batches->entries, j) {
                total_offsets +=
                    (int)(entry->end_offset - entry->start_offset + 1);
        }

        if (total_offsets == 0)
                return NULL;

        list = rd_kafka_share_partition_offsets_list_new(1);
        if (!list)
                return NULL;

        list->cnt = 1;
        elem      = &list->elems[0];

        rd_kafka_share_partition_offsets_init(elem, batches->rktpar->topic,
                                              batches->rktpar->partition,
                                              total_offsets);

        /* Fill offsets array */
        RD_LIST_FOREACH(entry, &batches->entries, j) {
                int64_t off;
                for (off = entry->start_offset; off <= entry->end_offset;
                     off++) {
                        elem->offsets[offset_idx++] = off;
                }
        }

        return list;
}


void rd_kafka_share_enqueue_ack_callback(rd_kafka_t *rk,
                                         rd_kafka_share_ack_batches_t *batches,
                                         rd_kafka_resp_err_t err) {
        rd_kafka_op_t *cb_rko;
        rd_kafka_share_partition_offsets_list_t *partitions;

        if (!rk->rk_conf.share_acknowledgement_commit_cb)
                return;

        partitions = rd_kafka_share_build_partition_offsets_list(batches);
        if (!partitions)
                return;

        cb_rko          = rd_kafka_op_new(RD_KAFKA_OP_SHARE_ACK_REPLY);
        cb_rko->rko_err = err;
        cb_rko->rko_u.share_ack_reply.partitions = partitions;
        cb_rko->rko_u.share_ack_reply.cb =
            rk->rk_conf.share_acknowledgement_commit_cb;
        cb_rko->rko_u.share_ack_reply.opaque = rk->rk_conf.opaque;

        rd_kafka_q_enq(rk->rk_rep, cb_rko);
}


size_t rd_kafka_share_partition_offsets_list_count(
    const rd_kafka_share_partition_offsets_list_t *list) {
        if (!list)
                return 0;
        return (size_t)list->cnt;
}


const rd_kafka_share_partition_offsets_t *
rd_kafka_share_partition_offsets_list_get(
    const rd_kafka_share_partition_offsets_list_t *list,
    size_t index) {
        if (!list || index >= (size_t)list->cnt)
                return NULL;
        return &list->elems[index];
}


void rd_kafka_share_partition_offsets_list_destroy(
    rd_kafka_share_partition_offsets_list_t *list) {
        int i;

        if (!list)
                return;

        for (i = 0; i < list->cnt; i++) {
                rd_kafka_share_partition_offsets_clear(&list->elems[i]);
        }
        rd_free(list->elems);
        rd_free(list);
}


const rd_kafka_topic_partition_t *rd_kafka_share_partition_offsets_partition(
    const rd_kafka_share_partition_offsets_t *partition_offsets) {
        return &partition_offsets->partition;
}


const int64_t *rd_kafka_share_partition_offsets_offsets(
    const rd_kafka_share_partition_offsets_t *partition_offsets) {
        return partition_offsets->offsets;
}


int rd_kafka_share_partition_offsets_offsets_cnt(
    const rd_kafka_share_partition_offsets_t *partition_offsets) {
        return partition_offsets->cnt;
}


rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batch_find(rd_list_t *ack_details,
                              const char *topic,
                              int32_t partition) {
        rd_kafka_share_ack_batches_t *ack_batch;
        int k;

        if (!ack_details)
                return NULL;

        RD_LIST_FOREACH(ack_batch, ack_details, k) {
                if (ack_batch->rktpar &&
                    !strcmp(ack_batch->rktpar->topic, topic) &&
                    ack_batch->rktpar->partition == partition)
                        return ack_batch;
        }
        return NULL;
}


void rd_kafka_share_dispatch_ack_callbacks(
    rd_kafka_t *rk,
    rd_list_t *ack_details,
    rd_kafka_topic_partition_list_t *ack_results,
    rd_kafka_resp_err_t err) {
        rd_kafka_share_ack_batches_t *ack_batch;
        int k;

        if (!rk->rk_conf.share_acknowledgement_commit_cb || !ack_details ||
            rd_list_cnt(ack_details) == 0)
                return;

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                /* Top-level error: apply to all partitions */
                RD_LIST_FOREACH(ack_batch, ack_details, k) {
                        rd_kafka_share_enqueue_ack_callback(rk, ack_batch, err);
                }
                return;
        }

        if (!ack_results)
                return;

        /* No top-level error: use per-partition results */
        for (int p = 0; p < ack_results->cnt; p++) {
                rd_kafka_topic_partition_t *rktpar = &ack_results->elems[p];

                ack_batch = rd_kafka_share_ack_batch_find(
                    ack_details, rktpar->topic, rktpar->partition);
                if (ack_batch)
                        rd_kafka_share_enqueue_ack_callback(rk, ack_batch,
                                                            rktpar->err);
        }
}
