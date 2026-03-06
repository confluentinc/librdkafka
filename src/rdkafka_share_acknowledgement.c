/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2015-2022, Magnus Edenhill,
 *               2026, Confluent Inc.
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
                               int64_t response_msgs_count) {
        rd_kafka_share_ack_batches_t *batches;

        batches                        = rd_calloc(1, sizeof(*batches));
        batches->rktpar                = rktpar;
        batches->response_leader_id    = response_leader_id;
        batches->response_leader_epoch = response_leader_epoch;
        batches->response_msgs_count   = response_msgs_count;
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
            src->response_msgs_count);

        /* Deep copy all entries */
        rd_list_copy_to(&dst->entries, &src->entries,
                        rd_kafka_share_ack_batch_entry_copy_void, NULL);
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

                key = rd_kafka_topic_partition_new_with_topic_id(
                    rd_kafka_topic_partition_get_topic_id(batches->rktpar),
                    batches->rktpar->partition);

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

                /* Replace inflight entries with ACQUIRED-only entries */
                rd_list_destroy(&inflight_batches->entries);
                inflight_batches->entries = new_entries;

                /* If no ACQUIRED offsets remain, mark for removal */
                if (rd_list_cnt(&inflight_batches->entries) == 0) {
                        rd_kafka_topic_partition_t *del_key =
                            rd_kafka_topic_partition_new_with_topic_id(
                                rd_kafka_topic_partition_get_topic_id(
                                    inflight_batches->rktpar),
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
