/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2015-2022, Magnus Edenhill,
 *               2023, Confluent Inc.
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
                                   int32_t types_cnt) {
        rd_kafka_share_ack_batch_entry_t *entry;

        entry                 = rd_calloc(1, sizeof(*entry));
        entry->start_offset   = start_offset;
        entry->end_offset     = end_offset;
        entry->size           = end_offset - start_offset + 1;
        entry->types_cnt      = types_cnt;
        entry->delivery_count = 0;
        entry->types    = rd_calloc((size_t)types_cnt, sizeof(*entry->types));
        entry->is_error = rd_calloc((size_t)types_cnt, sizeof(*entry->is_error));
        return entry;
}

void rd_kafka_share_ack_batch_entry_destroy(
    rd_kafka_share_ack_batch_entry_t *entry) {
        if (!entry)
                return;
        rd_free(entry->types);
        rd_free(entry->is_error);
        rd_free(entry);
}

rd_kafka_share_ack_batches_t *rd_kafka_share_ack_batches_new(void) {
        rd_kafka_share_ack_batches_t *batches;

        batches = rd_calloc(1, sizeof(*batches));
        rd_list_init(&batches->entries, 0, NULL);
        batches->rktpar                = NULL;
        batches->response_leader_id    = 0;
        batches->response_leader_epoch = 0;
        batches->response_msgs_count   = 0;
        return batches;
}

void rd_kafka_share_ack_batches_destroy(rd_kafka_share_ack_batches_t *batches,
                                        rd_bool_t free_rktpar) {
        rd_kafka_share_ack_batch_entry_t *entry;
        int i;

        if (!batches)
                return;
        RD_LIST_FOREACH(entry, &batches->entries, i)
        rd_kafka_share_ack_batch_entry_destroy(entry);
        rd_list_destroy(&batches->entries);
        if (free_rktpar && batches->rktpar)
                rd_kafka_topic_partition_destroy(batches->rktpar);
        rd_free(batches);
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
void rd_kafka_share_build_ack_mapping(rd_kafka_share_t *rkshare,
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
 * @brief Free a collated share ack batch and its entries.
 *
 * Note: Does NOT destroy rktpar as it's shared with the source inflight map.
 */
static void rd_kafka_share_collated_batch_destroy(void *ptr) {
        rd_kafka_share_ack_batches_t *batch = ptr;
        rd_kafka_share_ack_batch_entry_t *entry;
        int i;

        if (!batch)
                return;
        RD_LIST_FOREACH(entry, &batch->entries, i)
        rd_kafka_share_ack_batch_entry_destroy(entry);
        rd_list_destroy(&batch->entries);
        rd_free(batch);
}

/**
 * @brief Convert ACQUIRED to ACCEPT for sending to broker.
 *
 * Broker expects ACCEPT (AVAILABLE) for offsets we're acknowledging;
 * internally we use ACQUIRED until we send.
 */
static rd_kafka_share_internal_acknowledgement_type
rd_kafka_share_ack_convert_acquired_to_accept(
    rd_kafka_share_internal_acknowledgement_type type) {
        if (type == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED)
                return RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
        return type;
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
    rd_kafka_share_internal_acknowledgement_type type) {
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_kafka_share_ack_batch_entry_new(start_offset, end_offset, 1);
        entry->types[0] = type;
        return entry;
}

/**
 * @brief Collate a batch with per-offset types into single-type entries.
 *
 * Takes an input batch where each entry has a types[] array with per-offset
 * acknowledgement types, and produces an output batch where consecutive
 * offsets with the same type are merged into single entries (types_cnt=1,
 * types[0]=type, size=offset count).
 *
 * ACQUIRED type is converted to ACCEPT for sending to broker.
 *
 * @param src Source batch with per-offset types array
 * @param dst Output batch to populate with collated entries
 *            (must be pre-initialized with rd_list_init on entries)
 *
 * Example: Input entry with types [ACQ, ACQ, GAP, REJ, REJ] for offsets 1-5
 *          produces output entries:
 *          - {start:1, end:2, size:1, types[0]:ACCEPT}
 *          - {start:3, end:3, size:1, types[0]:GAP}
 *          - {start:4, end:5, size:1, types[0]:REJECT}
 */
static void
rd_kafka_share_ack_batches_collate(const rd_kafka_share_ack_batches_t *src,
                                   rd_kafka_share_ack_batches_t *dst) {
        rd_kafka_share_ack_batch_entry_t *entry;
        int i;

        RD_LIST_FOREACH(entry, &src->entries, i) {
                int64_t j;
                int64_t range_start = entry->start_offset;
                rd_kafka_share_internal_acknowledgement_type current_type =
                    rd_kafka_share_ack_convert_acquired_to_accept(
                        entry->types[0]);

                /* Collate consecutive offsets with same type */
                for (j = 1; j < entry->types_cnt; j++) {
                        rd_kafka_share_internal_acknowledgement_type this_type =
                            rd_kafka_share_ack_convert_acquired_to_accept(
                                entry->types[j]);

                        if (this_type != current_type) {
                                /* Type changed - emit collated entry */
                                rd_list_add(
                                    &dst->entries,
                                    rd_kafka_share_ack_batch_entry_collated_new(
                                        range_start,
                                        entry->start_offset + j - 1,
                                        current_type));

                                /* Start new range */
                                range_start  = entry->start_offset + j;
                                current_type = this_type;
                        }
                }

                /* Emit final collated entry */
                rd_list_add(&dst->entries,
                            rd_kafka_share_ack_batch_entry_collated_new(
                                range_start, entry->end_offset, current_type));
        }
}

/**
 * @brief Build collated acknowledgement batches from inflight map.
 *
 * Iterates through the inflight acknowledgement map and collates consecutive
 * offsets with the same type into ranges using
 * rd_kafka_share_ack_batches_collate(). ACQUIRED type is converted to AVAILABLE
 * (ACCEPT) for sending to broker.
 *
 * Each collated range is represented as rd_kafka_share_ack_batch_entry_t with
 * types_cnt=1 and types[0] holding the single ack type for the entire range.
 * The size field contains the actual number of offsets in the range.
 *
 * @param rkshare Share consumer handle
 * @param ack_batches_out Output list to populate with
 * rd_kafka_share_ack_batches_t*
 */
void rd_kafka_share_build_ack_batches_for_fetch(rd_kafka_share_t *rkshare,
                                                rd_list_t *ack_batches_out) {
        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *inflight_batches;

        rd_list_init(ack_batches_out, 0, rd_kafka_share_collated_batch_destroy);

        /* Iterate through all topic-partitions in the inflight map */
        RD_MAP_FOREACH(tp_key, inflight_batches,
                       &rkshare->rkshare_inflight_acks) {
                rd_kafka_share_ack_batches_t *batch;

                /* Unknown topics are filtered out during parsing */
                rd_dassert(inflight_batches->rktpar != NULL);

                /* Create output batch for this topic-partition.
                 * Reuse the rktpar from source (not copied, not destroyed). */
                batch         = rd_kafka_share_ack_batches_new();
                batch->rktpar = inflight_batches->rktpar;
                batch->response_leader_id =
                    inflight_batches->response_leader_id;
                batch->response_leader_epoch =
                    inflight_batches->response_leader_epoch;

                /* Collate entries from source to destination */
                rd_kafka_share_ack_batches_collate(inflight_batches, batch);

                rd_list_add(ack_batches_out, batch);
        }
}

rd_kafka_resp_err_t rd_kafka_share_inflight_ack_update_delivered(
    rd_kafka_share_t *rkshare,
    const char *topic,
    int32_t partition,
    int64_t offset,
    rd_kafka_share_internal_acknowledgement_type type) {
        rd_kafka_topic_partition_t lookup_key;
        rd_kafka_share_ack_batches_t *batches;
        rd_kafka_share_ack_batch_entry_t *entry;
        int i;
        int64_t idx;

        /* Find partition in inflight_acks map */
        lookup_key.topic     = (char *)topic;
        lookup_key.partition = partition;

        batches = RD_MAP_GET(&rkshare->rkshare_inflight_acks, &lookup_key);
        if (!batches)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        /* Find entry containing offset */
        RD_LIST_FOREACH(entry, &batches->entries, i) {
                if (offset >= entry->start_offset &&
                    offset <= entry->end_offset) {
                        /* Found the entry containing this offset */
                        idx = offset - entry->start_offset;

                        /* GAP records cannot be acknowledged */
                        if (entry->types[idx] ==
                            RD_KAFKA_SHARE_INTERNAL_ACK_GAP)
                                return RD_KAFKA_RESP_ERR__STATE;

                        /* Error records must use offset-based API */
                        if (entry->is_error && entry->is_error[idx])
                                return RD_KAFKA_RESP_ERR__STATE;

                        /* Update the type (allows re-acknowledgement) */
                        entry->types[idx] = type;

                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }
        }

        /* Offset not found in any entry */
        return RD_KAFKA_RESP_ERR__INVALID_ARG;
}

rd_kafka_resp_err_t rd_kafka_share_inflight_ack_update_error(
    rd_kafka_share_t *rkshare,
    const char *topic,
    int32_t partition,
    int64_t offset,
    rd_kafka_share_internal_acknowledgement_type type) {
        rd_kafka_topic_partition_t lookup_key;
        rd_kafka_share_ack_batches_t *batches;
        rd_kafka_share_ack_batch_entry_t *entry;
        int i;
        int64_t idx;

        /* Find partition in inflight_acks map */
        lookup_key.topic     = (char *)topic;
        lookup_key.partition = partition;

        batches = RD_MAP_GET(&rkshare->rkshare_inflight_acks, &lookup_key);
        if (!batches)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        /* Find entry containing offset */
        RD_LIST_FOREACH(entry, &batches->entries, i) {
                if (offset >= entry->start_offset &&
                    offset <= entry->end_offset) {
                        /* Found the entry containing this offset */
                        idx = offset - entry->start_offset;

                        /* GAP records cannot be acknowledged */
                        if (entry->types[idx] ==
                            RD_KAFKA_SHARE_INTERNAL_ACK_GAP)
                                return RD_KAFKA_RESP_ERR__STATE;

                        /* Only error records can use offset-based API */
                        if (!entry->is_error || !entry->is_error[idx])
                                return RD_KAFKA_RESP_ERR__STATE;

                        /* Update the type (allows re-acknowledgement) */
                        entry->types[idx] = type;

                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }
        }

        /* Offset not found in any entry */
        return RD_KAFKA_RESP_ERR__INVALID_ARG;
}
