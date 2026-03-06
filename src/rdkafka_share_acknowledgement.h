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
#ifndef _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_
#define _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_

#include "rdlist.h"
#include "rdkafka.h"

/* Forward declarations */
typedef struct rd_kafka_share_s rd_kafka_share_t;
typedef struct rd_kafka_op_s rd_kafka_op_t;

typedef enum rd_kafka_internal_ShareAcknowledgement_type_s {
        RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED =
            -1, /* Acquired records, not acknowledged yet */
        RD_KAFKA_SHARE_INTERNAL_ACK_GAP     = 0, /* gap */
        RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT  = 1, /* accept */
        RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE = 2, /* release */
        RD_KAFKA_SHARE_INTERNAL_ACK_REJECT  = 3  /* reject */
} rd_kafka_share_internal_acknowledgement_type;

/**
 * @brief Acknowledgement batch entry for a contiguous offset range.
 *
 * Tracks acknowledgement status for each offset in the range.
 * Used for building ShareAcknowledge requests.
 *
 * The size field always represents the number of offsets in the range
 * (end_offset - start_offset + 1).
 *
 * The types_cnt field represents the actual size of the types array:
 *   - For inflight tracking: types_cnt == size (one type per offset)
 *   - For collated batches: types_cnt == 1 (single consolidated type)
 */
/**
 * TODO KIP-932: Check naming.
 */
typedef struct rd_kafka_share_ack_batch_entry_s {
        int64_t start_offset;   /**< First offset in range */
        int64_t end_offset;     /**< Last offset in range (inclusive) */
        int64_t size;           /**< Number of offsets (end - start + 1) */
        int32_t types_cnt;      /**< Number of elements in types array */
        int16_t delivery_count; /**< From AcquiredRecords DeliveryCount */
        rd_kafka_share_internal_acknowledgement_type
            *types; /**< Array of ack types */
} rd_kafka_share_ack_batch_entry_t;

/**
 * @brief Per topic-partition inflight acknowledgement batches.
 *
 * Tracks all acquired records for a topic-partition that are
 * pending acknowledgement from the application.
 *
 * The rktpar field contains topic, partition, and in its _private:
 *   - rktp (toppar reference, refcount held)
 *   - topic_id
 *
 * The response_leader_id and response_leader_epoch are the leader info
 * at the time records were acquired. These may differ from the current
 * leader when sending acknowledgements.
 */
/**
 * TODO KIP-932: Check naming.
 */
typedef struct rd_kafka_share_ack_batches_s {
        rd_kafka_topic_partition_t
            *rktpar;                   /**< Topic-partition with rktp ref */
        int32_t response_leader_id;    /**< Leader broker id when records
                                        *   were acquired */
        int32_t response_leader_epoch; /**< Leader epoch when records
                                        *   were acquired */
        int64_t response_msgs_count;   /**< Total acquired messages */
        rd_list_t entries;             /**< rd_kafka_share_ack_batch_entry_t*,
                                        *   sorted by start_offset */
} rd_kafka_share_ack_batches_t;

/** Allocate and initialize a share ack batch entry (offset range + types
 * array). */
rd_kafka_share_ack_batch_entry_t *
rd_kafka_share_ack_batch_entry_new(int64_t start_offset,
                                   int64_t end_offset,
                                   int32_t types_cnt,
                                   int16_t delivery_count);
/** Destroy a share ack batch entry (frees types array and entry). */
void rd_kafka_share_ack_batch_entry_destroy(
    rd_kafka_share_ack_batch_entry_t *entry);

/** Deep copy a share ack batch entry. */
rd_kafka_share_ack_batch_entry_t *rd_kafka_share_ack_batch_entry_copy(
    const rd_kafka_share_ack_batch_entry_t *src);

/** Allocate and initialize a share ack batches.
 *  Takes ownership of \p rktpar. */
rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batches_new(rd_kafka_topic_partition_t *rktpar,
                               int32_t response_leader_id,
                               int32_t response_leader_epoch,
                               int64_t response_msgs_count);

/** Allocate an empty share ack batches (all fields zeroed). */
rd_kafka_share_ack_batches_t *rd_kafka_share_ack_batches_new_empty(void);

/** void* wrapper for rd_kafka_share_ack_batches_destroy.
 *  Suitable as rd_list_t / rd_map_t value destructor. */
void rd_kafka_share_ack_batches_destroy_free(void *ptr);

/** Destroy share ack batches (frees entries, rktpar, and struct). */
void rd_kafka_share_ack_batches_destroy(rd_kafka_share_ack_batches_t *batches);

/** Deep copy a share ack batches (copies rktpar and all entries). */
rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batches_copy(const rd_kafka_share_ack_batches_t *src);

/** void* wrapper for rd_kafka_share_ack_batches_copy.
 *  Suitable as rd_list_copy_to callback. */
void *rd_kafka_share_ack_batches_copy_void(const void *elem, void *opaque);

/**
 * @brief Transfer inflight acks from response RKO into rkshare's inflight map.
 */
void rd_kafka_share_build_inflight_acks_map(rd_kafka_share_t *rkshare,
                                      rd_kafka_op_t *response_rko);


/**
 * @brief Implicit ack: convert all ACQUIRED types to ACCEPT in inflight map.
 */
void rd_kafka_share_ack_all(rd_kafka_share_t *rkshare);

/**
 * @brief Extract acknowledged (non-ACQUIRED) records from inflight map.
 *
 * Non-ACQUIRED offsets are collated into ack_details for sending.
 * ACQUIRED offsets remain in the map. Empty entries are removed.
 *
 * @returns Allocated list or NULL if nothing to send. Caller must destroy.
 */
rd_list_t *rd_kafka_share_build_ack_details(rd_kafka_share_t *rkshare);

#endif /* _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_ */
