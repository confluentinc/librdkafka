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
#ifndef _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_
#define _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_

#include "rdlist.h"
#include "rdkafka.h"

/* Forward declarations */
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
        int64_t response_acquired_offsets_count; /**< Total acquired messages */
        rd_list_t entries; /**< rd_kafka_share_ack_batch_entry_t*,
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
 *  Takes ownership of \p rktpar.
 *  TODO KIP-932: We should keep a copy of the rktpar instead of taking
 * ownership. */
rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batches_new(rd_kafka_topic_partition_t *rktpar,
                               int32_t response_leader_id,
                               int32_t response_leader_epoch,
                               int64_t response_acquired_offsets_count);

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

/**
 * @brief Check if share consumer uses implicit acknowledgement mode.
 */
rd_bool_t
rd_kafka_share_acknowledgement_mode_is_implicit(rd_kafka_share_t *rkshare);

/**
 * @brief Check if share consumer uses explicit acknowledgement mode.
 */
rd_bool_t
rd_kafka_share_acknowledgement_mode_is_explicit(rd_kafka_share_t *rkshare);

/**
 * @brief In implicit mode, acknowledge all acquired records as ACCEPT.
 */
void rd_kafka_share_acknowledge_all_if_implicit(rd_kafka_share_t *rkshare);

/**
 * @brief In explicit mode, ensure all acquired records have been acknowledged.
 * @returns Error if there are unacknowledged records, NULL otherwise.
 */
rd_kafka_error_t *
rd_kafka_share_ensure_all_acknowledged_if_explicit(rd_kafka_share_t *rkshare);

/**
 * @brief Comparator for sorting/checking entries by start_offset.
 *
 * Used with rd_list_is_sorted().
 */
int rd_kafka_share_ack_entries_sort_cmp_ptr(const void *_a, const void *_b);


/**
 * @struct rd_kafka_share_partition_offsets_s
 * @brief Partition with set of acknowledged offsets.
 */
struct rd_kafka_share_partition_offsets_s {
        rd_kafka_topic_partition_t
            partition;    /**< Topic partition information */
        int64_t *offsets; /**< Array of acknowledged offsets */
        int cnt;          /**< Number of offsets in array */
};

/**
 * @struct rd_kafka_share_partition_offsets_list_s
 * @brief List of share partition offsets for callback.
 */
struct rd_kafka_share_partition_offsets_list_s {
        int cnt;                                   /**< Number of partitions */
        int size;                                  /**< Allocated size */
        rd_kafka_share_partition_offsets_t *elems; /**< Array of partition
                                                        offsets */
};

/**
 * @brief Initialize a partition offsets element.
 *
 * @param elem Element to initialize (must be pre-allocated).
 * @param topic Topic name (will be duplicated).
 * @param partition Partition id.
 * @param offsets_cnt Number of offsets to allocate space for.
 */
void rd_kafka_share_partition_offsets_init(
    rd_kafka_share_partition_offsets_t *elem,
    const char *topic,
    int32_t partition,
    int offsets_cnt);

/**
 * @brief Clear a partition offsets element (free owned memory).
 *
 * Does not free the element itself since it is stored inline in an array.
 *
 * @param elem Element to clear.
 */
void rd_kafka_share_partition_offsets_clear(
    rd_kafka_share_partition_offsets_t *elem);

/**
 * @brief Allocate and initialize a partition offsets list.
 *
 * @param capacity Initial capacity for elements.
 * @returns Newly allocated list, or NULL if capacity is 0.
 *          Caller must destroy with
 *          rd_kafka_share_partition_offsets_list_destroy().
 */
rd_kafka_share_partition_offsets_list_t *
rd_kafka_share_partition_offsets_list_new(int capacity);

/**
 * @brief Destroy a partition offsets list.
 *
 * Frees all elements and the list itself.
 *
 * @param list List to destroy (may be NULL).
 */
void rd_kafka_share_partition_offsets_list_destroy(
    rd_kafka_share_partition_offsets_list_t *list);

/**
 * @brief Build partition offsets list for a single partition.
 *
 * Creates an rd_kafka_share_partition_offsets_list_t with exactly one element
 * for the given batches. Used for per-partition callback invocation.
 *
 * @param batches Single partition's ack batches.
 * @returns Allocated list with one element, or NULL if no offsets.
 *          Caller must destroy with
 *          rd_kafka_share_partition_offsets_list_destroy().
 */
rd_kafka_share_partition_offsets_list_t *
rd_kafka_share_build_partition_offsets_list(
    rd_kafka_share_ack_batches_t *batches);

/**
 * @brief Enqueue share acknowledgement callback for a single partition.
 *
 * Creates and enqueues a callback op for the given partition with the
 * specified error code. Used for both per-partition acknowledgement results
 * and top-level errors.
 *
 * @param rk Kafka handle.
 * @param batches The ack batches for this partition (contains offsets).
 * @param err Error code to report in callback.
 */
void rd_kafka_share_enqueue_ack_callback(rd_kafka_t *rk,
                                         rd_kafka_share_ack_batches_t *batches,
                                         rd_kafka_resp_err_t err);

/**
 * @brief Find ack_batch matching the given topic/partition.
 *
 * @param ack_details List of ack batches.
 * @param topic Topic name.
 * @param partition Partition id.
 * @returns Matching ack_batch or NULL if not found.
 */
rd_kafka_share_ack_batches_t *
rd_kafka_share_ack_batch_find(rd_list_t *ack_details,
                              const char *topic,
                              int32_t partition);

/**
 * @brief Dispatch ack callbacks for all partitions in ack_details.
 *
 * If err is set (top-level error), all partitions receive the same error.
 * Otherwise, per-partition results from ack_results are used.
 *
 * @param rk Kafka handle.
 * @param ack_details List of ack batches.
 * @param ack_results Per-partition results (may be NULL).
 * @param err Top-level error code.
 */
void rd_kafka_share_dispatch_ack_callbacks(
    rd_kafka_t *rk,
    rd_list_t *ack_details,
    rd_kafka_topic_partition_list_t *ack_results,
    rd_kafka_resp_err_t err);

#endif /* _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_ */
