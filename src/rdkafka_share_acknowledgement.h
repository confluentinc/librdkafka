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
                                   int32_t types_cnt);
/** Destroy a share ack batch entry (frees types array and entry). */
void rd_kafka_share_ack_batch_entry_destroy(
    rd_kafka_share_ack_batch_entry_t *entry);

/** Allocate and initialize a share ack batches (list of entries). */
rd_kafka_share_ack_batches_t *rd_kafka_share_ack_batches_new(void);
/** Destroy share ack batches. If \p free_rktpar is true, destroys rktpar too.
 */
void rd_kafka_share_ack_batches_destroy(rd_kafka_share_ack_batches_t *batches,
                                        rd_bool_t free_rktpar);

/**
 * @brief Transfer inflight acks from response RKO into rkshare's inflight map.
 */
void rd_kafka_share_build_ack_mapping(rd_kafka_share_t *rkshare,
                                      rd_kafka_op_t *response_rko);

/**
 * @brief Build collated acknowledgement batches from inflight map.
 */
void rd_kafka_share_build_ack_batches_for_fetch(rd_kafka_share_t *rkshare,
                                                rd_list_t *ack_batches_out);

#endif /* _RDKAFKA_SHARE_ACKNOWLEDGEMENT_H_ */
