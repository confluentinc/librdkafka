/*
 * librdkafka - Apache Kafka C library
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

#ifndef _RDKAFKA_MSGSET_H_
#define _RDKAFKA_MSGSET_H_

/* This calculator is used to get accurate topic, partition, and message
 * counts to pass into rd_kafka_produce_ctx_init so that a buffer of
 * adequate size will be allocated.
 */
struct rd_kafka_produce_calculator_s
{
    /* Counts used to estimate payload size. */
    int rkpca_topic_cnt;
    int rkpca_partition_cnt;
    int rkpca_message_cnt;
    size_t rkpca_message_size;

    /* Topic specific configurations.
     * All topics in a batch must use the same values. */
    int rkpca_request_req_acks;
    int rkpca_request_timeout_ms;

    /* Cached header sizes used in calculation. */
    size_t rkpca_produce_header_size;
    size_t rkpca_topic_header_size;
    size_t rkpca_partition_header_size;
    size_t rkpca_message_set_header_size;
    size_t rkpca_message_overhead;

    /* Track previous topic added to see if it changed. */
    rd_kafka_topic_t *rkpca_rkt_prev;
};

/* Context used to store data while constructing a produce batch.
 * The resulting batch will be written into a buffer created in
 * rd_kafka_produce_ctx_init, and returned in rd_kafka_produce_ctx_finalize.
 * Toppars are added using rd_kafka_produce_ctx_append_toppar and must
 * conform to the settings passed into the init method or appending will fail.
 */
struct rd_kafka_produce_ctx_s {
        rd_kafka_broker_t *rkpc_rkb;

        /* User variables set in rd_kafka_produce_ctx_init */
        int rkpc_topic_max;             /* Max topics which can be added to batch. */
        int rkpc_partition_max;         /* Max partitions which can be added to batch. */
        int rkpc_message_max;           /* Max partitions which can be added to batch. */
        size_t rkpc_message_bytes_size; /* Max message bytes which can be added to batch. */
        void *rkpc_opaque;              /* User data associated with the request. */

        /* Produce batch option. All topics / partitions must share these options. */
        int rkpc_required_acks;
        int rkpc_request_timeout_ms;

        /* Options set by rd_kafka_produce_request_select_caps in rd_kafka_produce_ctx_init. */
        int rkpc_api_version;
        int rkpc_msg_version;
        int rkpc_features;

        /* Counts of topics / partitions / messages written to a produce batch. */
        int rkpc_appended_topic_cnt;
        int rkpc_appended_partition_cnt;
        int rkpc_appended_message_cnt;
        size_t rkpc_appended_message_bytes;

        /* As messages are added to a batch, this will be the earliest timeout
         * time of the messages in the batch. */
        rd_ts_t rkpc_first_timeout;

        /* The offset of topic count in the batch header. This will be updated
         * to the actual count in rd_kafka_produce_ctx_finalize. */
        size_t rkpc_topic_cnt_offset;

        /* The last topic appended to the batch. This is used when appending
         * partitions to know when a new topic header is needed. */
        rd_kafka_itopic_t *rkpc_active_topic;

        /* The offset of the partition count within a topic header. This will
         * be updated to the actual partition count when a partition for a
         * different topic is appended or in rd_kafka_produce_ctx_finalize. */
        size_t rkpc_active_topic_partition_cnt_offset;

        /* Count of partitions appended for the current topic.
         * Used to update the count at the offset above. */
        int rkpc_active_topic_partition_cnt;

        /* Track whether the previous append call was a partial write. In that
         * case, no more messages may be added so the context must be finalized. */
        int rkpc_full;

        /* The buffer used when writing. */
        rd_kafka_buf_t *rkpc_buf;

        /* Idempotent producer's current Producer Id */
        rd_kafka_pid_t rkpc_pid;

        /* Active toppar first message information */
        struct {
                uint64_t   msgid;  /**< Internal/original message id. */
                int32_t    seq;    /**< Epoch's sequence after adjusting
                                    *   for current epoch and wrapping. */
        } rkpc_active_firstmsg;
};

/**
 * @name Multiple message set produce request handling.
 * @{
 *
 * These functions are for packing multiple message
 * sets within a single produce request.
 */

void
rd_kafka_produce_calculator_init(rd_kafka_produce_calculator_t *rkpca,
                                 rd_kafka_broker_t *rkb);

int
rd_kafka_produce_calculator_add(rd_kafka_produce_calculator_t *rkpca,
                                rd_kafka_toppar_t *rktp);

int
rd_kafka_produce_ctx_init (rd_kafka_produce_ctx_t *rkpc,
                           rd_kafka_broker_t *rkb,
                           int topic_max,
                           int partition_max,
                           int message_max,
                           size_t message_bytes_size,
                           int required_acks,
                           int request_timeout_ms,
                           rd_kafka_pid_t pid,
                           void* opaque);

int
rd_kafka_produce_ctx_append_toppar (rd_kafka_produce_ctx_t *rkpc,
                                    rd_kafka_toppar_t *rktp,
                                    int *appended_msg_cnt,
                                    size_t *appended_msg_bytes);

rd_kafka_buf_t *
rd_kafka_produce_ctx_finalize (rd_kafka_produce_ctx_t *rkpc);

/**
 * @name MessageSet readers
 */
rd_kafka_resp_err_t
rd_kafka_msgset_parse (rd_kafka_buf_t *rkbuf,
                       rd_kafka_buf_t *request,
                       rd_kafka_toppar_t *rktp,
                       const struct rd_kafka_toppar_ver *tver);

#endif /* _RDKAFKA_MSGSET_H_ */
