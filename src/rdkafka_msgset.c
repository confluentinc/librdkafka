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

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_msgfmt.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"

#include "rdvarint.h"


typedef struct rd_kafka_msgset_writer_s {
        rd_kafka_buf_t *msetw_rkbuf;     /* Backing store buffer (refcounted)*/

        int16_t msetw_ApiVersion;        /* ProduceRequest ApiVersion */
        int     msetw_MsgVersion;        /* MsgVersion to construct */
        int     msetw_features;          /* Protocol features to use */


        size_t  msetw_of_MessageSetSize; /* offset of MessageSetSize */

        int     msetw_relative_offsets;  /* Bool: use relative offsets */

        /* First message information */
        struct {
                rd_kafka_buf_pos_t pos;  /* rkbuf's first message position */
                int64_t            timestamp;
        } msetw_firstmsg;

        rd_kafka_broker_t *msetw_rkb;    /* @warning Not a refcounted
                                          *          reference! */
        rd_kafka_toppar_t *msetw_rktp;   /* @warning Not a refcounted
                                          *          reference! */


} rd_kafka_msgset_writer_t;



/**
 * @brief Select ApiVersion and MsgVersion to use based on broker's
 *        feature compatibility.
 *
 * @locality broker thread
 */
static RD_INLINE void
rd_kafka_msgset_writer_select_MsgVersion (rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_broker_t *rkb = msetw->msetw_rkb;
        int feature;

        if ((feature = rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER2)) {
                msetw->msetw_ApiVersion = 3;
                msetw->msetw_MsgVersion = 2;
                msetw->msetw_features |= feature;
        } else if ((feature = rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER1)) {
                msetw->msetw_ApiVersion = 2;
                msetw->msetw_MsgVersion = 1;
                msetw->msetw_features |= feature;
        } else {
                if ((feature =
                     rkb->rkb_features & RD_KAFKA_FEATURE_THROTTLETIME)) {
                        msetw->msetw_ApiVersion = 1;
                        msetw->msetw_features |= feature;
                } else
                        msetw->msetw_ApiVersion = 0;
                msetw->msetw_MsgVersion = 0;
        }
}


/**
 * @brief Allocate buffer for messageset writer based on a previously set
 *        up \p msetw.
 *
 * Allocate iovecs to hold all headers and messages,
 * and allocate enough space to allow copies of small messages.
 * The allocated size is the minimum of message.max.bytes
 * or queued_bytes + msgcntmax * msg_overhead */
 */
static void
rd_kafka_msgset_writer_alloc_buf (rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_t *rk = msetw->msetw_rkb->rkb_rk;
        static const int iovs_per_msg = 2;
        size_t msg_overhead = 0;
        size_t hdrsize = 0;
        size_t msgsetsize = 0;
        size_t queuedsize;
        size_t bufsize;
        int iovcnt ;

        rd_kafka_assert(NULL, !msetw->msetw_rkbuf);

        /* Calculate worst-case buffer iovec count, produce header size,
         * message size, etc, for allocating the fixed buffer.
         *
         * ProduceRequest headers go in one iovec:
         *  ProduceRequest v0..2:
         *    RequiredAcks + Timeout +
         *    [Topic + [Partition + MessageSetSize]]
         *
         *  ProduceRequest v3:
         *    TransactionalId + RequiredAcks +
         *    [Topic + [Partition + MessageSetSize + MessageSet]]
         *
         * + (iovs_per_msg * msgcntmax)
         *
         * iovs_per_msg := 2 :=
         *          messagehdr and key (copied)
         *          [messagepayload (ext memory)] (or copied)
         */

        /*
         * ProduceRequest header sizes
         */
        switch (msetw->msetw_ApiVersion)
        {
        case 3:
                /* Add TransactionalId */
                hdrsize += RD_KAFKAP_STR_SIZE(rk->rk_eos.TransactionalId);
                /* FALLTHRU */
        case 0:
        case 1:
        case 2:
                hdrsize +=
                        /* RequiredAcks + Timeout + TopicCnt */
                        2 + 4 + 4 +
                        /* Topic */
                        RD_KAFKAP_STR_SIZE(msetw->msetw_rktp->
                                           rktp_rkt->rkt_topic) +
                        /* PartitionCnt + Partition + MessageSetSize */
                        4 + 4 + 4;
                break;

        default:
                RD_NOTREACHED();
        }


        /*
         * iov count
         */
        iovcnt = 1/*ProduceRequest header*/ +
                (iovs_per_msg * msetw->msetw_msgcntmax);

        /*
         * MsgVersion specific sizes:
         * - (Worst-case) Message overhead: message fields
         * - MessageSet header size
         */
        switch (msetw->msetw_MsgVersion)
        {
        case 1:
                /* Timestamp + MsgVer0 fields */
                msg_overhead += 8;
                /* FALLTHRU */
        case 0:
                /* Crc + MagicByte + Attributes + KeyLen + ValueLen */
                msg_overhead += 4 + 1 + 1 + 4 + 4;

                /* Offset + MessageSize */
                msgsetsize += 8 + 4;
                break;

        case 2:
                /* MsgVer2 uses varints, we calculate for the worst-case. */
                msg_overhead +=
                        /* Length (varint) */
                        RD_UVARINT_ENC_SIZEOF(int32_t) +
                        /* Attributes */
                        1 +
                        /* TimestampDelta (varint) */
                        RD_UVARINT_ENC_SIZEOF(int64_t) +
                        /* OffsetDelta (varint) */
                        RD_UVARINT_ENC_SIZEOF(msetw->msetw_msgcntmax) +
                        /* KeyLen (varint) */
                        RD_UVARINT_ENC_SIZEOF(int32_t) +
                        /* ValueLen (varint) */
                        RD_UVARINT_ENC_SIZEOF(int32_t) +
                        /* HeaderCnt (varint):
                         * FIXME: headers currently not supported, always 0 */
                        RD_UVARINT_ENC_SIZEOF(int);

                msgsetsize +=
                        8 /* FirstOffset */ +
                        4 /* Length */ +
                        4 /* PartitionLeaderEpoch */ +
                        1 /* Magic (MsgVersion) */ +
                        4 /* CRC (CRC32C) */ +
                        2 /* Attributes */ +
                        4 /* LastOffsetDelta */ +
                        8 /* FirstTimestamp */ +
                        8 /* MaxTimestamp */
                        8 /* PID */ +
                        2 /* ProducerEpoch */ +
                        4 /* FirstSequence */ +
                        4 /* MessageCount FIXME ? */;
                break;

        default:
                RD_NOTREACHED();
        }

        /*
         * Calculate total buffer size to allocate
         */
        bufsize = hdrsize + msgsetsize;

        /* If copying for small payloads is enabled, allocate enough
         * space for each message to be copied based on this limit.
         */
        if (rk->rk_conf.msg_copy_max_size > 0)
                bufsize += rd_kafka_msgq_size(&msetw->msetw_rktp->
                                              rktp_xmit_msgq);
        bufsize += msg_overhead * msetw->msetw_msgcntmax;

        /* Cap allocation at message.max.bytes */
        if (bufsize > (size_t)rk->rk_conf.max_msg_size)
                bufsize = (size_t)rk->rk_conf.max_msg_size;

        /*
         * Allocate iovecs to hold all headers and messages,
         * and allocate auxilliery space for message headers, etc.
         */
        msetw->msetw_rkbuf = rd_kafka_buf_new(rkb->rkb_rk, iovcnt, bufsize);

        rd_kafka_buf_ApiVersion_set(msetw->msetw_rkbuf,
                                    msetw->msetw_ApiVersion,
                                    msetw->msetw_features);
}


/**
 * @brief Write the MessageSet header.
 * @remark Must only be called for MsgVersion 2
 */
static void
rd_kafka_msgset_writer_write_MessageSet_v2_header (rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        rd_kafka_t *rk = msetw->msetw_rkb->rkb_rk;

        rd_kafka_assert(NULL, msetw->msetw_MsgVersion == 2);

        /* FirstOffset */
        rd_kafka_buf_write_i64(rkbuf, 0);

        /* Remaining Length: updated later */
        rd_kafka_buf_write_i32(rkbuf, 0);

        /* PartitionLeaderEpoch (KIP-101) */
        rd_kafka_buf_write_i32(rkbuf, 0);

        /* Magic (MsgVersion) */
        rd_kafka_buf_write_i8(rkbuf, msetw->msetw_MsgVersion);

        /* CRC (CRC32C): updated later */
        rd_kafka_buf_write_i32(rkbuf, 0);

        /* CRC needs to be done after the entire messageset+messages has
         * been constructed and the following headers updated. :( */

        /* Attributes */
        rd_kafka_buf_write_i16(rkbuf, 0);

        /* LastOffsetDelta: updated later */
        rd_kafka_buf_write_i64(rkbuf, 0);

        /* FirstTimestamp: updated later */
        rd_kafka_buf_write_i64(rkbuf, 0);

        /* MaxTimestamp: updated later */
        rd_kafka_buf_write_i64(rkbuf, 0);

        /* PID */
        rd_kafka_buf_write_i64(rkbuf, rk->rk_eos.PID);

        /* ProducerEpoch */
        rd_kafka_buf_write_i16(rkbuf, rk->rk_eos.ProducerEpoch);

        /* FirstSequence */
        rd_kafka_buf_write_i32(rkbuf, 0);

        /* FIXME: Should Messages be i32 prefixed? */
}


/**
 * @brief Calculate MessageSet v2 CRC (CRC32C) when messageset is complete.
 */
static void
rd_kafka_msgset_writer_calc_crc (rd_kafka_msgset_writer_t *msetw) {
        int32_t crc = 0;
        /* FIXME: call CRC32C */

        /* Update CRC at MessageSet v2 CRC offset */
        rd_kafka_buf_update_i32(msetw->msetw_rkbuf,
                                /* CRC byte offset */
                                msetw->msetw_of_MessageSetSize +
                                /* FirstOffset + Length +
                                 * PartitionLeaderEpoch + Magic */
                                8 + 4 + 4 + 1,
                                crc);
}


/**
 * @brief Write ProduceRequest headers.
 *        When this function returns the msgset is ready for
 *        writing individual messages.
 */
static void
rd_kafka_msgset_writer_write_Produce_header (rd_kafka_msgset_writer_t *msetw) {

        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        rd_kafka_t *rk = msetw->msetw_rkb->rkb_rk;
        rd_kafka_itopic_t *rkt = msetw->msetw_rktp->rktp_rkt;

        /* V3: TransactionalId */
        if (msetw->msetw_ApiVersion == 3)
                rd_kafka_buf_write_kstr(rkbuf, rk->rk_eos.TransactionalId);

        /* RequiredAcks */
        rd_kafka_buf_write_i16(rkbuf, rkt->rkt_conf.required_acks);

        /* Timeout */
        rd_kafka_buf_write_i32(rkbuf, rkt->rkt_conf.request_timeout_ms);

        /* TopicArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);

        /* Insert topic */
        rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);

        /* PartitionArrayCnt */
        rd_kafka_buf_write_i32(rkbuf, 1);

        /* Partition */
        rd_kafka_buf_write_i32(rkbuf, msetw->msetw_rktp->rktp_partition);

        /* MessageSetSize: Will be finalized later*/
        msetw->msetw_of_MessageSetSize = rd_kafka_buf_write_i32(rkbuf, 0);

        /* MessageSet v2 header */
        if (msetw->msetw_MsgVersion == 2)
                rd_kafka_msgset_writer_write_MessageSet_v2_header(msetw);

        /* Push write-buffer onto iovec stack to create a clean iovec boundary
         * for the compression codecs. */
        rd_kafka_buf_autopush(rkbuf);

        /* Record the current buffer position so it can be rewound later
         * in case of compression. */
        rd_kafka_buf_pos_get(rkbuf, &msetw->msetw_firstmsg.pos);
}


/**
 * @brief Initialize a ProduceRequest MessageSet writer for
 *        the given broker and partition.
 *
 *        A new buffer will be allocated to fit the pending messages in queue.
 *
 * @remark This currently constructs the entire ProduceRequest, containing
 *         a single outer MessageSet for a single partition.
 */
static void rd_kafka_msgset_writer_init (rd_kafka_msgset_writer_t *msetw,
                                         rd_kafka_broker_t *rkb,
                                         rd_kafka_toppar_t *rktp) {
        rd_kafka_buf_t *rkbuf;

        memset(msetw, 0, sizeof(*msetw));

        msetw->msetw_rktp = rktp;
        msetw->msetw_rkb = rkb;

        /* Max number of messages to send in a batch,
         * limited by current queue size or configured batch size,
         * whichever is lower. */
        msetw->msetw_msgcntmax =
                RD_MIN(rd_atomic32_get(&rktp->rktp_xmit_msgq.rkmq_msg_cnt);
                       rkb->rkb_rk->rk_conf.batch_num_messages);

        /* Select MsgVersion to use */
        rd_kafka_msgset_writer_select_MsgVersion(msetw);

        /* MsgVersion specific setup. */
        switch (msetw->msetw_MsgVersion)
        {
        case 1:
                if (rktp->rktp_rkt->rkt_conf.compression_codec)
                        msetw->msetw_relative_offsets = 1;
                break;
        }

        /* Allocate backing buffer */
        rd_kafka_msgset_writer_alloc_buf(msetw);


        /* Construct first part of Produce header */
        rd_kafka_msgset_writer_write_Produce_header(msetw);

        rd_kafka_buf_pos_get(rkbuf, &msetw->msetw_firstmsg);
}


/**
 * @brief Finalize the messageset - call when no more messages are to be
 *        added to the messageset.
 *
 *        Will compress, update final values, CRCs, etc.
 *
 *        The messageset writer is destroyed and the buffer is returned
 *        and ready to be transmitted.
 *
 * @param MessagetSetSizep will be set to the finalized MessageSetSize
 *
 * @returns the buffer to transmit or NULL if there were no messages
 *          in messageset.
 */
static rd_kafka_buf_t *
rd_kafka_msgset_writer_finalize (rd_kafka_msgset_writer_t *msetw,
                                 size_t *MessageSetSizep) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;

        /* No messages added, bail out early. */
        if (unlikely(rd_atomic32_get(&rkbuf->rkbuf_msgq.rkmq_msg_cnt) == 0)) {
                rd_kafka_buf_destroy(rkbuf);
                return NULL;
        }

        /* FIXME: assert on message.bytes.max being exceeded */

        /* Compress the message(s) */
        if (rktp->rktp_rkt->rkt_conf.compression_codec) {
                if (rd_kafka_msgset_writer_compress(msetw) == -1) {
                        /* Compression failed */
                        rd_kafka_buf_destroy(rkbuf);
                        return NULL;
                }
        }

        /* Update MessageSetSize */
        rd_kafka_buf_update_i32(rkbuf,
                                msetw->msetw_of_MessageSetSize,
                                msetw->msetw_MessageSetSize);

        *MessageSetSizep = msetw->msetw_MessageSetSize;

        return rkbuf;
}

/**
 * @brief Write message to messageset buffer with MsgVersion 0 or 1.
 * @returns the number of bytes written.
 */
static size_t
rd_kafka_msgset_writer_write_msg_v0_1 (rd_kafka_msgset_writer_t *msetw,
                                       rd_kafka_msg_t *rkm,
                                       int64_t Offset,
                                       int8_t MsgAttributes) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        size_t MessageSize;
        size_t begin_of;
        size_t of_Crc;
        size_t outlen;

        /*
         * MessageSet's (v0 and v1) per-Message header.
         */
        /* Offset */
        begin_of = rd_kafka_buf_write_i64(rkbuf, Offset);

        /* MessageSize */
        MessageSize =
                4 + 1 + 1 + /* Crc+MagicByte+Attributes */
                4 /* KeyLength */ + rkm->rkm_key_len +
                4 /* ValueLength */ + rkm->rkm_len;

        if (msetw->msetw_MsgVersion == 1)
                MessageSize += 8; /* Timestamp i64 */

        rd_kafka_buf_write_i32(rkbuf, MessageSize);

        /*
         * Message
         */
        /* Crc: will be updated later */
        of_Crc = rd_kafka_buf_write_i32(rkbuf, 0);

        /* Start Crc calculation of all buf writes. */
        rd_kafka_buf_crc_init(rkbuf);

        /* MagicByte */
        rd_kafka_buf_write_i8(rkbuf, msetw->msetw_MsgVersion);

        /* Attributes */
        rd_kafka_buf_write_i8(rkbuf, MsgAttributes);

        /* V1: Timestamp */
        if (msetw->msetw_MsgVersion == 1)
                rd_kafka_buf_write_i64(rkbuf, rkm->rkm_timestamp);

        /* Message Key */
        rd_kafka_buf_write_bytes(rkbuf, rkm->rkm_key, rkm->rkm_key_len);

        /* Value(payload) length */
        if (rkm->rkm_payload) {
                /* If payload is below the copy limit we copy the payload
                 * to the buffer, otherwise we push it onto the iovec stack.*/
                rd_kafka_buf_write_i32(rkbuf, rkm->rkm_len);
                if (rkm->rkm_len <= rkb->rkb_rk->rk_conf.msg_copy_max_size)
                        rd_kafka_buf_write(rkbuf,
                                           rkm->rkm_payload, rkm->rkm_len);
                else
                        rd_kafka_buf_push(rkbuf,
                                          rkm->rkm_payload, rkm->rkm_len);
        } else {
                rd_kafka_buf_write_i32(rkbuf, RD_KAFKAP_BYTES_LEN_NULL);
        }

        /* Finalize Crc */
        rd_kafka_buf_update_u32(rkbuf, of_Crc,
                                rd_kafka_buf_crc_finalize(rkbuf));


        /* Return written message size */
        return 8/*Offset*/ + 4/*MessageSize*/ + MessageSize;
}

/**
 * @brief Write message to messageset buffer with MsgVersion 2.
 * @returns the number of bytes written.
 */
static size_t
rd_kafka_msgset_writer_write_msg_v2 (rd_kafka_msgset_writer_t *msetw,
                                     rd_kafka_msg_t *rkm,
                                     int64_t Offset,
                                     int8_t MsgAttributes) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        size_t r;
        size_t MessageSize = 0;
        size_t begin_of;
        size_t of_Crc;
        size_t outlen;
        char varint_Length[RD_UVARINT_ENC_SIZEOF(int64_t)];
        char varint_TimestampDelta[RD_UVARINT_ENC_SIZEOF(int64_t)];
        char varint_OffsetDelta[RD_UVARINT_ENC_SIZEOF(int)];
        char varint_KeyLen[RD_UVARINT_ENC_SIZEOF(int32_t)];
        char varint_ValueLen[RD_UVARINT_ENC_SIZEOF(int32_t)];
        size_t sz_TimestampDelta;
        size_t sz_OffsetDelta;
        size_t sz_KeyLen;
        size_t sz_ValueLen;

        /* All varints, except for Length, needs to be pre-built
         * so that the Length field can be set correctly and thus have
         * correct varint encoded width. */

        sz_TimestampDelta = rd_uvarint_enc(
                varint_TimestampDelta, sizeof(varint_TimestampDelta),
                rkm->rkm_timestamp - msetw->msetw_firstmsg.timestamp);
        sz_OffsetDelta = rd_uvarint_enc(
                varint_OffsetDelta, sizeof(varint_OffsetDelta), Offset);
        sz_KeyLen = rd_uvarint_enc_i32(
                varint_KeyLen, sizeof(varint_KeyLen),
                rkm->rkm_key ? rkm->rkm_key_len : RD_KAFKAP_BYTES_LEN_NULL);
        sz_ValueLen = rd_uvarint_enc_i32(
                varint_ValueLen, sizeof(varint_ValueLen),
                rkm->rkm_value ? rkm->rkm_len : RD_KAFKAP_BYTES_LEN_NULL);

        /* Calculate MessageSize without length of Length (added later)
         * to store it in Length. */
        MessageSize =
                1 /* MsgAttributes */ +
                sz_TimestampDelta +
                sz_OffsetDelta +
                sz_KeyLen +
                rkm->rkm_key_len +
                rkm->rkm_value_len;

        /* Length */
        MessageSize += rd_buf_write_vi32(rkbuf, MessageSize);

        /* Attributes */
        rd_kafka_buf_write_i8(rkbuf, MsgAttributes);

        /* TimestampDelta */
        rd_kafka_buf_write(rkbuf, varint_TimestampDelta, sz_TimestampDelta);

        /* OffsetDelta */
        rd_kafka_buf_write(rkbuf, varint_OffsetDelta, sz_OffsetDelta);

        /* KeyLen */
        rd_kafka_buf_write(rkbuf, varint_KeyLen, sz_KeyLen);

        /* Key (if any) */
        if (rkm->rkm_key)
                rd_kafka_buf_write(rkbuf, rkm->rkm_key, rkm->rkm_key_len);

        /* ValueLen */
        rd_kafka_buf_write(rkbuf, varint_ValueLen, sz_ValueLen);

        if (rkm->rkm_payload) {
                /* If payload is below the copy limit we copy the payload
                 * to the buffer, otherwise we push it onto the iovec stack.*/
                if (rkm->rkm_len <= rkb->rkb_rk->rk_conf.msg_copy_max_size)
                        rd_kafka_buf_write(rkbuf,
                                           rkm->rkm_payload, rkm->rkm_len);
                else
                        rd_kafka_buf_push(rkbuf,
                                          rkm->rkm_payload, rkm->rkm_len);
        }

        /* Return written message size */
        return MessageSize;
}


/**
 * @brief Write message to messageset buffer.
 * @returns the number of bytes written.
 */
static size_t
rd_kafka_msgset_writer_write_msg (rd_kafka_msgset_writer_t *msetw,
                                  rd_kafka_msg_t *rkm,
                                  int64_t Offset) {
        size_t outlen;
        size_t (*writer[]) (rd_kafka_msgset_writer_t *,
                            rd_kafka_msg_t *, int8_t) = {
                [0] = rd_kafka_msgset_writer_write_msg_v0_1,
                [1] = rd_kafka_msgset_writer_write_msg_v0_1,
                [2] = rd_kafka_msgset_writer_write_msg_v2
        };

        outlen = writer[msetw->msetw_MsgVersion](msetw, rkm, Offset,
                                                 RD_KAFKA_COMPRESSION_NONE);

        rd_dassert(outlen <= rd_kafka_msg_wire_size(rkm));

        return outlen;

}

/**
 * @brief Write as many messages from the given message queue to
 *        the messageset.
 */
static void
rd_kafka_msgset_writer_write_msgq (rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;
        rd_kafka_msg_t *rkm;
        size_t hdrsize = msetw->msetw_of_MessageSetSize + 4/*MessageSet*/;
        size_t MessageSetSize = msetw->msetw_MessageSetSize;
        size_t max_msg_size = (size_t)msetw->msetw_rkb->rkb_rk->
                rk_conf.max_msg_size;
        rd_ts_t int_latency_base;
        rd_ts_t MaxTimestamp = 0;
        int msgcnt = 0;

        /* Internal latency calculation base.
         * Uses rkm_ts_timeout which is enqueue time + timeout */
        int_latency_base = rd_clock() +
                (rktp->rktp_rkt->rkt_conf.message_timeout_ms * 1000);

        /*
         * Write as many messages as possible until buffer is full
         * or limit reached.
         */
        while ((rkm = TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs))) {
                if (hdrsize + MessageSetSize + rd_kafka_msg_wire_size(rkm) >
                    max_msg_size) {
                        rd_rkb_dbg(rkb, MSG, "PRODUCE",
                                   "No more space in current MessageSet "
                                   "(%i message(s), %"PRIusz" bytes)",
                                   msgcnt, hdrsize + MessageSetSize);
                        break;
                }

                /* Move message to buffer's queue */
                rd_kafka_msgq_deq(&rktp->rktp_xmit_msgq, rkm, 1);
                rd_kafka_msgq_enq(&rkbuf->rkbuf_msgq, rkm);

                /* Add internal latency metrics */
                rd_avg_add(&rkb->rkb_avg_int_latency,
                           int_latency_base - rkm->rkm_ts_timeout);

                /* MessageSet v2's .MaxTimestamp field */
                if (unlikely(MaxTimestamp < rkm->rkm_timestamp))
                        MaxTimestamp = rkm->rkm_timestamp;

                /* Write message to buffer */
                MessageSetSize += rd_kafka_msgset_writer_write_msg(msetw, rkm);
                msgcnt++;
        }

}


/**
 * @brief Create ProduceRequest containing as many messages from
 *        the toppar's transmit queue as possible, limited by configuration,
 *        size, etc.
 *
 * @param rkb broker to create buffer for
 * @param rktp toppar to transmit messages for
 * @param MessagetSetSizep will be set to the final MessageSetSize
 *
 * @returns the buffer to transmit or NULL if there were no messages
 *          in messageset.
 */
rd_kafka_buf_t *
rd_kafka_msgset_create_ProduceRequest (rd_kafka_broker_t *rkb,
                                       rd_kafka_toppar_t *rktp,
                                       size_t *MessageSetSizep) {

        rd_kafka_msgset_writer_t msetw;

        rd_kafka_msgset_writer_init(&msetw, rkb, rktp);

        rd_kafka_msgset_writer_write_msgq(&msetw, &rktp->rkpt_xmit_msgq);

        return rd_kafka_msgset_writer_finalize(&msetw, &MessageSetSize);
}
