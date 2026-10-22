/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017-2022, Magnus Edenhill
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

/**
 * @name MessageSet reader interface
 *
 * Parses FetchResponse for Messages
 *
 *
 * @remark
 * The broker may send partial messages, when this happens we bail out
 * silently and keep the messages that we successfully parsed.
 *
 * "A Guide To The Kafka Protocol" states:
 *   "As an optimization the server is allowed to
 *    return a partial message at the end of the
 *    message set.
 *    Clients should handle this case."
 *
 * We're handling it by not passing the error upstream.
 * This is why most err_parse: goto labels (that are called from buf parsing
 * macros) suppress the error message and why log_decode_errors is off
 * unless PROTOCOL debugging is enabled.
 *
 * When a FetchResponse contains multiple partitions, each partition's
 * MessageSet may be partial, regardless of the other partitions.
 * To make sure the next partition can be parsed, each partition parse
 * uses its own sub-slice of only that partition's MessageSetSize length.
 */

#include "rd.h"
#include "rdunittest.h"
#include "rdavl.h"
#include "rdlist.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_msgset.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_header.h"
#include "rdkafka_lz4.h"
#if WITH_LZ4_EXT
#include <lz4frame.h>
#else
#include "lz4frame.h"
#endif

#include "rdvarint.h"
#include "crc32c.h"

#if WITH_ZLIB
#include "rdgz.h"
#include <zlib.h>
#endif
#if WITH_SNAPPY
#include "snappy.h"
#endif
#if WITH_ZSTD
#include "rdkafka_zstd.h"
#endif


static RD_INLINE int64_t
rd_kafka_aborted_txns_pop_offset(rd_kafka_aborted_txns_t *aborted_txns,
                                 int64_t pid,
                                 int64_t max_offset);
static RD_INLINE int64_t
rd_kafka_aborted_txns_get_offset(const rd_kafka_aborted_txns_t *aborted_txns,
                                 int64_t pid);


struct msgset_v2_hdr {
        int64_t BaseOffset;
        int32_t Length;
        int32_t PartitionLeaderEpoch;
        int8_t MagicByte;
        int32_t Crc;
        int16_t Attributes;
        int32_t LastOffsetDelta;
        int64_t BaseTimestamp;
        int64_t MaxTimestamp;
        int64_t PID;
        int16_t ProducerEpoch;
        int32_t BaseSequence;
        int32_t RecordCount;
};


/**
 * @struct rd_kafka_aborted_txn_start_offsets_t
 *
 * @brief A sorted list of aborted transaction start offsets
 * (ascending) for a PID, and an offset into that list.
 */
typedef struct rd_kafka_aborted_txn_start_offsets_s {
        rd_avl_node_t avl_node;
        int64_t pid;
        int offsets_idx;
        rd_list_t offsets;
} rd_kafka_aborted_txn_start_offsets_t;


typedef struct rd_kafka_msgset_reader_s {
        rd_kafka_buf_t *msetr_rkbuf; /**< Response read buffer */

        int msetr_relative_offsets; /**< Bool: using relative offsets */

        int msetr_inner; /**< Bool: inner MessageSet of a compressed
                          *   Message, which must not hold compressed
                          *   Messages itself. */

        /**< Outer/wrapper Message fields. */
        struct {
                int64_t offset; /**< Relative_offsets: outer message's
                                 *   Offset (last offset) */
                rd_kafka_timestamp_type_t tstype; /**< Compressed
                                                   *   MessageSet's
                                                   *   timestamp type. */
                int64_t timestamp;                /**< ... timestamp*/
        } msetr_outer;

        struct msgset_v2_hdr *msetr_v2_hdr; /**< MessageSet v2 header */

        /*
         * Aborted Transaction Start Offsets. These are arranged in a map
         * (ABORTED_TXN_OFFSETS), with PID as the key and value as follows:
         *  - OFFSETS:  sorted list of aborted transaction start offsets
         * (ascending)
         *  - IDX:      an index into OFFSETS list, initialized to 0.
         *
         * The logic for processing fetched data is as follows (note: this is
         * different from the Java client):
         *
         * 1. If the message is a transaction control message and the status is
         * ABORT then increment ABORTED_TXN_OFFSETS(PID).IDX. note: sanity check
         * that OFFSETS[ABORTED_TXN_OFFSETS(PID).IDX] is less than the current
         * offset before incrementing. If the status is COMMIT, do nothing.
         *
         * 2. If the message is a normal message, find the corresponding OFFSETS
         * list in ABORTED_TXN_OFFSETS. If it doesn't exist, then keep the
         * message. If the PID does exist, compare ABORTED_TXN_OFFSETS(PID).IDX
         * with len(OFFSETS). If it's >= then the message should be kept. If
         * not, compare the message offset with
         * OFFSETS[ABORTED_TXN_OFFSETS(PID).IDX]. If it's greater than or equal
         * to this value, then the message should be ignored. If it's less than,
         * then the message should be kept.
         *
         * Note: A MessageSet comprises messages from at most one transaction,
         * so the logic in step 2 is done at the message set level.
         */
        rd_kafka_aborted_txns_t *msetr_aborted_txns;

        const struct rd_kafka_toppar_ver *msetr_tver; /**< Toppar op version of
                                                       *   request. */

        int32_t msetr_leader_epoch; /**< Current MessageSet's partition
                                     *   leader epoch (or -1). */

        int32_t msetr_broker_id;       /**< Broker id (of msetr_rkb) */
        rd_kafka_broker_t *msetr_rkb;  /* @warning Not a refcounted
                                        *          reference! */
        rd_kafka_toppar_t *msetr_rktp; /* @warning Not a refcounted
                                        *          reference! */

        int msetr_msgcnt;            /**< Number of messages in rkq */
        int64_t msetr_msg_bytes;     /**< Number of bytes in rkq */
        rd_kafka_q_t msetr_rkq;      /**< Temp Message and error queue */
        rd_kafka_q_t *msetr_par_rkq; /**< Parent message and error queue,
                                      *   the temp msetr_rkq will be moved
                                      *   to this queue when parsing
                                      *   is done.
                                      *   Refcount is not increased. */

        int64_t msetr_next_offset; /**< Next offset to fetch after
                                    *   this reader run is done.
                                    *   Optional: only used for special
                                    *   cases where the per-message offset
                                    *   can't be relied on for next
                                    *   fetch offset, such as with
                                    *   compacted topics. */

        int msetr_ctrl_cnt; /**< Number of control messages
                             *   or MessageSets received. */

        int msetr_aborted_cnt; /**< Number of aborted MessageSets
                                *   encountered. */

        const char *msetr_srcname; /**< Optional message source string,
                                    *   used in debug logging to
                                    *   indicate messages were
                                    *   from an inner compressed
                                    *   message set.
                                    *   Not freed (use const memory).
                                    *   Add trailing space. */

        rd_kafka_compression_t msetr_compression; /**< Compression codec */
} rd_kafka_msgset_reader_t;



/* Forward declarations */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_run(rd_kafka_msgset_reader_t *msetr);
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_msgs_v2(rd_kafka_msgset_reader_t *msetr);


/**
 * @brief Set up a MessageSet reader but don't start reading messages.
 */
static void rd_kafka_msgset_reader_init(rd_kafka_msgset_reader_t *msetr,
                                        rd_kafka_buf_t *rkbuf,
                                        rd_kafka_toppar_t *rktp,
                                        const struct rd_kafka_toppar_ver *tver,
                                        rd_kafka_aborted_txns_t *aborted_txns,
                                        rd_kafka_q_t *par_rkq) {

        memset(msetr, 0, sizeof(*msetr));

        msetr->msetr_rkb          = rkbuf->rkbuf_rkb;
        msetr->msetr_leader_epoch = -1;
        msetr->msetr_broker_id    = rd_kafka_broker_id(msetr->msetr_rkb);
        msetr->msetr_rktp         = rktp;
        msetr->msetr_aborted_txns = aborted_txns;
        msetr->msetr_tver         = tver;
        msetr->msetr_rkbuf        = rkbuf;
        msetr->msetr_srcname      = "";

        rkbuf->rkbuf_uflow_mitigation = "truncated response from broker (ok)";

        /* All parsed messages are put on this temporary op
         * queue first and then moved in one go to the real op queue. */
        rd_kafka_q_init(&msetr->msetr_rkq, msetr->msetr_rkb->rkb_rk);

        /* Make sure enqueued ops get the correct serve/opaque reflecting the
         * original queue. */
        msetr->msetr_rkq.rkq_serve  = par_rkq->rkq_serve;
        msetr->msetr_rkq.rkq_opaque = par_rkq->rkq_opaque;

        /* Keep (non-refcounted) reference to parent queue for
         * moving the messages and events in msetr_rkq to when
         * parsing is done. */
        msetr->msetr_par_rkq = par_rkq;
}



/**
 * @brief Decompress MessageSet, pass the uncompressed MessageSet to
 *        the MessageSet reader.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_decompress(rd_kafka_msgset_reader_t *msetr,
                                  int MsgVersion,
                                  int Attributes,
                                  int64_t Timestamp,
                                  int64_t Offset,
                                  const void *compressed,
                                  size_t compressed_size) {
        struct iovec iov        = {.iov_base = NULL, .iov_len = 0};
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        int codec = Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        rd_kafka_buf_t *rkbufz;
        /* Decompressed data is bounded like a protocol response. */
        size_t max_size =
            (size_t)msetr->msetr_rkb->rkb_rk->rk_conf.recv_max_msg_size;

        msetr->msetr_compression = codec;

        if (unlikely(msetr->msetr_inner)) {
                /* Compressed MessageSets do not nest: Kafka never writes
                 * one inside another, and the Java client refuses it too.
                 * Each level would recurse and could multiply the
                 * decompressed size once more. */
                rd_rkb_dbg(msetr->msetr_rkb, MSG, "CODEC",
                           "%s [%" PRId32
                           "]: Compressed message at offset "
                           "%" PRId64
                           " inside a compressed MessageSet: "
                           "ignoring message",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           Offset);
                err = RD_KAFKA_RESP_ERR__BAD_MSG;
                goto err;
        }

        switch (codec) {
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP: {
                /* Decompress Message payload */
                iov.iov_base = rd_gz_decompress(compressed, compressed_size,
                                                max_size, &iov.iov_len);
                if (unlikely(!iov.iov_base)) {
                        rd_rkb_dbg(msetr->msetr_rkb, MSG, "GZIP",
                                   "Failed to decompress Gzip "
                                   "message at offset %" PRId64 " of %" PRIusz
                                   " bytes (corrupt, truncated or larger "
                                   "than receive.message.max.bytes %" PRIusz
                                   "): ignoring message",
                                   Offset, compressed_size, max_size);
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto err;
                }
        } break;
#endif

#if WITH_SNAPPY
        case RD_KAFKA_COMPRESSION_SNAPPY: {
                const char *inbuf = compressed;
                size_t inlen      = compressed_size;
                int r;
                static const unsigned char snappy_java_magic[] = {
                    0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0};
                static const size_t snappy_java_hdrlen = 8 + 4 + 4;

                /* snappy-java adds its own header (SnappyCodec)
                 * which is not compatible with the official Snappy
                 * implementation.
                 *   8: magic, 4: version, 4: compatible
                 * followed by any number of chunks:
                 *   4: length
                 * ...: snappy-compressed data. */
                if (likely(inlen > snappy_java_hdrlen + 4 &&
                           !memcmp(inbuf, snappy_java_magic, 8))) {
                        /* snappy-java framing */
                        char errstr[128];

                        inbuf = inbuf + snappy_java_hdrlen;
                        inlen -= snappy_java_hdrlen;
                        iov.iov_base = rd_kafka_snappy_java_uncompress(
                            inbuf, inlen, max_size, &iov.iov_len, errstr,
                            sizeof(errstr));

                        if (unlikely(!iov.iov_base)) {
                                rd_rkb_dbg(msetr->msetr_rkb, MSG, "SNAPPY",
                                           "%s [%" PRId32
                                           "]: "
                                           "Snappy decompression for message "
                                           "at offset %" PRId64
                                           " failed: %s: "
                                           "ignoring message",
                                           rktp->rktp_rkt->rkt_topic->str,
                                           rktp->rktp_partition, Offset,
                                           errstr);
                                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                                goto err;
                        }


                } else {
                        /* No framing */

                        /* Acquire uncompressed length */
                        if (unlikely(!rd_kafka_snappy_uncompressed_length(
                                inbuf, inlen, &iov.iov_len))) {
                                rd_rkb_dbg(msetr->msetr_rkb, MSG, "SNAPPY",
                                           "Failed to get length of Snappy "
                                           "compressed payload "
                                           "for message at offset %" PRId64
                                           " (%" PRIusz
                                           " bytes): "
                                           "ignoring message",
                                           Offset, inlen);
                                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                                goto err;
                        }

                        if (unlikely(iov.iov_len > max_size)) {
                                rd_rkb_dbg(msetr->msetr_rkb, MSG, "SNAPPY",
                                           "Snappy compressed payload "
                                           "for message at offset %" PRId64
                                           " (%" PRIusz
                                           " bytes) would uncompress to "
                                           "%" PRIusz
                                           " bytes, more than "
                                           "receive.message.max.bytes "
                                           "%" PRIusz ": ignoring message",
                                           Offset, inlen, iov.iov_len,
                                           max_size);
                                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                                goto err;
                        }

                        /* Allocate output buffer for uncompressed data */
                        iov.iov_base = rd_malloc(iov.iov_len);
                        if (unlikely(!iov.iov_base)) {
                                rd_rkb_dbg(msetr->msetr_rkb, MSG, "SNAPPY",
                                           "Failed to allocate Snappy "
                                           "decompress buffer of size %" PRIusz
                                           "for message at offset %" PRId64
                                           " (%" PRIusz
                                           " bytes): %s: "
                                           "ignoring message",
                                           iov.iov_len, Offset, inlen,
                                           rd_strerror(errno));
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                goto err;
                        }

                        /* Uncompress to outbuf */
                        if (unlikely((r = rd_kafka_snappy_uncompress(
                                          inbuf, inlen, iov.iov_base)))) {
                                rd_rkb_dbg(msetr->msetr_rkb, MSG, "SNAPPY",
                                           "Failed to decompress Snappy "
                                           "payload for message at offset "
                                           "%" PRId64 " (%" PRIusz
                                           " bytes): %s: "
                                           "ignoring message",
                                           Offset, inlen,
                                           rd_strerror(-r /*negative errno*/));
                                rd_free(iov.iov_base);
                                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                                goto err;
                        }
                }

        } break;
#endif

        case RD_KAFKA_COMPRESSION_LZ4: {
                err =
                    rd_kafka_lz4_decompress(msetr->msetr_rkb,
                                            /* Proper HC? */
                                            MsgVersion >= 1 ? 1 : 0, Offset,
                                            /* @warning Will modify compressed
                                             *          if no proper HC */
                                            (char *)compressed, compressed_size,
                                            &iov.iov_base, &iov.iov_len);
                if (err)
                        goto err;
        } break;

#if WITH_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD: {
                err = rd_kafka_zstd_decompress(
                    msetr->msetr_rkb, (char *)compressed, compressed_size,
                    &iov.iov_base, &iov.iov_len);
                if (err)
                        goto err;
        } break;
#endif

        default:
                rd_rkb_dbg(msetr->msetr_rkb, MSG, "CODEC",
                           "%s [%" PRId32 "]: Message at offset %" PRId64
                           " with unsupported "
                           "compression codec 0x%x: message ignored",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           Offset, (int)codec);

                err = RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
                goto err;
        }


        rd_assert(iov.iov_base);

        if (unlikely(iov.iov_len == 0)) {
                /* Kafka never writes a compressed MessageSet that
                 * decompresses to nothing, and no buffer can be read
                 * from an empty one. */
                rd_rkb_dbg(msetr->msetr_rkb, MSG, "CODEC",
                           "%s [%" PRId32 "]: Message at offset %" PRId64
                           " decompressed to 0 bytes: ignoring message",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           Offset);
                rd_free(iov.iov_base);
                err = RD_KAFKA_RESP_ERR__BAD_MSG;
                goto err;
        }

        /*
         * Decompression successful
         */

        /* Create a new buffer pointing to the uncompressed
         * allocated buffer (outbuf) and let messages keep a reference to
         * this new buffer. */
        rkbufz = rd_kafka_buf_new_shadow(iov.iov_base, iov.iov_len, rd_free);
        rkbufz->rkbuf_rkb = msetr->msetr_rkbuf->rkbuf_rkb;
        rd_kafka_broker_keep(rkbufz->rkbuf_rkb);


        /* In MsgVersion v0..1 the decompressed data contains
         * an inner MessageSet, pass it to a new MessageSet reader.
         *
         * For MsgVersion v2 the decompressed data are the list of messages.
         */

        if (MsgVersion <= 1) {
                /* Pass decompressed data (inner Messageset)
                 * to new instance of the MessageSet parser. */
                rd_kafka_msgset_reader_t inner_msetr;
                rd_kafka_msgset_reader_init(
                    &inner_msetr, rkbufz, msetr->msetr_rktp, msetr->msetr_tver,
                    /* there is no aborted transaction
                     * support for MsgVersion < 2 */
                    NULL, &msetr->msetr_rkq);

                inner_msetr.msetr_srcname = "compressed ";
                inner_msetr.msetr_inner   = 1;

                if (MsgVersion == 1) {
                        /* postproc() will convert relative to
                         * absolute offsets */
                        inner_msetr.msetr_relative_offsets = 1;
                        inner_msetr.msetr_outer.offset     = Offset;

                        /* Apply single LogAppendTime timestamp for
                         * all messages. */
                        if (Attributes & RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME) {
                                inner_msetr.msetr_outer.tstype =
                                    RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME;
                                inner_msetr.msetr_outer.timestamp = Timestamp;
                        }
                }

                /* Parse the inner MessageSet */
                err = rd_kafka_msgset_reader_run(&inner_msetr);

                /* Transfer message count from inner to outer */
                msetr->msetr_msgcnt += inner_msetr.msetr_msgcnt;
                msetr->msetr_msg_bytes += inner_msetr.msetr_msg_bytes;


        } else {
                /* MsgVersion 2 */
                rd_kafka_buf_t *orig_rkbuf = msetr->msetr_rkbuf;

                rkbufz->rkbuf_uflow_mitigation =
                    "truncated response from broker (ok)";

                /* Temporarily replace read buffer with uncompressed buffer */
                msetr->msetr_rkbuf = rkbufz;

                /* Read messages */
                err = rd_kafka_msgset_reader_msgs_v2(msetr);

                /* Restore original buffer */
                msetr->msetr_rkbuf = orig_rkbuf;
        }

        /* Loose our refcnt of the uncompressed rkbuf.
         * Individual messages/rko's will have their own reference. */
        rd_kafka_buf_destroy(rkbufz);

        return err;

err:
        /* Enqueue error messsage:
         * Create op and push on temporary queue. */
        if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                /* Regular consumer */
                rd_kafka_consumer_err(
                    &msetr->msetr_rkq, msetr->msetr_broker_id, err,
                    msetr->msetr_tver->version, NULL, rktp, Offset,
                    "Decompression (codec 0x%x) of message at %" PRIu64
                    " of %" PRIusz " bytes failed: %s",
                    codec, Offset, compressed_size, rd_kafka_err2str(err));
                return err;
        } else {
                /**
                 *  v0/v1 MessageSet: single error
                 * TODO KIP-932: Check the handling in case of v0/v1 messages
                 * for Share Consumers.
                 */
                if (msetr->msetr_v2_hdr == NULL) {
                        rd_kafka_consumer_err(
                            &msetr->msetr_rkq, msetr->msetr_broker_id, err,
                            msetr->msetr_tver->version, NULL, rktp, Offset,
                            "Decompression (codec 0x%x) of message at %" PRIu64
                            " of %" PRIusz " bytes failed: %s",
                            codec, Offset, compressed_size,
                            rd_kafka_err2str(err));
                } else {
                        /* Share consumer with MessageSet v2: report error for
                         * the entire batch with RELEASE acknowledgement —
                         * decompression capability varies by consumer, so
                         * another consumer might succeed on the same batch. */
                        int64_t LastOffset =
                            msetr->msetr_v2_hdr->BaseOffset +
                            msetr->msetr_v2_hdr->LastOffsetDelta;

                        /**
                         * TODO KIP-932: Add a TODO to translate errors inside
                         * this function. _BAD_COMPRESSION and _BAD_MSG should
                         * be sent as INVALID_MSG, and _CRIT_SYS_RESOURCE should
                         * be sent as _CRIT_SYS_RESOURCE.
                         */
                        rd_kafka_share_msgset_err_ops(
                            &msetr->msetr_rkq, msetr->msetr_broker_id, err,
                            msetr->msetr_tver->version, rktp,
                            msetr->msetr_v2_hdr->BaseOffset, LastOffset,
                            RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE,
                            "Decompression (codec 0x%x) of MessageSet at "
                            "offsets "
                            "%" PRId64 "-%" PRId64 " of %" PRIusz
                            " bytes failed: %s",
                            codec, msetr->msetr_v2_hdr->BaseOffset, LastOffset,
                            compressed_size, rd_kafka_err2str(err));
                }
        }

        /**
         * In case of Share Consumers the errors are propagated
         * as ops on the queue, but we return no error to make sure the
         * MessageSet parser continues parsing the next messagesets, which might
         * succeed even if this one failed.
         */
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



/**
 * @brief Message parser for MsgVersion v0..1
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or on single-message errors,
 *          or any other error code when the MessageSet parser should stop
 *          parsing (such as for partial Messages).
 * TODO KIP-932: We might need to check the handling of error ops
 *               for share consumers with v0/v1 messagesets.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_msg_v0_1(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_buf_t *rkbuf   = msetr->msetr_rkbuf;
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        rd_kafka_broker_t *rkb  = msetr->msetr_rkb;
        struct {
                int64_t Offset;      /* MessageSet header */
                int32_t MessageSize; /* MessageSet header */
                int32_t Crc;
                int8_t MagicByte; /* MsgVersion */
                int8_t Attributes;
                int64_t Timestamp; /* v1 */
        } hdr;                     /* Message header */
        rd_kafkap_bytes_t Key;
        rd_kafkap_bytes_t Value;
        int32_t Value_len;
        rd_kafka_op_t *rko;
        size_t hdrsize = 6; /* Header size following MessageSize */
        rd_slice_t crc_slice;
        rd_kafka_msg_t *rkm;
        int relative_offsets   = 0;
        const char *reloff_str = "";
        /* Only log decoding errors if protocol debugging enabled. */
        int log_decode_errors =
            (rkbuf->rkbuf_rkb->rkb_rk->rk_conf.debug & RD_KAFKA_DBG_PROTOCOL)
                ? LOG_DEBUG
                : 0;
        size_t message_end;

        rd_kafka_buf_read_i64(rkbuf, &hdr.Offset);
        rd_kafka_buf_read_i32(rkbuf, &hdr.MessageSize);
        message_end = rd_slice_offset(&rkbuf->rkbuf_reader) + hdr.MessageSize;

        rd_kafka_buf_read_i32(rkbuf, &hdr.Crc);
        if (!rd_slice_narrow_copy_relative(&rkbuf->rkbuf_reader, &crc_slice,
                                           hdr.MessageSize - 4))
                rd_kafka_buf_check_len(rkbuf, hdr.MessageSize - 4);

        rd_kafka_buf_read_i8(rkbuf, &hdr.MagicByte);
        rd_kafka_buf_read_i8(rkbuf, &hdr.Attributes);

        if (hdr.MagicByte == 1) { /* MsgVersion */
                rd_kafka_buf_read_i64(rkbuf, &hdr.Timestamp);
                hdrsize += 8;
                /* MsgVersion 1 has relative offsets for compressed
                 * MessageSets*/
                if (!(hdr.Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK) &&
                    msetr->msetr_relative_offsets) {
                        relative_offsets = 1;
                        reloff_str       = "relative ";
                }
        } else
                hdr.Timestamp = 0;

        /* Verify MessageSize */
        if (unlikely(hdr.MessageSize < (ssize_t)hdrsize))
                rd_kafka_buf_parse_fail(
                    rkbuf,
                    "Message at %soffset %" PRId64 " MessageSize %" PRId32
                    " < hdrsize %" PRIusz,
                    reloff_str, hdr.Offset, hdr.MessageSize, hdrsize);

        /* Early check for partial messages */
        rd_kafka_buf_check_len(rkbuf, hdr.MessageSize - hdrsize);

        if (rkb->rkb_rk->rk_conf.check_crcs) {
                /* Verify CRC32 if desired. */
                uint32_t calc_crc;

                calc_crc = rd_slice_crc32(&crc_slice);
                rd_dassert(rd_slice_remains(&crc_slice) == 0);

                if (unlikely(hdr.Crc != (int32_t)calc_crc)) {
                        /* Propagate CRC error to application and
                         * continue with next message. */
                        rd_kafka_consumer_err(
                            &msetr->msetr_rkq, msetr->msetr_broker_id,
                            RD_KAFKA_RESP_ERR__BAD_MSG,
                            msetr->msetr_tver->version, NULL, rktp, hdr.Offset,
                            "Message at %soffset %" PRId64 " (%" PRId32
                            " bytes) "
                            "failed CRC32 check "
                            "(original 0x%" PRIx32
                            " != "
                            "calculated 0x%" PRIx32 ")",
                            reloff_str, hdr.Offset, hdr.MessageSize, hdr.Crc,
                            calc_crc);
                        rd_kafka_buf_skip_to(rkbuf, message_end);
                        rd_atomic64_add(&rkb->rkb_c.rx_err, 1);
                        /* Continue with next message */
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }
        }


        /* Key and Value always use the fixed-width BYTES encoding, also
         * when this MessageSet is contained in a flexible-version
         * FetchResponse (v12+) whose buffer has the flexver flag set. */

        /* Extract key */
        rd_kafka_buf_read_kbytes_fixed(rkbuf, &Key);

        /* Extract Value */
        rd_kafka_buf_read_kbytes_fixed(rkbuf, &Value);
        Value_len = RD_KAFKAP_BYTES_LEN(&Value);

        /* MessageSets may contain offsets earlier than we
         * requested (compressed MessageSets in particular),
         * drop the earlier messages.
         * Note: the inner offset may only be trusted for
         *       absolute offsets. KIP-31 introduced
         *       ApiVersion 2 that maintains relative offsets
         *       of compressed messages and the base offset
         *       in the outer message is the offset of
         *       the *LAST* message in the MessageSet.
         *       This requires us to assign offsets
         *       after all messages have been read from
         *       the messageset, and it also means
         *       we cant perform this offset check here
         *       in that case. */
        if (!relative_offsets &&
            hdr.Offset < rktp->rktp_offsets.fetch_pos.offset &&
            !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk))
                return RD_KAFKA_RESP_ERR_NO_ERROR; /* Continue with next msg */

        /* Handle compressed MessageSet */
        if (unlikely(hdr.Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK))
                return rd_kafka_msgset_reader_decompress(
                    msetr, hdr.MagicByte, hdr.Attributes, hdr.Timestamp,
                    hdr.Offset, Value.data, Value_len);


        /* Pure uncompressed message, this is the innermost
         * handler after all compression and cascaded
         * MessageSets have been peeled off. */

        /* Create op/message container for message. */
        rko = rd_kafka_op_new_fetch_msg(
            &rkm, rktp, msetr->msetr_tver->version, rkbuf,
            RD_KAFKA_FETCH_POS(hdr.Offset, msetr->msetr_leader_epoch),
            (size_t)RD_KAFKAP_BYTES_LEN(&Key),
            RD_KAFKAP_BYTES_IS_NULL(&Key) ? NULL : Key.data,
            (size_t)RD_KAFKAP_BYTES_LEN(&Value),
            RD_KAFKAP_BYTES_IS_NULL(&Value) ? NULL : Value.data);

        rkm->rkm_broker_id = msetr->msetr_broker_id;

        /* Assign message timestamp.
         * If message was in a compressed MessageSet and the outer/wrapper
         * Message.Attribute had a LOG_APPEND_TIME set, use the
         * outer timestamp */
        if (msetr->msetr_outer.tstype == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME) {
                rkm->rkm_timestamp = msetr->msetr_outer.timestamp;
                rkm->rkm_tstype    = msetr->msetr_outer.tstype;

        } else if (hdr.MagicByte >= 1 && hdr.Timestamp) {
                rkm->rkm_timestamp = hdr.Timestamp;
                if (hdr.Attributes & RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME)
                        rkm->rkm_tstype = RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME;
                else
                        rkm->rkm_tstype = RD_KAFKA_TIMESTAMP_CREATE_TIME;
        }

        /* Enqueue message on temporary queue */
        rd_kafka_q_enq(&msetr->msetr_rkq, rko);
        msetr->msetr_msgcnt++;
        msetr->msetr_msg_bytes += rkm->rkm_key_len + rkm->rkm_len;

        return RD_KAFKA_RESP_ERR_NO_ERROR; /* Continue */

err_parse:
        /* Count all parse errors as partial message errors. */
        rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_partial, 1);
        return rkbuf->rkbuf_err;
}



/**
 * @brief Message parser for MsgVersion v2
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_msg_v2(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_buf_t *rkbuf   = msetr->msetr_rkbuf;
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        struct {
                int64_t Length;
                int8_t MsgAttributes;
                int64_t TimestampDelta;
                int64_t OffsetDelta;
                int64_t Offset; /* Absolute offset */
                rd_kafkap_bytes_t Key;
                rd_kafkap_bytes_t Value;
                rd_kafkap_bytes_t Headers;
        } hdr;
        rd_kafka_op_t *rko;
        rd_kafka_msg_t *rkm;
        /* Only log decoding errors if protocol debugging enabled. */
        int log_decode_errors =
            (rkbuf->rkbuf_rkb->rkb_rk->rk_conf.debug & RD_KAFKA_DBG_PROTOCOL)
                ? LOG_DEBUG
                : 0;
        size_t message_end;
        size_t record_start_pos;
        rd_kafka_fetch_pos_t msetr_pos;

        /* Capture buffer position before reading length varint so we can
         * compute the total wire-format size of this record (length varint
         * size + body bytes) for share consumer fetch.size telemetry. */
        record_start_pos = rd_slice_offset(&rkbuf->rkbuf_reader);

        rd_kafka_buf_read_varint(rkbuf, &hdr.Length);
        message_end =
            rd_slice_offset(&rkbuf->rkbuf_reader) + (size_t)hdr.Length;
        rd_kafka_buf_read_i8(rkbuf, &hdr.MsgAttributes);

        rd_kafka_buf_read_varint(rkbuf, &hdr.TimestampDelta);
        rd_kafka_buf_read_varint(rkbuf, &hdr.OffsetDelta);
        hdr.Offset = msetr->msetr_v2_hdr->BaseOffset + hdr.OffsetDelta;
        msetr_pos  = RD_KAFKA_FETCH_POS(hdr.Offset, msetr->msetr_leader_epoch);

        /* Skip message if outdated.
         * Don't check offset leader epoch, just log it, as if current leader
         * epoch is different the fetch will fail (KIP-320) and if offset leader
         * epoch is different it'll return an empty fetch (KIP-595). If we
         * checked it, it's possible to have a loop when moving from a broker
         * that supports leader epoch to one that doesn't.
         * Share consumers may receive re-delivered records at earlier offsets
         * after RELEASE, so skip this check for them. */
        if (hdr.Offset < rktp->rktp_offsets.fetch_pos.offset &&
            !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                rd_rkb_dbg(
                    msetr->msetr_rkb, MSG, "MSG",
                    "%s [%" PRId32
                    "]: "
                    "Skip %s < fetch %s",
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    rd_kafka_fetch_pos2str(msetr_pos),
                    rd_kafka_fetch_pos2str(rktp->rktp_offsets.fetch_pos));
                rd_kafka_buf_skip_to(rkbuf, message_end);
                return RD_KAFKA_RESP_ERR_NO_ERROR; /* Continue with next msg */
        }

        /**
         * Handle control messages
         * TODO KIP-932: GA: This branch shouldn't hit for share consumers,
         * as we skip control messages for share consumers.
         */
        if (msetr->msetr_v2_hdr->Attributes & RD_KAFKA_MSGSET_V2_ATTR_CONTROL) {
                struct {
                        int64_t KeySize;
                        int16_t Version;
                        int16_t Type;
                } ctrl_data;
                int64_t aborted_txn_start_offset;

                rd_kafka_buf_read_varint(rkbuf, &ctrl_data.KeySize);

                if (unlikely(ctrl_data.KeySize < 2))
                        rd_kafka_buf_parse_fail(
                            rkbuf,
                            "%s [%" PRId32
                            "]: "
                            "Ctrl message at %s"
                            " has invalid key size %" PRId64,
                            rktp->rktp_rkt->rkt_topic->str,
                            rktp->rktp_partition,
                            rd_kafka_fetch_pos2str(msetr_pos),
                            ctrl_data.KeySize);

                rd_kafka_buf_read_i16(rkbuf, &ctrl_data.Version);

                if (ctrl_data.Version != 0) {
                        rd_rkb_dbg(msetr->msetr_rkb, MSG, "MSG",
                                   "%s [%" PRId32
                                   "]: "
                                   "Skipping ctrl msg with "
                                   "unsupported version %" PRId16 " at %s",
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition, ctrl_data.Version,
                                   rd_kafka_fetch_pos2str(msetr_pos));
                        rd_kafka_buf_skip_to(rkbuf, message_end);
                        return RD_KAFKA_RESP_ERR_NO_ERROR; /* Continue with next
                                                              msg */
                }

                if (unlikely(ctrl_data.KeySize != 4))
                        rd_kafka_buf_parse_fail(
                            rkbuf,
                            "%s [%" PRId32
                            "]: "
                            "Ctrl message at %s"
                            " has invalid key size %" PRId64,
                            rktp->rktp_rkt->rkt_topic->str,
                            rktp->rktp_partition,
                            rd_kafka_fetch_pos2str(msetr_pos),
                            ctrl_data.KeySize);

                rd_kafka_buf_read_i16(rkbuf, &ctrl_data.Type);

                /* Client is uninterested in value of commit marker */
                rd_kafka_buf_skip(
                    rkbuf, (int32_t)(message_end -
                                     rd_slice_offset(&rkbuf->rkbuf_reader)));

                switch (ctrl_data.Type) {
                case RD_KAFKA_CTRL_MSG_COMMIT:
                        /* always ignore. */
                        break;

                case RD_KAFKA_CTRL_MSG_ABORT:
                        if (msetr->msetr_rkb->rkb_rk->rk_conf.isolation_level !=
                            RD_KAFKA_READ_COMMITTED)
                                break;

                        if (unlikely(!msetr->msetr_aborted_txns)) {
                                rd_rkb_dbg(msetr->msetr_rkb,
                                           MSG | RD_KAFKA_DBG_EOS, "TXN",
                                           "%s [%" PRId32
                                           "] received abort txn "
                                           "ctrl msg at %s"
                                           " for "
                                           "PID %" PRId64
                                           ", but there are no "
                                           "known aborted transactions: "
                                           "ignoring",
                                           rktp->rktp_rkt->rkt_topic->str,
                                           rktp->rktp_partition,
                                           rd_kafka_fetch_pos2str(msetr_pos),
                                           msetr->msetr_v2_hdr->PID);
                                break;
                        }

                        /* This marks the end of this (aborted) transaction,
                         * advance to next aborted transaction in list */
                        aborted_txn_start_offset =
                            rd_kafka_aborted_txns_pop_offset(
                                msetr->msetr_aborted_txns,
                                msetr->msetr_v2_hdr->PID, msetr_pos.offset);

                        if (unlikely(aborted_txn_start_offset == -1)) {
                                rd_rkb_dbg(msetr->msetr_rkb,
                                           MSG | RD_KAFKA_DBG_EOS, "TXN",
                                           "%s [%" PRId32
                                           "] received abort txn "
                                           "ctrl msg at %s"
                                           " for "
                                           "PID %" PRId64
                                           ", but this offset is "
                                           "not listed as an aborted "
                                           "transaction: aborted transaction "
                                           "was possibly empty: ignoring",
                                           rktp->rktp_rkt->rkt_topic->str,
                                           rktp->rktp_partition,
                                           rd_kafka_fetch_pos2str(msetr_pos),
                                           msetr->msetr_v2_hdr->PID);
                                break;
                        }
                        break;


                default:
                        rd_rkb_dbg(msetr->msetr_rkb, MSG,
                                   "TXN"
                                   "%s [%" PRId32
                                   "]: "
                                   "Unsupported ctrl message "
                                   "type %" PRId16
                                   " at "
                                   " %s: ignoring",
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition, ctrl_data.Type,
                                   rd_kafka_fetch_pos2str(msetr_pos));
                        break;
                }

                rko = rd_kafka_op_new_ctrl_msg(rktp, msetr->msetr_tver->version,
                                               rkbuf, msetr_pos);
                rd_kafka_q_enq(&msetr->msetr_rkq, rko);
                msetr->msetr_msgcnt++;

                return RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        /* Regular message */

        /* Note: messages in aborted transactions are skipped at the MessageSet
         * level */

        rd_kafka_buf_read_kbytes_varint(rkbuf, &hdr.Key);
        rd_kafka_buf_read_kbytes_varint(rkbuf, &hdr.Value);

        /* We parse the Headers later, just store the size (possibly truncated)
         * and pointer to the headers. */
        hdr.Headers.len =
            (int32_t)(message_end - rd_slice_offset(&rkbuf->rkbuf_reader));
        rd_kafka_buf_read_ptr(rkbuf, &hdr.Headers.data, hdr.Headers.len);

        /* Create op/message container for message. */
        rko = rd_kafka_op_new_fetch_msg(
            &rkm, rktp, msetr->msetr_tver->version, rkbuf, msetr_pos,
            (size_t)RD_KAFKAP_BYTES_LEN(&hdr.Key),
            RD_KAFKAP_BYTES_IS_NULL(&hdr.Key) ? NULL : hdr.Key.data,
            (size_t)RD_KAFKAP_BYTES_LEN(&hdr.Value),
            RD_KAFKAP_BYTES_IS_NULL(&hdr.Value) ? NULL : hdr.Value.data);

        rkm->rkm_broker_id = msetr->msetr_broker_id;

        /* Store pointer to unparsed message headers, they will
         * be parsed on the first access.
         * This pointer points to the rkbuf payload.
         * Note: can't perform struct copy here due to const fields (MSVC) */
        rkm->rkm_u.consumer.binhdrs.len  = hdr.Headers.len;
        rkm->rkm_u.consumer.binhdrs.data = hdr.Headers.data;

        /* Set timestamp.
         *
         * When broker assigns the timestamps (LOG_APPEND_TIME) it will
         * assign the same timestamp for all messages in a MessageSet
         * using MaxTimestamp.
         */
        if ((msetr->msetr_v2_hdr->Attributes &
             RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME) ||
            (hdr.MsgAttributes & RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME)) {
                rkm->rkm_tstype    = RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME;
                rkm->rkm_timestamp = msetr->msetr_v2_hdr->MaxTimestamp;
        } else {
                rkm->rkm_tstype = RD_KAFKA_TIMESTAMP_CREATE_TIME;
                rkm->rkm_timestamp =
                    msetr->msetr_v2_hdr->BaseTimestamp + hdr.TimestampDelta;
        }


        /* Cache the wire-format size of this record (length varint +
         * body) on the rkm.
         * Used by share consumer fetch.size telemetry. */
        rkm->rkm_u.consumer.wire_size =
            (int32_t)(message_end - record_start_pos);

        /* Enqueue message on temporary queue */
        rd_kafka_q_enq(&msetr->msetr_rkq, rko);
        msetr->msetr_msgcnt++;
        msetr->msetr_msg_bytes += rkm->rkm_key_len + rkm->rkm_len;

        return RD_KAFKA_RESP_ERR_NO_ERROR;

err_parse:
        /* Count all parse errors as partial message errors. */
        rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_partial, 1);
        return rkbuf->rkbuf_err;
}


/**
 * @brief Read v2 messages from current buffer position.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_msgs_v2(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_buf_t *rkbuf   = msetr->msetr_rkbuf;
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        /* Only log decoding errors if protocol debugging enabled. */
        int log_decode_errors =
            (rkbuf->rkbuf_rkb->rkb_rk->rk_conf.debug & RD_KAFKA_DBG_PROTOCOL)
                ? LOG_DEBUG
                : 0;

        if (msetr->msetr_aborted_txns != NULL &&
            (msetr->msetr_v2_hdr->Attributes &
             (RD_KAFKA_MSGSET_V2_ATTR_TRANSACTIONAL |
              RD_KAFKA_MSGSET_V2_ATTR_CONTROL)) ==
                RD_KAFKA_MSGSET_V2_ATTR_TRANSACTIONAL) {
                /* Transactional non-control MessageSet:
                 * check if it is part of an aborted transaction. */
                int64_t txn_start_offset = rd_kafka_aborted_txns_get_offset(
                    msetr->msetr_aborted_txns, msetr->msetr_v2_hdr->PID);

                if (txn_start_offset != -1 &&
                    msetr->msetr_v2_hdr->BaseOffset >= txn_start_offset) {
                        /* MessageSet is part of aborted transaction */
                        rd_rkb_dbg(msetr->msetr_rkb, MSG, "MSG",
                                   "%s [%" PRId32
                                   "]: "
                                   "Skipping %" PRId32
                                   " message(s) "
                                   "in aborted transaction "
                                   "at offset %" PRId64 " for PID %" PRId64,
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition,
                                   msetr->msetr_v2_hdr->RecordCount,
                                   txn_start_offset, msetr->msetr_v2_hdr->PID);
                        rd_kafka_buf_skip(
                            msetr->msetr_rkbuf,
                            rd_slice_remains(
                                &msetr->msetr_rkbuf->rkbuf_reader));
                        msetr->msetr_aborted_cnt++;
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }
        }

        while (rd_kafka_buf_read_remain(msetr->msetr_rkbuf)) {
                rd_kafka_resp_err_t err;
                err = rd_kafka_msgset_reader_msg_v2(msetr);
                if (unlikely(err))
                        return err;
        }

        return RD_KAFKA_RESP_ERR_NO_ERROR;

err_parse:
        /* Count all parse errors as partial message errors. */
        rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_partial, 1);
        msetr->msetr_v2_hdr = NULL;
        return rkbuf->rkbuf_err;
}



/**
 * @brief MessageSet reader for MsgVersion v2 (FetchRequest v4)
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_v2(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_buf_t *rkbuf   = msetr->msetr_rkbuf;
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        struct msgset_v2_hdr hdr;
        rd_slice_t save_slice;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len_start;
        size_t payload_size;
        int64_t LastOffset; /* Last absolute Offset in MessageSet header */
        /* Only log decoding errors if protocol debugging enabled. */
        int log_decode_errors =
            (rkbuf->rkbuf_rkb->rkb_rk->rk_conf.debug & RD_KAFKA_DBG_PROTOCOL)
                ? LOG_DEBUG
                : 0;

        rd_kafka_buf_read_i64(rkbuf, &hdr.BaseOffset);
        rd_kafka_buf_read_i32(rkbuf, &hdr.Length);
        len_start = rd_slice_offset(&rkbuf->rkbuf_reader);

        if (unlikely(hdr.Length < RD_KAFKAP_MSGSET_V2_SIZE - 8 - 4))
                rd_kafka_buf_parse_fail(rkbuf,
                                        "%s [%" PRId32
                                        "] "
                                        "MessageSet at offset %" PRId64
                                        " length %" PRId32 " < header size %d",
                                        rktp->rktp_rkt->rkt_topic->str,
                                        rktp->rktp_partition, hdr.BaseOffset,
                                        hdr.Length,
                                        RD_KAFKAP_MSGSET_V2_SIZE - 8 - 4);

        rd_kafka_buf_read_i32(rkbuf, &hdr.PartitionLeaderEpoch);
        msetr->msetr_leader_epoch = hdr.PartitionLeaderEpoch;

        rd_kafka_buf_read_i8(rkbuf, &hdr.MagicByte);
        rd_kafka_buf_read_i32(rkbuf, &hdr.Crc);

        if (msetr->msetr_rkb->rkb_rk->rk_conf.check_crcs) {
                /* Verify CRC32C if desired. */
                uint32_t calc_crc;
                rd_slice_t crc_slice;
                size_t crc_len = hdr.Length - 4 - 1 - 4;

                if (!rd_slice_narrow_copy_relative(&rkbuf->rkbuf_reader,
                                                   &crc_slice, crc_len))
                        rd_kafka_buf_check_len(rkbuf, crc_len);

                calc_crc = rd_slice_crc32c(&crc_slice);

                if (unlikely((uint32_t)hdr.Crc != calc_crc)) {
                        /* Propagate CRC error to application and
                         * continue with next message. */
                        if (!RD_KAFKA_IS_SHARE_CONSUMER(
                                msetr->msetr_rkb->rkb_rk)) {
                                rd_kafka_consumer_err(
                                    &msetr->msetr_rkq, msetr->msetr_broker_id,
                                    RD_KAFKA_RESP_ERR__BAD_MSG,
                                    msetr->msetr_tver->version, NULL, rktp,
                                    hdr.BaseOffset,
                                    "MessageSet at offset %" PRId64 " (%" PRId32
                                    " bytes) "
                                    "failed CRC32C check "
                                    "(original 0x%" PRIx32
                                    " != "
                                    "calculated 0x%" PRIx32 ")",
                                    hdr.BaseOffset, hdr.Length, hdr.Crc,
                                    calc_crc);
                                /* Skip to end of MessageSet */
                                /**
                                 * FIXME: `crc_len` is a byte *length*
                                 * (Length - 4 - 1 - 4), but
                                 * rd_kafka_buf_skip_to() interprets its
                                 * argument as an absolute slice position.
                                 * The current call leaves the reader 21
                                 * bytes short of the end of the MessageSet
                                 * (when the slice base offset is 0), and
                                 * for any subsequent MessageSet within
                                 * the same slice the size_t subtraction
                                 * inside skip_to underflows and triggers
                                 * a spurious __UNDERFLOW. The intended
                                 * target is end-of-MessageSet, i.e.
                                 * `len_start + hdr.Length` — see the
                                 * share-consumer branch below which does
                                 * this correctly. Replace with:
                                 *     rd_kafka_buf_skip_to(rkbuf,
                                 *         len_start + hdr.Length);
                                 */
                                rd_kafka_buf_skip_to(rkbuf, crc_len);

                        } else {
                                int16_t Attributes;
                                int32_t LastOffsetDelta;

                                /* Read LastOffsetDelta to
                                 * determine full offset range.
                                 *
                                 * TODO KIP-932: These bytes come from
                                 * within the CRC-covered region of the
                                 * MessageSet body, and the CRC has
                                 * just failed — so `Attributes` and
                                 * `LastOffsetDelta` may be corrupt. A
                                 * garbage `LastOffsetDelta` can:
                                 *  - be huge → fan-out emits millions of
                                 *    error ops for offsets that don't
                                 *    exist (memory/queue blowup);
                                 *  - be negative → `LastOffset <
                                 *    BaseOffset`, fan-out loop is a
                                 *    silent no-op (no error surfaced);
                                 *  - look valid but be off → REJECT ops
                                 *    issued for the wrong offset range.
                                 */
                                rd_kafka_buf_read_i16(rkbuf, &Attributes);
                                rd_kafka_buf_read_i32(rkbuf, &LastOffsetDelta);
                                LastOffset = hdr.BaseOffset + LastOffsetDelta;

                                rd_kafka_share_msgset_err_ops(
                                    &msetr->msetr_rkq, msetr->msetr_broker_id,
                                    RD_KAFKA_RESP_ERR__BAD_MSG,
                                    msetr->msetr_tver->version, rktp,
                                    hdr.BaseOffset, LastOffset,
                                    RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                                    "MessageSet at offsets %" PRId64 "-%" PRId64
                                    " (%" PRId32
                                    " bytes) "
                                    "failed CRC32C check "
                                    "(original 0x%" PRIx32
                                    " != "
                                    "calculated 0x%" PRIx32 ")",
                                    hdr.BaseOffset, LastOffset, hdr.Length,
                                    hdr.Crc, calc_crc);
                                /* Skip to end of MessageSet */
                                rd_kafka_buf_skip_to(rkbuf,
                                                     len_start + hdr.Length);
                        }

                        rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_err, 1);
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }
        }

        rd_kafka_buf_read_i16(rkbuf, &hdr.Attributes);
        rd_kafka_buf_read_i32(rkbuf, &hdr.LastOffsetDelta);
        LastOffset = hdr.BaseOffset + hdr.LastOffsetDelta;
        rd_kafka_buf_read_i64(rkbuf, &hdr.BaseTimestamp);
        rd_kafka_buf_read_i64(rkbuf, &hdr.MaxTimestamp);
        rd_kafka_buf_read_i64(rkbuf, &hdr.PID);
        rd_kafka_buf_read_i16(rkbuf, &hdr.ProducerEpoch);
        rd_kafka_buf_read_i32(rkbuf, &hdr.BaseSequence);
        rd_kafka_buf_read_i32(rkbuf, &hdr.RecordCount);

        /* Payload size is hdr.Length - MessageSet headers */
        payload_size =
            hdr.Length - (rd_slice_offset(&rkbuf->rkbuf_reader) - len_start);

        if (unlikely(payload_size > rd_kafka_buf_read_remain(rkbuf)))
                rd_kafka_buf_underflow_fail(
                    rkbuf, payload_size,
                    "%s [%" PRId32
                    "] "
                    "MessageSet at offset %" PRId64 " payload size %" PRIusz,
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    hdr.BaseOffset, payload_size);

        /* If entire MessageSet contains old outdated offsets, skip it.
         * Share consumers may receive re-delivered records at earlier offsets
         * after RELEASE, so skip this check for them. */
        if (LastOffset < rktp->rktp_offsets.fetch_pos.offset &&
            !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                rd_kafka_buf_skip(rkbuf, payload_size);
                goto done;
        }

        if (hdr.Attributes & RD_KAFKA_MSGSET_V2_ATTR_CONTROL)
                msetr->msetr_ctrl_cnt++;

        msetr->msetr_v2_hdr = &hdr;

        /* Handle compressed MessageSet */
        if (hdr.Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK) {
                const void *compressed;

                compressed =
                    rd_slice_ensure_contig(&rkbuf->rkbuf_reader, payload_size);
                rd_assert(compressed);

                err = rd_kafka_msgset_reader_decompress(
                    msetr, 2 /*MsgVersion v2*/, hdr.Attributes,
                    hdr.BaseTimestamp, hdr.BaseOffset, compressed,
                    payload_size);
                if (err)
                        goto err;

        } else {
                /* Read uncompressed messages */

                /* Save original slice, reduce size of the current one to
                 * be limited by the MessageSet.Length, and then start reading
                 * messages until the lesser slice is exhausted. */
                if (!rd_slice_narrow_relative(&rkbuf->rkbuf_reader, &save_slice,
                                              payload_size))
                        rd_kafka_buf_check_len(rkbuf, payload_size);

                /* Read messages */
                err = rd_kafka_msgset_reader_msgs_v2(msetr);

                /* Restore wider slice */
                rd_slice_widen(&rkbuf->rkbuf_reader, &save_slice);

                if (unlikely(err))
                        goto err;
        }


done:
        /* Set the next fetch offset to the MessageSet header's last offset + 1
         * to avoid getting stuck on compacted MessageSets where the last
         * Message in the MessageSet has an Offset < MessageSet header's
         * last offset.  See KAFKA-5443 */
        if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk))
                msetr->msetr_next_offset = LastOffset + 1;

        msetr->msetr_v2_hdr = NULL;

        return RD_KAFKA_RESP_ERR_NO_ERROR;

err_parse:
        /* Count all parse errors as partial message errors. */
        rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_partial, 1);
        err = rkbuf->rkbuf_err;
        /* FALLTHRU */
err:
        msetr->msetr_v2_hdr = NULL;
        return err;
}


/**
 * @brief Peek into the next MessageSet to find the MsgVersion.
 *
 * @param MagicBytep the MsgVersion is returned here on success.
 *
 * @returns an error on read underflow or if the MsgVersion is
 *          unsupported.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_peek_msg_version(rd_kafka_msgset_reader_t *msetr,
                                        int8_t *MagicBytep) {
        rd_kafka_buf_t *rkbuf   = msetr->msetr_rkbuf;
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        /* Only log decoding errors if protocol debugging enabled. */
        int log_decode_errors =
            (rkbuf->rkbuf_rkb->rkb_rk->rk_conf.debug & RD_KAFKA_DBG_PROTOCOL)
                ? LOG_DEBUG
                : 0;
        size_t read_offset = rd_slice_offset(&rkbuf->rkbuf_reader);

        rd_kafka_buf_peek_i8(rkbuf, read_offset + 8 + 4 + 4, MagicBytep);

        if (unlikely(*MagicBytep < 0 || *MagicBytep > 2)) {
                int64_t Offset; /* For error logging */
                int32_t Length;

                rd_kafka_buf_read_i64(rkbuf, &Offset);

                rd_rkb_dbg(msetr->msetr_rkb,
                           MSG | RD_KAFKA_DBG_PROTOCOL | RD_KAFKA_DBG_FETCH,
                           "MAGICBYTE",
                           "%s [%" PRId32
                           "]: "
                           "Unsupported Message(Set) MagicByte %d at "
                           "offset %" PRId64
                           " "
                           "(buffer position %" PRIusz "/%" PRIusz
                           "): skipping",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           (int)*MagicBytep, Offset, read_offset,
                           rd_slice_size(&rkbuf->rkbuf_reader));

                if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                        if (Offset >=
                            msetr->msetr_rktp->rktp_offsets.fetch_pos.offset) {
                                rd_kafka_consumer_err(
                                    &msetr->msetr_rkq, msetr->msetr_broker_id,
                                    RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
                                    msetr->msetr_tver->version, NULL, rktp,
                                    Offset,
                                    "Unsupported Message(Set) MagicByte %d "
                                    "at offset %" PRId64,
                                    (int)*MagicBytep, Offset);
                                /* Skip message(set) */
                                msetr->msetr_rktp->rktp_offsets.fetch_pos
                                    .offset = Offset + 1;
                        }

                        /* Skip this Message(Set).
                         * If the message is malformed, the skip may trigger
                         * err_parse and return ERR__BAD_MSG. */
                        rd_kafka_buf_read_i32(rkbuf, &Length);
                        rd_kafka_buf_skip(rkbuf, Length);
                } else {
                        /* Share consumer: peek further into the MessageSet v2
                         * header so we can compute the full offset range,
                         * emit a REJECT range-error op, and skip the
                         * remainder. Speculatively assumes the v2 header
                         * layout is preserved for any future MessageSet
                         * version above v2 (the only versions reaching this
                         * branch — v0/v1 are filtered by the MagicByte
                         * bounds check above).
                         * TODO KIP-932: Check if this handling is correct as
                         * we dont know the magic byte, and we are assuming
                         * its v2.*/
                        int32_t PartitionLeaderEpoch;
                        int8_t MagicByteActual;
                        int32_t Crc;
                        int16_t Attributes;
                        int32_t LastOffsetDelta;
                        int64_t LastOffset;
                        /* Bytes consumed below from the MessageSet body
                         * (everything counted by Length): PartitionLeaderEpoch
                         * (4) + MagicByte (1) + Crc (4) + Attributes (2) +
                         * LastOffsetDelta (4). */
                        const size_t header_bytes_read = 4 + 1 + 4 + 2 + 4;

                        rd_kafka_buf_read_i32(rkbuf, &Length);
                        rd_kafka_buf_read_i32(rkbuf, &PartitionLeaderEpoch);
                        rd_kafka_buf_read_i8(rkbuf, &MagicByteActual);
                        rd_kafka_buf_read_i32(rkbuf, &Crc);
                        rd_kafka_buf_read_i16(rkbuf, &Attributes);
                        rd_kafka_buf_read_i32(rkbuf, &LastOffsetDelta);

                        /* Skip remaining bytes in MessageSet. */
                        rd_kafka_buf_skip(rkbuf, Length - header_bytes_read);

                        LastOffset = Offset + LastOffsetDelta;

                        rd_kafka_share_msgset_err_ops(
                            &msetr->msetr_rkq, msetr->msetr_broker_id,
                            RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
                            msetr->msetr_tver->version, rktp, Offset,
                            LastOffset, RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                            "Unsupported Message(Set) MagicByte %d at offsets "
                            "%" PRId64 "-%" PRId64,
                            (int)MagicByteActual, Offset, LastOffset);
                }

                return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
        }

        return RD_KAFKA_RESP_ERR_NO_ERROR;

err_parse:
        return RD_KAFKA_RESP_ERR__BAD_MSG;
}


/**
 * @brief Parse and read messages from msgset reader buffer.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_buf_t *rkbuf = msetr->msetr_rkbuf;
        rd_kafka_resp_err_t (*reader[])(rd_kafka_msgset_reader_t *) = {
            /* Indexed by MsgVersion/MagicByte, pointing to
             * a Msg(Set)Version reader */
            [0] = rd_kafka_msgset_reader_msg_v0_1,
            [1] = rd_kafka_msgset_reader_msg_v0_1,
            [2] = rd_kafka_msgset_reader_v2};
        rd_kafka_resp_err_t err;

        /* Parse MessageSets until the slice is exhausted or an
         * error occurs (typically a partial message). */
        do {
                int8_t MagicByte;

                /* We dont know the MsgVersion at this point, peek where the
                 * MagicByte resides both in MsgVersion v0..1 and v2 to
                 * know which MessageSet reader to use. */
                err =
                    rd_kafka_msgset_reader_peek_msg_version(msetr, &MagicByte);
                if (unlikely(err)) {
                        if (err == RD_KAFKA_RESP_ERR__BAD_MSG)
                                /* Read underflow, not an error.
                                 * Broker may return a partial Fetch response
                                 * due to its use of sendfile(2). */
                                return RD_KAFKA_RESP_ERR_NO_ERROR;

                        /* For share consumer: continue on unsupported
                         * MsgVersions, the MessageSet will be skipped. Reset
                         * error so loop continues parsing remaining
                         * MessageSets. For regular consumer: stop parsing (old
                         * behavior). */
                        if (RD_KAFKA_IS_SHARE_CONSUMER(
                                msetr->msetr_rkb->rkb_rk) &&
                            err == RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED) {
                                err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        }
                        continue;
                }

                /* Use MsgVersion-specific reader */
                err = reader[(int)MagicByte](msetr);

        } while (!err && rd_slice_remains(&rkbuf->rkbuf_reader) > 0);

        return err;
}



/**
 * @brief MessageSet post-processing.
 *
 * @param last_offsetp will be set to the offset of the last message in the set,
 *                     or -1 if not applicable.
 */
static void rd_kafka_msgset_reader_postproc(rd_kafka_msgset_reader_t *msetr,
                                            int64_t *last_offsetp) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_q_last(&msetr->msetr_rkq, RD_KAFKA_OP_FETCH,
                              0 /* no error ops */);
        if (rko) {
                *last_offsetp = rko->rko_u.fetch.rkm.rkm_offset;

                /*
                 * TODO KIP-932: Though we shouldn't reach here for share
                 * consumers as msetr_relative_offsets is set to true only
                 * for MsgVersion v1 but still check if MsgVersion v1 is
                 * possible with share consumer. If not, this handling can
                 * be skipped safely. If yes, then we should use negative
                 * offset in place of min_offset instead of fetch_pos.offset.
                 */
                if (*last_offsetp != -1 && msetr->msetr_relative_offsets) {
                        /* Update messages to absolute offsets
                         * and purge any messages older than the current
                         * fetch offset. */
                        rd_kafka_q_fix_offsets(
                            &msetr->msetr_rkq,
                            msetr->msetr_rktp->rktp_offsets.fetch_pos.offset,
                            msetr->msetr_outer.offset - *last_offsetp);
                }
        }
}



/**
 * @brief Run the MessageSet reader, read messages until buffer is
 *        exhausted (or error encountered), enqueue parsed messages on
 *        partition queue.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR if MessageSet was successfully
 *          or partially parsed. When other error codes are returned it
 *          indicates a semi-permanent error (such as unsupported MsgVersion)
 *          and the fetcher should back off this partition to avoid
 *          busy-looping.
 */
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_run(rd_kafka_msgset_reader_t *msetr) {
        rd_kafka_toppar_t *rktp = msetr->msetr_rktp;
        rd_kafka_resp_err_t err;
        int64_t last_offset = -1;

        /* Parse MessageSets and messages */
        err = rd_kafka_msgset_reader(msetr);

        if (unlikely(rd_kafka_q_len(&msetr->msetr_rkq) == 0)) {
                /* The message set didn't contain at least one full message
                 * or no error was posted on the response queue.
                 * This means the size limit perhaps was too tight,
                 * increase it automatically.
                 * If there was at least one control message there
                 * is probably not a size limit and nothing is done.
                 * If there were aborted messagesets and no underflow then
                 * there is no error either (#2993).
                 *
                 * Also; avoid propagating underflow errors, which cause
                 * backoffs, since we'll want to continue fetching the
                 * remaining truncated messages as soon as possible.
                 */
                if (msetr->msetr_ctrl_cnt > 0) {
                        /* Noop */
                        if (err == RD_KAFKA_RESP_ERR__UNDERFLOW)
                                err = RD_KAFKA_RESP_ERR_NO_ERROR;

                } else if (rktp->rktp_fetch_msg_max_bytes < (1 << 30)) {
                        /* TODO KIP-932: Skip this branch for share consumers.
                         * ShareFetch has no per-partition MaxBytes field, so
                         * growing rktp_fetch_msg_max_bytes never changes what
                         * the broker returns.
                         */
                        rktp->rktp_fetch_msg_max_bytes *= 2;
                        rd_rkb_dbg(msetr->msetr_rkb, FETCH, "CONSUME",
                                   "Topic %s [%" PRId32
                                   "]: Increasing "
                                   "max fetch bytes to %" PRId32,
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition,
                                   rktp->rktp_fetch_msg_max_bytes);

                        if (err == RD_KAFKA_RESP_ERR__UNDERFLOW)
                                err = RD_KAFKA_RESP_ERR_NO_ERROR;

                } else if (!err && msetr->msetr_aborted_cnt == 0) {
                        if (!RD_KAFKA_IS_SHARE_CONSUMER(
                                msetr->msetr_rkb->rkb_rk)) {
                                rd_kafka_consumer_err(
                                    &msetr->msetr_rkq, msetr->msetr_broker_id,
                                    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
                                    msetr->msetr_tver->version, NULL, rktp,
                                    rktp->rktp_offsets.fetch_pos.offset,
                                    "Message at offset %" PRId64
                                    " might be too large to fetch, try "
                                    "increasing receive.message.max.bytes",
                                    rktp->rktp_offsets.fetch_pos.offset);
                        } else {
                                /* Share consumer: fetch_pos.offset is not
                                 * meaningful, and the temp_fetchq
                                 * acquired-range filter would drop any op
                                 * without a valid offset. Enqueue the error
                                 * directly onto the cgrp queue served by
                                 * rd_kafka_share_poll so the app
                                 * sees the partition-level failure. */
                                rd_kafka_consumer_err(
                                    msetr->msetr_rkb->rkb_rk->rk_cgrp->rkcg_q,
                                    msetr->msetr_broker_id,
                                    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
                                    msetr->msetr_tver->version, NULL, rktp,
                                    RD_KAFKA_OFFSET_INVALID,
                                    "Message might be too large to fetch, "
                                    "try increasing "
                                    "receive.message.max.bytes");
                        }

                } else if (msetr->msetr_aborted_cnt > 0) {
                        /* Noop */
                        if (err == RD_KAFKA_RESP_ERR__UNDERFLOW)
                                err = RD_KAFKA_RESP_ERR_NO_ERROR;
                }

        } else {
                /* MessageSet post-processing. */
                rd_kafka_msgset_reader_postproc(msetr, &last_offset);

                /* Ignore parse errors if there was at least one
                 * good message since it probably indicates a
                 * partial response rather than an erroneous one. */
                if (err == RD_KAFKA_RESP_ERR__UNDERFLOW &&
                    msetr->msetr_msgcnt > 0)
                        err = RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                rd_rkb_dbg(
                    msetr->msetr_rkb, MSG | RD_KAFKA_DBG_FETCH, "CONSUME",
                    "Enqueue %i %smessage(s) (%" PRId64
                    " bytes, %d ops) on %s [%" PRId32
                    "] fetch queue (qlen %d, v%d, last_offset %" PRId64
                    ", %d ctrl msgs, %d aborted msgsets, %s)",
                    msetr->msetr_msgcnt, msetr->msetr_srcname,
                    msetr->msetr_msg_bytes, rd_kafka_q_len(&msetr->msetr_rkq),
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    rd_kafka_q_len(msetr->msetr_par_rkq),
                    msetr->msetr_tver->version, last_offset,
                    msetr->msetr_ctrl_cnt, msetr->msetr_aborted_cnt,
                    msetr->msetr_compression
                        ? rd_kafka_compression2str(msetr->msetr_compression)
                        : "uncompressed");
        } else {
                rd_rkb_dbg(
                    msetr->msetr_rkb, MSG | RD_KAFKA_DBG_FETCH, "CONSUME",
                    "Enqueue %i %smessage(s) (%" PRId64
                    " bytes, %d ops) on %s [%" PRId32
                    "] temp fetchq (qlen %d, last_offset %" PRId64
                    ", %d ctrl msgs, %s)",
                    msetr->msetr_msgcnt, msetr->msetr_srcname,
                    msetr->msetr_msg_bytes, rd_kafka_q_len(&msetr->msetr_rkq),
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    rd_kafka_q_len(msetr->msetr_par_rkq), last_offset,
                    msetr->msetr_ctrl_cnt,
                    msetr->msetr_compression
                        ? rd_kafka_compression2str(msetr->msetr_compression)
                        : "uncompressed");
        }

        /* Concat all messages&errors onto the parent's queue
         * (the partition's fetch queue) */
        if (rd_kafka_q_concat(msetr->msetr_par_rkq, &msetr->msetr_rkq) != -1) {
                /* Update partition's fetch offset based on
                 * last message's offest. */
                if (likely(last_offset != -1) &&
                    !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk))
                        rktp->rktp_offsets.fetch_pos.offset = last_offset + 1;
        }

        /* Adjust next fetch offset if outlier code has indicated
         * an even later next offset. */
        if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
                if (msetr->msetr_next_offset >
                    rktp->rktp_offsets.fetch_pos.offset)
                        rktp->rktp_offsets.fetch_pos.offset =
                            msetr->msetr_next_offset;

                rktp->rktp_offsets.fetch_pos.leader_epoch =
                    msetr->msetr_leader_epoch;
        }

        rd_kafka_q_destroy_owner(&msetr->msetr_rkq);

        /* Skip remaining part of slice so caller can continue
         * with next partition. */
        rd_slice_read(&msetr->msetr_rkbuf->rkbuf_reader, NULL,
                      rd_slice_remains(&msetr->msetr_rkbuf->rkbuf_reader));
        return err;
}



/**
 * @brief Parse one MessageSet at the current buffer read position,
 *        enqueueing messages, propagating errors, etc.
 * @remark The current rkbuf_reader slice must be limited to the MessageSet size
 *
 * @returns see rd_kafka_msgset_reader_run()
 */
rd_kafka_resp_err_t
rd_kafka_msgset_parse(rd_kafka_buf_t *rkbuf,
                      rd_kafka_buf_t *request,
                      rd_kafka_toppar_t *rktp,
                      rd_kafka_aborted_txns_t *aborted_txns,
                      const struct rd_kafka_toppar_ver *tver) {
        rd_kafka_msgset_reader_t msetr;
        rd_kafka_resp_err_t err;

        rd_kafka_msgset_reader_init(&msetr, rkbuf, rktp, tver, aborted_txns,
                                    rktp->rktp_fetchq);

        /* Parse and handle the message set */
        err = rd_kafka_msgset_reader_run(&msetr);

        rd_atomic64_add(&rktp->rktp_c.rx_msgs, msetr.msetr_msgcnt);
        rd_atomic64_add(&rktp->rktp_c.rx_msg_bytes, msetr.msetr_msg_bytes);

        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchcnt,
                   (int64_t)msetr.msetr_msgcnt);
        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchsize,
                   (int64_t)msetr.msetr_msg_bytes);

        return err;
}


/**
 * @brief Parse one MessageSet at the current buffer read position,
 *        enqueueing messages, propagating errors, etc.
 * @remark The current rkbuf_reader slice must be limited to the MessageSet size
 *
 * @returns see rd_kafka_msgset_reader_run()
 */
rd_kafka_resp_err_t
rd_kafka_share_msgset_parse(rd_kafka_buf_t *rkbuf,
                            rd_kafka_toppar_t *rktp,
                            rd_kafka_aborted_txns_t *aborted_txns,
                            const struct rd_kafka_toppar_ver *tver,
                            rd_kafka_q_t *response_message_q) {
        rd_kafka_msgset_reader_t msetr;
        rd_kafka_resp_err_t err;

        rd_kafka_msgset_reader_init(&msetr, rkbuf, rktp, tver, aborted_txns,
                                    response_message_q);

        /* Parse and handle the message set */
        err = rd_kafka_msgset_reader_run(&msetr);

        rd_atomic64_add(&rktp->rktp_c.rx_msgs, msetr.msetr_msgcnt);
        rd_atomic64_add(&rktp->rktp_c.rx_msg_bytes, msetr.msetr_msg_bytes);

        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchcnt,
                   (int64_t)msetr.msetr_msgcnt);
        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchsize,
                   (int64_t)msetr.msetr_msg_bytes);

        return err;
}


/**
 * @brief Offset comparator
 */
static int rd_kafka_offset_cmp(const void *_a, const void *_b) {
        const int64_t *a = _a, *b = _b;
        return (*a > *b) - (*a < *b);
}


/**
 * @brief Pid comparator for rd_kafka_aborted_txn_start_offsets_t
 */
static int rd_kafka_aborted_txn_cmp_by_pid(const void *_a, const void *_b) {
        const rd_kafka_aborted_txn_start_offsets_t *a = _a, *b = _b;
        return (a->pid > b->pid) - (a->pid < b->pid);
}


/**
 * @brief Free resources associated with an AVL tree node.
 */
static void rd_kafka_aborted_txn_node_destroy(void *_node_ptr) {
        rd_kafka_aborted_txn_start_offsets_t *node_ptr = _node_ptr;
        rd_list_destroy(&node_ptr->offsets);
        rd_free(node_ptr);
}


/**
 * @brief Allocate memory for, and initialize a new
 * rd_kafka_aborted_txns_t struct.
 */
rd_kafka_aborted_txns_t *rd_kafka_aborted_txns_new(int32_t txn_cnt) {
        rd_kafka_aborted_txns_t *aborted_txns;
        aborted_txns = rd_malloc(sizeof(*aborted_txns));
        rd_avl_init(&aborted_txns->avl, rd_kafka_aborted_txn_cmp_by_pid, 0);
        rd_list_init(&aborted_txns->list, txn_cnt,
                     rd_kafka_aborted_txn_node_destroy);
        aborted_txns->cnt = txn_cnt;
        return aborted_txns;
}


/**
 * @brief Free all resources associated with a
 * rd_kafka_aborted_txns_t struct.
 */
void rd_kafka_aborted_txns_destroy(rd_kafka_aborted_txns_t *aborted_txns) {
        rd_list_destroy(&aborted_txns->list);
        rd_avl_destroy(&aborted_txns->avl);
        rd_free(aborted_txns);
}


/**
 * @brief Get the abort txn start offsets corresponding to
 * the specified pid.
 */
static RD_INLINE rd_kafka_aborted_txn_start_offsets_t *
rd_kafka_aborted_txns_offsets_for_pid(rd_kafka_aborted_txns_t *aborted_txns,
                                      int64_t pid) {
        rd_kafka_aborted_txn_start_offsets_t node;
        node.pid = pid;
        return RD_AVL_FIND(&aborted_txns->avl, &node);
}


/**
 * @brief Get the next aborted transaction start
 * offset for the specified pid.
 *
 * @param increment_idx if true, the offset index will be incremented.
 * @param max_offset If the next aborted offset is greater than \p max_offset
 *                   then the index is not incremented (regardless of
 *                   \p increment_idx) and the function returns -1.
 *                   This may be the case for empty aborted transactions
 *                   that have an ABORT marker but are not listed in the
 *                   AbortedTxns list.
 *
 *
 * @returns the start offset or -1 if there is none.
 */
static int64_t
rd_kafka_aborted_txns_next_offset(rd_kafka_aborted_txns_t *aborted_txns,
                                  int64_t pid,
                                  rd_bool_t increment_idx,
                                  int64_t max_offset) {
        int64_t abort_start_offset;
        rd_kafka_aborted_txn_start_offsets_t *node_ptr =
            rd_kafka_aborted_txns_offsets_for_pid(aborted_txns, pid);

        if (node_ptr == NULL)
                return -1;

        if (unlikely(node_ptr->offsets_idx >= rd_list_cnt(&node_ptr->offsets)))
                return -1;

        abort_start_offset = *(
            (int64_t *)rd_list_elem(&node_ptr->offsets, node_ptr->offsets_idx));

        if (unlikely(abort_start_offset > max_offset))
                return -1;

        if (increment_idx)
                node_ptr->offsets_idx++;

        return abort_start_offset;
}


/**
 * @brief Get the next aborted transaction start
 * offset for the specified pid and progress the
 * current index to the next one.
 *
 * @param max_offset If the next aborted offset is greater than \p max_offset
 *                   then no offset is popped and the function returns -1.
 *                   This may be the case for empty aborted transactions
 *                   that have an ABORT marker but are not listed in the
 *                   AbortedTxns list.
 *
 * @returns the start offset or -1 if there is none.
 */
static RD_INLINE int64_t
rd_kafka_aborted_txns_pop_offset(rd_kafka_aborted_txns_t *aborted_txns,
                                 int64_t pid,
                                 int64_t max_offset) {
        return rd_kafka_aborted_txns_next_offset(aborted_txns, pid, rd_true,
                                                 max_offset);
}


/**
 * @brief Get the next aborted transaction start
 * offset for the specified pid.
 *
 * @returns the start offset or -1 if there is none.
 */
static RD_INLINE int64_t
rd_kafka_aborted_txns_get_offset(const rd_kafka_aborted_txns_t *aborted_txns,
                                 int64_t pid) {
        return rd_kafka_aborted_txns_next_offset(
            (rd_kafka_aborted_txns_t *)aborted_txns, pid, rd_false, INT64_MAX);
}


/**
 * @brief Add a transaction start offset corresponding
 * to the specified pid to the aborted_txns collection.
 */
void rd_kafka_aborted_txns_add(rd_kafka_aborted_txns_t *aborted_txns,
                               int64_t pid,
                               int64_t first_offset) {
        int64_t *v;
        rd_kafka_aborted_txn_start_offsets_t *node_ptr =
            rd_kafka_aborted_txns_offsets_for_pid(aborted_txns, pid);

        if (!node_ptr) {
                node_ptr              = rd_malloc(sizeof(*node_ptr));
                node_ptr->pid         = pid;
                node_ptr->offsets_idx = 0;
                rd_list_init(&node_ptr->offsets, 0, NULL);
                /* Each PID list has no more than AbortedTxnCnt elements */
                rd_list_prealloc_elems(&node_ptr->offsets, sizeof(int64_t),
                                       aborted_txns->cnt, 0);
                RD_AVL_INSERT(&aborted_txns->avl, node_ptr, avl_node);
                rd_list_add(&aborted_txns->list, node_ptr);
        }

        v  = rd_list_add(&node_ptr->offsets, NULL);
        *v = first_offset;
}


/**
 * @brief Sort each of the abort transaction start
 * offset lists for each pid.
 */
void rd_kafka_aborted_txns_sort(rd_kafka_aborted_txns_t *aborted_txns) {
        int k;
        for (k = 0; k < rd_list_cnt(&aborted_txns->list); k++) {
                rd_kafka_aborted_txn_start_offsets_t *el =
                    rd_list_elem(&aborted_txns->list, k);
                rd_list_sort(&el->offsets, rd_kafka_offset_cmp);
        }
}


/**
 * @brief Unit tests for all functions that operate on
 * rd_kafka_aborted_txns_t
 */
int unittest_aborted_txns(void) {
        rd_kafka_aborted_txns_t *aborted_txns = NULL;
        int64_t start_offset;

        aborted_txns = rd_kafka_aborted_txns_new(7);
        rd_kafka_aborted_txns_add(aborted_txns, 1, 42);
        rd_kafka_aborted_txns_add(aborted_txns, 1, 44);
        rd_kafka_aborted_txns_add(aborted_txns, 1, 10);
        rd_kafka_aborted_txns_add(aborted_txns, 1, 100);
        rd_kafka_aborted_txns_add(aborted_txns, 2, 11);
        rd_kafka_aborted_txns_add(aborted_txns, 2, 7);
        rd_kafka_aborted_txns_add(aborted_txns, 1, 3);
        rd_kafka_aborted_txns_sort(aborted_txns);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(3 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 3",
                     start_offset);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(3 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 3",
                     start_offset);

        start_offset =
            rd_kafka_aborted_txns_pop_offset(aborted_txns, 1, INT64_MAX);
        RD_UT_ASSERT(3 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 3",
                     start_offset);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(10 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 10",
                     start_offset);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 2);
        RD_UT_ASSERT(7 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 7",
                     start_offset);

        rd_kafka_aborted_txns_pop_offset(aborted_txns, 1, INT64_MAX);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(42 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 42",
                     start_offset);

        rd_kafka_aborted_txns_pop_offset(aborted_txns, 1, INT64_MAX);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(44 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 44",
                     start_offset);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 2);
        RD_UT_ASSERT(7 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 7",
                     start_offset);

        rd_kafka_aborted_txns_pop_offset(aborted_txns, 2, INT64_MAX);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 2);
        RD_UT_ASSERT(11 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected 11",
                     start_offset);

        /* error cases */
        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 3);
        RD_UT_ASSERT(-1 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected -1",
                     start_offset);

        rd_kafka_aborted_txns_pop_offset(aborted_txns, 1, INT64_MAX);
        rd_kafka_aborted_txns_pop_offset(aborted_txns, 1, INT64_MAX);
        rd_kafka_aborted_txns_pop_offset(aborted_txns, 2, INT64_MAX);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 1);
        RD_UT_ASSERT(-1 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected -1",
                     start_offset);

        start_offset = rd_kafka_aborted_txns_get_offset(aborted_txns, 2);
        RD_UT_ASSERT(-1 == start_offset,
                     "queried start offset was %" PRId64
                     ", "
                     "expected -1",
                     start_offset);

        rd_kafka_aborted_txns_destroy(aborted_txns);

        RD_UT_PASS();
}


/**
 * @name Unit tests for decompressing MessageSets
 * @{
 */

/** receive.message.max.bytes of the decompression unit tests */
#define UT_DECOMPRESS_MAX_SIZE 100000

typedef enum {
        UT_GZIP,
        UT_SNAPPY,      /**< Unframed */
        UT_SNAPPY_JAVA, /**< Snappy-java framing */
        UT_LZ4,         /**< Uncompressed size not in the frame */
        UT_LZ4_SIZED,   /**< Uncompressed size in the frame */
} ut_codec_t;

static const char *ut_codec_names[] = {"gzip", "snappy", "snappy-java", "lz4",
                                       "lz4 (sized)"};

static const int ut_codec_attrs[] = {
    RD_KAFKA_COMPRESSION_GZIP, RD_KAFKA_COMPRESSION_SNAPPY,
    RD_KAFKA_COMPRESSION_SNAPPY, RD_KAFKA_COMPRESSION_LZ4,
    RD_KAFKA_COMPRESSION_LZ4};

/** @brief Write \p v to \p buf as a \p size bytes big-endian integer. */
static void ut_write_be(char *buf, int64_t v, int size) {
        uint64_t u = (uint64_t)v;
        int i;

        for (i = size - 1; i >= 0; i--, u >>= 8)
                buf[i] = (char)(u & 0xff);
}

/**
 * @brief Write a MsgVersion 1 Message with a null Key to \p buf.
 *
 * @returns the number of bytes written: 34 + \p value_len.
 */
static size_t ut_msg_v1_write(char *buf,
                              int64_t Offset,
                              int8_t Attributes,
                              const char *value,
                              size_t value_len) {
        size_t MessageSize = 4 + 1 + 1 + 8 + 4 + 4 + value_len;

        ut_write_be(buf, Offset, 8);
        ut_write_be(buf + 8, (int64_t)MessageSize, 4);
        buf[16] = 1; /* MagicByte */
        buf[17] = (char)Attributes;
        ut_write_be(buf + 18, 0, 8);  /* Timestamp */
        ut_write_be(buf + 26, -1, 4); /* Key */
        ut_write_be(buf + 30, (int64_t)value_len, 4);
        memcpy(buf + 34, value, value_len);
        /* The Crc covers the Message from its MagicByte */
        ut_write_be(buf + 12, rd_crc32(buf + 16, MessageSize - 4), 4);

        return 12 + MessageSize;
}

/**
 * @brief Write a MsgVersion 2 Record with a null Key and no Headers to
 *        \p buf, which must have room for 43 + \p value_len bytes.
 *
 * @returns the number of bytes written.
 */
static size_t ut_record_v2_write(char *buf,
                                 int32_t OffsetDelta,
                                 const char *value,
                                 size_t value_len) {
        char hdr[32];
        size_t hdrlen = 0;
        size_t of;

        hdr[hdrlen++] = 0;                                 /* Attributes */
        hdrlen += rd_uvarint_enc_i64(hdr + hdrlen, 10, 0); /* Timestamp */
        hdrlen += rd_uvarint_enc_i32(hdr + hdrlen, 10, OffsetDelta);
        hdr[hdrlen++] = 1; /* Key: null (zigzag -1) */
        hdrlen += rd_uvarint_enc_i64(hdr + hdrlen, 10, (int64_t)value_len);

        /* Length, then the Record, ending with a Headers count of 0 */
        of = rd_uvarint_enc_i64(buf, 10, (int64_t)(hdrlen + value_len + 1));
        memcpy(buf + of, hdr, hdrlen);
        of += hdrlen;
        memcpy(buf + of, value, value_len);
        of += value_len;
        buf[of++] = 0;

        return of;
}

/**
 * @returns what a compressed MessageSet of \p MsgVersion decompresses to:
 *          \p msgcnt uncompressed Messages (v1, at relative offsets 0..)
 *          or Records (v2), with Values of \p value_len bytes; and its
 *          size in \p sizep.
 */
static char *
ut_payload_new(int MsgVersion, int msgcnt, size_t value_len, size_t *sizep) {
        char *value = rd_malloc(value_len);
        char *buf   = rd_malloc((size_t)msgcnt * (43 + value_len));
        size_t of   = 0;
        int i;

        for (i = 0; i < msgcnt; i++) {
                memset(value, 'a' + (i % 26), value_len);
                if (MsgVersion == 1)
                        of += ut_msg_v1_write(buf + of, i, 0, value, value_len);
                else
                        of += ut_record_v2_write(buf + of, i, value, value_len);
        }

        rd_free(value);
        *sizep = of;
        return buf;
}

/**
 * @returns \p len bytes of \p in compressed as a producer would with
 *          \p codec, and the compressed size in \p sizep; or NULL if
 *          compression failed.
 */
static char *
ut_compress(ut_codec_t codec, const char *in, size_t len, size_t *sizep) {
        char *out = NULL;

        switch (codec) {
#if WITH_ZLIB
        case UT_GZIP: {
                z_stream strm = RD_ZERO_INIT;
                size_t outsize;
                int r;

                if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK)
                        return NULL;
                outsize        = deflateBound(&strm, (uLong)len);
                out            = rd_malloc(outsize);
                strm.next_in   = (unsigned char *)in;
                strm.avail_in  = (uInt)len;
                strm.next_out  = (unsigned char *)out;
                strm.avail_out = (uInt)outsize;
                r              = deflate(&strm, Z_FINISH);
                *sizep         = strm.total_out;
                deflateEnd(&strm);
                if (r != Z_STREAM_END) {
                        rd_free(out);
                        return NULL;
                }
        } break;
#endif

#if WITH_SNAPPY
        case UT_SNAPPY:
        case UT_SNAPPY_JAVA: {
                /* snappy-java: magic, version, compatible version, then
                 * chunks of at most 32 KiB, each its length and its
                 * Snappy data. */
                static const char snappy_java_hdr[16] = {
                    (char)0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0,
                    0,          0,   0,   1,   0,   0,   0,   1};
                rd_bool_t java = codec == UT_SNAPPY_JAVA;
                size_t chunk   = java ? 32 * 1024 : len;
                size_t of      = java ? 16 : 0;
                size_t in_of   = 0;
                struct snappy_env senv;

                out = rd_malloc(
                    of +
                    (len / (32 * 1024) + 1) *
                        (4 + rd_kafka_snappy_max_compressed_length(chunk)));
                if (java)
                        memcpy(out, snappy_java_hdr, 16);

                rd_kafka_snappy_init_env_sg(&senv, 1 /*iov enable*/);
                do {
                        size_t n            = RD_MIN(chunk, len - in_of);
                        struct iovec iov_in = {.iov_base = (void *)(in + in_of),
                                               .iov_len  = n};
                        struct iovec iov_out = {
                            .iov_base = out + of + (java ? 4 : 0),
                            .iov_len =
                                rd_kafka_snappy_max_compressed_length(n)};

                        if (rd_kafka_snappy_compress_iov(&senv, &iov_in, 1, n,
                                                         &iov_out)) {
                                rd_kafka_snappy_free_env(&senv);
                                rd_free(out);
                                return NULL;
                        }
                        if (java) {
                                ut_write_be(out + of, (int64_t)iov_out.iov_len,
                                            4);
                                of += 4;
                        }
                        of += iov_out.iov_len;
                        in_of += n;
                } while (in_of < len);
                rd_kafka_snappy_free_env(&senv);
                *sizep = of;
        } break;
#endif

        case UT_LZ4:
        case UT_LZ4_SIZED: {
                LZ4F_preferences_t prefs = {
                    .frameInfo = {.blockMode = LZ4F_blockIndependent}};
                size_t outsize;

                if (codec == UT_LZ4_SIZED)
                        prefs.frameInfo.contentSize = len;
                outsize = LZ4F_compressFrameBound(len, &prefs);
                out     = rd_malloc(outsize);
                *sizep  = LZ4F_compressFrame(out, outsize, in, len, &prefs);
                if (LZ4F_isError(*sizep)) {
                        rd_free(out);
                        return NULL;
                }
        } break;

        default:
                RD_NOTREACHED();
        }

        return out;
}

/**
 * @returns a compressed MessageSet of \p MsgVersion: a v1 Message or a v2
 *          RecordBatch with \p Attributes, \p msgcnt messages and
 *          \p z as its compressed Value or Records; its size in \p sizep.
 */
static char *ut_msgset_new(int MsgVersion,
                           int8_t Attributes,
                           int msgcnt,
                           const char *z,
                           size_t zsize,
                           size_t *sizep) {
        char *buf = rd_malloc(61 + zsize);

        if (MsgVersion == 1) {
                /* The Offset of the last inner Message */
                *sizep = ut_msg_v1_write(buf, msgcnt - 1, Attributes, z, zsize);
                return buf;
        }

        ut_write_be(buf, 0, 8);                         /* BaseOffset */
        ut_write_be(buf + 8, (int64_t)(49 + zsize), 4); /* Length */
        ut_write_be(buf + 12, 0, 4); /* PartitionLeaderEpoch */
        buf[16] = 2;                 /* MagicByte */
        ut_write_be(buf + 21, Attributes, 2);
        ut_write_be(buf + 23, msgcnt - 1, 4); /* LastOffsetDelta */
        ut_write_be(buf + 27, 0, 8);          /* BaseTimestamp */
        ut_write_be(buf + 35, 0, 8);          /* MaxTimestamp */
        ut_write_be(buf + 43, -1, 8);         /* ProducerId */
        ut_write_be(buf + 51, -1, 2);         /* ProducerEpoch */
        ut_write_be(buf + 53, -1, 4);         /* BaseSequence */
        ut_write_be(buf + 57, msgcnt, 4);     /* RecordCount */
        memcpy(buf + 61, z, zsize);
        /* The Crc (CRC-32C) covers the batch from its Attributes */
        ut_write_be(buf + 17, rd_crc32c(0, buf + 21, 40 + zsize), 4);

        *sizep = 61 + zsize;
        return buf;
}

/**
 * @brief Parse the MessageSet \p buf of \p size bytes (freed here).
 *
 * @returns the number of messages on the partition's fetch queue, and the
 *          first error there in \p errp.
 */
static int ut_msgset_parse(rd_kafka_t *rk,
                           char *buf,
                           size_t size,
                           rd_kafka_resp_err_t *errp) {
        rd_kafka_topic_t *rkt   = rd_kafka_topic_new(rk, "ut_decompress", NULL);
        rd_kafka_toppar_t *rktp = rd_kafka_toppar_new(rkt, 0);
        struct rd_kafka_toppar_ver tver = {.rktp = rktp, .version = 1};
        rd_kafka_buf_t *rkbuf = rd_kafka_buf_new_shadow(buf, size, rd_free);
        rd_kafka_op_t *rko;
        int msgcnt = 0;

        rkbuf->rkbuf_rkb = rd_kafka_broker_internal(rk);
        rd_kafka_msgset_parse(rkbuf, NULL, rktp, NULL, &tver);
        rd_kafka_buf_destroy(rkbuf);

        *errp = RD_KAFKA_RESP_ERR_NO_ERROR;
        while ((rko = rd_kafka_q_pop(rktp->rktp_fetchq, RD_POLL_NOWAIT, 0))) {
                if (rko->rko_type == RD_KAFKA_OP_FETCH)
                        msgcnt++;
                else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR && !*errp)
                        *errp = rko->rko_err;
                rd_kafka_op_destroy(rko);
        }

        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);

        return msgcnt;
}

/**
 * @brief A compressed MessageSet is read when it decompresses to at most
 *        receive.message.max.bytes, and refused when it decompresses to
 *        more, as with any response larger than that.
 */
static int ut_decompress_max_size(rd_kafka_t *rk, ut_codec_t codec) {
        /* Within the limit, at it and one byte beyond it (v1: a Message
         * is its Value and 34 bytes), and far beyond it. */
        static const struct {
                int msgcnt;
                size_t value_len;
        } cases[] = {{10, 1000},
                     {1, UT_DECOMPRESS_MAX_SIZE - 34},
                     {1, UT_DECOMPRESS_MAX_SIZE - 33},
                     {200, 1000}};
        int MsgVersion;
        int i;

        RD_UT_BEGIN();

        for (MsgVersion = 1; MsgVersion <= 2; MsgVersion++) {
                for (i = 0; i < (int)RD_ARRAYSIZE(cases); i++) {
                        int msgcnt = cases[i].msgcnt;
                        rd_kafka_resp_err_t err;
                        size_t size, zsize, msgset_size;
                        char *payload, *z, *msgset;
                        int got;

                        payload = ut_payload_new(MsgVersion, msgcnt,
                                                 cases[i].value_len, &size);
                        z       = ut_compress(codec, payload, size, &zsize);
                        RD_UT_ASSERT(z, "%s: compression failed",
                                     ut_codec_names[codec]);
                        RD_UT_ASSERT(zsize < UT_DECOMPRESS_MAX_SIZE / 4,
                                     "%s: %" PRIusz " compressed bytes",
                                     ut_codec_names[codec], zsize);

                        msgset =
                            ut_msgset_new(MsgVersion, ut_codec_attrs[codec],
                                          msgcnt, z, zsize, &msgset_size);
                        got = ut_msgset_parse(rk, msgset, msgset_size, &err);

                        if (size <= UT_DECOMPRESS_MAX_SIZE)
                                RD_UT_ASSERT(got == msgcnt && !err,
                                             "%s v%d: %" PRIusz
                                             " bytes: expected %d messages, "
                                             "got %d and error %s",
                                             ut_codec_names[codec], MsgVersion,
                                             size, msgcnt, got,
                                             rd_kafka_err2name(err));
                        else
                                RD_UT_ASSERT(
                                    got == 0 &&
                                        err ==
                                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                    "%s v%d: %" PRIusz
                                    " bytes: expected _BAD_COMPRESSION, "
                                    "got %d messages and error %s",
                                    ut_codec_names[codec], MsgVersion, size,
                                    got, rd_kafka_err2name(err));

                        rd_free(z);
                        rd_free(payload);
                }
        }

        RD_UT_PASS();
}

#if WITH_ZLIB
/**
 * @brief A gzip stream that ends early is refused.
 */
static int ut_decompress_gzip_truncated(rd_kafka_t *rk) {
        size_t size, zsize, msgset_size;
        char *payload, *z, *msgset;
        rd_kafka_resp_err_t err;
        int got;

        RD_UT_BEGIN();

        payload = ut_payload_new(1, 10, 1000, &size);
        z       = ut_compress(UT_GZIP, payload, size, &zsize);
        RD_UT_ASSERT(z, "compression failed");

        msgset = ut_msgset_new(1, RD_KAFKA_COMPRESSION_GZIP, 10, z, zsize / 2,
                               &msgset_size);
        got    = ut_msgset_parse(rk, msgset, msgset_size, &err);
        RD_UT_ASSERT(got == 0 && err == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "expected _BAD_COMPRESSION, got %d messages and error %s",
                     got, rd_kafka_err2name(err));

        rd_free(z);
        rd_free(payload);

        RD_UT_PASS();
}
#endif

/**
 * @brief A compressed MessageSet that decompresses to nothing is refused.
 */
static int ut_decompress_empty(rd_kafka_t *rk, ut_codec_t codec) {
        size_t zsize, msgset_size;
        char *z, *msgset;
        rd_kafka_resp_err_t err;
        int got;

        RD_UT_BEGIN();

        z = ut_compress(codec, "", 0, &zsize);
        RD_UT_ASSERT(z, "%s: compression failed", ut_codec_names[codec]);

        msgset =
            ut_msgset_new(1, ut_codec_attrs[codec], 1, z, zsize, &msgset_size);
        got = ut_msgset_parse(rk, msgset, msgset_size, &err);
        RD_UT_ASSERT(got == 0 && err == RD_KAFKA_RESP_ERR__BAD_MSG,
                     "%s: expected _BAD_MSG, got %d messages and error %s",
                     ut_codec_names[codec], got, rd_kafka_err2name(err));

        rd_free(z);

        RD_UT_PASS();
}

/**
 * @brief A compressed Message inside a compressed MessageSet is refused.
 */
static int ut_decompress_nested(rd_kafka_t *rk) {
        size_t size, zsize, inner_size, zinner_size, msgset_size;
        char *payload, *z, *inner, *zinner, *msgset;
        rd_kafka_resp_err_t err;
        int got;

        RD_UT_BEGIN();

        /* An LZ4 compressed MessageSet of 10 Messages, as the Value of the
         * one Message of an LZ4 compressed MessageSet. */
        payload = ut_payload_new(1, 10, 100, &size);
        z       = ut_compress(UT_LZ4, payload, size, &zsize);
        RD_UT_ASSERT(z, "compression failed");
        inner  = ut_msgset_new(1, RD_KAFKA_COMPRESSION_LZ4, 10, z, zsize,
                               &inner_size);
        zinner = ut_compress(UT_LZ4, inner, inner_size, &zinner_size);
        RD_UT_ASSERT(zinner, "compression failed");

        msgset = ut_msgset_new(1, RD_KAFKA_COMPRESSION_LZ4, 10, zinner,
                               zinner_size, &msgset_size);
        got    = ut_msgset_parse(rk, msgset, msgset_size, &err);
        RD_UT_ASSERT(got == 0 && err == RD_KAFKA_RESP_ERR__BAD_MSG,
                     "expected _BAD_MSG, got %d messages and error %s", got,
                     rd_kafka_err2name(err));

        rd_free(zinner);
        rd_free(inner);
        rd_free(z);
        rd_free(payload);

        RD_UT_PASS();
}

/**
 * @brief Unit tests for decompressing MessageSets.
 */
int unittest_msgset_decompress(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char max_size[16];
        char errstr[256];
        rd_kafka_t *rk;
        int fails = 0;

        /* The smallest message.max.bytes and fetch.max.bytes let
         * receive.message.max.bytes be small. */
        rd_snprintf(max_size, sizeof(max_size), "%d", UT_DECOMPRESS_MAX_SIZE);
        if (rd_kafka_conf_set(conf, "message.max.bytes", "1000", errstr,
                              sizeof(errstr)) ||
            rd_kafka_conf_set(conf, "fetch.max.bytes", "1000", errstr,
                              sizeof(errstr)) ||
            rd_kafka_conf_set(conf, "receive.message.max.bytes", max_size,
                              errstr, sizeof(errstr)))
                RD_UT_FAIL("%s", errstr);
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        RD_UT_ASSERT(rk, "%s", errstr);

#if WITH_ZLIB
        fails += ut_decompress_max_size(rk, UT_GZIP);
        fails += ut_decompress_gzip_truncated(rk);
        fails += ut_decompress_empty(rk, UT_GZIP);
#endif
#if WITH_SNAPPY
        fails += ut_decompress_max_size(rk, UT_SNAPPY);
        fails += ut_decompress_max_size(rk, UT_SNAPPY_JAVA);
        fails += ut_decompress_empty(rk, UT_SNAPPY);
#endif
        fails += ut_decompress_max_size(rk, UT_LZ4);
        fails += ut_decompress_max_size(rk, UT_LZ4_SIZED);
        fails += ut_decompress_empty(rk, UT_LZ4_SIZED);
        fails += ut_decompress_nested(rk);

        rd_kafka_destroy(rk);

        return fails;
}

/**@}*/
