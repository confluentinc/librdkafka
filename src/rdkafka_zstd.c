/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2018-2022, Magnus Edenhill
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
#include "rdkafka_zstd.h"
#include "rdunittest.h"

#if WITH_ZSTD_STATIC
/* Enable advanced/unstable API for initCStream_srcSize */
#define ZSTD_STATIC_LINKING_ONLY
#endif

#include <zstd.h>
#include <zstd_errors.h>

rd_kafka_resp_err_t rd_kafka_zstd_decompress(rd_kafka_broker_t *rkb,
                                             char *inbuf,
                                             size_t inlen,
                                             void **outbuf,
                                             size_t *outlenp) {
        /* The decompressed size is capped by receive.message.max.bytes */
        const size_t max_bufsize =
            (size_t)rkb->rkb_rk->rk_conf.recv_max_msg_size;
        unsigned long long content_size;
        ZSTD_DStream *dctx;
        ZSTD_inBuffer in;
        ZSTD_outBuffer out;
        rd_bool_t cached_dctx;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t ret;

        *outbuf = NULL;

        in.src  = inbuf;
        in.size = inlen;
        in.pos  = 0;

        out.dst  = NULL;
        out.size = 0;
        out.pos  = 0;

        content_size = ZSTD_getFrameContentSize(inbuf, inlen);

        switch (content_size) {
        case ZSTD_CONTENTSIZE_UNKNOWN:
                /* Decompressed size is unknown, make a guess (but never
                 * above the maximum): the buffer is grown as needed below. */
                out.size = (size_t)RD_MIN((unsigned long long)inlen * 2,
                                          (unsigned long long)max_bufsize);
                break;
        case ZSTD_CONTENTSIZE_ERROR:
                /* Error calculating frame content size */
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to begin ZSTD decompression "
                           "(out buffer is %llu bytes): %s",
                           content_size, "Error in determining frame size");
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        default:
                /* Frame states a larger size than we may return: fail
                 * before allocating or decompressing anything. */
                if (content_size > (unsigned long long)max_bufsize) {
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "Unable to decompress ZSTD "
                                   "(input buffer %" PRIusz
                                   ", output buffer %llu): "
                                   "output would exceed "
                                   "receive.message.max.bytes (%d)",
                                   inlen, content_size,
                                   rkb->rkb_rk->rk_conf.recv_max_msg_size);
                        return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                }
                out.size = (size_t)content_size;
                break;
        }

        /* An empty frame must still return a valid buffer. */
        if (out.size < 1)
                out.size = 1;

        /* A ZSTD_DStream must not be used concurrently, so only use the
         * broker's cached context when running on its own thread, which is
         * the case for all Fetch and ShareFetch responses. */
        cached_dctx = thrd_is_current(rkb->rkb_thread);
        if (cached_dctx) {
                if (!rkb->rkb_zstd_dctx)
                        rkb->rkb_zstd_dctx = ZSTD_createDStream();
                dctx = rkb->rkb_zstd_dctx;
        } else {
                dctx = ZSTD_createDStream();
        }

        if (unlikely(!dctx)) {
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to create ZSTD decompression context");
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        /* Also clears any error state left by a previously failed frame. */
        ret = ZSTD_initDStream(dctx);
        if (unlikely(ZSTD_isError(ret))) {
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to begin ZSTD decompression "
                           "(out buffer is %" PRIusz " bytes): %s",
                           out.size, ZSTD_getErrorName(ret));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        out.dst = rd_malloc(out.size);
        if (unlikely(!out.dst)) {
                rd_rkb_log(rkb, LOG_WARNING, "ZSTD",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes for %" PRIusz
                           " compressed bytes): %s",
                           out.size, inlen, rd_strerror(errno));
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

        /* Decompress input buffer to output buffer, growing it as needed. */
        for (;;) {
                size_t in_pos_prev  = in.pos;
                size_t out_pos_prev = out.pos;

                ret = ZSTD_decompressStream(dctx, &out, &in);
                if (unlikely(ZSTD_isError(ret))) {
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "ZSTD decompression failed "
                                   "(at %" PRIusz " of %" PRIusz
                                   " compressed bytes, output buffer is "
                                   "%" PRIusz " bytes): %s",
                                   in.pos, in.size, out.size,
                                   ZSTD_getErrorName(ret));
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }

                /* Frame boundary reached with all input consumed. */
                if (ret == 0 && in.pos == in.size)
                        break;

                if (out.pos == out.size) {
                        /* Need to grow output buffer, this shouldn't happen
                         * if the frame stated the content size. */
                        size_t extra;
                        size_t new_size;
                        char *tmp;

                        if (out.size >= max_bufsize) {
                                rd_rkb_dbg(
                                    rkb, MSG, "ZSTD",
                                    "Unable to decompress ZSTD "
                                    "(input buffer %" PRIusz
                                    ", output buffer %" PRIusz
                                    "): "
                                    "output would exceed "
                                    "receive.message.max.bytes (%d)",
                                    inlen, out.size,
                                    rkb->rkb_rk->rk_conf.recv_max_msg_size);
                                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                                goto done;
                        }

                        /* Grow exponentially with some factor > 1 (using
                         * 1.75) for amortized O(1) copying, but never past
                         * the maximum, so that it is always reachable. */
                        extra    = RD_MAX(out.size * 3 / 4, 1024);
                        new_size = RD_MIN(out.size + extra, max_bufsize);

                        rd_atomic64_add(&rkb->rkb_c.zbuf_grow, 1);

                        if (unlikely(!(tmp = rd_realloc(out.dst, new_size)))) {
                                rd_rkb_log(rkb, LOG_WARNING, "ZSTD",
                                           "Unable to grow decompression "
                                           "buffer from %" PRIusz " to %" PRIusz
                                           " bytes: %s",
                                           out.size, new_size,
                                           rd_strerror(errno));
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                goto done;
                        }

                        out.dst  = tmp;
                        out.size = new_size;

                        continue;
                }

                if (in.pos == in.size) {
                        /* More output expected but no input left. */
                        rd_rkb_dbg(
                            rkb, MSG, "ZSTD",
                            "Unable to decompress ZSTD "
                            "(input buffer %" PRIusz ", output buffer %" PRIusz
                            "): "
                            "truncated input, %" PRIusz " more bytes expected",
                            inlen, out.size, ret);
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }

                /* zstd always consumes input or produces output when both
                 * are available, but guard against stalling the broker
                 * thread in an endless loop. */
                if (unlikely(in.pos == in_pos_prev &&
                             out.pos == out_pos_prev)) {
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "Unable to decompress ZSTD "
                                   "(input buffer %" PRIusz
                                   ", output buffer %" PRIusz
                                   "): "
                                   "no progress at input offset %" PRIusz
                                   ", output offset %" PRIusz,
                                   inlen, out.size, in.pos, out.pos);
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

        *outbuf  = out.dst;
        *outlenp = out.pos;

done:
        if (!cached_dctx)
                ZSTD_freeDStream(dctx);

        if (err && out.dst)
                rd_free(out.dst);

        return err;
}


void rd_kafka_zstd_dctx_destroy(rd_kafka_broker_t *rkb) {
        if (!rkb->rkb_zstd_dctx)
                return;

        ZSTD_freeDStream(rkb->rkb_zstd_dctx);
        rkb->rkb_zstd_dctx = NULL;
}


rd_kafka_resp_err_t rd_kafka_zstd_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           void **outbuf,
                                           size_t *outlenp) {
        ZSTD_CStream *cctx;
        size_t r;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len              = rd_slice_remains(slice);
        ZSTD_outBuffer out;
        ZSTD_inBuffer in;

        *outbuf  = NULL;
        out.pos  = 0;
        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes): %s",
                           out.size, rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }


        cctx = ZSTD_createCStream();
        if (!cctx) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to create ZSTD compression context");
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

#if defined(WITH_ZSTD_STATIC) &&                                               \
    ZSTD_VERSION_NUMBER >= (1 * 100 * 100 + 2 * 100 + 1) /* v1.2.1 */
        r = ZSTD_initCStream_srcSize(cctx, comp_level, len);
#else
        /* libzstd not linked statically (or zstd version < 1.2.1):
         * decompression in consumer may be more costly due to
         * decompressed size not included in header by librdkafka producer */
        r = ZSTD_initCStream(cctx, comp_level);
#endif
        if (ZSTD_isError(r)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to begin ZSTD compression "
                           "(out buffer is %" PRIusz " bytes): %s",
                           out.size, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        while ((in.size = rd_slice_reader(slice, &in.src))) {
                in.pos = 0;
                r      = ZSTD_compressStream(cctx, &out, &in);
                if (unlikely(ZSTD_isError(r))) {
                        rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                                   "ZSTD compression failed "
                                   "(at of %" PRIusz
                                   " bytes, with "
                                   "%" PRIusz
                                   " bytes remaining in out buffer): "
                                   "%s",
                                   in.size, out.size - out.pos,
                                   ZSTD_getErrorName(r));
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }

                /* No space left in output buffer,
                 * but input isn't fully consumed */
                if (in.pos < in.size) {
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

        if (rd_slice_remains(slice) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %" PRIusz " bytes: %s",
                           len, "Unexpected trailing data");
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        r = ZSTD_endStream(cctx, &out);
        if (unlikely(ZSTD_isError(r) || r > 0)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %" PRIusz " bytes: %s",
                           len, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        *outbuf  = out.dst;
        *outlenp = out.pos;

done:
        if (cctx)
                ZSTD_freeCStream(cctx);

        if (err)
                rd_free(out.dst);

        return err;
}


/**
 * @name Unit tests
 * @{
 *
 */

/**
 * @brief Create a minimal broker object for decompression unit tests.
 *
 * The broker thread is set to the current thread so that the cached
 * decompression context path is exercised.
 */
static rd_kafka_broker_t *ut_zstd_broker_new(rd_kafka_t **rkp) {
        rd_kafka_t *rk;
        rd_kafka_broker_t *rkb;
        char errstr[512];

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(), errstr,
                          sizeof(errstr));
        if (!rk)
                return NULL;

        rkb              = rd_calloc(1, sizeof(*rkb));
        rkb->rkb_rk      = rk;
        rkb->rkb_thread  = thrd_current();
        rkb->rkb_source  = RD_KAFKA_CONFIGURED;
        rkb->rkb_logname = rd_strdup("ut-zstd");
        rd_strlcpy(rkb->rkb_name, "ut-zstd", sizeof(rkb->rkb_name));
        mtx_init(&rkb->rkb_logname_lock, mtx_plain);
        mtx_init(&rkb->rkb_lock, mtx_plain);
        rd_atomic64_init(&rkb->rkb_c.zbuf_grow, 0);

        *rkp = rk;
        return rkb;
}

static void ut_zstd_broker_destroy(rd_kafka_broker_t *rkb, rd_kafka_t *rk) {
        rd_kafka_zstd_dctx_destroy(rkb);
        mtx_destroy(&rkb->rkb_logname_lock);
        mtx_destroy(&rkb->rkb_lock);
        rd_free(rkb->rkb_logname);
        rd_free(rkb);
        rd_kafka_destroy(rk);
}


/**
 * @brief Compress \p src using the one-shot API, which states the
 *        decompressed size in the frame header.
 */
static char *
ut_zstd_compress_known(const char *src, size_t len, size_t *outlenp) {
        size_t bufsize = ZSTD_compressBound(len);
        char *buf      = rd_malloc(bufsize);
        size_t r;

        r = ZSTD_compress(buf, bufsize, src, len, 3);
        if (ZSTD_isError(r)) {
                rd_free(buf);
                return NULL;
        }

        *outlenp = r;
        return buf;
}


/**
 * @brief Compress \p src using the streaming API without pledging the
 *        source size, leaving the decompressed size out of the frame
 *        header. This is what a dynamically linked librdkafka producer and
 *        the Kafka brokers emit, and requires the buffer to grow.
 */
static char *
ut_zstd_compress_unknown(const char *src, size_t len, size_t *outlenp) {
        ZSTD_CStream *cctx;
        ZSTD_outBuffer out;
        ZSTD_inBuffer in;
        size_t r;

        cctx = ZSTD_createCStream();
        if (!cctx)
                return NULL;

        r = ZSTD_initCStream(cctx, 3);
        if (ZSTD_isError(r)) {
                ZSTD_freeCStream(cctx);
                return NULL;
        }

        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size);
        out.pos  = 0;

        in.src  = src;
        in.size = len;
        in.pos  = 0;

        r = ZSTD_compressStream(cctx, &out, &in);
        if (ZSTD_isError(r) || in.pos != in.size) {
                rd_free(out.dst);
                ZSTD_freeCStream(cctx);
                return NULL;
        }

        r = ZSTD_endStream(cctx, &out);
        ZSTD_freeCStream(cctx);
        if (ZSTD_isError(r) || r > 0) {
                rd_free(out.dst);
                return NULL;
        }

        *outlenp = out.pos;
        return out.dst;
}


int unittest_zstd(void) {
        static const size_t payload_size = 1024 * 1024;
        rd_kafka_t *rk;
        rd_kafka_broker_t *rkb;
        char *payload;
        char *known, *unknown, *concat;
        size_t known_len, unknown_len, concat_len;
        void *outbuf;
        size_t outlen;
        rd_kafka_resp_err_t err;
        int64_t grows;
        struct ZSTD_DCtx_s *dctx;
        size_t i;

        RD_UT_BEGIN();

        rkb = ut_zstd_broker_new(&rk);
        RD_UT_ASSERT(rkb, "failed to create unittest broker");

        /* Highly compressible payload, so that the inlen*2 guess used for
         * frames without a stated content size is far too small. */
        payload = rd_malloc(payload_size);
        for (i = 0; i < payload_size; i++)
                payload[i] = (char)('a' + (i % 16));

        known = ut_zstd_compress_known(payload, payload_size, &known_len);
        RD_UT_ASSERT(known, "failed to compress payload (known size)");
        RD_UT_ASSERT(ZSTD_getFrameContentSize(known, known_len) ==
                         (unsigned long long)payload_size,
                     "expected frame to state the content size");

        unknown = ut_zstd_compress_unknown(payload, payload_size, &unknown_len);
        RD_UT_ASSERT(unknown, "failed to compress payload (unknown size)");
        RD_UT_ASSERT(ZSTD_getFrameContentSize(unknown, unknown_len) ==
                         ZSTD_CONTENTSIZE_UNKNOWN,
                     "expected frame to not state the content size");
        RD_UT_ASSERT(unknown_len * 2 < payload_size,
                     "expected the inlen*2 guess (%" PRIusz
                     ") to be smaller than the decompressed size (%" PRIusz ")",
                     unknown_len * 2, payload_size);

        /*
         * Known content size, fits: exact allocation, no growth, and the
         * context is cached on the broker.
         */
        rk->rk_conf.recv_max_msg_size = (int)payload_size * 2;
        grows                         = rd_atomic64_get(&rkb->rkb_c.zbuf_grow);
        err = rd_kafka_zstd_decompress(rkb, known, known_len, &outbuf, &outlen);
        RD_UT_ASSERT(!err, "decompression failed: %s", rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(!memcmp(outbuf, payload, payload_size),
                     "decompressed data mismatch");
        RD_UT_ASSERT(rd_atomic64_get(&rkb->rkb_c.zbuf_grow) == grows,
                     "expected no buffer growth for a known content size");
        RD_UT_ASSERT(rkb->rkb_zstd_dctx,
                     "expected the decompression context to be cached");
        rd_free(outbuf);
        dctx = rkb->rkb_zstd_dctx;

        /*
         * Unknown content size: the buffer must grow, and the cached
         * context must be reused.
         */
        grows = rd_atomic64_get(&rkb->rkb_c.zbuf_grow);
        err   = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                         &outlen);
        RD_UT_ASSERT(!err, "decompression failed: %s", rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(!memcmp(outbuf, payload, payload_size),
                     "decompressed data mismatch");
        RD_UT_ASSERT(rd_atomic64_get(&rkb->rkb_c.zbuf_grow) > grows,
                     "expected the output buffer to have grown");
        RD_UT_ASSERT(rkb->rkb_zstd_dctx == dctx,
                     "expected the cached decompression context to be reused");
        rd_free(outbuf);

        /*
         * Unknown content size with receive.message.max.bytes just above
         * the decompressed size: the growth must be clamped so that the
         * maximum is always reachable (#5260).
         */
        rk->rk_conf.recv_max_msg_size = (int)payload_size + 100;
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                       &outlen);
        RD_UT_ASSERT(!err,
                     "decompression up to receive.message.max.bytes failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(!memcmp(outbuf, payload, payload_size),
                     "decompressed data mismatch");
        rd_free(outbuf);

        /*
         * Unknown content size that does not fit within
         * receive.message.max.bytes.
         */
        rk->rk_conf.recv_max_msg_size = (int)payload_size - 1;
        outbuf                        = (void *)0x1;
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                       &outlen);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "expected BAD_COMPRESSION, not %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(!outbuf, "expected no output buffer on failure");

        /*
         * Known content size that does not fit within
         * receive.message.max.bytes: must fail without allocating or
         * growing anything.
         */
        grows  = rd_atomic64_get(&rkb->rkb_c.zbuf_grow);
        outbuf = (void *)0x1;
        err = rd_kafka_zstd_decompress(rkb, known, known_len, &outbuf, &outlen);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "expected BAD_COMPRESSION, not %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(!outbuf, "expected no output buffer on failure");
        RD_UT_ASSERT(rd_atomic64_get(&rkb->rkb_c.zbuf_grow) == grows,
                     "expected no buffer growth when the stated content "
                     "size exceeds the maximum");

        rk->rk_conf.recv_max_msg_size = (int)payload_size * 2;

        /*
         * Corrupted data.
         */
        known[known_len / 2] = (char)~known[known_len / 2];
        known[known_len - 1] = (char)~known[known_len - 1];
        outbuf               = (void *)0x1;
        err = rd_kafka_zstd_decompress(rkb, known, known_len, &outbuf, &outlen);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "expected BAD_COMPRESSION, not %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(!outbuf, "expected no output buffer on failure");

        /*
         * A failed frame must not leave the cached context unusable.
         */
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                       &outlen);
        RD_UT_ASSERT(!err, "decompression after a failed frame failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(!memcmp(outbuf, payload, payload_size),
                     "decompressed data mismatch");
        rd_free(outbuf);

        /*
         * Truncated input.
         */
        outbuf = (void *)0x1;
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len - 16, &outbuf,
                                       &outlen);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "expected BAD_COMPRESSION for truncated input, not %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(!outbuf, "expected no output buffer on failure");

        /*
         * Two concatenated frames must both be decompressed.
         * ZSTD_getFrameContentSize() only reports the size of the first
         * frame, so this also covers growing past a stated content size.
         */
        concat_len = unknown_len * 2;
        concat     = rd_malloc(concat_len);
        memcpy(concat, unknown, unknown_len);
        memcpy(concat + unknown_len, unknown, unknown_len);
        err =
            rd_kafka_zstd_decompress(rkb, concat, concat_len, &outbuf, &outlen);
        RD_UT_ASSERT(!err, "decompression of concatenated frames failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size * 2,
                     "expected %" PRIusz " bytes, not %" PRIusz,
                     payload_size * 2, outlen);
        RD_UT_ASSERT(
            !memcmp(outbuf, payload, payload_size) &&
                !memcmp((char *)outbuf + payload_size, payload, payload_size),
            "decompressed data mismatch");
        rd_free(outbuf);
        rd_free(concat);

        /*
         * An empty frame must still return a valid buffer.
         */
        rd_free(known);
        known = ut_zstd_compress_known("", 0, &known_len);
        RD_UT_ASSERT(known, "failed to compress an empty payload");
        outbuf = NULL;
        err = rd_kafka_zstd_decompress(rkb, known, known_len, &outbuf, &outlen);
        RD_UT_ASSERT(!err, "decompression of an empty frame failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == 0, "expected 0 bytes, not %" PRIusz, outlen);
        RD_UT_ASSERT(outbuf, "expected a valid output buffer");
        rd_free(outbuf);

        /*
         * Called from a thread that is not the broker thread: a temporary
         * context is used and the cached one is left alone.
         */
        dctx            = rkb->rkb_zstd_dctx;
        rkb->rkb_thread = rk->rk_thread;
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                       &outlen);
        rkb->rkb_thread = thrd_current();
        RD_UT_ASSERT(!err, "off-thread decompression failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(!memcmp(outbuf, payload, payload_size),
                     "decompressed data mismatch");
        RD_UT_ASSERT(rkb->rkb_zstd_dctx == dctx,
                     "expected the cached context to be left untouched "
                     "when called off the broker thread");
        rd_free(outbuf);

        /*
         * Freeing the cached context (as done on connection teardown)
         * must make the next decompression create a new one.
         */
        rd_kafka_zstd_dctx_destroy(rkb);
        RD_UT_ASSERT(!rkb->rkb_zstd_dctx,
                     "expected the cached context to be freed");
        err = rd_kafka_zstd_decompress(rkb, unknown, unknown_len, &outbuf,
                                       &outlen);
        RD_UT_ASSERT(!err, "decompression after context destroy failed: %s",
                     rd_kafka_err2name(err));
        RD_UT_ASSERT(outlen == payload_size,
                     "expected %" PRIusz " bytes, not %" PRIusz, payload_size,
                     outlen);
        RD_UT_ASSERT(rkb->rkb_zstd_dctx,
                     "expected a new decompression context to be cached");
        rd_free(outbuf);

        rd_free(known);
        rd_free(unknown);
        rd_free(payload);
        ut_zstd_broker_destroy(rkb, rk);

        RD_UT_PASS();
}

/**@}*/
