/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2021 Magnus Edenhill
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
#include "rdkafka_compression.h"
#include "rdunittest.h"

#include "rdkafka_lz4.h"
#if WITH_LZ4_EXT
#include <lz4frame.h>
#else
#define LZ4F_STATIC_LINKING_ONLY
#include "lz4frame.h"
#endif

#if WITH_ZSTD
#include <zstd.h>
#include <zstd_errors.h>
#endif


static size_t overalloc;


/**
 * @struct Compression context that abstracts the underlying compression type.
 *
 * Only supports streaming compression (snappy is thus not supported).
 */
struct rd_kafka_compressor_s {
        rd_kafka_compression_t rkcompr_type; /**< Compression type */

        /**
         * Initialize the compression context.
         */
        rd_kafka_error_t *(*rkcompr_init)(struct rd_kafka_compressor_s *compr,
                                          rd_buf_t *output_rbuf,
                                          int compression_level);

        /**
         * Compress the data in \p buf (of size \p size) and append it to the
         * \p output_rbuf buffer, which will be resized as necessary.
         */
        rd_kafka_error_t *(*rkcompr_write)(struct rd_kafka_compressor_s *compr,
                                           rd_buf_t *output_rbuf,
                                           const void *buf,
                                           size_t size);

        /**
         * Finish and close the current compression frame,
         * freeing the underlying compressor context.
         *
         * If \p outbuf_rbuf is NULL the compressor context is just freed
         * and NULL (no error) is returned.
         */
        rd_kafka_error_t *(*rkcompr_close)(struct rd_kafka_compressor_s *compr,
                                           rd_buf_t *output_rbuf);

        /** Cached error from a failed write(). Is returned from close(). */
        rd_kafka_error_t *rkcompr_error;

        /** To avoid calling the compression library with a lot of small writes
         *  (such as individual message header fields (61 bytes)) we accumulate
         *  small writes in a fillbuffer that we flush when exceeded.
         *  This is especially important for gzip which has about >50%
         *  performance drop on small writes. */
        char *rkcompr_fillbuf;
        size_t rkcompr_fillbuf_len;  /**< Current fill buffer length */
        size_t rkcompr_fillbuf_size; /**< Fill buffer allocation size */

        size_t rkcompr_soft_size_limit; /**< Try to keep compressed size under
                                         *   this limit. */
        size_t rkcompr_insize;          /**< Uncompressed size */
        size_t rkcompr_origsize; /**< Original/starting size of output buffer.
                                  *   Used to calculate the compressed size
                                  *   and thus the compression ratio in
                                  *   ..close(). */
        int rkcompr_writecnt;    /**< Number of write()s. */
        int rkcompr_zwritecnt;   /**< Compression codec writes. */

        union {
#if WITH_ZLIB
                struct {
                        z_stream strm;
                } gzip;
#endif
                struct {
                        LZ4F_compressionContext_t cctx;
                        LZ4F_preferences_t prefs;
                } lz4;
#if WITH_STREAMING_ZSTD
                struct {
                        ZSTD_CCtx *cctx;
                } zstd;
#endif
        } rkcompr_u;

#define rkcompr_gzip rkcompr_u.gzip
#define rkcompr_lz4  rkcompr_u.lz4
#define rkcompr_zstd rkcompr_u.zstd
};



#if WITH_ZLIB
/**
 * GZIP compression
 *
 *
 */

rd_kafka_error_t *rd_kafka_compressor_gzip_close(rd_kafka_compressor_t *compr,
                                                 rd_buf_t *output_rbuf) {
        size_t needed           = 64;
        rd_kafka_error_t *error = NULL;

        if (unlikely(!output_rbuf))
                goto done;

        rd_dassert(compr->rkcompr_gzip.strm.avail_in == 0);
        compr->rkcompr_gzip.strm.next_in  = NULL;
        compr->rkcompr_gzip.strm.avail_in = 0;

        /* Flush and finalize output stream */
        while (1) {
                size_t pre_avail_out, written;
                int r;

                /* Allocate output buffer space if needed and
                 * get write pointer (and size). */
                compr->rkcompr_gzip.strm.avail_out = pre_avail_out =
                    rd_buf_ensure_writable(
                        output_rbuf, needed,
                        (void **)&compr->rkcompr_gzip.strm.next_out);

                /* Compress */
                r = deflate(&compr->rkcompr_gzip.strm, Z_FINISH);

                /* Get actual written size and update the output buffer
                 * as written accordingly. */
                written = pre_avail_out - compr->rkcompr_gzip.strm.avail_out;
                if (written)
                        rd_buf_write(output_rbuf, NULL, written);

                if (r == Z_STREAM_END)
                        break;
                else if (unlikely(r != Z_OK && r != Z_BUF_ERROR)) {
                        error = rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "Failed to finish gzip-compression: %s",
                            compr->rkcompr_gzip.strm.msg
                                ? compr->rkcompr_gzip.strm.msg
                                : "unspecified error");
                        break;
                }

                needed = RD_MIN(written / 2, 512);
        }

done:
        deflateEnd(&compr->rkcompr_gzip.strm);

        return error;
}



rd_kafka_error_t *rd_kafka_compressor_gzip_write(rd_kafka_compressor_t *compr,
                                                 rd_buf_t *output_rbuf,
                                                 const void *buf,
                                                 size_t size) {
        int flush_flag = Z_NO_FLUSH;
        size_t maxsz   = deflateBound(&compr->rkcompr_gzip.strm,
                                    compr->rkcompr_insize +
                                        compr->rkcompr_fillbuf_size + size);


        /* If we're approaching the output buffer limit then flush on each
         * deflate() to make sure it is not exceeded. */
        if (unlikely(maxsz >= compr->rkcompr_soft_size_limit))
                flush_flag = Z_PARTIAL_FLUSH;

        compr->rkcompr_gzip.strm.next_in  = (void *)buf;
        compr->rkcompr_gzip.strm.avail_in = size;

        do {
                size_t pre_avail_out, written;
                int r;

                /* Ensure space for 25% of the remaining buffer (plus 256
                 * to avoid too small allocations), and get write pointer
                 * and writable size. This caters to a compression ratio
                 * of at least 4x. */
                compr->rkcompr_gzip.strm.avail_out = pre_avail_out =
                    rd_buf_ensure_writable(
                        output_rbuf,
                        256 + compr->rkcompr_gzip.strm.avail_in / 4,
                        (void **)&compr->rkcompr_gzip.strm.next_out);

                /* Compress */
                r = deflate(&compr->rkcompr_gzip.strm, flush_flag);

                /* Get actual written size and update the output buffer
                 * as written accordingly. */
                written = pre_avail_out - compr->rkcompr_gzip.strm.avail_out;
                if (written)
                        rd_buf_write(output_rbuf, NULL, written);

                if (unlikely(r == Z_STREAM_ERROR))
                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "Failed to gzip-compress %" PRIusz " bytes: %s",
                            size,
                            compr->rkcompr_gzip.strm.msg
                                ? compr->rkcompr_gzip.strm.msg
                                : "unspecified error");

        } while (compr->rkcompr_gzip.strm.avail_in > 0);

        return NULL;
}


rd_kafka_error_t *rd_kafka_compressor_gzip_init(rd_kafka_compressor_t *compr,
                                                rd_buf_t *output_rbuf,
                                                int compression_level) {
        int r;

        r = deflateInit2(&compr->rkcompr_gzip.strm, compression_level,
                         Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY);
        if (r != Z_OK)
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__FAIL,
                    "Failed to initialize gzip compressor: "
                    "%s (ret=%d)",
                    compr->rkcompr_gzip.strm.msg ? compr->rkcompr_gzip.strm.msg
                                                 : "?",
                    r);

        return NULL;
}
#endif /* WITH_ZLIB */



/**
 * LZ4 compression
 *
 *
 */

rd_kafka_error_t *rd_kafka_compressor_lz4_close(rd_kafka_compressor_t *compr,
                                                rd_buf_t *output_rbuf) {
        rd_kafka_error_t *error = NULL;
        void *out;
        size_t out_size;
        size_t r;

        if (unlikely(!output_rbuf))
                goto done;

        /* Grow output buffer to allow for the frame content footer
         * and any remaining bytes not flushed yet, and get write pointer
         * (and size) of output buffer */
        out_size = rd_buf_ensure_writable(
            output_rbuf, LZ4F_compressBound(0, &compr->rkcompr_lz4.prefs),
            &out);

        r = LZ4F_compressEnd(compr->rkcompr_lz4.cctx, out, out_size, NULL);

        if (unlikely(LZ4F_isError(r)))
                error =
                    rd_kafka_error_new(RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                       "Failed to finalize LZ4 compression: %s",
                                       LZ4F_getErrorName(r));
        else
                rd_buf_write(output_rbuf, NULL, r);

done:
        LZ4F_freeCompressionContext(compr->rkcompr_lz4.cctx);

        return error;
}



rd_kafka_error_t *rd_kafka_compressor_lz4_write(rd_kafka_compressor_t *compr,
                                                rd_buf_t *output_rbuf,
                                                const void *buf,
                                                size_t size) {
        size_t of               = 0;
        const size_t block_size = (64 * (1 << 10)) - LZ4F_HEADER_SIZE_MAX;

        for (of = 0; of < size; of += block_size) {
                const char *chunk = (const char *)buf + of;
                size_t chunk_size = RD_MIN(block_size, size - of);
                size_t r;
                void *out;
                size_t out_size;
                size_t maxsz = (64 * (1 << 10));
                int retries  = 0;
                // LZ4F_compressBound(chunk_size, &compr->rkcompr_lz4.prefs);
                if (0)
                        printf("maxsz %zu for chunk_size %zu, tot %zu\n", maxsz,
                               chunk_size, size);

                        /* Base flushing on the uncompressed length since
                         * compressBound() for small sizes may be much larger (a
                         * compression block of 64K) than the actual final size.
                         * FIXME: But then we're missing out on the unflushed
                         * bytes.. */
#if 0
                rd_bool_t do_flush = rd_buf_len(output_rbuf) + size >=
                        compr->rkcompr_soft_size_limit;
#endif

        retry:
                out_size = rd_buf_ensure_writable_contig(
                    output_rbuf, maxsz,
                    RD_MIN(153600 /*overalloc*/,
                           compr->rkcompr_soft_size_limit / 2),
                    &out);

                /* Get pointer (and size) of contiguous output buffer */
                // out_size = rd_buf_get_writable(output_rbuf, &out);

                r = LZ4F_compressUpdate(compr->rkcompr_lz4.cctx, out, out_size,
                                        chunk, chunk_size, NULL);
                if (0)
                        printf(
                            "wrote %zu for chunk_size %zu, maxsz %zu out_size "
                            "%zu: "
                            "insize %zu, written size %zu, unwritten %zd, "
                            "soft %zu, retries %d\n",
                            r, chunk_size, maxsz, out_size,
                            compr->rkcompr_insize, rd_buf_len(output_rbuf),
                            compr->rkcompr_insize - rd_buf_len(output_rbuf),
                            compr->rkcompr_soft_size_limit, retries);

                if (unlikely(LZ4F_isError(r))) {
                        if (LZ4F_getErrorCode(r) ==
                                LZ4F_ERROR_dstMaxSize_tooSmall &&
                            retries++ == 0) {
                                /* If we're nearing the maximum compressor size
                                 * we need to flush LZ4's internal block buffers
                                 * for each call to avoid overflowing the output
                                 * buffer (which is not really overflowed, just
                                 * written past this compressor's soft limit).
                                 */

                                /* Get pointer (and size) of output buffer,
                                 * which is already big enough for the flush
                                 * bytes thanks to the compressBound() call
                                 * above. */

                                r = LZ4F_flush(compr->rkcompr_lz4.cctx, out,
                                               out_size, NULL);

                                if (unlikely(LZ4F_isError(r)))
                                        return rd_kafka_error_new(
                                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                            "LZ4 compression flush failed "
                                            "(%" PRIusz
                                            " output buffer bytes available, "
                                            "maxsz %" PRIusz "): %s",
                                            out_size, maxsz,
                                            LZ4F_getErrorName(r));

                                /* Update written length */
                                rd_buf_write(output_rbuf, NULL, r);

                                goto retry;
                        }

                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "LZ4 compression for %" PRIusz
                            " bytes failed "
                            "(%" PRIusz
                            " output buffer bytes available, "
                            "maxsz %" PRIusz "): %s",
                            chunk_size, out_size, maxsz, LZ4F_getErrorName(r));
                }

                /* Update written length */
                rd_buf_write(output_rbuf, NULL, r);
        }

#if 0
        /* If we're nearing the maximum compressor size we need to flush
         * LZ4's internal block buffers for each call to avoid overflowing
         * the output buffer (which is not really overflowed, just written
         * past this compressor's soft limit). */
        if (unlikely(do_flush)) {
                /* Get pointer (and size) of output buffer, which is already
                 * big enough for the flush bytes thanks to the compressBound()
                 * call above. */
                out_size = rd_buf_get_writable(output_rbuf, &out);

                r = LZ4F_flush(compr->rkcompr_lz4.cctx, out, out_size, NULL);

                if (unlikely(LZ4F_isError(r)))
                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "LZ4 compression flush failed "
                            "(%" PRIusz
                            " output buffer bytes available, "
                            "maxsz %" PRIusz "): %s",
                            out_size, maxsz, LZ4F_getErrorName(r));

                /* Update written length */
                rd_buf_write(output_rbuf, NULL, r);
        }
#endif

        return NULL;
}

rd_kafka_error_t *rd_kafka_compressor_lz4_init(rd_kafka_compressor_t *compr,
                                               rd_buf_t *output_rbuf,
                                               int compression_level) {
        LZ4F_errorCode_t r;
        void *out;
        size_t out_size;
        const size_t block_size = 64 * (1 << 10);

        compr->rkcompr_lz4.prefs.frameInfo.blockSizeID = LZ4F_max64KB;
        compr->rkcompr_lz4.prefs.frameInfo.blockMode   = LZ4F_blockIndependent;
        compr->rkcompr_lz4.prefs.compressionLevel      = compression_level;
        // FIXME: make sure this does not underflow
        compr->rkcompr_soft_size_limit -= block_size;
        if (compr->rkcompr_soft_size_limit < block_size + LZ4F_HEADER_SIZE_MAX)
                compr->rkcompr_soft_size_limit =
                    block_size + LZ4F_HEADER_SIZE_MAX;

        r = LZ4F_createCompressionContext(&compr->rkcompr_lz4.cctx,
                                          LZ4F_VERSION);
        if (LZ4F_isError(r))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                    "Failed to create LZ4 compression context: %s",
                    LZ4F_getErrorName(r));

        /* Allocate space for the LZ4F header and some more. */
        rd_buf_write_ensure(output_rbuf, LZ4F_HEADER_SIZE_MAX + 512, 0);

        /* Get pointer (and size) of output buffer */
        out_size = rd_buf_get_writable(output_rbuf, &out);

        r = LZ4F_compressBegin(compr->rkcompr_lz4.cctx, out, out_size,
                               &compr->rkcompr_lz4.prefs);
        if (LZ4F_isError(r)) {
                LZ4F_freeCompressionContext(compr->rkcompr_lz4.cctx);
                return rd_kafka_error_new(RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                          "Failed to begin LZ4 compression: %s",
                                          LZ4F_getErrorName(r));
        }

        /* Update written length */
        rd_buf_write(output_rbuf, NULL, r);

        return NULL;
}

/**
 * TODO:
 * - Pass 64KB / compressUpdate() to keep buffers down.
 * - flush() will never need more than one block size - preallocate it.
 * - see if LinkedBlocks can be used.
 */


#if WITH_STREAMING_ZSTD
/**
 * ZSTD compression
 *
 *
 */

rd_kafka_error_t *rd_kafka_compressor_zstd_close(rd_kafka_compressor_t *compr,
                                                 rd_buf_t *output_rbuf) {
        rd_kafka_error_t *error = NULL;
        ZSTD_inBuffer in        = {pos: 0, size: 0};
        size_t needed           = 0;


        if (unlikely(!output_rbuf))
                goto done;

        while (1) {
                size_t r;
                ZSTD_outBuffer out = {pos: 0};

                out.size =
                    rd_buf_ensure_writable(output_rbuf, needed, &out.dst);

                r = ZSTD_compressStream2(compr->rkcompr_zstd.cctx, &out, &in,
                                         ZSTD_e_end);

                /* Update written length */
                rd_buf_write(output_rbuf, NULL, out.pos);

                if (likely(r == 0))
                        break;
                else if (unlikely(ZSTD_isError(r))) {
                        error = rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "ZSTD compression end frame failed: %s",
                            ZSTD_getErrorName(r));
                        break;
                }

                /* Need more space */
                needed = r;
        }

done:
        ZSTD_freeCCtx(compr->rkcompr_zstd.cctx);

        return error;
}



rd_kafka_error_t *rd_kafka_compressor_zstd_write(rd_kafka_compressor_t *compr,
                                                 rd_buf_t *output_rbuf,
                                                 const void *buf,
                                                 size_t size) {
        ZSTD_inBuffer in = {pos: 0, size: size, src: buf};

        while (in.pos < in.size) {
                size_t remaining = in.size - in.pos;
                size_t maxsz     = ZSTD_compressBound(remaining);
                size_t r;
                ZSTD_outBuffer out = {pos: 0};

                out.size = rd_buf_ensure_writable(output_rbuf, maxsz, &out.dst);

                r = ZSTD_compressStream2(compr->rkcompr_zstd.cctx, &out, &in,
                                         ZSTD_e_continue);

                if (unlikely(ZSTD_isError(r)))
                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "ZSTD compresion failed at "
                            "%" PRIusz "/%" PRIusz ": %s",
                            in.pos, in.size, ZSTD_getErrorName(r));

                /* Update written length */
                rd_buf_write(output_rbuf, NULL, out.pos);
        }

        return NULL;
}

rd_kafka_error_t *rd_kafka_compressor_zstd_init(rd_kafka_compressor_t *compr,
                                                rd_buf_t *output_rbuf,
                                                int compression_level) {
        size_t r;

        compr->rkcompr_zstd.cctx = ZSTD_createCCtx();
        if (!compr->rkcompr_zstd.cctx)
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                    "Unable to create ZSTD compression context");

        r = ZSTD_CCtx_setParameter(compr->rkcompr_zstd.cctx,
                                   ZSTD_c_compressionLevel, compression_level);
        if (ZSTD_isError(r)) {
                ZSTD_freeCCtx(compr->rkcompr_zstd.cctx);
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Failed to set ZSTD compression level "
                    "to %d: %s",
                    compression_level, ZSTD_getErrorName(r));
        }

        return NULL;
}
#endif /* WITH_STREAMING_ZSTD */


/**
 * @brief Free a compressor previously allocated with
 *        rd_kafka_compressor_alloc().
 *
 * The compressor must have been closed first.
 */
void rd_kafka_compressor_free(rd_kafka_compressor_t *compr) {
        rd_free(compr);
}

/**
 * @brief Allocate an unitilized compressor.
 */
rd_kafka_compressor_t *rd_kafka_compressor_alloc(void) {
        return rd_malloc(sizeof(rd_kafka_compressor_t));
}


/**
 * @brief Flush the fill buffer to the output buffer.
 */
static RD_INLINE rd_kafka_error_t *
rd_kafka_compressor_fillbuf_flush(rd_kafka_compressor_t *compr,
                                  rd_buf_t *output_rbuf) {
        size_t size = compr->rkcompr_fillbuf_len;

        if (size == 0)
                return NULL;

        compr->rkcompr_fillbuf_len = 0;
        compr->rkcompr_zwritecnt++;

        return compr->rkcompr_write(compr, output_rbuf, compr->rkcompr_fillbuf,
                                    size);
}

/**
 * @brief Finalize (if \p output_rbuf is not NULL) the current compression
 *        and write footers, etc, to \p output_rbuf.
 *
 * The compression ratio is returned in \p *ratiop (if non-NULL) on success.
 */
rd_kafka_error_t *rd_kafka_compressor_close(rd_kafka_compressor_t *compr,
                                            rd_buf_t *output_rbuf,
                                            double *ratiop) {
        if (ratiop)
                *ratiop = 0.0;

        /* Check for previous write error */
        if (unlikely(compr->rkcompr_error != NULL))
                goto err_close;

        if (output_rbuf) {
                /* Flush the fillbuffer first. */
                compr->rkcompr_error =
                    rd_kafka_compressor_fillbuf_flush(compr, output_rbuf);

                if (unlikely(compr->rkcompr_error != NULL))
                        goto err_close;
        }

        RD_IF_FREE(compr->rkcompr_fillbuf, rd_free);

        /* Let compression codec finalize and close the compression */
        compr->rkcompr_error = compr->rkcompr_close(compr, output_rbuf);
        if (unlikely(compr->rkcompr_error != NULL))
                return compr->rkcompr_error;

        if (likely(output_rbuf && ratiop)) {
                *ratiop =
                    (double)compr->rkcompr_insize /
                    (double)(rd_buf_len(output_rbuf) - compr->rkcompr_origsize);
#if 0
                printf("%s: Buffer utilization: %.1f%%. Compr ratio %.2f, outlen %zu, inlen %zu, ova %zu\n",
                       rd_kafka_compression2str(compr->rkcompr_type),
                       rd_buf_utilization(output_rbuf) * 100.0, *ratiop,
                       rd_buf_len(output_rbuf), compr->rkcompr_insize,
                       overalloc);
                rd_buf_dump(output_rbuf, 0);
#endif
        }

        return NULL;

err_close:

        /* Compressor has failed, just free it
         * and propagate the original error. */
        compr->rkcompr_close(compr, NULL);

        return compr->rkcompr_error;
}


/**
 * @returns true if the compressor has failed, else true.
 *
 * Use rd_kafka_compressor_close() to free the compressor and return the
 * error.
 */
rd_bool_t rd_kafka_compressor_failed(const rd_kafka_compressor_t *compr) {
        return !!compr->rkcompr_error;
}


const rd_kafka_error_t *rd_kafka_compressor_write(rd_kafka_compressor_t *compr,
                                                  rd_buf_t *output_rbuf,
                                                  const void *buf,
                                                  size_t size) {
        rd_kafka_error_t *error;

        /* If compressor has already failed, do nothing, and wait
         * for close() to propagate the error. */
        if (unlikely(compr->rkcompr_error != NULL))
                return NULL;

        // printf("write %zu\n", size);
        compr->rkcompr_insize += size;
        compr->rkcompr_writecnt++;

        if (compr->rkcompr_fillbuf_size) {
                /* There's a fill buffer. */

                if (compr->rkcompr_fillbuf_len + size <=
                    compr->rkcompr_fillbuf_size) {
                        /* Write small writes to fill buffer */
                        memcpy(compr->rkcompr_fillbuf +
                                   compr->rkcompr_fillbuf_len,
                               buf, size);
                        compr->rkcompr_fillbuf_len += size;
                        return NULL;
                }

                /* Flush the fill buffer before writing */
                error = rd_kafka_compressor_fillbuf_flush(compr, output_rbuf);

                if (unlikely(error != NULL)) {
                        compr->rkcompr_error = error;
                        return error;
                }
        }

        /* Perform proper write */
        compr->rkcompr_zwritecnt++;
        error = compr->rkcompr_write(compr, output_rbuf, buf, size);

        if (unlikely(error != NULL))
                compr->rkcompr_error = error;

        return error;
}


/**
 * @brief Initialize streaming compressor for \p compression_type.
 *
 * @param compression_level is the compression-type specific compression level
 *                          as returned by
 *                          rd_kafka_compression_level_translate().
 * @param soft_size_limit is the soft output size limit, as the output buffer
 *                        approaches this limit the flushing will become
 *                        more aggressive. There are no guarantees that the
 *                        output buffer will be smaller or equal to this limit.
 */
rd_kafka_error_t *
rd_kafka_compressor_init(rd_kafka_compressor_t *compr,
                         rd_buf_t *output_rbuf,
                         rd_kafka_compression_t compression_type,
                         int compression_level,
                         size_t soft_size_limit) {
        static const rd_kafka_compressor_t tmpl[RD_KAFKA_COMPRESSION_NUM] = {
#if WITH_ZLIB
                [RD_KAFKA_COMPRESSION_GZIP] =
                    {
                        .rkcompr_init  = rd_kafka_compressor_gzip_init,
                        .rkcompr_write = rd_kafka_compressor_gzip_write,
                        .rkcompr_close = rd_kafka_compressor_gzip_close,
                        /* zlib has a high performance penalty for small writes,
                         * so use a large fillbuffer. */
                        .rkcompr_fillbuf_size = 1024,
                    },
#endif
                [RD_KAFKA_COMPRESSION_LZ4] =
                    {
                        .rkcompr_init  = rd_kafka_compressor_lz4_init,
                        .rkcompr_write = rd_kafka_compressor_lz4_write,
                        .rkcompr_close = rd_kafka_compressor_lz4_close,
                        //.rkcompr_fillbuf_size = 64*(1<<10),
                    },
#if WITH_STREAMING_ZSTD
                [RD_KAFKA_COMPRESSION_ZSTD] =
                    {
                        .rkcompr_init  = rd_kafka_compressor_zstd_init,
                        .rkcompr_write = rd_kafka_compressor_zstd_write,
                        .rkcompr_close = rd_kafka_compressor_zstd_close,
                    },
#endif
        };

        if (unlikely(!tmpl[compression_type].rkcompr_init))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                    "This build of librdkafka lacks %s compression support",
                    rd_kafka_compression2str(compression_type));

        *compr = tmpl[compression_type];

        compr->rkcompr_type            = compression_type;
        compr->rkcompr_origsize        = rd_buf_len(output_rbuf);
        compr->rkcompr_soft_size_limit = soft_size_limit;

        overalloc = atoi(rd_getenv("OVA", "262144"));

        if (compr->rkcompr_fillbuf_size > 0)
                compr->rkcompr_fillbuf = rd_malloc(compr->rkcompr_fillbuf_size);

        /* Pass further initialization to compression specific initializer. */
        return compr->rkcompr_init(compr, output_rbuf, compression_level);
}



/**
 * @brief Translate generic compression.level to compression-type
 *        specific compression level.
 */
int rd_kafka_compression_level_translate(
    rd_kafka_compression_t compression_type,
    int compression_level) {

        switch (compression_type) {
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP:
                if (compression_level == RD_KAFKA_COMPLEVEL_DEFAULT)
                        return Z_DEFAULT_COMPRESSION;
                else if (compression_level > RD_KAFKA_COMPLEVEL_GZIP_MAX)
                        return RD_KAFKA_COMPLEVEL_GZIP_MAX;
                break;
#endif
        case RD_KAFKA_COMPRESSION_LZ4:
                if (compression_level == RD_KAFKA_COMPLEVEL_DEFAULT)
                        /* LZ4 has no notion of system-wide default compression
                         * level, use zero in this case */
                        return 0;
                else if (compression_level > RD_KAFKA_COMPLEVEL_LZ4_MAX)
                        return RD_KAFKA_COMPLEVEL_LZ4_MAX;
                break;
#if WITH_STREAMING_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD:
                if (compression_level == RD_KAFKA_COMPLEVEL_DEFAULT)
                        return 3;
                else if (compression_level > RD_KAFKA_COMPLEVEL_ZSTD_MAX)
                        return RD_KAFKA_COMPLEVEL_ZSTD_MAX;
                break;
#endif
        case RD_KAFKA_COMPRESSION_SNAPPY:
        default:
                /* Compression level has no effect in this case */
                return RD_KAFKA_COMPLEVEL_DEFAULT;
        }

        return RD_KAFKA_COMPLEVEL_DEFAULT;
}


/**
 * @name Simple decompressors.
 *
 */
#if WITH_ZLIB
static rd_kafka_error_t *rd_kafka_gzip_decompress(rd_slice_t *in,
                                                  rd_buf_t *out) {
        z_stream strm = RD_ZERO_INIT;
        rd_kafka_error_t *error;
        int r;

        r = inflateInit2(&strm, 15 + 32);
        if (unlikely(r != Z_OK))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                    "Failed to initialize gzip decompressor: "
                    "%s (ret=%d)",
                    strm.msg ? strm.msg : "?", r);

        while ((strm.avail_in =
                    rd_slice_reader(in, (const void **)&strm.next_in))) {

                do {
                        size_t pre_avail_out, written;

                        strm.avail_out = pre_avail_out = rd_buf_ensure_writable(
                            out, strm.avail_in * 8, (void **)&strm.next_out);

                        r = inflate(&strm, Z_NO_FLUSH);

                        written = pre_avail_out - strm.avail_out;
                        if (likely(written))
                                rd_buf_write(out, NULL, written);

                        if (unlikely(r != Z_OK && r != Z_STREAM_END)) {
                                error = rd_kafka_error_new(
                                    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                    "Failed to decompress gzip: "
                                    "%s (ret=%d)",
                                    strm.msg ? strm.msg : "", r);
                                inflateEnd(&strm);
                                return error;
                        }
                } while (strm.avail_in);
        }

        r = inflateEnd(&strm);
        if (unlikely(r != Z_OK))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                    "Failed to finish gzip decompression: %s (ret=%d)",
                    strm.msg ? strm.msg : "", r);

        return NULL;
}
#endif


/**
 * @brief LZ4F decompressor.
 *
 * @warning No support for broken Kafka legacy lz4 framing.
 *          See rd_kafka_lz4_compress().
 */

static rd_kafka_error_t *rd_kafka_lz4_decompress2(rd_slice_t *in,
                                                  rd_buf_t *out) {
        LZ4F_errorCode_t code;
        LZ4F_decompressionContext_t dctx;
        size_t in_sz;
        size_t r;
        const void *inp;

        code = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
        if (LZ4F_isError(code))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                    "Unable to create LZ4 decompression context: %s",
                    LZ4F_getErrorName(code));



        while ((in_sz = rd_slice_peeker(in, &inp))) {
                void *outp;
                size_t out_sz = rd_buf_ensure_writable(out, in_sz * 4, &outp);

                r = LZ4F_decompress(dctx, outp, &out_sz, inp, &in_sz, NULL);

                if (unlikely(LZ4F_isError(r))) {
                        LZ4F_freeDecompressionContext(dctx);
                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                            "Failed to decompress LZ4F: %s",
                            LZ4F_getErrorName(r));
                }

                /* Update read and write positions */
                rd_slice_read(in, NULL, in_sz);
                rd_buf_write(out, NULL, out_sz);
        }

        code = LZ4F_freeDecompressionContext(dctx);
        if (LZ4F_isError(code))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                    "Failed to close LZ4 decompression context: %s",
                    LZ4F_getErrorName(code));

        return NULL;
}

#if WITH_ZSTD
static rd_kafka_error_t *rd_kafka_zstd_decompress(rd_slice_t *in,
                                                  rd_buf_t *out) {
        ZSTD_DCtx *dctx = ZSTD_createDCtx();
        ZSTD_inBuffer input;
        size_t last_ret = 0;
        size_t minsz    = ZSTD_DStreamOutSize();

        if (!dctx)
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                    "Failed to create ZSTD decompression context");

        while ((input.pos = 0, input.size = rd_slice_reader(in, &input.src))) {

                while (input.pos < input.size) {
                        ZSTD_outBuffer output = {pos: 0};
                        size_t r;

                        output.size =
                            rd_buf_ensure_writable(out, minsz, &output.dst);

                        r = ZSTD_decompressStream(dctx, &output, &input);
                        if (unlikely(ZSTD_isError(r))) {
                                ZSTD_freeDCtx(dctx);
                                return rd_kafka_error_new(
                                    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                                    "Failed to compress ZSTD: %s",
                                    ZSTD_getErrorName(r));
                        }

                        rd_buf_write(out, NULL, output.pos);
                        last_ret = r;
                }
        }

        if (last_ret != 0) {
                ZSTD_freeDCtx(dctx);
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                    "Reached end of ZSTD input without completing the "
                    "current frame");
        }

        ZSTD_freeDCtx(dctx);

        return NULL;
}
#endif


/**
 * @brief Simple decompressor.
 */
static rd_kafka_error_t *
rd_kafka_decompress(rd_kafka_compression_t compression_type,
                    rd_slice_t *in,
                    rd_buf_t *out) {
        switch (compression_type) {
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP:
                return rd_kafka_gzip_decompress(in, out);
#endif

        case RD_KAFKA_COMPRESSION_LZ4:
                return rd_kafka_lz4_decompress2(in, out);

#if WITH_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD:
                return rd_kafka_zstd_decompress(in, out);
#endif

        case RD_KAFKA_COMPRESSION_SNAPPY:
                /* This decompressor interface is currently only used by
                 * the streaming compressor unit tests, which do not support
                 * snappy, so we skip snappy support here until it is needed. */
                /*FALLTHRU*/
        default:
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                    "This build of librdkafka lacks %s compression support",
                    rd_kafka_compression2str(compression_type));
        }

        RD_NOTREACHED();
        return NULL;
}



/**
 * Unit tests
 *
 */


/**
 * @brief PRNG from http://burtleburtle.net/bob/rand/smallprng.html
 *
 */
typedef unsigned long int ut_u4;
typedef struct ut_ranctx {
        ut_u4 a;
        ut_u4 b;
        ut_u4 c;
        ut_u4 d;
} ut_ranctx;

#define ut_rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))

static ut_u4 ut_ranval(ut_ranctx *x) {
        ut_u4 e = x->a - ut_rot(x->b, 27);
        x->a    = x->b ^ ut_rot(x->c, 17);
        x->b    = x->c + x->d;
        x->c    = x->d + e;
        x->d    = e + x->a;
        return x->d;
}

static void ut_raninit(ut_ranctx *x, ut_u4 seed) {
        ut_u4 i;
        x->a = 0xf1ea5eed, x->b = x->c = x->d = seed;
        for (i = 0; i < 20; ++i) {
                (void)ut_ranval(x);
        }
}


static void ut_gen_entropy(ut_ranctx *ranctx, char *dstbuf, size_t size) {
        size_t remains = size;
        char *dst      = dstbuf;

        while (remains >= sizeof(ut_u4)) {
                ut_u4 v = htole32(ut_ranval(ranctx));
                memcpy(dst, &v, sizeof(v));
                remains -= sizeof(v);
                dst += sizeof(v);
        }

        while (remains-- > 0)
                *(dst++) = (char)(ut_ranval(ranctx) & 0xff);
}


/**
 * @brief Unit test for streaming compressors.
 */
static int ut_compressor(rd_kafka_compression_t compression_type) {
#define _UT_CHUNK_SIZE 100 /* Write chunk size */
        const struct {
                const char *name;
                struct {
                        enum { _REPEATING, /**< Compresses well */
                               _ENTROPY,   /**< Compresses not so well */
                               _MIX,       /**< Every other write is
                                            *   repeating or entropy. */
                        } pattern;
                        size_t size;
                        size_t cnt;
                } chunks[4];
                size_t soft_size_limit; /**< Passed to compressor_init() */
                size_t max_size;        /**< Max compressed size */
                double min_ratio;       /**< Minimum compression rate */
                double min_util; /**< Minimum buffer utilization: def 50% */
                rd_kafka_resp_err_t err; /**< Expected error */
        } scenario[] = {
            {
                .name            = "well-compressible",
                .soft_size_limit = 1000000,
                .min_ratio       = 10.0,
                .chunks =
                    {
                        {_REPEATING, _UT_CHUNK_SIZE, 1000000 / _UT_CHUNK_SIZE},
                    },
            },
            {
                .name            = "lousy-compressible",
                .soft_size_limit = 1000000 + 100000,
                .min_ratio       = 0.9,
                .chunks =
                    {
                        {_ENTROPY, _UT_CHUNK_SIZE, 1000000 / _UT_CHUNK_SIZE},
                    },
            },
            {
                .name = "lousy-compressible no limit "
                        "small writes",
                .soft_size_limit = 1000000,
                .min_ratio       = 0.99,
                .chunks =
                    {
                        {_ENTROPY, _UT_CHUNK_SIZE, 1000000 / _UT_CHUNK_SIZE},
                    },
            },
            {
                .name = "lousy-compressible near limit "
                        "small writes",
                .soft_size_limit = 900000,
                .min_ratio       = 0.99,
                .chunks =
                    {
                        {_ENTROPY, _UT_CHUNK_SIZE / 10,
                         1000000 / _UT_CHUNK_SIZE * 10},
                    },
            },
            {
                .name            = "lousy-compressible overflow small writes",
                .soft_size_limit = 50000,
                /* lz4 in particular doesn't like aggressive flushing */
                .min_ratio = 0.9,
                /* lz4's requirement for contiguous writes causes
                 * over-allocation and poor buffer utilization. */
                .min_util = 30.0,
                .chunks =
                    {
                        {_ENTROPY, _UT_CHUNK_SIZE / 10,
                         100000 / _UT_CHUNK_SIZE * 10},
                    },
            },
            {
                .name            = "mix",
                .soft_size_limit = 1000000,
                .min_ratio       = 2.0,
                .chunks =
                    {
                        {_MIX, _UT_CHUNK_SIZE, 1000000 / _UT_CHUNK_SIZE},
                    },
            },
            {
                .name            = "bad half, good half (100KB)",
                .soft_size_limit = 100000,
                .min_ratio       = 2.0,
                .chunks =
                    {
                        {_ENTROPY, _UT_CHUNK_SIZE, 100000 / _UT_CHUNK_SIZE / 2},
                        {_REPEATING, _UT_CHUNK_SIZE,
                         100000 / _UT_CHUNK_SIZE / 2},
                    },
            },
            {
                .name            = "good half, bad half (100KB)",
                .soft_size_limit = 100000,
                .min_ratio       = 2.0,
                .chunks =
                    {
                        {_REPEATING, _UT_CHUNK_SIZE,
                         100000 / _UT_CHUNK_SIZE / 2},
                        {_ENTROPY, _UT_CHUNK_SIZE, 100000 / _UT_CHUNK_SIZE / 2},
                    },
            },
            {
                .name            = "good 90%, bad 10% (100KB)",
                .soft_size_limit = 100000,
                .min_ratio       = 2.0,
                .chunks =
                    {
                        {_REPEATING, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.9)},
                        {_ENTROPY, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.1)},
                    },
            },
            {
                .name            = "bad 90%, good 10% (100KB)",
                .soft_size_limit = 100000,
                .min_ratio       = 1.0,
                .chunks =
                    {
                        {_REPEATING, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.1)},
                        {_ENTROPY, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.9)},
                    },
            },
            {
                .name            = "bad 90%, good 10% (100KB)",
                .soft_size_limit = 100000,
                .min_ratio       = 1.0,
                .chunks =
                    {
                        {_REPEATING, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.1)},
                        {_ENTROPY, _UT_CHUNK_SIZE,
                         (int)(100000 / _UT_CHUNK_SIZE * 0.9)},
                    },
            },
            {NULL},

        };
        int i;
        char tmpbuf[_UT_CHUNK_SIZE];
        char repbuf[_UT_CHUNK_SIZE];
        char rep[] = "RDKA";

        /* Fill well-compressible chunk with a repeated "RDKA" string. */
        rd_assert(_UT_CHUNK_SIZE % sizeof(rep) == 0);
        for (i = 0; i < _UT_CHUNK_SIZE; i += sizeof(rep))
                memcpy(repbuf + i, rep, sizeof(rep));


        for (i = 0; scenario[i].name; i++) {
                rd_kafka_compressor_t compr;
                rd_kafka_error_t *error;
                rd_buf_t outbuf, origbuf;
                size_t inlen = 0;
                size_t outlen;
                double ratio, reported_ratio = 0.12345;
                double buf_util, min_util    = scenario[i].min_util > 0.0
                                                ? scenario[i].min_util
                                                : 50.0;
                int j;
                ut_ranctx ranctx;

                ut_raninit(&ranctx, i);

                RD_UT_SAY("%s: %s (soft_size %" PRIusz ", min_ratio %.2fx)",
                          rd_kafka_compression2str(compression_type),
                          scenario[i].name, scenario[i].soft_size_limit,
                          scenario[i].min_ratio);

                rd_buf_init(&outbuf, 0, 0);
                rd_buf_init(&origbuf, 0, 0);

                error = rd_kafka_compressor_init(
                    &compr, &outbuf, compression_type,
                    rd_kafka_compression_level_translate(
                        compression_type, RD_KAFKA_COMPLEVEL_DEFAULT),
                    scenario[i].soft_size_limit);
                RD_UT_ASSERT(!error, "%s: compressor_init(%s) failed: %s",
                             scenario[i].name,
                             rd_kafka_compression2str(compression_type),
                             rd_kafka_error_string(error));

                for (j = 0; scenario[i].chunks[j].cnt; j++) {
                        size_t size = scenario[i].chunks[j].size;
                        size_t k;

                        rd_assert(size <= sizeof(tmpbuf));

                        for (k = 0; k < scenario[i].chunks[j].cnt; k++) {
                                char *buf   = repbuf; /* default: _REPEATING */
                                int pattern = scenario[i].chunks[j].pattern;

                                if (pattern == _MIX)
                                        pattern =
                                            (k & 1) ? _REPEATING : _ENTROPY;

                                if (pattern == _ENTROPY) {
                                        ut_gen_entropy(&ranctx, tmpbuf, size);
                                        buf = tmpbuf;
                                }

                                error = (rd_kafka_error_t *)
                                    rd_kafka_compressor_write(&compr, &outbuf,
                                                              buf, size);

                                RD_UT_ASSERT(
                                    !error,
                                    "%s: "
                                    "compressor_write(%s) failed: %s",
                                    scenario[i].name,
                                    rd_kafka_compression2str(compression_type),
                                    rd_kafka_error_string(error));

                                /* Maintain an uncompressed verbatim copy
                                 * of all writes that we use to compare with the
                                 * decompressed contents. */
                                rd_buf_write(&origbuf, buf, size);

                                inlen += size;
                        }
                }

                error =
                    rd_kafka_compressor_close(&compr, &outbuf, &reported_ratio);

                outlen   = rd_buf_len(&outbuf);
                ratio    = (double)inlen / (double)outlen;
                buf_util = rd_buf_utilization(&outbuf) * 100.0;
                if (buf_util < min_util)
                        rd_buf_dump(&outbuf, 0);

                RD_UT_SAY(
                    "%s: %s compression ratio %.2fx (min_ratio %.2fx), "
                    "outlen %" PRIusz " (soft_size %" PRIusz
                    ", "
                    "%.2f%% buf utilization): %s",
                    rd_kafka_compression2str(compression_type),
                    scenario[i].name, ratio, scenario[i].min_ratio, outlen,
                    scenario[i].soft_size_limit, buf_util,
                    error ? rd_kafka_error_string(error) : "success");

                RD_UT_ASSERT(rd_kafka_error_code(error) == scenario[i].err,
                             "%s: %s: compressor_close %s: "
                             "expected %s, got %s: %s",
                             rd_kafka_compression2str(compression_type),
                             scenario[i].name, error ? "failed" : "succeeded",
                             rd_kafka_err2name(scenario[i].err),
                             rd_kafka_error_name(error),
                             rd_kafka_error_string(error));

                RD_UT_ASSERT(outlen > 0, "no compression output");

                if (!error) {
                        rd_buf_t decomprbuf;
                        rd_slice_t comprslice, decomprslice, origslice;
                        uint32_t orig_crc32c, decompr_crc32c;
                        size_t max_size =
                            (size_t)((double)inlen / scenario[i].min_ratio);

                        RD_UT_ASSERT(ratio >= scenario[i].min_ratio,
                                     "%s: %s: compression ratio %.2f < "
                                     "min_ratio %.2f",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name, ratio,
                                     scenario[i].min_ratio);
                        RD_UT_ASSERT(outlen <= max_size,
                                     "%s: %s: compressed output size %" PRIusz
                                     " > max_size %" PRIusz,
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name, outlen, max_size);

                        /* Make sure reported ratio matches actual ratio. */
                        RD_UT_ASSERT(ratio * 0.99 < reported_ratio &&
                                         ratio * 1.01 > reported_ratio,
                                     "%s: %s: reported ratio %.2f differs more "
                                     "than 1%% from actual ratio %.2f",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name, reported_ratio, ratio);

                        rd_buf_init(&decomprbuf, 0, 0);
                        rd_slice_init_full(&comprslice, &outbuf);

                        error = rd_kafka_decompress(compression_type,
                                                    &comprslice, &decomprbuf);
                        RD_UT_ASSERT(!error, "%s: %s: decompression failed: %s",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name,
                                     rd_kafka_error_string(error));

                        RD_UT_ASSERT(rd_buf_len(&origbuf) ==
                                         rd_buf_len(&decomprbuf),
                                     "%s: %s: original %" PRIusz
                                     " and "
                                     "decompressed %" PRIusz " lengths differ",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name, rd_buf_len(&origbuf),
                                     rd_buf_len(&decomprbuf));
                        RD_UT_ASSERT(rd_slice_remains(&comprslice) == 0,
                                     "%s: %s: compressed slice still has "
                                     "%" PRIusz " bytes remaining",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name,
                                     rd_slice_remains(&comprslice));

                        /* Calculate and compare CRC32c for original
                         * and decompressed buffers. */
                        rd_slice_init_full(&origslice, &origbuf);
                        orig_crc32c = rd_slice_crc32c(&origslice);

                        rd_slice_init_full(&decomprslice, &decomprbuf);
                        decompr_crc32c = rd_slice_crc32c(&decomprslice);

                        RD_UT_ASSERT(
                            orig_crc32c == decompr_crc32c,
                            "%s: %s: CRC32c mismatch: "
                            "orig %" PRIx32 " != decompressed %" PRIx32,
                            rd_kafka_compression2str(compression_type),
                            scenario[i].name, orig_crc32c, decompr_crc32c);

                        rd_buf_destroy(&decomprbuf);

                        /* Verify that the buffer is not under-utilized by
                         * too many large segments with small writes. */
                        RD_UT_ASSERT(buf_util >= min_util ||
                                         rd_buf_segment_cnt(&outbuf) < 4,
                                     "%s: %s: buffer utilization %.2f%% < "
                                     "%.2f%% (%" PRIusz "/%" PRIusz
                                     " bytes used)",
                                     rd_kafka_compression2str(compression_type),
                                     scenario[i].name, buf_util, min_util,
                                     rd_buf_len(&outbuf), rd_buf_size(&outbuf));
                }

                rd_buf_destroy(&outbuf);

                rd_buf_destroy(&origbuf);
        }

        RD_UT_PASS();
}


int unittest_compression(void) {
        rd_kafka_compression_t compression_type;
        int fails = 0;

        for (compression_type = RD_KAFKA_COMPRESSION_NONE;
             compression_type < RD_KAFKA_COMPRESSION_NUM; compression_type++) {
                if (rd_kafka_compressor_can_stream(compression_type))
                        fails += ut_compressor(compression_type);
        }

        return fails;
}
