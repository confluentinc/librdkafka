/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2018 Magnus Edenhill
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

#include <zstd.h>
#include <zstd_errors.h>

rd_kafka_resp_err_t
rd_kafka_zstd_decompress (rd_kafka_broker_t *rkb,
                         char *inbuf, size_t inlen,
                         void **outbuf, size_t *outlenp) {

        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        char *decompressed = NULL;

        size_t out_bufsize = ZSTD_getFrameContentSize(inbuf, inlen);
        switch (out_bufsize) {
        case ZSTD_CONTENTSIZE_UNKNOWN:
                /* Decompressed size cannot be determined, make a guess */
                out_bufsize = inlen * 2;
                break;
        case ZSTD_CONTENTSIZE_ERROR:
                /* Error calculating frame content size */
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to begin ZSTD decompression "
                           "(out buffer is %zu bytes): %s",
                           out_bufsize, "Error in determining frame size");
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        default:
                break;
        }

        /* Multiple passes to allow resizing output buffer if it's too small */
        decompressed = rd_malloc(out_bufsize);
        if (!decompressed) {
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to allocate output buffer "
                           "(%zu bytes): %s",
                           out_bufsize, rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        for (size_t i = 0; i < 3; ++i) {
                /* Attempt to decompress */
                size_t ret = ZSTD_decompress(decompressed, out_bufsize, inbuf, inlen);
                if (!ZSTD_isError(ret)) {
                        *outlenp = ret;
                        *outbuf = decompressed;
                        goto done;
                }

                /* Check if the destination size is too small */
                if (ZSTD_getErrorCode(ret) == ZSTD_error_dstSize_tooSmall) {
                        char *newp;

                        /* Grow exponentially with some factor > 1 (using 1.75)
                         * for amortized O(1) copying */
                        out_bufsize += out_bufsize * 3 / 4;

                        rd_atomic64_add(&rkb->rkb_c.zbuf_grow, 1);
                        newp = rd_realloc(decompressed, out_bufsize);
                        if (!newp) {
                                rd_rkb_dbg(rkb, MSG, "ZSTD",
                                           "Unable to allocate output buffer "
                                           "(%zu bytes): %s",
                                           out_bufsize, rd_strerror(errno));
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                goto done;
                        }
                        decompressed = newp;
                } else {
                        /* Fail on any other error */
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "Unable to begin ZSTD decompression "
                                   "(out buffer is %zu bytes): %s",
                                   out_bufsize, ZSTD_getErrorName(ret));
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

done:

        if (err) {
                rd_free(decompressed);
        }

        return err;
}

rd_kafka_resp_err_t
rd_kafka_zstd_compress (rd_kafka_broker_t *rkb, int comp_level,
                       rd_slice_t *slice, void **outbuf, size_t *outlenp) {
        ZSTD_CStream* cctx;
        size_t r;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len = rd_slice_remains(slice);
        ZSTD_outBuffer out;
        ZSTD_inBuffer in;

        *outbuf = NULL;
        out.pos = 0;
        out.size = ZSTD_compressBound(len);
        out.dst = rd_malloc(out.size);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%"PRIusz" bytes): %s",
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

        r = ZSTD_initCStream(cctx, comp_level);
        if (ZSTD_isError(r)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to begin ZSTD compression "
                           "(out buffer is %"PRIusz" bytes): %s",
                           out.size, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        while ((in.size = rd_slice_reader(slice, &in.src))) {
                in.pos = 0;
                r = ZSTD_compressStream(cctx, &out, &in);
                if (unlikely(ZSTD_isError(r))) {
                        rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                                   "ZSTD compression failed "
                                   "(at of %"PRIusz" bytes, with "
                                   "%"PRIusz" bytes remaining in out buffer): "
                                   "%s",
                                   in.size, out.size - out.pos,
                                   ZSTD_getErrorName(r));
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }

                /* No space left in output buffer, but input isn't fully consumed */
                if (in.pos < in.size) {
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

        if (rd_slice_remains(slice) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %"PRIusz" bytes: %s",
                           len, "Unexpected trailing data");
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        r = ZSTD_endStream(cctx, &out);
        if (unlikely(ZSTD_isError(r) || r > 0)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %"PRIusz" bytes: %s",
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
