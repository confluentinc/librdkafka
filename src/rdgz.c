/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
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
#include "rdgz.h"

#include <zlib.h>


#define RD_GZ_CHUNK 262144

void *rd_gz_decompress_limited(const void *compressed,
                               int compressed_len,
                               uint64_t *decompressed_lenp,
                               uint64_t max_decompressed_len) {
        z_stream strm      = RD_ZERO_INIT;
        char *decompressed = NULL;
        size_t decompressed_len;
        size_t decompressed_size;
        size_t max_size;
        int r;

        if (compressed_len < 0)
                return NULL;

        /* Leave room for the nul terminator and clamp the limit to the
         * addressable range on this platform. */
        max_size = (size_t)RD_MIN(max_decompressed_len, (uint64_t)SIZE_MAX - 1);
        if (max_size == 0)
                return NULL;

        /* A caller-supplied length is an initial allocation hint, not a
         * promise that the stream will fit. */
        if (*decompressed_lenp != 0LLU) {
                if (*decompressed_lenp > max_size)
                        return NULL;
                decompressed_size = (size_t)*decompressed_lenp;
        } else {
                decompressed_size = RD_MIN((size_t)RD_GZ_CHUNK, max_size);
        }

        decompressed = rd_malloc(decompressed_size + 1);

        if ((r = inflateInit2(&strm, 15 + 32)) != Z_OK)
                goto fail;

        strm.next_in     = (void *)compressed;
        strm.avail_in    = compressed_len;
        decompressed_len = 0;

        for (;;) {
                size_t avail_out;
                size_t produced;

                if (decompressed_len == decompressed_size) {
                        size_t new_size;

                        if (decompressed_size >= max_size)
                                goto fail_inflate;

                        new_size     = decompressed_size > max_size / 2
                                           ? max_size
                                           : decompressed_size * 2;
                        decompressed = rd_realloc(decompressed, new_size + 1);
                        decompressed_size = new_size;
                }

                avail_out = RD_MIN(decompressed_size - decompressed_len,
                                   (size_t)UINT_MAX);
                strm.next_out =
                    (unsigned char *)decompressed + decompressed_len;
                strm.avail_out = (uInt)avail_out;

                r        = inflate(&strm, Z_NO_FLUSH);
                produced = avail_out - strm.avail_out;
                decompressed_len += produced;

                if (r == Z_STREAM_END)
                        break;
                if (r != Z_OK || (strm.avail_in == 0 && strm.avail_out != 0))
                        goto fail_inflate;
        }

        inflateEnd(&strm);
        decompressed[decompressed_len] = '\0';
        *decompressed_lenp             = decompressed_len;
        return decompressed;

fail_inflate:
        inflateEnd(&strm);
fail:
        rd_free(decompressed);
        return NULL;
}

void *rd_gz_decompress(const void *compressed,
                       int compressed_len,
                       uint64_t *decompressed_lenp) {
        return rd_gz_decompress_limited(compressed, compressed_len,
                                        decompressed_lenp, UINT64_MAX);
}
