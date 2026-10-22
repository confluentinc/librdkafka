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

/**
 * @brief Inflate \p compressed, which must hold one complete gzip or zlib
 *        stream, into \p out, or only count the inflated length if \p out
 *        is NULL.
 *
 * @param max_len the size of \p out, or when counting the most the stream
 *                may inflate to.
 * @param lenp the inflated length is returned here.
 *
 * @returns true on success, false if the stream is corrupt, ends early,
 *          or inflates to more than \p max_len bytes.
 */
static rd_bool_t rd_gz_inflate(const void *compressed,
                               size_t compressed_len,
                               char *out,
                               size_t max_len,
                               size_t *lenp) {
        z_stream strm = RD_ZERO_INIT;
        char buf[512];
        int r;

        if (compressed_len > UINT_MAX || inflateInit2(&strm, 15 + 32) != Z_OK)
                return rd_false;

        strm.next_in  = (void *)compressed;
        strm.avail_in = (uInt)compressed_len;

        do {
                if (out) {
                        /* Inflate into what is left of the output buffer */
                        strm.next_out = (unsigned char *)out + strm.total_out;
                        strm.avail_out =
                            (uInt)RD_MIN(max_len - strm.total_out, UINT_MAX);
                } else {
                        /* Only count: inflate into a scratch buffer */
                        strm.next_out  = (unsigned char *)buf;
                        strm.avail_out = sizeof(buf);
                }

                /* inflate() returns Z_OK only when it made progress, and
                 * Z_BUF_ERROR when it can make none, such as when the input
                 * ends before the stream does or the output buffer is full:
                 * so this loop ends. */
                r = inflate(&strm, Z_NO_FLUSH);
        } while (r == Z_OK && strm.total_out <= max_len);

        *lenp = strm.total_out;
        inflateEnd(&strm);

        return r == Z_STREAM_END && *lenp <= max_len;
}


void *rd_gz_decompress(const void *compressed,
                       size_t compressed_len,
                       size_t max_len,
                       size_t *decompressed_lenp) {
        char *decompressed;
        size_t len;

        /* First pass: calculate the decompressed size. */
        if (!rd_gz_inflate(compressed, compressed_len, NULL, max_len, &len))
                return NULL;

        /* For convenience of the caller we nul-terminate
         * the buffer. If it happens to be a string there
         * is no need for extra copies. */
        decompressed      = rd_malloc(len + 1);
        decompressed[len] = '\0';

        /* Second pass: perform the actual decompression. */
        if (!rd_gz_inflate(compressed, compressed_len, decompressed, len,
                           &len)) {
                rd_free(decompressed);
                return NULL;
        }

        *decompressed_lenp = len;

        return decompressed;
}
