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
#include "rdunittest.h"

#include <zlib.h>


#define RD_GZ_CHUNK 262144

/**
 * @returns the decompressed size recorded in a gzip stream's trailer, or 0
 *          when the input is not gzip.
 *
 * The trailer ends with the size modulo 2^32 (RFC 1952). It is read before
 * zlib has validated anything, and in a multi-member stream it gives the size
 * of the last member only, so the caller only starts its buffer from it.
 */
static size_t rd_gz_trailer_size(const void *compressed, int compressed_len) {
        const unsigned char *p = compressed;

        /* A 10-byte header and an 8-byte trailer, and the gzip magic:
         * zlib-wrapped input ends with a checksum instead. */
        if (compressed_len < 18 || p[0] != 0x1f || p[1] != 0x8b)
                return 0;

        p += compressed_len - 4;
        return (size_t)p[0] | (size_t)p[1] << 8 | (size_t)p[2] << 16 |
               (size_t)p[3] << 24;
}

void *rd_gz_decompress(const void *compressed,
                       int compressed_len,
                       uint64_t *decompressed_lenp) {
        z_stream strm = RD_ZERO_INIT;
        char *decompressed;
        size_t size;
        int r;

        if (compressed_len <= 0)
                return NULL;

        /* Inflate once into a buffer that grows as needed.
         *
         * The buffer starts from the caller's size hint, else from the size
         * in the gzip trailer plus a spare byte, so that an exact size does
         * not look full. The trailer is not validated yet, so its size is
         * kept between 1x and 32x the compressed size. */
        size = (size_t)*decompressed_lenp;
        if (size == 0)
                size = RD_MIN(
                    RD_MAX(rd_gz_trailer_size(compressed, compressed_len) + 1,
                           (size_t)compressed_len),
                    (size_t)compressed_len * 32);

        if (inflateInit2(&strm, 15 + 32) != Z_OK)
                return NULL;

        /* rd_malloc() and rd_realloc() abort on failure: no need for NULL
         * check. */
        decompressed = rd_malloc(size + 1);

        strm.next_in  = (void *)compressed;
        strm.avail_in = compressed_len;

        while (rd_true) {
                strm.next_out  = (unsigned char *)decompressed + strm.total_out;
                strm.avail_out = (uInt)RD_MIN(size - strm.total_out, UINT_MAX);

                r = inflate(&strm, Z_NO_FLUSH);
                if (r == Z_STREAM_END)
                        break;
                if (r != Z_OK && r != Z_BUF_ERROR)
                        goto fail;

                if (strm.avail_out == 0) {
                        /* Output buffer full: grow it by the output still to
                         * come, estimated from the ratio so far, and by at
                         * least the compressed size. total_in is not 0 once
                         * zlib has produced output; the RD_MAX only spares the
                         * division that reasoning. avail_in is below 2^31, so
                         * the product fits 64 bits for outputs below 8 GiB. */
                        uint64_t still_to_come = (uint64_t)strm.avail_in *
                                                 strm.total_out /
                                                 RD_MAX(strm.total_in, 1);
                        size += RD_MAX((size_t)compressed_len,
                                       (size_t)still_to_come);
                        decompressed = rd_realloc(decompressed, size + 1);
                } else if (strm.avail_in == 0) {
                        /* Truncated input. */
                        goto fail;
                }
        }

        *decompressed_lenp = strm.total_out;
        inflateEnd(&strm);

        /* Give back the unused part of the guess. */
        decompressed = rd_realloc(decompressed, (size_t)*decompressed_lenp + 1);

        /* For convenience of the caller we nul-terminate the buffer. If it
         * happens to be a string there is no need for extra copies. */
        decompressed[*decompressed_lenp] = '\0';

        return decompressed;

fail:
        inflateEnd(&strm);
        rd_free(decompressed);
        return NULL;
}


/**
 * @brief Compresses \p src in gzip format, for the unit tests.
 */
static void *ut_gz_compress(const void *src, size_t len, size_t *outlenp) {
        z_stream strm = RD_ZERO_INIT;
        size_t size;
        char *out;

        if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                         Z_DEFAULT_STRATEGY) != Z_OK)
                return NULL;
        size = deflateBound(&strm, (uLong)len);
        out  = rd_malloc(size);

        strm.next_in   = (void *)src;
        strm.avail_in  = (uInt)len;
        strm.next_out  = (unsigned char *)out;
        strm.avail_out = (uInt)size;
        if (deflate(&strm, Z_FINISH) != Z_STREAM_END) {
                deflateEnd(&strm);
                rd_free(out);
                return NULL;
        }
        *outlenp = strm.total_out;
        deflateEnd(&strm);
        return out;
}

int unittest_gz(void) {
        /* Data sizes chosen to need no, one and many output buffer
         * growths, for both highly and barely compressible data. */
        static const size_t sizes[] = {0, 1, 1000, 100000, 3000000};
        size_t i, j;

        for (i = 0; i < RD_ARRAYSIZE(sizes); i++) {
                size_t len = sizes[i];
                int compressible;

                /* 0: random, 1: text, 2: zeros, near deflate's maximum
                 * ratio, far beyond the 32x bound on the trailer's size. */
                for (compressible = 0; compressible <= 2; compressible++) {
                        char *src = rd_malloc(len + 1);
                        char *gz, *out;
                        size_t gzlen;
                        uint64_t outlen;
                        uint32_t seed = 1234;

                        for (j = 0; j < len; j++) {
                                seed   = seed * 1103515245 + 12345;
                                src[j] = compressible == 2 ? 0
                                         : compressible == 1
                                             ? "log line "[j % 9]
                                             : (char)(seed >> 16);
                        }

                        gz = ut_gz_compress(src, len, &gzlen);
                        RD_UT_ASSERT(gz != NULL, "compression failed");

                        /* Without, then with, a size hint. */
                        outlen = 0;
                        out    = rd_gz_decompress(gz, (int)gzlen, &outlen);
                        RD_UT_ASSERT(
                            out != NULL,
                            "decompression of %" PRIusz " bytes failed", len);
                        RD_UT_ASSERT(outlen == len,
                                     "expected %" PRIusz " bytes, got %" PRIu64,
                                     len, outlen);
                        RD_UT_ASSERT(!memcmp(out, src, len),
                                     "decompressed data differs");
                        RD_UT_ASSERT(out[len] == '\0',
                                     "output is not nul-terminated");
                        rd_free(out);

                        outlen = len;
                        out    = rd_gz_decompress(gz, (int)gzlen, &outlen);
                        RD_UT_ASSERT(out && outlen == len &&
                                         !memcmp(out, src, len),
                                     "decompression with a size hint failed");
                        rd_free(out);

                        /* Truncated and corrupt input must fail. Not for
                         * zeros: a corrupted copy distance there still copies
                         * zeros, so the output, and its checksum, are
                         * unchanged. */
                        if (gzlen > 10 && compressible != 2) {
                                outlen = 0;
                                RD_UT_ASSERT(!rd_gz_decompress(
                                                 gz, (int)gzlen - 10, &outlen),
                                             "truncated input was accepted");
                                gz[gzlen / 2] ^= 0x55;
                                gz[gzlen / 2 + 1] ^= 0x55;
                                outlen = 0;
                                out = rd_gz_decompress(gz, (int)gzlen, &outlen);
                                RD_UT_ASSERT(!out || outlen != len ||
                                                 memcmp(out, src, len),
                                             "corrupt input decoded as valid");
                                rd_free(out);
                        }

                        rd_free(gz);
                        rd_free(src);
                }
        }

        /* Two members: only the first is returned, as before, although the
         * trailer gives the size of the second. A forged trailer size fails
         * zlib's length check. */
        {
                char first[100000], second[10], *gz1, *gz2, *both, *out;
                size_t gz1len, gz2len;
                uint64_t outlen = 0;

                memset(first, 'a', sizeof(first));
                memset(second, 'b', sizeof(second));
                gz1  = ut_gz_compress(first, sizeof(first), &gz1len);
                gz2  = ut_gz_compress(second, sizeof(second), &gz2len);
                both = rd_malloc(gz1len + gz2len);
                memcpy(both, gz1, gz1len);
                memcpy(both + gz1len, gz2, gz2len);

                out = rd_gz_decompress(both, (int)(gz1len + gz2len), &outlen);
                RD_UT_ASSERT(out && outlen == sizeof(first) &&
                                 !memcmp(out, first, sizeof(first)),
                             "a two-member stream did not decode to its "
                             "first member");
                rd_free(out);

                memset(gz1 + gz1len - 4, 0xff, 4);
                outlen = 0;
                RD_UT_ASSERT(!rd_gz_decompress(gz1, (int)gz1len, &outlen),
                             "a forged trailer size was accepted");

                rd_free(both);
                rd_free(gz2);
                rd_free(gz1);
        }

        RD_UT_PASS();
}
