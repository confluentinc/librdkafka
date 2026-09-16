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

#ifndef _RDZSTD_H_
#define _RDZSTD_H_

/**
 * @brief Decompress ZSTD framed data.
 *
 * The output buffer is grown as needed while decompressing, up to
 * \c receive.message.max.bytes. Each growth increments the broker's
 * \c zbuf_grow counter.
 *
 * @returns allocated buffer in \p *outbuf, length in \p *outlenp.
 *
 * @locality any thread, but \p rkb 's cached decompression context is only
 *           used when called from its broker thread.
 */
rd_kafka_resp_err_t rd_kafka_zstd_decompress(rd_kafka_broker_t *rkb,
                                             char *inbuf,
                                             size_t inlen,
                                             void **outbuf,
                                             size_t *outlenp);

/**
 * @brief Free the broker's cached ZSTD decompression context, if any.
 *
 * @locality broker thread
 */
void rd_kafka_zstd_dctx_destroy(rd_kafka_broker_t *rkb);

/**
 * Allocate space for \p *outbuf and compress all \p iovlen buffers in \p iov.
 * @param MessageSetSize indicates (at least) full uncompressed data size,
 *                       possibly including MessageSet fields that will not
 *                       be compressed.
 *
 * @returns allocated buffer in \p *outbuf, length in \p *outlenp.
 */
rd_kafka_resp_err_t rd_kafka_zstd_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           void **outbuf,
                                           size_t *outlenp);

#endif /* _RDZSTD_H_ */
