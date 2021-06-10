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

#ifndef _RDKAFKA_COMPRESSION_H_
#define _RDKAFKA_COMPRESSION_H_


/**
 * MessageSet compression codecs
 */
typedef enum {
        RD_KAFKA_COMPRESSION_NONE,
        RD_KAFKA_COMPRESSION_GZIP = RD_KAFKA_MSG_ATTR_GZIP,
        RD_KAFKA_COMPRESSION_SNAPPY = RD_KAFKA_MSG_ATTR_SNAPPY,
        RD_KAFKA_COMPRESSION_LZ4 = RD_KAFKA_MSG_ATTR_LZ4,
        RD_KAFKA_COMPRESSION_ZSTD = RD_KAFKA_MSG_ATTR_ZSTD,
        RD_KAFKA_COMPRESSION_INHERIT, /* Inherit setting from global conf */
        RD_KAFKA_COMPRESSION_NUM
} rd_kafka_compression_t;

static RD_INLINE RD_UNUSED const char *
rd_kafka_compression2str (rd_kafka_compression_t compr) {
        static const char *names[RD_KAFKA_COMPRESSION_NUM] = {
                [RD_KAFKA_COMPRESSION_NONE] = "none",
                [RD_KAFKA_COMPRESSION_GZIP] = "gzip",
                [RD_KAFKA_COMPRESSION_SNAPPY] = "snappy",
                [RD_KAFKA_COMPRESSION_LZ4] = "lz4",
                [RD_KAFKA_COMPRESSION_ZSTD] = "zstd",
                [RD_KAFKA_COMPRESSION_INHERIT] = "inherit"
        };
        static RD_TLS char ret[32];

        if ((int)compr < 0 || compr >= RD_KAFKA_COMPRESSION_NUM) {
                rd_snprintf(ret, sizeof(ret),
                            "codec0x%x?", (int)compr);
                return ret;
        }

        return names[compr];
}

struct rd_kafka_compressor_s;
typedef struct rd_kafka_compressor_s rd_kafka_compressor_t;


void rd_kafka_compressor_free (rd_kafka_compressor_t *compr);
rd_kafka_compressor_t *rd_kafka_compressor_alloc (void);

rd_kafka_error_t *
rd_kafka_compressor_close (rd_kafka_compressor_t *compr,
                           rd_buf_t *output_rbuf,
                           double *ratiop);

const rd_kafka_error_t *
rd_kafka_compressor_write (rd_kafka_compressor_t *compr,
                           rd_buf_t *output_rbuf,
                           const void *buf, size_t size);

rd_kafka_error_t *
rd_kafka_compressor_init (rd_kafka_compressor_t *compr,
                          rd_buf_t *output_rbuf,
                          rd_kafka_compression_t compression_type,
                          int compression_level,
                          size_t soft_size_limit);

rd_bool_t rd_kafka_compressor_failed (const rd_kafka_compressor_t *compr);

/**
 * @returns true if the compression type supports streaming compression
 * with the given limits.
 */
static RD_UNUSED rd_bool_t
rd_kafka_compressor_can_stream (rd_kafka_compression_t compression_type) {
        static const rd_bool_t can_stream[RD_KAFKA_COMPRESSION_NUM] = {
#if WITH_ZLIB
                [RD_KAFKA_COMPRESSION_GZIP] = rd_true,
#endif
#if WITH_SNAPPY
                [RD_KAFKA_COMPRESSION_SNAPPY] = rd_false,
#endif
                [RD_KAFKA_COMPRESSION_LZ4] = rd_true,
#if WITH_ZSTD
                [RD_KAFKA_COMPRESSION_ZSTD] = rd_true,
#endif
        };

        return can_stream[compression_type];
}

int
rd_kafka_compression_level_translate (rd_kafka_compression_t compression_type,
                                      int compression_level);


#endif /* _RDKAFKA_COMPRESSION_H_ */
