/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2023 Confluent Inc.
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
#include "rdbase64.h"

/**
 * @brief Base64 encode binary input \p in, and write base64-encoded string
 *        and it's size to \p out
 *
 * out->ptr must be freed after use.
 */
void rd_base64_encode(const rd_chariov_t *in, rd_chariov_t *out) {
        size_t max_len;

        max_len  = (((in->size + 2) / 3) * 4) + 1;
        out->ptr = rd_malloc(max_len);
        rd_assert(out->ptr);

        out->size = EVP_EncodeBlock((uint8_t *)out->ptr, (uint8_t *)in->ptr,
                                    (int)in->size);

        rd_assert(out->size <= max_len);
        out->ptr[out->size] = 0;
}


/**
 * @brief Base64 encode binary input \p in
 * @returns a newly allocated, base64-encoded string
 *
 * Returned string must be freed after use
 */
char *rd_base64_encode_str(const rd_chariov_t *in) {
        rd_chariov_t out;
        rd_base64_encode(in, &out);
        return out.ptr;
}
