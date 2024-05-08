/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
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

#include "rdkafka_uuid.h"

#include "rdkafka.h" /* Required to RD_EXPORT public API */
#include "rdendian.h"
#include "rdstring.h"
#include "rdbase64.h"
#include "rdrand.h"
#include "rdmap.h"

int rd_kafka_Uuid_cmp(rd_kafka_Uuid_t a, rd_kafka_Uuid_t b) {
        if (a.most_significant_bits < b.most_significant_bits)
                return -1;
        if (a.most_significant_bits > b.most_significant_bits)
                return 1;
        if (a.least_significant_bits < b.least_significant_bits)
                return -1;
        if (a.least_significant_bits > b.least_significant_bits)
                return 1;
        return 0;
}

/**
 * Creates a new UUID.
 *
 * @return A newly allocated UUID.
 */
rd_kafka_Uuid_t *rd_kafka_Uuid_new(int64_t most_significant_bits,
                                   int64_t least_significant_bits) {
        rd_kafka_Uuid_t *uuid        = rd_calloc(1, sizeof(rd_kafka_Uuid_t));
        uuid->most_significant_bits  = most_significant_bits;
        uuid->least_significant_bits = least_significant_bits;
        return uuid;
}

/**
 * Returns a newly allocated copy of the given UUID.
 *
 * @param uuid UUID to copy.
 * @return Copy of the provided UUID.
 *
 * @remark Dynamically allocated. Deallocate (free) after use.
 */
rd_kafka_Uuid_t *rd_kafka_Uuid_copy(const rd_kafka_Uuid_t *uuid) {
        rd_kafka_Uuid_t *copy_uuid = rd_kafka_Uuid_new(
            uuid->most_significant_bits, uuid->least_significant_bits);
        if (*uuid->base64str)
                memcpy(copy_uuid->base64str, uuid->base64str, 23);
        return copy_uuid;
}

/**
 * Returns a new non cryptographically secure UUIDv4 (random).
 *
 * @return A UUIDv4.
 *
 * @remark Must be freed after use using rd_kafka_Uuid_destroy().
 */
rd_kafka_Uuid_t rd_kafka_Uuid_random() {
        int i;
        unsigned char rand_values_bytes[16] = {0};
        uint64_t *rand_values_uint64        = (uint64_t *)rand_values_bytes;
        unsigned char *rand_values_app;
        rd_kafka_Uuid_t ret = RD_KAFKA_UUID_ZERO;
        for (i = 0; i < 16; i += 2) {
                uint16_t rand_uint16 = (uint16_t)rd_jitter(0, INT16_MAX - 1);
                /* No need to convert endianess here because it's still only
                 * a random value. */
                rand_values_app = (unsigned char *)&rand_uint16;
                rand_values_bytes[i] |= rand_values_app[0];
                rand_values_bytes[i + 1] |= rand_values_app[1];
        }

        rand_values_bytes[6] &= 0x0f; /* clear version */
        rand_values_bytes[6] |= 0x40; /* version 4 */
        rand_values_bytes[8] &= 0x3f; /* clear variant */
        rand_values_bytes[8] |= 0x80; /* IETF variant */

        ret.most_significant_bits  = be64toh(rand_values_uint64[0]);
        ret.least_significant_bits = be64toh(rand_values_uint64[1]);
        return ret;
}

/**
 * @brief Destroy the provided uuid.
 *
 * @param uuid UUID
 */
void rd_kafka_Uuid_destroy(rd_kafka_Uuid_t *uuid) {
        rd_free(uuid);
}

/**
 * @brief Computes canonical encoding for the given uuid string.
 *        Mainly useful for testing.
 *
 * @param uuid UUID for which canonical encoding is required.
 *
 * @return canonical encoded string for the given UUID.
 *
 * @remark  Must be freed after use.
 */
const char *rd_kafka_Uuid_str(const rd_kafka_Uuid_t *uuid) {
        int i, j;
        unsigned char bytes[16];
        char *ret = rd_calloc(37, sizeof(*ret));

        for (i = 0; i < 8; i++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
                j = 7 - i;
#elif __BYTE_ORDER == __BIG_ENDIAN
                j = i;
#endif
                bytes[i]     = (uuid->most_significant_bits >> (8 * j)) & 0xFF;
                bytes[8 + i] = (uuid->least_significant_bits >> (8 * j)) & 0xFF;
        }

        rd_snprintf(ret, 37,
                    "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%"
                    "02x%02x%02x",
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                    bytes[6], bytes[7], bytes[8], bytes[9], bytes[10],
                    bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
        return ret;
}

const char *rd_kafka_Uuid_base64str(const rd_kafka_Uuid_t *uuid) {
        if (*uuid->base64str)
                return uuid->base64str;

        rd_chariov_t in_base64;
        char *out_base64_str;
        char *uuid_bytes;
        uint64_t input_uuid[2];

        input_uuid[0]  = htobe64(uuid->most_significant_bits);
        input_uuid[1]  = htobe64(uuid->least_significant_bits);
        uuid_bytes     = (char *)input_uuid;
        in_base64.ptr  = uuid_bytes;
        in_base64.size = sizeof(uuid->most_significant_bits) +
                         sizeof(uuid->least_significant_bits);

        out_base64_str = rd_base64_encode_str(&in_base64);
        if (!out_base64_str)
                return NULL;

        rd_strlcpy((char *)uuid->base64str, out_base64_str,
                   23 /* Removing extra ('=') padding */);
        rd_free(out_base64_str);
        return uuid->base64str;
}

unsigned int rd_kafka_Uuid_hash(const rd_kafka_Uuid_t *uuid) {
        unsigned char bytes[16];
        memcpy(bytes, &uuid->most_significant_bits, 8);
        memcpy(&bytes[8], &uuid->least_significant_bits, 8);
        return rd_bytes_hash(bytes, 16);
}

unsigned int rd_kafka_Uuid_map_hash(const void *key) {
        return rd_kafka_Uuid_hash(key);
}

int64_t rd_kafka_Uuid_least_significant_bits(const rd_kafka_Uuid_t *uuid) {
        return uuid->least_significant_bits;
}


int64_t rd_kafka_Uuid_most_significant_bits(const rd_kafka_Uuid_t *uuid) {
        return uuid->most_significant_bits;
}

/**
 * @brief UUID copier for rd_list_copy()
 */
void *rd_list_Uuid_copy(const void *elem, void *opaque) {
        return (void *)rd_kafka_Uuid_copy((rd_kafka_Uuid_t *)elem);
}

void rd_list_Uuid_destroy(void *uuid) {
        rd_kafka_Uuid_destroy((rd_kafka_Uuid_t *)uuid);
}

int rd_list_Uuid_cmp(const void *uuid1, const void *uuid2) {
        return rd_kafka_Uuid_cmp(*((rd_kafka_Uuid_t *)uuid1),
                                 *((rd_kafka_Uuid_t *)uuid2));
}
