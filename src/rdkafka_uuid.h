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

#ifndef _RDKAFKA_UUID_H_
#define _RDKAFKA_UUID_H_

#include "rd.h"

/**
 * @struct Struct representing UUID protocol primitive type.
 */
typedef struct rd_kafka_Uuid_s {
        int64_t
            most_significant_bits; /**< Most significant 64 bits for the UUID */
        int64_t least_significant_bits; /**< Least significant 64 bits for the
                                           UUID */
        char base64str[23]; /**< base64 encoding for the uuid. By default, it is
                               lazy loaded. Use function
                               `rd_kafka_Uuid_base64str()` as a getter for this
                               field. */
} rd_kafka_Uuid_t;

#define RD_KAFKA_UUID_ZERO                                                     \
        (rd_kafka_Uuid_t) {                                                    \
                0, 0, ""                                                       \
        }

#define RD_KAFKA_UUID_IS_ZERO(uuid)                                            \
        (!rd_kafka_Uuid_cmp(uuid, RD_KAFKA_UUID_ZERO))

#define RD_KAFKA_UUID_METADATA_TOPIC_ID                                        \
        (rd_kafka_Uuid_t) {                                                    \
                0, 1, ""                                                       \
        }

int rd_kafka_Uuid_cmp(rd_kafka_Uuid_t a, rd_kafka_Uuid_t b);

rd_kafka_Uuid_t rd_kafka_Uuid_random();

const char *rd_kafka_Uuid_str(const rd_kafka_Uuid_t *uuid);

unsigned int rd_kafka_Uuid_hash(const rd_kafka_Uuid_t *uuid);

unsigned int rd_kafka_Uuid_map_hash(const void *key);

void *rd_list_Uuid_copy(const void *elem, void *opaque);

void rd_list_Uuid_destroy(void *uuid);

int rd_list_Uuid_cmp(const void *uuid1, const void *uuid2);

#endif /* _RDKAFKA_UUID_H_ */
