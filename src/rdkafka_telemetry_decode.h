/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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

#ifndef _RDKAFKA_RDKAFKA_TELEMETRY_DECODE_H
#define _RDKAFKA_RDKAFKA_TELEMETRY_DECODE_H
#include "rd.h"
#include "rdkafka_telemetry_decode.h"
#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "nanopb/pb_decode.h"
#include "opentelemetry/metrics.pb.h"
#include "rdkafka_int.h"
#include "rdkafka_telemetry_encode.h"
#include "rdunittest.h"
#include "rdkafka_lz4.h"
#include "rdgz.h"
#include "rdkafka_zstd.h"
#include "snappy.h"

int rd_kafka_telemetry_decompress_metrics_payload(
    rd_kafka_broker_t *rkb,
    rd_kafka_compression_t compression_type,
    void *compressed_payload,
    size_t compressed_payload_size,
    void **decompressed_payload,
    size_t *decompressed_payload_size);
int rd_kafka_telemetry_decode_metrics(void *buffer,
                                      size_t size,
                                      rd_bool_t is_unit_test);

#endif /* _RDKAFKA_RDKAFKA_TELEMETRY_DECODE_H */
