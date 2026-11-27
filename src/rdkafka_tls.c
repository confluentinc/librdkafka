/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2015-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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
/* Private compile-time TLS dispatch. */
#include "rdkafka_int.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_tls.h"

#if WITH_TLS

#if WITH_SSL
const rd_kafka_tls_ops_t *const rd_kafka_tls_ops = &rd_kafka_openssl_ops;
#elif WITH_RUSTLS
const rd_kafka_tls_ops_t *const rd_kafka_tls_ops = &rd_kafka_rustls_ops;
#endif

void rd_kafka_tls_init(void) {
        rd_kafka_tls_ops->global_init();
}
void rd_kafka_tls_term(void) {
        rd_kafka_tls_ops->global_term();
}
void rd_kafka_tls_thread_term(void) {
        rd_kafka_tls_ops->thread_term();
}
int rd_kafka_tls_ctx_init(rd_kafka_t *rk, char *errstr, size_t errstr_size) {
        return rd_kafka_tls_ops->ctx_init(rk, errstr, errstr_size);
}
void rd_kafka_tls_ctx_term(rd_kafka_t *rk) {
        rd_kafka_tls_ops->ctx_term(rk);
}

#endif /* WITH_TLS */
