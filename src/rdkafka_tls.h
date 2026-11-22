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
/* Private, build-selected broker TLS interface. No public ABI. */
#ifndef _RDKAFKA_TLS_H_
#define _RDKAFKA_TLS_H_

#include "rdkafka_transport.h"

/* A backend owns immutable client configuration and an opaque per-socket
 * session. Socket ownership, timeouts and Kafka request accounting remain in
 * transport/broker. send() advances a slice only for bytes committed by the
 * backend. In rustls this requires draining ciphertext to the socket first;
 * the accepted-but-uncommitted prefix is retained across retries exactly once.
 * pending() means recv() can make progress without a new socket event.
 * interests() adds socket readiness needed even with an empty Kafka queue.
 * serve() services such work before normal Kafka reads/writes. */
typedef struct rd_kafka_tls_ops_s {
        const char *name;
        void (*global_init)(void);
        void (*global_term)(void);
        int (*ctx_init)(rd_kafka_t *, char *, size_t);
        void (*ctx_term)(rd_kafka_t *);
        void (*close)(rd_kafka_transport_t *);
        int (*connect)(rd_kafka_broker_t *, rd_kafka_transport_t *, char *, size_t);
        int (*handshake)(rd_kafka_transport_t *);
        ssize_t (*send)(rd_kafka_transport_t *, rd_slice_t *, char *, size_t);
        ssize_t (*recv)(rd_kafka_transport_t *, rd_buf_t *, char *, size_t);
        rd_bool_t (*pending)(rd_kafka_transport_t *);
        int (*interests)(rd_kafka_transport_t *);
        int (*serve)(rd_kafka_transport_t *, int, char *, size_t);
        /* An accepted-but-uncommitted prefix belongs to this request. The
         * broker must treat cancellation/timeout as a partially sent request
         * even if the externally visible slice offset is still zero. */
        rd_bool_t (*send_pending)(rd_kafka_transport_t *, const rd_slice_t *);
        void (*thread_term)(void);
} rd_kafka_tls_ops_t;

extern const rd_kafka_tls_ops_t rd_kafka_openssl_ops;
extern const rd_kafka_tls_ops_t rd_kafka_rustls_ops;
extern const rd_kafka_tls_ops_t *const rd_kafka_tls_ops;
void rd_kafka_tls_init(void);
void rd_kafka_tls_term(void);
void rd_kafka_tls_thread_term(void);
int rd_kafka_tls_ctx_init(rd_kafka_t *rk, char *errstr, size_t errstr_size);
void rd_kafka_tls_ctx_term(rd_kafka_t *rk);
#endif
