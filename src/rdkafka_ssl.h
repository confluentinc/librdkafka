/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2019-2022, Magnus Edenhill
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


#ifndef _RDKAFKA_SSL_H_
#define _RDKAFKA_SSL_H_

void rd_kafka_transport_ssl_close(rd_kafka_transport_t *rktrans);
int rd_kafka_transport_ssl_connect(rd_kafka_broker_t *rkb,
                                   rd_kafka_transport_t *rktrans,
                                   char *errstr,
                                   size_t errstr_size);
int rd_kafka_transport_ssl_handshake(rd_kafka_transport_t *rktrans);
ssize_t rd_kafka_transport_ssl_send(rd_kafka_transport_t *rktrans,
                                    rd_slice_t *slice,
                                    char *errstr,
                                    size_t errstr_size);
ssize_t rd_kafka_transport_ssl_recv(rd_kafka_transport_t *rktrans,
                                    rd_buf_t *rbuf,
                                    char *errstr,
                                    size_t errstr_size);


void rd_kafka_ssl_ctx_term(rd_kafka_t *rk);
int rd_kafka_ssl_ctx_init(rd_kafka_t *rk, char *errstr, size_t errstr_size);

/**
 * @brief Rebuild the SSL context from the currently configured file-based
 *        certificate locations and atomically swap it in for new connections.
 *        On failure the existing context is retained.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success, or an error code with a
 *          human-readable \p errstr on failure.
 *
 * @locality any thread
 */
rd_kafka_resp_err_t
rd_kafka_ssl_ctx_reload0(rd_kafka_t *rk, char *errstr, size_t errstr_size);

/**
 * @returns rd_true if the configured client certificates are file-based
 *          (ssl.*.location) and therefore eligible for hot reload, else
 *          rd_false.
 */
rd_bool_t rd_kafka_ssl_cert_files_are_reloadable(rd_kafka_t *rk);

/**
 * @brief Capture the current modification time/size of the watched
 *        certificate files as the baseline for change detection.
 */
void rd_kafka_ssl_cert_refresh_baseline(rd_kafka_t *rk);

/**
 * @brief Timer callback (rdkafka main thread) that polls the watched
 *        certificate files and reloads the SSL context when a change is
 *        detected.
 */
void rd_kafka_ssl_cert_refresh_tmr_cb(rd_kafka_timers_t *rkts, void *arg);

void rd_kafka_ssl_term(void);
void rd_kafka_ssl_init(void);

const char *rd_kafka_ssl_last_error_str(void);

int rd_kafka_ssl_hmac(rd_kafka_broker_t *rkb,
                      const EVP_MD *evp,
                      const rd_chariov_t *in,
                      const rd_chariov_t *salt,
                      int itcnt,
                      rd_chariov_t *out);

int rd_kafka_ssl_read_cert_chain_from_BIO(BIO *in,
                                          STACK_OF(X509) * chainp,
                                          pem_password_cb *password_cb,
                                          void *password_cb_opaque);

int rd_kafka_ssl_probe_and_set_default_ca_location(rd_kafka_t *rk,
                                                   const char *ctx_identifier,
                                                   SSL_CTX *ctx);

char *rd_kafka_ssl_error0(rd_kafka_t *rk,
                          rd_kafka_broker_t *rkb,
                          const char *ctx_identifier,
                          char *errstr,
                          size_t errstr_size);

#ifdef _WIN32
int rd_kafka_ssl_win_load_cert_stores(rd_kafka_t *rk,
                                      const char *ctx_identifier,
                                      SSL_CTX *ctx,
                                      const char *store_names);
#endif

#endif /* _RDKAFKA_SSL_H_ */
