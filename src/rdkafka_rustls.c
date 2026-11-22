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
/* Broker-only rustls-ffi adapter. Pinned to the public 0.15.7 C interface. */
#include "rdkafka_int.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_tls.h"
#include "rdkafka_cert.h"

#define DEFINE_AWS_LC_RS
#if WITH_RUSTLS_FIPS
#define DEFINE_FIPS
#endif
#include <rustls.h>

#if RUSTLS_VERSION_NUMBER != 0x000f0700
#error "The rustls backend requires the rustls-ffi 0.15.7 header and library"
#endif

#define RD_KAFKA_RUSTLS_BUFFER_LIMIT (64 * 1024)
#define RD_KAFKA_RUSTLS_CHUNK (16 * 1024)
#define RD_KAFKA_RUSTLS_PEM_LIMIT (16 * 1024 * 1024)

typedef struct rd_kafka_rustls_session_s {
        struct rustls_connection *conn;
        /* Plaintext copied to rustls but not yet committed to the caller.
         * Never retain a caller data pointer: rustls owns the ciphertext.
         * The buffer identity and offset detect an illegal changed retry. */
        const rd_buf_t *pending_buf;
        size_t pending_offset;
        size_t pending_n;
        rd_bool_t plaintext_pending;
        rd_bool_t eof;
} rd_kafka_rustls_session_t;

static void rd_kafka_rustls_error(rustls_result result,
                                  const char *operation,
                                  char *errstr,
                                  size_t errstr_size) {
        char msg[256];
        size_t n = 0;
        rustls_error(result, msg, sizeof(msg) - 1, &n);
        msg[RD_MIN(n, sizeof(msg) - 1)] = '\0';
        rd_snprintf(errstr, errstr_size, "rustls %s failed: %s (%u)",
                    operation, msg, (unsigned int)result);
}

static int rd_kafka_rustls_unsupported(rd_kafka_t *rk,
                                        char *errstr,
                                        size_t errstr_size) {
        const char *unsupported[] = {
            "ssl.cipher.suites", "ssl.curves.list", "ssl.sigalgs.list",
            "ssl.crl.location", "ssl.keystore.location",
            "ssl.keystore.password", "ssl.key.password", "ssl.providers",
            "ssl.engine.location", "ssl.engine.id", "ssl.fips.config",
            "ssl.ca.certificate.stores", NULL};
        const char **p;
        for (p = unsupported; *p; ++p) {
                if (rd_kafka_conf_is_modified(&rk->rk_conf, *p)) {
                        rd_snprintf(errstr, errstr_size,
                                    "%s is unsupported by the rustls backend",
                                    *p);
                        return -1;
                }
        }
        if (rk->rk_conf.ssl.cert_verify_cb ||
            rk->rk_conf.ssl.engine_callback_data) {
                rd_snprintf(errstr, errstr_size,
                            "OpenSSL certificate/engine callbacks are "
                            "unsupported by the rustls backend");
                return -1;
        }
        if ((!!rk->rk_conf.ssl.ca + !!rk->rk_conf.ssl.ca_pem +
             !!rk->rk_conf.ssl.ca_location) > 1 ||
            (!!rk->rk_conf.ssl.cert + !!rk->rk_conf.ssl.cert_pem +
             !!rk->rk_conf.ssl.cert_location) > 1 ||
            (!!rk->rk_conf.ssl.key + !!rk->rk_conf.ssl.key_pem +
             !!rk->rk_conf.ssl.key_location) > 1) {
                rd_snprintf(errstr, errstr_size,
                            "rustls requires exactly one configured source "
                            "per CA/client certificate/private key; memory, "
                            "PEM string and file sources are mutually exclusive");
                return -1;
        }
        if (!rk->rk_conf.ssl.enable_verify ||
            rk->rk_conf.ssl.endpoint_identification !=
                RD_KAFKA_SSL_ENDPOINT_ID_HTTPS) {
                rd_snprintf(errstr, errstr_size,
                            "rustls requires certificate and hostname "
                            "verification");
                return -1;
        }
        return 0;
}

/* File buffers can contain private material. Wipe temporary copies through a
 * volatile pointer so optimization cannot remove the stores. */
static void rd_kafka_rustls_pem_free(char *pem, size_t len) {
        volatile char *p = pem;
        while (len--)
                *p++ = 0;
        rd_free(pem);
}

static int rd_kafka_rustls_ctx_init(rd_kafka_t *rk,
                                    char *errstr,
                                    size_t errstr_size) {
        struct rustls_client_config_builder *builder = NULL;
        struct rustls_root_cert_store_builder *roots_builder = NULL;
        struct rustls_web_pki_server_cert_verifier_builder *verifier_builder = NULL;
        struct rustls_server_cert_verifier *verifier = NULL;
        const struct rustls_root_cert_store *roots = NULL;
        const struct rustls_crypto_provider *provider = NULL;
        const struct rustls_client_config *config = NULL;
        const struct rustls_certified_key *certified_key = NULL;
        struct rustls_signing_key *signing_key = NULL;
        const uint16_t versions[] = {RUSTLS_TLS_VERSION_TLSV1_3,
                                     RUSTLS_TLS_VERSION_TLSV1_2};
        const char *cert_pem = NULL, *key_pem = NULL;
        char *cert_file = NULL, *key_file = NULL;
        size_t cert_len = 0, key_len = 0;
        rustls_result result = RUSTLS_RESULT_OK;
        const char *operation = "configuration";
        int ret = -1;

        if (rd_kafka_rustls_unsupported(rk, errstr, errstr_size) == -1)
                return -1;
        if (rk->rk_conf.ssl.fips_mode) {
#if WITH_RUSTLS_FIPS
                provider = rustls_default_fips_provider();
                if (!provider || !rustls_crypto_provider_fips(provider)) {
                        rd_snprintf(errstr, errstr_size,
                                    "rustls FIPS provider is unavailable or "
                                    "not operating in FIPS mode");
                        goto done;
                }
#else
                rd_snprintf(errstr, errstr_size,
                            "ssl.fips.mode=required needs rustls-ffi built "
                            "with the fips feature; no fallback is permitted");
                goto done;
#endif
        } else {
                /* An explicit provider avoids a host application's unrelated
                 * process-default rustls provider changing this client. */
                provider = rustls_aws_lc_rs_crypto_provider();
        }
        if (!provider) {
                rd_snprintf(errstr, errstr_size,
                            "rustls AWS-LC provider initialization failed");
                goto done;
        }

#define CHECK_RESULT(expr, what) do { \
        operation = (what); \
        result = (expr); \
        if (result != RUSTLS_RESULT_OK) goto rustls_error; \
} while (0)

        /* This uses the selected provider's RNG and detects a reported RNG
         * initialization failure. It is not evidence of module identity. */
        {
                uint8_t probe[1];
                CHECK_RESULT(rustls_crypto_provider_random(provider, probe,
                                                           sizeof(probe)),
                             "provider random initialization");
        }
        CHECK_RESULT(rustls_client_config_builder_new_custom(
                         provider, versions, RD_ARRAYSIZE(versions), &builder),
                     "client builder");
        roots_builder = rustls_root_cert_store_builder_new();
        if (!roots_builder)
                goto allocation_error;
        if (rk->rk_conf.ssl.ca) {
                CHECK_RESULT(rustls_root_cert_store_builder_add_pem(
                                 roots_builder,
                                 (const uint8_t *)rk->rk_conf.ssl.ca->pem,
                                 rk->rk_conf.ssl.ca->pem_size, true),
                             "memory CA import");
        } else if (rk->rk_conf.ssl.ca_pem) {
                CHECK_RESULT(rustls_root_cert_store_builder_add_pem(
                                 roots_builder,
                                 (const uint8_t *)rk->rk_conf.ssl.ca_pem,
                                 strlen(rk->rk_conf.ssl.ca_pem), true),
                             "CA PEM import");
        } else if (rk->rk_conf.ssl.ca_location &&
                   strcmp(rk->rk_conf.ssl.ca_location, "probe")) {
                CHECK_RESULT(rustls_root_cert_store_builder_load_roots_from_file(
                                 roots_builder, rk->rk_conf.ssl.ca_location,
                                 true), "CA file import (PEM file required)");
        } else {
                rd_snprintf(errstr, errstr_size,
                            "rustls requires an explicit ssl.ca.location PEM "
                            "file, ssl.ca.pem or memory CA certificate; "
                            "directories and probe are unsupported");
                goto done;
        }
        CHECK_RESULT(rustls_root_cert_store_builder_build(roots_builder, &roots),
                     "root store");
        verifier_builder =
            rustls_web_pki_server_cert_verifier_builder_new_with_provider(
                provider, roots);
        if (!verifier_builder)
                goto allocation_error;
        CHECK_RESULT(rustls_web_pki_server_cert_verifier_builder_build(
                         verifier_builder, &verifier), "server verifier");
        rustls_client_config_builder_set_server_verifier(builder, verifier);

        /* Mutually exclusive sources were checked before importing. All key
         * imports use the same provider as the TLS configuration. */
        if (rk->rk_conf.ssl.cert) {
                cert_pem = rk->rk_conf.ssl.cert->pem;
                cert_len = rk->rk_conf.ssl.cert->pem_size;
        } else if (rk->rk_conf.ssl.cert_pem) {
                cert_pem = rk->rk_conf.ssl.cert_pem;
                cert_len = strlen(cert_pem);
        } else if (rk->rk_conf.ssl.cert_location) {
                cert_pem = cert_file = rd_file_read(
                    rk->rk_conf.ssl.cert_location, &cert_len,
                    RD_KAFKA_RUSTLS_PEM_LIMIT);
                if (!cert_file)
                        goto file_error;
        }
        if (rk->rk_conf.ssl.key) {
                key_pem = rk->rk_conf.ssl.key->pem;
                key_len = rk->rk_conf.ssl.key->pem_size;
        } else if (rk->rk_conf.ssl.key_pem) {
                key_pem = rk->rk_conf.ssl.key_pem;
                key_len = strlen(key_pem);
        } else if (rk->rk_conf.ssl.key_location) {
                key_pem = key_file = rd_file_read(
                    rk->rk_conf.ssl.key_location, &key_len,
                    RD_KAFKA_RUSTLS_PEM_LIMIT);
                if (!key_file)
                        goto file_error;
        }
        if (!!cert_pem != !!key_pem) {
                rd_snprintf(errstr, errstr_size,
                            "rustls mutual TLS requires both a client "
                            "certificate and an unencrypted private key");
                goto done;
        }
        if (cert_pem) {
                CHECK_RESULT(rustls_crypto_provider_load_key(
                                 provider, (const uint8_t *)key_pem, key_len,
                                 &signing_key), "client private key import");
                result = rustls_certified_key_build_with_signing_key(
                    (const uint8_t *)cert_pem, cert_len, signing_key,
                    &certified_key);
                signing_key = NULL; /* consumed even on error */
                operation = "client certificate import";
                if (result != RUSTLS_RESULT_OK)
                        goto rustls_error;
                CHECK_RESULT(rustls_certified_key_keys_match(certified_key),
                             "client certificate/private key match");
                CHECK_RESULT(rustls_client_config_builder_set_certified_key(
                                 builder, &certified_key, 1),
                             "client certificate selection");
        }
        result = rustls_client_config_builder_build(builder, &config);
        builder = NULL; /* consumed even on error */
        operation = "client configuration";
        if (result != RUSTLS_RESULT_OK)
                goto rustls_error;
        if (rk->rk_conf.ssl.fips_mode && !rustls_client_config_fips(config)) {
                rd_snprintf(errstr, errstr_size,
                            "rustls client configuration is not FIPS "
                            "compatible; no fallback is permitted");
                goto done;
        }
        rk->rk_conf.ssl.tls_ctx = (void *)config;
        config = NULL;
        ret = 0;
        rd_kafka_dbg(rk, SECURITY, "TLSBACKEND",
                     "Broker TLS backend rustls-ffi 0.15.7; "
                     "runtime FIPS enforcement %s (deployment qualification "
                     "requires separate module and environment evidence)",
                     rk->rk_conf.ssl.fips_mode ? "required" : "disabled");
        goto done;

file_error:
        rd_snprintf(errstr, errstr_size,
                    "rustls could not read client certificate/private key "
                    "file (maximum %d bytes)", RD_KAFKA_RUSTLS_PEM_LIMIT);
        goto done;
allocation_error:
        rd_snprintf(errstr, errstr_size,
                    "rustls configuration allocation failed");
        goto done;
rustls_error:
        rd_kafka_rustls_error(result, operation, errstr, errstr_size);
done:
        if (key_file)
                rd_kafka_rustls_pem_free(key_file, key_len);
        if (cert_file)
                rd_free(cert_file);
        rustls_signing_key_free(signing_key);
        rustls_certified_key_free(certified_key);
        rustls_client_config_free(config);
        rustls_client_config_builder_free(builder);
        rustls_server_cert_verifier_free(verifier);
        rustls_web_pki_server_cert_verifier_builder_free(verifier_builder);
        rustls_root_cert_store_free(roots);
        rustls_root_cert_store_builder_free(roots_builder);
        rustls_crypto_provider_free(provider);
        return ret;
#undef CHECK_RESULT
}

static void rd_kafka_rustls_ctx_term(rd_kafka_t *rk) {
        rustls_client_config_free(rk->rk_conf.ssl.tls_ctx);
        rk->rk_conf.ssl.tls_ctx = NULL;
}

static rd_bool_t rd_kafka_rustls_wouldblock(int error) {
#ifdef _WIN32
        return error == WSAEWOULDBLOCK;
#else
        return error == EAGAIN || error == EWOULDBLOCK;
#endif
}

static rustls_io_result rd_kafka_rustls_read_cb(void *opaque, uint8_t *buf,
                                               size_t len, size_t *out_n) {
        rd_kafka_transport_t *t = opaque;
        int n;
        do {
                n = (int)recv(t->rktrans_s, (void *)buf,
                              (int)RD_MIN(len, RD_KAFKA_RUSTLS_CHUNK), 0);
        } while (n < 0 && rd_socket_errno ==
#ifdef _WIN32
                 WSAEINTR
#else
                 EINTR
#endif
        );
        if (n < 0)
                return rd_socket_errno;
        *out_n = (size_t)n;
        return 0;
}

static rustls_io_result rd_kafka_rustls_write_cb(void *opaque,
                                                const uint8_t *buf,
                                                size_t len, size_t *out_n) {
        rd_kafka_transport_t *t = opaque;
        int flags = 0;
        int n;
#ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif
        do {
                n = (int)send(t->rktrans_s, (const void *)buf,
                              (int)RD_MIN(len, RD_KAFKA_RUSTLS_CHUNK), flags);
        } while (n < 0 && rd_socket_errno ==
#ifdef _WIN32
                 WSAEINTR
#else
                 EINTR
#endif
        );
        if (n < 0)
                return rd_socket_errno;
        *out_n = (size_t)n;
        return 0;
}

static int rd_kafka_rustls_interests(rd_kafka_transport_t *t) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        return (rustls_connection_wants_read(s->conn) ? POLLIN : 0) |
               ((rustls_connection_wants_write(s->conn) || s->pending_n) ?
                    POLLOUT : 0);
}

static void rd_kafka_rustls_update(rd_kafka_transport_t *t) {
        rd_kafka_transport_poll_set(t, rd_kafka_rustls_interests(t));
}

/* Drain bounded ciphertext independently of the Kafka send queue. 0 means
 * drained or would-block; the caller inspects wants_write to distinguish. */
static int rd_kafka_rustls_flush(rd_kafka_transport_t *t,
                                 char *errstr, size_t errstr_size) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        while (rustls_connection_wants_write(s->conn)) {
                size_t n = 0;
                rustls_io_result error = rustls_connection_write_tls(
                    s->conn, rd_kafka_rustls_write_cb, t, &n);
                if (error) {
                        if (rd_kafka_rustls_wouldblock(error)) {
                                rd_kafka_transport_set_blocked(t, rd_true);
                                rd_kafka_rustls_update(t);
                                return 0;
                        }
                        rd_snprintf(errstr, errstr_size,
                                    "rustls socket write failed: %s",
                                    rd_socket_strerror(error));
                        return -1;
                }
                if (!n) {
                        rd_snprintf(errstr, errstr_size,
                                    "Disconnected: rustls socket write "
                                    "made no progress");
                        return -1;
                }
        }
        rd_kafka_rustls_update(t);
        return 0;
}

/* Read one encrypted fragment and process it. 1 input, 0 would-block/EOF. */
static int rd_kafka_rustls_input(rd_kafka_transport_t *t,
                                 char *errstr, size_t errstr_size) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        size_t n = 0;
        rustls_result result;
        rustls_io_result error = rustls_connection_read_tls(
            s->conn, rd_kafka_rustls_read_cb, t, &n);
        if (error) {
                if (rd_kafka_rustls_wouldblock(error))
                        return 0;
                rd_snprintf(errstr, errstr_size,
                            "rustls socket read failed: %s",
                            rd_socket_strerror(error));
                return -1;
        }
        if (!n)
                s->eof = rd_true;
        result = rustls_connection_process_new_packets(s->conn);
        if (result != RUSTLS_RESULT_OK) {
                /* Send any queued fatal alert best-effort, without replacing
                 * the original verification/protocol error. */
                char ignored[256];
                rd_kafka_rustls_flush(t, ignored, sizeof(ignored));
                rd_kafka_rustls_error(result, "packet processing", errstr,
                                      errstr_size);
                return -1;
        }
        s->plaintext_pending = rd_true;
        rd_kafka_rustls_update(t);
        return n ? 1 : 0;
}

static void rd_kafka_rustls_close(rd_kafka_transport_t *t) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        if (!s)
                return;
        if (s->conn) {
                char ignored[256];
                /* Cancellation/timeout may have removed a request whose
                 * accepted prefix has not been committed. Never flush that
                 * application ciphertext during teardown. Handshake output
                 * is likewise discarded on an incomplete connection. */
                if (!s->pending_n &&
                    !rustls_connection_is_handshaking(s->conn)) {
                        rustls_connection_send_close_notify(s->conn);
                        rd_kafka_rustls_flush(t, ignored, sizeof(ignored));
                }
                rustls_connection_free(s->conn);
        }
        rd_free(s);
        t->rktrans_tls = NULL;
}

static int rd_kafka_rustls_connect(rd_kafka_broker_t *rkb,
                                   rd_kafka_transport_t *t,
                                   char *errstr, size_t errstr_size) {
        rd_kafka_rustls_session_t *s = rd_calloc(1, sizeof(*s));
        char name[RD_KAFKA_NODENAME_SIZE];
        size_t len;
        rustls_result result;
        t->rktrans_tls = s;
        rd_kafka_broker_lock(rkb);
        rd_kafka_nodename_to_hostname(rkb->rkb_nodename, name, sizeof(name));
        rd_kafka_broker_unlock(rkb);
        len = strlen(name);
        if (len > 1 && name[len - 1] == '.')
                name[len - 1] = '\0';
        result = rustls_client_connection_new(rkb->rkb_rk->rk_conf.ssl.tls_ctx,
                                               name, &s->conn);
        if (result != RUSTLS_RESULT_OK) {
                rd_kafka_rustls_error(result, "connection creation", errstr,
                                      errstr_size);
                return -1;
        }
        rustls_connection_set_buffer_limit(s->conn,
                                            RD_KAFKA_RUSTLS_BUFFER_LIMIT);
        return rd_kafka_rustls_flush(t, errstr, errstr_size);
}

static int rd_kafka_rustls_handshake(rd_kafka_transport_t *t) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        char errstr[512];
        if (rd_kafka_rustls_flush(t, errstr, sizeof(errstr)) == -1)
                goto fail;
        if (rustls_connection_is_handshaking(s->conn)) {
                if (rd_kafka_rustls_input(t, errstr, sizeof(errstr)) == -1)
                        goto fail;
                if (s->eof) {
                        rd_snprintf(errstr, sizeof(errstr),
                                    "Disconnected: EOF during rustls handshake");
                        goto fail;
                }
                if (rd_kafka_rustls_flush(t, errstr, sizeof(errstr)) == -1)
                        goto fail;
        }
        if (!rustls_connection_is_handshaking(s->conn) &&
            !rustls_connection_wants_write(s->conn)) {
                if (t->rktrans_rkb->rkb_rk->rk_conf.ssl.fips_mode &&
                    !rustls_connection_fips(s->conn)) {
                        rd_snprintf(errstr, sizeof(errstr),
                                    "rustls connection is not FIPS compatible; "
                                    "no fallback is permitted");
                        goto fail;
                }
                rd_kafka_transport_connect_done(t, NULL);
                return 1;
        }
        rd_kafka_rustls_update(t);
        return 0;
fail:
        rd_kafka_broker_fail(t->rktrans_rkb, LOG_ERR, RD_KAFKA_RESP_ERR__SSL,
                             "%s", errstr);
        return -1;
}

static ssize_t rd_kafka_rustls_send(rd_kafka_transport_t *t, rd_slice_t *slice,
                                    char *errstr, size_t errstr_size) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        ssize_t sum = 0;
        const void *p;
        size_t len;
        for (;;) {
                if (s->pending_n) {
                        if (slice->buf != s->pending_buf ||
                            rd_slice_abs_offset(slice) != s->pending_offset ||
                            rd_slice_remains(slice) < s->pending_n) {
                                rd_snprintf(errstr, errstr_size,
                                            "rustls send retry changed the "
                                            "pending request");
                                return -1;
                        }
                        if (rd_kafka_rustls_flush(t, errstr, errstr_size) == -1)
                                return -1;
                        if (rustls_connection_wants_write(s->conn))
                                return sum;
                        /* Commit once, only after the owned ciphertext has
                         * reached send(). Thus acks=0 completion cannot occur
                         * while application output is merely buffered here. */
                        rd_slice_read(slice, NULL, s->pending_n);
                        sum += (ssize_t)s->pending_n;
                        s->pending_n = 0;
                        s->pending_buf = NULL;
                }
                if (!(len = rd_slice_peeker(slice, &p)))
                        return sum;
                if (rd_kafka_rustls_flush(t, errstr, errstr_size) == -1)
                        return -1;
                if (rustls_connection_wants_write(s->conn))
                        return sum;
                {
                        rustls_result result;
                        size_t n = 0;
                        result = rustls_connection_write(
                            s->conn, p, RD_MIN(len, RD_KAFKA_RUSTLS_CHUNK), &n);
                        if (result != RUSTLS_RESULT_OK) {
                                rd_kafka_rustls_error(result, "plaintext write",
                                                      errstr, errstr_size);
                                return -1;
                        }
                        if (!n) {
                                rd_snprintf(errstr, errstr_size,
                                            "rustls plaintext write made no "
                                            "progress without pending output");
                                return -1;
                        }
                        s->pending_buf = slice->buf;
                        s->pending_offset = rd_slice_abs_offset(slice);
                        s->pending_n = n;
                }
        }
}

static ssize_t rd_kafka_rustls_recv(rd_kafka_transport_t *t, rd_buf_t *rbuf,
                                    char *errstr, size_t errstr_size) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        ssize_t sum = 0;
        size_t len;
        void *p;
        while ((len = rd_buf_get_writable(rbuf, &p))) {
                size_t n = 0;
                rustls_result result;
                len = RD_MIN(len, RD_KAFKA_RUSTLS_CHUNK);
                /* Required by the pinned FFI's initialized-buffer contract. */
                memset(p, 0, len);
                result = rustls_connection_read(s->conn, p, len, &n);
                if (result == RUSTLS_RESULT_OK && n) {
                        rd_buf_write(rbuf, NULL, n);
                        sum += (ssize_t)n;
                        s->plaintext_pending = rd_true;
                        continue;
                }
                s->plaintext_pending = rd_false;
                if (result == RUSTLS_RESULT_OK || s->eof ||
                    result == RUSTLS_RESULT_UNEXPECTED_EOF) {
                        if (sum)
                                return sum;
                        rd_snprintf(errstr, errstr_size,
                                    "Disconnected: rustls peer closed %s",
                                    s->eof ? "socket" : "TLS connection");
                        return -1;
                }
                if (result != RUSTLS_RESULT_PLAINTEXT_EMPTY) {
                        rd_kafka_rustls_error(result, "plaintext read", errstr,
                                              errstr_size);
                        return -1;
                }
                if (sum)
                        return sum;
                {
                        int input = rd_kafka_rustls_input(t, errstr, errstr_size);
                        if (input == -1)
                                return -1;
                        if (rd_kafka_rustls_flush(t, errstr, errstr_size) == -1)
                                return -1;
                        if (!input && !s->eof)
                                return 0;
                }
        }
        return sum;
}

static rd_bool_t rd_kafka_rustls_pending(rd_kafka_transport_t *t) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        return s->plaintext_pending;
}
static int rd_kafka_rustls_serve(rd_kafka_transport_t *t, int events,
                                 char *errstr, size_t errstr_size) {
        if (events & POLLOUT)
                return rd_kafka_rustls_flush(t, errstr, errstr_size);
        return 0;
}
static rd_bool_t rd_kafka_rustls_send_pending(rd_kafka_transport_t *t,
                                              const rd_slice_t *slice) {
        rd_kafka_rustls_session_t *s = t->rktrans_tls;
        return s->pending_n && s->pending_buf == slice->buf &&
               s->pending_offset == rd_slice_abs_offset(slice);
}
static void rd_kafka_rustls_global_noop(void) {
}

const rd_kafka_tls_ops_t rd_kafka_rustls_ops = {
    "rustls", rd_kafka_rustls_global_noop, rd_kafka_rustls_global_noop,
    rd_kafka_rustls_ctx_init, rd_kafka_rustls_ctx_term,
    rd_kafka_rustls_close, rd_kafka_rustls_connect,
    rd_kafka_rustls_handshake, rd_kafka_rustls_send,
    rd_kafka_rustls_recv, rd_kafka_rustls_pending,
    rd_kafka_rustls_interests, rd_kafka_rustls_serve,
    rd_kafka_rustls_send_pending, rd_kafka_rustls_global_noop};
