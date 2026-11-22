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
/* Deterministic whitebox transport invariants with real TLS over socketpair.
 * Compile the actual adapter into this test so its private state can be
 * inspected; no substitute TLS implementation or mocked Kafka is involved.
 * Linux/macOS only: native Windows socket behavior remains a platform check. */
#include "rdkafka_int.h"
#include "rdkafka_transport_int.h"
#include <sys/socket.h>
#include <unistd.h>

static int client_fd;
static int block_client_writes;
static int short_writes, fragmented_reads, blocked_writes, read_calls;
static ssize_t fault_send(int fd, const void *buf, size_t n, int flags) {
        if (fd == client_fd && block_client_writes) {
                blocked_writes++;
                errno = EAGAIN;
                return -1;
        }
        if (n > 67) {
                n = 67;
                short_writes++;
        }
        return send(fd, buf, n, flags);
}
static ssize_t fault_recv(int fd, void *buf, size_t n, int flags) {
        read_calls++;
        if (n > 31) {
                n = 31;
                fragmented_reads++;
        }
        return recv(fd, buf, n, flags);
}
#define send fault_send
#define recv fault_recv
#include "../../src/rdkafka_rustls.c"
#undef send
#undef recv

#define CHECK(expr) do { \
        if (!(expr)) { \
                fprintf(stderr, "transport invariant failed at line %d: %s\n", \
                        __LINE__, #expr); \
                return 1; \
        } \
} while (0)
#define OK(expr) CHECK((expr) == RUSTLS_RESULT_OK)

static int pump(rd_kafka_transport_t *a, rd_kafka_transport_t *b,
                 char *err, size_t err_size) {
        int n, m;
        if (rd_kafka_rustls_flush(a, err, err_size) == -1 ||
            rd_kafka_rustls_flush(b, err, err_size) == -1)
                return -1;
        n = rd_kafka_rustls_input(a, err, err_size);
        if (n == -1)
                return -1;
        m = rd_kafka_rustls_input(b, err, err_size);
        if (m == -1)
                return -1;
        return n + m;
}

int main(int argc, char **argv) {
        rd_kafka_t rk;
        rd_kafka_conf_t *conf;
        rd_kafka_transport_t client = {0}, server = {0};
        rd_kafka_rustls_session_t *cs, *ss;
        const struct rustls_crypto_provider *provider;
        struct rustls_server_config_builder *sb;
        const struct rustls_server_config *sc;
        const struct rustls_certified_key *key;
        const uint16_t versions[] = {RUSTLS_TLS_VERSION_TLSV1_3};
        char *cert_pem, *key_pem;
        size_t cert_len, key_len, n, received = 0;
        char err[512] = "";
        int sockets[2], i, small = 1024, calls;
        rd_buf_t sendbuf, recvbuf;
        rd_slice_t slice;
        unsigned char payload[131071], received_payload[sizeof(payload)] = {0};
        unsigned char reply[8192], actual_reply[sizeof(reply)];
        size_t reply_received = 0;
        if (argc != 4) {
                fprintf(stderr, "Usage: %s CA.pem server.pem server.key\n", argv[0]);
                return 2;
        }
        memset(&rk, 0, sizeof(rk));
        conf = rd_kafka_conf_new();
        CHECK(rd_kafka_conf_set(conf, "ssl.ca.location", argv[1], err,
                                sizeof(err)) == RD_KAFKA_CONF_OK);
        {
                rd_kafka_conf_t *unsupported = rd_kafka_conf_dup(conf);
                CHECK(rd_kafka_conf_set(unsupported, "ssl.fips.config",
                                        "must-not-be-ignored.cnf", err,
                                        sizeof(err)) == RD_KAFKA_CONF_OK);
                rk.rk_conf = *unsupported;
                CHECK(rd_kafka_rustls_unsupported(&rk, err, sizeof(err)) == -1);
                CHECK(strstr(err, "ssl.fips.config"));
                rd_kafka_conf_destroy(unsupported);
        }
        rk.rk_conf = *conf;
        rk.rk_conf.socket_max_fails = 0;
        rk.rk_conf.ssl.ca_pem = "conflicting CA source";
        CHECK(rd_kafka_rustls_unsupported(&rk, err, sizeof(err)) == -1);
        CHECK(strstr(err, "mutually exclusive"));
        rk.rk_conf.ssl.ca_pem = NULL;
        CHECK(rd_kafka_rustls_ctx_init(&rk, err, sizeof(err)) == 0);
        provider = rustls_aws_lc_rs_crypto_provider();
        cert_pem = rd_file_read(argv[2], &cert_len, 1024 * 1024);
        key_pem = rd_file_read(argv[3], &key_len, 1024 * 1024);
        CHECK(cert_pem && key_pem);
        OK(rustls_certified_key_build((const uint8_t *)cert_pem, cert_len,
                                      (const uint8_t *)key_pem, key_len, &key));
        rd_free(cert_pem);
        rd_kafka_rustls_pem_free(key_pem, key_len);
        OK(rustls_server_config_builder_new_custom(provider, versions, 1, &sb));
        OK(rustls_server_config_builder_set_certified_keys(sb, &key, 1));
        OK(rustls_server_config_builder_build(sb, &sc));
        rustls_certified_key_free(key);
        rustls_crypto_provider_free(provider);
        CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        client_fd = sockets[0];
        for (i = 0; i < 2; i++) {
                CHECK(fcntl(sockets[i], F_SETFL,
                            fcntl(sockets[i], F_GETFL) | O_NONBLOCK) == 0);
                CHECK(setsockopt(sockets[i], SOL_SOCKET, SO_SNDBUF, &small,
                                 sizeof(small)) == 0);
        }
        client.rktrans_s = sockets[0];
        server.rktrans_s = sockets[1];
        client.rktrans_tls = cs = rd_calloc(1, sizeof(*cs));
        server.rktrans_tls = ss = rd_calloc(1, sizeof(*ss));
        OK(rustls_client_connection_new(rk.rk_conf.ssl.tls_ctx, "localhost",
                                         &cs->conn));
        OK(rustls_server_connection_new(sc, &ss->conn));
        rustls_server_config_free(sc);
        rustls_connection_set_buffer_limit(cs->conn, RD_KAFKA_RUSTLS_BUFFER_LIMIT);
        rustls_connection_set_buffer_limit(ss->conn, RD_KAFKA_RUSTLS_BUFFER_LIMIT);
        for (i = 0; i < 100000; i++) {
                CHECK(pump(&client, &server, err, sizeof(err)) >= 0);
                if (!rustls_connection_is_handshaking(cs->conn) &&
                    !rustls_connection_is_handshaking(ss->conn) &&
                    !rustls_connection_wants_write(cs->conn) &&
                    !rustls_connection_wants_write(ss->conn))
                        break;
        }
        CHECK(i < 100000);
        for (i = 0; i < (int)sizeof(payload); i++)
                payload[i] = (unsigned char)(i * 31 + i / 97);
        rd_buf_init(&sendbuf, 1, sizeof(payload));
        rd_buf_write(&sendbuf, payload, sizeof(payload));
        rd_slice_init_full(&slice, &sendbuf);

        /* The first plaintext prefix is copied once, but remains uncommitted
         * through multiple EAGAIN retries, including a changed-retry failure. */
        block_client_writes = 1;
        CHECK(rd_kafka_rustls_send(&client, &slice, err, sizeof(err)) == 0);
        CHECK(cs->pending_n > 0 && cs->pending_n <= RD_KAFKA_RUSTLS_CHUNK);
        CHECK(rd_slice_offset(&slice) == 0);
        /* Both broker timeout scanning and queue/partition purge use this
         * predicate. Even with socket.max.fails=0, this request must count
         * as partial so timeout/queue purge disconnects and partition purge
         * leaves it in place rather than releasing its backing buffer. */
        CHECK(rk.rk_conf.socket_max_fails == 0);
        CHECK(rd_kafka_transport_request_has_started(&client, &slice));
        n = cs->pending_n;
        for (i = 0; i < 3; i++) {
                CHECK(rd_kafka_rustls_send(&client, &slice, err, sizeof(err)) == 0);
                CHECK(cs->pending_n == n && rd_slice_offset(&slice) == 0);
        }
        {
                rd_slice_t wrong = slice;
                rd_slice_read(&wrong, NULL, 1);
                CHECK(rd_kafka_rustls_send(&client, &wrong, err, sizeof(err)) == -1);
                CHECK(cs->pending_n == n && rd_slice_offset(&slice) == 0);
        }
        CHECK(rd_kafka_rustls_interests(&client) & POLLOUT);
        block_client_writes = 0;

        /* No Kafka queue or send() call: the TLS service must independently
         * drain pending ciphertext. It must not commit the caller slice. */
        for (i = 0; i < 100000 &&
                    rustls_connection_wants_write(cs->conn); i++) {
                CHECK(rd_kafka_rustls_serve(&client, POLLOUT, err, sizeof(err)) == 0);
                CHECK(rd_kafka_rustls_input(&server, err, sizeof(err)) >= 0);
        }
        CHECK(i < 100000 && cs->pending_n == n && rd_slice_offset(&slice) == 0);
        /* Flushing independently still requires pending commitment before
         * the request can safely be removed from the broker queue. */
        CHECK(rd_kafka_transport_request_has_started(&client, &slice));

        for (i = 0; i < 200000 &&
                    (rd_slice_remains(&slice) || received < sizeof(payload)); i++) {
                rustls_result result;
                if (rd_slice_remains(&slice))
                        CHECK(rd_kafka_rustls_send(&client, &slice, err, sizeof(err)) >= 0);
                CHECK(rd_kafka_rustls_input(&server, err, sizeof(err)) >= 0);
                n = 0;
                result = rustls_connection_read(ss->conn,
                    received_payload + received, sizeof(payload) - received, &n);
                CHECK(result == RUSTLS_RESULT_OK ||
                      result == RUSTLS_RESULT_PLAINTEXT_EMPTY);
                received += n;
                CHECK(cs->pending_n <= RD_KAFKA_RUSTLS_CHUNK);
        }
        CHECK(i < 200000 && received == sizeof(payload));
        CHECK(!memcmp(payload, received_payload, sizeof(payload)));
        CHECK(rd_slice_remains(&slice) == 0 && cs->pending_n == 0);
        rd_buf_destroy(&sendbuf);

        /* A small destination buffer leaves most of one TLS record inside
         * rustls. The next recv must use that plaintext without another read
         * callback or socket readiness event. */
        memset(reply, 0x97, sizeof(reply));
        OK(rustls_connection_write(ss->conn, reply, sizeof(reply), &n));
        CHECK(n == sizeof(reply));
        for (i = 0; i < 100000; i++) {
                int progress = pump(&client, &server, err, sizeof(err));
                CHECK(progress >= 0);
                if (!rustls_connection_wants_write(ss->conn) &&
                    !rustls_connection_wants_write(cs->conn) && !progress)
                        break;
        }
        CHECK(i < 100000);
        while (reply_received < sizeof(reply)) {
                rd_slice_t output;
                size_t want = RD_MIN(43, sizeof(reply) - reply_received);
                rd_buf_init(&recvbuf, 1, want);
                rd_buf_write_ensure(&recvbuf, want, want);
                calls = read_calls;
                CHECK(rd_kafka_rustls_recv(&client, &recvbuf, err, sizeof(err)) > 0);
                CHECK(read_calls == calls);
                rd_slice_init_full(&output, &recvbuf);
                n = rd_slice_read(&output, actual_reply + reply_received,
                                  rd_buf_len(&recvbuf));
                reply_received += n;
                CHECK(rd_kafka_rustls_pending(&client));
                rd_buf_destroy(&recvbuf);
        }
        CHECK(!memcmp(reply, actual_reply, sizeof(reply)));
        CHECK(short_writes > 0 && fragmented_reads > 0 && blocked_writes >= 4);

        rustls_connection_send_close_notify(ss->conn);
        for (i = 0; i < 100000 &&
                    rustls_connection_wants_write(ss->conn); i++) {
                CHECK(rd_kafka_rustls_flush(&server, err, sizeof(err)) == 0);
                CHECK(rd_kafka_rustls_input(&client, err, sizeof(err)) >= 0);
        }
        rd_buf_init(&recvbuf, 1, 128);
        rd_buf_write_ensure(&recvbuf, 128, 128);
        CHECK(rd_kafka_rustls_recv(&client, &recvbuf, err, sizeof(err)) == -1);
        CHECK(strstr(err, "Disconnected"));
        rd_buf_destroy(&recvbuf);
        rd_kafka_rustls_close(&client);
        rd_kafka_rustls_close(&server);
        close(sockets[0]);
        close(sockets[1]);
        /* EOF before a handshake completes must fail without accepting data. */
        CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        client.rktrans_s = sockets[0];
        client.rktrans_tls = cs = rd_calloc(1, sizeof(*cs));
        OK(rustls_client_connection_new(rk.rk_conf.ssl.tls_ctx, "localhost",
                                         &cs->conn));
        close(sockets[1]);
        CHECK(rd_kafka_rustls_input(&client, err, sizeof(err)) == 0);
        CHECK(cs->eof);
        rd_buf_init(&recvbuf, 1, 128);
        rd_buf_write_ensure(&recvbuf, 128, 128);
        CHECK(rd_kafka_rustls_recv(&client, &recvbuf, err, sizeof(err)) == -1);
        rd_buf_destroy(&recvbuf);
        rd_kafka_rustls_close(&client);
        close(sockets[0]);
        /* Failure before connection construction is also a valid teardown. */
        client.rktrans_tls = rd_calloc(1, sizeof(*cs));
        rd_kafka_rustls_close(&client);
        CHECK(!client.rktrans_tls);
        rd_kafka_rustls_ctx_term(&rk);
        rd_kafka_conf_destroy(conf);
        printf("transport invariants OK: short_writes=%d fragments=%d "
               "forced_EAGAIN=%d; exact bytes, deferred slice commit, "
               "empty-queue flush, buffered plaintext, bounded pending "
               "prefix, timeout/purge boundary with socket.max.fails=0, "
               "close-notify, handshake EOF and partial cleanup\n",
               short_writes, fragmented_reads, blocked_writes);
        return 0;
}
