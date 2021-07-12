/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2021, Magnus Edenhill
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

#include "test.h"


struct test_context {
    char *ca_cert_pem;
    size_t ca_cert_pem_length;

    char *direct_signed_client_cert_pem;
    size_t direct_signed_client_cert_pem_length;
    char *direct_signed_client_key_pem;
    size_t direct_signed_client_key_pem_length;

    char *self_signed_client_cert_pem;
    size_t self_signed_client_cert_pem_length;
    char *self_signed_client_key_pem;
    size_t self_signed_client_key_pem_length;

    char *intermediate_signed_client_cert_pem;
    size_t intermediate_signed_client_cert_pem_length;
    char *intermediate_signed_client_key_pem;
    size_t intermediate_signed_client_key_pem_length;
    char *intermediate_signed_intermediate_cert_pem;
    size_t intermediate_signed_intermediate_cert_pem_length;
    char *intermediate_signed_intermediate_key_pem;
    size_t intermediate_signed_intermediate_key_pem_length;
};

static rd_kafka_cert_fetch_cb_res_t ssl_client_cert_cb_good_direct (
        rd_kafka_t *rk,
        const char *broker_name,
        int32_t broker_id,
        rd_kafka_ssl_cert_fetch_cb_certs_t *certsp,
        char *errstr, size_t errstr_size,
        void *opaque
) {
        struct test_context *ctx;

        ctx = (struct test_context*)opaque;

        certsp->leaf_cert_len = ctx->direct_signed_client_cert_pem_length;
        certsp->leaf_cert = rd_kafka_mem_malloc(rk, certsp->leaf_cert_len);
        memcpy(certsp->leaf_cert,
           ctx->direct_signed_client_cert_pem,
           certsp->leaf_cert_len);

        certsp->pkey_len = ctx->direct_signed_client_key_pem_length;
        certsp->pkey = rd_kafka_mem_malloc(rk, certsp->pkey_len);
        memcpy(certsp->pkey,
               ctx->direct_signed_client_key_pem,
               certsp->pkey_len);

        certsp->format = RD_KAFKA_CERT_ENC_PEM;
        return RD_KAFKA_CERT_FETCH_OK;
}

static rd_kafka_cert_fetch_cb_res_t ssl_client_cert_cb_bad (
        rd_kafka_t *rk,
        const char *broker_name,
        int32_t broker_id,
        rd_kafka_ssl_cert_fetch_cb_certs_t *certsp,
        char *errstr, size_t errstr_size,
        void *opaque
) {
        struct test_context *ctx;

        ctx = (struct test_context*)opaque;

        certsp->leaf_cert_len = ctx->self_signed_client_cert_pem_length;
        certsp->leaf_cert = rd_kafka_mem_malloc(rk, certsp->leaf_cert_len);
        memcpy(certsp->leaf_cert,
               ctx->self_signed_client_cert_pem,
               certsp->leaf_cert_len);

        certsp->pkey_len = ctx->self_signed_client_key_pem_length;
        certsp->pkey = rd_kafka_mem_malloc(rk, certsp->pkey_len);
        memcpy(certsp->pkey,
               ctx->self_signed_client_key_pem,
               certsp->pkey_len);

        certsp->format = RD_KAFKA_CERT_ENC_PEM;
        return RD_KAFKA_CERT_FETCH_OK;
}

static rd_kafka_cert_fetch_cb_res_t ssl_client_cert_cb_good_via_intermediate (
        rd_kafka_t *rk,
        const char *broker_name,
        int32_t broker_id,
        rd_kafka_ssl_cert_fetch_cb_certs_t *certsp,
        char *errstr, size_t errstr_size,
        void *opaque
) {
        struct test_context *ctx;

        ctx = (struct test_context*)opaque;

        certsp->leaf_cert_len = ctx->intermediate_signed_client_cert_pem_length;
        certsp->leaf_cert = rd_kafka_mem_malloc(rk, certsp->leaf_cert_len);
        memcpy(certsp->leaf_cert,
               ctx->intermediate_signed_client_cert_pem,
               certsp->leaf_cert_len);

        certsp->pkey_len = ctx->intermediate_signed_client_key_pem_length;
        certsp->pkey = rd_kafka_mem_malloc(rk, certsp->pkey_len);
        memcpy(certsp->pkey,
               ctx->intermediate_signed_client_key_pem,
               certsp->pkey_len);

        certsp->chain_certs_cnt = 1;
        certsp->chain_cert_lens = rd_kafka_mem_malloc(
                rk, sizeof(certsp->chain_cert_lens[0]) * 1);
        certsp->chain_certs_buf = rd_kafka_mem_malloc(
                rk, ctx->intermediate_signed_intermediate_cert_pem_length);
        certsp->chain_cert_lens[0] =
                ctx->intermediate_signed_intermediate_cert_pem_length;
        memcpy(certsp->chain_certs_buf,
               ctx->intermediate_signed_intermediate_cert_pem,
               certsp->chain_cert_lens[0]);

        certsp->format = RD_KAFKA_CERT_ENC_PEM;
        return RD_KAFKA_CERT_FETCH_OK;
}


typedef rd_kafka_cert_fetch_cb_res_t (*ssl_cert_fetch_cb) (rd_kafka_t *rk,
                                  const char *broker_name,
                                  int32_t broker_id,
                                  rd_kafka_ssl_cert_fetch_cb_certs_t *certsp,
                                  char *errstr, size_t errstr_size,
                                  void *opaque);

static void do_test_ssl_cert (struct test_context *ctx, int expect_ok, ssl_cert_fetch_cb cb) {
        // Test needs to.....
        // ....construct a rdkafka config with a SSL CB that returns a GOOD cert
        // ....make a metadata request of some kind

        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t md_res;
        char errstr[512];

        test_conf_init(&conf, NULL, 10000);
        rd_kafka_conf_set_ssl_cert_fetch_cb(conf, cb);
        rd_kafka_conf_set_opaque(conf, ctx);

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                rd_kafka_conf_destroy(conf);
                TEST_FAIL("failed rd_kafka_new: %s\n", errstr);
                return;
        }

        md_res = rd_kafka_metadata(rk, 1, NULL, &md, 4000);
        if (md_res != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_destroy(rk);
                if (expect_ok) {
                        TEST_FAIL("failed rd_kafka_metadata: %s\n", rd_kafka_err2str(md_res));
                }
                return;
        }
        if (!expect_ok) {
                TEST_FAIL("passed rd_kafka_metadata, but expected to fail\n");
        }
        rd_kafka_metadata_destroy(md);
        rd_kafka_destroy(rk);
}


static int read_ssl_file_from_env (const char *envvar, char **dest, size_t *length) {
        const char *path_val;
        size_t szrt;
        FILE *fh;

        path_val = test_getenv(envvar, NULL);
        if (!path_val) {
                TEST_SKIP("Test requires %s env var\n", envvar);
                return 1;
        }
        fh = fopen(path_val, "rb");
        if (!fh) {
                TEST_FAIL("File %s could not be opened: %m\n", path_val);
                return 0;
        }
        fseek(fh, 0, SEEK_END);
        *length = ftell(fh);
        fseek(fh, 0, SEEK_SET);

        *dest = malloc(*length);
        // should probably handle short reads, but *shrug*
        szrt = fread(*dest, 1, *length, fh);
        if (!szrt) {
                TEST_FAIL("File %s could not be read: %m\n", path_val);
                fclose(fh);
                return 0;
        }
        fclose(fh);
        return 0;
}

static void test_good_direct (struct test_context *ctx) {
        SUB_TEST("cert directly signed by ca");
        do_test_ssl_cert(ctx, 1, ssl_client_cert_cb_good_direct);
        SUB_TEST_PASS();
}

static void test_good_intermediate (struct test_context *ctx) {
        SUB_TEST("cert signed by valid intermediate");
        do_test_ssl_cert(ctx, 1, ssl_client_cert_cb_good_via_intermediate);
        SUB_TEST_PASS();
}

static void test_bad (struct test_context *ctx) {
        SUB_TEST("cert without valid signature");
        do_test_ssl_cert(ctx, 0, ssl_client_cert_cb_bad);
        SUB_TEST_PASS();
}

int main_0125_ssl_cb (int argc, char **argv) {
        struct test_context ctx = RD_ZERO_INIT;
        int missing_env_vars = 0;

        if (!test_check_builtin("ssl")) {
                TEST_SKIP("Test requires SSL support\n");
                goto cleanup;
        }

        missing_env_vars += read_ssl_file_from_env(
            "RDK_SSL_ca_pem",
            &ctx.ca_cert_pem,
            &ctx.ca_cert_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_SSL_pub_pem",
            &ctx.direct_signed_client_cert_pem,
            &ctx.direct_signed_client_cert_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_SSL_priv_pem",
            &ctx.direct_signed_client_key_pem,
            &ctx.direct_signed_client_key_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_UNTRUSTEDSSL_pub_pem",
            &ctx.self_signed_client_cert_pem,
            &ctx.self_signed_client_cert_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_UNTRUSTEDSSL_priv_pem",
            &ctx.self_signed_client_key_pem,
            &ctx.self_signed_client_key_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_INTERMEDIATESSL_pub_pem",
            &ctx.intermediate_signed_client_cert_pem,
            &ctx.intermediate_signed_client_cert_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_INTERMEDIATESSL_priv_pem",
            &ctx.intermediate_signed_client_key_pem,
            &ctx.intermediate_signed_client_key_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_INTERMEDIATESSL_intermediate_pub_pem",
            &ctx.intermediate_signed_intermediate_cert_pem,
            &ctx.intermediate_signed_intermediate_cert_pem_length
        );
        missing_env_vars += read_ssl_file_from_env(
            "RDK_INTERMEDIATESSL_intermediate_priv_pem",
            &ctx.intermediate_signed_intermediate_key_pem,
            &ctx.intermediate_signed_intermediate_key_pem_length
        );
        if (missing_env_vars > 0) {
                TEST_SAY("Test is missing required env vars, skipping.");
                goto cleanup;
        }

        test_good_direct(&ctx);
        test_good_intermediate(&ctx);
        test_bad(&ctx);

        cleanup:
        free(ctx.ca_cert_pem);
        free(ctx.direct_signed_client_cert_pem);
        free(ctx.direct_signed_client_key_pem);
        free(ctx.self_signed_client_cert_pem);
        free(ctx.self_signed_client_key_pem);
        free(ctx.intermediate_signed_client_cert_pem);
        free(ctx.intermediate_signed_client_key_pem);
        free(ctx.intermediate_signed_intermediate_cert_pem);
        free(ctx.intermediate_signed_intermediate_key_pem);
        return 0;
}
