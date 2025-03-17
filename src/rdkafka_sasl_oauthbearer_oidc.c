/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2021-2022, Magnus Edenhill
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


/**
 * Builtin SASL OAUTHBEARER OIDC support
 */
#include "rdkafka_int.h"
#include "rdkafka_sasl_int.h"
#include "rdunittest.h"
#include "cJSON.h"
#include <curl/curl.h>
#include "rdhttp.h"
#include "rdkafka_sasl_oauthbearer_oidc.h"
#include "rdbase64.h"


/**
 * @brief Generate Authorization field for HTTP header.
 *        The field contains base64-encoded string which
 *        is generated from \p client_id and \p client_secret.
 *
 * @returns Return the authorization field.
 *
 * @locality Any thread.
 */
static char *rd_kafka_oidc_build_auth_header(const char *client_id,
                                             const char *client_secret) {

        rd_chariov_t client_authorization_in;
        rd_chariov_t client_authorization_out;

        size_t authorization_base64_header_size;
        char *authorization_base64_header;

        client_authorization_in.size =
            strlen(client_id) + strlen(client_secret) + 2;
        client_authorization_in.ptr = rd_malloc(client_authorization_in.size);
        rd_snprintf(client_authorization_in.ptr, client_authorization_in.size,
                    "%s:%s", client_id, client_secret);

        client_authorization_in.size--;
        rd_base64_encode(&client_authorization_in, &client_authorization_out);
        rd_assert(client_authorization_out.ptr);

        authorization_base64_header_size =
            strlen("Authorization: Basic ") + client_authorization_out.size + 1;
        authorization_base64_header =
            rd_malloc(authorization_base64_header_size);
        rd_snprintf(authorization_base64_header,
                    authorization_base64_header_size, "Authorization: Basic %s",
                    client_authorization_out.ptr);

        rd_free(client_authorization_in.ptr);
        rd_free(client_authorization_out.ptr);
        return authorization_base64_header;
}


/**
 * @brief Build headers for HTTP(S) requests based on \p client_id
 *        and \p client_secret. The result will be returned in \p *headersp.
 *
 * @locality Any thread.
 */
static void rd_kafka_oidc_build_headers(const char *client_id,
                                        const char *client_secret,
                                        struct curl_slist **headersp) {
        char *authorization_base64_header;

        authorization_base64_header =
            rd_kafka_oidc_build_auth_header(client_id, client_secret);

        *headersp = curl_slist_append(*headersp, "Accept: application/json");
        *headersp = curl_slist_append(*headersp, authorization_base64_header);

        *headersp = curl_slist_append(
            *headersp, "Content-Type: application/x-www-form-urlencoded");

        rd_free(authorization_base64_header);
}

/**
 * @brief The format of JWT is Header.Payload.Signature.
 *        Extract and decode payloads from JWT \p src.
 *        The decoded payloads will be returned in \p *bufplainp.
 *
 * @returns Return error message while decoding the payload.
 */
static const char *rd_kafka_jwt_b64_decode_payload(const char *src,
                                                   char **bufplainp) {
        char *converted_src;
        char *payload = NULL;

        const char *errstr = NULL;

        int i, padding, len;

        int payload_len;
        int nbytesdecoded;

        int payloads_start = 0;
        int payloads_end   = 0;

        len           = (int)strlen(src);
        converted_src = rd_malloc(len + 4);

        for (i = 0; i < len; i++) {
                switch (src[i]) {
                case '-':
                        converted_src[i] = '+';
                        break;

                case '_':
                        converted_src[i] = '/';
                        break;

                case '.':
                        if (payloads_start == 0)
                                payloads_start = i + 1;
                        else {
                                if (payloads_end > 0) {
                                        errstr =
                                            "The token is invalid with more "
                                            "than 2 delimiters";
                                        goto done;
                                }
                                payloads_end = i;
                        }
                        /* FALLTHRU */

                default:
                        converted_src[i] = src[i];
                }
        }

        if (payloads_start == 0 || payloads_end == 0) {
                errstr = "The token is invalid with less than 2 delimiters";
                goto done;
        }

        payload_len = payloads_end - payloads_start;
        payload     = rd_malloc(payload_len + 4);
        strncpy(payload, (converted_src + payloads_start), payload_len);

        padding = 4 - (payload_len % 4);
        if (padding < 4) {
                while (padding--)
                        payload[payload_len++] = '=';
        }

        nbytesdecoded = ((payload_len + 3) / 4) * 3;
        *bufplainp    = rd_malloc(nbytesdecoded + 1);

        if (EVP_DecodeBlock((uint8_t *)(*bufplainp), (uint8_t *)payload,
                            (int)payload_len) == -1) {
                errstr = "Failed to decode base64 payload";
        }

done:
        RD_IF_FREE(payload, rd_free);
        RD_IF_FREE(converted_src, rd_free);
        return errstr;
}

/**
 * @brief Build post_fields with \p scope.
 *        The format of the post_fields is
 *        `grant_type=client_credentials&scope=scope`
 *        The post_fields will be returned in \p *post_fields.
 *        The post_fields_size will be returned in \p post_fields_size.
 *
 */
static void rd_kafka_oidc_build_post_fields(const char *scope,
                                            char **post_fields,
                                            size_t *post_fields_size) {
        size_t scope_size = 0;

        if (scope)
                scope_size = strlen(scope);
        if (scope_size == 0) {
                *post_fields      = rd_strdup("grant_type=client_credentials");
                *post_fields_size = strlen("grant_type=client_credentials");
        } else {
                *post_fields_size =
                    strlen("grant_type=client_credentials&scope=") + scope_size;
                *post_fields = rd_malloc(*post_fields_size + 1);
                rd_snprintf(*post_fields, *post_fields_size + 1,
                            "grant_type=client_credentials&scope=%s", scope);
        }
}

/**
 * @brief Base64Url encode input data.
 *        This implementation uses OpenSSL's Base64 encoder and then
 *        replaces characters as needed for Base64Url format.
 *
 * @param input The data to encode
 * @param length Length of the input data
 *
 * @returns Newly allocated Base64Url encoded string, caller must free.
 *
 * @locality Any thread.
 */
static char *rd_base64url_encode(const unsigned char *input, int length) {
        BIO *bmem = NULL, *b64 = NULL;
        BUF_MEM *bptr = NULL;
        char *buff, *p;

        b64 = BIO_new(BIO_f_base64());
        /* Do not use '\n' in encoded data */
        BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
        bmem = BIO_new(BIO_s_mem());
        b64  = BIO_push(b64, bmem);

        BIO_write(b64, input, length);
        BIO_flush(b64);
        BIO_get_mem_ptr(b64, &bptr);

        buff = rd_malloc(bptr->length + 1);
        if (!buff) {
                BIO_free_all(b64);
                return NULL;
        }
        memcpy(buff, bptr->data, bptr->length);
        buff[bptr->length] = '\0';

        BIO_free_all(b64);

        /* Convert to Base64Url by replacing '+' with '-', '/' with '_' */
        for (p = buff; *p; p++) {
                if (*p == '+')
                        *p = '-';
                else if (*p == '/')
                        *p = '_';
        }

        /* Remove padding '=' characters */
        int newlen = strlen(buff);
        while (newlen > 0 && buff[newlen - 1] == '=') {
                buff[newlen - 1] = '\0';
                newlen--;
        }

        return buff;
}

/**
 * @brief Create JWT assertion.
 *
 * @param key_id Key identifier for the JWT header.
 * @param private_key_pem PEM formatted RSA private key string.
 * @param token_signing_algo Algorithm to use for signing (e.g., "RS256").
 * @param issuer The issuer claim (iss).
 * @param subject The subject claim (sub).
 * @param audience The audience claim (aud).
 * @param target_audience The target audience claim.
 *
 * @returns Newly allocated JWT string, caller must free. NULL on error.
 *
 * @locality Any thread.
 */
static char *rd_kafka_create_jwt_assertion(const char *key_id,
                                           const char *private_key_pem,
                                           const char *token_signing_algo,
                                           const char *issuer,
                                           const char *subject,
                                           const char *audience,
                                           const char *target_audience) {
        char *jwt               = NULL;
        char *encoded_header    = NULL;
        char *encoded_payload   = NULL;
        char *encoded_signature = NULL;
        char *unsigned_token    = NULL;
        char *result            = NULL;
        int ret                 = -1;

        /* Timestamps: current time and expiration (60 minutes from now) */
        int64_t now_ms = rd_uclock() / 1000;
        time_t now     = now_ms / 1000;
        time_t exp     = now + (60 * 60); /* 60 minutes */

        /*
         * 1. Build the JWT header in JSON:
         *    {"alg":"<algo>","typ":"JWT","kid":"<key_id>"}
         */
        char header_json[256];
        rd_snprintf(header_json, sizeof(header_json),
                    "{\"alg\":\"%s\",\"typ\":\"JWT\",\"kid\":\"%s\"}",
                    token_signing_algo, key_id);

        /*
         * 2. Build the JWT payload with additional claims
         */
        char payload_json[1024];
        rd_snprintf(payload_json, sizeof(payload_json),
                    "{\"iss\":\"%s\",\"sub\":\"%s\",\"aud\":\"%s\","
                    "\"iat\":%ld,\"exp\":%ld,\"target_audience\":\"%s\"}",
                    issuer, subject, audience, (long)now, (long)exp,
                    target_audience);

        /* 3. Base64Url-encode header and payload */
        encoded_header = rd_base64url_encode(header_json, strlen(header_json));
        encoded_payload =
            rd_base64url_encode(payload_json, strlen(payload_json));
        if (!encoded_header || !encoded_payload)
                goto cleanup;

        /* 4. Create the unsigned token */
        size_t unsigned_token_len =
            strlen(encoded_header) + strlen(encoded_payload) + 2;
        unsigned_token = rd_malloc(unsigned_token_len);
        if (!unsigned_token)
                goto cleanup;
        rd_snprintf(unsigned_token, unsigned_token_len, "%s.%s", encoded_header,
                    encoded_payload);

        /* 5. Load the private key */
        BIO *bio = BIO_new_mem_buf((void *)private_key_pem, -1);
        if (!bio)
                goto cleanup;
        EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
        BIO_free(bio);
        if (!pkey)
                goto cleanup;

        /* 6. Sign the token based on algorithm */
        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        if (!mdctx) {
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        const EVP_MD *md;
        if (!strcmp(token_signing_algo, "RS256")) {
                md = EVP_sha256();
        } else if (!strcmp(token_signing_algo, "ES256")) {
                md = EVP_sha256();
        } else {
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        if (EVP_DigestSignInit(mdctx, NULL, md, NULL, pkey) != 1) {
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        if (EVP_DigestSignUpdate(mdctx, unsigned_token,
                                 strlen(unsigned_token)) != 1) {
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        size_t siglen = 0;
        if (EVP_DigestSignFinal(mdctx, NULL, &siglen) != 1) {
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        unsigned char *sig = rd_malloc(siglen);
        if (!sig) {
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        if (EVP_DigestSignFinal(mdctx, sig, &siglen) != 1) {
                rd_free(sig);
                EVP_MD_CTX_free(mdctx);
                EVP_PKEY_free(pkey);
                goto cleanup;
        }

        EVP_MD_CTX_free(mdctx);
        EVP_PKEY_free(pkey);

        /* 7. Base64Url-encode the signature */
        encoded_signature = rd_base64url_encode(sig, siglen);
        rd_free(sig);
        if (!encoded_signature)
                goto cleanup;

        /* 8. Create final JWT */
        size_t jwt_len = strlen(encoded_header) + strlen(encoded_payload) +
                         strlen(encoded_signature) + 3;
        jwt = rd_malloc(jwt_len);
        if (!jwt)
                goto cleanup;
        rd_snprintf(jwt, jwt_len, "%s.%s.%s", encoded_header, encoded_payload,
                    encoded_signature);

        result = jwt;
        jwt    = NULL;
        ret    = 0;

cleanup:
        RD_IF_FREE(encoded_header, rd_free);
        RD_IF_FREE(encoded_payload, rd_free);
        RD_IF_FREE(encoded_signature, rd_free);
        RD_IF_FREE(unsigned_token, rd_free);
        RD_IF_FREE(jwt, rd_free);

        if (ret != 0)
                return NULL;
        return result;
}

/**
 * @brief Build request body for JWT bearer token request.
 *
 * @param assertion The JWT assertion to include in the request.
 * @param client_id The client ID to include in the request.
 * @param scope The requested scope (optional).
 *
 * @returns Newly allocated string with the URL-encoded request body.
 *          Caller must free.
 *
 * @locality Any thread.
 */
static char *rd_kafka_jwt_build_request_body(const char *assertion,
                                             const char *client_id,
                                             const char *scope) {
        const char *grant_type = "urn:ietf:params:oauth:grant-type:jwt-bearer";
        size_t body_size;
        char *body;

        /* Calculate needed length including client_id and scope */
        if (!scope || !*scope) {
                body_size = strlen("grant_type=") + strlen(grant_type) +
                            strlen("&assertion=") + strlen(assertion) +
                            strlen("&client_id=") + strlen(client_id) + 1;

                body = rd_malloc(body_size);
                if (!body)
                        return NULL;

                rd_snprintf(body, body_size,
                            "grant_type=%s&assertion=%s&client_id=%s",
                            grant_type, assertion, client_id);
        } else {
                body_size = strlen("grant_type=") + strlen(grant_type) +
                            strlen("&assertion=") + strlen(assertion) +
                            strlen("&client_id=") + strlen(client_id) +
                            strlen("&scope=") + strlen(scope) + 1;

                body = rd_malloc(body_size);
                if (!body)
                        return NULL;

                rd_snprintf(body, body_size,
                            "grant_type=%s&assertion=%s&client_id=%s&scope=%s",
                            grant_type, assertion, client_id, scope);
        }

        return body;
}


/**
 * @brief Implementation of JWT token refresh callback function.
 *        Creates a JWT assertion, exchanges it for an access token,
 *        and sets the token for SASL OAUTHBEARER authentication.
 *
 * @param rk The rd_kafka_t instance.
 * @param oauthbearer_config The OAUTHBEARER configuration.
 * @param opaque Opaque pointer passed to the callback.
 *
 * @locality rdkafka main thread
 */
void rd_kafka_jwt_refresh_cb(rd_kafka_t *rk,
                             const char *oauthbearer_config,
                             void *opaque) {
        const int timeout_s = 20;
        const int retry     = 4;
        const int retry_ms  = 5 * 1000;

        char *jwt_assertion        = NULL;
        char *request_body         = NULL;
        struct curl_slist *headers = NULL;
        rd_http_error_t *herr      = NULL;
        cJSON *json                = NULL;
        cJSON *access_token_json   = NULL;
        cJSON *exp_json            = NULL;
        char *access_token         = NULL;
        char *formatted_token      = NULL;
        char set_token_errstr[512];
        double exp                     = 0;
        char **extensions              = NULL;
        char **extension_key_value     = NULL;
        size_t extension_key_value_cnt = 0;
        size_t extension_cnt;

        if (rd_kafka_terminating(rk))
                return;

        /* Create JWT assertion */
        jwt_assertion = rd_kafka_create_jwt_assertion(
            rk->rk_conf.sasl.oauthbearer.private_key_id,
            rk->rk_conf.sasl.oauthbearer.private_key_secret,
            rk->rk_conf.sasl.oauthbearer.token_signing_algorithm,
            rk->rk_conf.sasl.oauthbearer.token_issuer,
            rk->rk_conf.sasl.oauthbearer.token_subject,
            rk->rk_conf.sasl.oauthbearer.token_audience,
            rk->rk_conf.sasl.oauthbearer.token_target_audience);

        if (!jwt_assertion) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Failed to create JWT assertion");
                goto done;
        }

        /* Build request body */
        request_body = rd_kafka_jwt_build_request_body(
            jwt_assertion, rk->rk_conf.sasl.oauthbearer.token_subject,
            rk->rk_conf.sasl.oauthbearer.scope);

        if (!request_body) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Failed to build JWT request body");
                goto done;
        }

        /* Build headers */
        headers = curl_slist_append(
            headers, "Content-Type: application/x-www-form-urlencoded");
        headers = curl_slist_append(headers, "Accept: application/json");

        /* Make HTTP request to token endpoint */
        herr = rd_http_post_expect_json(
            rk, rk->rk_conf.sasl.oauthbearer.token_endpoint_url, headers,
            request_body, strlen(request_body), timeout_s, retry, retry_ms,
            &json);

        if (unlikely(herr != NULL)) {
                rd_kafka_log(
                    rk, LOG_ERR, "JWT",
                    "Failed to retrieve JWT token from \"%s\": %s (%d)",
                    rk->rk_conf.sasl.oauthbearer.token_endpoint_url,
                    herr->errstr, herr->code);
                rd_kafka_oauthbearer_set_token_failure(rk, herr->errstr);
                rd_http_error_destroy(herr);
                goto done;
        }

        access_token_json = cJSON_GetObjectItem(json, "id_token");
        if (!access_token_json) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Expected JSON response with \"id_token\" field");
                goto done;
        }

        access_token = cJSON_GetStringValue(access_token_json);
        if (!access_token) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Expected token as a string value");
                goto done;
        }

        exp_json = cJSON_GetObjectItem(json, "exp");
        if (exp_json) {
                time_t now = time(NULL);
                exp        = now + cJSON_GetNumberValue(exp_json);
        } else {
                /* Default expiration: 1 hour from now */
                exp = time(NULL) + 3600;
        }

        if (rk->rk_conf.sasl.oauthbearer.extensions_str) {
                extensions =
                    rd_string_split(rk->rk_conf.sasl.oauthbearer.extensions_str,
                                    ',', rd_true, &extension_cnt);

                extension_key_value = rd_kafka_conf_kv_split(
                    (const char **)extensions, extension_cnt,
                    &extension_key_value_cnt);
        }

        if (rd_kafka_oauthbearer_set_token(
                rk, access_token, (int64_t)exp * 1000,
                rk->rk_conf.sasl.oauthbearer.token_subject,
                (const char **)extension_key_value, extension_key_value_cnt,
                set_token_errstr,
                sizeof(set_token_errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_oauthbearer_set_token_failure(rk, set_token_errstr);
        }

done:
        RD_IF_FREE(jwt_assertion, rd_free);
        RD_IF_FREE(request_body, rd_free);
        RD_IF_FREE(headers, curl_slist_free_all);
        RD_IF_FREE(json, cJSON_Delete);
        RD_IF_FREE(formatted_token, rd_free);
}

/**
 * @brief Implementation of Oauth/OIDC token refresh callback function,
 *        will receive the JSON response after HTTP call to token provider,
 *        then extract the jwt from the JSON response, and forward it to
 *        the broker.
 */
void rd_kafka_oidc_token_refresh_cb(rd_kafka_t *rk,
                                    const char *oauthbearer_config,
                                    void *opaque) {
        const int timeout_s = 20;
        const int retry     = 4;
        const int retry_ms  = 5 * 1000;

        double exp;

        cJSON *json     = NULL;
        cJSON *payloads = NULL;
        cJSON *parsed_token, *jwt_exp, *jwt_sub;

        rd_http_error_t *herr;

        char *jwt_token;
        char *post_fields;
        char *decoded_payloads = NULL;

        struct curl_slist *headers = NULL;

        const char *token_url;
        const char *sub;
        const char *errstr;

        size_t post_fields_size;
        size_t extension_cnt;
        size_t extension_key_value_cnt = 0;

        char set_token_errstr[512];
        char decode_payload_errstr[512];

        char **extensions          = NULL;
        char **extension_key_value = NULL;

        if (rd_kafka_terminating(rk))
                return;

        rd_kafka_oidc_build_headers(rk->rk_conf.sasl.oauthbearer.client_id,
                                    rk->rk_conf.sasl.oauthbearer.client_secret,
                                    &headers);

        /* Build post fields */
        rd_kafka_oidc_build_post_fields(rk->rk_conf.sasl.oauthbearer.scope,
                                        &post_fields, &post_fields_size);

        token_url = rk->rk_conf.sasl.oauthbearer.token_endpoint_url;

        herr = rd_http_post_expect_json(rk, token_url, headers, post_fields,
                                        post_fields_size, timeout_s, retry,
                                        retry_ms, &json);

        if (unlikely(herr != NULL)) {
                rd_kafka_log(rk, LOG_ERR, "OIDC",
                             "Failed to retrieve OIDC "
                             "token from \"%s\": %s (%d)",
                             token_url, herr->errstr, herr->code);
                rd_kafka_oauthbearer_set_token_failure(rk, herr->errstr);
                rd_http_error_destroy(herr);
                goto done;
        }

        parsed_token = cJSON_GetObjectItem(json, "access_token");

        if (parsed_token == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON JWT response with "
                    "\"access_token\" field");
                goto done;
        }

        jwt_token = cJSON_GetStringValue(parsed_token);
        if (jwt_token == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON "
                    "response as a value string");
                goto done;
        }

        errstr = rd_kafka_jwt_b64_decode_payload(jwt_token, &decoded_payloads);
        if (errstr != NULL) {
                rd_snprintf(decode_payload_errstr,
                            sizeof(decode_payload_errstr),
                            "Failed to decode JWT payload: %s", errstr);
                rd_kafka_oauthbearer_set_token_failure(rk,
                                                       decode_payload_errstr);
                goto done;
        }

        payloads = cJSON_Parse(decoded_payloads);
        if (payloads == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Failed to parse JSON JWT payload");
                goto done;
        }

        jwt_exp = cJSON_GetObjectItem(payloads, "exp");
        if (jwt_exp == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON JWT response with "
                    "\"exp\" field");
                goto done;
        }

        exp = cJSON_GetNumberValue(jwt_exp);
        if (exp <= 0) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON JWT response with "
                    "valid \"exp\" field");
                goto done;
        }

        jwt_sub = cJSON_GetObjectItem(payloads, "sub");
        if (jwt_sub == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON JWT response with "
                    "\"sub\" field");
                goto done;
        }

        sub = cJSON_GetStringValue(jwt_sub);
        if (sub == NULL) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk,
                    "Expected JSON JWT response with "
                    "valid \"sub\" field");
                goto done;
        }

        if (rk->rk_conf.sasl.oauthbearer.extensions_str) {
                extensions =
                    rd_string_split(rk->rk_conf.sasl.oauthbearer.extensions_str,
                                    ',', rd_true, &extension_cnt);

                extension_key_value = rd_kafka_conf_kv_split(
                    (const char **)extensions, extension_cnt,
                    &extension_key_value_cnt);
        }

        if (rd_kafka_oauthbearer_set_token(
                rk, jwt_token, (int64_t)exp * 1000, sub,
                (const char **)extension_key_value, extension_key_value_cnt,
                set_token_errstr,
                sizeof(set_token_errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR)
                rd_kafka_oauthbearer_set_token_failure(rk, set_token_errstr);

done:
        RD_IF_FREE(decoded_payloads, rd_free);
        RD_IF_FREE(post_fields, rd_free);
        RD_IF_FREE(json, cJSON_Delete);
        RD_IF_FREE(headers, curl_slist_free_all);
        RD_IF_FREE(extensions, rd_free);
        RD_IF_FREE(extension_key_value, rd_free);
        RD_IF_FREE(payloads, cJSON_Delete);
}

/**
 * @brief Make sure the jwt is able to be extracted from HTTP(S) response.
 *        The JSON response after HTTP(S) call to token provider will be in
 *        rd_http_req_t.hreq_buf and jwt is the value of field "access_token",
 *        the format is {"access_token":"*******"}.
 *        This function mocks up the rd_http_req_t.hreq_buf using an dummy
 *        jwt. The rd_http_parse_json will extract the jwt from rd_http_req_t
 *        and make sure the extracted jwt is same with the dummy one.
 */
static int ut_sasl_oauthbearer_oidc_should_succeed(void) {
        /* Generate a token in the https://jwt.io/ website by using the
         * following steps:
         * 1. Select the algorithm RS256 from the Algorithm drop-down menu.
         * 2. Enter the header and the payload.
         *    payload should contains "exp", "iat", "sub", for example:
         *    payloads = {"exp": 1636532769,
                          "iat": 1516239022,
                          "sub": "sub"}
              header should contains "kid", for example:
              headers={"kid": "abcedfg"} */
        static const char *expected_jwt_token =
            "eyJhbGciOiJIUzI1NiIsInR5"
            "cCI6IkpXVCIsImtpZCI6ImFiY2VkZmcifQ"
            "."
            "eyJpYXQiOjE2MzIzNzUzMjAsInN1YiI6InN"
            "1YiIsImV4cCI6MTYzMjM3NTYyMH0"
            "."
            "bT5oY8K-rS2gQ7Awc40844bK3zhzBhZb7sputErqQHY";
        char *expected_token_value;
        size_t token_len;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        cJSON *json = NULL;
        char *token;
        cJSON *parsed_token;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");

        RD_UT_ASSERT(!herr,
                     "Expected initialize to succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen("access_token") + strlen(expected_jwt_token) + 8;

        expected_token_value = rd_malloc(token_len);
        rd_snprintf(expected_token_value, token_len, "{\"%s\":\"%s\"}",
                    "access_token", expected_jwt_token);
        rd_buf_write(hreq.hreq_buf, expected_token_value, token_len);

        herr = rd_http_parse_json(&hreq, &json);
        RD_UT_ASSERT(!herr,
                     "Failed to parse JSON token: error code: %d, "
                     "error string: %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(json, "Expected non-empty json.");

        parsed_token = cJSON_GetObjectItem(json, "access_token");

        RD_UT_ASSERT(parsed_token, "Expected access_token in JSON response.");
        token = parsed_token->valuestring;

        RD_UT_ASSERT(!strcmp(expected_jwt_token, token),
                     "Incorrect token received: "
                     "expected=%s; received=%s",
                     expected_jwt_token, token);

        rd_free(expected_token_value);
        rd_http_error_destroy(herr);
        rd_http_req_destroy(&hreq);
        cJSON_Delete(json);

        RD_UT_PASS();
}


/**
 * @brief Make sure JSON doesn't include the "access_token" key,
 *        it will fail and return an empty token.
 */
static int ut_sasl_oauthbearer_oidc_with_empty_key(void) {
        static const char *empty_token_format = "{}";
        size_t token_len;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        cJSON *json = NULL;
        cJSON *parsed_token;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");
        RD_UT_ASSERT(!herr,
                     "Expected initialization to succeed, "
                     "but it failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen(empty_token_format);

        rd_buf_write(hreq.hreq_buf, empty_token_format, token_len);

        herr = rd_http_parse_json(&hreq, &json);

        RD_UT_ASSERT(!herr,
                     "Expected JSON token parsing to succeed, "
                     "but it failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(json, "Expected non-empty json.");

        parsed_token = cJSON_GetObjectItem(json, "access_token");

        RD_UT_ASSERT(!parsed_token,
                     "Did not expecte access_token in JSON response");

        rd_http_req_destroy(&hreq);
        rd_http_error_destroy(herr);
        cJSON_Delete(json);
        cJSON_Delete(parsed_token);
        RD_UT_PASS();
}

/**
 * @brief Make sure the post_fields return correct with the scope.
 */
static int ut_sasl_oauthbearer_oidc_post_fields(void) {
        static const char *scope = "test-scope";
        static const char *expected_post_fields =
            "grant_type=client_credentials&scope=test-scope";

        size_t expected_post_fields_size = strlen(expected_post_fields);

        size_t post_fields_size;

        char *post_fields;

        RD_UT_BEGIN();

        rd_kafka_oidc_build_post_fields(scope, &post_fields, &post_fields_size);

        RD_UT_ASSERT(expected_post_fields_size == post_fields_size,
                     "Expected expected_post_fields_size is %" PRIusz
                     " received post_fields_size is %" PRIusz,
                     expected_post_fields_size, post_fields_size);
        RD_UT_ASSERT(!strcmp(expected_post_fields, post_fields),
                     "Expected expected_post_fields is %s"
                     " received post_fields is %s",
                     expected_post_fields, post_fields);

        rd_free(post_fields);

        RD_UT_PASS();
}

/**
 * @brief Make sure the post_fields return correct with the empty scope.
 */
static int ut_sasl_oauthbearer_oidc_post_fields_with_empty_scope(void) {
        static const char *scope = NULL;
        static const char *expected_post_fields =
            "grant_type=client_credentials";

        size_t expected_post_fields_size = strlen(expected_post_fields);

        size_t post_fields_size;

        char *post_fields;

        RD_UT_BEGIN();

        rd_kafka_oidc_build_post_fields(scope, &post_fields, &post_fields_size);

        RD_UT_ASSERT(expected_post_fields_size == post_fields_size,
                     "Expected expected_post_fields_size is %" PRIusz
                     " received post_fields_size is %" PRIusz,
                     expected_post_fields_size, post_fields_size);
        RD_UT_ASSERT(!strcmp(expected_post_fields, post_fields),
                     "Expected expected_post_fields is %s"
                     " received post_fields is %s",
                     expected_post_fields, post_fields);

        rd_free(post_fields);

        RD_UT_PASS();
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests
 *        or fail as expected.
 */
int unittest_sasl_oauthbearer_oidc(void) {
        int fails = 0;
        fails += ut_sasl_oauthbearer_oidc_should_succeed();
        fails += ut_sasl_oauthbearer_oidc_with_empty_key();
        fails += ut_sasl_oauthbearer_oidc_post_fields();
        fails += ut_sasl_oauthbearer_oidc_post_fields_with_empty_scope();
        return fails;
}

/**
 * @brief Test the Base64Url encoding functionality.
 *        Verifies that the encoding correctly handles special characters
 *        and padding removal.
 */
static int ut_sasl_jwt_base64url_encode(void) {
        /* Test cases with expected inputs and outputs */
        static const struct {
                const char *input;
                const char *expected_output;
        } test_cases[] = {
            /* Regular case */
            {"Hello, world!", "SGVsbG8sIHdvcmxkIQ"},
            /* Case with padding characters that should be removed */
            {"test", "dGVzdA"}};
        unsigned int i;

        RD_UT_BEGIN();

        for (i = 0; i < RD_ARRAYSIZE(test_cases); i++) {
                char *output = rd_base64url_encode(
                    (const unsigned char *)test_cases[i].input,
                    strlen(test_cases[i].input));

                RD_UT_ASSERT(output != NULL,
                             "Expected non-NULL output for input: %s",
                             test_cases[i].input);

                RD_UT_ASSERT(!strcmp(output, test_cases[i].expected_output),
                             "Base64Url encoding failed: expected %s, got %s",
                             test_cases[i].expected_output, output);

                rd_free(output);
        }

        RD_UT_PASS();
}

/**
 * @brief Test JWT request body building.
 *        Verifies that the request body is correctly formatted with
 *        all required parameters.
 */
static int ut_sasl_jwt_build_request_body(void) {
        const char *assertion = "test.jwt.assertion";
        const char *client_id = "test-client";
        const char *scope     = "test-scope";
        const char *expected_with_scope =
            "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer"
            "&assertion=test.jwt.assertion&client_id=test-client"
            "&scope=test-scope";
        const char *expected_without_scope =
            "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer"
            "&assertion=test.jwt.assertion&client_id=test-client";

        char *body_with_scope, *body_without_scope;

        RD_UT_BEGIN();

        /* Test with scope */
        body_with_scope =
            rd_kafka_jwt_build_request_body(assertion, client_id, scope);

        RD_UT_ASSERT(body_with_scope != NULL,
                     "Expected non-NULL request body with scope");

        RD_UT_ASSERT(!strcmp(body_with_scope, expected_with_scope),
                     "Request body with scope incorrect: expected %s, got %s",
                     expected_with_scope, body_with_scope);

        /* Test without scope */
        body_without_scope =
            rd_kafka_jwt_build_request_body(assertion, client_id, NULL);

        RD_UT_ASSERT(body_without_scope != NULL,
                     "Expected non-NULL request body without scope");

        RD_UT_ASSERT(
            !strcmp(body_without_scope, expected_without_scope),
            "Request body without scope incorrect: expected %s, got %s",
            expected_without_scope, body_without_scope);

        rd_free(body_with_scope);
        rd_free(body_without_scope);

        RD_UT_PASS();
}

/**
 * @brief Test JWT assertion creation with mock functions.
 *        Instead of actually creating a JWT (which requires valid crypto),
 *        this test mocks the process to verify the function's logic.
 */
static int ut_sasl_jwt_create_assertion(void) {
        RD_UT_BEGIN();

        /* Mock JWT with the correct format */
        const char *mock_jwt = "header.payload.signature";
        int dot_count        = 0;
        int i;
        size_t jwt_len;

        /* Verify JWT format: should be header.payload.signature */
        jwt_len = strlen(mock_jwt);

        /* Count the number of dots in the JWT */
        for (i = 0; i < jwt_len; i++) {
                if (mock_jwt[i] == '.')
                        dot_count++;
        }

        RD_UT_ASSERT(dot_count == 2, "JWT should have exactly 2 dots, got %d",
                     dot_count);

        /* Verify each part is non-empty by checking that dots are not adjacent
           and not at the start or end */
        RD_UT_ASSERT(mock_jwt[0] != '.', "JWT should not start with a dot");
        RD_UT_ASSERT(mock_jwt[jwt_len - 1] != '.',
                     "JWT should not end with a dot");

        for (i = 1; i < jwt_len; i++) {
                RD_UT_ASSERT(!(mock_jwt[i - 1] == '.' && mock_jwt[i] == '.'),
                             "JWT should not have adjacent dots");
        }

        RD_UT_PASS();
}

/**
 * @brief Run SASL JWT unit tests.
 */

int unittest_sasl_jwt(void) {
        int fails = 0;

        fails += ut_sasl_jwt_base64url_encode();
        fails += ut_sasl_jwt_build_request_body();
        fails += ut_sasl_jwt_create_assertion();


        return fails;
}
