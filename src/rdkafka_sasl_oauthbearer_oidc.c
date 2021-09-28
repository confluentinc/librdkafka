/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2021 Magnus Edenhill
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
 * Builtin SASL OAUTHBEARER support
 */
#include "rdkafka_int.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl_int.h"
#include <openssl/evp.h>
#include "rdunittest.h"
#include "cJSON.h"
#include <curl/curl.h>
#include "rdhttp.h"
#include "rdkafka_sasl_oauthbearer_oidc.h"

/**
 * @brief Base64 encode binary input \p in
 * @returns return rd_true if encode succeed, else return rd_false.
 *          The base64-encoded string will be returned at \p out with it's
 *          size.
 */
static rd_bool_t rd_base64_encode (const rd_chariov_t *in, rd_chariov_t *out) {
        size_t max_len;

        /* OpenSSL takes an |int| argument so the input cannot exceed that. */
        if (in->size > INT_MAX) {
                return rd_false;
        }

        /* This does not overflow given the |INT_MAX| bound, above. */
        max_len = (((in->size + 2) / 3) * 4) + 1;
        out->ptr = rd_malloc(max_len);
        if (out->ptr == NULL) {
                return rd_false;
        }

        out->size = EVP_EncodeBlock((uint8_t*)out->ptr,
                                  (uint8_t*)in->ptr,
                                  (int)in->size);
        assert(out->size < max_len);
        out->ptr[out->size] = 0;
        return rd_true;
}


/**
 * @brief Generate Authorization field for HTTP header.
 *        The field contains base64-encoded string which
 *        is generated from client_id and client_secret.
 *
 * @returns a newly allocated string with partially base64-encoded string.
 *
 * @locality Any thread.
 */
static char *build_authorization_base64_header (const char *client_id,
        const char *client_secret, char **error) {

        static const char *authorization_header = "Authorization:Basic ";
        static const char *error_message = "Failed to generate base64-encoded "
                                           "string\n";
        size_t len;
        size_t client_id_len;
        size_t client_secret_len;
        size_t authorization_base64_header_size;
        rd_chariov_t client_authorization_in;
        rd_chariov_t client_authorization_out;
        char *authorization_base64_header;
        rd_bool_t rd_base64_encode_status;

        client_id_len = strlen(client_id);
        client_secret_len = strlen(client_secret);
        len = client_id_len + client_secret_len + 2;

        client_authorization_in.ptr = rd_malloc(len);
        memcpy(client_authorization_in.ptr, client_id, client_id_len);
        memcpy(client_authorization_in.ptr + client_id_len, ":", 1);
        memcpy(client_authorization_in.ptr + client_id_len + 1,
               client_secret,
               client_secret_len + 1);

        client_authorization_in.size = len;

        rd_base64_encode_status = rd_false;
        rd_base64_encode_status = rd_base64_encode(&client_authorization_in, &client_authorization_out);

        if (!rd_base64_encode_status) {
                *error = rd_malloc(strlen(error_message) + 1);
                strcpy(*error, error_message);
                rd_free(client_authorization_in.ptr);
                return NULL;
        }

        authorization_base64_header_size = strlen(authorization_header) +
                                           client_authorization_out.size + 1;
        authorization_base64_header =
                rd_malloc(authorization_base64_header_size);
        memcpy(authorization_base64_header,
               authorization_header,
               strlen(authorization_header));
        memcpy(authorization_base64_header + strlen(authorization_header),
               client_authorization_out.ptr,
               client_authorization_out.size + 1);

        rd_free(client_authorization_in.ptr);
        rd_free(client_authorization_out.ptr);
        return authorization_base64_header;
}


/**
 * @brief Build headers for HTTP(S) requests.
 *
 * @returns The result will be responsed in \p *headers.
 *          If succeed, return rd_true with NULL in \p *error,
 *          If failed, return rd_false with error message in \p *error.
 *
 * @locality Any thread.
 */
static rd_bool_t build_headers (rd_kafka_conf_t *conf,
                                struct curl_slist **headers,
                                char **error) {
        static const char *miss_client_id_error_message = "client_id is "
                "a required field";
        static const char *miss_client_secret_error_message = "client_secret "
                "is a required field";
        static const char *accept_header = "Accept:application/json";
        const char *client_id;
        const char *client_secret;
        char *authorization_base64_header;

        client_id = conf->sasl.oauthbearer.client_id;

        if (client_id == NULL) {
                *error = rd_malloc(strlen(miss_client_id_error_message)
                                   + 1);
                strcpy(*error, miss_client_id_error_message);
                return rd_false;
        }

        client_secret = conf->sasl.oauthbearer.client_secret;
        if (client_secret == NULL) {
                *error = rd_malloc(strlen(miss_client_secret_error_message)
                                   + 1);
                strcpy(*error, miss_client_secret_error_message);
                return rd_false;
        }

        authorization_base64_header =
                build_authorization_base64_header(client_id,
                                                  client_secret,
                                                  &*error);
        if (authorization_base64_header == NULL)
                return rd_false;

        *headers = curl_slist_append(*headers, accept_header);
        *headers = curl_slist_append(*headers, authorization_base64_header);
        rd_free(authorization_base64_header);
        return rd_true;
}


/**
 * @brief Implementation of Oauth/OIDC token refresh call back function,
 *        will receive the json response after HTTP call from token provider,
 *        then extract the jwt from the json response, and forward it to
 *        the broker.
 */
void rd_kafka_conf_set_oauthbearer_oidc_token_refresh_cb (rd_kafka_t *rk,
        const char *oauthbearer_config, void *opaque) {
        static const char *data = "grant_type=client_credentials,scope=%s";
        static const char *token_key = "access_token";
        static const long timeout = 20;
        static const size_t retry = 3;

        cJSON *json = NULL;
        rd_http_error_t *herr;
        char *jwt_token = NULL;
        struct curl_slist *headers = NULL;
        char *data_to_token;
        size_t data_to_token_size;
        const char *scope;
        const char *token_url;
        rd_bool_t build_headers_status;
        char *error;

        token_url = rk->rk_conf.sasl.oauthbearer.token_endpoint_url;
        if (token_url == NULL) {
                rd_kafka_log(rk, LOG_ERR, "OAUTH/OIDC",
                             "token_url is a required field");
                return;
        }

        build_headers_status = rd_false;
        build_headers_status = build_headers(&rk->rk_conf, &headers, &error);
        if (!build_headers_status) {
                rd_kafka_log(rk, LOG_ERR, "OAUTH/OIDC",
                             "Failed to build headers: %s",
                             error);
                rd_free(error);
                return;
        }

        /* Build data field */
        scope = rk->rk_conf.sasl.oauthbearer.scope;
        if (scope == NULL) {
                rd_kafka_log(rk, LOG_ERR, "OAUTH/OIDC",
                             "scope is a required field");
                return;
        }
        data_to_token_size = strlen(data) + strlen(scope) - 1;
        data_to_token = rd_malloc(data_to_token_size);
        rd_snprintf(data_to_token, data_to_token_size, data, scope);
        herr = rd_http_post_json(token_url,
                                 &headers,
                                 data_to_token,
                                 data_to_token_size,
                                 timeout,
                                 retry,
                                 &json);
        if (unlikely(herr != NULL)) {
                rd_kafka_log(rk, LOG_ERR, "OAUTH/OIDC",
                             "Failed to receive json result from HTTP(S), "
                             "returned error code: %d, returned error string: "
                             "%s", herr->code, herr->errstr);
                rd_http_error_destroy(herr);
                goto exit;
        }
        herr = rd_http_extract_jwt_from_json(&json, &jwt_token, token_key);
        if (unlikely(herr != NULL)) {
                rd_kafka_log(rk, LOG_ERR, "OAUTH/OIDC",
                             "Failed to extract jwt from json, "
                             "returned error code: %d, returned error string: "
                             "%s", herr->code, herr->errstr);
                rd_http_error_destroy(herr);
                goto exit;
        }

        /* TODO: Forward token to broker. */

exit:
        RD_IF_FREE(data_to_token, rd_free);
        cJSON_Delete(json);
        RD_IF_FREE(headers, curl_slist_free_all);
        RD_IF_FREE(jwt_token, rd_free);
        return;
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests.
 */
int unittest_sasl_oauthbearer_oidc_should_succeed (void) {

        static const char *token_key = "access_token";
        static const char *expected_jwt_token =
                "\"eyJhbGciOiJIUzI1NiIsInR5"
                "cCI6IkpXVCIsImtpZCI6ImFiY2VkZmcifQ"
                "."
                "eyJpYXQiOjE2MzIzNzUzMjAsInN1YiI6InN"
                "1YiIsImV4cCI6MTYzMjM3NTYyMH0"
                "."
                "bT5oY8K-rS2gQ7Awc40844bK3zhzBhZb7sputErqQHY\"";
        const char *token_format = "{\"%s\":%s}";
        char *expected_token_value;
        size_t token_len = 0;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        cJSON *json = NULL;
        char *token = NULL;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");
        RD_UT_ASSERT(!herr,
                     "Expected initializa succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen(token_key) + strlen(expected_jwt_token) + 8;
        expected_token_value = rd_malloc(token_len);
        rd_snprintf(expected_token_value,
                    token_len,
                    token_format,
                    token_key,
                    expected_jwt_token);

        hreq.hreq_buf = rd_buf_new(1, token_len + 1);
        rd_buf_write(hreq.hreq_buf, expected_token_value, token_len);

        herr = rd_http_parse_token_to_json(&hreq, &json);
        RD_UT_ASSERT(!herr,
                     "Expected parse token to json to succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        herr = rd_http_extract_jwt_from_json(&json, &token, token_key);
        RD_UT_ASSERT(!herr,
                     "Expected extract jwt from json to succeed, "
                     "but failed with error code: %d, error string %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(!strcmp(expected_jwt_token, token),
                     "Incorrect token received: "
                     "expected=%s; received=%s",
                     expected_jwt_token, token);

        rd_free(token);
        rd_free(expected_token_value);
        rd_http_error_destroy(herr);
        cJSON_Delete(json);
        RD_UT_PASS();
}


/**
 * @brief make sure the return expected error if received empty json
 *        from HTTP(S)
 */
int unittest_sasl_oauthbearer_oidc_with_empty_key (void) {
        static const char *token_key = "access_token";
        static const char *empty_token_format = "{}";
        static const char *expected_error_string =
                "Expected non-empty JSON response";
        static const int expected_error_code = -1;
        size_t token_len = 0;
        rd_http_req_t hreq;
        rd_http_error_t *herr = NULL;
        cJSON *json = NULL;
        char *token = NULL;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");
        RD_UT_ASSERT(!herr,
                     "Expected initializa succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen(empty_token_format);

        hreq.hreq_buf = rd_buf_new(1, token_len + 1);
        rd_buf_write(hreq.hreq_buf , empty_token_format, token_len);

        herr = rd_http_parse_token_to_json(&hreq, &json);

        herr = rd_http_extract_jwt_from_json(&json, &token, token_key);
        RD_UT_ASSERT(herr,
                     "Expecte extract jwt from json to fail, but not");
        RD_UT_ASSERT(herr->code == expected_error_code,
                     "%d is expected error code, but received %d",
                     expected_error_code, herr->code);
        RD_UT_ASSERT(!strcmp(herr->errstr, expected_error_string),
                     "%s is expected error string, but received %s",
                     expected_error_string, herr->errstr);

        rd_http_error_destroy(herr);
        cJSON_Delete(json);
        RD_UT_PASS();
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests.
 */
int unittest_sasl_oauthbearer_oidc (void) {

        int fails = 0;
        fails += unittest_sasl_oauthbearer_oidc_should_succeed();
        fails += unittest_sasl_oauthbearer_oidc_with_empty_key();
        return fails;
}
