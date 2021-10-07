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
 * Builtin SASL OAUTHBEARER OIDC support
 */
#include "rdkafka_int.h"
#include "rdkafka_sasl_int.h"
#include "rdunittest.h"
#include "cJSON.h"
#include <curl/curl.h>
#include "rdhttp.h"
#include "rdkafka_sasl_oauthbearer_oidc.h"

/**
 * @brief Base64 encode binary input \p in, and write base64-encoded string
 *        and it's size to \p out
 * @returns return rd_true if encode succeed, else return rd_false.
 *          The base64-encoded string will be returned at \p out with it's
 *          size.
 */
static rd_bool_t rd_base64_encode (const rd_chariov_t *in, rd_chariov_t *out) {
        size_t max_len;

        max_len = (((in->size + 2) / 3) * 4) + 1;
        out->ptr = rd_malloc(max_len);
        if (out->ptr == NULL)
                return rd_false;

        out->size = EVP_EncodeBlock((uint8_t*)out->ptr,
                                  (uint8_t*)in->ptr,
                                  (int)in->size);

        if (out->size > INT_MAX) {
                rd_free(out->ptr);
                return rd_false;
        }

        rd_assert(out->size < max_len);
        out->ptr[out->size] = 0;
        return rd_true;
}


/**
 * @brief Generate Authorization field for HTTP header.
 *        The field contains base64-encoded string which
 *        is generated from \p client_id and \p client_secret.
 *        The authorization field will be returned in
 *        \p *authorization_base64_headerp.
 *
 * @returns return NULL if succeed, else return the error message.
 *
 * @locality Any thread.
 */
static char *rd_kafka_oidc_build_auth_header (const char *client_id,
        const char *client_secret, char **authorization_base64_headerp) {

        rd_chariov_t client_authorization_in;
        rd_chariov_t client_authorization_out;

        size_t authorization_base64_header_size;

        client_authorization_in.size = strlen(client_id) +
                                       strlen(client_secret) + 2;
        client_authorization_in.ptr = rd_malloc(client_authorization_in.size);
        rd_snprintf(client_authorization_in.ptr,
                    client_authorization_in.size,
                    "%s:%s",
                    client_id,
                    client_secret);

        if (!rd_base64_encode(&client_authorization_in,
                &client_authorization_out)) {
                rd_free(client_authorization_in.ptr);
                return "Failed to generate base64-encoded string";
        }

        authorization_base64_header_size = strlen("Authorization: Basic ") +
                client_authorization_out.size + 1;
        *authorization_base64_headerp =
                rd_malloc(authorization_base64_header_size);
        rd_snprintf(*authorization_base64_headerp,
                    authorization_base64_header_size,
                    "Authorization: Basic %s",
                    client_authorization_out.ptr);

        rd_free(client_authorization_in.ptr);
        rd_free(client_authorization_out.ptr);
        return NULL;
}


/**
 * @brief Build headers for HTTP(S) requests based on \p client_id
 *        and \p client_secret. The result will be returned in \p *headers.
 *
 * @returns If succeed, return NULL, else return the error message.
 *
 * @locality Any thread.
 */
static char *rd_kafka_oidc_build_headers (const char *client_id,
                                          const char *client_secret,
                                          struct curl_slist **headersp) {
        char *authorization_base64_header = NULL;
        char *error;

        error = rd_kafka_oidc_build_auth_header(client_id,
                client_secret, &authorization_base64_header);
        if (error != NULL)
                return error;

        *headersp = curl_slist_append(*headersp, "Accept: application/json");
        *headersp = curl_slist_append(*headersp, authorization_base64_header);

        *headersp = curl_slist_append(*headersp,
                "Content-Type: application/x-www-form-urlencoded");

        rd_free(authorization_base64_header);
        return NULL;
}


/**
 * @brief Implementation of Oauth/OIDC token refresh call back function,
 *        will receive the json response after HTTP call from token provider,
 *        then extract the jwt from the json response, and forward it to
 *        the broker.
 */
void rd_kafka_oidc_token_refresh_cb (rd_kafka_t *rk,
        const char *oauthbearer_config, void *opaque) {
        static const long timeout = 20;
        static const int retry = 3;
        static const int interval = 30000000;

        cJSON *json = NULL;
        cJSON *parsed_token;
        rd_http_error_t *herr;
        char *jwt_token;
        struct curl_slist *headers = NULL;
        char *post_fields;
        size_t post_fields_size;
        char *errstr;

        errstr = rd_kafka_oidc_build_headers(
                rk->rk_conf.sasl.oauthbearer.client_id,
                rk->rk_conf.sasl.oauthbearer.client_secret,
                &headers);
        if (errstr != NULL) {
                rd_kafka_op_err(rk,
				RD_KAFKA_RESP_ERR__AUTH,
				"Failed to build OAUTHBEARER OIDC headers: %s",
                                errstr);
                return;
        }

        /* Build post fields */
        post_fields_size =
                strlen("grant_type=client_credentials,scope=")
                + strlen(rk->rk_conf.sasl.oauthbearer.scope) + 1;
        post_fields = rd_malloc(post_fields_size);
        rd_snprintf(post_fields,
                    post_fields_size,
                    "grant_type=client_credentials,scope=%s",
                    rk->rk_conf.sasl.oauthbearer.scope);
        herr = rd_http_post_expect_json(rk,
                                        headers,
                                        post_fields,
                                        post_fields_size,
                                        timeout,
                                        retry,
                                        interval,
                                        &json);
        if (unlikely(herr != NULL)) {
		rd_kafka_op_err(rk,
				RD_KAFKA_RESP_ERR__AUTH,
				"Failed to receive json result from HTTP(S), "
                                "returned error code: %d, returned error "
                                "string: %s", herr->code, herr->errstr);
                rd_http_error_destroy(herr);
                goto done;
        }
        parsed_token = cJSON_GetObjectItem(json, "access_token");
        if (unlikely(parsed_token == NULL)) {
                rd_kafka_op_err(rk,
				RD_KAFKA_RESP_ERR__AUTH,
				"Expected non-empty JSON response");
                rd_http_error_destroy(herr);
                goto done;
        }
        jwt_token = cJSON_Print(parsed_token);

        /* TODO: Forward token to broker. */

done:
        RD_IF_FREE(post_fields, rd_free);
        RD_IF_FREE(json, cJSON_Delete);
        RD_IF_FREE(headers, curl_slist_free_all);
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests.
 */
static int ut_sasl_oauthbearer_oidc_should_succeed (void) {
        static const char *token_key = "access_token";
        static const char *expected_jwt_token =
                "\"eyJhbGciOiJIUzI1NiIsInR5"
                "cCI6IkpXVCIsImtpZCI6ImFiY2VkZmcifQ"
                "."
                "eyJpYXQiOjE2MzIzNzUzMjAsInN1YiI6InN"
                "1YiIsImV4cCI6MTYzMjM3NTYyMH0"
                "."
                "bT5oY8K-rS2gQ7Awc40844bK3zhzBhZb7sputErqQHY\"";
        char *expected_token_value;
        size_t token_len = 0;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        cJSON *json = NULL;
        char *token = NULL;
        cJSON *parsed_token;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");

        RD_UT_ASSERT(!herr,
                     "Expected initialize succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen(token_key) + strlen(expected_jwt_token) + 8;

        expected_token_value = rd_malloc(token_len);
        rd_snprintf(expected_token_value,
                    token_len,
                    "{\"%s\":%s}",
                    "access_token",
                    expected_jwt_token);

        rd_buf_write(hreq.hreq_buf, expected_token_value, token_len);

        herr = rd_http_parse_json(&hreq, &json);
        RD_UT_ASSERT(!herr,
                     "Expected parse token to json to succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(json, "Expected non-empty json, but not.");

        parsed_token = cJSON_GetObjectItem(json, "access_token");

        RD_UT_ASSERT(parsed_token,
                     "Expected non-empty JSON response, but not");
        token = cJSON_Print(parsed_token);

        RD_UT_ASSERT(!strcmp(expected_jwt_token, token),
                     "Incorrect token received: "
                     "expected=%s; received=%s",
                     expected_jwt_token, token);

        rd_free(token);
        rd_free(expected_token_value);
        rd_http_error_destroy(herr);
        rd_http_req_destroy(&hreq);
        cJSON_Delete(json);

        RD_UT_PASS();
}


/**
 * @brief make sure if the json doesn't include the "access_token" key,
 *        it won't fail and return an empty token.
 */
static int ut_sasl_oauthbearer_oidc_with_empty_key (void) {
        static const char *empty_token_format = "{}";
        size_t token_len = 0;
        rd_http_req_t hreq;
        rd_http_error_t *herr = NULL;
        cJSON *json = NULL;
        cJSON *parsed_token;

        RD_UT_BEGIN();

        herr = rd_http_req_init(&hreq, "");
        RD_UT_ASSERT(!herr,
                     "Expected initialization succeed, "
                     "but it failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        token_len = strlen(empty_token_format);

        rd_buf_write(hreq.hreq_buf , empty_token_format, token_len);

        herr = rd_http_parse_json(&hreq, &json);

        RD_UT_ASSERT(!herr,
                     "Expected parse token to json to succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(json, "Expected non-empty json, but not.");

        parsed_token = cJSON_GetObjectItem(json, "access_token");

        RD_UT_ASSERT(!parsed_token,
                     "Expected empty JSON response, but not");

        rd_http_req_destroy(&hreq);
        rd_http_error_destroy(herr);
        cJSON_Delete(json);
        cJSON_Delete(parsed_token);
        RD_UT_PASS();
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests
 *        or fail as expected.
 */
int unittest_sasl_oauthbearer_oidc (void) {
        int fails = 0;
        fails += ut_sasl_oauthbearer_oidc_should_succeed();
        fails += ut_sasl_oauthbearer_oidc_with_empty_key();
        return fails;
}
