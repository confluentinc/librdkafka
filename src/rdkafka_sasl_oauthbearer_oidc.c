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
 * @returns a newly allocated, base64-encoded string or NULL on error.
 */
static char *rd_base64_encode (const rd_chariov_t *in) {
        char *ret;
        size_t ret_len, max_len;

        /* OpenSSL takes an |int| argument so the input cannot exceed that. */
        if (in->size > INT_MAX) {
                return NULL;
        }

        /* This does not overflow given the |INT_MAX| bound, above. */
        max_len = (((in->size + 2) / 3) * 4) + 1;
        ret = rd_malloc(max_len);
        if (ret == NULL) {
                return NULL;
        }

        ret_len = EVP_EncodeBlock((uint8_t*)ret,
                                  (uint8_t*)in->ptr,
                                  (int)in->size);
        assert(ret_len < max_len);
        ret[ret_len] = 0;

        return ret;
}


/**
 * @brief Generate Authorization field in HTTP header.
 *        The field contains base64-encoded string which
 *        is generated from client_id and client_secret.
 *
 * @returns a newly allocated string with partially base64-encoded string.
 *
 * @locality Any thread.
 */
static char *build_authorization_base64_header (const char *client_id,
                                       const char *client_secret) {
        
        static const char *authorization_header = "Authorization:Basic %s";

        size_t len;
        size_t authorization_base64_header_size;
        rd_chariov_t *client_authorization;
        char *authorization;
        char *authorization_base64 = NULL;
        char *authorization_base64_header = NULL;

        len = strlen(client_id) + strlen(client_secret) + 2;
        authorization = rd_malloc(len);
        authorization[0] = '\0';
        strcat(authorization,client_id);
        strcat(authorization,"|");
        strcat(authorization,client_secret);
        
        client_authorization = rd_calloc(1, sizeof(*client_authorization));

        client_authorization->ptr = rd_malloc(len);
        client_authorization->ptr[0] = '\0';
        client_authorization->ptr = authorization;
        client_authorization->size = len;

        authorization_base64 = rd_base64_encode(client_authorization);
        authorization_base64_header_size = strlen(authorization_header) - 1 +
                                           strlen(authorization_base64);
        authorization_base64_header =
                rd_malloc(authorization_base64_header_size);

        rd_snprintf(authorization_base64_header,
                    authorization_base64_header_size,
                    authorization_header,
                    authorization_base64);

        rd_free(authorization_base64);
        rd_free(authorization);
        rd_free(client_authorization);
        return authorization_base64_header;
}

/**
 * @brief Perform a blocking HTTP(S) request to \p url with HTTP(S)
 *        headers and data.
 *
 * @returns Returns the response (even if there's a HTTP error code returned)
 *          in \p hreq.
 *          Returns NULL on success (HTTP response code < 400), or an error
 *          object on transport or HTTP error - this error object must be
 *          destroyed by calling rd_http_error_destroy().
 *
 * @locality Any thread.
 */
rd_http_error_t
*rk_kafka_retrieve_from_token_end_point_url (rd_kafka_conf_t *conf,
                                            rd_http_req_t *hreq) {
#if WITH_CURL  
        
        static const char *data = "grant_type=client_credentials,scope=%s";
        static const char *accept_header = "Accept:application/json";
      
        const char *token_url;
        const char *client_id;
        const char *client_secret;
        char *data_to_token;
        size_t data_to_token_size;
        const char *scope;
        char *authorization_base64_header = NULL;

        rd_http_error_t *herr;

        struct curl_slist *headers = NULL;

        token_url = conf->sasl.oauthbearer.token_endpoint_url;
              
        herr = rd_http_req_init(hreq, token_url);
        if (unlikely(herr != NULL))
                return herr;
        
        client_id = conf->sasl.oauthbearer.client_id;
        client_secret = conf->sasl.oauthbearer.client_secret;
        authorization_base64_header =
                build_authorization_base64_header(client_id, client_secret);
        
        headers = curl_slist_append(headers, accept_header);
        headers = curl_slist_append(headers, authorization_base64_header);
        curl_easy_setopt(hreq->hreq_curl, CURLOPT_HTTPHEADER, headers);

        scope = conf->sasl.oauthbearer.scope;
        data_to_token_size = strlen(data) + strlen(scope) + 1;
        data_to_token = rd_alloca(data_to_token_size);
        rd_snprintf(data_to_token, data_to_token_size, data, scope);
        curl_easy_setopt(hreq->hreq_curl,
                         CURLOPT_POSTFIELDSIZE,
                         data_to_token_size);
        curl_easy_setopt(hreq->hreq_curl, CURLOPT_POSTFIELDS, data_to_token);

        herr = rd_http_req_perform_sync(hreq);
        if (unlikely(herr != NULL))
                return herr;

        rd_free(authorization_base64_header);
        return herr;
#endif
}


/**
 * @brief Implementation of Oauth/OIDC token refresh call back function,
 *        will receive the json response after HTTP call to token provider,
 *        then extract the jwt from the json response, then forward it to
 *        the broker.
 */
void rd_kafka_conf_set_oauthbearer_oidc_token_refresh_cb (rd_kafka_t *rk,
        const char *oauthbearer_config, void *opaque) {

        cJSON *json = NULL;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        char *jwt_token = NULL;

        herr = rk_kafka_retrieve_from_token_end_point_url(&rk->rk_conf, &hreq);
        rd_assert(!herr);

        herr = rd_http_parse_token_to_json(&hreq, &json);
        rd_assert(!herr);
        rd_http_extract_jwt_from_json(&json, &jwt_token);
}


/**
 * @brief make sure the jwt is able to be extracted from HTTP(S) requests.
 */
int unittest_sasl_oauthbearer_oidc (void) {
        RD_UT_BEGIN();

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
        size_t token_len;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        rd_buf_t buf;
        cJSON *json = NULL;
        char *token;

        token_len = strlen(token_key) + strlen(expected_jwt_token) + 8;
        expected_token_value = rd_malloc(token_len);
        rd_snprintf(expected_token_value,
                    token_len,
                    token_format,
                    token_key,
                    expected_jwt_token);      

        rd_buf_init(&buf, 1, token_len);
        rd_buf_write(&buf, expected_token_value, token_len);
        hreq.hreq_buf = &buf;
        
        herr = rd_http_parse_token_to_json(&hreq, &json);
        RD_UT_ASSERT(!herr, 
                     "Expected parse token to json to succeed, "
                     "but failed with error code: %d, error string: %s",
                     herr->code, herr->errstr);

        herr = rd_http_extract_jwt_from_json(&json, &token);
        RD_UT_ASSERT(!herr, 
                     "Expected extract jwt from json to succeed, "
                     "but failed with error code: %d, error string %s",
                     herr->code, herr->errstr);

        RD_UT_ASSERT(!strcmp(expected_jwt_token, token),
                     "Incorrect token received: "
                     "expected=%s; received=%s",
                     expected_jwt_token, token);
        
        rd_free(token);
        RD_UT_PASS();
        return 0;
}
