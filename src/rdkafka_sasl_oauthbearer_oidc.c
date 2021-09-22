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

static char *build_authorization_base64_header(const char *client_id,
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

rd_http_error_t
*rk_kafka_retrieve_from_token_end_point_url(rd_kafka_conf_t *conf,
                                            rd_http_req_t *hreq) {
#if WITH_CURL  
        
        static const char *data = "grant_type=client_credentials,scope=%s";
        static const char *accept_header = "Accept:application/json";
      
        const char *token_url;
        const char *client_id;
        const char *client_secret;
        char *data_to_token;
        int data_to_token_size;
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


void rd_kafka_conf_set_oauthbearer_oidc_token_refresh_cb (rd_kafka_t *rk,
        const char *oauthbearer_config, void *opaque) {

        static const char *token_key = "access_token";

        cJSON *json = NULL;
        cJSON *jval = NULL;
        cJSON *parsed_token = NULL;
        rd_bool_t empty;
        rd_http_req_t hreq;
        rd_http_error_t *herr;
        char *jwt_token = NULL;

        herr = rk_kafka_retrieve_from_token_end_point_url(&rk->rk_conf, &hreq);
        rd_assert(!herr);

        herr = rd_http_extract_jwt(&hreq, &json);
        rd_assert(!herr);
   
        empty = rd_true;
        cJSON_ArrayForEach(jval, json) {
                empty = rd_false;
                break;
        }

        rd_assert(!empty);

        parsed_token = cJSON_GetObjectItem(json, token_key);
        jwt_token = cJSON_Print(parsed_token);
}

int unittest_sasl_oauthbearer_oidc (void) {
        const char *base_url = rd_getenv("OAUTH_OIDC_URL", NULL);

        rd_kafka_conf_res_t res;

        rd_kafka_t *rk = NULL;
        rd_kafka_conf_t *conf;

        if (!base_url || !*base_url)
                RD_UT_SKIP("OAUTH_OIDC_URL environment variable not set");

        RD_UT_BEGIN();

        rk = rd_calloc(1, sizeof(*rk));

        conf = rd_kafka_conf_new();
        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.token.endpoint.url",
                                "http://localhost:8080/retrieve", NULL, 0);
        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.client.id",
                                "123", NULL, 0);
        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.client.secret",
                                "abc", NULL, 0);
        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.scope",
                                "test-scope", NULL, 0);
        res = rd_kafka_conf_set(conf, "sasl.oauthbearer.extensions",
                                "grant_type=client_credentials,"
                                "scope=test-scope",
                                NULL, 0);
        rk->rk_conf = *conf;

        rd_kafka_conf_set_oauthbearer_oidc_token_refresh_cb(rk, NULL, NULL);
        rd_free(rk);
        RD_UT_PASS();
        return 0;
}
