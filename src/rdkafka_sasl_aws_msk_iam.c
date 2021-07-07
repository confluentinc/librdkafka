/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2017 Magnus Edenhill
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
 * Builtin SASL AWS MSK IAM support
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <curl/curl.h>

#include "rdkafka_int.h"
#include "rdkafka_transport.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl.h"
#include "rdkafka_sasl_int.h"

#include "rdstringbuilder.h"
#include "rdtypes.h"
#include "rdunittest.h"

#if WITH_SSL
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#else
#error "WITH_SSL (OpenSSL) is required for SASL AWS MSK IAM"
#endif


struct rd_kafka_sasl_aws_msk_iam_state {
        enum {
            RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE,
            RD_KAFKA_SASL_AWS_MSK_IAM_RECEIVE_SERVER_RESPONSE,
        } state;
        char ymd[9];  /* yyyyMMdd of request */
        char hms[7];  /* HHmmss of request */
        rd_chariov_t hostname;  /* hostname from client_new */
        rd_chariov_t aws_access_key_id;  /* AWS access key id from conf */
        rd_chariov_t aws_secret_access_key;  /* AWS secret access key from conf */
        rd_chariov_t aws_region;  /* AWS region from conf */
        rd_chariov_t aws_security_token;  /* AWS security token from conf (optional) */
        const EVP_MD *md;  /* hash function pointer */
};


/**
 * @brief constructs full date format of yyyymmddTHHMMssZ
 * @remark amz_date.ptr will be allocated and must be freed.
 */
static rd_chariov_t rd_construct_amz_date (const char *ymd, const char *hms) {
        int amz_date_size = strlen(ymd) + strlen(hms) + strlen("TZ") + 1;
        char *amz_date_buf;
        
        amz_date_buf = rd_malloc(amz_date_size);
        rd_snprintf(amz_date_buf, amz_date_size, "%sT%sZ", ymd, hms);
        
        rd_chariov_t amz_date =
                { .ptr = amz_date_buf, .size = strlen(amz_date_buf) };
        
        return amz_date;
}


/**
 * @brief constructs amz_credential without access_key_id prefix
 * @remark amz_credential.ptr will be allocated and must be freed.
 */
static rd_chariov_t rd_construct_amz_credential (const rd_chariov_t aws_region, const char *ymd) {
        const char *aws_req = "kafka-cluster/aws4_request";
        int credential_size = strlen(ymd) + 
                aws_region.size + strlen(aws_req) + strlen("//") + 1;
        char *credential_buf;
        
        credential_buf = rd_malloc(credential_size);
        rd_snprintf(credential_buf, credential_size, "%s/%s/%s", ymd, aws_region.ptr, aws_req);
        
        rd_chariov_t credential =
                { .ptr = credential_buf, .size = strlen(credential_buf) };
        
        return credential;
}

/**
 * @brief constructs amz_credential without access_key_id prefix
 * @remark amz_credential.ptr will be allocated and must be freed.
 */
static rd_chariov_t rd_construct_amz_credential_add_key_prefix (const rd_chariov_t amz_credential, const rd_chariov_t aws_access_key_id) {
        int credential_size = amz_credential.size + aws_access_key_id.size + strlen("/") + 1;
        char *credential_buf;
        
        credential_buf = rd_malloc(credential_size);
        rd_snprintf(credential_buf, credential_size, "%s/%s", aws_access_key_id.ptr, amz_credential.ptr);
        
        rd_chariov_t credential =
                { .ptr = credential_buf, .size = strlen(credential_buf) };
        
        return credential;
}


/**
 * @brief Uri escapes a string
 * @remark ret string will be allocated and must be freed.
 */
static char *rd_uri_encode (const rd_chariov_t in) {
        char *ret;
        int ret_len;
        
        CURL *curl = curl_easy_init();
        if (curl) {
            char *encoded = curl_easy_escape(curl, in.ptr, (int)in.size);
            ret_len = strlen(encoded)+1;
            ret = rd_malloc(ret_len);
            memcpy(ret, encoded, ret_len);
            
            curl_free(encoded);
            curl_easy_cleanup(curl);
            return ret;
        }
        
        return NULL;
}


/**
 * @brief HMAC_SHA256 encoding
 */
static unsigned char *rd_hmac_sha256 (const void *key, int keylen,
                              const unsigned char *data, int datalen,
                              unsigned char *result, unsigned int *resultlen) {
    return HMAC(EVP_sha256(), key, keylen, data, datalen, result, resultlen);
}


/**
 * @brief Generates a canonical query string
 * @remark canonical_query_string will be allocated and must be freed.
 */
static char *rd_kafka_sasl_aws_msk_iam_build_canonical_query_string (struct rd_kafka_sasl_aws_msk_iam_state *state) {    
        char *action_str = "kafka-cluster:Connect";
        rd_chariov_t action = 
                { .ptr = action_str, .size = strlen(action_str) };
        char *uri_action = rd_uri_encode(action);
        
        rd_chariov_t credential_wo_prefix = rd_construct_amz_credential(state->aws_region, state->ymd);
        rd_chariov_t credential = rd_construct_amz_credential_add_key_prefix(credential_wo_prefix, state->aws_access_key_id);
        char *uri_credential = rd_uri_encode(credential);
        
        rd_chariov_t amz_date = rd_construct_amz_date(state->ymd, state->hms);
        char *uri_amz_date = rd_uri_encode(amz_date);
        
        str_builder_t *sb;
        sb = str_builder_create();
        str_builder_add_str(sb, "Action=");
        str_builder_add_str(sb, uri_action);
        str_builder_add_str(sb, "&");
        str_builder_add_str(sb, "X-Amz-Algorithm=AWS4-HMAC-SHA256&");
        str_builder_add_str(sb, "X-Amz-Credential=");
        str_builder_add_str(sb, uri_credential);
        str_builder_add_str(sb, "&");
        str_builder_add_str(sb, "X-Amz-Date=");
        str_builder_add_str(sb, uri_amz_date);
        str_builder_add_str(sb, "&");
        str_builder_add_str(sb, "X-Amz-Expires=900&");  // AWS recommends 900 seconds
        
        if (state->aws_security_token.ptr != NULL) {
            char *uri_amz_security_token = rd_uri_encode(state->aws_security_token);
            
            str_builder_add_str(sb, "X-Amz-Security-Token=");
            str_builder_add_str(sb, uri_amz_security_token);
            str_builder_add_str(sb, "&");
            
            RD_IF_FREE(uri_amz_security_token, rd_free);
        }
        
        str_builder_add_str(sb, "X-Amz-SignedHeaders=host");
        
        char *canonical_query_string = str_builder_dump(sb);
        
        str_builder_destroy(sb);
        
        RD_IF_FREE(uri_action, rd_free);
        RD_IF_FREE(credential_wo_prefix.ptr, rd_free);
        RD_IF_FREE(credential.ptr, rd_free);
        RD_IF_FREE(uri_credential, rd_free);
        RD_IF_FREE(amz_date.ptr, rd_free);
        RD_IF_FREE(uri_amz_date, rd_free);
        
        return canonical_query_string;
}


/**
 * @brief Generates a canonical request
 * @remark canonical_request will be allocated and must be freed.
 */
static char *rd_kafka_sasl_aws_msk_iam_build_canonical_request (struct rd_kafka_sasl_aws_msk_iam_state *state) {
        char *canonical_query_string = rd_kafka_sasl_aws_msk_iam_build_canonical_query_string(state);
        const char *hostname = state->hostname.ptr;
        int canonical_header_size = strlen(hostname) + strlen("host:\n") + 1;
        char canonical_header_buf[256];
        rd_snprintf(canonical_header_buf, canonical_header_size, "host:%s\n", hostname);
        
        const char *signed_headers = "host";
        
        unsigned char md_value[EVP_MAX_MD_SIZE];
        unsigned int md_len, i;
        EVP_MD_CTX *mdctx;
        
        mdctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex(mdctx, state->md, NULL);
        EVP_DigestUpdate(mdctx, "", 0);  // payload should be empty string
        EVP_DigestFinal_ex(mdctx, md_value, &md_len);
        EVP_MD_CTX_free(mdctx);
        
        char res_hexstring[64];
        for (i = 0; i < md_len; i++)
               sprintf(&(res_hexstring[i * 2]), "%02x", md_value[i]);  // save string in hex base 16
        
        str_builder_t *sb;
        sb = str_builder_create();
        str_builder_add_str(sb, "GET\n");
        str_builder_add_str(sb, "/\n");
        str_builder_add_str(sb, canonical_query_string);
        str_builder_add_str(sb, "\n");
        str_builder_add_str(sb, canonical_header_buf);
        str_builder_add_str(sb, "\n");
        str_builder_add_str(sb, signed_headers);
        str_builder_add_str(sb, "\n");
        str_builder_add_str(sb, res_hexstring);
        
        char *canonical_request = str_builder_dump(sb);
        
        str_builder_destroy(sb);
        
        RD_IF_FREE(canonical_query_string, rd_free);
        
        return canonical_request;
}


/**
 * @brief Generates a signature
 * @remark signature will be allocated and must be freed.
 */
static char *rd_kafka_sasl_aws_msk_iam_calculate_signature (struct rd_kafka_sasl_aws_msk_iam_state *state, 
                                                            rd_chariov_t canonical_request) {
        unsigned char md_value[EVP_MAX_MD_SIZE];
        unsigned int md_len, i;
        EVP_MD_CTX *mdctx;
        
        mdctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex(mdctx, state->md, NULL);
        EVP_DigestUpdate(mdctx, canonical_request.ptr, canonical_request.size);
        EVP_DigestFinal_ex(mdctx, md_value, &md_len);
        EVP_MD_CTX_free(mdctx);
        
        char hex_sha_canonical_request[65];
        for (i = 0; i < md_len; i++)
               sprintf(hex_sha_canonical_request + (i * 2), "%02x", md_value[i]);  // save string in hex base 16
        
        rd_chariov_t amz_date = rd_construct_amz_date((const char *)state->ymd, (const char *)state->hms);
        rd_chariov_t credential_wo_prefix = rd_construct_amz_credential(state->aws_region, (const char *)state->ymd);
        
        str_builder_t *sb;
        sb = str_builder_create();
        str_builder_add_str(sb, "AWS4-HMAC-SHA256\n");
        str_builder_add_str(sb, amz_date.ptr);
        str_builder_add_str(sb, "\n");
        str_builder_add_str(sb, credential_wo_prefix.ptr);
        str_builder_add_str(sb, "\n");
        str_builder_add_str(sb, hex_sha_canonical_request);
        char *string_to_sign = str_builder_dump(sb);
        
        str_builder_clear(sb);
        str_builder_add_str(sb, "AWS4");
        str_builder_add_str(sb, state->aws_secret_access_key.ptr);
        char *date_key = str_builder_dump(sb);
        
        str_builder_destroy(sb);
        
        unsigned char *hmac_date_key;
        unsigned int hmac_date_key_len = 32;
        hmac_date_key = rd_hmac_sha256((unsigned char *)date_key, strlen(date_key), (unsigned char *)state->ymd, strlen(state->ymd), NULL, NULL);
        
        unsigned char *hmac_date_region_key;
        unsigned int hmac_date_region_key_len = 32;
        hmac_date_region_key = rd_hmac_sha256(hmac_date_key, hmac_date_key_len, (unsigned char *)state->aws_region.ptr, state->aws_region.size, NULL, NULL);
      
        unsigned char *hmac_date_region_service_key;
        unsigned int hmac_date_region_service_key_len = 32;
        hmac_date_region_service_key = rd_hmac_sha256(hmac_date_region_key, hmac_date_region_key_len, (unsigned char *)"kafka-cluster", strlen("kafka-cluster"), NULL, NULL);
              
        unsigned char *hmac_signing_key;
        unsigned int hmac_signing_key_len = 32;
        hmac_signing_key = rd_hmac_sha256(hmac_date_region_service_key, hmac_date_region_service_key_len, (unsigned char *)"aws4_request", strlen("aws4_request"), NULL, NULL);
              
        unsigned char *hmac_signature;
        unsigned int hmac_signature_len = 32;
        hmac_signature = rd_hmac_sha256(hmac_signing_key, hmac_signing_key_len, (unsigned char *)string_to_sign, strlen(string_to_sign), NULL, NULL);
        
        char res_hexstring[64];
        for (i = 0; i < hmac_signature_len; i++)
               sprintf(&(res_hexstring[i * 2]), "%02x", hmac_signature[i]);  // save string in hex base 16
        
        char *signature = rd_strdup(res_hexstring);
        
        RD_IF_FREE(string_to_sign, rd_free);
        RD_IF_FREE(date_key, rd_free);
        RD_IF_FREE(amz_date.ptr, rd_free);
        RD_IF_FREE(credential_wo_prefix.ptr, rd_free);
        
        return signature;
}


/**
 * @brief Generates a sasl_payload
 * @remark sasl_payload will be allocated and must be freed.
 */
static char *rd_kafka_sasl_aws_msk_iam_build_request_json (
        char *hostname,
        char *credential,
        char *amz_date_str,
        char *signature,
        char *security_token) {
        /* Construct JSON payload */
        str_builder_t *sb;
        sb = str_builder_create();
        str_builder_add_str(sb, "{\"version\":\"2020_10_22\",");
        str_builder_add_str(sb, "\"host\":\"");
        str_builder_add_str(sb, hostname);
        str_builder_add_str(sb, "\",");
        str_builder_add_str(sb, "\"user-agent\":\"librdkafka\",");
        str_builder_add_str(sb, "\"action\":\"kafka-cluster:Connect\",");
        str_builder_add_str(sb, "\"x-amz-algorithm\":\"AWS4-HMAC-SHA256\",");
        str_builder_add_str(sb, "\"x-amz-credential\":\"");
        str_builder_add_str(sb, credential);
        str_builder_add_str(sb, "\",");
        str_builder_add_str(sb, "\"x-amz-date\":\"");
        str_builder_add_str(sb, amz_date_str);
        str_builder_add_str(sb, "\",");
        
        if (security_token != NULL) {
            str_builder_add_str(sb, "\"x-amz-security-token\":\"");
            str_builder_add_str(sb, security_token);
            str_builder_add_str(sb, "\",");
        }
        
        str_builder_add_str(sb, "\"x-amz-signedheaders\":\"host\",");
        str_builder_add_str(sb, "\"x-amz-expires\":\"900\",");
        str_builder_add_str(sb, "\"x-amz-signature\":\"");
        str_builder_add_str(sb, signature);
        str_builder_add_str(sb, "\"}");

        char *sasl_payload = str_builder_dump(sb);
        str_builder_destroy(sb);
        
        return sasl_payload;
}


/**
 * @brief Build client first message
 * 
 *        Builds the first message for the payload
 *        by combining canonical request, signature, and credentials.
 *
 * @remark out->ptr is allocated and will need to be freed.
 */
static void 
rd_kafka_sasl_aws_msk_iam_build_client_first_message (
        rd_kafka_transport_t *rktrans, 
        rd_chariov_t *out) {
        struct rd_kafka_sasl_aws_msk_iam_state *state = rktrans->rktrans_sasl.state;
        char *raw_canonical_request = rd_kafka_sasl_aws_msk_iam_build_canonical_request(state);
        
        rd_chariov_t canonical_request;
        canonical_request.ptr = raw_canonical_request;
        canonical_request.size = strlen(raw_canonical_request);

        char *signature = rd_kafka_sasl_aws_msk_iam_calculate_signature(state, canonical_request);
        
        rd_chariov_t credential_wo_prefix = rd_construct_amz_credential(state->aws_region, (const char *)state->ymd);
        rd_chariov_t credential = rd_construct_amz_credential_add_key_prefix(credential_wo_prefix, state->aws_access_key_id);
        
        rd_chariov_t amz_date = rd_construct_amz_date((const char *)state->ymd, (const char *)state->hms);

        char *sasl_payload = rd_kafka_sasl_aws_msk_iam_build_request_json(state->hostname.ptr, credential.ptr, amz_date.ptr, signature, state->aws_security_token.ptr);
        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY,
                           "SASLAWSMSKIAM",
                           "SASL payload calculated as %s",
                           sasl_payload);
        
        /* Save JSON to out pointer for sending */
        out->size = strlen(sasl_payload);
        out->ptr = rd_malloc(out->size + 1);
        
        rd_snprintf(out->ptr, out->size + 1,
                    "%s", sasl_payload);
        
        RD_IF_FREE(sasl_payload, rd_free);
        RD_IF_FREE(credential_wo_prefix.ptr, rd_free);
        RD_IF_FREE(credential.ptr, rd_free);
        RD_IF_FREE(amz_date.ptr, rd_free);
        RD_IF_FREE(signature, rd_free);
        RD_IF_FREE(raw_canonical_request, rd_free);
}


/**
 * @brief Handle server-response
 * 
 *        This is the end of authentication and the AWS MSK IAM state
 *        will be freed at the end of this function regardless of
 *        authentication outcome.
 *
 * @returns -1 on failure
 */
static int
rd_kafka_sasl_aws_msk_iam_handle_server_response (
        rd_kafka_transport_t *rktrans,
        const rd_chariov_t *in,
        char *errstr, size_t errstr_size) {
        if (in->size) {
            rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER, "SASLAWSMSKIAM",
                           "Received non-empty SASL AWS MSK IAM (builtin) "
                           "response from broker (%s)", in->ptr);
            rd_kafka_sasl_auth_done(rktrans);
            return 0;
        } else {
            rd_snprintf(errstr, errstr_size,
                        "SASL AWS MSK IAM authentication failed: "
                        "Broker response: %s", in->ptr);
            return -1;
        }
}


/**
 * @brief SASL AWS MSK IAM client state machine
 * @returns -1 on failure (errstr set), else 0.
 */
static int rd_kafka_sasl_aws_msk_iam_fsm (rd_kafka_transport_t *rktrans,
                                    const rd_chariov_t *in,
                                    char *errstr, size_t errstr_size) {
        static const char *state_names[] = {
                    "client-first-message",
                    "server-response",
        };
        struct rd_kafka_sasl_aws_msk_iam_state *state = rktrans->rktrans_sasl.state;
        rd_chariov_t out = RD_ZERO_INIT;
        int r = -1;
        rd_ts_t ts_start = rd_clock();
        int prev_state = state->state;
        
        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER, "SASLAWSMSKIAM",
                   "SASL AWS MSK IAM client in state %s",
                   state_names[state->state]);
        
        switch (state->state)
        {
        case RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE:
            rd_assert(!in); /* Not expecting any server-input */
            
            rd_kafka_sasl_aws_msk_iam_build_client_first_message(rktrans, &out);
            state->state = RD_KAFKA_SASL_AWS_MSK_IAM_RECEIVE_SERVER_RESPONSE;
            break;
        case RD_KAFKA_SASL_AWS_MSK_IAM_RECEIVE_SERVER_RESPONSE:
            rd_assert(in);  /* Requires server-input */
            r = rd_kafka_sasl_aws_msk_iam_handle_server_response(
                        rktrans, in, errstr, errstr_size);
            break;
        }
        
        if (out.ptr) {
                r = rd_kafka_sasl_send(rktrans, out.ptr, (int)out.size,
                                       errstr, errstr_size);
                RD_IF_FREE(out.ptr, rd_free);
        }
        
        ts_start = (rd_clock() - ts_start) / 1000;
        if (ts_start >= 100)
                rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER, "SASLAWSMSKIAM",
                           "SASL AWS MSK IAM state %s handled in %"PRId64"ms",
                           state_names[prev_state], ts_start);
        
        return r;
}


/**
 * @brief Initialize and start SASL AWS MSK IAM (builtin) authentication.
 *
 * Returns 0 on successful init and -1 on error.
 *
 * @locality broker thread
 */
static int rd_kafka_sasl_aws_msk_iam_client_new (rd_kafka_transport_t *rktrans,
                                    const char *hostname,
                                    char *errstr, size_t errstr_size) {
        struct rd_kafka_sasl_aws_msk_iam_state *state;
        const rd_kafka_conf_t *conf = &rktrans->rktrans_rkb->rkb_rk->rk_conf;
        
        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER, "SASLAWSMSKIAM",
                   "SASL AWS MSK IAM new client initializing");

        state = rd_calloc(1, sizeof(*state));
        state->hostname.ptr = (char *)hostname;
        state->hostname.size = strlen(hostname);
        state->aws_access_key_id.ptr = conf->sasl.aws_access_key_id;
        state->aws_access_key_id.size = strlen(conf->sasl.aws_access_key_id);
        state->aws_secret_access_key.ptr = conf->sasl.aws_secret_access_key;
        state->aws_secret_access_key.size = strlen(conf->sasl.aws_secret_access_key);
        state->aws_region.ptr = conf->sasl.aws_region;
        state->aws_region.size = strlen(conf->sasl.aws_region);
        
        if (conf->sasl.aws_security_token != NULL) {
            state->aws_security_token.ptr = conf->sasl.aws_security_token;
            state->aws_security_token.size = strlen(conf->sasl.aws_security_token);
        }
        
        state->md = EVP_get_digestbyname("SHA256");
        state->state = RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE;
        
        time_t t = time(&t);
        struct tm *tmp = gmtime(&t);  // must use UTC time
        strftime(state->ymd, sizeof(state->ymd), "%Y%m%d", tmp);
        strftime(state->hms, sizeof(state->hms), "%H%M%S", tmp);
        
        rktrans->rktrans_sasl.state = state;
        
        /* Kick off the FSM */
        return rd_kafka_sasl_aws_msk_iam_fsm(rktrans, NULL, errstr, errstr_size);
}


/**
 * @brief Handle received frame from broker.
 */
static int rd_kafka_sasl_aws_msk_iam_recv (rd_kafka_transport_t *rktrans,
                                     const void *buf, size_t size,
                                     char *errstr, size_t errstr_size) {
        const rd_chariov_t in = { .ptr = (char *)buf, .size = size };
        return rd_kafka_sasl_aws_msk_iam_fsm(rktrans, &in, errstr, errstr_size);
}


/**
 * @brief Validate AWS MSK IAM config and look up the hash function
 */
static int rd_kafka_sasl_aws_msk_iam_conf_validate (rd_kafka_t *rk,
                                              char *errstr,
                                              size_t errstr_size) {        
        if (!rk->rk_conf.sasl.aws_access_key_id || !rk->rk_conf.sasl.aws_secret_access_key || !rk->rk_conf.sasl.aws_region) {
                rd_snprintf(errstr, errstr_size,
                            "sasl.aws_access_key_id, sasl.aws_secret_access_key, and sasl.aws_region must be set");
                return -1;
        }
        
        return 0;
}


/**
 * @brief Close and free authentication state
 */
static void rd_kafka_sasl_aws_msk_iam_close (rd_kafka_transport_t *rktrans) {
        struct rd_kafka_sasl_aws_msk_iam_state *state = rktrans->rktrans_sasl.state;

        if (!state)
                return;
         
        rd_free(state);
}


const struct rd_kafka_sasl_provider rd_kafka_sasl_aws_msk_iam_provider = {
        .name           = "AWS_MSK_IAM",
        .client_new     = rd_kafka_sasl_aws_msk_iam_client_new,
        .recv           = rd_kafka_sasl_aws_msk_iam_recv,
        .close          = rd_kafka_sasl_aws_msk_iam_close,
        .conf_validate  = rd_kafka_sasl_aws_msk_iam_conf_validate,
};


/**
 * @name Unit tests
 */

/**
 * @brief Verify that a request JSON can be formed properly.
 */
static int unittest_build_request_json (void) {
        RD_UT_BEGIN();
        char *sasl_payload = rd_kafka_sasl_aws_msk_iam_build_request_json (
        "hostname",
        "AWS_ACCESS_KEY_ID/20100101/us-east-1/kafka-cluster/aws4_request",
        "20100101T000000Z",
        "d3eeeddfb2c2b76162d583d7499c2364eb9a92b248218e31866659b18997ef44",
        "security-token");
        
        const char *expected = 
            "{\"version\":\"2020_10_22\",\"host\":\"hostname\","
            "\"user-agent\":\"librdkafka\",\"action\":\"kafka-cluster:Connect\","
            "\"x-amz-algorithm\":\"AWS4-HMAC-SHA256\","
            "\"x-amz-credential\":\"AWS_ACCESS_KEY_ID/20100101/us-east-1/kafka-cluster/aws4_request\","
            "\"x-amz-date\":\"20100101T000000Z\","
            "\"x-amz-security-token\":\"security-token\","
            "\"x-amz-signedheaders\":\"host\","
            "\"x-amz-expires\":\"900\","
            "\"x-amz-signature\":\"d3eeeddfb2c2b76162d583d7499c2364eb9a92b248218e31866659b18997ef44\"}";
        RD_UT_ASSERT(strcmp(expected, sasl_payload) == 0, "expected: %s\nactual: %s", expected, sasl_payload);
        
        RD_IF_FREE(sasl_payload, rd_free);
        RD_UT_PASS();
}

/**
 * @brief Verify that a signature can be calculated properly.
 */
static int unittest_calculate_signature (void) {
        RD_UT_BEGIN();
        int hostname_str_size = strlen("hostname") + 1;
        char *hostname_str;
        
        hostname_str = rd_malloc(hostname_str_size);
        rd_snprintf(hostname_str, hostname_str_size, "hostname");
        
        rd_chariov_t hostname =
                { .ptr = hostname_str, .size = strlen(hostname_str) };
        
        struct rd_kafka_sasl_aws_msk_iam_state state =
        {
            RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE, "20100101", "000000", hostname
        };
        state.aws_access_key_id.ptr = "AWS_ACCESS_KEY_ID";
        state.aws_access_key_id.size = strlen("AWS_ACCESS_KEY_ID");
        state.aws_secret_access_key.ptr = "AWS_SECRET_ACCESS_KEY";
        state.aws_secret_access_key.size = strlen("AWS_SECRET_ACCESS_KEY");
        state.aws_region.ptr = "us-east-1";
        state.aws_region.size = strlen("us-east-1");
        state.md = EVP_get_digestbyname("SHA256");
        
        char *cr = rd_kafka_sasl_aws_msk_iam_build_canonical_request(&state);
        rd_chariov_t canonical_request =
                { .ptr = cr, .size = strlen(cr) };
        char *signature = rd_kafka_sasl_aws_msk_iam_calculate_signature(&state, canonical_request);
        
        const char *expected = "d3eeeddfb2c2b76162d583d7499c2364eb9a92b248218e31866659b18997ef44";
        RD_UT_ASSERT(strcmp(expected, signature) == 0, "expected: %s\nactual: %s", expected, signature);
    
        RD_IF_FREE(signature, rd_free);
        RD_IF_FREE(cr, rd_free);
        RD_IF_FREE(hostname.ptr, rd_free);
        RD_UT_PASS();
}


/**
 * @brief Verify that a canonical request can be formed properly.
 */
static int unittest_build_canonical_request (void) {       
        RD_UT_BEGIN();
        int hostname_str_size = strlen("hostname") + 1;
        char *hostname_str;
        
        hostname_str = rd_malloc(hostname_str_size);
        rd_snprintf(hostname_str, hostname_str_size, "hostname");
        
        rd_chariov_t hostname =
                { .ptr = hostname_str, .size = strlen(hostname_str) };
        
        struct rd_kafka_sasl_aws_msk_iam_state state =
        {
            RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE, "20100101", "000000", hostname
        };
        state.aws_access_key_id.ptr = "AWS_ACCESS_KEY_ID";
        state.aws_access_key_id.size = strlen("AWS_ACCESS_KEY_ID");
        state.aws_secret_access_key.ptr = "AWS_SECRET_ACCESS_KEY";
        state.aws_secret_access_key.size = strlen("AWS_SECRET_ACCESS_KEY");
        state.aws_region.ptr = "us-east-1";
        state.aws_region.size = strlen("us-east-1");
        state.md = EVP_get_digestbyname("SHA256");
        char *cr = rd_kafka_sasl_aws_msk_iam_build_canonical_request(&state);
        
        const char *expected = 
            "GET\n/\n"
            "Action=kafka-cluster%3AConnect&"
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            "X-Amz-Credential=AWS_ACCESS_KEY_ID%2F20100101%2Fus-east-1%2Fkafka-cluster%2Faws4_request&"
            "X-Amz-Date=20100101T000000Z&"
            "X-Amz-Expires=900&"
            "X-Amz-SignedHeaders=host\n"
            "host:hostname\n\n"
            "host\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        RD_UT_ASSERT(strcmp(expected, cr) == 0, "expected: %s\nactual: %s", expected, cr);
   
        RD_IF_FREE(cr, rd_free);
        RD_IF_FREE(hostname.ptr, rd_free);
        RD_UT_PASS();
}


/**
 * @brief Verify that a canonical request can be formed properly.
 */
static int unittest_build_canonical_request_with_security_token (void) {       
        RD_UT_BEGIN();
        int hostname_str_size = strlen("hostname") + 1;
        char *hostname_str;
        
        hostname_str = rd_malloc(hostname_str_size);
        rd_snprintf(hostname_str, hostname_str_size, "hostname");
        
        rd_chariov_t hostname =
                { .ptr = hostname_str, .size = strlen(hostname_str) };
        
        struct rd_kafka_sasl_aws_msk_iam_state state =
        {
            RD_KAFKA_SASL_AWS_MSK_IAM_SEND_CLIENT_FIRST_MESSAGE, "20100101", "000000", hostname
        };
        state.aws_access_key_id.ptr = "AWS_ACCESS_KEY_ID";
        state.aws_access_key_id.size = strlen("AWS_ACCESS_KEY_ID");
        state.aws_secret_access_key.ptr = "AWS_SECRET_ACCESS_KEY";
        state.aws_secret_access_key.size = strlen("AWS_SECRET_ACCESS_KEY");
        state.aws_region.ptr = "us-east-1";
        state.aws_region.size = strlen("us-east-1");
        state.aws_security_token.ptr = "security-token";
        state.aws_security_token.size = strlen("security-token");
        state.md = EVP_get_digestbyname("SHA256");
        char *cr = rd_kafka_sasl_aws_msk_iam_build_canonical_request(&state);
        
        const char *expected = 
            "GET\n/\n"
            "Action=kafka-cluster%3AConnect&"
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            "X-Amz-Credential=AWS_ACCESS_KEY_ID%2F20100101%2Fus-east-1%2Fkafka-cluster%2Faws4_request&"
            "X-Amz-Date=20100101T000000Z&"
            "X-Amz-Expires=900&"
            "X-Amz-Security-Token=security-token&"
            "X-Amz-SignedHeaders=host\n"
            "host:hostname\n\n"
            "host\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        RD_UT_ASSERT(strcmp(expected, cr) == 0, "expected: %s\nactual: %s", expected, cr);
   
        RD_IF_FREE(cr, rd_free);
        RD_IF_FREE(hostname.ptr, rd_free);
        RD_UT_PASS();
}


/**
 * @brief Verify that a uri encoding / escaping works as expected.
 */
static int unittest_uri_encode (void) {
        RD_UT_BEGIN();
        int test_str_size = strlen("testString-123/*&") + 1;
        char *test_str;
        
        test_str = rd_malloc(test_str_size);
        rd_snprintf(test_str, test_str_size, "testString-123/*&");
        
        rd_chariov_t in =
                { .ptr = test_str, .size = strlen(test_str) };

        char *retval = rd_uri_encode(in);

        const char *expected = "testString-123%2F%2A%26";
        RD_UT_ASSERT(strcmp(expected, retval) == 0, "expected: %s\nactual: %s", expected, retval);

        RD_IF_FREE(retval, rd_free);
        RD_IF_FREE(in.ptr, rd_free);
        RD_UT_PASS();
}


int unittest_aws_msk_iam (void) {
        int fails = 0;

        fails += unittest_uri_encode();
        fails += unittest_build_canonical_request();
        fails += unittest_build_canonical_request_with_security_token();
        fails += unittest_calculate_signature();
        fails += unittest_build_request_json();

        return fails;
}
