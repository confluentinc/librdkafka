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
 *
 * This implementation uses OpenSSL's Base64 encoder and then
 * replaces characters as needed for Base64Url format:
 * - '+' is replaced with '-'
 * - '/' is replaced with '_'
 * - Padding '=' characters are removed
 *
 * @param input The data to encode
 * @param length Length of the input data
 *
 * @returns Newly allocated Base64Url encoded string, caller must free with
 * rd_free(). Returns NULL if memory allocation fails.
 *
 * @locality Any thread.
 */
static char *rd_base64url_encode(const char *input, const size_t length) {
        BIO *bmem = NULL, *b64 = NULL;
        BUF_MEM *bptr = NULL;
        char *buff    = NULL, *p;

        b64 = BIO_new(BIO_f_base64());
        if (!b64)
                return NULL;

        /* Do not use '\n' in encoded data */
        BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

        bmem = BIO_new(BIO_s_mem());
        if (!bmem) {
                BIO_free_all(b64);
                return NULL;
        }

        b64 = BIO_push(b64, bmem);

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
 * @brief Get JWT algorithm label string for the specified signing algorithm.
 *
 * @param token_signing_algo The algorithm enum value
 *
 * @returns String representation of the algorithm. Default is "RS256".
 *
 * @locality Any thread.
 */
static char *get_algo_label(
    const rd_kafka_oauthbearer_assertion_algorithm_t token_signing_algo) {
        switch (token_signing_algo) {
        case RD_KAFKA_SASL_OAUTHBEARER_ASSERTION_ALGORITHM_RS256:
                return "RS256";
        case RD_KAFKA_SASL_OAUTHBEARER_ASSERTION_ALGORITHM_ES256:
                return "ES256";
        default:
                return "RS256";
        }
}

/**
 * @brief Process a JWT template file and extract header and payload JSON
 * objects.
 *
 * Reads and parses the JWT template file, which should contain a JSON object
 * with "header" and "payload" properties.
 *
 * @param rk
 * @param jwt_template_file_path Path to the template file
 * @param header Pointer to store the parsed header JSON object
 * @param payload Pointer to store the parsed payload JSON object
 *
 * @returns 0 on success, -1 on failure
 *
 * @locality Any thread.
 */
static int process_jwt_template_file(rd_kafka_t *rk,
                                     const char *jwt_template_file_path,
                                     cJSON **header,
                                     cJSON **payload) {
        char *template_content = NULL;
        cJSON *template_json   = NULL;
        int ret                = -1;
        long file_size;
        size_t read_size;
        FILE *fp;

        *header  = NULL;
        *payload = NULL;

        fp = fopen(jwt_template_file_path, "r");
        if (!fp) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to open JWT template file: %s",
                             jwt_template_file_path);
                return -1;
        }

        fseek(fp, 0, SEEK_END);
        file_size = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        if (file_size <= 0) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "JWT template file is empty or invalid");
                fclose(fp);
                return -1;
        }

        template_content = rd_malloc(file_size + 1);
        if (!template_content) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to allocate memory for template content");
                fclose(fp);
                return -1;
        }

        read_size = fread(template_content, 1, file_size, fp);
        fclose(fp);

        if (read_size != (size_t)file_size) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to read JWT template file");
                goto cleanup;
        }

        template_content[file_size] = '\0';

        template_json = cJSON_Parse(template_content);
        if (!template_json) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to parse JWT template JSON");
                goto cleanup;
        }

        cJSON *header_item  = cJSON_GetObjectItem(template_json, "header");
        cJSON *payload_item = cJSON_GetObjectItem(template_json, "payload");

        if (!header_item || !payload_item) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "JWT template must contain both 'header' "
                             "and 'payload' objects");
                goto cleanup;
        }

        *header  = cJSON_Duplicate(header_item, 1);
        *payload = cJSON_Duplicate(payload_item, 1);

        if (!*header || !*payload) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to duplicate header or payload objects");
                if (*header) {
                        cJSON_Delete(*header);
                        *header = NULL;
                }
                goto cleanup;
        }

        ret = 0;

cleanup:
        if (template_content)
                rd_free(template_content);
        if (template_json)
                cJSON_Delete(template_json);

        return ret;
}

/**
 * @brief Create JWT assertion.
 *
 * Creates a JWT token signed with the specified private key using the
 * algorithm specified. The token can be created from a template file or
 * will create a minimal default token if no template is provided.
 *
 * @param rk The rd_kafka_t instance for logging
 * @param private_key_pem PEM formatted private key string (mutually exclusive
 * with key_file_location)
 * @param key_file_location Path to private key file (mutually exclusive with
 * private_key_pem)
 * @param passphrase Optional passphrase for encrypted private key
 * @param token_signing_algo Algorithm to use for signing (RS256 or ES256)
 * @param jwt_template_file Optional path to JWT template file
 * @param issued_at Optional offset for the "iat" claim. If 0, current time is
 * used
 * @param expiry Optional token expiration time in seconds from now. If 0,
 * default 300s is used
 *
 * @returns Newly allocated JWT string, caller must free with rd_free(). NULL on
 * error.
 *
 * @locality Any thread.
 */
static char *rd_kafka_create_jwt_assertion(
    rd_kafka_t *rk,
    const char *private_key_pem,
    const char *key_file_location,
    const char *passphrase,
    const rd_kafka_oauthbearer_assertion_algorithm_t token_signing_algo,
    const char *jwt_template_file,
    const int issued_at,
    const int expiry) {

        char *encoded_header    = NULL;
        char *encoded_payload   = NULL;
        char *encoded_signature = NULL;
        char *unsigned_token    = NULL;
        char *result            = NULL;
        char *header_str        = NULL;
        char *payload_str       = NULL;
        EVP_PKEY *pkey          = NULL;
        BIO *bio                = NULL;
        cJSON *header_json_obj  = NULL;
        cJSON *payload_json_obj = NULL;
        EVP_MD_CTX *mdctx       = NULL;
        unsigned char *sig      = NULL;

        if ((private_key_pem && key_file_location) ||
            (!private_key_pem && !key_file_location)) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Exactly one of private_key_pem or "
                             "key_file_location must be provided");
                return NULL;
        }
        RD_UT_SAY("coming after key check");

        time_t now = issued_at ? rd_uclock() / 1000000 + issued_at
                               : rd_uclock() / 1000000;
        time_t exp = expiry ? rd_uclock() / 1000000 + expiry
                            : rd_uclock() / 1000000 + 300;

        if (jwt_template_file) {
                if (process_jwt_template_file(rk, jwt_template_file,
                                              &header_json_obj,
                                              &payload_json_obj) != 0) {
                        rd_kafka_log(rk, LOG_ERR, "JWT",
                                     "Failed to process JWT template file %s",
                                     jwt_template_file);
                        return NULL;
                }

                /* Add required header fields */
                cJSON_AddStringToObject(header_json_obj, "alg",
                                        get_algo_label(token_signing_algo));
                cJSON_AddStringToObject(header_json_obj, "typ", "JWT");

                /* Add required payload fields */
                cJSON_AddNumberToObject(payload_json_obj, "iat", (double)now);
                cJSON_AddNumberToObject(payload_json_obj, "exp", (double)exp);

                header_str  = cJSON_PrintUnformatted(header_json_obj);
                payload_str = cJSON_PrintUnformatted(payload_json_obj);

                if (!header_str || !payload_str) {
                        rd_kafka_log(
                            rk, LOG_ERR, "JWT",
                            "Failed to convert template objects to JSON");
                        goto cleanup;
                }
        } else {
                /* Build standard header and payload without template */
                header_str  = rd_malloc(256);
                payload_str = rd_malloc(1024);

                if (!header_str || !payload_str)
                        goto cleanup;

                rd_snprintf(header_str, 256, "{\"alg\":\"%s\",\"typ\":\"JWT\"}",
                            get_algo_label(token_signing_algo));

                rd_snprintf(payload_str, 1024, "{\"iat\":%ld,\"exp\":%ld}",
                            (long)now, (long)exp);
        }

        encoded_header  = rd_base64url_encode(header_str, strlen(header_str));
        encoded_payload = rd_base64url_encode(payload_str, strlen(payload_str));
        if (!encoded_header || !encoded_payload)
                goto cleanup;

        size_t unsigned_token_len =
            strlen(encoded_header) + strlen(encoded_payload) + 2;
        unsigned_token = rd_malloc(unsigned_token_len);
        if (!unsigned_token)
                goto cleanup;
        rd_snprintf(unsigned_token, unsigned_token_len, "%s.%s", encoded_header,
                    encoded_payload);
        RD_UT_SAY("unsigned_token %s", unsigned_token);

        if (private_key_pem) {
                bio = BIO_new_mem_buf((void *)private_key_pem, -1);
        } else if (key_file_location) {
                bio = BIO_new_file(key_file_location, "r");
        }

        if (!bio) {
                RD_UT_SAY("bio null");
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to create BIO for private key");
                goto cleanup;
        }
        RD_UT_SAY("bio %p", bio);

        if (passphrase) {
                pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL,
                                               (void *)passphrase);
        } else {
                pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
                RD_UT_SAY("pkey %p", pkey);
        }
        BIO_free(bio);
        bio = NULL;

        if (!pkey) {
                rd_kafka_log(rk, LOG_ERR, "JWT", "Failed to load private key");
                goto cleanup;
        }
        RD_UT_SAY("read pkey");

        mdctx = EVP_MD_CTX_new();
        if (!mdctx) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to create message digest context");
                goto cleanup;
        }

        const EVP_MD *md = EVP_sha256(); /* Both RS256 and ES256 use SHA-256 */

        if (EVP_DigestSignInit(mdctx, NULL, md, NULL, pkey) != 1) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to initialize signing context");
                goto cleanup;
        }

        if (EVP_DigestSignUpdate(mdctx, unsigned_token,
                                 strlen(unsigned_token)) != 1) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to update digest with token data");
                goto cleanup;
        }

        size_t siglen = 0;
        if (EVP_DigestSignFinal(mdctx, NULL, &siglen) != 1) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to get signature length");
                goto cleanup;
        }

        sig = rd_malloc(siglen);
        if (!sig) {
                rd_kafka_log(rk, LOG_ERR, "JWT",
                             "Failed to allocate memory for signature");
                goto cleanup;
        }

        if (EVP_DigestSignFinal(mdctx, sig, &siglen) != 1) {
                rd_kafka_log(rk, LOG_ERR, "JWT", "Failed to create signature");
                goto cleanup;
        }

        encoded_signature = rd_base64url_encode(sig, siglen);
        if (!encoded_signature)
                goto cleanup;

        size_t jwt_len = strlen(encoded_header) + strlen(encoded_payload) +
                         strlen(encoded_signature) + 3;
        result = rd_malloc(jwt_len);
        if (!result)
                goto cleanup;
        rd_snprintf(result, jwt_len, "%s.%s.%s", encoded_header,
                    encoded_payload, encoded_signature);

cleanup:
        if (encoded_header)
                rd_free(encoded_header);
        if (encoded_payload)
                rd_free(encoded_payload);
        if (encoded_signature)
                rd_free(encoded_signature);
        if (unsigned_token)
                rd_free(unsigned_token);
        if (sig)
                rd_free(sig);

        if (header_json_obj) {
                if (header_str)
                        free(header_str); /* cJSON_PrintUnformatted uses malloc
                                           */
                cJSON_Delete(header_json_obj);
        } else if (header_str) {
                rd_free(header_str); /* rd_malloc was used */
        }

        if (payload_json_obj) {
                if (payload_str)
                        free(payload_str); /* cJSON_PrintUnformatted uses malloc
                                            */
                cJSON_Delete(payload_json_obj);
        } else if (payload_str) {
                rd_free(payload_str); /* rd_malloc was used */
        }

        if (pkey)
                EVP_PKEY_free(pkey);
        if (mdctx)
                EVP_MD_CTX_free(mdctx);

        return result;
}


/**
 * @brief Build request body for JWT bearer token request.
 *
 * Creates a URL-encoded request body for token exchange with the JWT assertion.
 *
 * @param assertion The JWT assertion to include in the request.
 *
 * @returns Newly allocated string with the URL-encoded request body.
 *          Caller must free with rd_free(). NULL on memory allocation failure.
 *
 * @locality Any thread.
 */
static char *rd_kafka_jwt_build_request_body(const char *assertion) {
        const char *grant_type = "urn:ietf:params:oauth:grant-type:jwt-bearer";
        size_t body_size       = strlen("grant_type=") + strlen(grant_type) +
                           strlen("&assertion=") + strlen(assertion) + 1;
        char *body = rd_malloc(body_size);

        if (!body)
                return NULL;

        rd_snprintf(body, body_size, "grant_type=%s&assertion=%s", grant_type,
                    assertion);
        return body;
}

/*
 * Parse JWT assertion from file
 * @param file_path Path to the file containing the JWT assertion
 * @returns Newly allocated string with the JWT assertion.
 *          Caller must free with rd_free(). NULL on memory allocation failure.
 */
static char *parse_jwt_assertion_from_file(const char *file_path) {
        FILE *file;
        size_t file_size;
        char *jwt_assertion;
        size_t bytes_read;
        const size_t max_size = 1024 * 1024;

        if (!file_path) {
                return NULL;
        }

        file = fopen(file_path, "r");
        if (!file) {
                return NULL;
        }

        if (fseek(file, 0, SEEK_END) != 0) {
                fclose(file);
                return NULL;
        }
        file_size = ftell(file);
        if (fseek(file, 0, SEEK_SET) != 0) {
                fclose(file);
                return NULL;
        }

        /* Validate file size */
        if (file_size == 0 || file_size > max_size) {
                fclose(file);
                return NULL;
        }

        jwt_assertion = rd_malloc(file_size + 1);
        if (!jwt_assertion) {
                fclose(file);
                return NULL;
        }

        bytes_read = fread(jwt_assertion, 1, file_size, file);
        fclose(file);

        if (bytes_read != file_size) {
                rd_free(jwt_assertion);
                return NULL;
        }

        jwt_assertion[file_size] = '\0';

        return jwt_assertion;
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

        if (rk->rk_conf.sasl.oauthbearer.assertion_file) {
                jwt_assertion = parse_jwt_assertion_from_file(
                    rk->rk_conf.sasl.oauthbearer.assertion_file);
        } else {
                jwt_assertion = rd_kafka_create_jwt_assertion(
                    rk, rk->rk_conf.sasl.oauthbearer.assertion_private_key_pem,
                    rk->rk_conf.sasl.oauthbearer.assertion_private_key_file,
                    rk->rk_conf.sasl.oauthbearer
                        .assertion_private_key_passphrase,
                    rk->rk_conf.sasl.oauthbearer.assertion_signing_algorithm,
                    rk->rk_conf.sasl.oauthbearer.assertion_jwt_template_file,
                    rk->rk_conf.sasl.oauthbearer.assertion_issued_at,
                    rk->rk_conf.sasl.oauthbearer.assertion_expiration);
        }

        if (!jwt_assertion) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Failed to create JWT assertion");
                goto done;
        }

        request_body = rd_kafka_jwt_build_request_body(jwt_assertion);

        if (!request_body) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "Failed to build JWT request body");
                goto done;
        }

        headers = curl_slist_append(
            headers, "Content-Type: application/x-www-form-urlencoded");
        headers = curl_slist_append(headers, "Accept: application/json");

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

        exp = rk->rk_conf.sasl.oauthbearer.assertion_expiration
                  ? rd_uclock() / 1000000 +
                        rk->rk_conf.sasl.oauthbearer.assertion_expiration
                  : rd_uclock() / 1000000 + 300;
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
                rk->rk_conf.sasl.oauthbearer.client_id,
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
        RD_IF_FREE(extensions, rd_free);
        RD_IF_FREE(extension_key_value, rd_free);
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
            {"test", "dGVzdA"},
            /* Empty string */
            {"", ""},
            /* Special characters that trigger Base64 padding */
            {"f", "Zg"},
            {"fo", "Zm8"},
            {"foo", "Zm9v"},
            {"foob", "Zm9vYg"},
            {"fooba", "Zm9vYmE"},
            {"foobar", "Zm9vYmFy"},
            /* Characters that produce + and / in standard Base64 */
            {"\x3E\x3F",
             "Pj8"}, /* encodes to ">?" in standard Base64 with + and / */
        };
        unsigned int i;

        RD_UT_BEGIN();

        for (i = 0; i < RD_ARRAYSIZE(test_cases); i++) {
                char *output = rd_base64url_encode(test_cases[i].input,
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
 *        the required parameters.
 */
static int ut_sasl_jwt_build_request_body(void) {
        const char *assertion = "test.jwt.assertion";
        const char *expected =
            "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion="
            "test.jwt.assertion";
        char *body;

        RD_UT_BEGIN();

        body = rd_kafka_jwt_build_request_body(assertion);

        RD_UT_ASSERT(body != NULL, "Expected non-NULL request body");

        RD_UT_ASSERT(!strcmp(body, expected),
                     "Request body incorrect: expected '%s', got '%s'",
                     expected, body);

        rd_free(body);

        RD_UT_PASS();
}

/**
 * @brief Test JWT assertion file parsing.
 *        Verifies that the function correctly reads a JWT from a file.
 */
static int ut_sasl_jwt_parse_assertion_from_file(void) {
        char temp_name[L_tmpnam];
        char *tempfile_path;
        FILE *tempfile;
        const char *test_jwt = "header.payload.signature";
        char *result;

        RD_UT_BEGIN();

        /* Create temporary file with test JWT */
        tmpnam(temp_name);
        tempfile_path = rd_strdup(temp_name);

        tempfile = fopen(tempfile_path, "w");
        RD_UT_ASSERT(tempfile != NULL, "Failed to open temporary file");

        fprintf(tempfile, "%s", test_jwt);
        fclose(tempfile);

        /* Test parsing from file */
        result = parse_jwt_assertion_from_file(tempfile_path);
        RD_UT_ASSERT(result != NULL,
                     "Expected non-NULL result from parsing file");
        RD_UT_ASSERT(!strcmp(result, test_jwt),
                     "Incorrect JWT parsed: expected '%s', got '%s'", test_jwt,
                     result);

        rd_free(result);

        /* Test with NULL path */
        result = parse_jwt_assertion_from_file(NULL);
        RD_UT_ASSERT(result == NULL, "Expected NULL result with NULL path");

        /* Test with non-existent file */
        result = parse_jwt_assertion_from_file("/non/existent/file/path");
        RD_UT_ASSERT(result == NULL,
                     "Expected NULL result with non-existent file");

        remove(tempfile_path);
        rd_free(tempfile_path);

        RD_UT_PASS();
}

/**
 * @brief Mock function for testing JWT template processing.
 *        Creates a file with valid JWT template JSON.
 */
static char *create_mock_jwt_template_file(void) {
        char temp_name[L_tmpnam];
        char *tempfile_path;
        FILE *tempfile;
        const char *template_json =
            "{\n"
            "  \"header\": {\n"
            "    \"kid\": \"test-key-id\"\n"
            "  },\n"
            "  \"payload\": {\n"
            "    \"sub\": \"test-subject\",\n"
            "    \"aud\": \"test-audience\"\n"
            "  }\n"
            "}";

        tmpnam(temp_name);
        tempfile_path = rd_strdup(temp_name);

        tempfile = fopen(tempfile_path, "w");
        if (!tempfile) {
                rd_free(tempfile_path);
                return NULL;
        }

        fprintf(tempfile, "%s", template_json);
        fclose(tempfile);

        return tempfile_path;
}

/**
 * @brief Test JWT template file processing.
 *        Verifies that the function correctly parses header and payload from
 * template.
 */
static int ut_sasl_jwt_process_template_file(void) {
        char *template_path;
        rd_kafka_t *rk;
        cJSON *header = NULL, *payload = NULL;
        int result;

        RD_UT_BEGIN();

        rk = rd_calloc(1, sizeof(*rk));

        template_path = create_mock_jwt_template_file();
        RD_UT_ASSERT(template_path != NULL, "Failed to create template file");

        /* Test template processing */
        result =
            process_jwt_template_file(rk, template_path, &header, &payload);
        RD_UT_ASSERT(result == 0, "Expected success from template processing");
        RD_UT_ASSERT(header != NULL, "Expected non-NULL header JSON");
        RD_UT_ASSERT(payload != NULL, "Expected non-NULL payload JSON");

        /* Verify header contents */
        cJSON *kid = cJSON_GetObjectItem(header, "kid");
        RD_UT_ASSERT(kid != NULL, "Expected kid in header");
        RD_UT_ASSERT(cJSON_IsString(kid), "Expected kid to be string");
        RD_UT_ASSERT(!strcmp(cJSON_GetStringValue(kid), "test-key-id"),
                     "Incorrect kid value");

        /* Verify payload contents */
        cJSON *sub = cJSON_GetObjectItem(payload, "sub");
        RD_UT_ASSERT(sub != NULL, "Expected sub in payload");
        RD_UT_ASSERT(cJSON_IsString(sub), "Expected sub to be string");
        RD_UT_ASSERT(!strcmp(cJSON_GetStringValue(sub), "test-subject"),
                     "Incorrect sub value");

        cJSON *aud = cJSON_GetObjectItem(payload, "aud");
        RD_UT_ASSERT(aud != NULL, "Expected aud in payload");
        RD_UT_ASSERT(cJSON_IsString(aud), "Expected aud to be string");
        RD_UT_ASSERT(!strcmp(cJSON_GetStringValue(aud), "test-audience"),
                     "Incorrect aud value");

        /* Test with non-existent file */
        cJSON_Delete(header);
        cJSON_Delete(payload);
        header  = NULL;
        payload = NULL;

        result = process_jwt_template_file(rk, "/non/existent/file", &header,
                                           &payload);
        RD_UT_ASSERT(result == -1, "Expected failure with non-existent file");
        RD_UT_ASSERT(header == NULL,
                     "Expected NULL header with failed processing");
        RD_UT_ASSERT(payload == NULL,
                     "Expected NULL payload with failed processing");

        unlink(template_path);
        rd_free(template_path);
        rd_free(rk);
        if (header)
                cJSON_Delete(header);
        if (payload)
                cJSON_Delete(payload);

        RD_UT_PASS();
}

/**
 * @brief Test JWT assertion creation with minimal approach.
 *        Creates a simplified test that validates the format of the created
 * JWT.
 */
static int ut_sasl_jwt_create_assertion(void) {
        rd_kafka_t *rk;
        char *private_key_pem;
        char *jwt;
        char *header_part, *payload_part, *signature_part;
        char *dot1, *dot2;

        RD_UT_BEGIN();

        rk = rd_calloc(1, sizeof(*rk));

        /* Random key for signing */
        private_key_pem =
            "-----BEGIN PRIVATE KEY-----\n"
            "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCuBS7qG5Cd2voa\n"
            "7nSU2xaDbe6QOYU2P4bIY58SKHbFyq1iB517r61ImsWD+UfZuVxCqXRaWdxxnG/D\n"
            "5VGTQzBOZYlgSYxdJ1KvITXO8kj5i2zBT/LI9R9MTQ7nLFh+vQm1aM8Ts1PmA5t9\n"
            "zFtR9B8RfqN9kbt+2LnLY57aJxEkFC3D89D0WWT97UJWKo7/vxMqp9K9uAIL2Efo\n"
            "5rp9qwyPbx9LmTbfZ8Vog6mG6tAQQHSUqw0PnfhADCVCkYtkzYcyDZy3qZQFu1bY\n"
            "KuuMoMjssyCUL5tTHyNZju0p3Z0bSfOV/nkqHpSSjHKCeQkSKS18/7In6cfY/M4k\n"
            "8rM4HWkdAgMBAAECggEAFsTo2YrXxj/Dn8h5ioyMCpBUuZw9GNcBDLE0PAz9VW3q\n"
            "d7wlV+ypkKlnlJgGVa+SKcrARZ4iYN8mJIyZutn8tRVF/0pASmP9xppizvwWnkgm\n"
            "57hNPQwNl08x1v+PaK3VWl4nUh2RqbPpIXGetT9q3UAjpiduT++Nh9Y2D7cy3/Ro\n"
            "ritnpBDs1R6y5J3rxiE1s8kLYwhDRCPsgUg/ZtKPDTTFz42ArrFeqM91FmjHYP3t\n"
            "p9Uh6CIZ80D6CsMX/TnZFfhKe6EvKBSl4W6tcdFlnXW52fm/670iKSmcJ09+fzPO\n"
            "T1BLrkXGv51bFnlvUyJqQGVEv5+0+HUX/oTpTknMQQKBgQDbYhqip5e8r1f5v32B\n"
            "k1r3xtEiWU2mZoTHJu6bVeuigzVhz4pTMVZChElJ4QnhwwO0t5Oe4Su1MZtjMRw7\n"
            "qIE+YM2pXss25LRXbmWItuRWINzpe8omlxQSOj2tNO/67l0P4vmmrT5wkU2cG6TR\n"
            "ddzorO3NDA4MY4+Xdli+SHXwUQKBgQDLEMqlwyvaGjuZ30l6F13fWnEt9PNCtJsa\n"
            "nsdKJKyFMThdysY/PK40o2GTRRhgYa2jigN3OCYSSznRRZRlqznqL1bOLlYV6zS8\n"
            "TGhdLXuApyLAjZYIK4RtZJYGR9+yg8rH13uNektgW8KnHh5Ko/ptRVoEukf3SBsh\n"
            "f0Fib3ylDQKBgE11Bth0+bMJ6bLpNEPiphSjosVQ6ISe37R8/3Pi0y5uyxM8tqcG\n"
            "3WDg2gt2pAmM1CsjQcCv2cHAwQ81kLVTmkZO4W4yZOd9ulrARKMPh/EM61KYfVhA\n"
            "sTp6S7py3WQocr0gM2rw8gHGm7NJY1j9F0EjhVaHMhKXuGQOyehtJw7xAoGAPwuA\n"
            "jwRQSg+Y74XmbxRwHZcbynPhTpV6DkK7huZp9ZQ5ds0szZdOUqNi+PEbx1isKzj/\n"
            "KHVzRHy8f5+FmicV/QIjhjHWokl6/vcN89faHzBE1tleejzgiYIQHfUUm3zVaUQa\n"
            "ZOtSGaGDhpUQPIY6itBcSVl4XGqzmavDpgcNAMUCgYBFFGtG+RbSySzKfRUp3vc5\n"
            "8YqIdrtXfW9gc9s1+Pw8wfgrY0Rrvy+e3ClSwgGENxgxBvWvhzq2m0S8x2jdLAl1\n"
            "b+VLGCOpUvS4iN2yrHkoHS7BSW40wLuVooJUAaNOIEPqiv1JC75q2dhTRrANp6WB\n"
            "bm+7yWVTNlXYuKQqtuOkNQ==\n"
            "-----END PRIVATE KEY-----\n";

        jwt = rd_kafka_create_jwt_assertion(
            rk, private_key_pem, NULL, NULL,
            RD_KAFKA_SASL_OAUTHBEARER_ASSERTION_ALGORITHM_RS256, NULL, 0, 300);

        RD_UT_ASSERT(jwt != NULL, "Failed to create JWT assertion");

        dot1 = strchr(jwt, '.');
        RD_UT_ASSERT(dot1 != NULL, "JWT missing first dot separator");

        dot2 = strchr(dot1 + 1, '.');
        RD_UT_ASSERT(dot2 != NULL, "JWT missing second dot separator");

        header_part    = rd_strndup(jwt, dot1 - jwt);
        payload_part   = rd_strndup(dot1 + 1, dot2 - (dot1 + 1));
        signature_part = rd_strdup(dot2 + 1);

        RD_UT_ASSERT(strlen(header_part) > 0, "JWT header part is empty");
        RD_UT_ASSERT(strlen(payload_part) > 0, "JWT payload part is empty");
        RD_UT_ASSERT(strlen(signature_part) > 0, "JWT signature part is empty");

        RD_UT_ASSERT(!strchr(header_part, '='),
                     "JWT header contains padding character");
        RD_UT_ASSERT(!strchr(payload_part, '='),
                     "JWT payload contains padding character");
        RD_UT_ASSERT(!strchr(signature_part, '='),
                     "JWT signature contains padding character");

        RD_UT_ASSERT(!strchr(header_part, '+'),
                     "JWT header contains '+' character");
        RD_UT_ASSERT(!strchr(header_part, '/'),
                     "JWT header contains '/' character");
        RD_UT_ASSERT(!strchr(payload_part, '+'),
                     "JWT payload contains '+' character");
        RD_UT_ASSERT(!strchr(payload_part, '/'),
                     "JWT payload contains '/' character");
        RD_UT_ASSERT(!strchr(signature_part, '+'),
                     "JWT signature contains '+' character");
        RD_UT_ASSERT(!strchr(signature_part, '/'),
                     "JWT signature contains '/' character");

        rd_free(header_part);
        rd_free(payload_part);
        rd_free(signature_part);
        rd_free(jwt);
        rd_free(rk);

        RD_UT_PASS();
}


int unittest_sasl_jwt(void) {
        int fails = 0;

        fails += ut_sasl_jwt_base64url_encode();
        fails += ut_sasl_jwt_build_request_body();
        fails += ut_sasl_jwt_parse_assertion_from_file();
        fails += ut_sasl_jwt_process_template_file();
        fails += ut_sasl_jwt_create_assertion();

        return fails;
}
