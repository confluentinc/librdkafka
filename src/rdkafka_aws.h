/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2019 Magnus Edenhill
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

#ifndef _RDKAFKA_AWS_H
#define _RDKAFKA_AWS_H

#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/ossl_typ.h>

/**
 * @brief AWS credential to use for setting refresh
 */
typedef struct rd_kafka_aws_credential_s {
        char *aws_access_key_id;
        char *aws_secret_access_key;
        char *aws_region;
        char *aws_security_token;
        int64_t md_lifetime_ms;
} rd_kafka_aws_credential_t;

char *rd_kafka_aws_uri_encode (const char *in);
char *rd_kafka_aws_build_sasl_canonical_querystring (
        const char *action,
        const char *aws_access_key_id,
        const char *aws_region,
        const char *ymd,
        const char *hms,
        const char *aws_service,
        const char *aws_security_token);
char *rd_kafka_aws_build_sasl_payload (
        const char *ymd,
        const char *hms,
        const char *host,
        const char *aws_access_key_id, 
        const char *aws_secret_access_key,
        const char *aws_security_token,
        const char *aws_region,
        const char *aws_service,
        const char *method,
        const char *algorithm,
        const char *canonical_headers,
        const char *canonical_querystring,
        const char *signed_headers,
        const char *request_parameters,
        const EVP_MD *md);
int rd_kafka_aws_send_request (
        rd_kafka_aws_credential_t *credential,
        const char *ymd,
        const char *hms,
        const char *host,
        const char *aws_access_key_id,
        const char *aws_secret_access_key,
        const char *aws_security_token,
        const char *aws_region,
        const char *aws_service,
        const char *method,
        const char *algorithm,
        const char *canonical_headers,
        const char *signed_headers,
        const char *request_parameters,
        const EVP_MD *md);

int unittest_aws (void);

#endif /* _RDKAFKA_AWS_H */