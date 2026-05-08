/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * @brief AWS STS GetWebIdentityToken C++ implementation.
 *
 * This is the only .cpp file in src/ and the only file in the entire library
 * that includes AWS SDK headers. All AWS SDK interaction is contained here.
 *
 * The extern "C" linkage makes rd_kafka_aws_sts_get_web_identity_token()
 * callable from the pure C files in this library.
 *
 * This file must include rdkafka.h (public API only) and must NOT include
 * rdkafka_int.h. rdkafka_int.h contains C-only constructs (implicit void*
 * conversions, C99 designated initialisers, unscoped enums) that are illegal
 * in C++ and produce compile errors. extern "C" affects linkage only — it
 * does not suppress C++ type checking on included headers.
 *
 * As a result, the function signature has no rd_kafka_t* parameter — that
 * would require dereferencing the opaque rd_kafka_t which is only defined
 * in rdkafka_int.h.
 *
 * AWS credentials are resolved automatically by the SDK's default credential
 * provider chain:
 *   1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 *   2. ~/.aws/credentials and ~/.aws/config files
 *   3. EC2 instance metadata (IMDSv2)
 *   4. ECS task role
 *   5. IAM role for service accounts (IRSA / pod identity)
 */

#include <cstring>

#include <aws/core/Aws.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/GetWebIdentityTokenRequest.h>
#include <aws/sts/model/GetWebIdentityTokenResult.h>

extern "C" {
#include "rdkafka.h"         /* public API only — do NOT include rdkafka_int.h */
#include "rdkafka_aws_sts.h"
}

/**
 * @brief Obtain a signed JWT from AWS STS via GetWebIdentityToken.
 *
 * Initialises the AWS SDK, calls GetWebIdentityToken, then shuts the SDK
 * down. The inner scope ensures STSClient is destroyed before ShutdownAPI,
 * as required by the AWS SDK lifecycle contract.
 *
 * @param audience        Optional audience claim. Pass NULL to omit.
 * @param token_buf       Output buffer for the null-terminated JWT string.
 * @param token_buf_size  Size of \p token_buf in bytes.
 * @param expiry_ms       Set to expiry time in milliseconds since Unix epoch
 *                        on success.
 * @param errstr          Output buffer for error description on failure.
 * @param errstr_size     Size of \p errstr in bytes.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success, error code otherwise.
 */
extern "C" RD_KAFKA_AWS_STS_API rd_kafka_resp_err_t
rd_kafka_aws_sts_get_web_identity_token(const char *audience,
                                        char *token_buf,
                                        size_t token_buf_size,
                                        int64_t *expiry_ms,
                                        char *errstr,
                                        size_t errstr_size) {
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        rd_kafka_resp_err_t ret = RD_KAFKA_RESP_ERR_NO_ERROR;

        {
                /* Inner scope: STSClient must be destroyed before
                 * ShutdownAPI — the AWS SDK lifecycle requires this. */
                Aws::STS::STSClient client;
                Aws::STS::Model::GetWebIdentityTokenRequest req;

                if (audience)
                        req.AddAudience(audience);

                auto outcome = client.GetWebIdentityToken(req);

                if (!outcome.IsSuccess()) {
                        const auto &err = outcome.GetError();
                        snprintf(errstr, errstr_size,
                                 "GetWebIdentityToken failed: %s",
                                 err.GetMessage().c_str());
                        ret = RD_KAFKA_RESP_ERR__AUTHENTICATION;

                } else {
                        const auto &result = outcome.GetResult();
                        const auto &token  = result.GetWebIdentityToken();
                        const auto &expiry = result.GetExpiration();

                        if (token.size() >= token_buf_size) {
                                snprintf(errstr, errstr_size,
                                         "JWT token too large (%zu bytes)",
                                         token.size());
                                ret = RD_KAFKA_RESP_ERR__BAD_MSG;
                        } else {
                                memcpy(token_buf, token.c_str(),
                                       token.size() + 1);
                                *expiry_ms = (int64_t)expiry.Millis();
                        }
                }
        }

        Aws::ShutdownAPI(options);
        return ret;
}
