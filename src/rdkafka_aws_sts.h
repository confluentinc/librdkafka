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

#ifndef _RDKAFKA_AWS_STS_H_
#define _RDKAFKA_AWS_STS_H_

/**
 * @brief Obtain a signed JWT from AWS STS via GetWebIdentityToken.
 *
 * Uses the AWS credentials available in the current environment (instance
 * role, ECS task role, environment variables, ~/.aws/credentials, etc.)
 * to call GetWebIdentityToken and return a signed JWT representing the
 * machine's AWS identity.
 *
 * The implementation lives in rdkafka_aws_sts_impl.cpp — the only file in
 * src/ that includes AWS SDK headers. This is the sole entry point across
 * the C/C++ boundary for AWS STS functionality.
 *
 * @param audience        Optional audience claim for the JWT. Pass NULL
 *                        to omit.
 * @param token_buf       Buffer to receive the null-terminated JWT string.
 * @param token_buf_size  Size of \p token_buf in bytes.
 * @param expiry_ms       Set to the token expiry time in milliseconds
 *                        since the Unix epoch on success.
 * @param errstr          Buffer to receive a human-readable error description
 *                        on failure.
 * @param errstr_size     Size of \p errstr in bytes.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success, or an error code with
 *          a description written to \p errstr on failure.
 */
rd_kafka_resp_err_t rd_kafka_aws_sts_get_web_identity_token(
    const char *audience,
    char *token_buf,
    size_t token_buf_size,
    int64_t *expiry_ms,
    char *errstr,
    size_t errstr_size);

#endif /* _RDKAFKA_AWS_STS_H_ */
