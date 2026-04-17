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
 * @brief AWS STS GetWebIdentityToken smoke test.
 *
 * Calls rd_kafka_aws_sts_get_web_identity_token() directly and verifies:
 *   - The function is reachable and does not crash.
 *   - On success: token buffer is non-empty and expiry is in the future.
 *   - On auth failure (no AWS credentials in the environment): the error
 *     string is non-empty and the return code is
 *     RD_KAFKA_RESP_ERR__AUTHENTICATION, which is the expected outcome when
 *     running locally without AWS credentials.
 *
 * The test does NOT require a Kafka broker or real AWS credentials to pass.
 * A missing-credentials error is treated as a successful smoke test — it
 * proves the code path is compiled in, linked, and reachable.
 *
 * To run against real AWS credentials set:
 *   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional)
 * or use an instance/task role when running in AWS.
 */

#include "test.h"

#if WITH_AWS_STS
#include "../src/rdkafka_aws_sts.h"
#endif


int main_0154_aws_sts(int argc, char **argv) {
#if !WITH_AWS_STS
        TEST_SKIP("librdkafka not built with AWS STS support\n");
        return 0;
#else
        char token[4096];
        char errstr[512];
        int64_t expiry_ms = 0;
        rd_kafka_resp_err_t err;

        /* Audience can be overridden via environment variable so the same
         * test binary works both locally (no creds → auth error expected)
         * and on EC2 with a real IAM policy that requires a specific audience.
         * Example: TEST_AWS_STS_AUDIENCE=https://api.example.com */
        const char *audience = getenv("TEST_AWS_STS_AUDIENCE");

        TEST_SAY("Calling rd_kafka_aws_sts_get_web_identity_token() "
                 "with audience=%s\n",
                 audience ? audience : "(none)");

        err = rd_kafka_aws_sts_get_web_identity_token(
            audience,
            token, sizeof(token),
            &expiry_ms,
            errstr, sizeof(errstr));

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                /* Got a real token — verify basic sanity. */
                TEST_ASSERT(token[0] != '\0',
                            "Expected non-empty JWT token on success");
                TEST_ASSERT(expiry_ms > 0,
                            "Expected positive expiry_ms on success, got %" PRId64,
                            expiry_ms);
                TEST_SAY("AWS STS token obtained successfully "
                         "(expiry_ms=%" PRId64 ", token length=%zu)\n",
                         expiry_ms, strlen(token));

        } else if (err == RD_KAFKA_RESP_ERR__AUTHENTICATION) {
                /* No AWS credentials in this environment — expected locally.
                 * The error string must be non-empty and descriptive. */
                TEST_ASSERT(errstr[0] != '\0',
                            "Expected non-empty errstr on auth failure");
                TEST_SAY("No AWS credentials available (expected locally): "
                         "%s\n", errstr);
                TEST_SAY("Smoke test passed — AWS STS code path is reachable\n");

        } else {
                /* Any other error code is unexpected. */
                TEST_FAIL("Unexpected error code %d (%s): %s",
                          err, rd_kafka_err2str(err), errstr);
        }

        return 0;
#endif /* WITH_AWS_STS */
}
