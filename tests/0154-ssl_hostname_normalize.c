/*
 * librdkafka - Apache Kafka C library
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
 * POSSIBILITY OF SUCH LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Unit test for SSL hostname normalization (trailing dot stripping).
 *
 * Tests the fix for issue #4348 where brokers advertising hostnames with
 * trailing dots (absolute FQDNs) would fail SSL certificate verification
 * because X.509 certificates don't include trailing dots in SANs.
 *
 * This test calls the actual rd_kafka_ssl_normalize_hostname() function
 * from rdkafka_ssl.c to verify it handles various edge cases correctly.
 */

#include "test.h"

#if WITH_SSL

/* Function under test (exposed via ENABLE_DEVEL) */
extern const char *rd_kafka_ssl_normalize_hostname(const char *hostname,
                                                   char *normalized,
                                                   size_t size);


/**
 * @brief Test hostname normalization with edge cases.
 *
 * Tests rd_kafka_ssl_normalize_hostname() from rdkafka_ssl.c with various
 * inputs to verify the trailing dot stripping logic works correctly.
 *
 * This test verifies:
 * - Hostname with trailing dot should have dot removed
 * - Hostname without trailing dot should remain unchanged
 * - Single dot should remain unchanged (edge case: len > 1 check)
 * - Empty string should remain unchanged (edge case)
 */
static void test_hostname_normalize(void) {
        /* Test data: input hostname and expected output after normalization */
        struct {
                const char *input;
                const char *expected;
                const char *description;
        } test_cases[] = {
            {"broker.example.com.", "broker.example.com",
             "FQDN with trailing dot"},
            {"broker.example.com", "broker.example.com",
             "FQDN without trailing dot"},
            {"localhost.", "localhost", "localhost with trailing dot"},
            {"localhost", "localhost", "localhost without trailing dot"},
            {".", ".", "single dot (edge case - should remain unchanged)"},
            {"", "", "empty string (edge case)"},
            {"broker-1.example.com.", "broker-1.example.com",
             "hostname with dash and trailing dot"},
            {"192.168.1.1", "192.168.1.1", "IP address (no trailing dot)"},
            {NULL, NULL, NULL}};

        int i;

        SUB_TEST_QUICK();

        TEST_SAY("Testing hostname normalization edge cases\n");

        for (i = 0; test_cases[i].input != NULL; i++) {
                char normalized[256];
                const char *input    = test_cases[i].input;
                const char *expected = test_cases[i].expected;
                const char *desc     = test_cases[i].description;
                const char *result;

                /* Call the actual function under test */
                result = rd_kafka_ssl_normalize_hostname(input, normalized,
                                                         sizeof(normalized));

                TEST_SAYL(3, "Test case %d: %s\n", i + 1, desc);
                TEST_SAYL(3, "  Input:    \"%s\"\n", input);
                TEST_SAYL(3, "  Expected: \"%s\"\n", expected);
                TEST_SAYL(3, "  Got:      \"%s\"\n", result);

                TEST_ASSERT(result == normalized,
                            "Function should return the normalized buffer");

                TEST_ASSERT(!strcmp(result, expected),
                            "Hostname normalization failed for %s: "
                            "expected \"%s\" but got \"%s\"",
                            desc, expected, result);

                TEST_SAY("✓ Test case %d passed: %s\n", i + 1, desc);
        }

        TEST_SAY("All %d hostname normalization edge cases passed\n", i);

        SUB_TEST_PASS();
}

#endif /* WITH_SSL */


int main_0154_ssl_hostname_normalize(int argc, char **argv) {
#if WITH_SSL
        test_hostname_normalize();
        return 0;
#else
        TEST_SKIP("Test requires SSL support\n");
        return 0;
#endif
}
