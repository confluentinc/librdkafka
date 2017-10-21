/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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

 #include "rd.h"
 #include "rdunittest.h"
 #include "rdmurmur2.h"

int unittest_murmurhashneutral2 (void) {
  const char * keysToTest[] = {
    "kafka",
    "amqp",
    "giberish123456789"
  };

  const uint32_t java_murmur2_results[] = {
    1348980580, // kafka
    767747138, // amqp
    257239820 // giberish123456789
  };

  int keys_length = sizeof(java_murmur2_results) / sizeof(uint32_t);
  for (int i = 0; i < keys_length; i++) {
    uint32_t murmur2_result = rd_murmur2(keysToTest[i], strlen(keysToTest[i]));
    RD_UT_SAY("GOT HASH: %u"
              " FOR KEY: %s"
              " USING SEED: %d",
              murmur2_result, keysToTest[i], MURMUR2_SEED);
    RD_UT_ASSERT(murmur2_result == java_murmur2_results[i],
                 "Calculated murmur2 hash %u"
                 " not matching expected murmur2 hash %u",
                 murmur2_result, java_murmur2_results[i]);
  }
  RD_UT_PASS();
}
