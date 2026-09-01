/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Magnus Edenhill
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
#include "rdkafka.h"
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
        if (size < 2)
                return 0;

        uint8_t key_len = data[0];
        if (key_len > size - 1)
                key_len = size - 1;

        char *key = malloc(key_len + 1);
        memcpy(key, data + 1, key_len);
        key[key_len] = '\0';

        size_t val_len = size - 1 - key_len;
        char *val = malloc(val_len + 1);
        memcpy(val, data + 1 + key_len, val_len);
        val[val_len] = '\0';

        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char errstr[512];

        rd_kafka_conf_set(conf, key, val, errstr, sizeof(errstr));

        rd_kafka_conf_destroy(conf);

        /* Also test topic config */
        rd_kafka_topic_conf_t *tconf = rd_kafka_topic_conf_new();
        rd_kafka_topic_conf_set(tconf, key, val, errstr, sizeof(errstr));
        rd_kafka_topic_conf_destroy(tconf);

        /* Test SSL certificate parsing if we have enough data */
        if (size > 3) {
                rd_kafka_cert_type_t cert_type = data[0] % RD_KAFKA_CERT__CNT;
                rd_kafka_cert_enc_t cert_enc = data[1] % RD_KAFKA_CERT_ENC__CNT;
                rd_kafka_conf_t *conf2 = rd_kafka_conf_new();
                rd_kafka_conf_set_ssl_cert(conf2, cert_type, cert_enc,
                                           data + 2, size - 2,
                                           errstr, sizeof(errstr));
                rd_kafka_conf_destroy(conf2);
        }

        free(key);
        free(val);

        return 0;
}
