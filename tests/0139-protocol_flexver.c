/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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

#include "test.h"

#include "../src/rdkafka_proto.h"

#include <stdarg.h>

/**
 * @brief
 */
static void do_test_metadata_long_topic_name(void) {
        const char *topic =
            test_mk_topic_name("a-thirty-four-character-topic-name", 1);
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;
        const struct rd_kafka_metadata *md;

        SUB_TEST_QUICK();

        test_conf_init(&conf, NULL, 60);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        test_CreateTopics_simple(rk, NULL, (char **)&topic, 1, 1, NULL);

        err = rd_kafka_metadata(rk, 1, NULL, &md, tmout_multip(2000));
        TEST_ASSERT(!err, "metadata failed: %s", rd_kafka_err2str(err));

        rd_kafka_metadata_destroy(md);

        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

int main_0139_protocol_flexver(int argc, char **argv) {

        do_test_metadata_long_topic_name();

        return 0;
}
