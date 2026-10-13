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
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

static rd_bool_t brokers_add_all_brokers_down_received = rd_false;

/**
 * @brief Error callback for the brokers_add test.
 *        It expects to receive an error indicating that all brokers are down
 *        or a transport error.
 */
static void brokers_add_all_brokers_down_received_error_cb(rd_kafka_t *rk,
                                                           int error,
                                                           const char *reason,
                                                           void *opaque) {
        TEST_SAY("Error callback received %d: %s\n", error, reason);
        TEST_ASSERT(error == RD_KAFKA_RESP_ERR__TRANSPORT ||
                    error == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN);
        if (error == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                brokers_add_all_brokers_down_received = rd_true;
        }
}

/**
 * @brief Verify the case where the client is re-bootstrapped after brokers were
 * added manually.
 */
static void do_test_rebootstrap_brokers_add(rd_kafka_type_t rk_type,
                                            char *bootstrap_servers) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        brokers_add_all_brokers_down_received = rd_false;

        SUB_TEST_QUICK("%s",
                       rk_type == RD_KAFKA_PRODUCER ? "producer" : "consumer");
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        rd_kafka_conf_set_error_cb(
            conf, brokers_add_all_brokers_down_received_error_cb);
        rk = test_create_handle(rk_type, conf);
        rd_kafka_brokers_add(rk, "localhost:9999");

        /* Await a ALL_BROKERS_DOWN error*/
        while (!brokers_add_all_brokers_down_received) {
                rd_kafka_poll(rk, 1000);
        }
        brokers_add_all_brokers_down_received = rd_false;
        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

int main_0152_rebootstrap_local(int argc, char **argv) {
        rd_kafka_type_t rk_type;
        char *bootstrap_servers[] = {NULL, "", "localhost:9998"};
        for (rk_type = RD_KAFKA_PRODUCER; rk_type <= RD_KAFKA_CONSUMER;
             rk_type++) {
                size_t i;
                for (i = 0; i < RD_ARRAY_SIZE(bootstrap_servers); i++) {
                        do_test_rebootstrap_brokers_add(rk_type,
                                                        bootstrap_servers[i]);
                }
        }

        return 0;
}
