/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
 * Schema Registry producer example.
 *
 * Production applications should use Schema Registry. Producing raw bytes
 * leads to data-quality issues, broken consumers, and ungovernable data.
 *
 * librdkafka has no built-in Schema Registry serializer, so schema-first
 * production in C is done with libserdes (Confluent's C serde library) plus
 * avro-c. This example produces an Avro-encoded message whose schema is
 * registered in Schema Registry, rather than raw bytes.
 *
 * Unlike the other examples in this directory, this one needs two libraries
 * that librdkafka does not depend on:
 *
 *   - libserdes  (-lserdes)  https://github.com/confluentinc/libserdes
 *   - avro-c     (-lavro)
 *
 * For that reason it is not part of the default `make` target. Build it
 * explicitly once those libraries are installed:
 *
 *   make producer_schema_registry
 *
 * or directly:
 *
 *   cc producer_schema_registry.c -o producer_schema_registry \
 *      -lrdkafka -lserdes -lavro
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <librdkafka/rdkafka.h>
#include <libserdes/serdes.h>
#include <libserdes/serdes-avro.h>
#include <avro.h>

static const char *SCHEMA_DEF =
    "{"
    "  \"type\": \"record\","
    "  \"name\": \"User\","
    "  \"fields\": ["
    "    {\"name\": \"name\", \"type\": \"string\"},"
    "    {\"name\": \"favorite_number\", \"type\": \"int\"}"
    "  ]"
    "}";

static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
        if (rkmessage->err)
                fprintf(stderr, "%% Delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else
                fprintf(stderr, "%% Delivered message to %s [%d]\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        (int)rkmessage->partition);
}

int main(int argc, char **argv) {
        const char *brokers = "localhost:9092";
        const char *sr_url  = "http://localhost:8081";
        const char *topic   = "myTopic";
        char errstr[512];

        /* 1. Configure and create the librdkafka producer. */
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                          sizeof(errstr));
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create producer: %s\n", errstr);
                return 1;
        }

        /* 2. Configure libserdes with the Schema Registry URL. */
        serdes_conf_t *sconf =
            serdes_conf_new(NULL, 0, "schema.registry.url", sr_url, NULL);
        serdes_t *serdes = serdes_new(sconf, errstr, sizeof(errstr));
        if (!serdes) {
                fprintf(stderr, "%% Failed to create serdes: %s\n", errstr);
                return 1;
        }

        /* 3. Register (or look up) the schema under <topic>-value. */
        serdes_schema_t *sschema = serdes_schema_add(
            serdes, "myTopic-value", -1, SCHEMA_DEF, -1, errstr,
            sizeof(errstr));
        if (!sschema) {
                fprintf(stderr, "%% Failed to add schema: %s\n", errstr);
                return 1;
        }

        /* 4. Build an Avro record matching the schema. */
        avro_schema_t aschema;
        if (avro_schema_from_json_length(SCHEMA_DEF, strlen(SCHEMA_DEF),
                                         &aschema)) {
                fprintf(stderr, "%% Failed to parse Avro schema: %s\n",
                        avro_strerror());
                return 1;
        }
        avro_value_iface_t *aclass = avro_generic_class_from_schema(aschema);
        avro_value_t record;
        avro_generic_value_new(aclass, &record);

        avro_value_t field;
        avro_value_get_by_name(&record, "name", &field, NULL);
        avro_value_set_string(&field, "Confluent");
        avro_value_get_by_name(&record, "favorite_number", &field, NULL);
        avro_value_set_int(&field, 42);

        /* 5. Serialize with the Schema Registry framing
         *    (magic byte + schema id). */
        void *payload      = NULL;
        size_t payload_size = 0;
        if (serdes_schema_serialize_avro(sschema, &record, &payload,
                                         &payload_size, errstr,
                                         sizeof(errstr))) {
                fprintf(stderr, "%% Serialization failed: %s\n", errstr);
                return 1;
        }

        /* 6. Produce the schema-registered message. */
        rd_kafka_resp_err_t err = rd_kafka_producev(
            rk, RD_KAFKA_V_TOPIC(topic),
            RD_KAFKA_V_VALUE(payload, payload_size),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);
        if (err)
                fprintf(stderr, "%% Produce failed: %s\n",
                        rd_kafka_err2str(err));

        /* 7. Wait for delivery and clean up. */
        rd_kafka_flush(rk, 15 * 1000);

        free(payload);
        avro_value_decref(&record);
        avro_value_iface_decref(aclass);
        avro_schema_decref(aschema);
        serdes_destroy(serdes);
        rd_kafka_destroy(rk);

        return 0;
}
