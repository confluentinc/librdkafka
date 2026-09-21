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
 * production is done with libschemaregistry, Confluent's C++ Schema Registry
 * client and serde library. This example produces an Avro-encoded message
 * whose schema is registered in Schema Registry, rather than raw bytes.
 *
 * Unlike the other examples in this directory, this one needs two libraries
 * that librdkafka does not depend on, and a C++17 compiler:
 *
 *   - libschemaregistry (-lschemaregistry)
 *     https://github.com/confluentinc/libschemaregistry
 *   - avro-cpp          (-lavrocpp)
 *
 * For that reason it is not part of the default `make` target. Build it
 * explicitly once those libraries are installed:
 *
 *   make producer_schema_registry
 *
 * libschemaregistry also ships a CMake package, which is the more usual way
 * to build against it:
 *
 *   find_package(schemaregistry CONFIG REQUIRED)
 *   target_link_libraries(myapp PRIVATE schemaregistry::schemaregistry)
 *
 * Usage:
 *   ./producer_schema_registry <brokers> <topic> <schema-registry-url>
 */

#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

/* Avro C++ */
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/ValidSchema.hh>

/* libschemaregistry */
#include <schemaregistry/rest/ClientConfiguration.h>
#include <schemaregistry/rest/SchemaRegistryClient.h>
#include <schemaregistry/rest/model/Schema.h>
#include <schemaregistry/serdes/Serde.h>
#include <schemaregistry/serdes/SerdeConfig.h>
#include <schemaregistry/serdes/SerdeTypes.h>
#include <schemaregistry/serdes/avro/AvroSerializer.h>

/*
 * Typical include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include "rdkafkacpp.h"

static const std::string SCHEMA_STR = R"({
  "namespace": "confluent.io.examples.serialization.avro",
  "name": "User",
  "type": "record",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favorite_number", "type": "long"}
  ]
})";


class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr()
                << std::endl;
    else
      std::cerr << "% Message delivered to topic " << message.topic_name()
                << " [" << message.partition() << "] at offset "
                << message.offset() << std::endl;
  }
};


int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " <brokers> <topic> <schema-registry-url>\n";
    return 1;
  }

  std::string brokers = argv[1];
  std::string topic   = argv[2];
  std::string sr_url  = argv[3];

  /*
   * Create the Schema Registry client.
   *
   * ClientConfiguration takes a list of Schema Registry URLs. Authentication,
   * TLS and OAuth settings are configured on the same object; see
   * libschemaregistry's ClientConfiguration.h.
   */
  auto client_config =
      std::make_shared<schemaregistry::rest::ClientConfiguration>(
          std::vector<std::string> {sr_url});
  auto sr_client =
      schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

  /*
   * Describe the writer schema and parse it with avro-cpp so that records
   * can be built against it.
   */
  schemaregistry::rest::model::Schema schema;
  schema.setSchemaType("AVRO");
  schema.setSchema(SCHEMA_STR);

  avro::ValidSchema valid_schema;
  std::istringstream schema_stream(SCHEMA_STR);
  avro::compileJsonSchema(schema_stream, valid_schema);

  /*
   * Create the Avro serializer.
   *
   * The serializer registers the schema (auto_register_schemas = true) under
   * the subject derived from the topic — <topic>-value by default — and
   * prefixes each serialized payload with the wire-format header that carries
   * the resulting schema id.
   *
   * In production, prefer auto_register_schemas = false and register schemas
   * through a controlled process instead, so that an application cannot
   * evolve the contract by accident.
   */
  std::unordered_map<std::string, std::string> rule_config;
  schemaregistry::serdes::SerializerConfig ser_config(
      /* auto_register_schemas */ true,
      /* use_schema */ std::nullopt,
      /* normalize_schemas */ true,
      /* validate */ false, rule_config);

  schemaregistry::serdes::avro::AvroSerializer serializer(sr_client, schema,
                                                          nullptr, ser_config);

  /*
   * Create the producer.
   */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;

  if (conf->set("bootstrap.servers", brokers, errstr) !=
      RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    return 1;
  }

  ExampleDeliveryReportCb ex_dr_cb;
  if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    return 1;
  }

  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    return 1;
  }

  delete conf;

  /*
   * The serialization context tells the serializer which subject to use:
   * the topic, and whether this is the message key or value.
   */
  schemaregistry::serdes::SerializationContext ser_ctx(
      topic, schemaregistry::serdes::SerdeType::Value,
      schemaregistry::serdes::SerdeFormat::Avro);

  try {
    /* Build a record matching the schema. */
    avro::GenericDatum datum(valid_schema);
    avro::GenericRecord &record = datum.value<avro::GenericRecord>();
    record.setFieldAt(record.fieldIndex("name"),
                      avro::GenericDatum(std::string("Confluent")));
    record.setFieldAt(record.fieldIndex("favorite_number"),
                      avro::GenericDatum(static_cast<int64_t>(42)));

    /* Serialize: registers the schema if needed and returns the
     * wire-format payload (header + Avro binary). */
    std::vector<uint8_t> payload = serializer.serialize(ser_ctx, datum);

    RdKafka::ErrorCode err = producer->produce(
        topic, RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */, payload.data(),
        payload.size(),
        /* Key */ NULL, 0,
        /* Timestamp (defaults to current time) */ 0,
        /* Message headers, if any */ NULL,
        /* Per-message opaque value passed to delivery report */ NULL);

    if (err != RdKafka::ERR_NO_ERROR)
      std::cerr << "% Failed to produce to topic " << topic << ": "
                << RdKafka::err2str(err) << std::endl;
    else
      std::cerr << "% Enqueued message (" << payload.size() << " bytes) "
                << "for topic " << topic << std::endl;

    producer->poll(0);
  } catch (const std::exception &e) {
    std::cerr << "% Serialization failed: " << e.what() << std::endl;
    delete producer;
    return 1;
  }

  /* Wait for final messages to be delivered or fail. */
  std::cerr << "% Flushing final messages..." << std::endl;
  producer->flush(10 * 1000 /* wait for max 10 seconds */);

  if (producer->outq_len() > 0)
    std::cerr << "% " << producer->outq_len()
              << " message(s) were not delivered" << std::endl;

  serializer.close();
  delete producer;

  return 0;
}
