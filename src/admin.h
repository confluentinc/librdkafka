/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_ADMIN_H_
#define SRC_ADMIN_H_

#include <nan.h>
#include <uv.h>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h" // NOLINT
#include "rdkafka.h"  // NOLINT

#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"

namespace NodeKafka {

/**
 * @brief KafkaConsumer v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

class AdminClient : public Connection {
 public:
  static void Init(v8::Local<v8::Object>);
  static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value>);

  void ActivateDispatchers();
  void DeactivateDispatchers();

  Baton Connect();
  Baton Disconnect();

  Baton CreateTopic(rd_kafka_NewTopic_t* topic, int timeout_ms);
  Baton DeleteTopic(rd_kafka_DeleteTopic_t* topic, int timeout_ms);
  Baton CreatePartitions(rd_kafka_NewPartitions_t* topic, int timeout_ms);
  // Baton AlterConfig(rd_kafka_NewTopic_t* topic, int timeout_ms);
  // Baton DescribeConfig(rd_kafka_NewTopic_t* topic, int timeout_ms);
  Baton ListGroups(bool is_match_states_set,
                   std::vector<rd_kafka_consumer_group_state_t>& match_states,
                   int timeout_ms,
                   rd_kafka_event_t** event_response);
  Baton DescribeGroups(std::vector<std::string>& groups,
                       bool include_authorized_operations, int timeout_ms,
                       rd_kafka_event_t** event_response);
  Baton DeleteGroups(rd_kafka_DeleteGroup_t** group_list, size_t group_cnt,
                     int timeout_ms, rd_kafka_event_t** event_response);
  Baton ListConsumerGroupOffsets(rd_kafka_ListConsumerGroupOffsets_t** req,
                                 size_t req_cnt,
                                 bool require_stable_offsets, int timeout_ms,
                                 rd_kafka_event_t** event_response);
  Baton DeleteRecords(rd_kafka_DeleteRecords_t** del_records,
                      size_t del_records_cnt, int operation_timeout_ms,
                      int timeout_ms, rd_kafka_event_t** event_response);
  Baton DescribeTopics(rd_kafka_TopicCollection_t* topics,
                       bool include_authorized_operations, int timeout_ms,
                       rd_kafka_event_t** event_response);

 protected:
  static Nan::Persistent<v8::Function> constructor;
  static void New(const Nan::FunctionCallbackInfo<v8::Value>& info);

  explicit AdminClient(Conf* globalConfig);
  explicit AdminClient(Connection* existingConnection);
  ~AdminClient();

  bool is_derived = false;

 private:
  // Node methods
  // static NAN_METHOD(NodeValidateTopic);
  static NAN_METHOD(NodeCreateTopic);
  static NAN_METHOD(NodeDeleteTopic);
  static NAN_METHOD(NodeCreatePartitions);

  // Consumer group operations
  static NAN_METHOD(NodeListGroups);
  static NAN_METHOD(NodeDescribeGroups);
  static NAN_METHOD(NodeDeleteGroups);
  static NAN_METHOD(NodeListConsumerGroupOffsets);
  static NAN_METHOD(NodeDeleteRecords);
  static NAN_METHOD(NodeDescribeTopics);

  static NAN_METHOD(NodeConnect);
  static NAN_METHOD(NodeDisconnect);
};

}  // namespace NodeKafka

#endif  // SRC_ADMIN_H_
