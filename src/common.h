/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#include <nan.h>

#include <list>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h" // NOLINT
#include "rdkafka.h"  // NOLINT

#include "src/errors.h"

typedef std::vector<const RdKafka::BrokerMetadata*> BrokerMetadataList;
typedef std::vector<const RdKafka::PartitionMetadata*> PartitionMetadataList;
typedef std::vector<const RdKafka::TopicMetadata *> TopicMetadataList;

namespace NodeKafka {

void Log(std::string);

template<typename T> T GetParameter(v8::Local<v8::Object>, std::string, T);
template<> std::string GetParameter<std::string>(
  v8::Local<v8::Object>, std::string, std::string);
template<> std::vector<std::string> GetParameter<std::vector<std::string> >(
  v8::Local<v8::Object>, std::string, std::vector<std::string>);
template<> v8::Local<v8::Array> GetParameter<v8::Local<v8::Array> >(
  v8::Local<v8::Object>, std::string, v8::Local<v8::Array>);
// template int GetParameter<int>(v8::Local<v8::Object, std::string, int);
std::vector<std::string> v8ArrayToStringVector(v8::Local<v8::Array>);
std::list<std::string> v8ArrayToStringList(v8::Local<v8::Array>);

class scoped_mutex_lock {
 public:
  explicit scoped_mutex_lock(uv_mutex_t& lock_) :  // NOLINT
    async_lock(lock_) {
      uv_mutex_lock(&async_lock);
  }

  ~scoped_mutex_lock() {
    uv_mutex_unlock(&async_lock);
  }

 private:
  uv_mutex_t &async_lock;
};

/*
int uv_rwlock_tryrdlock(uv_rwlock_t* rwlock)

int uv_rwlock_trywrlock(uv_rwlock_t* rwlock)
 */

class scoped_shared_write_lock {
 public:
  explicit scoped_shared_write_lock(uv_rwlock_t& lock_) :  // NOLINT
    async_lock(lock_) {
      uv_rwlock_wrlock(&async_lock);
    }

  ~scoped_shared_write_lock() {
    uv_rwlock_wrunlock(&async_lock);
  }

 private:
  uv_rwlock_t &async_lock;
};

class scoped_shared_read_lock {
 public:
  explicit scoped_shared_read_lock(uv_rwlock_t& lock_) :  // NOLINT
    async_lock(lock_) {
      uv_rwlock_rdlock(&async_lock);
    }

  ~scoped_shared_read_lock() {
    uv_rwlock_rdunlock(&async_lock);
  }

 private:
  uv_rwlock_t &async_lock;
};

namespace Conversion {

namespace Util {
std::vector<std::string> ToStringVector(v8::Local<v8::Array>);
v8::Local<v8::Array> ToV8Array(std::vector<std::string>);
v8::Local<v8::Array> ToV8Array(const rd_kafka_error_t **error_list,
                               size_t error_cnt);
v8::Local<v8::Array> ToV8Array(const rd_kafka_AclOperation_t *, size_t);

v8::Local<v8::Object> ToV8Object(const rd_kafka_Node_t *);
}  // namespace Util

namespace Admin {
// Topics from topic object, or topic object array
rd_kafka_NewTopic_t *FromV8TopicObject(v8::Local<v8::Object>,
                                       std::string &errstr);
rd_kafka_NewTopic_t **FromV8TopicObjectArray(v8::Local<v8::Array>);

// ListGroups: request
std::vector<rd_kafka_consumer_group_state_t> FromV8GroupStateArray(
    v8::Local<v8::Array>);

// ListGroups: response
v8::Local<v8::Object> FromListConsumerGroupsResult(
    const rd_kafka_ListConsumerGroups_result_t *);

// DescribeGroups: response
v8::Local<v8::Object> FromMemberDescription(
    const rd_kafka_MemberDescription_t *member);
v8::Local<v8::Object> FromConsumerGroupDescription(
    const rd_kafka_ConsumerGroupDescription_t *desc);
v8::Local<v8::Object> FromDescribeConsumerGroupsResult(
    const rd_kafka_DescribeConsumerGroups_result_t *);

// DeleteGroups: Response
v8::Local<v8::Array> FromDeleteGroupsResult(
    const rd_kafka_DeleteGroups_result_t *);

// ListConsumerGroupOffsets: Response
v8::Local<v8::Array> FromListConsumerGroupOffsetsResult(
    const rd_kafka_ListConsumerGroupOffsets_result_t *result);

// DeleteRecords: Response
v8::Local<v8::Array> FromDeleteRecordsResult(
    const rd_kafka_DeleteRecords_result_t* result);
}  // namespace Admin

namespace TopicPartition {

v8::Local<v8::Array> ToV8Array(std::vector<RdKafka::TopicPartition *> &);
v8::Local<v8::Array> ToTopicPartitionV8Array(
    const rd_kafka_topic_partition_list_t *, bool include_offset);
RdKafka::TopicPartition *FromV8Object(v8::Local<v8::Object>);
std::vector<RdKafka::TopicPartition *> FromV8Array(const v8::Local<v8::Array> &);  // NOLINT
rd_kafka_topic_partition_list_t *TopicPartitionv8ArrayToTopicPartitionList(
    v8::Local<v8::Array> parameter, bool include_offset);

}  // namespace TopicPartition

namespace Metadata {

v8::Local<v8::Object> ToV8Object(RdKafka::Metadata*);

}  // namespace Metadata

namespace Message {

v8::Local<v8::Object> ToV8Object(RdKafka::Message*);
v8::Local<v8::Object> ToV8Object(RdKafka::Message*, bool, bool);

}

}  // namespace Conversion

namespace Util {
  std::string FromV8String(v8::Local<v8::String>);
}

}  // namespace NodeKafka

#endif  // SRC_COMMON_H_
