/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
#include "src/common.h"

#include <iostream>
#include <list>
#include <string>
#include <vector>

namespace NodeKafka {

void Log(std::string str) {
  std::cerr << "% " << str.c_str() << std::endl;
}

template<typename T>
T GetParameter(v8::Local<v8::Object> object, std::string field_name, T def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    Nan::Maybe<T> maybeT = Nan::To<T>(Nan::Get(object, field).ToLocalChecked());
    if (maybeT.IsNothing()) {
      return def;
    } else {
      return maybeT.FromJust();
    }
  }
  return def;
}

template<>
int64_t GetParameter<int64_t>(v8::Local<v8::Object> object,
  std::string field_name, int64_t def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> v = Nan::Get(object, field).ToLocalChecked();

    if (!v->IsNumber()) {
      return def;
    }

    Nan::Maybe<int64_t> maybeInt = Nan::To<int64_t>(v);
    if (maybeInt.IsNothing()) {
      return def;
    } else {
      return maybeInt.FromJust();
    }
  }
  return def;
}

template<>
bool GetParameter<bool>(v8::Local<v8::Object> object,
  std::string field_name, bool def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> v = Nan::Get(object, field).ToLocalChecked();

    if (!v->IsBoolean()) {
      return def;
    }

    Nan::Maybe<bool> maybeInt = Nan::To<bool>(v);
    if (maybeInt.IsNothing()) {
      return def;
    } else {
      return maybeInt.FromJust();
    }
  }
  return def;
}

template<>
int GetParameter<int>(v8::Local<v8::Object> object,
  std::string field_name, int def) {
  return static_cast<int>(GetParameter<int64_t>(object, field_name, def));
}

template<>
std::string GetParameter<std::string>(v8::Local<v8::Object> object,
                                      std::string field_name,
                                      std::string def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> parameter =
      Nan::Get(object, field).ToLocalChecked();
      // Nan::To<v8::String>();

    if (!parameter->IsUndefined() && !parameter->IsNull()) {
      v8::Local<v8::String> val = Nan::To<v8::String>(parameter)
        .ToLocalChecked();

      if (!val->IsUndefined() && !val->IsNull()) {
        Nan::Utf8String parameterValue(val);
        std::string parameterString(*parameterValue);

        return parameterString;
      }
    }
  }
  return def;
}

template<>
std::vector<std::string> GetParameter<std::vector<std::string> >(
  v8::Local<v8::Object> object, std::string field_name,
  std::vector<std::string> def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();

  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> maybeArray = Nan::Get(object, field).ToLocalChecked();
    if (maybeArray->IsArray()) {
      v8::Local<v8::Array> parameter = maybeArray.As<v8::Array>();
      return v8ArrayToStringVector(parameter);
    }
  }
  return def;
}

std::vector<std::string> v8ArrayToStringVector(v8::Local<v8::Array> parameter) {
  std::vector<std::string> newItem;

  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> v;
      if (!Nan::Get(parameter, i).ToLocal(&v)) {
        continue;
      }
      Nan::MaybeLocal<v8::String> p = Nan::To<v8::String>(v);
      if (p.IsEmpty()) {
        continue;
      }
      Nan::Utf8String pVal(p.ToLocalChecked());
      std::string pString(*pVal);
      newItem.push_back(pString);
    }
  }
  return newItem;
}

std::list<std::string> v8ArrayToStringList(v8::Local<v8::Array> parameter) {
  std::list<std::string> newItem;
  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> v;
      if (!Nan::Get(parameter, i).ToLocal(&v)) {
        continue;
      }
      Nan::MaybeLocal<v8::String> p = Nan::To<v8::String>(v);
      if (p.IsEmpty()) {
        continue;
      }
      Nan::Utf8String pVal(p.ToLocalChecked());
      std::string pString(*pVal);
      newItem.push_back(pString);
    }
  }
  return newItem;
}

template<> v8::Local<v8::Array> GetParameter<v8::Local<v8::Array> >(
  v8::Local<v8::Object> object,
  std::string field_name,
  v8::Local<v8::Array> def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();

  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> maybeArray = Nan::Get(object, field).ToLocalChecked();
    if (maybeArray->IsArray()) {
      v8::Local<v8::Array> parameter = maybeArray.As<v8::Array>();
      return parameter;
    }
  }

  return def;
}

namespace Conversion {

namespace Util {
std::vector<std::string> ToStringVector(v8::Local<v8::Array> parameter) {
  std::vector<std::string> newItem;

  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> element;
      if (!Nan::Get(parameter, i).ToLocal(&element)) {
        continue;
      }

      if (!element->IsRegExp()) {
        Nan::MaybeLocal<v8::String> p = Nan::To<v8::String>(element);

        if (p.IsEmpty()) {
          continue;
        }

        Nan::Utf8String pVal(p.ToLocalChecked());
        std::string pString(*pVal);

        newItem.push_back(pString);
      } else {
        Nan::Utf8String pVal(element.As<v8::RegExp>()->GetSource());
        std::string pString(*pVal);

        Log(pString);

        newItem.push_back(pString);
      }
    }
  }

  return newItem;
}

v8::Local<v8::Array> ToV8Array(std::vector<std::string> parameter) {
  v8::Local<v8::Array> newItem = Nan::New<v8::Array>();

  for (size_t i = 0; i < parameter.size(); i++) {
    std::string topic = parameter[i];
    Nan::Set(newItem, i, Nan::New<v8::String>(topic).ToLocalChecked());
  }

  return newItem;
}

/**
 * @brief Converts a list of rd_kafka_error_t* into a v8 array of RdKafkaError
 * objects.
 */
v8::Local<v8::Array> ToV8Array(const rd_kafka_error_t** error_list,
                               size_t error_cnt) {
  v8::Local<v8::Array> errors = Nan::New<v8::Array>();

  for (size_t i = 0; i < error_cnt; i++) {
    RdKafka::ErrorCode code =
        static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(error_list[i]));
    std::string msg = std::string(rd_kafka_error_string(error_list[i]));
    Nan::Set(errors, i, RdKafkaError(code, msg));
  }

  return errors;
}

/**
 * @brief Converts a rd_kafka_Node_t* into a v8 object.
 */
v8::Local<v8::Object> ToV8Object(const rd_kafka_Node_t* node) {
  /* Return object type
   {
      id: number
      host: string
      port: number
      rack?: string
    }
  */
  v8::Local<v8::Object> obj = Nan::New<v8::Object>();

  Nan::Set(obj, Nan::New("id").ToLocalChecked(),
           Nan::New<v8::Number>(rd_kafka_Node_id(node)));
  Nan::Set(obj, Nan::New("host").ToLocalChecked(),
           Nan::New<v8::String>(rd_kafka_Node_host(node)).ToLocalChecked());
  Nan::Set(obj, Nan::New("port").ToLocalChecked(),
           Nan::New<v8::Number>(rd_kafka_Node_port(node)));

  const char* rack = rd_kafka_Node_rack(node);
  if (rack) {
    Nan::Set(obj, Nan::New("rack").ToLocalChecked(),
             Nan::New<v8::String>(rack).ToLocalChecked());
  }

  return obj;
}

/**
 * @brief Converts a rd_kafka_Uuid_t* into a v8 object.
 */
v8::Local<v8::Object> UuidToV8Object(const rd_kafka_Uuid_t* uuid) {
  /*Return object type
    {
        mostSignificantBits: bigint
        leastSignificantBits: bigint
        base64: string
    }
  */
  v8::Local<v8::Object> obj = Nan::New<v8::Object>();

  Nan::Set(obj, Nan::New("mostSignificantBits").ToLocalChecked(),
           v8::BigInt::New(v8::Isolate::GetCurrent(),
                           rd_kafka_Uuid_most_significant_bits(uuid)));
  Nan::Set(obj, Nan::New("leastSignificantBits").ToLocalChecked(),
           v8::BigInt::New(v8::Isolate::GetCurrent(),
                           rd_kafka_Uuid_least_significant_bits(uuid)));
  Nan::Set(
      obj, Nan::New("base64").ToLocalChecked(),
      Nan::New<v8::String>(rd_kafka_Uuid_base64str(uuid)).ToLocalChecked());

  return obj;
}

/**
 * @brief Converts a list of rd_kafka_AclOperation_t into a v8 array.
 */
v8::Local<v8::Array> ToV8Array(
    const rd_kafka_AclOperation_t* authorized_operations,
    size_t authorized_operations_cnt) {
  v8::Local<v8::Array> array = Nan::New<v8::Array>();

  for (size_t i = 0; i < authorized_operations_cnt; i++) {
    Nan::Set(array, i, Nan::New<v8::Number>(authorized_operations[i]));
  }

  return array;
}

}  // namespace Util

namespace TopicPartition {

/**
 * @brief RdKafka::TopicPartition vector to a v8 Array
 *
 * @see v8ArrayToTopicPartitionVector
 * @note This method returns a v8 array of a mix of topic partition
 *       objects and errors. For a more uniform return type of
 *       topic partitions (which have an internal error property),
 *       use `ToTopicPartitionV8Array(const rd_kafka_topic_partition_list_t*,
 *       bool)`.
 */
v8::Local<v8::Array> ToV8Array(
  std::vector<RdKafka::TopicPartition*> & topic_partition_list) {  // NOLINT
  v8::Local<v8::Array> array = Nan::New<v8::Array>();
  for (size_t topic_partition_i = 0;
    topic_partition_i < topic_partition_list.size(); topic_partition_i++) {
    RdKafka::TopicPartition* topic_partition =
      topic_partition_list[topic_partition_i];

    // TODO: why do we set the entire array element to be an error rather adding
    // an error field to TopicPartition? Or create a TopicPartitionError?
    if (topic_partition->err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
      Nan::Set(array, topic_partition_i,
        Nan::Error(Nan::New(RdKafka::err2str(topic_partition->err()))
        .ToLocalChecked()));
    } else {
      // We have the list now let's get the properties from it
      v8::Local<v8::Object> obj = Nan::New<v8::Object>();

      if (topic_partition->offset() != RdKafka::Topic::OFFSET_INVALID) {
        Nan::Set(obj, Nan::New("offset").ToLocalChecked(),
          Nan::New<v8::Number>(topic_partition->offset()));
      }

      // If present, size >= 1, since it will include at least the
      // null terminator.
      if (topic_partition->get_metadata().size() > 0) {
        Nan::Set(obj, Nan::New("metadata").ToLocalChecked(),
          Nan::New<v8::String>(
            reinterpret_cast<const char*>(topic_partition->get_metadata().data()), // NOLINT
            // null terminator is not required by the constructor.
            topic_partition->get_metadata().size() - 1)
          .ToLocalChecked());
      }

      Nan::Set(obj, Nan::New("partition").ToLocalChecked(),
        Nan::New<v8::Number>(topic_partition->partition()));
      Nan::Set(obj, Nan::New("topic").ToLocalChecked(),
        Nan::New<v8::String>(topic_partition->topic().c_str())
        .ToLocalChecked());

      int leader_epoch = topic_partition->get_leader_epoch();
      if (leader_epoch >= 0) {
        Nan::Set(obj, Nan::New("leaderEpoch").ToLocalChecked(),
                 Nan::New<v8::Number>(leader_epoch));
      }

      Nan::Set(array, topic_partition_i, obj);
    }
  }

  return array;
}

/**
 * @brief Converts a rd_kafka_topic_partition_list_t* into a list of v8 objects.
 *
 * @param topic_partition_list The list of topic partitions to convert.
 * @param include_offset Whether to include the offset in the output.
 * @returns [{topic: string, partition: number, offset?: number, error?:
 * LibrdKafkaError}]
 *
 * @note Contains error within the topic partitions object, and not as separate
 * array elements, unlike the `ToV8Array(std::vector<RdKafka::TopicPartition*> &
 * topic_partition_list)`.
 */
v8::Local<v8::Array> ToTopicPartitionV8Array(
    const rd_kafka_topic_partition_list_t* topic_partition_list,
    bool include_offset) {
  v8::Local<v8::Array> array = Nan::New<v8::Array>();

  for (int topic_partition_i = 0; topic_partition_i < topic_partition_list->cnt;
       topic_partition_i++) {
    rd_kafka_topic_partition_t topic_partition =
        topic_partition_list->elems[topic_partition_i];
    v8::Local<v8::Object> obj = Nan::New<v8::Object>();

    Nan::Set(obj, Nan::New("partition").ToLocalChecked(),
             Nan::New<v8::Number>(topic_partition.partition));
    Nan::Set(obj, Nan::New("topic").ToLocalChecked(),
             Nan::New<v8::String>(topic_partition.topic).ToLocalChecked());

    if (topic_partition.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      v8::Local<v8::Object> error = NodeKafka::RdKafkaError(
          static_cast<RdKafka::ErrorCode>(topic_partition.err));
      Nan::Set(obj, Nan::New("error").ToLocalChecked(), error);
    }

    if (include_offset) {
      Nan::Set(obj, Nan::New("offset").ToLocalChecked(),
               Nan::New<v8::Number>(topic_partition.offset));
    }

    int leader_epoch =
        rd_kafka_topic_partition_get_leader_epoch(&topic_partition);
    if (leader_epoch >= 0) {
      Nan::Set(obj, Nan::New("leaderEpoch").ToLocalChecked(),
               Nan::New<v8::Number>(leader_epoch));
    }

    Nan::Set(array, topic_partition_i, obj);
  }
  return array;
}

/**
 * @brief v8 Array of topic partitions to RdKafka::TopicPartition vector
 *
 * @see v8ArrayToTopicPartitionVector
 *
 * @note You must delete all the pointers inside here when you are done!!
 */
std::vector<RdKafka::TopicPartition*> FromV8Array(
  const v8::Local<v8::Array> & topic_partition_list) {
  // NOTE: ARRAY OF POINTERS! DELETE THEM WHEN YOU ARE FINISHED
  std::vector<RdKafka::TopicPartition*> array;

  for (size_t topic_partition_i = 0;
    topic_partition_i < topic_partition_list->Length(); topic_partition_i++) {
    v8::Local<v8::Value> topic_partition_value;
    if (!Nan::Get(topic_partition_list, topic_partition_i)
        .ToLocal(&topic_partition_value)) {
      continue;
    }

    if (topic_partition_value->IsObject()) {
      array.push_back(FromV8Object(
        Nan::To<v8::Object>(topic_partition_value).ToLocalChecked()));
    }
  }

  return array;
}

/**
 * @brief v8 Array of Topic Partitions to rd_kafka_topic_partition_list_t
 *
 * @note Converts a v8 array of type [{topic: string, partition: number,
 *       offset?: number}] to a rd_kafka_topic_partition_list_t
 */
rd_kafka_topic_partition_list_t* TopicPartitionv8ArrayToTopicPartitionList(
    v8::Local<v8::Array> parameter, bool include_offset) {
  rd_kafka_topic_partition_list_t* newList =
      rd_kafka_topic_partition_list_new(parameter->Length());

  for (unsigned int i = 0; i < parameter->Length(); i++) {
    v8::Local<v8::Value> v;
    if (!Nan::Get(parameter, i).ToLocal(&v)) {
      continue;
    }

    if (!v->IsObject()) {
      return NULL;  // Return NULL to indicate an error
    }

    v8::Local<v8::Object> item = v.As<v8::Object>();

    std::string topic = GetParameter<std::string>(item, "topic", "");
    int partition = GetParameter<int>(item, "partition", -1);

    rd_kafka_topic_partition_t* toppar =
        rd_kafka_topic_partition_list_add(newList, topic.c_str(), partition);

    if (include_offset) {
      int64_t offset = GetParameter<int64_t>(item, "offset", 0);
      toppar->offset = offset;
    }
  }
  return newList;
}

/**
 * @brief v8 Array of Topic Partitions with offsetspec to
 *        rd_kafka_topic_partition_list_t
 * 
 * @note Converts a v8 array of type [{topic: string, partition: number,
 *      offset: {timestamp: number}}] to a rd_kafka_topic_partition_list_t
 */
rd_kafka_topic_partition_list_t*
TopicPartitionOffsetSpecv8ArrayToTopicPartitionList(
    v8::Local<v8::Array> parameter) {
  rd_kafka_topic_partition_list_t* newList =
      rd_kafka_topic_partition_list_new(parameter->Length());

  for (unsigned int i = 0; i < parameter->Length(); i++) {
    v8::Local<v8::Value> v;
    if (!Nan::Get(parameter, i).ToLocal(&v)) {
      continue;
    }

    if (!v->IsObject()) {
      return NULL;  // Return NULL to indicate an error
    }

    v8::Local<v8::Object> item = v.As<v8::Object>();

    std::string topic = GetParameter<std::string>(item, "topic", "");
    int partition = GetParameter<int>(item, "partition", -1);

    rd_kafka_topic_partition_t* toppar =
        rd_kafka_topic_partition_list_add(newList, topic.c_str(), partition);

    v8::Local<v8::Value> offsetValue =
        Nan::Get(item, Nan::New("offset").ToLocalChecked()).ToLocalChecked();
    v8::Local<v8::Object> offsetObject = offsetValue.As<v8::Object>();
    int64_t offset = GetParameter<int64_t>(offsetObject, "timestamp", 0);

    toppar->offset = offset;
  }
  return newList;
}

/**
 * @brief v8::Object to RdKafka::TopicPartition
 *
 */
RdKafka::TopicPartition * FromV8Object(v8::Local<v8::Object> topic_partition) {
  std::string topic = GetParameter<std::string>(topic_partition, "topic", "");
  int partition = GetParameter<int>(topic_partition, "partition", -1);
  int64_t offset = GetParameter<int64_t>(topic_partition, "offset", 0);

  if (partition == -1) {
return NULL;
  }

  if (topic.empty()) {
    return NULL;
  }

  RdKafka::TopicPartition *toppar =
    RdKafka::TopicPartition::create(topic, partition, offset);

  v8::Local<v8::String> metadataKey = Nan::New("metadata").ToLocalChecked();
  if (Nan::Has(topic_partition, metadataKey).FromMaybe(false)) {
    v8::Local<v8::Value> metadataValue =
        Nan::Get(topic_partition, metadataKey).ToLocalChecked();

    if (metadataValue->IsString()) {
      Nan::Utf8String metadataValueUtf8Str(metadataValue.As<v8::String>());
      std::string metadataValueStr(*metadataValueUtf8Str);
      std::vector<unsigned char> metadataVector(metadataValueStr.begin(),
                                                metadataValueStr.end());
      metadataVector.push_back(
          '\0');  // The null terminator is not included in the iterator.
      toppar->set_metadata(metadataVector);
    }
  }

  toppar->set_leader_epoch(-1);
  v8::Local<v8::String> leaderEpochKey =
      Nan::New("leaderEpoch").ToLocalChecked();
  if (Nan::Has(topic_partition, leaderEpochKey).FromMaybe(false)) {
    v8::Local<v8::Value> leaderEpochValue =
        Nan::Get(topic_partition, leaderEpochKey).ToLocalChecked();

    if (leaderEpochValue->IsNumber()) {
      int32_t leaderEpoch = Nan::To<int32_t>(leaderEpochValue).FromJust();
      toppar->set_leader_epoch(leaderEpoch);
    }
  }

  return toppar;
}

}  // namespace TopicPartition

namespace Metadata {

/**
 * @brief RdKafka::Metadata to v8::Object
 *
 */
v8::Local<v8::Object> ToV8Object(RdKafka::Metadata* metadata) {
  v8::Local<v8::Object> obj = Nan::New<v8::Object>();

  v8::Local<v8::Array> broker_data = Nan::New<v8::Array>();
  v8::Local<v8::Array> topic_data = Nan::New<v8::Array>();

  const BrokerMetadataList* brokers = metadata->brokers();  // NOLINT

  unsigned int broker_i = 0;

  for (BrokerMetadataList::const_iterator it = brokers->begin();
    it != brokers->end(); ++it, broker_i++) {
    // Start iterating over brokers and set the object up

    const RdKafka::BrokerMetadata* x = *it;

    v8::Local<v8::Object> current_broker = Nan::New<v8::Object>();

    Nan::Set(current_broker, Nan::New("id").ToLocalChecked(),
      Nan::New<v8::Number>(x->id()));
    Nan::Set(current_broker, Nan::New("host").ToLocalChecked(),
      Nan::New<v8::String>(x->host().c_str()).ToLocalChecked());
    Nan::Set(current_broker, Nan::New("port").ToLocalChecked(),
      Nan::New<v8::Number>(x->port()));

    Nan::Set(broker_data, broker_i, current_broker);
  }

  unsigned int topic_i = 0;

  const TopicMetadataList* topics = metadata->topics();

  for (TopicMetadataList::const_iterator it = topics->begin();
    it != topics->end(); ++it, topic_i++) {
    // Start iterating over topics

    const RdKafka::TopicMetadata* x = *it;

    v8::Local<v8::Object> current_topic = Nan::New<v8::Object>();

    Nan::Set(current_topic, Nan::New("name").ToLocalChecked(),
      Nan::New<v8::String>(x->topic().c_str()).ToLocalChecked());

    v8::Local<v8::Array> current_topic_partitions = Nan::New<v8::Array>();

    const PartitionMetadataList* current_partition_data = x->partitions();

    unsigned int partition_i = 0;
    PartitionMetadataList::const_iterator itt;

    for (itt = current_partition_data->begin();
      itt != current_partition_data->end(); ++itt, partition_i++) {
      // partition iterate
      const RdKafka::PartitionMetadata* xx = *itt;

      v8::Local<v8::Object> current_partition = Nan::New<v8::Object>();

      Nan::Set(current_partition, Nan::New("id").ToLocalChecked(),
        Nan::New<v8::Number>(xx->id()));
      Nan::Set(current_partition, Nan::New("leader").ToLocalChecked(),
        Nan::New<v8::Number>(xx->leader()));

      const std::vector<int32_t> * replicas  = xx->replicas();
      const std::vector<int32_t> * isrs = xx->isrs();

      std::vector<int32_t>::const_iterator r_it;
      std::vector<int32_t>::const_iterator i_it;

      unsigned int r_i = 0;
      unsigned int i_i = 0;

      v8::Local<v8::Array> current_replicas = Nan::New<v8::Array>();

      for (r_it = replicas->begin(); r_it != replicas->end(); ++r_it, r_i++) {
        Nan::Set(current_replicas, r_i, Nan::New<v8::Int32>(*r_it));
      }

      v8::Local<v8::Array> current_isrs = Nan::New<v8::Array>();

      for (i_it = isrs->begin(); i_it != isrs->end(); ++i_it, i_i++) {
        Nan::Set(current_isrs, i_i, Nan::New<v8::Int32>(*i_it));
      }

      Nan::Set(current_partition, Nan::New("replicas").ToLocalChecked(),
        current_replicas);
      Nan::Set(current_partition, Nan::New("isrs").ToLocalChecked(),
        current_isrs);

      Nan::Set(current_topic_partitions, partition_i, current_partition);
    }  // iterate over partitions

    Nan::Set(current_topic, Nan::New("partitions").ToLocalChecked(),
      current_topic_partitions);

    Nan::Set(topic_data, topic_i, current_topic);
  }  // End iterating over topics

  Nan::Set(obj, Nan::New("orig_broker_id").ToLocalChecked(),
    Nan::New<v8::Number>(metadata->orig_broker_id()));

  Nan::Set(obj, Nan::New("orig_broker_name").ToLocalChecked(),
    Nan::New<v8::String>(metadata->orig_broker_name()).ToLocalChecked());

  Nan::Set(obj, Nan::New("topics").ToLocalChecked(), topic_data);
  Nan::Set(obj, Nan::New("brokers").ToLocalChecked(), broker_data);

  return obj;
}

}  // namespace Metadata

namespace Message {

// Overload for all use cases except delivery reports
v8::Local<v8::Object> ToV8Object(RdKafka::Message *message) {
  return ToV8Object(message, true, true);
}

v8::Local<v8::Object> ToV8Object(RdKafka::Message *message,
                                bool include_payload,
                                bool include_headers) {
  if (message->err() == RdKafka::ERR_NO_ERROR) {
    v8::Local<v8::Object> pack = Nan::New<v8::Object>();

    const void* message_payload = message->payload();

    if (!include_payload) {
      Nan::Set(pack, Nan::New<v8::String>("value").ToLocalChecked(),
        Nan::Undefined());
    } else if (message_payload) {
      Nan::Set(pack, Nan::New<v8::String>("value").ToLocalChecked(),
        Nan::Encode(message_payload, message->len(), Nan::Encoding::BUFFER));
    } else {
      Nan::Set(pack, Nan::New<v8::String>("value").ToLocalChecked(),
        Nan::Null());
    }

    RdKafka::Headers* headers;
    if (((headers = message->headers()) != 0) && include_headers) {
      v8::Local<v8::Array> v8headers = Nan::New<v8::Array>();
      int index = 0;
      std::vector<RdKafka::Headers::Header> all = headers->get_all();
      for (std::vector<RdKafka::Headers::Header>::iterator it = all.begin();
                                                     it != all.end(); it++) {
        v8::Local<v8::Object> v8header = Nan::New<v8::Object>();
        Nan::Set(v8header, Nan::New<v8::String>(it->key()).ToLocalChecked(),
          Nan::Encode(it->value_string(),
            it->value_size(), Nan::Encoding::BUFFER));
        Nan::Set(v8headers, index, v8header);
        index++;
      }
      Nan::Set(pack,
        Nan::New<v8::String>("headers").ToLocalChecked(), v8headers);
    }

    Nan::Set(pack, Nan::New<v8::String>("size").ToLocalChecked(),
      Nan::New<v8::Number>(message->len()));

    const void* key_payload = message->key_pointer();

    if (key_payload) {
      // We want this to also be a buffer to avoid corruption
      // https://github.com/confluentinc/confluent-kafka-javascript/issues/208
      Nan::Set(pack, Nan::New<v8::String>("key").ToLocalChecked(),
        Nan::Encode(key_payload, message->key_len(), Nan::Encoding::BUFFER));
    } else {
      Nan::Set(pack, Nan::New<v8::String>("key").ToLocalChecked(),
        Nan::Null());
    }

    Nan::Set(pack, Nan::New<v8::String>("topic").ToLocalChecked(),
      Nan::New<v8::String>(message->topic_name()).ToLocalChecked());
    Nan::Set(pack, Nan::New<v8::String>("offset").ToLocalChecked(),
      Nan::New<v8::Number>(message->offset()));
    Nan::Set(pack, Nan::New<v8::String>("partition").ToLocalChecked(),
      Nan::New<v8::Number>(message->partition()));
    Nan::Set(pack, Nan::New<v8::String>("timestamp").ToLocalChecked(),
      Nan::New<v8::Number>(message->timestamp().timestamp));

    int32_t leader_epoch = message->leader_epoch();
    if (leader_epoch >= 0) {
      Nan::Set(pack, Nan::New<v8::String>("leaderEpoch").ToLocalChecked(),
               Nan::New<v8::Number>(leader_epoch));
    }

    return pack;
  } else {
    return RdKafkaError(message->err());
  }
}

}  // namespace Message

/**
 * @section Admin API models
 */

namespace Admin {

/**
 * Create a low level rdkafka handle to represent a topic
 *
 *
 */
rd_kafka_NewTopic_t* FromV8TopicObject(
  v8::Local<v8::Object> object, std::string &errstr) {  // NOLINT
  std::string topic_name = GetParameter<std::string>(object, "topic", "");
  int num_partitions = GetParameter<int>(object, "num_partitions", 0);
  int replication_factor = GetParameter<int>(object, "replication_factor", 0);

  char errbuf[512];

  rd_kafka_NewTopic_t* new_topic = rd_kafka_NewTopic_new(
    topic_name.c_str(),
    num_partitions,
    replication_factor,
    errbuf,
    sizeof(errbuf));

  if (new_topic == NULL) {
    errstr = std::string(errbuf);
    return NULL;
  }

  rd_kafka_resp_err_t err;

  if (Nan::Has(object, Nan::New("config").ToLocalChecked()).FromMaybe(false)) {
    // Get the config v8::Object that we can get parameters on
    v8::Local<v8::Object> config =
      Nan::Get(object, Nan::New("config").ToLocalChecked())
      .ToLocalChecked().As<v8::Object>();

    // Get all of the keys of the object
    v8::MaybeLocal<v8::Array> config_keys = Nan::GetOwnPropertyNames(config);

    if (!config_keys.IsEmpty()) {
      v8::Local<v8::Array> field_array = config_keys.ToLocalChecked();
      for (size_t i = 0; i < field_array->Length(); i++) {
        v8::Local<v8::String> config_key = Nan::Get(field_array, i)
          .ToLocalChecked().As<v8::String>();
        v8::Local<v8::Value> config_value = Nan::Get(config, config_key)
          .ToLocalChecked();

        // If the config value is a string...
        if (config_value->IsString()) {
          Nan::Utf8String pKeyVal(config_key);
          std::string pKeyString(*pKeyVal);

          Nan::Utf8String pValueVal(config_value.As<v8::String>());
          std::string pValString(*pValueVal);

          err = rd_kafka_NewTopic_set_config(
            new_topic, pKeyString.c_str(), pValString.c_str());

          if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            errstr = rd_kafka_err2str(err);
            rd_kafka_NewTopic_destroy(new_topic);
            return NULL;
          }
        } else {
          errstr = "Config values must all be provided as strings.";
          rd_kafka_NewTopic_destroy(new_topic);
          return NULL;
        }
      }
    }
  }

  return new_topic;
}

rd_kafka_NewTopic_t** FromV8TopicObjectArray(v8::Local<v8::Array>) {
  return NULL;
}

/**
 * @brief Converts a v8 array of group states into a vector of
 * rd_kafka_consumer_group_state_t.
 */
std::vector<rd_kafka_consumer_group_state_t> FromV8GroupStateArray(
    v8::Local<v8::Array> array) {
  v8::Local<v8::Array> parameter = array.As<v8::Array>();
  std::vector<rd_kafka_consumer_group_state_t> returnVec;
  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> v;
      if (!Nan::Get(parameter, i).ToLocal(&v)) {
        continue;
      }
      Nan::Maybe<int64_t> maybeT = Nan::To<int64_t>(v);
      if (maybeT.IsNothing()) {
        continue;
      }
      int64_t state_number = maybeT.FromJust();
      if (state_number >= RD_KAFKA_CONSUMER_GROUP_STATE__CNT) {
        continue;
      }
      returnVec.push_back(
          static_cast<rd_kafka_consumer_group_state_t>(state_number));
    }
  }
  return returnVec;
}

/**
 * @brief Converts a v8 array of group types into a vector of
 * rd_kafka_consumer_group_type_t.
 */
std::vector<rd_kafka_consumer_group_type_t> FromV8GroupTypeArray(
    v8::Local<v8::Array> array) {
  v8::Local<v8::Array> parameter = array.As<v8::Array>();
  std::vector<rd_kafka_consumer_group_type_t> returnVec;
  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> v;
      if (!Nan::Get(parameter, i).ToLocal(&v)) {
        continue;
      }
      Nan::Maybe<int64_t> maybeT = Nan::To<int64_t>(v);
      if (maybeT.IsNothing()) {
        continue;
      }
      int64_t type_number = maybeT.FromJust();
      if (type_number < 0 || type_number >= RD_KAFKA_CONSUMER_GROUP_TYPE__CNT) {
        continue;
      }
      returnVec.push_back(
          static_cast<rd_kafka_consumer_group_type_t>(type_number));
    }
  }
  return returnVec;
}

/**
 * @brief Converts a rd_kafka_ListConsumerGroups_result_t* into a v8 object.
 */
v8::Local<v8::Object> FromListConsumerGroupsResult(
    const rd_kafka_ListConsumerGroups_result_t* result) {
  /* Return object type:
    {
      groups: {
        groupId: string,
        protocolType: string,
        isSimpleConsumerGroup: boolean,
        state: ConsumerGroupState (internally a number)
        type: ConsumerGroupType (internally a number)
      }[],
      errors: LibrdKafkaError[]
    }
  */
  v8::Local<v8::Object> returnObject = Nan::New<v8::Object>();

  size_t error_cnt;
  const rd_kafka_error_t** error_list =
      rd_kafka_ListConsumerGroups_result_errors(result, &error_cnt);
  Nan::Set(returnObject, Nan::New("errors").ToLocalChecked(),
           Conversion::Util::ToV8Array(error_list, error_cnt));

  v8::Local<v8::Array> groups = Nan::New<v8::Array>();
  size_t groups_cnt;
  const rd_kafka_ConsumerGroupListing_t** groups_list =
      rd_kafka_ListConsumerGroups_result_valid(result, &groups_cnt);

  for (size_t i = 0; i < groups_cnt; i++) {
    const rd_kafka_ConsumerGroupListing_t* group = groups_list[i];
    v8::Local<v8::Object> groupObject = Nan::New<v8::Object>();

    Nan::Set(groupObject, Nan::New("groupId").ToLocalChecked(),
             Nan::New<v8::String>(rd_kafka_ConsumerGroupListing_group_id(group))
                 .ToLocalChecked());

    bool is_simple =
        rd_kafka_ConsumerGroupListing_is_simple_consumer_group(group);
    Nan::Set(groupObject, Nan::New("isSimpleConsumerGroup").ToLocalChecked(),
             Nan::New<v8::Boolean>(is_simple));

    std::string protocol_type = is_simple ? "simple" : "consumer";
    Nan::Set(groupObject, Nan::New("protocolType").ToLocalChecked(),
             Nan::New<v8::String>(protocol_type).ToLocalChecked());

    Nan::Set(groupObject, Nan::New("state").ToLocalChecked(),
             Nan::New<v8::Number>(rd_kafka_ConsumerGroupListing_state(group)));

    Nan::Set(groupObject, Nan::New("type").ToLocalChecked(),
              Nan::New<v8::Number>(rd_kafka_ConsumerGroupListing_type(group)));

    Nan::Set(groups, i, groupObject);
  }

  Nan::Set(returnObject, Nan::New("groups").ToLocalChecked(), groups);
  return returnObject;
}

/**
 * @brief Converts a rd_kafka_MemberDescription_t* into a v8 object.
 */
v8::Local<v8::Object> FromMemberDescription(
    const rd_kafka_MemberDescription_t* member) {
  /* Return object type:
    {
        clientHost: string
        clientId: string
        memberId: string
        memberAssignment: Buffer // will be always null
        memberMetadata: Buffer // will be always null
        groupInstanceId: string
        assignment: {
          topicPartitions: TopicPartition[]
        },
        targetAssignment?: {
          topicPartitions: TopicPartition[]
        }
    }
  */
  v8::Local<v8::Object> returnObject = Nan::New<v8::Object>();

  // clientHost
  Nan::Set(returnObject, Nan::New("clientHost").ToLocalChecked(),
           Nan::New<v8::String>(rd_kafka_MemberDescription_host(member))
               .ToLocalChecked());

  // clientId
  Nan::Set(returnObject, Nan::New("clientId").ToLocalChecked(),
           Nan::New<v8::String>(rd_kafka_MemberDescription_client_id(member))
               .ToLocalChecked());

  // memberId
  Nan::Set(returnObject, Nan::New("memberId").ToLocalChecked(),
           Nan::New<v8::String>(rd_kafka_MemberDescription_consumer_id(member))
               .ToLocalChecked());

  // memberAssignment - not passed to user, always null
  Nan::Set(returnObject, Nan::New("memberAssignment").ToLocalChecked(),
           Nan::Null());

  // memberMetadata - not passed to user, always null
  Nan::Set(returnObject, Nan::New("memberMetadata").ToLocalChecked(),
           Nan::Null());

  // groupInstanceId
  const char* group_instance_id =
      rd_kafka_MemberDescription_group_instance_id(member);
  if (group_instance_id) {
    Nan::Set(returnObject, Nan::New("groupInstanceId").ToLocalChecked(),
             Nan::New<v8::String>(group_instance_id).ToLocalChecked());
  }

  // assignment
  const rd_kafka_MemberAssignment_t* assignment =
      rd_kafka_MemberDescription_assignment(member);
  const rd_kafka_topic_partition_list_t* partitions =
      rd_kafka_MemberAssignment_partitions(assignment);
  v8::Local<v8::Array> topicPartitions =
      Conversion::TopicPartition::ToTopicPartitionV8Array(partitions, false);
  v8::Local<v8::Object> assignmentObject = Nan::New<v8::Object>();
  Nan::Set(assignmentObject, Nan::New("topicPartitions").ToLocalChecked(),
           topicPartitions);
  Nan::Set(returnObject, Nan::New("assignment").ToLocalChecked(),
           assignmentObject);

  // targetAssignment
  const rd_kafka_MemberAssignment_t* target_assignment =
      rd_kafka_MemberDescription_target_assignment(member);
  if (target_assignment) {
    const rd_kafka_topic_partition_list_t* target_partitions =
        rd_kafka_MemberAssignment_partitions(target_assignment);
    v8::Local<v8::Array> targetTopicPartitions =
        Conversion::TopicPartition::ToTopicPartitionV8Array(
            target_partitions, false);
    v8::Local<v8::Object> targetAssignmentObject = Nan::New<v8::Object>();
    Nan::Set(targetAssignmentObject,
             Nan::New("topicPartitions").ToLocalChecked(),
             targetTopicPartitions);
    Nan::Set(returnObject, Nan::New("targetAssignment").ToLocalChecked(),
             targetAssignmentObject);
  }

  return returnObject;
}

/**
 * @brief Converts a rd_kafka_ConsumerGroupDescription_t* into a v8 object.
 */
v8::Local<v8::Object> FromConsumerGroupDescription(
    const rd_kafka_ConsumerGroupDescription_t* desc) {
  /* Return object type:
    {
      groupId: string,
      error: LibrdKafkaError,
      members: MemberDescription[],
      protocol: string
      isSimpleConsumerGroup: boolean
      protocolType: string
      partitionAssignor: string
      state: ConsumerGroupState - internally a number
      coordinator: Node
      authorizedOperations: AclOperationType[] - internally numbers
    }
  */
  v8::Local<v8::Object> returnObject = Nan::New<v8::Object>();

  // groupId
  Nan::Set(
      returnObject, Nan::New("groupId").ToLocalChecked(),
      Nan::New<v8::String>(rd_kafka_ConsumerGroupDescription_group_id(desc))
          .ToLocalChecked());

  // error
  const rd_kafka_error_t* error = rd_kafka_ConsumerGroupDescription_error(desc);
  if (error) {
    RdKafka::ErrorCode code =
        static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(error));
    std::string msg = std::string(rd_kafka_error_string(error));
    Nan::Set(returnObject, Nan::New("error").ToLocalChecked(),
             RdKafkaError(code, msg));
  }

  // members
  v8::Local<v8::Array> members = Nan::New<v8::Array>();
  size_t member_cnt = rd_kafka_ConsumerGroupDescription_member_count(desc);
  for (size_t i = 0; i < member_cnt; i++) {
    const rd_kafka_MemberDescription_t* member =
        rd_kafka_ConsumerGroupDescription_member(desc, i);
    Nan::Set(members, i, FromMemberDescription(member));
  }
  Nan::Set(returnObject, Nan::New("members").ToLocalChecked(), members);

  // isSimpleConsumerGroup
  bool is_simple =
      rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(desc);
  Nan::Set(returnObject, Nan::New("isSimpleConsumerGroup").ToLocalChecked(),
           Nan::New<v8::Boolean>(is_simple));

  // protocolType
  std::string protocolType = is_simple ? "simple" : "consumer";
  Nan::Set(returnObject, Nan::New("protocolType").ToLocalChecked(),
           Nan::New<v8::String>(protocolType).ToLocalChecked());

  // protocol
  Nan::Set(returnObject, Nan::New("protocol").ToLocalChecked(),
           Nan::New<v8::String>(
               rd_kafka_ConsumerGroupDescription_partition_assignor(desc))
               .ToLocalChecked());

  // partitionAssignor
  Nan::Set(returnObject, Nan::New("partitionAssignor").ToLocalChecked(),
           Nan::New<v8::String>(
               rd_kafka_ConsumerGroupDescription_partition_assignor(desc))
               .ToLocalChecked());

  // state
  Nan::Set(returnObject, Nan::New("state").ToLocalChecked(),
           Nan::New<v8::Number>(rd_kafka_ConsumerGroupDescription_state(desc)));

  // type
  Nan::Set(returnObject, Nan::New("type").ToLocalChecked(),
           Nan::New<v8::Number>(rd_kafka_ConsumerGroupDescription_type(desc)));

  // coordinator
  const rd_kafka_Node_t* coordinator =
      rd_kafka_ConsumerGroupDescription_coordinator(desc);
  if (coordinator) {
    v8::Local<v8::Object> coordinatorObject =
        Conversion::Util::ToV8Object(coordinator);
    Nan::Set(returnObject, Nan::New("coordinator").ToLocalChecked(),
             coordinatorObject);
  }

  // authorizedOperations
  size_t authorized_operations_cnt;
  const rd_kafka_AclOperation_t* authorized_operations =
      rd_kafka_ConsumerGroupDescription_authorized_operations(
          desc, &authorized_operations_cnt);
  if (authorized_operations) {
    Nan::Set(returnObject, Nan::New("authorizedOperations").ToLocalChecked(),
             Conversion::Util::ToV8Array(authorized_operations,
                                         authorized_operations_cnt));
  }

  return returnObject;
}

/**
 * @brief Converts a rd_kafka_DescribeConsumerGroups_result_t* into a v8 object.
 */
v8::Local<v8::Object> FromDescribeConsumerGroupsResult(
    const rd_kafka_DescribeConsumerGroups_result_t* result) {
  /* Return object type:
    { groups: GroupDescription[] }
  */
  v8::Local<v8::Object> returnObject = Nan::New<v8::Object>();
  v8::Local<v8::Array> groups = Nan::New<v8::Array>();
  size_t groups_cnt;
  const rd_kafka_ConsumerGroupDescription_t** groups_list =
      rd_kafka_DescribeConsumerGroups_result_groups(result, &groups_cnt);

  for (size_t i = 0; i < groups_cnt; i++) {
    const rd_kafka_ConsumerGroupDescription_t* group = groups_list[i];
    Nan::Set(groups, i, FromConsumerGroupDescription(group));
  }

  Nan::Set(returnObject, Nan::New("groups").ToLocalChecked(), groups);
  return returnObject;
}

/**
 * @brief Converts a rd_kafka_DeleteGroups_result_t* into a v8 array.
*/
v8::Local<v8::Array> FromDeleteGroupsResult(
    const rd_kafka_DeleteGroups_result_t* result) {
  /* Return object type:
    [{
      groupId: string
      errorCode?: number
      error?: LibrdKafkaError
    }]
  */
  v8::Local<v8::Array> returnArray = Nan::New<v8::Array>();
  size_t result_cnt;
  const rd_kafka_group_result_t** results =
      rd_kafka_DeleteGroups_result_groups(result, &result_cnt);

  for (size_t i = 0; i < result_cnt; i++) {
    const rd_kafka_group_result_t* group_result = results[i];
    v8::Local<v8::Object> group_object = Nan::New<v8::Object>();

    Nan::Set(group_object, Nan::New("groupId").ToLocalChecked(),
             Nan::New<v8::String>(rd_kafka_group_result_name(group_result))
                 .ToLocalChecked());

    const rd_kafka_error_t* error = rd_kafka_group_result_error(group_result);
    if (!error) {
      Nan::Set(group_object, Nan::New("errorCode").ToLocalChecked(),
               Nan::New<v8::Number>(RD_KAFKA_RESP_ERR_NO_ERROR));
    } else {
      RdKafka::ErrorCode code =
          static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(error));
      const char* msg = rd_kafka_error_string(error);

      Nan::Set(group_object, Nan::New("errorCode").ToLocalChecked(),
               Nan::New<v8::Number>(code));
      Nan::Set(group_object, Nan::New("error").ToLocalChecked(),
               RdKafkaError(code, msg));
    }
    Nan::Set(returnArray, i, group_object);
  }

  return returnArray;
}

/**
 * @brief Converts a rd_kafka_ListConsumerGroupOffsets_result_t* 
 *        into a v8 Array.
 */
v8::Local<v8::Array> FromListConsumerGroupOffsetsResult(
    const rd_kafka_ListConsumerGroupOffsets_result_t* result) {
  /* Return Object type:
    GroupResults[] = [{
      groupId : string,
      error? : LibrdKafkaError,
      partitions : TopicPartitionOffset[]
    }]

    TopicPartitionOffset:
    {
      topic : string,
      partition : number,
      offset : number,
      metadata : string | null,
      leaderEpoch? : number,
      error? : LibrdKafkaError
    }
  */

  v8::Local<v8::Array> returnArray = Nan::New<v8::Array>();
  size_t result_cnt;
  const rd_kafka_group_result_t** res =
      rd_kafka_ListConsumerGroupOffsets_result_groups(result, &result_cnt);

  for (size_t i = 0; i < result_cnt; i++) {
    const rd_kafka_group_result_t* group_result = res[i];

    // Create group result object
    v8::Local<v8::Object> group_object = Nan::New<v8::Object>();

    // Set groupId
    std::string groupId = rd_kafka_group_result_name(group_result);
    Nan::Set(group_object, Nan::New("groupId").ToLocalChecked(),
             Nan::New<v8::String>(groupId.c_str()).ToLocalChecked());

    // Set group-level error (if any)
    const rd_kafka_error_t* group_error =
        rd_kafka_group_result_error(group_result);
    if (group_error) {
      RdKafka::ErrorCode code =
          static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(group_error));
      const char* msg = rd_kafka_error_string(group_error);
      Nan::Set(group_object, Nan::New("error").ToLocalChecked(),
               RdKafkaError(code, msg));
    }

    // Get the list of partitions for this group
    const rd_kafka_topic_partition_list_t* partitionList =
        rd_kafka_group_result_partitions(group_result);

    // Prepare array for TopicPartitionOffset[]
    v8::Local<v8::Array> partitionsArray = Nan::New<v8::Array>();
    int partitionIndex = 0;

    for (int j = 0; j < partitionList->cnt; j++) {
      const rd_kafka_topic_partition_t* partition = &partitionList->elems[j];

      // Create the TopicPartitionOffset object
      v8::Local<v8::Object> partition_object = Nan::New<v8::Object>();

      // Set topic, partition, and offset
      Nan::Set(partition_object, Nan::New("topic").ToLocalChecked(),
               Nan::New<v8::String>(partition->topic).ToLocalChecked());
      Nan::Set(partition_object, Nan::New("partition").ToLocalChecked(),
               Nan::New<v8::Number>(partition->partition));
      Nan::Set(partition_object, Nan::New("offset").ToLocalChecked(),
               Nan::New<v8::Number>(partition->offset));

      // Set metadata (if available)
      if (partition->metadata != nullptr) {
        Nan::Set(
            partition_object, Nan::New("metadata").ToLocalChecked(),
            Nan::New<v8::String>(static_cast<const char*>(partition->metadata))
                .ToLocalChecked());
      } else {
        Nan::Set(partition_object, Nan::New("metadata").ToLocalChecked(),
                 Nan::Null());
      }

      // Set leaderEpoch (if available)
      int32_t leader_epoch =
          rd_kafka_topic_partition_get_leader_epoch(partition);
      if (leader_epoch >= 0) {
        Nan::Set(partition_object, Nan::New("leaderEpoch").ToLocalChecked(),
                 Nan::New<v8::Number>(leader_epoch));
      }

      // Set partition-level error (if any)
      if (partition->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        RdKafka::ErrorCode code =
            static_cast<RdKafka::ErrorCode>(partition->err);
        Nan::Set(group_object, Nan::New("error").ToLocalChecked(),
                 RdKafkaError(code, rd_kafka_err2str(partition->err)));
      }

      Nan::Set(partitionsArray, partitionIndex++, partition_object);
    }

    Nan::Set(group_object, Nan::New("partitions").ToLocalChecked(),
             partitionsArray);

    Nan::Set(returnArray, i, group_object);
  }

  return returnArray;
}

/**
 * @brief Converts a rd_kafka_DeleteRecords_result_t* into a v8 Array.
 */
v8::Local<v8::Array> FromDeleteRecordsResult(
    const rd_kafka_DeleteRecords_result_t* result) {
  /* Return object type:
    [{
      topic: string,
      partition: number,
      lowWatermark: number,
      error?: LibrdKafkaError
    }]
  */
  const rd_kafka_topic_partition_list_t* partitionList =
      rd_kafka_DeleteRecords_result_offsets(result);

  v8::Local<v8::Array> partitionsArray = Nan::New<v8::Array>();
  int partitionIndex = 0;

  for (int j = 0; j < partitionList->cnt; j++) {
    const rd_kafka_topic_partition_t* partition = &partitionList->elems[j];

    // Create the TopicPartitionOffset object
    v8::Local<v8::Object> partition_object = Nan::New<v8::Object>();

    // Set topic, partition, and offset and error(if required)
    Nan::Set(partition_object, Nan::New("topic").ToLocalChecked(),
             Nan::New<v8::String>(partition->topic).ToLocalChecked());
    Nan::Set(partition_object, Nan::New("partition").ToLocalChecked(),
             Nan::New<v8::Number>(partition->partition));
    Nan::Set(partition_object, Nan::New("lowWatermark").ToLocalChecked(),
             Nan::New<v8::Number>(partition->offset));

    if (partition->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      RdKafka::ErrorCode code = static_cast<RdKafka::ErrorCode>(partition->err);
      Nan::Set(partition_object, Nan::New("error").ToLocalChecked(),
               RdKafkaError(code, rd_kafka_err2str(partition->err)));
    }

    Nan::Set(partitionsArray, partitionIndex++, partition_object);
  }

  return partitionsArray;
}

/**
 * @brief Converts a rd_kafka_DescribeTopics_result_t* into a v8 Array.
 */
v8::Local<v8::Array> FromDescribeTopicsResult(
    const rd_kafka_DescribeTopics_result_t* result) {
  /* Return object type:
   [{
     name: string,
     topicId: Uuid,
     isInternal: boolean,
     partitions: [{
       partition: number,
       leader: Node,
       isr: Node[],
       replicas: Node[],
     }]
     error?: LibrdKafkaError,
     authorizedOperations?: AclOperationType[]
    }]
  */

  /*
    Node:
    {
      id: number,
      host: string,
      port: number
      rack?: string
    }
  */

  v8::Local<v8::Array> returnArray = Nan::New<v8::Array>();
  size_t result_cnt;
  const rd_kafka_TopicDescription_t** results =
      rd_kafka_DescribeTopics_result_topics(result, &result_cnt);

  int topicIndex = 0;

  for (size_t i = 0; i < result_cnt; i++) {
    v8::Local<v8::Object> topic_object = Nan::New<v8::Object>();

    const char* topic_name = rd_kafka_TopicDescription_name(results[i]);
    Nan::Set(topic_object, Nan::New("name").ToLocalChecked(),
             Nan::New<v8::String>(topic_name).ToLocalChecked());

    const rd_kafka_Uuid_t* topic_id =
        rd_kafka_TopicDescription_topic_id(results[i]);
    Nan::Set(topic_object, Nan::New("topicId").ToLocalChecked(),
             Conversion::Util::UuidToV8Object(topic_id));

    int is_internal = rd_kafka_TopicDescription_is_internal(results[i]);
    Nan::Set(topic_object, Nan::New("isInternal").ToLocalChecked(),
             Nan::New<v8::Boolean>(is_internal));

    const rd_kafka_error_t* error = rd_kafka_TopicDescription_error(results[i]);
    if (error) {
      RdKafka::ErrorCode code =
          static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(error));
      Nan::Set(topic_object, Nan::New("error").ToLocalChecked(),
               RdKafkaError(code, rd_kafka_error_string(error)));
    }

    size_t authorized_operations_cnt;
    const rd_kafka_AclOperation_t* authorized_operations =
        rd_kafka_TopicDescription_authorized_operations(
            results[i], &authorized_operations_cnt);
    if (authorized_operations) {
      Nan::Set(topic_object, Nan::New("authorizedOperations").ToLocalChecked(),
               Conversion::Util::ToV8Array(authorized_operations,
                                           authorized_operations_cnt));
    }

    size_t partition_cnt;
    const rd_kafka_TopicPartitionInfo_t** partitions =
        rd_kafka_TopicDescription_partitions(results[i], &partition_cnt);
    v8::Local<v8::Array> partitionsArray = Nan::New<v8::Array>();
    for (size_t j = 0; j < partition_cnt; j++) {
      v8::Local<v8::Object> partition_object = Nan::New<v8::Object>();
      const rd_kafka_TopicPartitionInfo_t* partition = partitions[j];
      Nan::Set(partition_object, Nan::New("partition").ToLocalChecked(),
               Nan::New<v8::Number>(
                   rd_kafka_TopicPartitionInfo_partition(partition)));

      const rd_kafka_Node_t* leader =
          rd_kafka_TopicPartitionInfo_leader(partition);
      Nan::Set(partition_object, Nan::New("leader").ToLocalChecked(),
               Conversion::Util::ToV8Object(leader));

      size_t isr_cnt;
      const rd_kafka_Node_t** isr =
          rd_kafka_TopicPartitionInfo_isr(partition, &isr_cnt);
      v8::Local<v8::Array> isrArray = Nan::New<v8::Array>();
      for (size_t k = 0; k < isr_cnt; k++) {
        Nan::Set(isrArray, k, Conversion::Util::ToV8Object(isr[k]));
      }
      Nan::Set(partition_object, Nan::New("isr").ToLocalChecked(), isrArray);

      size_t replicas_cnt;
      const rd_kafka_Node_t** replicas =
          rd_kafka_TopicPartitionInfo_replicas(partition, &replicas_cnt);
      v8::Local<v8::Array> replicasArray = Nan::New<v8::Array>();
      for (size_t k = 0; k < replicas_cnt; k++) {
        Nan::Set(replicasArray, k, Conversion::Util::ToV8Object(replicas[k]));
      }
      Nan::Set(partition_object, Nan::New("replicas").ToLocalChecked(),
               replicasArray);

      Nan::Set(partitionsArray, j, partition_object);
    }
    Nan::Set(topic_object, Nan::New("partitions").ToLocalChecked(),
             partitionsArray);

    Nan::Set(returnArray, topicIndex++, topic_object);
  }

  return returnArray;
}

/**
 * @brief Converts a rd_kafka_ListOffsets_result_t* into a v8 Array.
 */
v8::Local<v8::Array> FromListOffsetsResult(
    const rd_kafka_ListOffsets_result_t* result) {
  /* Return object type:
   [{
     topic: string,
     partition: number,
     offset: number,
     error: LibrdKafkaError
     timestamp: number
   }]
  */

  size_t result_cnt, i;
  const rd_kafka_ListOffsetsResultInfo_t** results =
      rd_kafka_ListOffsets_result_infos(result, &result_cnt);

  v8::Local<v8::Array> resultArray = Nan::New<v8::Array>();
  int partitionIndex = 0;

  for (i = 0; i < result_cnt; i++) {
    const rd_kafka_topic_partition_t* partition =
        rd_kafka_ListOffsetsResultInfo_topic_partition(results[i]);
    int64_t timestamp = rd_kafka_ListOffsetsResultInfo_timestamp(results[i]);

    // Create the ListOffsetsResult object
    v8::Local<v8::Object> partition_object = Nan::New<v8::Object>();

    // Set topic, partition, offset, error and timestamp
    Nan::Set(partition_object, Nan::New("topic").ToLocalChecked(),
             Nan::New<v8::String>(partition->topic).ToLocalChecked());
    Nan::Set(partition_object, Nan::New("partition").ToLocalChecked(),
             Nan::New<v8::Number>(partition->partition));
    Nan::Set(partition_object, Nan::New("offset").ToLocalChecked(),
             Nan::New<v8::Number>(partition->offset));
    if (partition->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      RdKafka::ErrorCode code = static_cast<RdKafka::ErrorCode>(partition->err);
      Nan::Set(partition_object, Nan::New("error").ToLocalChecked(),
               RdKafkaError(code, rd_kafka_err2str(partition->err)));
    }
    // Set leaderEpoch (if available)
    int32_t leader_epoch =
        rd_kafka_topic_partition_get_leader_epoch(partition);
    if (leader_epoch >= 0) {
      Nan::Set(partition_object, Nan::New("leaderEpoch").ToLocalChecked(),
                Nan::New<v8::Number>(leader_epoch));
    }
    Nan::Set(partition_object, Nan::New("timestamp").ToLocalChecked(),
             Nan::New<v8::Number>(timestamp));

    Nan::Set(resultArray, partitionIndex++, partition_object);
  }

  return resultArray;
}

}  // namespace Admin

}  // namespace Conversion

namespace Util {
  std::string FromV8String(v8::Local<v8::String> val) {
    Nan::Utf8String keyUTF8(val);
    return std::string(*keyUTF8);
  }
}  // Namespace Util

}  // namespace NodeKafka
