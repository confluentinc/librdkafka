/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>

#include "src/errors.h"

namespace NodeKafka {

v8::Local<v8::Object> RdKafkaError(const RdKafka::ErrorCode &err,
                                   const std::string &errstr) {
  int code = static_cast<int>(err);

  v8::Local<v8::Object> ret = Nan::New<v8::Object>();

  Nan::Set(ret, Nan::New("message").ToLocalChecked(),
    Nan::New<v8::String>(errstr).ToLocalChecked());
  Nan::Set(ret, Nan::New("code").ToLocalChecked(),
    Nan::New<v8::Number>(code));

  return ret;
}

v8::Local<v8::Object> RdKafkaError(const RdKafka::ErrorCode &err) {
  std::string errstr = RdKafka::err2str(err);
  return RdKafkaError(err, errstr);
}

v8::Local<v8::Object> RdKafkaError(
  const RdKafka::ErrorCode &err, std::string errstr,
  bool isFatal, bool isRetriable, bool isTxnRequiresAbort) {
  v8::Local<v8::Object> ret = RdKafkaError(err, errstr);

  Nan::Set(ret, Nan::New("isFatal").ToLocalChecked(),
    Nan::New<v8::Boolean>(isFatal));
  Nan::Set(ret, Nan::New("isRetriable").ToLocalChecked(),
    Nan::New<v8::Boolean>(isRetriable));
  Nan::Set(ret, Nan::New("isTxnRequiresAbort").ToLocalChecked(),
    Nan::New<v8::Boolean>(isTxnRequiresAbort));

  return ret;
}

Baton::Baton(const RdKafka::ErrorCode &code) {
  m_err = code;
}

Baton::Baton(const RdKafka::ErrorCode &code, std::string errstr) {
  m_err = code;
  m_errstr = errstr;
}

Baton::Baton(void* data) {
  m_err = RdKafka::ERR_NO_ERROR;
  m_data = data;
}

Baton::Baton(const RdKafka::ErrorCode &code, std::string errstr, bool isFatal,
             bool isRetriable, bool isTxnRequiresAbort) {
  m_err = code;
  m_errstr = errstr;
  m_isFatal = isFatal;
  m_isRetriable = isRetriable;
  m_isTxnRequiresAbort = isTxnRequiresAbort;
}

/**
 * Creates a Baton from an rd_kafka_error_t* and destroys it.
 */
Baton Baton::BatonFromErrorAndDestroy(rd_kafka_error_t *error) {
  std::string errstr = rd_kafka_error_string(error);
  RdKafka::ErrorCode err =
      static_cast<RdKafka::ErrorCode>(rd_kafka_error_code(error));
  rd_kafka_error_destroy(error);
  return Baton(err, errstr);
}

/**
 * Creates a Baton from an RdKafka::Error* and deletes it.
 */
Baton Baton::BatonFromErrorAndDestroy(RdKafka::Error *error) {
  std::string errstr = error->str();
  RdKafka::ErrorCode err = error->code();
  delete error;
  return Baton(err, errstr);
}

v8::Local<v8::Object> Baton::ToObject() {
  if (m_errstr.empty()) {
    return RdKafkaError(m_err);
  } else {
    return RdKafkaError(m_err, m_errstr);
  }
}

v8::Local<v8::Object> Baton::ToTxnObject() {
  return RdKafkaError(m_err, m_errstr, m_isFatal, m_isRetriable, m_isTxnRequiresAbort); // NOLINT
}

RdKafka::ErrorCode Baton::err() {
  return m_err;
}

std::string Baton::errstr() {
  if (m_errstr.empty()) {
    return RdKafka::err2str(m_err);
  } else {
    return m_errstr;
  }
}

}  // namespace NodeKafka
