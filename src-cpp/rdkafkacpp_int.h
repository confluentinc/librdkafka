/*
 * librdkafka - Apache Kafka C/C++ library
 *
 * Copyright (c) 2014 Magnus Edenhill
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

#pragma once

#include <string>
#include <iostream>
#include <cstring>

#include "rdkafkacpp.h"

extern "C" {
#include "../src/rdkafka.h"
};

namespace RdKafka {


void log_cb_trampoline (const rd_kafka_t *rk, int level,
                        const char *fac, const char *buf);
void error_cb_trampoline (rd_kafka_t *rk, int err, const char *reason,
                          void *opaque);
int stats_cb_trampoline (rd_kafka_t *rk, char *json, size_t json_len,
                         void *opaque);
int socket_cb_trampoline (int domain, int type, int protocol, void *opaque);
int open_cb_trampoline (const char *pathname, int flags, mode_t mode,
                        void *opaque);



class EventImpl : public Event {
 public:
  ~EventImpl () {};

  EventImpl (Type type, ErrorCode err, Severity severity,
             const char *fac, const char *str):
      type_(type), err_(err), severity_(severity), fac_(fac ? : ""), str_(str)
  { };

  Type        type () const { return type_; }
  ErrorCode   err () const { return err_; }
  Severity    severity () const { return severity_; }
  std::string fac () const { return fac_; }
  std::string str () const { return str_; }

  Type        type_;
  ErrorCode   err_;
  Severity    severity_;
  std::string fac_;
  std::string str_;
};


class MessageImpl : public Message {
 public:
  ~MessageImpl () {
    if (free_rkmessage_)
      rd_kafka_message_destroy(const_cast<rd_kafka_message_t *>(rkmessage_));
    delete key_;
  };

  MessageImpl (RdKafka::Topic *topic, rd_kafka_message_t *rkmessage):
      topic_(topic), rkmessage_(rkmessage), free_rkmessage_(true), key_(NULL) { }

  MessageImpl (RdKafka::Topic *topic, rd_kafka_message_t *rkmessage,
               bool dofree):
      topic_(topic), rkmessage_(rkmessage), free_rkmessage_(dofree), key_(NULL) { }

  MessageImpl (RdKafka::Topic *topic, const rd_kafka_message_t *rkmessage):
      topic_(topic), rkmessage_(rkmessage), free_rkmessage_(false), key_(NULL) { }

  /* Create errored message */
  MessageImpl (RdKafka::Topic *topic, RdKafka::ErrorCode err):
      topic_(topic), free_rkmessage_(false), key_(NULL) {
    rkmessage_ = &rkmessage_err_;
    memset(&rkmessage_err_, 0, sizeof(rkmessage_err_));
    rkmessage_err_.err = static_cast<rd_kafka_resp_err_t>(err);
  }

  std::string         errstr() const {
    /* FIXME: If there is a error string in payload (for consume_cb)
     *        it wont be shown since 'payload' is reused for errstr
     *        and we cant distinguish between consumer and producer.
     *        For the producer case the payload needs to be the original
     *        payload pointer. */
    const char *es = rd_kafka_err2str(rkmessage_->err);
    return std::string(es ? : "");
  }

  ErrorCode           err () const {
    return static_cast<RdKafka::ErrorCode>(rkmessage_->err);
  }

  Topic              *topic () const { return topic_; }
  int32_t             partition () const { return rkmessage_->partition; }
  void               *payload () const { return rkmessage_->payload; }
  size_t              len () const { return rkmessage_->len; }
  const std::string  *key () const {
    if (key_) {
      return key_;
    } else if (rkmessage_->key) {
      key_ = new std::string(static_cast<char const*>(rkmessage_->key), rkmessage_->key_len);
      return key_;
    }
    return NULL;
  }
  int64_t             offset () const { return rkmessage_->offset; }
  void               *msg_opaque () const { return rkmessage_->_private; };

  RdKafka::Topic *topic_;
  const rd_kafka_message_t *rkmessage_;
  bool free_rkmessage_;
  /* For error signalling by the C++ layer the .._err_ message is
   * used as a place holder and rkmessage_ is set to point to it. */
  rd_kafka_message_t rkmessage_err_;
  mutable std::string* key_; /* mutable because it's a cached value */

private:
  /* "delete" copy ctor + copy assignment, for safety of key_ */
  MessageImpl(MessageImpl const&) /*= delete*/;
  MessageImpl& operator=(MessageImpl const&) /*= delete*/;
};


class ConfImpl : public Conf {
 public:
  ConfImpl()
      :dr_cb_(NULL),
      event_cb_(NULL),
      socket_cb_(NULL),
      open_cb_(NULL),
      partitioner_cb_(NULL),
      rk_conf_(NULL),
      rkt_conf_(NULL){}
  ~ConfImpl () {
    if (rk_conf_)
      rd_kafka_conf_destroy(rk_conf_);
    else if (rkt_conf_)
      rd_kafka_topic_conf_destroy(rkt_conf_);
  }

  Conf::ConfResult set(const std::string &name,
                       const std::string &value,
                       std::string &errstr);

  Conf::ConfResult set (const std::string &name, DeliveryReportCb *dr_cb,
                        std::string &errstr) {
    if (name != "dr_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    dr_cb_ = dr_cb;
    return Conf::CONF_OK;
  }

  Conf::ConfResult set (const std::string &name, EventCb *event_cb,
                        std::string &errstr) {
    if (name != "event_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    event_cb_ = event_cb;
    return Conf::CONF_OK;
  }

  Conf::ConfResult set (const std::string &name, PartitionerCb *partitioner_cb,
                        std::string &errstr) {
    if (name != "partitioner_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rkt_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_TOPIC object";
      return Conf::CONF_INVALID;
    }

    partitioner_cb_ = partitioner_cb;
    return Conf::CONF_OK;
  }


  Conf::ConfResult set (const std::string &name, SocketCb *socket_cb,
                        std::string &errstr) {
    if (name != "socket_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rkt_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_TOPIC object";
      return Conf::CONF_INVALID;
    }

    socket_cb_ = socket_cb;
    return Conf::CONF_OK;
  }


  Conf::ConfResult set (const std::string &name, OpenCb *open_cb,
                        std::string &errstr) {
    if (name != "open_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rkt_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_TOPIC object";
      return Conf::CONF_INVALID;
    }

    open_cb_ = open_cb;
    return Conf::CONF_OK;
  }


  std::list<std::string> *dump ();

  DeliveryReportCb *dr_cb_;
  EventCb *event_cb_;
  SocketCb *socket_cb_;
  OpenCb *open_cb_;
  PartitionerCb *partitioner_cb_;
  ConfType conf_type_;
  rd_kafka_conf_t *rk_conf_;
  rd_kafka_topic_conf_t *rkt_conf_;
};


class HandleImpl : virtual public Handle {
 public:
  ~HandleImpl() {};
  HandleImpl () {};
  const std::string name () { return std::string(rd_kafka_name(rk_)); };
  int poll (int timeout_ms) { return rd_kafka_poll(rk_, timeout_ms); };
  int outq_len () { return rd_kafka_outq_len(rk_); };

  void set_common_config (RdKafka::ConfImpl *confimpl);


  rd_kafka_t *rk_;
  /* All Producer and Consumer callbacks must reside in HandleImpl and
   * the opaque provided to rdkafka must be a pointer to HandleImpl, since
   * ProducerImpl and ConsumerImpl classes cannot be safely directly cast to
   * HandleImpl due to the skewed diamond inheritance. */
  EventCb *event_cb_;
  SocketCb *socket_cb_;
  OpenCb *open_cb_;
  DeliveryReportCb *dr_cb_;
  PartitionerCb *partitioner_cb_;
};


class TopicImpl : public Topic {
 public:
  ~TopicImpl () {
    rd_kafka_topic_destroy(rkt_);
  }

  const std::string name () {
    return rd_kafka_topic_name(rkt_);
  }

  bool partition_available (int32_t partition) {
    return rd_kafka_topic_partition_available(rkt_, partition);
  }

  ErrorCode offset_store (int32_t partition, int64_t offset) {
    return static_cast<RdKafka::ErrorCode>(
        rd_kafka_offset_store(rkt_, partition, offset));
  }

  static Topic *create (Handle &base, const std::string &topic,
                        Conf *conf);

  rd_kafka_topic_t *rkt_;
  PartitionerCb *partitioner_cb_;
};






class ConsumerImpl : virtual public Consumer, virtual public HandleImpl {
 public:
  ~ConsumerImpl () {
    rd_kafka_destroy(rk_); };
  static Consumer *create (Conf *conf, std::string &errstr);

  ErrorCode start (Topic *topic, int32_t partition, int64_t offset);
  ErrorCode stop (Topic *topic, int32_t partition);
  Message *consume (Topic *topic, int32_t partition, int timeout_ms);

};



class ProducerImpl : virtual public Producer, virtual public HandleImpl {

 public:
  ~ProducerImpl () { rd_kafka_destroy(rk_); };

  ErrorCode produce (Topic *topic, int32_t partition,
                     int msgflags,
                     void *payload, size_t len,
                     const std::string *key,
                     void *msg_opaque);

  static Producer *create (Conf *conf, std::string &errstr);

};



};
