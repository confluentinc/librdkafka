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
#include <stdlib.h>

#include "rdkafkacpp.h"

extern "C" {
#include "../src/rdkafka.h"
}

#ifdef _MSC_VER
typedef int mode_t;
#endif

namespace RdKafka {


void log_cb_trampoline (const rd_kafka_t *rk, int level,
                        const char *fac, const char *buf);
void error_cb_trampoline (rd_kafka_t *rk, int err, const char *reason,
                          void *opaque);
void throttle_cb_trampoline (rd_kafka_t *rk, const char *broker_name,
			     int32_t broker_id, int throttle_time_ms,
			     void *opaque);
int stats_cb_trampoline (rd_kafka_t *rk, char *json, size_t json_len,
                         void *opaque);
int socket_cb_trampoline (int domain, int type, int protocol, void *opaque);
int open_cb_trampoline (const char *pathname, int flags, mode_t mode,
                        void *opaque);
void rebalance_cb_trampoline (rd_kafka_t *rk,
                              rd_kafka_resp_err_t err,
                              rd_kafka_topic_partition_list_t *c_partitions,
                              void *opaque);
void offset_commit_cb_trampoline (rd_kafka_t *rk,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *c_offsets,
                                  void *opaque);

rd_kafka_topic_partition_list_t *
    partitions_to_c_parts (const std::vector<TopicPartition*> &partitions);

/**
 * @brief Update the application provided 'partitions' with info from 'c_parts'
 */
void update_partitions_from_c_parts (std::vector<TopicPartition*> &partitions,
                                     const rd_kafka_topic_partition_list_t *c_parts);


class EventImpl : public Event {
 public:
  ~EventImpl () {};

  EventImpl (Type type, ErrorCode err, Severity severity,
             const char *fac, const char *str):
  type_(type), err_(err), severity_(severity), fac_(fac ? fac : ""),
	  str_(str), id_(0), throttle_time_(0) {};

  EventImpl (Type type):
  type_(type), err_(ERR_NO_ERROR), severity_(EVENT_SEVERITY_EMERG),
	  fac_(""), str_(""), id_(0), throttle_time_(0) {};

  Type        type () const { return type_; }
  ErrorCode   err () const { return err_; }
  Severity    severity () const { return severity_; }
  std::string fac () const { return fac_; }
  std::string str () const { return str_; }
  std::string broker_name () const {
	  if (type_ == EVENT_THROTTLE)
		  return str_;
	  else
		  return std::string("");
  }
  int         broker_id () const { return id_; }
  int         throttle_time () const { return throttle_time_; }

  Type        type_;
  ErrorCode   err_;
  Severity    severity_;
  std::string fac_;
  std::string str_;         /* reused for THROTTLE broker_name */
  int         id_;
  int         throttle_time_;
};


class MessageImpl : public Message {
 public:
  ~MessageImpl () {
    if (free_rkmessage_)
      rd_kafka_message_destroy(const_cast<rd_kafka_message_t *>(rkmessage_));
    if (key_)
            delete key_;
  };

  MessageImpl (RdKafka::Topic *topic, rd_kafka_message_t *rkmessage):
  topic_(topic), rkmessage_(rkmessage), free_rkmessage_(true), key_(NULL) {}

  MessageImpl (RdKafka::Topic *topic, rd_kafka_message_t *rkmessage,
               bool dofree):
  topic_(topic), rkmessage_(rkmessage), free_rkmessage_(dofree), key_(NULL) { }

  MessageImpl (RdKafka::Topic *topic, const rd_kafka_message_t *rkmessage):
  topic_(topic), rkmessage_(rkmessage), free_rkmessage_(false), key_(NULL) { }

  MessageImpl (rd_kafka_message_t *rkmessage):
  topic_(NULL), rkmessage_(rkmessage), free_rkmessage_(true), key_(NULL) {
    if (rkmessage->rkt) {
      /* Possibly NULL */
      topic_ = static_cast<Topic *>(rd_kafka_topic_opaque(rkmessage->rkt));
    }
  }

  /* Create errored message */
  MessageImpl (RdKafka::Topic *topic, RdKafka::ErrorCode err):
  topic_(topic), free_rkmessage_(false), key_(NULL) {
    rkmessage_ = &rkmessage_err_;
    memset(&rkmessage_err_, 0, sizeof(rkmessage_err_));
    rkmessage_err_.err = static_cast<rd_kafka_resp_err_t>(err);
  }

  std::string         errstr() const {
    /* FIXME: If there is an error string in payload (for consume_cb)
     *        it wont be shown since 'payload' is reused for errstr
     *        and we cant distinguish between consumer and producer.
     *        For the producer case the payload needs to be the original
     *        payload pointer. */
    const char *es = rd_kafka_err2str(rkmessage_->err);
    return std::string(es ? es : "");
  }

  ErrorCode           err () const {
    return static_cast<RdKafka::ErrorCode>(rkmessage_->err);
  }

  Topic              *topic () const { return topic_; }
  std::string         topic_name  () const {
          if (rkmessage_->rkt)
                  return rd_kafka_topic_name(rkmessage_->rkt);
          else
                  return "";
  }
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
  const void         *key_pointer () const { return rkmessage_->key; }
  size_t              key_len () const { return rkmessage_->key_len; }

  int64_t             offset () const { return rkmessage_->offset; }
  void               *msg_opaque () const { return rkmessage_->_private; };

  RdKafka::Topic *topic_;
  const rd_kafka_message_t *rkmessage_;
  bool free_rkmessage_;
  /* For error signalling by the C++ layer the .._err_ message is
   * used as a place holder and rkmessage_ is set to point to it. */
  rd_kafka_message_t rkmessage_err_;
  mutable std::string *key_; /* mutable because it's a cached value */

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
      partitioner_kp_cb_(NULL),
      rebalance_cb_(NULL),
      offset_commit_cb_(NULL),
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

  Conf::ConfResult set (const std::string &name, const Conf *topic_conf,
                        std::string &errstr) {
    const ConfImpl *tconf_impl =
        dynamic_cast<const RdKafka::ConfImpl *>(topic_conf);
    if (name != "default_topic_conf" || !tconf_impl->rkt_conf_) {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    rd_kafka_conf_set_default_topic_conf(rk_conf_,
                                         rd_kafka_topic_conf_dup(tconf_impl->
                                                                 rkt_conf_));

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

  Conf::ConfResult set (const std::string &name,
                        PartitionerKeyPointerCb *partitioner_kp_cb,
                        std::string &errstr) {
    if (name != "partitioner_key_pointer_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rkt_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_TOPIC object";
      return Conf::CONF_INVALID;
    }

    partitioner_kp_cb_ = partitioner_kp_cb;
    return Conf::CONF_OK;
  }

  Conf::ConfResult set (const std::string &name, SocketCb *socket_cb,
                        std::string &errstr) {
    if (name != "socket_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
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

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    open_cb_ = open_cb;
    return Conf::CONF_OK;
  }


  Conf::ConfResult set (const std::string &name, RebalanceCb *rebalance_cb,
                        std::string &errstr) {
    if (name != "rebalance_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    rebalance_cb_ = rebalance_cb;
    return Conf::CONF_OK;
  }


  Conf::ConfResult set (const std::string &name,
                        OffsetCommitCb *offset_commit_cb,
                        std::string &errstr) {
    if (name != "offset_commit_cb") {
      errstr = "Invalid value type";
      return Conf::CONF_INVALID;
    }

    if (!rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      return Conf::CONF_INVALID;
    }

    offset_commit_cb_ = offset_commit_cb;
    return Conf::CONF_OK;
  }

  Conf::ConfResult get(const std::string &name, std::string &value) const {
	  size_t size;
	  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;
	  if (rk_conf_) {
		  if ((res = rd_kafka_conf_get(rk_conf_,
			  name.c_str(), NULL, &size)) != RD_KAFKA_CONF_OK)
			  return static_cast<Conf::ConfResult>(res);

		  value.resize(size);
		  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(),
			  (char *)value.c_str(), &size)) != RD_KAFKA_CONF_OK)
			  return static_cast<Conf::ConfResult>(res);
	  }
	  else if (rkt_conf_) {
		  if ((res = rd_kafka_topic_conf_get(rkt_conf_,
			  name.c_str(), NULL, &size)) != RD_KAFKA_CONF_OK)
			  return static_cast<Conf::ConfResult>(res);

		  value.resize(size);
		  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(),
			  (char *)value.c_str(), &size)) != RD_KAFKA_CONF_OK)
			  return static_cast<Conf::ConfResult>(res);
	  }

	  return Conf::CONF_OK;
  }



  std::list<std::string> *dump ();

  DeliveryReportCb *dr_cb_;
  EventCb *event_cb_;
  SocketCb *socket_cb_;
  OpenCb *open_cb_;
  PartitionerCb *partitioner_cb_;
  PartitionerKeyPointerCb *partitioner_kp_cb_;
  RebalanceCb *rebalance_cb_;
  OffsetCommitCb *offset_commit_cb_;
  ConfType conf_type_;
  rd_kafka_conf_t *rk_conf_;
  rd_kafka_topic_conf_t *rkt_conf_;
};


class HandleImpl : virtual public Handle {
 public:
  ~HandleImpl() {};
  HandleImpl () {};
  const std::string name () const { return std::string(rd_kafka_name(rk_)); };
  const std::string memberid () const {
	  char *str = rd_kafka_memberid(rk_);
	  std::string memberid = str ? str : "";
	  if (str)
		  rd_kafka_mem_free(rk_, str);
	  return memberid;
  }
  int poll (int timeout_ms) { return rd_kafka_poll(rk_, timeout_ms); };
  int outq_len () { return rd_kafka_outq_len(rk_); };

  void set_common_config (RdKafka::ConfImpl *confimpl);

  RdKafka::ErrorCode metadata (bool all_topics,const Topic *only_rkt,
            Metadata **metadatap, int timeout_ms);

  ErrorCode pause (std::vector<TopicPartition*> &partitions);
  ErrorCode resume (std::vector<TopicPartition*> &partitions);

  ErrorCode get_offsets (const std::string &topic, int32_t partition,
			 int64_t *low, int64_t *high, int timeout_ms) {
	  return static_cast<RdKafka::ErrorCode>(
		  rd_kafka_get_offsets(
			  rk_, topic.c_str(), partition,
			  low, high, timeout_ms));
  }


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
  PartitionerKeyPointerCb *partitioner_kp_cb_;
  RebalanceCb *rebalance_cb_;
  OffsetCommitCb *offset_commit_cb_;
};


class TopicImpl : public Topic {
 public:
  ~TopicImpl () {
    rd_kafka_topic_destroy(rkt_);
  }

  const std::string name () const {
    return rd_kafka_topic_name(rkt_);
  }

  bool partition_available (int32_t partition) const {
    return !!rd_kafka_topic_partition_available(rkt_, partition);
  }

  ErrorCode offset_store (int32_t partition, int64_t offset) {
    return static_cast<RdKafka::ErrorCode>(
        rd_kafka_offset_store(rkt_, partition, offset));
  }

  static Topic *create (Handle &base, const std::string &topic,
                        Conf *conf);

  rd_kafka_topic_t *rkt_;
  PartitionerCb *partitioner_cb_;
  PartitionerKeyPointerCb *partitioner_kp_cb_;
};


/**
 * Topic and Partition
 */
class TopicPartitionImpl : public TopicPartition {
public:
  ~TopicPartitionImpl() {};

  static TopicPartition *create (const std::string &topic, int partition);

  TopicPartitionImpl (const std::string &topic, int partition):
  topic_(topic), partition_(partition), offset_(RdKafka::Topic::OFFSET_INVALID),
      err_(ERR_NO_ERROR) {}

  TopicPartitionImpl (const rd_kafka_topic_partition_t *c_part) {
    topic_ = std::string(c_part->topic);
    partition_ = c_part->partition;
    offset_ = c_part->offset;
    err_ = static_cast<ErrorCode>(c_part->err);
    // FIXME: metadata
  }

  int partition () { return partition_; }
  const std::string &topic () const { return topic_ ; }

  int64_t offset () { return offset_; }

  ErrorCode err () { return err_; }

  void set_offset (int64_t offset) { offset_ = offset; }

  std::ostream& operator<<(std::ostream &ostrm) const {
    return ostrm << topic_ << " [" << partition_ << "]";
  }

  std::string topic_;
  int partition_;
  int64_t offset_;
  ErrorCode err_;
};



class KafkaConsumerImpl : virtual public KafkaConsumer, virtual public HandleImpl {
public:
  ~KafkaConsumerImpl () {

  }

  static KafkaConsumer *create (Conf *conf, std::string &errstr);

  ErrorCode assignment (std::vector<TopicPartition*> &partitions);
  ErrorCode subscription (std::vector<std::string> &topics);
  ErrorCode subscribe (const std::vector<std::string> &topics);
  ErrorCode unsubscribe ();
  ErrorCode assign (const std::vector<TopicPartition*> &partitions);
  ErrorCode unassign () {
	  std::vector<TopicPartition*> empty;
	  return assign(empty);
  }
  Message *consume (int timeout_ms);
  ErrorCode commitSync () {
    return static_cast<ErrorCode>(rd_kafka_commit(rk_, NULL, 0/*sync*/));
  }
  ErrorCode commitAsync () {
    return static_cast<ErrorCode>(rd_kafka_commit(rk_, NULL, 1/*async*/));
  }
  ErrorCode commitSync (Message *message) {
	  MessageImpl *msgimpl = dynamic_cast<MessageImpl*>(message);
	  return static_cast<ErrorCode>(
                  rd_kafka_commit_message(rk_, msgimpl->rkmessage_, 0/*sync*/));
  }
  ErrorCode commitAsync (Message *message) {
	  MessageImpl *msgimpl = dynamic_cast<MessageImpl*>(message);
	  return static_cast<ErrorCode>(
                  rd_kafka_commit_message(rk_, msgimpl->rkmessage_,1/*async*/));
  }

  ErrorCode commitSync (std::vector<TopicPartition*> &offsets) {
	  rd_kafka_topic_partition_list_t *c_parts =
		  partitions_to_c_parts(offsets);
	  rd_kafka_resp_err_t err =
		  rd_kafka_commit(rk_, c_parts, 0);
	  if (!err)
		  update_partitions_from_c_parts(offsets, c_parts);
	  rd_kafka_topic_partition_list_destroy(c_parts);
	  return static_cast<ErrorCode>(err);
  }

  ErrorCode commitAsync (const std::vector<TopicPartition*> &offsets) {
	  rd_kafka_topic_partition_list_t *c_parts =
		  partitions_to_c_parts(offsets);
	  rd_kafka_resp_err_t err =
		  rd_kafka_commit(rk_, c_parts, 1);
	  rd_kafka_topic_partition_list_destroy(c_parts);
	  return static_cast<ErrorCode>(err);
  }


  ErrorCode position (std::vector<TopicPartition*> &partitions, int timeout_ms);

  ErrorCode close ();
};


class MetadataImpl : public Metadata {
 public:
  MetadataImpl(const rd_kafka_metadata_t *metadata);
  ~MetadataImpl();

  const std::vector<const BrokerMetadata *> *brokers() const {
    return &brokers_;
  }

  const std::vector<const TopicMetadata *>  *topics() const {
    return &topics_;
  }

  const std::string orig_broker_name() const {
    return std::string(metadata_->orig_broker_name);
  }

  int32_t orig_broker_id() const {
    return metadata_->orig_broker_id;
  }

private:
  const rd_kafka_metadata_t *metadata_;
  std::vector<const BrokerMetadata *> brokers_;
  std::vector<const TopicMetadata *> topics_;
  std::string orig_broker_name_;
};


class QueueImpl : public Queue {
 public:
  QueueImpl () { }
  ~QueueImpl () {
    rd_kafka_queue_destroy(queue_);
  }
  rd_kafka_queue_t *queue_;
};





class ConsumerImpl : virtual public Consumer, virtual public HandleImpl {
 public:
  ~ConsumerImpl () {
    rd_kafka_destroy(rk_); };
  static Consumer *create (Conf *conf, std::string &errstr);

  ErrorCode start (Topic *topic, int32_t partition, int64_t offset);
  ErrorCode start (Topic *topic, int32_t partition, int64_t offset,
                   Queue *queue);
  ErrorCode stop (Topic *topic, int32_t partition);
  Message *consume (Topic *topic, int32_t partition, int timeout_ms);
  Message *consume (Queue *queue, int timeout_ms);
  int consume_callback (Topic *topic, int32_t partition, int timeout_ms,
                        ConsumeCb *cb, void *opaque);
  int consume_callback (Queue *queue, int timeout_ms,
                        RdKafka::ConsumeCb *consume_cb, void *opaque);
};



class ProducerImpl : virtual public Producer, virtual public HandleImpl {

 public:
  ~ProducerImpl () { if (rk_) rd_kafka_destroy(rk_); };

  ErrorCode produce (Topic *topic, int32_t partition,
                     int msgflags,
                     void *payload, size_t len,
                     const std::string *key,
                     void *msg_opaque);

  ErrorCode produce (Topic *topic, int32_t partition,
                     int msgflags,
                     void *payload, size_t len,
                     const void *key, size_t key_len,
                     void *msg_opaque);

  ErrorCode produce (Topic *topic, int32_t partition,
                     const std::vector<char> *payload,
                     const std::vector<char> *key,
                     void *msg_opaque);

  static Producer *create (Conf *conf, std::string &errstr);

};



}
