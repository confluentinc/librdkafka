/*
 * librdkafka - Apache Kafka C/C++ library
 *
 * Copyright (c) 2015 Samuele Maci
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

#include <iostream>
#include <string>
#include <list>
#include <cerrno>

#include "rdkafkacpp_int.h"

RdKafka::Queue::~Queue() {
}

RdKafka::Queue *RdKafka::Queue::create(Consumer *consumer) {
  RdKafka::ConsumerImpl *consumer_impl =
      dynamic_cast<RdKafka::ConsumerImpl *>(consumer);
  RdKafka::QueueImpl *queue_impl = new RdKafka::QueueImpl();
  queue_impl->rkqu = rd_kafka_queue_new(consumer_impl->rk_);
  return queue_impl;
}


RdKafka::ErrorCode RdKafka::QueueImpl::start(Topic *topic, int32_t partition,
    int64_t offset) {
  RdKafka::TopicImpl *topicimpl = dynamic_cast<RdKafka::TopicImpl *>(topic);

  if (rd_kafka_consume_start_queue(topicimpl->rkt_, partition, offset, rkqu) == -1)
    return static_cast<RdKafka::ErrorCode>(rd_kafka_errno2err(errno));

  return RdKafka::ERR_NO_ERROR;
}

RdKafka::ErrorCode RdKafka::QueueImpl::stop(Topic *topic, int32_t partition) {
  RdKafka::TopicImpl *topicimpl = dynamic_cast<RdKafka::TopicImpl *>(topic);

  if (rd_kafka_consume_stop(topicimpl->rkt_, partition) == -1)
    return static_cast<RdKafka::ErrorCode>(rd_kafka_errno2err(errno));

  return RdKafka::ERR_NO_ERROR;
}

void setTopic(rd_kafka_message_t *rkmessage, RdKafka::TopicImpl* topic) {
  topic->rkt_ = rkmessage->rkt;
  topic->partitioner_cb_ = NULL;
  topic->partitioner_kp_cb_ = NULL;
}

RdKafka::Message *RdKafka::QueueImpl::consume(int timeout_ms) {
  //TODO: check if could be correct
  RdKafka::TopicImpl *topicimpl = new RdKafka::TopicImpl();
  rd_kafka_message_t *rkmessage;

  rkmessage = rd_kafka_consume_queue(rkqu, timeout_ms);
  if (!rkmessage) {
    return new RdKafka::MessageImpl(topicimpl,
        static_cast<RdKafka::ErrorCode>(rd_kafka_errno2err(errno)));
  }
  setTopic(rkmessage, topicimpl);
  return new RdKafka::MessageImpl(topicimpl, rkmessage);
}

namespace {
/* Helper struct for `consume_callback'.
 * Encapsulates the values we need in order to call `rd_kafka_consume_callback_queue'
 * and keep track of the C++ callback function and `opaque' value.
 */
struct QueueImplCallback {
  QueueImplCallback(RdKafka::ConsumeCb* cb, void* data) :
      cb_cls(cb), cb_data(data) {
  }
  /* This function is the one we give to `rd_kafka_consume_callback', with
   * the `opaque' pointer pointing to an instance of this struct, in which
   * we can find the C++ callback and `cb_data'.
   */
  static void consume_cb_trampoline(rd_kafka_message_t *msg, void *opaque) {
    QueueImplCallback *instance = static_cast<QueueImplCallback*>(opaque);
    RdKafka::TopicImpl *topicimpl = new RdKafka::TopicImpl();
    setTopic(msg, topicimpl);
    RdKafka::MessageImpl message(topicimpl, msg, false /*don't free*/);
    instance->cb_cls->consume_cb(message, instance->cb_data);
  }
  RdKafka::ConsumeCb *cb_cls;
  void *cb_data;
};
}

int RdKafka::QueueImpl::consume_callback(int timeout_ms,
                                          RdKafka::ConsumeCb *consume_cb,
                                          void *opaque) {
  QueueImplCallback context(consume_cb, NULL);
  return rd_kafka_consume_callback_queue(rkqu, timeout_ms,
      &QueueImplCallback::consume_cb_trampoline, &context);
}
