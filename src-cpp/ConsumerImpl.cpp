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

#include <iostream>
#include <string>
#include <list>
#include <cerrno>

#include "rdkafkacpp_int.h"

RdKafka::Consumer::~Consumer () {}

RdKafka::Consumer *RdKafka::Consumer::create (RdKafka::Conf *conf,
                                              std::string &errstr) {
  char errbuf[512];
  RdKafka::ConfImpl *confimpl = dynamic_cast<RdKafka::ConfImpl *>(conf);
  RdKafka::ConsumerImpl *rkc = new RdKafka::ConsumerImpl();
  rd_kafka_conf_t *rk_conf = NULL;

  if (confimpl) {
    if (!confimpl->rk_conf_) {
      errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
      delete rkc;
      return NULL;
    }

    rkc->set_common_config(confimpl);

    rk_conf = rd_kafka_conf_dup(confimpl->rk_conf_);
  }

  rd_kafka_t *rk;
  if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, rk_conf,
                          errbuf, sizeof(errbuf)))) {
    errstr = errbuf;
    delete rkc;
    return NULL;
  }

  rkc->rk_ = rk;

  /* Redirect logging to event callback */
  if (confimpl && confimpl->event_cb_)
    rd_kafka_set_logger(rk, RdKafka::log_cb_trampoline);

  return rkc;
}


RdKafka::ErrorCode RdKafka::ConsumerImpl::start (Topic *topic,
                                                 int32_t partition,
                                                 int64_t offset) {
  RdKafka::TopicImpl *topicimpl = dynamic_cast<RdKafka::TopicImpl *>(topic);

  if (rd_kafka_consume_start(topicimpl->rkt_, partition, offset) == -1)
    return static_cast<RdKafka::ErrorCode>(rd_kafka_errno2err(errno));

  return RdKafka::ERR_NO_ERROR;
}



RdKafka::ErrorCode RdKafka::ConsumerImpl::stop (Topic *topic,
                                                int32_t partition) {
  RdKafka::TopicImpl *topicimpl = dynamic_cast<RdKafka::TopicImpl *>(topic);

  if (rd_kafka_consume_stop(topicimpl->rkt_, partition) == -1)
    return static_cast<RdKafka::ErrorCode>(rd_kafka_errno2err(errno));

  return RdKafka::ERR_NO_ERROR;
}


RdKafka::Message *RdKafka::ConsumerImpl::consume (Topic *topic,
                                                  int32_t partition,
                                                  int timeout_ms) {
  RdKafka::TopicImpl *topicimpl = dynamic_cast<RdKafka::TopicImpl *>(topic);
  rd_kafka_message_t *rkmessage;

  rkmessage = rd_kafka_consume(topicimpl->rkt_, partition, timeout_ms);
  if (!rkmessage)
    return new RdKafka::MessageImpl(topic,
                                    static_cast<RdKafka::ErrorCode>
                                    (rd_kafka_errno2err(errno)));

  return new RdKafka::MessageImpl(topic, rkmessage);
}
