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

#include "rdkafkacpp_int.h"

void RdKafka::log_cb_trampoline (const rd_kafka_t *rk, int level,
                                 const char *fac, const char *buf) {
  if (!rk) {
    rd_kafka_log_print(rk, level, fac, buf);
    return;
  }

  void *opaque = rd_kafka_opaque(rk);
  RdKafka::HandleImpl *handle = static_cast<RdKafka::HandleImpl *>(opaque);

  if (!handle->event_cb_) {
    rd_kafka_log_print(rk, level, fac, buf);
    return;
  }

  RdKafka::EventImpl event(RdKafka::Event::EVENT_LOG,
                           RdKafka::ERR_NO_ERROR,
                           static_cast<RdKafka::Event::Severity>(level),
                           fac, buf);

  handle->event_cb_->event_cb(event);
}


void RdKafka::error_cb_trampoline (rd_kafka_t *rk, int err,
                                   const char *reason, void *opaque) {
  RdKafka::HandleImpl *handle = static_cast<RdKafka::HandleImpl *>(opaque);

  RdKafka::EventImpl event(RdKafka::Event::EVENT_ERROR,
                           static_cast<RdKafka::ErrorCode>(err),
                           RdKafka::Event::EVENT_SEVERITY_ERROR,
                           NULL,
                           reason);

  handle->event_cb_->event_cb(event);
}


int RdKafka::stats_cb_trampoline (rd_kafka_t *rk, char *json, size_t json_len,
                                  void *opaque) {
  RdKafka::HandleImpl *handle = static_cast<RdKafka::HandleImpl *>(opaque);

  RdKafka::EventImpl event(RdKafka::Event::EVENT_STATS,
                           RdKafka::ERR_NO_ERROR,
                           RdKafka::Event::EVENT_SEVERITY_INFO,
                           NULL, json);

  handle->event_cb_->event_cb(event);

  return 0;
}


int RdKafka::socket_cb_trampoline (int domain, int type, int protocol,
                                   void *opaque) {
  RdKafka::HandleImpl *handle = static_cast<RdKafka::HandleImpl *>(opaque);

  return handle->socket_cb_->socket_cb(domain, type, protocol);
}


int RdKafka::open_cb_trampoline (const char *pathname, int flags, mode_t mode,
                                 void *opaque) {
  RdKafka::HandleImpl *handle = static_cast<RdKafka::HandleImpl *>(opaque);

  return handle->open_cb_->open_cb(pathname, flags, static_cast<int>(mode));
}


void RdKafka::HandleImpl::set_common_config (RdKafka::ConfImpl *confimpl) {

  rd_kafka_conf_set_opaque(confimpl->rk_conf_, this);

  if (confimpl->event_cb_) {
    rd_kafka_conf_set_error_cb(confimpl->rk_conf_,
                               RdKafka::error_cb_trampoline);
    rd_kafka_conf_set_stats_cb(confimpl->rk_conf_,
                               RdKafka::stats_cb_trampoline);
    event_cb_ = confimpl->event_cb_;
  }

  if (confimpl->socket_cb_) {
    rd_kafka_conf_set_socket_cb(confimpl->rk_conf_,
                                RdKafka::socket_cb_trampoline);
    socket_cb_ = confimpl->socket_cb_;
  }

  if (confimpl->open_cb_) {
    rd_kafka_conf_set_open_cb(confimpl->rk_conf_, RdKafka::open_cb_trampoline);
    open_cb_ = confimpl->open_cb_;
  }

}
