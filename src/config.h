/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_CONFIG_H_
#define SRC_CONFIG_H_

#include <nan.h>
#include <iostream>
#include <vector>
#include <list>
#include <string>

#include "rdkafkacpp.h" // NOLINT
#include "src/common.h"
#include "src/callbacks.h"

namespace NodeKafka {

class Conf : public RdKafka::Conf {
 public:
  ~Conf();

  static Conf* create(RdKafka::Conf::ConfType, v8::Local<v8::Object>, std::string &);  // NOLINT
  static void DumpConfig(std::list<std::string> *);

  void listen();
  void stop();

  void ConfigureCallback(
    const std::string &string_key,
    const v8::Local<v8::Function> &cb,
    bool add, std::string &errstr);

  bool is_sasl_oauthbearer() const;

 private:
  NodeKafka::Callbacks::Rebalance *rebalance_cb() const;
  NodeKafka::Callbacks::OffsetCommit *offset_commit_cb() const;
  NodeKafka::Callbacks::OAuthBearerTokenRefresh *oauthbearer_token_refresh_cb()
      const;

  // NOTE: Do NOT add any members to this class.
  // Internally, to get an instance of this class, we just cast RdKafka::Conf*
  // that we obtain from RdKafka::Conf::create(). However, that's internally an
  // instance of a sub-class, ConfImpl. This means that any members here are
  // aliased to that with the wrong name (for example, the first member of this
  // class, if it's a pointer, will be aliased to consume_cb_ in the ConfImpl,
  // and and changing one will change the other!)
  // TODO: Just don't inherit from RdKafka::Conf, and instead have a member of
  // type RdKafka::Conf*.
};

}  // namespace NodeKafka

#endif  // SRC_CONFIG_H_
