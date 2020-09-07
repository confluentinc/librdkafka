/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020, Magnus Edenhill
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
#include <map>
#include <cstring>
#include <cstdlib>
#include "testcpp.h"

/**
 * Test KafkaConsumer close and destructor behaviour.
 */


static void do_test_consumer_close (bool do_subscribe,
                                    bool do_close) {
  Test::Say(tostr() << _C_MAG << "[ Test C++ KafkaConsumer close " <<
            "subscribe=" << do_subscribe << ", close=" << do_close << " ]\n");

  /* Create consumer */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  Test::conf_set(conf, "test.mock.num.brokers", "1");
  Test::conf_set(conf, "group.id", "mygroup");

  std::string errstr;
  RdKafka::KafkaConsumer *c = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  if (do_subscribe) {
    std::vector<std::string> topics;
    topics.push_back("some_topic");
    RdKafka::ErrorCode err;
    if ((err = c->subscribe(topics)))
      Test::Fail("subscribe failed: " + RdKafka::err2str(err));
  }

  RdKafka::Message *msg = c->consume(500);
  if (msg)
    delete msg;

  RdKafka::ErrorCode err;
  if (do_close) {
    if ((err = c->close()))
      Test::Fail("close failed: " + RdKafka::err2str(err));

    /* A second call should fail */
    if ((err = c->close()) != RdKafka::ERR__DESTROY)
      Test::Fail("Expected second close to fail with DESTROY, not " +
                 RdKafka::err2str(err));
  }

  /* Call an async method that will do nothing but verify that we're not
   * crashing due to use-after-free. */
  if ((err = c->commitAsync()))
    Test::Fail("Expected commitAsync close to succeed, got " +
               RdKafka::err2str(err));

  delete c;
}

extern "C" {
  int main_0116_kafkaconsumer_close (int argc, char **argv) {
    /* Parameters:
     *  subscribe, close */
    do_test_consumer_close(true, true);
    do_test_consumer_close(true, false);
    do_test_consumer_close(false, true);
    do_test_consumer_close(false, false);

    return 0;
  }
}
