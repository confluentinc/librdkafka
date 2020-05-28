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
 * The beginnings of an integration test for cooperative rebalancing.
 * MH: what i'm currently using to debug with.
 */


extern "C" {
  int main_0112_cooperative_rebalance (int argc, char **argv) {

    std::string topic_str = Test::mk_topic_name("0112-cooperative_rebalance", 1);

    /* Create consumer 1 */
    RdKafka::Conf *conf;
    Test::conf_init(&conf, NULL, 20);
    Test::conf_set(conf, "group.id", topic_str);
    std::string bootstraps;
    if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
      Test::Fail("Failed to retrieve bootstrap.servers");
    std::string errstr;
    RdKafka::KafkaConsumer *c1 = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!c1)
      Test::Fail("Failed to create KafkaConsumer: " + errstr);
    delete conf;

    /* Create consumer 2 */
    Test::conf_init(&conf, NULL, 20);
    Test::conf_set(conf, "group.id", topic_str);
    if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
      Test::Fail("Failed to retrieve bootstrap.servers");
    RdKafka::KafkaConsumer *c2 = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!c2)
      Test::Fail("Failed to create KafkaConsumer: " + errstr);
    delete conf;

    /* Create topics */
    Test::create_topic(c1, topic_str.c_str(), 1, 1);

    /*
    * Consumer #1 subscribe
    */
    std::vector<std::string> topics;
    topics.push_back(topic_str);
    RdKafka::ErrorCode err;
    if ((err = c1->subscribe(topics)))
      Test::Fail("consumer 1 subscribe failed: " + RdKafka::err2str(err));

    /* Start consuming until EOF is reached, which indicates that we have an
     * assignment and any errors should have been reported. */
    bool run = true;
    int cnt = 0;
    while (run) {
      RdKafka::Message *msg = c1->consume(tmout_multip(1000));
      cnt += 1;
      if (cnt == 5) {
        /*
          * Consumer #2 subscribe
          */
        if ((err = c2->subscribe(topics)))
          Test::Fail("consumer 2 subscribe failed: " + RdKafka::err2str(err));
      }
      switch (msg->err())
        {
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR_NO_ERROR:
        default:
          run = false;
          break;
        }
    }

    c1->close();
    delete c1;

    c2->close();
    delete c2;

    return 0;
  }
}
