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
extern "C" {
#include "test.h"
}

/**
 * Test consumer fetch.queue.backoff.ms behaviour.
 *
 * 1. Produce N messages, 1 message per batch.
 * 2. Configure consumer with queued.min.messages=1 and
 *    fetch.queue.backoff.ms=A|B.
 * 3. Verify that the consume() latency is <= fetch.queue.backoff.ms.
 */


static void do_test_queue_backoff(const std::string &topic, int backoff_ms) {
  SUB_TEST("backoff_ms = %d", backoff_ms);

  /* Create consumer */
  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, 60);
  Test::conf_set(conf, "group.id", topic);
  Test::conf_set(conf, "enable.auto.commit", "false");
  Test::conf_set(conf, "auto.offset.reset", "beginning");
  Test::conf_set(conf, "queued.min.messages", "1");
  Test::conf_set(conf, "fetch.queue.backoff.ms", tostr() << backoff_ms);
  Test::conf_set(conf, "fetch.message.max.bytes", "12000");

  std::string errstr;

  RdKafka::KafkaConsumer *c = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  std::vector<RdKafka::TopicPartition *> parts;
  parts.push_back(RdKafka::TopicPartition::create(topic, 0));

  RdKafka::ErrorCode err;
  if ((err = c->assign(parts)))
    Test::Fail("assigned failed: " + RdKafka::err2str(err));

  int received       = 0;
  int in_profile_cnt = 0;
  int dmax           = (int)(/*max overhead*/ 100.0 + (double)backoff_ms * 1.2);

  int64_t ts_consume = test_clock();

  while (received < 10) {
    RdKafka::Message *msg = c->consume(3000 + backoff_ms);

    rd_ts_t now     = test_clock();
    int latency     = (test_clock() - ts_consume) / 1000;
    ts_consume      = now;
    bool in_profile = latency <= dmax;

    if (!msg)
      Test::Fail(tostr() << "No message for " << (1000 + backoff_ms) << "ms");
    if (msg->err())
      Test::Fail("Unexpected consumer error: " + msg->errstr());

    Test::Say(tostr() << "Message #" << received << " consumed in " << latency
                      << "ms (expecting <= " << dmax << "ms)"
                      << (received == 0 ? ": skipping first" : "")
                      << (in_profile ? ": in profile" : ": OUT OF PROFILE")
                      << "\n");

    if (received++ > 0 && in_profile)
      in_profile_cnt++;

    delete msg;
  }

  Test::Say(tostr() << in_profile_cnt << "/" << received << " messages were "
                    << "in profile (<= " << dmax
                    << ") for backoff_ms=" << backoff_ms << "\n");

  TEST_ASSERT(
      (double)in_profile_cnt / (double)received >= (test_on_ci ? 0.1 : 0.9),
      "Only %d/%d messages were in profile", in_profile_cnt, received);

  delete c;

  SUB_TEST_PASS();
}


extern "C" {
int main_0127_fetch_queue_backoff(int argc, char **argv) {
  std::string topic = Test::mk_topic_name("0127_fetch_queue_backoff", 1);

  /* Prime the topic with messages. */
  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, 10);
  Test::conf_set(conf, "batch.num.messages", "1");
  std::string errstr;
  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
    Test::Fail(tostr() << __FUNCTION__
                       << ": Failed to create producer: " << errstr);
  delete conf;

  Test::produce_msgs(p, topic, 0, 100, 10000, true /*flush*/);
  delete p;

  do_test_queue_backoff(topic, 2000);
  do_test_queue_backoff(topic, 10);
  do_test_queue_backoff(topic, 0);
  return 0;
}
}
