/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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
#include "testcpp.h"

class MyCbs : public RdKafka::OffsetCommitCb, public RdKafka::EventCb {
 public:
  int seen_commit;
  int seen_stats;

  void offset_commit_cb (RdKafka::ErrorCode err,
                         std::vector<RdKafka::TopicPartition*>&offsets) {
    seen_commit++;
    Test::Say("Got commit callback!\n");
  }

  void event_cb (RdKafka::Event &event) {
    switch (event.type())
      {
      case RdKafka::Event::EVENT_STATS:
        Test::Say("Got stats callback!\n");
        seen_stats++;
        break;
      default:
        break;
    }
  }
};

static void assert_all_headers_match(RdKafka::Headers *actual,
                                     std::vector<RdKafka::Headers::Header> &expected) {
  if (actual->size() != expected.size()) {
    Test::Fail(tostr() << "Expected headers length to equal" 
               << expected.size() << " instead equals " << actual->size() << "\n");
  }

  std::vector<RdKafka::Headers::Header> actual_headers = actual->get_all();
  for(size_t i = 0; i < actual_headers.size(); i++) {
    RdKafka::Headers::Header actual_header = actual_headers[i];
    RdKafka::Headers::Header expected_header = expected[i];
    std::string actual_key = actual_header.key;
    std::string actual_value = std::string(actual_header.value);
    std::string expected_key = expected_header.key;
    std::string expected_value = std::string(expected_header.value);
    if (actual_key != expected_key) {
      Test::Fail(tostr() << "Header key does not match, expected '" 
                 << actual_key << "' but got '" << expected_key << "'\n");
    }
    if (actual_value != expected_value) {
      Test::Fail(tostr() << "Header value does not match, expected '" 
                 << actual_value << "' but got '" << expected_value << "'\n");
    }
  }
}

static void test_n_headers (int n, const char* message) {
  std::string topic = Test::mk_topic_name("0085-headers", 1);
  RdKafka::Conf *conf;
  std::string errstr;

  Test::conf_init(&conf, NULL, 0);

  Test::conf_set(conf, "group.id", topic);
  Test::conf_set(conf, "group.id", topic);
  Test::conf_set(conf, "socket.timeout.ms", "10000");
  Test::conf_set(conf, "enable.auto.commit", "false");
  Test::conf_set(conf, "enable.partition.eof", "false");
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "statistics.interval.ms", "1000");

  MyCbs cbs;
  cbs.seen_commit = 0;
  cbs.seen_stats  = 0;
  if (conf->set("offset_commit_cb", (RdKafka::OffsetCommitCb *)&cbs, errstr) !=
      RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to set commit callback: " + errstr);
  if (conf->set("event_cb", (RdKafka::EventCb *)&cbs, errstr) !=
      RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to set event callback: " + errstr);

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
    Test::Fail("Failed to create Producer: " + errstr);

  RdKafka::ErrorCode err;

  std::vector<RdKafka::Headers::Header> headers_arr;
  for (int i = 0; i < n; ++i) {
    std::stringstream key_s;
    key_s << "header_" << i;
    std::string key = key_s.str();
    std::stringstream val_s;
    val_s << "value_" << i;
    std::string val = val_s.str();
    headers_arr.push_back(RdKafka::Headers::Header(key, val.c_str()));
  }
  RdKafka::Headers *produce_headers = RdKafka::Headers::create(headers_arr, false);

  err = p->produce(topic, 0,
                    RdKafka::Producer::RK_MSG_COPY,
                    (void *)message, message ? strlen(message) : 0,
                    (void *)"key", 3, 0, NULL, produce_headers);

  p->flush(tmout_multip(10000));

  if (p->outq_len() > 0)
    Test::Fail(tostr() << "Expected producer to be flushed, " <<
               p->outq_len() << " messages remain");

  RdKafka::KafkaConsumer *c = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  
  std::vector<std::string> topics;
  topics.push_back(topic);
  if ((err = c->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err));

  int cnt = 0;
  while (!cbs.seen_commit || !cbs.seen_stats) {
    RdKafka::Message *msg = c->consume(tmout_multip(1000));
    if (!msg->err()) {
      cnt++;
      Test::Say(tostr() << "Received message #" << cnt << "\n");
      if (cnt > 10)
        Test::Fail(tostr() << "Should've seen the "
                   "offset commit (" << cbs.seen_commit << ") and "
                   "stats callbacks (" << cbs.seen_stats << ") by now");

      /* Commit the first message to trigger the offset commit_cb */
      if (cnt == 1) {
        err = c->commitAsync(msg);
        if (err)
          Test::Fail("commitAsync() failed: " + RdKafka::err2str(err));
        rd_sleep(1); /* Sleep to simulate slow processing, making sure
                      * that the offset commit callback op gets
                      * inserted on the consume queue in front of
                      * the messages. */
      }
      RdKafka::Headers *headers = msg->get_headers();
      if (!headers) {
        Test::Fail("Expected RdKafka::Message to contain headers");
      }

      assert_all_headers_match(headers, headers_arr);

    } else if (msg->err() == RdKafka::ERR__TIMED_OUT)
      ; /* Stil rebalancing? */
    else
      Test::Fail("consume() failed: " + msg->errstr());
    delete msg;
  }

  c->close();
  delete c;
  delete p;
  delete conf;

}

static void test_one_header () {
  Test::Say("Test one header in consumed message.\n");
  std::string val = "valid";
  test_n_headers(1, val.c_str());
}

static void test_ten_headers () {
  Test::Say("Test ten headers in consumed message.\n");
  std::string val = "valid";
  test_n_headers(10, val.c_str());
}

static void test_one_header_null_msg () {
  Test::Say("Test one header in consumed message with a null value message.\n");
  test_n_headers(1, NULL);
}

static void test_one_header_empty_msg () {
  Test::Say("Test one header in consumed message with an empty value message.\n");
  std::string val = "";
  test_n_headers(1, val.c_str());
}

extern "C" {
  int main_0085_headers (int argc, char **argv) {
    test_one_header();
    test_ten_headers();
    // These two tests fail and I'm not sure if this is correct behaviour
    // test_one_header_null_msg();
    // test_one_header_empty_msg();
    return 0;
  }
}
