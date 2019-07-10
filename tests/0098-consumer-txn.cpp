/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2016, Magnus Edenhill
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
#include <cstring>
#include <cstdlib>
#include "testcpp.h"
#include <assert.h>
#include <sstream>


/**
 * @name Consumer Transactions.
 *
 * - Uses the TransactionProducerCli Java application to produce messages
 *   that are part of abort and commit transactions in various combinations
 *   and tests that librdkafka consumes them as expected. Refer to 
 *   TransactionProducerCli.java for scenarios covered.
 */


static void test_assert(bool cond, std::string msg) {
  if (!cond)
    Test::Say(msg);
  assert(cond);
}


static void execute_java_produce_cli(std::string &bootstrapServers, std::string &topic, std::string cmd) {
  std::stringstream ss;
  ss << "./java/run-class.sh TransactionProducerCli " + bootstrapServers + " " + topic + " " + cmd;
  int status = system(ss.str().c_str());
  test_assert(!status, "./java/run-class.sh TransactionProducerCli failed with error code: " + status);
}

static std::vector<RdKafka::Message *> consume_messages(RdKafka::KafkaConsumer *c, std::string topic, int partition) {
  RdKafka::ErrorCode err;

  /* Assign partitions */
  std::vector<RdKafka::TopicPartition*> parts;
  parts.push_back(RdKafka::TopicPartition::create(topic, partition));
  if ((err = c->assign(parts)))
    Test::Fail("assign failed: " + RdKafka::err2str(err));
  RdKafka::TopicPartition::destroy(parts);

  Test::Say("Consuming from topic " + topic + "\n");  
  std::vector<RdKafka::Message *> result = std::vector<RdKafka::Message *>();

  while (true) {
    RdKafka::Message *msg = c->consume(tmout_multip(1000));
    switch (msg->err())
    {
      case RdKafka::ERR__TIMED_OUT:
        continue;
      case RdKafka::ERR__PARTITION_EOF:
        break;
      case RdKafka::ERR_NO_ERROR:
        result.push_back(msg);
        continue;
      default:
        Test::Fail("Error consuming from topic " + topic + ": " + msg->errstr());
        break;
    }
    break;
  }

  Test::Say("Read all messages from topic: " + topic + "\n");

  return result;
}


static void delete_messages(std::vector<RdKafka::Message *> &messages) {
  for (size_t i=0; i<messages.size(); ++i)
    delete messages[i];
}


static std::string get_bootstrap_servers() {
  RdKafka::Conf *conf;
  std::string bootstrap_servers;
  Test::conf_init(&conf, NULL, 40);
  conf->get("bootstrap.servers", bootstrap_servers);
  delete conf;
  return bootstrap_servers;
}


static RdKafka::KafkaConsumer *create_consumer(
    std::string &topic_name,
    const char *isolation_level) {
  RdKafka::Conf *conf;
  std::string errstr;

  Test::conf_init(&conf, NULL, 40);
  Test::conf_set(conf, "group.id", topic_name);
  Test::conf_set(conf, "enable.auto.commit", "false");
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "enable.partition.eof", "true");
  Test::conf_set(conf, "isolation.level", isolation_level);

  RdKafka::KafkaConsumer *c = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);

  delete conf;

  return c;
}


static void do_test_consumer_txn_test (void) {
  std::string errstr;
  std::string topic_name;
  RdKafka::KafkaConsumer *c;
  std::vector<RdKafka::Message *> msgs;

  std::string bootstrap_servers = get_bootstrap_servers();
  Test::Say("bootstrap.servers: " + bootstrap_servers);


  // Test 0

  topic_name = Test::mk_topic_name("0098-consumer_txn-0", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "0");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 5, tostr() << "Consumed unexpected number of messages. Expected 5, got: " << msgs.size());
  test_assert(0 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(4 == msgs[4]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(4 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x10 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x14 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;



  // Test 0.1

  topic_name = Test::mk_topic_name("0098-consumer_txn-0.1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "0.1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 5, tostr() << "Consumed unexpected number of messages. Expected 5, got: " << msgs.size());
  test_assert(0 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(4 == msgs[4]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(4 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x10 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x14 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 0.2

  topic_name = Test::mk_topic_name("0098-consumer_txn-0.2", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "0.2");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 5, tostr() << "Consumed unexpected number of messages. Expected 5, got: " << msgs.size());
  test_assert(0x30 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x34 == msgs[4]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0x10 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x14 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x30 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x34 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;



  // Test 1

  topic_name = Test::mk_topic_name("0098-consumer_txn-1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0x10 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x14 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x50 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x54 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 1.1

  topic_name = Test::mk_topic_name("0098-consumer_txn-1.1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "1.1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0x40 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x44 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x60 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x64 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 1.2

  topic_name = Test::mk_topic_name("0098-consumer_txn-1.2", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "1.2");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 10, tostr() << "Consumed unexpected number of messages. Expected 10, got: " << msgs.size());
  test_assert(0x10 == msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x14 == msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x30 == msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x34 == msgs[9]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  
  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 2

  topic_name = Test::mk_topic_name("0098-consumer_txn-2", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "2");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 7, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  test_assert(0x20 == (unsigned char)msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x40 == (unsigned char)msgs[1]->key()->c_str()[0], "Unexpected key");
  test_assert(0x60 == (unsigned char)msgs[2]->key()->c_str()[0], "Unexpected key");
  test_assert(0x80 == (unsigned char)msgs[3]->key()->c_str()[0], "Unexpected key");
  test_assert(0xa0 == (unsigned char)msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0xb0 == (unsigned char)msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0xc0 == (unsigned char)msgs[6]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 2.1

  topic_name = Test::mk_topic_name("0098-consumer_txn-2.1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "2.1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 7, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  test_assert(0x20 == (unsigned char)msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x40 == (unsigned char)msgs[1]->key()->c_str()[0], "Unexpected key");
  test_assert(0x60 == (unsigned char)msgs[2]->key()->c_str()[0], "Unexpected key");
  test_assert(0x80 == (unsigned char)msgs[3]->key()->c_str()[0], "Unexpected key");
  test_assert(0xa0 == (unsigned char)msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0xb0 == (unsigned char)msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0xc0 == (unsigned char)msgs[6]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 12, tostr() << "Consumed unexpected number of messages. Expected 12, got: "<< msgs.size());
  test_assert(0x10 == (unsigned char)msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x20 == (unsigned char)msgs[1]->key()->c_str()[0], "Unexpected key");
  test_assert(0x30 == (unsigned char)msgs[2]->key()->c_str()[0], "Unexpected key");
  test_assert(0x40 == (unsigned char)msgs[3]->key()->c_str()[0], "Unexpected key");
  test_assert(0x50 == (unsigned char)msgs[4]->key()->c_str()[0], "Unexpected key");
  test_assert(0x60 == (unsigned char)msgs[5]->key()->c_str()[0], "Unexpected key");
  test_assert(0x70 == (unsigned char)msgs[6]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 3

  topic_name = Test::mk_topic_name("0098-consumer_txn-3", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 2, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "3");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 6, tostr() << "Consumed unexpected number of messages. Expected 6, got: " << msgs.size());
  delete_messages(msgs);
  msgs = consume_messages(c, topic_name, 1);
  test_assert(msgs.size() == 3, tostr() << "Consumed unexpected number of messages. Expected 3, got: " << msgs.size());
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 6, tostr() << "Consumed unexpected number of messages. Expected 6, got: " << msgs.size());
  delete_messages(msgs);
  msgs = consume_messages(c, topic_name, 1);
  test_assert(msgs.size() == 3, tostr() << "Consumed unexpected number of messages. Expected 3, got: " << msgs.size());
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 3.1

  topic_name = Test::mk_topic_name("0098-consumer_txn-3.1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 2, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "3.1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 2, tostr() << "Consumed unexpected number of messages. Expected 2, got: " << msgs.size());
  test_assert(0x55 == (unsigned char)msgs[0]->key()->c_str()[0], "Unexpected key");
  test_assert(0x00 == (unsigned char)msgs[1]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);
  msgs = consume_messages(c, topic_name, 1);
  test_assert(msgs.size() == 1, tostr() << "Consumed unexpected number of messages. Expected 1, got: " << msgs.size());
  test_assert(0x44 == (unsigned char)msgs[0]->key()->c_str()[0], "Unexpected key");
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 4

  topic_name = Test::mk_topic_name("0098-consumer_txn-4", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "4");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 7, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 13, tostr() << "Consumed unexpected number of messages. Expected 13, got: " << msgs.size());
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 4.1

  topic_name = Test::mk_topic_name("0098-consumer_txn-4.1", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "4.1");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 7, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 13, tostr() << "Consumed unexpected number of messages. Expected 13, got: " << msgs.size());
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 4.2

  topic_name = Test::mk_topic_name("0098-consumer_txn-4.2", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "4.2");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 13, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 13, tostr() << "Consumed unexpected number of messages. Expected 13, got: " << msgs.size());
  delete_messages(msgs);
  
  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 4.3

  topic_name = Test::mk_topic_name("0098-consumer_txn-4.3", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "4.3");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 1, tostr() << "Consumed unexpected number of messages. Expected 7, got: " << msgs.size());
  delete_messages(msgs);
  c->close();
  delete c;

  c = create_consumer(topic_name, "READ_UNCOMMITTED");
  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 13, tostr() << "Consumed unexpected number of messages. Expected 13, got: " << msgs.size());
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;


  // Test 5

  topic_name = Test::mk_topic_name("0098-consumer_txn-5", 1);
  c = create_consumer(topic_name, "READ_COMMITTED");
  Test::create_topic(c, topic_name.c_str(), 1, 3);

  execute_java_produce_cli(bootstrap_servers, topic_name, "5");

  msgs = consume_messages(c, topic_name, 0);
  test_assert(msgs.size() == 9, tostr() << "Consumed unexpected number of messages. Expected 9, got: " << msgs.size());
  delete_messages(msgs);

  Test::delete_topic(c, topic_name.c_str());

  c->close();
  delete c;
}

extern "C" {
  int main_0098_consumer_txn (int argc, char **argv) {
    do_test_consumer_txn_test();
    return 0;
  }
}
