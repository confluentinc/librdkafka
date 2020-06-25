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
#include <assert.h>
#include "testcpp.h"
#include <fstream>
using namespace std;



/** Incremental assign, then assign(NULL).
 */
static void assign_test_1 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::ErrorCode err;
  RdKafka::Error *error;
  std::vector<RdKafka::TopicPartition *> assignment;

  Test::Say("Incremental assign, then assign(NULL)\n");

  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
  if ((error = consumer->incremental_assign(toppars1))) {
    Test::Fail(tostr() << "Incremental assign failed: " << error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment)))
    Test::Fail(tostr() << "Failed to get current assignment: " << RdKafka::err2str(err));
  if (assignment.size() != 1)
    Test::Fail(tostr() << "Expecting current assignment to have size 1, not: " << assignment.size());
  delete assignment[0];
  assignment.clear();
  if ((err = consumer->unassign())) Test::Fail("Unassign failed: " + RdKafka::err2str(err));
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
}


/** Assign, then incremental unassign.
 */
static void assign_test_2 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::ErrorCode err;
  RdKafka::Error *error;
  std::vector<RdKafka::TopicPartition *> assignment;

  Test::Say("Assign, then incremental unassign\n");

  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
  if ((err = consumer->assign(toppars1))) Test::Fail("Assign failed: " + RdKafka::err2str(err));
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 1)
    Test::Fail(tostr() << "Expecting current assignment to have size 1, not: " << assignment.size());
  delete assignment[0];
  assignment.clear();
  if ((error = consumer->incremental_unassign(toppars1))) {
    Test::Fail("Incremental unassign failed: " + error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
}


/** Incremental assign, then incremental unassign.
 */
static void assign_test_3 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::ErrorCode err;
  RdKafka::Error *error;
  std::vector<RdKafka::TopicPartition *> assignment;

  Test::Say("Incremental assign, then incremental unassign\n");

  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
  if ((error = consumer->incremental_assign(toppars1))) {
    Test::Fail("Incremental assign failed: " + error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 1)
    Test::Fail(tostr() << "Expecting current assignment to have size 1, not: " << assignment.size());
  delete assignment[0];
  assignment.clear();
  if ((error = consumer->incremental_unassign(toppars1))) {
    Test::Fail("Incremental unassign failed: " + error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
}


/** Multi-topic incremental assign and unassign + message consumption.
 */
static void assign_test_4 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  std::vector<RdKafka::TopicPartition *> assignment;

  Test::Say("Multi-topic incremental assign and unassign + message consumption\n");

  consumer->incremental_assign(toppars1);
  consumer->assignment(assignment);
  if (assignment.size() != 1)
    Test::Fail(tostr() << "Expecting current assignment to have size 1, not: " << assignment.size());
  delete assignment[0];
  assignment.clear();
  RdKafka::Message *m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  if (m->len() != 100)
    Test::Fail(tostr() << "Expecting msg len to be 100, not: " << m->len()); /* implies read from topic 1. */
  delete m;

  consumer->incremental_unassign(toppars1);
  consumer->assignment(assignment);
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());

  m = consumer->consume(100);
  if (m->err() != RdKafka::ERR__TIMED_OUT)
    Test::Fail("Not expecting a consumed message.");
  delete m;

  consumer->incremental_assign(toppars2);
  consumer->assignment(assignment);
  if (assignment.size() != 1)
    Test::Fail(tostr() << "Expecting current assignment to have size 1, not: " << assignment.size());
  delete assignment[0];
  assignment.clear();
  m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  if (m->len() != 200)
    Test::Fail(tostr() << "Expecting msg len to be 200, not: " << m->len()); /* implies read from topic 2. */
  delete m;

  consumer->incremental_assign(toppars1);
  consumer->assignment(assignment);
  if (assignment.size() != 2)
    Test::Fail(tostr() << "Expecting current assignment to have size 2, not: " << assignment.size());
  delete assignment[0];
  delete assignment[1];
  assignment.clear();

  m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  delete m;

  consumer->incremental_unassign(toppars2);
  consumer->incremental_unassign(toppars1);
  consumer->assignment(assignment);
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0. not: " << assignment.size());
}


/** Incremental assign and unassign of empty collection.
 */
static void assign_test_5 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::ErrorCode err;
  RdKafka::Error *error;
  std::vector<RdKafka::TopicPartition *> assignment;
  std::vector<RdKafka::TopicPartition *> toppars3;

  Test::Say("Incremental assign and unassign of empty collection\n");

  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
  if ((error = consumer->incremental_assign(toppars3))) {
    Test::Fail("Incremental assign failed: " + error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
  if ((error = consumer->incremental_unassign(toppars3))) {
    Test::Fail("Incremental unassign failed: " + error->str());
    delete error;
  }
  if ((err = consumer->assignment(assignment))) Test::Fail("Failed to get current assignment: " + RdKafka::err2str(err));
  if (assignment.size() != 0)
    Test::Fail(tostr() << "Expecting current assignment to have size 0, not: " << assignment.size());
}


static void run_test (std::string &t1, std::string &t2,
                      void (*test)(RdKafka::KafkaConsumer *consumer,
                                   std::vector<RdKafka::TopicPartition *> toppars1,
                                   std::vector<RdKafka::TopicPartition *> toppars2)) {
    std::vector<RdKafka::TopicPartition *> toppars1;
    toppars1.push_back(RdKafka::TopicPartition::create(t1, 0,
                                                       RdKafka::Topic::OFFSET_BEGINNING));
    std::vector<RdKafka::TopicPartition *> toppars2;
    toppars2.push_back(RdKafka::TopicPartition::create(t2, 0,
                                                       RdKafka::Topic::OFFSET_BEGINNING));

    RdKafka::Conf *conf;
    Test::conf_init(&conf, NULL, 20);
    Test::conf_set(conf, "group.id", t1); // just reuse a (random) topic name as the group name.
    Test::conf_set(conf, "auto.offset.reset", "earliest");
    std::string bootstraps;
    if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
      Test::Fail("Failed to retrieve bootstrap.servers");
    std::string errstr;
    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer)
      Test::Fail("Failed to create KafkaConsumer: " + errstr);
    delete conf;

    test(consumer, toppars1, toppars2);

    delete toppars1[0];
    delete toppars2[0];

    consumer->close();
    delete consumer;
}


static void assign_tests () {
    int msgcnt = 1000;
    const int msgsize1 = 100;
    const int msgsize2 = 200;

    std::string topic1_str = Test::mk_topic_name("0113-cooperative_rebalance", 1);
    test_create_topic(NULL, topic1_str.c_str(), 1, 1);
    test_produce_msgs_easy_size(topic1_str.c_str(), 0, 0, msgcnt, msgsize1);

    std::string topic2_str = Test::mk_topic_name("0113-cooperative_rebalance", 1);
    test_create_topic(NULL, topic2_str.c_str(), 1, 1);
    test_produce_msgs_easy_size(topic2_str.c_str(), 0, 0, msgcnt, msgsize2);

    run_test(topic1_str, topic2_str, assign_test_1);
    run_test(topic1_str, topic2_str, assign_test_2);
    run_test(topic1_str, topic2_str, assign_test_3);
    run_test(topic1_str, topic2_str, assign_test_4);
    run_test(topic1_str, topic2_str, assign_test_5);
}




class TestRebalanceCb : public RdKafka::RebalanceCb {

private:
  static std::string part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions) {
    ostringstream ss;
    for (unsigned int i = 0 ; i < partitions.size() ; i++)
      ss << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
    ss << "\n";
    return ss.str();
  }

public:
  int assign_call_cnt;
  int revoke_call_cnt;
  int partitions_assigned_net;

  TestRebalanceCb () {
    assign_call_cnt = 0;
    revoke_call_cnt = 0;
    partitions_assigned_net = 0;
  }

  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		                 RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    Test::Say(tostr() << "RebalanceCb: " << consumer->name() << " " << RdKafka::err2str(err) << ": " << part_list_print(partitions) << "\n");
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->incremental_assign(partitions);
      assign_call_cnt += 1;
      partitions_assigned_net += partitions.size();
    } else {
      if (consumer->assignment_lost())
        Test::Fail("Not expecting lost assignment");
      consumer->incremental_unassign(partitions);
      revoke_call_cnt += 1;
      partitions_assigned_net -= partitions.size();
    }
  }
};


static void subscribe_with_cb_test () {
  /* construct test topic (2 partitions) */
  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name);

  int test_timeout_s = 120;
  std::string bootstraps;
  std::string errstr;

  /* Create consumer 1 */
  TestRebalanceCb rebalance_cb1;
  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, test_timeout_s);
  Test::conf_set(conf, "client.id", "C_1");
  Test::conf_set(conf, "group.id", topic_name.c_str()); /* just reuse a (random) topic name as the group name. */
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");
  if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to retrieve bootstrap.servers");
  if (conf->set("rebalance_cb", &rebalance_cb1, errstr))
    Test::Fail("Failed to set rebalance_cb: " + errstr);
  RdKafka::KafkaConsumer *c1 = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c1)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  /* Create consumer 2 */
  TestRebalanceCb rebalance_cb2;
  Test::conf_init(&conf, NULL, test_timeout_s);
  Test::conf_set(conf, "client.id", "C_2");
  Test::conf_set(conf, "group.id", topic_name.c_str()); /* same group as c1 */
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");
  if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to retrieve bootstrap.servers");
  if (conf->set("rebalance_cb", &rebalance_cb2, errstr))
    Test::Fail("Failed to set rebalance_cb: " + errstr);
  RdKafka::KafkaConsumer *c2 = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c2)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("consumer 1 subscribe failed: " + RdKafka::err2str(err));

  bool c2_subscribed = false;
  while (true) {
    RdKafka::Message *msg1 = c1->consume(100);
    RdKafka::Message *msg2 = c2->consume(100);
    delete msg1;
    delete msg2;

    /* failure case: test will time out. */
    if (rebalance_cb1.assign_call_cnt == 3 &&
        rebalance_cb2.assign_call_cnt == 2) {
      break;
    }

    /* start c2 after c1 has received initial assignment */
    if (!c2_subscribed && rebalance_cb1.assign_call_cnt > 0) {
      if ((err = c2->subscribe(topics)))
        Test::Fail("consumer 2 subscribe failed: " + RdKafka::err2str(err));
      c2_subscribed = true;
    }
  }

  /**
   * Sequence of events:
   *
   * 1. c1 joins group.
   * 2. c1 gets assigned 2 partitions.
   *     - there isn't a follow-on rebalance because there aren't any revoked partitions.
   * 3. c2 joins group.
   * 4. This results in a rebalance where one partition being revoked from c1, and no
   *    partitions assigned to either c1 or c2 (however the rebalance callback will be
   *    called in each case with an empty set).
   * 5. c1 then re-joins the group since it had a partition revoked. 
   * 6. c2 is now assigned a single partition, and c1's incremental assignment is empty.
   * 7. Since there were no revoked partitions, no further rebalance is triggered.
   */

  /* The rebalance cb is always called on assign, even if empty. */
  if (rebalance_cb1.assign_call_cnt != 3)
    Test::Fail(tostr() << "Expecting 3 assign calls on consumer 1, not " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expecting 2 assign calls on consumer 2, not: " << rebalance_cb2.assign_call_cnt);

  /* The rebalance cb is not called on and empty revoke (unless partitions lost) */
  if (rebalance_cb1.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expecting 1 revoke call on consumer 1, not: " << rebalance_cb1.revoke_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 0)
    Test::Fail(tostr() << "Expecting 0 revoke calls on consumer 2, not: " << rebalance_cb2.revoke_call_cnt);

  /* Final state */
  if (rebalance_cb1.partitions_assigned_net != 1)
    Test::Fail(tostr() << "Expecting consumer 1 to have net 1 assigned partition, not: " << rebalance_cb1.partitions_assigned_net);
  if (rebalance_cb2.partitions_assigned_net != 1)
    Test::Fail(tostr() << "Expecting consumer 2 to have net 1 assigned partition, not: " << rebalance_cb2.partitions_assigned_net);
  std::vector<RdKafka::TopicPartition*> partitions;
  c1->assignment(partitions);
  if (partitions.size() != 1)
    Test::Fail(tostr() << "Expecting consumer 1 to have 1 assigned partition, not: " << partitions.size());
  for (size_t i = 0; i<partitions.size(); i++)
    delete partitions[i];
  partitions.clear();
  c2->assignment(partitions);
  if (partitions.size() != 1)
    Test::Fail(tostr() << "Expecting consumer 2 to have 1 assigned partition, not: " << partitions.size());
  for (size_t i = 0; i<partitions.size(); i++)
    delete partitions[i];
  partitions.clear();

  /* Make sure the fetchers are running */
  int msgcnt = 100;
  const int msgsize1 = 100;
  test_produce_msgs_easy_size(topic_name.c_str(), 0, 0, msgcnt, msgsize1);
  test_produce_msgs_easy_size(topic_name.c_str(), 0, 1, msgcnt, msgsize1);

  bool consumed_from_c1 = false;
  bool consumed_from_c2 = false;
  while (true) {
    RdKafka::Message *msg1 = c1->consume(100);
    RdKafka::Message *msg2 = c2->consume(100);

    if (msg1->err() == RdKafka::ERR_NO_ERROR)
      consumed_from_c1 = true;
    if (msg1->err() == RdKafka::ERR_NO_ERROR)
      consumed_from_c2 = true;

    delete msg1;
    delete msg2;

    /* failure case: test will timeout. */
    if (consumed_from_c1 && consumed_from_c2)
      break;
  }

  c1->close();
  c2->close();

  /* Closing the consumer should trigger rebalance_cb (revoke). */
  if (rebalance_cb1.revoke_call_cnt != 2)
    Test::Fail(tostr() << "Expecting 2 revoke calls on consumer 1, not: " << rebalance_cb1.revoke_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expecting 1 revoke calls on consumer 2, not: " << rebalance_cb2.revoke_call_cnt);
  if (rebalance_cb1.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 1 to have net 0 assigned partitions, not: " << rebalance_cb1.partitions_assigned_net);
  if (rebalance_cb2.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 2 to have net 0 assigned partitions, not: " << rebalance_cb2.partitions_assigned_net);

  delete c1;
  delete c2;
}


static void subscribe_no_cb_test () {
  /* construct test topic (2 partitions) */
  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name);

  int test_timeout_s = 120;
  std::string bootstraps;
  std::string errstr;

  /* Create consumer 1 */
  TestRebalanceCb rebalance_cb1;
  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, test_timeout_s);
  Test::conf_set(conf, "client.id", "C_1");
  Test::conf_set(conf, "group.id", topic_name.c_str()); /* just reuse a (random) topic name as the group name. */
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");
  if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to retrieve bootstrap.servers");
  RdKafka::KafkaConsumer *c1 = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c1)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  /* Create consumer 2 */
  TestRebalanceCb rebalance_cb2;
  Test::conf_init(&conf, NULL, test_timeout_s);
  Test::conf_set(conf, "client.id", "C_2");
  Test::conf_set(conf, "group.id", topic_name.c_str()); /* same group as c1 */
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");
  if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to retrieve bootstrap.servers");
  RdKafka::KafkaConsumer *c2 = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c2)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  Test::Say("Subscribing consumer 1 to topic: " + topic_name + "\n");
  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("Consumer 1 subscribe failed: " + RdKafka::err2str(err));

  std::vector<RdKafka::TopicPartition*> partitions1;
  std::vector<RdKafka::TopicPartition*> partitions2;

  bool c2_subscribed = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    RdKafka::Message *msg2 = c2->consume(100);
    delete msg1;
    delete msg2;

    c1->assignment(partitions1);
    if (partitions1.size() == 2 && !c2_subscribed) {
      Test::Say("Subscribing consumer 2 to topic: " + topic_name + "\n");
      if ((err = c2->subscribe(topics)))
        Test::Fail("Consumer 2 subscribe failed: " + RdKafka::err2str(err));
      c2_subscribed = true;
    }

    if (partitions1.size() == 1) {
      c2->assignment(partitions2);
      if (partitions2.size() == 1) {
        Test::Say("Consumer 1 and 2 are both assigned to single partition.\n");
        done = true;
      }
      for (size_t i = 0; i<partitions2.size(); i++)
        delete partitions2[i];
      partitions2.clear();
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  c1->close();
  c2->close();

  delete c1;
  delete c2;
}

static void cooperative_close_empty_assignment_test () {
  // NULL is used for assign in close case, test no confusion.
}

static void unsubscribe_with_cb_test () {
}

static void unsubscribe_no_cb_test () {
}

static void lost_partitions_poll_timeout_test () {
}

static void lost_partitions_session_timeout_test() {
}

static void lost_partitions_heartbeat_unknown_member_id_test () {
}

static void lost_partitions_heartbeat_illegal_generation_id_test () {
}

static void lost_partitions_heartbeat_fenced_instance_id_test () {
}

static void rolling_upgrade_test () {
}

static void stress_test () {
  // many consumers joining / unjoining.
}

static void java_interop_test () {
  // one java consumer, one librdkafka consumer.
}

static void fetchers_test () {
  // generally concerned about fetchers stopping / starting as required.
  // test this.
}

extern "C" {
  int main_0113_cooperative_rebalance (int argc, char **argv) {
    assign_tests();
    subscribe_with_cb_test();
    subscribe_no_cb_test();
    cooperative_close_empty_assignment_test();
    unsubscribe_with_cb_test();
    unsubscribe_no_cb_test();
    lost_partitions_poll_timeout_test();
    lost_partitions_session_timeout_test();
    lost_partitions_heartbeat_unknown_member_id_test();
    lost_partitions_heartbeat_illegal_generation_id_test();
    lost_partitions_heartbeat_fenced_instance_id_test();
    rolling_upgrade_test();
    stress_test();
    java_interop_test();
    fetchers_test();
    return 0;
  }
}
