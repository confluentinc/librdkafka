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

extern "C" {
#include "../src/rdkafka_proto.h"
#include "test.h"
#include "rdkafka.h"
#include "rdkafka_mock.h"
}
#include <iostream>
#include <map>
#include <cstring>
#include <cstdlib>
#include <assert.h>
#include "testcpp.h"
#include <fstream>

using namespace std;


static std::string get_bootstrap_servers() {
  RdKafka::Conf *conf;
  std::string bootstrap_servers;
  Test::conf_init(&conf, NULL, 40);
  conf->get("bootstrap.servers", bootstrap_servers);
  delete conf;
  return bootstrap_servers;
}

static RdKafka::KafkaConsumer *make_consumer (string client_id,
                                              string group_id,
                                              bool quick_metadata_refresh,
                                              string assignment_strategy,
                                              RdKafka::RebalanceCb *rebalance_cb) {

  int test_timeout_s = 120;
  std::string bootstraps;
  std::string errstr;

  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, test_timeout_s);
  Test::conf_set(conf, "client.id", client_id);
  Test::conf_set(conf, "group.id", group_id.c_str());
  if (quick_metadata_refresh)
    Test::conf_set(conf, "topic.metadata.refresh.interval.ms", "3000");
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "enable.auto.commit", "false");
  Test::conf_set(conf, "partition.assignment.strategy", assignment_strategy);
  if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to retrieve bootstrap.servers");
  if (rebalance_cb) {
    if (conf->set("rebalance_cb", rebalance_cb, errstr))
      Test::Fail("Failed to set rebalance_cb: " + errstr);
  }
  RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!consumer)
    Test::Fail("Failed to create KafkaConsumer: " + errstr);
  delete conf;

  return consumer;
}


void expect_assignment(RdKafka::KafkaConsumer *consumer, size_t count) {
  std::vector<RdKafka::TopicPartition*> partitions;
  consumer->assignment(partitions);
  if (partitions.size() != count)
    Test::Fail(tostr() << "Expecting consumer " << consumer->name() << " to have " << count << " assigned partition(s), not: " << partitions.size());
  for (size_t i = 0; i<partitions.size(); i++)
    delete partitions[i];
  partitions.clear();
}


class DefaultRebalanceCb : public RdKafka::RebalanceCb {

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
  int lost_call_cnt;
  int partitions_assigned_net;

  DefaultRebalanceCb () {
    assign_call_cnt = 0;
    revoke_call_cnt = 0;
    lost_call_cnt = 0;
    partitions_assigned_net = 0;
  }

  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		                 RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    Test::Say(tostr() << "RebalanceCb: " << consumer->name() << " " << RdKafka::err2str(err) << ": " << part_list_print(partitions) << "\n");
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      RdKafka::Error *error = consumer->incremental_assign(partitions);
      if (error)
        Test::Fail(tostr() << "consumer->incremental_assign() failed: " << error->str());
      assign_call_cnt += 1;
      partitions_assigned_net += partitions.size();
    } else {
      if (consumer->assignment_lost())
        lost_call_cnt += 1;
      RdKafka::Error *error = consumer->incremental_unassign(partitions);
      if (error)
        Test::Fail(tostr() << "consumer->incremental_unassign() failed: " << error->str());
      revoke_call_cnt += 1;
      partitions_assigned_net -= partitions.size();
    }
  }

};





/* -------- a_assign_tests
 *
 * check behavior incremental assign / unassign outside the context of a rebalance.
 */


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

    RdKafka::KafkaConsumer *consumer = make_consumer("C_1", t1, false, "cooperative-sticky", NULL);

    test(consumer, toppars1, toppars2);

    delete toppars1[0];
    delete toppars2[0];

    consumer->close();
    delete consumer;
}


static void a_assign_tests () {
    Test::Say("Executing a_assign_tests\n");

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



/* -------- b_subscribe_with_cb_test
 *
 * check behavior when:
 *   1. single topic with 2 partitions.
 *   2. consumer 1 (with rebalance_cb) subscribes to it.
 *   3. consumer 2 (with rebalance_cb) subscribes to it.
 *   4. close.
 */

static void b_subscribe_with_cb_test (rd_bool_t close_consumer) {
  Test::Say("Executing b_subscribe_with_cb_test\n");

  /* construct test topic (2 partitions) */
  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", &rebalance_cb1);
  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, false, "cooperative-sticky", &rebalance_cb2);

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
   * 4. This results in a rebalance with one partition being revoked from c1, and no
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

  /* The rebalance cb is not called on and empty revoke (unless partitions lost, which is not the case here) */
  if (rebalance_cb1.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expecting 1 revoke call on consumer 1, not: " << rebalance_cb1.revoke_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 0)
    Test::Fail(tostr() << "Expecting 0 revoke calls on consumer 2, not: " << rebalance_cb2.revoke_call_cnt);

  /* Final state */

  /* Expect both consumers to have 1 assigned partition (via net calculation in rebalance_cb) */
  if (rebalance_cb1.partitions_assigned_net != 1)
    Test::Fail(tostr() << "Expecting consumer 1 to have net 1 assigned partition, not: " << rebalance_cb1.partitions_assigned_net);
  if (rebalance_cb2.partitions_assigned_net != 1)
    Test::Fail(tostr() << "Expecting consumer 2 to have net 1 assigned partition, not: " << rebalance_cb2.partitions_assigned_net);

  /* Expect both consumers to have 1 assigned partition (via ->assignment() query) */
  expect_assignment(c1, 1);
  expect_assignment(c2, 1);

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

  if (!close_consumer) {
    delete c1;
    delete c2;
    return;
  }

  c1->close();
  c2->close();

  /* Closing the consumer should trigger rebalance_cb (revoke): */
  if (rebalance_cb1.revoke_call_cnt != 2)
    Test::Fail(tostr() << "Expecting 2 revoke calls on consumer 1, not: " << rebalance_cb1.revoke_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expecting 1 revoke calls on consumer 2, not: " << rebalance_cb2.revoke_call_cnt);

  /* and net assigned partitions should drop to 0 in both cases: */
  if (rebalance_cb1.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 1 to have net 0 assigned partitions, not: " << rebalance_cb1.partitions_assigned_net);
  if (rebalance_cb2.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 2 to have net 0 assigned partitions, not: " << rebalance_cb2.partitions_assigned_net);

  /* nothing in this test should result in lost partitions */
  if (rebalance_cb1.lost_call_cnt > 0)
    Test::Fail(tostr() << "Expecting consumer 1 to have 0 lost partition events, not: " << rebalance_cb1.lost_call_cnt);
  if (rebalance_cb2.lost_call_cnt > 0)
    Test::Fail(tostr() << "Expecting consumer 2 to have 0 lost partition events, not: " << rebalance_cb2.lost_call_cnt);

  delete c1;
  delete c2;
}



/* -------- c_subscribe_no_cb_test
 *
 * check behavior when:
 *   1. single topic with 2 partitions.
 *   2. consumer 1 (no rebalance_cb) subscribes to it.
 *   3. consumer 2 (no rebalance_cb) subscribes to it.
 *   4. close.
 */

static void c_subscribe_no_cb_test (rd_bool_t close_consumer) {
  Test::Say("Executing c_subscribe_no_cb_test\n");

  /* construct test topic (2 partitions) */
  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name);

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", NULL);
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, false, "cooperative-sticky", NULL);

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

  if (close_consumer) {
    Test::Say("Closing consumer 1.\n");
    c1->close();
    Test::Say("Closing consumer 2.\n");
    c2->close();
  } else {
    Test::Say("Skipping close() of consumer 1 and 2.\n");
  }

  delete c1;
  delete c2;
}



/* ------- d_change_subscription_add_topic
 *
 * check behavior when:
 *   1. single consumer (no rebalance_cb) subscribes to topic.
 *   2. subscription is changed (topic added).
 *   3. consumer is closed.
 */

static void d_change_subscription_add_topic (rd_bool_t close_consumer) {
  Test::Say("Executing d_change_subscription_add_topic\n");

  /* construct test topics (two partitions) */
  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 2, 1);
  std::string topic_name2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name2.c_str(), 2, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", NULL);

  Test::Say("Subscribing to one topic\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics_1;
  topics_1.push_back(topic_name1);
  if ((err = c1->subscribe(topics_1)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool subscribed_to_one_topic = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);

    if (partitions1.size() == 2 && !subscribed_to_one_topic) {
      subscribed_to_one_topic = true;
      Test::Say("Subscribing to two topics\n");
      std::vector<std::string> topics_2;
      topics_2.push_back(topic_name1);
      topics_2.push_back(topic_name2);
      if ((err = c1->subscribe(topics_2)))
        Test::Fail("Subscribe to two topics failed: " + RdKafka::err2str(err) + "\n");
    }

    if (partitions1.size() == 4) {
      Test::Say("Consumer is assigned to two topics.\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  if (close_consumer) {
    Test::Say("Closing consumer.\n");
    c1->close();
  } else {
    Test::Say("Skipping close() of consumer 1.\n");
  }

  delete c1;
}



/* ------- e_change_subscription_remove_topic
 *
 * check behavior when:
 *   1. single consumer (no rebalance_cb) subscribes to topic.
 *   2. subscription is changed (topic added).
 *   3. consumer is closed.
 */

static void e_change_subscription_remove_topic (rd_bool_t close_consumer) {
  Test::Say("Executing e_change_subscription_remove_topic\n");

  /* construct test topics (two partitions) */
  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 2, 1);
  std::string topic_name2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name2.c_str(), 2, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", NULL);

  Test::Say("Subscribing to two topics\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics_2;
  topics_2.push_back(topic_name1);
  topics_2.push_back(topic_name2);
  if ((err = c1->subscribe(topics_2)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool subscribed_to_two_topics = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);
    Test::Say(tostr() << "Current assignment size: " << partitions1.size() << "\n");

    if (partitions1.size() == 4 && !subscribed_to_two_topics) {
      subscribed_to_two_topics = true;
      Test::Say("Subscribing to one topic\n");
      std::vector<std::string> topics_1;
      topics_1.push_back(topic_name1);
      if ((err = c1->subscribe(topics_1)))
        Test::Fail("Subscribe to one topics failed: " + RdKafka::err2str(err) + "\n");
    }

    if (partitions1.size() == 2) {
      Test::Say("Consumer is assigned to one topic\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  if (!close_consumer) {
    Test::Say("Closing consumer.\n");
    c1->close();
  } else {
    Test::Say("Skipping close() of consumer 1.\n");
  }

  delete c1;
}



/* ------- f_assign_call_cooperative
 *
 * check that use of consumer->assign() and consumer->unassign() is disallowed when a
 * COOPERATIVE assignor is in use.
 */

class FTestRebalanceCb : public RdKafka::RebalanceCb {
public:
  int assigned;

  FTestRebalanceCb () {
    assigned = 0;
  }

  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		                 RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    Test::Say(tostr() << "RebalanceCb: " << consumer->name() << " " << RdKafka::err2str(err) << "\n");

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      RdKafka::ErrorCode err_resp = consumer->assign(partitions);
      Test::Say(tostr() << "consumer->assign() response code: " << err_resp << "\n");
      if (err_resp != RdKafka::ERR__STATE)
        Test::Fail(tostr() << "Expected assign to fail with error code: " << RdKafka::ERR__STATE << "(ERR__STATE)");

      RdKafka::Error *error = consumer->incremental_assign(partitions);
      if (error)
        Test::Fail(tostr() << "consumer->incremental_unassign() failed: " << error->str());

      assigned = 1;

    } else {
      RdKafka::ErrorCode err_resp = consumer->unassign();
      Test::Say(tostr() << "consumer->unassign() response code: " << err_resp << "\n");
      if (err_resp != RdKafka::ERR__STATE)
        Test::Fail(tostr() << "Expected assign to fail with error code: " << RdKafka::ERR__STATE << "(ERR__STATE)");

      RdKafka::Error *error = consumer->incremental_unassign(partitions);
      if (error)
        Test::Fail(tostr() << "consumer->incremental_unassign() failed: " << error->str());
    }
  }
};

static void f_assign_call_cooperative () {
  Test::Say("Executing f_assign_call_cooperative\n");

  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  FTestRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", &rebalance_cb);

  Test::Say(tostr() << "Subscribing to " << topic_name1 << "\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics;
  topics.push_back(topic_name1);
  if ((err = c1->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  while (!rebalance_cb.assigned) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;
  }

  c1->close();
  delete c1;
}



/* ------- g_incremental_assign_call_eager
 *
 * check that use of consumer->incremental_assign() and consumer->incremental_unassign() is
 * disallowed when an EAGER assignor is in use.
 */
class GTestRebalanceCb : public RdKafka::RebalanceCb {
public:
  int assigned;

  GTestRebalanceCb () {
    assigned = 0;
  }

  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		                 RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    Test::Say(tostr() << "RebalanceCb: " << consumer->name() << " " << RdKafka::err2str(err) << "\n");

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      RdKafka::Error *error = consumer->incremental_assign(partitions);
      Test::Say(tostr() << "consumer->incremental_assign() response: " << (!error ? "NULL" : error->str()) << "\n");
      if (!error)
        Test::Fail("Expected consumer->incremental_assign() to fail");
      if (error->code() != RdKafka::ERR__STATE)
        Test::Fail(tostr() << "Expected consumer->incremental_assign() to fail with error code " << RdKafka::ERR__STATE);
      delete error;

      RdKafka::ErrorCode err_resp = consumer->assign(partitions);
      if (err_resp)
        Test::Fail(tostr() << "consumer->assign() failed: " << err_resp);

      assigned = 1;

    } else {
      RdKafka::Error *error = consumer->incremental_unassign(partitions);
      Test::Say(tostr() << "consumer->incremental_unassign() response: " << (!error ? "NULL" : error->str()) << "\n");
      if (!error)
        Test::Fail("Expected consumer->incremental_unassign() to fail");
      if (error->code() != RdKafka::ERR__STATE)
        Test::Fail(tostr() << "Expected consumer->incremental_unassign() to fail with error code " << RdKafka::ERR__STATE);
      delete error;

      RdKafka::ErrorCode err_resp = consumer->unassign();
      if (err_resp)
        Test::Fail(tostr() << "consumer->unassign() failed: " << err_resp);
    }
  }
};

static void g_incremental_assign_call_eager() {
  Test::Say("Executing g_incremental_assign_call_eager\n");

  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  GTestRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "roundrobin", &rebalance_cb);

  Test::Say(tostr() << "Subscribing to " << topic_name1 << "\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics;
  topics.push_back(topic_name1);
  if ((err = c1->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  while (!rebalance_cb.assigned) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;
  }

  c1->close();
  delete c1;
}


/* ------- h_delete_topic
 *
 * check behavior when:
 *   1. single consumer (rebalance_cb) subscribes to two topics.
 *   2. one of the topics is deleted.
 *   3. consumer is closed.
 */

static void h_delete_topic () {
  Test::Say("Executing h_delete_topic\n");

  /* construct test two topics (one partitions) */
  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);
  std::string topic_name2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name2.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", &rebalance_cb1);

  Test::Say("Subscribing\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics;
  topics.push_back(topic_name1);
  topics.push_back(topic_name2);
  if ((err = c1->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool deleted = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);
    Test::Say(tostr() << "Current assignment size: " << partitions1.size() << "\n");

    if (partitions1.size() == 2 && !deleted) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected 1 assign call, saw " << rebalance_cb1.assign_call_cnt << "\n");
      Test::delete_topic(c1, topic_name2.c_str());
      deleted = true;
    }

    if (partitions1.size() == 1 && deleted) {
      if (partitions1[0]->topic() != topic_name1)
        Test::Fail(tostr() << "Expecting subscribed topic to be '" << topic_name1 << "' not '" << partitions1[0]->topic() << "'");
      Test::Say(tostr() << "Assignment no longer includes deleted topic '" << topic_name2 << "'\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer\n");
  c1->close();

  delete c1;
}



/* ------- i_delete_topic_2
 *
 * check behavior when:
 *   1. single consumer (rebalance_cb) subscribes to a single topic.
 *   2. that topic is deleted leaving no topics.
 *   3. consumer is closed.
 */

static void i_delete_topic_2 () {
  Test::Say("Executing i_delete_topic_2\n");

  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", &rebalance_cb1);

  Test::Say("Subscribing\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics;
  topics.push_back(topic_name1);
  if ((err = c1->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool deleted = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);
    Test::Say(tostr() << "Current assignment size: " << partitions1.size() << "\n");

    if (partitions1.size() == 1 && !deleted) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected one assign call, saw " << rebalance_cb1.assign_call_cnt << "\n");
      Test::delete_topic(c1, topic_name1.c_str());
      deleted = true;
    }

    if (partitions1.size() == 0 && deleted) {
      Test::Say(tostr() << "Assignment is empty following deletion of topic\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer.\n");
  c1->close();

  delete c1;
}



/* ------- j_delete_topic_no_rb_callback
 *
 * check behavior when:
 *   1. single consumer (without rebalance_cb) subscribes to a single topic.
 *   2. that topic is deleted leaving no topics.
 *   3. consumer is closed.
 */

static void j_delete_topic_no_rb_callback () {
  Test::Say("Executing j_delete_topic_no_rb_callback\n");

  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", NULL);

  Test::Say("Subscribing\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics;
  topics.push_back(topic_name1);
  if ((err = c1->subscribe(topics)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool deleted = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);
    Test::Say(tostr() << "Current assignment size: " << partitions1.size() << "\n");

    if (partitions1.size() == 1 && !deleted) {
      Test::delete_topic(c1, topic_name1.c_str());
      deleted = true;
    }

    if (partitions1.size() == 0 && deleted) {
      Test::Say(tostr() << "Assignment is empty following deletion of topic\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer.\n");
  c1->close();

  delete c1;
}



/* ------- k_add_partition
 *
 * check behavior when:
 *   1. single consumer (rebalance_cb) subscribes to a 1 partition topic.
 *   2. number of partitions is increased to 2.
 *   3. consumer is closed.
 */

static void k_add_partition () {
  Test::Say("Executing k_add_partition\n");

  /* construct test topics (one partition) */
  std::string topic_name1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", &rebalance_cb1);

  Test::Say("Subscribing\n");
  RdKafka::ErrorCode err;
  std::vector<std::string> topics_1;
  topics_1.push_back(topic_name1);
  if ((err = c1->subscribe(topics_1)))
    Test::Fail("subscribe failed: " + RdKafka::err2str(err) + "\n");

  bool subscribed = false;
  bool done = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(1000);
    delete msg1;

    std::vector<RdKafka::TopicPartition*> partitions1;
    c1->assignment(partitions1);
    Test::Say(tostr() << "Current assignment size: " << partitions1.size() << "\n");

    if (partitions1.size() == 1 && !subscribed) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected 1 assign call, saw " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb1.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expected 0 revoke calls, saw " << rebalance_cb1.revoke_call_cnt);
      Test::create_partitions(c1, topic_name1.c_str(), 2);
      subscribed = true;
    }

    if (partitions1.size() == 2 && subscribed) {
      if (rebalance_cb1.assign_call_cnt != 2)
        Test::Fail(tostr() << "Expected 2 assign calls, saw " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb1.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expected 0 revoke calls, saw " << rebalance_cb1.revoke_call_cnt);
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer.\n");
  c1->close();

  if (rebalance_cb1.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expected 2 assign calls, saw " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb1.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expected 1 revoke call, saw " << rebalance_cb1.revoke_call_cnt);

  delete c1;
}


/* ------- l_unsubscribe
 *
 * check behavior when:
 *   1. two consumers (with rebalance_cb's) subscribe to two topics.
 *   2. one of the consumers calls unsubscribe.
 *   3. consumers closed.
 */

static void l_unsubscribe () {
  Test::Say("Executing l_unsubscribe\n");

  /* construct test topic (2 partitions) */
  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  test_create_topic(NULL, topic_name_2.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name_1);
  topics.push_back(topic_name_2);

  /* hack: wait for a bit to have better certainty the topics created above exist. */
  sleep(3);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", &rebalance_cb1);
  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, false, "cooperative-sticky", &rebalance_cb2);

  Test::Say("Subscribing consumer 1 to both topics\n");
  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("Consumer 1 subscribe failed: " + RdKafka::err2str(err));

  Test::Say("Subscribing consumer 2 to both topics\n");
  if ((err = c2->subscribe(topics)))
    Test::Fail("Consumer 2 subscribe failed: " + RdKafka::err2str(err));

  std::vector<RdKafka::TopicPartition*> partitions1;
  std::vector<RdKafka::TopicPartition*> partitions2;

  bool done = false;
  bool unsubscribed = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    RdKafka::Message *msg2 = c2->consume(100);
    delete msg1;
    delete msg2;

    c1->assignment(partitions1);
    c2->assignment(partitions2);

    if (partitions1.size() == 2 && partitions2.size() == 2) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb2.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 1 not: " << rebalance_cb2.assign_call_cnt);
      Test::Say("Unsubscribing consumer 1 from both topics\n");
      c1->unsubscribe();
      unsubscribed = true;
    }

    if (unsubscribed && partitions1.size() == 0 && partitions2.size() == 4) {
      if (rebalance_cb1.assign_call_cnt != 1) /* is now unsubscribed, so rebalance_cb will no longer be called. */
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb2.assign_call_cnt != 2)
        Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 2 not: " << rebalance_cb2.assign_call_cnt);
      if (rebalance_cb1.revoke_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb2.revoke_call_cnt != 0) /* the rebalance_cb should not be called if the revoked partition list is empty */
        Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 0 not: " << rebalance_cb2.assign_call_cnt);
      Test::Say("Unsubscribe completed");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();

    for (size_t i = 0; i<partitions2.size(); i++)
      delete partitions2[i];
    partitions2.clear();
  }

  Test::Say("Closing consumer 1.\n");
  c1->close();
  Test::Say("Closing consumer 2.\n");
  c2->close();

  /* there should be no assign rebalance_cb calls on close */
  if (rebalance_cb1.assign_call_cnt != 1)
    Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 2 not: " << rebalance_cb2.assign_call_cnt);

  if (rebalance_cb1.revoke_call_cnt != 1) /* should not be called a second revoke rebalance_cb */
    Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 1 not: " << rebalance_cb2.assign_call_cnt);

  if (rebalance_cb1.lost_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 1's lost_call_cnt to be 0, not: " << rebalance_cb1.lost_call_cnt);
  if (rebalance_cb2.lost_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 2's lost_call_cnt to be 0, not: " << rebalance_cb2.lost_call_cnt);

  delete c1;
  delete c2;
}


/* ------- m_unsubscribe_2
 *
 * check behavior when:
 *   1. a consumers (with no rebalance_cb) subscribes to a topic.
 *   2. the consumer calls unsubscribe.
 *   3. consumers closed.
 */

static void m_unsubscribe_2 () {
  Test::Say("Executing m_unsubscribe_2\n");

  /* construct test topic (2 partitions) */
  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name_1);

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", NULL);

  Test::Say("Subscribing consumer 1\n");
  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("Consumer 1 subscribe failed: " + RdKafka::err2str(err));

  std::vector<RdKafka::TopicPartition*> partitions1;

  bool done = false;
  bool unsubscribed = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    delete msg1;

    c1->assignment(partitions1);

    if (partitions1.size() == 2) {
      Test::Say("Unsubscribing consumer 1\n");
      c1->unsubscribe();
      unsubscribed = true;
    }

    if (unsubscribed && partitions1.size() == 0) {
      Test::Say("Unsubscribe completed");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer 1.\n");
  c1->close();

  delete c1;
}


/* ------- n_wildcard
 *
 * check behavior when:
 *   1. two consumers (with rebalance_cb) subscribe to a regex (no matching topics exist)
 *   2. create two topics.
 *   3. remove one of the topics.
 *   3. consumers closed.
 */

static void n_wildcard () {
  Test::Say("Executing n_wildcard\n");

  uint64_t random = test_id_generate();
  string topic_sub_name = tostr() << "0113-coop_regex_" << random;

  /* construct test topic (2 partitions) */
  std::string topic_name_1 = Test::mk_topic_name(topic_sub_name, 1);
  std::string topic_name_2 = Test::mk_topic_name(topic_sub_name, 1);
  std::string group_name = Test::mk_unique_group_name("0113-coop_regex");
  std::vector<std::string> topics;
  topics.push_back(tostr() << "^rdkafkatest.*" << topic_sub_name);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, true, "cooperative-sticky", &rebalance_cb1);
  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, true, "cooperative-sticky", &rebalance_cb2);

  Test::Say("Subscribing consumer 1 to regex\n");
  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("Consumer 1 subscribe failed: " + RdKafka::err2str(err));

  Test::Say("Subscribing consumer 2 to regex\n");
  if ((err = c2->subscribe(topics)))
    Test::Fail("Consumer 2 subscribe failed: " + RdKafka::err2str(err));

  std::vector<RdKafka::TopicPartition*> partitions1;
  std::vector<RdKafka::TopicPartition*> partitions2;

  /* there are no matching topics, so the consumers should not join the group initially */
  RdKafka::Message *msg1 = c1->consume(1000);
  RdKafka::Message *msg2 = c2->consume(1000);
  delete msg1;
  delete msg2;
  if (rebalance_cb1.assign_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 0 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 0 not: " << rebalance_cb2.assign_call_cnt);

  bool done = false;
  bool created_topics = false;
  bool deleted_topic = false;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    RdKafka::Message *msg2 = c2->consume(100);
    delete msg1;
    delete msg2;

    c1->assignment(partitions1);
    c2->assignment(partitions2);

    if (partitions1.size() == 0 && partitions2.size() == 0 && !created_topics) {
      Test::Say("Creating two topics with 2 partitions each that match regex\n");
      test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
      test_create_topic(NULL, topic_name_2.c_str(), 2, 1);
      created_topics = true;
    }

    if (partitions1.size() == 2 && partitions2.size() == 2 && !deleted_topic) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb2.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 1 not: " << rebalance_cb2.assign_call_cnt);

      if (rebalance_cb1.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 0 not: " << rebalance_cb1.revoke_call_cnt);
      if (rebalance_cb2.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 0 not: " << rebalance_cb2.revoke_call_cnt);

      Test::Say("Deleting topic 1\n");
      Test::delete_topic(c1, topic_name_1.c_str());
      deleted_topic = true;
    }

    if (partitions1.size() == 1 && partitions2.size() == 1 && deleted_topic) {
      if (rebalance_cb1.revoke_call_cnt != 1) /* accumulated in lost case as well */
        Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 1 not: " << rebalance_cb1.revoke_call_cnt);
      if (rebalance_cb2.revoke_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 1 not: " << rebalance_cb2.revoke_call_cnt);

      if (rebalance_cb1.lost_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's lost_call_cnt to be 1 not: " << rebalance_cb1.lost_call_cnt);
      if (rebalance_cb2.lost_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's lost_call_cnt to be 1 not: " << rebalance_cb2.lost_call_cnt);

      /* consumers will rejoin group after revoking the lost partitions.
       * this will result in an rebalance_cb assign (empty partitions).
       * it follows the revoke, which has alrady been confirmed to have happened. */
      Test::Say("Waiting for rebalance_cb assigns\n");
      while (rebalance_cb1.assign_call_cnt != 2 || rebalance_cb2.assign_call_cnt != 2) {
        msg1 = c1->consume(100);
        msg2 = c2->consume(100);
        delete msg1;
        delete msg2;
      }

      Test::Say("Consumers are subscribed to one partition each\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();

    for (size_t i = 0; i<partitions2.size(); i++)
      delete partitions2[i];
    partitions2.clear();
  }

  Test::Say("Closing consumer 1.\n");
  c1->close();
  Test::Say("Closing consumer 2.\n");
  c2->close();

  /* there should be no assign rebalance_cb calls on close */
  if (rebalance_cb1.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 2 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 2 not: " << rebalance_cb2.assign_call_cnt);

  if (rebalance_cb1.revoke_call_cnt != 2)
    Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 2 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 2)
    Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 2 not: " << rebalance_cb2.assign_call_cnt);

  if (rebalance_cb1.lost_call_cnt != 1)
    Test::Fail(tostr() << "Expecting consumer 1's lost_call_cnt to be 1, not: " << rebalance_cb1.lost_call_cnt);
  if (rebalance_cb2.lost_call_cnt != 1)
    Test::Fail(tostr() << "Expecting consumer 2's lost_call_cnt to be 1, not: " << rebalance_cb2.lost_call_cnt);

  delete c1;
  delete c2;
}


/* ------- o_java_interop
 *
 * check behavior when:
 *   1. consumer (librdkafka) subscribes to two topics (2 and 6 partitions).
 *   2. consumer (java) subscribes to the same two topics.
 *   3. consumer (librdkafka) unsubscribes from the two partition topic.
 *   4. consumer (java) process closes upon detecting the above unsubscribe.
 *   5. consumer (librdkafka) will now be subscribed to 6 partitions.
 *   6. close librdkafka consumer.
 */

static void o_java_interop() {
  Test::Say("Executing o_java_interop\n");

  /* construct test topic (2 partitions) */
  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  test_create_topic(NULL, topic_name_2.c_str(), 6, 1);
  std::vector<std::string> topics;
  topics.push_back(topic_name_1);
  topics.push_back(topic_name_2);

  /* hack: wait for a bit to have better certainty the topics created above exist. */
  sleep(3);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, false, "cooperative-sticky", &rebalance_cb1);

  Test::Say("Subscribing consumer 1 to topic\n");
  RdKafka::ErrorCode err;
  if ((err = c1->subscribe(topics)))
    Test::Fail("Consumer 1 subscribe failed: " + RdKafka::err2str(err));

  std::vector<RdKafka::TopicPartition*> partitions1;

  bool done = false;
  bool java_started = false;
  bool changed_subscription = false;
  bool changed_subscription_done = false;
  int java_pid;
  while (!done) {
    RdKafka::Message *msg1 = c1->consume(100);
    delete msg1;

    c1->assignment(partitions1);

    if (partitions1.size() == 8 && !java_started) {
      Test::Say("Consumer 1 assigned to 8 partitions\n");
      string bootstrapServers = get_bootstrap_servers();
      const char **argv = (const char **)rd_alloca(sizeof(*argv) * (1 + 1 + 1 + 1 + 1 + 1));
      size_t i = 0;
      argv[i++] = "test1";
      argv[i++] = bootstrapServers.c_str();
      argv[i++] = topic_name_1.c_str();
      argv[i++] = topic_name_2.c_str();
      argv[i++] = group_name.c_str();
      argv[i] = NULL;
      java_pid = test_run_java("IncrementalRebalanceCli", (const char **)argv);
      java_started = true;
    }

    if (partitions1.size() == 4 && java_started && !changed_subscription) {
      if (rebalance_cb1.assign_call_cnt != 2)
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 2 not: " << rebalance_cb1.assign_call_cnt);
      Test::Say("Java consumer is now part of the group\n");
      std::vector<std::string> topic_1_only;
      topic_1_only.push_back(topic_name_1);
      if ((err = c1->subscribe(topic_1_only)))
        Test::Fail("Consumer 1 subscribe to one topic failed: " + RdKafka::err2str(err));
      changed_subscription = true;
    }

    if (partitions1.size() == 2 && changed_subscription && rebalance_cb1.assign_call_cnt == 3 && changed_subscription && !changed_subscription_done) {
      /* All topic 1 partitions will be allocated to this consumer whether or not the Java
       * consumer has unsubscribed yet because the sticky algorithm attempts to ensure
       * partition counts are even. */
      Test::Say("Consumer 1 has unsubscribed from topic 2\n");
      changed_subscription_done = true;
    }

    if (partitions1.size() == 2 && changed_subscription && rebalance_cb1.assign_call_cnt == 4 && changed_subscription_done) {
      /* When the java consumer closes, this will cause an empty assign rebalance_cb event,
       * allowing detection of when this has happened. */
      Test::Say("Java consumer has left the group\n");
      done = true;
    }

    for (size_t i = 0; i<partitions1.size(); i++)
      delete partitions1[i];
    partitions1.clear();
  }

  Test::Say("Closing consumer 1.\n");
  c1->close();

  /* Expected behavior is IncrementalRebalanceCli will exit cleanly, timeout otherwise. */
  test_waitpid(java_pid);

  delete c1;
}



extern "C" {

  static int rebalance_cnt;
  static rd_kafka_resp_err_t rebalance_exp_event;
  static rd_bool_t rebalance_exp_lost;

  static void rebalance_cb (rd_kafka_t *rk,
                            rd_kafka_resp_err_t err,
                            rd_kafka_topic_partition_list_t *parts,
                            void *opaque) {
    rebalance_cnt++;
    TEST_SAY("Rebalance #%d: %s: %d partition(s)\n",
             rebalance_cnt, rd_kafka_err2name(err), parts->cnt);

    TEST_ASSERT(err == rebalance_exp_event ||
                rebalance_exp_event == RD_KAFKA_RESP_ERR_NO_ERROR,
                "Expected rebalance event %s, not %s",
                rd_kafka_err2name(rebalance_exp_event),
                rd_kafka_err2name(err));

    if (rebalance_exp_lost) {
      TEST_ASSERT(rd_kafka_assignment_lost(rk),
                  "Expected partitions lost");
      TEST_SAY("Partitions were lost\n");
    }

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
      test_consumer_incremental_assign("assign", rk, parts);
    } else {
      test_consumer_incremental_unassign("unassign", rk, parts);
    }
  }

  /**
   * @brief Wait for an expected rebalance event, or fail.
   */
  static void expect_rebalance (const char *what, rd_kafka_t *c,
                                rd_kafka_resp_err_t exp_event,
                                rd_bool_t exp_lost,
                                int timeout_s) {
    int64_t tmout = test_clock() + (timeout_s * 1000000);
    int start_cnt = rebalance_cnt;

    TEST_SAY("Waiting for %s (%s) for %ds\n",
      what, rd_kafka_err2name(exp_event), timeout_s);

    rebalance_exp_lost = exp_lost;
    rebalance_exp_event = exp_event;

    while (tmout > test_clock() && rebalance_cnt == start_cnt) {
      TEST_SAY("Poll once\n");
      if (test_consumer_poll_once(c, NULL, 1000))
        rd_sleep(1);
    }

    if (rebalance_cnt == start_cnt + 1) {
      rebalance_exp_event = RD_KAFKA_RESP_ERR_NO_ERROR;
      rebalance_exp_lost = exp_lost = rd_false;
      return;
    }

    TEST_FAIL("Timed out waiting for %s (%s)",
      what, rd_kafka_err2name(exp_event));
  }


  /* ------- p_lost_partitions_heartbeat_illegal_generation_test
   *
   * check lost partitions revoke occurs on ILLEGAL_GENERATION heartbeat error.
   */

  static void p_lost_partitions_heartbeat_illegal_generation_test () {
    TEST_SAY("Executing p_lost_partitions_heartbeat_illegal_generation_test\n");

    const char *bootstraps;
    rd_kafka_mock_cluster_t *mcluster;
    const char *groupid = "mygroup";
    const char *topic = "test";
    rd_kafka_t *c;
    rd_kafka_conf_t *conf;

    mcluster = test_mock_cluster_new(3, &bootstraps);

    rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

    /* Seed the topic with messages */
    test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10,
                             "bootstrap.servers", bootstraps,
                             "batch.num.messages", "10",
                             NULL);

    test_conf_init(&conf, NULL, 30);
    test_conf_set(conf, "bootstrap.servers", bootstraps);
    test_conf_set(conf, "security.protocol", "PLAINTEXT");
    test_conf_set(conf, "group.id", groupid);
    test_conf_set(conf, "session.timeout.ms", "5000");
    test_conf_set(conf, "heartbeat.interval.ms", "1000");
    test_conf_set(conf, "auto.offset.reset", "earliest");
    test_conf_set(conf, "enable.auto.commit", "false");
    test_conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");

    c = test_create_consumer(groupid, rebalance_cb, conf, NULL);

    test_consumer_subscribe(c, topic);

    expect_rebalance("initial assignment", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*don't expect lost*/, 5+2);

    /* fail heartbeats */
    rd_kafka_mock_push_request_errors(
      mcluster, RD_KAFKAP_Heartbeat,
      5,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION);

    expect_rebalance("lost partitions", c,
                     RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
                     rd_true/*expect lost*/, 10+2);

    rd_kafka_mock_clear_request_errors(
      mcluster, RD_KAFKAP_Heartbeat);

    expect_rebalance("rejoin after lost", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*don't expect lost*/, 10+2);

    TEST_SAY("Closing consumer\n");
    test_consumer_close(c);

    TEST_SAY("Destroying consumer\n");
    rd_kafka_destroy(c);

    TEST_SAY("Destroying mock cluster\n");
    test_mock_cluster_destroy(mcluster);
  }


  /* ------- q_lost_partitions_illegal_generation_test
   *
   * check lost partitions revoke occurs on ILLEGAL_GENERATION JoinGroup
   * or SyncGroup error.
   */

  static void q_lost_partitions_illegal_generation_test (
      rd_bool_t test_joingroup_fail) {

    TEST_SAY("Executing q_lost_partitions_illegal_generation_test\n");

    const char *bootstraps;
    rd_kafka_mock_cluster_t *mcluster;
    const char *groupid = "mygroup";
    const char *topic1 = "test1";
    const char *topic2 = "test2";
    rd_kafka_t *c;
    rd_kafka_conf_t *conf;
    rd_kafka_resp_err_t err;
    rd_kafka_topic_partition_list_t *topics;

    mcluster = test_mock_cluster_new(3, &bootstraps);

    rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

    /* Seed the topic1 with messages */
    test_produce_msgs_easy_v(topic1, 0, 0, 0, 100, 10,
                             "bootstrap.servers", bootstraps,
                             "batch.num.messages", "10",
                             NULL);

    /* Seed the topic2 with messages */
    test_produce_msgs_easy_v(topic2, 0, 0, 0, 100, 10,
                             "bootstrap.servers", bootstraps,
                             "batch.num.messages", "10",
                             NULL);

    test_conf_init(&conf, NULL, 30);
    test_conf_set(conf, "bootstrap.servers", bootstraps);
    test_conf_set(conf, "security.protocol", "PLAINTEXT");
    test_conf_set(conf, "group.id", groupid);
    test_conf_set(conf, "session.timeout.ms", "5000");
    test_conf_set(conf, "heartbeat.interval.ms", "1000");
    test_conf_set(conf, "auto.offset.reset", "earliest");
    test_conf_set(conf, "enable.auto.commit", "false");
    test_conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");

    c = test_create_consumer(groupid, rebalance_cb, conf, NULL);

    test_consumer_subscribe(c, topic1);

    expect_rebalance("initial assignment", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*don't expect lost*/, 5+2);

    /* fail JoinGroups or SyncGroups */
    rd_kafka_mock_push_request_errors(
      mcluster,
      test_joingroup_fail ? RD_KAFKAP_JoinGroup : RD_KAFKAP_SyncGroup,
      5,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION);

	  topics = rd_kafka_topic_partition_list_new(2);
    rd_kafka_topic_partition_list_add(topics, topic1,
					  RD_KAFKA_PARTITION_UA);
    rd_kafka_topic_partition_list_add(topics, topic2,
					  RD_KAFKA_PARTITION_UA);
    err = rd_kafka_subscribe(c, topics);
    if (err)
            TEST_FAIL("%s: Failed to subscribe to topics: %s\n",
                      rd_kafka_name(c), rd_kafka_err2str(err));
    rd_kafka_topic_partition_list_destroy(topics);

    expect_rebalance("lost partitions", c,
                     RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
                     rd_true/*expect lost*/, 10+2);

    rd_kafka_mock_clear_request_errors(
      mcluster,
      test_joingroup_fail ? RD_KAFKAP_JoinGroup : RD_KAFKAP_SyncGroup);

    expect_rebalance("rejoin group", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*expect lost*/, 10+2);

    TEST_SAY("Closing consumer\n");
    test_consumer_close(c);

    TEST_SAY("Destroying consumer\n");
    rd_kafka_destroy(c);

    TEST_SAY("Destroying mock cluster\n");
    test_mock_cluster_destroy(mcluster);
  }


  /* ------- r_lost_partitions_commit_illegal_generation_test
   *
   * check lost partitions revoke occurs on ILLEGAL_GENERATION Commit
   * error.
   */

  static void r_lost_partitions_commit_illegal_generation_test () {
    TEST_SAY("Executing r_lost_partitions_commit_illegal_generation_test\n");

    const char *bootstraps;
    rd_kafka_mock_cluster_t *mcluster;
    const char *groupid = "mygroup";
    const char *topic = "test";
    rd_kafka_t *c;
    rd_kafka_conf_t *conf;

    mcluster = test_mock_cluster_new(3, &bootstraps);

    rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

    /* Seed the topic with messages */
    test_produce_msgs_easy_v(topic, 0, 0, 0, 100, 10,
                             "bootstrap.servers", bootstraps,
                             "batch.num.messages", "10",
                             NULL);

    test_conf_init(&conf, NULL, 30);
    test_conf_set(conf, "bootstrap.servers", bootstraps);
    test_conf_set(conf, "security.protocol", "PLAINTEXT");
    test_conf_set(conf, "group.id", groupid);
    test_conf_set(conf, "auto.offset.reset", "earliest");
    test_conf_set(conf, "enable.auto.commit", "false");
    test_conf_set(conf, "partition.assignment.strategy", "cooperative-sticky");

    c = test_create_consumer(groupid, rebalance_cb, conf, NULL);

    test_consumer_subscribe(c, topic);

    expect_rebalance("initial assignment", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*don't expect lost*/, 5+2);

    /* fail heartbeats */
    rd_kafka_mock_push_request_errors(
      mcluster, RD_KAFKAP_OffsetCommit,
      5,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION);

    rd_kafka_commit(c, NULL, rd_false);

    expect_rebalance("lost partitions", c,
                     RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
                     rd_true/*expect lost*/, 10+2);

    expect_rebalance("rejoin group", c,
                     RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                     rd_false/*expect lost*/, 20+2);

    TEST_SAY("Closing consumer\n");
    test_consumer_close(c);

    TEST_SAY("Destroying consumer\n");
    rd_kafka_destroy(c);

    TEST_SAY("Destroying mock cluster\n");
    test_mock_cluster_destroy(mcluster);
  }


  int main_0113_cooperative_rebalance (int argc, char **argv) {
    // TODO: re-enable. working tests.
    if (true) {
      a_assign_tests();
      b_subscribe_with_cb_test(true/*close consumer*/);
      b_subscribe_with_cb_test(false/*don't close consumer*/);
      c_subscribe_no_cb_test(true/*close consumer*/);
      c_subscribe_no_cb_test(false/*don't close consumer*/);
      d_change_subscription_add_topic(true/*close consumer*/);
      d_change_subscription_add_topic(false/*don't close consumer*/);
      e_change_subscription_remove_topic(true/*close consumer*/);
      e_change_subscription_remove_topic(false/*don't close consumer*/);
      f_assign_call_cooperative();
      g_incremental_assign_call_eager();
      h_delete_topic();
      i_delete_topic_2();
      j_delete_topic_no_rb_callback();
      k_add_partition();
      l_unsubscribe();
      m_unsubscribe_2();
      n_wildcard();
      o_java_interop();
      p_lost_partitions_heartbeat_illegal_generation_test();
      q_lost_partitions_illegal_generation_test(rd_false/*joingroup*/);
      q_lost_partitions_illegal_generation_test(rd_true/*syncgroup*/);
      r_lost_partitions_commit_illegal_generation_test();
    }

    return 0;
  }
}




// -- TODO ideas --

// unit test assinors of different types.
// auto commit tests.

// kip says: ConsumerCoordinator will check if the newly assigned / revoked / lost partitions set is
// empty or not; and if not, we will not trigger the corresponding listener.
// however it also says: For those newly-added-partitions, call the rebalance listener's
// onPartitionsAssigned (even if empty). the latter is what matches java (double check)

// lost_partitions_poll_timeout_test();
// lost_partitions_session_timeout_test();
// lost_partitions_heartbeat_fenced_instance_id_test();
