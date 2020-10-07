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
                                              string assignment_strategy,
                                              std::vector<std::pair<std::string, std::string> > *additional_conf,
                                              RdKafka::RebalanceCb *rebalance_cb,
                                              int timeout_s) {

  std::string bootstraps;
  std::string errstr;
  std::vector<std::pair<std::string, std::string> >::iterator itr;

  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, timeout_s);
  Test::conf_set(conf, "client.id", client_id);
  Test::conf_set(conf, "group.id", group_id);
  Test::conf_set(conf, "auto.offset.reset", "earliest");
  Test::conf_set(conf, "enable.auto.commit", "false");
  Test::conf_set(conf, "partition.assignment.strategy", assignment_strategy);
  if (additional_conf != NULL) {
    for (itr = (*additional_conf).begin(); itr != (*additional_conf).end(); itr++)
      Test::conf_set(conf, itr->first, itr->second);
  }

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
  RdKafka::TopicPartition::destroy(partitions);
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

    const char *lost_str = consumer->assignment_lost() ? " (LOST)" : "";
    Test::Say(tostr() << "RebalanceCb: " << consumer->name() << " " << RdKafka::err2str(err) << lost_str << ": " << part_list_print(partitions) << "\n");
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      if (consumer->assignment_lost())
        Test::Fail("unexpected lost assignment during ASSIGN rebalance");
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

  Test::Say("Incremental assign, then assign(NULL)\n");

  if ((error = consumer->incremental_assign(toppars1)))
    Test::Fail(tostr() << "Incremental assign failed: " << error->str());
  Test::check_assignment(consumer, 1, &toppars1[0]->topic());

  if ((err = consumer->unassign()))
    Test::Fail("Unassign failed: " + RdKafka::err2str(err));
  Test::check_assignment(consumer, 0, NULL);
}


/** Assign, then incremental unassign.
 */
static void assign_test_2 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::ErrorCode err;
  RdKafka::Error *error;

  Test::Say("Assign, then incremental unassign\n");

  if ((err = consumer->assign(toppars1)))
    Test::Fail("Assign failed: " + RdKafka::err2str(err));
  Test::check_assignment(consumer, 1, &toppars1[0]->topic());

  if ((error = consumer->incremental_unassign(toppars1)))
    Test::Fail("Incremental unassign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);
}


/** Incremental assign, then incremental unassign.
 */
static void assign_test_3 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::Error *error;

  Test::Say("Incremental assign, then incremental unassign\n");

  if ((error = consumer->incremental_assign(toppars1)))
    Test::Fail("Incremental assign failed: " + error->str());
  Test::check_assignment(consumer, 1, &toppars1[0]->topic());

  if ((error = consumer->incremental_unassign(toppars1)))
    Test::Fail("Incremental unassign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);
}


/** Multi-topic incremental assign and unassign + message consumption.
 */
static void assign_test_4 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::Error *error;

  Test::Say("Multi-topic incremental assign and unassign + message consumption\n");

  if ((error = consumer->incremental_assign(toppars1)))
    Test::Fail("Incremental assign failed: " + error->str());
  Test::check_assignment(consumer, 1, &toppars1[0]->topic());

  RdKafka::Message *m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  if (m->len() != 100)
    Test::Fail(tostr() << "Expecting msg len to be 100, not: " << m->len()); /* implies read from topic 1. */
  delete m;

  if ((error = consumer->incremental_unassign(toppars1)))
    Test::Fail("Incremental unassign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);

  m = consumer->consume(100);
  if (m->err() != RdKafka::ERR__TIMED_OUT)
    Test::Fail("Not expecting a consumed message.");
  delete m;

  if ((error = consumer->incremental_assign(toppars2)))
    Test::Fail("Incremental assign failed: " + error->str());
  Test::check_assignment(consumer, 1, &toppars2[0]->topic());

  m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  if (m->len() != 200)
    Test::Fail(tostr() << "Expecting msg len to be 200, not: " << m->len()); /* implies read from topic 2. */
  delete m;

  if ((error = consumer->incremental_assign(toppars1)))
    Test::Fail("Incremental assign failed: " + error->str());
  if (Test::assignment_partition_count(consumer) != 2)
    Test::Fail(tostr() << "Expecting current assignment to have size 2, not: " << Test::assignment_partition_count(consumer));

  m = consumer->consume(5000);
  if (m->err() != RdKafka::ERR_NO_ERROR)
    Test::Fail("Expecting a consumed message.");
  delete m;

  if ((error = consumer->incremental_unassign(toppars2)))
    Test::Fail("Incremental unassign failed: " + error->str());
  if ((error = consumer->incremental_unassign(toppars1)))
    Test::Fail("Incremental unassign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);
}


/** Incremental assign and unassign of empty collection.
 */
static void assign_test_5 (RdKafka::KafkaConsumer *consumer,
                           std::vector<RdKafka::TopicPartition *> toppars1,
                           std::vector<RdKafka::TopicPartition *> toppars2) {
  RdKafka::Error *error;
  std::vector<RdKafka::TopicPartition *> toppars3;

  Test::Say("Incremental assign and unassign of empty collection\n");

  if ((error = consumer->incremental_assign(toppars3)))
    Test::Fail("Incremental assign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);

  if ((error = consumer->incremental_unassign(toppars3)))
    Test::Fail("Incremental unassign failed: " + error->str());
  Test::check_assignment(consumer, 0, NULL);
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

    RdKafka::KafkaConsumer *consumer = make_consumer("C_1", t1, "cooperative-sticky", NULL, NULL, 10);

    test(consumer, toppars1, toppars2);

    RdKafka::TopicPartition::destroy(toppars1);
    RdKafka::TopicPartition::destroy(toppars2);

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
    std::string topic2_str = Test::mk_topic_name("0113-cooperative_rebalance", 1);
    test_create_topic(NULL, topic2_str.c_str(), 1, 1);

    test_produce_msgs_easy_size(topic1_str.c_str(), 0, 0, msgcnt, msgsize1);
    test_produce_msgs_easy_size(topic2_str.c_str(), 0, 0, msgcnt, msgsize2);

    run_test(topic1_str, topic2_str, assign_test_1);
    run_test(topic1_str, topic2_str, assign_test_2);
    run_test(topic1_str, topic2_str, assign_test_3);
    run_test(topic1_str, topic2_str, assign_test_4);
    run_test(topic1_str, topic2_str, assign_test_5);
}



/* Check behavior when:
 *   1. single topic with 2 partitions.
 *   2. consumer 1 (with rebalance_cb) subscribes to it.
 *   3. consumer 2 (with rebalance_cb) subscribes to it.
 *   4. close.
 */

static void b_subscribe_with_cb_test (rd_bool_t close_consumer) {
  Test::Say("Executing b_subscribe_with_cb_test\n");

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name.c_str(), 2, 1);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, "cooperative-sticky", NULL, &rebalance_cb1, 25);
  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, "cooperative-sticky", NULL, &rebalance_cb2, 25);
  test_wait_topic_exists(c1->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c1, topic_name);

  bool c2_subscribed = false;
  while (true) {
    Test::poll_once(c1, 500);
    Test::poll_once(c2, 500);

    /* Start c2 after c1 has received initial assignment */
    if (!c2_subscribed && rebalance_cb1.assign_call_cnt > 0) {
      Test::subscribe(c2, topic_name);
      c2_subscribed = true;
    }

    /* Failure case: test will time out. */
    if (rebalance_cb1.assign_call_cnt == 3 &&
        rebalance_cb2.assign_call_cnt == 2) {
      break;
    }
  }

  /* Sequence of events:
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

    /* Failure case: test will timeout. */
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

  /* ..and net assigned partitions should drop to 0 in both cases: */
  if (rebalance_cb1.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 1 to have net 0 assigned partitions, not: " << rebalance_cb1.partitions_assigned_net);
  if (rebalance_cb2.partitions_assigned_net != 0)
    Test::Fail(tostr() << "Expecting consumer 2 to have net 0 assigned partitions, not: " << rebalance_cb2.partitions_assigned_net);

  /* Nothing in this test should result in lost partitions */
  if (rebalance_cb1.lost_call_cnt > 0)
    Test::Fail(tostr() << "Expecting consumer 1 to have 0 lost partition events, not: " << rebalance_cb1.lost_call_cnt);
  if (rebalance_cb2.lost_call_cnt > 0)
    Test::Fail(tostr() << "Expecting consumer 2 to have 0 lost partition events, not: " << rebalance_cb2.lost_call_cnt);

  delete c1;
  delete c2;
}



/* Check behavior when:
 *   1. Single topic with 2 partitions.
 *   2. Consumer 1 (no rebalance_cb) subscribes to it.
 *   3. Consumer 2 (no rebalance_cb) subscribes to it.
 *   4. Close.
 */

static void c_subscribe_no_cb_test (rd_bool_t close_consumer) {
  Test::Say("Executing c_subscribe_no_cb_test\n");

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name.c_str(), 2, 1);

  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, "cooperative-sticky", NULL, NULL, 20);
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, "cooperative-sticky", NULL, NULL, 20);
  test_wait_topic_exists(c1->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c1, topic_name);

  bool c2_subscribed = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c1, 500);
    Test::poll_once(c2, 500);

    if (Test::assignment_partition_count(c1) == 2 && !c2_subscribed) {
      Test::subscribe(c2, topic_name);
      c2_subscribed = true;
    }

    if (Test::assignment_partition_count(c1) == 1 &&
        Test::assignment_partition_count(c2) == 1) {
      Test::Say("Consumer 1 and 2 are both assigned to single partition.\n");
      done = true;
    }
  }

  if (close_consumer) {
    Test::Say("Closing consumer 1\n");
    c1->close();
    Test::Say("Closing consumer 2\n");
    c2->close();
  } else {
    Test::Say("Skipping close() of consumer 1 and 2.\n");
  }

  delete c1;
  delete c2;
}



/* Check behavior when:
 *   1. Single consumer (no rebalance_cb) subscribes to topic.
 *   2. Subscription is changed (topic added).
 *   3. Consumer is closed.
 */

static void d_change_subscription_add_topic (rd_bool_t close_consumer) {
  Test::Say("Executing d_change_subscription_add_topic\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_2.c_str(), 2, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", NULL, NULL, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_2.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1);

  bool subscribed_to_one_topic = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 2 && !subscribed_to_one_topic) {
      subscribed_to_one_topic = true;
      Test::subscribe(c, topic_name_1, topic_name_2);
    }

    if (Test::assignment_partition_count(c) == 4) {
      Test::Say("Consumer is assigned to two topics.\n");
      done = true;
    }
  }

  if (close_consumer) {
    Test::Say("Closing consumer\n");
    c->close();
  } else
    Test::Say("Skipping close() of consumer\n");

  delete c;
}



/* Check behavior when:
 *   1. Single consumer (no rebalance_cb) subscribes to topic.
 *   2. Subscription is changed (topic added).
 *   3. Consumer is closed.
 */

static void e_change_subscription_remove_topic (rd_bool_t close_consumer) {
  Test::Say("Executing e_change_subscription_remove_topic\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_2.c_str(), 2, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", NULL, NULL, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_2.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1, topic_name_2);

  bool subscribed_to_two_topics = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 4 && !subscribed_to_two_topics) {
      subscribed_to_two_topics = true;
      Test::subscribe(c, topic_name_1);
    }

    if (Test::assignment_partition_count(c) == 2) {
      Test::Say("Consumer is assigned to one topic\n");
      done = true;
    }
  }

  if (!close_consumer) {
    Test::Say("Closing consumer\n");
    c->close();
  } else
    Test::Say("Skipping close() of consumer\n");

  delete c;
}



/* Check that use of consumer->assign() and consumer->unassign() is disallowed when a
 * COOPERATIVE assignor is in use.
 */

class FTestRebalanceCb : public RdKafka::RebalanceCb {
public:
  rd_bool_t assigned;

  FTestRebalanceCb () {
    assigned = rd_false;
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

      assigned = rd_true;

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

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  FTestRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c, topic_name);

  while (!rebalance_cb.assigned)
    Test::poll_once(c, 500);

  c->close();
  delete c;
}



/* Check that use of consumer->incremental_assign() and consumer->incremental_unassign() is
 * disallowed when an EAGER assignor is in use.
 */
class GTestRebalanceCb : public RdKafka::RebalanceCb {
public:
  rd_bool_t assigned;

  GTestRebalanceCb () {
    assigned = rd_false;
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

      assigned = rd_true;

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

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  GTestRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "roundrobin", &additional_conf, &rebalance_cb, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c, topic_name);

  while (!rebalance_cb.assigned)
    Test::poll_once(c, 500);

  c->close();
  delete c;
}



/* Check behavior when:
 *   1. Single consumer (rebalance_cb) subscribes to two topics.
 *   2. One of the topics is deleted.
 *   3. Consumer is closed.
 */

static void h_delete_topic () {
  Test::Say("Executing h_delete_topic\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_1.c_str(), 1, 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_2.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  DefaultRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_2.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1, topic_name_2);

  bool deleted = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    std::vector<RdKafka::TopicPartition*> partitions;
    c->assignment(partitions);

    if (partitions.size() == 2 && !deleted) {
      if (rebalance_cb.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected 1 assign call, saw " << rebalance_cb.assign_call_cnt << "\n");
      Test::delete_topic(c, topic_name_2.c_str());
      deleted = true;
    }

    if (partitions.size() == 1 && deleted) {
      if (partitions[0]->topic() != topic_name_1)
        Test::Fail(tostr() << "Expecting subscribed topic to be '" << topic_name_1 << "' not '" << partitions[0]->topic() << "'");
      Test::Say(tostr() << "Assignment no longer includes deleted topic '" << topic_name_2 << "'\n");
      done = true;
    }

    RdKafka::TopicPartition::destroy(partitions);
  }

  Test::Say("Closing consumer\n");
  c->close();

  delete c;
}



/* Check behavior when:
 *   1. Single consumer (rebalance_cb) subscribes to a single topic.
 *   2. That topic is deleted leaving no topics.
 *   3. Consumer is closed.
 */

static void i_delete_topic_2 () {
  Test::Say("Executing i_delete_topic_2\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_1.c_str(), 1, 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  DefaultRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1);

  bool deleted = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 1 && !deleted) {
      if (rebalance_cb.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected one assign call, saw " << rebalance_cb.assign_call_cnt << "\n");
      Test::delete_topic(c, topic_name_1.c_str());
      deleted = true;
    }

    if (Test::assignment_partition_count(c) == 0 && deleted) {
      Test::Say(tostr() << "Assignment is empty following deletion of topic\n");
      done = true;
    }
  }

  Test::Say("Closing consumer\n");
  c->close();

  delete c;
}



/* Check behavior when:
 *   1. single consumer (without rebalance_cb) subscribes to a single topic.
 *   2. that topic is deleted leaving no topics.
 *   3. consumer is closed.
 */

static void j_delete_topic_no_rb_callback () {
  Test::Say("Executing j_delete_topic_no_rb_callback\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name_1.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, NULL, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1);

  bool deleted = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 1 && !deleted) {
      Test::delete_topic(c, topic_name_1.c_str());
      deleted = true;
    }

    if (Test::assignment_partition_count(c) == 0 && deleted) {
      Test::Say(tostr() << "Assignment is empty following deletion of topic\n");
      done = true;
    }
  }

  Test::Say("Closing consumer\n");
  c->close();

  delete c;
}



/* Check behavior when:
 *   1. Single consumer (rebalance_cb) subscribes to a 1 partition topic.
 *   2. Number of partitions is increased to 2.
 *   3. Consumer is closed.
 */

static void k_add_partition () {
  Test::Say("Executing k_add_partition\n");

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  test_create_topic(NULL, topic_name.c_str(), 1, 1);

  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));
  DefaultRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c, topic_name);

  bool subscribed = false;
  bool done = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 1 && !subscribed) {
      if (rebalance_cb.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expected 1 assign call, saw " << rebalance_cb.assign_call_cnt);
      if (rebalance_cb.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expected 0 revoke calls, saw " << rebalance_cb.revoke_call_cnt);
      Test::create_partitions(c, topic_name.c_str(), 2);
      subscribed = true;
    }

    if (Test::assignment_partition_count(c) == 2 && subscribed) {
      if (rebalance_cb.assign_call_cnt != 2)
        Test::Fail(tostr() << "Expected 2 assign calls, saw " << rebalance_cb.assign_call_cnt);
      if (rebalance_cb.revoke_call_cnt != 0)
        Test::Fail(tostr() << "Expected 0 revoke calls, saw " << rebalance_cb.revoke_call_cnt);
      done = true;
    }
  }

  Test::Say("Closing consumer\n");
  c->close();

  if (rebalance_cb.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expected 2 assign calls, saw " << rebalance_cb.assign_call_cnt);
  if (rebalance_cb.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expected 1 revoke call, saw " << rebalance_cb.revoke_call_cnt);

  delete c;
}



/* Check behavior when:
 *   1. two consumers (with rebalance_cb's) subscribe to two topics.
 *   2. one of the consumers calls unsubscribe.
 *   3. consumers closed.
 */

static void l_unsubscribe () {
  Test::Say("Executing l_unsubscribe\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  test_create_topic(NULL, topic_name_2.c_str(), 2, 1);

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, "cooperative-sticky", NULL, &rebalance_cb1, 30);
  test_wait_topic_exists(c1->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c1->c_ptr(), topic_name_2.c_str(), 10*1000);

  Test::subscribe(c1, topic_name_1, topic_name_2);

  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, "cooperative-sticky", NULL, &rebalance_cb2, 30);
  Test::subscribe(c2, topic_name_1, topic_name_2);

  bool done = false;
  bool unsubscribed = false;
  while (!done) {
    Test::poll_once(c1, 500);
    Test::poll_once(c2, 500);

    if (Test::assignment_partition_count(c1) == 2 && Test::assignment_partition_count(c2) == 2) {
      if (rebalance_cb1.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 1 not: " << rebalance_cb1.assign_call_cnt);
      if (rebalance_cb2.assign_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 1 not: " << rebalance_cb2.assign_call_cnt);
      Test::Say("Unsubscribing consumer 1 from both topics\n");
      c1->unsubscribe();
      unsubscribed = true;
    }

    if (unsubscribed && Test::assignment_partition_count(c1) == 0 && Test::assignment_partition_count(c2) == 4) {
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
  }

  Test::Say("Closing consumer 1\n");
  c1->close();
  Test::Say("Closing consumer 2\n");
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



/* Check behavior when:
 *   1. A consumers (with no rebalance_cb) subscribes to a topic.
 *   2. The consumer calls unsubscribe.
 *   3. Consumers closed.
 */

static void m_unsubscribe_2 () {
  Test::Say("Executing m_unsubscribe_2\n");

  std::string topic_name = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name.c_str(), 2, 1);

  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", NULL, NULL, 15);
  test_wait_topic_exists(c->c_ptr(), topic_name.c_str(), 10*1000);

  Test::subscribe(c, topic_name);

  bool done = false;
  bool unsubscribed = false;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 2) {
      Test::unsubscribe(c);
      unsubscribed = true;
    }

    if (unsubscribed && Test::assignment_partition_count(c) == 0) {
      Test::Say("Unsubscribe completed");
      done = true;
    }
  }

  Test::Say("Closing consumer\n");
  c->close();

  delete c;
}



/* Check behavior when:
 *   1. Two consumers (with rebalance_cb) subscribe to a regex (no matching topics exist)
 *   2. Create two topics.
 *   3. Remove one of the topics.
 *   3. Consumers closed.
 */

static void n_wildcard () {
  Test::Say("Executing n_wildcard\n");

  uint64_t random = test_id_generate();
  string topic_sub_name = tostr() << "0113-coop_regex_" << random;

  std::string topic_name_1 = Test::mk_topic_name(topic_sub_name, 1);
  std::string topic_name_2 = Test::mk_topic_name(topic_sub_name, 1);
  std::string group_name = Test::mk_unique_group_name("0113-coop_regex");
  std::string topic_regex = tostr() << "^rdkafkatest.*" << topic_sub_name;

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("topic.metadata.refresh.interval.ms"), std::string("3000")));

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb1, 30);
  Test::subscribe(c1, topic_regex);

  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb2, 30);
  Test::subscribe(c2, topic_regex);

  /* There are no matching topics, so the consumers should not join the group initially */
  Test::poll_once(c1, 500);
  Test::poll_once(c2, 500);

  if (rebalance_cb1.assign_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 0 not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 0)
    Test::Fail(tostr() << "Expecting consumer 2's assign_call_cnt to be 0 not: " << rebalance_cb2.assign_call_cnt);

  bool done = false;
  bool created_topics = false;
  bool deleted_topic = false;
  while (!done) {
    Test::poll_once(c1, 500);
    Test::poll_once(c2, 500);

    if (Test::assignment_partition_count(c1) == 0 && Test::assignment_partition_count(c2) == 0 && !created_topics) {
      Test::Say("Creating two topics with 2 partitions each that match regex\n");
      test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
      test_create_topic(NULL, topic_name_2.c_str(), 2, 1);
      test_wait_topic_exists(c1->c_ptr(), topic_name_1.c_str(), 10*1000);
      test_wait_topic_exists(c1->c_ptr(), topic_name_2.c_str(), 10*1000);
      created_topics = true;
    }

    if (Test::assignment_partition_count(c1) == 2 && Test::assignment_partition_count(c2) == 2 && !deleted_topic) {
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

    if (Test::assignment_partition_count(c1) == 1 && Test::assignment_partition_count(c2) == 1 && deleted_topic) {
      if (rebalance_cb1.revoke_call_cnt != 1) /* accumulated in lost case as well */
        Test::Fail(tostr() << "Expecting consumer 1's revoke_call_cnt to be 1 not: " << rebalance_cb1.revoke_call_cnt);
      if (rebalance_cb2.revoke_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's revoke_call_cnt to be 1 not: " << rebalance_cb2.revoke_call_cnt);

      if (rebalance_cb1.lost_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 1's lost_call_cnt to be 1 not: " << rebalance_cb1.lost_call_cnt);
      if (rebalance_cb2.lost_call_cnt != 1)
        Test::Fail(tostr() << "Expecting consumer 2's lost_call_cnt to be 1 not: " << rebalance_cb2.lost_call_cnt);

      /* Consumers will rejoin group after revoking the lost partitions.
       * this will result in an rebalance_cb assign (empty partitions).
       * it follows the revoke, which has alrady been confirmed to have happened. */
      Test::Say("Waiting for rebalance_cb assigns\n");
      while (rebalance_cb1.assign_call_cnt != 2 || rebalance_cb2.assign_call_cnt != 2) {
        Test::poll_once(c1, 500);
        Test::poll_once(c2, 500);
      }

      Test::Say("Consumers are subscribed to one partition each\n");
      done = true;
    }
  }

  Test::Say("Closing consumer 1\n");
  c1->close();
  Test::Say("Closing consumer 2\n");
  c2->close();

  /* There should be no assign rebalance_cb calls on close */
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



/* Check behavior when:
 *   1. Consumer (librdkafka) subscribes to two topics (2 and 6 partitions).
 *   2. Consumer (java) subscribes to the same two topics.
 *   3. Consumer (librdkafka) unsubscribes from the two partition topic.
 *   4. Consumer (java) process closes upon detecting the above unsubscribe.
 *   5. Consumer (librdkafka) will now be subscribed to 6 partitions.
 *   6. Close librdkafka consumer.
 */

static void o_java_interop() {
  Test::Say("Executing o_java_interop\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);
  test_create_topic(NULL, topic_name_2.c_str(), 6, 1);

  DefaultRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", NULL, &rebalance_cb, 25);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_2.c_str(), 10*1000);

  Test::subscribe(c, topic_name_1, topic_name_2);

  bool done = false;
  bool changed_subscription = false;
  bool changed_subscription_done = false;
  int java_pid = 0;
  while (!done) {
    Test::poll_once(c, 500);

    if (Test::assignment_partition_count(c) == 8 && !java_pid != 0) {
      Test::Say("librdkafka consumer assigned to 8 partitions\n");
      string bootstrapServers = get_bootstrap_servers();
      const char *argv[1 + 1 + 1 + 1 + 1 + 1];
      size_t i = 0;
      argv[i++] = "test1";
      argv[i++] = bootstrapServers.c_str();
      argv[i++] = topic_name_1.c_str();
      argv[i++] = topic_name_2.c_str();
      argv[i++] = group_name.c_str();
      argv[i] = NULL;
      java_pid = test_run_java("IncrementalRebalanceCli", argv);
      if (java_pid <= 0)
        Test::Fail(tostr() << "Unexpected pid: " << java_pid);
    }

    if (Test::assignment_partition_count(c) == 4 && java_pid != 0 && !changed_subscription) {
      if (rebalance_cb.assign_call_cnt != 2)
        Test::Fail(tostr() << "Expecting consumer 1's assign_call_cnt to be 2 not: " << rebalance_cb.assign_call_cnt);
      Test::Say("Java consumer is now part of the group\n");
      Test::subscribe(c, topic_name_1);
      changed_subscription = true;
    }

    if (Test::assignment_partition_count(c) == 2 && changed_subscription && rebalance_cb.assign_call_cnt == 3 && changed_subscription && !changed_subscription_done) {
      /* All topic 1 partitions will be allocated to this consumer whether or not the Java
       * consumer has unsubscribed yet because the sticky algorithm attempts to ensure
       * partition counts are even. */
      Test::Say("Consumer 1 has unsubscribed from topic 2\n");
      changed_subscription_done = true;
    }

    if (Test::assignment_partition_count(c) == 2 && changed_subscription && rebalance_cb.assign_call_cnt == 4 && changed_subscription_done) {
      /* When the java consumer closes, this will cause an empty assign rebalance_cb event,
       * allowing detection of when this has happened. */
      Test::Say("Java consumer has left the group\n");
      done = true;
    }
  }

  Test::Say("Closing consumer\n");
  c->close();

  /* Expected behavior is IncrementalRebalanceCli will exit cleanly, timeout otherwise. */
  test_waitpid(java_pid);

  delete c;
}



/* Check behavior when:
 *  - Single consumer subscribes to topic.
 *  - Soon after (timing such that rebalance is probably in progress) it subscribes to a different topic.
 */

static void s_subscribe_when_rebalancing(int variation) {
  Test::Say(tostr() << "Executing s_subscribe_when_rebalancing, variation: " << variation << "\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_2 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string topic_name_3 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 1, 1);
  test_create_topic(NULL, topic_name_2.c_str(), 1, 1);
  test_create_topic(NULL, topic_name_3.c_str(), 1, 1);

  DefaultRebalanceCb rebalance_cb;
  RdKafka::KafkaConsumer *c = make_consumer("C_1", group_name, "cooperative-sticky", NULL, &rebalance_cb, 25);
  test_wait_topic_exists(c->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_2.c_str(), 10*1000);
  test_wait_topic_exists(c->c_ptr(), topic_name_3.c_str(), 10*1000);

  if (variation == 2 || variation == 4 || variation == 6) {
    /* Pre-cache metadata for all topics. */
    class RdKafka::Metadata *metadata;
    c->metadata(true, NULL, &metadata, 5000);
    delete metadata;
  }

  Test::subscribe(c, topic_name_1);
  Test::wait_for_assignment(c, 1, &topic_name_1);

  Test::subscribe(c, topic_name_2);

  if (variation == 3 || variation == 5)
    Test::poll_once(c, 500);

  if (variation < 5) {
    // Very quickly after subscribing to topic 2, subscribe to topic 3.
    Test::subscribe(c, topic_name_3);
    Test::wait_for_assignment(c, 1, &topic_name_3);
  } else {
    // ..or unsubscribe.
    Test::unsubscribe(c);
    Test::wait_for_assignment(c, 0, NULL);
  }

  delete c;
}



/* Check behavior when:
 *  - Two consumer subscribe to a topic.
 *  - Max poll interval is exceeded on the first consumer.
 */

static void t_max_poll_interval_exceeded(int variation) {
  Test::Say(tostr() << "Executing t_max_poll_interval_exceeded, variation: " << variation << "\n");

  std::string topic_name_1 = Test::mk_topic_name("0113-cooperative_rebalance", 1);
  std::string group_name = Test::mk_unique_group_name("0113-cooperative_rebalance");
  test_create_topic(NULL, topic_name_1.c_str(), 2, 1);

  std::vector<std::pair<std::string, std::string> > additional_conf;
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("session.timeout.ms"), std::string("6000")));
  additional_conf.push_back(std::pair<std::string, std::string>(std::string("max.poll.interval.ms"), std::string("7000")));

  DefaultRebalanceCb rebalance_cb1;
  RdKafka::KafkaConsumer *c1 = make_consumer("C_1", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb1, 30);
  DefaultRebalanceCb rebalance_cb2;
  RdKafka::KafkaConsumer *c2 = make_consumer("C_2", group_name, "cooperative-sticky", &additional_conf, &rebalance_cb2, 30);

  test_wait_topic_exists(c1->c_ptr(), topic_name_1.c_str(), 10*1000);
  test_wait_topic_exists(c2->c_ptr(), topic_name_1.c_str(), 10*1000);

  Test::subscribe(c1, topic_name_1);
  Test::subscribe(c2, topic_name_1);

  bool done = false;
  bool both_have_been_assigned = false;
  while (!done) {
    if (!both_have_been_assigned)
      Test::poll_once(c1, 500);
    Test::poll_once(c2, 500);

    if (Test::assignment_partition_count(c1) == 1 && Test::assignment_partition_count(c2) == 1 && !both_have_been_assigned) {
      Test::Say(tostr() << "Both consumers are assigned to topic " << topic_name_1 << ". WAITING 7 seconds for max.poll.interval.ms to be exceeded\n");
      both_have_been_assigned = true;
    }

    if (Test::assignment_partition_count(c2) == 2 && both_have_been_assigned) {
      Test::Say("Consumer 1 is no longer assigned any partitions, done\n");
      done = true;
    }
  }

  if (variation == 1) {
    if (rebalance_cb1.lost_call_cnt != 0)
      Test::Fail(tostr() << "Expected consumer 1 lost revoke count to be 0, not: " << rebalance_cb1.lost_call_cnt);
    Test::poll_once(c1, 500); /* Eat the max poll interval exceeded error message */
    Test::poll_once(c1, 500); /* Trigger the rebalance_cb with lost partitions */
    if (rebalance_cb1.lost_call_cnt != 1)
      Test::Fail(tostr() << "Expected consumer 1 lost revoke count to be 1, not: " << rebalance_cb1.lost_call_cnt);
  }

  c1->close();
  c2->close();

  if (rebalance_cb1.lost_call_cnt != 1)
    Test::Fail(tostr() << "Expected consumer 1 lost revoke count to be 1, not: " << rebalance_cb1.lost_call_cnt);

  if (rebalance_cb1.assign_call_cnt != 1)
    Test::Fail(tostr() << "Expected consumer 1 assign count to be 1, not: " << rebalance_cb1.assign_call_cnt);
  if (rebalance_cb2.assign_call_cnt != 2)
    Test::Fail(tostr() << "Expected consumer 1 assign count to be 2, not: " << rebalance_cb1.assign_call_cnt);

  if (rebalance_cb1.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expected consumer 1 revoke count to be 1, not: " << rebalance_cb1.revoke_call_cnt);
  if (rebalance_cb2.revoke_call_cnt != 1)
    Test::Fail(tostr() << "Expected consumer 1 revoke count to be 1, not: " << rebalance_cb1.revoke_call_cnt);

  delete c1;
  delete c2;
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



  /* Check lost partitions revoke occurs on ILLEGAL_GENERATION heartbeat error.
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

    /* Fail heartbeats */
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



  /* Check lost partitions revoke occurs on ILLEGAL_GENERATION JoinGroup
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

    /* Fail JoinGroups or SyncGroups */
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



  /* Check lost partitions revoke occurs on ILLEGAL_GENERATION Commit
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

    /* Fail heartbeats */
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
    int i;

    a_assign_tests();
    b_subscribe_with_cb_test(true/*close consumer*/);

    if (test_quick) {
      Test::Say("Skipping tests c -> s due to quick mode\n");
      return 0;
    }

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
    for (i = 1 ; i <= 6 ; i++) /* iterate over 6 different test variations */
      s_subscribe_when_rebalancing(i);
    t_max_poll_interval_exceeded(1);
    t_max_poll_interval_exceeded(2);

    return 0;
  }
}
