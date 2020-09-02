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

/**
 * Test sticky.partitioning.linger.ms producer property.
 *
 */

#include <iostream>
#include <fstream>
#include <iterator>
#include <string>
#include "testcpp.h"

/**
 * @brief Specify sticky.partitioning.linger.ms and check consumed
 * messages to verify it takes effect.
 */
static void do_test_sticky_partitioning (int sticky_delay_ms,
                                         int produce_time_ms,
                                         int partition_cnt,
                                         int min_active_cnt,
                                         int max_active_cnt) {

  Test::Say(tostr() << _C_MAG <<
            "[ Test sticky.partitioning.linger.ms=" << sticky_delay_ms <<
            ", partition_cnt=" << partition_cnt <<
            ", expected active_cnt=" << min_active_cnt << ".." <<
            max_active_cnt << " ]\n");

  std::string topic = Test::mk_topic_name(__FILE__, 1);
  Test::create_topic(NULL, topic.c_str(), partition_cnt, 1);

  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, 0);

  Test::conf_set(conf, "sticky.partitioning.linger.ms",
                 tostr() << sticky_delay_ms);

  std::string errstr;
  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
          Test::Fail("Failed to create Producer: " + errstr);

  int msgcnt = 0;
  const int msgsize = 10;

  /* Produce messages */
  char val[msgsize];
  memset(val, 'a', msgsize);

  /* Prime the producer to avoid any startup/metadata acquisition times */
  RdKafka::ErrorCode err = p->produce(topic, RdKafka::Topic::PARTITION_UA,
                                      RdKafka::Producer::RK_MSG_COPY,
                                      val, msgsize, NULL, 0, -1, NULL);
  if (err)
    Test::Fail("Produce failed: " + RdKafka::err2str(err));
  p->flush(5*1000);

  /* Produce messages */
  test_timing_t t_produce;
  TIMING_START(&t_produce, "produce");

  int64_t ts_end = test_clock() + (produce_time_ms * 1000);
  while (test_clock() < ts_end) {
    err = p->produce(topic, RdKafka::Topic::PARTITION_UA,
                     RdKafka::Producer::RK_MSG_COPY,
                     val, msgsize, NULL, 0, -1, NULL);
    if (err)
      Test::Fail("Produce failed: " + RdKafka::err2str(err));

    msgcnt++;

    /* Produce 1000 msgs/s */
    rd_usleep(1000000/1000, NULL);
  }
  TIMING_STOP(&t_produce);

  Test::Say(tostr() << "Produced " << msgcnt << " messages\n");
  p->flush(5*1000);
  delete p;

  /* Consume messages */
  Test::conf_set(conf, "group.id", topic);
  RdKafka::KafkaConsumer *c = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!c)
          Test::Fail("Failed to create Consumer: " + errstr);

  delete conf;

  std::vector<RdKafka::TopicPartition*> partitions;
  for (int i = 0 ; i < partition_cnt ; i++)
    partitions.push_back(RdKafka::TopicPartition::create(topic, i,
                                                         RdKafka::Topic::OFFSET_BEGINNING));

  err = c->assign(partitions);
  if (err)
    Test::Fail("Failed to assign partitions: " + RdKafka::err2str(err));
  RdKafka::TopicPartition::destroy(partitions);

  std::vector<int> partition_msgcnt(partition_cnt);

  int recv_cnt = 0;
  while (recv_cnt < msgcnt) {

    RdKafka::Message *msg = c->consume(1000);

    if (!msg)
      continue;

    switch (msg->err())
    {
        case RdKafka::ERR__TIMED_OUT:
          break;

        case RdKafka::ERR_NO_ERROR:
          partition_msgcnt[msg->partition()]++;
          recv_cnt++;
          break;

        default:
          Test::Fail("Consume error: " + msg->errstr());
          break;
    }

    delete msg;

    if (!(recv_cnt % 1000))
      Test::Say(tostr() << recv_cnt << " messages consumed\n");
  }

  TEST_ASSERT(recv_cnt == msgcnt);

  delete c;

  /* For the long delay segment of this test, partitions that receive a
   * small portion (< 10%) of all messages are not deemed 'active'. This
   * is because while topics are being updated, it is possible for some
   * number of messages to be partitioned to joining partitions before
   * they become available. This can cause some initial turnover in
   * selecting a sticky partition. This behavior is acceptable, and is
   * not important for the purpose of this segment of the test.
   * We thus filter out any partition that has less than 10% of its
   * expected message count. */
  int exp_msg_cnt =
    (int)(((double)max_active_cnt * msgcnt * 0.1) / partition_cnt);
  int num_partitions_active = 0;
  Test::Say(tostr() <<
            "Partition Message Count (requires at least " << exp_msg_cnt <<
            " to be considered active):\n");
  for (int i = 0; i < partition_cnt; i++) {
    if (partition_msgcnt[i] > exp_msg_cnt)
      num_partitions_active++;
    Test::Say(tostr() << " " << i << ": " << partition_msgcnt[i] << "\n");
  }

  if (num_partitions_active < min_active_cnt ||
      num_partitions_active > max_active_cnt)
    Test::Fail(tostr()
               << "Expected " << min_active_cnt << ".." <<
               max_active_cnt << " active partitions "
               << "to receive msgs but " << num_partitions_active
               << " partitions received msgs");
}

extern "C" {
  int main_0114_sticky_partitioning (int argc, char **argv) {

    /* When sticky.partitioning.linger.ms is long (greater than expected
     * length of run), one partition should be sticky and receive messages.
     * sticky.partitioning.linger.ms=100s, producetime=5s */
    do_test_sticky_partitioning(100*1000, 5*1000, 3, 1, 1);

    /* When sticky.partitioning.linger.ms is short (sufficiently smaller than
     * length of run), it is extremely likely that all partitions are sticky
     * at least once and receive messages.
     * sticky.partitioning.linger.ms=1ms, producetime=5s */
    do_test_sticky_partitioning(1, 5*1000, 3, 3, 3);

    /* And finally verify the middle case where the sticky time only allows
     * 2 partitions to be hit, but since the partition is picked randomly
     * it is possible for it to pick the same partition again, so allow
     * a span of 1..2 partitions.
     * sticky.partitioning.linger.ms=2s, producetime=3s */
    do_test_sticky_partitioning(2*1000, 3*1000, 10, 1, 2);

    return 0;
  }
}
