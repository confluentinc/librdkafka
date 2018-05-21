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

#if WITH_RAPIDJSON
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#endif


#if WITH_RAPIDJSON
static void verify_json (const std::string &stats_str) {
  /* Read schema from from file. */

  std::ifstream f("../src/statistics_schema.json");
  std::string schema_str(std::ifstreambuf_iterator<char>(f),
                         std::ifstreambuf_iterator<char>());

  Document sd;

  if (sd.Parse(schema_str).HasParseError())
    Test::Fail("Failed to parse statistics schema"); // FIXME error str

  SchemaDocument schema(sd);

  Document d;

  if (d.Parse(stats_str).HasParseError())
    Test::Fail("Failed to parse stats JSON"); // FIXME error str

  SchemaValidator validator(schema);
  if (!d.Accept(validator))

}
#endif

class myEventCb : public RdKafka::EventCb {
 public:
  myEventCb(bool verify) {
    stats_cnt = 0;
    verify_ = verify;
  }
  int stats_cnt;
  bool verify_;
  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_STATS:
        Test::Say(tostr() << "Stats (#" << stats_cnt << "): " <<
                  event.str() << "\n");
        if (event.str().length() > 20)
          stats_cnt += 1;
        if (verify_)
          verify_json(event.str());
        break;
      default:
        break;
    }
  }

 private:
};

/**
 * @brief Verify that stats are emitted according to statistics.interval.ms
 */
void test_stats_timing () {
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  myEventCb my_event = myEventCb(false);
  std::string errstr;

  if (conf->set("statistics.interval.ms", "100", errstr) != RdKafka::Conf::CONF_OK)
          Test::Fail(errstr);

  if (conf->set("event_cb", &my_event, errstr) != RdKafka::Conf::CONF_OK)
          Test::Fail(errstr);

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
          Test::Fail("Failed to create Producer: " + errstr);
  delete conf;

  int64_t t_start = test_clock();

  while (my_event.stats_cnt < 12)
    p->poll(1000);

  int elapsed = (int)((test_clock() - t_start) / 1000);
  const int expected_time = 1200;

  Test::Say(tostr() << my_event.stats_cnt << " (expected 12) stats callbacks received in " <<
            elapsed << "ms (expected " << expected_time << "ms +-25%)\n");

  if (elapsed < expected_time * 0.75 ||
      elapsed > expected_time * 1.25) {
    /* We can't rely on CIs giving our test job enough CPU to finish
     * in time, so don't error out even if the time is outside the window */
    if (test_on_ci)
      Test::Say(tostr() << "WARNING: Elapsed time " << elapsed << "ms outside +-25% window (" <<
                expected_time << "ms), cnt " << my_event.stats_cnt);
    else
      Test::Fail(tostr() << "Elapsed time " << elapsed << "ms outside +-25% window (" <<
                 expected_time << "ms), cnt " << my_event.stats_cnt);
  }
  delete p;
}



#if WITH_RAPIDJSON

/**
 * @brief Verify stats JSON structure and individual metric fields.
 *
 * To capture as much verifiable data as possible we run a full
 * producer - consumer end to end test and verify that counters
 * and states are emitted accordingly.
 *
 * Requires RapidJSON (for parsing the stats).
 */
static void test_stats () {
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  myEventCb my_event = myEventCb(true);
  std::string errstr;

  if (conf->set("statistics.interval.ms", "100", errstr) != RdKafka::Conf::CONF_OK)
          Test::Fail(errstr);

  if (conf->set("event_cb", &my_event, errstr) != RdKafka::Conf::CONF_OK)
          Test::Fail(errstr);

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
          Test::Fail("Failed to create Producer: " + errstr);
  delete conf;

  int64_t t_start = test_clock();

  while (my_event.stats_cnt < 12)
    p->poll(1000);

  int elapsed = (int)((test_clock() - t_start) / 1000);
  const int expected_time = 1200;

  Test::Say(tostr() << my_event.stats_cnt << " (expected 12) stats callbacks received in " <<
            elapsed << "ms (expected " << expected_time << "ms +-25%)\n");

  if (elapsed < expected_time * 0.75 ||
      elapsed > expected_time * 1.25) {
    /* We can't rely on CIs giving our test job enough CPU to finish
     * in time, so don't error out even if the time is outside the window */
    if (test_on_ci)
      Test::Say(tostr() << "WARNING: Elapsed time " << elapsed << "ms outside +-25% window (" <<
                expected_time << "ms), cnt " << my_event.stats_cnt);
    else
      Test::Fail(tostr() << "Elapsed time " << elapsed << "ms outside +-25% window (" <<
                 expected_time << "ms), cnt " << my_event.stats_cnt);
  }
  delete p;
}
#endif

extern "C" {
  int main_0053_stats_local (int argc, char **argv) {
    //test_stats_timing();
    return 0;
  }

  int main_0053_stats (int argc, char **argv) {
#if WITH_RAPIDJSON
    test_stats();
#else
    //TEST_SKIP("RapidJSON not available: can't verify JSON stats\n");
#endif
    return 0;
  }
}
