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
 * Test batch.size producer property.
 *
 */

#include <iostream>
#include <fstream>
#include <iterator>
#include <string>
#include "testcpp.h"

#if WITH_RAPIDJSON
#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <rapidjson/error/en.h>


class myAvgStatsCb : public RdKafka::EventCb {
 public:
  myAvgStatsCb(std::string topic):
      avg_batchsize(0), min_batchsize(0), max_batchsize(0),
      max_batchcnt(0), topic_(topic) {}

  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_LOG:
        Test::Say(event.str() + "\n");
        break;
      case RdKafka::Event::EVENT_STATS:
        read_batch_stats(event.str());
        break;
      default:
        break;
    }
  }

  int avg_batchsize;
  int min_batchsize;
  int max_batchsize;
  int max_batchcnt;

 private:
  void read_val(rapidjson::Document &d, const std::string &path, int &val) {
    rapidjson::Pointer jpath(path.c_str());

    if (!jpath.IsValid())
      Test::Fail(tostr() << "json pointer parse " << path << " failed at "
                         << jpath.GetParseErrorOffset() << " with error code "
                         << jpath.GetParseErrorCode());

    rapidjson::Value *pp = rapidjson::GetValueByPointer(d, jpath);
    if (!pp) {
      Test::Say(tostr() << "Could not find " << path << " in stats\n");
      return;
    }

    val = pp->GetInt();
  }

  void read_batch_stats(const std::string &stats) {
    rapidjson::Document d;

    if (d.Parse(stats.c_str()).HasParseError())
      Test::Fail(tostr() << "Failed to parse stats JSON: "
                         << rapidjson::GetParseError_En(d.GetParseError())
                         << " at " << d.GetErrorOffset());

    read_val(d, "/topics/" + topic_ + "/batchsize/avg", avg_batchsize);
    read_val(d, "/topics/" + topic_ + "/batchsize/min", min_batchsize);
    read_val(d, "/topics/" + topic_ + "/batchsize/max", max_batchsize);
    read_val(d, "/topics/" + topic_ + "/batchcnt/max", max_batchcnt);
  }

  std::string topic_;
};


/**
 * @brief Specify batch.size and parse stats to verify it takes effect.
 *
 */
static void do_test_batch_size (const std::string &compression_type) {

  SUB_TEST_QUICK("compression_type %s", compression_type.c_str());

  std::string topic = Test::mk_topic_name(__FILE__, 0);

  myAvgStatsCb event_cb(topic);

  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, 0);

  bool with_compression = compression_type != "none";
  bool with_streaming_compression = with_compression &&
    compression_type != "snappy";
  const int msgcnt = 2000;
  const int msgsize = 1000;
  int batchsize = 5000;
  /* Compressed batches should allow for at least 10 times the number of
   * messages per batch (batchcnt == average number of messages per batch),
   * with no upper bound (1000x). */
  int exp_batchcnt = (batchsize / msgsize) *
    (with_streaming_compression ? 10 : 1);
  int exp_min_batchcnt = (int)((float)exp_batchcnt * 0.8);
  int exp_max_batchcnt = (int)((float)exp_batchcnt *
                               (with_streaming_compression ? 1000.0 : 1.2));
  /* With compression the last batch may be much smaller than the previous
   * filled ones and thus mess up the average batchsize metric.
   * So give it some extra lower leeway in case of compression. */
  int exp_min_batchsize = exp_batchcnt * msgsize *
    (with_compression ? 0.01 : 1.0);
  int exp_max_batchsize = exp_batchcnt * msgsize *
    (with_compression ? 0.2 : 1.1);

  if (compression_type != "none")
    exp_batchcnt /= 10;

  Test::conf_set(conf, "batch.size", tostr() << batchsize);

  /* Make sure batch.size takes precedence by setting the following high */
  Test::conf_set(conf, "batch.num.messages", "100000");
  Test::conf_set(conf, "linger.ms", "2000");
  Test::conf_set(conf, "compression.type", compression_type);

  Test::conf_set(conf, "statistics.interval.ms", "7000");
  std::string errstr;
  if (conf->set("event_cb", &event_cb, errstr) != RdKafka::Conf::CONF_OK)
    Test::Fail(errstr);

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
    Test::Fail("Failed to create Producer: " + errstr);

  /* Produce messages */
  char val[msgsize];
  memset(val, 'a', msgsize);

  for (int i = 0; i < msgcnt; i++) {
    RdKafka::ErrorCode err =
        p->produce(topic, 0, RdKafka::Producer::RK_MSG_COPY, val, msgsize, NULL,
                   0, -1, NULL);
    if (err)
      Test::Fail("Produce failed: " + RdKafka::err2str(err));
  }

  Test::Say(tostr() << "Produced " << msgcnt << " messages\n");
  p->flush(5 * 1000);

  Test::Say("Waiting for stats\n");
  while (event_cb.avg_batchsize == 0)
    p->poll(1000);

  Test::Say(tostr() << "Batchsize: "
                    << "configured " << batchsize << ", min "
                    << event_cb.min_batchsize << ", max "
                    << event_cb.max_batchsize << ", avg "
                    << event_cb.avg_batchsize << "\n");

  Test::Say(tostr() << "Batchcnt: max " << event_cb.max_batchcnt << "\n");


  /* The average batchsize should within a message size from batch.size. */
  if (event_cb.avg_batchsize < exp_min_batchsize ||
      event_cb.avg_batchsize > exp_max_batchsize)
    Test::Fail(tostr() << "Expected avg batchsize to be within " <<
               exp_min_batchsize << ".." << exp_max_batchsize <<
               " but got " << event_cb.avg_batchsize);

  /* All messages should fit one single batch with compression */
  if (event_cb.max_batchcnt < exp_min_batchcnt ||
      event_cb.max_batchcnt > exp_max_batchcnt)
    Test::Fail(tostr() << "Expected batch count to be within " <<
               exp_min_batchcnt << ".." << exp_max_batchcnt <<
               " but got " << event_cb.max_batchcnt);
  delete p;

  SUB_TEST_PASS();
}


#endif

extern "C" {
int main_0110_batch_size(int argc, char **argv) {
#if WITH_RAPIDJSON
    do_test_batch_size("none");
    do_test_batch_size("lz4");
    if (test_quick)
      return 0;

#if WITH_ZLIB
    do_test_batch_size("gzip");
#endif
#if WITH_SNAPPY
    do_test_batch_size("snappy");
#endif
#if WITH_ZSTD
    do_test_batch_size("zstd");
#endif
#else
  Test::Skip("RapidJSON >=1.1.0 not available\n");
#endif
  return 0;
}
}
