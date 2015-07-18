/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014, Magnus Edenhill
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
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#include <getopt.h>


/*
 * Typically include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include "rdkafkacpp.h"



static bool run = true;

static void sigterm (int sig) {
  run = false;
}


class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb (RdKafka::Message &message) {
    std::cout << "Message delivery for (" << message.len() << " bytes): " <<
        message.errstr() << std::endl;
  }
};


class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
          run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
        break;

      default:
        std::cerr << "EVENT " << event.type() <<
            " (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;
    }
  }
};





/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class MyHashPartitionerCb : public RdKafka::PartitionerCb {
 public:
  int32_t partitioner_cb (const RdKafka::Topic *topic, const std::string *key,
                          int32_t partition_cnt, void *msg_opaque) {
    return djb_hash(key->c_str(), key->size()) % partition_cnt;
  }
 private:

  static inline unsigned int djb_hash (const char *str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0 ; i < len ; i++)
      hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
};



int main (int argc, char **argv) {
  std::string brokers = "localhost";
  std::string errstr;
  std::string topic_str;
  std::string mode;
  std::string debug;
  int32_t partition = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
  bool exit_eof = false;
  bool do_conf_dump = false;
  char opt;
  MyHashPartitionerCb hash_partitioner;

  /*
   * Create configuration objects
   */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);


  while ((opt = getopt(argc, argv, "PCt:p:b:z:qd:o:eX:AM:")) != -1) {
    switch (opt) {
    case 'P':
    case 'C':
      mode = opt;
      break;
    case 't':
      topic_str = optarg;
      break;
    case 'p':
      if (!strcmp(optarg, "random"))
        /* default */;
      else if (!strcmp(optarg, "hash")) {
        if (tconf->set("partitioner_cb", &hash_partitioner, errstr) !=
            RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
      } else
        partition = std::atoi(optarg);
      break;
    case 'b':
      brokers = optarg;
      break;
    case 'z':
      if (conf->set("compression.codec", optarg, errstr) !=
	  RdKafka::Conf::CONF_OK) {
	std::cerr << errstr << std::endl;
	exit(1);
      }
      break;
    case 'o':
      if (!strcmp(optarg, "end"))
	start_offset = RdKafka::Topic::OFFSET_END;
      else if (!strcmp(optarg, "beginning"))
	start_offset = RdKafka::Topic::OFFSET_BEGINNING;
      else if (!strcmp(optarg, "stored"))
	start_offset = RdKafka::Topic::OFFSET_STORED;
      else
	start_offset = strtoll(optarg, NULL, 10);
      break;
    case 'e':
      exit_eof = true;
      break;
    case 'd':
      debug = optarg;
      break;
    case 'M':
      if (conf->set("statistics.interval.ms", optarg, errstr) !=
          RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
      }
      break;
    case 'X':
      {
	char *name, *val;

	if (!strcmp(optarg, "dump")) {
	  do_conf_dump = true;
	  continue;
	}

	name = optarg;
	if (!(val = strchr(name, '='))) {
          std::cerr << "%% Expected -X property=value, not " <<
              name << std::endl;
	  exit(1);
	}

	*val = '\0';
	val++;

	/* Try "topic." prefixed properties on topic
	 * conf first, and then fall through to global if
	 * it didnt match a topic configuration property. */
        RdKafka::Conf::ConfResult res;
	if (!strncmp(name, "topic.", strlen("topic.")))
          res = tconf->set(name+strlen("topic."), val, errstr);
        else
	  res = conf->set(name, val, errstr);

	if (res != RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
	  exit(1);
	}
      }
      break;

    default:
      goto usage;
    }
  }

  if (mode.empty() || topic_str.empty() || optind != argc) {
  usage:
    fprintf(stderr,
            "Usage: %s [-C|-P] -t <topic> "
            "[-p <partition>] [-b <host1:port1,host2:port2,..>]\n"
            "\n"
            "librdkafka version %s (0x%08x)\n"
            "\n"
            " Options:\n"
            "  -C | -P         Consumer or Producer mode\n"
            "  -t <topic>      Topic to fetch / produce\n"
            "  -p <num>        Partition (random partitioner)\n"
            "  -p <func>       Use partitioner:\n"
            "                  random (default), hash\n"
            "  -b <brokers>    Broker address (localhost:9092)\n"
            "  -z <codec>      Enable compression:\n"
            "                  none|gzip|snappy\n"
            "  -o <offset>     Start offset (consumer)\n"
            "  -e              Exit consumer when last message\n"
            "                  in partition has been received.\n"
            "  -d [facs..]     Enable debugging contexts:\n"
            "                  %s\n"
            "  -M <intervalms> Enable statistics\n"
            "  -X <prop=name>  Set arbitrary librdkafka "
            "configuration property\n"
            "                  Properties prefixed with \"topic.\" "
            "will be set on topic object.\n"
            "                  Use '-X list' to see the full list\n"
            "                  of supported properties.\n"
            "\n"
            " In Consumer mode:\n"
            "  writes fetched messages to stdout\n"
            " In Producer mode:\n"
            "  reads messages from stdin and sends to broker\n"
            "\n"
            "\n"
            "\n",
	    argv[0],
	    RdKafka::version_str().c_str(), RdKafka::version(),
	    RdKafka::Conf::DEBUG_CONTEXTS.c_str());
    exit(1);
  }


  /*
   * Set configuration properties
   */
  conf->set("metadata.broker.list", brokers, errstr);

  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
  }

  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);


  if (do_conf_dump) {
    int pass;

    for (pass = 0 ; pass < 2 ; pass++) {
      std::list<std::string> *dump;
      if (pass == 0) {
        dump = conf->dump();
        std::cout << "# Global config" << std::endl;
      } else {
        dump = tconf->dump();
        std::cout << "# Topic config" << std::endl;
      }

      for (std::list<std::string>::iterator it = dump->begin();
           it != dump->end(); ) {
        std::cout << *it << " = ";
        it++;
        std::cout << *it << std::endl;
        it++;
      }
      std::cout << std::endl;
    }
    exit(0);
  }

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);


  if (mode == "P") {
    /*
     * Producer mode
     */
    ExampleDeliveryReportCb ex_dr_cb;

    /* Set delivery report callback */
    conf->set("dr_cb", &ex_dr_cb, errstr);

    /*
     * Create producer using accumulated global configuration.
     */
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
      std::cerr << "Failed to create producer: " << errstr << std::endl;
      exit(1);
    }

    std::cout << "% Created producer " << producer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic = RdKafka::Topic::create(producer, topic_str,
						   tconf, errstr);
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

    /*
     * Read messages from stdin and produce to broker.
     */
    for (std::string line; run and std::getline(std::cin, line);) {
      if (line.empty()) {
        producer->poll(0);
	continue;
      }

      /*
       * Produce message
       */
      RdKafka::ErrorCode resp =
	producer->produce(topic, partition,
			  RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
			  const_cast<char *>(line.c_str()), line.size(),
			  NULL, NULL);
      if (resp != RdKafka::ERR_NO_ERROR)
	std::cerr << "% Produce failed: " <<
	  RdKafka::err2str(resp) << std::endl;
      else
	std::cerr << "% Produced message (" << line.size() << " bytes)" <<
	  std::endl;

      producer->poll(0);
    }
    run = true;

    while (run and producer->outq_len() > 0) {
      std::cerr << "Waiting for " << producer->outq_len() << std::endl;
      producer->poll(1000);
    }

    delete topic;
    delete producer;


  } else {
    /*
     * Consumer mode
     */

    /*
     * Create consumer using accumulated global configuration.
     */
    RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
    if (!consumer) {
      std::cerr << "Failed to create consumer: " << errstr << std::endl;
      exit(1);
    }

    std::cout << "% Created consumer " << consumer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_str,
						   tconf, errstr);
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

    /*
     * Start consumer for topic+partition at start offset
     */
    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to start consumer: " <<
	RdKafka::err2str(resp) << std::endl;
      exit(1);
    }

    /*
     * Consume messages
     */
    while (run) {
      RdKafka::Message *msg = consumer->consume(topic, partition, 1000);

      switch (msg->err())
      {
        case RdKafka::ERR__TIMED_OUT:
          break;

        case RdKafka::ERR_NO_ERROR:
	  /* Real message */
	  std::cerr << "Read msg at offset " << msg->offset() << std::endl;
	  if (msg->key()) {
		  std::cerr << "Key: " << *msg->key() << std::endl;
	  }
          printf("%.*s\n",
                 static_cast<int>(msg->len()),
                 static_cast<const char *>(msg->payload()));
	  break;

	case RdKafka::ERR__PARTITION_EOF:
	  /* Last message */
	  if (exit_eof)
	    run = false;
	  break;

	default:
	  /* Errors */
	  std::cerr << "Consume failed: " << msg->errstr() << std::endl;
	  run = false;
	}

      delete msg;

      consumer->poll(0);
    }

    /*
     * Stop consumer
     */
    consumer->stop(topic, partition);

    consumer->poll(1000);

    delete topic;
    delete consumer;
  }


  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (when check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  RdKafka::wait_destroyed(5000);

  return 0;
}
