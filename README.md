librdkafka - Apache Kafka C client library
==========================================

Copyright (c) 2012-2013, [Magnus Edenhill](http://www.edenhill.se/), et.al.

[https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka)

**librdkafka** is a C library implementation of the
[Apache Kafka](http://incubator.apache.org/kafka/) protocol, containing both
Producer and Consumer support.

**WORK IN PROGRESS**

**NOTE: This is a development branch for Kafka protocol 0.8 support.**

**It is functional but comes with the following limitations:**

  * Only producer support, no consumer support.
  * No compression support
  * Only supports protocol 0.8, not 0.7.
  * API is not compatible with librdkafka 0.7
  * Work in progress, APIs may change.


**librdkafka** is licensed under the 2-clause BSD license.

And finally, dont hesitate to tell me if you are using librdkafka.


# Usage

## Requirements
	The GNU toolchain
   	pthreads
	zlib

## Instructions

### Building

      make all
      sudo make install
      # or to install in another location than /usr/local, set DESTDIR env
      # to the filesystem root of your choice.
      sudo make DESTDIR=/usr make install


### Usage in code

See `examples/rdkafka_performance.c` for an example producer


      #include <librdkafka/rdkafka.h>

      ..

      rd_kafka_t *rk;
      rd_kafka_conf_t conf;
      rd_kafka_topic_t *rkt;
      rd_kafka_topic_conf_t topic_conf;
      char errstr[512];

      /* Base our Kafka configuration on the default configuration */
      rd_kafka_defaultconf_set(&conf);
      conf.error_cb       = error_cb;
      conf.producer.dr_cb = msg_delivered_cb;

      if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, &conf,
                              errstr, sizeof(errstr)))) {
            printf("Kafka initialization failed: %s\n", errstr);
            exit(1);
      }

      /* Base our topic configuration on the default topic configuration */
      rd_kafka_topic_defaultconf_set(&topic_conf);
      topic_conf.required_acks      = 2;
      topic_conf.message_timeout_ms = 1000*5*60; /* 5 minutes */

      /* Create local handle for topic "testtopic" */
      if (!(rkt = rd_kafka_topic_new(rk, "testtopic", &topic_conf))) {
       	    printf("Failed to add topic: %s\n", strerror(errno));
	    exit(1);
      }

      /* Add list of initial brokers. All brokers will eventually be
       * discovered by quering these brokers for the full list of brokers. */
      if (!rd_kafka_brokers_add(rk, "localhost,remotehost1,remotehost2:9099")) {
            printf("No valid brokers specified\n");
            exit(1);
      }

      ...

      /* Produce message */
      if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA /* random partition */,
                           RD_KAFKA_MSG_F_COPY,
                           mydata, mydata_len,
                           mykey, mykey_len,
                           per_message_opaque) == -1) {
            /* Try again in a short while if queue is full, or drop,
	     * decision is left to the application. /
            if (errno == ENOBUFS)
               ... retry message later ...
      }

 
      ...

      /* Serve kafka events (error and delivery report callbacks) */
      rd_kafka_poll(rk, 1000/*ms*/);
      
      ...

      /* Decommission kafka handle */
      rd_kafka_destroy(rk);

    

Link your program with `-lrdkafka -lz -lpthread -lrt`.


## Documentation

The API is documented in `rdkafka.h`

## Examples

See the `examples/`sub-directory.


