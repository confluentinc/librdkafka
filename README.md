librdkafka - Apache Kafka C client library
==========================================

Copyright (c) 2012-2013, [Magnus Edenhill](http://www.edenhill.se/), et.al.

[https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka)

[![endorse](http://api.coderwall.com/edenhill/endorsecount.png)](http://coderwall.com/edenhill)


**librdkafka** is a C implementation of the
[Apache Kafka](http://incubator.apache.org/kafka/) protocol, containing both
Producer and Consumer support.
It currently supports Apache Kafka version 0.7.* (and possibly earlier), version 0.8 support will be available Q2 2013.
**librdkafka** has been in use in production since 2012.

ZooKeeper integration is planned but currently not available.

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
      make install
      # or to install in another location than /usr/local:
      PREFIX=/my/prefix make install


### Usage in code

See `examples/rdkafka_example.c` for full examples of both
producer and consumer sides.


      #include <librdkafka/rdkafka.h>

      ..

      rd_kafka_t *rk;

      rk = rd_kafka_new_consumer(broker, topic, partition, 0, &conf)

      while (run) {      
          rko = rd_kafka_consume(rk, RD_POLL_INFINITE);
          if (rko->rko_err)
            ..errhandling..
          else if (rko->rko_len)
            handle_message(rko->rko_payload, rko->rko_len);
      }

      rd_kafka_destroy(rk);

    

Link your program with `-lrdkafka -lz -lpthread -lrt`.


## Documentation

The API is documented in `rdkafka.h`

## Examples

See the `examples/`sub-directory.


