librdkafka - Apache Kafka C client library
==========================================

Copyright (c) 2012-2013, [Magnus Edenhill](http://www.edenhill.se/).

[https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka)

**librdkafka** is a C library implementation of the
[Apache Kafka](http://kafka.apache.org/) protocol, containing both
Producer and Consumer support. It was designed with message delivery reliability
and high performance in mind, current figures exceed 800000 msgs/second for
the producer and 3 million msgs/second for the consumer.

**librdkafka** is licensed under the 2-clause BSD license.

For an introduction to the performance and usage of librdkafka, see
[INTRODUCTION.md](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md)


**NOTE**: A C++ interface is currently in testing, see the `c++` branch.


**Apache Kafka 0.8 support:**

  * Branch: master
  * Producer: supported
  * Consumer: supported
  * Compression: snappy and gzip
  * Debian package: librdkafka1 and librdkafka-dev in Debian testing and unstable
  * ZooKeeper: not supported
  * API: Stable, not backwards compatible
  * Tests: Regression tests in `tests/` directory.
  * Statistics: JSON formatted, see `rd_kafka_conf_set_stats_cb` in `rdkafka.h`.
  * Status: Stable


**Apache Kafka 0.7 support:**

  * Branch: 0.7
  * Producer: supported
  * Consumer: supported
  * Compression: not supported
  * ZooKeeper: not supported
  * API: backwards compatible with 0.6
  * Status: Stable


**Apache Kafka 0.6 support:**

  * Branch: 0.6
  * Producer: supported
  * Consumer: supported
  * Compression: not supported
  * ZooKeeper: not supported
  * Status: Testing




#Users of librdkafka#

  * [Wikimedia's varnishkafka](https://github.com/wikimedia/varnishkafka)
  * [redBorder](http://www.redborder.net)
  * [Headweb](http://www.headweb.com/)
  * [node-kafka](https://github.com/sutoiku/node-kafka)
  * [Produban's log2kafka](https://github.com/Produban/log2kafka)
  * [phpkafka](https://github.com/salebab/phpkafka)
  * *Let [me](mailto:rdkafka@edenhill.se) know if you are using librdkafka*



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

See [examples/rdkafka_example.c](https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_example.c) for an example producer and consumer.

Link your program with `-lrdkafka -lz -lpthread -lrt`.


## Documentation

The API is documented in `rdkafka.h`

Configuration properties are documented in
[CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

For a librdkafka introduction, see
[INTRODUCTION.md](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md)


## Examples

See the `examples/`sub-directory.


## Tests

See the `tests/`sub-directory.


## Support

File bug reports, feature requests and questions using
[GitHub Issues](https://github.com/edenhill/librdkafka/issues)


Questions and discussions are also welcome on irc.freenode.org, #apache-kafka,
nickname Snaps.


### Commercial support

Commercial support is available from [Edenhill services](http://www.edenhill.se)
