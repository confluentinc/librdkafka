librdkafka - Apache Kafka C/C++ client library
==============================================

Copyright (c) 2012-2014, [Magnus Edenhill](http://www.edenhill.se/).

[https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka)

**librdkafka** is a C library implementation of the
[Apache Kafka](http://kafka.apache.org/) protocol, containing both
Producer and Consumer support. It was designed with message delivery reliability
and high performance in mind, current figures exceed 800000 msgs/second for
the producer and 3 million msgs/second for the consumer.

**librdkafka** is licensed under the 2-clause BSD license.

For an introduction to the performance and usage of librdkafka, see
[INTRODUCTION.md](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md)

See the [wiki](https://github.com/edenhill/librdkafka/wiki) for a FAQ.

**NOTE**: The `master` branch is actively developed, use latest release for production use.

[![Gitter chat](https://badges.gitter.im/edenhill/librdkafka.png)](https://gitter.im/edenhill/librdkafka)

**Apache Kafka 0.8 support:**

  * Branch: master
  * Producer: supported
  * Consumer: supported
  * Compression: snappy and gzip
  * Debian package: librdkafka1 and librdkafka-dev in Debian and Ubuntu
  * ZooKeeper: not supported
  * C API: Stable, ABI safe, not backwards compatible with 0.7
  * C++ API: Testing
  * Tests: Regression tests in `tests/` directory.
  * Statistics: JSON formatted, see `rd_kafka_conf_set_stats_cb` in `rdkafka.h`.
  * Status: Stable


**Apache Kafka 0.7 support:**

  * Branch: 0.7
  * Producer: supported
  * Consumer: supported
  * Compression: not supported
  * ZooKeeper: not supported
  * C API: backwards compatible with 0.6
  * Status: Stable


**Apache Kafka 0.6 support:**

  * Branch: 0.6
  * Producer: supported
  * Consumer: supported
  * Compression: not supported
  * ZooKeeper: not supported
  * Status: Testing




#Language bindings#

  * Haskell: [haskakafka](https://github.com/cosbynator/haskakafka)
  * Haskell: [haskell-kafka](https://github.com/yanatan16/haskell-kafka)
  * Node.js: [node-kafka](https://github.com/sutoiku/node-kafka)
  * Lua: [in progress](https://github.com/edenhill/librdkafka/issues/196)
  * OCaml: [ocaml-kafka](https://github.com/didier-wenzek/ocaml-kafka)
  * PHP: [phpkafka](https://github.com/salebab/phpkafka)
  * Python: [python-librdkafka](https://bitbucket.org/yungchin/python-librdkafka)
  * Python: [PyKafka](https://github.com/Parsely/pykafka)
  * Ruby: [Hermann](https://github.com/stancampbell3/Hermann)

#Users of librdkafka#

  * [kafkacat](https://github.com/edenhill/kafkacat) - Apache Kafka swiss army knife
  * [Wikimedia's varnishkafka](https://github.com/wikimedia/varnishkafka) - Varnish cache web log producer
  * [Wikimedia's kafkatee](https://github.com/wikimedia/analytics-kafkatee) - Kafka multi consumer with filtering and fanout
  * [rsyslog](http://www.rsyslog.com)
  * [syslog-ng](http://syslog-ng.org)
  * [collectd](http://collectd.org)
  * [redBorder](http://www.redborder.net)
  * [Headweb](http://www.headweb.com/)
  * [Produban's log2kafka](https://github.com/Produban/log2kafka) - Web log producer
  * [fuse_kafka](https://github.com/yazgoo/fuse_kafka) - FUSE file system layer
  * [node-kafkacat](https://github.com/Rafflecopter/node-kafkacat)
  * [OVH](http://ovh.com) - [AntiDDOS](http://www.slideshare.net/hugfrance/hugfr-6-oct2014ovhantiddos)
  * large unnamed financial institution
  * *Let [me](mailto:rdkafka@edenhill.se) know if you are using librdkafka*



# Usage

## Requirements
	The GNU toolchain
	GNU make
   	pthreads
	zlib

## Instructions

### Building

      ./configure
      make
      sudo make install


### Usage in code

See [examples/rdkafka_example.c](https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_example.c) for an example producer and consumer.

Link your program with `-lrdkafka -lz -lpthread -lrt`.


## Documentation

The **C** API is documented in [src/rdkafka.h](src/rdkafka.h)

The **C++** API is documented in [src-cpp/rdkafkacpp.h](src-cpp/rdkafkacpp.h)

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
