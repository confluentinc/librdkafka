librdkafka - Apache Kafka C/C++ client library
==============================================

Copyright (c) 2012-2015, [Magnus Edenhill](http://www.edenhill.se/).

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

Version support

 | 0.9.x | 0.8.x | 0.7.x
----|-------|-------|------
Branch | master | 0.8 | 0.7
Producer | Supported | Supported | Supported
High-level balanced KafkaConsumer | Supported | - | -
Simple Consumer | Supported | Supported | Supported
Compression | Snappy, GZIP | Snappy, GZIP | -
Broker Version | 0.8+ | 0.8+ | 0.7
Debian Package | librdkafka1, librdkafka-dev | librdkafka1, librdkafka-dev | -
Zookeeper | - | - | -
C API | Stable, ABi safe, backwards compatible with 0.8 | Stable, ABI safe, not backwards compatible with 0.7 | -
C++ API | Stable | Testing | -
Tests | Regression tests in `tests/` directory | Regression tests in `tests/` directory | -
Statistics | JSON formatted, see `rd_kafka_conf_set_stats_cb` in `rdkafka.h` | JSON formatted, see `rd_kafka_conf_set_stats_cb` in `rdkafka.h` | -
Status | Testing | Stable | Stable, Deprecated, Unsupported


#Language bindings#

  * Haskell: [haskakafka](https://github.com/cosbynator/haskakafka)
  * Haskell: [haskell-kafka](https://github.com/yanatan16/haskell-kafka)
  * Node.js: [node-kafka](https://github.com/sutoiku/node-kafka)
  * Node.js: [kafka-native](https://github.com/jut-io/node-kafka-native)
  * Lua: [luardkafka](https://github.com/mistsv/luardkafka)
  * OCaml: [ocaml-kafka](https://github.com/didier-wenzek/ocaml-kafka)
  * PHP: [phpkafka](https://github.com/EVODelavega/phpkafka)
  * PHP: [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka)
  * Python: [python-librdkafka](https://bitbucket.org/yungchin/python-librdkafka)
  * Python: [PyKafka](https://github.com/Parsely/pykafka)
  * Ruby: [Hermann](https://github.com/stancampbell3/Hermann)
  * Tcl: [KafkaTcl](https://github.com/flightaware/kafkatcl)

#Users of librdkafka#

  * [kafkacat](https://github.com/edenhill/kafkacat) - Apache Kafka swiss army knife
  * [Wikimedia's varnishkafka](https://github.com/wikimedia/varnishkafka) - Varnish cache web log producer
  * [Wikimedia's kafkatee](https://github.com/wikimedia/analytics-kafkatee) - Kafka multi consumer with filtering and fanout
  * [rsyslog](http://www.rsyslog.com)
  * [syslog-ng](http://syslog-ng.org)
  * [collectd](http://collectd.org)
  * [logkafka](https://github.com/Qihoo360/logkafka) - Collect logs and send to Kafka
  * [redBorder](http://www.redborder.net)
  * [Headweb](http://www.headweb.com/)
  * [Produban's log2kafka](https://github.com/Produban/log2kafka) - Web log producer
  * [fuse_kafka](https://github.com/yazgoo/fuse_kafka) - FUSE file system layer
  * [node-kafkacat](https://github.com/Rafflecopter/node-kafkacat)
  * [OVH](http://ovh.com) - [AntiDDOS](http://www.slideshare.net/hugfrance/hugfr-6-oct2014ovhantiddos)
  * [otto.de](http://otto.de)'s [trackdrd](https://github.com/otto-de/trackrdrd) - Varnish log reader
  * [Microwish](https://github.com/microwish) has a range of Kafka utilites for log aggregation, HDFS integration, etc.
  * large unnamed financial institution
  * *Let [me](mailto:rdkafka@edenhill.se) know if you are using librdkafka*



# Usage

## Requirements
	The GNU toolchain
	GNU make
   	pthreads
	zlib (optional, for gzip compression support)
	libssl-dev (optional, for SSL support)
	libsasl2-dev (optional, for SASL support)

## Instructions

### Building

      ./configure
      make
      sudo make install


**NOTE**: See [README.win32](README.win32) for instructions how to build
          on Windows with Microsoft Visual Studio.

### Usage in code

See [examples/rdkafka_example.c](https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_example.c) for an example producer and consumer.

Link your program with `-lrdkafka -lz -lpthread -lrt`.


## Documentation

The public APIs are documented in their respective header files:
 * The **C** API is documented in [src/rdkafka.h](src/rdkafka.h)
 * The **C++** API is documented in [src-cpp/rdkafkacpp.h](src-cpp/rdkafkacpp.h)

To generate Doxygen documents for the API, type:

    make docs


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
