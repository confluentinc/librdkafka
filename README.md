Confluent's Javascript Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-js** is Confluent's Javascript client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/).


Features:

- **High performance** - confluent-kafka-js is a lightweight wrapper around
[librdkafka](https://github.com/confluentinc/librdkafka), a finely tuned C
client.

- **Reliability** - There are a lot of details to get right when writing an Apache Kafka
client. We get them right in one place (librdkafka) and leverage this work
across all of our clients (also [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python),
[confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) and
and [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet)).

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/compare/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/compare/).

## This library is currently not ready for production use. It's an early-access preview in active development, pre-1.0.0, and there might be breaking changes.

This library is based heavily on [node-rdkafka](https://github.com/Blizzard/node-rdkafka).

This library contains a promisified API, very similar to the one in [kafkajs](https://github.com/tulios/kafkajs). Some of the tests are also based on the ones in kafkajs.

__This library currently uses `librdkafka` based off of the master branch.__


## Requirements

The following configurations are supported for this early access preview:

* Any supported version of Node.js (The two LTS versions, 18 and 20, and the latest version, 21).
* Linux (x64 and arm64) - both glibc and musl/alpine.
* macOS - arm64/m1.

Installation on any of these platforms is meant to be seamless, without any C/C++ compilation required. It can be installed
from GitHub:

```bash
$ npm install "git+ssh://git@github.com/confluentinc/confluent-kafka-js.git#v0.1.6-devel"
```

Yarn and pnpm support is experimental.

# Getting Started

1. If you're migrating from `kafkajs`, you can use the [migration guide](MIGRATION.md#kafkajs).
2. If you're migrating from `node-rdkafka`, you can use the [migration guide](MIGRATION.md#node-rdkafka).
3. If you're starting afresh, you can use the [quickstart guide](QUICKSTART.md).

An in-depth reference may be found at [INTRODUCTION.md](INTRODUCTION.md).

## Contributing

Bug reports and early-access feedback is appreciated in the form of Github Issues.
For guidelines on contributing please see [CONTRIBUTING.md](CONTRIBUTING.md)
