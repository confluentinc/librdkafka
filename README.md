Confluent's JavaScript Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-javascript** is Confluent's JavaScript client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/). The goal is to provide an highly performant, reliable and easy to use JavaScript client that is based on [node-rdkafka](https://github.com/Blizzard/node-rdkafka) yet also API compatible with [KafkaJS](https://github.com/tulios/kafkajs) to provide flexibility to users and streamline migrations from other clients.

Features:

- **High performance** - confluent-kafka-javascript is a lightweight wrapper around
[librdkafka](https://github.com/confluentinc/librdkafka), a finely tuned C
client.

- **Reliability** - There are a lot of details to get right when writing an Apache Kafka
client. We get them right in one place (librdkafka) and leverage this work
across all of our clients.

- **Supported** - Commercial support is offered by [Confluent](https://confluent.io/).

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/).

This library leverages the work and concepts from two popular Apache Kafka JavaScript clients: [node-rdkafka](https://github.com/Blizzard/node-rdkafka) and [KafkaJS](https://github.com/tulios/kafkajs). The core is heavily based on the node-rdkafka library, which uses our own [librdkafka](https://github.com/confluentinc/librdkafka) library for core client functionality. However, we leverage a promisified API and a more idiomatic interface, similar to the one in KafkaJS, making it easy for developers to migrate and adopt this client depending on the patterns and interface they prefer. We're very happy to have been able to leverage the excellent work of the many authors of these libraries!

To use **Schema Registry**, use the [@confluentinc/schemaregistry](https://www.npmjs.com/package/@confluentinc/schemaregistry) library that is compatible with this library. For a simple schema registry example, see [sr.js](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/examples/kafkajs/sr.js).

## Requirements

The following configurations are supported:

* Linux (x64 and arm64) - we support both musl and glibc based distributions:

| Distribution                              | Supported Node Versions |
| ----------------------------------------- | ----------------------- |
| Debian Bullseye/Ubuntu 20.04              | 18, 20, 21, 22, 23      |
| Debian Bookworm/Ubuntu 22.04              | 18, 20, 21, 22, 23      |
| Alpine Linux 3.20+                        | 18, 20, 21, 22, 23      |
| AlmaLinux 9/Rocky Linux 9/CentOS Stream 9 | 18, 20, 21, 22, 23      |

Other distributions will probably work given a modern version of gcc/glibc, but we don't test the pre-built binaries with them.

* macOS - arm64/m1. macOS (Intel) is supported on a best-effort basis. Node versions 18, 20, 21, 22, and 23 are supported.
* Windows - x64. Node versions 18, 20, 21, 22, and 23 are supported.

> [!WARNING]
> Pre-built binary support will be dropped after the EOL of the node version or the OS.

Installation on any of these platforms is meant to be seamless, without any C/C++ compilation required.

```bash
npm install @confluentinc/kafka-javascript
```

In case your system configuration is not within the supported ones, check the detailed [installation instructions](./INTRODUCTION.md#Installation-Instructions) for more information.

Yarn and pnpm support is experimental.

# Getting Started

Below is a simple produce example using the promisified API.

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
    const producer = new Kafka().producer({
        'bootstrap.servers': '<fill>',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '<fill>',
        'sasl.password': '<fill>',
    });

    await producer.connect();
    console.log("Connected successfully");

    const res = []
    for (let i = 0; i < 50; i++) {
        res.push(producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'v', partition: 0, key: 'x' },
            ]
        }));
    }
    await Promise.all(res);

    await producer.disconnect();
    console.log("Disconnected successfully");
}

producerStart();
```

There are two variants of the API offered by this library. A promisified API and a callback-based API.

1. If you're starting afresh, you should use the [promisified API](INTRODUCTION.md#promisified-api).
2. If you're migrating from `kafkajs`, you can use the [migration guide](MIGRATION.md#kafkajs) to get started quickly.
3. If you're migrating from `node-rdkafka`, you can use the [migration guide](MIGRATION.md#node-rdkafka).

An in-depth reference may be found at [INTRODUCTION.md](INTRODUCTION.md).

## Contributing

Bug reports and feedback is appreciated in the form of Github Issues.
For guidelines on contributing please see [CONTRIBUTING.md](CONTRIBUTING.md)

## Librdkafka Version

| confluent-kafka-javascript | librdkafka |
| -------------------------- | ---------- |
| 1.0.0                      | 2.6.1      |
| 1.2.0                      | 2.8.0      |
| 1.3.0                      | 2.10.0     |

This mapping is applicable if you're using a pre-built binary. Otherwise, you can check the librdkafka version with the following command:

```bash
node -e 'console.log(require("@confluentinc/kafka-javascript").librdkafkaVersion)'
```
