Confluent's Javascript Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-javascript** is Confluent's JavaScript client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/). This is an **limited availability** library. The goal is to provide an highly performant, reliable and easy to use JavaScript client that is based on [node-rdkafka](https://github.com/Blizzard/node-rdkafka) yet also API compatible with [KafkaJS](https://github.com/tulios/kafkajs) to provide flexibility to users and streamline migrations from other clients.

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

### This library is currently in limited-availability

To use **Schema Registry**, use the existing [@confluentinc/schemaregistry](https://www.npmjs.com/package/@confluentinc/schemaregistry) library that is compatible with this library. For a simple schema registry example, see [sr.js](https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/examples/kafkajs/sr.js).


## Requirements

The following configurations are supported:

* Any supported version of Node.js (The two LTS versions, 18 and 20, and the latest versions, 21 and 22).
* Linux (x64 and arm64) - both glibc and musl/alpine.
* macOS - arm64/m1.
* Windows - x64.

Installation on any of these platforms is meant to be seamless, without any C/C++ compilation required.

In case your system configuration is not within the supported ones, [a supported version of Python](https://devguide.python.org/versions/) must be available on the system for the installation process. [This is required for the `node-gyp` build tool.](https://github.com/nodejs/node-gyp?tab=readme-ov-file#configuring-python-dependency).

```bash
npm install @confluentinc/kafka-javascript
```

Yarn and pnpm support is experimental.

# Getting Started

Below is a simple produce example for users migrating from KafkaJS.

```javascript
// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;

async function producerStart() {
    const kafka = new Kafka({
        kafkaJS: {
            brokers: ['<fill>'],
            ssl: true,
            sasl: {
                mechanism: 'plain',
                username: '<fill>',
                password: '<fill>',
            },
        }
    });

    const producer = kafka.producer();

    await producer.connect();

    console.log("Connected successfully");

    const res = []
    for (let i = 0; i < 50; i++) {
        res.push(producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'v222', partition: 0 },
                { value: 'v11', partition: 0, key: 'x' },
            ]
        }));
    }
    await Promise.all(res);

    await producer.disconnect();

    console.log("Disconnected successfully");
}

producerStart();
```

1. If you're migrating from `kafkajs`, you can use the [migration guide](MIGRATION.md#kafkajs).
2. If you're migrating from `node-rdkafka`, you can use the [migration guide](MIGRATION.md#node-rdkafka).
3. If you're starting afresh, you can use the [quickstart guide](QUICKSTART.md).

An in-depth reference may be found at [INTRODUCTION.md](INTRODUCTION.md).

## Contributing

Bug reports and feedback is appreciated in the form of Github Issues.
For guidelines on contributing please see [CONTRIBUTING.md](CONTRIBUTING.md)
