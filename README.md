Confluent's Javascript Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-javascript** is Confluent's JavaScript client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/). This is an **early access** library. The goal is to provide an highly performant, reliable and easy to use JavaScript client that is based on [node-rdkafka](https://github.com/Blizzard/node-rdkafka) yet also API compatible with [KafkaJS](https://github.com/tulios/kafkajs) to provide flexibility to users and streamline migrations from other clients.

This library leverages the work and concepts from two popular Apache Kafka JavaScript clients: [node-rdkafka](https://github.com/Blizzard/node-rdkafka) and [KafkaJS](https://github.com/tulios/kafkajs). The core is heavily based on the node-rdkafka library, which uses our own [librdkafka](https://github.com/confluentinc/librdkafka/tree/v2.3.0) library for core client functionality. However, we leverage a promisified API and a more idiomatic interface, similar to the one in KafkaJS, making it easy for developers to migrate and adopt this client depending on the patterns and interface they prefer.
__This library currently uses `librdkafka` based off of the master branch.__

## This library is currently in early access and not meant for production use

**This library is in active development, pre-1.0.0, and it is likely to have many breaking changes.**

For this early-access release, we aim to get feedback from JavaScript developers within the Apache Kafka community to help meet your needs. Some areas of feedback we are looking for include:
- Usability of the API compared to other clients
- Migration experience from the node-rdkafka and KafkaJs
- Overall quality and reliability

We invite you to raise issues to highlight any feedback you may have.

Within the early-access, only **basic produce and consume functionality** as well as the ability to **create and delete topics** are supported. All other admin client functionality is coming in future releases. See [INTRODUCTION.md](INTRODUCTION.md) for more details on what is supported.

To use **Schema Registry**, use the existing [kafkajs/confluent-schema-registry](https://github.com/kafkajs/confluent-schema-registry) library that is compatible with this library. For a simple schema registry example, see [sr.js](https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/examples/kafkajs/sr.js). **DISCLAIMER:** Although it is compatible with **confluent-kafka-javascript**, Confluent does not own or maintain kafkajs/confluent-schema-registry, and the use and functionality of the library should be considered "as is".


## Requirements

The following configurations are supported for this early access preview:

* Any supported version of Node.js (The two LTS versions, 18 and 20, and the latest versions, 21 and 22).
* Linux (x64 and arm64) - both glibc and musl/alpine.
* macOS - arm64/m1.
* Windows - x64 (experimentally available in EA).

[A supported version of Python](https://devguide.python.org/versions/) must be available on the system for the installation process. [This is required for the `node-gyp` build tool.](https://github.com/nodejs/node-gyp?tab=readme-ov-file#configuring-python-dependency).

Installation on any of these platforms is meant to be seamless, without any C/C++ compilation required.

```bash
$ npm install @confluentinc/kafka-javascript
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

Bug reports and early-access feedback is appreciated in the form of Github Issues.
For guidelines on contributing please see [CONTRIBUTING.md](CONTRIBUTING.md)
