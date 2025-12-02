Confluent's JavaScript Client for Schema Registry<sup>TM</sup>
=====================================================

Confluent's JavaScript client for [Schema Registry](https://docs.confluent.io/cloud/current/sr/index.html) supports Avro, Protobuf and JSON Schema, and is designed to work with
[Confluent's JavaScript Client for Apache Kafka](https://www.npmjs.com/package/@confluentinc/kafka-javascript).
The goal is to provide a highly performant, reliable and easy to use JavaScript client in line with other Schema Registry clients
such as our [Go](https://github.com/confluentinc/confluent-kafka-go), [.NET](https://github.com/confluentinc/confluent-kafka-dotnet),
and [Java](https://github.com/confluentinc/schema-registry) clients.

## Installation
```bash
npm install @confluentinc/schemaregistry
```

## Getting Started
Below is a simple example of using Avro serialization with the Schema Registry client and the KafkaJS client.
```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient, SerdeType, AvroSerializer, AvroDeserializer} = require('@confluentinc/schemaregistry');

const registry = new SchemaRegistryClient({ baseURLs: ['http://localhost:8081'] })
const kafka = new Kafka({
  kafkaJS: {
    brokers: ['localhost:9092']
  }
});

let consumer = kafka.consumer({
  kafkaJS: {
    groupId: "test-group",
    fromBeginning: true,
  },
});
let producer = kafka.producer();

const schema = {
  type: 'record',
  namespace: 'examples',
  name: 'RandomTest',
  fields: [
    { name: 'fullName', type: 'string' }
  ],
};

const topicName = 'test-topic';
const subjectName = topicName + '-value';

const run = async () => {
  // Register schema
  const id = await registry.register(
    subjectName,
    {
      schemaType: 'AVRO',
      schema: JSON.stringify(schema)
    }
  )

  // Create an Avro serializer
  const ser = new AvroSerializer(registry, SerdeType.VALUE, { useLatestVersion: true });

  // Produce a message with the schema
  await producer.connect()
  const outgoingMessage = {
    key: 'key',
    value: await ser.serialize(topicName, { fullName: 'John Doe' }),
  }
  await producer.send({
    topic: topicName,
    messages: [outgoingMessage]
  });
  console.log("Producer sent its message.")
  await producer.disconnect();
  producer = null;

  // Create an Avro deserializer
  const deser = new AvroDeserializer(registry, SerdeType.VALUE, {});

  await consumer.connect()
  await consumer.subscribe({ topic: topicName })

  let messageRcvd = false;
  await consumer.run({
    eachMessage: async ({ message }) => {
      const decodedMessage = {
        ...message,
        value: await deser.deserialize(topicName, message.value)
      };
      console.log("Consumer received message.\nBefore decoding: " + JSON.stringify(message) + "\nAfter decoding: " + JSON.stringify(decodedMessage));
      messageRcvd = true;
    },
  });

  // Wait around until we get a message, and then disconnect.
  while (!messageRcvd) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  await consumer.disconnect();
  consumer = null;
}

run().catch (async e => {
  console.error(e);
  consumer && await consumer.disconnect();
  producer && await producer.disconnect();
  process.exit(1);
})
```

## Features and Limitations
- Avro, Protobuf, and JSON Schema support
- Support for CSFLE (Client-Side Field Level Encryption)
- Support for data quality rules
- Support for schema migration rules for Avro and JSON Schema
- Support for OAuth

## Contributing

Bug reports and feedback is appreciated in the form of Github Issues.
For guidelines on contributing please see [CONTRIBUTING.md](CONTRIBUTING.md)
