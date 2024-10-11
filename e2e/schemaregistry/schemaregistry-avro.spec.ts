import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  Metadata,
  SchemaRegistryClient,
  SchemaInfo
} from '../../schemaregistry/schemaregistry-client';
import { beforeEach, afterEach, describe, expect, it } from '@jest/globals';
import { clientConfig } from '../../test/schemaregistry/test-constants';
import { AvroDeserializer, AvroSerializer, AvroSerializerConfig } from '../../schemaregistry/serde/avro';
import { SerdeType } from "../../schemaregistry/serde/serde";
import stringify from 'json-stringify-deterministic';
import { v4 } from 'uuid';

let schemaRegistryClient: SchemaRegistryClient;
let serializerConfig: AvroSerializerConfig;
let serializer: AvroSerializer;
let deserializer: AvroDeserializer;
let producer: KafkaJS.Producer;
let consumer: KafkaJS.Consumer;

const kafkaBrokerList = 'localhost:9092';
const kafka = new KafkaJS.Kafka({
  kafkaJS: {
    brokers: [kafkaBrokerList],
  },
});


const userSchemaString: string = stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
  ],
});

const messageValue = {
  "name": "Bob Jones",
  "age": 25
};

const metadata: Metadata = {
  properties: {
    owner: 'Bob Jones',
    email: 'bob@acme.com',
  },
};

const schemaInfo: SchemaInfo = {
  schema: userSchemaString,
  metadata: metadata
};

describe('Schema Registry Avro Integration Test', () => {

  beforeEach(async () => {
    schemaRegistryClient = new SchemaRegistryClient(clientConfig);

    producer = kafka.producer({
      kafkaJS: {
        allowAutoTopicCreation: true,
        acks: 1,
        compression: KafkaJS.CompressionTypes.GZIP,
      }
    });
    await producer.connect();

    consumer = kafka.consumer({
      kafkaJS: {
        groupId: 'test-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });
  });

  afterEach(async () => {
    await producer.disconnect();
  });

  it("Should serialize and deserialize Avro", async () => {
    const testTopic = v4();

    await schemaRegistryClient.register(testTopic + "-value", schemaInfo);

    serializerConfig = { useLatestVersion: true };
    serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
    deserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(testTopic, messageValue)
    };

    await producer.send({
      topic: testTopic,
      messages: [outgoingMessage]
    });

    await consumer.connect();
    await consumer.subscribe({ topic: testTopic });
    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(testTopic, message.value as Buffer)
        };
        messageRcvd = true;

        expect(decodedMessage.value).toMatchObject(messageValue);
      },
    });

    // Wait around until we get a message, and then disconnect.
    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should fail to serialize with useLatestVersion enabled and autoRegisterSchemas disabled', async () => {
    const testTopic = v4();

    serializerConfig = { autoRegisterSchemas: false, useLatestVersion: true };
    serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);

    const messageValue = { "name": "Bob Jones", "age": 25 };

    await expect(serializer.serialize(testTopic, messageValue)).rejects.toThrowError();
  });

  it('Should serialize with autoRegisterSchemas enabled and useLatestVersion disabled', async () => {
    const testTopic = v4();
    await schemaRegistryClient.register(testTopic +' -value', schemaInfo);

    serializerConfig = { autoRegisterSchemas: true, useLatestVersion: false };
    serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);

    const messageValue = { "name": "Bob Jones", "age": 25 };

    await serializer.serialize(testTopic, messageValue);
  });
  //TODO: Add test for Incompatible Types. The current Kafka Client runs console.error instead of throwing error
  //Should use a spy, Jest wasn't playing nice with the spy

  it('Should produce generic message to multiple topics', async () => {
    const topic1 = v4();
    const topic2 = v4();

    await schemaRegistryClient.register(topic1, schemaInfo);
    await schemaRegistryClient.register(topic2, schemaInfo);

    serializerConfig = { autoRegisterSchemas: true };
    serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
    deserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    const outgoingMessage1 = {
      key: 'key',
      value: await serializer.serialize(topic1, messageValue)
    };

    const outgoingMessage2 = {
      key: 'key',
      value: await serializer.serialize(topic2, messageValue)
    };

    await producer.send(
      { topic: topic1, messages: [outgoingMessage1] },
    );

    await producer.send(
      { topic: topic2, messages: [outgoingMessage2] },
    );

    let consumer2 = kafka.consumer({
      kafkaJS: {
        groupId: 'test-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });

    await consumer.connect();
    await consumer.subscribe({ topic: topic1 });
    await consumer2.connect();
    await consumer2.subscribe({ topic: topic2 });

    let messageRcvd = false;
    let messageRcvd2 = false;

    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic1, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toMatchObject(messageValue);
      },
    });

    await consumer2.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic2, message.value as Buffer)
        };
        messageRcvd2 = true;
        expect(decodedMessage.value).toMatchObject(messageValue);
      },
    });

    while (!messageRcvd || !messageRcvd2) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
    await consumer2.disconnect();
  }, 30000);
});

describe('Schema Registry Avro Integration Test - Primitives', () => {
  beforeEach(async () => {
    schemaRegistryClient = new SchemaRegistryClient(clientConfig);

    producer = kafka.producer({
      kafkaJS: {
        allowAutoTopicCreation: true,
        acks: 1,
        compression: KafkaJS.CompressionTypes.GZIP,
      }
    });
    await producer.connect();
    serializerConfig = { useLatestVersion: true };

    serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
    deserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});
    consumer = kafka.consumer({
      kafkaJS: {
        groupId: 'test-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });
  });

  afterEach(async () => {
    await producer.disconnect();
  });

  it('Should serialize and deserialize string', async () => {
    const stringTopic = v4();

    const stringSchemaString = stringify({
      type: 'string',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: stringSchemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(stringTopic + "-value", stringSchemaInfo);

    const stringMessageValue = "Hello, World!";
    const outgoingStringMessage = {
      key: 'key',
      value: await serializer.serialize(stringTopic, stringMessageValue)
    };

    await producer.send({
      topic: stringTopic,
      messages: [outgoingStringMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic: stringTopic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(stringTopic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(stringMessageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize bytes', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'bytes',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = Buffer.from("Hello, World!");
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize int', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'int',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = 25;
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize long', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'long',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = 25;
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize boolean', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'boolean',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = true;
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize float', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'float',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = 1.354;
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);

  it('Should serialize and deserialize double', async () => {
    const topic = v4();

    const schemaString = stringify({
      type: 'double',
    });

    const stringSchemaInfo: SchemaInfo = {
      schema: schemaString,
      metadata: metadata
    };

    await schemaRegistryClient.register(topic + "-value", stringSchemaInfo);

    const messageValue = 1.354;
    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(topic, messageValue)
    };

    await producer.send({
      topic: topic,
      messages: [outgoingMessage]
    });

    await consumer.connect();

    await consumer.subscribe({ topic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(topic, message.value as Buffer)
        };
        messageRcvd = true;
        expect(decodedMessage.value).toBe(messageValue);
      },
    });

    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);
  
  //Waiting on the null case
});