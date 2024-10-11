import { SerdeType,AvroDeserializer, ClientConfig, SchemaRegistryClient } from "@confluentinc/schemaregistry";
import { CreateAxiosDefaults } from "axios";
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  basicAuthCredentials,
  clusterApiKey, clusterApiSecret,
  clusterBootstrapUrl, baseUrl
} from "./constants";

async function kafkaProducerAvro() {
  const createAxiosDefaults: CreateAxiosDefaults = {
    timeout: 10000
  };

  const clientConfig: ClientConfig = {
    baseURLs: [baseUrl],
    createAxiosDefaults: createAxiosDefaults,
    cacheCapacity: 512,
    cacheLatestTtlSecs: 60,
    basicAuthCredentials: basicAuthCredentials
  };

  const schemaRegistryClient = new SchemaRegistryClient(clientConfig);

  const kafka: KafkaJS.Kafka = new KafkaJS.Kafka({
    kafkaJS: {
      brokers: [clusterBootstrapUrl],
      ssl: true,
      sasl: {
        mechanism: 'plain',
        username: clusterApiKey,
        password: clusterApiSecret,
      },
    },
  });

  const userTopic = 'example-user-topic';

  const deserializer: AvroDeserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

  const consumer: KafkaJS.Consumer = kafka.consumer({
    kafkaJS: {
      groupId: 'example-group',
      fromBeginning: true,
      partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
    },
  });

  await consumer.connect();
  await consumer.subscribe({ topic: userTopic });

  let messageRcvd = false;
  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("Message value", message.value);
      const decodedMessage = {
        ...message,
        value: await deserializer.deserialize(userTopic, message.value as Buffer)
      };
      console.log("Decoded message", decodedMessage);
      messageRcvd = true;
    },
  });

  while (!messageRcvd) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  await consumer.disconnect();
}

kafkaProducerAvro();
