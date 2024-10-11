import {
  JsonSerializer, JsonSerializerConfig, SerdeType,
  BearerAuthCredentials, ClientConfig,
  SchemaRegistryClient, SchemaInfo
} from "@confluentinc/schemaregistry";
import { CreateAxiosDefaults } from "axios";
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  basicAuthCredentials,
  clusterApiKey, clusterApiSecret,
  clusterBootstrapUrl,
  baseUrl
} from "./constants";

async function kafkaProducerJson() {


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

  const producer: KafkaJS.Producer = kafka.producer({
    kafkaJS: {
      allowAutoTopicCreation: true,
      acks: 1,
      compression: KafkaJS.CompressionTypes.GZIP,
    }
  });

  const schemaString: string = JSON.stringify({
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "User",
    "type": "object",
    "properties": {
      "name": {
        "type": "string"
      },
      "age": {
        "type": "integer"
      }
    },
    "required": ["name", "age"]
  });

  const schemaInfo: SchemaInfo = {
    schemaType: 'JSON',
    schema: schemaString,
  };

  const userTopic = 'example-user-topic';
  await schemaRegistryClient.register(userTopic + "-value", schemaInfo);

  const userInfo = { name: 'Alice N Bob', age: 30 };

  const jsonSerializerConfig: JsonSerializerConfig = { useLatestVersion: true };

  const serializer: JsonSerializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, jsonSerializerConfig);

  const outgoingMessage = {
    key: "1",
    value: await serializer.serialize(userTopic, userInfo)
  };

  console.log("Outgoing message: ", outgoingMessage);

  await producer.connect();

  await producer.send({
    topic: userTopic,
    messages: [outgoingMessage]
  });

  await producer.disconnect();
}

kafkaProducerJson();