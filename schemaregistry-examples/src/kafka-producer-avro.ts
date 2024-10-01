import {
  AvroSerializer, AvroSerializerConfig, SerdeType,
 ClientConfig, SchemaRegistryClient, SchemaInfo
} from "@confluentinc/schemaregistry";
import { CreateAxiosDefaults } from "axios";
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  basicAuthCredentials,
  clusterApiKey, clusterApiSecret,
  clusterBootstrapUrl,
  baseUrl
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

  const producer: KafkaJS.Producer = kafka.producer({
    kafkaJS: {
      allowAutoTopicCreation: true,
      acks: 1,
      compression: KafkaJS.CompressionTypes.GZIP,
    }
  });

  const schemaString: string = JSON.stringify({
    type: 'record',
    name: 'User',
    fields: [
      { name: 'name', type: 'string' },
      { name: 'age', type: 'int' },
    ],
  });

  const schemaInfo: SchemaInfo = {
    schemaType: 'AVRO',
    schema: schemaString,
  };

  const userTopic = 'example-user-topic';
  await schemaRegistryClient.register(userTopic + "-value", schemaInfo);

  const userInfo = { name: 'Alice N Bob', age: 30 };

  const avroSerializerConfig: AvroSerializerConfig = { useLatestVersion: true };

  const serializer: AvroSerializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, avroSerializerConfig);

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

kafkaProducerAvro();