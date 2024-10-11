import {
  AvroSerializer, AvroSerializerConfig, SerdeType,
  ClientConfig, SchemaRegistryClient, SchemaInfo, BearerAuthCredentials
} from "@confluentinc/schemaregistry";
import { CreateAxiosDefaults } from "axios";
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  clusterBootstrapUrl,
  baseUrl,
  issuerEndpointUrl, oauthClientId, oauthClientSecret, scope,
  identityPoolId, schemaRegistryLogicalCluster, kafkaLogicalCluster
} from "./constants";
import axios from 'axios';

// Only showing the producer, will be the same implementation for the consumer

async function token_refresh() {
  try {
    // Make a POST request to get the access token
    const response = await axios.post(issuerEndpointUrl, new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: oauthClientId,
      client_secret: oauthClientSecret,
      scope: scope
    }), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    // Extract the token and expiration time from the response
    const token = response.data.access_token;
    const exp_seconds = Math.floor(Date.now() / 1000) + response.data.expires_in;
    const exp_ms = exp_seconds * 1000;

    const principal = 'admin'; // You can adjust this based on your needs
    const extensions = {
      traceId: '123',
      logicalCluster: kafkaLogicalCluster,
      identityPoolId: identityPoolId
    };

    return { value: token, lifetime: exp_ms, principal, extensions };
  } catch (error) {
    console.error('Failed to retrieve OAuth token:', error);
    throw new Error('Failed to retrieve OAuth token');
  }
}

async function kafkaProducerAvro() {

  const createAxiosDefaults: CreateAxiosDefaults = {
    timeout: 10000
  };

  const bearerAuthCredentials: BearerAuthCredentials = {
    credentialsSource: 'OAUTHBEARER',
    issuerEndpointUrl: issuerEndpointUrl,
    clientId: oauthClientId,
    clientSecret: oauthClientSecret,
    scope: scope,
    identityPoolId: identityPoolId,
    logicalCluster: schemaRegistryLogicalCluster
  }

  const clientConfig: ClientConfig = {
    baseURLs: [baseUrl],
    createAxiosDefaults: createAxiosDefaults,
    cacheCapacity: 512,
    cacheLatestTtlSecs: 60,
    bearerAuthCredentials: bearerAuthCredentials
  };

  const schemaRegistryClient = new SchemaRegistryClient(clientConfig);

  const kafka: KafkaJS.Kafka = new KafkaJS.Kafka({
    kafkaJS: {
      brokers: [clusterBootstrapUrl],
      ssl: true,
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: token_refresh
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

  console.log("Producer created");

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