import {
    AvroSerializer, AvroSerializerConfig, SerdeType,
    AvroDeserializer, ClientConfig,
    SchemaRegistryClient, SchemaInfo, Rule, RuleMode,
    RuleRegistry, FieldEncryptionExecutor, AwsKmsDriver, RuleSet
  } from "@confluentinc/schemaregistry";
  import { CreateAxiosDefaults } from "axios";
  import { KafkaJS } from '@confluentinc/kafka-javascript';
  import {
    basicAuthCredentials, clusterApiKey, clusterApiSecret, 
    clusterBootstrapUrl, baseUrl
  } from "./constants";
  
  FieldEncryptionExecutor.register();
  AwsKmsDriver.register();
  
  async function csfle() {
  
    const schemaString: string = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' },
        {
          name: 'address', type: 'string',
          "confluent:tags": ["PII"]
        },
      ],
    });
  
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
  
    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'csfle-example',
        'encrypt.kms.type': 'aws-kms',
        'encrypt.kms.key.id': 'your-key-id',
      },
      onFailure: 'ERROR,NONE'
    };
  
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    };
  
    const schemaInfo: SchemaInfo = {
      schemaType: 'AVRO',
      schema: schemaString,
      ruleSet: ruleSet
    };
  
    const userInfo = { name: 'Alice N Bob', age: 30, address: '369 Main St' };
    const userTopic = 'csfle-topic';
  
    await schemaRegistryClient.register(userTopic+"-value", schemaInfo);
  
    const serializerConfig: AvroSerializerConfig = { useLatestVersion: true };
    const serializer: AvroSerializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
  
    const outgoingMessage = {
      key: "1",
      value: await serializer.serialize(userTopic, userInfo)
    };
  
    console.log("Outgoing Message:", outgoingMessage);
  
    await producer.connect();
  
    await producer.send({
      topic: userTopic,
      messages: [outgoingMessage]
    });
  
    await producer.disconnect();
  
    const consumer: KafkaJS.Consumer = kafka.consumer({
      kafkaJS: {
        groupId: 'demo-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });
  
    await consumer.connect();
  
    const deserializer: AvroDeserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});
  
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
        let registry = new RuleRegistry();
        const weakDeserializer: AvroDeserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {}, registry);
        const weakDecodedMessage = {
          ...message,
          value: await weakDeserializer.deserialize(userTopic, message.value as Buffer)
        };
        console.log("Weak decoded message", weakDecodedMessage);
        messageRcvd = true;
      },
    });
  
    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  
    await consumer.disconnect();
  }
  
  csfle();