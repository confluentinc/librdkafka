import {
  AvroSerializer, AvroDeserializer, AvroSerializerConfig, SerdeType, Serializer, Deserializer,
  JsonSerializer, JsonDeserializer, JsonSerializerConfig,
  ClientConfig, SchemaRegistryClient, SchemaInfo
} from "@confluentinc/schemaregistry";
import { localAuthCredentials } from "../constants";
import { v4 } from "uuid";
import { beforeEach, describe, it } from '@jest/globals';

const clientConfig: ClientConfig = {
  baseURLs: ['http://localhost:8081'],
  cacheCapacity: 512,
  cacheLatestTtlSecs: 60,
  basicAuthCredentials: localAuthCredentials,
};

const avroSchemaString: string = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' },
    { name: 'address', type: 'string' },
  ],
});

const jsonSchemaString: string = JSON.stringify({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User",
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    },
    "age": {
      "type": "integer"
    },
    "address": {
      "type": "string"
    }
  },
  "required": ["name", "age", "address"]
});

const avroSchemaInfo: SchemaInfo = {
  schema: avroSchemaString,
  schemaType: 'AVRO'
};

const jsonSchemaInfo: SchemaInfo = {
  schema: jsonSchemaString,
  schemaType: 'JSON'
};

const data: { name: string; age: number; address: string; }[] = [];

let schemaRegistryClient: SchemaRegistryClient;

function generateData(numRecords: number) {
  for (let i = 0; i < numRecords; i++) {
    data.push({
      name: `User ${i}`,
      age: Math.floor(Math.random() * 100),
      address: v4()
    });
  }
}

generateData(10000);

async function serializeAndDeserializeSchemas(serializer: Serializer, deserializer: Deserializer, topic: string) {
  Promise.all(
    data.map(async (record) => {
      const serialized = await serializer.serialize(topic, record);
      await deserializer.deserialize(topic, serialized);
    })
  );
}

describe('Serialization Performance Test', () => {

  beforeEach(async () => {
    schemaRegistryClient = new SchemaRegistryClient(clientConfig);
  });

  it("Should measure serialization and deserialization performance for JSON", async () => {
    const topic = v4();
    await schemaRegistryClient.register(topic + "-value", jsonSchemaInfo);
    
    const jsonSerializerConfig: JsonSerializerConfig = { useLatestVersion: true };
    const jsonSerializer: JsonSerializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, jsonSerializerConfig);
    const jsonDeserializer: JsonDeserializer = new JsonDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    const start = performance.now();
    await serializeAndDeserializeSchemas(jsonSerializer, jsonDeserializer, topic);
    const end = performance.now();

    console.log(`JSON serialization and deserialization took ${end - start} ms`);
  });

  it("Should measure serialization and deserialization performance for Avro", async () => {
    const topic = v4();
    await schemaRegistryClient.register(topic + "-value", avroSchemaInfo);

    const avroSerializerConfig: AvroSerializerConfig = { useLatestVersion: true };
    const serializer: AvroSerializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, avroSerializerConfig);
    const deserializer: AvroDeserializer = new AvroDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    const start = performance.now();
    await serializeAndDeserializeSchemas(serializer, deserializer, topic);
    const end = performance.now();

    console.log(`Avro serialization and deserialization took ${end - start} ms`);
  });

  // it("Should measure serialization and deserialization performance for Protobuf", async () => {
  //   const protobufSerializerConfig: ProtobufSerializerConfig = { useLatestVersion: true };
  //   const serializer: ProtobufSerializer = new ProtobufSerializer(schemaRegistryClient, SerdeType.VALUE, protobufSerializerConfig);
  //   const deserializer: ProtobufDeserializer = new ProtobufDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

  //   const start = performance.now();
  //   await serializeAndDeserializeSchemas(serializer, deserializer, topic);
  //   const end = performance.now();

  //   console.log(`Protobuf serialization and deserialization took ${end - start} ms`);
  // });
});
