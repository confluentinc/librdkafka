import {afterEach, describe, expect, it} from '@jest/globals';
import {ClientConfig} from "../../../schemaregistry/rest-service";
import {
  AvroDeserializer, AvroDeserializerConfig,
  AvroSerializer,
  AvroSerializerConfig
} from "../../../schemaregistry/serde/avro";
import {SerdeType} from "../../../schemaregistry/serde/serde";
import {
  Rule,
  RuleMode,
  RuleSet,
  SchemaInfo,
  SchemaRegistryClient
} from "../../../schemaregistry/schemaregistry-client";
import {LocalKmsDriver} from "../../../schemaregistry/rules/encryption/localkms/local-driver";
import {FieldEncryptionExecutor} from "../../../schemaregistry/rules/encryption/encrypt-executor";

const demoSchema = `
{
  "name": "DemoSchema",
  "type": "record",
  "fields": [
    {
      "name": "intField",
      "type": "int"
    },
    {
      "name": "doubleField",
      "type": "double"
    },
    {
      "name": "stringField",
      "type": "string",
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "boolField",
      "type": "boolean"
    },
    {
      "name": "bytesField",
      "type": "bytes",
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
const demoSchemaWithLogicalType = `
{
  "name": "DemoSchema",
  "type": "record",
  "fields": [
    {
      "name": "intField",
      "type": "int"
    },
    {
      "name": "doubleField",
      "type": "double"
    },
    {
      "name": "stringField",
      "type": {
        "type": "string",
        "logicalType": "uuid"
      },
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "boolField",
      "type": "boolean"
    },
    {
      "name": "bytesField",
      "type": "bytes",
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
const rootPointerSchema = `
{
  "name": "NestedTestPointerRecord",
  "type": "record",
  "fields": [
  {
    "name": "otherField",
    "type": ["null", "DemoSchema"]
  }
]
}
`
const f1Schema = `
{
  "name": "F1Schema",
  "type": "record",
  "fields": [
    {
      "name": "f1",
      "type": "string",
      "confluent:tags": [ "PII" ]
    }
  ]
}
`

const fieldEncryptionExecutor = FieldEncryptionExecutor.register()
LocalKmsDriver.register()

//const baseURL = 'http://localhost:8081'
const baseURL = 'mock://'

const topic = 'topic1'
const subject = topic + '-value'

describe('AvroSerializer', () => {
  afterEach(async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    await client.deleteSubject(subject, false)
    await client.deleteSubject(subject, true)
  })
  it('basic serialization', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('serialize nested', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})

    let nested = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let obj = {
      otherField: nested
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.otherField.intField).toEqual(nested.intField);
    expect(obj2.otherField.doubleField).toBeCloseTo(nested.doubleField, 0.001);
    expect(obj2.otherField.stringField).toEqual(nested.stringField);
    expect(obj2.otherField.boolField).toEqual(nested.boolField);
    expect(obj2.otherField.bytesField).toEqual(nested.bytesField);
  })
  it('serialize reference', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {useLatestVersion: true})

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: demoSchema,
    }
    await client.register('demo-value', info , false)

    info = {
      schemaType: 'AVRO',
      schema: rootPointerSchema,
      references: [{
        name: 'DemoSchema',
        subject: 'demo-value',
        version: 1
      }]
    }
    await client.register(subject, info , false)

    let nested = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let obj = {
      otherField: nested
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.otherField.intField).toEqual(nested.intField);
    expect(obj2.otherField.doubleField).toBeCloseTo(nested.doubleField, 0.001);
    expect(obj2.otherField.stringField).toEqual(nested.stringField);
    expect(obj2.otherField.boolField).toEqual(nested.boolField);
    expect(obj2.otherField.bytesField).toEqual(nested.bytesField);
  })
  it('basic encryption', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)
    let dekClient = fieldEncryptionExecutor.client!

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
      },
      onFailure: 'ERROR,ERROR'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: demoSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.stringField = 'hi'
    obj.bytesField = Buffer.from([1, 2])

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('basic encryption with logical type', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)
    let dekClient = fieldEncryptionExecutor.client!

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
      },
      onFailure: 'ERROR,ERROR'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: demoSchemaWithLogicalType,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.stringField = 'hi'
    obj.bytesField = Buffer.from([1, 2])

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('basic encryption with preserialized data', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
      },
      onFailure: 'ERROR,ERROR'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: f1Schema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      f1: 'hello world'
    }

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let dekClient = fieldEncryptionExecutor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4="
    await dekClient.registerDek("kek1", subject, "AES256_GCM", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
  it('deterministic encryption with preserialized data', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
        'encrypt.dek.algorithm': 'AES256_SIV',
      },
      onFailure: 'ERROR,ERROR'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: f1Schema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      f1: 'hello world'
    }

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let dekClient = fieldEncryptionExecutor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A=="
    await dekClient.registerDek("kek1", subject, "AES256_SIV", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
  it('dek rotation encryption with preserialized data', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT',
      tags: ['PII'],
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
        'encrypt.dek.expiry.days': '1',
      },
      onFailure: 'ERROR,ERROR'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: f1Schema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      f1: 'hello world'
    }

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let dekClient = fieldEncryptionExecutor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4="
    await dekClient.registerDek("kek1", subject, "AES256_GCM", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
})
