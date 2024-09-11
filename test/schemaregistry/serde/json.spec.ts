import {afterEach, describe, expect, it} from '@jest/globals';
import {ClientConfig} from "../../../schemaregistry/rest-service";
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
import {
  JsonDeserializer, JsonDeserializerConfig,
  JsonSerializer,
  JsonSerializerConfig
} from "../../../schemaregistry/serde/json";

const fieldEncryptionExecutor = FieldEncryptionExecutor.register()
LocalKmsDriver.register()

//const baseURL = 'http://localhost:8081'
const baseURL = 'mock://'

const topic = 'topic1'
const subject = topic + '-value'

const rootSchema = `
{
  "type": "object",
  "properties": {
    "otherField": { "$ref": "DemoSchema" }
  }
}
`

const demoSchema = `
{
  "type": "object",
  "properties": {
    "intField": { "type": "integer" },
    "doubleField": { "type": "number" },
    "stringField": {
       "type": "string",
       "confluent:tags": [ "PII" ]
    },
    "boolField": { "type": "boolean" },
    "bytesField": {
       "type": "string",
       "contentEncoding": "base64",
       "confluent:tags": [ "PII" ]
    }
  }
}
`

describe('JsonSerializer', () => {
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
    let ser = new JsonSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new JsonDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('serialize nested', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new JsonSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new JsonDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('serialize reference', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new JsonSerializer(client, SerdeType.VALUE, {useLatestVersion: true})

    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: demoSchema
    }
    await client.register('demo-value', info, false)

    info = {
      schemaType: 'JSON',
      schema: rootSchema,
      references: [{
        name: 'DemoSchema',
        subject: 'demo-value',
        version: 1
      }]
    }
    await client.register(subject, info, false)

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let bytes = await ser.serialize(topic, obj)

    let deser = new JsonDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('basic encryption', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: JsonSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let ser = new JsonSerializer(client, SerdeType.VALUE, serConfig)
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
      schemaType: 'JSON',
      schema: demoSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.stringField = 'hi'
    obj.bytesField = Buffer.from([0, 0, 0, 1]).toString('base64')

    let deserConfig: JsonDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new JsonDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
})
