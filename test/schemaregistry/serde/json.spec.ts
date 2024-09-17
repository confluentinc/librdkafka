import {afterEach, describe, expect, it} from '@jest/globals';
import {ClientConfig} from "../../../schemaregistry/rest-service";
import {SerdeType, SerializationError, Serializer} from "../../../schemaregistry/serde/serde";
import {
  Client,
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
import {RuleRegistry} from "@confluentinc/schemaregistry/serde/rule-registry";
import stringify from "json-stringify-deterministic";
import {JsonataExecutor} from "@confluentinc/schemaregistry/rules/jsonata/jsonata-executor";
import {clearKmsClients} from "@confluentinc/schemaregistry/rules/encryption/kms-registry";

const fieldEncryptionExecutor = FieldEncryptionExecutor.register()
JsonataExecutor.register()
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
const demoSchemaWithUnion = `
{
  "type": "object",
  "properties": {
    "intField": { "type": "integer" },
    "doubleField": { "type": "number" },
    "stringField": {
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ],
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
const demoSchema2020_12 = `
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
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
const complexSchema = `
{
  "type": "object",
  "properties": {
    "arrayField": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "confluent:tags": [ "PII" ]
    },
    "objectField": {
      "type": "object",
      "properties": {
        "stringField": { "type": "string" }
      },
      "confluent:tags": [ "PII" ]
    },
    "unionField": {
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ],
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
  it('basic serialization 2020-12', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new JsonSerializer(client, SerdeType.VALUE, {
      useLatestVersion: true,
      validate: true
    })

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: demoSchema2020_12
    }

    await client.register(subject, info, false)

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
  it('basic failing validation', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new JsonSerializer(client, SerdeType.VALUE, {
      useLatestVersion: true,
      validate: true
    })

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let jsonSchema = JsonSerializer.messageToSchema(obj)
    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: JSON.stringify(jsonSchema)
    }

    await client.register(subject, info, false)

    let bytes = await ser.serialize(topic, obj)

    let deser = new JsonDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)

    let diffObj = {
      intField: '123',
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }

    await expect(() => ser.serialize(topic, diffObj)).rejects.toThrow(SerializationError)
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
      onFailure: 'ERROR,NONE'
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

    clearKmsClients()
    let registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    deser = new JsonDeserializer(client, SerdeType.VALUE, {}, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).not.toEqual(obj);
  })
  it('basic encryption 2020-12', async () => {
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
      onFailure: 'ERROR,NONE'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: demoSchema2020_12,
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

    clearKmsClients()
    let registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    deser = new JsonDeserializer(client, SerdeType.VALUE, {}, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).not.toEqual(obj);
  })
  it('encryption with union', async () => {
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
      schema: demoSchemaWithUnion,
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
  it('encryption with reference', async () => {
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

    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: demoSchema,
    }
    await client.register('demo-value', info, false)

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

    info = {
      schemaType: 'JSON',
      schema: rootSchema,
      references: [{
        name: 'DemoSchema',
        subject: 'demo-value',
        version: 1
      }],
      ruleSet
    }
    await client.register(subject, info, false)

    let nested = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([0, 0, 0, 1]).toString('base64')
    }
    let obj = {
      otherField: nested
    }
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    nested.stringField = 'hi'
    nested.bytesField = Buffer.from([0, 0, 0, 1]).toString('base64')

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
  it('complex encryption', async () => {
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
      onFailure: 'ERROR,NONE'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      objectField: { 'stringField': 'world' },
      unionField: 'bye',
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: JsonDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new JsonDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello' ]);
    expect(obj2.objectField.stringField).toEqual('world');
    expect(obj2.unionField).toEqual('bye');
  })
  it('complex encryption with null', async () => {
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
      onFailure: 'ERROR,NONE'
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      objectField: { 'stringField': 'world' },
      unionField: null,
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: JsonDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new JsonDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello' ]);
    expect(obj2.objectField.stringField).toEqual('world');
    expect(obj2.unionField).toEqual(null);
  })
  it('jsonata fully compatible', async () => {
    let rule1To2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
    let rule2To1 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
    let rule2To3 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
    let rule3To2 = "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)

    client.updateConfig(subject, {
      compatibilityGroup: 'application.version'
    })

    let widget = {
      name: 'alice',
      size: 123,
      version: 1,
    }
    let jsonSchema = JsonSerializer.messageToSchema(widget)
    let info: SchemaInfo = {
      schemaType: 'JSON',
      schema: JSON.stringify(jsonSchema),
      metadata: {
        properties: {
          "application.version": "v1"
        }
      }
    }

    await client.register(subject, info, false)

    let newWidget = {
      name: 'alice',
      height: 123,
      version: 1,
    }
    jsonSchema = JsonSerializer.messageToSchema(newWidget)
    info = {
      schemaType: 'JSON',
      schema: JSON.stringify(jsonSchema),
      metadata: {
        properties: {
          "application.version": "v2"
        }
      },
      ruleSet: {
        migrationRules: [
          {
            name: 'myRule1',
            kind: 'TRANSFORM',
            mode: RuleMode.UPGRADE,
            type: 'JSONATA',
            expr: rule1To2,
          },
          {
            name: 'myRule2',
            kind: 'TRANSFORM',
            mode: RuleMode.DOWNGRADE,
            type: 'JSONATA',
            expr: rule2To1,
          },
        ]
      }
    }

    await client.register(subject, info, false)

    let newerWidget = {
      name: 'alice',
      length: 123,
      version: 1,
    }
    jsonSchema = JsonSerializer.messageToSchema(newerWidget)
    info = {
      schemaType: 'JSON',
      schema: JSON.stringify(jsonSchema),
      metadata: {
        properties: {
          "application.version": "v3"
        }
      },
      ruleSet: {
        migrationRules: [
          {
            name: 'myRule1',
            kind: 'TRANSFORM',
            mode: RuleMode.UPGRADE,
            type: 'JSONATA',
            expr: rule2To3,
          },
          {
            name: 'myRule2',
            kind: 'TRANSFORM',
            mode: RuleMode.DOWNGRADE,
            type: 'JSONATA',
            expr: rule3To2,
          },
        ]
      }
    }

    await client.register(subject, info, false)

    let serConfig1 = {
      useLatestWithMetadata: {
        "application.version": "v1"
      }
    }
    let ser1 = new JsonSerializer(client, SerdeType.VALUE, serConfig1)
    let bytes = await ser1.serialize(topic, widget)

    await deserializeWithAllVersions(client, ser1, bytes, widget, newWidget, newerWidget)

    let serConfig2 = {
      useLatestWithMetadata: {
        "application.version": "v2"
      }
    }
    let ser2 = new JsonSerializer(client, SerdeType.VALUE, serConfig2)
    bytes = await ser2.serialize(topic, newWidget)

    await deserializeWithAllVersions(client, ser2, bytes, widget, newWidget, newerWidget)

    let serConfig3 = {
      useLatestWithMetadata: {
        "application.version": "v3"
      }
    }
    let ser3 = new JsonSerializer(client, SerdeType.VALUE, serConfig3)
    bytes = await ser3.serialize(topic, newerWidget)

    await deserializeWithAllVersions(client, ser3, bytes, widget, newWidget, newerWidget)
  })

  async function deserializeWithAllVersions(client: Client, ser: Serializer, bytes: Buffer,
                                            widget: any, newWidget: any, newerWidget: any) {
    let deserConfig1: JsonDeserializerConfig = {
      useLatestWithMetadata: {
        "application.version": "v1"
      }
    }
    let deser1 = new JsonDeserializer(client, SerdeType.VALUE, deserConfig1)
    deser1.client = ser.client

    let newobj = await deser1.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(widget));

    let deserConfig2 = {
      useLatestWithMetadata: {
        "application.version": "v2"
      }
    }
    let deser2 = new JsonDeserializer(client, SerdeType.VALUE, deserConfig2)
    newobj = await deser2.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(newWidget));

    let deserConfig3 = {
      useLatestWithMetadata: {
        "application.version": "v3"
      }
    }
    let deser3 = new JsonDeserializer(client, SerdeType.VALUE, deserConfig3)
    newobj = await deser3.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(newerWidget));
  }
})
