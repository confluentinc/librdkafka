import {afterEach, describe, expect, it} from '@jest/globals';
import {ClientConfig} from "../../rest-service";
import {
  AvroDeserializer,
  AvroDeserializerConfig,
  AvroSerializer,
  AvroSerializerConfig
} from "../../serde/avro";
import {HeaderSchemaIdSerializer, SerdeType, SerializationError, Serializer} from "../../serde/serde";
import {
  Client,
  Rule,
  RuleMode,
  RuleSet,
  SchemaInfo,
  SchemaRegistryClient
} from "../../schemaregistry-client";
import {LocalKmsDriver} from "../../rules/encryption/localkms/local-driver";
import {
  Clock, EncryptionExecutor,
  FieldEncryptionExecutor
} from "../../rules/encryption/encrypt-executor";
import {GcpKmsDriver} from "../../rules/encryption/gcpkms/gcp-driver";
import {AwsKmsDriver} from "../../rules/encryption/awskms/aws-driver";
import {AzureKmsDriver} from "../../rules/encryption/azurekms/azure-driver";
import {HcVaultDriver} from "../../rules/encryption/hcvault/hcvault-driver";
import {JsonataExecutor} from "@confluentinc/schemaregistry/rules/jsonata/jsonata-executor";
import stringify from "json-stringify-deterministic";
import {RuleRegistry} from "@confluentinc/schemaregistry/serde/rule-registry";
import {
  clearKmsClients
} from "@confluentinc/schemaregistry/rules/encryption/kms-registry";
import {CelExecutor} from "../../rules/cel/cel-executor";
import {CelFieldExecutor} from "../../rules/cel/cel-field-executor";

const rootSchema = `
{
  "name": "NestedTestRecord",
  "type": "record",
  "fields": [
    {
      "name": "otherField",
      "type": "DemoSchema"
    }
  ]
}
`
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
const nameSchema = `
{
  "type": "record",
  "namespace": "examples",
  "name": "NameSchema",
  "fields": [
    { "name": "fullName", "type": "string" },
    { "name": "lastName", "type": "string" }
  ],
  "version": "1"
}
`
const demoSchemaSingleTag = `
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
      "type": "bytes"
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
const demoSchemaWithUnion = `
{
  "name": "DemoSchemaWithUnion",
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
      "type": ["null", "string"],
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "boolField",
      "type": "boolean"
    },
    {
      "name": "bytesField",
      "type": ["null", "bytes"],
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
const schemaEvolution1 = `
{
  "name": "SchemaEvolution",
  "type": "record",
  "fields": [
    {
      "name": "fieldToDelete",
      "type": "string"
    }
  ]
}
`
const schemaEvolution2 = `
{
  "name": "SchemaEvolution",
  "type": "record",
  "fields": [
    {
      "name": "newOptionalField",
      "type": ["string", "null"],
      "default": "optional"
    }
  ]
}
`
const complexSchema = `
{
  "name": "ComplexSchema",
  "type": "record",
  "fields": [
    {
      "name": "arrayField",
      "type": {
        "type": "array",
        "items": "string"
       },
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "mapField",
      "type": {
        "type": "map",
        "values": "string"
       },
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "unionField",
      "type": ["null", "string"],
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
const unionFieldSchema = `
{
  "type": "record",
    "name": "UnionTest",
    "namespace": "test",
    "fields": [
    {
      "name": "color",
      "type": [
        "string",
        {
          "type": "enum",
          "name": "Color",
          "symbols": [
            "RED",
            "BLUE"
          ]
        }
      ],
      "default": "BLUE"

    }
  ],
  "version": "1"
}`;

const complexNestedSchema = `
{
  "type": "record",
    "name": "UnionTest",
    "namespace": "test",
    "fields": [
    {
      "name": "emails",
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Email",
            "fields": [
              {
                "name": "email",
                "type": [
                  "null",
                  "string"
                ],
                "doc": "Email address",
                "default": null,
                "confluent:tags": [
                  "PII"
                ]
              }
            ]
          }
        }
      ],
      "doc": "Communication Email",
      "default": null
    }
  ]
}`;

class FakeClock extends Clock {
  fixedNow: number = 0

  override now() {
    return this.fixedNow
  }
}

const encryptionExecutor = EncryptionExecutor.registerWithClock(new FakeClock())
const fieldEncryptionExecutor = FieldEncryptionExecutor.registerWithClock(new FakeClock())
CelExecutor.register()
CelFieldExecutor.register()
JsonataExecutor.register()
AwsKmsDriver.register()
AzureKmsDriver.register()
GcpKmsDriver.register()
HcVaultDriver.register()
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
  it('bad serialization', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {useLatestVersion: true})
    let info = {
      schemaType: 'AVRO',
      schema: nameSchema
    }
    await client.register(subject, info, false)
    try {
      await ser.serialize(topic, { lastName: "lastName" })
    } catch (err) {
      expect(err).toBeInstanceOf(SerializationError)
    }
  })
  it('guid in header', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE,
      {autoRegisterSchemas: true, schemaIdSerializer: HeaderSchemaIdSerializer})
    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    let headers = {}
    let bytes = await ser.serialize(topic, obj, headers)

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes, headers)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('serialize bytes', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})

    let obj = Buffer.from([0x02, 0x03, 0x04])
    let bytes = await ser.serialize(topic, obj)
    expect(bytes).toEqual(Buffer.from([0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04]));

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj);
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
    await client.register('demo-value', info, false)

    info = {
      schemaType: 'AVRO',
      schema: rootPointerSchema,
      references: [{
        name: 'DemoSchema',
        subject: 'demo-value',
        version: 1
      }]
    }
    await client.register(subject, info, false)

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
  it('field with union with non-applicable rule', async () => {
    const conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    };
    const client = SchemaRegistryClient.newClient(conf)
    const serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret'
      }
    };
    const ser = new AvroSerializer(client, SerdeType.VALUE, serConfig);
    const dekClient = fieldEncryptionExecutor.executor.client!;

    const encRule: Rule = {
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
    };
    const ruleSet: RuleSet = {
      domainRules: [encRule]
    };

    const info = {
      schemaType: 'AVRO',
      schema: unionFieldSchema,
      ruleSet
    };

    await client.register(subject, info, false);

    const obj = {
      color: {"test.Color": "BLUE"}
    };
    const bytes = await ser.serialize(topic, obj);

    const deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    };
    const deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig);
    fieldEncryptionExecutor.executor.client = dekClient;
    const obj2 = await deser.deserialize(topic, bytes);
    expect(obj2.color).toEqual(obj.color);
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
    await client.register('demo-value', info, false)

    info = {
      schemaType: 'AVRO',
      schema: rootPointerSchema,
      references: [{
        name: 'DemoSchema',
        subject: 'demo-value',
        version: 1
      }]
    }
    await client.register(subject, info, false)

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
  it('serialize union with references', async () => {
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
    await client.register('demo-value', info, false)

    info = {
      schemaType: 'AVRO',
      schema: complexSchema,
    }
    await client.register('complex-value', info, false)

    info = {
      schemaType: 'AVRO',
      schema: '[ "DemoSchema", "ComplexSchema" ]',
      references: [
        {
          name: 'DemoSchema',
          subject: 'demo-value',
          version: 1
        },
        {
          name: 'ComplexSchema',
          subject: 'complex-value',
          version: 1
        }]
    }
    await client.register(subject, info, false)

    let obj = {
      intField: 123,
      doubleField: 45.67,
      stringField: 'hi',
      boolField: true,
      bytesField: Buffer.from([1, 2]),
    }
    // need to wrap union
    let union = { DemoSchema: obj }
    let bytes = await ser.serialize(topic, union)

    let deser = new AvroDeserializer(client, SerdeType.VALUE, {})
    let union2 = await deser.deserialize(topic, bytes)

    // need to unwrap union
    let obj2 = union2.DemoSchema

    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('schema evolution', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new AvroSerializer(client, SerdeType.VALUE, {useLatestVersion: true})

    let obj = {
      fieldToDelete: "bye",
    }
    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: schemaEvolution1,
    }

    await client.register(subject, info, false)

    let bytes = await ser.serialize(topic, obj)

    info = {
      schemaType: 'AVRO',
      schema: schemaEvolution2,
    }

    await client.register(subject, info, false)

    client.clearLatestCaches()
    let deser = new AvroDeserializer(client, SerdeType.VALUE, {useLatestVersion: true})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.fieldToDelete).toEqual(undefined);
    expect(obj2.newOptionalField).toEqual("optional");
  })
  it('cel condition', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'CONDITION',
      mode: RuleMode.WRITE,
      type: 'CEL',
      expr: "message.stringField == 'hi'"
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('cel condition fail', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'CONDITION',
      mode: RuleMode.WRITE,
      type: 'CEL',
      expr: "message.stringField != 'hi'"
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
    try {
      await ser.serialize(topic, obj)
      expect(true).toBe(false)
    } catch (err) {
      expect(err).toBeInstanceOf(SerializationError)
    }
  })
  it('cel condition ignore fail', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'CONDITION',
      mode: RuleMode.WRITE,
      type: 'CEL',
      expr: "message.stringField != 'hi'",
      onFailure: 'NONE'
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('cel field transform', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITE,
      type: 'CEL_FIELD',
      expr: "name == 'stringField' ; value + '-suffix'"
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual('hi-suffix');
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('cel field complex transform', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITE,
      type: 'CEL_FIELD',
      expr: "typeName == 'STRING' ; value + '-suffix'",
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      mapField: { 'key': 'world' },
      unionField: 'bye',
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: AvroDeserializerConfig = {
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello-suffix' ]);
    expect(obj2.mapField).toEqual({ 'key': 'world-suffix' });
    expect(obj2.unionField).toEqual('bye-suffix');
  })
  it('cel field complex transform with null', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITE,
      type: 'CEL_FIELD',
      expr: "typeName == 'STRING' ; value + '-suffix'",
    }
    let ruleSet: RuleSet = {
      domainRules: [encRule]
    }

    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      mapField: { 'key': 'world' },
      unionField: null,
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: AvroDeserializerConfig = {
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello-suffix' ]);
    expect(obj2.mapField).toEqual({ 'key': 'world-suffix' });
    expect(obj2.unionField).toEqual(null);
  })
  it('cel field condition', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'CONDITION',
      mode: RuleMode.WRITE,
      type: 'CEL_FIELD',
      expr: "name == 'stringField' ; value == 'hi'"
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('cel field condition fail', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)

    let encRule: Rule = {
      name: 'test-cel',
      kind: 'CONDITION',
      mode: RuleMode.WRITE,
      type: 'CEL_FIELD',
      expr: "name == 'stringField' ; value != 'hi'"
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
    try {
      await ser.serialize(topic, obj)
      expect(true).toBe(false)
    } catch (err) {
      expect(err).toBeInstanceOf(SerializationError)
    }
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);

    let registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    registry.registerOverride({type: 'ENCRYPT', disabled: true})
    deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.stringField).not.toEqual("hi");
    expect(obj2.bytesField).not.toEqual(Buffer.from([1, 2]));

    clearKmsClients()
    registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    deser = new AvroDeserializer(client, SerdeType.VALUE, {}, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.stringField).not.toEqual("hi");
    expect(obj2.bytesField).not.toEqual(Buffer.from([1, 2]));
  })
  it('payload encryption', async () => {
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
    let dekClient = encryptionExecutor.client!

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT_PAYLOAD',
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
      },
      onFailure: 'ERROR,NONE'
    }
    let ruleSet: RuleSet = {
      encodingRules: [encRule]
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    encryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('encryption with alternate keks', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: AvroSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret',
        'encrypt.alternate.kms.key.ids': 'mykey2,mykey3'
      }
    }
    let ser = new AvroSerializer(client, SerdeType.VALUE, serConfig)
    let dekClient = encryptionExecutor.client!

    let encRule: Rule = {
      name: 'test-encrypt',
      kind: 'TRANSFORM',
      mode: RuleMode.WRITEREAD,
      type: 'ENCRYPT_PAYLOAD',
      params: {
        'encrypt.kek.name': 'kek1',
        'encrypt.kms.type': 'local-kms',
        'encrypt.kms.key.id': 'mykey',
      },
      onFailure: 'ERROR,NONE'
    }
    let ruleSet: RuleSet = {
      encodingRules: [encRule]
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    encryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('deterministic encryption', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
      onFailure: 'ERROR,NONE'
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
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);

    let registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    registry.registerOverride({type: 'ENCRYPT', disabled: true})
    deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.stringField).not.toEqual("hi");
    expect(obj2.bytesField).not.toEqual(Buffer.from([1, 2]));

    clearKmsClients()
    registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    deser = new AvroDeserializer(client, SerdeType.VALUE, {}, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.stringField).not.toEqual("hi");
    expect(obj2.bytesField).not.toEqual(Buffer.from([1, 2]));
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('basic encryption with dek rotation', async () => {
    const fieldEncryptionExecutor = FieldEncryptionExecutor.registerWithClock(new FakeClock());
    (fieldEncryptionExecutor.executor.clock as FakeClock).fixedNow = Date.now()
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
      schema: demoSchemaSingleTag,
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

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);

    let dek = await dekClient.getDek("kek1", subject, 'AES256_GCM', -1, false)
    expect(1).toEqual(dek.version);

    // advance time by 2 days
    (fieldEncryptionExecutor.executor.clock as FakeClock).fixedNow += 2 * 24 * 60 * 60 * 1000

    bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.stringField = 'hi'

    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);

    dek = await dekClient.getDek("kek1", subject, 'AES256_GCM', -1, false)
    expect(2).toEqual(dek.version);

    // advance time by 2 days
    (fieldEncryptionExecutor.executor.clock as FakeClock).fixedNow += 2 * 24 * 60 * 60 * 1000

    bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.stringField = 'hi'

    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);

    dek = await dekClient.getDek("kek1", subject, 'AES256_GCM', -1, false)
    expect(3).toEqual(dek.version);
  })
  it('basic encryption with preserialized data', async () => {
    const fieldEncryptionExecutor = FieldEncryptionExecutor.registerWithClock(new FakeClock());
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
    let dekClient = fieldEncryptionExecutor.executor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4="
    await dekClient.registerDek("kek1", subject, "AES256_GCM", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
  it('deterministic encryption with preserialized data', async () => {
    const fieldEncryptionExecutor = FieldEncryptionExecutor.registerWithClock(new FakeClock());
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
    let dekClient = fieldEncryptionExecutor.executor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A=="
    await dekClient.registerDek("kek1", subject, "AES256_SIV", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
  it('dek rotation encryption with preserialized data', async () => {
    const fieldEncryptionExecutor = FieldEncryptionExecutor.registerWithClock(new FakeClock());
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
    let dekClient = fieldEncryptionExecutor.executor.client!

    await dekClient.registerKek("kek1", "local-kms", "mykey", false)
    const encryptedDek = "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4="
    await dekClient.registerDek("kek1", subject, "AES256_GCM", 1, encryptedDek)

    const bytes = Buffer.from([0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61])
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.f1).toEqual(obj.f1);
  })
  it('encryption with references', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

    let info: SchemaInfo = {
      schemaType: 'AVRO',
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
      schemaType: 'AVRO',
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
      bytesField: Buffer.from([1, 2]),
    }
    let obj = {
      otherField: nested
    }
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    nested.stringField = 'hi'
    nested.bytesField = Buffer.from([1, 2])

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.otherField.intField).toEqual(nested.intField);
    expect(obj2.otherField.doubleField).toBeCloseTo(nested.doubleField, 0.001);
    expect(obj2.otherField.stringField).toEqual(nested.stringField);
    expect(obj2.otherField.boolField).toEqual(nested.boolField);
    expect(obj2.otherField.bytesField).toEqual(nested.bytesField);
  })
  it('encryption with union', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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

    let info = {
      schemaType: 'AVRO',
      schema: demoSchemaWithUnion,
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
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.intField).toEqual(obj.intField);
    expect(obj2.doubleField).toBeCloseTo(obj.doubleField, 0.001);
    expect(obj2.stringField).toEqual(obj.stringField);
    expect(obj2.boolField).toEqual(obj.boolField);
    expect(obj2.bytesField).toEqual(obj.bytesField);
  })
  it('complex encryption', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
      schemaType: 'AVRO',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      mapField: { 'key': 'world' },
      unionField: 'bye',
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello' ]);
    expect(obj2.mapField).toEqual({ 'key': 'world' });
    expect(obj2.unionField).toEqual('bye');
  })
  it('complex encryption with null', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
      schemaType: 'AVRO',
      schema: complexSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      arrayField: [ 'hello' ],
      mapField: { 'key': 'world' },
      unionField: null
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.arrayField).toEqual([ 'hello' ]);
    expect(obj2.mapField).toEqual({ 'key': 'world' });
    expect(obj2.unionField).toEqual(null);
  })
  it('complex nested encryption', async () => {
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
    let dekClient = fieldEncryptionExecutor.executor.client!

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
      schemaType: 'AVRO',
      schema: complexNestedSchema,
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = {
      emails: [ {
        email: "john@acme.com",
      } ],
    }
    let bytes = await ser.serialize(topic, obj)

    let deserConfig: AvroDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new AvroDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.executor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.emails[0].email).toEqual('john@acme.com');
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
    let avroSchema = AvroSerializer.messageToSchema(widget)
    let info: SchemaInfo = {
      schemaType: 'AVRO',
      schema: JSON.stringify(avroSchema),
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
    avroSchema = AvroSerializer.messageToSchema(newWidget)
    info = {
      schemaType: 'AVRO',
      schema: JSON.stringify(avroSchema),
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
    avroSchema = AvroSerializer.messageToSchema(newerWidget)
    info = {
      schemaType: 'AVRO',
      schema: JSON.stringify(avroSchema),
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
    let ser1 = new AvroSerializer(client, SerdeType.VALUE, serConfig1)
    let bytes = await ser1.serialize(topic, widget)

    await deserializeWithAllVersions(client, ser1, bytes, widget, newWidget, newerWidget)

    let serConfig2 = {
      useLatestWithMetadata: {
        "application.version": "v2"
      }
    }
    let ser2 = new AvroSerializer(client, SerdeType.VALUE, serConfig2)
    bytes = await ser2.serialize(topic, newWidget)

    await deserializeWithAllVersions(client, ser2, bytes, widget, newWidget, newerWidget)

    let serConfig3 = {
      useLatestWithMetadata: {
        "application.version": "v3"
      }
    }
    let ser3 = new AvroSerializer(client, SerdeType.VALUE, serConfig3)
    bytes = await ser3.serialize(topic, newerWidget)

    await deserializeWithAllVersions(client, ser3, bytes, widget, newWidget, newerWidget)
  })

  async function deserializeWithAllVersions(client: Client, ser: Serializer, bytes: Buffer,
                                            widget: any, newWidget: any, newerWidget: any) {
    let deserConfig1: AvroDeserializerConfig = {
      useLatestWithMetadata: {
        "application.version": "v1"
      }
    }
    let deser1 = new AvroDeserializer(client, SerdeType.VALUE, deserConfig1)
    deser1.client = ser.client

    let newobj = await deser1.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(widget));

    let deserConfig2 = {
      useLatestWithMetadata: {
        "application.version": "v2"
      }
    }
    let deser2 = new AvroDeserializer(client, SerdeType.VALUE, deserConfig2)
    newobj = await deser2.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(newWidget));

    let deserConfig3 = {
      useLatestWithMetadata: {
        "application.version": "v3"
      }
    }
    let deser3 = new AvroDeserializer(client, SerdeType.VALUE, deserConfig3)
    newobj = await deser3.deserialize(topic, bytes)
    expect(stringify(newobj)).toEqual(stringify(newerWidget));
  }
})
