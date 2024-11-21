import {afterEach, describe, expect, it} from '@jest/globals';
import {ClientConfig} from "../../rest-service";
import {
  ProtobufDeserializer, ProtobufDeserializerConfig,
  ProtobufSerializer, ProtobufSerializerConfig,
} from "../../serde/protobuf";
import {SerdeType} from "../../serde/serde";
import {
  Rule,
  RuleMode,
  RuleSet,
  SchemaInfo,
  SchemaRegistryClient
} from "../../schemaregistry-client";
import {LocalKmsDriver} from "../../rules/encryption/localkms/local-driver";
import {FieldEncryptionExecutor} from "../../rules/encryption/encrypt-executor";
import {AuthorSchema, file_test_schemaregistry_serde_example, PizzaSchema} from "./test/example_pb";
import {create, toBinary} from "@bufbuild/protobuf";
import {FileDescriptorProtoSchema} from "@bufbuild/protobuf/wkt";
import {
  NestedMessage_InnerMessageSchema
} from "./test/nested_pb";
import {TestMessageSchema} from "./test/test_pb";
import {DependencyMessageSchema} from "./test/dep_pb";
import {RuleRegistry} from "@confluentinc/schemaregistry/serde/rule-registry";
import {LinkedListSchema} from "./test/cycle_pb";
import {clearKmsClients} from "@confluentinc/schemaregistry/rules/encryption/kms-registry";

const fieldEncryptionExecutor = FieldEncryptionExecutor.register()
LocalKmsDriver.register()

//const baseURL = 'http://localhost:8081'
const baseURL = 'mock://'

const topic = 'topic1'
const subject = topic + '-value'

describe('ProtobufSerializer', () => {
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
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    ser.registry.add(AuthorSchema)
    let obj = create(AuthorSchema, {
      name: 'Kafka',
      id: 123,
      picture: Buffer.from([1, 2]),
      works: ['The Castle', 'The Trial']
    })
    let bytes = await ser.serialize(topic, obj)

    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('serialize second messsage', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    ser.registry.add(PizzaSchema)
    let obj = create(PizzaSchema, {
      size: 'Extra extra large',
      toppings: ['anchovies', 'mushrooms']
    })
    let bytes = await ser.serialize(topic, obj)

    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('serialize nested messsage', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    ser.registry.add(NestedMessage_InnerMessageSchema)
    let obj = create(NestedMessage_InnerMessageSchema, {
      id: "inner"
    })
    let bytes = await ser.serialize(topic, obj)

    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('serialize reference', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    ser.registry.add(TestMessageSchema)
    ser.registry.add(DependencyMessageSchema)
    let msg = create(TestMessageSchema, {
      testString: "hi",
      testBool: true,
      testBytes: Buffer.from([1, 2]),
      testDouble: 1.23,
      testFloat: 3.45,
      testFixed32: 67,
      testFixed64: 89n,
      testInt32: 100,
      testInt64: 200n,
      testSfixed32: 300,
      testSfixed64: 400n,
      testSint32: 500,
      testSint64: 600n,
      testUint32: 700,
      testUint64: 800n,
    })
    let obj = create(DependencyMessageSchema, {
      isActive: true,
      testMesssage: msg
    })
    let bytes = await ser.serialize(topic, obj)

    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2.testMesssage.testString).toEqual(msg.testString);
    expect(obj2.testMesssage.testBool).toEqual(msg.testBool);
    expect(obj2.testMesssage.testBytes).toEqual(msg.testBytes);
    expect(obj2.testMesssage.testDouble).toBeCloseTo(msg.testDouble, 0.001);
    expect(obj2.testMesssage.testFloat).toBeCloseTo(msg.testFloat, 0.001);
    expect(obj2.testMesssage.testFixed32).toEqual(msg.testFixed32);
    expect(obj2.testMesssage.testFixed64).toEqual(msg.testFixed64);
  })
  it('serialize cycle', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, {autoRegisterSchemas: true})
    ser.registry.add(LinkedListSchema)
    let inner = create(LinkedListSchema, {
      value: 100,
    })
    let obj = create(LinkedListSchema, {
      value: 1,
      next: inner
    })
    let bytes = await ser.serialize(topic, obj)

    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, {})
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)
  })
  it('basic encryption', async () => {
    let conf: ClientConfig = {
      baseURLs: [baseURL],
      cacheCapacity: 1000
    }
    let client = SchemaRegistryClient.newClient(conf)
    let serConfig: ProtobufSerializerConfig = {
      useLatestVersion: true,
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let ser = new ProtobufSerializer(client, SerdeType.VALUE, serConfig)
    ser.registry.add(AuthorSchema)
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
      schemaType: 'PROTOBUF',
      schema: Buffer.from(toBinary(FileDescriptorProtoSchema, file_test_schemaregistry_serde_example.proto)).toString('base64'),
      ruleSet
    }

    await client.register(subject, info, false)

    let obj = create(AuthorSchema, {
      name: 'Kafka',
      id: 123,
      picture: Buffer.from([1, 2]),
      works: ['The Castle', 'The Trial']
    })
    let bytes = await ser.serialize(topic, obj)

    // reset encrypted field
    obj.name = 'Kafka'
    obj.picture = Buffer.from([1, 2])

    let deserConfig: ProtobufDeserializerConfig = {
      ruleConfig: {
        secret: 'mysecret'
      }
    }
    let deser = new ProtobufDeserializer(client, SerdeType.VALUE, deserConfig)
    fieldEncryptionExecutor.client = dekClient
    let obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).toEqual(obj)

    clearKmsClients()
    let registry = new RuleRegistry()
    registry.registerExecutor(new FieldEncryptionExecutor())
    deser = new ProtobufDeserializer(client, SerdeType.VALUE, {}, registry)
    obj2 = await deser.deserialize(topic, bytes)
    expect(obj2).not.toEqual(obj);
  })
})
