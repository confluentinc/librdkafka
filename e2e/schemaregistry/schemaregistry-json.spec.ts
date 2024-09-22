import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  Metadata,
  SchemaRegistryClient,
  SchemaInfo,
  Reference
} from '../../schemaregistry/schemaregistry-client';
import { beforeEach, afterEach, describe, expect, it } from '@jest/globals';
import { clientConfig } from '../../test/schemaregistry/test-constants';
import { JsonSerializer, JsonSerializerConfig, JsonDeserializer } from '../../schemaregistry/serde/json';
import { SerdeType } from "../../schemaregistry/serde/serde";
import stringify from 'json-stringify-deterministic';

let schemaRegistryClient: SchemaRegistryClient;
let producer: any;

const testServerConfigSubject = 'integ-test-server-config-subject';

const kafkaBrokerList = 'localhost:9092';
const kafka = new KafkaJS.Kafka({
  kafkaJS: {
    brokers: [kafkaBrokerList],
  },
});
const testTopic = `test-topic`;
const testTopicValue = testTopic + '-value';

//Inspired by dotnet client
const schemaString: string = stringify({
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Person",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "FirstName",
    "LastName"
  ],
  "properties": {
    "FirstName": {
      "type": "string"
    },
    "MiddleName": {
      "type": [
        "null",
        "string"
      ]
    },
    "LastName": {
      "type": "string"
    },
    "Gender": {
      "oneOf": [
        {
          "$ref": "#/definitions/Gender"
        }
      ]
    },
    "NumberWithRange": {
      "type": "integer",
      "format": "int32",
      "maximum": 5.0,
      "minimum": 2.0
    },
    "Birthday": {
      "type": "string",
      "format": "date-time"
    },
    "Company": {
      "oneOf": [
        {
          "$ref": "#/definitions/Company"
        },
        {
          "type": "null"
        }
      ]
    },
    "Cars": {
      "type": [
        "array",
        "null"
      ],
      "items": {
        "$ref": "#/definitions/Car"
      }
    }
  },
  "definitions": {
    "Gender": {
      "type": "integer",
      "description": "",
      "x-enumNames": [
        "Male",
        "Female"
      ],
      "enum": [
        0,
        1
      ]
    },
    "Company": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "Name": {
          "type": [
            "null",
            "string"
          ]
        }
      }
    },
    "Car": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "Name": {
          "type": [
            "null",
            "string"
          ]
        },
        "Manufacturer": {
          "oneOf": [
            {
              "$ref": "#/definitions/Company"
            },
            {
              "type": "null"
            }
          ]
        }
      }
    }
  }
});

const orderDetailsSchema: SchemaInfo = {

  schema: stringify({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/order_details.schema.json",
    "title": "OrderDetails",
    "description": "Order Details",
    "type": "object",
    "properties": {
        "id": {
            "description": "Order Id",
            "type": "integer"
        },
        "customer": {
            "description": "Customer",
            "$ref": "http://example.com/customer.schema.json"
        },
        "payment_id": {
            "description": "Payment Id",
            "type": "string"
        }
    },
    "required": [ "id", "customer"]
}),
  schemaType: 'JSON',
};

const orderSchema: SchemaInfo = {
  schema: stringify({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/referencedproduct.schema.json",
    "title": "Order",
    "description": "Order",
    "type": "object",
    "properties": {
      "order_details": {
        "description": "Order Details",
        "$ref": "http://example.com/order_details.schema.json"
      },
      "order_date": {
        "description": "Order Date",
        "type": "string",
        "format": "date-time"
      }
    },
    "required": ["order_details"]
  }),
  schemaType: 'JSON',
};

const customerSchema: SchemaInfo = {
  schema: stringify({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/customer.schema.json",
    "title": "Customer",
    "description": "Customer Data",
    "type": "object",
    "properties": {
      "name": {
        "Description": "Customer name",
        "type": "string"
      },
      "id": {
        "description": "Customer id",
        "type": "integer"
      },
      "email": {
        "description": "Customer email",
        "type": "string"
      }
    },
    "required": ["name", "id"]
  }),
  schemaType: 'JSON',
};

const messageValue = {
  "firstName": "Real",
  "middleName": "Name",
  "lastName": "LastName D. Roger",
  "gender": "Male",
  "numberWithRange": 3,
  "birthday": 7671,
  "company": {
    "name": "WarpStream"
  },
  "cars": [
    {
      "name": "Flink",
      "manufacturer": {
        "name": "Immerok"
      }
    },
    {
      "name": "Car",
      "manufacturer": {
        "name": "Car Maker"
      }
    }
  ]
};


const metadata: Metadata = {
  properties: {
    owner: 'Bob Jones',
    email: 'bob@acme.com',
  },
};

const schemaInfo: SchemaInfo = {
  schema: schemaString,
  metadata: metadata,
  schemaType: 'JSON'
};

const customerSubject = 'Customer';
const orderSubject = 'Order';
const orderDetailsSubject = 'OrderDetails';

const subjectList = [testTopic, testTopicValue, testServerConfigSubject, orderSubject, orderDetailsSubject, customerSubject];

describe('SchemaRegistryClient json Integration Test', () => {

  beforeEach(async () => {
    schemaRegistryClient = new SchemaRegistryClient(clientConfig);

    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.deleteTopics({
        topics: [testTopic],
        timeout: 5000,
      });
    } catch (error) {
      // Topic may not exist; ignore error
    }
    await admin.disconnect();

    producer = kafka.producer({
      kafkaJS: {
        allowAutoTopicCreation: true,
        acks: 1,
        compression: KafkaJS.CompressionTypes.GZIP,
      }
    });
    await producer.connect();
    const subjects: string[] = await schemaRegistryClient.getAllSubjects();

    for (const subject of subjectList) {
      if (subjects && subjects.includes(subject)) {
        await schemaRegistryClient.deleteSubject(subject);
        await schemaRegistryClient.deleteSubject(subject, true);
      }
    }
  });

  afterEach(async () => {
    await producer.disconnect();
    producer = null;
  });

  it("Should serialize and deserialize json", async () => {

    await schemaRegistryClient.register(testTopic, schemaInfo);

    const serializerConfig: JsonSerializerConfig = { autoRegisterSchemas: true };
    const serializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
    const deserializer = new JsonDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(testTopic, messageValue)
    };

    await producer.send({
      topic: testTopic,
      messages: [outgoingMessage]
    });

    let consumer = kafka.consumer({
      kafkaJS: {
        groupId: 'test-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });

    await consumer.connect();
    await consumer.subscribe({ topic: testTopic });
    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(testTopic, message.value as Buffer)
        };
        messageRcvd = true;

        expect(decodedMessage.value).toMatchObject(messageValue);
      },
    });

    // Wait around until we get a message, and then disconnect.
    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
    expect(1).toEqual(1);
  }, 30000);

  it("Should serialize with UseLatestVersion enabled", async () => {
    await schemaRegistryClient.register(testTopic, schemaInfo);

    const serializerConfig: JsonSerializerConfig = { autoRegisterSchemas: true, useLatestVersion: true };
    const serializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);

    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(testTopic, messageValue)
    };

    await producer.send({
      topic: testTopic,
      messages: [outgoingMessage]
    });

  });

  it('Should fail to serialize with UseLatestVersion enabled and autoRegisterSchemas disabled', async () => {
    await schemaRegistryClient.register(testTopic, schemaInfo);

    const serializerConfig: JsonSerializerConfig = { autoRegisterSchemas: false, useLatestVersion: true };
    const serializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);

    const messageValue = { "name": "Bob Jones", "age": 25 };

    await expect(serializer.serialize(testTopic, messageValue)).rejects.toThrowError();
  });

  it("Should serialize referenced schemas", async () => {
    const serializerConfig: JsonSerializerConfig = { autoRegisterSchemas: true };
    const serializer = new JsonSerializer(schemaRegistryClient, SerdeType.VALUE, serializerConfig);
    const deserializer = new JsonDeserializer(schemaRegistryClient, SerdeType.VALUE, {});

    await schemaRegistryClient.register(customerSubject, customerSchema);
    const customerIdVersion: number = (await schemaRegistryClient.getLatestSchemaMetadata(customerSubject)).version!;
    
    const customerReference: Reference = {
      name: "http://example.com/customer.schema.json",
      subject: customerSubject,
      version: customerIdVersion,
    };
    orderDetailsSchema.references = [customerReference];

    await schemaRegistryClient.register(orderDetailsSubject, orderDetailsSchema);
    const orderDetailsIdVersion: number = (await schemaRegistryClient.getLatestSchemaMetadata(orderDetailsSubject)).version!;

    const orderDetailsReference: Reference = {
      name: "http://example.com/order_details.schema.json",
      subject: orderDetailsSubject,
      version: orderDetailsIdVersion,
    };
    orderSchema.references = [orderDetailsReference];

    const orderId = await schemaRegistryClient.register(orderSubject, orderSchema);
    await schemaRegistryClient.register(orderSubject, orderSchema);
    console.log(`Order schema id: ${orderId}`);

    const order = {
      order_details: {
        id: 1,
        customer: {
          name: "Bob Jones",
          id: 1,
          email: "bob@jones.com"
        },
        payment_id: "1234"
      },
      order_date: "2021-07-15T12:00:00Z"
    };

    const outgoingMessage = {
      key: 'key',
      value: await serializer.serialize(orderSubject, order)
    };

    await producer.send({
      topic: testTopic,
      messages: [outgoingMessage]
    });

    let consumer = kafka.consumer({
      kafkaJS: {
        groupId: 'test-group',
        fromBeginning: true,
        partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      },
    });

    await consumer.connect();

    await consumer.subscribe({ topic: testTopic });

    let messageRcvd = false;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const decodedMessage = {
          ...message,
          value: await deserializer.deserialize(orderSubject, message.value as Buffer)
        };
        messageRcvd = true;

        expect(decodedMessage.value).toMatchObject(order);
      },
    });

    // Wait around until we get a message, and then disconnect.
    while (!messageRcvd) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
  }, 30000);
});