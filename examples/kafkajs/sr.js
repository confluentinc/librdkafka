// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

// Note: The @confluentinc/schemaregistry will need to be installed separately to run this example,
//       as it isn't a dependency of confluent-kafka-javascript.
const { SchemaRegistryClient, SerdeType, AvroSerializer, AvroDeserializer} = require('@confluentinc/schemaregistry');

const registry = new SchemaRegistryClient({ baseURLs: ['<fill>'] })
const kafka = new Kafka({
    kafkaJS: {
        brokers: ['<fill>'],
        ssl: true,
        sasl: {
            mechanism: 'plain',
            username: '<fill>',
            password: '<fill>',
        },
    }
});

let consumer = kafka.consumer({
    kafkaJS: {
        groupId: "test-group",
        fromBeginning: true,
    },
});
let producer = kafka.producer();

const schemaA = {
    type: 'record',
    namespace: 'test',
    name: 'A',
    fields: [
        { name: 'id', type: 'int' },
        { name: 'b', type: 'test.B' },
    ],
};

const schemaB = {
    type: 'record',
    namespace: 'test',
    name: 'B',
    fields: [{ name: 'id', type: 'int' }],
};

const topicName = 'test-topic';
const subjectName = topicName + '-value';

const run = async () => {
    // Register schemaB.
    await registry.register(
        'avro-b',
        {
            schemaType: 'AVRO',
            schema: JSON.stringify(schemaB),
        }
    );
    const response = await registry.getLatestSchemaMetadata('avro-b');
    const version = response.version

    // Register schemaA, which references schemaB.
    const id = await registry.register(
        subjectName,
        {
            schemaType: 'AVRO',
            schema: JSON.stringify(schemaA),
            references: [
                {
                    name: 'test.B',
                    subject: 'avro-b',
                    version,
                },
            ],
        }
    )

    // Create an Avro serializer
    const ser = new AvroSerializer(registry, SerdeType.VALUE, { useLatestVersion: true });

    // Produce a message with schemaA.
    await producer.connect()
    const outgoingMessage = {
        key: 'key',
        value: await ser.serialize(topicName, { id: 1, b: { id: 2 } }),
    }
    await producer.send({
        topic: topicName,
        messages: [outgoingMessage]
    });
    console.log("Producer sent its message.")
    await producer.disconnect();
    producer = null;

    // Create an Avro deserializer
    const deser = new AvroDeserializer(registry, SerdeType.VALUE, {});

    await consumer.connect()
    await consumer.subscribe({ topic: topicName })

    let messageRcvd = false;
    await consumer.run({
        eachMessage: async ({ message }) => {
            const decodedMessage = {
                ...message,
                value: await deser.deserialize(topicName, message.value)
            };
            console.log("Consumer received message.\nBefore decoding: " + JSON.stringify(message) + "\nAfter decoding: " + JSON.stringify(decodedMessage));
            messageRcvd = true;
        },
    });

    // Wait around until we get a message, and then disconnect.
    while (!messageRcvd) {
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await consumer.disconnect();
    consumer = null;
}

run().catch (async e => {
    console.error(e);
    consumer && await consumer.disconnect();
    producer && await producer.disconnect();
    process.exit(1);
})
