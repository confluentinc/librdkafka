import { KafkaJS } from '@confluentinc/kafka-javascript';

const bootstrapServer = '<fill>';

async function runProducer() {
    const kafka = new KafkaJS.Kafka({
        kafkaJS: {
            brokers: [bootstrapServer],
        },
    });

    const producer = kafka.producer({
        kafkaJS: {
            allowAutoTopicCreation: true,
            acks: 1,
            compression: KafkaJS.CompressionTypes.GZIP,
        }
    });

    await producer.connect();

    await producer.send({
        topic: 'test-topic',
        messages: [
            {
                value: 'Hello World!',
                key: 'key1',
                headers: {
                    'header1': 'value1',
                    'header2': [Buffer.from('value2'), 'value3']
                }
            },
        ],
    });

    await producer.disconnect();
}

async function runConsumer() {
    const kafka = new KafkaJS.Kafka({
        kafkaJS: {
            brokers: [bootstrapServer],
        },
    });

    const consumer = kafka.consumer({
        kafkaJS: {
            groupId: 'test-group' + Math.random(),
            fromBeginning: true,
            partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
        },
    });

    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic' });

    await consumer.run({
        eachMessage: async ({ message }) => {
            console.log({
                key: message.key ? message.key.toString() : null,
                value: message.value ? message.value.toString() : null,
                headers: message.headers,
            });
        },
    });

    await new Promise((resolve) => setTimeout(resolve, 30000));
    await consumer.disconnect();
}

async function runAdminClient() {
    const kafka = new KafkaJS.Kafka({
        kafkaJS: {
            brokers: [bootstrapServer],
        },
    });

    const admin = kafka.admin()
    await admin.connect();

    await admin.createTopics({ topics: [{ topic: 'test-topic' }] });
    console.log("Created topic");

    await admin.disconnect();
}

runAdminClient()
    .then(runProducer)
    .then(runConsumer)
    .catch(console.error);