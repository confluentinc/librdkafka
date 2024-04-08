// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
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

    const producer = kafka.producer();

    await producer.connect();

    console.log("Connected successfully");

    const res = []
    for (let i = 0; i < 50; i++) {
        res.push(producer.send({
            topic: 'topic2',
            messages: [
                { value: 'v222', partition: 0 },
                { value: 'v11', partition: 0, key: 'x' },
            ]
        }));
    }
    await Promise.all(res);

    await producer.disconnect();

    console.log("Disconnected successfully");
}

producerStart();
