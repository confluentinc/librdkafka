// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
    const CLUSTER_BOOTSTRAP_URL = 'your_cluster_url_here';
    const CLUSTER_API_KEY = 'your_cluster_api_key_here';
    const CLUSTER_API_SECRET = 'your_cluster_api_secret_here';

    const kafka = new Kafka({
        kafkaJS: {
            brokers: [`${CLUSTER_BOOTSTRAP_URL}`],
            ssl: true,
            sasl: {
                mechanism: 'plain',
                username: `${CLUSTER_API_KEY}`,
                password: `${CLUSTER_API_SECRET}`,
            },
        }
    });

    const producer = kafka.producer();

    await producer.connect();

    console.log("Connected successfully");

    const res = []
    for (let i = 0; i < 50; i++) {
        res.push(producer.send({
            topic: 'test-topic',
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
