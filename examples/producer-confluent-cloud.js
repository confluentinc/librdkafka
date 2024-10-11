const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
    const CLUSTER_BOOTSTRAP_URL = 'your_cluster_url_here';
    const CLUSTER_API_KEY = 'your_cluster_api_key_here';
    const CLUSTER_API_SECRET = 'your_cluster_api_secret_here';

    const producer = new Kafka().producer({
        'bootstrap.servers': `${CLUSTER_BOOTSTRAP_URL}`,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': `${CLUSTER_API_KEY}`,
        'sasl.password': `${CLUSTER_API_SECRET}`,
        'acks': 'all',
    });

    await producer.connect();
    console.log("Connected successfully");

    const res = []
    for (let i = 0; i < 50; i++) {
        res.push(producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'v222', partition: 1 },
                { value: 'v11', partition: 0, key: 'x' },
            ]
        }));
    }

    const produceRecords = await Promise.all(res);

    // Produce records is an array of delivery reports for each call to `send`.
    // In case `messages` contains more than one message to the same topic-partition, only the last
    // delivery report is included in the array.
    console.log("Produced messages, first delivery report:\n" + JSON.stringify(produceRecords[0], null, 2));
    console.log("Produced messages, last delivery report:\n" + JSON.stringify(produceRecords[produceRecords.length - 1], null, 2));

    await producer.disconnect();

    console.log("Disconnected successfully");
}

producerStart();
