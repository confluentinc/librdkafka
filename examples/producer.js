const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
    const producer = new Kafka().producer({
        'bootstrap.servers': 'localhost:9092',
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
