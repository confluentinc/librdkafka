import * as RdKafka from '@confluentinc/kafka-javascript';

const bootstrapServers = '<fill>';

function runProducer() {
    const producer = new RdKafka.Producer({
        'bootstrap.servers': bootstrapServers,
        'dr_msg_cb': true,
    });

    producer.connect();

    producer.on('ready', () => {
        console.log("Producer is ready");
        producer.setPollInterval(100);
        producer.produce('test-topic', null, Buffer.from('Hello World!'), null, Date.now());
    });

    producer.on('event.error', (err) => {
        console.error(err);
    });

    producer.on('delivery-report', (err, report) => {
        console.log("Delivery report received:");
        console.log({err, report});
        producer.disconnect(err => {
            if (err)
                console.log("Error disconnecting producer ", err);
            console.log("Disconnected producer");
        });
    });
}

function runConsumer() {
    const consumer = new RdKafka.KafkaConsumer({
        'group.id': 'test-group',
        'bootstrap.servers': bootstrapServers,
    }, {
        'auto.offset.reset': 'earliest',
    });

    consumer.connect();

    consumer.on('ready', () => {
        console.log("Consumer is ready");
        consumer.subscribe(['test-topic']);
        consumer.consume();
    });

    consumer.on('data', (data) => {
        console.log("Received data");
        console.log(data);
    });

    consumer.on('event.error', (err) => {
        console.error(err);
    });

    setTimeout(() => consumer.disconnect(), 30000);
}

function runAdminClient() {
    const admin = RdKafka.AdminClient.create({
        "bootstrap.servers": bootstrapServers,
    });

    admin.createTopic({ topic: "test-topic", num_partitions: 1, replication_factor: 1 }, (err) => {
        if (err) {
            console.error(err);
            admin.disconnect();
            return;
        }
        console.log("Created topic");
        admin.disconnect();
    });

}

// As an example, run each with some time gap to allow the prior one to finish.
runAdminClient();
setTimeout(runProducer, 5000);
setTimeout(runConsumer, 25000);