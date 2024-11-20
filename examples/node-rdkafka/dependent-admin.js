const Kafka = require('@confluentinc/kafka-javascript');
const admin = require('../../lib/admin');

const bootstrapServers = 'localhost:9092';

function adminFromProducer(callback) {
    const producer = new Kafka.Producer({
        'bootstrap.servers': bootstrapServers,
        'dr_msg_cb': true,
    });

    const createAdminAndListAndDescribeTopics = (done) => {
        // Create an admin client from the producer, which must be connected.
        // Thus, this is called from the producer's 'ready' event.
        const admin = Kafka.AdminClient.createFrom(producer);

        // The admin client can be used until the producer is connected.
        admin.listTopics((err, topics) => {
            if (err) {
                console.error(err);
                return;
            }
            console.log("Topics: ", topics);

            // A common use case for the dependent admin client is to make sure the topic
            // is cached before producing to it. This avoids delay in sending the first
            // message to any topic. Using the admin client linked to the producer allows
            // us to do this, by calling `describeTopics` before we produce.
            // Here, we cache all possible topics, but it's advisable to only cache the
            // topics you are going to produce to (if you know it in advance),
            // and avoid calling listTopics().
            // Once a topic is cached, it will stay cached for `metadata.max.age.ms`,
            // which is 15 minutes by default, after which it will be removed if
            // it has not been produced to.
            admin.describeTopics(topics, null, (err, topicDescriptions) => {
                if (err) {
                    console.error(err);
                    return;
                }
                console.log("Topic descriptions fetched successfully");
                admin.disconnect();
                done();
            });
        });
    };

    producer.connect();

    producer.on('ready', () => {
        console.log("Producer is ready");
        producer.setPollInterval(100);

        // After the producer is ready, it can be used to create an admin client.
        createAdminAndListAndDescribeTopics(() => {
            // The producer can also be used normally to produce messages.
            producer.produce('test-topic', null, Buffer.from('Hello World!'), null, Date.now());
        });

    });

    producer.on('event.error', (err) => {
        console.error(err);
        producer.disconnect(callback);
    });

    producer.on('delivery-report', (err, report) => {
        console.log("Delivery report received:", report);
        producer.disconnect(callback);
    });
}

function adminFromConsumer() {
    const consumer = new Kafka.KafkaConsumer({
        'bootstrap.servers': bootstrapServers,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest',
    });

    const createAdminAndListTopics = () => {
        // Create an admin client from the consumer, which must be connected.
        // Thus, this is called from the consumer's 'ready' event.
        const admin = Kafka.AdminClient.createFrom(consumer);

        // The admin client can be used until the consumer is connected.
        admin.listTopics((err, topics) => {
            if (err) {
                console.error(err);
                return;
            }
            console.log("Topics: ", topics);
            admin.disconnect();
        });
    };

    consumer.connect();

    consumer.on('ready', () => {
        console.log("Consumer is ready");

        // After the consumer is ready, it can be used to create an admin client.
        createAdminAndListTopics();

        // It can also be used normally to consume messages.
        consumer.subscribe(['test-topic']);
        consumer.consume();
    });

    consumer.on('data', (data) => {
        // Quit after receiving a message.
        console.log("Consumer:data", data);
        consumer.disconnect();
    });

    consumer.on('event.error', (err) => {
        console.error("Consumer:error", err);
        consumer.disconnect();
    });
}

adminFromProducer(() => adminFromConsumer());
