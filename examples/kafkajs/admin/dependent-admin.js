// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function adminFromConsumer() {
    const kafka = new Kafka({
        kafkaJS: {
            brokers: ['localhost:9092'],
        }
    });

    const consumer = kafka.consumer({
        kafkaJS: {
            groupId: 'test-group',
            fromBeginning: true,
        }
    });

    await consumer.connect();

    // The consumer can be used as normal
    await consumer.subscribe({ topic: 'test-topic' });
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                topic,
                partition,
                offset: message.offset,
                key: message.key?.toString(),
                value: message.value.toString(),
            });
        },
    });

    // And the same consumer can create an admin client - the consumer must have successfully
    // been connected before the admin client can be created.
    const admin = consumer.dependentAdmin();
    await admin.connect();

    // The admin client can be used until the consumer is connected.
    const listTopicsResult = await admin.listTopics();
    console.log(listTopicsResult);

    await new Promise(resolve => setTimeout(resolve, 10000));

    // Disconnect the consumer and admin clients in the correct order.
    await admin.disconnect();
    await consumer.disconnect();
}

async function adminFromProducer() {
    const kafka = new Kafka({
        kafkaJS: {
            brokers: ['localhost:9092'],
        }
    });

    const producer = kafka.producer({
        'metadata.max.age.ms': 900000, /* This is set to the default value. */
    });

    await producer.connect();

    // And the same producer can create an admin client - the producer must have successfully
    // been connected before the admin client can be created.
    const admin = producer.dependentAdmin();
    await admin.connect();

    // The admin client can be used until the producer is connected.
    const listTopicsResult = await admin.listTopics();
    console.log(listTopicsResult);

    // A common use case for the dependent admin client is to make sure the topic
    // is cached before producing to it. This avoids delay in sending the first
    // message to any topic. Using the admin client linked to the producer allows
    // us to do this, by calling `fetchTopicMetadata` before we produce.
    // Here, we cache all possible topics, but it's advisable to only cache the
    // topics you are going to produce to (if you know it in advance),
    // and avoid calling listTopics().
    // Once a topic is cached, it will stay cached for `metadata.max.age.ms`,
    // which is 15 minutes by default, after which it will be removed if
    // it has not been produced to.
    await admin.fetchTopicMetadata({ topics: listTopicsResult }).catch(e => {
        console.error('Error caching topics: ', e);
    })

    // The producer can be used as usual.
    await producer.send({ topic: 'test-topic', messages: [{ value: 'Hello!' }] });

    // Disconnect the producer and admin clients in the correct order.
    await admin.disconnect();
    await producer.disconnect();
}

adminFromProducer().then(() => adminFromConsumer()).catch(console.error);
