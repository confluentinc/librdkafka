const { Kafka } = require('../..').KafkaJS
//const { Kafka } = require('kafkajs')

async function consumerStart() {
    const kafka = new Kafka({
        brokers: ['pkc-8w6ry7.us-west-2.aws.devel.cpdev.cloud:9092'],
        ssl: true,
        sasl: {
            mechanism: 'plain',
            username: '<fill>',
            password: '<fill>',
        }
    });

    const consumer = kafka.consumer({ groupId: 'test-group' });

    await consumer.connect();
    console.log("Connected successfully");

    await consumer.subscribe({
      topics: [
        "topic2"
      ]
    })

    consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: message.value.toString(),
        })
      },
    });

    const disconnect = () =>
      consumer.disconnect().then(() => {
        console.log("Disconnected successfully");
      });
    process.on('SIGINT', disconnect);
    process.on('SIGTERM', disconnect);
}

consumerStart()
