const { Kafka } = require('../..').KafkaJS
//const { Kafka } = require('kafkajs')

async function consumerStart() {
    let consumer;
    const kafka = new Kafka({
        brokers: ['<fill>'],
        ssl: true,
        connectionTimeout: 5000,
        sasl: {
            mechanism: 'plain',
            username: '<fill>',
            password: '<fill>',
        },
        rebalanceListener: {
          onPartitionsAssigned: async (assignment) => {
            console.log(`Assigned partitions ${JSON.stringify(assignment)}`);
          },
          onPartitionsRevoked: async (assignment) => {
            console.log(`Revoked partitions ${JSON.stringify(assignment)}`);
            await consumer.commitOffsets().catch((e) => {
              console.error(`Failed to commit ${e}`);
            })
          }
        },
        rdKafka: {
          'enable.auto.commit': false
        }
    });

    consumer = kafka.consumer({ groupId: 'test-group' });

    await consumer.connect();
    console.log("Connected successfully");

    await consumer.subscribe({
      topics: [
        "topic2"
      ]
    })

    var batch = 0;
    consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: message.value.toString(),
        })

        if (++batch % 100 == 0) {
          await consumer.seek({
            topic,
            partition,
            offset: -2
          });
          await consumer.commitOffsets();
          batch = 0;
        }
      },
    });

    const disconnect = () => {
      process.off('SIGINT', disconnect);
      process.off('SIGTERM', disconnect);
      consumer.disconnect().finally(() => {
        console.log("Disconnected successfully");
      });
    }
    process.on('SIGINT', disconnect);
    process.on('SIGTERM', disconnect);
}

consumerStart()
