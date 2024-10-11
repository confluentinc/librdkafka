// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function consumerStart() {
  let consumer;
  var stopped = false;

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

  consumer = kafka.consumer({
    kafkaJS: {
      groupId: 'test-group',
    },

    /* Properties from librdkafka can also be used */
    'auto.commit.interval.ms': 6000,
  });

  await consumer.connect();
  console.log("Connected successfully");

  await consumer.subscribe({
    topics: [
      "test-topic"
    ]
  })

  // Start consuming messages.
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

  // Disconnect example
  const disconnect = () => {
    process.off('SIGINT', disconnect);
    process.off('SIGTERM', disconnect);
    stopped = true;
    consumer.commitOffsets()
      .finally(() =>
        consumer.disconnect()
      )
      .finally(() =>
        console.log("Disconnected successfully")
      );
  }
  process.on('SIGINT', disconnect);
  process.on('SIGTERM', disconnect);
}

consumerStart();
