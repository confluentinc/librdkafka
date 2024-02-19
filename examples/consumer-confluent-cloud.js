const { Kafka, ErrorCodes } = require('@confluentinc/kafka-javascript').KafkaJS;

async function consumerStart() {
  let consumer;
  let stopped = false;

  const CLUSTER_BOOTSTRAP_URL = 'your_cluster_url_here';
  const CLUSTER_API_KEY = 'your_cluster_api_key_here';
  const CLUSTER_API_SECRET = 'your_cluster_api_secret_here';

  // Set up signals for a graceful shutdown.
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

  // Initialization
  consumer = new Kafka().consumer({
    'bootstrap.servers': `${CLUSTER_BOOTSTRAP_URL}`,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': `${CLUSTER_API_KEY}`,
    'sasl.password': `${CLUSTER_API_SECRET}`,
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',
    'enable.partition.eof': 'true',
  });

  await consumer.connect();
  console.log("Connected successfully");
  await consumer.subscribe({ topics: ["test-topic"] });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      });
    }
  });
}

consumerStart();
