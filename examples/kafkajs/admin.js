const { Kafka } = require('../..').KafkaJS
//const { Kafka } = require('kafkajs')

async function adminStart() {
  const kafka = new Kafka({
    kafkaJS: {
      brokers: ['localhost:9092'],
    }
  });

  const admin = kafka.admin();
  await admin.connect();

  await admin.createTopics({
    topics: [
      {
        topic: 'test-topic',
        numPartitions: 3,
        replicationFactor: 1,
      }
    ]
  }).then(() => {
    console.log("Topic created successfully");
  }).catch((err) => {
    console.log("Topic creation failed", err);
  });

  await admin.deleteTopics({
    topics: ['test-topic'],
    timeout: 5600,
  }).then(() => {
    console.log("Topic deleted successfully");
  }).catch((err) => {
    console.log("Topic deletion failed", err);
  });

  await admin.disconnect();
}

adminStart();
