// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

async function adminStart() {
  const args = parseArgs({
    options: {
      'bootstrap-servers': {
        type: 'string',
        short: 'b',
        default: 'localhost:9092',
      },
      'topic': {
        type: 'string',
        short: 't',
        default: 'test-topic',
      },
      'timeout': {
        type: 'string',
        short: 'm',
        default: undefined,
      },
      'num-partitions': {
        type: 'string',
        short: 'p',
        default: '3',
      },
      'replication-factor': {
        type: 'string',
        short: 'r',
        default: '1',
      }
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    timeout,
    'num-partitions': numPartitions,
    'replication-factor': replicationFactor,	
    topic,
  } = args.values;

  if (timeout) {
    timeout = Number(timeout) || 0;
  }

  numPartitions = Number(numPartitions) || 3;
  replicationFactor = Number(replicationFactor) || 1;
  
  const kafka = new Kafka({
    kafkaJS: {
      brokers: [bootstrapServers],
    }
  });

  const admin = kafka.admin();
  await admin.connect();
  
  try {
    await admin.createTopics({
      topics: [
        {
          topic: topic,
          numPartitions: numPartitions,
          replicationFactor: replicationFactor,
        }
      ],
      timeout,
    });
    console.log(`Topic "${topic}" created successfully`);
  } catch(err) {
    console.log(`Topic creation failed`, err);
  }

  await admin.disconnect();
}

adminStart();
