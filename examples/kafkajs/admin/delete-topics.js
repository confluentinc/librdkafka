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
      'timeout': {
        type: 'string',
        short: 'm',
        default: undefined,
      },
      'topics': {
        type: 'string',
        short: 't',
        multiple: true,
        default: [],
      },
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    timeout,
    topics,
  } = args.values;

  if (!topics.length) {
    console.error('Topics names is required');
    process.exit(1);
  }
  
  if (timeout) {
    timeout = Number(timeout) || 0;
  }
  
  const kafka = new Kafka({
    kafkaJS: {
      brokers: [bootstrapServers],
    }
  });

  const admin = kafka.admin();
  await admin.connect();
  
  try {
    await admin.deleteTopics({
      topics,
      timeout,
    });
    console.log(`Topics "${topics.join(',')}" deleted successfully`);
  } catch(err) {
    console.log(`Topic deletion failed`, err);
  }

  await admin.disconnect();
}

adminStart();
