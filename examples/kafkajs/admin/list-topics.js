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
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    timeout
  } = args.values;
  
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
    const topics = await admin.listTopics({ timeout });
    for (const topic of topics) {
      console.log(`Topic name: ${topic}`);
    }
  } catch(err) {
    console.log('List topics failed', err);
  }

  await admin.disconnect();
}

adminStart();
