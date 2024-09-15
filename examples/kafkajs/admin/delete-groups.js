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
      'group-ids': {
        type: 'string',
        short: 'g',
        multiple: true,
        default: [],
      },
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    timeout,
    'group-ids': groupIds,
  } = args.values;
  
  if (!groupIds.length) {
    console.error('Group ids are required');
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
    await admin.deleteGroups(
      groupIds,
      { timeout },
    );
    console.log(`Groups "${groupIds.join(',')}" deleted successfully`);
  } catch(err) {
    console.log(`Group deletion failed`, err);
  }

  await admin.disconnect();
}

adminStart();
