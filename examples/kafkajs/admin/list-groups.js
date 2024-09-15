// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka, ConsumerGroupStates } = require('@confluentinc/kafka-javascript').KafkaJS;
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
      'states': {
        type: 'string',
        short: 's',
        multiple: true,
        default: [],
      }
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    states: matchConsumerGroupStates,
    timeout,
  } = args.values;

  if (timeout) {
    timeout = Number(timeout) || 0;
  }
  matchConsumerGroupStates = matchConsumerGroupStates.map(
    state => ConsumerGroupStates[state]);
  
  const kafka = new Kafka({
    kafkaJS: {
      brokers: [bootstrapServers],
    }
  });

  const admin = kafka.admin();
  await admin.connect();

  try {
    const groupOverview = await admin.listGroups({
      timeout,
      matchConsumerGroupStates
    });
    for (const group of groupOverview.groups) {
      console.log(`Group id: ${group.groupId}`);
      console.log(`\tType: ${group.protocolType}`);
      console.log(`\tIs simple: ${group.isSimpleConsumerGroup}`);
      console.log(`\tState: ${group.state}`);
    }
  } catch(err) {
    console.log('List topics failed', err);
  }

  await admin.disconnect();
}

adminStart();
