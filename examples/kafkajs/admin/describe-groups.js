// require('kafkajs') is replaced with require('@confluentinc/kafka-javascript').KafkaJS.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

function printNode(node, prefix = '') {
  if (!node)
    return;
  console.log(`${prefix}\tHost: ${node.host}`);
  console.log(`${prefix}\tPort: ${node.port}`);
  console.log(`${prefix}\tRack: ${node.rack}`);
}

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
      'groups': {
        type: 'string',
        short: 'g',
        multiple: true,
        default: [],
      },
      'include-authorized-operations': {
        type: 'boolean',
        short: 'i',
        default: false,
      }
    },
  });

  let {
    'bootstrap-servers': bootstrapServers,
    timeout,
    groups,
    'include-authorized-operations': includeAuthorizedOperations,
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
    const groupDescriptions = await admin.describeGroups(
      groups,
      {
        timeout,
        includeAuthorizedOperations,
      }
    );
    for (const group of groupDescriptions.groups) {
      console.log(`Group id: ${group.groupId}`);
      console.log(`\tError: ${group.error}`);
      console.log(`\tProtocol: ${group.protocol}`);
      console.log(`\tProtocol type: ${group.protocolType}`);
      console.log(`\tPartition assignor: ${group.partitionAssignor}`);
      console.log(`\tState: ${group.state}`);
      console.log(`\tType: ${group.type}`);
      console.log(`\tCoordinator: ${group.coordinator ? group.coordinator.id : group.coordinator}`);
      printNode(group.coordinator, '\t');
      console.log(`\tAuthorized operations: ${group.authorizedOperations}`);
      console.log(`\tIs simple: ${group.isSimpleConsumerGroup}`);
      console.log(`\tState: ${group.state}`);
    }
  } catch(err) {
    console.log('Describe groups failed', err);
  }

  await admin.disconnect();
}

adminStart();
