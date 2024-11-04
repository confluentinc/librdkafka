const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

async function fetchOffsets() {
  const args = parseArgs({
    allowPositionals: true,
    options: {
      'bootstrap-servers': {
        type: 'string',
        short: 'b',
        default: 'localhost:9092',
      },
      'timeout': {
        type: 'string',
        short: 'm',
        default: '5000',
      },
      'require-stable-offsets': {
        type: 'boolean',
        short: 'r',
        default: false,
      },
    },
  });

  const {
    'bootstrap-servers': bootstrapServers,
    timeout,
    'require-stable-offsets': requireStableOffsets,
  } = args.values;

  const [groupId, ...rest] = args.positionals;

  if (!groupId) {
    console.error('Group ID is required');
    process.exit(1);
  }

  const kafka = new Kafka({
    kafkaJS: {
      brokers: [bootstrapServers],
    },
  });

  const admin = kafka.admin();
  await admin.connect();

  try {
    // Parse topics and partitions from remaining arguments
    const topicInput = parseTopicsAndPartitions(rest);

    // Fetch offsets for the specified consumer group
    const offsets = await admin.fetchOffsets({
      groupId: groupId,
      topics: topicInput,
      requireStableOffsets,
      timeout: Number(timeout),
    });

    console.log(`Offsets for Consumer Group "${groupId}":`, JSON.stringify(offsets, null, 2));
  } catch (err) {
    console.error('Error fetching consumer group offsets:', err);
  } finally {
    await admin.disconnect();
  }
}

// Helper function to parse topics and partitions from arguments
function parseTopicsAndPartitions(args) {
  if (args.length === 0) return undefined;

  const topicInput = [];
  let i = 0;

  while (i < args.length) {
    const topic = args[i];
    i++;

    const partitions = [];
    while (i < args.length && !isNaN(args[i])) {
      partitions.push(Number(args[i]));
      i++;
    }

    // Add topic with partitions (or an empty array if no partitions specified)
    if (partitions.length > 0) {
      topicInput.push({ topic, partitions });
    } else {
      topicInput.push(topic); // Add as a string if no partitions specified
    }
  }

  return topicInput;
}

fetchOffsets();
