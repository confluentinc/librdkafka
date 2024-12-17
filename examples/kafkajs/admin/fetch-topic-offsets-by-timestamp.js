const { Kafka, IsolationLevel } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

async function fetchOffsetsByTimestamp() {
    // Parse command-line arguments
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
                short: 't',
                default: '5000',
            },
            'isolation-level': {
                type: 'string',
                short: 'i',
                default: '0', // Default to '0' for read_uncommitted
            },
            'timestamp': {
                type: 'string',
                short: 's',
            },
        },
    });

    const {
        'bootstrap-servers': bootstrapServers,
        timeout,
        'isolation-level': isolationLevel,
        timestamp,
    } = args.values;

    const [topic] = args.positionals;

    if (!topic) {
        console.error('Topic name is required');
        process.exit(1);
    }

    // Determine the isolation level
    let isolationLevelValue;
    if (isolationLevel === '0') {
        isolationLevelValue = IsolationLevel.READ_UNCOMMITTED;
    } else if (isolationLevel === '1') {
        isolationLevelValue = IsolationLevel.READ_COMMITTED;
    } else {
        console.error('Invalid isolation level. Use 0 for READ_UNCOMMITTED or 1 for READ_COMMITTED.');
        process.exit(1);
    }

    // Parse the timestamp if provided
    const timestampValue = timestamp ? Number(timestamp) : undefined;

    const kafka = new Kafka({
        kafkaJS: {
            brokers: [bootstrapServers],
        },
    });

    const admin = kafka.admin();
    await admin.connect();

    try {
        // Prepare options
        const options = {
            isolationLevel: isolationLevelValue,
            timeout: Number(timeout),
        };

        // Fetch offsets by timestamp for the specified topic
        const offsets = await admin.fetchTopicOffsetsByTimestamp(
            topic,
            timestampValue, // Only pass timestamp if provided
            options
        );

        console.log(`Offsets for topic "${topic}" with timestamp ${timestampValue || 'not provided'}:`, JSON.stringify(offsets, null, 2));
    } catch (err) {
        console.error('Error fetching topic offsets by timestamp:', err);
    } finally {
        await admin.disconnect();
    }
}

fetchOffsetsByTimestamp();
