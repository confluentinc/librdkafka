const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

async function fetchMetadata() {
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
            'include-authorized-operations': {
                type: 'boolean',
                short: 'a',
                default: false,
            },
        },
    });

    const {
        'bootstrap-servers': bootstrapServers,
        timeout,
        'include-authorized-operations': includeAuthorizedOperations,
    } = args.values;

    const [topicName] = args.positionals;

    if (!topicName) {
        console.error('Topic name is required');
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
        // Fetch the topic metadata with specified options
        const metadata = await admin.fetchTopicMetadata(
            {
                topics: [topicName],
                includeAuthorizedOperations: includeAuthorizedOperations,
                timeout: Number(timeout), // Convert timeout to a number
            });

        console.log(`Metadata for topic "${topicName}":`, stringifyBigInt(metadata));
    } catch (err) {
        console.error('Error fetching topic metadata:', err);
    } finally {
        await admin.disconnect();
    }
}

function stringifyBigInt(obj) {
    return JSON.stringify(
        obj,
        (key, value) =>
            typeof value === 'bigint'
                ? value.toString() // Convert BigInt to string
                : value,
        2
    );
}

fetchMetadata();
