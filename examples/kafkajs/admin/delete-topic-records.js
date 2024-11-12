const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { parseArgs } = require('node:util');

async function deleteTopicRecords() {
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
            'operation-timeout': {
                type: 'string',
                short: 'o',
                default: '60000',
            },
        },
    });

    const {
        'bootstrap-servers': bootstrapServers,
        timeout,
        'operation-timeout': operationTimeout,
    } = args.values;

    const [topic, ...rest] = args.positionals;

    if (!topic || rest.length % 2 !== 0) {
        console.error("Usage: node deleteTopicRecords.js --bootstrap-servers <servers> --timeout <timeout> --operation-timeout <operation-timeout> <topic> <partition offset ...>");
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
        // Parse partitions and offsets, ensuring pairs of partition and offset are provided
        const partitionsInput = parsePartitionsAndOffsets(rest);

        // Delete records for the specified topic and partitions
        const result = await admin.deleteTopicRecords({
            topic: topic,
            partitions: partitionsInput,
            timeout: Number(timeout),
            operationTimeout: Number(operationTimeout),
        });

        console.log(`Records deleted for Topic "${topic}":`, JSON.stringify(result, null, 2));
    } catch (err) {
        console.error("Error deleting topic records:", err);
    } finally {
        await admin.disconnect();
    }
}

// Helper function to parse partitions and offsets from arguments
function parsePartitionsAndOffsets(args) {
    const partitions = [];
    for (let i = 0; i < args.length; i += 2) {
        const partition = parseInt(args[i]);
        const offset = args[i + 1];
        if (isNaN(partition) || isNaN(parseInt(offset))) {
            console.error("Partition and offset should be numbers and provided in pairs.");
            process.exit(1);
          }
        partitions.push({ partition, offset });
    }
    return partitions;
}

deleteTopicRecords();
