const { runProducer, runConsumer, runConsumeTransformProduce } = require('./performance-primitives');

const { CompressionTypes } = require('../../').KafkaJS;

const brokers = process.env.KAFKA_BROKERS || 'localhost:9092';
const topic = process.env.KAFKA_TOPIC || 'test-topic';
const topic2 = process.env.KAFKA_TOPIC2 || 'test-topic2';
const messageCount = process.env.MESSAGE_COUNT ? +process.env.MESSAGE_COUNT : 1000000;
const messageSize = process.env.MESSAGE_SIZE ? +process.env.MESSAGE_SIZE : 256;
const batchSize = process.env.BATCH_SIZE ? +process.env.BATCH_SIZE : 100;
const compression = process.env.COMPRESSION || CompressionTypes.NONE;
const warmupMessages = process.env.WARMUP_MESSAGES ? +process.env.WARMUP_MESSAGES : (batchSize * 10);

(async function () {
    const producer = process.argv.includes('--producer');
    const consumer = process.argv.includes('--consumer');
    const ctp = process.argv.includes('--ctp');
    const all = process.argv.includes('--all');

    if (producer || all) {
        console.log("=== Running Basic Producer Performance Test:")
        console.log(`  Brokers: ${brokers}`);
        console.log(`  Topic: ${topic}`);
        console.log(`  Message Count: ${messageCount}`);
        console.log(`  Message Size: ${messageSize}`);
        console.log(`  Batch Size: ${batchSize}`);
        console.log(`  Compression: ${compression}`);
        console.log(`  Warmup Messages: ${warmupMessages}`);
        const producerRate = await runProducer(brokers, topic, batchSize, warmupMessages, messageCount, messageSize, compression);
        console.log("=== Producer Rate: ", producerRate);
    }

    if (consumer || all) {
        // If user runs this without --producer then they are responsible for seeding the topic.
        console.log("=== Running Basic Consumer Performance Test:")
        console.log(`  Brokers: ${brokers}`);
        console.log(`  Topic: ${topic}`);
        console.log(`  Message Count: ${messageCount}`);
        const consumerRate = await runConsumer(brokers, topic, messageCount);
        console.log("=== Consumer Rate: ", consumerRate);
    }

    if (ctp || all) {
        console.log("=== Running Consume-Transform-Produce Performance Test:")
        console.log(`  Brokers: ${brokers}`);
        console.log(`  ConsumeTopic: ${topic}`);
        console.log(`  ProduceTopic: ${topic2}`);
        console.log(`  Message Count: ${messageCount}`);
        // Seed the topic with messages
        await runProducer(brokers, topic, batchSize, warmupMessages, messageCount, messageSize, compression);
        const ctpRate = await runConsumeTransformProduce(brokers, topic, topic2, messageCount);
        console.log("=== Consume-Transform-Produce Rate: ", ctpRate);
    }

})();