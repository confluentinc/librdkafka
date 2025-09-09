/* eslint-disable no-unused-vars */
jest.setTimeout(30000);

const process = require('process');
const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    sleep,
} = require('../testhelpers');

// See https://github.com/confluentinc/confluent-kafka-javascript/issues/285
describe('Consumer commit', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;
        await createTopic({ topic: topicName, partitions: 1 });
        producer = createProducer({});
        consumer = createConsumer({
            groupId,
            fromBeginning: true,
            autoCommit: false,
        });
    });

    afterEach(async () => {
        if (consumer) await consumer.disconnect();
        if (producer) await producer.disconnect();
    });

    it('should not have significant latency after pause-seek-resume', async () => {
        const initialMessages = Array(30).fill().map(() => ({ value: `value-${secureRandom()}` }));
        await producer.connect();
        await producer.send({ topic: topicName, messages: initialMessages });
        await producer.flush();

        let msgCount = 0;
        let maxLatencyMs = 0;
        let startTime = process.hrtime.bigint();

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const endTime = process.hrtime.bigint();
                const latencyMs = Number((endTime - startTime) / BigInt(1e6));
                if (msgCount > 0) maxLatencyMs = Math.max(maxLatencyMs, latencyMs);

                msgCount++;
                await sleep(10);
                startTime = process.hrtime.bigint();
            }
        });

        // Wait for 5 messages to be processed
        await waitFor(() => msgCount >= 5, () => null, { delay: 100, timeout: 10000 });

        await consumer.pause([{ topic: topicName, partition: 0 }]);
        await consumer.seek({ topic: topicName, partition: 0, offset: '0' });
        await consumer.resume([{ topic: topicName, partition: 0 }]);
        startTime = process.hrtime.bigint(); // Reset timer after resume

        // Wait for all messages to be processed (30 original + 5 after seek)
        await waitFor(() => msgCount >= 35, () => null, { delay: 100, timeout: 10000 });

        // Assert latency is reasonable (e.g., < 4000ms)
        expect(maxLatencyMs).toBeLessThan(4000);

        // Assert all messages processed
        expect(msgCount).toBeGreaterThanOrEqual(35);
    });
});