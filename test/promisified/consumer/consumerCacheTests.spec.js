jest.setTimeout(30000)

const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    waitForMessages,
    sleep,
} = require('../testhelpers');

describe.each([[false], [true]])('Consumer message cache', (isAutoCommit) => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`
        groupId = `consumer-group-id-${secureRandom()}`

        await createTopic({ topic: topicName, partitions: 3 })

        producer = createProducer({});

        const common = {};
        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            autoCommit: isAutoCommit,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
    });

    it('is cleared on pause', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        consumer.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                if (event.partition === 0 && (+event.message.offset) === 1023) {
                    consumer.pause([{ topic: topicName, partitions: [0] }]);
                }
            }
        });

        /* Evenly distribute 1024*9 messages across 3 partitions */
        let i = 0;
        const messages = Array(1024 * 9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })

        await producer.send({ topic: topicName, messages })

        // Wait for the messages.
        // We consume 1024 messages from partition 0, and 1024*3 from partition 1 and 2.
        await waitForMessages(messagesConsumed, { number: 1024 * 7 });

        // We should not consume even one more message than that.
        await sleep(1000);
        expect(messagesConsumed.length).toEqual(1024 * 7);

        // check if all offsets are present
        // partition 0
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.message.offset)).toEqual(Array(1024).fill().map((_, i) => `${i}`));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.message.offset)).toEqual(Array(1024 * 3).fill().map((_, i) => `${i}`));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.message.offset)).toEqual(Array(1024 * 3).fill().map((_, i) => `${i}`));
    });

    it('is cleared on seek', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        let hasBeenSeeked = false;
        consumer.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                if (event.partition === 0 && (+event.message.offset) === 1023 && !hasBeenSeeked) {
                    consumer.seek({ topic: topicName, partition: 0, offset: 0 });
                    hasBeenSeeked = true;
                }
            }
        });

        /* Evenly distribute 1024*9 messages across 3 partitions */
        let i = 0;
        const messages = Array(1024 * 9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })

        await producer.send({ topic: topicName, messages })

        // Wait for the messages.
        // We consume 1024*4 messages from partition 0, and 1024*3 from partition 1 and 2.
        await waitForMessages(messagesConsumed, { number: 1024 * 10 });

        // We should not consume even one more message than that.
        await sleep(1000);
        expect(messagesConsumed.length).toEqual(1024 * 10);

        // check if all offsets are present
        // partition 0
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.message.offset))
            .toEqual(Array(1024 * 4).fill().map((_, i) => i < 1024 ? `${i}` : `${i - 1024}`));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.message.offset)).toEqual(Array(1024 * 3).fill().map((_, i) => `${i}`));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.message.offset)).toEqual(Array(1024 * 3).fill().map((_, i) => `${i}`));
    });

    it('is cleared before rebalance', async () => {
        const consumer2 = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            autoCommit: isAutoCommit,
        });

        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        const messagesConsumedConsumer1 = [];
        const messagesConsumedConsumer2 = [];
        let consumer2ConsumeRunning = false;

        consumer.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                messagesConsumedConsumer1.push(event);
                if (!isAutoCommit)
                    await consumer.commitOffsets([
                        { topic: event.topic, partition: event.partition, offset: Number(event.message.offset) + 1 },
                    ]);

                /* Until the second consumer joins, consume messages slowly so as to not consume them all
                 * before the rebalance triggers. */
                if (messagesConsumed.length > 1024 && !consumer2ConsumeRunning) {
                    await sleep(10);
                }
            }
        });

        /* Evenly distribute 1024*9 messages across 3 partitions */
        let i = 0;
        const messages = Array(1024 * 10)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: (i++) % 3 }
            })

        await producer.send({ topic: topicName, messages })

        // Wait for the messages - some of them, before starting the
        // second consumer.
        await waitForMessages(messagesConsumed, { number: 1024 });

        await consumer2.connect();
        await consumer2.subscribe({ topic: topicName });
        consumer2.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                messagesConsumedConsumer2.push(event);
            }
        });

        await waitFor(() => consumer2.assignment().length > 0, () => null);
        consumer2ConsumeRunning = true;

        /* Now that both consumers have joined, wait for all msgs to be consumed */
        await waitForMessages(messagesConsumed, { number: 1024 * 10 });

        /* No extra messages should be consumed. */
        await sleep(1000);
        expect(messagesConsumed.length).toEqual(1024 * 10);

        /* Check if all messages were consumed. */
        expect(messagesConsumed.map(event => (+event.message.offset)).sort((a, b) => a - b))
            .toEqual(Array(1024 * 10).fill().map((_, i) => Math.floor(i / 3)));

        /* Consumer2 should have consumed at least one message. */
        expect(messagesConsumedConsumer2.length).toBeGreaterThan(0);

        await consumer2.disconnect();
    });

    it('does not hold up polling for non-message events', async () => {
        /* Even if the cache is full of messages, we should still be polling for
         * non-message events like rebalances, etc. Internally, this is to make sure that
         * we call poll() at least once within max.poll.interval.ms even if the cache is
         * still full. This depends on us expiring the cache on time. */
        const impatientConsumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            rebalanceTimeout: 10000, /* also changes max.poll.interval.ms */
            sessionTimeout: 10000,
            autoCommitInterval: 1000,
            clientId: "impatientConsumer",
            autoCommit: isAutoCommit,
        });

        await producer.connect();
        await impatientConsumer.connect();
        await impatientConsumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        let impatientConsumerMessages = [];
        let consumer1Messages = [];
        let consumer1TryingToJoin = false;

        impatientConsumer.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                impatientConsumerMessages.push(event);
                if (!isAutoCommit)
                    await impatientConsumer.commitOffsets([
                        { topic: event.topic, partition: event.partition, offset: Number(event.message.offset) + 1 },
                    ]);

                /* When the second consumer is joining, deliberately slow down message consumption.
                 * This is so the cache remains full.
                 * We should still have a rebalance very soon, since we will expire the cache and
                 * trigger a rebalance before max.poll.interval.ms.
                 */
                if (consumer1TryingToJoin) {
                    await sleep(1000);
                }
            }
        });

        /* Distribute 1024*10 messages across 3 partitions */
        let i = 0;
        const messages = Array(1024 * 10)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: (i++) % 3 }
            })

        await producer.send({ topic: topicName, messages })

        /* Wait for the messages - some of them, before starting the
         * second consumer.
         * FIXME: This can get a bit flaky depending on the system, as sometimes
         * the impatientConsumer consumes all the messages before consumer1TryingToJoin
         * can be set to true  */
        await waitForMessages(messagesConsumed, { number: 1024, delay: 100 });
        consumer1TryingToJoin = true;

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({
            eachMessage: async event => {
                messagesConsumed.push(event);
                consumer1Messages.push(event);
            }
        });
        await waitFor(() => consumer.assignment().length > 0, () => null);
        consumer1TryingToJoin = false;

        /* Now that both consumers have joined, wait for all msgs to be consumed */
        await waitForMessages(messagesConsumed, { number: 1024 * 10 });

        // No extra messages should be consumed.
        await sleep(1000);
        expect(messagesConsumed.length).toEqual(1024 * 10);

        /* Each consumer should have consumed at least one message. */
        expect(consumer1Messages.length).toBeGreaterThan(0);
        expect(impatientConsumerMessages.length).toBeGreaterThan(0);

        await impatientConsumer.disconnect();
    }, 60000);
});
