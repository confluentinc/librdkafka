jest.setTimeout(10000);

const {
    secureRandom,
    createTopic,
    waitForMessages,
    createProducer,
    createConsumer,
} = require('../testhelpers');
const { KafkaJSError } = require('../../../lib').KafkaJS;

describe('Producer > Idempotent producer', () => {
    let producer, consumer, topicName, cluster, messages;

    beforeAll(async () => {
        messages = Array(4)
            .fill()
            .map((_, i) => {
                const value = secureRandom();
                return { key: `key-${value}`, value: `${i}` };
            });
    });

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        producer = createProducer({
            idempotent: true,
        });
        consumer = createConsumer({
            groupId: `consumer-group-id-${secureRandom()}`,
            maxWaitTimeInMs: 0,
            fromBeginning: true,
        });
        await createTopic({ topic: topicName, partitions: 1 });
        await Promise.all([producer.connect(), consumer.connect()]);
        await consumer.subscribe({ topic: topicName });
    });

    afterEach(
        async () =>
            await Promise.all([
                producer && (await producer.disconnect()),
                consumer && (await consumer.disconnect()),
            ])
    );

    it('sequential produce() calls > all messages are written to the partition once, in order', async () => {
        const messagesConsumed = [];

        for (const m of messages) {
            await producer.send({ topic: topicName, messages: [m] });
        }

        await consumer.run({ eachMessage: async message => messagesConsumed.push(message) });
        await waitForMessages(messagesConsumed, { number: messages.length });

        messagesConsumed.forEach(({ message: { value } }, i) =>
            expect(value.toString()).toEqual(`${i}`)
        );
    });

    /* Skip as we don't have the mock broker available */
    it.skip('sequential produce() calls > where produce() throws a retriable error, all messages are written to the partition once, in order', async () => {
        for (const nodeId of [0, 1, 2]) {
            const broker = await cluster.findBroker({ nodeId });

            const brokerProduce = jest.spyOn(broker, 'produce');
            brokerProduce.mockImplementationOnce(() => {
                throw new KafkaJSError('retriable error');
            });
        }

        const messagesConsumed = [];

        for (const m of messages) {
            await producer.send({ acks: -1, topic: topicName, messages: [m] });
        }

        await consumer.run({ eachMessage: async message => messagesConsumed.push(message) });

        await waitForMessages(messagesConsumed, { number: messages.length });

        messagesConsumed.forEach(({ message: { value } }, i) =>
            expect(value.toString()).toEqual(`${i}`)
        );
    });

    /* Skip as we don't have the mock broker available */
    it.skip('sequential produce() calls > where produce() throws a retriable error after the message is written to the log, all messages are written to the partition once, in order', async () => {
        for (const nodeId of [0, 1, 2]) {
            const broker = await cluster.findBroker({ nodeId });
            const originalCall = broker.produce.bind(broker);
            const brokerProduce = jest.spyOn(broker, 'produce');
            brokerProduce.mockImplementationOnce();
            brokerProduce.mockImplementationOnce();
            brokerProduce.mockImplementationOnce(async (...args) => {
                await originalCall(...args);
                throw new KafkaJSError('retriable error');
            });
        }

        const messagesConsumed = [];

        for (const m of messages) {
            await producer.send({ acks: -1, topic: topicName, messages: [m] });
        }

        await consumer.run({ eachMessage: async message => messagesConsumed.push(message) });

        await waitForMessages(messagesConsumed, { number: messages.length });

        messagesConsumed.forEach(({ message: { value } }, i) =>
            expect(value.toString()).toEqual(`${i}`)
        );
    });

    it('concurrent produce() calls > all messages are written to the partition once', async () => {
        const messagesConsumed = [];

        await Promise.all(
            messages.map(m => producer.send({ topic: topicName, messages: [m] }))
        );

        await consumer.run({ eachMessage: async message => messagesConsumed.push(message) });

        await waitForMessages(messagesConsumed, { number: messages.length });
        expect(messagesConsumed).toHaveLength(messages.length);
    });
});
