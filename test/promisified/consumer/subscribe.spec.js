jest.setTimeout(30000);

const { ErrorCodes } = require('../../../lib').KafkaJS;
const { secureRandom,
    createTopic,
    waitFor,
    waitForMessages,
    waitForConsumerToJoinGroup,
    createProducer,
    createConsumer } = require('../testhelpers');

describe('Consumer', () => {
    let groupId, consumer, producer;

    beforeEach(async () => {
        groupId = `consumer-group-id-${secureRandom()}`;
        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 1,
            maxBytesPerPartition: 180,
            fromBeginning: true
        });

        producer = createProducer({});
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        producer && (await producer.disconnect());
    });

    describe('when subscribing to multiple topics', () => {
        it('throws an error if one of the topics is invalid', async () => {
            await consumer.connect();
            await expect(consumer.subscribe({ topics: [1] })).rejects.toHaveProperty(
                'code',
                ErrorCodes.ERR__INVALID_ARG,
            );
        });

        it('subscribes by topic name as a string or regex', async () => {
            const testScope = secureRandom();
            const regexMatchingTopic = `pattern-${testScope}-regex-${secureRandom()}`;
            const topics = [`topic-${secureRandom()}`, `topic-${secureRandom()}`, regexMatchingTopic];

            await Promise.all(topics.map(topic => createTopic({ topic })));

            const messagesConsumed = [];
            await consumer.connect();
            await consumer.subscribe({
                topics: [topics[0], topics[1], new RegExp(`^pattern-${testScope}-regex-.*`)],
            });

            consumer.run({ eachMessage: async event => messagesConsumed.push(event) });
            await waitFor(() => consumer.assignment().length > 0, () => null);

            await producer.connect();
            await producer.sendBatch({
                topicMessages: [
                    { topic: topics[0], messages: [{ key: 'drink', value: 'drink' }] },
                    { topic: topics[1], messages: [{ key: 'your', value: 'your' }] },
                    { topic: topics[2], messages: [{ key: 'tea', value: 'tea' }] },
                ],
            });

            await waitForMessages(messagesConsumed, { number: 3 });
            expect(messagesConsumed.map(m => m.message.value.toString())).toEqual(
                expect.arrayContaining(['drink', 'your', 'tea'])
            );
        });
    });

    describe('Deprecated "topic" interface', () => {
        describe('when subscribing', () => {
            it('throws an error if the topic is invalid', async () => {
                await consumer.connect();
                await expect(consumer.subscribe({ topic: null })).rejects.toHaveProperty(
                    'code',
                    ErrorCodes.ERR__INVALID_ARG
                );
            });

            it('throws an error if the topic is not a String or RegExp', async () => {
                await consumer.connect();
                await expect(consumer.subscribe({ topic: 1 })).rejects.toHaveProperty(
                    'code',
                    ErrorCodes.ERR__INVALID_ARG
                );
            });

            describe('with a string', () => {
                it('subscribes to the topic', async () => {
                    const topic = `topic-${secureRandom()}`;

                    await createTopic({ topic });

                    const messagesConsumed = [];
                    await consumer.connect();
                    await consumer.subscribe({ topic });

                    consumer.run({ eachMessage: async event => messagesConsumed.push(event) });
                    await waitForConsumerToJoinGroup(consumer);

                    await producer.connect();
                    await producer.sendBatch({
                        topicMessages: [{ topic, messages: [{ key: 'key-a', value: 'value-a' }] }],
                    });

                    await waitForMessages(messagesConsumed, { number: 1 });
                    expect(messagesConsumed.map(m => m.message.value.toString()).sort()).toEqual(['value-a']);
                });
            });

            describe('with regex', () => {
                it('subscribes to all matching topics', async () => {
                    const testScope = secureRandom();
                    const topicUS = `pattern-${testScope}-us-${secureRandom()}`;
                    const topicSE = `pattern-${testScope}-se-${secureRandom()}`;
                    const topicUK = `pattern-${testScope}-uk-${secureRandom()}`;
                    const topicBR = `pattern-${testScope}-br-${secureRandom()}`;

                    await Promise.all(
                        [topicUS, topicSE, topicUK, topicBR].map(topic => createTopic({ topic }))
                    );

                    const messagesConsumed = [];
                    await consumer.connect();
                    await consumer.subscribe({
                        topic: new RegExp(`pattern-${testScope}-(se|br)-.*`),
                    });

                    consumer.run({ eachMessage: async event => messagesConsumed.push(event) });
                    await waitFor(() => consumer.assignment().length > 0, () => null, 100);

                    await producer.connect();
                    await producer.sendBatch({
                        topicMessages: [
                            { topic: topicUS, messages: [{ key: `key-us`, value: `value-us` }] },
                            { topic: topicUK, messages: [{ key: `key-uk`, value: `value-uk` }] },
                            { topic: topicSE, messages: [{ key: `key-se`, value: `value-se` }] },
                            { topic: topicBR, messages: [{ key: `key-br`, value: `value-br` }] },
                        ],
                    });

                    await waitForMessages(messagesConsumed, { number: 2 });
                    expect(messagesConsumed.map(m => m.message.value.toString()).sort()).toEqual([
                        'value-br',
                        'value-se',
                    ]);
                });
            });
        });
    });
});

