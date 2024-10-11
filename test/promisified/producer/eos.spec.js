jest.setTimeout(30000);

const {
    secureRandom,
    createConsumer,
    createProducer,
    createTopic,
    waitForMessages,
} = require('../testhelpers');
const { ErrorCodes } = require('../../../lib').KafkaJS;

describe('Producer > Transactional producer', () => {
    let producer, basicProducer, topicName, topicName2, transactionalId, message, consumer, groupId;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        topicName2 = `test-topic2-${secureRandom()}`;
        transactionalId = `transactional-id-${secureRandom()}`;
        message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` };
        groupId = `group-id-${secureRandom()}`;

        producer = createProducer({
            idempotent: true,
            transactionalId,
            transactionTimeout: 1000,
        });

        basicProducer = createProducer({});

        consumer = createConsumer({ groupId, autoCommit: false, fromBeginning: true });

        await createTopic({ topic: topicName, partitions: 1 });
        await createTopic({ topic: topicName2 });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        producer && (await producer.disconnect());
        basicProducer && (await basicProducer.disconnect());
    });

    it('fails when using consumer group id while sending offsets from transactional producer', async () => {
        await producer.connect();
        await basicProducer.connect();
        await consumer.connect();

        await basicProducer.send({ topic: topicName, messages: [message] });

        await consumer.subscribe({ topic: topicName });

        let messagesConsumed = [];
        await consumer.run({
            eachMessage: async ({ message }) => {
                const transaction = await producer.transaction();
                await transaction.send({ topic: topicName, messages: [message] });

                await expect(
                    transaction.sendOffsets({ consumerGroupId: groupId })).rejects.toHaveProperty('code', ErrorCodes.ERR__INVALID_ARG);
                await expect(
                    transaction.sendOffsets({ consumerGroupId: groupId, consumer })).rejects.toHaveProperty('code', ErrorCodes.ERR__INVALID_ARG);

                await transaction.abort();
                messagesConsumed.push(message);
            }
        });

        await waitForMessages(messagesConsumed, { number: 1 });
        expect(messagesConsumed.length).toBe(1);
    });

    it('sends offsets when transaction is committed', async () => {
        await producer.connect();
        await basicProducer.connect();
        await consumer.connect();

        await basicProducer.send({ topic: topicName, messages: [message] });

        await consumer.subscribe({ topic: topicName });

        let messagesConsumed = [];
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const transaction = await producer.transaction();
                await transaction.send({ topic: topicName2, messages: [message] });

                await transaction.sendOffsets({ consumer, topics: [
                    {
                        topic,
                        partitions: [
                            { partition, offset: Number(message.offset) + 1 },
                        ],
                    }
                ], });

                await transaction.commit();
                messagesConsumed.push(message);
            }
        });

        await waitForMessages(messagesConsumed, { number: 1 });
        expect(messagesConsumed.length).toBe(1);
        const committed = await consumer.committed();
        expect(committed).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    topic: topicName,
                    offset: '1',
                    partition: 0,
                }),
            ])
        );
    });

    it('sends no offsets when transaction is aborted', async () => {
        await producer.connect();
        await basicProducer.connect();
        await consumer.connect();

        await basicProducer.send({ topic: topicName, messages: [message] });

        await consumer.subscribe({ topic: topicName });

        let messagesConsumed = [];
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const transaction = await producer.transaction();
                await transaction.send({ topic: topicName2, messages: [message] });

                await transaction.sendOffsets({ consumer, topics: [
                    {
                        topic,
                        partitions: [
                            { partition, offset: Number(message.offset) + 1 },
                        ],
                    }
                ], });

                await transaction.abort();
                messagesConsumed.push(message);
            }
        });

        await waitForMessages(messagesConsumed, { number: 1 });
        expect(messagesConsumed.length).toBe(1);
        const committed = await consumer.committed();
        expect(committed).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    topic: topicName,
                    offset: null,
                    partition: 0,
                }),
            ])
        );
    });
});
