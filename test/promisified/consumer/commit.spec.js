jest.setTimeout(30000)

const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    sleep,
} = require('../testhelpers');

describe('Consumer commit', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`
        groupId = `consumer-group-id-${secureRandom()}`

        await createTopic({ topic: topicName, partitions: 3 })

        producer = createProducer({});

        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            autoCommit: false,
            autoCommitInterval: 500,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
    });

    it('should commit offsets', async () => {
        /* Evenly distribute 30 messages across 3 partitions */
        let i = 0;
        const messages = Array(3 * 10)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: (i++) % 3 }
            })

        await producer.connect();
        await producer.send({ topic: topicName, messages })
        await producer.flush();

        let msgCount = 0;
        await consumer.connect();
        await consumer.subscribe({ topic: topicName })
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                msgCount++;
                const offset = (Number(message.offset) + 1).toString();
                await expect(() => consumer.commitOffsets([{ topic, partition, offset }])).not.toThrow();
            }
        });
        await waitFor(() => msgCount >= 30, () => null, { delay: 100 });
        expect(msgCount).toEqual(30);

        await consumer.disconnect();

        /* Send 30 more messages */
        await producer.send({ topic: topicName, messages })
        await producer.flush();


        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
        });

        msgCount = 0;
        await consumer.connect();
        await consumer.subscribe({ topic: topicName })
        await consumer.run({
            eachMessage: async ({ message }) => {
                msgCount++;
            }
        })
        /* Only the extra 30 messages should come to us */
        await waitFor(() => msgCount >= 30, () => null, { delay: 100 });
        await sleep(1000);
        expect(msgCount).toEqual(30);
    });

    it('should commit offsets with metadata', async () => {
        /* Evenly distribute 30 messages across 3 partitions */
        let i = 0;
        const messages = Array(3 * 10)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: (i++) % 3 }
            })

        await producer.connect();
        await producer.send({ topic: topicName, messages })
        await producer.flush();

        let msgCount = 0;
        const metadata = 'unicode-metadata-😊';
        await consumer.connect();
        await consumer.subscribe({ topic: topicName })
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                msgCount++;
                const offset = (Number(message.offset) + 1).toString();
                const leaderEpoch = message.leaderEpoch;
                await expect(() => consumer.commitOffsets([{ topic, partition, offset, metadata, leaderEpoch }])).not.toThrow();
            }
        });
        await waitFor(() => msgCount >= 30, () => null, { delay: 100 });
        expect(msgCount).toEqual(30);

        let committed = await consumer.committed(null, 5000);
        expect(committed).toEqual([
            { topic: topicName, partition: 0, offset: '10', metadata, leaderEpoch: expect.any(Number) },
            { topic: topicName, partition: 1, offset: '10', metadata, leaderEpoch: expect.any(Number) },
            { topic: topicName, partition: 2, offset: '10', metadata, leaderEpoch: expect.any(Number) }
        ]);

        await consumer.disconnect();

        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
        });

        msgCount = 0;
        await consumer.connect();
        await consumer.subscribe({ topic: 'not-a-real-topic-name' })

        /* At this point, we're not actually assigned anything, but we should be able to fetch
         * the stored offsets and metadata anyway since we're of the same consumer group. */
        committed = await consumer.committed([
            { topic: topicName, partition: 0 },
            { topic: topicName, partition: 1 },
            { topic: topicName, partition: 2 },
        ]);
        expect(committed).toEqual([
            { topic: topicName, partition: 0, offset: '10', metadata,leaderEpoch: expect.any(Number) },
            { topic: topicName, partition: 1, offset: '10', metadata,leaderEpoch: expect.any(Number) },
            { topic: topicName, partition: 2, offset: '10', metadata,leaderEpoch: expect.any(Number) }
        ]);
    });
});
