jest.setTimeout(30000)

const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    sleep,
} = require('../testhelpers');
const { ErrorCodes } = require('../../../lib').KafkaJS;

describe.each([[false], [true]])('Consumer store', (isAutoCommit) => {
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
            autoCommit: isAutoCommit,
            autoCommitInterval: 500,
        }, {
            'enable.auto.offset.store': false,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
    });

    it('should not work if enable.auto.offset.store = true', async () => {
        let assignment = [];
        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
        }, {
            /* Set to true manually - the default value with kafkaJS block is false. */
            'enable.auto.offset.store': true,
            'rebalance_cb': function (err, asg) {
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    assignment = asg;
                }
            }
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName })
        await consumer.run({
            eachMessage: async () => {
            }
        });
        await waitFor(() => assignment.length > 0, () => null, 1000);
        expect(
            () => consumer.storeOffsets([{ topic: topicName, partition: 0, offset: '10' }])
        ).toThrow(/Store can only be called when enable.auto.offset.store is explicitly set to false/);
    });

    it('should not work if enable.auto.offset.store is unset', async () => {
        let assignment = [];
        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
        }, {
            /* Set to true manually - the default value with kafkaJS block is false. */
            'rebalance_cb': function (err, asg) {
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    assignment = asg;
                }
            }
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName })
        await consumer.run({
            eachMessage: async () => {
            }
        });
        await waitFor(() => assignment.length > 0, () => null, 1000);
        expect(
            () => consumer.storeOffsets([{ topic: topicName, partition: 0, offset: '10' }])
        ).toThrow(/Store can only be called when enable.auto.offset.store is explicitly set to false/);
    });

    it('should commit stored offsets', async () => {
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
                expect(() => consumer.storeOffsets([{ topic, partition, offset }])).not.toThrow();
            }
        });
        await waitFor(() => msgCount >= 30, () => null, { delay: 100 });
        expect(msgCount).toEqual(30);

        if (!isAutoCommit)
            await expect(consumer.commitOffsets()).resolves.toBeUndefined();
        else
            await sleep(1000); /* Wait for auto-commit */

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

});
