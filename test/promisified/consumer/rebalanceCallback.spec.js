jest.setTimeout(30000);

const { waitFor,
    secureRandom,
    createTopic,
    createConsumer,
    createProducer,
    sleep, } = require("../testhelpers");
const { ErrorCodes } = require('../../../lib').KafkaJS;

describe('Consumer', () => {
    let consumer;
    let groupId, topicName;
    let consumerConfig;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;
        consumerConfig = {
            groupId,
        };
        consumer = null;
        await createTopic({ topic: topicName, partitions: 3 });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
    });

    it('calls rebalance callback', async () => {
        let calls = 0;
        consumer = createConsumer(consumerConfig, {
            rebalance_cb: function () {
                calls++;
            }
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });
        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(calls).toBe(1); /* assign */
        await consumer.disconnect();
        expect(calls).toBe(2); /* assign + unassign */
        consumer = null;
    });

    it('allows modifying the assignment via returns', async () => {
        consumer = createConsumer(consumerConfig, {
            rebalance_cb: function (err, assignment) {
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    expect(assignment.length).toBe(3);
                    return assignment.filter(a => a.partition !== 0);
                }
            }
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });
        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(consumer.assignment().length).toBe(2);
        expect(consumer.assignment()).toEqual(
            expect.arrayContaining([
                { topic: topicName, partition: 1 },
                { topic: topicName, partition: 2 }]));
    });

    it('allows modifying the assigment via assignment functions', async () => {
        let calls = 0;
        consumer = createConsumer(consumerConfig, {
            rebalance_cb: function (err, assignment, assignmentFns) {
                calls++;
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    expect(assignment.length).toBe(3);
                    assignmentFns.assign(assignment.filter(a => a.partition !== 0));
                } else {
                    assignmentFns.unassign(assignment);
                }
            }
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });
        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(consumer.assignment().length).toBe(2);
        expect(consumer.assignment()).toEqual(
            expect.arrayContaining([
                { topic: topicName, partition: 1 },
                { topic: topicName, partition: 2 }]));
        await consumer.disconnect();
        expect(calls).toBe(2);
        consumer = null;
    });

    it('pauses correctly from the rebalance callback after assign', async () => {
        consumer = createConsumer(consumerConfig, {
            rebalance_cb: function (err, assignment, assignmentFns) {
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    expect(assignment.length).toBe(3);

                    /* Assign first so we can pause. */
                    assignmentFns.assign(assignment);

                    /* Convert the assignment into format suitable for pause argument. */
                    const pausablePartitions = [{ topic: topicName, partitions: [0, 1, 2] }];
                    consumer.pause(pausablePartitions);
                } else {
                    assignmentFns.unassign(assignment);
                }
            }
        });

        let messagesConsumed = [];
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async (e) => { messagesConsumed.push(e); } });
        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        const producer = createProducer({});
        await producer.connect();
        const key1 = secureRandom();
        for (const partition of [0, 1, 2]) {
            const message = { key: `key-${key1}`, value: `value-${key1}`, partition };
            await producer.send({
                topic: topicName,
                messages: [message],
            });
        }
        await producer.disconnect();

        expect(consumer.paused()).toEqual([{ topic: topicName, partitions: [0, 1, 2] }]);

        /* Give it some extra time just in case - should be enough to get the messages if a partition isn't paused. */
        await sleep(1000);
        expect(messagesConsumed.length).toBe(0);

        consumer.resume([ { topic: topicName } ]);
        await waitFor(() => messagesConsumed.length === 3, () => null, 1000);
        expect(messagesConsumed.length).toBe(3);
    });
});